//! Full frozen dataset/model parity verification and publication evidence.
//!
//! Unlike runtime replay, this verifier needs no prior live report. It reloads
//! the immutable Parquet v2 and model artifact from their content addresses,
//! recomputes the model-input transform that can be derived from the frozen
//! rows, and persists a subject-bound `Full` parity run. This is the safe
//! bootstrap path for a newly trained model while the global parity latch is
//! intentionally still uninitialized/open.

use std::sync::Arc;

use chrono::Utc;
use quant_pivot_error::{QuantError, QuantResult, research::ResearchError, storage::StorageError};
use quant_pivot_models::{
    clickhouse::QuantFeatureParityEventRow,
    domain::{
        data_plane::DecisionBoundary,
        quant::{
            CompleteFeatureParityRun, FeatureParityRunInfo, ModelSpecInfo, ModelVersionInfo,
            ModelVersionParityEvidence, NewFeatureParityRun, NewFrozenModelParitySubject,
            TrainingDatasetInfo, model_version_parity_evidence_hash,
        },
    },
    enums::{
        model::ClassicalKind,
        quant::{
            FeatureParityEventStatus, FeatureParityRunKind, FeatureParityRunStatus,
            FeatureParityStage, FeatureParityStateTransition, PublicationStatus,
            TrainingDatasetStatus,
        },
    },
    types::{
        ContentHash, DiagnosticCode, FeatureParityDetail, FeatureParityDetailSource,
        FeatureParityEventId, FeatureParityRunId, ModelInputContract, RoleCode,
    },
};
use quant_pivot_repository::traits::{
    FactWriter, FeatureParityRepository, ModelRegistryRepository, TrainingDatasetRepository,
};
use quant_pivot_research::{
    artifact::ArtifactStore,
    factors::FrozenReferenceQuantiles,
    hashing::ResearchHasher,
    model::{
        ClassicalModelArtifact, ClassicalOutputSemantics, LabelSelector, ModelArtifact,
        SellScorerArtifact, WeightedFactorModelArtifact, fit_frozen_reference_quantiles,
        load_hash_verified_artifact, model_input_contract_hash, weighted_training_input_hash,
    },
    training::{LabelName, RETURN_TO_HORIZON, SETTLEMENT_OUTCOME, TrainingExample},
};
#[cfg(feature = "ml-classical")]
use quant_pivot_research::{
    model::{ClassicalRuntime, FittedInputTransform},
    training::{FeatureColumnSpec, FeatureMatrixSpec, build_training_matrix, training_input_hash},
};
use serde::Serialize;

use crate::service::training_dataset::{
    require_dataset_materialization, verify_frozen_dataset_artifact,
};

/// Persistence and content-addressed stores used by frozen model parity.
pub struct FrozenModelParityDeps {
    pub dataset_repo: Arc<dyn TrainingDatasetRepository>,
    pub model_registry_repo: Arc<dyn ModelRegistryRepository>,
    pub parity_repo: Arc<dyn FeatureParityRepository>,
    pub artifact_store: Arc<dyn ArtifactStore>,
    pub evidence_writer: Arc<dyn FactWriter<QuantFeatureParityEventRow>>,
}

/// Verifies an immutable model version against its exact frozen training input.
pub struct FrozenModelParityService {
    deps: FrozenModelParityDeps,
}

struct FrozenParityProof {
    feature_contract_hash: ContentHash,
    transform_hash: ContentHash,
    evidence_rows: Vec<QuantFeatureParityEventRow>,
}

impl FrozenModelParityService {
    #[must_use]
    pub const fn new(deps: FrozenModelParityDeps) -> Self {
        Self { deps }
    }

    /// Verify and persist one model/dataset-bound full run. A prior successful
    /// run for the same immutable subject is reused; a later failed run is never
    /// hidden by an older pass.
    pub async fn verify_and_record(
        &self,
        version: &ModelVersionInfo,
        triggered_by: &str,
        reason: &str,
    ) -> QuantResult<FeatureParityRunInfo> {
        let (dataset, spec) = self.load_subject(version).await?;
        if let Some(existing) = self
            .deps
            .parity_repo
            .latest_full_for_model(&version.model_version_id, &dataset.training_dataset_id)
            .await?
            && existing.status == FeatureParityRunStatus::Passed
        {
            validate_passed_model_parity_run(&existing, version, &dataset)?;
            return Ok(existing);
        }

        let materialization = require_dataset_materialization(&dataset)?;
        let run_id = FeatureParityRunId::from_v7();
        let frozen_subject = NewFrozenModelParitySubject {
            model_version_id: version.model_version_id.clone(),
            training_dataset_id: dataset.training_dataset_id.clone(),
            subject_generation: version.artifact_hash.clone(),
            evidence_hash: model_version_parity_evidence_hash(&ModelVersionParityEvidence {
                model_version_id: &version.model_version_id,
                model_spec_id: &version.model_spec_id,
                artifact_hash: &version.artifact_hash,
                training_dataset_id: &dataset.training_dataset_id,
                dataset_hash: materialization.dataset_hash,
                manifest_hash: materialization.manifest_hash,
                artifact_bytes_hash: materialization.artifact_bytes_hash,
            })?,
        };
        let queued = self
            .deps
            .parity_repo
            .create_frozen_model_run(
                NewFeatureParityRun {
                    run_id: run_id.clone(),
                    kind: FeatureParityRunKind::Full,
                    status: FeatureParityRunStatus::Queued,
                    window_start: dataset.window_start,
                    window_end: dataset.window_end,
                    report_id: None,
                    model_version_id: Some(version.model_version_id.clone()),
                    training_dataset_id: Some(dataset.training_dataset_id.clone()),
                    triggered_by: triggered_by.to_owned(),
                    requested_by: None,
                    acting_role: RoleCode::new("system"),
                    reason: reason.to_owned(),
                    total_count: 0,
                    compared_count: 0,
                    matched_count: 0,
                    mismatched_count: 0,
                    pending_materialization_count: 0,
                    feature_contract_hash: Some(materialization.feature_schema_hash.clone()),
                    transform_hash: None,
                    failure_code: None,
                    failure_detail: None,
                    started_at: None,
                    pending_since: None,
                    containment_completed_at: None,
                    finished_at: None,
                },
                frozen_subject,
            )
            .await?;
        self.deps.parity_repo.mark_running(&queued.run_id).await?;

        match self.verify_subject(&run_id, version, &dataset, &spec).await {
            Ok(proof) => self.record_pass(&run_id, proof).await,
            Err(error) => {
                self.fail_integrity(&run_id, materialization.feature_schema_hash, &error)
                    .await?;
                Err(error)
            }
        }
    }

    async fn record_pass(
        &self,
        run_id: &FeatureParityRunId,
        proof: FrozenParityProof,
    ) -> QuantResult<FeatureParityRunInfo> {
        let total_count = i64::try_from(proof.evidence_rows.len()).map_err(|error| {
            ResearchError::Determinism {
                detail: format!("frozen parity evidence count does not fit i64: {error}"),
            }
        })?;
        if let Err(storage_error) = self
            .deps
            .evidence_writer
            .write_batch(proof.evidence_rows)
            .await
        {
            let error = QuantError::from(storage_error);
            self.fail_integrity(run_id, &proof.feature_contract_hash, &error)
                .await?;
            return Err(error);
        }
        self.deps
            .parity_repo
            .complete_run(
                run_id,
                CompleteFeatureParityRun {
                    status: FeatureParityRunStatus::Passed,
                    total_count,
                    compared_count: total_count,
                    matched_count: total_count,
                    mismatched_count: 0,
                    pending_materialization_count: 0,
                    feature_contract_hash: Some(proof.feature_contract_hash),
                    transform_hash: Some(proof.transform_hash),
                    failure_code: None,
                    failure_detail: None,
                },
            )
            .await
            .map_err(Into::into)
    }

    /// Require the newest exact-subject full run to be a complete pass. This is
    /// the service-level publication guard; the repository repeats the exact
    /// run binding inside the publication transaction.
    pub async fn require_passed(
        &self,
        version: &ModelVersionInfo,
    ) -> QuantResult<FeatureParityRunInfo> {
        let (dataset, _) = self.load_subject(version).await?;
        let run = self
            .deps
            .parity_repo
            .latest_full_for_model(&version.model_version_id, &dataset.training_dataset_id)
            .await?
            .ok_or_else(|| ResearchError::Determinism {
                detail: format!(
                    "model version {} has no full parity run bound to training dataset {}",
                    version.model_version_id, dataset.training_dataset_id
                ),
            })?;
        validate_passed_model_parity_run(&run, version, &dataset)?;
        Ok(run)
    }

    async fn load_subject(
        &self,
        version: &ModelVersionInfo,
    ) -> QuantResult<(TrainingDatasetInfo, ModelSpecInfo)> {
        if !matches!(
            version.publication_status,
            PublicationStatus::Candidate | PublicationStatus::Shadow | PublicationStatus::Retired
        ) {
            return Err(ResearchError::Determinism {
                detail: format!(
                    "frozen model parity only verifies candidate, shadow, or retired rollback subjects, got {} for {}",
                    version.publication_status.as_str(),
                    version.model_version_id
                ),
            }
            .into());
        }
        let dataset_id =
            version
                .training_dataset_id
                .as_ref()
                .ok_or_else(|| ResearchError::Determinism {
                    detail: format!(
                        "model version {} has no training_dataset_id",
                        version.model_version_id
                    ),
                })?;
        let dataset = self
            .deps
            .dataset_repo
            .find_by_id(dataset_id)
            .await?
            .ok_or_else(|| StorageError::NotFound {
                entity: "training_dataset",
                id: dataset_id.to_string(),
            })?;
        if dataset.status != TrainingDatasetStatus::Ready
            || dataset.model_spec_id != version.model_spec_id
        {
            return Err(ResearchError::Determinism {
                detail: format!(
                    "model {} is not bound to a Ready dataset owned by spec {}",
                    version.model_version_id, version.model_spec_id
                ),
            }
            .into());
        }
        let spec = self
            .deps
            .model_registry_repo
            .find_model_spec_by_id(&version.model_spec_id)
            .await?
            .ok_or_else(|| StorageError::NotFound {
                entity: "model_spec",
                id: version.model_spec_id.to_string(),
            })?;
        spec.input_contract.validate().map_err(|detail| {
            QuantError::from(ResearchError::Determinism {
                detail: format!("invalid model input contract: {detail}"),
            })
        })?;
        spec.training_contract.validate().map_err(|detail| {
            QuantError::from(ResearchError::Determinism {
                detail: format!("invalid model training contract: {detail}"),
            })
        })?;
        let recomputed_spec_hash = spec.definition().content_hash().map_err(|error| {
            QuantError::from(ResearchError::Determinism {
                detail: format!("model spec definition hash failed: {error}"),
            })
        })?;
        if recomputed_spec_hash != spec.definition_hash {
            return Err(ResearchError::Determinism {
                detail: format!(
                    "model spec {} definition hash does not match its immutable fields",
                    spec.model_spec_id
                ),
            }
            .into());
        }
        Ok((dataset, spec))
    }

    async fn verify_subject(
        &self,
        run_id: &FeatureParityRunId,
        version: &ModelVersionInfo,
        dataset: &TrainingDatasetInfo,
        spec: &ModelSpecInfo,
    ) -> QuantResult<FrozenParityProof> {
        let materialization = require_dataset_materialization(dataset)?;
        let parquet = self
            .deps
            .artifact_store
            .get(materialization.parquet_uri)
            .await?;
        let examples = verify_frozen_dataset_artifact(dataset, &parquet)?;
        if examples.is_empty() {
            return Err(ResearchError::Determinism {
                detail: "full frozen model parity requires at least one committed dataset row"
                    .to_owned(),
            }
            .into());
        }
        let artifact = load_hash_verified_artifact(&self.deps.artifact_store, version).await?;
        let header = artifact.header();
        let manifest =
            dataset
                .manifest_json
                .as_ref()
                .ok_or_else(|| ResearchError::Determinism {
                    detail: format!(
                        "dataset {} is missing its immutable manifest",
                        dataset.training_dataset_id
                    ),
                })?;
        let model_spec_definition_hash = &spec.definition_hash;
        if header.model_version_id != version.model_version_id
            || &manifest.model_spec_definition_hash != model_spec_definition_hash
            || &header.model_spec_definition_hash != model_spec_definition_hash
            || header.model_family != spec.model_family
            || version.model_family != spec.model_family
            || header.feature_schema_hash != *materialization.feature_schema_hash
            || (!header.model_family.is_classical()
                && header.factor_schema_hash != *materialization.factor_schema_hash)
        {
            return Err(ResearchError::Determinism {
                detail: format!(
                    "model artifact {} header/family/schema bindings disagree with frozen dataset {}",
                    version.model_version_id, dataset.training_dataset_id
                ),
            }
            .into());
        }

        let label = LabelSelector {
            name: LabelName::new(spec.training_contract.target_label_name.clone()),
            horizon_secs: spec.training_contract.target_label_horizon_secs,
        };
        let (transform_hash, training_input_hash) = match &artifact {
            ModelArtifact::WeightedFactor(weighted) => verify_weighted_artifact(
                &examples,
                &label,
                weighted,
                materialization.dataset_hash,
                &spec.input_contract,
            )?,
            ModelArtifact::Classical(classical) => {
                verify_classical_artifact_binding(
                    classical,
                    materialization.dataset_hash,
                    materialization.label_schema_hash,
                    spec,
                )?;
                #[cfg(feature = "ml-classical")]
                {
                    let transform_hash = self.verify_classical(&examples, spec, classical).await?;
                    (transform_hash, classical.training_input_hash.clone())
                }
                #[cfg(not(feature = "ml-classical"))]
                {
                    return Err(classical_runtime_unavailable(classical));
                }
            }
            ModelArtifact::SellScorer(sell_artifact) => verify_sell_scorer_artifact(
                &examples,
                &label,
                sell_artifact,
                materialization.dataset_hash,
                materialization.label_schema_hash,
                &spec.input_contract,
            )?,
        };
        let evidence_request = FrozenEvidenceRequest {
            run_id,
            version,
            dataset,
            examples: &examples,
            feature_contract_hash: materialization.feature_schema_hash,
            dataset_hash: materialization.dataset_hash,
            training_input_hash: &training_input_hash,
            transform_hash: &transform_hash,
        };
        let evidence_rows = frozen_model_evidence_rows(&evidence_request)?;
        Ok(FrozenParityProof {
            feature_contract_hash: materialization.feature_schema_hash.clone(),
            transform_hash,
            evidence_rows,
        })
    }

    #[cfg(feature = "ml-classical")]
    async fn verify_classical(
        &self,
        examples: &[TrainingExample],
        spec: &ModelSpecInfo,
        artifact: &ClassicalModelArtifact,
    ) -> QuantResult<ContentHash> {
        let matrix_spec = FeatureMatrixSpec {
            columns: artifact
                .input_transform
                .inputs
                .iter()
                .map(|input| FeatureColumnSpec {
                    feature: input.feature.clone(),
                    unit: input.unit,
                    value_kind: input.value_kind,
                    required: input.required,
                })
                .collect(),
            label_name: LabelName::new(spec.training_contract.target_label_name.clone()),
            label_horizon_secs: spec.training_contract.target_label_horizon_secs,
        };
        let matrix = build_training_matrix(examples, &matrix_spec)?;
        let (refitted, standardized) = FittedInputTransform::fit(&matrix)?;
        let refitted_hash = refitted.transform_hash()?;
        if refitted != artifact.input_transform
            || refitted_hash != artifact.input_transform_hash
            || training_input_hash(&standardized, &matrix.labels)? != artifact.training_input_hash
        {
            return Err(ResearchError::Determinism {
                detail:
                    "classical fitted transform/training-input bytes differ from the frozen dataset"
                        .to_owned(),
            }
            .into());
        }
        let estimator_bytes = self
            .deps
            .artifact_store
            .get(&artifact.serialized_model_uri)
            .await?;
        ClassicalRuntime::load(artifact.clone(), &estimator_bytes)?;
        Ok(refitted_hash)
    }

    async fn fail_integrity(
        &self,
        run_id: &FeatureParityRunId,
        feature_contract_hash: &ContentHash,
        error: &QuantError,
    ) -> QuantResult<()> {
        self.deps
            .parity_repo
            .complete_run(
                run_id,
                CompleteFeatureParityRun {
                    status: FeatureParityRunStatus::Failed,
                    total_count: 1,
                    compared_count: 1,
                    matched_count: 0,
                    mismatched_count: 1,
                    pending_materialization_count: 0,
                    feature_contract_hash: Some(feature_contract_hash.clone()),
                    transform_hash: None,
                    failure_code: Some(DiagnosticCode::new("frozen_model_integrity")),
                    failure_detail: Some(error.to_string()),
                },
            )
            .await?;
        self.deps
            .parity_repo
            .open_latch(
                run_id,
                FeatureParityStateTransition::IntegrityFailure,
                format!("frozen dataset/model parity failed: {error}"),
            )
            .await?;
        self.deps
            .parity_repo
            .mark_containment_complete(run_id)
            .await?;
        Ok(())
    }
}

fn verify_weighted_artifact(
    examples: &[TrainingExample],
    label: &LabelSelector,
    artifact: &WeightedFactorModelArtifact,
    dataset_hash: &ContentHash,
    spec_contract: &ModelInputContract,
) -> QuantResult<(ContentHash, ContentHash)> {
    verify_input_contract_binding(
        "weighted",
        &artifact.input_contract,
        &artifact.input_contract_hash,
        spec_contract,
    )?;
    if artifact.training_dataset_hash != *dataset_hash {
        return Err(ResearchError::Determinism {
            detail: "weighted artifact semantic dataset hash differs from its registry-bound frozen dataset"
                .to_owned(),
        }
        .into());
    }
    let factors = artifact
        .weights
        .iter()
        .map(|weight| weight.factor.clone())
        .collect::<Vec<_>>();
    let refitted = fit_frozen_reference_quantiles(
        examples,
        label,
        &factors,
        Some(&artifact.factor_cross_section),
    )?;
    if refitted != artifact.frozen_reference_quantiles {
        return Err(ResearchError::Determinism {
            detail: "weighted artifact frozen reference CDF differs from its exact training rows"
                .to_owned(),
        }
        .into());
    }
    let training_input_hash = weighted_training_input_hash(
        examples,
        label,
        &factors,
        &refitted,
        Some(&artifact.factor_cross_section),
    )?;
    if training_input_hash != artifact.training_input_hash {
        return Err(ResearchError::Determinism {
            detail:
                "weighted artifact training-input commitment differs from the exact frozen rows"
                    .to_owned(),
        }
        .into());
    }
    Ok((artifact.input_transform_hash()?, training_input_hash))
}

fn verify_classical_artifact_binding(
    artifact: &ClassicalModelArtifact,
    dataset_hash: &ContentHash,
    label_schema_hash: &ContentHash,
    spec: &ModelSpecInfo,
) -> QuantResult<()> {
    verify_input_contract_binding(
        "classical",
        &artifact.input_contract,
        &artifact.input_contract_hash,
        &spec.input_contract,
    )?;
    let prediction_horizon_secs = u64::try_from(spec.prediction_horizon_secs).map_err(|error| {
        ResearchError::Determinism {
            detail: format!("model spec prediction horizon is invalid: {error}"),
        }
    })?;
    if artifact.prediction_horizon_secs != prediction_horizon_secs {
        return Err(ResearchError::Determinism {
            detail: format!(
                "classical artifact horizon {}s differs from model spec {}s",
                artifact.prediction_horizon_secs, prediction_horizon_secs
            ),
        }
        .into());
    }
    let target = spec.training_contract.target_label_name.as_str();
    let target_semantics = if artifact.kind == ClassicalKind::LogisticRegression
        && target == SETTLEMENT_OUTCOME.as_str()
    {
        ClassicalOutputSemantics::SettlementProbability
    } else if !matches!(artifact.kind, ClassicalKind::LogisticRegression)
        && target == RETURN_TO_HORIZON.as_str()
        && spec.training_contract.target_label_horizon_secs == prediction_horizon_secs
    {
        ClassicalOutputSemantics::ForwardReturnBps
    } else {
        return Err(ResearchError::Determinism {
            detail: format!(
                "classical kind {} has unsupported frozen target `{target}` at {}s",
                artifact.kind, spec.training_contract.target_label_horizon_secs
            ),
        }
        .into());
    };
    if artifact.output_semantics != target_semantics {
        return Err(ResearchError::Determinism {
            detail: "classical artifact output semantics differ from its frozen training target"
                .to_owned(),
        }
        .into());
    }
    if artifact.training_dataset_hash != *dataset_hash {
        return Err(ResearchError::Determinism {
            detail: "classical artifact semantic dataset hash differs from its registry-bound frozen dataset"
                .to_owned(),
        }
        .into());
    }
    if artifact.label_schema_hash != *label_schema_hash {
        return Err(ResearchError::Determinism {
            detail:
                "classical artifact label schema differs from its registry-bound frozen dataset"
                    .to_owned(),
        }
        .into());
    }
    Ok(())
}

fn verify_sell_scorer_artifact(
    examples: &[TrainingExample],
    label: &LabelSelector,
    artifact: &SellScorerArtifact,
    dataset_hash: &ContentHash,
    label_schema_hash: &ContentHash,
    spec_contract: &ModelInputContract,
) -> QuantResult<(ContentHash, ContentHash)> {
    verify_input_contract_binding(
        "sell scorer",
        &artifact.input_contract,
        &artifact.input_contract_hash,
        spec_contract,
    )?;
    if artifact.training_dataset_hash != *dataset_hash {
        return Err(ResearchError::Determinism {
            detail:
                "sell scorer semantic dataset hash differs from its registry-bound frozen dataset"
                    .to_owned(),
        }
        .into());
    }
    if artifact.label_schema_hash != *label_schema_hash {
        return Err(ResearchError::Determinism {
            detail: "sell scorer label schema differs from its frozen dataset".to_owned(),
        }
        .into());
    }
    let factors = artifact
        .weights
        .iter()
        .map(|weight| weight.factor.clone())
        .collect::<Vec<_>>();
    let training_input_hash = weighted_training_input_hash(
        examples,
        label,
        &factors,
        &FrozenReferenceQuantiles::empty(),
        None,
    )?;
    if training_input_hash != artifact.training_input_hash {
        return Err(ResearchError::Determinism {
            detail: "sell scorer training-input commitment differs from the exact frozen rows"
                .to_owned(),
        }
        .into());
    }
    Ok((artifact.input_transform_hash()?, training_input_hash))
}

fn verify_input_contract_binding(
    artifact_kind: &str,
    artifact_contract: &ModelInputContract,
    artifact_contract_hash: &ContentHash,
    spec_contract: &ModelInputContract,
) -> QuantResult<()> {
    let spec_contract_hash = model_input_contract_hash(spec_contract)?;
    if artifact_contract != spec_contract || artifact_contract_hash != &spec_contract_hash {
        return Err(ResearchError::Determinism {
            detail: format!(
                "{artifact_kind} artifact input contract differs from its owning model spec"
            ),
        }
        .into());
    }
    Ok(())
}

#[cfg(not(feature = "ml-classical"))]
fn classical_runtime_unavailable(artifact: &ClassicalModelArtifact) -> QuantError {
    ResearchError::RuntimeUnavailable {
        family: artifact.kind.to_string(),
        detail: "classical frozen parity requires the ml-classical build".to_owned(),
    }
    .into()
}

struct FrozenEvidenceRequest<'a> {
    run_id: &'a FeatureParityRunId,
    version: &'a ModelVersionInfo,
    dataset: &'a TrainingDatasetInfo,
    examples: &'a [TrainingExample],
    feature_contract_hash: &'a ContentHash,
    dataset_hash: &'a ContentHash,
    training_input_hash: &'a ContentHash,
    transform_hash: &'a ContentHash,
}

#[derive(Serialize)]
struct FrozenExampleCommitment<'a> {
    scope: &'static str,
    model_version_id: String,
    training_dataset_id: String,
    feature_contract_hash: &'a ContentHash,
    transform_hash: &'a ContentHash,
    dataset_hash: &'a ContentHash,
    training_input_hash: &'a ContentHash,
    example_id: String,
    market_id: String,
    decision_boundary: &'a DecisionBoundary,
}

fn frozen_model_evidence_rows(
    request: &FrozenEvidenceRequest<'_>,
) -> QuantResult<Vec<QuantFeatureParityEventRow>> {
    let ingestion_time = Utc::now().timestamp_millis();
    request
        .examples
        .iter()
        .map(|example| {
            let commitment = FrozenExampleCommitment {
                scope: "global_canonical_training_input_commitment",
                model_version_id: request.version.model_version_id.to_string(),
                training_dataset_id: request.dataset.training_dataset_id.to_string(),
                feature_contract_hash: request.feature_contract_hash,
                transform_hash: request.transform_hash,
                dataset_hash: request.dataset_hash,
                training_input_hash: request.training_input_hash,
                example_id: example.example_id.to_string(),
                market_id: example.market_id.to_string(),
                decision_boundary: &example.decision_boundary,
            };
            let fingerprint = ResearchHasher::canonical(&commitment)?;
            let event_identity = ResearchHasher::canonical(&(
                request.run_id.to_string(),
                fingerprint.as_str(),
                FeatureParityEventStatus::Matched.as_str(),
            ))?;
            let detail = FeatureParityDetail::Compared {
                sampling_key: format!("{}/{}", request.run_id, example.example_id),
                source: Box::new(FeatureParityDetailSource::FrozenModelCommitment {
                    example_id: example.example_id.clone(),
                    decision_boundary: example.decision_boundary.clone(),
                    feature_contract_hash: request.feature_contract_hash.clone(),
                    transform_hash: request.transform_hash.clone(),
                    dataset_hash: request.dataset_hash.clone(),
                    training_input_hash: request.training_input_hash.clone(),
                }),
            };
            detail
                .validate_for(
                    FeatureParityStage::ModelInput,
                    FeatureParityEventStatus::Matched,
                )
                .map_err(|detail| ResearchError::Determinism {
                    detail: detail.to_owned(),
                })?;
            let detail_json =
                serde_json::to_string(&detail).map_err(|error| ResearchError::Serialization {
                    detail: format!("serialize frozen parity evidence: {error}"),
                })?;
            Ok(QuantFeatureParityEventRow {
                event_time: example.decision_at().timestamp_millis(),
                parity_event_id: FeatureParityEventId::from_evidence_hash(&event_identity),
                parity_run_id: request.run_id.clone(),
                decision_at: example.decision_at().timestamp_millis(),
                stage: FeatureParityStage::ModelInput.as_str().to_owned(),
                status: FeatureParityEventStatus::Matched.as_str().to_owned(),
                report_id: None,
                model_run_id: None,
                model_version_id: Some(request.version.model_version_id.clone()),
                training_dataset_id: Some(request.dataset.training_dataset_id.clone()),
                market_id: Some(example.market_id.clone()),
                feature_name: None,
                reason: Some("global_canonical_commitment".to_owned()),
                online_state: None,
                replay_state: None,
                online_value: None,
                replay_value: None,
                online_effective_at: None,
                online_available_at: None,
                online_cutoff: Some(
                    example
                        .decision_boundary
                        .knowledge_cutoff()
                        .timestamp_millis(),
                ),
                replay_effective_at: None,
                replay_available_at: None,
                replay_cutoff: Some(
                    example
                        .decision_boundary
                        .knowledge_cutoff()
                        .timestamp_millis(),
                ),
                feature_contract_hash: request.feature_contract_hash.as_str().to_owned(),
                transform_hash: request.transform_hash.as_str().to_owned(),
                online_fingerprint: fingerprint.as_str().to_owned(),
                replay_fingerprint: fingerprint.as_str().to_owned(),
                detail_json,
                ingestion_time,
            })
        })
        .collect()
}

/// Validate the exact subject and completeness of a publication permit.
pub(crate) fn validate_passed_model_parity_run(
    run: &FeatureParityRunInfo,
    version: &ModelVersionInfo,
    dataset: &TrainingDatasetInfo,
) -> QuantResult<()> {
    let valid = run.kind == FeatureParityRunKind::Full
        && run.status == FeatureParityRunStatus::Passed
        && run.model_version_id.as_ref() == Some(&version.model_version_id)
        && run.training_dataset_id.as_ref() == Some(&dataset.training_dataset_id)
        && run.report_id.is_none()
        && run.window_start == dataset.window_start
        && run.window_end == dataset.window_end
        && run.total_count > 0
        && run.compared_count == run.total_count
        && run.matched_count == run.total_count
        && run.mismatched_count == 0
        && run.pending_materialization_count == 0
        && run.feature_contract_hash.as_ref() == dataset.feature_schema_hash.as_ref()
        && run.transform_hash.is_some()
        && run
            .finished_at
            .is_some_and(|finished| finished >= version.created_at);
    if !valid {
        return Err(ResearchError::Determinism {
            detail: format!(
                "full parity run {} is not a complete permit for model {} and dataset {}",
                run.run_id, version.model_version_id, dataset.training_dataset_id
            ),
        }
        .into());
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use chrono::{DateTime, Duration, Utc};
    use quant_pivot_models::{
        domain::{
            data_plane::{DecisionClock, DecisionSource},
            quant::{FeatureParityRunInfo, ModelVersionInfo, TrainingDatasetInfo},
        },
        enums::{
            common::MarketCategory,
            model::ModelFamily,
            quant::{
                DataQualityStatus, DatasetPurpose, FeatureParityRunKind, FeatureParityRunStatus,
                PublicationStatus, TrainingDatasetStatus,
            },
        },
        types::{
            ContentHash, DecisionPolicySnapshotId, EventId, FeatureParityDetail,
            FeatureParityDetailSource, FeatureParityRunId, MarketId, ModelInputContract,
            ModelInputSpec, ModelSpecId, ModelVersionId, RoleCode, SchemaVersion, TokenId,
            TrainingDatasetId, TrainingExampleId, TrainingHorizonsSecs, TrainingSampleSource,
            model_metrics::ModelVersionMetrics, model_training::ModelTrainingObjective,
        },
    };
    use quant_pivot_research::{
        features::FeatureVector, model::model_input_contract_hash, selection::SelectedMarket,
        training::TrainingExample,
    };

    use super::{
        FrozenEvidenceRequest, frozen_model_evidence_rows, validate_passed_model_parity_run,
        verify_input_contract_binding,
    };
    use crate::test_fixtures::{
        execution_pg_seed::fixture_profile_ref, model_spec_fixtures::model_spec_lineage_fixture,
    };

    fn hash(seed: char) -> ContentHash {
        ContentHash::parse(format!("blake3:{}", seed.to_string().repeat(64)))
            .expect("valid test hash")
    }

    fn subject() -> (ModelVersionInfo, TrainingDatasetInfo, FeatureParityRunInfo) {
        let now = Utc::now();
        let model_version_id = ModelVersionId::from_v7();
        let model_spec_id = ModelSpecId::from_v7();
        let training_dataset_id = TrainingDatasetId::from_v7();
        let feature_hash = hash('a');
        let (model_spec_thesis, model_spec_definition_hash) =
            model_spec_lineage_fixture("frozen-parity-test-spec");
        let version = ModelVersionInfo {
            model_version_id: model_version_id.clone(),
            model_spec_id: model_spec_id.clone(),
            model_spec_name: "frozen-parity-test-spec".to_owned(),
            model_family: ModelFamily::WeightedFactor,
            model_spec_thesis,
            model_spec_definition_hash,
            version: 1,
            artifact_hash: hash('b'),
            category_scope: None,
            profile_ref: fixture_profile_ref(),
            training_dataset_id: Some(training_dataset_id.clone()),
            trade_policy_artifact_id: None,
            trade_policy_hash: None,
            publish_path_set_id: None,
            derivation_kind: ModelVersionInfo::training_derivation_kind(),
            parent_model_version_id: None,
            source_backtest_report_id: None,
            calibration_artifact_id: None,
            score_multiplier_calibration_report: None,
            derivation_evidence_hash: None,
            metrics: ModelVersionMetrics::not_measured("test fixture"),
            training_objective: ModelTrainingObjective::hand_authored("test fixture"),
            quality_gate_report: None,
            publication_status: PublicationStatus::Candidate,
            published_at: None,
            retired_at: None,
            created_at: now,
        };
        let dataset = TrainingDatasetInfo {
            training_dataset_id: training_dataset_id.clone(),
            model_spec_id,
            model_spec_definition_hash: hash('d'),
            window_start: now - Duration::hours(2),
            window_end: now - Duration::hours(1),
            status: TrainingDatasetStatus::Ready,
            purpose: DatasetPurpose::Training,
            feature_schema_hash: Some(feature_hash.clone()),
            factor_schema_hash: None,
            label_schema_hash: None,
            dataset_hash: None,
            manifest_hash: None,
            manifest_json: None,
            artifact_bytes_hash: None,
            parquet_uri: None,
            sample_count: None,
            knowledge_lag_secs: 0,
            sample_interval_secs: 60,
            horizons_secs: TrainingHorizonsSecs(Vec::new()),
            feature_schema_version: None,
            sample_sources: None,
            coverage_json: None,
            decision_policy_snapshot_id: DecisionPolicySnapshotId::from_v7(),
            failure_detail: None,
            completed_at: Some(now),
            created_at: now - Duration::hours(3),
        };
        let run = FeatureParityRunInfo {
            run_id: FeatureParityRunId::from_v7(),
            kind: FeatureParityRunKind::Full,
            status: FeatureParityRunStatus::Passed,
            window_start: dataset.window_start,
            window_end: dataset.window_end,
            report_id: None,
            model_version_id: Some(model_version_id),
            training_dataset_id: Some(training_dataset_id),
            triggered_by: "test".to_owned(),
            requested_by: None,
            acting_role: RoleCode::new("system"),
            reason: "test".to_owned(),
            total_count: 4,
            compared_count: 4,
            matched_count: 4,
            mismatched_count: 0,
            pending_materialization_count: 0,
            feature_contract_hash: Some(feature_hash),
            transform_hash: Some(hash('c')),
            failure_code: None,
            failure_detail: None,
            started_at: Some(now),
            pending_since: None,
            containment_completed_at: None,
            finished_at: Some(now + Duration::seconds(1)),
            created_at: now,
            updated_at: now,
        };
        (version, dataset, run)
    }

    fn training_example(decision_at: DateTime<Utc>) -> TrainingExample {
        let market_id = MarketId::new("frozen-evidence-market");
        let token_id = TokenId::new("frozen-evidence-token");
        let decision_boundary = DecisionClock::new(7)
            .boundary(decision_at)
            .and_then(|boundary| boundary.with_source_cutoff(DecisionSource::Catalog, 11))
            .expect("valid decision boundary");
        TrainingExample {
            example_id: TrainingExampleId::from_v7(),
            market_id: market_id.clone(),
            token_id: token_id.clone(),
            selected_market: SelectedMarket {
                market_id: market_id.clone(),
                event_id: EventId::new("frozen-evidence-event"),
                category: MarketCategory::Sports,
                primary_token_id: token_id.clone(),
                secondary_token_id: None,
                liquidity_usd: None,
                volume_24h_usd: None,
                source_refs: Vec::new(),
            },
            decision_boundary,
            sample_source: TrainingSampleSource::HistoricalPit,
            feature_vector: FeatureVector {
                market_id,
                token_id: Some(token_id),
                decision_at,
                generic_schema_version: SchemaVersion::FIRST,
                generic: BTreeMap::new(),
                domain: None,
                data_quality: DataQualityStatus::Fresh,
            },
            factor_values: Vec::new(),
            labels: Vec::new(),
            source_refs: Vec::new(),
            decision_capture: None,
            lot_context: None,
            position_state: None,
            book_fidelity: None,
        }
    }

    #[test]
    fn exact_subject_full_pass_is_a_valid_publish_permit() {
        let (version, dataset, run) = subject();
        validate_passed_model_parity_run(&run, &version, &dataset).expect("valid permit");
    }

    #[test]
    fn swapped_subject_or_incomplete_transform_is_rejected() {
        let (version, dataset, mut run) = subject();
        run.model_version_id = Some(ModelVersionId::from_v7());
        assert!(validate_passed_model_parity_run(&run, &version, &dataset).is_err());

        run.model_version_id = Some(version.model_version_id.clone());
        run.transform_hash = None;
        assert!(validate_passed_model_parity_run(&run, &version, &dataset).is_err());
    }

    #[test]
    fn swapped_or_malformed_artifact_input_contract_is_rejected() {
        let spec = ModelInputContract {
            inputs: vec![
                ModelInputSpec::required("book.mid"),
                ModelInputSpec::optional("market.age_secs"),
            ],
        };
        let spec_hash = model_input_contract_hash(&spec).expect("spec hash");
        verify_input_contract_binding("classical", &spec, &spec_hash, &spec)
            .expect("exact contract");

        let swapped = ModelInputContract {
            inputs: vec![spec.inputs[1].clone(), spec.inputs[0].clone()],
        };
        let swapped_hash = model_input_contract_hash(&swapped).expect("swapped hash");
        assert!(
            verify_input_contract_binding("classical", &swapped, &swapped_hash, &spec).is_err()
        );
        assert!(verify_input_contract_binding("weighted", &swapped, &swapped_hash, &spec).is_err());

        let malformed = ModelInputContract {
            inputs: vec![spec.inputs[0].clone(), spec.inputs[0].clone()],
        };
        assert!(
            verify_input_contract_binding("sell scorer", &malformed, &spec_hash, &spec).is_err()
        );
    }

    #[test]
    fn frozen_evidence_is_row_anchored_without_fabricated_values() {
        let (version, dataset, run) = subject();
        let examples = vec![
            training_example(dataset.window_start + Duration::minutes(10)),
            training_example(dataset.window_start + Duration::minutes(20)),
        ];
        let feature_contract_hash = dataset
            .feature_schema_hash
            .as_ref()
            .expect("feature contract hash");
        let dataset_hash = hash('d');
        let training_input_hash = hash('e');
        let transform_hash = hash('f');
        let rows = frozen_model_evidence_rows(&FrozenEvidenceRequest {
            run_id: &run.run_id,
            version: &version,
            dataset: &dataset,
            examples: &examples,
            feature_contract_hash,
            dataset_hash: &dataset_hash,
            training_input_hash: &training_input_hash,
            transform_hash: &transform_hash,
        })
        .expect("frozen evidence");

        assert_eq!(rows.len(), examples.len());
        assert_ne!(rows[0].online_fingerprint, rows[1].online_fingerprint);
        for row in rows {
            assert_eq!(row.stage, "model_input");
            assert_eq!(row.status, "matched");
            assert_eq!(row.online_fingerprint, row.replay_fingerprint);
            assert!(row.online_value.is_none() && row.replay_value.is_none());
            assert!(row.online_state.is_none() && row.replay_state.is_none());
            assert_eq!(row.feature_contract_hash, feature_contract_hash.as_str());
            assert_eq!(row.transform_hash, transform_hash.as_str());
            let detail: FeatureParityDetail =
                serde_json::from_str(&row.detail_json).expect("structured evidence");
            let FeatureParityDetail::Compared { source, .. } = detail else {
                panic!("expected frozen model commitment detail");
            };
            let FeatureParityDetailSource::FrozenModelCommitment {
                dataset_hash: actual_dataset_hash,
                training_input_hash: actual_training_input_hash,
                ..
            } = source.as_ref()
            else {
                panic!("expected frozen model commitment source");
            };
            assert_eq!(actual_dataset_hash, &dataset_hash);
            assert_eq!(actual_training_input_hash, &training_input_hash);
        }
    }
}
