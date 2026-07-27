//! Full frozen dataset/model parity verification and publication evidence.
//!
//! Unlike runtime replay, this verifier needs no prior live report. It reloads
//! the immutable Parquet v3 and model artifact from their content addresses,
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
            TrainingDatasetInfo,
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
        model_serving::ModelServingContract,
    },
};
use quant_pivot_repository::traits::{
    FactWriter, FeatureParityRepository, ModelRegistryRepository, TrainingDatasetRepository,
};
use quant_pivot_research::{
    artifact::ArtifactStore,
    hashing::ResearchHasher,
    model::{
        CancellationProbe, ClassicalOutputSemantics, LabelSelector, ModelArtifact,
        SellScorerTrainer, TrainSellScorerRequest,
        artifact::{
            ClassicalModelPayload, ModelPayload, SellScorerPayload, WeightedFactorModelPayload,
        },
        fit_frozen_reference_quantiles, model_input_contract_hash, weighted_training_input_hash,
    },
    training::{LabelName, RETURN_TO_HORIZON, TOKEN_PAYOUT_RATIO, TrainingExample},
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
            validate_passed_parity(&existing, version, &dataset)?;
            return Ok(existing);
        }

        let materialization = require_dataset_materialization(&dataset)?;
        let run_id = FeatureParityRunId::from_v7();
        let frozen_subject = NewFrozenModelParitySubject {
            model_version_id: version.model_version_id,
            training_dataset_id: dataset.training_dataset_id,
            subject_generation: version.artifact_hash,
            evidence_hash: ModelVersionParityEvidence {
                model_version_id: &version.model_version_id,
                model_spec_id: &version.model_spec_id,
                artifact_hash: &version.artifact_hash,
                training_dataset_id: &dataset.training_dataset_id,
                dataset_hash: materialization.dataset_hash,
                manifest_hash: materialization.manifest_hash,
                artifact_bytes_hash: materialization.artifact_bytes_hash,
            }
            .content_hash()?,
        };
        let queued = self
            .deps
            .parity_repo
            .create_frozen_model_run(
                NewFeatureParityRun {
                    run_id,
                    kind: FeatureParityRunKind::Full,
                    status: FeatureParityRunStatus::Queued,
                    window_start: dataset.window_start,
                    window_end: dataset.window_end,
                    report_id: None,
                    model_version_id: Some(version.model_version_id),
                    training_dataset_id: Some(dataset.training_dataset_id),
                    triggered_by: triggered_by.to_owned(),
                    requested_by: None,
                    acting_role: RoleCode::new("system"),
                    reason: reason.to_owned(),
                    total_count: 0,
                    compared_count: 0,
                    matched_count: 0,
                    mismatched_count: 0,
                    pending_materialization_count: 0,
                    feature_contract_hash: Some(*materialization.feature_schema_hash),
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
        validate_passed_parity(&run, version, &dataset)?;
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
            .find_model_spec(&version.model_spec_id)
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
        let artifact =
            ModelArtifact::load_verified(self.deps.artifact_store.as_ref(), version).await?;
        let header = artifact.header();
        let persisted_contract = header.serving_contract();
        let bindings = persisted_contract.bindings();
        let bound_bias_hash = bindings
            .factors
            .bias_table
            .as_ref()
            .map(|binding| binding.content_hash);
        if &bindings.dataset.manifest != materialization.manifest
            || bindings.dataset.manifest_hash != *materialization.manifest_hash
            || bindings.dataset.artifact_bytes_hash != *materialization.artifact_bytes_hash
            || bindings.schemas.feature_schema_hash != *materialization.feature_schema_hash
            || bindings.schemas.label_schema_hash != *materialization.label_schema_hash
            || &bindings.factors.plane != materialization.factor_serving_plane
            || bound_bias_hash != materialization.coverage.bias_table_hash
            || bindings.transform.training_dataset_hash != *materialization.dataset_hash
            || bindings.model.model_spec_definition_hash != spec.definition_hash
            || bindings.model.model_family != spec.model_family
        {
            return Err(ResearchError::Determinism {
                detail: format!(
                    "model artifact {} serving contract disagrees with frozen dataset/spec {}",
                    version.model_version_id, dataset.training_dataset_id
                ),
            }
            .into());
        }

        let label = LabelSelector {
            name: LabelName::new(spec.training_contract.target_label_name.clone()),
            horizon_secs: spec.training_contract.target_label_horizon_secs,
        };
        let (transform_hash, training_input_hash) = match artifact.payload() {
            ModelPayload::WeightedFactor(weighted) => verify_weighted_payload(
                &examples,
                &label,
                weighted,
                &spec.input_contract,
                persisted_contract,
            )?,
            ModelPayload::Classical(classical) => {
                verify_classical_binding(classical, spec, persisted_contract)?;
                #[cfg(feature = "ml-classical")]
                {
                    self.verify_classical(&examples, spec, &artifact, classical, persisted_contract)
                        .await?
                }
                #[cfg(not(feature = "ml-classical"))]
                {
                    return Err(classical_runtime_unavailable(classical));
                }
            }
            ModelPayload::SellScorer(sell_payload) => verify_sell_payload(
                &examples,
                &label,
                sell_payload,
                &spec.input_contract,
                persisted_contract,
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
            feature_contract_hash: *materialization.feature_schema_hash,
            transform_hash,
            evidence_rows,
        })
    }

    #[cfg(feature = "ml-classical")]
    async fn verify_classical(
        &self,
        examples: &[TrainingExample],
        spec: &ModelSpecInfo,
        artifact: &ModelArtifact,
        payload: &ClassicalModelPayload,
        contract: &ModelServingContract,
    ) -> QuantResult<(ContentHash, ContentHash)> {
        let matrix_spec = FeatureMatrixSpec {
            columns: payload
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
        let refitted_input_hash = training_input_hash(&standardized, &matrix.labels)?;
        let transform = &contract.bindings().transform;
        if refitted != payload.input_transform
            || refitted_hash != transform.input_transform_hash
            || refitted_input_hash != transform.training_input_hash
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
            .get(&payload.serialized_model_uri)
            .await?;
        ClassicalRuntime::load(artifact.clone(), &estimator_bytes)?;
        Ok((refitted_hash, refitted_input_hash))
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
                    feature_contract_hash: Some(*feature_contract_hash),
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

fn verify_weighted_payload(
    examples: &[TrainingExample],
    label: &LabelSelector,
    payload: &WeightedFactorModelPayload,
    spec_contract: &ModelInputContract,
    contract: &ModelServingContract,
) -> QuantResult<(ContentHash, ContentHash)> {
    let bindings = contract.bindings();
    verify_input_binding(
        "weighted",
        &payload.input_contract,
        bindings.transform.input_contract_hash,
        spec_contract,
    )?;
    let reference_factors = bindings
        .factors
        .plane
        .definitions()
        .iter()
        .filter(|revision| revision.definition().normalization.is_cross_sectional())
        .map(|revision| revision.factor_name().clone())
        .collect::<Vec<_>>();
    let refitted = fit_frozen_reference_quantiles(
        examples,
        label,
        &reference_factors,
        Some(&payload.factor_cross_section),
    )?;
    if refitted != payload.frozen_reference_quantiles {
        return Err(ResearchError::Determinism {
            detail: "weighted artifact frozen reference CDF differs from its exact training rows"
                .to_owned(),
        }
        .into());
    }
    let factors = bindings
        .factors
        .plane
        .definitions()
        .iter()
        .filter(|revision| !revision.definition().is_diagnostic())
        .map(|revision| revision.factor_name().clone())
        .collect::<Vec<_>>();
    let training_input_hash = weighted_training_input_hash(
        examples,
        label,
        &factors,
        &refitted,
        Some(&payload.factor_cross_section),
    )?;
    let transform_hash = payload.input_transform_hash()?;
    if training_input_hash != bindings.transform.training_input_hash
        || transform_hash != bindings.transform.input_transform_hash
    {
        return Err(ResearchError::Determinism {
            detail: "weighted artifact transform/training-input commitment differs from the exact frozen rows"
                .to_owned(),
        }
        .into());
    }
    Ok((transform_hash, training_input_hash))
}

fn verify_classical_binding(
    payload: &ClassicalModelPayload,
    spec: &ModelSpecInfo,
    contract: &ModelServingContract,
) -> QuantResult<()> {
    let bindings = contract.bindings();
    verify_input_binding(
        "classical",
        &payload.input_contract,
        bindings.transform.input_contract_hash,
        &spec.input_contract,
    )?;
    let prediction_horizon_secs = u64::try_from(spec.prediction_horizon_secs).map_err(|error| {
        ResearchError::Determinism {
            detail: format!("model spec prediction horizon is invalid: {error}"),
        }
    })?;
    if bindings.model.prediction_horizon_secs != prediction_horizon_secs {
        return Err(ResearchError::Determinism {
            detail: format!(
                "classical serving horizon {}s differs from model spec {}s",
                bindings.model.prediction_horizon_secs, prediction_horizon_secs
            ),
        }
        .into());
    }
    let target = spec.training_contract.target_label_name.as_str();
    let target_semantics = if payload.kind == ClassicalKind::LogisticRegression
        && target == TOKEN_PAYOUT_RATIO.as_str()
    {
        ClassicalOutputSemantics::FullPayoutProbability
    } else if !matches!(payload.kind, ClassicalKind::LogisticRegression)
        && target == RETURN_TO_HORIZON.as_str()
        && spec.training_contract.target_label_horizon_secs == prediction_horizon_secs
    {
        ClassicalOutputSemantics::ForwardReturnBps
    } else {
        return Err(ResearchError::Determinism {
            detail: format!(
                "classical kind {} has unsupported frozen target `{target}` at {}s",
                payload.kind, spec.training_contract.target_label_horizon_secs
            ),
        }
        .into());
    };
    if payload.output_semantics != target_semantics {
        return Err(ResearchError::Determinism {
            detail: "classical artifact output semantics differ from its frozen training target"
                .to_owned(),
        }
        .into());
    }
    Ok(())
}

fn verify_sell_payload(
    examples: &[TrainingExample],
    label: &LabelSelector,
    payload: &SellScorerPayload,
    spec_contract: &ModelInputContract,
    contract: &ModelServingContract,
) -> QuantResult<(ContentHash, ContentHash)> {
    let bindings = contract.bindings();
    verify_input_binding(
        "sell scorer",
        &payload.input_contract,
        bindings.transform.input_contract_hash,
        spec_contract,
    )?;
    let output = SellScorerTrainer::new().train_sell_scorer(&TrainSellScorerRequest {
        cancellation: CancellationProbe::default(),
        examples: Arc::<[TrainingExample]>::from(examples.to_vec()),
        label: label.clone(),
        factor_plane: bindings.factors.plane.clone(),
        factor_head: payload.factor_head.clone(),
        estimator: payload.estimator.clone(),
        output_spec: payload.output_spec.clone(),
        input_contract: payload.input_contract.clone(),
        factor_cross_section: payload.factor_cross_section.clone(),
    })?;
    if &output.payload != payload
        || output.training_input_hash != bindings.transform.training_input_hash
        || output.input_contract_hash != bindings.transform.input_contract_hash
        || output.input_transform_hash != bindings.transform.input_transform_hash
    {
        return Err(ResearchError::Determinism {
            detail: "sell scorer payload/transform/training-input commitment differs from the exact frozen rows"
                .to_owned(),
        }
        .into());
    }
    Ok((output.input_transform_hash, output.training_input_hash))
}

fn verify_input_binding(
    artifact_kind: &str,
    artifact_contract: &ModelInputContract,
    bound_contract_hash: ContentHash,
    spec_contract: &ModelInputContract,
) -> QuantResult<()> {
    let spec_contract_hash = model_input_contract_hash(spec_contract)?;
    if artifact_contract != spec_contract || bound_contract_hash != spec_contract_hash {
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
fn classical_runtime_unavailable(payload: &ClassicalModelPayload) -> QuantError {
    ResearchError::RuntimeUnavailable {
        family: payload.kind.to_string(),
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
                fingerprint,
                FeatureParityEventStatus::Matched.as_str(),
            ))?;
            let detail = FeatureParityDetail::Compared {
                sampling_key: format!("{}/{}", request.run_id, example.example_id),
                source: Box::new(FeatureParityDetailSource::FrozenModelCommitment {
                    example_id: example.example_id,
                    decision_boundary: example.decision_boundary.clone(),
                    feature_contract_hash: *request.feature_contract_hash,
                    transform_hash: *request.transform_hash,
                    dataset_hash: *request.dataset_hash,
                    training_input_hash: *request.training_input_hash,
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
                parity_run_id: *request.run_id,
                decision_at: example.decision_at().timestamp_millis(),
                stage: FeatureParityStage::ModelInput.to_string(),
                status: FeatureParityEventStatus::Matched.to_string(),
                report_id: None,
                model_run_id: None,
                model_version_id: Some(request.version.model_version_id),
                training_dataset_id: Some(request.dataset.training_dataset_id),
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
                feature_contract_hash: request.feature_contract_hash.to_string(),
                transform_hash: request.transform_hash.to_string(),
                online_fingerprint: fingerprint.to_string(),
                replay_fingerprint: fingerprint.to_string(),
                detail_json,
                ingestion_time,
            })
        })
        .collect()
}

/// Validate the exact subject and completeness of a publication permit.
pub(crate) fn validate_passed_parity(
    run: &FeatureParityRunInfo,
    version: &ModelVersionInfo,
    dataset: &TrainingDatasetInfo,
) -> QuantResult<()> {
    version
        .verified_serving_contract()
        .map_err(|error| ResearchError::Determinism {
            detail: format!(
                "model {} persisted serving contract is invalid: {error}",
                version.model_version_id
            ),
        })?;
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
        && run.feature_contract_hash == Some(dataset.feature_schema_hash)
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
            model::{ClassicalKind, ModelFamily},
            quant::{
                DataQualityStatus, DatasetPurpose, FeatureParityRunKind, FeatureParityRunStatus,
                ModelSerializationFormat, PublicationStatus, TrainingDatasetStatus,
            },
        },
        runtime_config::ImmutableProfileArtifacts,
        types::{
            CapabilityRegistryHashes, ContentHash, DATASET_ARTIFACT_FORMAT_VERSION,
            DATASET_SOURCE_LINEAGE_FORMAT_VERSION, DatasetManifest, DatasetSourceLineage,
            DecisionPolicySnapshotId, EventId, FeatureParityDetail, FeatureParityDetailSource,
            FeatureParityRunId, MarketId, ModelInputContract, ModelInputSpec, ModelSpecId,
            ModelVersionId, ReaderContractVersion, ResearchProfileArtifactId, ResearchProfileRef,
            RoleCode, SchemaContractVersion, SchemaVersion, SourceSliceId, TokenId,
            TrainingDatasetId, TrainingExampleId, TrainingHorizonsSecs, TrainingSampleSource,
            factor::FactorServingPlane,
            model_metrics::ModelVersionMetrics,
            model_serving::{
                ModelServingBindings, ModelServingContract, ModelServingDatasetBinding,
                ModelServingEstimatorBinding, ModelServingFactorBinding, ModelServingModelBinding,
                ModelServingPolicySnapshotBinding, ModelServingSchemaBinding,
                ModelServingTransformBinding,
            },
            model_training::ModelTrainingObjective,
        },
    };
    use quant_pivot_research::{
        features::FeatureVector, model::model_input_contract_hash, selection::SelectedMarket,
        training::TrainingExample,
    };

    use super::{
        FrozenEvidenceRequest, frozen_model_evidence_rows, validate_passed_parity,
        verify_input_binding,
    };
    use crate::test_fixtures::{
        execution_pg_seed::{fixture_profile_ref, source_slice_ref},
        model_spec_fixtures::model_spec_lineage_fixture,
    };

    fn hash(seed: char) -> ContentHash {
        ContentHash::parse(&format!("blake3:{}", seed.to_string().repeat(64)))
            .expect("valid test hash")
    }

    struct FrozenParitySubject {
        now: DateTime<Utc>,
        model_version_id: ModelVersionId,
        model_spec_id: ModelSpecId,
        training_dataset_id: TrainingDatasetId,
        feature_hash: ContentHash,
        factor_serving_plane: FactorServingPlane,
        profile_ref: ResearchProfileRef,
        policy_hash: ContentHash,
        source_lineage: DatasetSourceLineage,
        model_spec_definition_hash: ContentHash,
    }

    impl FrozenParitySubject {
        fn new() -> Self {
            let now = Utc::now();
            let profile_ref = fixture_profile_ref();
            let policy_hash = hash('1');
            let decision_policy_snapshot_id =
                DecisionPolicySnapshotId::from_content_hash(&policy_hash);
            let source_lineage =
                Self::source_lineage(now, &profile_ref, policy_hash, decision_policy_snapshot_id);
            let (_, model_spec_definition_hash) =
                model_spec_lineage_fixture("frozen-parity-test-spec");
            Self {
                now,
                model_version_id: ModelVersionId::from_v7(),
                model_spec_id: ModelSpecId::from_v7(),
                training_dataset_id: TrainingDatasetId::from_v7(),
                feature_hash: hash('a'),
                factor_serving_plane: FactorServingPlane::try_empty()
                    .expect("canonical factor-free plane"),
                profile_ref,
                policy_hash,
                source_lineage,
                model_spec_definition_hash,
            }
        }

        fn source_lineage(
            now: DateTime<Utc>,
            profile_ref: &ResearchProfileRef,
            policy_hash: ContentHash,
            decision_policy_snapshot_id: DecisionPolicySnapshotId,
        ) -> DatasetSourceLineage {
            DatasetSourceLineage {
                format_version: DATASET_SOURCE_LINEAGE_FORMAT_VERSION,
                source_slice_id: SourceSliceId::from_v7(),
                source_slice_identity_hash: hash('c'),
                research_profile_artifact_id: ResearchProfileArtifactId::from_profile_ref(
                    profile_ref,
                ),
                research_program_hash: hash('e'),
                source_slice: source_slice_ref('f'),
                source_window_start: now - Duration::hours(3),
                source_window_end: now,
                pit_cutoff: now,
                decision_policy_snapshot_id,
                runtime_config_hash: policy_hash,
                reader_contract_version: ReaderContractVersion::v1(),
                schema_contract_version: SchemaContractVersion::v1(),
                source_schema_hash: hash('2'),
                capability_registry_hashes: CapabilityRegistryHashes::try_new(vec![hash('3')])
                    .expect("canonical capabilities"),
            }
        }

        fn manifest(&self) -> DatasetManifest {
            DatasetManifest {
                format_version: DATASET_ARTIFACT_FORMAT_VERSION,
                training_dataset_id: self.training_dataset_id,
                source_lineage: self.source_lineage.clone(),
                cohort_manifest: None,
                model_spec_id: self.model_spec_id,
                model_family: ModelFamily::ClassicalRandomForest,
                model_spec_definition_hash: self.model_spec_definition_hash,
                trade_policy_artifact_id: None,
                trade_policy_hash: None,
                window_start: self.now - Duration::hours(2),
                window_end: self.now - Duration::hours(1),
                purpose: DatasetPurpose::Training,
                knowledge_lag_secs: 0,
                sample_interval_secs: 60,
                horizons_secs: vec![900],
                feature_schema_version: SchemaVersion::FIRST,
                feature_schema_hash: self.feature_hash,
                factor_serving_plane: self.factor_serving_plane.clone(),
                label_schema_hash: hash('5'),
                semantic_dataset_hash: hash('4'),
                source_fingerprint: hash('6'),
                sample_count: 1,
            }
        }

        fn serving_contract(&self, manifest: DatasetManifest) -> ModelServingContract {
            let input_contract = ModelInputContract {
                inputs: vec![ModelInputSpec::required("book.mid")],
            };
            let manifest_hash = manifest.content_hash().expect("manifest hash");
            ModelServingContract::try_seal(ModelServingBindings {
                policy_snapshot: ModelServingPolicySnapshotBinding {
                    decision_policy_snapshot_id: self.source_lineage.decision_policy_snapshot_id,
                    snapshot_hash: self.policy_hash,
                    profile_artifacts: ImmutableProfileArtifacts::default()
                        .references()
                        .expect("profile references"),
                },
                required_domain_families: Vec::new(),
                capability_registry_hashes: self.source_lineage.capability_registry_hashes.clone(),
                factors: ModelServingFactorBinding {
                    plane: self.factor_serving_plane.clone(),
                    bias_table: None,
                },
                schemas: ModelServingSchemaBinding {
                    feature_schema_hash: self.feature_hash,
                    label_schema_hash: hash('5'),
                },
                transform: ModelServingTransformBinding {
                    input_contract_hash: model_input_contract_hash(&input_contract)
                        .expect("input contract hash"),
                    input_transform_hash: hash('7'),
                    training_input_hash: hash('8'),
                    training_dataset_hash: hash('4'),
                },
                model: ModelServingModelBinding {
                    model_version_id: self.model_version_id,
                    model_spec_id: self.model_spec_id,
                    model_spec_definition_hash: self.model_spec_definition_hash,
                    model_family: ModelFamily::ClassicalRandomForest,
                    category_scope: None,
                    profile_ref: self.profile_ref.clone(),
                    prediction_horizon_secs: 900,
                    estimator: ModelServingEstimatorBinding::Classical {
                        kind: ClassicalKind::RandomForest,
                        model_payload_hash: hash('9'),
                        serialized_model_hash: hash('a'),
                        serialization_format: ModelSerializationFormat::Bincode,
                    },
                    calibration: None,
                },
                trade_policy: None,
                dataset: ModelServingDatasetBinding {
                    manifest,
                    manifest_hash,
                    artifact_bytes_hash: hash('b'),
                },
            })
            .expect("serving contract")
        }

        fn version(&self, serving_contract: ModelServingContract) -> ModelVersionInfo {
            let (model_spec_thesis, _) = model_spec_lineage_fixture("frozen-parity-test-spec");
            ModelVersionInfo {
                model_version_id: self.model_version_id,
                model_spec_id: self.model_spec_id,
                model_spec_name: "frozen-parity-test-spec".to_owned(),
                model_family: ModelFamily::ClassicalRandomForest,
                model_spec_thesis,
                model_spec_definition_hash: self.model_spec_definition_hash,
                model_spec_prediction_horizon_secs: 900,
                version: 1,
                artifact_hash: hash('b'),
                serving_contract_hash: serving_contract.contract_hash(),
                serving_contract,
                category_scope: None,
                profile_ref: self.profile_ref.clone(),
                training_dataset_id: Some(self.training_dataset_id),
                trade_policy_artifact_id: None,
                trade_policy_hash: None,
                publish_path_set_id: None,
                derivation_kind: ModelVersionInfo::training_derivation_kind(),
                parent_model_version_id: None,
                calibration_artifact_id: None,
                derivation_evidence_hash: None,
                metrics: ModelVersionMetrics::not_measured("test fixture"),
                training_objective: ModelTrainingObjective::hand_authored("test fixture"),
                quality_gate_report: None,
                publication_status: PublicationStatus::Candidate,
                published_at: None,
                retired_at: None,
                created_at: self.now,
            }
        }

        fn dataset(&self) -> TrainingDatasetInfo {
            TrainingDatasetInfo {
                training_dataset_id: self.training_dataset_id,
                model_spec_id: self.model_spec_id,
                model_family: ModelFamily::ClassicalRandomForest,
                model_spec_definition_hash: self.model_spec_definition_hash,
                factor_schema_hash: self.factor_serving_plane.factor_schema_hash(),
                factor_serving_plane: self.factor_serving_plane.clone(),
                research_profile_artifact_id: self
                    .source_lineage
                    .research_profile_artifact_id
                    .clone(),
                source_slice_id: self.source_lineage.source_slice_id,
                pit_cutoff: self.source_lineage.pit_cutoff,
                source_lineage: self.source_lineage.clone(),
                feedback_cohort: None,
                cohort_manifest: None,
                window_start: self.now - Duration::hours(2),
                window_end: self.now - Duration::hours(1),
                status: TrainingDatasetStatus::Ready,
                purpose: DatasetPurpose::Training,
                feature_schema_hash: self.feature_hash,
                label_schema_hash: None,
                dataset_hash: None,
                manifest_hash: None,
                manifest: None,
                artifact_bytes_hash: None,
                parquet_uri: None,
                sample_count: None,
                knowledge_lag_secs: 0,
                sample_interval_secs: 60,
                horizons_secs: TrainingHorizonsSecs(Vec::new()),
                feature_schema_version: SchemaVersion::FIRST,
                sample_sources: None,
                coverage: None,
                decision_policy_snapshot_id: self.source_lineage.decision_policy_snapshot_id,
                failure_detail: None,
                completed_at: Some(self.now),
                created_at: self.now - Duration::hours(3),
            }
        }

        fn run(&self, dataset: &TrainingDatasetInfo) -> FeatureParityRunInfo {
            FeatureParityRunInfo {
                run_id: FeatureParityRunId::from_v7(),
                kind: FeatureParityRunKind::Full,
                status: FeatureParityRunStatus::Passed,
                window_start: dataset.window_start,
                window_end: dataset.window_end,
                report_id: None,
                model_version_id: Some(self.model_version_id),
                training_dataset_id: Some(self.training_dataset_id),
                triggered_by: "test".to_owned(),
                requested_by: None,
                acting_role: RoleCode::new("system"),
                reason: "test".to_owned(),
                total_count: 4,
                compared_count: 4,
                matched_count: 4,
                mismatched_count: 0,
                pending_materialization_count: 0,
                feature_contract_hash: Some(self.feature_hash),
                transform_hash: Some(hash('c')),
                failure_code: None,
                failure_detail: None,
                started_at: Some(self.now),
                pending_since: None,
                containment_completed_at: None,
                finished_at: Some(self.now + Duration::seconds(1)),
                created_at: self.now,
                updated_at: self.now,
            }
        }

        fn build(self) -> (ModelVersionInfo, TrainingDatasetInfo, FeatureParityRunInfo) {
            let serving_contract = self.serving_contract(self.manifest());
            let version = self.version(serving_contract);
            let dataset = self.dataset();
            let run = self.run(&dataset);
            (version, dataset, run)
        }
    }

    fn subject() -> (ModelVersionInfo, TrainingDatasetInfo, FeatureParityRunInfo) {
        FrozenParitySubject::new().build()
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
    fn exact_subject_full_permit() {
        let (version, dataset, run) = subject();
        validate_passed_parity(&run, &version, &dataset).expect("valid permit");
    }

    #[test]
    fn swapped_subject_incomplete_rejected() {
        let (mut version, dataset, mut run) = subject();
        run.model_version_id = Some(ModelVersionId::from_v7());
        assert!(validate_passed_parity(&run, &version, &dataset).is_err());

        run.model_version_id = Some(version.model_version_id);
        run.transform_hash = None;
        assert!(validate_passed_parity(&run, &version, &dataset).is_err());

        run.transform_hash = Some(hash('c'));
        version.serving_contract_hash = hash('d');
        assert!(validate_passed_parity(&run, &version, &dataset).is_err());
    }

    #[test]
    fn swapped_malformed_artifact_rejected() {
        let spec = ModelInputContract {
            inputs: vec![
                ModelInputSpec::required("book.mid"),
                ModelInputSpec::optional("market.age_secs"),
            ],
        };
        let spec_hash = model_input_contract_hash(&spec).expect("spec hash");
        verify_input_binding("classical", &spec, spec_hash, &spec).expect("exact contract");

        let swapped = ModelInputContract {
            inputs: vec![spec.inputs[1].clone(), spec.inputs[0].clone()],
        };
        let swapped_hash = model_input_contract_hash(&swapped).expect("swapped hash");
        assert!(verify_input_binding("classical", &swapped, swapped_hash, &spec).is_err());
        assert!(verify_input_binding("weighted", &swapped, swapped_hash, &spec).is_err());

        let malformed = ModelInputContract {
            inputs: vec![spec.inputs[0].clone(), spec.inputs[0].clone()],
        };
        assert!(verify_input_binding("sell scorer", &malformed, spec_hash, &spec).is_err());
    }

    #[test]
    fn frozen_evidence_without_values() {
        let (version, dataset, run) = subject();
        let examples = vec![
            training_example(dataset.window_start + Duration::minutes(10)),
            training_example(dataset.window_start + Duration::minutes(20)),
        ];
        let feature_contract_hash = &dataset.feature_schema_hash;
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
            assert_eq!(row.feature_contract_hash, feature_contract_hash.to_string());
            assert_eq!(row.transform_hash, transform_hash.to_string());
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
