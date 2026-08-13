//! Offline model-training orchestration.
//!
//! Loads a frozen training dataset's Parquet as the complete feature/factor/label
//! truth, verifies its semantic content hash, trains with the pure research
//! trainer, content-addresses
//! the artifact into the [`ArtifactStore`], and registers a **Candidate**
//! `quant_model_version` plus a `Training` `quant_model_run`. Training never
//! rematerializes or replaces frozen rows. The weighted-factor path is always available;
//! the classical (smartcore) path is linked only under the `ml-classical`
//! feature and otherwise fails closed with `RuntimeUnavailable`.

#[cfg(not(feature = "ml-classical"))]
use std::future;
use std::{collections::BTreeSet, sync::Arc};

use quant_pivot_compute::{ComputeExecutor, OfflineMemory};
use quant_pivot_error::{QuantError, QuantResult, research::ResearchError, storage::StorageError};
use quant_pivot_models::{
    domain::{
        governance::DecisionPolicySnapshotInfo,
        quant::{
            FactorRegistrationOutcome, JobProgressSink, ModelSpecInfo, ModelVersionInfo,
            NewFactorDefinition, NewModelRun, NewModelVersion, TrainingDatasetInfo,
            TrainingDatasetMaterialization,
        },
    },
    enums::{
        common::MarketCategory,
        domain::DomainFamily,
        factor::FactorFamily,
        model::{ClassicalKind, ModelFamily},
        quant::{
            CalibrationKind, DatasetPurpose, ModelRunErrorCode, ModelRunKind, TrainingDatasetStatus,
        },
    },
    types::{
        CalibrationArtifactId, ContentHash, DecisionPolicySnapshotId, ModelInputContract,
        ModelRunId, ModelVersionId, ResearchJobProgress, ResearchProfileArtifact,
        TrainingDatasetId,
        factor::{FactorDefinitionRef, FactorServingPlane},
        model_lineage::ModelVersionDerivation,
        model_metrics::{
            GovernedSellEstimatorMetrics, HeldOutMetricKind, LearningToRankInSampleMetrics,
            ModelArtifactTrainingLineage, ModelValidationMetrics, ModelVersionMetrics,
            ObjectiveComponentMetrics, RankingDiagnosticsMetrics,
        },
        model_serving::{
            ModelServingBindings, ModelServingCalibrationArtifactRef, ModelServingContract,
            ModelServingDatasetBinding, ModelServingEstimatorBinding, ModelServingEstimatorInput,
            ModelServingFactorBinding, ModelServingModelBinding, ModelServingPolicySnapshotBinding,
            ModelServingSchemaBinding, ModelServingTradePolicyBinding,
            ModelServingTransformBinding,
        },
        model_training::ModelTrainingObjective,
        stable_name::FeatureName,
        training::TrainingSampleSource,
    },
};
#[cfg(feature = "ml-classical")]
use quant_pivot_models::{
    enums::quant::ModelSerializationFormat,
    hashing::CanonicalDigest,
    types::{
        model_metrics::{ClassicalInSampleMetrics, ModelFeatureImportance},
        stable_name::ModelMetricName,
    },
};
use quant_pivot_repository::traits::{
    CalibrationArtifactRepository, FactorRepository, ModelRegistryRepository, ModelRunRepository,
    TrainingDatasetRepository,
};
use quant_pivot_research::{
    artifact::ArtifactStore,
    factors::{FactorEngine, names::STRUCT_FAVORITE_LONGSHOT},
    features::{FeatureSchema, SourceRequirement},
    hashing::ResearchHasher,
    model::{
        CancellationProbe, FavoriteLongshotBiasTable, HorizonMultipliers, LabelSelector,
        ModelArtifact, ModelTrainer, ReturnModelSpec, SellScorerTrainer,
        SubstitutionConfidenceRules, TrainModelRequest, TrainSellScorerRequest,
        TrainingObjectiveReport, ValidationReport, ValidationSpec, WeightedFactorTrainer,
        artifact::{ModelPayload, SellEstimatorSpec, SellScorerOutputSpec},
        factor_heads::FactorHeadSpec,
        model_input_contract_hash,
        objective::{ObjectiveComponentReport, RankingDiagnostics, runtime_training_objective},
        sell_scorer::trainer::SellTrainingMetrics,
    },
    training::{LabelName, TrainingExample, label_names_for_sources},
    validation::PurgeConfig,
};
#[cfg(feature = "ml-classical")]
use quant_pivot_research::{
    artifact::{ArtifactKey, ArtifactNamespace},
    model::{
        ClassicalAdapterRegistry, ClassicalOutputSemantics, ScoreMultiplierSpec,
        artifact::ClassicalModelPayload,
    },
    training::{
        RETURN_TO_HORIZON, TOKEN_PAYOUT_RATIO, TrainingMatrix, build_borrowed_matrix,
        matrix_spec_from_contract,
    },
};
use tokio::runtime::Handle;
use tokio_util::sync::CancellationToken;

use crate::service::{
    model_serving_preimage::ModelServingPreimageService,
    trade_policy_evidence::TradePolicyEvidenceDurability,
    trade_policy_preimage::{TradePolicyPreimageTarget, TradePolicyPreimageVerifier},
    training_dataset::{require_dataset_materialization, verify_frozen_dataset_artifact},
};

const MODEL_RUN_DIAGNOSTIC_MAX_CHARS: usize = 4_096;

/// Fail closed with a terminal [`ResearchError::Cancelled`] when the job was
/// cooperatively cancelled (operator cancel, lease loss, or graceful shutdown),
/// checked at each coarse training-stage boundary.
fn ensure_not_cancelled(cancel: &CancellationToken, phase: &str) -> QuantResult<()> {
    if cancel.is_cancelled() {
        return Err(ResearchError::Cancelled {
            detail: format!("model training cancelled at `{phase}`"),
        }
        .into());
    }
    Ok(())
}

fn cancellation_probe(cancel: &CancellationToken) -> CancellationProbe {
    let cancel = cancel.clone();
    CancellationProbe::new(move || cancel.is_cancelled())
}

/// Repository + store dependencies for the trainer service.
pub struct ModelTrainerServiceDeps {
    /// Process-wide offline CPU and memory governor.
    pub compute: Arc<ComputeExecutor>,
    /// Frozen training-dataset ledger.
    pub dataset_repo: Arc<dyn TrainingDatasetRepository>,
    /// Immutable factor-definition registry.
    pub factor_repo: Arc<dyn FactorRepository>,
    /// Content-addressed artifact store (model bytes).
    pub artifact_store: Arc<dyn ArtifactStore>,
    /// Model registry (spec/version lifecycle).
    pub model_registry_repo: Arc<dyn ModelRegistryRepository>,
    /// Model-run ledger.
    pub model_run_repo: Arc<dyn ModelRunRepository>,
    /// Immutable calibration-artifact ledger used to reverify bias-table preimages.
    pub calibration_repo: Arc<dyn CalibrationArtifactRepository>,
    /// Canonical resolver for executable-label `TradePolicy` dependency graphs.
    pub trade_policy_preimages: Arc<TradePolicyPreimageVerifier>,
    /// Shared side-effect-free Dataset/Source Slice serving-preimage verifier.
    pub serving_preimages: Arc<ModelServingPreimageService>,
}

/// Complete persisted policy snapshot governing training.
pub struct ModelTrainerConfig {
    /// The database-authoritative row plus all resolved profile preimages.
    pub policy_snapshot: DecisionPolicySnapshotInfo,
}

/// A training request resolved by the admin port.
pub struct TrainModelInput {
    /// Pre-assigned registry id (async job engine) or minted for direct calls.
    pub model_version_id: ModelVersionId,
    /// Pre-assigned run id frozen in the durable job for exact lease recovery.
    pub model_run_id: ModelRunId,
    /// Complete immutable target model specification.
    pub model_spec: ModelSpecInfo,
    /// Frozen dataset to train on.
    pub training_dataset_id: TrainingDatasetId,
}

/// Successful training outcome — version row plus the materialization run id.
pub struct TrainModelOutcome {
    pub version: ModelVersionInfo,
    pub model_run_id: ModelRunId,
}

struct VerifiedServingInputs {
    feature_schema: FeatureSchema,
    factor_plane: FactorServingPlane,
    training_dataset_hash: ContentHash,
    policy_snapshot: ModelServingPolicySnapshotBinding,
    profile: ResearchProfileArtifact,
    bias_table: Option<ModelServingCalibrationArtifactRef>,
    trade_policy: Option<ModelServingTradePolicyBinding>,
}

struct PendingRankingMetrics {
    in_sample: TrainingObjectiveReport,
    validation: ValidationReport,
}

#[cfg(feature = "ml-classical")]
struct PendingClassicalMetrics {
    kind: ClassicalKind,
    in_sample: ClassicalInSampleMetrics,
    validation: ModelValidationMetrics,
    feature_importances: Vec<ModelFeatureImportance>,
}

enum PendingModelMetrics {
    LearningToRank(Box<PendingRankingMetrics>),
    SellGoverned(SellTrainingMetrics),
    #[cfg(feature = "ml-classical")]
    Classical(Box<PendingClassicalMetrics>),
}

struct PreparedModelPayload {
    payload: ModelPayload,
    transform: ModelServingTransformBinding,
    metrics: PendingModelMetrics,
    objective: ModelTrainingObjective,
}

struct ModelTrainingCommit<'a> {
    model_run_id: &'a ModelRunId,
    model_version_id: &'a ModelVersionId,
    input: &'a TrainModelInput,
    dataset: &'a TrainingDatasetInfo,
    examples: &'a Arc<[TrainingExample]>,
    serving: &'a VerifiedServingInputs,
    cancel: &'a CancellationToken,
}

/// Offline trainer service.
pub struct ModelTrainerService {
    deps: ModelTrainerServiceDeps,
    config: ModelTrainerConfig,
}

impl TrainModelInput {
    fn label(&self) -> LabelSelector {
        LabelSelector {
            name: LabelName::new(
                self.model_spec
                    .training_contract
                    .target
                    .label_name()
                    .to_owned(),
            ),
            horizon_secs: self
                .model_spec
                .training_contract
                .target
                .label_horizon_secs(),
        }
    }

    fn prediction_horizon(&self) -> QuantResult<u64> {
        u64::try_from(self.model_spec.prediction_horizon_secs).map_err(|error| {
            QuantError::from(ResearchError::InvalidModelArtifact {
                detail: format!("model prediction horizon does not fit u64: {error}"),
            })
        })
    }
}

impl ModelTrainerConfig {
    fn verified_policy_binding(&self) -> QuantResult<ModelServingPolicySnapshotBinding> {
        let info = &self.policy_snapshot;
        let recomputed_hash = info.snapshot.persistence_hash().map_err(|error| {
            ResearchError::InvalidModelArtifact {
                detail: format!("policy snapshot persistence hash failed: {error}"),
            }
        })?;
        if recomputed_hash != info.snapshot_hash
            || DecisionPolicySnapshotId::from_content_hash(&recomputed_hash)
                != info.decision_policy_snapshot_id
        {
            return Err(ResearchError::InvalidModelArtifact {
                detail: format!(
                    "policy snapshot identity mismatch: row id={} hash={}, recomputed id={} hash={recomputed_hash}",
                    info.decision_policy_snapshot_id,
                    info.snapshot_hash,
                    DecisionPolicySnapshotId::from_content_hash(&recomputed_hash),
                ),
            }
            .into());
        }
        let revisions = &info.snapshot.revisions;
        let revisions_match = revisions.recommendation_policy.as_ref()
            == Some(&info.recommendation_policy_revision_id)
            && revisions.execution_risk_policy.as_ref()
                == Some(&info.execution_risk_policy_revision_id)
            && revisions.model_routing.as_ref() == Some(&info.model_routing_revision_id)
            && revisions.report_schedule.as_ref() == Some(&info.report_schedule_revision_id)
            && revisions.operations_policy.as_ref() == Some(&info.operations_policy_revision_id)
            && revisions.execution_automation_policy.as_ref()
                == Some(&info.execution_automation_policy_revision_id);
        if !revisions_match {
            return Err(ResearchError::InvalidModelArtifact {
                detail: "policy snapshot revision projections differ from the persisted row"
                    .to_owned(),
            }
            .into());
        }
        let profile_artifacts = info
            .snapshot
            .profile_artifacts
            .references()
            .map_err(|error| ResearchError::InvalidModelArtifact {
                detail: format!("policy profile preimage verification failed: {error}"),
            })?;
        Ok(ModelServingPolicySnapshotBinding {
            decision_policy_snapshot_id: info.decision_policy_snapshot_id,
            snapshot_hash: info.snapshot_hash,
            profile_artifacts,
        })
    }

    fn validation_purge(&self) -> PurgeConfig {
        let snapshot = &self.policy_snapshot.snapshot;
        PurgeConfig {
            embargo_pct: snapshot
                .profile_artifacts
                .research_method
                .research
                .validation
                .purge
                .embargo_pct
                .value,
            min_embargo_secs: snapshot
                .profile_artifacts
                .features
                .definition
                .max_lookback_secs(),
        }
    }
}

impl ModelTrainerService {
    /// Assemble the service from persistence dependencies and frozen trainer config.
    #[must_use]
    pub const fn new(deps: ModelTrainerServiceDeps, config: ModelTrainerConfig) -> Self {
        Self { deps, config }
    }

    /// Train a model and register it as a Candidate version.
    ///
    /// Reports coarse but honest phases (`load → decode → verify → register → fit`) to
    /// `progress`; the fit itself is a single opaque research-trainer call,
    /// offloaded to the governed offline pool. `cancel` is polled at each stage boundary.
    pub async fn train(
        &self,
        input: TrainModelInput,
        progress: &dyn JobProgressSink,
        cancel: &CancellationToken,
    ) -> QuantResult<TrainModelOutcome> {
        ensure_not_cancelled(cancel, "load")?;
        progress.report(ResearchJobProgress::indeterminate("load", 0));
        let dataset = self.load_ready_dataset(&input.training_dataset_id).await?;
        let policy_snapshot = self.config.verified_policy_binding()?;
        let (feature_schema, factor_plane, profile) =
            self.validate_dataset_contracts(&dataset, &input, &policy_snapshot)?;
        self.deps
            .serving_preimages
            .verify_dataset_objects(&dataset, &profile)
            .await?;
        let bias_table = self
            .load_bias_table(&factor_plane, input.model_spec.model_family)
            .await?;
        let trade_policy = Box::pin(self.deps.trade_policy_preimages.verify(
            &self.deps.serving_preimages,
            TradePolicyPreimageTarget {
                dataset: &dataset,
                model_spec: &input.model_spec,
                policy_snapshot: &policy_snapshot,
                profile: &profile,
            },
            TradePolicyEvidenceDurability::ContentVerified,
        ))
        .await?
        .map(|verified| verified.binding().clone());
        let training_dataset_hash = *require_dataset_materialization(&dataset)?.dataset_hash;
        let serving_inputs = VerifiedServingInputs {
            feature_schema,
            factor_plane,
            training_dataset_hash,
            policy_snapshot,
            profile,
            bias_table,
            trade_policy,
        };
        progress.report(ResearchJobProgress::indeterminate("decode", 0));
        let examples: Arc<[TrainingExample]> = self.decode_examples(&dataset).await?.into();
        Self::validate_example_contracts(&dataset, &input, &examples)?;
        Self::validate_example_scope(&serving_inputs.profile, &examples)?;
        ensure_not_cancelled(cancel, "verify")?;
        progress.report(ResearchJobProgress::indeterminate(
            "verify",
            examples.len() as u64,
        ));
        ensure_not_cancelled(cancel, "register")?;
        progress.report(ResearchJobProgress::indeterminate(
            "register",
            serving_inputs.factor_plane.definitions().len() as u64,
        ));
        if !input.model_spec.model_family.is_classical() {
            self.register_factor_plane(&serving_inputs.factor_plane)
                .await?;
        }

        let model_version_id = input.model_version_id;
        let model_run_id = input.model_run_id;
        self.create_run(&model_run_id, &dataset).await?;

        let result = async {
            ensure_not_cancelled(cancel, "fit")?;
            progress.report(ResearchJobProgress::indeterminate(
                "fit",
                examples.len() as u64,
            ));
            let version = self
                .train_and_commit(ModelTrainingCommit {
                    model_run_id: &model_run_id,
                    model_version_id: &model_version_id,
                    input: &input,
                    dataset: &dataset,
                    examples: &examples,
                    serving: &serving_inputs,
                    cancel,
                })
                .await?;
            QuantResult::Ok(version)
        }
        .await;
        match result {
            Ok(version) => Ok(TrainModelOutcome {
                version,
                model_run_id,
            }),
            Err(error) => {
                self.finalize_run_error(&model_run_id, &error).await?;
                Err(error)
            }
        }
    }

    async fn finalize_run_error(
        &self,
        model_run_id: &ModelRunId,
        error: &QuantError,
    ) -> QuantResult<()> {
        let diagnostic = Self::run_diagnostic(error);
        let finalization = if matches!(error, QuantError::Research(ResearchError::Cancelled { .. }))
        {
            self.deps
                .model_run_repo
                .cancel(model_run_id, diagnostic)
                .await
        } else {
            self.deps
                .model_run_repo
                .fail(model_run_id, ModelRunErrorCode::TrainingFailed, diagnostic)
                .await
        };
        finalization.map(|_| ()).map_err(|finalizer| {
            ResearchError::ModelRunFinalization {
                primary: error.to_string(),
                finalizer: finalizer.to_string(),
            }
            .into()
        })
    }

    fn run_diagnostic(error: &QuantError) -> String {
        let detail = error.to_string();
        let mut chars = detail.chars();
        let bounded = chars
            .by_ref()
            .take(MODEL_RUN_DIAGNOSTIC_MAX_CHARS)
            .collect::<String>();
        if chars.next().is_none() {
            return bounded;
        }
        let mut truncated = bounded
            .chars()
            .take(MODEL_RUN_DIAGNOSTIC_MAX_CHARS - 1)
            .collect::<String>();
        truncated.push('…');
        truncated
    }

    fn validate_dataset_lineage<'a>(
        dataset: &'a TrainingDatasetInfo,
        input: &TrainModelInput,
        policy_snapshot: &ModelServingPolicySnapshotBinding,
    ) -> QuantResult<TrainingDatasetMaterialization<'a>> {
        let definition = input.model_spec.definition();
        definition
            .validate()
            .map_err(|detail| ResearchError::InvalidModelArtifact {
                detail: format!("invalid persisted model specification: {detail}"),
            })?;
        let definition_hash =
            definition
                .content_hash()
                .map_err(|error| ResearchError::InvalidModelArtifact {
                    detail: format!("model-spec definition hash failed: {error}"),
                })?;
        if definition_hash != input.model_spec.definition_hash {
            return Err(ResearchError::InvalidModelArtifact {
                detail: format!(
                    "model-spec definition mismatch: row {}, recomputed {definition_hash}",
                    input.model_spec.definition_hash
                ),
            }
            .into());
        }
        if dataset.model_spec_id != input.model_spec.model_spec_id {
            return Err(ResearchError::DatasetBuild {
                detail: format!(
                    "frozen dataset model spec mismatch: dataset {}, requested {}",
                    dataset.model_spec_id, input.model_spec.model_spec_id
                ),
            }
            .into());
        }
        if dataset.model_spec_definition_hash != definition_hash {
            return Err(ResearchError::DatasetBuild {
                detail: format!(
                    "frozen dataset model-spec definition mismatch: dataset {}, verified {definition_hash}",
                    dataset.model_spec_definition_hash
                ),
            }
            .into());
        }
        if dataset.decision_policy_snapshot_id != policy_snapshot.decision_policy_snapshot_id {
            return Err(ResearchError::DatasetBuild {
                detail: format!(
                    "frozen dataset policy snapshot mismatch: dataset {}, requested {}",
                    dataset.decision_policy_snapshot_id,
                    policy_snapshot.decision_policy_snapshot_id
                ),
            }
            .into());
        }
        if dataset.source_lineage.runtime_config_hash != policy_snapshot.snapshot_hash {
            return Err(ResearchError::DatasetBuild {
                detail: format!(
                    "frozen source runtime hash mismatch: dataset {}, policy {}",
                    dataset.source_lineage.runtime_config_hash, policy_snapshot.snapshot_hash
                ),
            }
            .into());
        }
        if dataset.model_family != input.model_spec.model_family {
            return Err(ResearchError::DatasetBuild {
                detail: format!(
                    "frozen dataset model family mismatch: dataset {}, requested {}",
                    dataset.model_family.as_str(),
                    input.model_spec.model_family.as_str()
                ),
            }
            .into());
        }
        let materialization = require_dataset_materialization(dataset)?;
        materialization
            .manifest
            .validate()
            .map_err(|error| ResearchError::DatasetBuild {
                detail: format!("invalid frozen Dataset v3 manifest: {error}"),
            })?;
        if materialization.manifest.model_family != input.model_spec.model_family {
            return Err(ResearchError::DatasetBuild {
                detail: format!(
                    "frozen manifest model family mismatch: manifest {}, requested {}",
                    materialization.manifest.model_family.as_str(),
                    input.model_spec.model_family.as_str()
                ),
            }
            .into());
        }
        if materialization.manifest.model_spec_definition_hash != definition_hash
            || materialization.manifest.model_spec_id != input.model_spec.model_spec_id
        {
            return Err(ResearchError::DatasetBuild {
                detail: "frozen Dataset v3 manifest differs from the verified model specification"
                    .to_owned(),
            }
            .into());
        }
        if materialization.manifest.trade_policy_artifact_id
            != input
                .model_spec
                .training_contract
                .evaluation_trade_policy_artifact_id
        {
            return Err(ResearchError::DatasetBuild {
                detail:
                    "frozen Dataset v3 trade-policy binding differs from the model training contract"
                        .to_owned(),
            }
            .into());
        }
        let target_horizon = input
            .model_spec
            .training_contract
            .target
            .label_horizon_secs();
        if target_horizon > 0
            && !materialization
                .manifest
                .horizons_secs
                .contains(&target_horizon)
        {
            return Err(ResearchError::DatasetBuild {
                detail: format!("frozen Dataset v3 omitted target label horizon {target_horizon}s"),
            }
            .into());
        }
        Ok(materialization)
    }

    fn validate_dataset_contracts(
        &self,
        dataset: &TrainingDatasetInfo,
        input: &TrainModelInput,
        policy_snapshot: &ModelServingPolicySnapshotBinding,
    ) -> QuantResult<(FeatureSchema, FactorServingPlane, ResearchProfileArtifact)> {
        let materialization = Self::validate_dataset_lineage(dataset, input, policy_snapshot)?;
        let runtime = &self.config.policy_snapshot.snapshot;
        let feature_schema = FeatureSchema::build(&runtime.profile_artifacts.features.definition)?;
        let feature_schema_hash = ResearchHasher::feature_schema(&feature_schema)?;
        if feature_schema.version() != dataset.feature_schema_version
            || feature_schema.version() != input.model_spec.feature_schema_version
        {
            return Err(ResearchError::DatasetBuild {
                detail: format!(
                    "feature schema version mismatch: dataset {}, model spec {}, runtime {}",
                    dataset.feature_schema_version,
                    input.model_spec.feature_schema_version,
                    feature_schema.version()
                ),
            }
            .into());
        }
        if &feature_schema_hash != materialization.feature_schema_hash {
            return Err(ResearchError::DatasetBuild {
                detail: format!(
                    "frozen dataset feature contract mismatch: dataset {}, runtime {}",
                    materialization.feature_schema_hash, feature_schema_hash
                ),
            }
            .into());
        }
        let profile_ref = materialization
            .manifest
            .source_lineage
            .research_profile_artifact_id
            .profile_ref();
        let profile = profile_ref
            .resolve_builtin_research_profile()
            .map_err(|detail| ResearchError::DatasetBuild {
                detail: format!("research profile preimage verification failed: {detail}"),
            })?;
        if dataset.research_profile_artifact_id != profile.profile_ref.artifact_id()
            || dataset.source_lineage.research_profile_artifact_id
                != profile.profile_ref.artifact_id()
        {
            return Err(ResearchError::DatasetBuild {
                detail: "dataset research-profile projections differ from the verified builtin"
                    .to_owned(),
            }
            .into());
        }
        let prediction_horizon_secs = input.prediction_horizon()?;
        if prediction_horizon_secs != profile.spec.target_horizon_secs {
            return Err(ResearchError::DatasetBuild {
                detail: format!(
                    "model prediction horizon {prediction_horizon_secs}s differs from ResearchProfile {}@{} target {}s",
                    profile.profile_ref.id,
                    profile.profile_ref.version,
                    profile.spec.target_horizon_secs,
                ),
            }
            .into());
        }
        let expected_plane = if input.model_spec.model_family.is_classical() {
            FactorServingPlane::try_empty().map_err(|error| ResearchError::DatasetBuild {
                detail: format!("build factor-free classical serving plane: {error}"),
            })?
        } else {
            FactorEngine::for_model_scope(
                &runtime.profile_artifacts.scoring.definition,
                &runtime.profile_artifacts.features.definition,
                &runtime.profile_artifacts.domain.definition,
                profile.spec.category,
                None,
            )
            .serving_plane()?
            .clone()
        };
        if &expected_plane != materialization.factor_serving_plane {
            return Err(ResearchError::DatasetBuild {
                detail: format!(
                    "frozen dataset factor plane mismatch: dataset {} ({} revisions), runtime {} ({} revisions)",
                    materialization.factor_serving_plane.factor_schema_hash(),
                    materialization.factor_serving_plane.definitions().len(),
                    expected_plane.factor_schema_hash(),
                    expected_plane.definitions().len()
                ),
            }
            .into());
        }
        Self::validate_category_plane(
            input.model_spec.model_family,
            profile.spec.category,
            &expected_plane,
        )?;
        model_input_contract_hash(&input.model_spec.input_contract)?;
        Self::required_domain_families(
            &feature_schema,
            &expected_plane,
            &input.model_spec.input_contract,
            profile.spec.category,
        )?;
        Ok((feature_schema, expected_plane, profile))
    }

    fn validate_category_plane(
        model_family: ModelFamily,
        category_scope: Option<MarketCategory>,
        plane: &FactorServingPlane,
    ) -> QuantResult<()> {
        if model_family.is_classical() {
            return Ok(());
        }
        let required_family = match category_scope {
            Some(MarketCategory::Crypto) => Some(FactorFamily::DomainCrypto),
            Some(MarketCategory::Weather) => Some(FactorFamily::DomainWeather),
            _ => None,
        };
        if required_family.is_some_and(|family| {
            !plane
                .definitions()
                .iter()
                .any(|revision| revision.definition().family == family)
        }) {
            return Err(ResearchError::DatasetBuild {
                detail: format!(
                    "{category_scope:?} ResearchProfile requires an enabled matching domain factor plane"
                ),
            }
            .into());
        }
        Ok(())
    }

    async fn load_bias_table(
        &self,
        plane: &FactorServingPlane,
        model_family: ModelFamily,
    ) -> QuantResult<Option<ModelServingCalibrationArtifactRef>> {
        if model_family.is_classical() {
            return Ok(None);
        }
        let reference = self
            .config
            .policy_snapshot
            .snapshot
            .profile_artifacts
            .scoring
            .definition
            .structural
            .favorite_longshot
            .bias_table_ref
            .as_deref();
        let Some(reference) = reference else {
            return Ok(None);
        };
        let artifact_id = reference
            .parse::<CalibrationArtifactId>()
            .map_err(|error| ResearchError::DatasetBuild {
                detail: format!("bias-table reference `{reference}` is invalid: {error}"),
            })?;
        let info = self
            .deps
            .calibration_repo
            .find_by_id(&artifact_id)
            .await?
            .ok_or_else(|| StorageError::NotFound {
                entity: "calibration_artifact",
                id: artifact_id.to_string(),
            })?;
        let table = FavoriteLongshotBiasTable::from_persisted(&info)?;
        if table.table_id != artifact_id
            || table.content_hash != info.content_hash
            || !plane
                .definitions()
                .iter()
                .any(|definition| definition.factor_name() == &STRUCT_FAVORITE_LONGSHOT)
        {
            return Err(ResearchError::DatasetBuild {
                detail: "verified bias-table preimage is incompatible with the frozen factor plane"
                    .to_owned(),
            }
            .into());
        }
        Ok(Some(ModelServingCalibrationArtifactRef {
            artifact_id,
            kind: CalibrationKind::MarketPriceBias,
            content_hash: info.content_hash,
        }))
    }

    async fn register_factor_plane(&self, plane: &FactorServingPlane) -> QuantResult<()> {
        let registrations = plane
            .definitions()
            .iter()
            .cloned()
            .map(NewFactorDefinition::from)
            .collect();
        let outcomes = self
            .deps
            .factor_repo
            .register_definitions(registrations)
            .await?;
        let mut registered = Vec::with_capacity(outcomes.len());
        for outcome in &outcomes {
            let definition = match outcome {
                FactorRegistrationOutcome::Inserted(definition)
                | FactorRegistrationOutcome::AlreadyPresent(definition) => definition,
            };
            registered.push(FactorDefinitionRef::try_from(definition).map_err(|error| {
                ResearchError::DatasetBuild {
                    detail: format!(
                        "persisted factor revision `{}` failed reconstruction: {error}",
                        definition.name
                    ),
                }
            })?);
        }
        let registered_plane = FactorServingPlane::try_seal(registered).map_err(|error| {
            ResearchError::DatasetBuild {
                detail: format!("seal registered factor serving plane: {error}"),
            }
        })?;
        if &registered_plane != plane {
            return Err(ResearchError::DatasetBuild {
                detail: format!(
                    "registered factor plane mismatch: frozen {} ({} revisions), persisted {} ({} revisions)",
                    plane.factor_schema_hash(),
                    plane.definitions().len(),
                    registered_plane.factor_schema_hash(),
                    registered_plane.definitions().len()
                ),
            }
            .into());
        }
        Ok(())
    }

    /// Load the dataset, accepting only an integrity-gated `Ready` artifact.
    async fn load_ready_dataset(
        &self,
        training_dataset_id: &TrainingDatasetId,
    ) -> QuantResult<TrainingDatasetInfo> {
        let dataset = self
            .deps
            .dataset_repo
            .find_by_id(training_dataset_id)
            .await?
            .ok_or_else(|| StorageError::NotFound {
                entity: "training_dataset",
                id: training_dataset_id.to_string(),
            })?;
        if dataset.status != TrainingDatasetStatus::Ready {
            return Err(ResearchError::DatasetBuild {
                detail: format!(
                    "training requires a Ready dataset, got {}",
                    dataset.status.as_str()
                ),
            }
            .into());
        }
        if dataset.purpose != DatasetPurpose::Training {
            return Err(ResearchError::DatasetBuild {
                detail: format!(
                    "model training requires purpose=training, got {}",
                    dataset.purpose.as_str()
                ),
            }
            .into());
        }
        Ok(dataset)
    }

    /// Fetch + decode the dataset's Parquet examples.
    async fn decode_examples(
        &self,
        dataset: &TrainingDatasetInfo,
    ) -> QuantResult<Vec<TrainingExample>> {
        let materialization = require_dataset_materialization(dataset)?;
        let bytes = self
            .deps
            .artifact_store
            .get(materialization.parquet_uri)
            .await?;
        verify_frozen_dataset_artifact(dataset, &bytes)
    }

    fn validate_example_contracts(
        dataset: &TrainingDatasetInfo,
        input: &TrainModelInput,
        examples: &[TrainingExample],
    ) -> QuantResult<()> {
        let sample_sources =
            dataset
                .sample_sources
                .as_ref()
                .ok_or_else(|| ResearchError::DatasetBuild {
                    detail: format!(
                        "training Dataset {} has no frozen sample-source contract",
                        dataset.training_dataset_id
                    ),
                })?;
        let manifest = require_dataset_materialization(dataset)?.manifest;
        let label_names = label_names_for_sources(
            sample_sources.as_slice(),
            manifest.trade_policy_artifact_id.is_some(),
        );
        let label_schema_hash = ResearchHasher::label_schema(&label_names)?;
        if label_schema_hash != manifest.label_schema_hash {
            return Err(ResearchError::DatasetBuild {
                detail: format!(
                    "training Dataset {} label schema differs from its sample-source/trade-policy contract",
                    dataset.training_dataset_id
                ),
            }
            .into());
        }

        let target = input.label();
        if !label_names.contains(&target.name) {
            return Err(ResearchError::DatasetBuild {
                detail: format!(
                    "ModelSpec target {} is absent from Dataset {} canonical label schema",
                    target.name, dataset.training_dataset_id
                ),
            }
            .into());
        }
        let mut target_rows = 0_usize;
        for example in examples {
            let mut labels = BTreeSet::new();
            for label in &example.labels {
                if !label_names.contains(&label.label_name) {
                    return Err(ResearchError::DatasetBuild {
                        detail: format!(
                            "Dataset example {} contains undeclared label {}",
                            example.example_id, label.label_name
                        ),
                    }
                    .into());
                }
                if !labels.insert((label.label_name.clone(), label.horizon_secs)) {
                    return Err(ResearchError::DatasetBuild {
                        detail: format!(
                            "Dataset example {} duplicates label {}@{}s",
                            example.example_id, label.label_name, label.horizon_secs
                        ),
                    }
                    .into());
                }
                if (&label.label_name, label.horizon_secs) == (&target.name, target.horizon_secs) {
                    target_rows = target_rows.saturating_add(1);
                }
            }
        }
        if target_rows == 0 {
            return Err(ResearchError::DatasetBuild {
                detail: format!(
                    "Dataset {} has no rows for ModelSpec target {}@{}s",
                    dataset.training_dataset_id, target.name, target.horizon_secs
                ),
            }
            .into());
        }
        Ok(())
    }

    fn validate_example_scope(
        profile: &ResearchProfileArtifact,
        examples: &[TrainingExample],
    ) -> QuantResult<()> {
        let Some(category) = profile.spec.category else {
            return Ok(());
        };
        if let Some(example) = examples
            .iter()
            .find(|example| example.selected_market.category != category)
        {
            return Err(ResearchError::DatasetBuild {
                detail: format!(
                    "ResearchProfile {}@{} is scoped to {category}, but dataset example {} belongs to {}",
                    profile.profile_ref.id,
                    profile.profile_ref.version,
                    example.example_id,
                    example.selected_market.category,
                ),
            }
            .into());
        }
        Ok(())
    }

    fn required_domain_families(
        schema: &FeatureSchema,
        plane: &FactorServingPlane,
        input_contract: &ModelInputContract,
        category_scope: Option<MarketCategory>,
    ) -> QuantResult<Vec<DomainFamily>> {
        let mut required = BTreeSet::new();
        match category_scope {
            Some(MarketCategory::Crypto) => {
                required.insert(DomainFamily::Crypto);
            }
            Some(MarketCategory::Weather) => {
                required.insert(DomainFamily::Weather);
            }
            _ => {}
        }
        for factor in plane.definitions() {
            match factor.definition().family {
                FactorFamily::DomainCrypto => {
                    required.insert(DomainFamily::Crypto);
                }
                FactorFamily::DomainWeather => {
                    required.insert(DomainFamily::Weather);
                }
                _ => {}
            }
        }
        for input in &input_contract.inputs {
            let feature_name = FeatureName::new(input.feature_name.clone());
            let feature = schema.by_name(&feature_name).ok_or_else(|| {
                ResearchError::DatasetBuild {
                    detail: format!(
                        "model input `{feature_name}` is absent from the verified feature schema"
                    ),
                }
            })?;
            match feature.source_requirement {
                SourceRequirement::DomainCryptoObservationWindow => {
                    required.insert(DomainFamily::Crypto);
                }
                SourceRequirement::DomainWeatherObservationWindow => {
                    required.insert(DomainFamily::Weather);
                }
                _ => {}
            }
        }
        Ok(required.into_iter().collect())
    }

    /// Train + register, dispatching on family.
    async fn train_and_commit(
        &self,
        commit: ModelTrainingCommit<'_>,
    ) -> QuantResult<ModelVersionInfo> {
        let ModelTrainingCommit {
            model_run_id,
            model_version_id,
            input,
            dataset,
            examples,
            serving,
            cancel,
        } = commit;
        let materialization = require_dataset_materialization(dataset)?;
        let category_scope = serving.profile.spec.category;
        let prepared = if input.model_spec.model_family.is_exit_scorer() {
            self.train_sell_scorer(input, examples, serving, cancel)
                .await?
        } else {
            match input.model_spec.model_family.classical_kind() {
                None => {
                    self.train_weighted(input, examples, serving, cancel)
                        .await?
                }
                Some(kind) => {
                    self.train_classical(input, examples, serving, kind, cancel)
                        .await?
                }
            }
        };
        let required_domain_families = Self::required_domain_families(
            &serving.feature_schema,
            &serving.factor_plane,
            &input.model_spec.input_contract,
            serving.profile.spec.category,
        )?;
        let estimator = prepared
            .payload
            .serving_estimator_binding(&serving.factor_plane)?;
        let prediction_horizon_secs = input.prediction_horizon()?;
        let contract = ModelServingContract::try_seal(ModelServingBindings {
            policy_snapshot: serving.policy_snapshot.clone(),
            required_domain_families,
            capability_registry_hashes: dataset.source_lineage.capability_registry_hashes.clone(),
            factors: ModelServingFactorBinding {
                plane: serving.factor_plane.clone(),
                bias_table: serving.bias_table.clone(),
            },
            schemas: ModelServingSchemaBinding {
                feature_schema_hash: *materialization.feature_schema_hash,
                label_schema_hash: *materialization.label_schema_hash,
            },
            transform: prepared.transform,
            model: ModelServingModelBinding {
                model_version_id: *model_version_id,
                model_spec_id: input.model_spec.model_spec_id,
                model_spec_definition_hash: input.model_spec.definition_hash,
                model_family: input.model_spec.model_family,
                category_scope,
                profile_ref: serving.profile.profile_ref.clone(),
                prediction_horizon_secs,
                estimator,
                calibration: None,
            },
            trade_policy: serving.trade_policy.clone(),
            dataset: ModelServingDatasetBinding {
                manifest: materialization.manifest.clone(),
                manifest_hash: *materialization.manifest_hash,
                artifact_bytes_hash: *materialization.artifact_bytes_hash,
            },
        })
        .map_err(|error| ResearchError::InvalidModelArtifact {
            detail: format!("seal complete model-serving contract: {error}"),
        })?;
        let artifact = ModelArtifact::try_seal(contract, prepared.payload)?;
        let metrics = prepared.metrics.finalize(&artifact)?;
        let serving_contract = artifact.header().serving_contract().clone();
        let bindings = serving_contract.bindings();
        let trade_policy = bindings
            .trade_policy
            .as_ref()
            .map(|binding| (binding.artifact_id, binding.content_hash));
        let artifact_hash = artifact.content_hash()?;
        let key = ModelArtifact::artifact_key(&artifact_hash)?;
        self.deps
            .artifact_store
            .put(key, &artifact.to_bytes()?)
            .await?;

        let version = self
            .deps
            .model_registry_repo
            .next_version_for_spec(&input.model_spec.model_spec_id)
            .await?;
        ensure_not_cancelled(cancel, "commit")?;
        let registered = self
            .deps
            .model_registry_repo
            .commit_training_model_version(
                model_run_id,
                NewModelVersion {
                    model_version_id: *model_version_id,
                    model_spec_id: input.model_spec.model_spec_id,
                    version,
                    artifact_hash,
                    serving_contract,
                    category_scope,
                    profile_ref: serving.profile.profile_ref.clone(),
                    training_dataset_id: Some(input.training_dataset_id),
                    trade_policy_artifact_id: trade_policy.map(|binding| binding.0),
                    trade_policy_hash: trade_policy.map(|binding| binding.1),
                    derivation: ModelVersionDerivation::Training,
                    metrics,
                    training_objective: prepared.objective,
                },
            )
            .await?;
        Ok(registered)
    }

    /// Weighted-factor training path (always linked).
    async fn train_weighted(
        &self,
        input: &TrainModelInput,
        examples: &Arc<[TrainingExample]>,
        serving: &VerifiedServingInputs,
        cancel: &CancellationToken,
    ) -> QuantResult<PreparedModelPayload> {
        let factors = &self
            .config
            .policy_snapshot
            .snapshot
            .profile_artifacts
            .scoring
            .definition;
        let seed_head = FactorHeadSpec::from_config(&serving.factor_plane, &factors.factor_head)?;
        let objective = runtime_training_objective(
            &self
                .config
                .policy_snapshot
                .snapshot
                .profile_artifacts
                .research_method
                .research
                .training,
        )?;
        let validation_purge = self.config.validation_purge();
        let request = TrainModelRequest {
            cancellation: cancellation_probe(cancel),
            examples: Arc::clone(examples),
            label: input.label(),
            factor_plane: serving.factor_plane.clone(),
            seed_head,
            objective: objective.clone(),
            validation: ValidationSpec {
                folds: input.model_spec.training_contract.validation_folds,
                embargo_pct: validation_purge.embargo_pct,
                min_embargo_secs: validation_purge.min_embargo_secs,
            },
            horizon_multipliers: HorizonMultipliers::conservative(),
            substitution_rules: SubstitutionConfidenceRules::conservative(),
            return_model: ReturnModelSpec::heuristic_default(),
            input_contract: input.model_spec.input_contract.clone(),
            factor_cross_section: factors.cross_section.clone(),
        };
        let runtime = Handle::current();
        let cancellation = cancel.clone();
        let trained = self
            .deps
            .compute
            .run_offline_cancellable(OfflineMemory::try_gib(8)?, cancel, move || {
                ensure_not_cancelled(&cancellation, "weighted fit start")?;
                let trained = runtime.block_on(WeightedFactorTrainer::new().train(request))?;
                ensure_not_cancelled(&cancellation, "weighted fit completion")?;
                Ok(trained)
            })
            .await?;
        Ok(PreparedModelPayload {
            payload: ModelPayload::WeightedFactor(Box::new(trained.payload)),
            transform: ModelServingTransformBinding {
                input_contract_hash: trained.input_contract_hash,
                input_transform_hash: trained.input_transform_hash,
                training_input_hash: trained.training_input_hash,
                training_dataset_hash: serving.training_dataset_hash,
            },
            metrics: PendingModelMetrics::LearningToRank(Box::new(PendingRankingMetrics {
                in_sample: trained.in_sample_metrics,
                validation: trained.validation_metrics,
            })),
            objective: ModelTrainingObjective::learning_to_rank(objective),
        })
    }

    /// Validate and freeze a governed Sell estimator without same-data refit.
    async fn train_sell_scorer(
        &self,
        input: &TrainModelInput,
        examples: &Arc<[TrainingExample]>,
        serving: &VerifiedServingInputs,
        cancel: &CancellationToken,
    ) -> QuantResult<PreparedModelPayload> {
        if !examples
            .iter()
            .all(|example| example.sample_source == TrainingSampleSource::ExitDecision)
        {
            return Err(ResearchError::DatasetBuild {
                detail: "HoldVsExitWeighted training requires ExitDecision-only samples".to_owned(),
            }
            .into());
        }
        let factors = &self
            .config
            .policy_snapshot
            .snapshot
            .profile_artifacts
            .scoring
            .definition;
        let factor_head = FactorHeadSpec::from_config(&serving.factor_plane, &factors.factor_head)?;
        let request = TrainSellScorerRequest {
            cancellation: cancellation_probe(cancel),
            examples: Arc::clone(examples),
            label: input.label(),
            factor_plane: serving.factor_plane.clone(),
            factor_head,
            estimator: SellEstimatorSpec::try_from(&factors.sell_scorer)?,
            output_spec: SellScorerOutputSpec::try_from(&factors.sell_scorer)?,
            input_contract: input.model_spec.input_contract.clone(),
            factor_cross_section: factors.cross_section.clone(),
        };
        let cancellation = cancel.clone();
        let trained = self
            .deps
            .compute
            .run_offline_cancellable(OfflineMemory::try_gib(8)?, cancel, move || {
                ensure_not_cancelled(&cancellation, "sell-scorer fit start")?;
                let trained = SellScorerTrainer::new().train_sell_scorer(&request)?;
                ensure_not_cancelled(&cancellation, "sell-scorer fit completion")?;
                Ok(trained)
            })
            .await?;
        let fit_status = trained.metrics.fit_status;
        Ok(PreparedModelPayload {
            payload: ModelPayload::SellScorer(Box::new(trained.payload)),
            transform: ModelServingTransformBinding {
                input_contract_hash: trained.input_contract_hash,
                input_transform_hash: trained.input_transform_hash,
                training_input_hash: trained.training_input_hash,
                training_dataset_hash: serving.training_dataset_hash,
            },
            metrics: PendingModelMetrics::SellGoverned(trained.metrics),
            objective: ModelTrainingObjective::governed_sell(fit_status),
        })
    }

    /// Create the `Training` run record (status `Running`).
    ///
    /// The produced version does not exist yet (training is its output), so the
    /// run starts without `model_version_id` (FK to `quant_model_version`).
    /// [`ModelRunRepository::succeed`] backfills the version id after registration;
    /// `output_hash` records the artifact hash for content-addressed linkage.
    async fn create_run(
        &self,
        model_run_id: &ModelRunId,
        dataset: &TrainingDatasetInfo,
    ) -> QuantResult<()> {
        let materialization = require_dataset_materialization(dataset)?;
        self.deps
            .model_run_repo
            .start_exact(NewModelRun {
                model_run_id: *model_run_id,
                run_kind: ModelRunKind::Training,
                model_version_id: None,
                decision_policy_snapshot_id: dataset.decision_policy_snapshot_id,
                market_selection_id: None,
                window_start: dataset.window_start,
                window_end: dataset.window_end,
                input_hash: *materialization.dataset_hash,
            })
            .await?;
        Ok(())
    }
}

const fn objective_component_metrics(
    value: &ObjectiveComponentReport,
) -> ObjectiveComponentMetrics {
    ObjectiveComponentMetrics {
        rank_loss: value.rank_loss,
        tail_penalty: value.tail_penalty,
        turnover_penalty: value.turnover_penalty,
        l2_penalty: value.l2_penalty,
        total_loss: value.total_loss,
        group_count: value.group_count,
        rank_loss_group_count: value.rank_loss_group_count,
        pair_count: value.pair_count,
    }
}

const fn ranking_diagnostics_metrics(value: &RankingDiagnostics) -> RankingDiagnosticsMetrics {
    RankingDiagnosticsMetrics {
        mean_rank_ic: value.mean_rank_ic,
        mean_ndcg_at_k: value.mean_ndcg_at_k,
        ndcg_k: value.ndcg_k,
        group_count: value.group_count,
    }
}

fn validation_metrics(
    value: &ValidationReport,
    held_out_metric: HeldOutMetricKind,
) -> ModelValidationMetrics {
    ModelValidationMetrics {
        held_out_objective: value.held_out_objective,
        held_out_components: value
            .held_out_components
            .as_ref()
            .map(objective_component_metrics),
        held_out_diagnostics: value
            .held_out_diagnostics
            .as_ref()
            .map(ranking_diagnostics_metrics),
        fold_objectives: value.fold_objectives.clone(),
        fold_components: value
            .fold_components
            .iter()
            .map(objective_component_metrics)
            .collect(),
        sample_count: value.sample_count,
        dropped_singleton_groups: value.dropped_singleton_groups,
        dropped_singleton_rows: value.dropped_singleton_rows,
        coordinate_search_effective_trials: value.coord_search_effective_n,
        held_out_metric,
    }
}

fn artifact_training_lineage(
    artifact: &ModelArtifact,
) -> QuantResult<ModelArtifactTrainingLineage> {
    artifact.validate()?;
    let bindings = artifact.header().serving_contract().bindings();
    let transform = &bindings.transform;
    match (&bindings.model.estimator, artifact.payload()) {
        (
            ModelServingEstimatorBinding::FactorNative { ordered_inputs, .. },
            ModelPayload::WeightedFactor(_) | ModelPayload::SellScorer(_),
        ) => {
            let mut factor_inputs = Vec::new();
            for estimator_input in ordered_inputs {
                let ModelServingEstimatorInput::GovernedFactor {
                    factor_definition_id,
                } = estimator_input
                else {
                    continue;
                };
                let factor = bindings
                    .factors
                    .plane
                    .definitions()
                    .iter()
                    .find(|factor| factor.factor_definition_id() == *factor_definition_id)
                    .ok_or_else(|| ResearchError::InvalidModelArtifact {
                        detail: format!(
                            "serving lineage references unknown factor definition {factor_definition_id}"
                        ),
                    })?;
                factor_inputs.push(factor.factor_name().clone());
            }
            Ok(ModelArtifactTrainingLineage::FactorNative {
                training_dataset_hash: transform.training_dataset_hash,
                training_input_hash: transform.training_input_hash,
                input_contract_hash: transform.input_contract_hash,
                input_transform_hash: transform.input_transform_hash,
                factor_inputs,
            })
        }
        (
            ModelServingEstimatorBinding::Classical {
                kind,
                serialized_model_hash,
                serialization_format,
                ..
            },
            ModelPayload::Classical(_),
        ) => Ok(ModelArtifactTrainingLineage::FittedFeatureMatrix {
            model_kind: *kind,
            training_dataset_hash: transform.training_dataset_hash,
            training_input_hash: transform.training_input_hash,
            input_contract_hash: transform.input_contract_hash,
            input_transform_hash: transform.input_transform_hash,
            serialized_model_hash: *serialized_model_hash,
            serialization_format: *serialization_format,
        }),
        _ => Err(ResearchError::InvalidModelArtifact {
            detail: "serving estimator and header-free model payload families diverge".to_owned(),
        }
        .into()),
    }
}

fn learning_to_rank_metrics(
    in_sample: &TrainingObjectiveReport,
    validation: &ValidationReport,
    artifact_lineage: ModelArtifactTrainingLineage,
) -> ModelVersionMetrics {
    ModelVersionMetrics::learning_to_rank(
        LearningToRankInSampleMetrics {
            objective_value: in_sample.objective_value,
            components: objective_component_metrics(&in_sample.components),
            diagnostics: in_sample
                .diagnostics
                .as_ref()
                .map(ranking_diagnostics_metrics),
            summary: in_sample.summary.clone(),
        },
        validation_metrics(
            validation,
            HeldOutMetricKind::NegativeTotalLearningToRankLoss,
        ),
        artifact_lineage,
    )
}

impl PendingModelMetrics {
    fn finalize(self, artifact: &ModelArtifact) -> QuantResult<ModelVersionMetrics> {
        let artifact_lineage = artifact_training_lineage(artifact)?;
        match self {
            Self::LearningToRank(metrics) => Ok(learning_to_rank_metrics(
                &metrics.in_sample,
                &metrics.validation,
                artifact_lineage,
            )),
            Self::SellGoverned(metrics) => Ok(ModelVersionMetrics::governed_sell(
                GovernedSellEstimatorMetrics {
                    resolved_label_rows: metrics.resolved_label_rows,
                    position_state_rows: metrics.position_state_rows,
                    fit_status: metrics.fit_status,
                },
                artifact_lineage,
            )),
            #[cfg(feature = "ml-classical")]
            Self::Classical(metrics) => Ok(ModelVersionMetrics::classical_pointwise(
                metrics.kind,
                metrics.in_sample,
                metrics.validation,
                metrics.feature_importances,
                artifact_lineage,
            )),
        }
    }
}

/// Classical training path — only linked under `ml-classical`.
#[cfg(feature = "ml-classical")]
impl ModelTrainerService {
    async fn train_classical(
        &self,
        input: &TrainModelInput,
        examples: &Arc<[TrainingExample]>,
        serving: &VerifiedServingInputs,
        kind: ClassicalKind,
        cancel: &CancellationToken,
    ) -> QuantResult<PreparedModelPayload> {
        let label = input.label();
        let output_semantics =
            classical_output_semantics(kind, &label, input.prediction_horizon()?)?;
        let validation = ValidationSpec {
            folds: input.model_spec.training_contract.validation_folds,
            embargo_pct: self.config.validation_purge().embargo_pct,
            min_embargo_secs: self.config.validation_purge().min_embargo_secs,
        };
        let examples = Arc::clone(examples);
        let schema = serving.feature_schema.clone();
        let input_contract = input.model_spec.input_contract.clone();
        let cancellation = cancel.clone();
        let (output, validation) = self
            .deps
            .compute
            .run_offline_cancellable(OfflineMemory::try_gib(8)?, cancel, move || {
                ensure_not_cancelled(&cancellation, "classical matrix")?;
                let matrix =
                    build_classical_matrix(examples.iter(), &label, &schema, &input_contract)?;
                ensure_not_cancelled(&cancellation, "classical fit")?;
                let adapter = ClassicalAdapterRegistry::adapter_for(kind);
                let output = adapter.train(&matrix)?;
                ensure_not_cancelled(&cancellation, "classical validation")?;
                let validation =
                    adapter.validate(&matrix, validation, &cancellation_probe(&cancellation))?;
                ensure_not_cancelled(&cancellation, "classical completion")?;
                Ok((output, validation))
            })
            .await?;
        if output.input_contract != input.model_spec.input_contract {
            return Err(ResearchError::Determinism {
                detail: "classical trainer input contract differs from its owning model spec"
                    .to_owned(),
            }
            .into());
        }

        let model_key = ArtifactKey::new(
            ArtifactNamespace::Model,
            CanonicalDigest::raw_hex(&output.model_bytes),
            "bin",
        )?;
        let serialized_model_uri = self
            .deps
            .artifact_store
            .put(model_key, &output.model_bytes)
            .await?;

        let objective = ModelTrainingObjective::classical(kind);
        let in_sample_metrics = ClassicalInSampleMetrics {
            validation_objective: output.metrics.validation_objective,
            train_samples: output.metrics.train_samples,
            feature_count: output.metrics.feature_count,
        };
        let validation_metrics =
            validation_metrics(&validation, HeldOutMetricKind::MeanRollingFoldRankIc);
        let feature_importances = output
            .metrics
            .feature_importances
            .iter()
            .map(|importance| ModelFeatureImportance {
                feature: ModelMetricName::new(importance.feature.as_str()),
                importance: importance.importance,
            })
            .collect();
        Ok(PreparedModelPayload {
            payload: ModelPayload::Classical(Box::new(ClassicalModelPayload {
                kind,
                crate_name: output.crate_name,
                crate_version: output.crate_version,
                output_semantics,
                multipliers: ScoreMultiplierSpec::conservative(),
                substitution_confidence_rules: SubstitutionConfidenceRules::conservative(),
                input_contract: output.input_contract,
                serialized_model_uri,
                serialized_model_hash: output.model_bytes_hash,
                serialization_format: ModelSerializationFormat::Bincode,
                input_transform: output.input_transform,
                tree_shap: output.tree_shap,
                metrics: output.metrics,
            })),
            transform: ModelServingTransformBinding {
                input_contract_hash: output.input_contract_hash,
                input_transform_hash: output.input_transform_hash,
                training_input_hash: output.training_input_hash,
                training_dataset_hash: serving.training_dataset_hash,
            },
            metrics: PendingModelMetrics::Classical(Box::new(PendingClassicalMetrics {
                kind,
                in_sample: in_sample_metrics,
                validation: validation_metrics,
                feature_importances,
            })),
            objective,
        })
    }
}

#[cfg(feature = "ml-classical")]
pub(crate) fn classical_output_semantics(
    kind: ClassicalKind,
    label: &LabelSelector,
    prediction_horizon_secs: u64,
) -> QuantResult<ClassicalOutputSemantics> {
    if prediction_horizon_secs == 0 {
        return Err(ResearchError::DatasetBuild {
            detail: "classical model prediction horizon must be positive".to_owned(),
        }
        .into());
    }
    match kind {
        ClassicalKind::LogisticRegression if label.name == TOKEN_PAYOUT_RATIO => {
            Ok(ClassicalOutputSemantics::FullPayoutProbability)
        }
        ClassicalKind::LogisticRegression => Err(ResearchError::DatasetBuild {
            detail: format!(
                "logistic classical model requires `{TOKEN_PAYOUT_RATIO}` target, got `{}`",
                label.name
            ),
        }
        .into()),
        _ if label.name == RETURN_TO_HORIZON
            && label.horizon_secs == prediction_horizon_secs =>
        {
            Ok(ClassicalOutputSemantics::ForwardReturnBps)
        }
        _ => Err(ResearchError::DatasetBuild {
            detail: format!(
                "classical regressor requires `{RETURN_TO_HORIZON}` at the model prediction horizon {prediction_horizon_secs}s, got `{}` at {}s",
                label.name, label.horizon_secs
            ),
        }
        .into()),
    }
}

/// Build the standardizable classical feature matrix from the dataset examples.
///
/// Examples are time-ordered (so the rolling-validation holdout splits on
/// wall-clock time, never leaking). Columns come from the governed
/// [`FeatureSchema`] — not an ad hoc scan of whichever
/// numeric names happen to appear in this particular example batch — so the
/// classical path respects the same requiredness / `unit` / `value_kind`
/// contract the online governed path enforces (e.g. `Bps`-unit features are
/// correctly scaled, and a contract-required column genuinely gates row
/// admission instead of silently being treated as fillable). This also makes
/// the column set reproducible across runs and comparable to the schema
/// hash, rather than an artifact of which markets happened to be sampled.
/// Shared by [`ModelTrainerService::train_classical`] and the
/// CPCV/trial-grid orchestration (`quant-pivot-core::service::cpcv_backtest`),
/// so every classical fold — production or validation — builds its matrix
/// through the identical governed [`FeatureSchema`] column contract.
#[cfg(feature = "ml-classical")]
pub(crate) fn build_classical_matrix<'a>(
    examples: impl IntoIterator<Item = &'a TrainingExample>,
    label: &LabelSelector,
    schema: &FeatureSchema,
    input_contract: &ModelInputContract,
) -> QuantResult<TrainingMatrix> {
    let mut sorted: Vec<_> = examples.into_iter().collect();
    sorted.sort_by(|a, b| {
        a.decision_at()
            .cmp(&b.decision_at())
            .then_with(|| a.market_id.as_str().cmp(b.market_id.as_str()))
            .then_with(|| a.token_id.as_str().cmp(b.token_id.as_str()))
    });

    let spec = matrix_spec_from_contract(
        schema,
        input_contract,
        label.name.clone(),
        label.horizon_secs,
    )?;
    build_borrowed_matrix(&sorted, &spec)
}

/// Classical training is not linked in this build.
#[cfg(not(feature = "ml-classical"))]
impl ModelTrainerService {
    async fn train_classical(
        &self,
        _input: &TrainModelInput,
        _examples: &Arc<[TrainingExample]>,
        _serving: &VerifiedServingInputs,
        kind: ClassicalKind,
        _cancel: &CancellationToken,
    ) -> QuantResult<PreparedModelPayload> {
        future::ready(Err(ResearchError::RuntimeUnavailable {
            family: kind.to_string(),
            detail: "classical training requires the `ml-classical` build".to_owned(),
        }
        .into()))
        .await
    }
}

#[cfg(test)]
mod tests {
    use quant_pivot_error::{QuantError, research::ResearchError};
    use quant_pivot_models::{
        enums::{common::MarketCategory, domain::DomainFamily},
        runtime_config::FeaturesConfig,
        types::{ModelInputContract, ModelInputSpec, factor::FactorServingPlane},
    };
    use quant_pivot_research::features::{
        FeatureSchema,
        names::{
            book::MID, domain_crypto::DISTANCE_TO_STRIKE, domain_weather::CONTRACT_PROBABILITY,
        },
    };

    use super::{MODEL_RUN_DIAGNOSTIC_MAX_CHARS, ModelTrainerService};

    #[test]
    fn run_diagnostic_is_bounded() {
        let error = QuantError::from(ResearchError::DatasetBuild {
            detail: "界".repeat(MODEL_RUN_DIAGNOSTIC_MAX_CHARS + 64),
        });
        let diagnostic = ModelTrainerService::run_diagnostic(&error);
        assert_eq!(diagnostic.chars().count(), MODEL_RUN_DIAGNOSTIC_MAX_CHARS);
        assert!(diagnostic.ends_with('…'));
        assert!(error.to_string().chars().count() > diagnostic.chars().count());
    }

    #[test]
    fn domain_union_tracks_contract() -> Result<(), QuantError> {
        let schema = FeatureSchema::build(&FeaturesConfig::default())?;
        let plane =
            FactorServingPlane::try_empty().map_err(|error| ResearchError::DatasetBuild {
                detail: format!("empty factor plane failed: {error}"),
            })?;
        let input_contract = ModelInputContract {
            inputs: vec![
                ModelInputSpec::required(DISTANCE_TO_STRIKE.to_string()),
                ModelInputSpec::required(CONTRACT_PROBABILITY.to_string()),
            ],
        };

        let actual =
            ModelTrainerService::required_domain_families(&schema, &plane, &input_contract, None)?;

        assert_eq!(actual, vec![DomainFamily::Crypto, DomainFamily::Weather]);
        let weather = ModelTrainerService::required_domain_families(
            &schema,
            &plane,
            &ModelInputContract::single_required(MID.to_string()),
            Some(MarketCategory::Weather),
        )?;
        assert_eq!(weather, vec![DomainFamily::Weather]);
        Ok(())
    }
}
