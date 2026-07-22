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

use std::{collections::BTreeSet, sync::Arc};

use chrono::Utc;
use quant_pivot_error::{QuantError, QuantResult, research::ResearchError, storage::StorageError};
use quant_pivot_models::{
    domain::quant::{
        JobProgressSink, ModelVersionInfo, NewModelRun, NewModelVersion, TrainingDatasetInfo,
    },
    enums::{
        common::MarketCategory,
        model::{ClassicalKind, ModelFamily},
        quant::{
            ModelRunErrorCode, ModelRunKind, ModelRunStatus, PublicationStatus,
            TrainingDatasetStatus,
        },
    },
    runtime_config::sections::FactorsConfig,
    types::{
        DecisionPolicySnapshotId, ModelInputContract, ModelRunId, ModelSpecId, ModelVersionId,
        ResearchJobProgress, TrainingDatasetId,
        model_lineage::ModelVersionDerivation,
        model_metrics::{
            HeldOutMetricKind, LearningToRankInSampleMetrics, ModelArtifactTrainingLineage,
            ModelValidationMetrics, ModelVersionMetrics, ObjectiveComponentMetrics,
            RankingDiagnosticsMetrics,
        },
        model_training::{ModelTrainingObjective, TrainingObjectiveSpec},
        stable_name::FactorName,
        training::TrainingSampleSource,
    },
};
#[cfg(feature = "ml-classical")]
use quant_pivot_models::{
    enums::quant::ModelSerializationFormat,
    hashing::CanonicalDigest,
    types::{
        ModelArtifactId,
        model_metrics::{ClassicalInSampleMetrics, ModelFeatureImportance},
        stable_name::ModelMetricName,
    },
};
use quant_pivot_repository::traits::{
    ModelRegistryRepository, ModelRunRepository, TrainingDatasetRepository,
};
use quant_pivot_research::{
    artifact::ArtifactStore,
    factors::{
        FactorEngine,
        names::{POSITION_PEAK_DRAWDOWN, POSITION_TIME_IN_TRADE, POSITION_UNREALIZED_PNL},
    },
    features::FeatureSchema,
    hashing::ResearchHasher,
    model::{
        FactorWeight, LabelSelector, ModelArtifact, ModelArtifactHeader, ModelTrainer,
        ReturnModelSpec, ScoreMultiplierSpec, SellScorerOutputSpec, SellScorerTrainer,
        SubstitutionConfidenceRules, TrainModelRequest, TrainSellScorerRequest,
        TrainedModelArtifact, TrainingObjectiveReport, ValidationReport, ValidationSpec,
        WeightedFactorTrainer, infer_training_category_scope,
        objective::{ObjectiveComponentReport, RankingDiagnostics},
    },
    selection::ModelFeatureRequirements,
    training::TrainingExample,
    validation::PurgeConfig,
};
#[cfg(feature = "ml-classical")]
use quant_pivot_research::{
    artifact::{ArtifactKey, ArtifactNamespace},
    model::{ClassicalAdapterRegistry, ClassicalOutputSemantics, artifact::ClassicalModelArtifact},
    training::{
        RETURN_TO_HORIZON, SETTLEMENT_OUTCOME, TrainingMatrix, build_training_matrix_from_refs,
        matrix_spec_from_contract,
    },
};
use rust_decimal::Decimal;
use tokio::{runtime::Handle, task};
use tokio_util::sync::CancellationToken;

use crate::service::{
    historical_replay::ReplayConfig,
    training_dataset::{require_dataset_materialization, verify_frozen_dataset_artifact},
};

/// Derive the candidate factor seed: configured `factor_weights` if present,
/// else a uniform seed over the (sorted, de-duplicated) factors observed in
/// `examples`. Shared by [`ModelTrainerService`] and the CPCV/trial-grid
/// orchestration (`quant-pivot-core::service::cpcv_backtest`) so every
/// `WeightedFactor` fold — production or validation — starts from the same
/// governed seed.
pub(crate) fn weighted_seed_weights(
    factors: &FactorsConfig,
    examples: &[TrainingExample],
) -> Vec<FactorWeight> {
    let configured: Vec<FactorWeight> = factors
        .factor_weights
        .weights
        .iter()
        .map(|(name, value)| FactorWeight {
            factor: FactorName::new(name.clone()),
            weight: value.value,
        })
        .collect();
    if !configured.is_empty() {
        return configured;
    }
    let mut names: BTreeSet<String> = BTreeSet::new();
    for example in examples {
        for factor in &example.factor_values {
            names.insert(factor.name.to_string());
        }
    }
    let count = names.len().max(1);
    let weight = Decimal::ONE / Decimal::from(count as u64);
    names
        .into_iter()
        .map(|name| FactorWeight {
            factor: FactorName::new(name),
            weight,
        })
        .collect()
}

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

/// Repository + store dependencies for the trainer service.
pub struct ModelTrainerServiceDeps {
    /// Frozen training-dataset ledger.
    pub dataset_repo: Arc<dyn TrainingDatasetRepository>,
    /// Content-addressed artifact store (model bytes).
    pub artifact_store: Arc<dyn ArtifactStore>,
    /// Model registry (spec/version lifecycle).
    pub model_registry_repo: Arc<dyn ModelRegistryRepository>,
    /// Model-run ledger.
    pub model_run_repo: Arc<dyn ModelRunRepository>,
}

/// Frozen config governing training (from the runtime-config version).
pub struct ModelTrainerConfig {
    /// Factor config (the `factor_weights` training seed).
    pub factors: FactorsConfig,
    /// Governed objective snapshot from `research.training`.
    pub objective: TrainingObjectiveSpec,
    /// Label-horizon purge/embargo for trainer CV (same knobs as CPCV).
    pub validation_purge: PurgeConfig,
}

/// A training request resolved by the admin port.
pub struct TrainModelInput {
    /// Pre-assigned registry id (async job engine) or minted for direct calls.
    pub model_version_id: ModelVersionId,
    /// Target model spec.
    pub model_spec_id: ModelSpecId,
    /// Frozen dataset to train on.
    pub training_dataset_id: TrainingDatasetId,
    /// Frozen runtime-config version (provenance on the run).
    pub decision_policy_snapshot_id: DecisionPolicySnapshotId,
    /// Family to train.
    pub model_family: ModelFamily,
    /// Exact ordered raw-input contract frozen by the owning model spec.
    pub input_contract: ModelInputContract,
    /// Supervised target label.
    pub label: LabelSelector,
    /// Model-intrinsic prediction horizon (seconds), frozen into the artifact.
    pub prediction_horizon_secs: u64,
    /// Rolling validation folds.
    pub validation_folds: u32,
    /// Categories enabled in the frozen selection policy (for scope inference).
    pub selection_enabled_categories: Vec<MarketCategory>,
    /// Explicit category scope override; when `None`, inferred from spec + examples.
    pub category_scope: Option<MarketCategory>,
}

/// Successful training outcome — version row plus the materialization run id.
pub struct TrainModelOutcome {
    pub version: ModelVersionInfo,
    pub model_run_id: ModelRunId,
}

/// Offline trainer service.
pub struct ModelTrainerService {
    deps: ModelTrainerServiceDeps,
    config: ModelTrainerConfig,
    replay: ReplayConfig,
}

impl ModelTrainerService {
    /// Assemble the service from persistence dependencies and frozen trainer config.
    #[must_use]
    pub const fn new(
        deps: ModelTrainerServiceDeps,
        config: ModelTrainerConfig,
        replay: ReplayConfig,
    ) -> Self {
        Self {
            deps,
            config,
            replay,
        }
    }

    /// Train a model and register it as a Candidate version.
    ///
    /// Reports coarse but honest phases (`load → decode → verify → fit`) to
    /// `progress`; the fit itself is a single opaque research-trainer call,
    /// offloaded to a blocking thread. `cancel` is polled at each stage boundary.
    pub async fn train(
        &self,
        input: TrainModelInput,
        progress: &dyn JobProgressSink,
        cancel: &CancellationToken,
    ) -> QuantResult<TrainModelOutcome> {
        ensure_not_cancelled(cancel, "load")?;
        progress.report(ResearchJobProgress::indeterminate("load", 0));
        let dataset = self.load_ready_dataset(&input.training_dataset_id).await?;
        self.validate_dataset_contracts(&dataset)?;
        progress.report(ResearchJobProgress::indeterminate("decode", 0));
        let examples: Arc<[TrainingExample]> = self.decode_examples(&dataset).await?.into();
        ensure_not_cancelled(cancel, "verify")?;
        progress.report(ResearchJobProgress::indeterminate(
            "verify",
            examples.len() as u64,
        ));

        let model_version_id = input.model_version_id;
        let model_run_id = ModelRunId::from_v7();
        self.create_run(&model_run_id, &input, &dataset).await?;

        ensure_not_cancelled(cancel, "fit")?;
        progress.report(ResearchJobProgress::indeterminate(
            "fit",
            examples.len() as u64,
        ));
        match self
            .train_and_register(&model_version_id, &input, &dataset, &examples)
            .await
        {
            Ok(version) => {
                self.deps
                    .model_run_repo
                    .succeed(
                        &model_run_id,
                        version.artifact_hash,
                        Utc::now(),
                        Some(model_version_id),
                    )
                    .await?;
                Ok(TrainModelOutcome {
                    version,
                    model_run_id,
                })
            }
            Err(error) => {
                let _ = self
                    .deps
                    .model_run_repo
                    .fail(
                        &model_run_id,
                        ModelRunErrorCode::TrainingFailed,
                        error.to_string(),
                        Utc::now(),
                    )
                    .await;
                Err(error)
            }
        }
    }

    fn validate_dataset_contracts(&self, dataset: &TrainingDatasetInfo) -> QuantResult<()> {
        let materialization = require_dataset_materialization(dataset)?;
        let feature_schema = FeatureSchema::build(&self.replay.features)?;
        let feature_schema_hash = ResearchHasher::feature_schema(&feature_schema)?;
        if &feature_schema_hash != materialization.feature_schema_hash {
            return Err(ResearchError::DatasetBuild {
                detail: format!(
                    "frozen dataset feature contract mismatch: dataset {}, runtime {}",
                    materialization.feature_schema_hash, feature_schema_hash
                ),
            }
            .into());
        }
        let factor_schema_hash = FactorEngine::new(
            &self.replay.factors,
            &self.replay.features,
            &self.replay.domain,
            None,
        )
        .factor_schema_hash()?;
        if &factor_schema_hash != materialization.factor_schema_hash {
            return Err(ResearchError::DatasetBuild {
                detail: format!(
                    "frozen dataset factor contract mismatch: dataset {}, runtime {}",
                    materialization.factor_schema_hash, factor_schema_hash
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

    /// Train + register, dispatching on family.
    async fn train_and_register(
        &self,
        model_version_id: &ModelVersionId,
        input: &TrainModelInput,
        dataset: &TrainingDatasetInfo,
        examples: &Arc<[TrainingExample]>,
    ) -> QuantResult<ModelVersionInfo> {
        let materialization = require_dataset_materialization(dataset)?;
        let manifest =
            dataset
                .manifest_json
                .as_ref()
                .ok_or_else(|| ResearchError::DatasetBuild {
                    detail: "v1 dataset is missing its immutable manifest".to_owned(),
                })?;
        let header = ModelArtifactHeader {
            model_version_id: *model_version_id,
            model_spec_definition_hash: manifest.model_spec_definition_hash,
            profile_ref: manifest.profile_ref.clone(),
            model_family: input.model_family,
            feature_schema_hash: *materialization.feature_schema_hash,
            factor_schema_hash: *materialization.factor_schema_hash,
            trade_policy_artifact_id: manifest.trade_policy_artifact_id,
            trade_policy_hash: manifest.trade_policy_hash,
        };

        let (artifact, metrics, training_objective) = if input.model_family.is_exit_scorer() {
            self.train_sell_scorer(header, input, dataset, examples)
                .await?
        } else {
            match input.model_family.classical_kind() {
                None => {
                    self.train_weighted(header, input, dataset, examples)
                        .await?
                }
                Some(kind) => {
                    self.train_classical(header, input, dataset, examples, kind)
                        .await?
                }
            }
        };

        let category_scope = artifact.category_scope();
        let trade_policy_artifact_id = artifact.header().trade_policy_artifact_id;
        let trade_policy_hash = artifact.header().trade_policy_hash;
        let artifact_hash = artifact.content_hash()?;
        let key = ModelArtifact::artifact_key(&artifact_hash)?;
        self.deps
            .artifact_store
            .put(key, &artifact.to_bytes()?)
            .await?;

        let version = self
            .deps
            .model_registry_repo
            .next_version_for_spec(&input.model_spec_id)
            .await?;
        let registered = self
            .deps
            .model_registry_repo
            .create_model_version(NewModelVersion {
                model_version_id: *model_version_id,
                model_spec_id: input.model_spec_id,
                version,
                artifact_hash,
                category_scope,
                profile_ref: manifest.profile_ref.clone(),
                training_dataset_id: Some(input.training_dataset_id),
                trade_policy_artifact_id,
                trade_policy_hash,
                publish_path_set_id: None,
                derivation: ModelVersionDerivation::Training,
                metrics,
                training_objective,
                quality_gate_report: None,
                publication_status: PublicationStatus::Candidate,
                published_at: None,
                retired_at: None,
            })
            .await?;
        Ok(registered)
    }

    /// Weighted-factor training path (always linked).
    async fn train_weighted(
        &self,
        header: ModelArtifactHeader,
        input: &TrainModelInput,
        dataset: &TrainingDatasetInfo,
        examples: &Arc<[TrainingExample]>,
    ) -> QuantResult<(ModelArtifact, ModelVersionMetrics, ModelTrainingObjective)> {
        let materialization = require_dataset_materialization(dataset)?;
        let requirements = ModelFeatureRequirements::from_input_contract(&input.input_contract);
        let category_scope = input.category_scope.or_else(|| {
            infer_training_category_scope(
                examples,
                &requirements,
                &input.selection_enabled_categories,
            )
        });
        let seed_weights = weighted_seed_weights(&self.config.factors, examples);
        let request = TrainModelRequest {
            examples: Arc::clone(examples),
            training_dataset_hash: *materialization.dataset_hash,
            label: input.label.clone(),
            seed_weights,
            objective: self.config.objective.clone(),
            validation: ValidationSpec {
                folds: input.validation_folds.max(2),
                embargo_pct: self.config.validation_purge.embargo_pct,
                min_embargo_secs: self.config.validation_purge.min_embargo_secs,
            },
            header,
            prediction_horizon_secs: input.prediction_horizon_secs,
            multipliers: ScoreMultiplierSpec::conservative(),
            substitution_rules: SubstitutionConfidenceRules::conservative(),
            return_model: ReturnModelSpec::heuristic_default(),
            input_contract: input.input_contract.clone(),
            factor_cross_section: self.config.factors.cross_section.clone(),
            category_scope,
        };
        // Offload the CPU-bound fit to a blocking thread so it never occupies an
        // async runtime worker (starving other jobs' heartbeats).
        let trained: TrainedModelArtifact = task::spawn_blocking(move || {
            Handle::current().block_on(WeightedFactorTrainer::new().train(request))
        })
        .await
        .map_err(|error| {
            QuantError::from(ResearchError::DatasetBuild {
                detail: format!("weighted-factor trainer task join failed: {error}"),
            })
        })??;
        let objective = ModelTrainingObjective::learning_to_rank(self.config.objective.clone());
        let metrics = learning_to_rank_metrics(
            &trained.in_sample_metrics,
            &trained.validation_metrics,
            &trained.artifact,
        )?;
        Ok((trained.artifact, metrics, objective))
    }

    /// Sell-side hold-vs-exit training path. Seeds over the market
    /// factors plus the position-state pseudo-factors and fits the shared LTR
    /// simplex against the `hold_vs_exit_alpha_bps` label.
    async fn train_sell_scorer(
        &self,
        header: ModelArtifactHeader,
        input: &TrainModelInput,
        dataset: &TrainingDatasetInfo,
        examples: &Arc<[TrainingExample]>,
    ) -> QuantResult<(ModelArtifact, ModelVersionMetrics, ModelTrainingObjective)> {
        let materialization = require_dataset_materialization(dataset)?;
        if !examples
            .iter()
            .all(|example| example.sample_source == TrainingSampleSource::ExitDecision)
        {
            return Err(ResearchError::DatasetBuild {
                detail: "HoldVsExitWeighted training requires ExitDecision-only samples".to_owned(),
            }
            .into());
        }
        let seed_weights = Self::seed_weights(examples);
        let request = TrainSellScorerRequest {
            examples: Arc::clone(examples),
            label: input.label.clone(),
            seed_weights,
            objective: self.config.objective.clone(),
            validation: ValidationSpec {
                folds: input.validation_folds.max(2),
                embargo_pct: self.config.validation_purge.embargo_pct,
                min_embargo_secs: self.config.validation_purge.min_embargo_secs,
            },
            header,
            prediction_horizon_secs: input.prediction_horizon_secs,
            output_spec: SellScorerOutputSpec::conservative(),
            label_schema_hash: *materialization.label_schema_hash,
            training_dataset_hash: *materialization.dataset_hash,
            input_contract: input.input_contract.clone(),
        };
        // Offload the CPU-bound fit to a blocking thread (keeps the runtime free).
        let trained =
            task::spawn_blocking(move || SellScorerTrainer::new().train_sell_scorer(&request))
                .await
                .map_err(|error| {
                    QuantError::from(ResearchError::DatasetBuild {
                        detail: format!("sell-scorer trainer task join failed: {error}"),
                    })
                })??;
        let objective = ModelTrainingObjective::learning_to_rank(self.config.objective.clone());
        let metrics = learning_to_rank_metrics(
            &trained.in_sample_metrics,
            &trained.validation_metrics,
            &trained.artifact,
        )?;
        Ok((trained.artifact, metrics, objective))
    }

    /// Seed the Sell scorer over the observed market factors plus the three
    /// position-state pseudo-factors (uniform), so the trainer can weigh the
    /// lot's own state alongside market factors.
    fn seed_weights(examples: &[TrainingExample]) -> Vec<FactorWeight> {
        let mut names: BTreeSet<String> = BTreeSet::new();
        for example in examples {
            for factor in &example.factor_values {
                names.insert(factor.name.to_string());
            }
        }
        for pseudo in [
            POSITION_UNREALIZED_PNL,
            POSITION_TIME_IN_TRADE,
            POSITION_PEAK_DRAWDOWN,
        ] {
            names.insert(pseudo.to_string());
        }
        let count = names.len().max(1);
        let weight = Decimal::ONE / Decimal::from(count as u64);
        names
            .into_iter()
            .map(|name| FactorWeight {
                factor: FactorName::new(name),
                weight,
            })
            .collect()
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
        input: &TrainModelInput,
        dataset: &TrainingDatasetInfo,
    ) -> QuantResult<()> {
        let materialization = require_dataset_materialization(dataset)?;
        self.deps
            .model_run_repo
            .create(NewModelRun {
                model_run_id: *model_run_id,
                run_kind: ModelRunKind::Training,
                model_version_id: None,
                decision_policy_snapshot_id: input.decision_policy_snapshot_id,
                market_selection_id: None,
                window_start: dataset.window_start,
                window_end: dataset.window_end,
                status: ModelRunStatus::Running,
                input_hash: *materialization.dataset_hash,
                output_hash: None,
                error_code: None,
                error_message: None,
                started_at: Utc::now(),
                finished_at: None,
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
    match artifact {
        ModelArtifact::WeightedFactor(weighted) => Ok(ModelArtifactTrainingLineage::FactorNative {
            training_dataset_hash: weighted.training_dataset_hash,
            training_input_hash: weighted.training_input_hash,
            input_contract_hash: weighted.input_contract_hash,
            input_transform_hash: weighted.input_transform_hash()?,
            factor_inputs: weighted
                .weights
                .iter()
                .map(|weight| weight.factor.clone())
                .collect(),
        }),
        ModelArtifact::Classical(classical) => {
            Ok(ModelArtifactTrainingLineage::FittedFeatureMatrix {
                model_kind: classical.kind,
                training_dataset_hash: classical.training_dataset_hash,
                training_input_hash: classical.training_input_hash,
                input_contract_hash: classical.input_contract_hash,
                input_transform_hash: classical.input_transform_hash,
                serialized_model_hash: classical.serialized_model_hash,
                serialization_format: classical.serialization_format,
            })
        }
        ModelArtifact::SellScorer(sell) => Ok(ModelArtifactTrainingLineage::FactorNative {
            training_dataset_hash: sell.training_dataset_hash,
            training_input_hash: sell.training_input_hash,
            input_contract_hash: sell.input_contract_hash,
            input_transform_hash: sell.input_transform_hash()?,
            factor_inputs: sell
                .weights
                .iter()
                .map(|weight| weight.factor.clone())
                .collect(),
        }),
    }
}

fn learning_to_rank_metrics(
    in_sample: &TrainingObjectiveReport,
    validation: &ValidationReport,
    artifact: &ModelArtifact,
) -> QuantResult<ModelVersionMetrics> {
    Ok(ModelVersionMetrics::learning_to_rank(
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
        artifact_training_lineage(artifact)?,
    ))
}

/// Classical training path — only linked under `ml-classical`.
#[cfg(feature = "ml-classical")]
impl ModelTrainerService {
    async fn train_classical(
        &self,
        header: ModelArtifactHeader,
        input: &TrainModelInput,
        dataset: &TrainingDatasetInfo,
        examples: &Arc<[TrainingExample]>,
        kind: ClassicalKind,
    ) -> QuantResult<(ModelArtifact, ModelVersionMetrics, ModelTrainingObjective)> {
        let materialization = require_dataset_materialization(dataset)?;
        let output_semantics =
            classical_output_semantics(kind, &input.label, input.prediction_horizon_secs)?;
        let schema = FeatureSchema::build(&self.replay.features)?;
        let schema_hash = ResearchHasher::feature_schema(&schema)?;
        if &schema_hash != materialization.feature_schema_hash {
            return Err(ResearchError::DatasetBuild {
                detail: format!(
                    "classical frozen dataset feature schema mismatch: dataset {}, runtime {}",
                    materialization.feature_schema_hash, schema_hash
                ),
            }
            .into());
        }
        let matrix = build_classical_matrix(
            examples.iter(),
            &input.label,
            &schema,
            &input.input_contract,
        )?;
        let folds = input.validation_folds.max(2);
        let validation = ValidationSpec {
            folds,
            embargo_pct: self.config.validation_purge.embargo_pct,
            min_embargo_secs: self.config.validation_purge.min_embargo_secs,
        };
        // Offload the CPU-bound classical fit + rolling validation to a blocking
        // thread (keeps the async runtime free for other jobs' heartbeats).
        let (output, validation) = task::spawn_blocking(move || {
            let adapter = ClassicalAdapterRegistry::adapter_for(kind);
            let output = adapter.train(&matrix)?;
            let validation = adapter.validate(&matrix, validation)?;
            QuantResult::Ok((output, validation))
        })
        .await
        .map_err(|error| {
            QuantError::from(ResearchError::DatasetBuild {
                detail: format!("classical trainer task join failed: {error}"),
            })
        })??;
        if output.input_contract != input.input_contract {
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
        let artifact = ModelArtifact::Classical(Box::new(ClassicalModelArtifact {
            header,
            artifact_id: ModelArtifactId::from_v7(),
            kind,
            crate_name: output.crate_name.clone(),
            crate_version: output.crate_version.clone(),
            label_schema_hash: *materialization.label_schema_hash,
            training_dataset_hash: *materialization.dataset_hash,
            prediction_horizon_secs: input.prediction_horizon_secs,
            output_semantics,
            multipliers: ScoreMultiplierSpec::conservative(),
            substitution_confidence_rules: SubstitutionConfidenceRules::conservative(),
            input_contract: output.input_contract,
            input_contract_hash: output.input_contract_hash,
            input_transform_hash: output.input_transform_hash,
            training_input_hash: output.training_input_hash,
            serialized_model_uri,
            serialized_model_hash: output.model_bytes_hash,
            serialization_format: ModelSerializationFormat::Bincode,
            input_transform: output.input_transform,
            metrics: output.metrics,
        }));
        let metrics = ModelVersionMetrics::classical_pointwise(
            kind,
            in_sample_metrics,
            validation_metrics,
            feature_importances,
            artifact_training_lineage(&artifact)?,
        );
        Ok((artifact, metrics, objective))
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
        ClassicalKind::LogisticRegression if label.name == SETTLEMENT_OUTCOME => {
            Ok(ClassicalOutputSemantics::SettlementProbability)
        }
        ClassicalKind::LogisticRegression => Err(ResearchError::DatasetBuild {
            detail: format!(
                "logistic classical model requires `{SETTLEMENT_OUTCOME}` target, got `{}`",
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
    build_training_matrix_from_refs(&sorted, &spec)
}

/// Classical training is not linked in this build.
#[cfg(not(feature = "ml-classical"))]
impl ModelTrainerService {
    #[allow(clippy::unused_async)]
    async fn train_classical(
        &self,
        _header: ModelArtifactHeader,
        _input: &TrainModelInput,
        _dataset: &TrainingDatasetInfo,
        _examples: &Arc<[TrainingExample]>,
        kind: ClassicalKind,
    ) -> QuantResult<(ModelArtifact, ModelVersionMetrics, ModelTrainingObjective)> {
        Err(ResearchError::RuntimeUnavailable {
            family: kind.to_string(),
            detail: "classical training requires the `ml-classical` build".to_owned(),
        }
        .into())
    }
}
