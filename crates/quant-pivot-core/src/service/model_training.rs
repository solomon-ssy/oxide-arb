//! Offline model-training orchestration (Phase 3.6).
//!
//! Loads a frozen training dataset's Parquet, decodes its **schedule and label
//! truth**, rematerializes every example's features and factors point-in-time
//! through the shared [`materialize_cross_section`] kernel (same path as the
//! backtest replay), trains with the pure research trainer, content-addresses
//! the artifact into the [`ArtifactStore`], and registers a **Candidate**
//! `quant_model_version` plus a `Training` `quant_model_run`. Parquet is never
//! trusted for features or factors. The weighted-factor path is always available;
//! the classical (smartcore) path is linked only under the `ml-classical`
//! feature and otherwise fails closed with `RuntimeUnavailable`.

use std::{collections::BTreeSet, sync::Arc, time::Duration};

use chrono::Utc;
use tokio::{runtime::Handle, task};
use tokio_util::sync::CancellationToken;

use quant_pivot_error::{QuantError, QuantResult, research::ResearchError, storage::StorageError};
use quant_pivot_models::{
    domain::{
        JobProgressSink, ModelVersionInfo, NewModelRun, NewModelVersion, ResearchJobProgress,
        TrainingDatasetInfo,
    },
    enums::{
        common::MarketCategory,
        quant::{
            ModelRunErrorCode, ModelRunKind, ModelRunStatus, PublicationStatus,
            TrainingDatasetStatus,
        },
    },
    runtime_config::sections::FactorsConfig,
    types::{
        ModelRunId, ModelSpecId, ModelVersionId, RuntimeConfigVersionId, TrainingDatasetId,
        training::TrainingSampleSource,
    },
};
#[cfg(feature = "ml-classical")]
use quant_pivot_models::{
    enums::quant::ModelSerializationFormat, hashing::CanonicalDigest, types::ModelArtifactId,
};
use quant_pivot_repository::traits::{
    EventRepository, MarketLinkageRepository, MarketRepository, ModelRegistryRepository,
    ModelRunRepository, QuantFactReadRepository, TrainingDatasetRepository,
};
use quant_pivot_research::{
    artifact::ArtifactStore,
    factors::{FactorName, names as factor_names},
    model::{
        ClassicalKind, FactorWeight, LabelSelector, ModelArtifact, ModelArtifactHeader,
        ModelFamily, ModelTrainer, ReturnModelSpec, ScoreMultiplierSpec, SellScorerOutputSpec,
        SellScorerTrainer, SubstitutionConfidenceRules, TrainModelRequest, TrainSellScorerRequest,
        TrainedModelArtifact, TrainingObjectiveSpec, ValidationSpec, WeightedFactorTrainer,
        infer_training_category_scope,
    },
    selection::ModelFeatureRequirements,
    training::{DatasetParquetCodec, TrainingExample},
    validation::PurgeConfig,
};
#[cfg(feature = "ml-classical")]
use quant_pivot_research::{
    artifact::{ArtifactKey, ArtifactNamespace},
    features::FeatureSchema,
    model::{
        ClassicalAdapterRegistry, ClassicalTrainOutput, ValidationReport,
        artifact::ClassicalModelArtifact,
    },
    training::{TrainingMatrix, build_training_matrix, matrix_spec_from_schema},
};
use rust_decimal::Decimal;

use crate::service::{
    dataset_replay::{
        RematerializeInputs, rematerialize_exit_decision_examples, rematerialize_training_examples,
    },
    historical_replay::ReplayConfig,
};

/// Derive the candidate factor seed: configured `factor_weights` if present,
/// else a uniform seed over the (sorted, de-duplicated) factors observed in
/// `examples`. Shared by [`ModelTrainerService`] and Phase 11.5's CPCV/trial-grid
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
        .filter_map(|(name, value)| {
            value
                .value
                .parse::<Decimal>()
                .ok()
                .map(|weight| FactorWeight {
                    factor: FactorName::new(name.clone()),
                    weight,
                })
        })
        .collect();
    if !configured.is_empty() {
        return configured;
    }
    let mut names: BTreeSet<String> = BTreeSet::new();
    for example in examples {
        for factor in &example.factor_values {
            names.insert(factor.name.as_str().to_owned());
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
/// checked at each coarse phase boundary.
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
    /// `ClickHouse` fact reader for point-in-time rematerialization.
    pub fact_read: Arc<dyn QuantFactReadRepository>,
    /// Postgres market catalog for the replay window.
    pub market_repo: Arc<dyn MarketRepository>,
    /// Postgres event catalog snapshot for neg-risk leg enumeration.
    pub event_repo: Arc<dyn EventRepository>,
    /// Frozen market → external-subject linkage ledger (11.2.2).
    pub linkage_repo: Arc<dyn MarketLinkageRepository>,
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
    pub runtime_config_version_id: RuntimeConfigVersionId,
    /// Family to train.
    pub model_family: ModelFamily,
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
    max_book_staleness: Duration,
}

impl ModelTrainerService {
    /// Assemble the service from deps, frozen config, and replay parameters.
    #[must_use]
    pub const fn new(
        deps: ModelTrainerServiceDeps,
        config: ModelTrainerConfig,
        replay: ReplayConfig,
        max_book_staleness: Duration,
    ) -> Self {
        Self {
            deps,
            config,
            replay,
            max_book_staleness,
        }
    }

    /// Train a model and register it as a Candidate version.
    ///
    /// Reports coarse but honest phases (`load → decode → materialize → fit`) to
    /// `progress`; the fit itself is a single opaque research-trainer call,
    /// offloaded to a blocking thread. `cancel` is polled at each phase boundary.
    pub async fn train(
        &self,
        input: TrainModelInput,
        progress: &dyn JobProgressSink,
        cancel: &CancellationToken,
    ) -> QuantResult<TrainModelOutcome> {
        ensure_not_cancelled(cancel, "load")?;
        progress.report(ResearchJobProgress::indeterminate("load", 0));
        let dataset = self.load_ready_dataset(&input.training_dataset_id).await?;
        progress.report(ResearchJobProgress::indeterminate("decode", 0));
        let parquet_examples = self.decode_examples(&dataset).await?;
        // Rematerialize features/factors point-in-time (the dominant pre-fit cost).
        ensure_not_cancelled(cancel, "materialize")?;
        progress.report(ResearchJobProgress::indeterminate(
            "materialize",
            parquet_examples.len() as u64,
        ));
        // Exit scorers train on per-lot `ExitDecision` rows: recompute market
        // factors PIT but preserve each lot's frozen labels + position-state
        // (the generic rematerialize would rewrite them to `HistoricalPit` and
        // drop the lot state, tripping the Sell-training guard).
        let rematerialize = RematerializeInputs {
            dataset: &dataset,
            parquet_examples: &parquet_examples,
            fact_read: Arc::clone(&self.deps.fact_read),
            market_repo: Arc::clone(&self.deps.market_repo),
            event_repo: Arc::clone(&self.deps.event_repo),
            linkage_repo: Arc::clone(&self.deps.linkage_repo),
            replay: &self.replay,
            max_book_staleness: self.max_book_staleness,
        };
        let examples = if input.model_family.is_exit_scorer() {
            rematerialize_exit_decision_examples(&rematerialize).await?
        } else {
            rematerialize_training_examples(&rematerialize).await?
        };

        let model_version_id = input.model_version_id.clone();
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
                        version.artifact_hash.clone(),
                        version.metrics_json.clone(),
                        Utc::now(),
                        Some(model_version_id.clone()),
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

    /// Load the dataset, rejecting any status outside `{Built, Ready}` (§6.1).
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
        if !matches!(
            dataset.status,
            TrainingDatasetStatus::Built | TrainingDatasetStatus::Ready
        ) {
            return Err(ResearchError::DatasetBuild {
                detail: format!(
                    "training requires a Built/Ready dataset, got {}",
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
        let bytes = self.deps.artifact_store.get(&dataset.parquet_uri).await?;
        DatasetParquetCodec::decode(&bytes)
    }

    /// Train + register, dispatching on family.
    async fn train_and_register(
        &self,
        model_version_id: &ModelVersionId,
        input: &TrainModelInput,
        dataset: &TrainingDatasetInfo,
        examples: &[TrainingExample],
    ) -> QuantResult<ModelVersionInfo> {
        let header = ModelArtifactHeader {
            model_version_id: model_version_id.clone(),
            model_family: input.model_family,
            feature_schema_hash: dataset.feature_schema_hash.clone(),
            factor_schema_hash: dataset.factor_schema_hash.clone(),
        };

        let (artifact, metrics_json, training_objective_json) =
            if input.model_family.is_exit_scorer() {
                self.train_sell_scorer(header, input, dataset, examples)
                    .await?
            } else {
                match input.model_family.classical_kind() {
                    None => self.train_weighted(header, input, examples).await?,
                    Some(kind) => {
                        self.train_classical(header, input, dataset, examples, kind)
                            .await?
                    }
                }
            };

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
                model_version_id: model_version_id.clone(),
                model_spec_id: input.model_spec_id.clone(),
                version,
                artifact_hash,
                training_dataset_id: Some(input.training_dataset_id.clone()),
                metrics_json,
                training_objective_json,
                quality_gate_report: serde_json::json!({}),
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
        examples: &[TrainingExample],
    ) -> QuantResult<(ModelArtifact, serde_json::Value, serde_json::Value)> {
        let spec = self
            .deps
            .model_registry_repo
            .find_model_spec_by_id(&input.model_spec_id)
            .await
            .map_err(QuantError::from)?
            .ok_or_else(|| {
                QuantError::from(ResearchError::InvalidModelArtifact {
                    detail: format!("model spec {} not found for training", input.model_spec_id),
                })
            })?;
        let requirements: ModelFeatureRequirements =
            serde_json::from_value(spec.feature_requirements).map_err(|error| {
                QuantError::config(format!(
                    "model spec {} has invalid feature_requirements: {error}",
                    input.model_spec_id
                ))
            })?;
        let category_scope = input.category_scope.or_else(|| {
            infer_training_category_scope(
                examples,
                &requirements,
                &input.selection_enabled_categories,
            )
        });
        let required_features = match category_scope {
            Some(category) => requirements.for_category(category),
            None => requirements.generic.clone(),
        };
        let seed_weights = weighted_seed_weights(&self.config.factors, examples);
        let request = TrainModelRequest {
            examples: examples.to_vec(),
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
            required_features,
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
        let objective_json = serde_json::to_value(&self.config.objective).map_err(|error| {
            QuantError::from(ResearchError::Serialization {
                detail: format!("training objective serialization failed: {error}"),
            })
        })?;
        let metrics_json = serde_json::json!({
            "objective": objective_json,
            "in_sample": {
                "objective_value": trained.in_sample_metrics.objective_value,
                "components": trained.in_sample_metrics.components,
                "diagnostics": trained.in_sample_metrics.diagnostics,
                "summary": trained.in_sample_metrics.summary,
            },
            "validation": {
                "held_out_objective": trained.validation_metrics.held_out_objective,
                "held_out_components": trained.validation_metrics.held_out_components,
                "held_out_diagnostics": trained.validation_metrics.held_out_diagnostics,
                "fold_objectives": trained.validation_metrics.fold_objectives,
                "fold_components": trained.validation_metrics.fold_components,
                "sample_count": trained.validation_metrics.sample_count,
                "dropped_singleton_groups": trained.validation_metrics.dropped_singleton_groups,
                "dropped_singleton_rows": trained.validation_metrics.dropped_singleton_rows,
                "coord_search_effective_n": trained.validation_metrics.coord_search_effective_n,
                "held_out_metric": "neg_total_ltr_loss",
            },
        });
        Ok((trained.artifact, metrics_json, objective_json))
    }

    /// Sell-side hold-vs-exit training path (Phase 06.1). Seeds over the market
    /// factors plus the position-state pseudo-factors and fits the shared LTR
    /// simplex against the `hold_vs_exit_alpha_bps` label.
    async fn train_sell_scorer(
        &self,
        header: ModelArtifactHeader,
        input: &TrainModelInput,
        dataset: &TrainingDatasetInfo,
        examples: &[TrainingExample],
    ) -> QuantResult<(ModelArtifact, serde_json::Value, serde_json::Value)> {
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
            examples: examples.to_vec(),
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
            label_schema_hash: dataset.label_schema_hash.clone(),
            required_features: Vec::new(),
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
        let objective_json = serde_json::to_value(&self.config.objective).map_err(|error| {
            QuantError::from(ResearchError::Serialization {
                detail: format!("training objective serialization failed: {error}"),
            })
        })?;
        let metrics_json = serde_json::json!({
            "objective": objective_json,
            "in_sample": {
                "objective_value": trained.in_sample_metrics.objective_value,
                "components": trained.in_sample_metrics.components,
                "diagnostics": trained.in_sample_metrics.diagnostics,
                "summary": trained.in_sample_metrics.summary,
            },
            "validation": {
                "held_out_objective": trained.validation_metrics.held_out_objective,
                "held_out_components": trained.validation_metrics.held_out_components,
                "held_out_diagnostics": trained.validation_metrics.held_out_diagnostics,
                "fold_objectives": trained.validation_metrics.fold_objectives,
                "fold_components": trained.validation_metrics.fold_components,
                "sample_count": trained.validation_metrics.sample_count,
                "dropped_singleton_groups": trained.validation_metrics.dropped_singleton_groups,
                "dropped_singleton_rows": trained.validation_metrics.dropped_singleton_rows,
                "held_out_metric": "neg_total_ltr_loss",
            },
        });
        Ok((trained.artifact, metrics_json, objective_json))
    }

    /// Seed the Sell scorer over the observed market factors plus the three
    /// position-state pseudo-factors (uniform), so the trainer can weigh the
    /// lot's own state alongside market factors.
    fn seed_weights(examples: &[TrainingExample]) -> Vec<FactorWeight> {
        let mut names: BTreeSet<String> = BTreeSet::new();
        for example in examples {
            for factor in &example.factor_values {
                names.insert(factor.name.as_str().to_owned());
            }
        }
        for pseudo in [
            factor_names::POSITION_UNREALIZED_PNL,
            factor_names::POSITION_TIME_IN_TRADE,
            factor_names::POSITION_PEAK_DRAWDOWN,
        ] {
            names.insert(pseudo.as_str().to_owned());
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
        self.deps
            .model_run_repo
            .create(NewModelRun {
                model_run_id: model_run_id.clone(),
                run_kind: ModelRunKind::Training,
                model_version_id: None,
                runtime_config_version_id: input.runtime_config_version_id.clone(),
                market_selection_id: None,
                window_start: dataset.window_start,
                window_end: dataset.window_end,
                status: ModelRunStatus::Running,
                input_hash: dataset.dataset_hash.clone(),
                output_hash: None,
                metrics_json: serde_json::json!({}),
                error_code: None,
                error_message: None,
                started_at: Utc::now(),
                finished_at: None,
            })
            .await?;
        Ok(())
    }
}

/// Classical training path — only linked under `ml-classical`.
#[cfg(feature = "ml-classical")]
impl ModelTrainerService {
    async fn train_classical(
        &self,
        header: ModelArtifactHeader,
        input: &TrainModelInput,
        dataset: &TrainingDatasetInfo,
        examples: &[TrainingExample],
        kind: ClassicalKind,
    ) -> QuantResult<(ModelArtifact, serde_json::Value, serde_json::Value)> {
        let schema = FeatureSchema::build(&self.replay.features);
        let matrix = build_classical_matrix(examples, &input.label, &schema)?;
        let folds = input.validation_folds.max(2);
        // Offload the CPU-bound classical fit + rolling validation to a blocking
        // thread (keeps the async runtime free for other jobs' heartbeats).
        let (output, validation) = task::spawn_blocking(move || {
            let adapter = ClassicalAdapterRegistry::adapter_for(kind);
            let output = adapter.train(&matrix)?;
            let validation = adapter.validate(&matrix, folds)?;
            QuantResult::Ok((output, validation))
        })
        .await
        .map_err(|error| {
            QuantError::from(ResearchError::DatasetBuild {
                detail: format!("classical trainer task join failed: {error}"),
            })
        })??;

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

        let objective_json = classical_objective_json(kind);
        let metrics_json = classical_metrics_json(kind, &output, &validation, &objective_json);
        let artifact = ModelArtifact::Classical(Box::new(ClassicalModelArtifact {
            header,
            artifact_id: ModelArtifactId::from_v7(),
            kind,
            crate_name: output.crate_name.clone(),
            crate_version: output.crate_version.clone(),
            label_schema_hash: dataset.label_schema_hash.clone(),
            training_dataset_hash: dataset.dataset_hash.clone(),
            serialized_model_uri,
            serialization_format: ModelSerializationFormat::Bincode,
            preprocessing: output.preprocessing,
            metrics: output.metrics,
        }));
        Ok((artifact, metrics_json, objective_json))
    }
}

/// Build the standardizable classical feature matrix from the dataset examples.
///
/// Examples are time-ordered (so the rolling-validation holdout splits on
/// wall-clock time, never leaking). Columns come from the governed
/// [`FeatureSchema`] (11.2.2 remediation R2) — not an ad hoc scan of whichever
/// numeric names happen to appear in this particular example batch — so the
/// classical path respects the same `critical` / `unit` / `value_kind`
/// contract the online governed path enforces (e.g. `Bps`-unit features are
/// correctly scaled, and a schema-`critical` column genuinely gates row
/// admission instead of silently being treated as fillable). This also makes
/// the column set reproducible across runs and comparable to the schema
/// hash, rather than an artifact of which markets happened to be sampled.
/// Shared by [`ModelTrainerService::train_classical`] and Phase 11.5's
/// CPCV/trial-grid orchestration (`quant-pivot-core::service::cpcv_backtest`),
/// so every classical fold — production or validation — builds its matrix
/// through the identical governed [`FeatureSchema`] column contract.
#[cfg(feature = "ml-classical")]
pub(crate) fn build_classical_matrix(
    examples: &[TrainingExample],
    label: &LabelSelector,
    schema: &FeatureSchema,
) -> QuantResult<TrainingMatrix> {
    let mut sorted: Vec<_> = examples.to_vec();
    sorted.sort_by(|a, b| {
        a.as_of
            .cmp(&b.as_of)
            .then_with(|| a.market_id.as_str().cmp(b.market_id.as_str()))
            .then_with(|| a.token_id.as_str().cmp(b.token_id.as_str()))
    });

    let spec = matrix_spec_from_schema(schema, label.name.clone(), label.horizon_secs);
    build_training_matrix(&sorted, &spec)
}

/// Assemble the classical run's `metrics_json`: in-sample fit, the out-of-sample
/// rolling validation, the kind, and the global feature importances.
#[cfg(feature = "ml-classical")]
fn classical_metrics_json(
    kind: ClassicalKind,
    output: &ClassicalTrainOutput,
    validation: &ValidationReport,
    objective_json: &serde_json::Value,
) -> serde_json::Value {
    serde_json::json!({
        "kind": kind.as_str(),
        "objective": objective_json,
        "in_sample": {
            "validation_objective": output.metrics.validation_objective,
            "train_samples": output.metrics.train_samples,
            "feature_count": output.metrics.feature_count,
        },
        "validation": {
            "held_out_objective": validation.held_out_objective,
            "fold_objectives": validation.fold_objectives,
            "sample_count": validation.sample_count,
            "dropped_singleton_groups": validation.dropped_singleton_groups,
            "dropped_singleton_rows": validation.dropped_singleton_rows,
            "held_out_metric": "mean_rolling_fold_rank_ic",
        },
        "feature_importances": output.metrics.feature_importances
            .iter()
            .map(|fi| serde_json::json!({
                "feature": fi.feature.as_str(),
                "importance": fi.importance,
            }))
            .collect::<Vec<_>>(),
    })
}

#[cfg(feature = "ml-classical")]
fn classical_objective_json(kind: ClassicalKind) -> serde_json::Value {
    serde_json::json!({
        "family": "classical_pointwise_baseline",
        "kind": kind.as_str(),
        "rank_loss": serde_json::Value::Null,
        "optimizer": serde_json::Value::Null,
        "note": "classical smartcore adapters train pointwise supervised models and validate by rank_ic; they are not LTR rankers",
    })
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
        _examples: &[TrainingExample],
        kind: ClassicalKind,
    ) -> QuantResult<(ModelArtifact, serde_json::Value, serde_json::Value)> {
        Err(ResearchError::RuntimeUnavailable {
            family: kind.to_string(),
            detail: "classical training requires the `ml-classical` build".to_owned(),
        }
        .into())
    }
}
