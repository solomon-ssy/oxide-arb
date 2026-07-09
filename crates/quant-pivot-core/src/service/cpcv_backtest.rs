//! Phase 11.5 Combinatorial Purged Cross-Validation + governed trial-grid
//! orchestration for the `WeightedFactor` model family.
//!
//! Mirrors the crate boundary the single-path [`BacktestService`](crate::service::backtest::BacktestService)
//! and [`ModelTrainerService`](crate::service::model_training::ModelTrainerService)
//! already establish: this service does the **impure** work (dataset load,
//! Parquet decode, PIT rematerialization of both training examples and
//! backtest ticks over the identical window — done exactly **once**, then
//! reused across every CPCV fold and every trial), and delegates every
//! **pure** algorithm (purge/embargo, φ-path reconstruction, DSR/PSR/MinTRL,
//! CSCV/PBO) to [`quant_pivot_research::validation`]. No live `BookStore` is
//! ever touched (the window is batch-prefetched from `ClickHouse`, exactly
//! like the single-path replay).

use std::{
    collections::{BTreeMap, HashSet},
    sync::Arc,
    time::Duration as StdDuration,
};

use async_trait::async_trait;
use chrono::{DateTime, Duration as ChronoDuration, Utc};
use rayon::prelude::*;
use tokio::{runtime::Handle, task};
use tokio_util::sync::CancellationToken;

use quant_pivot_error::{QuantError, QuantResult, research::ResearchError, storage::StorageError};
use quant_pivot_models::{
    domain::{JobProgressSink, ResearchJobProgress, TrainingDatasetInfo},
    enums::{common::MarketCategory, quant::TrainingDatasetStatus},
    runtime_config::sections::FactorsConfig,
    types::{
        BacktestPathSetId, BacktestReportId, ModelRunId, ModelVersionId, RuntimeConfigVersionId,
        TrainingDatasetId,
    },
};
use quant_pivot_repository::traits::{
    EventRepository, MarketLinkageRepository, MarketRepository, QuantFactReadRepository,
    TrainingDatasetRepository,
};
use quant_pivot_research::{
    artifact::ArtifactStore,
    backtest::{
        BacktestInputs, BacktestRequest, BacktestTick, Backtester, PortfolioCaps,
        PortfolioReplayBacktester, sharpe_ratio,
    },
    model::{
        FactorWeight, LabelSelector, ModelArtifact, ModelArtifactHeader, ModelFamily, ModelTrainer,
        QuantModelRuntime, ReturnModelSpec, ScoreMultiplierSpec, SubstitutionConfidenceRules,
        TrainModelRequest, TrainingObjectiveSpec, ValidationSpec, WeightedFactorRuntime,
        WeightedFactorTrainer,
    },
    stats,
    training::{DatasetParquetCodec, TrainingExample},
    validation::{
        BacktestPath, BacktestPathSet, CombinatorialPurgedBacktester, CpcvConfig, CpcvRequest,
        DefaultCombinatorialPurgedBacktester, DsrInput, DsrReport, FoldModelSource,
        GroupEvaluation, GroupRowFilter, PboInput, PurgeConfig, ReplayEngine, TimelineGroup, Trial,
        TrialGridSpec, TrialPerformanceMatrix, deflated_sharpe_ratio, min_track_record_length,
        probability_of_backtest_overfitting,
    },
};
use rust_decimal::Decimal;

use crate::{
    prefetch::historical_window::{HistoricalWindowLoader, WindowSpec},
    service::{
        backtest::materialize_ticks,
        dataset_replay::{
            RematerializeInputs, ReplaySchedule, max_horizon, rematerialize_training_examples,
        },
        historical_replay::ReplayConfig,
        model_training::weighted_seed_weights,
    },
};

/// Repository + store dependencies.
///
/// This service needs both the trainer's and the backtester's read paths:
/// it mirrors `BacktestServiceDeps` (`crate::service::backtest`) combined
/// with `ModelTrainerServiceDeps` (`crate::service::model_training`).
pub struct CpcvBacktestServiceDeps {
    pub dataset_repo: Arc<dyn TrainingDatasetRepository>,
    pub artifact_store: Arc<dyn ArtifactStore>,
    pub fact_read: Arc<dyn QuantFactReadRepository>,
    pub market_repo: Arc<dyn MarketRepository>,
    pub event_repo: Arc<dyn EventRepository>,
    pub linkage_repo: Arc<dyn MarketLinkageRepository>,
}

/// Governed methodology configuration (`research.validation.*`, Phase 11.5 §6).
#[derive(Debug, Clone)]
pub struct CpcvBacktestConfig {
    /// Seed factor weights (`factors.factor_weights`), shared with production training.
    pub factors: FactorsConfig,
    /// Base training objective (`research.training.*`) every CPCV fold trains against.
    pub objective: TrainingObjectiveSpec,
    /// CPCV partition config (`research.validation.cpcv.*`).
    pub cpcv: CpcvConfig,
    /// Purge/embargo config (`research.validation.purge.*`).
    pub purge: PurgeConfig,
    /// Governed hyperparameter trial grid (`research.validation.trials.*`).
    pub trials: TrialGridSpec,
    /// CSCV block config (`research.validation.pbo.*`).
    pub pbo: PboInput,
    /// `MinTRL` target significance (`research.validation.gates.dsr_significance`).
    pub dsr_significance: Decimal,
}

/// A CPCV/trial-grid request resolved by the admin port.
pub struct CpcvBacktestInput {
    pub training_dataset_id: TrainingDatasetId,
    pub runtime_config_version_id: RuntimeConfigVersionId,
    pub label: LabelSelector,
    /// The model family under validation. `WeightedFactor` trains via
    /// [`WeightedFactorTrainer`]; any `is_classical()` family trains via
    /// [`quant_pivot_research::model::ClassicalAdapterRegistry`] (`ml-classical`
    /// feature — Phase 11.5 §3.6's second-tier family, sharing every CPCV/
    /// trial-grid/DSR/PBO algorithm with `WeightedFactor` through
    /// [`FoldModelSource`]).
    pub model_family: ModelFamily,
    pub prediction_horizon_secs: u64,
    pub category_scope: Option<MarketCategory>,
    /// Pre-assigned path-set id (async job engine); minted when absent.
    pub path_set_id: Option<BacktestPathSetId>,
    /// Effective independent trials from the production model's
    /// `coordinate_search` (read from `metrics_json.validation.coord_search_effective_n`).
    /// Classical / missing → 0 (trial grid alone supplies N).
    pub coord_search_effective_n: u32,
}

/// The full Phase 11.5 validation outcome: CPCV path distribution, the
/// trial-grid-corrected Deflated Sharpe Ratio, PBO, and `MinTRL`.
#[derive(Debug, Clone)]
pub struct CpcvBacktestOutcome {
    pub path_set: BacktestPathSet,
    pub dsr: DsrReport,
    pub pbo: Decimal,
    pub min_track_record_length: Option<ChronoDuration>,
    /// Total DSR N = `trial_grid_count` + `coord_search_effective_n`.
    pub trial_count: u32,
    pub trial_grid_count: u32,
    pub coord_search_effective_n: u32,
    /// The frozen training dataset's replay window (echoed for the caller's
    /// persistence row — the same `window_start`/`window_end` convention
    /// [`crate::service::backtest::BacktestService`] uses).
    pub window_start: DateTime<Utc>,
    pub window_end: DateTime<Utc>,
}

/// The CPCV + trial-grid orchestration service.
pub struct CpcvBacktestService {
    deps: CpcvBacktestServiceDeps,
    config: CpcvBacktestConfig,
    caps: PortfolioCaps,
    replay: ReplayConfig,
    max_book_staleness: StdDuration,
}

impl CpcvBacktestService {
    /// Assemble the service from deps + the frozen replay/portfolio config
    /// (the same `portfolio: &PortfolioConfig` convention
    /// [`crate::service::backtest::BacktestService::new`] uses).
    #[must_use]
    pub fn new(
        deps: CpcvBacktestServiceDeps,
        config: CpcvBacktestConfig,
        portfolio: &quant_pivot_models::runtime_config::PortfolioConfig,
        replay: ReplayConfig,
        max_book_staleness: StdDuration,
    ) -> Self {
        Self {
            deps,
            config,
            caps: PortfolioCaps::from(portfolio),
            replay,
            max_book_staleness,
        }
    }

    /// Run CPCV + the governed trial grid, producing the full Phase 11.5
    /// validation outcome. Materializes training examples and backtest ticks
    /// **once** over the dataset's frozen window (real `ClickHouse` I/O),
    /// then runs every fold/trial as pure, `spawn_blocking`-offloaded,
    /// rayon-parallel compute with no further I/O.
    pub async fn run(
        &self,
        input: CpcvBacktestInput,
        progress: &dyn JobProgressSink,
        cancel: &CancellationToken,
    ) -> QuantResult<CpcvBacktestOutcome> {
        let dataset = self.load_ready_dataset(&input.training_dataset_id).await?;
        let (fold_template, replay_template) = self
            .prepare_templates(&dataset, &input, progress, cancel)
            .await?;
        let groups = Arc::clone(fold_template.groups());

        progress.report(ResearchJobProgress::indeterminate("cpcv", 0));
        let path_set_id = input.path_set_id.unwrap_or_else(BacktestPathSetId::from_v7);
        let path_set = self
            .run_cpcv(path_set_id, &fold_template, &replay_template, &groups)
            .await?;

        progress.report(ResearchJobProgress::indeterminate("trial_grid", 0));
        let (matrix, trial_grid_count) = self
            .run_trials(&fold_template, &replay_template, &groups)
            .await?;

        let (dsr, pbo) = compute_dsr_and_pbo(
            &dataset,
            &path_set,
            &matrix,
            trial_grid_count,
            input.coord_search_effective_n,
            &self.config,
        )?;
        let min_track_record_length = representative_path(&path_set)
            .and_then(|path| min_trl_for_path(&dataset, path, &self.config.dsr_significance));
        let trial_count = trial_grid_count.saturating_add(input.coord_search_effective_n);

        Ok(CpcvBacktestOutcome {
            path_set,
            dsr,
            pbo,
            min_track_record_length,
            trial_count,
            trial_grid_count,
            coord_search_effective_n: input.coord_search_effective_n,
            window_start: dataset.window_start,
            window_end: dataset.window_end,
        })
    }

    /// Materialize training examples + backtest ticks once, and assemble the
    /// `Arc`-shared templates every CPCV fold and trial trains/evaluates
    /// against. Dispatches on `input.model_family` to build either a
    /// `WeightedFactor` or a classical-ML [`FoldTemplate`] — the **only**
    /// family branch point in this service.
    async fn prepare_templates(
        &self,
        dataset: &TrainingDatasetInfo,
        input: &CpcvBacktestInput,
        progress: &dyn JobProgressSink,
        cancel: &CancellationToken,
    ) -> QuantResult<(Arc<FoldTemplate>, Arc<ReplayTemplate>)> {
        progress.report(ResearchJobProgress::indeterminate("load", 0));
        let parquet_examples = self.decode_examples(dataset).await?;

        progress.report(ResearchJobProgress::indeterminate(
            "materialize_examples",
            parquet_examples.len() as u64,
        ));
        let examples = rematerialize_training_examples(&RematerializeInputs {
            dataset,
            parquet_examples: &parquet_examples,
            fact_read: Arc::clone(&self.deps.fact_read),
            market_repo: Arc::clone(&self.deps.market_repo),
            event_repo: Arc::clone(&self.deps.event_repo),
            linkage_repo: Arc::clone(&self.deps.linkage_repo),
            replay: &self.replay,
            max_book_staleness: self.max_book_staleness,
        })
        .await?;

        let groups: Arc<[TimelineGroup]> = build_timeline_groups(&examples, &input.label)?.into();
        let header_template = ModelArtifactHeader {
            model_version_id: ModelVersionId::from_v7(),
            model_family: input.model_family,
            feature_schema_hash: dataset.feature_schema_hash.clone(),
            factor_schema_hash: dataset.factor_schema_hash.clone(),
        };
        let probe_required_features = self.probe_required_features(input.model_family);

        progress.report(ResearchJobProgress::indeterminate("materialize_ticks", 0));
        let ticks = self
            .materialize_full_window_ticks(
                dataset,
                &parquet_examples,
                &header_template,
                &probe_required_features,
                progress,
                cancel,
            )
            .await?;
        let Some(ticks) = ticks else {
            return Err(ResearchError::Cancelled {
                detail: "cpcv backtest cancelled during tick materialization".to_owned(),
            }
            .into());
        };
        let ticks_by_as_of: Arc<BTreeMap<DateTime<Utc>, BacktestTick>> =
            Arc::new(ticks.into_iter().map(|tick| (tick.as_of, tick)).collect());
        let handle = Handle::current();
        let replay_template = Arc::new(ReplayTemplate {
            ticks_by_as_of,
            groups: Arc::clone(&groups),
            caps: self.caps.clone(),
            handle: handle.clone(),
        });

        let fold_template = match input.model_family.classical_kind() {
            None => {
                let seed_weights = weighted_seed_weights(&self.config.factors, &examples);
                Arc::new(FoldTemplate::WeightedFactor(FoldTrainTemplate {
                    examples: examples.into(),
                    label: input.label.clone(),
                    seed_weights,
                    header_template,
                    base_objective: self.config.objective.clone(),
                    prediction_horizon_secs: input.prediction_horizon_secs,
                    category_scope: input.category_scope,
                    groups: Arc::clone(&groups),
                    purge: self.config.purge,
                    handle,
                }))
            }
            #[cfg(feature = "ml-classical")]
            Some(kind) => Arc::new(FoldTemplate::Classical(self.classical_fold_template(
                dataset,
                &examples,
                &input.label,
                header_template,
                kind,
                groups,
            ))),
            #[cfg(not(feature = "ml-classical"))]
            Some(_kind) => {
                return Err(ResearchError::RuntimeUnavailable {
                    family: input.model_family.to_string(),
                    detail: "classical CPCV requires the `ml-classical` feature".to_owned(),
                }
                .into());
            }
        };
        Ok((fold_template, replay_template))
    }

    /// The feature list a classical-family probe/fold reads (the governed
    /// [`quant_pivot_research::features::FeatureSchema`]'s full column set);
    /// empty for `WeightedFactor` (its factor table shape needs no
    /// `required_features` hint).
    fn probe_required_features(
        &self,
        model_family: ModelFamily,
    ) -> Vec<quant_pivot_research::features::FeatureName> {
        if model_family.is_classical() {
            quant_pivot_research::features::FeatureSchema::build(&self.replay.features).names()
        } else {
            Vec::new()
        }
    }

    #[cfg(feature = "ml-classical")]
    fn classical_fold_template(
        &self,
        dataset: &TrainingDatasetInfo,
        examples: &[TrainingExample],
        label: &LabelSelector,
        header_template: ModelArtifactHeader,
        kind: quant_pivot_research::model::ClassicalKind,
        groups: Arc<[TimelineGroup]>,
    ) -> ClassicalFoldTemplate {
        ClassicalFoldTemplate {
            examples: examples.to_vec().into(),
            label: label.clone(),
            header_template,
            kind,
            schema: Arc::new(quant_pivot_research::features::FeatureSchema::build(
                &self.replay.features,
            )),
            label_schema_hash: dataset.label_schema_hash.clone(),
            training_dataset_hash: dataset.dataset_hash.clone(),
            groups,
        }
    }

    /// Run the CPCV fold sweep on a blocking thread (rayon-parallel across
    /// combinations internally).
    async fn run_cpcv(
        &self,
        path_set_id: BacktestPathSetId,
        fold_template: &Arc<FoldTemplate>,
        replay_template: &Arc<ReplayTemplate>,
        groups: &Arc<[TimelineGroup]>,
    ) -> QuantResult<BacktestPathSet> {
        let fold_template = Arc::clone(fold_template);
        let replay_template = Arc::clone(replay_template);
        let groups = Arc::clone(groups);
        let cpcv_config = self.config.cpcv;
        let purge_config = self.config.purge;
        task::spawn_blocking(move || -> QuantResult<BacktestPathSet> {
            let fold_source = fold_template.fold_source(None)?;
            let replay_engine = PortfolioReplayEngineAdapter {
                template: &replay_template,
            };
            DefaultCombinatorialPurgedBacktester::new().run(CpcvRequest {
                path_set_id,
                groups: &groups,
                cpcv: cpcv_config,
                purge: purge_config,
                fold_source: fold_source.as_ref(),
                replay: &replay_engine,
            })
        })
        .await
        .map_err(|error| {
            QuantError::from(ResearchError::ValidationMethodology {
                detail: format!("cpcv fold task join failed: {error}"),
            })
        })?
    }

    /// Run the governed trial grid on a blocking thread, returning the
    /// resulting performance matrix + trial count.
    async fn run_trials(
        &self,
        fold_template: &Arc<FoldTemplate>,
        replay_template: &Arc<ReplayTemplate>,
        groups: &Arc<[TimelineGroup]>,
    ) -> QuantResult<(TrialPerformanceMatrix, u32)> {
        let trials = self.config.trials.generate(&self.config.objective)?;
        let trial_count = u32::try_from(trials.len()).unwrap_or(u32::MAX);
        let fold_template = Arc::clone(fold_template);
        let replay_template = Arc::clone(replay_template);
        let groups = Arc::clone(groups);
        let matrix = task::spawn_blocking(move || -> QuantResult<TrialPerformanceMatrix> {
            run_trial_grid(&trials, &fold_template, &replay_template, &groups)
        })
        .await
        .map_err(|error| {
            QuantError::from(ResearchError::ValidationMethodology {
                detail: format!("trial grid task join failed: {error}"),
            })
        })??;
        Ok((matrix, trial_count))
    }

    async fn materialize_full_window_ticks(
        &self,
        dataset: &TrainingDatasetInfo,
        parquet_examples: &[TrainingExample],
        header_template: &ModelArtifactHeader,
        required_features: &[quant_pivot_research::features::FeatureName],
        progress: &dyn JobProgressSink,
        cancel: &CancellationToken,
    ) -> QuantResult<Option<Vec<BacktestTick>>> {
        // A "shape probe" runtime: `materialize_ticks`/`build_runtime_input` only
        // consult `model.model_family()` / `required_features()` to decide the
        // tick's input shape — no inference happens here. Every fold and trial
        // later builds and scores its *own* freshly trained runtime; this
        // probe is never scored.
        let probe_runtime = ProbeRuntime {
            model_family: header_template.model_family,
            feature_schema_hash: header_template.feature_schema_hash.clone(),
            required_features: required_features.to_vec(),
        };

        let schedule = ReplaySchedule::from_examples(parquet_examples);
        let source_delay =
            StdDuration::from_secs(u64::try_from(dataset.source_delay_secs).unwrap_or(0));
        let lookback = StdDuration::from_secs(self.replay.features.max_lookback_secs());
        let max_horizon_secs = max_horizon(dataset);
        let loader = HistoricalWindowLoader::new(
            Arc::clone(&self.deps.fact_read),
            Arc::clone(&self.deps.market_repo),
            Arc::clone(&self.deps.event_repo),
            Arc::clone(&self.deps.linkage_repo),
            self.max_book_staleness,
        );
        let window = loader
            .load(&WindowSpec {
                window_start: dataset.window_start,
                window_end: dataset.window_end,
                samples: schedule.sample_set.clone(),
                lookback,
                source_delay,
                max_horizon_secs,
                domain: self.replay.domain.clone(),
            })
            .await?;
        let model_run_id = ModelRunId::from_v7();
        materialize_ticks(
            &window,
            &schedule,
            &self.replay,
            &probe_runtime,
            &model_run_id,
            source_delay,
            lookback,
            cancel,
            progress,
        )
        .await
    }

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
                    "cpcv backtest requires a Built/Ready dataset, got {}",
                    dataset.status.as_str()
                ),
            }
            .into());
        }
        Ok(dataset)
    }

    async fn decode_examples(
        &self,
        dataset: &TrainingDatasetInfo,
    ) -> QuantResult<Vec<TrainingExample>> {
        let bytes = self.deps.artifact_store.get(&dataset.parquet_uri).await?;
        DatasetParquetCodec::decode(&bytes)
    }
}

/// A minimal, never-scored [`QuantModelRuntime`] used only to determine
/// [`quant_pivot_research::model::ModelRuntimeInput`]'s shape during tick
/// materialization (`build_runtime_input` consults `model_family()` /
/// `required_features()` only — no inference). Every real CPCV fold and
/// trial builds and scores its own freshly trained runtime instead.
struct ProbeRuntime {
    model_family: ModelFamily,
    feature_schema_hash: quant_pivot_models::types::ContentHash,
    required_features: Vec<quant_pivot_research::features::FeatureName>,
}

#[async_trait]
impl QuantModelRuntime for ProbeRuntime {
    fn model_version_id(&self) -> ModelVersionId {
        ModelVersionId::from_v7()
    }

    fn model_family(&self) -> ModelFamily {
        self.model_family
    }

    fn feature_schema_hash(&self) -> quant_pivot_models::types::ContentHash {
        self.feature_schema_hash.clone()
    }

    fn required_features(&self) -> Vec<quant_pivot_research::features::FeatureName> {
        self.required_features.clone()
    }

    async fn infer_batch(
        &self,
        _input: quant_pivot_research::model::ModelRuntimeInput,
    ) -> QuantResult<quant_pivot_research::model::ModelRuntimeOutput> {
        unreachable!("ProbeRuntime is never scored, only used to determine tick input shape")
    }
}

/// Build one [`TimelineGroup`] per distinct `as_of` in `examples`, ascending.
/// `label_horizon_end` is the maximum `matured_at` across the group's rows
/// carrying the selected `(label.name, label.horizon_secs)` — the
/// conservative upper bound the [`quant_pivot_research::validation::PurgedSplitter`]
/// purges against. Groups with no row carrying the selected label are
/// dropped (mirrors the weighted trainer's own singleton/unlabeled-group drop
/// in [`quant_pivot_research::model::trainer`]).
///
/// # Errors
///
/// Returns [`ResearchError::ValidationMethodology`] when no example carries
/// the selected label (nothing to build a timeline from).
fn build_timeline_groups(
    examples: &[TrainingExample],
    label: &LabelSelector,
) -> QuantResult<Vec<TimelineGroup>> {
    let mut by_as_of: BTreeMap<DateTime<Utc>, DateTime<Utc>> = BTreeMap::new();
    for example in examples {
        let matching_label = |row: &&quant_pivot_research::training::TrainingLabel| {
            let name_matches = row.label_name == label.name;
            let horizon_matches = row.horizon_secs == label.horizon_secs;
            name_matches && horizon_matches
        };
        let Some(matured_at) = example
            .labels
            .iter()
            .filter(matching_label)
            .map(|row| row.matured_at)
            .max()
        else {
            continue;
        };
        by_as_of
            .entry(example.as_of)
            .and_modify(|end| *end = (*end).max(matured_at))
            .or_insert(matured_at);
    }
    if by_as_of.is_empty() {
        return Err(ResearchError::ValidationMethodology {
            detail: format!(
                "no example carries label `{}` @ horizon {}s — cannot build CPCV timeline groups",
                label.name, label.horizon_secs
            ),
        }
        .into());
    }
    Ok(by_as_of
        .into_iter()
        .map(|(as_of, label_horizon_end)| TimelineGroup {
            as_of,
            label_horizon_end,
        })
        .collect())
}

/// Everything a [`FoldModelSource`] needs to train a `WeightedFactor` fold.
/// `handle` lets [`WeightedFactorFoldSource::train_fold`] block on the async
/// trainer from inside a rayon worker thread (rayon threads carry no tokio
/// runtime context, so `Handle::current()` would panic there — the handle
/// must be captured once, from the original async caller, and threaded
/// through explicitly).
struct FoldTrainTemplate {
    examples: Arc<[TrainingExample]>,
    label: LabelSelector,
    seed_weights: Vec<FactorWeight>,
    header_template: ModelArtifactHeader,
    /// The governed `research.training.*` objective every CPCV fold trains
    /// against; a trial-grid trial overrides this per trial (never per fold).
    base_objective: TrainingObjectiveSpec,
    prediction_horizon_secs: u64,
    category_scope: Option<MarketCategory>,
    groups: Arc<[TimelineGroup]>,
    /// Same purge/embargo as the outer CPCV run (trainer nested CV).
    purge: PurgeConfig,
    handle: Handle,
}

/// A family-dispatching fold template: selects which concrete
/// [`FoldModelSource`] backs CPCV/trial-grid training, based on
/// [`CpcvBacktestInput::model_family`]. Every algorithm downstream of
/// [`FoldModelSource`] (CPCV, trial-grid, DSR, PBO) is identical across
/// variants — this enum is the **only** family branch point.
enum FoldTemplate {
    WeightedFactor(FoldTrainTemplate),
    #[cfg(feature = "ml-classical")]
    Classical(ClassicalFoldTemplate),
}

impl FoldTemplate {
    const fn groups(&self) -> &Arc<[TimelineGroup]> {
        match self {
            Self::WeightedFactor(template) => &template.groups,
            #[cfg(feature = "ml-classical")]
            Self::Classical(template) => &template.groups,
        }
    }

    /// Build the fold source for a CPCV fold (`trial = None`, uses the
    /// template's own governed base config) or for one governed trial
    /// (`trial = Some`, must carry an override matching this template's family).
    fn fold_source<'a>(
        &'a self,
        trial: Option<&'a Trial>,
    ) -> QuantResult<Box<dyn FoldModelSource + 'a>> {
        match self {
            Self::WeightedFactor(template) => {
                let objective = match trial {
                    Some(trial) => trial.weighted_factor_objective.as_ref().ok_or_else(|| {
                        QuantError::from(ResearchError::ValidationMethodology {
                            detail: format!(
                                "trial {} has no WeightedFactor objective override",
                                trial.trial_id
                            ),
                        })
                    })?,
                    None => &template.base_objective,
                };
                Ok(Box::new(WeightedFactorFoldSource {
                    template,
                    objective,
                }))
            }
            #[cfg(feature = "ml-classical")]
            Self::Classical(template) => {
                let params_override = match trial {
                    Some(trial) => Some(trial.classical_params.as_ref().ok_or_else(|| {
                        QuantError::from(ResearchError::ValidationMethodology {
                            detail: format!(
                                "trial {} has no classical params override",
                                trial.trial_id
                            ),
                        })
                    })?),
                    None => None,
                };
                Ok(Box::new(ClassicalFoldSource {
                    template,
                    params_override,
                }))
            }
        }
    }
}

struct ReplayTemplate {
    ticks_by_as_of: Arc<BTreeMap<DateTime<Utc>, BacktestTick>>,
    groups: Arc<[TimelineGroup]>,
    caps: PortfolioCaps,
    handle: Handle,
}

/// Per-`as_of` accumulator for [`PortfolioReplayEngineAdapter::evaluate`]'s
/// group-return derivation.
#[derive(Default, Clone)]
struct TickAccumulator {
    allocated: Decimal,
    pnl: Decimal,
    scores: Vec<Decimal>,
    realized_bps: Vec<Decimal>,
}

/// [`FoldModelSource`] for the `WeightedFactor` family: filters `template`'s
/// examples down to `filter`'s groups' `as_of`s, then trains via the exact
/// same [`WeightedFactorTrainer`] production training uses.
struct WeightedFactorFoldSource<'a> {
    template: &'a FoldTrainTemplate,
    objective: &'a TrainingObjectiveSpec,
}

impl FoldModelSource for WeightedFactorFoldSource<'_> {
    fn train_fold(&self, filter: &GroupRowFilter) -> QuantResult<Box<dyn QuantModelRuntime>> {
        let allowed_as_of: HashSet<DateTime<Utc>> = filter
            .group_indices
            .iter()
            .map(|&idx| self.template.groups[idx].as_of)
            .collect();
        let fold_examples: Vec<TrainingExample> = self
            .template
            .examples
            .iter()
            .filter(|example| allowed_as_of.contains(&example.as_of))
            .cloned()
            .collect();

        let mut header = self.template.header_template.clone();
        header.model_version_id = ModelVersionId::from_v7();
        let request = TrainModelRequest {
            examples: fold_examples,
            label: self.template.label.clone(),
            seed_weights: self.template.seed_weights.clone(),
            objective: self.objective.clone(),
            validation: ValidationSpec {
                // CPCV already supplies the OOS distribution; fold training is a
                // full-window fit on the purged train subset (`folds = 1`).
                folds: 1,
                embargo_pct: self.template.purge.embargo_pct,
                min_embargo_secs: self.template.purge.min_embargo_secs,
            },
            header,
            prediction_horizon_secs: self.template.prediction_horizon_secs,
            multipliers: ScoreMultiplierSpec::conservative(),
            substitution_rules: SubstitutionConfidenceRules::conservative(),
            return_model: ReturnModelSpec::heuristic_default(),
            required_features: Vec::new(),
            category_scope: self.template.category_scope,
        };
        let trained = self
            .template
            .handle
            .block_on(WeightedFactorTrainer::new().train(request))?;
        let ModelArtifact::WeightedFactor(weighted) = trained.artifact else {
            return Err(ResearchError::ValidationMethodology {
                detail: "weighted-factor trainer produced a non-weighted-factor artifact"
                    .to_owned(),
            }
            .into());
        };
        let runtime = WeightedFactorRuntime::new(*weighted, None, None)?;
        Ok(Box::new(runtime))
    }
}

/// Everything a [`ClassicalFoldSource`] needs to train a classical-ML fold
/// (Phase 11.5 §3.6's second-tier family). Classical training
/// ([`ClassicalAdapterRegistry::adapter_for`]) is fully synchronous CPU work
/// (`smartcore`, no async I/O), so unlike [`FoldTrainTemplate`] this needs no
/// [`Handle`] to cross the rayon-thread boundary.
#[cfg(feature = "ml-classical")]
struct ClassicalFoldTemplate {
    examples: Arc<[TrainingExample]>,
    label: LabelSelector,
    header_template: ModelArtifactHeader,
    kind: quant_pivot_research::model::ClassicalKind,
    schema: Arc<quant_pivot_research::features::FeatureSchema>,
    label_schema_hash: quant_pivot_models::types::ContentHash,
    training_dataset_hash: quant_pivot_models::types::ContentHash,
    groups: Arc<[TimelineGroup]>,
}

/// [`FoldModelSource`] for classical-ML families: filters `template`'s
/// examples down to `filter`'s groups' `as_of`s, builds the governed
/// [`quant_pivot_research::training::TrainingMatrix`], and fits via the exact
/// same [`quant_pivot_research::model::ClassicalAdapterRegistry`] production
/// training uses — with `params_override` (Phase 11.5 §3.5's classical trial
/// grid) in place of the governed production defaults when set.
#[cfg(feature = "ml-classical")]
struct ClassicalFoldSource<'a> {
    template: &'a ClassicalFoldTemplate,
    params_override: Option<&'a quant_pivot_research::model::classical::ClassicalParams>,
}

#[cfg(feature = "ml-classical")]
impl FoldModelSource for ClassicalFoldSource<'_> {
    fn train_fold(&self, filter: &GroupRowFilter) -> QuantResult<Box<dyn QuantModelRuntime>> {
        use quant_pivot_research::model::{ClassicalAdapterRegistry, ClassicalRuntime};

        let allowed_as_of: HashSet<DateTime<Utc>> = filter
            .group_indices
            .iter()
            .map(|&idx| self.template.groups[idx].as_of)
            .collect();
        let fold_examples: Vec<TrainingExample> = self
            .template
            .examples
            .iter()
            .filter(|example| allowed_as_of.contains(&example.as_of))
            .cloned()
            .collect();

        let matrix = crate::service::model_training::build_classical_matrix(
            &fold_examples,
            &self.template.label,
            &self.template.schema,
        )?;
        let adapter = self.params_override.map_or_else(
            || ClassicalAdapterRegistry::adapter_for(self.template.kind),
            |params| ClassicalAdapterRegistry::adapter_with_params(self.template.kind, *params),
        );
        let output = adapter.train(&matrix)?;

        let mut header = self.template.header_template.clone();
        header.model_version_id = ModelVersionId::from_v7();
        let artifact = quant_pivot_research::model::artifact::ClassicalModelArtifact {
            header,
            artifact_id: quant_pivot_models::types::ModelArtifactId::from_v7(),
            kind: self.template.kind,
            crate_name: output.crate_name.clone(),
            crate_version: output.crate_version.clone(),
            label_schema_hash: self.template.label_schema_hash.clone(),
            training_dataset_hash: self.template.training_dataset_hash.clone(),
            // Ephemeral validation fold: never persisted to the artifact
            // store, so this URI is never dereferenced — `ClassicalRuntime::load`
            // takes the estimator bytes directly, below.
            serialized_model_uri: quant_pivot_models::types::ArtifactUri::parse(
                "memory://cpcv-ephemeral-fold",
            )
            .map_err(|error| ResearchError::ValidationMethodology {
                detail: format!("ephemeral fold artifact URI failed to parse: {error}"),
            })?,
            serialization_format: output.serialization_format,
            preprocessing: output.preprocessing.clone(),
            metrics: output.metrics.clone(),
        };
        let runtime = ClassicalRuntime::load(artifact, &output.model_bytes)?;
        Ok(Box::new(runtime))
    }
}

/// [`ReplayEngine`] for the `WeightedFactor` family: scores the model over
/// `filter`'s groups' pre-materialized ticks, then derives one
/// [`GroupEvaluation`] per group from the resulting per-sample outcomes
/// (grouped back by `as_of`; `return_value = tick_pnl / tick_allocated`,
/// mirroring [`quant_pivot_research::backtest::runner`]'s own Sharpe-input
/// convention).
struct PortfolioReplayEngineAdapter<'a> {
    template: &'a ReplayTemplate,
}

impl ReplayEngine for PortfolioReplayEngineAdapter<'_> {
    fn evaluate(
        &self,
        model: &dyn QuantModelRuntime,
        filter: &GroupRowFilter,
    ) -> QuantResult<Vec<GroupEvaluation>> {
        let mut ticks: Vec<BacktestTick> = filter
            .group_indices
            .iter()
            .filter_map(|&idx| {
                self.template
                    .ticks_by_as_of
                    .get(&self.template.groups[idx].as_of)
                    .cloned()
            })
            .collect();
        ticks.sort_by_key(|tick| tick.as_of);

        let request = BacktestRequest {
            backtest_report_id: BacktestReportId::from_v7(),
            model_version_id: model.model_version_id(),
            runtime_config_version_id: RuntimeConfigVersionId::from_v7(),
            window_start: ticks.first().map_or_else(Utc::now, |tick| tick.as_of),
            window_end: ticks.last().map_or_else(Utc::now, |tick| tick.as_of),
        };
        let result = self
            .template
            .handle
            .block_on(PortfolioReplayBacktester::new().run(BacktestInputs {
                request,
                model,
                ticks,
                caps: self.template.caps.clone(),
            }))?;

        let mut by_as_of: BTreeMap<DateTime<Utc>, TickAccumulator> = BTreeMap::new();
        for sample in &result.sample_outcomes {
            let entry = by_as_of.entry(sample.as_of).or_default();
            entry.allocated += sample.allocated_usd.inner();
            entry.pnl +=
                sample.allocated_usd.inner() * sample.realized_return_bps / Decimal::from(10_000);
            entry.scores.push(sample.composite_score.inner());
            entry.realized_bps.push(sample.realized_return_bps);
        }

        let mut evaluations = Vec::with_capacity(filter.group_indices.len());
        for &group_index in &filter.group_indices {
            let as_of = self.template.groups[group_index].as_of;
            let Some(accumulator) = by_as_of.get(&as_of) else {
                return Err(ResearchError::ValidationMethodology {
                    detail: format!(
                        "CPCV replay missing materialized tick for group as_of={as_of} \
                         (group_index={group_index}) — refuse to invent a zero return"
                    ),
                }
                .into());
            };
            let return_value = if accumulator.allocated > Decimal::ZERO {
                accumulator.pnl / accumulator.allocated
            } else {
                Decimal::ZERO
            };
            let rank_ic = if accumulator.scores.len() >= 2 {
                Some(stats::spearman(
                    &accumulator.scores,
                    &accumulator.realized_bps,
                ))
            } else {
                None
            };
            evaluations.push(GroupEvaluation {
                group_index,
                return_value,
                rank_ic,
            });
        }
        Ok(evaluations)
    }
}

/// Run every trial's **full-window** (no purge/embargo) train + evaluate,
/// producing one [`TrialPerformanceMatrix`] column per trial. Reuses the
/// exact same [`FoldModelSource`]/[`ReplayEngine`] the CPCV folds use, just
/// with `filter` covering every group and a per-trial objective override.
fn run_trial_grid(
    trials: &[Trial],
    fold_template: &FoldTemplate,
    replay_template: &ReplayTemplate,
    groups: &[TimelineGroup],
) -> QuantResult<TrialPerformanceMatrix> {
    let all_indices = GroupRowFilter {
        group_indices: (0..groups.len()).collect(),
    };
    let periods: Vec<DateTime<Utc>> = groups.iter().map(|group| group.as_of).collect();
    let replay_engine = PortfolioReplayEngineAdapter {
        template: replay_template,
    };

    let columns: Vec<QuantResult<Vec<Decimal>>> = trials
        .par_iter()
        .map(|trial| -> QuantResult<Vec<Decimal>> {
            let fold_source = fold_template.fold_source(Some(trial))?;
            let model = fold_source.train_fold(&all_indices)?;
            let evaluations = replay_engine.evaluate(model.as_ref(), &all_indices)?;
            let mut by_group: BTreeMap<usize, Decimal> = BTreeMap::new();
            for evaluation in evaluations {
                by_group.insert(evaluation.group_index, evaluation.return_value);
            }
            Ok((0..groups.len())
                .map(|idx| by_group.get(&idx).copied().unwrap_or(Decimal::ZERO))
                .collect())
        })
        .collect();
    let mut trial_returns = Vec::with_capacity(columns.len());
    for column in columns {
        trial_returns.push(column?);
    }

    // Transpose trial_returns (N_trials x T) into returns (T x N_trials).
    let returns: Vec<Vec<Decimal>> = (0..periods.len())
        .map(|period| trial_returns.iter().map(|column| column[period]).collect())
        .collect();
    Ok(TrialPerformanceMatrix { periods, returns })
}

/// The CPCV path whose Sharpe is the distribution's median — the
/// "representative path" DSR's `SR_hat`/`T`/skew/kurtosis are computed from
/// (Phase 11.5 plan §3.4). `None` for an empty path set (never constructed by
/// [`DefaultCombinatorialPurgedBacktester`], but handled defensively).
fn representative_path(path_set: &BacktestPathSet) -> Option<&BacktestPath> {
    let median = path_set.sharpe_distribution.median;
    path_set
        .paths
        .iter()
        .min_by_key(|path| (path.sharpe - median).abs())
}

fn compute_dsr_and_pbo(
    dataset: &TrainingDatasetInfo,
    path_set: &BacktestPathSet,
    matrix: &TrialPerformanceMatrix,
    trial_grid_count: u32,
    coord_search_effective_n: u32,
    config: &CpcvBacktestConfig,
) -> QuantResult<(DsrReport, Decimal)> {
    let Some(path) = representative_path(path_set) else {
        return Err(ResearchError::ValidationMethodology {
            detail: "cpcv produced an empty path set".to_owned(),
        }
        .into());
    };
    let trial_sharpes = trial_sharpe_series(matrix);
    let dsr_input = DsrInput {
        observed_sharpe: path.sharpe,
        returns_period_count: path.group_returns.len() as u64,
        period_length: ChronoDuration::seconds(dataset.sample_interval_secs.max(1)),
        skewness: stats::skewness(&path.group_returns),
        kurtosis: stats::kurtosis(&path.group_returns),
        // Bailey multiple-testing N: governed trial grid + production
        // coordinate_search effective trials (correlated local moves collapse
        // to one effective trial per improving round).
        trial_count: trial_grid_count.saturating_add(coord_search_effective_n),
        trial_sharpe_variance: stats::variance(&trial_sharpes),
    };
    let dsr = deflated_sharpe_ratio(&dsr_input);
    let pbo = probability_of_backtest_overfitting(matrix, &config.pbo)?;
    Ok((dsr, pbo))
}

/// One Sharpe ratio per trial column of `matrix` (used as the multiple-testing
/// correction's dispersion estimate — the variance of Sharpe *across trials*,
/// never conflated with the CPCV path-to-path variance).
fn trial_sharpe_series(matrix: &TrialPerformanceMatrix) -> Vec<Decimal> {
    (0..matrix.trial_count())
        .map(|trial| {
            let column: Vec<Decimal> = matrix.returns.iter().map(|row| row[trial]).collect();
            sharpe_ratio(&column, Decimal::ONE)
        })
        .collect()
}

fn min_trl_for_path(
    dataset: &TrainingDatasetInfo,
    path: &BacktestPath,
    dsr_significance: &Decimal,
) -> Option<ChronoDuration> {
    let input = DsrInput {
        observed_sharpe: path.sharpe,
        returns_period_count: path.group_returns.len() as u64,
        period_length: ChronoDuration::seconds(dataset.sample_interval_secs.max(1)),
        skewness: stats::skewness(&path.group_returns),
        kurtosis: stats::kurtosis(&path.group_returns),
        trial_count: 1,
        trial_sharpe_variance: Decimal::ZERO,
    };
    min_track_record_length(&input, *dsr_significance)
}
