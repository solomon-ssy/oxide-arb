//! Phase 11.5 Combinatorial Purged Cross-Validation + governed trial-grid
//! orchestration for the `WeightedFactor` and Sell (`HoldVsExitWeighted`,
//! Phase 11.5.1) model families.
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
    collections::{BTreeMap, BTreeSet, HashSet},
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
    runtime_config::{PortfolioConfig, sections::FactorsConfig},
    types::{
        BacktestPathSetId, BacktestReportId, ContentHash, ModelRunId, ModelVersionId, PositionId,
        RuntimeConfigVersionId, TrainingDatasetId, TrainingSampleSource,
    },
};
use quant_pivot_repository::traits::{
    EventRepository, MarketLinkageRepository, MarketRepository, QuantFactReadRepository,
    TrainingDatasetRepository,
};
use quant_pivot_research::{
    artifact::ArtifactStore,
    backtest::{
        BacktestInputs, BacktestRequest, BacktestTick, Backtester, CalendarReturn,
        LotBacktestInputs, LotBacktester, LotDecisionSequence, LotOutcome, LotReplayBacktester,
        PortfolioCaps, PortfolioReplayBacktester, SellNullBaseline, active_observation_count,
        calendarize_lot_returns, mean_calendar_return, replay_lot_null_baseline, sharpe_ratio,
    },
    factors::{FactorName, names as factor_names},
    features::FeatureName,
    model::{
        FactorWeight, LabelSelector, ModelArtifact, ModelArtifactHeader, ModelFamily, ModelTrainer,
        QuantModelRuntime, ReturnModelSpec, ScoreMultiplierSpec, SellScorerOutputSpec,
        SellScorerTrainer, SellSignalPolicy, SubstitutionConfidenceRules, TrainModelRequest,
        TrainSellScorerRequest, TrainingObjectiveSpec, ValidationSpec, WeightedFactorRuntime,
        WeightedFactorTrainer, WeightedSellScorerRuntime,
    },
    precision::RESEARCH_DECIMAL_SCALE,
    stats,
    training::{DatasetParquetCodec, TrainingExample},
    validation::{
        BacktestPath, BacktestPathSet, CombinatorialPurgedBacktester, CpcvConfig, CpcvRequest,
        DefaultCombinatorialPurgedBacktester, DsrInput, DsrReport, FoldModelSource, FoldRuntime,
        GroupEvaluation, GroupRowFilter, PboInput, PurgeConfig, RankObservation, ReplayEngine,
        SharpeDistribution, TimelineGroup, Trial, TrialGridSpec, TrialPerformanceMatrix,
        deflated_sharpe_ratio, min_track_record_length, probability_of_backtest_overfitting,
    },
};
use rust_decimal::{Decimal, prelude::ToPrimitive};

use crate::{
    prefetch::historical_window::{HistoricalWindowLoader, WindowSpec},
    service::{
        backtest::materialize_ticks,
        dataset_replay::{
            RematerializeInputs, ReplaySchedule, max_horizon, rematerialize_exit_decision_examples,
            rematerialize_training_examples,
        },
        historical_replay::ReplayConfig,
        model_training::weighted_seed_weights,
    },
};

/// Coarse CPCV job progress budget (units of work for `ResearchJobProgress`).
///
/// Phases are sequential; each reports `with_total(..., TOTAL)` so the UI can
/// show a determinate percentage plus a named phase.
struct CpcvProgress;

impl CpcvProgress {
    const TOTAL: u64 = 100;
    const LOAD: ProgressPhase = ProgressPhase { start: 0 };
    const MATERIALIZE_EXAMPLES: ProgressPhase = ProgressPhase { start: 10 };
    const MATERIALIZE_TICKS: ProgressPhase = ProgressPhase { start: 25 };
    const CPCV: ProgressPhase = ProgressPhase { start: 45 };
    const TRIAL_GRID: ProgressPhase = ProgressPhase { start: 75 };
    const FINALIZE: ProgressPhase = ProgressPhase { start: 95 };
}

struct ProgressPhase {
    start: u64,
}

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
    /// Governed opportunistic-exit thresholds Sell-side lot replay fires on
    /// (Phase 11.5.1) — the exact same thresholds
    /// `OpportunisticSellSignalEvaluator` uses live, frozen from
    /// `execution.exit_monitor.opportunistic_sell`, so the CPCV-replayed
    /// decision rule can never drift from production.
    pub sell_policy: SellSignalPolicy,
}

/// A CPCV/trial-grid request resolved by the admin port.
pub struct CpcvBacktestInput {
    pub training_dataset_id: TrainingDatasetId,
    pub runtime_config_version_id: RuntimeConfigVersionId,
    pub label: LabelSelector,
    /// The model family under validation. `WeightedFactor` trains via
    /// [`WeightedFactorTrainer`]; `HoldVsExitWeighted` trains via
    /// [`SellScorerTrainer`] over lot-grouped timelines (Phase 11.5.1) —
    /// sharing every CPCV/trial-grid/DSR/PBO algorithm with `WeightedFactor`
    /// through [`FoldModelSource`].
    pub model_family: ModelFamily,
    pub prediction_horizon_secs: u64,
    pub category_scope: Option<MarketCategory>,
    /// Pre-assigned path-set id (async job engine); minted when absent.
    pub path_set_id: Option<BacktestPathSetId>,
    /// Audit-only: production `coordinate_search` effective trials (persisted
    /// for operator visibility). **Not** part of DSR N — Bailey's N/V must
    /// describe the same trial population, and V comes only from the governed
    /// trial-grid Sharpe series.
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
    /// DSR multiple-testing N (= `trial_grid_count`). Same population as V.
    pub trial_count: u32,
    pub trial_grid_count: u32,
    /// Audit-only production coord-search effort (not included in DSR N).
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
        portfolio: &PortfolioConfig,
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

        progress.report(ResearchJobProgress::with_total(
            "cpcv",
            CpcvProgress::CPCV.start,
            CpcvProgress::TOTAL,
        ));
        let path_set_id = input.path_set_id.unwrap_or_else(BacktestPathSetId::from_v7);
        let mut path_set = self
            .run_cpcv(path_set_id, &fold_template, &replay_template, &groups)
            .await?;
        if let (FoldTemplate::Sell(_), ReplayTemplate::Sell(template)) =
            (fold_template.as_ref(), replay_template.as_ref())
        {
            // Collapse coincident lots into an activity-only series (never
            // zero-pad empty wall-clock buckets) before DSR/PBO.
            calendarize_sell_path_set(
                &mut path_set,
                &groups,
                template,
                u64::try_from(dataset.sample_interval_secs.max(1)).unwrap_or(1),
            )?;
        }

        progress.report(ResearchJobProgress::with_total(
            "trial_grid",
            CpcvProgress::TRIAL_GRID.start,
            CpcvProgress::TOTAL,
        ));
        let (matrix, trial_grid_count) = self
            .run_trials(
                &fold_template,
                &replay_template,
                &groups,
                u64::try_from(dataset.sample_interval_secs.max(1)).unwrap_or(1),
            )
            .await?;

        progress.report(ResearchJobProgress::with_total(
            "finalize",
            CpcvProgress::FINALIZE.start,
            CpcvProgress::TOTAL,
        ));
        let (dsr, pbo) =
            compute_dsr_and_pbo(&dataset, &path_set, &matrix, trial_grid_count, &self.config)?;
        let min_track_record_length = representative_path(&path_set)
            .and_then(|path| min_trl_for_path(&dataset, path, &self.config.dsr_significance));
        // Bailey DSR N/V must describe the same trial population: the governed
        // trial grid that produced `matrix` (and thus V). Coord-search is
        // audit-only and must not inflate N without a matching Sharpe column.
        let trial_count = trial_grid_count;

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
    /// `WeightedFactor` or a Sell (Phase 11.5.1) [`FoldTemplate`] — the
    /// **only** family branch point in this service.
    async fn prepare_templates(
        &self,
        dataset: &TrainingDatasetInfo,
        input: &CpcvBacktestInput,
        progress: &dyn JobProgressSink,
        cancel: &CancellationToken,
    ) -> QuantResult<(Arc<FoldTemplate>, Arc<ReplayTemplate>)> {
        progress.report(ResearchJobProgress::with_total(
            "load",
            CpcvProgress::LOAD.start,
            CpcvProgress::TOTAL,
        ));
        let parquet_examples = self.decode_examples(dataset).await?;

        if input.model_family.is_exit_scorer() {
            return self
                .prepare_sell_templates(dataset, input, parquet_examples, progress)
                .await;
        }

        progress.report(ResearchJobProgress::with_total(
            "materialize_examples",
            CpcvProgress::MATERIALIZE_EXAMPLES.start,
            CpcvProgress::TOTAL,
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
        // Fail before the expensive CPCV fold loop when the timeline cannot
        // support CSCV/PBO (T < block_count).
        if groups.len() < self.config.pbo.block_count as usize {
            return Err(ResearchError::ValidationMethodology {
                detail: format!(
                    "CPCV timeline has {} groups but research.validation.pbo.block_count={} \
                     — need at least block_count periods for CSCV/PBO",
                    groups.len(),
                    self.config.pbo.block_count
                ),
            }
            .into());
        }
        let header_template = ModelArtifactHeader {
            model_version_id: ModelVersionId::from_v7(),
            model_family: input.model_family,
            feature_schema_hash: dataset.feature_schema_hash.clone(),
            factor_schema_hash: dataset.factor_schema_hash.clone(),
        };
        let probe_required_features = self.probe_required_features(input.model_family);

        progress.report(ResearchJobProgress::with_total(
            "materialize_ticks",
            CpcvProgress::MATERIALIZE_TICKS.start,
            CpcvProgress::TOTAL,
        ));
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
        let replay_template = Arc::new(ReplayTemplate::Portfolio(PortfolioReplayTemplate {
            ticks_by_as_of,
            groups: Arc::clone(&groups),
            caps: self.caps.clone(),
            handle: handle.clone(),
        }));
        #[cfg(not(feature = "ml-classical"))]
        if input.model_family.is_classical() {
            return Err(ResearchError::RuntimeUnavailable {
                family: input.model_family.to_string(),
                detail: "classical CPCV requires the `ml-classical` feature".to_owned(),
            }
            .into());
        }
        let fold_template =
            self.build_fold_template(dataset, input, examples, header_template, groups, handle);
        Ok((fold_template, replay_template))
    }

    /// Build the family-specific [`FoldTemplate`] shared across CPCV folds and trials.
    ///
    /// Classical availability is gated in [`Self::prepare_templates`] so this
    /// helper stays a pure constructor (no `Result`) under both feature sets.
    fn build_fold_template(
        &self,
        dataset: &TrainingDatasetInfo,
        input: &CpcvBacktestInput,
        examples: Vec<TrainingExample>,
        header_template: ModelArtifactHeader,
        groups: Arc<[TimelineGroup]>,
        handle: Handle,
    ) -> Arc<FoldTemplate> {
        #[cfg(feature = "ml-classical")]
        if let Some(kind) = input.model_family.classical_kind() {
            return Arc::new(FoldTemplate::Classical(self.classical_fold_template(
                dataset,
                &examples,
                &input.label,
                header_template,
                kind,
                groups,
            )));
        }
        #[cfg(not(feature = "ml-classical"))]
        let _ = dataset;
        let seed_weights = weighted_seed_weights(&self.config.factors, &examples);
        Arc::new(FoldTemplate::WeightedFactor(FoldTrainTemplate {
            examples: examples.into(),
            label: input.label.clone(),
            seed_weights,
            header_template,
            base_objective: self.config.objective.clone(),
            prediction_horizon_secs: input.prediction_horizon_secs,
            category_scope: input.category_scope,
            groups,
            purge: self.config.purge,
            handle,
        }))
    }

    /// Build the Sell-side (`HoldVsExitWeighted`, Phase 11.5.1) fold + replay
    /// templates: lot-grouped timeline (§3.2), lot-decision sequences shared
    /// by training and replay, no ticks (Sell replay never touches the
    /// cross-sectional allocator).
    async fn prepare_sell_templates(
        &self,
        dataset: &TrainingDatasetInfo,
        input: &CpcvBacktestInput,
        parquet_examples: Vec<TrainingExample>,
        progress: &dyn JobProgressSink,
    ) -> QuantResult<(Arc<FoldTemplate>, Arc<ReplayTemplate>)> {
        if input.model_family != ModelFamily::HoldVsExitWeighted {
            return Err(ResearchError::ValidationMethodology {
                detail: format!(
                    "Sell-side CPCV only supports HoldVsExitWeighted, got {}",
                    input.model_family
                ),
            }
            .into());
        }

        progress.report(ResearchJobProgress::with_total(
            "materialize_exit_decisions",
            CpcvProgress::MATERIALIZE_EXAMPLES.start,
            CpcvProgress::TOTAL,
        ));
        let examples = rematerialize_exit_decision_examples(&RematerializeInputs {
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
        validate_sell_examples(&examples, &input.label)?;

        let (groups, sequences) = build_lot_timeline_groups(&examples, &input.label)?;
        // DSR/PBO consume the *activity-only* lot-native series (coincident
        // lots may collapse into one observation; empty wall-clock buckets are
        // never invented). Gate on that effective observation count — not raw
        // lot count and not zero-padded calendar span.
        let period_secs = u64::try_from(dataset.sample_interval_secs.max(1)).unwrap_or(1);
        let probe_outcomes: Vec<LotOutcome> = groups
            .iter()
            .map(|group| LotOutcome {
                position_id: PositionId::from_v7(),
                as_of: group.as_of,
                return_value: Decimal::ZERO,
                cumulative_exit_pct: Decimal::ZERO,
                rank_pairs: Vec::new(),
                path_diverged: false,
            })
            .collect();
        let active_obs = active_observation_count(&probe_outcomes, period_secs);
        if active_obs < self.config.pbo.block_count as usize {
            return Err(ResearchError::ValidationMethodology {
                detail: format!(
                    "Sell CPCV activity-only series has {active_obs} observation(s) from {} lots \
                     (period_secs={period_secs}) but research.validation.pbo.block_count={} — need \
                     at least block_count distinct active buckets for CSCV/PBO (widen the lot \
                     as_of span or shorten sample_interval_secs; empty calendar buckets are never \
                     zero-padded)",
                    groups.len(),
                    self.config.pbo.block_count
                ),
            }
            .into());
        }
        let groups: Arc<[TimelineGroup]> = groups.into();
        let sequences: Arc<[LotDecisionSequence]> = sequences.into();
        let seed_weights = sell_seed_weights(&examples);
        let header_template = ModelArtifactHeader {
            model_version_id: ModelVersionId::from_v7(),
            model_family: input.model_family,
            feature_schema_hash: dataset.feature_schema_hash.clone(),
            factor_schema_hash: dataset.factor_schema_hash.clone(),
        };
        let fold_template = Arc::new(FoldTemplate::Sell(SellFoldTemplate {
            sequences: Arc::clone(&sequences),
            label: input.label.clone(),
            seed_weights,
            header_template,
            base_objective: self.config.objective.clone(),
            prediction_horizon_secs: input.prediction_horizon_secs,
            label_schema_hash: dataset.label_schema_hash.clone(),
            groups: Arc::clone(&groups),
            purge: self.config.purge,
        }));
        let replay_template = Arc::new(ReplayTemplate::Sell(SellReplayTemplate {
            sequences,
            policy: self.config.sell_policy,
            label: input.label.clone(),
        }));
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
            let replay_engine = FoldReplayEngineAdapter {
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
        sample_interval_secs: u64,
    ) -> QuantResult<(TrialPerformanceMatrix, u32)> {
        let trials = self.config.trials.generate(&self.config.objective)?;
        let trial_count = u32::try_from(trials.len()).unwrap_or(u32::MAX);
        let fold_template = Arc::clone(fold_template);
        let replay_template = Arc::clone(replay_template);
        let groups = Arc::clone(groups);
        let matrix = task::spawn_blocking(move || -> QuantResult<TrialPerformanceMatrix> {
            run_trial_grid(
                &trials,
                &fold_template,
                &replay_template,
                &groups,
                sample_interval_secs,
            )
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
    feature_schema_hash: ContentHash,
    required_features: Vec<FeatureName>,
}

#[async_trait]
impl QuantModelRuntime for ProbeRuntime {
    fn model_version_id(&self) -> ModelVersionId {
        ModelVersionId::from_v7()
    }

    fn model_family(&self) -> ModelFamily {
        self.model_family
    }

    fn feature_schema_hash(&self) -> ContentHash {
        self.feature_schema_hash.clone()
    }

    fn required_features(&self) -> Vec<FeatureName> {
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

/// The selected label's `matured_at`, taking the max across every row on
/// `example` carrying `(label.name, label.horizon_secs)` (defensive; a Sell
/// `ExitDecision` row carries exactly one row per label in practice).
fn selected_label_matured_at(
    example: &TrainingExample,
    label: &LabelSelector,
) -> Option<DateTime<Utc>> {
    example
        .labels
        .iter()
        .filter(|row| {
            let name_matches = row.label_name == label.name;
            let horizon_matches = row.horizon_secs == label.horizon_secs;
            name_matches && horizon_matches
        })
        .map(|row| row.matured_at)
        .max()
}

/// Build one [`TimelineGroup`] per lot (`lot_context.position_id`), grouped
/// so purge/embargo can never split one lot's decision points across folds
/// (Phase 11.5.1 §3.2's core invariant). `as_of` is the lot's earliest
/// decision instant; `label_horizon_end` is the max `matured_at` across the
/// lot's rows carrying the selected label (uniformly the lot's `closed_at`
/// per [`quant_pivot_research::training::HoldVsExitProceedsLabeler`], taking
/// the max here only as a defensive upper bound).
///
/// Returns the groups alongside one [`LotDecisionSequence`] per group,
/// index-aligned — `sequences[i]` is `groups[i]`'s lot, sorted ascending by
/// `as_of` within the lot. The same `sequences` back both the Sell
/// [`FoldModelSource`] (training) and [`ReplayEngine`] (replay), so a lot's
/// decisions are read from exactly one materialized copy.
///
/// # Errors
///
/// Fails closed when an example is missing `lot_context`, or when no lot
/// carries the selected label at all.
fn build_lot_timeline_groups(
    examples: &[TrainingExample],
    label: &LabelSelector,
) -> QuantResult<(Vec<TimelineGroup>, Vec<LotDecisionSequence>)> {
    let mut by_position: BTreeMap<String, (PositionId, TimelineGroup, Vec<TrainingExample>)> =
        BTreeMap::new();
    for example in examples {
        let Some(ctx) = &example.lot_context else {
            return Err(ResearchError::ValidationMethodology {
                detail: "Sell CPCV example is missing lot_context".to_owned(),
            }
            .into());
        };
        let Some(matured_at) = selected_label_matured_at(example, label) else {
            continue;
        };
        by_position
            .entry(ctx.position_id.to_string())
            .and_modify(|(_, group, decisions)| {
                group.as_of = group.as_of.min(example.as_of);
                group.label_horizon_end = group.label_horizon_end.max(matured_at);
                decisions.push(example.clone());
            })
            .or_insert_with(|| {
                (
                    ctx.position_id.clone(),
                    TimelineGroup {
                        as_of: example.as_of,
                        label_horizon_end: matured_at,
                    },
                    vec![example.clone()],
                )
            });
    }
    if by_position.is_empty() {
        return Err(ResearchError::ValidationMethodology {
            detail: format!(
                "no ExitDecision lot carries label `{}` @ horizon {}s — cannot build Sell CPCV timeline groups",
                label.name, label.horizon_secs
            ),
        }
        .into());
    }
    let mut ordered: Vec<(PositionId, TimelineGroup, Vec<TrainingExample>)> =
        by_position.into_values().collect();
    ordered.sort_by_key(|(_, group, _)| group.as_of);

    let mut groups = Vec::with_capacity(ordered.len());
    let mut sequences = Vec::with_capacity(ordered.len());
    for (position_id, group, mut decisions) in ordered {
        decisions.sort_by_key(|example| example.as_of);
        groups.push(group);
        sequences.push(LotDecisionSequence {
            position_id,
            decisions,
        });
    }
    Ok((groups, sequences))
}

/// Fail-closed guard: every example must be an `ExitDecision` row with lot
/// context, position-state, and the selected label present. A malformed
/// dataset must abort the whole CPCV run, never silently skip rows.
fn validate_sell_examples(examples: &[TrainingExample], label: &LabelSelector) -> QuantResult<()> {
    if examples.is_empty() {
        return Err(ResearchError::ValidationMethodology {
            detail: "Sell CPCV requires at least one ExitDecision example".to_owned(),
        }
        .into());
    }
    for example in examples {
        if example.sample_source != TrainingSampleSource::ExitDecision {
            return Err(ResearchError::ValidationMethodology {
                detail: "Sell CPCV requires ExitDecision-only samples".to_owned(),
            }
            .into());
        }
        if example.lot_context.is_none() {
            return Err(ResearchError::ValidationMethodology {
                detail: "Sell CPCV example is missing lot_context".to_owned(),
            }
            .into());
        }
        if example.position_state.is_none() {
            return Err(ResearchError::ValidationMethodology {
                detail: "Sell CPCV example is missing position_state".to_owned(),
            }
            .into());
        }
        if selected_label_matured_at(example, label).is_none() {
            return Err(ResearchError::ValidationMethodology {
                detail: format!(
                    "Sell CPCV example has no label `{}` @ horizon {}s",
                    label.name, label.horizon_secs
                ),
            }
            .into());
        }
    }
    Ok(())
}

/// Uniform seed weights over every factor name present across the Sell
/// examples plus the three position-state pseudo-factors. Distinct from
/// [`weighted_seed_weights`] (Buy-side): Sell has no governed
/// `factors.factor_weights` config section, and must additionally seed the
/// position-state pseudo-factors those config-driven weights never cover.
fn sell_seed_weights(examples: &[TrainingExample]) -> Vec<FactorWeight> {
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

/// Everything a Sell-side [`FoldModelSource`] needs to train a lot-grouped
/// fold (Phase 11.5.1). `sequences` is index-aligned with `groups` and is the
/// **single** materialized copy of every lot's decisions — shared with the
/// [`SellReplayTemplate`] built alongside it, never duplicated.
struct SellFoldTemplate {
    sequences: Arc<[LotDecisionSequence]>,
    label: LabelSelector,
    seed_weights: Vec<FactorWeight>,
    header_template: ModelArtifactHeader,
    base_objective: TrainingObjectiveSpec,
    prediction_horizon_secs: u64,
    label_schema_hash: ContentHash,
    groups: Arc<[TimelineGroup]>,
    /// Same purge/embargo as the outer CPCV run (trainer nested CV).
    purge: PurgeConfig,
}

/// A family-dispatching fold template: selects which concrete
/// [`FoldModelSource`] backs CPCV/trial-grid training, based on
/// [`CpcvBacktestInput::model_family`]. Every algorithm downstream of
/// [`FoldModelSource`] (CPCV, trial-grid, DSR, PBO) is identical across
/// variants — this enum is the **only** family branch point.
enum FoldTemplate {
    WeightedFactor(FoldTrainTemplate),
    Sell(SellFoldTemplate),
    #[cfg(feature = "ml-classical")]
    Classical(ClassicalFoldTemplate),
}

impl FoldTemplate {
    const fn groups(&self) -> &Arc<[TimelineGroup]> {
        match self {
            Self::WeightedFactor(template) => &template.groups,
            Self::Sell(template) => &template.groups,
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
            Self::Sell(template) => {
                // Sell reuses the WeightedFactor trial-grid dimension
                // verbatim (§0/§3.4): `SellScorerTrainer::train_sell_scorer`
                // consumes the exact same `TrainingObjectiveSpec` type
                // `fit_simplex_weights` shares with the Buy-side trainer, so
                // perturbing it via `TrialGridSpec::WeightedFactor` is not a
                // convenience shortcut — it is the correct grid for this
                // family, and a bespoke `TrialGridSpec::Sell` variant would
                // duplicate it for no semantic gain.
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
                Ok(Box::new(SellFoldSource {
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

/// A family-dispatching replay template, mirroring [`FoldTemplate`]. The
/// **only** other family branch point in this service.
enum ReplayTemplate {
    Portfolio(PortfolioReplayTemplate),
    Sell(SellReplayTemplate),
}

struct PortfolioReplayTemplate {
    ticks_by_as_of: Arc<BTreeMap<DateTime<Utc>, BacktestTick>>,
    groups: Arc<[TimelineGroup]>,
    caps: PortfolioCaps,
    handle: Handle,
}

/// Everything [`evaluate_sell_groups`] needs to replay a Sell fold.
/// `sequences` is the same `Arc` [`SellFoldTemplate::sequences`] shares — one
/// materialized copy of every lot's decisions, read by both training and
/// replay.
struct SellReplayTemplate {
    sequences: Arc<[LotDecisionSequence]>,
    policy: SellSignalPolicy,
    label: LabelSelector,
}

/// Per-`as_of` accumulator for [`evaluate_portfolio_groups`]'s group-return
/// derivation.
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
    fn train_fold(&self, filter: &GroupRowFilter) -> QuantResult<FoldRuntime> {
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
        Ok(FoldRuntime::Buy(Box::new(runtime)))
    }
}

/// [`FoldModelSource`] for the Sell-side `HoldVsExitWeighted` family
/// (Phase 11.5.1): filters `template.sequences` down to `filter`'s lots, then
/// trains via the exact same [`SellScorerTrainer`] production training uses.
struct SellFoldSource<'a> {
    template: &'a SellFoldTemplate,
    objective: &'a TrainingObjectiveSpec,
}

impl FoldModelSource for SellFoldSource<'_> {
    fn train_fold(&self, filter: &GroupRowFilter) -> QuantResult<FoldRuntime> {
        let fold_examples: Vec<TrainingExample> = filter
            .group_indices
            .iter()
            .filter_map(|&idx| self.template.sequences.get(idx))
            .flat_map(|sequence| sequence.decisions.iter().cloned())
            .collect();
        if fold_examples.is_empty() {
            return Err(ResearchError::ValidationMethodology {
                detail: "Sell CPCV fold has no training examples after purge".to_owned(),
            }
            .into());
        }

        let mut header = self.template.header_template.clone();
        header.model_version_id = ModelVersionId::from_v7();
        let request = TrainSellScorerRequest {
            examples: fold_examples,
            label: self.template.label.clone(),
            seed_weights: self.template.seed_weights.clone(),
            objective: self.objective.clone(),
            validation: ValidationSpec {
                folds: 1,
                embargo_pct: self.template.purge.embargo_pct,
                min_embargo_secs: self.template.purge.min_embargo_secs,
            },
            header,
            prediction_horizon_secs: self.template.prediction_horizon_secs,
            output_spec: SellScorerOutputSpec::conservative(),
            label_schema_hash: self.template.label_schema_hash.clone(),
            required_features: Vec::new(),
        };
        let trained = SellScorerTrainer::new().train_sell_scorer(&request)?;
        let ModelArtifact::SellScorer(sell_artifact) = trained.artifact else {
            return Err(ResearchError::ValidationMethodology {
                detail: "sell-scorer trainer produced a non-sell artifact".to_owned(),
            }
            .into());
        };
        let runtime = WeightedSellScorerRuntime::new(*sell_artifact)?;
        Ok(FoldRuntime::Sell(Box::new(runtime)))
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
    label_schema_hash: ContentHash,
    training_dataset_hash: ContentHash,
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
    fn train_fold(&self, filter: &GroupRowFilter) -> QuantResult<FoldRuntime> {
        use quant_pivot_models::types::{ArtifactUri, ModelArtifactId};
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
            artifact_id: ModelArtifactId::from_v7(),
            kind: self.template.kind,
            crate_name: output.crate_name.clone(),
            crate_version: output.crate_version.clone(),
            label_schema_hash: self.template.label_schema_hash.clone(),
            training_dataset_hash: self.template.training_dataset_hash.clone(),
            // Ephemeral validation fold: never persisted to the artifact
            // store, so this URI is never dereferenced — `ClassicalRuntime::load`
            // takes the estimator bytes directly, below.
            serialized_model_uri: ArtifactUri::parse("memory://cpcv-ephemeral-fold").map_err(
                |error| ResearchError::ValidationMethodology {
                    detail: format!("ephemeral fold artifact URI failed to parse: {error}"),
                },
            )?,
            serialization_format: output.serialization_format,
            preprocessing: output.preprocessing.clone(),
            metrics: output.metrics.clone(),
        };
        let runtime = ClassicalRuntime::load(artifact, &output.model_bytes)?;
        Ok(FoldRuntime::Buy(Box::new(runtime)))
    }
}

/// [`ReplayEngine`] shared by every family: dispatches on `template`'s
/// variant to the matching pure evaluator, projecting the [`FoldRuntime`] to
/// that family's concrete trait via [`FoldRuntime::as_buy`]/
/// [`FoldRuntime::as_sell`] (fails closed on a mismatched pairing — a
/// construction bug, never reachable in practice since [`FoldTemplate`] and
/// [`ReplayTemplate`] are always built together for the same family).
struct FoldReplayEngineAdapter<'a> {
    template: &'a ReplayTemplate,
}

impl ReplayEngine for FoldReplayEngineAdapter<'_> {
    fn evaluate(
        &self,
        model: &FoldRuntime,
        filter: &GroupRowFilter,
    ) -> QuantResult<Vec<GroupEvaluation>> {
        match self.template {
            ReplayTemplate::Portfolio(template) => {
                evaluate_portfolio_groups(template, model.as_buy()?, filter)
            }
            ReplayTemplate::Sell(template) => {
                evaluate_sell_groups(template, model.as_sell()?, filter)
            }
        }
    }
}

/// Scores the model over `filter`'s groups' pre-materialized ticks, then
/// derives one [`GroupEvaluation`] per group from the resulting per-sample
/// outcomes (grouped back by `as_of`; `return_value = tick_pnl /
/// tick_allocated`, mirroring [`quant_pivot_research::backtest::runner`]'s
/// own Sharpe-input convention; `rank_observations` carries every scored
/// candidate's `(composite_score, realized_return_bps)` pair, pooled at the
/// path level by [`quant_pivot_research::validation::cpcv::build_path`]).
fn evaluate_portfolio_groups(
    template: &PortfolioReplayTemplate,
    model: &dyn QuantModelRuntime,
    filter: &GroupRowFilter,
) -> QuantResult<Vec<GroupEvaluation>> {
    let mut ticks: Vec<BacktestTick> = filter
        .group_indices
        .iter()
        .filter_map(|&idx| {
            template
                .ticks_by_as_of
                .get(&template.groups[idx].as_of)
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
    let result = template
        .handle
        .block_on(PortfolioReplayBacktester::new().run(BacktestInputs {
            request,
            model,
            ticks,
            caps: template.caps.clone(),
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
        let as_of = template.groups[group_index].as_of;
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
        let rank_observations = accumulator
            .scores
            .iter()
            .zip(&accumulator.realized_bps)
            .map(|(&score, &realized)| RankObservation { score, realized })
            .collect();
        evaluations.push(GroupEvaluation {
            group_index,
            return_value,
            rank_observations,
        });
    }
    Ok(evaluations)
}

/// Scores the model over `filter`'s lots via [`LotReplayBacktester`]
/// (Phase 11.5.1 §3.3), then derives one [`GroupEvaluation`] per lot from the
/// resulting outcomes. Lot-level `return_value` stays the on-path residual-shares
/// alpha; activity-only collapse for Sharpe/DSR happens after φ-path
/// reconstruction ([`calendarize_sell_path_set`]).
fn evaluate_sell_groups(
    template: &SellReplayTemplate,
    model: &dyn quant_pivot_research::model::SellScorerRuntime,
    filter: &GroupRowFilter,
) -> QuantResult<Vec<GroupEvaluation>> {
    let mut lots = Vec::with_capacity(filter.group_indices.len());
    for &group_index in &filter.group_indices {
        let Some(sequence) = template.sequences.get(group_index) else {
            return Err(ResearchError::ValidationMethodology {
                detail: format!(
                    "Sell CPCV replay missing lot sequence for group_index={group_index}"
                ),
            }
            .into());
        };
        lots.push(sequence.clone());
    }
    let result = LotReplayBacktester::new().run(LotBacktestInputs {
        model,
        policy: template.policy,
        label: template.label.clone(),
        lots: &lots,
    })?;
    if result.lots.len() != filter.group_indices.len() {
        return Err(ResearchError::ValidationMethodology {
            detail: "Sell CPCV lot replay returned a different lot count than requested".to_owned(),
        }
        .into());
    }
    Ok(filter
        .group_indices
        .iter()
        .zip(result.lots)
        .map(|(&group_index, outcome)| GroupEvaluation {
            group_index,
            return_value: outcome.return_value,
            rank_observations: outcome
                .rank_pairs
                .into_iter()
                .map(|(score, realized)| RankObservation { score, realized })
                .collect(),
        })
        .collect())
}

/// Run every trial's **full-window** (no purge/embargo) train + evaluate,
/// producing one [`TrialPerformanceMatrix`] column per trial. Reuses the
/// exact same [`FoldModelSource`]/[`ReplayEngine`] the CPCV folds use, just
/// with `filter` covering every group and a per-trial objective override.
///
/// For Sell, each trial column is collapsed onto the same activity-only
/// lot-native buckets the φ-path Sharpe/DSR use — otherwise DSR's trial-Sharpe
/// variance `V` would be computed on a different return process than the
/// observed series. Empty wall-clock buckets are never zero-padded.
fn run_trial_grid(
    trials: &[Trial],
    fold_template: &FoldTemplate,
    replay_template: &ReplayTemplate,
    groups: &[TimelineGroup],
    sample_interval_secs: u64,
) -> QuantResult<TrialPerformanceMatrix> {
    let all_indices = GroupRowFilter {
        group_indices: (0..groups.len()).collect(),
    };
    let replay_engine = FoldReplayEngineAdapter {
        template: replay_template,
    };
    let calendarize_sell = matches!(replay_template, ReplayTemplate::Sell(_));
    let period_secs = sample_interval_secs.max(1);

    let columns: Vec<QuantResult<Vec<Decimal>>> = trials
        .par_iter()
        .map(|trial| -> QuantResult<Vec<Decimal>> {
            let fold_source = fold_template.fold_source(Some(trial))?;
            let model = fold_source.train_fold(&all_indices)?;
            let evaluations = replay_engine.evaluate(&model, &all_indices)?;
            let mut by_group: BTreeMap<usize, Decimal> = BTreeMap::new();
            for evaluation in evaluations {
                by_group.insert(evaluation.group_index, evaluation.return_value);
            }
            // Fail-closed: never invent zero returns for missing groups (same
            // invariant as CPCV fold replay). Silent zeros would bias PBO/DSR V.
            let mut column = Vec::with_capacity(groups.len());
            for idx in 0..groups.len() {
                let Some(return_value) = by_group.get(&idx).copied() else {
                    return Err(ResearchError::ValidationMethodology {
                        detail: format!(
                            "trial-grid replay missing group_index={idx} — refuse to invent a zero return"
                        ),
                    }
                    .into());
                };
                column.push(return_value);
            }
            if calendarize_sell {
                let outcomes = synthetic_lot_outcomes(groups, &column);
                let calendar = calendarize_lot_returns(&outcomes, period_secs);
                if calendar.len() < 2 {
                    return Err(ResearchError::ValidationMethodology {
                        detail: format!(
                            "Sell trial-grid activity-only series has {} observation(s); \
                             need at least 2 for Sharpe/DSR V (empty calendar buckets are \
                             never zero-padded)",
                            calendar.len()
                        ),
                    }
                    .into());
                }
                Ok(calendar.into_iter().map(|row| row.return_value).collect())
            } else {
                Ok(column)
            }
        })
        .collect();
    let mut trial_returns = Vec::with_capacity(columns.len());
    for column in columns {
        trial_returns.push(column?);
    }

    let periods: Vec<DateTime<Utc>> = if calendarize_sell {
        let outcomes = synthetic_lot_outcomes(groups, &vec![Decimal::ZERO; groups.len()]);
        calendarize_lot_returns(&outcomes, period_secs)
            .into_iter()
            .map(|row| row.period_start)
            .collect()
    } else {
        groups.iter().map(|group| group.as_of).collect()
    };

    // Transpose trial_returns (N_trials x T) into returns (T x N_trials).
    let returns: Vec<Vec<Decimal>> = (0..periods.len())
        .map(|period| trial_returns.iter().map(|column| column[period]).collect())
        .collect();
    Ok(TrialPerformanceMatrix { periods, returns })
}

/// Rewrites Sell φ-path metrics from irregular lot returns into an
/// **activity-only** lot-native series before DSR/MinTRL/PBO consume them.
///
/// Empty wall-clock buckets are never zero-padded (that silently inflates
/// Sharpe). Coincident lots in the same `period_secs` bucket are summed.
fn calendarize_sell_path_set(
    path_set: &mut BacktestPathSet,
    groups: &[TimelineGroup],
    template: &SellReplayTemplate,
    period_secs: u64,
) -> QuantResult<()> {
    let mut path_mean_returns = Vec::with_capacity(path_set.paths.len());
    for path in &mut path_set.paths {
        if path.group_returns.len() != groups.len() {
            return Err(ResearchError::ValidationMethodology {
                detail: format!(
                    "Sell CPCV path {} has {} lot returns but timeline has {} groups",
                    path.path_index,
                    path.group_returns.len(),
                    groups.len()
                ),
            }
            .into());
        }
        let outcomes = synthetic_lot_outcomes(groups, &path.group_returns);
        let calendar_returns = calendarize_lot_returns(&outcomes, period_secs);
        // Bailey–López de Prado Sharpe/DSR need ≥ 2 activity observations.
        // A single active bucket forces `sharpe_ratio` → 0 and would silently
        // fail (or pass) DeflatedSharpe without a readable methodology error.
        if calendar_returns.len() < 2 {
            return Err(ResearchError::ValidationMethodology {
                detail: format!(
                    "Sell CPCV path {} activity-only series has {} observation(s) \
                     (period_secs={period_secs}, lots={}); need at least 2 distinct active \
                     buckets for Sharpe/DSR — widen the lot as_of span or shorten \
                     sample_interval_secs (empty calendar buckets are never zero-padded)",
                    path.path_index,
                    calendar_returns.len(),
                    groups.len()
                ),
            }
            .into());
        }
        path_mean_returns.push(mean_calendar_return(&calendar_returns));
        path.group_returns = calendar_returns
            .iter()
            .map(|row| row.return_value)
            .collect();
        path.sharpe = sharpe_ratio(&path.group_returns, Decimal::ONE);
        path.max_drawdown = max_drawdown_from_returns(&path.group_returns);
        path.tail_loss = tail_loss_from_returns(&path.group_returns, Decimal::new(10, 2));
    }

    // Hard gate baseline is exit-at-first (non-trivial null). AlwaysHold is
    // definitionally zero alpha and is covered by unit tests / offline compare,
    // not persisted into SharpeDistribution.
    let baseline_uplift = median_decimal(&path_mean_returns)
        - mean_calendar_return(&baseline_calendar_returns(
            template,
            SellNullBaseline::ExitAtFirstDecision,
            period_secs,
        )?);
    path_set.sharpe_distribution = sharpe_distribution_with_diagnostics(
        &path_set.paths,
        Some(baseline_uplift.round_dp(RESEARCH_DECIMAL_SCALE)),
    );
    Ok(())
}

fn synthetic_lot_outcomes(groups: &[TimelineGroup], returns: &[Decimal]) -> Vec<LotOutcome> {
    groups
        .iter()
        .zip(returns)
        .map(|(group, &return_value)| LotOutcome {
            position_id: PositionId::from_v7(),
            as_of: group.as_of,
            return_value,
            cumulative_exit_pct: Decimal::ONE,
            rank_pairs: Vec::new(),
            path_diverged: false,
        })
        .collect()
}

fn baseline_calendar_returns(
    template: &SellReplayTemplate,
    baseline: SellNullBaseline,
    period_secs: u64,
) -> QuantResult<Vec<CalendarReturn>> {
    let outcomes: Vec<QuantResult<LotOutcome>> = template
        .sequences
        .iter()
        .map(|sequence| replay_lot_null_baseline(baseline, &template.label, sequence))
        .collect();
    let mut lots = Vec::with_capacity(outcomes.len());
    for outcome in outcomes {
        lots.push(outcome?);
    }
    Ok(calendarize_lot_returns(&lots, period_secs))
}

fn sharpe_distribution_with_diagnostics(
    paths: &[BacktestPath],
    baseline_uplift: Option<Decimal>,
) -> SharpeDistribution {
    let mut sharpes: Vec<Decimal> = paths.iter().map(|path| path.sharpe).collect();
    sharpes.sort();
    let mut max_drawdowns: Vec<Decimal> = paths.iter().map(|path| path.max_drawdown).collect();
    max_drawdowns.sort();
    let mut tail_losses: Vec<Decimal> = paths.iter().map(|path| path.tail_loss).collect();
    tail_losses.sort();
    SharpeDistribution {
        min: percentile(&sharpes, Decimal::ZERO),
        p25: percentile(&sharpes, Decimal::new(25, 2)),
        median: percentile(&sharpes, Decimal::new(5, 1)),
        p75: percentile(&sharpes, Decimal::new(75, 2)),
        max: percentile(&sharpes, Decimal::ONE),
        median_max_drawdown: Some(percentile(&max_drawdowns, Decimal::new(5, 1))),
        median_tail_loss: Some(percentile(&tail_losses, Decimal::new(5, 1))),
        baseline_uplift,
    }
}

fn max_drawdown_from_returns(returns: &[Decimal]) -> Decimal {
    let mut cumulative = Decimal::ZERO;
    let mut peak = Decimal::ZERO;
    let mut max_dd = Decimal::ZERO;
    for &r in returns {
        cumulative += r;
        peak = peak.max(cumulative);
        max_dd = max_dd.max(peak - cumulative);
    }
    max_dd.round_dp(RESEARCH_DECIMAL_SCALE)
}

fn tail_loss_from_returns(returns: &[Decimal], quantile: Decimal) -> Decimal {
    if returns.is_empty() {
        return Decimal::ZERO;
    }
    let mut sorted = returns.to_vec();
    sorted.sort();
    let n = sorted.len();
    let raw = (Decimal::from(n as u64) * quantile).ceil();
    let take = raw
        .to_u64()
        .and_then(|value| usize::try_from(value).ok())
        .unwrap_or(1)
        .max(1)
        .min(n);
    stats::mean(&sorted[..take]).round_dp(RESEARCH_DECIMAL_SCALE)
}

/// `sorted` must already be ascending. Nearest-rank percentile (`0` = min,
/// `1` = max); `0` for an empty series.
fn percentile(sorted: &[Decimal], fraction: Decimal) -> Decimal {
    if sorted.is_empty() {
        return Decimal::ZERO;
    }
    let last = Decimal::from(u64::try_from(sorted.len() - 1).unwrap_or(u64::MAX));
    let index = (last * fraction)
        .round()
        .to_u64()
        .and_then(|value| usize::try_from(value).ok())
        .unwrap_or(0);
    sorted[index.min(sorted.len() - 1)]
}

fn median_decimal(values: &[Decimal]) -> Decimal {
    let mut sorted = values.to_vec();
    sorted.sort();
    percentile(&sorted, Decimal::new(5, 1))
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
        // Bailey multiple-testing N/V: same population — the governed trial
        // grid that produced `matrix`. Coord-search is audit-only.
        trial_count: trial_grid_count,
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

#[cfg(test)]
mod tests {
    use super::{
        LotDecisionSequence, SellReplayTemplate, TimelineGroup, build_lot_timeline_groups,
        calendarize_sell_path_set, selected_label_matured_at, validate_sell_examples,
    };
    use chrono::{DateTime, TimeZone, Utc};
    use quant_pivot_models::types::{
        BacktestPathSetId, MarketId, ModelVersionId, OrderIntentId, Price, SchemaVersion, Shares,
        TokenId, TrainingExampleId, TrainingSampleSource,
    };
    use quant_pivot_models::{enums::quant::DataQualityStatus, types::PositionId};
    use quant_pivot_research::{
        features::FeatureVector,
        model::{LabelSelector, PositionStateFeatures, SellSignalPolicy},
        training::{HOLD_VS_EXIT_ALPHA_BPS, LotTrainingContext, TrainingExample, TrainingLabel},
        validation::{BacktestPath, BacktestPathSet, SharpeDistribution},
    };
    use rust_decimal::Decimal;
    use rust_decimal_macros::dec;
    use std::collections::BTreeMap;
    use std::sync::Arc;

    fn ts(secs: i64) -> DateTime<Utc> {
        Utc.timestamp_opt(1_700_000_000 + secs, 0).unwrap()
    }

    fn label() -> LabelSelector {
        LabelSelector {
            name: HOLD_VS_EXIT_ALPHA_BPS,
            horizon_secs: 0,
        }
    }

    fn lot_decision(
        position_id: PositionId,
        as_of: DateTime<Utc>,
        closed_at: DateTime<Utc>,
    ) -> TrainingExample {
        let market_id = MarketId::new("0xmarket");
        let token_id = TokenId::new("yes");
        TrainingExample {
            example_id: TrainingExampleId::from_v7(),
            market_id: market_id.clone(),
            token_id: token_id.clone(),
            as_of,
            sample_source: TrainingSampleSource::ExitDecision,
            feature_vector: FeatureVector {
                market_id,
                token_id: Some(token_id),
                as_of,
                generic_schema_version: SchemaVersion::FIRST,
                generic: BTreeMap::new(),
                domain: None,
                substitutions: Vec::new(),
                data_quality: DataQualityStatus::Fresh,
                staleness_ms: 0,
                source_refs: Vec::new(),
            },
            factor_values: Vec::new(),
            labels: vec![TrainingLabel {
                label_name: HOLD_VS_EXIT_ALPHA_BPS,
                horizon_secs: 0,
                value: dec!(10),
                is_resolved: true,
                matured_at: closed_at,
            }],
            source_refs: Vec::new(),
            lot_context: Some(LotTrainingContext {
                order_intent_id: OrderIntentId::from_v7(),
                position_id,
                remaining_shares: Shares::new(dec!(100)),
                avg_price: Price::new(dec!(0.5)),
                peak_mark: None,
                opened_at: as_of,
                max_hold_secs: 86_400,
            }),
            position_state: Some(PositionStateFeatures {
                unrealized_pnl_pct: dec!(0),
                time_in_trade_ratio: dec!(0.1),
                peak_mark_drawdown: dec!(0),
            }),
            book_fidelity: None,
        }
    }

    /// The core Phase 11.5.1 invariant: every decision point belonging to one
    /// lot (`position_id`) must land in exactly one [`TimelineGroup`] — never
    /// split across groups, or purge/embargo could put one lot's early
    /// decisions in train and its later decisions in test (same-lot leakage).
    #[test]
    fn lot_timeline_groups_keep_one_lots_decisions_in_one_group() {
        let lot_a = PositionId::from_v7();
        let lot_b = PositionId::from_v7();
        let closed_a = ts(200);
        let closed_b = ts(300);
        let examples = vec![
            lot_decision(lot_a.clone(), ts(0), closed_a),
            lot_decision(lot_a.clone(), ts(60), closed_a),
            lot_decision(lot_b.clone(), ts(30), closed_b),
        ];
        let (groups, sequences) = build_lot_timeline_groups(&examples, &label()).expect("groups");
        assert_eq!(groups.len(), 2, "two distinct lots ⇒ two groups");
        assert_eq!(sequences.len(), 2);
        for sequence in &sequences {
            let lot_id = if sequence.position_id == lot_a {
                &lot_a
            } else {
                &lot_b
            };
            assert!(
                sequence.decisions.iter().all(|d| {
                    d.lot_context
                        .as_ref()
                        .is_some_and(|ctx| &ctx.position_id == lot_id)
                }),
                "every decision in a sequence must belong to that sequence's own lot"
            );
        }
        let lot_a_sequence = sequences
            .iter()
            .find(|s| s.position_id == lot_a)
            .expect("lot a sequence");
        assert_eq!(
            lot_a_sequence.decisions.len(),
            2,
            "both of lot a's decisions land in the same sequence"
        );
    }

    #[test]
    fn lot_timeline_groups_are_sorted_ascending_by_as_of() {
        let lot_a = PositionId::from_v7();
        let lot_b = PositionId::from_v7();
        let examples = vec![
            lot_decision(lot_b, ts(100), ts(500)),
            lot_decision(lot_a, ts(0), ts(400)),
        ];
        let (groups, _) = build_lot_timeline_groups(&examples, &label()).expect("groups");
        assert!(
            groups[0].as_of < groups[1].as_of,
            "groups must be ascending by as_of"
        );
    }

    #[test]
    fn selected_label_matured_at_returns_none_for_unlabeled_example() {
        let example = lot_decision(PositionId::from_v7(), ts(0), ts(100));
        let other = LabelSelector {
            name: quant_pivot_research::training::LabelName::new("settlement_outcome"),
            horizon_secs: 0,
        };
        assert!(selected_label_matured_at(&example, &other).is_none());
    }

    #[test]
    fn validate_sell_examples_rejects_missing_position_state() {
        let mut example = lot_decision(PositionId::from_v7(), ts(0), ts(100));
        example.position_state = None;
        assert!(validate_sell_examples(&[example], &label()).is_err());
    }

    #[test]
    fn validate_sell_examples_accepts_well_formed_rows() {
        let example = lot_decision(PositionId::from_v7(), ts(0), ts(100));
        assert!(validate_sell_examples(&[example], &label()).is_ok());
    }

    #[test]
    fn calendarize_sell_path_set_rewrites_lot_returns_and_diagnostics() {
        let lot_a = PositionId::from_v7();
        let lot_b = PositionId::from_v7();
        // Spaced across two 60s buckets so Sharpe/DSR have ≥ 2 periods.
        let examples = vec![
            lot_decision(lot_a, ts(0), ts(100)),
            lot_decision(lot_b, ts(90), ts(190)),
        ];
        let (groups, sequences) = build_lot_timeline_groups(&examples, &label()).expect("groups");
        let template = SellReplayTemplate {
            sequences: Arc::from(sequences),
            policy: SellSignalPolicy {
                min_confidence: dec!(0),
                min_p_exit_better: dec!(0),
                min_expected_alpha_bps: dec!(0),
                max_sell_pct: dec!(1),
            },
            label: label(),
        };
        let mut path_set = BacktestPathSet {
            path_set_id: BacktestPathSetId::from_v7(),
            paths: vec![BacktestPath {
                path_index: 0,
                group_returns: vec![dec!(0.02), dec!(-0.01)],
                sharpe: Decimal::ZERO,
                rank_ic: dec!(0.5),
                max_drawdown: Decimal::ZERO,
                tail_loss: Decimal::ZERO,
            }],
            combination_count: 1,
            sharpe_distribution: SharpeDistribution {
                min: Decimal::ZERO,
                p25: Decimal::ZERO,
                median: Decimal::ZERO,
                p75: Decimal::ZERO,
                max: Decimal::ZERO,
                median_max_drawdown: None,
                median_tail_loss: None,
                baseline_uplift: None,
            },
            median_rank_ic: dec!(0.5),
        };

        calendarize_sell_path_set(&mut path_set, &groups, &template, 60).expect("calendarize");

        assert_eq!(path_set.paths[0].group_returns.len(), 2);
        assert!(
            path_set.sharpe_distribution.median_tail_loss.is_some(),
            "calendarize must persist median_tail_loss"
        );
        assert!(
            path_set.sharpe_distribution.baseline_uplift.is_some(),
            "calendarize must persist baseline_uplift"
        );
        assert_eq!(path_set.median_rank_ic, dec!(0.5));
    }

    #[test]
    fn calendarize_sell_path_set_rejects_single_bucket() {
        let lot_a = PositionId::from_v7();
        let lot_b = PositionId::from_v7();
        // Both lots land in the same 3600s bucket → Sharpe not estimable.
        let examples = vec![
            lot_decision(lot_a, ts(0), ts(100)),
            lot_decision(lot_b, ts(30), ts(130)),
        ];
        let (groups, sequences) = build_lot_timeline_groups(&examples, &label()).expect("groups");
        let template = SellReplayTemplate {
            sequences: Arc::from(sequences),
            policy: SellSignalPolicy {
                min_confidence: dec!(0),
                min_p_exit_better: dec!(0),
                min_expected_alpha_bps: dec!(0),
                max_sell_pct: dec!(1),
            },
            label: label(),
        };
        let mut path_set = BacktestPathSet {
            path_set_id: BacktestPathSetId::from_v7(),
            paths: vec![BacktestPath {
                path_index: 0,
                group_returns: vec![dec!(0.02), dec!(-0.01)],
                sharpe: Decimal::ZERO,
                rank_ic: dec!(0.5),
                max_drawdown: Decimal::ZERO,
                tail_loss: Decimal::ZERO,
            }],
            combination_count: 1,
            sharpe_distribution: SharpeDistribution {
                min: Decimal::ZERO,
                p25: Decimal::ZERO,
                median: Decimal::ZERO,
                p75: Decimal::ZERO,
                max: Decimal::ZERO,
                median_max_drawdown: None,
                median_tail_loss: None,
                baseline_uplift: None,
            },
            median_rank_ic: dec!(0.5),
        };

        let err = calendarize_sell_path_set(&mut path_set, &groups, &template, 3600)
            .expect_err("single activity bucket must fail closed");
        let detail = err.to_string();
        assert!(
            detail.contains("need at least 2 distinct active"),
            "expected activity-observation methodology error, got: {detail}"
        );
    }

    /// Phase 11.5.1 integration: synthetic multi-lot `ExitDecision` fixture →
    /// `SellFoldSource` + `LotReplayBacktester` CPCV → calendarize → diagnostics
    /// → quality-gate input shape. Exercises the full algorithm path without
    /// `ClickHouse` / Postgres (those are covered by governance bind e2e).
    #[test]
    #[allow(clippy::too_many_lines)]
    fn sell_cpcv_pipeline_produces_calendarized_diagnostics_for_gates() {
        use super::{
            FoldReplayEngineAdapter, FoldTemplate, ReplayTemplate, SellFoldTemplate,
            calendarize_sell_path_set, sell_seed_weights,
        };
        use quant_pivot_models::{
            enums::{factor::FactorFamily, quant::FactorDirection},
            types::{ContentHash, FactorDefinitionId, Probability},
        };
        use quant_pivot_research::{
            factors::{FactorExplanation, FactorValue, NormalizedFactor, names},
            gates::{
                CpcvPathSetGateInput, DefaultModelQualityGate, GateId, GateIntent, GateSubject,
                ModelQualityGate, QualityGateInput, QualityGateThresholds,
                SellQualityGateThresholds, ValidationGateThresholds,
            },
            model::{ModelArtifactHeader, ModelFamily, TrainingObjectiveSpec},
            training::DatasetCoverage,
            validation::{
                CombinatorialPurgedBacktester, CpcvConfig, CpcvRequest,
                DefaultCombinatorialPurgedBacktester, PurgeConfig,
            },
        };

        fn hash(seed: u8) -> ContentHash {
            let hex = format!("{seed:02x}{}", "0".repeat(62));
            ContentHash::parse(format!("blake3:{hex}")).expect("hash")
        }

        fn market_factor(score: rust_decimal::Decimal) -> FactorValue {
            FactorValue {
                definition_id: FactorDefinitionId::from_v7(),
                name: names::MOMENTUM_ROC,
                family: FactorFamily::Momentum,
                raw_value: Some(score),
                normalization: NormalizedFactor::cross_section(Probability::new(score)),
                direction: FactorDirection::Positive,
                confidence: Probability::new(dec!(0.9)),
                explanation: FactorExplanation {
                    headline: "t".to_owned(),
                    drivers: Vec::new(),
                },
                input_feature_refs: Vec::new(),
            }
        }

        fn lot_with_signal(
            position_id: &PositionId,
            decision_offsets_secs: &[i64],
            momentum: rust_decimal::Decimal,
            alpha_bps: rust_decimal::Decimal,
        ) -> Vec<TrainingExample> {
            let first = ts(*decision_offsets_secs.first().unwrap_or(&0));
            let closed_at = ts(*decision_offsets_secs.last().unwrap_or(&0) + 3_600);
            let n = decision_offsets_secs.len().max(1) as u64;
            decision_offsets_secs
                .iter()
                .enumerate()
                .map(|(i, &offset)| {
                    let as_of = ts(offset);
                    // Later decisions within a lot get a slightly weaker signal so
                    // rank_pairs are not constant, while same-as_of rows across
                    // lots stay comparable for LTR grouping.
                    let scale = Decimal::from(n - i as u64) / Decimal::from(n);
                    let mut example = lot_decision(position_id.clone(), as_of, closed_at);
                    example.factor_values = vec![market_factor(momentum * scale)];
                    example.position_state = Some(PositionStateFeatures {
                        unrealized_pnl_pct: momentum * dec!(0.5) * scale,
                        time_in_trade_ratio: momentum * scale,
                        peak_mark_drawdown: dec!(0),
                    });
                    example.labels[0].value = alpha_bps * scale;
                    // Keep lot_context.opened_at = first decision so calendar
                    // anchors stay on the lot's first as_of.
                    if let Some(ctx) = example.lot_context.as_mut() {
                        ctx.opened_at = first;
                    }
                    example
                })
                .collect()
        }

        // Non-overlapping lot windows (gap ≫ label horizon) so purge leaves a
        // non-empty train set; paired lots share as_of so LTR simplex sees
        // ≥2-row cross-sections; first-as_of values still span many calendar
        // buckets for Sharpe/DSR.
        //
        //   Pair 0/1: 0h, 0.5h  → closed 1h
        //   Pair 2/3: ~2.8h, 3.3h → closed ~3.8h
        //   Pair 4/5: ~5.6h, 6.1h → closed ~6.6h
        //   Pair 6/7: ~8.3h, 8.8h → closed ~9.3h
        let mut examples = Vec::new();
        let signals = [
            (dec!(0.9), dec!(80)),
            (dec!(0.85), dec!(70)),
            (dec!(0.7), dec!(40)),
            (dec!(0.65), dec!(30)),
            (dec!(0.4), dec!(-10)),
            (dec!(0.35), dec!(-20)),
            (dec!(0.2), dec!(-50)),
            (dec!(0.15), dec!(-60)),
        ];
        let schedules: [[i64; 2]; 8] = [
            [0, 1_800],
            [0, 1_800],
            [10_000, 11_800],
            [10_000, 11_800],
            [20_000, 21_800],
            [20_000, 21_800],
            [30_000, 31_800],
            [30_000, 31_800],
        ];
        for ((momentum, alpha), schedule) in signals.into_iter().zip(schedules) {
            let lot = PositionId::from_v7();
            examples.extend(lot_with_signal(&lot, &schedule, momentum, alpha));
        }

        validate_sell_examples(&examples, &label()).expect("validate");
        let (groups, sequences) = build_lot_timeline_groups(&examples, &label()).expect("groups");
        assert_eq!(groups.len(), 8);

        let sequences: Arc<[LotDecisionSequence]> = Arc::from(sequences);
        let groups_arc: Arc<[TimelineGroup]> = Arc::from(groups);
        let seed_weights = sell_seed_weights(&examples);
        assert!(
            seed_weights.iter().any(|w| w.factor == names::MOMENTUM_ROC),
            "seed weights must cover market factors present on examples"
        );

        let fold_template = FoldTemplate::Sell(SellFoldTemplate {
            sequences: Arc::clone(&sequences),
            label: label(),
            seed_weights,
            header_template: ModelArtifactHeader {
                model_version_id: ModelVersionId::from_v7(),
                model_family: ModelFamily::HoldVsExitWeighted,
                feature_schema_hash: hash(1),
                factor_schema_hash: hash(2),
            },
            base_objective: TrainingObjectiveSpec::default(),
            prediction_horizon_secs: 86_400,
            label_schema_hash: hash(3),
            groups: Arc::clone(&groups_arc),
            purge: PurgeConfig::pct_only(Decimal::ZERO),
        });
        let replay_template = ReplayTemplate::Sell(SellReplayTemplate {
            sequences: Arc::clone(&sequences),
            policy: SellSignalPolicy {
                // Low bars so the scorer's ranking skill is visible in replay
                // returns (policy still shared with live via try_from_runtime).
                min_confidence: dec!(0),
                min_p_exit_better: dec!(0),
                min_expected_alpha_bps: dec!(0),
                max_sell_pct: dec!(1),
            },
            label: label(),
        });

        let fold_source = fold_template.fold_source(None).expect("fold source");
        let replay_engine = FoldReplayEngineAdapter {
            template: &replay_template,
        };
        let mut path_set = DefaultCombinatorialPurgedBacktester::new()
            .run(CpcvRequest {
                path_set_id: BacktestPathSetId::from_v7(),
                groups: &groups_arc,
                cpcv: CpcvConfig {
                    n_groups: 4,
                    k_test: 2,
                },
                purge: PurgeConfig::pct_only(Decimal::ZERO),
                fold_source: fold_source.as_ref(),
                replay: &replay_engine,
            })
            .expect("cpcv");

        // N=4,k=2 → φ = C(3,1) = 3 paths; C(4,2) = 6 combinations.
        assert_eq!(path_set.paths.len(), 3);
        assert_eq!(path_set.combination_count, 6);

        let ReplayTemplate::Sell(sell_replay) = &replay_template else {
            panic!("expected Sell replay template");
        };
        calendarize_sell_path_set(&mut path_set, &groups_arc, sell_replay, 3_600)
            .expect("calendarize");

        let dist = &path_set.sharpe_distribution;
        assert!(
            dist.median_max_drawdown.is_some(),
            "calendarize must persist median_max_drawdown"
        );
        assert!(
            dist.median_tail_loss.is_some(),
            "calendarize must persist median_tail_loss"
        );
        assert!(
            dist.baseline_uplift.is_some(),
            "calendarize must persist baseline_uplift"
        );
        for path in &path_set.paths {
            assert!(
                path.group_returns.len() >= 2,
                "each path must have ≥ 2 calendar buckets after calendarize"
            );
        }

        // Gate consumes the same diagnostic fields governance persists.
        let gate_input = CpcvPathSetGateInput {
            median_rank_ic: path_set.median_rank_ic,
            deflated_sharpe: dec!(0.97), // DSR computed in service finalize; assert shape here
            pbo: dec!(0.20),
            min_track_record_length_secs: Some(86_400),
            median_max_drawdown: dist.median_max_drawdown,
            median_tail_loss: dist.median_tail_loss,
            baseline_uplift: dist.baseline_uplift,
            window_start: Some(groups_arc.first().expect("group").as_of),
            window_end: Some(groups_arc.last().expect("group").label_horizon_end),
        };
        let decision = DefaultModelQualityGate::new()
            .evaluate(QualityGateInput {
                subject: GateSubject::ModelVersion(ModelVersionId::from_v7()),
                intent: GateIntent::Publish,
                backtest: None,
                dataset: DatasetCoverage {
                    exit_decision_built: 990,
                    exit_fill_l2_rows: 900,
                    exit_fill_fallback_rows: 90,
                    planned_samples: 990,
                    built_examples: 990,
                    markets: 8,
                    labels_available: 990,
                    ..Default::default()
                },
                leakage: quant_pivot_research::training::LeakageFindings::default(),
                shadow_stability: Some(Probability::new(dec!(0.80))),
                thresholds: QualityGateThresholds::conservative(),
                validation_thresholds: ValidationGateThresholds::conservative(),
                path_set: Some(gate_input),
                sell_thresholds: SellQualityGateThresholds::default(),
                model_family: Some(ModelFamily::HoldVsExitWeighted),
                return_model_calibrated: false,
                calibration_gate_enabled: true,
            })
            .expect("evaluate");

        let report = decision.report();
        for gate in [
            GateId::CpcvRequired,
            GateId::SellBaselineUplift,
            GateId::MaxDrawdown,
            GateId::TailLossBudget,
            GateId::CalibrationRequired,
        ] {
            assert!(
                report.gates.iter().any(|row| row.gate == gate),
                "Sell publish ledger must include {gate:?}"
            );
        }
        let calibration = report
            .gates
            .iter()
            .find(|row| row.gate == GateId::CalibrationRequired)
            .expect("calibration row");
        assert_eq!(
            calibration.status,
            quant_pivot_research::gates::GateStatus::NotApplicable
        );
    }
}
