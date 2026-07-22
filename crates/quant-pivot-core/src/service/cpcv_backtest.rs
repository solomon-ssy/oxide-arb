//! Combinatorial Purged Cross-Validation + governed trial-grid
//! orchestration for the `WeightedFactor` and Sell (`HoldVsExitWeighted`)
//! model families.
//!
//! Mirrors the crate boundary the single-path [`BacktestService`](crate::service::backtest::BacktestService)
//! and [`ModelTrainerService`](crate::service::model_training::ModelTrainerService)
//! already establish: this service does the **impure** work (dataset load,
//! Parquet decode plus exact frozen-input tick assembly — done exactly **once**,
//! then reused across every CPCV fold and every trial), and delegates every
//! **pure** algorithm (purge/embargo, φ-path reconstruction, DSR/PSR/MinTRL,
//! CSCV/PBO) to [`quant_pivot_research::validation`]. No live `BookStore` is
//! ever touched and no current feature/factor code replaces frozen rows.

use std::{
    collections::{BTreeMap, BTreeSet},
    ops::Range,
    sync::Arc,
};

use async_trait::async_trait;
use chrono::{DateTime, Duration as ChronoDuration, Utc};
use quant_pivot_compute::{ComputeExecutor, OfflineMemory};
use quant_pivot_error::{QuantError, QuantResult, research::ResearchError, storage::StorageError};
use quant_pivot_models::{
    domain::quant::{JobProgressSink, TrainingDatasetInfo},
    enums::{common::MarketCategory, model::ModelFamily, quant::TrainingDatasetStatus},
    runtime_config::{FactorCrossSectionConfig, PortfolioConfig, sections::FactorsConfig},
    types::{
        BacktestPathSetId, BacktestReportId, Bps, ContentHash, DecisionPolicySnapshotId,
        ModelInputContract, ModelRunId, ModelVersionId, PositionId, ResearchJobProgress,
        TrainingDatasetId, TrainingSampleSource,
        backtest::{BacktestPath, SharpeDistribution},
        model_training::TrainingObjectiveSpec,
        stable_name::{FactorName, FeatureName},
    },
};
#[cfg(feature = "ml-classical")]
use quant_pivot_models::{
    enums::model::ClassicalKind,
    types::{ArtifactUri, ModelArtifactId},
};
use quant_pivot_repository::traits::TrainingDatasetRepository;
#[cfg(feature = "ml-classical")]
use quant_pivot_research::model::{
    ClassicalAdapterRegistry, ClassicalModelArtifact, ClassicalParams, ClassicalRuntime,
};
use quant_pivot_research::{
    artifact::ArtifactStore,
    backtest::{
        BacktestInputs, BacktestRequest, BacktestTick, Backtester, CalendarReturn,
        LotBacktestInputs, LotBacktester, LotDecisionSequence, LotOutcome, LotReplayBacktester,
        PortfolioCaps, PortfolioReplayBacktester, SellNullBaseline, active_observation_count,
        calendarize_lot_returns, mean_calendar_return, replay_lot_null_baseline, sharpe_ratio,
    },
    factors::names::{POSITION_PEAK_DRAWDOWN, POSITION_TIME_IN_TRADE, POSITION_UNREALIZED_PNL},
    features::FeatureSchema,
    hashing::ResearchHasher,
    model::{
        CancellationProbe, FactorWeight, LabelSelector, ModelArtifact, ModelArtifactHeader,
        ModelRuntimeInput, ModelRuntimeOutput, ModelTrainer, QuantModelRuntime, ReturnModelSpec,
        ScoreMultiplierSpec, SellScorerOutputSpec, SellScorerRuntime, SellScorerTrainer,
        SellSignalPolicy, SubstitutionConfidenceRules, TrainModelRequest, TrainSellScorerRequest,
        ValidationSpec, WeightedFactorRuntime, WeightedFactorTrainer, WeightedSellScorerRuntime,
    },
    precision::RESEARCH_DECIMAL_SCALE,
    selection::ModelFeatureRequirements,
    stats,
    training::{TrainingExample, TrainingLabel},
    validation::{
        BacktestPathSet, CombinatorialPurgedBacktester, CpcvConfig, CpcvRequest,
        DefaultCombinatorialPurgedBacktester, DsrInput, DsrReport, FoldModelSource, FoldRuntime,
        GroupEvaluation, GroupRowFilter, PboInput, PurgeConfig, RankObservation, ReplayEngine,
        TimelineGroup, Trial, TrialGridSpec, TrialPerformanceMatrix, deflated_sharpe_ratio,
        min_track_record_length, probability_of_backtest_overfitting,
    },
};
use rayon::prelude::*;
use rust_decimal::{Decimal, prelude::ToPrimitive};
use tokio::runtime::Handle;
use tokio_util::sync::CancellationToken;

#[cfg(feature = "ml-classical")]
use crate::service::model_training;
use crate::{
    prefetch::source_slice::SourceSliceReader,
    service::{
        backtest::frozen_ticks,
        historical_replay::ReplayConfig,
        model_training::weighted_seed_weights,
        training_dataset::{require_dataset_materialization, verify_frozen_dataset_artifact},
    },
};

/// Coarse CPCV job progress budget (units of work for `ResearchJobProgress`).
///
/// Progress stages are sequential; each reports `with_total(..., TOTAL)` so the
/// UI can show a determinate percentage plus a named stage.
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

fn ensure_cpcv_not_cancelled(cancel: &CancellationToken, phase: &str) -> QuantResult<()> {
    if cancel.is_cancelled() {
        return Err(ResearchError::Cancelled {
            detail: format!("cpcv backtest cancelled at `{phase}`"),
        }
        .into());
    }
    Ok(())
}

fn cancellation_probe(cancel: &CancellationToken) -> CancellationProbe {
    let cancel = cancel.clone();
    CancellationProbe::new(move || cancel.is_cancelled())
}

struct CancellableFoldSource<'a> {
    inner: &'a dyn FoldModelSource,
    cancel: &'a CancellationToken,
}

impl FoldModelSource for CancellableFoldSource<'_> {
    fn train_fold(&self, filter: &GroupRowFilter) -> QuantResult<FoldRuntime> {
        ensure_cpcv_not_cancelled(self.cancel, "fold train boundary")?;
        let model = self.inner.train_fold(filter)?;
        ensure_cpcv_not_cancelled(self.cancel, "fold train completion")?;
        Ok(model)
    }
}

struct CancellableReplayEngine<'a> {
    inner: &'a dyn ReplayEngine,
    cancel: &'a CancellationToken,
}

impl ReplayEngine for CancellableReplayEngine<'_> {
    fn evaluate(
        &self,
        model: &FoldRuntime,
        filter: &GroupRowFilter,
    ) -> QuantResult<Vec<GroupEvaluation>> {
        ensure_cpcv_not_cancelled(self.cancel, "fold replay boundary")?;
        let evaluations = self.inner.evaluate(model, filter)?;
        ensure_cpcv_not_cancelled(self.cancel, "fold replay completion")?;
        Ok(evaluations)
    }
}

/// Repository + store dependencies.
///
/// This service needs both the trainer's and the backtester's read paths:
/// it mirrors `BacktestServiceDeps` (`crate::service::backtest`) combined
/// with `ModelTrainerServiceDeps` (`crate::service::model_training`).
pub struct CpcvBacktestServiceDeps {
    pub compute: Arc<ComputeExecutor>,
    pub dataset_repo: Arc<dyn TrainingDatasetRepository>,
    pub artifact_store: Arc<dyn ArtifactStore>,
}

/// Governed methodology configuration (`research.validation.*`).
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
    /// Frozen aggressive-entry slippage cap shared with report composition.
    pub entry_max_slippage_bps: Bps,
    /// Governed opportunistic-exit thresholds Sell-side lot replay fires on
    /// — the exact same thresholds
    /// `OpportunisticSellSignalEvaluator` uses live, frozen from
    /// `execution.exit_monitor.opportunistic_sell`, so the CPCV-replayed
    /// decision rule can never drift from production.
    pub sell_policy: SellSignalPolicy,
}

/// A CPCV/trial-grid request resolved by the admin port.
pub struct CpcvBacktestInput {
    /// Real persisted CPCV run id carried into every runtime input.
    pub model_run_id: ModelRunId,
    pub training_dataset_id: TrainingDatasetId,
    pub decision_policy_snapshot_id: DecisionPolicySnapshotId,
    pub label: LabelSelector,
    /// The model family under validation. `WeightedFactor` trains via
    /// [`WeightedFactorTrainer`]; `HoldVsExitWeighted` trains via
    /// [`SellScorerTrainer`] over lot-grouped timelines —
    /// sharing every CPCV/trial-grid/DSR/PBO algorithm with `WeightedFactor`
    /// through [`FoldModelSource`].
    pub model_family: ModelFamily,
    pub prediction_horizon_secs: u64,
    pub category_scope: Option<MarketCategory>,
    /// Ordered raw inputs frozen on the dataset's owning model spec.
    pub input_contract: ModelInputContract,
    /// Pre-assigned path-set id (async job engine); minted when absent.
    pub path_set_id: Option<BacktestPathSetId>,
    /// Audit-only: production `coordinate_search` effective trials (persisted
    /// for operator visibility). **Not** part of DSR N — Bailey's N/V must
    /// describe the same trial population, and V comes only from the governed
    /// trial-grid Sharpe series.
    pub coord_search_effective_n: u32,
}

/// The full validation outcome: CPCV path distribution, the
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
}

impl CpcvBacktestService {
    /// Assemble the service from deps + the frozen replay/portfolio config
    /// (the same `portfolio: &PortfolioConfig` convention
    /// [`crate::service::backtest::BacktestService::new`] uses).
    ///
    /// # Errors
    ///
    /// Returns a typed validation error when any frozen portfolio cap is not a
    /// valid decimal; invalid caps never become a zero budget/cap.
    pub fn new(
        deps: CpcvBacktestServiceDeps,
        config: CpcvBacktestConfig,
        portfolio: &PortfolioConfig,
        replay: ReplayConfig,
    ) -> QuantResult<Self> {
        Ok(Self {
            deps,
            config,
            caps: PortfolioCaps::try_from(portfolio)?,
            replay,
        })
    }

    /// Run CPCV + the governed trial grid, producing the full
    /// validation outcome. Materializes training examples and backtest ticks
    /// **once** over the dataset's frozen window (real `ClickHouse` I/O),
    /// then runs every fold/trial through the process-wide governed offline
    /// executor with no further I/O.
    pub async fn run(
        &self,
        input: CpcvBacktestInput,
        progress: &dyn JobProgressSink,
        cancel: &CancellationToken,
    ) -> QuantResult<CpcvBacktestOutcome> {
        let dataset = self.load_ready_dataset(&input.training_dataset_id).await?;
        let sample_interval_secs = validated_sample_interval_secs(&dataset)?;
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
            .run_cpcv(
                path_set_id,
                &fold_template,
                &replay_template,
                &groups,
                cancel,
            )
            .await?;
        if let (FoldTemplate::Sell(_), ReplayTemplate::Sell(template)) =
            (fold_template.as_ref(), replay_template.as_ref())
        {
            // Collapse coincident lots into an activity-only series (never
            // zero-pad empty wall-clock buckets) before DSR/PBO.
            calendarize_sell_path_set(&mut path_set, &groups, template, sample_interval_secs)?;
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
                sample_interval_secs,
                cancel,
            )
            .await?;

        progress.report(ResearchJobProgress::with_total(
            "finalize",
            CpcvProgress::FINALIZE.start,
            CpcvProgress::TOTAL,
        ));
        let (dsr, pbo, min_track_record_length) = self
            .deps
            .compute
            .run_offline_scoped(OfflineMemory::try_gib(2)?, cancel, || {
                ensure_cpcv_not_cancelled(cancel, "final statistics start")?;
                let (dsr, pbo) = compute_dsr_and_pbo(
                    &dataset,
                    &path_set,
                    &matrix,
                    trial_grid_count,
                    &self.config,
                )?;
                let min_track_record_length = representative_path(&path_set)
                    .map(|path| min_trl_for_path(&dataset, path, &self.config.dsr_significance))
                    .transpose()?
                    .flatten();
                ensure_cpcv_not_cancelled(cancel, "final statistics completion")?;
                Ok((dsr, pbo, min_track_record_length))
            })
            .await?;
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
    /// `WeightedFactor` or a Sell [`FoldTemplate`] — the
    /// **only** family branch point in this service.
    async fn prepare_templates(
        &self,
        dataset: &TrainingDatasetInfo,
        input: &CpcvBacktestInput,
        progress: &dyn JobProgressSink,
        cancel: &CancellationToken,
    ) -> QuantResult<(Arc<FoldTemplate>, Arc<ReplayTemplate>)> {
        let materialization = require_dataset_materialization(dataset)?;
        self.validate_cpcv_input_contract(
            &input.input_contract,
            materialization.feature_schema_hash,
        )?;
        progress.report(ResearchJobProgress::with_total(
            "load",
            CpcvProgress::LOAD.start,
            CpcvProgress::TOTAL,
        ));
        let mut parquet_examples = self.decode_examples(dataset).await?;

        if input.model_family.is_exit_scorer() {
            return self.prepare_sell_templates(dataset, input, parquet_examples, progress, cancel);
        }

        progress.report(ResearchJobProgress::with_total(
            "verify_frozen_examples",
            CpcvProgress::MATERIALIZE_EXAMPLES.start,
            CpcvProgress::TOTAL,
        ));
        parquet_examples.sort_by(|left, right| {
            left.decision_at()
                .cmp(&right.decision_at())
                .then_with(|| left.market_id.as_str().cmp(right.market_id.as_str()))
                .then_with(|| left.token_id.as_str().cmp(right.token_id.as_str()))
        });
        let examples: Arc<[TrainingExample]> = parquet_examples.into();

        let groups: Arc<[TimelineGroup]> = build_timeline_groups(&examples, &input.label)?.into();
        let group_example_ranges: Arc<[Range<usize>]> =
            build_group_example_ranges(&examples, &groups)?.into();
        // Fail before the expensive CPCV fold loop when the timeline cannot
        // support CSCV/PBO (T < block_count).
        let pbo_block_count = validated_pbo_block_count(self.config.pbo.block_count)?;
        if groups.len() < pbo_block_count {
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
            model_spec_definition_hash: dataset
                .manifest_json
                .as_ref()
                .ok_or_else(|| ResearchError::ValidationMethodology {
                    detail: "v5 CPCV dataset is missing its manifest".to_owned(),
                })?
                .model_spec_definition_hash,
            profile_ref: dataset
                .manifest_json
                .as_ref()
                .ok_or_else(|| ResearchError::ValidationMethodology {
                    detail: "v5 CPCV dataset is missing its manifest".to_owned(),
                })?
                .profile_ref
                .clone(),
            model_family: input.model_family,
            feature_schema_hash: *materialization.feature_schema_hash,
            factor_schema_hash: *materialization.factor_schema_hash,
            trade_policy_artifact_id: dataset
                .manifest_json
                .as_ref()
                .and_then(|manifest| manifest.trade_policy_artifact_id),
            trade_policy_hash: dataset
                .manifest_json
                .as_ref()
                .and_then(|manifest| manifest.trade_policy_hash),
        };
        progress.report(ResearchJobProgress::with_total(
            "frozen_input_ticks",
            CpcvProgress::MATERIALIZE_TICKS.start,
            CpcvProgress::TOTAL,
        ));
        let probe_runtime = ProbeRuntime {
            model_family: header_template.model_family,
            feature_schema_hash: header_template.feature_schema_hash,
            input_contract: input.input_contract.clone(),
        };
        let source_slice = dataset
            .manifest_json
            .as_ref()
            .ok_or_else(|| ResearchError::DatasetBuild {
                detail: "CPCV dataset has no immutable v1 manifest".to_owned(),
            })?
            .source_slice
            .clone();
        let frozen_source = SourceSliceReader::new(Arc::clone(&self.deps.artifact_store))
            .read_ref(&source_slice)
            .await?;
        let ticks = frozen_ticks(
            &examples,
            &frozen_source,
            self.config.entry_max_slippage_bps,
            &probe_runtime,
            &input.model_run_id,
            cancel,
            progress,
        )?;
        let Some(ticks) = ticks else {
            return Err(ResearchError::Cancelled {
                detail: "cpcv backtest cancelled during tick materialization".to_owned(),
            }
            .into());
        };
        let ticks_by_as_of: Arc<BTreeMap<DateTime<Utc>, BacktestTick>> = Arc::new(
            ticks
                .into_iter()
                .map(|tick| (tick.decision_at, tick))
                .collect(),
        );
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
        let fold_build = FoldTemplateBuild {
            examples,
            group_example_ranges,
            header_template,
            groups,
            handle,
            cancellation: cancellation_probe(cancel),
        };
        #[cfg(feature = "ml-classical")]
        let fold_template = self.build_fold_template(dataset, input, fold_build)?;
        #[cfg(not(feature = "ml-classical"))]
        let fold_template = self.build_fold_template(dataset, input, fold_build);
        Ok((fold_template, replay_template))
    }

    fn validate_cpcv_input_contract(
        &self,
        input_contract: &ModelInputContract,
        frozen_feature_schema_hash: &ContentHash,
    ) -> QuantResult<()> {
        input_contract.validate().map_err(|detail| {
            QuantError::from(ResearchError::ValidationMethodology {
                detail: format!("invalid CPCV model input contract: {detail}"),
            })
        })?;
        if input_contract.inputs.is_empty() {
            return Err(ResearchError::ValidationMethodology {
                detail: "CPCV model input contract must contain at least one raw feature"
                    .to_owned(),
            }
            .into());
        }
        let feature_schema = FeatureSchema::build(&self.replay.features)?;
        let feature_schema_hash = ResearchHasher::feature_schema(&feature_schema)?;
        if &feature_schema_hash != frozen_feature_schema_hash {
            return Err(ResearchError::ValidationMethodology {
                detail: format!(
                    "CPCV feature schema {feature_schema_hash} differs from frozen dataset \
                     {frozen_feature_schema_hash}"
                ),
            }
            .into());
        }
        if let Some(unknown) = input_contract
            .inputs
            .iter()
            .find(|raw| !feature_schema.contains(&FeatureName::new(raw.feature_name.clone())))
        {
            return Err(ResearchError::ValidationMethodology {
                detail: format!(
                    "CPCV input contract references unknown feature `{}`",
                    unknown.feature_name
                ),
            }
            .into());
        }
        Ok(())
    }

    /// Build the family-specific [`FoldTemplate`] shared across CPCV folds and trials.
    ///
    /// Classical availability is gated in [`Self::prepare_templates`] so this
    /// helper stays a pure constructor (no `Result`) under both feature sets.
    #[cfg(feature = "ml-classical")]
    fn build_fold_template(
        &self,
        dataset: &TrainingDatasetInfo,
        input: &CpcvBacktestInput,
        build: FoldTemplateBuild,
    ) -> QuantResult<Arc<FoldTemplate>> {
        let FoldTemplateBuild {
            examples,
            group_example_ranges,
            header_template,
            groups,
            handle,
            cancellation,
        } = build;
        if let Some(kind) = input.model_family.classical_kind() {
            let materialization = require_dataset_materialization(dataset)?;
            return Ok(Arc::new(FoldTemplate::Classical(ClassicalFoldTemplate {
                examples,
                group_example_ranges,
                label: input.label.clone(),
                input_contract: Arc::new(input.input_contract.clone()),
                header_template,
                kind,
                schema: Arc::new(FeatureSchema::build(&self.replay.features)?),
                label_schema_hash: *materialization.label_schema_hash,
                training_dataset_hash: *materialization.dataset_hash,
                prediction_horizon_secs: input.prediction_horizon_secs,
                groups,
            })));
        }
        let seed_weights = weighted_seed_weights(&self.config.factors, &examples);
        Ok(Arc::new(FoldTemplate::WeightedFactor(FoldTrainTemplate {
            examples,
            group_example_ranges,
            label: input.label.clone(),
            seed_weights,
            header_template,
            base_objective: self.config.objective.clone(),
            prediction_horizon_secs: input.prediction_horizon_secs,
            factor_cross_section: self.config.factors.cross_section.clone(),
            category_scope: input.category_scope,
            input_contract: Arc::new(input.input_contract.clone()),
            groups,
            purge: self.config.purge,
            handle,
            cancellation,
        })))
    }

    #[cfg(not(feature = "ml-classical"))]
    fn build_fold_template(
        &self,
        _dataset: &TrainingDatasetInfo,
        input: &CpcvBacktestInput,
        build: FoldTemplateBuild,
    ) -> Arc<FoldTemplate> {
        let FoldTemplateBuild {
            examples,
            group_example_ranges,
            header_template,
            groups,
            handle,
            cancellation,
        } = build;
        let seed_weights = weighted_seed_weights(&self.config.factors, &examples);
        Arc::new(FoldTemplate::WeightedFactor(FoldTrainTemplate {
            examples,
            group_example_ranges,
            label: input.label.clone(),
            seed_weights,
            header_template,
            base_objective: self.config.objective.clone(),
            prediction_horizon_secs: input.prediction_horizon_secs,
            factor_cross_section: self.config.factors.cross_section.clone(),
            category_scope: input.category_scope,
            input_contract: Arc::new(input.input_contract.clone()),
            groups,
            purge: self.config.purge,
            handle,
            cancellation,
        }))
    }

    /// Build the Sell-side (`HoldVsExitWeighted`) fold and replay
    /// templates: lot-grouped timeline, lot-decision sequences shared
    /// by training and replay, no ticks (Sell replay never touches the
    /// cross-sectional allocator).
    fn prepare_sell_templates(
        &self,
        dataset: &TrainingDatasetInfo,
        input: &CpcvBacktestInput,
        parquet_examples: Vec<TrainingExample>,
        progress: &dyn JobProgressSink,
        cancel: &CancellationToken,
    ) -> QuantResult<(Arc<FoldTemplate>, Arc<ReplayTemplate>)> {
        let materialization = require_dataset_materialization(dataset)?;
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
            "verify_frozen_exit_decisions",
            CpcvProgress::MATERIALIZE_EXAMPLES.start,
            CpcvProgress::TOTAL,
        ));
        let examples = parquet_examples;
        validate_sell_examples(&examples, &input.label)?;

        let (groups, sequences) = build_lot_timeline_groups(&examples, &input.label)?;
        // DSR/PBO consume the *activity-only* lot-native series (coincident
        // lots may collapse into one observation; empty wall-clock buckets are
        // never invented). Gate on that effective observation count — not raw
        // lot count and not zero-padded calendar span.
        let period_secs = validated_sample_interval_secs(dataset)?;
        let probe_outcomes: Vec<LotOutcome> = groups
            .iter()
            .map(|group| LotOutcome {
                position_id: PositionId::from_v7(),
                decision_at: group.decision_at,
                return_value: Decimal::ZERO,
                cumulative_exit_pct: Decimal::ZERO,
                rank_pairs: Vec::new(),
                path_diverged: false,
            })
            .collect();
        let active_obs = active_observation_count(&probe_outcomes, period_secs);
        let pbo_block_count = validated_pbo_block_count(self.config.pbo.block_count)?;
        if active_obs < pbo_block_count {
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
        let seed_weights = sell_seed_weights(&examples)?;
        let header_template = ModelArtifactHeader {
            model_version_id: ModelVersionId::from_v7(),
            model_spec_definition_hash: dataset
                .manifest_json
                .as_ref()
                .ok_or_else(|| ResearchError::ValidationMethodology {
                    detail: "v5 sell CPCV dataset is missing its manifest".to_owned(),
                })?
                .model_spec_definition_hash,
            profile_ref: dataset
                .manifest_json
                .as_ref()
                .ok_or_else(|| ResearchError::ValidationMethodology {
                    detail: "v5 sell CPCV dataset is missing its manifest".to_owned(),
                })?
                .profile_ref
                .clone(),
            model_family: input.model_family,
            feature_schema_hash: *materialization.feature_schema_hash,
            factor_schema_hash: *materialization.factor_schema_hash,
            trade_policy_artifact_id: dataset
                .manifest_json
                .as_ref()
                .and_then(|manifest| manifest.trade_policy_artifact_id),
            trade_policy_hash: dataset
                .manifest_json
                .as_ref()
                .and_then(|manifest| manifest.trade_policy_hash),
        };
        let fold_template = Arc::new(FoldTemplate::Sell(SellFoldTemplate {
            sequences: Arc::clone(&sequences),
            label: input.label.clone(),
            seed_weights,
            header_template,
            base_objective: self.config.objective.clone(),
            prediction_horizon_secs: input.prediction_horizon_secs,
            label_schema_hash: *materialization.label_schema_hash,
            input_contract: Arc::new(input.input_contract.clone()),
            groups: Arc::clone(&groups),
            purge: self.config.purge,
            cancellation: cancellation_probe(cancel),
        }));
        let replay_template = Arc::new(ReplayTemplate::Sell(SellReplayTemplate {
            sequences,
            policy: self.config.sell_policy,
            label: input.label.clone(),
        }));
        Ok((fold_template, replay_template))
    }

    /// Run the CPCV fold sweep in the offline pool (rayon-parallel across
    /// combinations internally).
    async fn run_cpcv(
        &self,
        path_set_id: BacktestPathSetId,
        fold_template: &Arc<FoldTemplate>,
        replay_template: &Arc<ReplayTemplate>,
        groups: &Arc<[TimelineGroup]>,
        cancel: &CancellationToken,
    ) -> QuantResult<BacktestPathSet> {
        let fold_template = Arc::clone(fold_template);
        let replay_template = Arc::clone(replay_template);
        let groups = Arc::clone(groups);
        let cpcv_config = self.config.cpcv;
        let purge_config = self.config.purge;
        let cancellation = cancel.clone();
        self.deps
            .compute
            .run_offline_cancellable(OfflineMemory::try_gib(6)?, cancel, move || {
                ensure_cpcv_not_cancelled(&cancellation, "fold sweep start")?;
                let fold_source = fold_template.fold_source(None)?;
                let replay_engine = FoldReplayEngineAdapter {
                    template: &replay_template,
                };
                let fold_source = CancellableFoldSource {
                    inner: fold_source.as_ref(),
                    cancel: &cancellation,
                };
                let replay_engine = CancellableReplayEngine {
                    inner: &replay_engine,
                    cancel: &cancellation,
                };
                let path_set = DefaultCombinatorialPurgedBacktester::new().run(CpcvRequest {
                    path_set_id,
                    groups: &groups,
                    cpcv: cpcv_config,
                    purge: purge_config,
                    fold_source: &fold_source,
                    replay: &replay_engine,
                })?;
                ensure_cpcv_not_cancelled(&cancellation, "fold sweep completion")?;
                Ok(path_set)
            })
            .await
    }

    /// Run the governed trial grid in the offline pool, returning the
    /// resulting performance matrix + trial count.
    async fn run_trials(
        &self,
        fold_template: &Arc<FoldTemplate>,
        replay_template: &Arc<ReplayTemplate>,
        groups: &Arc<[TimelineGroup]>,
        sample_interval_secs: u64,
        cancel: &CancellationToken,
    ) -> QuantResult<(TrialPerformanceMatrix, u32)> {
        let trials = self.config.trials.generate(&self.config.objective)?;
        let trial_count =
            u32::try_from(trials.len()).map_err(|error| ResearchError::ValidationMethodology {
                detail: format!("governed trial count does not fit u32: {error}"),
            })?;
        let fold_template = Arc::clone(fold_template);
        let replay_template = Arc::clone(replay_template);
        let groups = Arc::clone(groups);
        let cancellation = cancel.clone();
        let matrix = self
            .deps
            .compute
            .run_offline_cancellable(OfflineMemory::try_gib(6)?, cancel, move || {
                ensure_cpcv_not_cancelled(&cancellation, "trial grid start")?;
                let matrix = run_trial_grid(
                    &trials,
                    &fold_template,
                    &replay_template,
                    &groups,
                    sample_interval_secs,
                    &cancellation,
                )?;
                ensure_cpcv_not_cancelled(&cancellation, "trial grid completion")?;
                Ok(matrix)
            })
            .await?;
        Ok((matrix, trial_count))
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
        if dataset.status != TrainingDatasetStatus::Ready {
            return Err(ResearchError::DatasetBuild {
                detail: format!(
                    "cpcv backtest requires a Ready dataset, got {}",
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
        let materialization = require_dataset_materialization(dataset)?;
        let bytes = self
            .deps
            .artifact_store
            .get(materialization.parquet_uri)
            .await?;
        verify_frozen_dataset_artifact(dataset, &bytes)
    }
}

fn validated_sample_interval_secs(dataset: &TrainingDatasetInfo) -> QuantResult<u64> {
    let value = u64::try_from(dataset.sample_interval_secs).map_err(|error| {
        ResearchError::DatasetBuild {
            detail: format!("dataset sample_interval_secs is invalid: {error}"),
        }
    })?;
    if value == 0 {
        return Err(ResearchError::DatasetBuild {
            detail: "dataset sample_interval_secs must be positive".to_owned(),
        }
        .into());
    }
    Ok(value)
}

fn validated_pbo_block_count(block_count: u32) -> QuantResult<usize> {
    usize::try_from(block_count).map_err(|error| {
        ResearchError::ValidationMethodology {
            detail: format!("pbo.block_count does not fit usize: {error}"),
        }
        .into()
    })
}

fn validated_period_length(dataset: &TrainingDatasetInfo) -> QuantResult<ChronoDuration> {
    validated_sample_interval_secs(dataset)?;
    Ok(ChronoDuration::seconds(dataset.sample_interval_secs))
}

/// A minimal, never-scored [`QuantModelRuntime`] used only to determine
/// [`quant_pivot_research::model::ModelRuntimeInput`]'s shape during tick
/// materialization (`build_runtime_input` consults the family and typed input
/// contract projections only — no inference). Every real CPCV fold and
/// trial builds and scores its own freshly trained runtime instead.
struct ProbeRuntime {
    model_family: ModelFamily,
    feature_schema_hash: ContentHash,
    input_contract: ModelInputContract,
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
        self.feature_schema_hash
    }

    fn required_features(&self) -> Vec<FeatureName> {
        ModelFeatureRequirements::from_input_contract(&self.input_contract).generic
    }

    fn input_features(&self) -> Vec<FeatureName> {
        self.input_contract
            .inputs
            .iter()
            .map(|input| FeatureName::new(input.feature_name.clone()))
            .collect()
    }

    async fn infer_batch(&self, _input: ModelRuntimeInput) -> QuantResult<ModelRuntimeOutput> {
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
        let matching_label = |row: &&TrainingLabel| {
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
            .entry(example.decision_at())
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
        .map(|(decision_at, label_horizon_end)| TimelineGroup {
            decision_at,
            label_horizon_end,
        })
        .collect())
}

fn build_group_example_ranges(
    examples: &[TrainingExample],
    groups: &[TimelineGroup],
) -> QuantResult<Vec<Range<usize>>> {
    let mut ranges = Vec::with_capacity(groups.len());
    let mut cursor = 0usize;
    for group in groups {
        while examples
            .get(cursor)
            .is_some_and(|example| example.decision_at() < group.decision_at)
        {
            cursor += 1;
        }
        let start = cursor;
        while examples
            .get(cursor)
            .is_some_and(|example| example.decision_at() == group.decision_at)
        {
            cursor += 1;
        }
        if start == cursor {
            return Err(ResearchError::ValidationMethodology {
                detail: format!(
                    "CPCV timeline group {} has no index-aligned training examples",
                    group.decision_at
                ),
            }
            .into());
        }
        ranges.push(start..cursor);
    }
    Ok(ranges)
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
/// (the core same-lot isolation invariant). `as_of` is the lot's earliest
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
                group.decision_at = group.decision_at.min(example.decision_at());
                group.label_horizon_end = group.label_horizon_end.max(matured_at);
                decisions.push(example.clone());
            })
            .or_insert_with(|| {
                (
                    ctx.position_id,
                    TimelineGroup {
                        decision_at: example.decision_at(),
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
    ordered.sort_by_key(|(_, group, _)| group.decision_at);

    let mut groups = Vec::with_capacity(ordered.len());
    let mut sequences = Vec::with_capacity(ordered.len());
    for (position_id, group, mut decisions) in ordered {
        decisions.sort_by_key(TrainingExample::decision_at);
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
fn sell_seed_weights(examples: &[TrainingExample]) -> QuantResult<Vec<FactorWeight>> {
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
    let count =
        u64::try_from(names.len()).map_err(|error| ResearchError::ValidationMethodology {
            detail: format!("Sell factor count does not fit u64: {error}"),
        })?;
    if count == 0 {
        return Err(ResearchError::ValidationMethodology {
            detail: "Sell factor set must not be empty".to_owned(),
        }
        .into());
    }
    let weight = Decimal::ONE / Decimal::from(count);
    Ok(names
        .into_iter()
        .map(|name| FactorWeight {
            factor: FactorName::new(name),
            weight,
        })
        .collect())
}

struct FoldTemplateBuild {
    examples: Arc<[TrainingExample]>,
    group_example_ranges: Arc<[Range<usize>]>,
    header_template: ModelArtifactHeader,
    groups: Arc<[TimelineGroup]>,
    handle: Handle,
    cancellation: CancellationProbe,
}

/// Everything a [`FoldModelSource`] needs to train a `WeightedFactor` fold.
/// `handle` lets [`WeightedFactorFoldSource::train_fold`] block on the async
/// trainer from inside a rayon worker thread (rayon threads carry no tokio
/// runtime context, so `Handle::current` would panic there — the handle
/// must be captured once, from the original async caller, and threaded
/// through explicitly).
struct FoldTrainTemplate {
    examples: Arc<[TrainingExample]>,
    group_example_ranges: Arc<[Range<usize>]>,
    label: LabelSelector,
    seed_weights: Vec<FactorWeight>,
    header_template: ModelArtifactHeader,
    /// The governed `research.training.*` objective every CPCV fold trains
    /// against; a trial-grid trial overrides this per trial (never per fold).
    base_objective: TrainingObjectiveSpec,
    prediction_horizon_secs: u64,
    factor_cross_section: FactorCrossSectionConfig,
    category_scope: Option<MarketCategory>,
    input_contract: Arc<ModelInputContract>,
    groups: Arc<[TimelineGroup]>,
    /// Same purge/embargo as the outer CPCV run (trainer nested CV).
    purge: PurgeConfig,
    handle: Handle,
    cancellation: CancellationProbe,
}

/// Everything a Sell-side [`FoldModelSource`] needs to train a lot-grouped
/// fold. `sequences` is index-aligned with `groups` and is the
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
    input_contract: Arc<ModelInputContract>,
    groups: Arc<[TimelineGroup]>,
    /// Same purge/embargo as the outer CPCV run (trainer nested CV).
    purge: PurgeConfig,
    cancellation: CancellationProbe,
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
                // verbatim: `SellScorerTrainer::train_sell_scorer`
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

fn validated_group_indices(filter: &GroupRowFilter, range_count: usize) -> QuantResult<&[usize]> {
    if filter
        .group_indices
        .windows(2)
        .any(|window| window[0] >= window[1])
    {
        return Err(ResearchError::ValidationMethodology {
            detail: "CPCV group filter must be strictly ascending and unique".to_owned(),
        }
        .into());
    }
    if let Some(&index) = filter
        .group_indices
        .last()
        .filter(|&&index| index >= range_count)
    {
        return Err(ResearchError::ValidationMethodology {
            detail: format!(
                "CPCV group index {index} exceeds {range_count} precomputed example ranges"
            ),
        }
        .into());
    }
    Ok(&filter.group_indices)
}

impl FoldModelSource for WeightedFactorFoldSource<'_> {
    fn train_fold(&self, filter: &GroupRowFilter) -> QuantResult<FoldRuntime> {
        let group_indices =
            validated_group_indices(filter, self.template.group_example_ranges.len())?;
        let capacity = group_indices
            .iter()
            .map(|&index| self.template.group_example_ranges[index].len())
            .sum();
        let mut fold_examples = Vec::with_capacity(capacity);
        for &index in group_indices {
            let range = &self.template.group_example_ranges[index];
            fold_examples.extend_from_slice(&self.template.examples[range.clone()]);
        }
        let training_dataset_hash = ResearchHasher::canonical(&fold_examples)?;

        let mut header = self.template.header_template.clone();
        header.model_version_id = ModelVersionId::from_v7();
        let request = TrainModelRequest {
            cancellation: self.template.cancellation.clone(),
            examples: fold_examples.into(),
            training_dataset_hash,
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
            input_contract: self.template.input_contract.as_ref().clone(),
            factor_cross_section: self.template.factor_cross_section.clone(),
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

/// [`FoldModelSource`] for the Sell-side `HoldVsExitWeighted` family: filters
/// `template.sequences` down to `filter`'s lots, then
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
        let training_dataset_hash = ResearchHasher::canonical(&fold_examples)?;

        let mut header = self.template.header_template.clone();
        header.model_version_id = ModelVersionId::from_v7();
        let request = TrainSellScorerRequest {
            cancellation: self.template.cancellation.clone(),
            examples: fold_examples.into(),
            training_dataset_hash,
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
            label_schema_hash: self.template.label_schema_hash,
            input_contract: self.template.input_contract.as_ref().clone(),
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

/// Everything a [`ClassicalFoldSource`] needs to train a classical-ML fold.
/// Classical training
/// ([`ClassicalAdapterRegistry::adapter_for`]) is fully synchronous CPU work
/// (`smartcore`, no async I/O), so unlike [`FoldTrainTemplate`] this needs no
/// [`Handle`] to cross the rayon-thread boundary.
#[cfg(feature = "ml-classical")]
struct ClassicalFoldTemplate {
    examples: Arc<[TrainingExample]>,
    group_example_ranges: Arc<[Range<usize>]>,
    label: LabelSelector,
    input_contract: Arc<ModelInputContract>,
    header_template: ModelArtifactHeader,
    kind: ClassicalKind,
    schema: Arc<FeatureSchema>,
    label_schema_hash: ContentHash,
    training_dataset_hash: ContentHash,
    prediction_horizon_secs: u64,
    groups: Arc<[TimelineGroup]>,
}

/// [`FoldModelSource`] for classical-ML families: filters `template`'s
/// examples down to `filter`'s groups' `as_of`s, builds the governed
/// [`quant_pivot_research::training::TrainingMatrix`], and fits via the exact
/// same [`quant_pivot_research::model::ClassicalAdapterRegistry`] production
/// training uses — with `params_override` from the governed classical trial
/// grid in place of the production defaults when set.
#[cfg(feature = "ml-classical")]
struct ClassicalFoldSource<'a> {
    template: &'a ClassicalFoldTemplate,
    params_override: Option<&'a ClassicalParams>,
}

#[cfg(feature = "ml-classical")]
impl FoldModelSource for ClassicalFoldSource<'_> {
    fn train_fold(&self, filter: &GroupRowFilter) -> QuantResult<FoldRuntime> {
        let group_indices =
            validated_group_indices(filter, self.template.group_example_ranges.len())?;
        let matrix = model_training::build_classical_matrix(
            group_indices.iter().flat_map(|&index| {
                self.template.examples[self.template.group_example_ranges[index].clone()].iter()
            }),
            &self.template.label,
            &self.template.schema,
            &self.template.input_contract,
        )?;
        let adapter = self.params_override.map_or_else(
            || ClassicalAdapterRegistry::adapter_for(self.template.kind),
            |params| ClassicalAdapterRegistry::adapter_with_params(self.template.kind, *params),
        );
        let output = adapter.train(&matrix)?;
        if output.input_contract != *self.template.input_contract {
            return Err(ResearchError::ValidationMethodology {
                detail: "classical fold input contract differs from its owning model spec"
                    .to_owned(),
            }
            .into());
        }

        let mut header = self.template.header_template.clone();
        header.model_version_id = ModelVersionId::from_v7();
        let output_semantics = model_training::classical_output_semantics(
            self.template.kind,
            &self.template.label,
            self.template.prediction_horizon_secs,
        )?;
        let artifact = ClassicalModelArtifact {
            header,
            artifact_id: ModelArtifactId::from_v7(),
            kind: self.template.kind,
            crate_name: output.crate_name.clone(),
            crate_version: output.crate_version.clone(),
            label_schema_hash: self.template.label_schema_hash,
            training_dataset_hash: self.template.training_dataset_hash,
            prediction_horizon_secs: self.template.prediction_horizon_secs,
            output_semantics,
            multipliers: ScoreMultiplierSpec::conservative(),
            substitution_confidence_rules: SubstitutionConfidenceRules::conservative(),
            input_contract: output.input_contract.clone(),
            input_contract_hash: output.input_contract_hash,
            input_transform_hash: output.input_transform_hash,
            training_input_hash: output.training_input_hash,
            // Ephemeral validation fold: never persisted to the artifact
            // store, so this URI is never dereferenced — `ClassicalRuntime::load`
            // takes the estimator bytes directly, below.
            serialized_model_uri: ArtifactUri::parse("memory://cpcv-ephemeral-fold").map_err(
                |error| ResearchError::ValidationMethodology {
                    detail: format!("ephemeral fold artifact URI failed to parse: {error}"),
                },
            )?,
            serialized_model_hash: output.model_bytes_hash,
            serialization_format: output.serialization_format,
            input_transform: output.input_transform.clone(),
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
                .get(&template.groups[idx].decision_at)
                .cloned()
        })
        .collect();
    ticks.sort_by_key(|tick| tick.decision_at);
    // Distinguish an explicitly materialized no-trade/no-fill tick (zero
    // strategy PnL) from a missing replay tick (methodology failure). This is
    // calendar-time CPCV, so silently dropping flat periods would inflate the
    // activity-conditioned Sharpe distribution.
    let mut by_as_of = ticks
        .iter()
        .map(|tick| (tick.decision_at, TickAccumulator::default()))
        .collect::<BTreeMap<_, _>>();

    let request = BacktestRequest {
        backtest_report_id: BacktestReportId::from_v7(),
        model_version_id: model.model_version_id(),
        decision_policy_snapshot_id: DecisionPolicySnapshotId::from_v7(),
        window_start: ticks.first().map_or_else(Utc::now, |tick| tick.decision_at),
        window_end: ticks.last().map_or_else(Utc::now, |tick| tick.decision_at),
    };
    let result = template
        .handle
        .block_on(PortfolioReplayBacktester::new().run(BacktestInputs {
            request,
            model,
            ticks,
            caps: template.caps.clone(),
        }))?;

    for sample in &result.sample_outcomes {
        let entry = by_as_of.entry(sample.decision_at).or_default();
        entry.allocated += sample.allocated_usd.inner();
        entry.pnl +=
            sample.allocated_usd.inner() * sample.realized_return_bps / Decimal::from(10_000);
        entry.scores.push(sample.composite_score.inner());
        entry.realized_bps.push(sample.realized_return_bps);
    }

    let mut evaluations = Vec::with_capacity(filter.group_indices.len());
    for &group_index in &filter.group_indices {
        let as_of = template.groups[group_index].decision_at;
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

/// Scores the model over `filter`'s lots via [`LotReplayBacktester`], then
/// derives one [`GroupEvaluation`] per lot from the
/// resulting outcomes. Lot-level `return_value` stays the on-path residual-shares
/// alpha; activity-only collapse for Sharpe/DSR happens after φ-path
/// reconstruction ([`calendarize_sell_path_set`]).
fn evaluate_sell_groups(
    template: &SellReplayTemplate,
    model: &dyn SellScorerRuntime,
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
    cancel: &CancellationToken,
) -> QuantResult<TrialPerformanceMatrix> {
    let all_indices = GroupRowFilter {
        group_indices: (0..groups.len()).collect(),
    };
    let replay_engine = FoldReplayEngineAdapter {
        template: replay_template,
    };
    let calendarize_sell = matches!(replay_template, ReplayTemplate::Sell(_));
    if sample_interval_secs == 0 {
        return Err(ResearchError::ValidationMethodology {
            detail: "trial-grid sample_interval_secs must be positive".to_owned(),
        }
        .into());
    }
    let period_secs = sample_interval_secs;

    let columns: Vec<QuantResult<Vec<Decimal>>> = trials
        .par_iter()
        .map(|trial| -> QuantResult<Vec<Decimal>> {
            ensure_cpcv_not_cancelled(cancel, "trial train boundary")?;
            let fold_source = fold_template.fold_source(Some(trial))?;
            let model = fold_source.train_fold(&all_indices)?;
            ensure_cpcv_not_cancelled(cancel, "trial replay boundary")?;
            let evaluations = replay_engine.evaluate(&model, &all_indices)?;
            let mut by_group = vec![None; groups.len()];
            for evaluation in evaluations {
                let slot = by_group.get_mut(evaluation.group_index).ok_or_else(|| {
                    ResearchError::ValidationMethodology {
                        detail: format!(
                            "trial-grid replay returned out-of-range group_index={}",
                            evaluation.group_index
                        ),
                    }
                })?;
                if slot.replace(evaluation.return_value).is_some() {
                    return Err(ResearchError::ValidationMethodology {
                        detail: format!(
                            "trial-grid replay returned duplicate group_index={}",
                            evaluation.group_index
                        ),
                    }
                    .into());
                }
            }
            // Fail-closed: never invent zero returns for missing groups (same
            // invariant as CPCV fold replay). Silent zeros would bias PBO/DSR V.
            let mut column = Vec::with_capacity(groups.len());
            for (idx, return_value) in by_group.into_iter().enumerate() {
                ensure_cpcv_not_cancelled(cancel, "trial group boundary")?;
                let Some(return_value) = return_value else {
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
        groups.iter().map(|group| group.decision_at).collect()
    };

    TrialPerformanceMatrix::from_columns(periods, &trial_returns)
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
        path.tail_loss = tail_loss_from_returns(&path.group_returns, Decimal::new(10, 2))?;
    }

    // Hard gate baseline is exit-at-first (non-trivial null). AlwaysHold is
    // definitionally zero alpha and is covered by unit tests / offline compare,
    // not persisted into SharpeDistribution.
    let baseline_uplift = median_decimal(&path_mean_returns)?
        - mean_calendar_return(&baseline_calendar_returns(
            template,
            SellNullBaseline::ExitAtFirstDecision,
            period_secs,
        )?);
    path_set.sharpe_distribution = sharpe_distribution_with_diagnostics(
        &path_set.paths,
        Some(baseline_uplift.round_dp(RESEARCH_DECIMAL_SCALE)),
    )?;
    Ok(())
}

fn synthetic_lot_outcomes(groups: &[TimelineGroup], returns: &[Decimal]) -> Vec<LotOutcome> {
    groups
        .iter()
        .zip(returns)
        .map(|(group, &return_value)| LotOutcome {
            position_id: PositionId::from_v7(),
            decision_at: group.decision_at,
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
) -> QuantResult<SharpeDistribution> {
    let mut sharpes: Vec<Decimal> = paths.iter().map(|path| path.sharpe).collect();
    sharpes.sort();
    let mut max_drawdowns: Vec<Decimal> = paths.iter().map(|path| path.max_drawdown).collect();
    max_drawdowns.sort();
    let mut tail_losses: Vec<Decimal> = paths.iter().map(|path| path.tail_loss).collect();
    tail_losses.sort();
    Ok(SharpeDistribution {
        min: percentile(&sharpes, Decimal::ZERO)?,
        p25: percentile(&sharpes, Decimal::new(25, 2))?,
        median: percentile(&sharpes, Decimal::new(5, 1))?,
        p75: percentile(&sharpes, Decimal::new(75, 2))?,
        max: percentile(&sharpes, Decimal::ONE)?,
        median_max_drawdown: Some(percentile(&max_drawdowns, Decimal::new(5, 1))?),
        median_tail_loss: Some(percentile(&tail_losses, Decimal::new(5, 1))?),
        baseline_uplift,
    })
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

fn tail_loss_from_returns(returns: &[Decimal], quantile: Decimal) -> QuantResult<Decimal> {
    if returns.is_empty() {
        return Ok(Decimal::ZERO);
    }
    if quantile <= Decimal::ZERO || quantile > Decimal::ONE {
        return Err(ResearchError::ValidationMethodology {
            detail: format!("tail-loss quantile must be in (0, 1], got {quantile}"),
        }
        .into());
    }
    let mut sorted = returns.to_vec();
    sorted.sort();
    let n = sorted.len();
    let n_u64 = u64::try_from(n).map_err(|error| ResearchError::ValidationMethodology {
        detail: format!("return count does not fit u64: {error}"),
    })?;
    let raw = Decimal::from(n_u64)
        .checked_mul(quantile)
        .ok_or_else(|| ResearchError::ValidationMethodology {
            detail: "tail-loss quantile multiplication overflowed Decimal".to_owned(),
        })?
        .ceil();
    let take = raw
        .to_u64()
        .ok_or_else(|| ResearchError::ValidationMethodology {
            detail: format!("tail-loss count {raw} does not fit u64"),
        })?;
    let take = usize::try_from(take).map_err(|error| ResearchError::ValidationMethodology {
        detail: format!("tail-loss count does not fit usize: {error}"),
    })?;
    if take == 0 || take > n {
        return Err(ResearchError::ValidationMethodology {
            detail: format!("tail-loss count {take} is outside 1..={n}"),
        }
        .into());
    }
    Ok(stats::mean(&sorted[..take]).round_dp(RESEARCH_DECIMAL_SCALE))
}

/// `sorted` must already be ascending. Nearest-rank percentile (`0` = min,
/// `1` = max); `0` for an empty series.
fn percentile(sorted: &[Decimal], fraction: Decimal) -> QuantResult<Decimal> {
    if sorted.is_empty() {
        return Ok(Decimal::ZERO);
    }
    if !(Decimal::ZERO..=Decimal::ONE).contains(&fraction) {
        return Err(ResearchError::ValidationMethodology {
            detail: format!("percentile fraction must be in [0, 1], got {fraction}"),
        }
        .into());
    }
    let last_index =
        sorted
            .len()
            .checked_sub(1)
            .ok_or_else(|| ResearchError::ValidationMethodology {
                detail: "non-empty percentile input has no last index".to_owned(),
            })?;
    let last = Decimal::from(u64::try_from(last_index).map_err(|error| {
        ResearchError::ValidationMethodology {
            detail: format!("percentile index does not fit u64: {error}"),
        }
    })?);
    let index =
        (last * fraction)
            .round()
            .to_u64()
            .ok_or_else(|| ResearchError::ValidationMethodology {
                detail: "percentile index does not fit u64".to_owned(),
            })?;
    let index = usize::try_from(index).map_err(|error| ResearchError::ValidationMethodology {
        detail: format!("percentile index does not fit usize: {error}"),
    })?;
    sorted.get(index).copied().ok_or_else(|| {
        ResearchError::ValidationMethodology {
            detail: format!("percentile index {index} is out of bounds"),
        }
        .into()
    })
}

fn median_decimal(values: &[Decimal]) -> QuantResult<Decimal> {
    let mut sorted = values.to_vec();
    sorted.sort();
    percentile(&sorted, Decimal::new(5, 1))
}

/// The CPCV path whose Sharpe is the distribution's median — the
/// "representative path" DSR's `SR_hat`/`T`/skew/kurtosis are computed from
/// this path. `None` for an empty path set (never constructed by
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
    let returns_period_count = u64::try_from(path.group_returns.len()).map_err(|error| {
        ResearchError::ValidationMethodology {
            detail: format!("representative path period count does not fit u64: {error}"),
        }
    })?;
    let dsr_input = DsrInput {
        observed_sharpe: path.sharpe,
        returns_period_count,
        period_length: validated_period_length(dataset)?,
        skewness: stats::skewness(&path.group_returns),
        kurtosis: stats::kurtosis(&path.group_returns),
        // Bailey multiple-testing N/V: same population — the governed trial
        // grid that produced `matrix`. Coord-search is audit-only.
        trial_count: trial_grid_count,
        trial_sharpe_variance: stats::variance(&trial_sharpes),
    };
    let dsr = deflated_sharpe_ratio(&dsr_input)?;
    let pbo = probability_of_backtest_overfitting(matrix, &config.pbo)?;
    Ok((dsr, pbo))
}

/// One Sharpe ratio per trial column of `matrix` (used as the multiple-testing
/// correction's dispersion estimate — the variance of Sharpe *across trials*,
/// never conflated with the CPCV path-to-path variance).
fn trial_sharpe_series(matrix: &TrialPerformanceMatrix) -> Vec<Decimal> {
    (0..matrix.trial_count())
        .map(|trial| {
            let column: Vec<Decimal> = matrix.rows().map(|row| row[trial]).collect();
            sharpe_ratio(&column, Decimal::ONE)
        })
        .collect()
}

fn min_trl_for_path(
    dataset: &TrainingDatasetInfo,
    path: &BacktestPath,
    dsr_significance: &Decimal,
) -> QuantResult<Option<ChronoDuration>> {
    let returns_period_count = u64::try_from(path.group_returns.len()).map_err(|error| {
        ResearchError::ValidationMethodology {
            detail: format!("representative path period count does not fit u64: {error}"),
        }
    })?;
    let input = DsrInput {
        observed_sharpe: path.sharpe,
        returns_period_count,
        period_length: validated_period_length(dataset)?,
        skewness: stats::skewness(&path.group_returns),
        kurtosis: stats::kurtosis(&path.group_returns),
        trial_count: 1,
        trial_sharpe_variance: Decimal::ZERO,
    };
    min_track_record_length(&input, *dsr_significance)
}

#[cfg(test)]
mod tests {
    use std::{collections::BTreeMap, sync::Arc};

    use chrono::{DateTime, TimeZone, Utc};
    use quant_pivot_models::{
        domain::data_plane::DecisionClock,
        enums::{
            common::MarketCategory,
            factor::FactorFamily,
            model::ModelFamily,
            quant::{DataQualityStatus, FactorDirection},
        },
        types::{
            BacktestPathSetId, ContentHash, DatasetCoverage, EventId, FactorDefinitionId, MarketId,
            ModelInputContract, ModelVersionId, OrderIntentId, PositionId, Price, Probability,
            SchemaVersion, Shares, TokenId, TrainingExampleId, TrainingSampleSource,
            backtest::{BacktestPath, SharpeDistribution},
            factor::FactorExplanation,
            model_quality::{GateId, GateIntent, GateStatus, GateSubject},
            model_training::TrainingObjectiveSpec,
        },
    };
    use quant_pivot_research::{
        factors::{FactorValue, NormalizedFactor, names, names::MOMENTUM_ROC},
        features::FeatureVector,
        gates::{
            CpcvPathSetGateInput, DefaultModelQualityGate, ModelQualityGate, QualityGateInput,
            QualityGateThresholds, SellQualityGateThresholds, ValidationGateThresholds,
        },
        model::{
            CancellationProbe, LabelSelector, ModelArtifactHeader, PositionStateFeatures,
            SellSignalPolicy,
        },
        selection::SelectedMarket,
        training::{
            HOLD_VS_EXIT_ALPHA_BPS, LabelName, LeakageFindings, LotTrainingContext,
            TrainingExample, TrainingLabel,
        },
        validation::{
            BacktestPathSet, CombinatorialPurgedBacktester, CpcvConfig, CpcvRequest,
            DefaultCombinatorialPurgedBacktester, GroupRowFilter, PurgeConfig,
        },
    };
    use rust_decimal::Decimal;
    use rust_decimal_macros::dec;

    use super::{
        FoldReplayEngineAdapter, FoldTemplate, LotDecisionSequence, ReplayTemplate,
        SellFoldTemplate, SellReplayTemplate, TimelineGroup, build_lot_timeline_groups,
        calendarize_sell_path_set, selected_label_matured_at, sell_seed_weights,
        validate_sell_examples, validated_group_indices,
    };
    use crate::test_fixtures::execution_pg_seed::fixture_profile_ref;

    fn ts(secs: i64) -> DateTime<Utc> {
        Utc.timestamp_opt(1_700_000_000 + secs, 0).unwrap()
    }

    fn label() -> LabelSelector {
        LabelSelector {
            name: HOLD_VS_EXIT_ALPHA_BPS,
            horizon_secs: 0,
        }
    }

    #[test]
    fn precomputed_group_indices_are_strictly_sorted_unique_and_bounded() {
        let ranges = [0..2, 2..5, 5..6];
        let filter = GroupRowFilter {
            group_indices: vec![0, 2],
        };
        let selected =
            validated_group_indices(&filter, ranges.len()).expect("selected group indices");
        assert_eq!(selected, &[0, 2]);
        assert!(
            validated_group_indices(
                &GroupRowFilter {
                    group_indices: vec![2, 0, 2],
                },
                ranges.len(),
            )
            .is_err()
        );
        assert!(
            validated_group_indices(
                &GroupRowFilter {
                    group_indices: vec![3],
                },
                ranges.len(),
            )
            .is_err()
        );
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
            selected_market: SelectedMarket {
                market_id: market_id.clone(),
                event_id: EventId::new("event:test"),
                category: MarketCategory::Sports,
                primary_token_id: token_id.clone(),
                secondary_token_id: None,
                liquidity_usd: None,
                volume_24h_usd: None,
                source_refs: Vec::new(),
            },
            decision_boundary: DecisionClock::new(0).boundary(as_of).expect("boundary"),
            sample_source: TrainingSampleSource::ExitDecision,
            feature_vector: FeatureVector {
                market_id,
                token_id: Some(token_id),
                decision_at: as_of,
                generic_schema_version: SchemaVersion::FIRST,
                generic: BTreeMap::new(),
                domain: None,
                data_quality: DataQualityStatus::Fresh,
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
            decision_capture: None,
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
                unrealized_pnl_pct: Some(dec!(0)),
                time_in_trade_ratio: dec!(0.1),
                peak_mark_drawdown: Some(dec!(0)),
            }),
            book_fidelity: None,
        }
    }

    /// The core invariant: every decision point belonging to one
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
            lot_decision(lot_a, ts(0), closed_a),
            lot_decision(lot_a, ts(60), closed_a),
            lot_decision(lot_b, ts(30), closed_b),
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
            groups[0].decision_at < groups[1].decision_at,
            "groups must be ascending by as_of"
        );
    }

    #[test]
    fn selected_label_matured_at_returns_none_for_unlabeled_example() {
        let example = lot_decision(PositionId::from_v7(), ts(0), ts(100));
        let other = LabelSelector {
            name: LabelName::new("settlement_outcome"),
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

    fn content_hash_seed(seed: u8) -> ContentHash {
        let hex = format!("{seed:02x}{}", "0".repeat(62));
        ContentHash::parse(&format!("blake3:{hex}")).expect("hash")
    }

    fn market_factor(score: Decimal) -> FactorValue {
        FactorValue {
            definition_id: FactorDefinitionId::from_v7(),
            name: MOMENTUM_ROC,
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
        momentum: Decimal,
        alpha_bps: Decimal,
    ) -> Vec<TrainingExample> {
        let first = ts(*decision_offsets_secs.first().unwrap_or(&0));
        let closed_at = ts(*decision_offsets_secs.last().unwrap_or(&0) + 3_600);
        let n = u64::try_from(decision_offsets_secs.len().max(1)).expect("len");
        decision_offsets_secs
            .iter()
            .enumerate()
            .map(|(i, &offset)| {
                let as_of = ts(offset);
                let scale = Decimal::from(n - u64::try_from(i).expect("i")) / Decimal::from(n);
                let mut example = lot_decision(*position_id, as_of, closed_at);
                example.factor_values = vec![market_factor(momentum * scale)];
                example.position_state = Some(PositionStateFeatures {
                    unrealized_pnl_pct: Some(momentum * dec!(0.5) * scale),
                    time_in_trade_ratio: momentum * scale,
                    peak_mark_drawdown: Some(dec!(0)),
                });
                example.labels[0].value = alpha_bps * scale;
                if let Some(ctx) = example.lot_context.as_mut() {
                    ctx.opened_at = first;
                }
                example
            })
            .collect()
    }

    fn sell_cpcv_examples() -> Vec<TrainingExample> {
        let signals = [
            (dec!(0.9), dec!(80)),
            (dec!(0.15), dec!(-60)),
            (dec!(0.85), dec!(70)),
            (dec!(0.2), dec!(-50)),
            (dec!(0.7), dec!(40)),
            (dec!(0.35), dec!(-20)),
            (dec!(0.65), dec!(30)),
            (dec!(0.4), dec!(-10)),
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
        let mut examples = Vec::new();
        for ((momentum, alpha), schedule) in signals.into_iter().zip(schedules) {
            let lot = PositionId::from_v7();
            examples.extend(lot_with_signal(&lot, &schedule, momentum, alpha));
        }
        examples
    }

    /// End-to-end algorithm integration: synthetic multi-lot `ExitDecision` fixture →
    /// `SellFoldSource` + `LotReplayBacktester` CPCV → calendarize → diagnostics
    /// → quality-gate input shape. Exercises the full algorithm path without
    /// `ClickHouse` / Postgres (those are covered by governance bind e2e).
    #[test]
    fn sell_cpcv_pipeline_produces_calendarized_diagnostics_for_gates() {
        let examples = sell_cpcv_examples();
        validate_sell_examples(&examples, &label()).expect("validate");
        let (groups, sequences) = build_lot_timeline_groups(&examples, &label()).expect("groups");
        assert_eq!(groups.len(), 8);

        let sequences: Arc<[LotDecisionSequence]> = Arc::from(sequences);
        let groups_arc: Arc<[TimelineGroup]> = Arc::from(groups);
        let seed_weights = sell_seed_weights(&examples).expect("seed weights");
        assert!(
            seed_weights.iter().any(|w| w.factor == names::MOMENTUM_ROC),
            "seed weights must cover market factors present on examples"
        );

        let fold_template = FoldTemplate::Sell(SellFoldTemplate {
            cancellation: CancellationProbe::default(),
            sequences: Arc::clone(&sequences),
            label: label(),
            seed_weights,
            header_template: ModelArtifactHeader {
                model_version_id: ModelVersionId::from_v7(),
                model_spec_definition_hash: content_hash_seed(0),
                profile_ref: fixture_profile_ref(),
                model_family: ModelFamily::HoldVsExitWeighted,
                feature_schema_hash: content_hash_seed(1),
                factor_schema_hash: content_hash_seed(2),
                trade_policy_artifact_id: None,
                trade_policy_hash: None,
            },
            base_objective: TrainingObjectiveSpec::default(),
            prediction_horizon_secs: 86_400,
            label_schema_hash: content_hash_seed(3),
            input_contract: Arc::new(ModelInputContract::single_required("book.mid")),
            groups: Arc::clone(&groups_arc),
            purge: PurgeConfig::pct_only(Decimal::ZERO),
        });
        let replay_template = ReplayTemplate::Sell(SellReplayTemplate {
            sequences: Arc::clone(&sequences),
            policy: SellSignalPolicy {
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

        assert_eq!(path_set.paths.len(), 3);
        assert_eq!(path_set.combination_count, 6);

        let ReplayTemplate::Sell(sell_replay) = &replay_template else {
            panic!("expected Sell replay template");
        };
        calendarize_sell_path_set(&mut path_set, &groups_arc, sell_replay, 3_600)
            .expect("calendarize");
        assert_sell_cpcv_gate_shape(&path_set, &groups_arc);
    }

    fn assert_sell_cpcv_gate_shape(path_set: &BacktestPathSet, groups_arc: &[TimelineGroup]) {
        let dist = &path_set.sharpe_distribution;
        assert!(dist.median_max_drawdown.is_some());
        assert!(dist.median_tail_loss.is_some());
        assert!(dist.baseline_uplift.is_some());
        for path in &path_set.paths {
            assert!(path.group_returns.len() >= 2);
        }

        let gate_input = CpcvPathSetGateInput {
            median_rank_ic: path_set.median_rank_ic,
            deflated_sharpe: dec!(0.97),
            pbo: dec!(0.20),
            min_track_record_length_secs: Some(86_400),
            median_max_drawdown: dist.median_max_drawdown,
            median_tail_loss: dist.median_tail_loss,
            baseline_uplift: dist.baseline_uplift,
            window_start: Some(groups_arc.first().expect("group").decision_at),
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
                leakage: LeakageFindings::default(),
                shadow_stability: Some(Probability::new(dec!(0.80))),
                thresholds: QualityGateThresholds::conservative(),
                validation_thresholds: ValidationGateThresholds::conservative(),
                path_set: Some(gate_input),
                sell_thresholds: SellQualityGateThresholds::default(),
                model_family: Some(ModelFamily::HoldVsExitWeighted),
                return_model_calibrated: false,
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
        assert_eq!(calibration.status, GateStatus::NotApplicable);
    }
}
