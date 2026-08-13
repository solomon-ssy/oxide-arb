//! Combinatorial Purged Cross-Validation + governed trial-grid
//! orchestration for fitted Buy-side model families.
//!
//! Mirrors the crate boundary the single-path [`BacktestService`](crate::service::backtest::BacktestService)
//! and [`ModelTrainerService`](crate::service::model_training::ModelTrainerService)
//! already establish: this service does the **impure** work (dataset load,
//! Parquet decode plus exact frozen execution/economic tick assembly — done
//! exactly **once**, then reused across every CPCV fold and every trial), and
//! reapplies each fitted weighted model's own frozen reference-CDF transform to
//! the immutable feature vectors before scoring. It delegates every
//! **pure** algorithm (purge/embargo, φ-path reconstruction, DSR/PSR/MinTRL,
//! CSCV/PBO) to [`quant_pivot_research::validation`]. No live `BookStore` is
//! ever touched and no current feature/factor code replaces frozen rows.

use std::{
    collections::BTreeMap,
    future::Future,
    ops::Range,
    sync::{
        Arc, Condvar, Mutex,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};

use async_trait::async_trait;
use chrono::{DateTime, Duration as ChronoDuration, Utc};
use quant_pivot_compute::{ComputeExecutor, OfflineMemory};
use quant_pivot_error::{
    QuantError, QuantResult, hashing::CanonicalDigestError, research::ResearchError,
};
use quant_pivot_models::{
    config::PortfolioSolverDeployConfig,
    domain::{
        governance::DecisionPolicySnapshotInfo,
        quant::{
            JobProgressSink, ModelSpecInfo, ModelVersionInfo, PortfolioScenarioVisibility,
            RouteCompatibilityDigests, RouteContractHash, TrainingDatasetInfo,
        },
    },
    enums::{
        common::MarketCategory,
        model::ModelFamily,
        quant::{CalibrationKind, CalibrationMethod, OutcomeSide},
    },
    hashing::CanonicalDigest,
    runtime_config::{
        DecimalValue, DecisionPolicySnapshot, FactorCrossSectionConfig, sections::FactorsConfig,
    },
    types::{
        BacktestPathSetId, BacktestReportId, Bps, ContentHash, DecisionPolicySnapshotId,
        ModelInputContract, ModelRunId, ModelVersionId, ResearchJobProgress,
        ResearchProfileArtifact, TrainingDatasetId,
        backtest::{
            BacktestPath, CpcvEstimatorIdentity, CpcvFoldArtifact, CpcvFoldArtifacts,
            CpcvFoldCalibrationPolicy, CpcvMethodologyBinding, CpcvPathSetSubject,
            CpcvTrialPathBinding, CscvSelectionEvidence, CscvTrialGridBinding,
        },
        factor::FactorServingPlane,
        model_lineage::ModelVersionDerivation,
        model_serving::{ModelServingBindings, ModelServingContract, ModelServingTransformBinding},
        model_training::TrainingObjectiveSpec,
        stable_name::FeatureName,
    },
};
#[cfg(feature = "ml-classical")]
use quant_pivot_models::{enums::model::ClassicalKind, types::ArtifactUri};
#[cfg(feature = "ml-classical")]
use quant_pivot_research::model::{
    ClassicalAdapterRegistry, ClassicalParams, ClassicalRuntime, ScoreMultiplierSpec,
    artifact::ClassicalModelPayload,
};
use quant_pivot_research::{
    artifact::ArtifactStore,
    backtest::{
        BacktestRankTarget, BacktestRequest, BacktestScenarioContext, BacktestTick,
        CalibrationReplayTick, ModelCalibrationOutcome, ModelCalibrationReplay,
        PortfolioReplayBacktester, PrecomputedBacktestInputs, PrecomputedBacktestTick,
    },
    factors::FactorEngine,
    features::FeatureSchema,
    hashing::ResearchHasher,
    model::{
        CancellationProbe, CrossFittedRuntime, HorizonMultipliers, LabelSelector, ModelArtifact,
        ModelRankScore, ModelRuntimeInput, ModelRuntimeOutput, NestedCalibrationFitInput,
        NestedCalibrationFitter, NestedCalibrationObservation, NestedCalibrationPolicy,
        PreparedWeightedFold, QuantModelRuntime, ResolvedCalibration, ReturnModelSpec,
        SubstitutionConfidenceRules, TrainModelRequest, ValidationSpec, WeightedFactorRuntime,
        WeightedModelTrainingOutput, artifact::ModelPayload, factor_heads::FactorHeadSpec,
        objective::runtime_training_objective,
    },
    portfolio::{
        PortfolioScenarioFoldFitInput, PortfolioScenarioMethodology, PortfolioScenarioModelFitter,
        PortfolioScenarioResidualObservation,
    },
    selection::ModelFeatureRequirements,
    stats,
    training::{LabelName, TrainingExample, TrainingLabel},
    validation::{
        BacktestPathSet, ClassicalTrialGrid, CombinatorialPurgedBacktester, CpcvConfig,
        CpcvRequest, DefaultCombinatorialPurgedBacktester, DefaultPurgedSplitter, DsrInput,
        DsrReport, FoldModelSource, FoldRuntime, FoldTrainingIdentity, FoldTrainingRequest,
        GroupEvaluation, GroupRowFilter, PathEconomicReplay, PboInput, PurgeConfig,
        PurgedPortfolioFoldRuntime, PurgedSplitter, RankObservation, ReplayEngine, TimelineGroup,
        Trial, TrialGridSpec, TrialPerformanceMatrix, WeightedFactorTrialGrid,
        analyze_selection_bias, min_track_record_length,
    },
};
use rayon::prelude::*;
use rust_decimal::Decimal;
use serde::Serialize;
use tokio::{
    runtime::Handle,
    time::{MissedTickBehavior, interval},
};
use tokio_util::sync::CancellationToken;
use tracing::info;

#[cfg(feature = "ml-classical")]
use crate::service::model_training;
use crate::{
    prefetch::source_slice::{FrozenSourceSlice, SourceSliceReader},
    projection::inference_batch::build_runtime_input,
    service::{
        backtest::{FrozenTickBuild, frozen_ticks},
        historical_replay::ReplayConfig,
        model_serving_preimage::VerifiedModelServingPreimage,
        portfolio_context::PromotedPortfolioContextLoader,
        training_dataset::{require_dataset_materialization, verify_frozen_dataset_artifact},
    },
};

/// CPCV job stage boundaries and durable exact-work supervision.
struct CpcvProgress;

/// Lock-free completed-work counter shared by Rayon workers and the async
/// progress supervisor. Relaxed ordering is sufficient because this is
/// advisory monotone evidence; the offline result channel remains the memory
/// synchronization boundary for the actual financial result.
#[derive(Clone)]
struct WorkCounter {
    phase: &'static str,
    completed: Arc<AtomicU64>,
    total: u64,
}

impl WorkCounter {
    fn try_new(phase: &'static str, total: u64) -> QuantResult<Self> {
        if total == 0 {
            return Err(ResearchError::ValidationMethodology {
                detail: format!("{phase} progress requires a positive work total"),
            }
            .into());
        }
        Ok(Self {
            phase,
            completed: Arc::new(AtomicU64::new(0)),
            total,
        })
    }

    fn advance(&self) -> QuantResult<()> {
        let mut current = self.completed.load(Ordering::Relaxed);
        loop {
            let Some(next) = current.checked_add(1).filter(|next| *next <= self.total) else {
                return Err(ResearchError::ValidationMethodology {
                    detail: format!(
                        "{} progress exceeded its precommitted total {}",
                        self.phase, self.total
                    ),
                }
                .into());
            };
            match self.completed.compare_exchange_weak(
                current,
                next,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => return Ok(()),
                Err(observed) => current = observed,
            }
        }
    }

    fn completed(&self) -> u64 {
        self.completed.load(Ordering::Relaxed)
    }

    #[cfg(test)]
    fn snapshot(&self) -> ResearchJobProgress {
        ResearchJobProgress::with_total(self.phase, self.completed(), self.total)
    }
}

/// Deterministic complete OOS path selected from every trial CPCV run for the
/// PBO/DSR performance matrix. The methodology hash binds this rule.
/// Default path selected before any model or trial performance is observed.
/// The resulting binding is frozen into every CPCV run and is the single OOS
/// functional used by both the serving subject and its governed trial grid.
const DEFAULT_SELECTION_PATH_INDEX: u32 = 0;

fn rank_target(model_spec: &ModelSpecInfo) -> LabelSelector {
    LabelSelector {
        name: LabelName::new(model_spec.training_contract.target.label_name().to_owned()),
        horizon_secs: model_spec.training_contract.target.label_horizon_secs(),
    }
}

impl CpcvProgress {
    const REPORT_INTERVAL: Duration = Duration::from_secs(1);
    const TOTAL: u64 = 100;
    const LOAD: ProgressPhase = ProgressPhase { start: 0 };
    const MATERIALIZE_EXAMPLES: ProgressPhase = ProgressPhase { start: 10 };
    const MATERIALIZE_TICKS: ProgressPhase = ProgressPhase { start: 25 };
    const FINALIZE: ProgressPhase = ProgressPhase { start: 95 };

    async fn monitor<T, F, S>(
        sink: &dyn JobProgressSink,
        work: F,
        mut snapshot: S,
    ) -> QuantResult<T>
    where
        F: Future<Output = QuantResult<T>>,
        S: FnMut() -> ResearchJobProgress,
    {
        sink.report(snapshot());
        let mut heartbeat = interval(Self::REPORT_INTERVAL);
        heartbeat.set_missed_tick_behavior(MissedTickBehavior::Delay);
        heartbeat.tick().await;
        tokio::pin!(work);
        loop {
            tokio::select! {
                result = &mut work => {
                    sink.report(snapshot());
                    return result;
                }
                _ = heartbeat.tick() => sink.report(snapshot()),
            }
        }
    }
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
    fn train_fold(&self, request: FoldTrainingRequest<'_>) -> QuantResult<FoldRuntime> {
        ensure_cpcv_not_cancelled(self.cancel, "fold train boundary")?;
        let model = self.inner.train_fold(request)?;
        ensure_cpcv_not_cancelled(self.cancel, "fold train completion")?;
        Ok(model)
    }
}

struct TrialPathFoldSource<'a> {
    inner: &'a dyn FoldModelSource,
    trial_id: u32,
    path_index: u32,
}

impl FoldModelSource for TrialPathFoldSource<'_> {
    fn train_fold(&self, request: FoldTrainingRequest<'_>) -> QuantResult<FoldRuntime> {
        let FoldTrainingIdentity::Validation {
            combination_index,
            test_partitions,
            test_groups,
        } = request.identity
        else {
            return Err(ResearchError::ValidationMethodology {
                detail: "trial CPCV source received a nested trial identity".to_owned(),
            }
            .into());
        };
        self.inner.train_fold(FoldTrainingRequest {
            identity: FoldTrainingIdentity::TrialPathValidation {
                trial_id: self.trial_id,
                path_index: self.path_index,
                combination_index,
                test_partitions,
                test_groups,
            },
            filter: request.filter,
        })
    }
}

struct CancellableReplayEngine<'a> {
    inner: &'a dyn ReplayEngine,
    cancel: &'a CancellationToken,
    completed_folds: &'a WorkCounter,
    completed_paths: Option<&'a WorkCounter>,
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
        self.completed_folds.advance()?;
        Ok(evaluations)
    }

    fn replay_path(
        &self,
        path_index: u32,
        groups: &[TimelineGroup],
        evaluations: &[&GroupEvaluation],
    ) -> QuantResult<Option<PathEconomicReplay>> {
        ensure_cpcv_not_cancelled(self.cancel, "path replay boundary")?;
        let replay = self.inner.replay_path(path_index, groups, evaluations)?;
        ensure_cpcv_not_cancelled(self.cancel, "path replay completion")?;
        if let Some(completed_paths) = self.completed_paths {
            completed_paths.advance()?;
        }
        Ok(replay)
    }
}

#[derive(Default)]
struct TrialPathReplayCache {
    entries: Mutex<BTreeMap<ContentHash, Arc<TrialPathReplayEntry>>>,
    computed: AtomicU64,
    reused: AtomicU64,
}

enum TrialPathReplayState {
    Computing,
    Ready(PathEconomicReplay),
    Aborted,
}

struct TrialPathReplayEntry {
    state: Mutex<TrialPathReplayState>,
    ready: Condvar,
}

impl TrialPathReplayEntry {
    const fn computing() -> Self {
        Self {
            state: Mutex::new(TrialPathReplayState::Computing),
            ready: Condvar::new(),
        }
    }

    fn wait_for(&self, cancel: &CancellationToken) -> QuantResult<Option<PathEconomicReplay>> {
        let mut state = self.state.lock().map_err(|_| {
            QuantError::from(ResearchError::ValidationMethodology {
                detail: "trial replay cache entry mutex was poisoned".to_owned(),
            })
        })?;
        loop {
            match &*state {
                TrialPathReplayState::Ready(replay) => return Ok(Some(replay.clone())),
                TrialPathReplayState::Aborted => return Ok(None),
                TrialPathReplayState::Computing => {
                    ensure_cpcv_not_cancelled(cancel, "trial replay cache wait")?;
                    let (next, _) = self
                        .ready
                        .wait_timeout(state, Duration::from_millis(25))
                        .map_err(|_| ResearchError::ValidationMethodology {
                            detail: "trial replay cache wait mutex was poisoned".to_owned(),
                        })?;
                    state = next;
                }
            }
        }
    }
}

struct TrialReplayOwner<'a> {
    cache: &'a TrialPathReplayCache,
    key: ContentHash,
    entry: Arc<TrialPathReplayEntry>,
    committed: bool,
}

impl TrialReplayOwner<'_> {
    fn commit(&mut self, replay: PathEconomicReplay) -> QuantResult<()> {
        let mut state = self.entry.state.lock().map_err(|_| {
            QuantError::from(ResearchError::ValidationMethodology {
                detail: "trial replay cache commit mutex was poisoned".to_owned(),
            })
        })?;
        if !matches!(*state, TrialPathReplayState::Computing) {
            return Err(ResearchError::ValidationMethodology {
                detail: "trial replay cache owner lost its computing state".to_owned(),
            }
            .into());
        }
        *state = TrialPathReplayState::Ready(replay);
        self.cache.computed.fetch_add(1, Ordering::Relaxed);
        self.committed = true;
        drop(state);
        self.entry.ready.notify_all();
        Ok(())
    }
}

impl Drop for TrialReplayOwner<'_> {
    fn drop(&mut self) {
        if self.committed {
            return;
        }
        if let Ok(mut state) = self.entry.state.lock() {
            *state = TrialPathReplayState::Aborted;
        }
        if let Ok(mut entries) = self.cache.entries.lock()
            && entries
                .get(&self.key)
                .is_some_and(|entry| Arc::ptr_eq(entry, &self.entry))
        {
            entries.remove(&self.key);
        }
        self.entry.ready.notify_all();
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct TrialReplayCacheAudit {
    computed: u64,
    reused: u64,
}

impl TrialPathReplayCache {
    fn claim(&self, key: ContentHash) -> QuantResult<(Arc<TrialPathReplayEntry>, bool)> {
        let mut entries = self.entries.lock().map_err(|_| {
            QuantError::from(ResearchError::ValidationMethodology {
                detail: "trial replay cache index mutex was poisoned".to_owned(),
            })
        })?;
        if let Some(entry) = entries.get(&key) {
            let entry = Arc::clone(entry);
            drop(entries);
            return Ok((entry, false));
        }
        let entry = Arc::new(TrialPathReplayEntry::computing());
        entries.insert(key, Arc::clone(&entry));
        drop(entries);
        Ok((entry, true))
    }

    fn get_or_run<F>(
        &self,
        key: ContentHash,
        cancel: &CancellationToken,
        action: F,
    ) -> QuantResult<PathEconomicReplay>
    where
        F: FnOnce() -> QuantResult<PathEconomicReplay>,
    {
        let mut action = Some(action);
        loop {
            ensure_cpcv_not_cancelled(cancel, "trial replay cache boundary")?;
            let (entry, owner) = self.claim(key)?;
            if owner {
                let mut owner = TrialReplayOwner {
                    cache: self,
                    key,
                    entry,
                    committed: false,
                };
                let action = action
                    .take()
                    .ok_or_else(|| ResearchError::ValidationMethodology {
                        detail: "trial replay cache action was consumed before ownership"
                            .to_owned(),
                    })?;
                let replay = action()?;
                owner.commit(replay.clone())?;
                return Ok(replay);
            }
            if let Some(replay) = entry.wait_for(cancel)? {
                self.reused.fetch_add(1, Ordering::Relaxed);
                return Ok(replay);
            }
        }
    }

    fn audit(&self, expected: u64) -> QuantResult<TrialReplayCacheAudit> {
        let audit = TrialReplayCacheAudit {
            computed: self.computed.load(Ordering::Relaxed),
            reused: self.reused.load(Ordering::Relaxed),
        };
        if audit.computed == 0
            || audit
                .computed
                .checked_add(audit.reused)
                .is_none_or(|observed| observed != expected)
        {
            return Err(ResearchError::ValidationMethodology {
                detail: format!(
                    "trial replay cache audited computed={} reused={} against expected={expected}",
                    audit.computed, audit.reused
                ),
            }
            .into());
        }
        Ok(audit)
    }
}

/// Repository + store dependencies.
///
/// This service needs both the trainer's and the backtester's read paths:
/// it mirrors `BacktestServiceDeps` (`crate::service::backtest`) combined
/// with `ModelTrainerServiceDeps` (`crate::service::model_training`).
pub struct CpcvBacktestServiceDeps {
    pub compute: Arc<ComputeExecutor>,
    pub artifact_store: Arc<dyn ArtifactStore>,
}

/// Governed methodology configuration (`research.validation.*`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CpcvBacktestConfig {
    /// Complete scoring and factor-head policy shared with production training.
    pub factors: FactorsConfig,
    /// Base training objective (`research.training.*`) every CPCV fold trains against.
    pub objective: TrainingObjectiveSpec,
    /// CPCV partition config (`research.validation.cpcv.*`).
    pub cpcv: CpcvConfig,
    /// Inner holdout share used to fit calibration and scenario residuals
    /// without observing the outer test population.
    pub nested_estimator_holdout_bps: u32,
    /// Initial inner holdout group floor. The fold planner expands this lower
    /// bound only when real label intervals require additional purge capacity.
    pub nested_estimator_min_groups: u32,
    /// Preferred fold-local probability-calibration method.
    pub calibration_method: CalibrationMethod,
    /// Data floor above which isotonic is admitted; smaller folds use Platt.
    pub calibration_min_samples_isotonic: u64,
    /// Wilson/reliability confidence level for fold-local uncertainty.
    pub calibration_ci_confidence: Decimal,
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
}

impl CpcvBacktestConfig {
    /// Project the governed `research.validation.*` snapshot into the exact
    /// family-specific CPCV methodology. Callers cannot substitute a separate
    /// trial grid or training objective for the serving subject.
    ///
    /// # Errors
    ///
    /// Returns a typed research error when the frozen training objective is
    /// invalid.
    pub fn from_policy(
        runtime: &DecisionPolicySnapshot,
        model_family: ModelFamily,
    ) -> QuantResult<Self> {
        let validation = &runtime
            .profile_artifacts
            .research_method
            .research
            .validation;
        let multipliers =
            |values: &[DecimalValue]| values.iter().map(|value| value.value).collect::<Vec<_>>();
        let trials = if model_family.is_classical() {
            TrialGridSpec::Classical(ClassicalTrialGrid {
                forest_n_trees_multipliers: multipliers(
                    &validation.trials.forest_n_trees_multipliers,
                ),
                linear_alpha_multipliers: multipliers(&validation.trials.linear_alpha_multipliers),
                max_trials: validation.trials.max_trials,
            })
        } else {
            TrialGridSpec::WeightedFactor(WeightedFactorTrialGrid {
                lambda_multipliers: multipliers(&validation.trials.lambda_multipliers),
                rank_loss_kinds: validation.trials.rank_loss_kinds.clone(),
                max_trials: validation.trials.max_trials,
            })
        };

        Ok(Self {
            factors: runtime.profile_artifacts.scoring.definition.clone(),
            objective: runtime_training_objective(
                &runtime.profile_artifacts.research_method.research.training,
            )?,
            cpcv: CpcvConfig {
                n_groups: validation.cpcv.n_groups,
                k_test: validation.cpcv.k_test,
            },
            nested_estimator_holdout_bps: validation.cpcv.nested_estimator_holdout_bps,
            nested_estimator_min_groups: validation.cpcv.nested_estimator_min_groups,
            calibration_method: runtime.model_routing.model.calibration.method,
            calibration_min_samples_isotonic: runtime
                .model_routing
                .model
                .calibration
                .min_samples_isotonic,
            calibration_ci_confidence: runtime.model_routing.model.calibration.ci_confidence.value,
            purge: PurgeConfig {
                embargo_pct: validation.purge.embargo_pct.value,
                min_embargo_secs: runtime
                    .profile_artifacts
                    .features
                    .definition
                    .max_lookback_secs(),
            },
            trials,
            pbo: PboInput {
                block_count: validation.pbo.block_count,
            },
            dsr_significance: validation.gates.dsr_significance.value,
            entry_max_slippage_bps: Bps::new(Decimal::from(
                runtime.execution_risk.entry_order_policy.max_slippage_bps,
            )),
        })
    }
}

/// A serving graph and methodology that have been resolved together before
/// cache lookup or durable run creation.
///
/// The fields are intentionally private: a caller can inspect the commitments
/// needed for persistence, but cannot pair an unrelated fold policy or hash
/// binding with the verified serving preimage.
#[derive(Clone)]
pub struct PreparedCpcvRun {
    source: Arc<VerifiedModelServingPreimage>,
    fold_calibration: CpcvFoldCalibration,
    binding: CpcvRunBinding,
}

impl PreparedCpcvRun {
    /// Fully verified serving preimage used by the run.
    #[must_use]
    pub fn source(&self) -> &VerifiedModelServingPreimage {
        &self.source
    }

    /// Immutable serving subject committed by the path-set hash.
    #[must_use]
    pub const fn subject(&self) -> CpcvPathSetSubject {
        self.binding.subject
    }

    /// Exact governed methodology committed by the path-set hash.
    #[must_use]
    pub const fn methodology(&self) -> &CpcvMethodologyBinding {
        &self.binding.methodology
    }

    /// Canonical input hash used by `ModelRunKind::Cpcv`.
    #[must_use]
    pub const fn input_hash(&self) -> ContentHash {
        self.binding.input_hash
    }
}

/// A CPCV/trial-grid request resolved by the admin port.
pub struct CpcvBacktestInput {
    /// Real persisted CPCV run id carried into every runtime input.
    model_run_id: ModelRunId,
    /// Fully resolved model, `ModelSpec`, Dataset, policy, Source Slice,
    /// artifact, calibration, and `TradePolicy` graph.
    source: Arc<VerifiedModelServingPreimage>,
    /// Explicit fold calibration treatment derived from verified lineage.
    fold_calibration: CpcvFoldCalibration,
    /// Frozen subject/methodology preimage computed before cache lookup and run
    /// creation.
    binding: CpcvRunBinding,
    /// Pre-assigned path-set id (async job engine); minted when absent.
    path_set_id: BacktestPathSetId,
    /// Audit-only: production `coordinate_search` effective trials (persisted
    /// for operator visibility). **Not** part of DSR N — Bailey's N/V must
    /// describe the same trial population, and V comes only from the governed
    /// trial-grid Sharpe series.
    coord_search_effective_n: u32,
}

struct TrialGridExecution<'a> {
    path_set_id: BacktestPathSetId,
    fold_template: Arc<FoldTemplate>,
    replay_template: Arc<PortfolioReplayTemplate>,
    groups: Arc<[TimelineGroup]>,
    selection_path: CpcvTrialPathBinding,
    progress: &'a dyn JobProgressSink,
    cancel: &'a CancellationToken,
}

impl CpcvBacktestInput {
    /// Bind durable run/path identifiers to an already verified CPCV subject.
    ///
    /// The expensive serving graph and methodology validation happens in
    /// [`CpcvBacktestService::prepare_run`]; this constructor cannot accept
    /// caller-supplied evidence hashes or fold policies.
    #[must_use]
    pub fn from_prepared(
        prepared: &PreparedCpcvRun,
        model_run_id: ModelRunId,
        path_set_id: BacktestPathSetId,
        coord_search_effective_n: u32,
    ) -> Self {
        Self {
            model_run_id,
            source: Arc::clone(&prepared.source),
            fold_calibration: prepared.fold_calibration.clone(),
            binding: prepared.binding.clone(),
            path_set_id,
            coord_search_effective_n,
        }
    }
}

impl CpcvFoldCalibration {
    pub(crate) fn resolve(
        version: &ModelVersionInfo,
        source: &VerifiedModelServingPreimage,
        parent: Option<(&ModelVersionInfo, &VerifiedModelServingPreimage)>,
    ) -> QuantResult<Self> {
        let derivation =
            version
                .verified_derivation()
                .map_err(|error| ResearchError::InvalidModelArtifact {
                    detail: format!("verify CPCV subject derivation: {error}"),
                })?;
        let contract = source.artifact().header().serving_contract();
        match source.artifact().payload() {
            ModelPayload::Classical(_) => {
                if !matches!(derivation, ModelVersionDerivation::Training)
                    || parent.is_some()
                    || contract.bindings().model.calibration.is_some()
                {
                    return Err(ResearchError::InvalidModelArtifact {
                        detail: "classical CPCV subject has weighted calibration lineage"
                            .to_owned(),
                    }
                    .into());
                }
                Ok(Self {
                    evidence: CpcvFoldCalibrationPolicy::NotApplicable,
                    return_model: None,
                })
            }
            ModelPayload::WeightedFactor(weighted) => {
                match (&weighted.return_model, derivation, parent) {
                    (
                        return_model @ ReturnModelSpec::Heuristic(_),
                        ModelVersionDerivation::Training,
                        None,
                    ) if contract.bindings().model.calibration.is_none() => Ok(Self {
                        evidence: CpcvFoldCalibrationPolicy::SubjectHeuristic {
                            return_model_hash: return_model_hash(return_model)?,
                        },
                        return_model: Some(return_model.clone()),
                    }),
                    (
                        ReturnModelSpec::Calibrated(calibrated),
                        ModelVersionDerivation::ReturnCalibration {
                            parent_model_version_id,
                            calibration_artifact_id,
                        },
                        Some((parent_version, parent_source)),
                    ) => {
                        let calibration = contract
                            .bindings()
                            .model
                            .calibration
                            .as_ref()
                            .ok_or_else(|| ResearchError::InvalidModelArtifact {
                                detail:
                                    "calibrated CPCV subject has no serving calibration binding"
                                        .to_owned(),
                            })?;
                        let parent_contract = parent_source.artifact().header().serving_contract();
                        let ModelPayload::WeightedFactor(parent_weighted) =
                            parent_source.artifact().payload()
                        else {
                            return Err(ResearchError::InvalidModelArtifact {
                                detail: "calibrated weighted CPCV parent is not WeightedFactor"
                                    .to_owned(),
                            }
                            .into());
                        };
                        let ReturnModelSpec::Heuristic(parent_return_model) =
                            &parent_weighted.return_model
                        else {
                            return Err(ResearchError::InvalidModelArtifact {
                                detail: "calibrated CPCV subject parent must carry the exact \
                                     uncalibrated heuristic return model"
                                    .to_owned(),
                            }
                            .into());
                        };
                        if calibrated.calibrator_ref != calibration_artifact_id
                            || calibration.kind != CalibrationKind::ModelScore
                            || calibration.artifact_id != calibration_artifact_id
                            || parent_version.model_version_id != parent_model_version_id
                            || parent_contract.bindings().model.model_version_id
                                != parent_model_version_id
                            || parent_contract.bindings().model.calibration.is_some()
                        {
                            return Err(ResearchError::InvalidModelArtifact {
                                detail: "CPCV calibrated-child lineage, serving calibration, and \
                                     verified heuristic parent disagree"
                                    .to_owned(),
                            }
                            .into());
                        }
                        let parent_return_model =
                            ReturnModelSpec::Heuristic(parent_return_model.clone());
                        let parent_artifact_hash = parent_source.artifact().content_hash()?;
                        if parent_artifact_hash != parent_version.artifact_hash {
                            return Err(ResearchError::InvalidModelArtifact {
                                detail: "CPCV parent artifact hash differs from registry"
                                    .to_owned(),
                            }
                            .into());
                        }
                        Ok(Self {
                            evidence: CpcvFoldCalibrationPolicy::CalibratedSubjectParentHeuristic {
                                calibration_artifact_id,
                                calibration_hash: calibration.content_hash,
                                parent_model_version_id,
                                parent_artifact_hash,
                                parent_serving_contract_hash: parent_contract.contract_hash(),
                                parent_return_model_hash: return_model_hash(&parent_return_model)?,
                            },
                            return_model: Some(parent_return_model),
                        })
                    }
                    _ => Err(ResearchError::InvalidModelArtifact {
                        detail: "CPCV weighted subject return-model, persisted derivation, and \
                             verified parent do not form a supported exact fold policy"
                            .to_owned(),
                    }
                    .into()),
                }
            }
            ModelPayload::SellScorer(_) => Err(ResearchError::SellOofEstimatorRequired.into()),
        }
    }

    fn rebind_contract(&self, bindings: &mut ModelServingBindings) -> QuantResult<()> {
        match (&self.evidence, bindings.model.calibration.as_ref()) {
            (
                CpcvFoldCalibrationPolicy::NotApplicable
                | CpcvFoldCalibrationPolicy::SubjectHeuristic { .. },
                None,
            ) => Ok(()),
            (
                CpcvFoldCalibrationPolicy::CalibratedSubjectParentHeuristic {
                    calibration_artifact_id,
                    calibration_hash,
                    ..
                },
                Some(binding),
            ) if binding.kind == CalibrationKind::ModelScore
                && binding.artifact_id == *calibration_artifact_id
                && binding.content_hash == *calibration_hash =>
            {
                // This is an explicit methodological transformation: a newly
                // fitted fold estimator cannot reuse the subject estimator's
                // calibrator. The verified parent's exact heuristic was
                // selected by `resolve`, and the persisted policy records why
                // this binding is removed.
                bindings.model.calibration = None;
                Ok(())
            }
            _ => Err(ResearchError::InvalidModelArtifact {
                detail: "CPCV fold calibration policy differs from subject serving binding"
                    .to_owned(),
            }
            .into()),
        }
    }

    fn weighted_return_model(&self) -> QuantResult<ReturnModelSpec> {
        self.return_model.clone().ok_or_else(|| {
            ResearchError::ValidationMethodology {
                detail: "weighted CPCV fold has no explicit return-model policy".to_owned(),
            }
            .into()
        })
    }
}

fn return_model_hash(return_model: &ReturnModelSpec) -> QuantResult<ContentHash> {
    CanonicalDigest::content_hash_typed("quant-pivot/cpcv-fold-return-model", 1, return_model)
        .map_err(|error| {
            ResearchError::ValidationMethodology {
                detail: format!("hash CPCV fold return-model policy: {error}"),
            }
            .into()
        })
}

/// Canonical pre-run commitments used for cache verification and `ModelRun`
/// identity before any durable write is allowed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct CpcvRunBinding {
    pub subject: CpcvPathSetSubject,
    pub methodology: CpcvMethodologyBinding,
    pub input_hash: ContentHash,
}

/// Private executable fold policy paired with its public persistence evidence.
#[derive(Debug, Clone)]
pub(crate) struct CpcvFoldCalibration {
    evidence: CpcvFoldCalibrationPolicy,
    return_model: Option<ReturnModelSpec>,
}

/// The full validation outcome: CPCV path distribution, the
/// trial-grid-corrected Deflated Sharpe Ratio, PBO, and `MinTRL`.
#[derive(Debug, Clone)]
pub struct CpcvBacktestOutcome {
    pub path_set: BacktestPathSet,
    pub dsr: DsrReport,
    pub cscv_selection_evidence: CscvSelectionEvidence,
    pub min_track_record_length: Option<ChronoDuration>,
    /// Conservative dependence-adjusted DSR multiple-testing N derived from
    /// the complete raw trial-return population.
    pub dsr_conservative_independent_trial_count: u32,
    pub trial_grid_count: u32,
    /// Audit-only production coord-search effort (not included in DSR N).
    pub coord_search_effective_n: u32,
    /// Every ephemeral estimator used to produce the result, frozen in
    /// deterministic semantic order.
    pub fold_artifacts: CpcvFoldArtifacts,
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
    portfolio_contexts: PromotedPortfolioContextLoader,
    evaluation_frozen_at: DateTime<Utc>,
    replay: ReplayConfig,
}

impl CpcvBacktestService {
    pub(crate) fn validate_family(model_family: ModelFamily) -> QuantResult<()> {
        if model_family.is_exit_scorer() {
            return Err(ResearchError::SellOofEstimatorRequired.into());
        }
        Ok(())
    }

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
        policy: &DecisionPolicySnapshotInfo,
        solver: PortfolioSolverDeployConfig,
        replay: ReplayConfig,
    ) -> QuantResult<Self> {
        let portfolio_contexts = PromotedPortfolioContextLoader::new(
            Arc::clone(&deps.artifact_store),
            solver,
            policy.snapshot.clone(),
        );
        Ok(Self {
            deps,
            config,
            portfolio_contexts,
            evaluation_frozen_at: policy.created_at,
            replay,
        })
    }

    /// Resolve one exact serving subject into an immutable CPCV preparation.
    ///
    /// This is the sole constructor for [`CpcvBacktestInput`]'s semantic
    /// preimage. It verifies that the caller's service configuration, replay
    /// configuration, portfolio caps, model version, serving artifact, and
    /// optional calibration parent all describe the same frozen graph.
    ///
    /// # Errors
    ///
    /// Fails closed on any subject, methodology, factor-plane, bias-table, or
    /// lineage mismatch.
    pub fn prepare_run(
        &self,
        version: &ModelVersionInfo,
        source: Arc<VerifiedModelServingPreimage>,
        parent: Option<(&ModelVersionInfo, &VerifiedModelServingPreimage)>,
    ) -> QuantResult<PreparedCpcvRun> {
        Self::validate_family(version.model_family)?;
        let artifact = source.artifact();
        let contract = artifact.header().serving_contract();
        let bindings = contract.bindings();
        let artifact_hash = artifact.content_hash()?;
        if bindings.model.model_version_id != version.model_version_id
            || bindings.model.model_spec_id != version.model_spec_id
            || bindings.model.model_family != version.model_family
            || artifact_hash != version.artifact_hash
            || contract.contract_hash() != version.serving_contract_hash
            || contract
                != version.verified_serving_contract().map_err(|error| {
                    ResearchError::InvalidModelArtifact {
                        detail: format!("verify CPCV registry serving contract: {error}"),
                    }
                })?
        {
            return Err(ResearchError::InvalidModelArtifact {
                detail: "CPCV registry version and verified serving preimage differ".to_owned(),
            }
            .into());
        }
        self.verify_methodology(&source)?;
        let fold_calibration = CpcvFoldCalibration::resolve(version, &source, parent)?;
        let binding = self.run_binding(&source, &fold_calibration)?;
        Ok(PreparedCpcvRun {
            source,
            fold_calibration,
            binding,
        })
    }

    fn verify_methodology(&self, source: &VerifiedModelServingPreimage) -> QuantResult<()> {
        let runtime = &source.policy_snapshot().snapshot;
        let contract = source.artifact().header().serving_contract();
        let bindings = contract.bindings();
        let model_family = bindings.model.model_family;
        let expected_config = CpcvBacktestConfig::from_policy(runtime, model_family)?;
        if self.config != expected_config
            || self.portfolio_contexts.policy().execution_risk.portfolio
                != runtime.execution_risk.portfolio
        {
            return Err(ResearchError::ValidationMethodology {
                detail: "CPCV service methodology or portfolio caps differ from the frozen \
                         policy snapshot"
                    .to_owned(),
            }
            .into());
        }
        if self.replay.features != runtime.profile_artifacts.features.definition
            || self.replay.factors != runtime.profile_artifacts.scoring.definition
            || self.replay.domain != runtime.profile_artifacts.domain.definition
            || self.replay.data_quality != runtime.recommendation.data_quality
        {
            return Err(ResearchError::ValidationMethodology {
                detail: "CPCV replay configuration differs from the frozen policy snapshot"
                    .to_owned(),
            }
            .into());
        }
        let bias_matches = match (
            self.replay.bias_table.as_deref(),
            bindings.factors.bias_table.as_ref(),
        ) {
            (None, None) => true,
            (Some(table), Some(binding)) => {
                binding.kind == CalibrationKind::MarketPriceBias
                    && binding.artifact_id == table.table_id
                    && binding.content_hash == table.content_hash
            }
            _ => false,
        };
        if !bias_matches {
            return Err(ResearchError::InvalidModelArtifact {
                detail: "CPCV bias-table preimage differs from the exact serving contract"
                    .to_owned(),
            }
            .into());
        }
        let expected_plane = if model_family.is_classical() {
            FactorServingPlane::try_empty().map_err(|error| {
                ResearchError::InvalidModelArtifact {
                    detail: format!("build CPCV classical factor plane: {error}"),
                }
            })?
        } else {
            FactorEngine::for_model_scope(
                &self.replay.factors,
                &self.replay.features,
                &self.replay.domain,
                source.profile().spec.category,
                self.replay.bias_table.clone(),
            )
            .serving_plane()?
            .clone()
        };
        if expected_plane != bindings.factors.plane {
            return Err(ResearchError::InvalidModelArtifact {
                detail: format!(
                    "CPCV factor-plane preimage differs from the exact serving contract: \
                     rebuilt {} ({} revisions), contract {} ({} revisions)",
                    expected_plane.factor_schema_hash(),
                    expected_plane.definitions().len(),
                    bindings.factors.plane.factor_schema_hash(),
                    bindings.factors.plane.definitions().len(),
                ),
            }
            .into());
        }
        Ok(())
    }

    pub(crate) fn run_binding(
        &self,
        source: &VerifiedModelServingPreimage,
        fold_calibration: &CpcvFoldCalibration,
    ) -> QuantResult<CpcvRunBinding> {
        let artifact = source.artifact();
        let contract = artifact.header().serving_contract();
        let bindings = contract.bindings();
        let dataset = source.training_dataset();
        let materialization = require_dataset_materialization(dataset)?;
        let artifact_hash = artifact.content_hash()?;
        let subject = CpcvPathSetSubject::new(
            artifact_hash,
            contract.contract_hash(),
            *materialization.dataset_hash,
            *materialization.manifest_hash,
            *materialization.artifact_bytes_hash,
            source.policy_snapshot().snapshot_hash,
        );
        let methodology = CpcvMethodologyBinding::new(
            self.config_hash()?,
            CanonicalDigest::content_hash_typed(
                "quant-pivot/cpcv-portfolio-policy",
                1,
                &self.portfolio_contexts.policy().execution_risk.portfolio,
            )
            .map_err(|error| methodology_hash_error(&error))?,
            self.replay_hash()?,
            fold_calibration.evidence.clone(),
            self.config.cpcv.trial_path(DEFAULT_SELECTION_PATH_INDEX)?,
            self.trial_grid_binding()?,
        );
        let input_hash = CanonicalDigest::content_hash_typed(
            "quant-pivot/cpcv-run-input",
            2,
            &CpcvRunHashInput {
                model_version_id: bindings.model.model_version_id,
                training_dataset_id: dataset.training_dataset_id,
                decision_policy_snapshot_id: bindings.policy_snapshot.decision_policy_snapshot_id,
                window_start: dataset.window_start,
                window_end: dataset.window_end,
                subject: &subject,
                methodology: &methodology,
            },
        )
        .map_err(|error| methodology_hash_error(&error))?;
        Ok(CpcvRunBinding {
            subject,
            methodology,
            input_hash,
        })
    }

    fn trial_grid_binding(&self) -> QuantResult<CscvTrialGridBinding> {
        let trials = self.config.trials.generate(&self.config.objective)?;
        let descriptors = trials
            .iter()
            .map(Trial::descriptor)
            .collect::<QuantResult<Vec<_>>>()?;
        CscvTrialGridBinding::try_new(self.config.pbo.block_count, descriptors).map_err(|error| {
            ResearchError::ValidationMethodology {
                detail: format!("invalid governed CSCV trial grid: {error}"),
            }
            .into()
        })
    }

    fn config_hash(&self) -> QuantResult<ContentHash> {
        let trials_hash = match &self.config.trials {
            TrialGridSpec::WeightedFactor(grid) => CanonicalDigest::content_hash_typed(
                "quant-pivot/cpcv-weighted-trials",
                1,
                &(
                    &grid.lambda_multipliers,
                    &grid.rank_loss_kinds,
                    grid.max_trials,
                ),
            ),
            TrialGridSpec::Classical(grid) => CanonicalDigest::content_hash_typed(
                "quant-pivot/cpcv-classical-trials",
                1,
                &(
                    &grid.forest_n_trees_multipliers,
                    &grid.linear_alpha_multipliers,
                    grid.max_trials,
                ),
            ),
        }
        .map_err(|error| methodology_hash_error(&error))?;
        CanonicalDigest::content_hash_typed(
            "quant-pivot/cpcv-methodology-config",
            4,
            &(
                &self.config.factors,
                &self.config.objective,
                self.config.cpcv.n_groups,
                self.config.cpcv.k_test,
                self.config.nested_estimator_holdout_bps,
                self.config.nested_estimator_min_groups,
                self.config.calibration_method,
                self.config.calibration_min_samples_isotonic,
                self.config.calibration_ci_confidence,
                self.config.purge.embargo_pct,
                self.config.purge.min_embargo_secs,
                trials_hash,
                self.config.pbo.block_count,
                self.config.dsr_significance,
                self.config.entry_max_slippage_bps,
                DEFAULT_SELECTION_PATH_INDEX,
            ),
        )
        .map_err(|error| methodology_hash_error(&error))
    }

    fn replay_hash(&self) -> QuantResult<ContentHash> {
        CanonicalDigest::content_hash_typed(
            "quant-pivot/cpcv-replay-config",
            1,
            &(
                &self.replay.features,
                &self.replay.factors,
                &self.replay.domain,
                &self.replay.data_quality,
                self.replay.bias_table.as_deref(),
            ),
        )
        .map_err(|error| methodology_hash_error(&error))
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
        let contract = input.source.artifact().header().serving_contract();
        let model_family = contract.bindings().model.model_family;
        Self::validate_family(model_family)?;
        let expected_binding = self.run_binding(&input.source, &input.fold_calibration)?;
        if expected_binding != input.binding {
            return Err(ResearchError::ValidationMethodology {
                detail: "CPCV run binding drifted after cache verification".to_owned(),
            }
            .into());
        }
        let dataset = input.source.training_dataset();
        let fold_ledger = FoldArtifactLedger::default();
        let (fold_template, replay_template) = self
            .prepare_templates(&input, fold_ledger.clone(), progress, cancel)
            .await?;
        let groups = Arc::clone(fold_template.groups());
        let selection_path = input.binding.methodology.trial_path.clone();

        let path_set = self
            .run_cpcv(
                input.path_set_id,
                &fold_template,
                &replay_template,
                &groups,
                progress,
                cancel,
            )
            .await?;

        let (matrix, trial_grid_count, subject_trial_id) = self
            .run_trials(TrialGridExecution {
                path_set_id: input.path_set_id,
                fold_template: Arc::clone(&fold_template),
                replay_template: Arc::clone(&replay_template),
                groups: Arc::clone(&groups),
                selection_path: selection_path.clone(),
                progress,
                cancel,
            })
            .await?;
        let fold_artifacts = fold_ledger.freeze()?;
        let period_length = validation_period_length(&matrix.periods)?;
        let trial_grid = input.binding.methodology.trial_grid.clone();
        if usize::try_from(trial_grid_count).ok() != Some(trial_grid.trials.len()) {
            return Err(ResearchError::ValidationMethodology {
                detail: format!(
                    "executed trial count {trial_grid_count} differs from precommitted grid {}",
                    trial_grid.trials.len()
                ),
            }
            .into());
        }
        verify_fold_function_parity(&fold_artifacts, subject_trial_id)?;
        verify_subject_trial_parity(&path_set, &matrix, &selection_path, subject_trial_id)?;

        progress.report(ResearchJobProgress::with_total(
            "finalize",
            CpcvProgress::FINALIZE.start,
            CpcvProgress::TOTAL,
        ));
        let (dsr, cscv_selection_evidence, min_track_record_length) = self
            .deps
            .compute
            .run_offline_scoped(OfflineMemory::try_gib(2)?, cancel, || {
                ensure_cpcv_not_cancelled(cancel, "final statistics start")?;
                let (dsr, cscv_selection_evidence) = compute_validation_stats(
                    &path_set,
                    &matrix,
                    &trial_grid,
                    &selection_path,
                    period_length,
                )?;
                let min_track_record_length = representative_path(&path_set)
                    .map(|path| {
                        min_trl_for_path(path, &self.config.dsr_significance, period_length)
                    })
                    .transpose()?
                    .flatten();
                ensure_cpcv_not_cancelled(cancel, "final statistics completion")?;
                Ok((dsr, cscv_selection_evidence, min_track_record_length))
            })
            .await?;
        // Bailey DSR N/V must describe the same non-redundant economic trial
        // population. Exact OOS return-column duplicates form one behavioral
        // class. N is either the conservative ceiling of the representatives'
        // average-correlation estimate or the complete behavioral-class count
        // when a no-trade representative makes Pearson undefined. V is the
        // representatives' Sharpe dispersion. Coord-search has no matching OOS
        // return columns and remains audit-only.
        let dsr_conservative_independent_trial_count = cscv_selection_evidence
            .trial_dependence
            .conservative_independent_trial_count();

        Ok(CpcvBacktestOutcome {
            path_set,
            dsr,
            cscv_selection_evidence,
            min_track_record_length,
            dsr_conservative_independent_trial_count,
            trial_grid_count,
            coord_search_effective_n: input.coord_search_effective_n,
            fold_artifacts,
            window_start: dataset.window_start,
            window_end: dataset.window_end,
        })
    }

    /// Materialize training examples + backtest ticks once, and assemble the
    /// `Arc`-shared templates every CPCV fold and trial trains/evaluates
    /// against. Dispatches on `input.model_family` to build either a
    /// `WeightedFactor` or feature-gated classical [`FoldTemplate`].
    async fn prepare_templates(
        &self,
        input: &CpcvBacktestInput,
        fold_ledger: FoldArtifactLedger,
        progress: &dyn JobProgressSink,
        cancel: &CancellationToken,
    ) -> QuantResult<(Arc<FoldTemplate>, Arc<PortfolioReplayTemplate>)> {
        let dataset = input.source.training_dataset();
        let model_spec = input.source.model_spec();
        let research_profile = input.source.profile();
        let serving_contract =
            Arc::new(input.source.artifact().header().serving_contract().clone());
        let bindings = serving_contract.bindings();
        let model_family = bindings.model.model_family;
        let label = rank_target(model_spec);
        let input_contract = &model_spec.input_contract;
        let materialization = require_dataset_materialization(dataset)?;
        self.validate_cpcv_input_contract(input_contract, materialization.feature_schema_hash)?;
        progress.report(ResearchJobProgress::with_total(
            "load",
            CpcvProgress::LOAD.start,
            CpcvProgress::TOTAL,
        ));
        let mut parquet_examples = self.decode_examples(dataset).await?;

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

        let groups: Arc<[TimelineGroup]> = build_timeline_groups(&examples, &label)?.into();
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
        progress.report(ResearchJobProgress::with_total(
            "frozen_input_ticks",
            CpcvProgress::MATERIALIZE_TICKS.start,
            CpcvProgress::TOTAL,
        ));
        let probe_runtime = ProbeRuntime::for_cpcv(
            bindings,
            *materialization.feature_schema_hash,
            input_contract.clone(),
        );
        let frozen_source = self.load_cpcv_source(dataset, research_profile).await?;
        let portfolio = self
            .portfolio_contexts
            .load_cpcv_single(
                &input.source,
                self.evaluation_frozen_at,
                self.portfolio_contexts
                    .policy()
                    .recommendation
                    .reports
                    .ad_hoc_default_top_n,
            )
            .await?;
        let ticks = self
            .deps
            .compute
            .run_offline_scoped(OfflineMemory::try_gib(6)?, cancel, || {
                frozen_ticks(FrozenTickBuild {
                    examples: &examples,
                    frozen_source: &frozen_source,
                    entry_max_slippage_bps: self.config.entry_max_slippage_bps,
                    rank_target: &label,
                    model: &probe_runtime,
                    model_run_id: &input.model_run_id,
                    portfolio: &portfolio.portfolio,
                    cancel,
                    sink: progress,
                })
            })
            .await?;
        let Some(ticks) = ticks else {
            return Err(ResearchError::Cancelled {
                detail: "cpcv backtest cancelled during tick materialization".to_owned(),
            }
            .into());
        };
        let handle = Handle::current();
        let replay_template = self.replay_template(PortfolioReplayTemplateBuild {
            dataset_id: dataset.training_dataset_id,
            decision_policy_snapshot_id: bindings.policy_snapshot.decision_policy_snapshot_id,
            model_family,
            category_scope: research_profile.spec.category,
            ticks,
            groups: Arc::clone(&groups),
            examples: Arc::clone(&examples),
            group_example_ranges: Arc::clone(&group_example_ranges),
            handle,
            model_run_id: input.model_run_id,
            scenario_methodology: portfolio.scenario_methodology,
            evaluation_frozen_at: self.evaluation_frozen_at,
        });
        #[cfg(not(feature = "ml-classical"))]
        if model_family.is_classical() {
            return Err(ResearchError::RuntimeUnavailable {
                family: model_family.to_string(),
                detail: "classical CPCV requires the `ml-classical` feature".to_owned(),
            }
            .into());
        }
        let fold_build = FoldTemplateBuild {
            examples,
            group_example_ranges,
            serving_contract,
            subject_payload: input.source.artifact().payload().clone(),
            label,
            input_contract: Arc::new(input_contract.clone()),
            groups,
            cancellation: cancellation_probe(cancel),
            fold_ledger,
            fold_calibration: input.fold_calibration.clone(),
            replay: Arc::clone(&replay_template),
        };
        #[cfg(feature = "ml-classical")]
        let fold_template = self.build_fold_template(fold_build)?;
        #[cfg(not(feature = "ml-classical"))]
        let fold_template = self.build_fold_template(fold_build)?;
        Ok((fold_template, replay_template))
    }

    async fn load_cpcv_source(
        &self,
        dataset: &TrainingDatasetInfo,
        research_profile: &ResearchProfileArtifact,
    ) -> QuantResult<FrozenSourceSlice> {
        let source_slice = dataset
            .manifest
            .as_ref()
            .ok_or_else(|| ResearchError::DatasetBuild {
                detail: "CPCV dataset has no immutable v2 manifest".to_owned(),
            })?
            .source_lineage
            .source_slice
            .clone();
        let frozen_source = SourceSliceReader::new(Arc::clone(&self.deps.artifact_store))
            .read_ref(&source_slice)
            .await?;
        dataset
            .source_lineage
            .verify_manifest(&frozen_source.manifest)
            .map_err(|error| ResearchError::DatasetBuild {
                detail: format!(
                    "CPCV Dataset source lineage differs from the verified Source Slice: {error}"
                ),
            })?;
        frozen_source
            .manifest
            .validate_for_profile(
                research_profile,
                &dataset.source_lineage.research_program_hash,
                dataset.window_start,
                dataset.window_end,
                dataset.pit_cutoff,
            )
            .map_err(|detail| ResearchError::DatasetBuild {
                detail: format!("CPCV Source Slice profile/PIT contract failed: {detail}"),
            })?;
        Ok(frozen_source)
    }

    fn replay_template(&self, input: PortfolioReplayTemplateBuild) -> Arc<PortfolioReplayTemplate> {
        let weighted =
            (input.model_family == ModelFamily::WeightedFactor).then(|| WeightedPortfolioReplay {
                factor_engine: FactorEngine::for_model_scope(
                    &self.replay.factors,
                    &self.replay.features,
                    &self.replay.domain,
                    input.category_scope,
                    self.replay.bias_table.clone(),
                ),
                factor_config: self.replay.factors.clone(),
                examples: input.examples,
                group_example_ranges: input.group_example_ranges,
            });
        Arc::new(PortfolioReplayTemplate::from_input(
            PortfolioReplayTemplateInput {
                dataset_id: input.dataset_id,
                decision_policy_snapshot_id: input.decision_policy_snapshot_id,
                ticks: input.ticks,
                groups: input.groups,
                handle: input.handle,
                model_run_id: input.model_run_id,
                weighted,
                scenario_methodology: input.scenario_methodology,
                evaluation_frozen_at: input.evaluation_frozen_at,
            },
        ))
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
    /// Classical availability is gated in [`Self::prepare_templates`]. Factor
    /// native construction also expands the complete revision-bound seed head.
    #[cfg(feature = "ml-classical")]
    fn build_fold_template(&self, build: FoldTemplateBuild) -> QuantResult<Arc<FoldTemplate>> {
        let FoldTemplateBuild {
            examples,
            group_example_ranges,
            serving_contract,
            subject_payload,
            label,
            input_contract,
            groups,
            cancellation,
            fold_ledger,
            fold_calibration,
            replay,
        } = build;
        let economic_policy = FoldEconomicPolicy {
            holdout_bps: self.config.nested_estimator_holdout_bps,
            minimum_groups: self.config.nested_estimator_min_groups,
            calibration_method: self.config.calibration_method,
            min_samples_isotonic: self.config.calibration_min_samples_isotonic,
            ci_confidence: self.config.calibration_ci_confidence,
        };
        if let ModelPayload::Classical(classical) = subject_payload {
            return Ok(Arc::new(FoldTemplate::Classical(Box::new(
                ClassicalFoldTemplate {
                    examples,
                    group_example_ranges,
                    label,
                    input_contract,
                    serving_contract,
                    kind: classical.kind,
                    multipliers: classical.multipliers,
                    substitution_rules: classical.substitution_confidence_rules,
                    schema: Arc::new(FeatureSchema::build(&self.replay.features)?),
                    groups,
                    purge: self.config.purge,
                    fold_ledger,
                    fold_calibration,
                    replay,
                    economic_policy,
                },
            ))));
        }
        let ModelPayload::WeightedFactor(weighted) = subject_payload else {
            return Err(ResearchError::SellOofEstimatorRequired.into());
        };
        let factor_plane = serving_contract.bindings().factors.plane.clone();
        let seed_head =
            FactorHeadSpec::from_config(&factor_plane, &self.config.factors.factor_head)?;
        Ok(Arc::new(FoldTemplate::WeightedFactor(Box::new(
            FoldTrainTemplate {
                examples,
                group_example_ranges,
                label,
                factor_plane,
                seed_head,
                serving_contract,
                base_objective: self.config.objective.clone(),
                factor_cross_section: self.config.factors.cross_section.clone(),
                horizon_multipliers: weighted.horizon_multipliers,
                substitution_rules: weighted.substitution_confidence_rules,
                input_contract,
                groups,
                purge: self.config.purge,
                cancellation,
                fold_ledger,
                fold_calibration,
                replay,
                economic_policy,
            },
        ))))
    }

    #[cfg(not(feature = "ml-classical"))]
    fn build_fold_template(&self, build: FoldTemplateBuild) -> QuantResult<Arc<FoldTemplate>> {
        let FoldTemplateBuild {
            examples,
            group_example_ranges,
            serving_contract,
            subject_payload,
            label,
            input_contract,
            groups,
            cancellation,
            fold_ledger,
            fold_calibration,
            replay,
        } = build;
        let economic_policy = FoldEconomicPolicy {
            holdout_bps: self.config.nested_estimator_holdout_bps,
            minimum_groups: self.config.nested_estimator_min_groups,
            calibration_method: self.config.calibration_method,
            min_samples_isotonic: self.config.calibration_min_samples_isotonic,
            ci_confidence: self.config.calibration_ci_confidence,
        };
        let ModelPayload::WeightedFactor(weighted) = subject_payload else {
            return Err(ResearchError::RuntimeUnavailable {
                family: serving_contract.bindings().model.model_family.to_string(),
                detail: "classical CPCV requires the `ml-classical` feature".to_owned(),
            }
            .into());
        };
        let factor_plane = serving_contract.bindings().factors.plane.clone();
        let seed_head =
            FactorHeadSpec::from_config(&factor_plane, &self.config.factors.factor_head)?;
        Ok(Arc::new(FoldTemplate::WeightedFactor(Box::new(
            FoldTrainTemplate {
                examples,
                group_example_ranges,
                label,
                factor_plane,
                seed_head,
                serving_contract,
                base_objective: self.config.objective.clone(),
                factor_cross_section: self.config.factors.cross_section.clone(),
                horizon_multipliers: weighted.horizon_multipliers,
                substitution_rules: weighted.substitution_confidence_rules,
                input_contract,
                groups,
                purge: self.config.purge,
                cancellation,
                fold_ledger,
                fold_calibration,
                replay,
                economic_policy,
            },
        ))))
    }

    /// Run the CPCV fold sweep in the offline pool (rayon-parallel across
    /// combinations internally).
    async fn run_cpcv(
        &self,
        path_set_id: BacktestPathSetId,
        fold_template: &Arc<FoldTemplate>,
        replay_template: &Arc<PortfolioReplayTemplate>,
        groups: &Arc<[TimelineGroup]>,
        progress: &dyn JobProgressSink,
        cancel: &CancellationToken,
    ) -> QuantResult<BacktestPathSet> {
        let fold_template = Arc::clone(fold_template);
        let replay_template = Arc::clone(replay_template);
        let groups = Arc::clone(groups);
        let cpcv_config = self.config.cpcv;
        let purge_config = self.config.purge;
        let cancellation = cancel.clone();
        let completed_folds =
            WorkCounter::try_new("cpcv_fold_evaluations", cpcv_config.combination_count()?)?;
        let completed_paths = WorkCounter::try_new("cpcv_path_replays", cpcv_config.path_count()?)?;
        let total_work = completed_folds
            .total
            .checked_add(completed_paths.total)
            .ok_or_else(|| ResearchError::ValidationMethodology {
                detail: "CPCV fold and path progress total overflowed u64".to_owned(),
            })?;
        let worker_folds = completed_folds.clone();
        let worker_paths = completed_paths.clone();
        let work = self.deps.compute.run_offline_cancellable(
            OfflineMemory::try_gib(6)?,
            cancel,
            move || {
                ensure_cpcv_not_cancelled(&cancellation, "fold sweep start")?;
                let fold_source = fold_template.subject_source();
                let replay_engine = FoldReplayEngineAdapter {
                    template: &replay_template,
                    trial_cache: None,
                };
                let fold_source = CancellableFoldSource {
                    inner: fold_source.as_ref(),
                    cancel: &cancellation,
                };
                let replay_engine = CancellableReplayEngine {
                    inner: &replay_engine,
                    cancel: &cancellation,
                    completed_folds: &worker_folds,
                    completed_paths: Some(&worker_paths),
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
            },
        );
        CpcvProgress::monitor(progress, work, || {
            ResearchJobProgress::with_total(
                format!(
                    "cpcv_work;folds={}/{};paths={}/{}",
                    completed_folds.completed(),
                    completed_folds.total,
                    completed_paths.completed(),
                    completed_paths.total,
                ),
                completed_folds.completed() + completed_paths.completed(),
                total_work,
            )
        })
        .await
    }

    /// Run the governed trial grid in the offline pool, returning the
    /// resulting performance matrix + trial count.
    async fn run_trials(
        &self,
        input: TrialGridExecution<'_>,
    ) -> QuantResult<(TrialPerformanceMatrix, u32, Option<u32>)> {
        let TrialGridExecution {
            path_set_id,
            fold_template,
            replay_template,
            groups,
            selection_path,
            progress,
            cancel,
        } = input;
        let trials = self.config.trials.generate(&self.config.objective)?;
        let subject_trial_id = match &self.config.trials {
            TrialGridSpec::WeightedFactor(_) => {
                Some(weighted_subject_trial_id(&trials, &self.config.objective)?)
            }
            TrialGridSpec::Classical(_) => None,
        };
        let trial_count =
            u32::try_from(trials.len()).map_err(|error| ResearchError::ValidationMethodology {
                detail: format!("governed trial count does not fit u32: {error}"),
            })?;
        let cpcv_config = self.config.cpcv;
        let purge_config = self.config.purge;
        let cancellation = cancel.clone();
        let expected_selection_path = cpcv_config.trial_path(selection_path.path_index)?;
        if expected_selection_path != selection_path {
            return Err(ResearchError::ValidationMethodology {
                detail: "frozen selection-path binding drifted from the CPCV partition contract"
                    .to_owned(),
            }
            .into());
        }
        let trial_path_fold_count = u64::try_from(selection_path.combination_indices.len())
            .map_err(|error| ResearchError::ValidationMethodology {
                detail: format!("trial path fold count does not fit u64: {error}"),
            })?;
        let selection_path_index = selection_path.path_index;
        let total_fold_count = u64::from(trial_count)
            .checked_mul(trial_path_fold_count)
            .ok_or_else(|| ResearchError::ValidationMethodology {
                detail: "trial-grid fold count overflowed u64".to_owned(),
            })?;
        let completed_folds =
            WorkCounter::try_new("trial_grid_fold_evaluations", total_fold_count)?;
        let completed_trials = WorkCounter::try_new("trial_grid_trials", u64::from(trial_count))?;
        let worker_folds = completed_folds.clone();
        let worker_trials = completed_trials.clone();
        let work = self.deps.compute.run_offline_cancellable(
            OfflineMemory::try_gib(6)?,
            cancel,
            move || {
                ensure_cpcv_not_cancelled(&cancellation, "trial grid start")?;
                let matrix = TrialGridRun {
                    path_set_id,
                    trials: &trials,
                    fold_template: &fold_template,
                    replay_template: &replay_template,
                    groups: &groups,
                    cpcv: cpcv_config,
                    purge: purge_config,
                    selection_path_index,
                    cancel: &cancellation,
                    completed_folds: &worker_folds,
                    completed_trials: &worker_trials,
                }
                .run()?;
                ensure_cpcv_not_cancelled(&cancellation, "trial grid completion")?;
                Ok(matrix)
            },
        );
        let matrix = CpcvProgress::monitor(progress, work, || {
            ResearchJobProgress::with_total(
                format!(
                    "trial_grid_fold_evaluations;trials={}/{}",
                    completed_trials.completed(),
                    completed_trials.total
                ),
                completed_folds.completed(),
                completed_folds.total,
            )
        })
        .await?;
        Ok((matrix, trial_count, subject_trial_id))
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

#[derive(Serialize)]
struct CpcvRunHashInput<'a> {
    model_version_id: ModelVersionId,
    training_dataset_id: TrainingDatasetId,
    decision_policy_snapshot_id: DecisionPolicySnapshotId,
    window_start: DateTime<Utc>,
    window_end: DateTime<Utc>,
    subject: &'a CpcvPathSetSubject,
    methodology: &'a CpcvMethodologyBinding,
}

fn methodology_hash_error(error: &CanonicalDigestError) -> QuantError {
    ResearchError::ValidationMethodology {
        detail: format!("hash frozen CPCV methodology: {error}"),
    }
    .into()
}

fn validated_pbo_block_count(block_count: u32) -> QuantResult<usize> {
    usize::try_from(block_count).map_err(|error| {
        ResearchError::ValidationMethodology {
            detail: format!("pbo.block_count does not fit usize: {error}"),
        }
        .into()
    })
}

fn validation_period_length(periods: &[DateTime<Utc>]) -> QuantResult<ChronoDuration> {
    let first = periods
        .first()
        .ok_or_else(|| ResearchError::ValidationMethodology {
            detail: "validation timeline has no return periods".to_owned(),
        })?;
    let last = periods
        .last()
        .ok_or_else(|| ResearchError::ValidationMethodology {
            detail: "validation timeline has no return periods".to_owned(),
        })?;
    if periods.len() < 2 || periods.windows(2).any(|pair| pair[0] >= pair[1]) {
        return Err(ResearchError::ValidationMethodology {
            detail: "validation timeline requires at least two strictly ascending periods"
                .to_owned(),
        }
        .into());
    }
    let interval_count =
        i32::try_from(periods.len() - 1).map_err(|error| ResearchError::ValidationMethodology {
            detail: format!("validation interval count does not fit i32: {error}"),
        })?;
    let period_length = last.signed_duration_since(*first) / interval_count;
    if period_length <= ChronoDuration::zero() {
        return Err(ResearchError::ValidationMethodology {
            detail: "validation timeline has a non-positive effective period length".to_owned(),
        }
        .into());
    }
    Ok(period_length)
}

/// A minimal, never-scored [`QuantModelRuntime`] used only to determine
/// [`quant_pivot_research::model::ModelRuntimeInput`]'s shape during tick
/// materialization (`build_runtime_input` consults the family and typed input
/// contract projections only — no inference). Every real CPCV fold and
/// trial builds and scores its own freshly trained runtime instead.
struct ProbeRuntime {
    model_version_id: ModelVersionId,
    model_family: ModelFamily,
    feature_schema_hash: ContentHash,
    input_contract: ModelInputContract,
}

impl ProbeRuntime {
    const fn for_cpcv(
        bindings: &ModelServingBindings,
        feature_schema_hash: ContentHash,
        input_contract: ModelInputContract,
    ) -> Self {
        Self {
            model_version_id: bindings.model.model_version_id,
            model_family: bindings.model.model_family,
            feature_schema_hash,
            input_contract,
        }
    }
}

#[async_trait]
impl QuantModelRuntime for ProbeRuntime {
    fn model_version_id(&self) -> ModelVersionId {
        self.model_version_id
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

#[derive(Clone, Default)]
struct FoldArtifactLedger {
    artifacts: Arc<Mutex<Vec<CpcvFoldArtifact>>>,
}

struct FoldFunctionHashes {
    calibration_function: ContentHash,
    scenario_economic_function: ContentHash,
    calibration_artifact: ContentHash,
    scenario_model: ContentHash,
}

impl From<&PurgedPortfolioFoldRuntime> for FoldFunctionHashes {
    fn from(runtime: &PurgedPortfolioFoldRuntime) -> Self {
        Self {
            calibration_function: runtime.calibration_function_hash,
            scenario_economic_function: runtime.scenario_economic_function_hash,
            calibration_artifact: runtime.calibration_artifact_hash,
            scenario_model: runtime.scenario.model().content_hash,
        }
    }
}

impl FoldArtifactLedger {
    fn held_out(
        test_partitions: &[usize],
        test_groups: &[usize],
    ) -> QuantResult<(ContentHash, u64, ContentHash, u64)> {
        let test_partition_count = u64::try_from(test_partitions.len()).map_err(|error| {
            ResearchError::ValidationMethodology {
                detail: format!("CPCV test partition count does not fit u64: {error}"),
            }
        })?;
        let test_group_count = u64::try_from(test_groups.len()).map_err(|error| {
            ResearchError::ValidationMethodology {
                detail: format!("CPCV test group count does not fit u64: {error}"),
            }
        })?;
        let test_partitions_hash = CanonicalDigest::content_hash_typed(
            "quant-pivot/cpcv-fold-test-partitions",
            1,
            test_partitions,
        )
        .map_err(|error| methodology_hash_error(&error))?;
        let test_groups_hash = CanonicalDigest::content_hash_typed(
            "quant-pivot/cpcv-fold-test-groups",
            1,
            test_groups,
        )
        .map_err(|error| methodology_hash_error(&error))?;
        Ok((
            test_partitions_hash,
            test_partition_count,
            test_groups_hash,
            test_group_count,
        ))
    }

    fn estimator_identity(
        identity: FoldTrainingIdentity<'_>,
    ) -> QuantResult<CpcvEstimatorIdentity> {
        let (trial, combination_index, test_partitions, test_groups) = match identity {
            FoldTrainingIdentity::Validation {
                combination_index,
                test_partitions,
                test_groups,
            } => (None, combination_index, test_partitions, test_groups),
            FoldTrainingIdentity::TrialPathValidation {
                trial_id,
                path_index,
                combination_index,
                test_partitions,
                test_groups,
            } => (
                Some((trial_id, path_index)),
                combination_index,
                test_partitions,
                test_groups,
            ),
        };
        let (test_partitions_hash, test_partition_count, test_groups_hash, test_group_count) =
            Self::held_out(test_partitions, test_groups)?;
        Ok(trial.map_or(
            CpcvEstimatorIdentity::Validation {
                combination_index,
                test_partitions_hash,
                test_partition_count,
                test_groups_hash,
                test_group_count,
            },
            |(trial_id, path_index)| CpcvEstimatorIdentity::TrialPathValidation {
                trial_id,
                path_index,
                combination_index,
                test_partitions_hash,
                test_partition_count,
                test_groups_hash,
                test_group_count,
            },
        ))
    }

    fn record(
        &self,
        identity: FoldTrainingIdentity<'_>,
        split: &NestedFoldSplit,
        artifact: &ModelArtifact,
        functions: &FoldFunctionHashes,
    ) -> QuantResult<()> {
        let model_fit_groups = &split.model.group_indices;
        let calibration_fit_groups = &split.calibration.group_indices;
        let scenario_fit_groups = &split.scenario.group_indices;
        let training_group_count = u64::try_from(model_fit_groups.len()).map_err(|error| {
            ResearchError::ValidationMethodology {
                detail: format!("CPCV fold group count does not fit u64: {error}"),
            }
        })?;
        let training_groups_hash = CanonicalDigest::content_hash_typed(
            "quant-pivot/cpcv-fold-training-groups",
            1,
            model_fit_groups,
        )
        .map_err(|error| methodology_hash_error(&error))?;
        let calibration_fit_group_count =
            u64::try_from(calibration_fit_groups.len()).map_err(|error| {
                ResearchError::ValidationMethodology {
                    detail: format!("CPCV calibration-fit group count does not fit u64: {error}"),
                }
            })?;
        let calibration_fit_groups_hash = CanonicalDigest::content_hash_typed(
            "quant-pivot/cpcv-fold-calibration-fit-groups",
            1,
            calibration_fit_groups,
        )
        .map_err(|error| methodology_hash_error(&error))?;
        let scenario_fit_group_count =
            u64::try_from(scenario_fit_groups.len()).map_err(|error| {
                ResearchError::ValidationMethodology {
                    detail: format!("CPCV scenario-fit group count does not fit u64: {error}"),
                }
            })?;
        let scenario_fit_groups_hash = CanonicalDigest::content_hash_typed(
            "quant-pivot/cpcv-fold-scenario-fit-groups",
            1,
            scenario_fit_groups,
        )
        .map_err(|error| methodology_hash_error(&error))?;
        let identity = Self::estimator_identity(identity)?;
        let serving_contract_hash = artifact.header().serving_contract().contract_hash();
        let evidence = CpcvFoldArtifact {
            identity,
            training_groups_hash,
            training_group_count,
            calibration_fit_groups_hash,
            calibration_fit_group_count,
            scenario_fit_groups_hash,
            scenario_fit_group_count,
            model_artifact_hash: artifact.content_hash()?,
            serving_contract_hash,
            model_payload_hash: artifact.payload().model_payload_hash()?,
            calibration_function_hash: functions.calibration_function,
            scenario_economic_function_hash: functions.scenario_economic_function,
            calibration_artifact_hash: functions.calibration_artifact,
            scenario_model_hash: functions.scenario_model,
        };
        self.artifacts
            .lock()
            .map_err(|_| ResearchError::ValidationMethodology {
                detail: "CPCV fold-artifact ledger mutex was poisoned".to_owned(),
            })?
            .push(evidence);
        Ok(())
    }

    fn freeze(&self) -> QuantResult<CpcvFoldArtifacts> {
        let artifacts = self
            .artifacts
            .lock()
            .map_err(|_| ResearchError::ValidationMethodology {
                detail: "CPCV fold-artifact ledger mutex was poisoned".to_owned(),
            })?
            .clone();
        CpcvFoldArtifacts::try_new(artifacts).map_err(|error| {
            ResearchError::ValidationMethodology {
                detail: format!("freeze CPCV fold-artifact ledger: {error}"),
            }
            .into()
        })
    }
}

struct FoldTemplateBuild {
    examples: Arc<[TrainingExample]>,
    group_example_ranges: Arc<[Range<usize>]>,
    serving_contract: Arc<ModelServingContract>,
    subject_payload: ModelPayload,
    label: LabelSelector,
    input_contract: Arc<ModelInputContract>,
    groups: Arc<[TimelineGroup]>,
    cancellation: CancellationProbe,
    fold_ledger: FoldArtifactLedger,
    fold_calibration: CpcvFoldCalibration,
    replay: Arc<PortfolioReplayTemplate>,
}

struct FoldArtifactInput<'a> {
    serving_contract: &'a ModelServingContract,
    payload: ModelPayload,
    input_contract_hash: ContentHash,
    input_transform_hash: ContentHash,
    training_input_hash: ContentHash,
    fold_calibration: &'a CpcvFoldCalibration,
}

impl FoldArtifactInput<'_> {
    fn seal(self) -> QuantResult<ModelArtifact> {
        let factor_plane = &self.serving_contract.bindings().factors.plane;
        let estimator = self.payload.serving_estimator_binding(factor_plane)?;
        let mut bindings = self.serving_contract.bindings().clone();
        bindings.model.estimator = estimator;
        self.fold_calibration.rebind_contract(&mut bindings)?;
        bindings.transform = ModelServingTransformBinding {
            input_contract_hash: self.input_contract_hash,
            input_transform_hash: self.input_transform_hash,
            training_input_hash: self.training_input_hash,
            training_dataset_hash: bindings.dataset.manifest.semantic_dataset_hash,
        };
        let contract = ModelServingContract::try_seal(bindings).map_err(|error| {
            ResearchError::InvalidModelArtifact {
                detail: format!("seal CPCV fold serving contract: {error}"),
            }
        })?;
        ModelArtifact::try_seal(contract, self.payload)
    }
}

/// Everything a [`FoldModelSource`] needs to train a `WeightedFactor` fold.
/// Subject and trial estimators use one prepared-fold algorithm. Subject
/// preparations are released after each of the complete CPCV combinations;
/// governed trials cache only their shared selection-path splits.
struct FoldTrainTemplate {
    examples: Arc<[TrainingExample]>,
    group_example_ranges: Arc<[Range<usize>]>,
    label: LabelSelector,
    factor_plane: FactorServingPlane,
    seed_head: FactorHeadSpec,
    serving_contract: Arc<ModelServingContract>,
    /// The governed `research.training.*` objective every CPCV fold trains
    /// against; a trial-grid trial overrides this per trial (never per fold).
    base_objective: TrainingObjectiveSpec,
    factor_cross_section: FactorCrossSectionConfig,
    horizon_multipliers: HorizonMultipliers,
    substitution_rules: SubstitutionConfidenceRules,
    input_contract: Arc<ModelInputContract>,
    groups: Arc<[TimelineGroup]>,
    /// Same purge/embargo as the outer CPCV run (trainer nested CV).
    purge: PurgeConfig,
    cancellation: CancellationProbe,
    fold_ledger: FoldArtifactLedger,
    fold_calibration: CpcvFoldCalibration,
    replay: Arc<PortfolioReplayTemplate>,
    economic_policy: FoldEconomicPolicy,
}

#[derive(Debug, Clone, Copy)]
struct FoldEconomicPolicy {
    holdout_bps: u32,
    minimum_groups: u32,
    calibration_method: CalibrationMethod,
    min_samples_isotonic: u64,
    ci_confidence: Decimal,
}

/// A family-dispatching fold template: selects which concrete
/// [`FoldModelSource`] backs CPCV/trial-grid training, based on
/// [`CpcvBacktestInput::model_family`]. Every algorithm downstream of
/// [`FoldModelSource`] (CPCV, trial-grid, DSR, PBO) is identical across
/// variants — this enum is the **only** family branch point.
enum FoldTemplate {
    WeightedFactor(Box<FoldTrainTemplate>),
    #[cfg(feature = "ml-classical")]
    Classical(Box<ClassicalFoldTemplate>),
}

impl FoldTemplate {
    const fn groups(&self) -> &Arc<[TimelineGroup]> {
        match self {
            Self::WeightedFactor(template) => &template.groups,
            #[cfg(feature = "ml-classical")]
            Self::Classical(template) => &template.groups,
        }
    }

    /// Build the subject CPCV source with the governed base estimator.
    fn subject_source(&self) -> Box<dyn FoldModelSource + '_> {
        match self {
            Self::WeightedFactor(template) => Box::new(WeightedFactorFoldSource {
                template,
                objective: &template.base_objective,
                preparation: WeightedFoldPreparation::Ephemeral,
            }),
            #[cfg(feature = "ml-classical")]
            Self::Classical(template) => Box::new(ClassicalFoldSource {
                template,
                params_override: None,
            }),
        }
    }

    /// Build one governed trial source. Weighted trials share only the immutable preparation for
    /// an identical purged model split; every objective still receives an independent exact fit.
    fn trial_source<'a>(
        &'a self,
        trial: &'a Trial,
        cache: &'a WeightedFoldPreparationCache,
    ) -> QuantResult<Box<dyn FoldModelSource + 'a>> {
        match self {
            Self::WeightedFactor(template) => {
                let objective = trial.weighted_factor_objective.as_ref().ok_or_else(|| {
                    QuantError::from(ResearchError::ValidationMethodology {
                        detail: format!(
                            "trial {} has no WeightedFactor objective override",
                            trial.trial_id
                        ),
                    })
                })?;
                Ok(Box::new(WeightedFactorFoldSource {
                    template,
                    objective,
                    preparation: WeightedFoldPreparation::Shared(cache),
                }))
            }
            #[cfg(feature = "ml-classical")]
            Self::Classical(template) => {
                let params_override = trial.classical_params.as_ref().ok_or_else(|| {
                    QuantError::from(ResearchError::ValidationMethodology {
                        detail: format!(
                            "trial {} has no classical params override",
                            trial.trial_id
                        ),
                    })
                })?;
                Ok(Box::new(ClassicalFoldSource {
                    template,
                    params_override: Some(params_override),
                }))
            }
        }
    }
}

struct WeightedPortfolioReplay {
    factor_engine: FactorEngine,
    factor_config: FactorsConfig,
    examples: Arc<[TrainingExample]>,
    group_example_ranges: Arc<[Range<usize>]>,
}

impl WeightedPortfolioReplay {
    fn model_input(
        &self,
        model: &dyn QuantModelRuntime,
        model_run_id: &ModelRunId,
        group_index: usize,
    ) -> QuantResult<ModelRuntimeInput> {
        let range = self.group_example_ranges.get(group_index).ok_or_else(|| {
            ResearchError::ValidationMethodology {
                detail: format!(
                    "weighted CPCV replay group {group_index} has no frozen example range"
                ),
            }
        })?;
        let examples = self.examples.get(range.clone()).ok_or_else(|| {
            ResearchError::ValidationMethodology {
                detail: format!(
                    "weighted CPCV replay group {group_index} has an invalid frozen example range"
                ),
            }
        })?;
        if examples.is_empty() {
            return Err(ResearchError::ValidationMethodology {
                detail: format!("weighted CPCV replay group {group_index} is empty"),
            }
            .into());
        }
        if examples.iter().any(|example| {
            example.token_id != example.selected_market.primary_token_id
                || example.market_id != example.selected_market.market_id
        }) {
            return Err(ResearchError::ValidationMethodology {
                detail: format!(
                    "weighted CPCV replay group {group_index} is not bound to canonical primary-token examples"
                ),
            }
            .into());
        }

        let runtime_plane =
            model
                .factor_serving_plane()
                .ok_or_else(|| ResearchError::InvalidModelArtifact {
                    detail: format!(
                        "weighted CPCV runtime {} has no factor serving plane",
                        model.model_version_id()
                    ),
                })?;
        if self.factor_engine.serving_plane()? != runtime_plane {
            return Err(ResearchError::InvalidModelArtifact {
                detail: format!(
                    "weighted CPCV runtime {} factor plane differs from the frozen replay engine",
                    model.model_version_id()
                ),
            }
            .into());
        }
        let references = model.frozen_reference_quantiles().ok_or_else(|| {
            ResearchError::InvalidModelArtifact {
                detail: format!(
                    "weighted CPCV runtime {} has no frozen reference distributions",
                    model.model_version_id()
                ),
            }
        })?;
        let mut factor_config = self.factor_config.clone();
        factor_config.cross_section = model.factor_cross_section().cloned().ok_or_else(|| {
            ResearchError::InvalidModelArtifact {
                detail: format!(
                    "weighted CPCV runtime {} has no cross-section transform",
                    model.model_version_id()
                ),
            }
        })?;
        let vectors = examples
            .iter()
            .map(|example| example.feature_vector.clone())
            .collect::<Vec<_>>();
        let markets = examples
            .iter()
            .map(|example| example.selected_market.clone())
            .collect::<Vec<_>>();
        let outcomes =
            self.factor_engine
                .compute_batch_with_refs(&vectors, &factor_config, references)?;
        Ok(build_runtime_input(
            model,
            model_run_id,
            examples[0].decision_at(),
            &markets,
            &vectors,
            &outcomes,
        ))
    }
}

struct PortfolioReplayTemplateBuild {
    dataset_id: TrainingDatasetId,
    decision_policy_snapshot_id: DecisionPolicySnapshotId,
    model_family: ModelFamily,
    category_scope: Option<MarketCategory>,
    ticks: Vec<BacktestTick>,
    groups: Arc<[TimelineGroup]>,
    examples: Arc<[TrainingExample]>,
    group_example_ranges: Arc<[Range<usize>]>,
    handle: Handle,
    model_run_id: ModelRunId,
    scenario_methodology: PortfolioScenarioMethodology,
    evaluation_frozen_at: DateTime<Utc>,
}

struct PortfolioReplayTemplateInput {
    dataset_id: TrainingDatasetId,
    decision_policy_snapshot_id: DecisionPolicySnapshotId,
    ticks: Vec<BacktestTick>,
    groups: Arc<[TimelineGroup]>,
    handle: Handle,
    model_run_id: ModelRunId,
    weighted: Option<WeightedPortfolioReplay>,
    scenario_methodology: PortfolioScenarioMethodology,
    evaluation_frozen_at: DateTime<Utc>,
}

struct PortfolioReplayTemplate {
    dataset_id: TrainingDatasetId,
    decision_policy_snapshot_id: DecisionPolicySnapshotId,
    ticks_by_as_of: Arc<BTreeMap<DateTime<Utc>, BacktestTick>>,
    groups: Arc<[TimelineGroup]>,
    handle: Handle,
    model_run_id: ModelRunId,
    weighted: Option<WeightedPortfolioReplay>,
    scenario_methodology: PortfolioScenarioMethodology,
    evaluation_frozen_at: DateTime<Utc>,
}

impl PortfolioReplayTemplate {
    fn from_input(input: PortfolioReplayTemplateInput) -> Self {
        Self {
            dataset_id: input.dataset_id,
            decision_policy_snapshot_id: input.decision_policy_snapshot_id,
            ticks_by_as_of: Arc::new(
                input
                    .ticks
                    .into_iter()
                    .map(|tick| (tick.decision_at, tick))
                    .collect(),
            ),
            groups: input.groups,
            handle: input.handle,
            model_run_id: input.model_run_id,
            weighted: input.weighted,
            scenario_methodology: input.scenario_methodology,
            evaluation_frozen_at: input.evaluation_frozen_at,
        }
    }

    fn tick_for(
        &self,
        group_index: usize,
        model: &dyn QuantModelRuntime,
    ) -> QuantResult<BacktestTick> {
        let group =
            self.groups
                .get(group_index)
                .ok_or_else(|| ResearchError::ValidationMethodology {
                    detail: format!("CPCV replay group index {group_index} is out of range"),
                })?;
        let mut tick = self
            .ticks_by_as_of
            .get(&group.decision_at)
            .cloned()
            .ok_or_else(|| ResearchError::ValidationMethodology {
                detail: format!(
                    "CPCV replay has no frozen economic tick for group {group_index} at {}",
                    group.decision_at
                ),
            })?;
        if let Some(weighted) = &self.weighted {
            tick.model_input = weighted.model_input(model, &self.model_run_id, group_index)?;
        }
        Ok(tick)
    }

    fn complete_fold(
        &self,
        identity: FoldTrainingIdentity<'_>,
        split: &NestedFoldSplit,
        artifact: &ModelArtifact,
        model: Box<dyn QuantModelRuntime>,
        policy: FoldEconomicPolicy,
    ) -> QuantResult<PurgedPortfolioFoldRuntime> {
        let calibration_outcomes =
            self.replay_calibration(&split.calibration.group_indices, model.as_ref())?;
        let scenario_outcomes =
            self.replay_calibration(&split.scenario.group_indices, model.as_ref())?;
        let evidence = NestedFoldEvidence::seal(
            identity,
            split,
            artifact,
            &calibration_outcomes,
            &scenario_outcomes,
        )?;
        let fitted_calibration = NestedCalibrationFitter::fit(&NestedCalibrationFitInput {
            fit_observations: &calibration_outcomes
                .iter()
                .map(|outcome| NestedCalibrationObservation {
                    composite_score: outcome.composite_score,
                    token_payout_ratio: outcome.token_payout_ratio,
                    max_adverse_excursion_bps: outcome.max_adverse_excursion_bps,
                })
                .collect::<Vec<_>>(),
            validation_observations: &scenario_outcomes
                .iter()
                .map(|outcome| NestedCalibrationObservation {
                    composite_score: outcome.composite_score,
                    token_payout_ratio: outcome.token_payout_ratio,
                    max_adverse_excursion_bps: outcome.max_adverse_excursion_bps,
                })
                .collect::<Vec<_>>(),
            policy: NestedCalibrationPolicy {
                preferred_method: policy.calibration_method,
                min_samples_isotonic: policy.min_samples_isotonic,
                ci_confidence: policy.ci_confidence,
            },
            fit_evidence_hash: evidence.calibration_fit_hash,
            validation_evidence_hash: evidence.scenario_validation_hash,
        })?;
        let calibration_function_hash = fitted_calibration.resolved.runtime_function_hash()?;
        let mut residual_observations = scenario_outcomes
            .iter()
            .map(|outcome| {
                let expected = fitted_calibration
                    .resolved
                    .calibrate_distribution(outcome.composite_score.inner())?
                    .expected_payout()
                    .inner();
                Ok(PortfolioScenarioResidualObservation {
                    decision_at: outcome.decision_at,
                    market_id: outcome.market_id.clone(),
                    token_id: outcome.token_id.clone(),
                    economic_residual: (outcome.token_payout_ratio.inner() - expected).normalize(),
                })
            })
            .collect::<QuantResult<Vec<_>>>()?;
        residual_observations.sort_by(|left, right| {
            (
                left.decision_at,
                left.market_id.as_str(),
                left.token_id.as_str(),
            )
                .cmp(&(
                    right.decision_at,
                    right.market_id.as_str(),
                    right.token_id.as_str(),
                ))
        });
        let contract = artifact.header().serving_contract();
        let bindings = contract.bindings();
        let trade_policy =
            bindings
                .trade_policy
                .as_ref()
                .ok_or_else(|| ResearchError::InvalidModelArtifact {
                    detail: "CPCV fold serving contract has no Trade Policy binding".to_owned(),
                })?;
        let tick = self.ticks_by_as_of.values().next().ok_or_else(|| {
            ResearchError::ValidationMethodology {
                detail: "CPCV replay template has no portfolio tick".to_owned(),
            }
        })?;
        let route = tick.portfolio_contract.route;
        let represented_routes = &tick.portfolio_contract.represented_routes;
        let compatibility = RouteCompatibilityDigests::try_new(
            represented_routes,
            &[RouteContractHash {
                route,
                content_hash: contract.contract_hash(),
            }],
            &[RouteContractHash {
                route,
                content_hash: fitted_calibration.content_hash,
            }],
            &[RouteContractHash {
                route,
                content_hash: trade_policy.content_hash,
            }],
        )
        .map_err(|error| ResearchError::ValidationMethodology {
            detail: format!("build fold scenario compatibility: {error}"),
        })?;
        let fitted_scenario =
            PortfolioScenarioModelFitter::fit_fold(&PortfolioScenarioFoldFitInput {
                methodology: &self.scenario_methodology,
                represented_routes,
                compatibility,
                route,
                model_version_id: artifact.header().model_version_id(),
                model_artifact_hash: artifact.content_hash()?,
                serving_contract_hash: contract.contract_hash(),
                calibration_artifact_hash: fitted_calibration.content_hash,
                calibration: &fitted_calibration.resolved,
                trade_policy_contract_hash: trade_policy.content_hash,
                prediction_horizon_secs: bindings.model.prediction_horizon_secs,
                observations: &residual_observations,
                estimator_identity_hash: evidence.estimator_identity_hash,
                model_fit_groups_hash: evidence.model_groups_hash,
                calibration_fit_groups_hash: evidence.calibration_groups_hash,
                scenario_fit_groups_hash: evidence.scenario_groups_hash,
                bound_at: self.evaluation_frozen_at,
            })?;
        let scenario = BacktestScenarioContext::try_new(
            fitted_scenario.binding,
            fitted_scenario.artifact,
            represented_routes.clone(),
        )?;
        let scenario_economic_function_hash = scenario.economic_function_hash();
        let test_groups_hash = evidence.test_groups_hash();
        let calibration = fitted_calibration.resolved;
        let calibrated_model: Box<dyn QuantModelRuntime> =
            Box::new(CrossFittedRuntime::new(model, calibration.clone()));
        Ok(PurgedPortfolioFoldRuntime {
            model: calibrated_model,
            calibration,
            calibration_artifact_hash: fitted_calibration.content_hash,
            calibration_function_hash,
            scenario,
            scenario_economic_function_hash,
            model_fit_groups_hash: evidence.model_groups_hash,
            calibration_fit_groups_hash: evidence.calibration_groups_hash,
            scenario_fit_groups_hash: evidence.scenario_groups_hash,
            test_groups_hash,
            model_fit_groups: split.model.group_indices.clone(),
            calibration_fit_groups: split.calibration.group_indices.clone(),
            scenario_fit_groups: split.scenario.group_indices.clone(),
        })
    }

    fn replay_calibration(
        &self,
        group_indices: &[usize],
        model: &dyn QuantModelRuntime,
    ) -> QuantResult<Vec<ModelCalibrationOutcome>> {
        let ticks = group_indices
            .iter()
            .map(|&group_index| {
                let tick = self.tick_for(group_index, model)?;
                Ok(CalibrationReplayTick {
                    decision_at: tick.decision_at,
                    model_input: tick.model_input,
                    outcomes: tick.outcomes,
                    downside_trajectories: tick.downside_trajectories,
                })
            })
            .collect::<QuantResult<Vec<_>>>()?;
        self.handle
            .block_on(ModelCalibrationReplay::new().run(model, ticks))
    }
}

/// [`FoldModelSource`] for the `WeightedFactor` family: filters `template`'s
/// examples down to `filter`'s groups' `as_of`s, prepares one immutable fold
/// matrix, and fits the governed objective against that exact matrix.
struct WeightedFactorFoldSource<'a> {
    template: &'a FoldTrainTemplate,
    objective: &'a TrainingObjectiveSpec,
    preparation: WeightedFoldPreparation<'a>,
}

#[derive(Clone, Copy)]
enum WeightedFoldPreparation<'a> {
    Ephemeral,
    Shared(&'a WeightedFoldPreparationCache),
}

#[derive(Default)]
struct WeightedFoldPreparationCache {
    folds: Mutex<BTreeMap<Vec<usize>, Arc<PreparedWeightedFold>>>,
}

impl WeightedFoldPreparationCache {
    fn train(
        &self,
        group_indices: &[usize],
        request: TrainModelRequest,
    ) -> QuantResult<WeightedModelTrainingOutput> {
        let objective = request.objective.clone();
        let prepared = {
            let mut folds = self.folds.lock().map_err(|_| {
                QuantError::from(ResearchError::ValidationMethodology {
                    detail: "weighted trial fold preparation cache mutex was poisoned".to_owned(),
                })
            })?;
            if let Some(prepared) = folds.get(group_indices) {
                Arc::clone(prepared)
            } else {
                let prepared = Arc::new(request.prepare_fold()?);
                folds.insert(group_indices.to_vec(), Arc::clone(&prepared));
                prepared
            }
        };
        prepared.train(&objective)
    }
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

struct NestedFoldSplit {
    model: GroupRowFilter,
    calibration: GroupRowFilter,
    scenario: GroupRowFilter,
}

struct NestedEstimatorPartition {
    calibration: Vec<usize>,
    scenario: Vec<usize>,
}

impl NestedEstimatorPartition {
    fn is_preferred_to(&self, current: &Self) -> bool {
        let population_floor = self.calibration.len().min(self.scenario.len());
        let current_floor = current.calibration.len().min(current.scenario.len());
        let imbalance = self.calibration.len().abs_diff(self.scenario.len());
        let current_imbalance = current.calibration.len().abs_diff(current.scenario.len());

        population_floor > current_floor
            || (population_floor == current_floor && imbalance < current_imbalance)
            || (population_floor == current_floor
                && imbalance == current_imbalance
                && self.scenario.len() > current.scenario.len())
    }
}

struct NestedFoldEvidence {
    estimator_identity: CpcvEstimatorIdentity,
    estimator_identity_hash: ContentHash,
    model_groups_hash: ContentHash,
    calibration_groups_hash: ContentHash,
    scenario_groups_hash: ContentHash,
    calibration_fit_hash: ContentHash,
    scenario_validation_hash: ContentHash,
}

impl NestedFoldEvidence {
    const fn test_groups_hash(&self) -> ContentHash {
        match self.estimator_identity {
            CpcvEstimatorIdentity::Validation {
                test_groups_hash, ..
            }
            | CpcvEstimatorIdentity::TrialPathValidation {
                test_groups_hash, ..
            } => test_groups_hash,
        }
    }

    fn seal(
        identity: FoldTrainingIdentity<'_>,
        split: &NestedFoldSplit,
        artifact: &ModelArtifact,
        calibration_outcomes: &[ModelCalibrationOutcome],
        scenario_outcomes: &[ModelCalibrationOutcome],
    ) -> QuantResult<Self> {
        let estimator_identity = FoldArtifactLedger::estimator_identity(identity)?;
        let estimator_identity_hash = CanonicalDigest::content_hash_typed(
            "quant-pivot/cpcv-estimator-identity",
            1,
            &estimator_identity,
        )?;
        let model_groups_hash = CanonicalDigest::content_hash_typed(
            "quant-pivot/cpcv-fold-training-groups",
            1,
            &split.model.group_indices,
        )?;
        let calibration_groups_hash = CanonicalDigest::content_hash_typed(
            "quant-pivot/cpcv-fold-calibration-fit-groups",
            1,
            &split.calibration.group_indices,
        )?;
        let scenario_groups_hash = CanonicalDigest::content_hash_typed(
            "quant-pivot/cpcv-fold-scenario-fit-groups",
            1,
            &split.scenario.group_indices,
        )?;
        let artifact_hash = artifact.content_hash()?;
        let calibration_fit_hash = CanonicalDigest::content_hash_typed(
            "quant-pivot/cpcv-nested-calibration-fit-source",
            1,
            &(
                estimator_identity_hash,
                model_groups_hash,
                calibration_groups_hash,
                artifact_hash,
                calibration_outcomes,
            ),
        )?;
        let scenario_validation_hash = CanonicalDigest::content_hash_typed(
            "quant-pivot/cpcv-nested-calibration-validation-source",
            1,
            &(
                estimator_identity_hash,
                model_groups_hash,
                calibration_groups_hash,
                scenario_groups_hash,
                artifact_hash,
                scenario_outcomes,
            ),
        )?;
        Ok(Self {
            estimator_identity,
            estimator_identity_hash,
            model_groups_hash,
            calibration_groups_hash,
            scenario_groups_hash,
            calibration_fit_hash,
            scenario_validation_hash,
        })
    }
}

/// A fitted population with only one decision time cannot demonstrate temporal
/// variation and makes the fold scenario window degenerate. This is a hard
/// methodology floor, not a tunable quality gate.
const MIN_NESTED_POPULATION_GROUPS: usize = 2;

fn nested_fold_split(
    outer_training: &GroupRowFilter,
    groups: &[TimelineGroup],
    purge: PurgeConfig,
    holdout_bps: u32,
    minimum_groups: u32,
) -> QuantResult<NestedFoldSplit> {
    let outer = validated_group_indices(outer_training, groups.len())?;
    let minimum =
        usize::try_from(minimum_groups).map_err(|error| ResearchError::ValidationMethodology {
            detail: format!("nested calibration group floor does not fit usize: {error}"),
        })?;
    let minimum_holdout = MIN_NESTED_POPULATION_GROUPS.checked_mul(2).ok_or_else(|| {
        ResearchError::ValidationMethodology {
            detail: "nested population floor overflowed usize".to_owned(),
        }
    })?;
    let minimum_outer = minimum
        .checked_add(MIN_NESTED_POPULATION_GROUPS)
        .ok_or_else(|| ResearchError::ValidationMethodology {
            detail: "nested estimator and model population floor overflowed usize".to_owned(),
        })?;
    if holdout_bps == 0
        || holdout_bps >= 10_000
        || minimum < minimum_holdout
        || outer.len() < minimum_outer
    {
        return Err(ResearchError::ValidationMethodology {
            detail: format!(
                "invalid nested estimator split: outer_groups={}, holdout_bps={holdout_bps}, minimum_groups={minimum}",
                outer.len()
            ),
        }
        .into());
    }
    let scaled = outer
        .len()
        .checked_mul(usize::try_from(holdout_bps).map_err(|error| {
            ResearchError::ValidationMethodology {
                detail: format!("nested calibration basis points do not fit usize: {error}"),
            }
        })?)
        .and_then(|value| value.checked_add(9_999))
        .ok_or_else(|| ResearchError::ValidationMethodology {
            detail: "nested calibration holdout size overflowed usize".to_owned(),
        })?
        / 10_000;
    let requested_estimator_count = scaled.max(minimum);
    let maximum_estimator_count = outer
        .len()
        .checked_sub(MIN_NESTED_POPULATION_GROUPS)
        .ok_or_else(|| ResearchError::ValidationMethodology {
            detail: "outer fold cannot retain the model population floor".to_owned(),
        })?;
    if requested_estimator_count > maximum_estimator_count {
        return Err(ResearchError::ValidationMethodology {
            detail: format!(
                "nested estimator minimum consumes the model population: outer_groups={}, requested_estimator_groups={requested_estimator_count}, maximum_estimator_groups={maximum_estimator_count}",
                outer.len(),
            ),
        }
        .into());
    }

    // A raw `2 + 2` split is not a capacity proof: a calibration label can
    // mature inside the first scenario interval and must then be purged. Walk
    // the smallest chronological estimator suffix that can satisfy the real
    // intervals. Returning from the first feasible suffix maximizes model-fit
    // history; the boundary preference below balances the two downstream
    // evidence populations and then favors more scenario residual history.
    let splitter = DefaultPurgedSplitter::new();
    let mut maximum_model_groups = 0usize;
    let mut maximum_calibration_groups = 0usize;
    let mut maximum_scenario_groups = 0usize;
    for estimator_count in requested_estimator_count..=maximum_estimator_count {
        let estimator_indices = &outer[outer.len() - estimator_count..];
        let model_purged = splitter.split(groups, estimator_indices, &purge)?;
        let model_fit_indices = model_purged
            .train_indices
            .into_iter()
            .filter(|index| outer.binary_search(index).is_ok())
            .collect::<Vec<_>>();
        maximum_model_groups = maximum_model_groups.max(model_fit_indices.len());
        if model_fit_indices.len() < MIN_NESTED_POPULATION_GROUPS {
            continue;
        }

        let last_boundary = estimator_count - MIN_NESTED_POPULATION_GROUPS;
        let mut selected: Option<NestedEstimatorPartition> = None;
        for boundary in MIN_NESTED_POPULATION_GROUPS..=last_boundary {
            let calibration_candidates = &estimator_indices[..boundary];
            let scenario_indices = &estimator_indices[boundary..];
            maximum_scenario_groups = maximum_scenario_groups.max(scenario_indices.len());
            let calibration_purged = splitter.split(groups, scenario_indices, &purge)?;
            let calibration_fit_indices = calibration_purged
                .train_indices
                .into_iter()
                .filter(|index| calibration_candidates.binary_search(index).is_ok())
                .collect::<Vec<_>>();
            maximum_calibration_groups =
                maximum_calibration_groups.max(calibration_fit_indices.len());
            if calibration_fit_indices.len() < MIN_NESTED_POPULATION_GROUPS {
                continue;
            }
            let candidate = NestedEstimatorPartition {
                calibration: calibration_fit_indices,
                scenario: scenario_indices.to_vec(),
            };
            if selected
                .as_ref()
                .is_none_or(|current| candidate.is_preferred_to(current))
            {
                selected = Some(candidate);
            }
        }
        if let Some(selected) = selected {
            return Ok(NestedFoldSplit {
                model: GroupRowFilter {
                    group_indices: model_fit_indices,
                },
                calibration: GroupRowFilter {
                    group_indices: selected.calibration,
                },
                scenario: GroupRowFilter {
                    group_indices: selected.scenario,
                },
            });
        }
    }

    Err(ResearchError::ValidationMethodology {
        detail: format!(
            "nested purge/embargo cannot retain the required disjoint temporal populations: outer_groups={}, requested_estimator_groups={requested_estimator_count}, maximum_estimator_groups={maximum_estimator_count}, maximum_model_fit_groups={maximum_model_groups}, maximum_calibration_fit_groups={maximum_calibration_groups}, maximum_scenario_fit_groups={maximum_scenario_groups}, minimum_per_population={MIN_NESTED_POPULATION_GROUPS}",
            outer.len(),
        ),
    }
    .into())
}

impl FoldModelSource for WeightedFactorFoldSource<'_> {
    fn train_fold(&self, request: FoldTrainingRequest<'_>) -> QuantResult<FoldRuntime> {
        let FoldTrainingRequest { identity, filter } = request;
        let split = nested_fold_split(
            filter,
            &self.template.groups,
            self.template.purge,
            self.template.economic_policy.holdout_bps,
            self.template.economic_policy.minimum_groups,
        )?;
        let group_indices =
            validated_group_indices(&split.model, self.template.group_example_ranges.len())?;
        let capacity = group_indices
            .iter()
            .map(|&index| self.template.group_example_ranges[index].len())
            .sum();
        let mut fold_examples = Vec::with_capacity(capacity);
        for &index in group_indices {
            let range = &self.template.group_example_ranges[index];
            fold_examples.extend_from_slice(&self.template.examples[range.clone()]);
        }
        let request = TrainModelRequest {
            cancellation: self.template.cancellation.clone(),
            examples: fold_examples.into(),
            label: self.template.label.clone(),
            factor_plane: self.template.factor_plane.clone(),
            seed_head: self.template.seed_head.clone(),
            objective: self.objective.clone(),
            validation: ValidationSpec {
                // CPCV already supplies the OOS distribution; fold training is a
                // full-window fit on the purged train subset (`folds = 1`).
                folds: 1,
                embargo_pct: self.template.purge.embargo_pct,
                min_embargo_secs: self.template.purge.min_embargo_secs,
            },
            horizon_multipliers: self.template.horizon_multipliers.clone(),
            substitution_rules: self.template.substitution_rules.clone(),
            return_model: self.template.fold_calibration.weighted_return_model()?,
            input_contract: self.template.input_contract.as_ref().clone(),
            factor_cross_section: self.template.factor_cross_section.clone(),
        };
        let trained = match self.preparation {
            WeightedFoldPreparation::Ephemeral => request.prepare_fold()?.train(self.objective)?,
            WeightedFoldPreparation::Shared(cache) => {
                cache.train(&split.model.group_indices, request)?
            }
        };
        let artifact = FoldArtifactInput {
            serving_contract: &self.template.serving_contract,
            payload: ModelPayload::WeightedFactor(Box::new(trained.payload)),
            input_contract_hash: trained.input_contract_hash,
            input_transform_hash: trained.input_transform_hash,
            training_input_hash: trained.training_input_hash,
            fold_calibration: &self.template.fold_calibration,
        }
        .seal()?;
        let runtime = WeightedFactorRuntime::new(artifact.clone(), None)?;
        let complete = self.template.replay.complete_fold(
            identity,
            &split,
            &artifact,
            Box::new(runtime),
            self.template.economic_policy,
        )?;
        self.template.fold_ledger.record(
            identity,
            &split,
            &artifact,
            &FoldFunctionHashes::from(&complete),
        )?;
        Ok(FoldRuntime::BuyPortfolio(Box::new(complete)))
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
    serving_contract: Arc<ModelServingContract>,
    kind: ClassicalKind,
    multipliers: ScoreMultiplierSpec,
    substitution_rules: SubstitutionConfidenceRules,
    schema: Arc<FeatureSchema>,
    groups: Arc<[TimelineGroup]>,
    purge: PurgeConfig,
    fold_ledger: FoldArtifactLedger,
    fold_calibration: CpcvFoldCalibration,
    replay: Arc<PortfolioReplayTemplate>,
    economic_policy: FoldEconomicPolicy,
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
    fn train_fold(&self, request: FoldTrainingRequest<'_>) -> QuantResult<FoldRuntime> {
        let FoldTrainingRequest { identity, filter } = request;
        let split = nested_fold_split(
            filter,
            &self.template.groups,
            self.template.purge,
            self.template.economic_policy.holdout_bps,
            self.template.economic_policy.minimum_groups,
        )?;
        let group_indices =
            validated_group_indices(&split.model, self.template.group_example_ranges.len())?;
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

        let prediction_horizon_secs = self
            .template
            .serving_contract
            .bindings()
            .model
            .prediction_horizon_secs;
        let output_semantics = model_training::classical_output_semantics(
            self.template.kind,
            &self.template.label,
            prediction_horizon_secs,
        )?;
        let payload = ClassicalModelPayload {
            kind: self.template.kind,
            crate_name: output.crate_name.clone(),
            crate_version: output.crate_version.clone(),
            output_semantics,
            multipliers: self.template.multipliers.clone(),
            substitution_confidence_rules: self.template.substitution_rules.clone(),
            input_contract: output.input_contract.clone(),
            // `ClassicalRuntime::load` receives these immutable bytes directly.
            // The URI still carries their exact content address; a fixed
            // placeholder would let different folds serialize to the same
            // purported artifact identity.
            serialized_model_uri: ArtifactUri::parse(format!(
                "memory://cpcv-fold/{}",
                output.model_bytes_hash
            ))
            .map_err(|error| ResearchError::ValidationMethodology {
                detail: format!("content-addressed fold artifact URI failed to parse: {error}"),
            })?,
            serialized_model_hash: output.model_bytes_hash,
            serialization_format: output.serialization_format,
            input_transform: output.input_transform.clone(),
            tree_shap: output.tree_shap.clone(),
            metrics: output.metrics.clone(),
        };
        let artifact = FoldArtifactInput {
            serving_contract: &self.template.serving_contract,
            payload: ModelPayload::Classical(Box::new(payload)),
            input_contract_hash: output.input_contract_hash,
            input_transform_hash: output.input_transform_hash,
            training_input_hash: output.training_input_hash,
            fold_calibration: &self.template.fold_calibration,
        }
        .seal()?;
        let runtime = ClassicalRuntime::load(artifact.clone(), &output.model_bytes)?;
        let complete = self.template.replay.complete_fold(
            identity,
            &split,
            &artifact,
            Box::new(runtime),
            self.template.economic_policy,
        )?;
        self.template.fold_ledger.record(
            identity,
            &split,
            &artifact,
            &FoldFunctionHashes::from(&complete),
        )?;
        Ok(FoldRuntime::BuyPortfolio(Box::new(complete)))
    }
}

/// [`ReplayEngine`] shared by the supported Buy-side families.
#[derive(Clone, Copy)]
struct TrialReplayCacheContext<'a> {
    cache: &'a TrialPathReplayCache,
    cancel: &'a CancellationToken,
}

struct FoldReplayEngineAdapter<'a> {
    template: &'a PortfolioReplayTemplate,
    trial_cache: Option<TrialReplayCacheContext<'a>>,
}

impl FoldReplayEngineAdapter<'_> {
    fn replay_digest(
        &self,
        path_index: u32,
        groups: &[TimelineGroup],
        ticks: &[PrecomputedBacktestTick],
    ) -> QuantResult<ContentHash> {
        let group_contract = groups
            .iter()
            .map(|group| (group.decision_at, group.label_horizon_end))
            .collect::<Vec<_>>();
        let tick_digests = ticks
            .iter()
            .map(PrecomputedBacktestTick::economic_replay_digest)
            .collect::<QuantResult<Vec<_>>>()?;
        CanonicalDigest::content_hash_typed(
            "quant-pivot/cpcv-trial-path-economic-replay",
            1,
            &(
                path_index,
                self.template.dataset_id,
                self.template.decision_policy_snapshot_id,
                group_contract,
                tick_digests,
            ),
        )
        .map_err(QuantError::from)
    }

    fn run_ticks(
        &self,
        path_index: u32,
        groups: &[TimelineGroup],
        ticks: Vec<PrecomputedBacktestTick>,
    ) -> QuantResult<PathEconomicReplay> {
        let model_version_id =
            ticks
                .first()
                .map(|tick| tick.model_version_id)
                .ok_or_else(|| ResearchError::ValidationMethodology {
                    detail: format!("CPCV portfolio path {path_index} has no replay ticks"),
                })?;
        if ticks
            .iter()
            .any(|input| input.model_version_id != model_version_id)
        {
            return Err(ResearchError::ValidationMethodology {
                detail: format!("CPCV portfolio path {path_index} mixes model-version identities"),
            }
            .into());
        }
        let input_evidence = ticks
            .iter()
            .map(|input| {
                let inference_hash = CanonicalDigest::content_hash_typed(
                    "quant-pivot/cpcv-path-oos-inference",
                    1,
                    &(&input.tick, &input.output),
                )?;
                Ok((
                    input.tick.decision_at,
                    inference_hash,
                    input.scenario.model().content_hash,
                ))
            })
            .collect::<QuantResult<Vec<_>>>()?;
        let replay_hash = CanonicalDigest::content_hash_typed(
            "quant-pivot/cpcv-stateful-portfolio-replay",
            1,
            &(
                path_index,
                self.template.dataset_id,
                self.template.decision_policy_snapshot_id,
                self.template.model_run_id,
                model_version_id,
                &input_evidence,
            ),
        )?;
        let window_start = groups
            .first()
            .map(|group| group.decision_at)
            .ok_or_else(|| ResearchError::ValidationMethodology {
                detail: format!("CPCV portfolio path {path_index} has no timeline groups"),
            })?;
        let window_end = groups
            .last()
            .map(|group| group.label_horizon_end)
            .ok_or_else(|| ResearchError::ValidationMethodology {
                detail: "non-empty CPCV path lost its final group".to_owned(),
            })?;
        if window_end <= window_start {
            return Err(ResearchError::ValidationMethodology {
                detail: format!("CPCV portfolio path {path_index} has non-positive replay window"),
            }
            .into());
        }
        let result =
            PortfolioReplayBacktester::new().run_precomputed(PrecomputedBacktestInputs {
                request: BacktestRequest {
                    backtest_report_id: BacktestReportId::from_content_hash(&replay_hash),
                    model_version_id,
                    dataset_id: self.template.dataset_id,
                    decision_policy_snapshot_id: self.template.decision_policy_snapshot_id,
                    window_start,
                    window_end,
                },
                ticks,
            })?;
        if result.portfolio_returns.len() != groups.len()
            || result.tick_cash_turnover.len() != groups.len()
            || result
                .portfolio_returns
                .iter()
                .zip(groups)
                .any(|(observation, group)| observation.decision_at != group.decision_at)
        {
            return Err(ResearchError::ValidationMethodology {
                detail: format!(
                    "CPCV stateful replay path {path_index} did not preserve its decision clock"
                ),
            }
            .into());
        }
        Ok(PathEconomicReplay {
            group_returns: result
                .portfolio_returns
                .iter()
                .map(|observation| observation.net_return_bps.inner() / Decimal::from(10_000))
                .collect(),
            executed_turnover: result.report.turnover,
        })
    }
}

impl ReplayEngine for FoldReplayEngineAdapter<'_> {
    fn evaluate(
        &self,
        model: &FoldRuntime,
        filter: &GroupRowFilter,
    ) -> QuantResult<Vec<GroupEvaluation>> {
        let estimator = model.as_portfolio_buy()?;
        let visibility = estimator.visibility_for(filter)?;
        evaluate_portfolio_groups(
            self.template,
            estimator.model.as_ref(),
            &estimator.scenario,
            visibility,
            &estimator.calibration,
            filter,
        )
    }

    fn replay_path(
        &self,
        path_index: u32,
        groups: &[TimelineGroup],
        evaluations: &[&GroupEvaluation],
    ) -> QuantResult<Option<PathEconomicReplay>> {
        if groups.len() != evaluations.len() || groups.is_empty() {
            return Err(ResearchError::ValidationMethodology {
                detail: format!(
                    "CPCV stateful path {path_index} has {} groups and {} evaluations",
                    groups.len(),
                    evaluations.len()
                ),
            }
            .into());
        }
        let ticks = groups
            .iter()
            .zip(evaluations)
            .map(|(group, evaluation)| {
                let replay = evaluation.portfolio_replay.clone().ok_or_else(|| {
                    ResearchError::ValidationMethodology {
                        detail: format!(
                            "CPCV portfolio path {path_index} group {} has no OOS replay input",
                            evaluation.group_index
                        ),
                    }
                })?;
                if replay.tick.decision_at != group.decision_at {
                    return Err(ResearchError::ValidationMethodology {
                        detail: format!(
                            "CPCV portfolio path {path_index} replay clock {} differs from group {}",
                            replay.tick.decision_at, group.decision_at
                        ),
                    }
                    .into());
                }
                Ok(replay)
            })
            .collect::<QuantResult<Vec<_>>>()?;
        let replay = if let Some(context) = self.trial_cache {
            let digest = self.replay_digest(path_index, groups, &ticks)?;
            context.cache.get_or_run(digest, context.cancel, || {
                self.run_ticks(path_index, groups, ticks)
            })?
        } else {
            self.run_ticks(path_index, groups, ticks)?
        };
        Ok(Some(replay))
    }
}

/// Infer fold-local OOS groups without allocating them independently. Exact
/// ranking/calibration evidence is available immediately; economic inputs stay
/// attached until φ-path reconstruction can run one self-financing ledger over
/// the complete ordered timeline.
fn evaluate_portfolio_groups(
    template: &PortfolioReplayTemplate,
    model: &dyn QuantModelRuntime,
    scenario: &BacktestScenarioContext,
    scenario_visibility: PortfolioScenarioVisibility,
    calibration: &ResolvedCalibration,
    filter: &GroupRowFilter,
) -> QuantResult<Vec<GroupEvaluation>> {
    let mut evaluations = Vec::with_capacity(filter.group_indices.len());
    for &group_index in &filter.group_indices {
        let tick = template.tick_for(group_index, model)?;
        let output = template
            .handle
            .block_on(model.infer_batch(tick.model_input.clone()))?;
        let rank_observations = rank_observations(&tick, &output)?;
        let scenario_residual = scenario_residual(&tick, &output, calibration)?;
        evaluations.push(GroupEvaluation {
            group_index,
            return_value: Decimal::ZERO,
            scenario_residual: Some(scenario_residual),
            rank_observations,
            executed_turnover: None,
            portfolio_replay: Some(PrecomputedBacktestTick {
                model_version_id: model.model_version_id(),
                tick,
                output,
                scenario: scenario.clone(),
                scenario_visibility,
            }),
        });
    }
    Ok(evaluations)
}

fn scenario_residual(
    tick: &BacktestTick,
    output: &ModelRuntimeOutput,
    calibration: &ResolvedCalibration,
) -> QuantResult<Decimal> {
    let mut outcomes = BTreeMap::new();
    for outcome in &tick.outcomes {
        if outcomes
            .insert(outcome.market_id.as_str(), outcome)
            .is_some()
        {
            return Err(ResearchError::ValidationMethodology {
                detail: format!(
                    "CPCV tick {} duplicates settlement outcome for market {}",
                    tick.decision_at, outcome.market_id
                ),
            }
            .into());
        }
    }
    let mut residual_sum = Decimal::ZERO;
    let mut residual_count = 0_u64;
    for score in &output.calibration_scores {
        let Some(yes_payout) = outcomes
            .get(score.market_id.as_str())
            .and_then(|outcome| outcome.yes_payout_ratio)
        else {
            continue;
        };
        let realized = match score.outcome_side {
            OutcomeSide::Yes => yes_payout,
            OutcomeSide::No => yes_payout.complement(),
        };
        let expected = calibration
            .calibrate_distribution(score.composite_score.inner())?
            .expected_payout()
            .inner();
        residual_sum = residual_sum
            .checked_add(realized.inner() - expected)
            .ok_or_else(|| ResearchError::ValidationMethodology {
                detail: "CPCV scenario residual sum overflowed Decimal".to_owned(),
            })?;
        residual_count =
            residual_count
                .checked_add(1)
                .ok_or_else(|| ResearchError::ValidationMethodology {
                    detail: "CPCV test scenario residual count overflowed u64".to_owned(),
                })?;
    }
    if residual_count == 0 {
        return Err(ResearchError::ValidationMethodology {
            detail: format!(
                "CPCV replay produced no allocation-independent scenario residuals at {}",
                tick.decision_at
            ),
        }
        .into());
    }
    Ok((residual_sum / Decimal::from(residual_count)).normalize())
}

fn rank_observations(
    tick: &BacktestTick,
    output: &ModelRuntimeOutput,
) -> QuantResult<Vec<RankObservation>> {
    rank_pairs(tick.decision_at, &tick.rank_targets, &output.rank_scores)
}

fn rank_pairs(
    decision_at: DateTime<Utc>,
    rank_targets: &[BacktestRankTarget],
    rank_scores: &[ModelRankScore],
) -> QuantResult<Vec<RankObservation>> {
    let mut targets = BTreeMap::new();
    for target in rank_targets {
        if targets.insert(target.market_id.as_str(), target).is_some() {
            return Err(ResearchError::ValidationMethodology {
                detail: format!(
                    "CPCV tick {} duplicates rank target for market {}",
                    decision_at, target.market_id
                ),
            }
            .into());
        }
    }
    let mut observations = Vec::new();
    for score in rank_scores {
        let Some(target) = targets.get(score.market_id.as_str()) else {
            continue;
        };
        if target.token_id != score.token_id || target.target != score.target {
            return Err(ResearchError::ValidationMethodology {
                detail: format!(
                    "CPCV rank binding mismatch for market {} at {}",
                    score.market_id, decision_at
                ),
            }
            .into());
        }
        observations.push(RankObservation {
            score: score.score,
            realized: target.realized,
        });
    }
    Ok(observations)
}

struct TrialGridRun<'a> {
    path_set_id: BacktestPathSetId,
    trials: &'a [Trial],
    fold_template: &'a FoldTemplate,
    replay_template: &'a PortfolioReplayTemplate,
    groups: &'a [TimelineGroup],
    cpcv: CpcvConfig,
    purge: PurgeConfig,
    selection_path_index: u32,
    cancel: &'a CancellationToken,
    completed_folds: &'a WorkCounter,
    completed_trials: &'a WorkCounter,
}

impl TrialGridRun<'_> {
    /// Run every trial through the same purge/embargo CPCV design as the
    /// subject, selecting the hash-bound canonical complete OOS path as that
    /// trial's [`TrialPerformanceMatrix`] column. In-sample trial evaluation is
    /// forbidden because it leaks selection bias into PBO/DSR.
    fn run(&self) -> QuantResult<TrialPerformanceMatrix> {
        let periods = self
            .groups
            .iter()
            .map(|group| group.decision_at)
            .collect::<Vec<_>>();
        let preparation_cache = WeightedFoldPreparationCache::default();
        let replay_cache = TrialPathReplayCache::default();
        let columns = self
            .trials
            .par_iter()
            .map(|trial| self.run_trial(trial, &periods, &preparation_cache, &replay_cache))
            .collect::<Vec<_>>();
        let mut trial_returns = Vec::with_capacity(columns.len());
        for column in columns {
            trial_returns.push(column?);
        }
        let trial_count = u64::try_from(self.trials.len()).map_err(|error| {
            ResearchError::ValidationMethodology {
                detail: format!("trial replay cache population does not fit u64: {error}"),
            }
        })?;
        let audit = replay_cache.audit(trial_count)?;
        info!(
            raw_trial_count = trial_count,
            computed_path_replays = audit.computed,
            reused_path_replays = audit.reused,
            "trial-grid stateful economic replay cache audited"
        );

        TrialPerformanceMatrix::from_columns(periods, &trial_returns)
    }

    fn run_trial(
        &self,
        trial: &Trial,
        periods: &[DateTime<Utc>],
        preparation_cache: &WeightedFoldPreparationCache,
        replay_cache: &TrialPathReplayCache,
    ) -> QuantResult<Vec<Decimal>> {
        ensure_cpcv_not_cancelled(self.cancel, "trial CPCV boundary")?;
        let replay_engine = FoldReplayEngineAdapter {
            template: self.replay_template,
            trial_cache: Some(TrialReplayCacheContext {
                cache: replay_cache,
                cancel: self.cancel,
            }),
        };
        let fold_source = self.fold_template.trial_source(trial, preparation_cache)?;
        let trial_source = TrialPathFoldSource {
            inner: fold_source.as_ref(),
            trial_id: trial.trial_id,
            path_index: self.selection_path_index,
        };
        let fold_source = CancellableFoldSource {
            inner: &trial_source,
            cancel: self.cancel,
        };
        let replay = CancellableReplayEngine {
            inner: &replay_engine,
            cancel: self.cancel,
            completed_folds: self.completed_folds,
            completed_paths: None,
        };
        let path = DefaultCombinatorialPurgedBacktester::new().run_path(
            CpcvRequest {
                path_set_id: self.path_set_id,
                groups: self.groups,
                cpcv: self.cpcv,
                purge: self.purge,
                fold_source: &fold_source,
                replay: &replay,
            },
            self.selection_path_index,
        )?;
        if path.path_index != self.selection_path_index {
            return Err(ResearchError::ValidationMethodology {
                detail: format!(
                    "trial {} canonical OOS path index drifted: expected {}, got {}",
                    trial.trial_id, self.selection_path_index, path.path_index
                ),
            }
            .into());
        }
        if path.decision_times != periods {
            return Err(ResearchError::ValidationMethodology {
                detail: format!(
                    "trial {} canonical OOS path period axis differs from the frozen timeline",
                    trial.trial_id
                ),
            }
            .into());
        }
        self.completed_trials.advance()?;
        Ok(path.group_returns)
    }
}

/// The CPCV path whose Sharpe is closest to the distribution median.
///
/// This remains the distributional summary path for `MinTRL` and scenario-model
/// refit. DSR/CSCV selection statistics use the independently precommitted
/// selection path instead, so the subject and trial population are the same
/// statistical functional.
fn representative_path(path_set: &BacktestPathSet) -> Option<&BacktestPath> {
    let median = path_set.sharpe_distribution.median;
    path_set
        .paths
        .iter()
        .min_by_key(|path| (path.sharpe - median).abs())
}

fn bound_selection_path<'a>(
    path_set: &'a BacktestPathSet,
    matrix: &TrialPerformanceMatrix,
    binding: &CpcvTrialPathBinding,
) -> QuantResult<&'a BacktestPath> {
    binding.validate().map_err(|error| {
        QuantError::from(ResearchError::ValidationMethodology {
            detail: format!("invalid frozen selection-path binding: {error}"),
        })
    })?;
    let path = path_set
        .paths
        .iter()
        .find(|path| path.path_index == binding.path_index)
        .ok_or_else(|| ResearchError::ValidationMethodology {
            detail: format!(
                "subject CPCV path set has no frozen selection path {}",
                binding.path_index
            ),
        })?;
    if path.decision_times != matrix.periods
        || path.group_returns.len() != matrix.periods.len()
        || path.decision_times.len() != path.group_returns.len()
    {
        return Err(ResearchError::ValidationMethodology {
            detail: format!(
                "subject selection path {} differs from the governed trial period axis",
                binding.path_index
            ),
        }
        .into());
    }
    Ok(path)
}

fn weighted_subject_trial_id(
    trials: &[Trial],
    base_objective: &TrainingObjectiveSpec,
) -> QuantResult<u32> {
    let matches = trials
        .iter()
        .filter(|trial| trial.weighted_factor_objective.as_ref() == Some(base_objective))
        .map(|trial| trial.trial_id)
        .collect::<Vec<_>>();
    match matches.as_slice() {
        [trial_id] => Ok(*trial_id),
        [] => Err(ResearchError::ValidationMethodology {
            detail: "weighted trial grid does not contain the exact serving objective".to_owned(),
        }
        .into()),
        _ => Err(ResearchError::ValidationMethodology {
            detail: format!(
                "weighted trial grid contains duplicate serving objectives at trial ids {matches:?}"
            ),
        }
        .into()),
    }
}

fn verify_fold_function_parity(
    artifacts: &CpcvFoldArtifacts,
    subject_trial_id: Option<u32>,
) -> QuantResult<()> {
    let Some(subject_trial_id) = subject_trial_id else {
        return Ok(());
    };
    for trial in artifacts.iter().filter(|artifact| {
        matches!(
            artifact.identity,
            CpcvEstimatorIdentity::TrialPathValidation { trial_id, .. }
                if trial_id == subject_trial_id
        )
    }) {
        let CpcvEstimatorIdentity::TrialPathValidation {
            combination_index, ..
        } = trial.identity
        else {
            continue;
        };
        let subject = artifacts
            .iter()
            .find(|artifact| {
                matches!(
                    artifact.identity,
                    CpcvEstimatorIdentity::Validation {
                        combination_index: subject_combination,
                        ..
                    } if subject_combination == combination_index
                )
            })
            .ok_or_else(|| ResearchError::ValidationMethodology {
                detail: format!(
                    "base trial {subject_trial_id} has no subject fold for combination {combination_index}"
                ),
            })?;

        let differing_boundary = if subject.training_groups_hash != trial.training_groups_hash
            || subject.training_group_count != trial.training_group_count
        {
            Some("model_fit_population")
        } else if subject.calibration_fit_groups_hash != trial.calibration_fit_groups_hash
            || subject.calibration_fit_group_count != trial.calibration_fit_group_count
        {
            Some("calibration_fit_population")
        } else if subject.scenario_fit_groups_hash != trial.scenario_fit_groups_hash
            || subject.scenario_fit_group_count != trial.scenario_fit_group_count
        {
            Some("scenario_fit_population")
        } else if subject.model_payload_hash != trial.model_payload_hash {
            Some("model_runtime_function")
        } else if subject.serving_contract_hash != trial.serving_contract_hash
            || subject.model_artifact_hash != trial.model_artifact_hash
        {
            Some("model_serving_contract")
        } else if subject.calibration_function_hash != trial.calibration_function_hash {
            Some("calibration_runtime_function")
        } else if subject.scenario_economic_function_hash != trial.scenario_economic_function_hash {
            Some("scenario_economic_function")
        } else {
            None
        };
        if let Some(boundary) = differing_boundary {
            return Err(ResearchError::ValidationMethodology {
                detail: format!(
                    "subject/base-trial fold functional parity failed: trial_id={subject_trial_id}, combination_index={combination_index}, first_differing_boundary={boundary}"
                ),
            }
            .into());
        }
    }
    Ok(())
}

fn verify_subject_trial_parity(
    path_set: &BacktestPathSet,
    matrix: &TrialPerformanceMatrix,
    binding: &CpcvTrialPathBinding,
    subject_trial_id: Option<u32>,
) -> QuantResult<()> {
    let Some(subject_trial_id) = subject_trial_id else {
        return Ok(());
    };
    let trial_index = usize::try_from(subject_trial_id).map_err(|error| {
        ResearchError::ValidationMethodology {
            detail: format!("subject trial id does not fit usize: {error}"),
        }
    })?;
    let path = bound_selection_path(path_set, matrix, binding)?;
    for (period, (&decision_at, expected)) in path
        .decision_times
        .iter()
        .zip(&path.group_returns)
        .enumerate()
    {
        let actual = matrix.return_at(period, trial_index).ok_or_else(|| {
            ResearchError::ValidationMethodology {
                detail: format!(
                    "subject trial {subject_trial_id} has no return at selection period {period}"
                ),
            }
        })?;
        if actual != *expected {
            return Err(ResearchError::ValidationMethodology {
                detail: format!(
                    "subject/trial selection-path parity failed at {decision_at}: \
                     subject={expected}, trial={actual}, trial_id={subject_trial_id}"
                ),
            }
            .into());
        }
    }
    Ok(())
}

fn compute_validation_stats(
    path_set: &BacktestPathSet,
    matrix: &TrialPerformanceMatrix,
    trial_grid: &CscvTrialGridBinding,
    selection_path: &CpcvTrialPathBinding,
    period_length: ChronoDuration,
) -> QuantResult<(DsrReport, CscvSelectionEvidence)> {
    let path = bound_selection_path(path_set, matrix, selection_path)?;
    let cscv_selection_evidence = analyze_selection_bias(matrix, trial_grid)?;
    let returns_period_count = u64::try_from(path.group_returns.len()).map_err(|error| {
        ResearchError::ValidationMethodology {
            detail: format!("selection path period count does not fit u64: {error}"),
        }
    })?;
    let dsr_input = DsrInput {
        observed_sharpe: path.sharpe,
        returns_period_count,
        period_length,
        skewness: stats::skewness(&path.group_returns),
        kurtosis: stats::kurtosis(&path.group_returns),
        // Bailey multiple-testing N/V: the identified estimation branch and V
        // both come from the exact governed trial matrix.
        trial_count: cscv_selection_evidence
            .trial_dependence
            .conservative_independent_trial_count(),
        trial_sharpe_variance: cscv_selection_evidence.behavioral_trial_sharpe_variance,
    };
    let dsr = dsr_input.deflated_sharpe_ratio()?;
    Ok((dsr, cscv_selection_evidence))
}

fn min_trl_for_path(
    path: &BacktestPath,
    dsr_significance: &Decimal,
    period_length: ChronoDuration,
) -> QuantResult<Option<ChronoDuration>> {
    let returns_period_count = u64::try_from(path.group_returns.len()).map_err(|error| {
        ResearchError::ValidationMethodology {
            detail: format!("representative path period count does not fit u64: {error}"),
        }
    })?;
    let input = DsrInput {
        observed_sharpe: path.sharpe,
        returns_period_count,
        period_length,
        skewness: stats::skewness(&path.group_returns),
        kurtosis: stats::kurtosis(&path.group_returns),
        trial_count: 1,
        trial_sharpe_variance: Decimal::ZERO,
    };
    min_track_record_length(&input, *dsr_significance)
}

#[cfg(test)]
mod tests {
    use std::{
        sync::{
            Arc, Barrier, Mutex,
            atomic::{AtomicU64, Ordering},
        },
        thread,
        time::Duration as StdDuration,
    };

    use chrono::{Duration, TimeZone, Utc};
    use quant_pivot_error::{QuantError, QuantResult, research::ResearchError};
    use quant_pivot_models::{
        domain::quant::JobProgressSink,
        enums::model::ModelFamily,
        types::{
            BacktestPathSetId, ContentHash, MarketId, ResearchJobProgress, TokenId,
            backtest::{BacktestPath, CpcvTrialPathBinding, SharpeDistribution},
        },
    };
    use quant_pivot_research::{
        backtest::BacktestRankTarget,
        model::{ModelRankScore, ModelRankTarget},
        training::TOKEN_PAYOUT_RATIO,
        validation::{
            BacktestPathSet, DefaultPurgedSplitter, FoldRuntime, GroupEvaluation, GroupRowFilter,
            PathEconomicReplay, PurgeConfig, PurgedSplitter, ReplayEngine, TimelineGroup,
            TrialPerformanceMatrix,
        },
    };
    use rust_decimal::Decimal;
    use rust_decimal_macros::dec;
    use tokio::task::yield_now;
    use tokio_util::sync::CancellationToken;

    use super::{
        CancellableReplayEngine, CpcvBacktestService, CpcvProgress, TrialPathReplayCache,
        TrialReplayCacheAudit, WorkCounter, nested_fold_split, rank_pairs, validated_group_indices,
        validation_period_length, verify_subject_trial_parity,
    };

    #[derive(Default)]
    struct RecordingProgress {
        snapshots: Mutex<Vec<ResearchJobProgress>>,
    }

    impl JobProgressSink for RecordingProgress {
        fn report(&self, progress: ResearchJobProgress) {
            self.snapshots
                .lock()
                .expect("progress test mutex")
                .push(progress);
        }
    }

    struct EmptyReplay;

    impl ReplayEngine for EmptyReplay {
        fn evaluate(
            &self,
            _model: &FoldRuntime,
            _filter: &GroupRowFilter,
        ) -> QuantResult<Vec<GroupEvaluation>> {
            Ok(Vec::new())
        }

        fn replay_path(
            &self,
            _path_index: u32,
            _groups: &[TimelineGroup],
            _evaluations: &[&GroupEvaluation],
        ) -> QuantResult<Option<PathEconomicReplay>> {
            Ok(None)
        }
    }

    fn cached_replay() -> PathEconomicReplay {
        PathEconomicReplay {
            group_returns: vec![dec!(0.01), dec!(-0.005)],
            executed_turnover: dec!(0.125),
        }
    }

    #[test]
    fn replay_cache_singleflights() {
        const CALLERS: usize = 8;
        let cache = Arc::new(TrialPathReplayCache::default());
        let calls = Arc::new(AtomicU64::new(0));
        let barrier = Arc::new(Barrier::new(CALLERS));
        let key = ContentHash::from_bytes([0x41; 32]);
        let workers = (0..CALLERS)
            .map(|_| {
                let cache = Arc::clone(&cache);
                let calls = Arc::clone(&calls);
                let barrier = Arc::clone(&barrier);
                thread::spawn(move || {
                    let cancel = CancellationToken::new();
                    barrier.wait();
                    cache
                        .get_or_run(key, &cancel, || {
                            calls.fetch_add(1, Ordering::Relaxed);
                            thread::sleep(StdDuration::from_millis(50));
                            Ok(cached_replay())
                        })
                        .expect("singleflight replay")
                })
            })
            .collect::<Vec<_>>();
        for worker in workers {
            assert_eq!(worker.join().expect("singleflight worker"), cached_replay());
        }

        assert_eq!(calls.load(Ordering::Relaxed), 1);
        let audit = cache
            .audit(u64::try_from(CALLERS).expect("caller count"))
            .expect("singleflight audit");
        assert_eq!(
            audit,
            TrialReplayCacheAudit {
                computed: 1,
                reused: 7,
            }
        );
    }

    #[test]
    fn replay_cache_separates_keys() {
        let cache = TrialPathReplayCache::default();
        let cancel = CancellationToken::new();
        let calls = AtomicU64::new(0);
        for byte in [0x51, 0x52] {
            let replay = cache
                .get_or_run(ContentHash::from_bytes([byte; 32]), &cancel, || {
                    calls.fetch_add(1, Ordering::Relaxed);
                    Ok(cached_replay())
                })
                .expect("distinct replay");
            assert_eq!(replay, cached_replay());
        }

        assert_eq!(calls.load(Ordering::Relaxed), 2);
        let audit = cache.audit(2).expect("distinct-key audit");
        assert_eq!(
            audit,
            TrialReplayCacheAudit {
                computed: 2,
                reused: 0,
            }
        );
    }

    #[test]
    fn replay_cache_retries_abort() {
        let cache = TrialPathReplayCache::default();
        let cancel = CancellationToken::new();
        let key = ContentHash::from_bytes([0x61; 32]);
        let failed = cache.get_or_run(key, &cancel, || {
            Err(ResearchError::ValidationMethodology {
                detail: "injected replay failure".to_owned(),
            }
            .into())
        });
        assert!(failed.is_err());

        assert_eq!(
            cache
                .get_or_run(key, &cancel, || Ok(cached_replay()))
                .expect("retry replay"),
            cached_replay()
        );
        let audit = cache.audit(1).expect("retry audit");
        assert_eq!(
            audit,
            TrialReplayCacheAudit {
                computed: 1,
                reused: 0,
            }
        );
    }

    fn separated_timeline(group_count: usize) -> Vec<TimelineGroup> {
        let origin = Utc
            .timestamp_opt(1_700_000_000, 0)
            .single()
            .expect("valid timeline origin");
        (0..group_count)
            .map(|index| {
                let decision_at =
                    origin + Duration::hours(i64::try_from(index).expect("group index fits i64"));
                TimelineGroup {
                    decision_at,
                    label_horizon_end: decision_at + Duration::minutes(10),
                }
            })
            .collect()
    }

    #[tokio::test]
    async fn progress_tracks_exact_units() {
        let sink = RecordingProgress::default();
        let progress = WorkCounter::try_new("folds", 2).expect("valid exact-work counter");
        let worker = progress.clone();

        CpcvProgress::monitor(
            &sink,
            async move {
                worker.advance()?;
                yield_now().await;
                worker.advance()?;
                Ok::<(), QuantError>(())
            },
            || progress.snapshot(),
        )
        .await
        .expect("monitored work");

        let snapshots = sink.snapshots.lock().expect("progress test mutex");
        assert_eq!(
            snapshots.first(),
            Some(&ResearchJobProgress::with_total("folds", 0, 2))
        );
        assert_eq!(
            snapshots.last(),
            Some(&ResearchJobProgress::with_total("folds", 2, 2))
        );
        assert!(
            snapshots
                .windows(2)
                .all(|window| window[0].processed <= window[1].processed)
        );
        drop(snapshots);
        assert!(progress.advance().is_err());
    }

    #[test]
    fn path_progress_is_exact() -> QuantResult<()> {
        let folds = WorkCounter::try_new("folds", 1)?;
        let paths = WorkCounter::try_new("paths", 1)?;
        let cancel = CancellationToken::new();
        let replay = CancellableReplayEngine {
            inner: &EmptyReplay,
            cancel: &cancel,
            completed_folds: &folds,
            completed_paths: Some(&paths),
        };

        assert_eq!(replay.replay_path(0, &[], &[])?, None);
        assert_eq!(folds.completed(), 0);
        assert_eq!(paths.completed(), 1);
        assert!(replay.replay_path(1, &[], &[]).is_err());
        Ok(())
    }

    #[test]
    fn subject_trial_matches_path() -> QuantResult<()> {
        let periods = [
            Utc.timestamp_opt(1_700_000_000, 0)
                .single()
                .expect("first period"),
            Utc.timestamp_opt(1_700_003_600, 0)
                .single()
                .expect("second period"),
        ];
        let selection_returns = vec![dec!(0.01), dec!(-0.005)];
        let path_set = BacktestPathSet {
            path_set_id: BacktestPathSetId::from_v7(),
            paths: vec![
                BacktestPath {
                    path_index: 0,
                    decision_times: periods.to_vec(),
                    group_returns: selection_returns.clone(),
                    scenario_residuals: vec![None, None],
                    sharpe: dec!(0.25),
                    rank_ic: dec!(0.1),
                    max_drawdown: dec!(0.005),
                    tail_loss: dec!(-0.005),
                    turnover: Some(dec!(0.1)),
                },
                BacktestPath {
                    path_index: 1,
                    decision_times: periods.to_vec(),
                    group_returns: vec![dec!(0.02), dec!(0.01)],
                    scenario_residuals: vec![None, None],
                    sharpe: dec!(2),
                    rank_ic: dec!(0.2),
                    max_drawdown: Decimal::ZERO,
                    tail_loss: dec!(0.01),
                    turnover: Some(dec!(0.1)),
                },
            ],
            combination_count: 2,
            sharpe_distribution: SharpeDistribution {
                min: dec!(0.25),
                p25: dec!(0.5),
                median: dec!(2),
                p75: dec!(2),
                max: dec!(2),
                median_max_drawdown: Some(Decimal::ZERO),
                median_tail_loss: Some(dec!(0.01)),
                median_turnover: Some(dec!(0.1)),
                baseline_uplift: None,
            },
            median_rank_ic: dec!(0.15),
        };
        let binding = CpcvTrialPathBinding::try_new(0, vec![0]).expect("selection-path binding");
        let matrix = TrialPerformanceMatrix::from_columns(
            periods.to_vec(),
            &[selection_returns, vec![dec!(0.02), dec!(0.01)]],
        )?;

        verify_subject_trial_parity(&path_set, &matrix, &binding, Some(0))?;
        let mismatched = TrialPerformanceMatrix::from_columns(
            periods.to_vec(),
            &[vec![dec!(0.01), dec!(0.005)], vec![dec!(0.02), dec!(0.01)]],
        )?;
        assert!(verify_subject_trial_parity(&path_set, &mismatched, &binding, Some(0)).is_err());
        Ok(())
    }

    fn rolling_timeline(group_count: usize) -> Vec<TimelineGroup> {
        assert!(group_count > 1);
        let origin = Utc
            .timestamp_opt(1_700_000_000, 0)
            .single()
            .expect("valid timeline origin");
        let divisor = i32::try_from(group_count + 1).expect("timeline divisor");
        let step = Duration::days(90) / divisor;
        let decisions = (0..group_count)
            .map(|index| origin + step * i32::try_from(index + 1).expect("timeline group ordinal"))
            .collect::<Vec<_>>();
        decisions
            .iter()
            .enumerate()
            .map(|(index, &decision_at)| TimelineGroup {
                decision_at,
                // The production closure fixture rolls its market universe by
                // 50% at every tick. Half of this group's labels therefore
                // mature with the next tick, not in isolated two-tick pairs.
                label_horizon_end: decisions.get(index + 1).copied().unwrap_or(decision_at)
                    + Duration::days(1),
            })
            .collect()
    }

    #[test]
    fn event_period_elapsed_mean() {
        let periods = [
            Utc.timestamp_opt(0, 0).single().expect("first period"),
            Utc.timestamp_opt(60, 0).single().expect("second period"),
            Utc.timestamp_opt(240, 0).single().expect("third period"),
        ];
        assert_eq!(
            validation_period_length(&periods).expect("effective period length"),
            Duration::seconds(120)
        );
    }

    #[test]
    fn event_period_rejects_duplicates() {
        let period = Utc.timestamp_opt(0, 0).single().expect("period");
        assert!(validation_period_length(&[period, period]).is_err());
    }

    #[test]
    fn precomputed_group_unique_bounded() {
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

    #[test]
    fn nested_split_is_disjoint() {
        let groups = separated_timeline(8);
        let outer_training = GroupRowFilter {
            group_indices: (0..8).collect(),
        };

        let first = nested_fold_split(
            &outer_training,
            &groups,
            PurgeConfig::pct_only(dec!(0)),
            2_500,
            4,
        )
        .expect("valid nested calibration split");
        let repeated = nested_fold_split(
            &outer_training,
            &groups,
            PurgeConfig::pct_only(dec!(0)),
            2_500,
            4,
        )
        .expect("deterministic nested calibration split");

        assert_eq!(first.model.group_indices, vec![0, 1, 2, 3]);
        assert_eq!(first.model.group_indices, repeated.model.group_indices);
        assert_eq!(
            first.calibration.group_indices,
            repeated.calibration.group_indices
        );
        assert_eq!(
            first.scenario.group_indices,
            repeated.scenario.group_indices
        );
        assert!(first.model.group_indices.iter().all(|index| {
            first
                .calibration
                .group_indices
                .binary_search(index)
                .is_err()
                && first.scenario.group_indices.binary_search(index).is_err()
        }));
        assert_eq!(first.calibration.group_indices, vec![4, 5]);
        assert_eq!(first.scenario.group_indices, vec![6, 7]);
    }

    #[test]
    fn nested_split_purges_overlap() {
        let mut groups = separated_timeline(8);
        groups[3].label_horizon_end = groups[4].decision_at + Duration::minutes(5);
        let outer_training = GroupRowFilter {
            group_indices: (0..8).collect(),
        };

        let split = nested_fold_split(
            &outer_training,
            &groups,
            PurgeConfig::pct_only(dec!(0)),
            5_000,
            4,
        )
        .expect("purged nested calibration split");

        assert_eq!(split.model.group_indices, vec![0, 1, 2]);
        assert_eq!(split.calibration.group_indices, vec![4, 5]);
        assert_eq!(split.scenario.group_indices, vec![6, 7]);
    }

    #[test]
    fn nested_split_handles_purge() {
        let mut groups = separated_timeline(8);
        groups[5].label_horizon_end = groups[6].decision_at + Duration::minutes(5);
        let outer_training = GroupRowFilter {
            group_indices: (0..8).collect(),
        };

        let split = nested_fold_split(
            &outer_training,
            &groups,
            PurgeConfig::pct_only(dec!(0)),
            5_000,
            4,
        )
        .expect("label-aware estimator expansion");

        assert_eq!(split.model.group_indices, vec![0, 1, 2]);
        assert_eq!(split.calibration.group_indices, vec![3, 4]);
        assert_eq!(split.scenario.group_indices, vec![6, 7]);
    }

    #[test]
    fn nested_split_rejects_contracts() {
        let groups = separated_timeline(4);
        let outer_training = GroupRowFilter {
            group_indices: (0..4).collect(),
        };

        for (holdout_bps, minimum_groups) in [
            (0, 4),
            (10_000, 4),
            (2_500, 0),
            (2_500, 1),
            (2_500, 2),
            (2_500, 3),
            (2_500, 4),
        ] {
            assert!(
                nested_fold_split(
                    &outer_training,
                    &groups,
                    PurgeConfig::pct_only(dec!(0)),
                    holdout_bps,
                    minimum_groups,
                )
                .is_err(),
                "holdout_bps={holdout_bps}, minimum_groups={minimum_groups} must fail closed"
            );
        }
    }

    #[test]
    fn default_nested_has_capacity() {
        const PARTITION_COUNT: usize = 8;
        const GROUPS_PER_PARTITION: usize = 4;
        let groups = rolling_timeline(PARTITION_COUNT * GROUPS_PER_PARTITION);
        let purge = PurgeConfig {
            embargo_pct: dec!(0.02),
            min_embargo_secs: 604_800,
        };
        let splitter = DefaultPurgedSplitter::new();
        let mut fold_count = 0;

        for first in 0..PARTITION_COUNT - 2 {
            for second in first + 1..PARTITION_COUNT - 1 {
                for third in second + 1..PARTITION_COUNT {
                    let test_indices = [first, second, third]
                        .into_iter()
                        .flat_map(|partition| {
                            let start = partition * GROUPS_PER_PARTITION;
                            start..start + GROUPS_PER_PARTITION
                        })
                        .collect::<Vec<_>>();
                    let outer = splitter
                        .split(&groups, &test_indices, &purge)
                        .expect("outer purged split");
                    let nested = nested_fold_split(
                        &GroupRowFilter {
                            group_indices: outer.train_indices,
                        },
                        &groups,
                        purge,
                        2_000,
                        4,
                    )
                    .expect("all production-topology CPCV folds retain nested populations");
                    assert!(nested.model.group_indices.len() >= 2);
                    assert!(nested.calibration.group_indices.len() >= 2);
                    assert!(nested.scenario.group_indices.len() >= 2);
                    fold_count += 1;
                }
            }
        }

        assert_eq!(fold_count, 56);
    }

    #[test]
    fn rank_uses_canonical_scores() {
        let decision_at = Utc
            .timestamp_opt(1_700_000_000, 0)
            .single()
            .expect("valid decision time");
        let targets = [
            BacktestRankTarget {
                market_id: MarketId::new("winner"),
                token_id: TokenId::new("winner-yes"),
                target: ModelRankTarget {
                    label_name: TOKEN_PAYOUT_RATIO,
                    label_horizon_secs: 0,
                },
                realized: dec!(1),
            },
            BacktestRankTarget {
                market_id: MarketId::new("loser"),
                token_id: TokenId::new("loser-yes"),
                target: ModelRankTarget {
                    label_name: TOKEN_PAYOUT_RATIO,
                    label_horizon_secs: 0,
                },
                realized: dec!(0),
            },
        ];
        let scores = [
            ModelRankScore {
                market_id: MarketId::new("winner"),
                token_id: TokenId::new("winner-yes"),
                score: dec!(0.9),
                target: ModelRankTarget {
                    label_name: TOKEN_PAYOUT_RATIO,
                    label_horizon_secs: 0,
                },
            },
            ModelRankScore {
                market_id: MarketId::new("loser"),
                token_id: TokenId::new("loser-yes"),
                score: dec!(-0.9),
                target: ModelRankTarget {
                    label_name: TOKEN_PAYOUT_RATIO,
                    label_horizon_secs: 0,
                },
            },
        ];

        let observations = rank_pairs(decision_at, &targets, &scores).expect("rank observations");
        assert_eq!(observations.len(), 2);
        assert_eq!(observations[0].score, dec!(0.9));
        assert_eq!(observations[0].realized, dec!(1));
        assert_eq!(observations[1].score, dec!(-0.9));
        assert_eq!(observations[1].realized, dec!(0));
    }

    #[test]
    fn sell_requires_oof_estimator() {
        let error = CpcvBacktestService::validate_family(ModelFamily::HoldVsExitWeighted)
            .expect_err("unfitted Sell CPCV must fail before dataset or trial work");
        let QuantError::Research(error) = error else {
            panic!("expected typed research error");
        };
        assert!(matches!(error, ResearchError::SellOofEstimatorRequired));
    }
}
