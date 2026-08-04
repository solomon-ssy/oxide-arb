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
    ops::Range,
    sync::{Arc, Mutex},
};

use async_trait::async_trait;
use chrono::{DateTime, Duration as ChronoDuration, Utc};
use quant_pivot_compute::{ComputeExecutor, OfflineMemory};
use quant_pivot_error::{
    QuantError, QuantResult, hashing::CanonicalDigestError, research::ResearchError,
};
use quant_pivot_models::{
    domain::quant::{JobProgressSink, ModelSpecInfo, ModelVersionInfo, TrainingDatasetInfo},
    enums::{common::MarketCategory, model::ModelFamily, quant::CalibrationKind},
    hashing::CanonicalDigest,
    runtime_config::{
        DecimalValue, DecisionPolicySnapshot, FactorCrossSectionConfig, PortfolioConfig,
        sections::FactorsConfig,
    },
    types::{
        BacktestPathSetId, BacktestReportId, Bps, ContentHash, DecisionPolicySnapshotId,
        ModelInputContract, ModelRunId, ModelVersionId, ResearchJobProgress, TrainingDatasetId,
        backtest::{
            BacktestPath, CpcvEstimatorIdentity, CpcvFoldArtifact, CpcvFoldArtifacts,
            CpcvFoldCalibrationPolicy, CpcvMethodologyBinding, CpcvPathSetSubject,
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
        BacktestInputs, BacktestRequest, BacktestTick, Backtester, ModelRankOutcome, PortfolioCaps,
        PortfolioReplayBacktester, sharpe_ratio,
    },
    factors::FactorEngine,
    features::FeatureSchema,
    hashing::ResearchHasher,
    model::{
        CancellationProbe, HorizonMultipliers, LabelSelector, ModelArtifact, ModelRuntimeInput,
        ModelRuntimeOutput, ModelTrainer, QuantModelRuntime, ReturnModelSpec,
        SubstitutionConfidenceRules, TrainModelRequest, ValidationSpec, WeightedFactorRuntime,
        WeightedFactorTrainer, artifact::ModelPayload, factor_heads::FactorHeadSpec,
        objective::runtime_training_objective,
    },
    selection::ModelFeatureRequirements,
    stats,
    training::{LabelName, TrainingExample, TrainingLabel},
    validation::{
        BacktestPathSet, ClassicalTrialGrid, CombinatorialPurgedBacktester, CpcvConfig,
        CpcvRequest, DefaultCombinatorialPurgedBacktester, DsrInput, DsrReport, FoldModelSource,
        FoldRuntime, FoldTrainingIdentity, FoldTrainingRequest, GroupEvaluation, GroupRowFilter,
        PboInput, PurgeConfig, RankObservation, ReplayEngine, TimelineGroup, Trial, TrialGridSpec,
        TrialPerformanceMatrix, WeightedFactorTrialGrid, min_track_record_length,
        probability_of_backtest_overfitting,
    },
};
use rayon::prelude::*;
use rust_decimal::Decimal;
use serde::Serialize;
use tokio::runtime::Handle;
use tokio_util::sync::CancellationToken;

#[cfg(feature = "ml-classical")]
use crate::service::model_training;
use crate::{
    prefetch::source_slice::SourceSliceReader,
    projection::inference_batch::build_runtime_input,
    service::{
        backtest::{FrozenTickBuild, frozen_ticks},
        historical_replay::ReplayConfig,
        model_serving_preimage::VerifiedModelServingPreimage,
        training_dataset::{require_dataset_materialization, verify_frozen_dataset_artifact},
    },
};

/// Coarse CPCV job progress budget (units of work for `ResearchJobProgress`).
///
/// Progress stages are sequential; each reports `with_total(..., TOTAL)` so the
/// UI can show a determinate percentage plus a named stage.
struct CpcvProgress;

fn rank_target(model_spec: &ModelSpecInfo) -> LabelSelector {
    LabelSelector {
        name: LabelName::new(model_spec.training_contract.target_label_name.clone()),
        horizon_secs: model_spec.training_contract.target_label_horizon_secs,
    }
}

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
    fn train_fold(&self, request: FoldTrainingRequest<'_>) -> QuantResult<FoldRuntime> {
        ensure_cpcv_not_cancelled(self.cancel, "fold train boundary")?;
        let model = self.inner.train_fold(request)?;
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
    pub pbo: Decimal,
    pub min_track_record_length: Option<ChronoDuration>,
    /// DSR multiple-testing N (= `trial_grid_count`). Same population as V.
    pub trial_count: u32,
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
    caps: PortfolioCaps,
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
        let expected_caps = PortfolioCaps::try_from(&runtime.execution_risk.portfolio)?;
        if self.config != expected_config || self.caps != expected_caps {
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
            CanonicalDigest::content_hash_typed("quant-pivot/cpcv-portfolio-caps", 1, &self.caps)
                .map_err(|error| methodology_hash_error(&error))?,
            self.replay_hash()?,
            fold_calibration.evidence.clone(),
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
            1,
            &(
                &self.config.factors,
                &self.config.objective,
                self.config.cpcv.n_groups,
                self.config.cpcv.k_test,
                self.config.purge.embargo_pct,
                self.config.purge.min_embargo_secs,
                trials_hash,
                self.config.pbo.block_count,
                self.config.dsr_significance,
                self.config.entry_max_slippage_bps,
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

        progress.report(ResearchJobProgress::with_total(
            "cpcv",
            CpcvProgress::CPCV.start,
            CpcvProgress::TOTAL,
        ));
        let path_set = self
            .run_cpcv(
                input.path_set_id,
                &fold_template,
                &replay_template,
                &groups,
                cancel,
            )
            .await?;

        progress.report(ResearchJobProgress::with_total(
            "trial_grid",
            CpcvProgress::TRIAL_GRID.start,
            CpcvProgress::TOTAL,
        ));
        let (matrix, trial_grid_count) = self
            .run_trials(&fold_template, &replay_template, &groups, cancel)
            .await?;
        let fold_artifacts = fold_ledger.freeze()?;
        let period_length = validation_period_length(&matrix.periods)?;

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
                    &path_set,
                    &matrix,
                    trial_grid_count,
                    &self.config,
                    period_length,
                )?;
                let min_track_record_length = representative_path(&path_set)
                    .map(|path| {
                        min_trl_for_path(path, &self.config.dsr_significance, period_length)
                    })
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
            model_family,
            category_scope: research_profile.spec.category,
            ticks,
            groups: Arc::clone(&groups),
            examples: Arc::clone(&examples),
            group_example_ranges: Arc::clone(&group_example_ranges),
            handle: handle.clone(),
            model_run_id: input.model_run_id,
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
            handle,
            cancellation: cancellation_probe(cancel),
            fold_ledger,
            fold_calibration: input.fold_calibration.clone(),
        };
        #[cfg(feature = "ml-classical")]
        let fold_template = self.build_fold_template(fold_build)?;
        #[cfg(not(feature = "ml-classical"))]
        let fold_template = self.build_fold_template(fold_build)?;
        Ok((fold_template, replay_template))
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
                ticks: input.ticks,
                groups: input.groups,
                caps: self.caps.clone(),
                handle: input.handle,
                model_run_id: input.model_run_id,
                weighted,
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
            handle,
            cancellation,
            fold_ledger,
            fold_calibration,
        } = build;
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
                    fold_ledger,
                    fold_calibration,
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
                handle,
                cancellation,
                fold_ledger,
                fold_calibration,
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
            handle,
            cancellation,
            fold_ledger,
            fold_calibration,
        } = build;
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
                handle,
                cancellation,
                fold_ledger,
                fold_calibration,
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
        replay_template: &Arc<PortfolioReplayTemplate>,
        groups: &Arc<[TimelineGroup]>,
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
                    &cancellation,
                )?;
                ensure_cpcv_not_cancelled(&cancellation, "trial grid completion")?;
                Ok(matrix)
            })
            .await?;
        Ok((matrix, trial_count))
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

impl FoldArtifactLedger {
    fn record(
        &self,
        identity: FoldTrainingIdentity<'_>,
        group_indices: &[usize],
        artifact: &ModelArtifact,
    ) -> QuantResult<()> {
        let training_group_count = u64::try_from(group_indices.len()).map_err(|error| {
            ResearchError::ValidationMethodology {
                detail: format!("CPCV fold group count does not fit u64: {error}"),
            }
        })?;
        let training_groups_hash = CanonicalDigest::content_hash_typed(
            "quant-pivot/cpcv-fold-training-groups",
            1,
            group_indices,
        )
        .map_err(|error| methodology_hash_error(&error))?;
        let identity = match identity {
            FoldTrainingIdentity::Validation {
                combination_index,
                test_partitions,
                test_groups,
            } => {
                let test_partition_count =
                    u64::try_from(test_partitions.len()).map_err(|error| {
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
                CpcvEstimatorIdentity::Validation {
                    combination_index,
                    test_partitions_hash,
                    test_partition_count,
                    test_groups_hash,
                    test_group_count,
                }
            }
            FoldTrainingIdentity::Trial { trial_id } => CpcvEstimatorIdentity::Trial { trial_id },
        };
        let serving_contract_hash = artifact.header().serving_contract().contract_hash();
        let evidence = CpcvFoldArtifact {
            identity,
            training_groups_hash,
            training_group_count,
            model_artifact_hash: artifact.content_hash()?,
            serving_contract_hash,
            model_payload_hash: artifact.payload().model_payload_hash()?,
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
    handle: Handle,
    cancellation: CancellationProbe,
    fold_ledger: FoldArtifactLedger,
    fold_calibration: CpcvFoldCalibration,
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
/// `handle` lets [`WeightedFactorFoldSource::train_fold`] block on the async
/// trainer from inside a rayon worker thread (rayon threads carry no tokio
/// runtime context, so `Handle::current` would panic there — the handle
/// must be captured once, from the original async caller, and threaded
/// through explicitly).
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
    handle: Handle,
    cancellation: CancellationProbe,
    fold_ledger: FoldArtifactLedger,
    fold_calibration: CpcvFoldCalibration,
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
    model_family: ModelFamily,
    category_scope: Option<MarketCategory>,
    ticks: Vec<BacktestTick>,
    groups: Arc<[TimelineGroup]>,
    examples: Arc<[TrainingExample]>,
    group_example_ranges: Arc<[Range<usize>]>,
    handle: Handle,
    model_run_id: ModelRunId,
}

struct PortfolioReplayTemplateInput {
    dataset_id: TrainingDatasetId,
    ticks: Vec<BacktestTick>,
    groups: Arc<[TimelineGroup]>,
    caps: PortfolioCaps,
    handle: Handle,
    model_run_id: ModelRunId,
    weighted: Option<WeightedPortfolioReplay>,
}

struct PortfolioReplayTemplate {
    dataset_id: TrainingDatasetId,
    ticks_by_as_of: Arc<BTreeMap<DateTime<Utc>, BacktestTick>>,
    groups: Arc<[TimelineGroup]>,
    caps: PortfolioCaps,
    handle: Handle,
    model_run_id: ModelRunId,
    weighted: Option<WeightedPortfolioReplay>,
}

impl PortfolioReplayTemplate {
    fn from_input(input: PortfolioReplayTemplateInput) -> Self {
        Self {
            dataset_id: input.dataset_id,
            ticks_by_as_of: Arc::new(
                input
                    .ticks
                    .into_iter()
                    .map(|tick| (tick.decision_at, tick))
                    .collect(),
            ),
            groups: input.groups,
            caps: input.caps,
            handle: input.handle,
            model_run_id: input.model_run_id,
            weighted: input.weighted,
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
    fn train_fold(&self, request: FoldTrainingRequest<'_>) -> QuantResult<FoldRuntime> {
        let FoldTrainingRequest { identity, filter } = request;
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
        let trained = self
            .template
            .handle
            .block_on(WeightedFactorTrainer::new().train(request))?;
        let artifact = FoldArtifactInput {
            serving_contract: &self.template.serving_contract,
            payload: ModelPayload::WeightedFactor(Box::new(trained.payload)),
            input_contract_hash: trained.input_contract_hash,
            input_transform_hash: trained.input_transform_hash,
            training_input_hash: trained.training_input_hash,
            fold_calibration: &self.template.fold_calibration,
        }
        .seal()?;
        self.template
            .fold_ledger
            .record(identity, group_indices, &artifact)?;
        let runtime = WeightedFactorRuntime::new(artifact, None)?;
        Ok(FoldRuntime::Buy(Box::new(runtime)))
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
    fold_ledger: FoldArtifactLedger,
    fold_calibration: CpcvFoldCalibration,
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
        self.template
            .fold_ledger
            .record(identity, group_indices, &artifact)?;
        let runtime = ClassicalRuntime::load(artifact, &output.model_bytes)?;
        Ok(FoldRuntime::Buy(Box::new(runtime)))
    }
}

/// [`ReplayEngine`] shared by the supported Buy-side families.
struct FoldReplayEngineAdapter<'a> {
    template: &'a PortfolioReplayTemplate,
}

impl ReplayEngine for FoldReplayEngineAdapter<'_> {
    fn evaluate(
        &self,
        model: &FoldRuntime,
        filter: &GroupRowFilter,
    ) -> QuantResult<Vec<GroupEvaluation>> {
        evaluate_portfolio_groups(self.template, model.as_buy()?, filter)
    }
}

/// Scores the model over `filter`'s groups' pre-materialized ticks, then
/// derives one [`GroupEvaluation`] per group from the resulting canonical
/// portfolio-return observation (`return_value = tick_pnl / total_budget`,
/// exactly matching [`quant_pivot_research::backtest::runner`]'s capital-base
/// convention); `rank_observations` carries every resolved model score's
/// allocation-independent `(composite_score, token_payout_ratio)` pair,
/// pooled at the path level by
/// [`quant_pivot_research::validation::cpcv::build_path`]). Portfolio
/// selection and executable economics must not censor the model-quality
/// population used by rank IC.
fn evaluate_portfolio_groups(
    template: &PortfolioReplayTemplate,
    model: &dyn QuantModelRuntime,
    filter: &GroupRowFilter,
) -> QuantResult<Vec<GroupEvaluation>> {
    let mut ticks = filter
        .group_indices
        .iter()
        .map(|&group_index| template.tick_for(group_index, model))
        .collect::<QuantResult<Vec<_>>>()?;
    ticks.sort_by_key(|tick| tick.decision_at);
    let tick_decision_times = ticks
        .iter()
        .map(|tick| tick.decision_at)
        .collect::<Vec<_>>();

    let request = BacktestRequest {
        backtest_report_id: BacktestReportId::from_v7(),
        model_version_id: model.model_version_id(),
        dataset_id: template.dataset_id,
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
    if result.tick_weights.len() != tick_decision_times.len()
        || result.portfolio_returns.len() != tick_decision_times.len()
    {
        return Err(ResearchError::ValidationMethodology {
            detail: format!(
                "CPCV replay returned {} allocation-weight rows and {} portfolio-return rows for {} decision ticks",
                result.tick_weights.len(),
                result.portfolio_returns.len(),
                tick_decision_times.len()
            ),
        }
        .into());
    }
    let mut replay_by_as_of = BTreeMap::new();
    for ((decision_at, observation), weights) in tick_decision_times
        .into_iter()
        .zip(&result.portfolio_returns)
        .zip(&result.tick_weights)
    {
        if observation.decision_at != decision_at {
            return Err(ResearchError::ValidationMethodology {
                detail: format!(
                    "CPCV replay portfolio return at {} differs from expected decision tick {}",
                    observation.decision_at, decision_at
                ),
            }
            .into());
        }
        if observation.capital_base_usd.inner() != template.caps.total_budget_usd {
            return Err(ResearchError::ValidationMethodology {
                detail: format!(
                    "CPCV replay capital base {} differs from frozen portfolio budget {} at {}",
                    observation.capital_base_usd, template.caps.total_budget_usd, decision_at
                ),
            }
            .into());
        }
        let return_value = observation.net_return_bps.inner() / Decimal::from(10_000);
        if replay_by_as_of
            .insert(decision_at, (return_value, weights.clone()))
            .is_some()
        {
            return Err(ResearchError::ValidationMethodology {
                detail: format!("CPCV replay returned duplicate decision tick {decision_at}"),
            }
            .into());
        }
    }
    if replay_by_as_of.len() != result.tick_weights.len() {
        return Err(ResearchError::ValidationMethodology {
            detail: "CPCV replay returned duplicate decision ticks for turnover reconstruction"
                .to_owned(),
        }
        .into());
    }

    let rank_observations_by_as_of = rank_observations_by_tick(&result.rank_outcomes);

    let mut evaluations = Vec::with_capacity(filter.group_indices.len());
    for &group_index in &filter.group_indices {
        let as_of = template.groups[group_index].decision_at;
        let Some((return_value, allocation_weights)) = replay_by_as_of.get(&as_of) else {
            return Err(ResearchError::ValidationMethodology {
                detail: format!(
                    "CPCV replay missing materialized tick for group as_of={as_of} \
                     (group_index={group_index}) — refuse to invent a zero return"
                ),
            }
            .into());
        };
        let rank_observations = rank_observations_by_as_of
            .get(&as_of)
            .cloned()
            .unwrap_or_default();
        evaluations.push(GroupEvaluation {
            group_index,
            return_value: *return_value,
            rank_observations,
            allocation_weights: Some(allocation_weights.clone()),
        });
    }
    Ok(evaluations)
}

fn rank_observations_by_tick(
    outcomes: &[ModelRankOutcome],
) -> BTreeMap<DateTime<Utc>, Vec<RankObservation>> {
    let mut by_as_of = BTreeMap::new();
    for outcome in outcomes {
        by_as_of
            .entry(outcome.decision_at)
            .or_insert_with(Vec::new)
            .push(RankObservation {
                score: outcome.score,
                realized: outcome.realized,
            });
    }
    by_as_of
}

/// Run every trial's **full-window** (no purge/embargo) train + evaluate,
/// producing one [`TrialPerformanceMatrix`] column per trial. Reuses the
/// exact same [`FoldModelSource`]/[`ReplayEngine`] the CPCV folds use, just
/// with `filter` covering every group and a per-trial objective override.
fn run_trial_grid(
    trials: &[Trial],
    fold_template: &FoldTemplate,
    replay_template: &PortfolioReplayTemplate,
    groups: &[TimelineGroup],
    cancel: &CancellationToken,
) -> QuantResult<TrialPerformanceMatrix> {
    let all_indices = GroupRowFilter {
        group_indices: (0..groups.len()).collect(),
    };
    let replay_engine = FoldReplayEngineAdapter {
        template: replay_template,
    };

    let columns: Vec<QuantResult<Vec<Decimal>>> = trials
        .par_iter()
        .map(|trial| -> QuantResult<Vec<Decimal>> {
            ensure_cpcv_not_cancelled(cancel, "trial train boundary")?;
            let fold_source = fold_template.fold_source(Some(trial))?;
            let model = fold_source.train_fold(FoldTrainingRequest {
                identity: FoldTrainingIdentity::Trial {
                    trial_id: trial.trial_id,
                },
                filter: &all_indices,
            })?;
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
            Ok(column)
        })
        .collect();
    let mut trial_returns = Vec::with_capacity(columns.len());
    for column in columns {
        trial_returns.push(column?);
    }

    let periods = groups.iter().map(|group| group.decision_at).collect();

    TrialPerformanceMatrix::from_columns(periods, &trial_returns)
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
    path_set: &BacktestPathSet,
    matrix: &TrialPerformanceMatrix,
    trial_grid_count: u32,
    config: &CpcvBacktestConfig,
    period_length: ChronoDuration,
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
        period_length,
        skewness: stats::skewness(&path.group_returns),
        kurtosis: stats::kurtosis(&path.group_returns),
        // Bailey multiple-testing N/V: same population — the governed trial
        // grid that produced `matrix`. Coord-search is audit-only.
        trial_count: trial_grid_count,
        trial_sharpe_variance: stats::variance(&trial_sharpes),
    };
    let dsr = dsr_input.deflated_sharpe_ratio()?;
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
    use chrono::{Duration, TimeZone, Utc};
    use quant_pivot_error::{QuantError, research::ResearchError};
    use quant_pivot_models::{
        enums::model::ModelFamily,
        types::{MarketId, TokenId},
    };
    use quant_pivot_research::{
        backtest::ModelRankOutcome, model::ModelRankTarget, training::TOKEN_PAYOUT_RATIO,
        validation::GroupRowFilter,
    };
    use rust_decimal_macros::dec;

    use super::{
        CpcvBacktestService, rank_observations_by_tick, validated_group_indices,
        validation_period_length,
    };

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
    fn rank_uses_canonical_scores() {
        let decision_at = Utc
            .timestamp_opt(1_700_000_000, 0)
            .single()
            .expect("valid decision time");
        let outcomes = [
            ModelRankOutcome {
                decision_at,
                market_id: MarketId::new("winner"),
                token_id: TokenId::new("winner-yes"),
                score: dec!(0.9),
                target: ModelRankTarget {
                    label_name: TOKEN_PAYOUT_RATIO,
                    label_horizon_secs: 0,
                },
                realized: dec!(1),
            },
            ModelRankOutcome {
                decision_at,
                market_id: MarketId::new("loser"),
                token_id: TokenId::new("loser-yes"),
                score: dec!(-0.9),
                target: ModelRankTarget {
                    label_name: TOKEN_PAYOUT_RATIO,
                    label_horizon_secs: 0,
                },
                realized: dec!(0),
            },
        ];

        let grouped = rank_observations_by_tick(&outcomes);
        let observations = grouped
            .get(&decision_at)
            .expect("decision-time observations");
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
