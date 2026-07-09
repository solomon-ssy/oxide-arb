//! [`DefaultModelQualityGate`]: the concrete, deterministic model-publication gate.
//!
//! A quality gate is a **pure function** of a frozen backtest report, the
//! dataset coverage accounting, the point-in-time leakage scan, and (for
//! publish / auto intents) the shadow overlap stability — evaluated against a
//! governed [`QualityGateThresholds`] snapshot. It never touches a database or
//! the network.
//!
//! Gates split into **hard** (any failure ⇒ the model may not advance) and
//! **soft** (recorded as warnings, never blocking). The intent
//! ([`GateIntent`]) selects which hard gates apply: a `DatasetReady` promotion
//! has no backtest, a `Publish` adds shadow-stability, and an `AutoExecution`
//! evaluation additionally requires liquidity-exit feasibility (parent §18).
//!
//! The resulting [`QualityGateReport`] is content-addressed and serializes into
//! `quant_model_version.quality_gate_report`; its `evaluated_at` drives the 3.7
//! load-time staleness deny (`min_quality_gate_age_secs`).

use chrono::{DateTime, Utc};
use quant_pivot_error::QuantResult;
use quant_pivot_models::{
    enums::model::ModelFamily,
    types::{ContentHash, ModelVersionId, Probability, TrainingDatasetId},
};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

use crate::{
    backtest::BacktestReport,
    gates::{ModelQualityGate, QualityGateDecision},
    hashing::ResearchHasher,
    precision::RESEARCH_DECIMAL_SCALE,
    training::{DatasetCoverage, LeakageFindings},
};

/// What a gate evaluation is gating: a model version (publish path) or a training
/// dataset (promotion path). Self-describing so the persisted report / audit
/// detail carries the subject.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "kind", content = "id")]
pub enum GateSubject {
    /// A model version under candidate / publish / auto evaluation.
    ModelVersion(ModelVersionId),
    /// A training dataset under `Built → Ready` promotion.
    TrainingDataset(TrainingDatasetId),
}

impl GateSubject {
    /// The subject id rendered as a string (for error / audit context).
    #[must_use]
    pub fn id_string(&self) -> String {
        match self {
            Self::ModelVersion(id) => id.to_string(),
            Self::TrainingDataset(id) => id.to_string(),
        }
    }

    /// The subject kind label (for error / audit context).
    #[must_use]
    pub const fn kind(&self) -> &'static str {
        match self {
            Self::ModelVersion(_) => "model_version",
            Self::TrainingDataset(_) => "training_dataset",
        }
    }
}

/// What a gate evaluation is gating: each intent selects the applicable hard gates.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum GateIntent {
    /// Promote a `Built` training dataset to `Ready` (coverage + leakage only).
    DatasetReady,
    /// Register a trained model as a candidate (coverage + leakage + backtest).
    Candidate,
    /// Publish a model version (adds shadow overlap stability).
    Publish,
    /// Evaluate readiness for auto-execution (adds liquidity-exit feasibility).
    AutoExecution,
}

impl GateIntent {
    /// Whether this intent requires shadow overlap stability (publish / auto).
    #[must_use]
    pub const fn requires_shadow_stability(self) -> bool {
        matches!(self, Self::Publish | Self::AutoExecution)
    }

    /// Whether this intent requires liquidity-exit feasibility (auto only).
    #[must_use]
    pub const fn requires_liquidity_feasibility(self) -> bool {
        matches!(self, Self::AutoExecution)
    }

    /// Whether this intent requires a persisted backtest report (not `DatasetReady`).
    #[must_use]
    pub const fn requires_backtest(self) -> bool {
        matches!(self, Self::Candidate | Self::Publish | Self::AutoExecution)
    }

    /// Stable `snake_case` wire name (matches the serde representation).
    #[must_use]
    pub const fn wire_name(self) -> &'static str {
        match self {
            Self::DatasetReady => "dataset_ready",
            Self::Candidate => "candidate",
            Self::Publish => "publish",
            Self::AutoExecution => "auto_execution",
        }
    }
}

/// Stable, queryable identity of one gate. Append-only wire labels.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum GateId {
    /// Resolved sample count (hard).
    SampleCount,
    /// Fraction of labels resolved (hard).
    LabelCoverage,
    /// Fraction of planned samples materialized (hard).
    CriticalFeatureCoverage,
    /// No point-in-time leakage (hard).
    NoPitLeakage,
    /// Maximum drawdown within budget (hard, backtest intents).
    MaxDrawdown,
    /// Liquidity-exit feasibility (hard, auto-execution only).
    LiquidityExitFeasible,
    /// Shadow overlap stability (hard, publish / auto).
    ShadowOverlapStability,
    /// A frozen backtest report must exist (hard, model intents with backtest metrics).
    BacktestRequired,
    /// A persisted CPCV path set must exist (hard, model intents with backtest metrics).
    CpcvRequired,
    /// Rank information coefficient from the CPCV path-set median (hard).
    RankIc,
    /// Deflated Sharpe Ratio significance (hard).
    DeflatedSharpe,
    /// Probability of Backtest Overfitting (hard).
    Pbo,
    /// Minimum track record length advisory (soft).
    MinTrackRecordLength,
    /// Single-path turnover budget (hard, risk/execution realism).
    TurnoverBudget,
    /// Single-path tail-loss floor in bps (hard, risk/execution realism).
    TailLossBudget,
    /// Directional hit rate (soft).
    HitRate,
    /// Per-category concentration within budget (soft).
    CategoryConcentration,
    /// `ExitDecision` L2 book fidelity ratio (hard, sell family).
    SellL2BookFidelity,
    /// `ExitDecision` microstructure fallback ratio (hard, sell family).
    SellFallbackRatio,
    /// The return model must be `Calibrated` (hard, Buy family, publish/auto;
    /// Phase 11.3 #5/#13 fail-closed).
    CalibrationRequired,
}

impl GateId {
    /// Stable `snake_case` wire name (matches the serde representation).
    #[must_use]
    pub const fn wire_name(self) -> &'static str {
        match self {
            Self::SampleCount => "sample_count",
            Self::LabelCoverage => "label_coverage",
            Self::CriticalFeatureCoverage => "critical_feature_coverage",
            Self::NoPitLeakage => "no_pit_leakage",
            Self::MaxDrawdown => "max_drawdown",
            Self::LiquidityExitFeasible => "liquidity_exit_feasible",
            Self::ShadowOverlapStability => "shadow_overlap_stability",
            Self::BacktestRequired => "backtest_required",
            Self::CpcvRequired => "cpcv_required",
            Self::RankIc => "rank_ic",
            Self::DeflatedSharpe => "deflated_sharpe",
            Self::Pbo => "pbo",
            Self::MinTrackRecordLength => "min_track_record_length",
            Self::TurnoverBudget => "turnover_budget",
            Self::TailLossBudget => "tail_loss_budget",
            Self::HitRate => "hit_rate",
            Self::CategoryConcentration => "category_concentration",
            Self::SellL2BookFidelity => "sell_l2_book_fidelity",
            Self::SellFallbackRatio => "sell_fallback_ratio",
            Self::CalibrationRequired => "calibration_required",
        }
    }
}

/// One failed gate, carrying the observed value and the threshold it missed.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct QualityGateFailure {
    /// Which gate failed.
    pub gate: GateId,
    /// The observed value (rendered).
    pub observed: String,
    /// The threshold the observed value missed (rendered).
    pub threshold: String,
    /// Human-readable failure detail.
    pub detail: String,
}

/// Whether a gate blocks the advance (`Hard`) or is advisory only (`Soft`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum GateClass {
    /// A blocking gate — any failure denies the advance.
    Hard,
    /// An advisory gate — a miss is recorded as a warning, never blocking.
    Soft,
}

impl GateClass {
    /// Stable `snake_case` wire name (matches the serde representation).
    #[must_use]
    pub const fn wire_name(self) -> &'static str {
        match self {
            Self::Hard => "hard",
            Self::Soft => "soft",
        }
    }
}

/// The evaluated state of one gate against its threshold.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum GateStatus {
    /// The gate cleared its threshold.
    Pass,
    /// A hard gate missed its threshold (blocking).
    Fail,
    /// A soft gate missed its threshold (advisory).
    Warn,
    /// The gate does not apply to the evaluated intent (e.g. shadow stability
    /// under a `Candidate` evaluation).
    NotApplicable,
}

impl GateStatus {
    /// Stable `snake_case` wire name (matches the serde representation).
    #[must_use]
    pub const fn wire_name(self) -> &'static str {
        match self {
            Self::Pass => "pass",
            Self::Fail => "fail",
            Self::Warn => "warn",
            Self::NotApplicable => "not_applicable",
        }
    }
}

/// One evaluated gate — the complete, self-describing scorecard row.
///
/// Unlike [`QualityGateFailure`] (only failures / warnings), this records
/// *every* gate the evaluation touched, including passing and not-applicable
/// ones, so a UI can render the full readiness picture.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GateOutcome {
    /// Which gate this row describes.
    pub gate: GateId,
    /// Whether the gate is blocking (`Hard`) or advisory (`Soft`).
    pub class: GateClass,
    /// The evaluated state.
    pub status: GateStatus,
    /// The observed value (rendered).
    pub observed: String,
    /// The threshold compared against (rendered).
    pub threshold: String,
    /// Human-readable description of the failing/advisory condition.
    pub detail: String,
}

impl GateOutcome {
    /// Project a failing / warning outcome onto the legacy failure shape.
    fn as_failure(&self) -> QualityGateFailure {
        QualityGateFailure {
            gate: self.gate,
            observed: self.observed.clone(),
            threshold: self.threshold.clone(),
            detail: self.detail.clone(),
        }
    }
}

/// Accumulator that records every evaluated gate as a [`GateOutcome`].
///
/// The blocking / advisory / not-applicable helpers keep each `evaluate_*`
/// site declarative — the ledger owns the pass/fail/warn mapping so the full
/// scorecard and the derived failure projections come from a single source.
#[derive(Debug, Default)]
struct GateLedger {
    outcomes: Vec<GateOutcome>,
}

impl GateLedger {
    /// Record a blocking gate: `cleared == false` ⇒ [`GateStatus::Fail`].
    fn hard(
        &mut self,
        gate: GateId,
        cleared: bool,
        observed: impl Into<String>,
        threshold: impl Into<String>,
        detail: impl Into<String>,
    ) {
        let status = if cleared {
            GateStatus::Pass
        } else {
            GateStatus::Fail
        };
        self.push(gate, GateClass::Hard, status, observed, threshold, detail);
    }

    /// Record an advisory gate: `cleared == false` ⇒ [`GateStatus::Warn`].
    fn soft(
        &mut self,
        gate: GateId,
        cleared: bool,
        observed: impl Into<String>,
        threshold: impl Into<String>,
        detail: impl Into<String>,
    ) {
        let status = if cleared {
            GateStatus::Pass
        } else {
            GateStatus::Warn
        };
        self.push(gate, GateClass::Soft, status, observed, threshold, detail);
    }

    /// Record a gate that does not apply to the evaluated intent.
    fn not_applicable(
        &mut self,
        gate: GateId,
        class: GateClass,
        threshold: impl Into<String>,
        detail: impl Into<String>,
    ) {
        self.push(
            gate,
            class,
            GateStatus::NotApplicable,
            "n/a",
            threshold,
            detail,
        );
    }

    fn push(
        &mut self,
        gate: GateId,
        class: GateClass,
        status: GateStatus,
        observed: impl Into<String>,
        threshold: impl Into<String>,
        detail: impl Into<String>,
    ) {
        self.outcomes.push(GateOutcome {
            gate,
            class,
            status,
            observed: observed.into(),
            threshold: threshold.into(),
            detail: detail.into(),
        });
    }
}

/// Governed quality-gate thresholds (assembled from `QualityGateConfig` + spec).
///
/// Money / probability semantics stay `Decimal`; these are governed knobs the
/// runtime-config `quality_gate` section carries (hot-reloadable).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct QualityGateThresholds {
    /// Minimum resolved sample count (parent §18 default 500).
    pub min_sample_count: u64,
    /// Minimum label coverage in `[0, 1]` (default 0.70).
    pub min_label_coverage: Decimal,
    /// Minimum critical-feature (build) coverage in `[0, 1]` (default 0.95).
    pub min_critical_feature_coverage: Decimal,
    /// Maximum tolerated drawdown in `[0, 1]` (configured).
    pub max_drawdown: Decimal,
    /// Minimum liquidity-exit feasibility in `[0, 1]` (auto, default 0.90).
    pub min_liquidity_exit_feasibility: Decimal,
    /// Minimum shadow overlap stability in `[0, 1]` (publish, default 0.60).
    pub min_shadow_overlap_stability: Decimal,
    /// Maximum (soft) per-category concentration in `[0, 1]` (default 0.60).
    pub max_category_concentration: Decimal,
}

/// Phase 11.5 CPCV alpha-significance gate thresholds (`research.validation.gates.*`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ValidationGateThresholds {
    /// Minimum CPCV path-set median rank IC (hard gate).
    pub rank_ic_min: Decimal,
    /// Target significance (`α`) the Deflated Sharpe Ratio must clear:
    /// `deflated_sharpe >= 1 - dsr_significance`.
    pub dsr_significance: Decimal,
    /// Maximum tolerated Probability of Backtest Overfitting.
    pub max_pbo: Decimal,
    /// Maximum tolerated single-path turnover.
    pub max_turnover: Decimal,
    /// Minimum tolerated single-path tail loss, in bps (typically negative).
    pub min_tail_loss_bps: Decimal,
}

impl ValidationGateThresholds {
    /// Conservative defaults matching `research.validation.gates` schema defaults.
    #[must_use]
    pub fn conservative() -> Self {
        Self {
            rank_ic_min: Decimal::new(2, 2),
            dsr_significance: Decimal::new(5, 2),
            max_pbo: Decimal::new(50, 2),
            max_turnover: Decimal::new(50, 2),
            min_tail_loss_bps: Decimal::new(-500, 0),
        }
    }
}

impl Default for ValidationGateThresholds {
    fn default() -> Self {
        Self::conservative()
    }
}

/// Frozen CPCV path-set metrics consumed by the alpha-significance gates.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CpcvPathSetGateInput {
    pub median_rank_ic: Decimal,
    pub deflated_sharpe: Decimal,
    pub pbo: Decimal,
    pub min_track_record_length_secs: Option<i64>,
}

/// Sell-side hold-vs-exit gate thresholds (Phase 06.1).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SellQualityGateThresholds {
    pub min_sample_count: u64,
    pub min_label_coverage: Decimal,
    pub min_exit_alpha_rank_ic: Decimal,
    pub min_l2_book_fidelity_ratio: Decimal,
    pub max_fallback_ratio: Decimal,
}

impl Default for SellQualityGateThresholds {
    fn default() -> Self {
        Self {
            min_sample_count: 200,
            min_label_coverage: Decimal::new(60, 2),
            min_exit_alpha_rank_ic: Decimal::new(5, 2),
            min_l2_book_fidelity_ratio: Decimal::new(50, 2),
            max_fallback_ratio: Decimal::new(50, 2),
        }
    }
}

impl QualityGateThresholds {
    /// Conservative defaults matching parent §18.
    #[must_use]
    pub fn conservative() -> Self {
        Self {
            min_sample_count: 500,
            min_label_coverage: Decimal::new(70, 2),
            min_critical_feature_coverage: Decimal::new(95, 2),
            max_drawdown: Decimal::new(30, 2),
            min_liquidity_exit_feasibility: Decimal::new(90, 2),
            min_shadow_overlap_stability: Decimal::new(60, 2),
            max_category_concentration: Decimal::new(60, 2),
        }
    }
}

impl Default for QualityGateThresholds {
    fn default() -> Self {
        Self::conservative()
    }
}

/// Inputs to a quality-gate evaluation (all frozen, no IO).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct QualityGateInput {
    /// What the evaluation gates (model version or training dataset).
    pub subject: GateSubject,
    /// What the evaluation gates (selects the applicable hard gates).
    pub intent: GateIntent,
    /// Frozen backtest report (`None` for a `DatasetReady` promotion).
    pub backtest: Option<BacktestReport>,
    /// Dataset coverage accounting.
    pub dataset: DatasetCoverage,
    /// Point-in-time leakage scan.
    pub leakage: LeakageFindings,
    /// Shadow overlap stability over the required window (publish / auto).
    pub shadow_stability: Option<Probability>,
    /// Governed thresholds.
    pub thresholds: QualityGateThresholds,
    /// Phase 11.5 CPCV alpha-significance thresholds.
    pub validation_thresholds: ValidationGateThresholds,
    /// Latest persisted CPCV path-set metrics (`None` when absent).
    pub path_set: Option<CpcvPathSetGateInput>,
    /// Sell-side thresholds (used when [`Self::model_family`] is an exit scorer).
    pub sell_thresholds: SellQualityGateThresholds,
    /// Model family under evaluation (`None` ⇒ buy-oriented defaults).
    pub model_family: Option<ModelFamily>,
    /// Whether the evaluated artifact's `ReturnModelSpec` is `Calibrated`
    /// (Phase 11.3 #5/#13), resolved through the **same deep check**
    /// (`resolve_return_model_calibration`) the report builder, admission,
    /// and intent creation share — never a shallow enum-tag read. Buy-family
    /// `Publish`/`AutoExecution` intents hard-gate on this; exit scorers and
    /// other intents ignore it (they have no `ReturnModelSpec` concept).
    pub return_model_calibrated: bool,
    /// `model.calibration.require_for_publish` (Phase 11.3 closed-loop
    /// hardening): when `false`, `GateId::CalibrationRequired` records an
    /// explicit `NotApplicable` (never a silent pass) instead of hard-gating —
    /// an auditable, operator-governed cold-start bootstrap window. `true`
    /// (the production default) preserves the original hard-gate behavior.
    pub calibration_gate_enabled: bool,
}

/// A content-addressed, persisted quality-gate evaluation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct QualityGateReport {
    /// Subject evaluated (model version or training dataset).
    pub subject: GateSubject,
    /// Intent the evaluation gated.
    pub intent: GateIntent,
    /// When the gate ran (drives the load-time staleness deny).
    pub evaluated_at: DateTime<Utc>,
    /// Every evaluated gate (pass / fail / warn / not-applicable) — the complete
    /// scorecard. `hard_failures` / `soft_warnings` are derived projections.
    pub gates: Vec<GateOutcome>,
    /// Hard gate failures (any ⇒ `passed = false`).
    pub hard_failures: Vec<QualityGateFailure>,
    /// Soft gate warnings (never blocking).
    pub soft_warnings: Vec<QualityGateFailure>,
    /// Whether every hard gate cleared.
    pub passed: bool,
    /// Content hash over the decision (excludes `evaluated_at`).
    pub report_hash: ContentHash,
}

/// Canonical, time-free projection of a report for content addressing.
#[derive(Serialize)]
struct ReportHashInput<'a> {
    subject: &'a GateSubject,
    intent: GateIntent,
    hard_failures: &'a [QualityGateFailure],
    soft_warnings: &'a [QualityGateFailure],
    passed: bool,
}

/// The default, deterministic model-publication gate (parent §18).
#[derive(Debug, Default, Clone, Copy)]
pub struct DefaultModelQualityGate;

impl DefaultModelQualityGate {
    /// Build the gate.
    #[must_use]
    pub const fn new() -> Self {
        Self
    }
}

impl ModelQualityGate for DefaultModelQualityGate {
    fn evaluate(&self, input: QualityGateInput) -> QuantResult<QualityGateDecision> {
        let mut ledger = GateLedger::default();

        let is_exit = input.model_family.is_some_and(ModelFamily::is_exit_scorer);
        evaluate_coverage_gates(&input, is_exit, &mut ledger);
        if is_exit {
            evaluate_sell_gates(&input, &mut ledger);
            // Sell publish is still gated on shadow overlap stability (the buy
            // liquidity-feasibility/backtest gates do not apply to exit scorers).
            evaluate_shadow_stability_gate(&input, &mut ledger);
        } else {
            evaluate_backtest_presence(&input, &mut ledger);
            if let Some(report) = &input.backtest {
                evaluate_backtest_risk_gates(
                    report,
                    &input.thresholds,
                    &input.validation_thresholds,
                    &mut ledger,
                );
            }
            evaluate_cpcv_alpha_gates(&input, &mut ledger);
            evaluate_intent_gates(&input, &mut ledger);
        }
        // Sell scorers structurally never carry a return model — record an
        // explicit `NotApplicable` row (not an absent one) so the gate report
        // is auditable end to end regardless of family.
        evaluate_calibration_gate(&input, is_exit, &mut ledger);

        let gates = ledger.outcomes;
        let hard_failures: Vec<QualityGateFailure> = gates
            .iter()
            .filter(|outcome| {
                outcome.class == GateClass::Hard && outcome.status == GateStatus::Fail
            })
            .map(GateOutcome::as_failure)
            .collect();
        let soft_warnings: Vec<QualityGateFailure> = gates
            .iter()
            .filter(|outcome| {
                outcome.class == GateClass::Soft && outcome.status == GateStatus::Warn
            })
            .map(GateOutcome::as_failure)
            .collect();

        let passed = hard_failures.is_empty();
        let report_hash = ResearchHasher::canonical(&ReportHashInput {
            subject: &input.subject,
            intent: input.intent,
            hard_failures: &hard_failures,
            soft_warnings: &soft_warnings,
            passed,
        })?;
        let report = QualityGateReport {
            subject: input.subject,
            intent: input.intent,
            evaluated_at: Utc::now(),
            gates,
            hard_failures: hard_failures.clone(),
            soft_warnings,
            passed,
            report_hash,
        };

        if passed {
            Ok(QualityGateDecision::Pass { report })
        } else {
            Ok(QualityGateDecision::Fail {
                report,
                hard_failures,
            })
        }
    }
}

/// Coverage + leakage hard gates (every intent).
///
/// The sample-count and label-coverage bars are family-specific: for exit
/// scorers the Sell gate owns them (against the `sell.*` thresholds), so they are
/// skipped here to avoid double-applying the Buy `min_sample_count` bar to a
/// sell-only dataset. Feature coverage and PIT leakage are universal.
fn evaluate_coverage_gates(input: &QualityGateInput, is_exit: bool, ledger: &mut GateLedger) {
    let t = &input.thresholds;
    if !is_exit {
        // Sample count: prefer the backtest's resolved samples, else the dataset's
        // built examples (DatasetReady has no backtest).
        let samples = input
            .backtest
            .as_ref()
            .map_or(input.dataset.built_examples, |report| report.sample_count);
        ledger.hard(
            GateId::SampleCount,
            samples >= t.min_sample_count,
            samples.to_string(),
            t.min_sample_count.to_string(),
            "insufficient resolved samples",
        );

        let label_coverage = input.dataset.label_coverage();
        ledger.hard(
            GateId::LabelCoverage,
            label_coverage >= t.min_label_coverage,
            label_coverage.to_string(),
            t.min_label_coverage.to_string(),
            "label coverage below minimum",
        );
    }

    let feature_coverage = input.dataset.feature_build_coverage();
    ledger.hard(
        GateId::CriticalFeatureCoverage,
        feature_coverage >= t.min_critical_feature_coverage,
        feature_coverage.to_string(),
        t.min_critical_feature_coverage.to_string(),
        "critical-feature coverage below minimum",
    );

    ledger.hard(
        GateId::NoPitLeakage,
        input.leakage.is_clean(),
        input.leakage.violation_count().to_string(),
        "0",
        "point-in-time leakage detected in training features",
    );
}

/// Sell-side hold-vs-exit hard/soft gates (Phase 06.1).
fn evaluate_sell_gates(input: &QualityGateInput, ledger: &mut GateLedger) {
    let t = &input.sell_thresholds;
    let samples = input
        .dataset
        .exit_decision_built
        .max(input.dataset.built_examples);
    ledger.hard(
        GateId::SampleCount,
        samples >= t.min_sample_count,
        samples.to_string(),
        t.min_sample_count.to_string(),
        "insufficient ExitDecision training samples",
    );
    let label_coverage = input.dataset.label_coverage();
    ledger.hard(
        GateId::LabelCoverage,
        label_coverage >= t.min_label_coverage,
        label_coverage.to_string(),
        t.min_label_coverage.to_string(),
        "sell label coverage below minimum",
    );
    let l2_ratio = input.dataset.exit_l2_fidelity_ratio();
    ledger.hard(
        GateId::SellL2BookFidelity,
        l2_ratio >= t.min_l2_book_fidelity_ratio,
        l2_ratio.to_string(),
        t.min_l2_book_fidelity_ratio.to_string(),
        "ExitDecision L2 book fidelity below minimum",
    );
    let fallback_ratio = input.dataset.exit_fallback_ratio();
    ledger.hard(
        GateId::SellFallbackRatio,
        fallback_ratio <= t.max_fallback_ratio,
        fallback_ratio.to_string(),
        t.max_fallback_ratio.to_string(),
        "ExitDecision microstructure fallback ratio above maximum",
    );
    if let Some(report) = &input.backtest {
        ledger.soft(
            GateId::RankIc,
            report.rank_ic >= t.min_exit_alpha_rank_ic,
            report.rank_ic.to_string(),
            t.min_exit_alpha_rank_ic.to_string(),
            "exit-alpha rank IC below soft minimum",
        );
    }
}

/// Hard gate: model intents that consume backtest metrics must carry a report.
fn evaluate_backtest_presence(input: &QualityGateInput, ledger: &mut GateLedger) {
    if !input.intent.requires_backtest() {
        return;
    }
    let present = input.backtest.is_some();
    ledger.hard(
        GateId::BacktestRequired,
        present,
        if present { "present" } else { "none" },
        "required",
        "a frozen backtest report is required before advancing this model",
    );
}

/// Single-path risk/execution realism gates plus soft alpha diagnostics.
fn evaluate_backtest_risk_gates(
    report: &BacktestReport,
    t: &QualityGateThresholds,
    validation: &ValidationGateThresholds,
    ledger: &mut GateLedger,
) {
    ledger.hard(
        GateId::MaxDrawdown,
        report.max_drawdown <= t.max_drawdown,
        report.max_drawdown.to_string(),
        t.max_drawdown.to_string(),
        "max drawdown exceeds budget",
    );
    ledger.hard(
        GateId::TurnoverBudget,
        report.turnover <= validation.max_turnover,
        report.turnover.to_string(),
        validation.max_turnover.to_string(),
        "turnover exceeds budget",
    );
    ledger.hard(
        GateId::TailLossBudget,
        report.tail_loss >= validation.min_tail_loss_bps,
        report.tail_loss.to_string(),
        validation.min_tail_loss_bps.to_string(),
        "tail loss exceeds budget",
    );
    let hit_rate = report.hit_rate.inner();
    ledger.soft(
        GateId::HitRate,
        hit_rate >= Decimal::new(5, 1),
        hit_rate.to_string(),
        "0.5",
        "directional hit rate below 0.5",
    );
    let concentration = max_category_concentration(report);
    ledger.soft(
        GateId::CategoryConcentration,
        concentration <= t.max_category_concentration,
        concentration.to_string(),
        t.max_category_concentration.to_string(),
        "samples concentrated in a single category",
    );
}

/// CPCV alpha-significance gates (hard) plus `MinTRL` advisory (soft).
fn evaluate_cpcv_alpha_gates(input: &QualityGateInput, ledger: &mut GateLedger) {
    if !input.intent.requires_backtest() {
        return;
    }
    let validation = &input.validation_thresholds;
    let dsr_floor = Decimal::ONE - validation.dsr_significance;
    if let Some(path_set) = &input.path_set {
        ledger.hard(
            GateId::CpcvRequired,
            true,
            "present",
            "required",
            "a persisted CPCV path set is required before advancing this model",
        );
        ledger.hard(
            GateId::RankIc,
            path_set.median_rank_ic >= validation.rank_ic_min,
            path_set.median_rank_ic.to_string(),
            validation.rank_ic_min.to_string(),
            "CPCV median rank IC below minimum",
        );
        ledger.hard(
            GateId::DeflatedSharpe,
            path_set.deflated_sharpe >= dsr_floor,
            path_set.deflated_sharpe.to_string(),
            dsr_floor.to_string(),
            "deflated Sharpe ratio below significance floor",
        );
        ledger.hard(
            GateId::Pbo,
            path_set.pbo <= validation.max_pbo,
            path_set.pbo.to_string(),
            validation.max_pbo.to_string(),
            "probability of backtest overfitting above maximum",
        );
        if let (Some(mintrl_secs), Some(backtest)) =
            (path_set.min_track_record_length_secs, &input.backtest)
        {
            let observed_secs = backtest
                .window_end
                .signed_duration_since(backtest.window_start)
                .num_seconds()
                .max(0);
            ledger.soft(
                GateId::MinTrackRecordLength,
                observed_secs >= mintrl_secs,
                observed_secs.to_string(),
                mintrl_secs.to_string(),
                "track record shorter than minimum track record length",
            );
        } else {
            ledger.not_applicable(
                GateId::MinTrackRecordLength,
                GateClass::Soft,
                "n/a",
                "MinTRL unavailable when representative Sharpe is non-positive",
            );
        }
        return;
    }
    ledger.hard(
        GateId::CpcvRequired,
        false,
        "none",
        "required",
        "a persisted CPCV path set is required before advancing this model",
    );
    ledger.not_applicable(
        GateId::RankIc,
        GateClass::Hard,
        validation.rank_ic_min.to_string(),
        "requires a CPCV path set",
    );
    ledger.not_applicable(
        GateId::DeflatedSharpe,
        GateClass::Hard,
        dsr_floor.to_string(),
        "requires a CPCV path set",
    );
    ledger.not_applicable(
        GateId::Pbo,
        GateClass::Hard,
        validation.max_pbo.to_string(),
        "requires a CPCV path set",
    );
    ledger.not_applicable(
        GateId::MinTrackRecordLength,
        GateClass::Soft,
        "n/a",
        "requires a CPCV path set",
    );
}

/// Intent-specific hard gates: liquidity feasibility (auto) + shadow stability
/// (publish / auto).
fn evaluate_intent_gates(input: &QualityGateInput, ledger: &mut GateLedger) {
    let t = &input.thresholds;
    if input.intent.requires_liquidity_feasibility() {
        match &input.backtest {
            Some(report) => {
                let feasible = report.liquidity_feasibility.inner();
                ledger.hard(
                    GateId::LiquidityExitFeasible,
                    feasible >= t.min_liquidity_exit_feasibility,
                    feasible.to_string(),
                    t.min_liquidity_exit_feasibility.to_string(),
                    "liquidity-exit feasibility below minimum",
                );
            }
            None => ledger.hard(
                GateId::LiquidityExitFeasible,
                false,
                "none",
                t.min_liquidity_exit_feasibility.to_string(),
                "auto-execution gate requires a backtest report",
            ),
        }
    } else {
        ledger.not_applicable(
            GateId::LiquidityExitFeasible,
            GateClass::Hard,
            t.min_liquidity_exit_feasibility.to_string(),
            "only evaluated for auto-execution",
        );
    }

    evaluate_shadow_stability_gate(input, ledger);
}

/// Hard gate (Phase 11.3 #5/#13): `Publish` / `AutoExecution` intents on a Buy
/// model require a `Calibrated` return model. `uncalibrated` (`Heuristic`)
/// artifacts are bootstrap-only and must never reach publish or
/// auto-execution — fail-closed, never a silent downgrade to the heuristic
/// default. `is_exit` (Sell/Hold-vs-Exit scorers) never carries a return
/// model, so it is always `NotApplicable` regardless of intent.
fn evaluate_calibration_gate(input: &QualityGateInput, is_exit: bool, ledger: &mut GateLedger) {
    let applies = !is_exit
        && matches!(
            input.intent,
            GateIntent::Publish | GateIntent::AutoExecution
        );
    if applies && !input.calibration_gate_enabled {
        ledger.not_applicable(
            GateId::CalibrationRequired,
            GateClass::Hard,
            "calibrated",
            "disabled by model.calibration.require_for_publish=false — an operator-governed \
             cold-start bootstrap window, not a silent pass",
        );
        return;
    }
    if !applies {
        let detail = if is_exit {
            "sell / hold-vs-exit scorers never carry a return model"
        } else {
            "only evaluated for publish / auto-execution"
        };
        ledger.not_applicable(
            GateId::CalibrationRequired,
            GateClass::Hard,
            "calibrated",
            detail,
        );
        return;
    }
    ledger.hard(
        GateId::CalibrationRequired,
        input.return_model_calibrated,
        if input.return_model_calibrated {
            "calibrated"
        } else {
            "heuristic"
        },
        "calibrated",
        "return model must be calibrated on an independent held-out split before publish / auto-execution",
    );
}

/// Hard gate: publish / auto intents require an established shadow-overlap
/// stability. Family-agnostic, so both Buy and Sell publishes are gated on it.
fn evaluate_shadow_stability_gate(input: &QualityGateInput, ledger: &mut GateLedger) {
    let t = &input.thresholds;
    if !input.intent.requires_shadow_stability() {
        ledger.not_applicable(
            GateId::ShadowOverlapStability,
            GateClass::Hard,
            t.min_shadow_overlap_stability.to_string(),
            "only evaluated for publish / auto-execution",
        );
        return;
    }
    match input.shadow_stability {
        Some(stability) => ledger.hard(
            GateId::ShadowOverlapStability,
            stability.inner() >= t.min_shadow_overlap_stability,
            stability.inner().to_string(),
            t.min_shadow_overlap_stability.to_string(),
            "shadow overlap stability below minimum",
        ),
        None => ledger.hard(
            GateId::ShadowOverlapStability,
            false,
            "none",
            t.min_shadow_overlap_stability.to_string(),
            "shadow stability not established over the required window",
        ),
    }
}

/// The largest share of resolved samples held by any single category, in
/// `[0, 1]`. Zero when there are no categorized samples.
fn max_category_concentration(report: &BacktestReport) -> Decimal {
    let total: u64 = report
        .category_breakdown
        .iter()
        .map(|metric| metric.sample_count)
        .sum();
    if total == 0 {
        return Decimal::ZERO;
    }
    let max = report
        .category_breakdown
        .iter()
        .map(|metric| metric.sample_count)
        .max()
        .unwrap_or(0);
    (Decimal::from(max) / Decimal::from(total)).round_dp(RESEARCH_DECIMAL_SCALE)
}

#[cfg(test)]
mod tests {
    use super::{
        CpcvPathSetGateInput, DefaultModelQualityGate, GateId, GateIntent, GateStatus, GateSubject,
        QualityGateInput, QualityGateThresholds, SellQualityGateThresholds,
        ValidationGateThresholds,
    };
    use chrono::Utc;
    use quant_pivot_models::{
        enums::model::ModelFamily,
        types::{
            BacktestReportId, ContentHash, MarketId, ModelVersionId, Probability,
            RuntimeConfigVersionId, TokenId, TrainingDatasetId,
        },
    };
    use rust_decimal_macros::dec;

    use crate::{
        backtest::{BacktestReport, ExpectedVsRealized, PnlSimulation},
        gates::ModelQualityGate,
        training::{DatasetCoverage, LeakageFindings, LeakageViolation},
    };

    fn hash() -> ContentHash {
        ContentHash::parse(format!("blake3:{}", "0".repeat(64))).expect("hash")
    }

    /// A healthy backtest report that clears every hard + soft gate.
    fn healthy_backtest() -> BacktestReport {
        BacktestReport {
            backtest_report_id: BacktestReportId::from_v7(),
            model_version_id: ModelVersionId::from_v7(),
            runtime_config_version_id: RuntimeConfigVersionId::from_v7(),
            window_start: Utc::now(),
            window_end: Utc::now(),
            coverage: dec!(0.99),
            sample_count: 2_000,
            missing_feature_count: 0,
            rank_ic: dec!(0.15),
            sharpe: dec!(1.2),
            hit_rate: Probability::new(dec!(0.62)),
            expected_vs_realized: ExpectedVsRealized {
                mean_expected_bps: dec!(120),
                mean_realized_bps: dec!(110),
                correlation: dec!(0.4),
                bias_bps: dec!(10),
            },
            max_drawdown: dec!(0.10),
            turnover: dec!(0.2),
            liquidity_feasibility: Probability::new(dec!(0.95)),
            category_breakdown: Vec::new(),
            tail_loss: dec!(-50),
            report_pnl_simulation: PnlSimulation {
                total_allocated_usd: dec!(10000),
                realized_pnl_usd: dec!(500),
                gross_return: dec!(0.05),
                pnl_curve: Vec::new(),
            },
            report_hash: hash(),
        }
    }

    /// Dataset coverage that clears the coverage gates.
    fn healthy_coverage() -> DatasetCoverage {
        DatasetCoverage {
            planned_samples: 2_000,
            built_examples: 1_980,
            markets: 50,
            labels_available: 1_900,
            labels_not_mature: 50,
            labels_unavailable: 50,
            samples_dropped_insufficient: 20,
            live_attribution_candidates: 0,
            live_attribution_dropped_missing_evidence: 0,
            book_decode_failures: 0,
            matrix_probe: None,
            ..Default::default()
        }
    }

    fn passing_path_set() -> CpcvPathSetGateInput {
        CpcvPathSetGateInput {
            median_rank_ic: dec!(0.15),
            deflated_sharpe: dec!(0.97),
            pbo: dec!(0.20),
            min_track_record_length_secs: Some(86_400),
        }
    }

    fn passing_input(intent: GateIntent) -> QualityGateInput {
        QualityGateInput {
            subject: GateSubject::ModelVersion(ModelVersionId::from_v7()),
            intent,
            backtest: Some(healthy_backtest()),
            dataset: healthy_coverage(),
            leakage: LeakageFindings::default(),
            shadow_stability: Some(Probability::new(dec!(0.80))),
            thresholds: QualityGateThresholds::conservative(),
            validation_thresholds: ValidationGateThresholds::conservative(),
            path_set: Some(passing_path_set()),
            sell_thresholds: SellQualityGateThresholds::default(),
            model_family: None,
            return_model_calibrated: true,
            calibration_gate_enabled: true,
        }
    }

    #[test]
    fn publish_requires_calibrated_return_model() {
        let mut input = passing_input(GateIntent::Publish);
        input.return_model_calibrated = false;
        let decision = DefaultModelQualityGate::new()
            .evaluate(input)
            .expect("evaluate");
        assert!(
            !decision.is_pass(),
            "uncalibrated return model must block publish"
        );
        assert!(
            decision
                .report()
                .hard_failures
                .iter()
                .any(|failure| failure.gate == GateId::CalibrationRequired)
        );
        // Candidate intent does not require calibration yet.
        let candidate = DefaultModelQualityGate::new()
            .evaluate(QualityGateInput {
                intent: GateIntent::Candidate,
                return_model_calibrated: false,
                ..passing_input(GateIntent::Candidate)
            })
            .expect("evaluate");
        assert!(
            candidate.is_pass(),
            "candidate intent does not require calibration"
        );
    }

    #[test]
    fn calibration_gate_disabled_records_not_applicable_not_a_silent_pass() {
        // `model.calibration.require_for_publish = false` must still record an
        // explicit, auditable `NotApplicable` row (never simply omit the gate
        // or silently flip it to `Pass`), and the overall decision must still
        // pass since no hard failure was recorded.
        let input = QualityGateInput {
            return_model_calibrated: false,
            calibration_gate_enabled: false,
            ..passing_input(GateIntent::Publish)
        };
        let decision = DefaultModelQualityGate::new()
            .evaluate(input)
            .expect("evaluate");
        assert!(
            decision.is_pass(),
            "a disabled calibration gate must not itself block publish"
        );
        let calibration_row = decision
            .report()
            .gates
            .iter()
            .find(|outcome| outcome.gate == GateId::CalibrationRequired)
            .expect("CalibrationRequired row must still be recorded when disabled");
        assert_eq!(calibration_row.status, GateStatus::NotApplicable);
    }

    #[test]
    fn sell_family_records_calibration_required_as_not_applicable() {
        // Sell / hold-vs-exit scorers never carry a return model, so the
        // gate report must record an explicit `NotApplicable` row for
        // `CalibrationRequired` (auditable end to end) rather than omitting
        // it entirely, regardless of the overall pass/fail outcome.
        let input = QualityGateInput {
            model_family: Some(ModelFamily::HoldVsExitWeighted),
            ..passing_input(GateIntent::Publish)
        };
        let decision = DefaultModelQualityGate::new()
            .evaluate(input)
            .expect("evaluate");
        let calibration_row = decision
            .report()
            .gates
            .iter()
            .find(|outcome| outcome.gate == GateId::CalibrationRequired)
            .expect("CalibrationRequired row must be present for the sell family");
        assert_eq!(calibration_row.status, GateStatus::NotApplicable);
    }

    #[test]
    fn passes_when_every_hard_gate_is_clear() {
        let decision = DefaultModelQualityGate::new()
            .evaluate(passing_input(GateIntent::Publish))
            .expect("evaluate");
        assert!(
            decision.is_pass(),
            "healthy model must clear the publish gate"
        );
        assert!(decision.report().passed);
    }

    #[test]
    fn quality_gate_blocks_low_coverage_model() {
        let mut input = passing_input(GateIntent::Publish);
        // Mostly immature / unavailable labels → low label coverage.
        input.dataset.labels_available = 100;
        input.dataset.labels_not_mature = 900;
        input.dataset.labels_unavailable = 900;
        let decision = DefaultModelQualityGate::new()
            .evaluate(input)
            .expect("evaluate");
        assert!(!decision.is_pass(), "low label coverage must be rejected");
        assert!(
            decision
                .report()
                .hard_failures
                .iter()
                .any(|failure| failure.gate == GateId::LabelCoverage),
            "the label-coverage gate must be the recorded failure"
        );
    }

    #[test]
    fn quality_gate_hard_failure_lists_failures() {
        let mut input = passing_input(GateIntent::Publish);
        input.backtest.as_mut().unwrap().sample_count = 10; // below 500
        input.backtest.as_mut().unwrap().max_drawdown = dec!(0.90); // above budget
        let decision = DefaultModelQualityGate::new()
            .evaluate(input)
            .expect("evaluate");
        assert!(!decision.is_pass());
        let gates: Vec<GateId> = decision
            .report()
            .hard_failures
            .iter()
            .map(|failure| failure.gate)
            .collect();
        assert!(gates.contains(&GateId::SampleCount));
        assert!(gates.contains(&GateId::MaxDrawdown));
    }

    #[test]
    fn quality_gate_blocks_pit_leakage() {
        let mut input = passing_input(GateIntent::Publish);
        input.leakage = LeakageFindings {
            scanned: 100,
            violations: vec![LeakageViolation {
                market_id: MarketId::new("m"),
                token_id: TokenId::new("t"),
                as_of: Utc::now(),
                cutoff: Utc::now(),
                reference: "future_book".to_owned(),
                observed_at: Utc::now(),
            }],
        };
        let decision = DefaultModelQualityGate::new()
            .evaluate(input)
            .expect("evaluate");
        assert!(!decision.is_pass(), "pit leakage must hard-block the gate");
        assert!(
            decision
                .report()
                .hard_failures
                .iter()
                .any(|failure| failure.gate == GateId::NoPitLeakage)
        );
    }

    #[test]
    fn publish_requires_shadow_stability() {
        let mut input = passing_input(GateIntent::Publish);
        input.shadow_stability = None; // not established
        let decision = DefaultModelQualityGate::new()
            .evaluate(input)
            .expect("evaluate");
        assert!(!decision.is_pass());
        assert!(
            decision
                .report()
                .hard_failures
                .iter()
                .any(|failure| failure.gate == GateId::ShadowOverlapStability)
        );
        // The candidate intent does not require shadow stability.
        let candidate = DefaultModelQualityGate::new()
            .evaluate(QualityGateInput {
                intent: GateIntent::Candidate,
                shadow_stability: None,
                ..passing_input(GateIntent::Candidate)
            })
            .expect("evaluate");
        assert!(
            candidate.is_pass(),
            "candidate intent ignores shadow stability"
        );
    }

    #[test]
    fn auto_execution_requires_liquidity_feasibility() {
        let mut input = passing_input(GateIntent::AutoExecution);
        input.backtest.as_mut().unwrap().liquidity_feasibility = Probability::new(dec!(0.10));
        let decision = DefaultModelQualityGate::new()
            .evaluate(input)
            .expect("evaluate");
        assert!(!decision.is_pass());
        assert!(
            decision
                .report()
                .hard_failures
                .iter()
                .any(|failure| failure.gate == GateId::LiquidityExitFeasible)
        );
    }

    #[test]
    fn publish_requires_backtest_report() {
        let mut input = passing_input(GateIntent::Publish);
        input.backtest = None;
        let decision = DefaultModelQualityGate::new()
            .evaluate(input)
            .expect("evaluate");
        assert!(!decision.is_pass());
        assert!(
            decision
                .report()
                .hard_failures
                .iter()
                .any(|failure| failure.gate == GateId::BacktestRequired)
        );
    }

    #[test]
    fn publish_requires_cpcv_path_set() {
        let mut input = passing_input(GateIntent::Publish);
        input.path_set = None;
        let decision = DefaultModelQualityGate::new()
            .evaluate(input)
            .expect("evaluate");
        assert!(!decision.is_pass());
        assert!(
            decision
                .report()
                .hard_failures
                .iter()
                .any(|failure| failure.gate == GateId::CpcvRequired)
        );
    }

    #[test]
    fn rank_ic_is_hard_gate_reads_path_set_median() {
        let mut input = passing_input(GateIntent::Publish);
        input.path_set.as_mut().unwrap().median_rank_ic = dec!(0.001);
        let decision = DefaultModelQualityGate::new()
            .evaluate(input)
            .expect("evaluate");
        assert!(!decision.is_pass());
        assert!(
            decision
                .report()
                .hard_failures
                .iter()
                .any(|failure| failure.gate == GateId::RankIc)
        );
    }

    #[test]
    fn dataset_ready_gate_needs_no_backtest() {
        let decision = DefaultModelQualityGate::new()
            .evaluate(QualityGateInput {
                subject: GateSubject::TrainingDataset(TrainingDatasetId::from_v7()),
                intent: GateIntent::DatasetReady,
                backtest: None,
                ..passing_input(GateIntent::DatasetReady)
            })
            .expect("evaluate");
        assert!(
            decision.is_pass(),
            "dataset-ready clears on coverage + leakage"
        );
    }
}
