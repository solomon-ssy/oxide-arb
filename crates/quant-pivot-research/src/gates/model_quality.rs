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
//! ([`GateIntent`]) selects which hard gates apply: a `Publish` adds
//! shadow-stability, and an `AutoExecution`
//! evaluation additionally requires liquidity-exit feasibility.
//!
//! The resulting [`QualityGateReport`] is content-addressed and serializes into
//! `quant_model_version.quality_gate_report`; its `evaluated_at` drives the
//! load-time staleness deny (`min_quality_gate_age_secs`).

use chrono::{DateTime, Utc};
use quant_pivot_error::QuantResult;
use quant_pivot_models::{
    enums::model::ModelFamily,
    types::{
        Probability,
        model_quality::{
            GateClass, GateId, GateIntent, GateOutcome, GateStatus, GateSubject,
            QUALITY_GATE_REPORT_FORMAT_VERSION, QualityGateFailure, QualityGateReport,
        },
    },
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
    /// Minimum resolved sample count (default 500).
    pub min_sample_count: u64,
    /// Minimum label coverage in `[0, 1]` (default 0.70).
    pub min_label_coverage: Decimal,
    /// Minimum planned-sample materialization coverage in `[0, 1]` (default 0.95).
    pub min_materialization_coverage: Decimal,
    /// Maximum tolerated drawdown in `[0, 1]` (configured).
    pub max_drawdown: Decimal,
    /// Minimum liquidity-exit feasibility in `[0, 1]` (auto, default 0.90).
    pub min_liquidity_exit_feasibility: Decimal,
    /// Minimum shadow overlap stability in `[0, 1]` (publish, default 0.60).
    pub min_shadow_overlap_stability: Decimal,
    /// Maximum (soft) per-category concentration in `[0, 1]` (default 0.60).
    pub max_category_concentration: Decimal,
}

/// CPCV alpha-significance gate thresholds (`research.validation.gates.*`).
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
    pub median_max_drawdown: Option<Decimal>,
    pub median_tail_loss: Option<Decimal>,
    pub baseline_uplift: Option<Decimal>,
    pub window_start: Option<DateTime<Utc>>,
    pub window_end: Option<DateTime<Utc>>,
}

/// Sell-side hold-vs-exit gate thresholds with hard alpha-significance fields.
///
/// DSR significance (`α`) is **not** duplicated here — Sell publish reads the
/// single authority [`ValidationGateThresholds::dsr_significance`]
/// (`research.validation.gates.dsr_significance`), the same knob CPCV uses to
/// compute Deflated Sharpe / `MinTRL`. Rank IC / PBO remain Sell-scoped so
/// operators can set exit-scorer bars independently of Buy.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SellQualityGateThresholds {
    pub min_sample_count: u64,
    pub min_label_coverage: Decimal,
    /// Minimum CPCV path-set median rank IC. This hard gate replaces the
    /// retired single-path soft `min_exit_alpha_rank_ic` and mirrors the
    /// Buy-side [`ValidationGateThresholds::rank_ic_min`].
    pub rank_ic_min: Decimal,
    /// Maximum tolerated Probability of Backtest Overfitting (hard gate).
    pub max_pbo: Decimal,
    pub min_l2_book_fidelity_ratio: Decimal,
    pub max_fallback_ratio: Decimal,
}

impl Default for SellQualityGateThresholds {
    fn default() -> Self {
        Self {
            min_sample_count: 200,
            min_label_coverage: Decimal::new(60, 2),
            rank_ic_min: Decimal::new(2, 2),
            max_pbo: Decimal::new(50, 2),
            min_l2_book_fidelity_ratio: Decimal::new(50, 2),
            max_fallback_ratio: Decimal::new(50, 2),
        }
    }
}

impl QualityGateThresholds {
    /// Conservative schema defaults.
    #[must_use]
    pub fn conservative() -> Self {
        Self {
            min_sample_count: 500,
            min_label_coverage: Decimal::new(70, 2),
            min_materialization_coverage: Decimal::new(95, 2),
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
    /// Frozen backtest report.
    pub backtest: Option<BacktestReport>,
    /// Dataset coverage accounting.
    pub dataset: DatasetCoverage,
    /// Point-in-time leakage scan.
    pub leakage: LeakageFindings,
    /// Shadow overlap stability over the required window (publish / auto).
    pub shadow_stability: Option<Probability>,
    /// Governed thresholds.
    pub thresholds: QualityGateThresholds,
    /// CPCV alpha-significance thresholds.
    pub validation_thresholds: ValidationGateThresholds,
    /// Latest persisted CPCV path-set metrics (`None` when absent).
    pub path_set: Option<CpcvPathSetGateInput>,
    /// Sell-side thresholds (used when [`Self::model_family`] is an exit scorer).
    pub sell_thresholds: SellQualityGateThresholds,
    /// Model family under evaluation (`None` ⇒ buy-oriented defaults).
    pub model_family: Option<ModelFamily>,
    /// Whether the evaluated artifact's `ReturnModelSpec` is `Calibrated`,
    /// resolved through the **same deep check**
    /// (`resolve_return_model_calibration`) the report builder, admission,
    /// and intent creation share — never a shallow enum-tag read. Buy-family
    /// `Publish`/`AutoExecution` intents hard-gate on this; exit scorers and
    /// other intents ignore it (they have no `ReturnModelSpec` concept).
    pub return_model_calibrated: bool,
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

/// The default, deterministic model-publication gate.
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
            format_version: QUALITY_GATE_REPORT_FORMAT_VERSION,
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
        // built examples when a caller has not supplied a backtest.
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
        GateId::MaterializationCoverage,
        feature_coverage >= t.min_materialization_coverage,
        feature_coverage.to_string(),
        t.min_materialization_coverage.to_string(),
        "planned-sample materialization coverage below minimum",
    );

    ledger.hard(
        GateId::NoPitLeakage,
        input.leakage.is_clean(),
        input.leakage.violation_count().to_string(),
        "0",
        "point-in-time leakage detected in training features",
    );
}

/// Sell-side hold-vs-exit hard/soft gates.
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
    let backtest_window = input
        .path_set
        .as_ref()
        .and_then(|path_set| path_set.window_start.zip(path_set.window_end));
    // DSR α is the single authority under `research.validation.gates` —
    // never a parallel `quality_gate.sell.dsr_significance` that could drift
    // from the value CPCV used when computing `deflated_sharpe`.
    evaluate_alpha_significance_gates(
        input.intent,
        input.path_set.as_ref(),
        backtest_window,
        t.rank_ic_min,
        input.validation_thresholds.dsr_significance,
        t.max_pbo,
        ledger,
    );
    if input.intent.requires_backtest() {
        evaluate_sell_path_set_risk_gates(
            input.path_set.as_ref(),
            &input.thresholds,
            &input.validation_thresholds,
            ledger,
        );
    }
}

/// Sell path-set risk/baseline gates over calendarized lot returns.
fn evaluate_sell_path_set_risk_gates(
    path_set: Option<&CpcvPathSetGateInput>,
    thresholds: &QualityGateThresholds,
    validation: &ValidationGateThresholds,
    ledger: &mut GateLedger,
) {
    let Some(path_set) = path_set else {
        ledger.not_applicable(
            GateId::MaxDrawdown,
            GateClass::Hard,
            thresholds.max_drawdown.to_string(),
            "requires a CPCV path set",
        );
        ledger.not_applicable(
            GateId::TailLossBudget,
            GateClass::Hard,
            validation.min_tail_loss_bps.to_string(),
            "requires a CPCV path set",
        );
        ledger.not_applicable(
            GateId::SellBaselineUplift,
            GateClass::Hard,
            "> 0",
            "requires a CPCV path set",
        );
        return;
    };

    match path_set.median_max_drawdown {
        Some(max_drawdown) => ledger.hard(
            GateId::MaxDrawdown,
            max_drawdown <= thresholds.max_drawdown,
            max_drawdown.to_string(),
            thresholds.max_drawdown.to_string(),
            "Sell CPCV median calendarized max drawdown exceeds budget",
        ),
        None => ledger.hard(
            GateId::MaxDrawdown,
            false,
            "none",
            thresholds.max_drawdown.to_string(),
            "Sell CPCV path set is missing median max drawdown",
        ),
    }

    match path_set.median_tail_loss {
        Some(tail_loss) => {
            let tail_loss_bps = tail_loss * Decimal::from(10_000);
            ledger.hard(
                GateId::TailLossBudget,
                tail_loss_bps >= validation.min_tail_loss_bps,
                tail_loss_bps.to_string(),
                validation.min_tail_loss_bps.to_string(),
                "Sell CPCV median calendarized tail loss exceeds budget",
            );
        }
        None => ledger.hard(
            GateId::TailLossBudget,
            false,
            "none",
            validation.min_tail_loss_bps.to_string(),
            "Sell CPCV path set is missing median tail loss",
        ),
    }

    match path_set.baseline_uplift {
        Some(uplift) => ledger.hard(
            GateId::SellBaselineUplift,
            uplift > Decimal::ZERO,
            uplift.to_string(),
            "> 0",
            "Sell CPCV median calendar return does not beat exit-at-first baseline",
        ),
        None => ledger.hard(
            GateId::SellBaselineUplift,
            false,
            "none",
            "> 0",
            "Sell CPCV path set is missing baseline uplift",
        ),
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

/// CPCV alpha-significance gates (hard) plus `MinTRL` advisory (soft) — the
/// Buy-side entry point into the family-shared
/// [`evaluate_alpha_significance_gates`] helper.
fn evaluate_cpcv_alpha_gates(input: &QualityGateInput, ledger: &mut GateLedger) {
    let validation = &input.validation_thresholds;
    let backtest_window = input
        .backtest
        .as_ref()
        .map(|report| (report.window_start, report.window_end));
    evaluate_alpha_significance_gates(
        input.intent,
        input.path_set.as_ref(),
        backtest_window,
        validation.rank_ic_min,
        validation.dsr_significance,
        validation.max_pbo,
        ledger,
    );
}

/// CPCV/lot-replay alpha-significance gates (hard) plus `MinTRL` advisory
/// (soft) — shared by the Buy (`evaluate_cpcv_alpha_gates`) and Sell
/// (`evaluate_sell_gates`) branches.
///
/// Both families' publish readiness is judged by the identical persisted
/// [`BacktestPathSet`](crate::validation::BacktestPathSet) methodology
/// (`CpcvRequired`/`RankIc`/`DeflatedSharpe`/`Pbo`/`MinTrackRecordLength`
/// reuse the exact same [`GateId`] variants for both families — mirroring how
/// `SampleCount`/`LabelCoverage` are already shared in
/// [`evaluate_coverage_gates`] and [`evaluate_sell_gates`] — only the
/// threshold source and the availability of a single-path debug
/// `backtest_window` (Buy only; Sell has none) differ.
fn evaluate_alpha_significance_gates(
    intent: GateIntent,
    path_set: Option<&CpcvPathSetGateInput>,
    backtest_window: Option<(DateTime<Utc>, DateTime<Utc>)>,
    rank_ic_min: Decimal,
    dsr_significance: Decimal,
    max_pbo: Decimal,
    ledger: &mut GateLedger,
) {
    if !intent.requires_backtest() {
        return;
    }
    let dsr_floor = Decimal::ONE - dsr_significance;
    if let Some(path_set) = path_set {
        ledger.hard(
            GateId::CpcvRequired,
            true,
            "present",
            "required",
            "a persisted CPCV path set is required before advancing this model",
        );
        ledger.hard(
            GateId::RankIc,
            path_set.median_rank_ic >= rank_ic_min,
            path_set.median_rank_ic.to_string(),
            rank_ic_min.to_string(),
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
            path_set.pbo <= max_pbo,
            path_set.pbo.to_string(),
            max_pbo.to_string(),
            "probability of backtest overfitting above maximum",
        );
        if let (Some(mintrl_secs), Some((window_start, window_end))) =
            (path_set.min_track_record_length_secs, backtest_window)
        {
            let observed_secs = window_end
                .signed_duration_since(window_start)
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
                "MinTRL unavailable when representative Sharpe is non-positive, or no single-path debug window to compare against",
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
        rank_ic_min.to_string(),
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
        max_pbo.to_string(),
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

/// Hard gate: `Publish` / `AutoExecution` intents on a Buy
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
    use chrono::{Duration, Utc};
    use quant_pivot_models::{
        enums::model::ModelFamily,
        types::{
            BacktestReportId, ContentHash, DecisionPolicySnapshotId, MarketId, ModelVersionId,
            Probability, TokenId,
            backtest::{ExpectedVsRealized, PnlSimulation},
        },
    };
    use rust_decimal_macros::dec;

    use super::{
        CpcvPathSetGateInput, DefaultModelQualityGate, GateId, GateIntent, GateStatus, GateSubject,
        QualityGateInput, QualityGateThresholds, SellQualityGateThresholds,
        ValidationGateThresholds,
    };
    use crate::{
        backtest::BacktestReport,
        gates::ModelQualityGate,
        training::{DatasetCoverage, LeakageFindings, LeakageViolation},
    };

    fn hash() -> ContentHash {
        ContentHash::parse(&format!("blake3:{}", "0".repeat(64))).expect("hash")
    }

    /// A healthy backtest report that clears every hard + soft gate.
    fn healthy_backtest() -> BacktestReport {
        BacktestReport {
            backtest_report_id: BacktestReportId::from_v7(),
            model_version_id: ModelVersionId::from_v7(),
            decision_policy_snapshot_id: DecisionPolicySnapshotId::from_v7(),
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
            median_max_drawdown: Some(dec!(0.10)),
            median_tail_loss: Some(dec!(-0.005)),
            baseline_uplift: Some(dec!(0.001)),
            window_start: Some(Utc::now()),
            window_end: Some(Utc::now() + Duration::hours(48)),
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
    fn sell_publish_requires_baseline_uplift() {
        let mut input = QualityGateInput {
            model_family: Some(ModelFamily::HoldVsExitWeighted),
            ..passing_input(GateIntent::Publish)
        };
        input.path_set.as_mut().unwrap().baseline_uplift = None;

        let decision = DefaultModelQualityGate::new()
            .evaluate(input)
            .expect("evaluate");

        assert!(!decision.is_pass());
        assert!(
            decision
                .report()
                .hard_failures
                .iter()
                .any(|failure| failure.gate == GateId::SellBaselineUplift)
        );
    }

    #[test]
    fn sell_publish_blocks_high_path_set_drawdown() {
        let mut input = QualityGateInput {
            model_family: Some(ModelFamily::HoldVsExitWeighted),
            ..passing_input(GateIntent::Publish)
        };
        input.path_set.as_mut().unwrap().median_max_drawdown = Some(dec!(0.90));

        let decision = DefaultModelQualityGate::new()
            .evaluate(input)
            .expect("evaluate");

        assert!(!decision.is_pass());
        assert!(
            decision
                .report()
                .hard_failures
                .iter()
                .any(|failure| failure.gate == GateId::MaxDrawdown)
        );
    }

    #[test]
    fn sell_publish_blocks_bad_path_set_tail_loss() {
        let mut input = QualityGateInput {
            model_family: Some(ModelFamily::HoldVsExitWeighted),
            ..passing_input(GateIntent::Publish)
        };
        // Calendarized fractional tail loss → bps far below default min_tail_loss_bps (-500).
        input.path_set.as_mut().unwrap().median_tail_loss = Some(dec!(-0.20));

        let decision = DefaultModelQualityGate::new()
            .evaluate(input)
            .expect("evaluate");

        assert!(!decision.is_pass());
        assert!(
            decision
                .report()
                .hard_failures
                .iter()
                .any(|failure| failure.gate == GateId::TailLossBudget)
        );
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
                decision_at: Utc::now(),
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
    fn pbo_gate_blocks_overfit_synthetic_strategy() {
        let mut input = passing_input(GateIntent::Publish);
        input.path_set.as_mut().unwrap().pbo = dec!(0.95);
        let decision = DefaultModelQualityGate::new()
            .evaluate(input)
            .expect("evaluate");
        assert!(!decision.is_pass());
        assert!(
            decision
                .report()
                .hard_failures
                .iter()
                .any(|failure| failure.gate == GateId::Pbo)
        );
    }

    #[test]
    fn deflated_sharpe_gate_blocks_insignificant_path_set() {
        let mut input = passing_input(GateIntent::Publish);
        // Floor is 1 - dsr_significance (default 0.05) ⇒ 0.95.
        input.path_set.as_mut().unwrap().deflated_sharpe = dec!(0.50);
        let decision = DefaultModelQualityGate::new()
            .evaluate(input)
            .expect("evaluate");
        assert!(!decision.is_pass());
        assert!(
            decision
                .report()
                .hard_failures
                .iter()
                .any(|failure| failure.gate == GateId::DeflatedSharpe)
        );
    }
}
