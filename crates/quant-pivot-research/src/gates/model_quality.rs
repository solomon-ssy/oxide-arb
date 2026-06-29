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
use quant_pivot_models::types::{ContentHash, ModelVersionId, Probability, TrainingDatasetId};
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
    /// Rank information coefficient positive (soft).
    RankIc,
    /// Directional hit rate (soft).
    HitRate,
    /// Per-category concentration within budget (soft).
    CategoryConcentration,
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
    /// Minimum (soft) rank IC; `<=` triggers a soft warning (default 0).
    pub min_rank_ic: Decimal,
    /// Maximum (soft) per-category concentration in `[0, 1]` (default 0.60).
    pub max_category_concentration: Decimal,
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
            min_rank_ic: Decimal::ZERO,
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
        let mut hard: Vec<QualityGateFailure> = Vec::new();
        let mut soft: Vec<QualityGateFailure> = Vec::new();

        evaluate_coverage_gates(&input, &mut hard);
        evaluate_backtest_presence(&input, &mut hard);
        if let Some(report) = &input.backtest {
            evaluate_backtest_gates(report, &input.thresholds, &mut hard, &mut soft);
        }
        evaluate_intent_gates(&input, &mut hard);

        let passed = hard.is_empty();
        let report_hash = ResearchHasher::canonical(&ReportHashInput {
            subject: &input.subject,
            intent: input.intent,
            hard_failures: &hard,
            soft_warnings: &soft,
            passed,
        })?;
        let report = QualityGateReport {
            subject: input.subject,
            intent: input.intent,
            evaluated_at: Utc::now(),
            hard_failures: hard.clone(),
            soft_warnings: soft,
            passed,
            report_hash,
        };

        if passed {
            Ok(QualityGateDecision::Pass { report })
        } else {
            Ok(QualityGateDecision::Fail {
                report,
                hard_failures: hard,
            })
        }
    }
}

/// Coverage + leakage hard gates (every intent).
fn evaluate_coverage_gates(input: &QualityGateInput, hard: &mut Vec<QualityGateFailure>) {
    let t = &input.thresholds;
    // Sample count: prefer the backtest's resolved samples, else the dataset's
    // built examples (DatasetReady has no backtest).
    let samples = input
        .backtest
        .as_ref()
        .map_or(input.dataset.built_examples, |report| report.sample_count);
    if samples < t.min_sample_count {
        hard.push(QualityGateFailure {
            gate: GateId::SampleCount,
            observed: samples.to_string(),
            threshold: t.min_sample_count.to_string(),
            detail: "insufficient resolved samples".to_owned(),
        });
    }

    let label_coverage = input.dataset.label_coverage();
    if label_coverage < t.min_label_coverage {
        hard.push(QualityGateFailure {
            gate: GateId::LabelCoverage,
            observed: label_coverage.to_string(),
            threshold: t.min_label_coverage.to_string(),
            detail: "label coverage below minimum".to_owned(),
        });
    }

    let feature_coverage = input.dataset.feature_build_coverage();
    if feature_coverage < t.min_critical_feature_coverage {
        hard.push(QualityGateFailure {
            gate: GateId::CriticalFeatureCoverage,
            observed: feature_coverage.to_string(),
            threshold: t.min_critical_feature_coverage.to_string(),
            detail: "critical-feature coverage below minimum".to_owned(),
        });
    }

    if !input.leakage.is_clean() {
        hard.push(QualityGateFailure {
            gate: GateId::NoPitLeakage,
            observed: input.leakage.violation_count().to_string(),
            threshold: "0".to_owned(),
            detail: "point-in-time leakage detected in training features".to_owned(),
        });
    }
}

/// Hard gate: model intents that consume backtest metrics must carry a report.
fn evaluate_backtest_presence(input: &QualityGateInput, hard: &mut Vec<QualityGateFailure>) {
    if !input.intent.requires_backtest() {
        return;
    }
    if input.backtest.is_some() {
        return;
    }
    hard.push(QualityGateFailure {
        gate: GateId::BacktestRequired,
        observed: "none".to_owned(),
        threshold: "required".to_owned(),
        detail: "a frozen backtest report is required before advancing this model".to_owned(),
    });
}

/// Backtest-metric gates: drawdown (hard) + rank IC / hit rate / concentration (soft).
fn evaluate_backtest_gates(
    report: &BacktestReport,
    t: &QualityGateThresholds,
    hard: &mut Vec<QualityGateFailure>,
    soft: &mut Vec<QualityGateFailure>,
) {
    if report.max_drawdown > t.max_drawdown {
        hard.push(QualityGateFailure {
            gate: GateId::MaxDrawdown,
            observed: report.max_drawdown.to_string(),
            threshold: t.max_drawdown.to_string(),
            detail: "max drawdown exceeds budget".to_owned(),
        });
    }
    if report.rank_ic <= t.min_rank_ic {
        soft.push(QualityGateFailure {
            gate: GateId::RankIc,
            observed: report.rank_ic.to_string(),
            threshold: format!("> {}", t.min_rank_ic),
            detail: "rank IC is not positive".to_owned(),
        });
    }
    let hit_rate = report.hit_rate.inner();
    if hit_rate < Decimal::new(5, 1) {
        soft.push(QualityGateFailure {
            gate: GateId::HitRate,
            observed: hit_rate.to_string(),
            threshold: "0.5".to_owned(),
            detail: "directional hit rate below 0.5".to_owned(),
        });
    }
    let concentration = max_category_concentration(report);
    if concentration > t.max_category_concentration {
        soft.push(QualityGateFailure {
            gate: GateId::CategoryConcentration,
            observed: concentration.to_string(),
            threshold: t.max_category_concentration.to_string(),
            detail: "samples concentrated in a single category".to_owned(),
        });
    }
}

/// Intent-specific hard gates: liquidity feasibility (auto) + shadow stability
/// (publish / auto).
fn evaluate_intent_gates(input: &QualityGateInput, hard: &mut Vec<QualityGateFailure>) {
    let t = &input.thresholds;
    if input.intent.requires_liquidity_feasibility() {
        match &input.backtest {
            Some(report) => {
                let feasible = report.liquidity_feasibility.inner();
                if feasible < t.min_liquidity_exit_feasibility {
                    hard.push(QualityGateFailure {
                        gate: GateId::LiquidityExitFeasible,
                        observed: feasible.to_string(),
                        threshold: t.min_liquidity_exit_feasibility.to_string(),
                        detail: "liquidity-exit feasibility below minimum".to_owned(),
                    });
                }
            }
            None => hard.push(QualityGateFailure {
                gate: GateId::LiquidityExitFeasible,
                observed: "none".to_owned(),
                threshold: t.min_liquidity_exit_feasibility.to_string(),
                detail: "auto-execution gate requires a backtest report".to_owned(),
            }),
        }
    }

    if input.intent.requires_shadow_stability() {
        match input.shadow_stability {
            Some(stability) if stability.inner() >= t.min_shadow_overlap_stability => {}
            Some(stability) => hard.push(QualityGateFailure {
                gate: GateId::ShadowOverlapStability,
                observed: stability.inner().to_string(),
                threshold: t.min_shadow_overlap_stability.to_string(),
                detail: "shadow overlap stability below minimum".to_owned(),
            }),
            None => hard.push(QualityGateFailure {
                gate: GateId::ShadowOverlapStability,
                observed: "none".to_owned(),
                threshold: t.min_shadow_overlap_stability.to_string(),
                detail: "shadow stability not established over the required window".to_owned(),
            }),
        }
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
        DefaultModelQualityGate, GateId, GateIntent, GateSubject, QualityGateInput,
        QualityGateThresholds,
    };
    use chrono::Utc;
    use quant_pivot_models::types::{
        BacktestReportId, ContentHash, MarketId, ModelVersionId, Probability,
        RuntimeConfigVersionId, TokenId, TrainingDatasetId,
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
                equity_curve: Vec::new(),
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
        }
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
