//! Model-version quality-gate preview HTTP contract.
//!
//! `GET /research/models/{id}/quality-gate` runs the candidate gate as a read-only
//! dry-run (no persistence, no state change) and returns the full per-gate
//! scorecard so an operator can judge route-activation readiness before acting.
//!
//! The gate itself lives in `quant-pivot-research`; `quant-pivot-core` maps its
//! `QualityGateReport` onto these wire types (research → models mapping is done
//! in core, which sees both crates). The wire carries the complete `gates`
//! ledger plus `passed`; the `hard_failures` / `soft_warnings` split is derived
//! client-side by filtering on `class` / `status`.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::types::{BacktestReportId, model_quality::QualityGateReport};

/// Which model lifecycle transition a preview evaluates.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum GatePreviewIntent {
    /// Candidate registration readiness (coverage + leakage + backtest metrics).
    Candidate,
    /// Champion route-activation readiness (adds shadow overlap stability).
    #[default]
    RouteActivation,
    /// Auto-execution readiness (adds liquidity-exit feasibility).
    AutoExecution,
}

/// Query for `GET /research/models/{id}/quality-gate`.
#[derive(Debug, Clone, Default, Deserialize)]
pub struct QualityGatePreviewQuery {
    /// Transition to evaluate (defaults to `publish`).
    #[serde(default)]
    pub intent: GatePreviewIntent,
    /// Evaluate against a specific frozen backtest report; defaults to the
    /// version's most recent report.
    pub backtest_report_id: Option<BacktestReportId>,
}

/// One evaluated gate row — the self-describing scorecard entry.
///
/// `gate` / `class` / `status` are stable `snake_case` wire strings (`sample_count`,
/// `hard`, `pass`, …) so new gate ids flow through without a wire-schema bump;
/// the SPA keys its labels off `gate` and colors off `status`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct GateOutcomeView {
    /// Gate identity wire name (e.g. `"sample_count"`, `"shadow_overlap_stability"`).
    pub gate: String,
    /// `"hard"` (blocking) or `"soft"` (advisory).
    pub class: String,
    /// `"pass"` | `"fail"` | `"warn"` | `"not_applicable"`.
    pub status: String,
    /// The observed value (rendered).
    pub observed: String,
    /// The threshold compared against (rendered).
    pub threshold: String,
    /// Human-readable description of the failing / advisory condition.
    pub detail: String,
}

/// Read-only quality-gate evaluation for one model version.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct QualityGateReportView {
    /// Transition evaluated (`candidate`, `route_activation`, or `auto_execution`).
    pub intent: String,
    /// When the dry-run ran.
    pub evaluated_at: DateTime<Utc>,
    /// Whether every hard gate cleared.
    pub passed: bool,
    /// Every evaluated gate (pass / fail / warn / not-applicable).
    pub gates: Vec<GateOutcomeView>,
    /// Content hash over the decision projection.
    pub report_hash: String,
}

impl From<&QualityGateReport> for QualityGateReportView {
    fn from(report: &QualityGateReport) -> Self {
        Self {
            intent: report.intent.wire_name().to_owned(),
            evaluated_at: report.evaluated_at,
            passed: report.passed,
            gates: report
                .gates
                .iter()
                .map(|outcome| GateOutcomeView {
                    gate: outcome.gate.wire_name().to_owned(),
                    class: outcome.class.wire_name().to_owned(),
                    status: outcome.status.wire_name().to_owned(),
                    observed: outcome.observed.clone(),
                    threshold: outcome.threshold.clone(),
                    detail: outcome.detail.clone(),
                })
                .collect(),
            report_hash: report.report_hash.to_string(),
        }
    }
}
