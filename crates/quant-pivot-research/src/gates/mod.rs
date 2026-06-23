//! Quality-gate plane: the [`ModelQualityGate`] contract and its decision type.
//!
//! Offline governance closure (3.7). 3.0 fixes the trait + decision contract;
//! the concrete gates (metric thresholds, drift, coverage) and their inputs are
//! filled in 3.7. Evaluation is synchronous and pure — a gate is a deterministic
//! function of a backtest report's metrics.

use quant_pivot_error::QuantResult;
use quant_pivot_models::types::{BacktestReportId, ModelVersionId};
use serde::{Deserialize, Serialize};

/// Inputs to a quality-gate evaluation.
///
/// Extended in 3.7 with the backtest metrics, drift stats, and coverage.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct QualityGateInput {
    /// Model version under evaluation.
    pub model_version_id: ModelVersionId,
    /// Backtest report feeding the decision.
    pub backtest_report_id: BacktestReportId,
}

/// The outcome of a quality-gate evaluation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "decision")]
pub enum QualityGateDecision {
    /// Cleared every gate.
    Pass,
    /// Failed one or more gates (hard reject).
    Reject {
        /// Human-readable failure reasons.
        reasons: Vec<String>,
    },
    /// Borderline — requires human review before publication.
    NeedsReview {
        /// Human-readable review reasons.
        reasons: Vec<String>,
    },
}

/// Evaluates whether a model version may be published.
pub trait ModelQualityGate: Send + Sync {
    /// Evaluate the gate against its inputs.
    fn evaluate(&self, input: QualityGateInput) -> QuantResult<QualityGateDecision>;
}
