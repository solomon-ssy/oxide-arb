//! Quality-gate plane: the [`ModelQualityGate`] contract and its decision type.
//!
//! Offline governance closure. The trait + decision contract live here;
//! the concrete gate ([`DefaultModelQualityGate`]) and its inputs / report /
//! thresholds live in [`model_quality`]. Evaluation is synchronous and pure — a
//! gate is a deterministic function of a frozen backtest report, dataset
//! coverage, the leakage scan, and the shadow overlap stability.

pub mod model_quality;

pub use model_quality::{
    CpcvPathSetGateInput, DefaultModelQualityGate, QualityGateInput, QualityGateThresholds,
    SellQualityGateThresholds, ValidationGateThresholds,
};
use quant_pivot_error::QuantResult;
use quant_pivot_models::types::model_quality::{QualityGateFailure, QualityGateReport};

/// The outcome of a quality-gate evaluation.
///
/// Both arms carry the full [`QualityGateReport`] (which serializes into
/// `quant_model_version.quality_gate_report`); `Fail` additionally surfaces the
/// hard failures directly so callers reject the advance without re-scanning.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum QualityGateDecision {
    /// Cleared every hard gate (soft warnings may still be present in the report).
    Pass {
        /// The persisted evaluation.
        report: QualityGateReport,
    },
    /// Failed one or more hard gates.
    Fail {
        /// The persisted evaluation.
        report: QualityGateReport,
        /// The hard failures that blocked the advance.
        hard_failures: Vec<QualityGateFailure>,
    },
}

impl QualityGateDecision {
    /// The persisted report, regardless of outcome.
    #[must_use]
    pub const fn report(&self) -> &QualityGateReport {
        match self {
            Self::Pass { report } | Self::Fail { report, .. } => report,
        }
    }

    /// Whether the model cleared every hard gate.
    #[must_use]
    pub const fn is_pass(&self) -> bool {
        matches!(self, Self::Pass { .. })
    }
}

/// Evaluates whether a model version (or dataset promotion) may advance.
pub trait ModelQualityGate: Send + Sync {
    /// Evaluate the gate against its inputs.
    ///
    /// # Errors
    ///
    /// Propagates canonical-hash failures when sealing the report.
    fn evaluate(&self, input: QualityGateInput) -> QuantResult<QualityGateDecision>;
}
