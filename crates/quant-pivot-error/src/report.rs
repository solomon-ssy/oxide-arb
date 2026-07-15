//! Report generation pipeline errors.

use thiserror::Error;

/// Failures during `TopN` report composition and persistence.
#[derive(Debug, Error)]
pub enum ReportError {
    /// A pipeline stage found missing upstream artifacts.
    #[error("report pipeline invariant at {stage}: {detail}")]
    InvariantViolation { stage: &'static str, detail: String },

    /// A count or rank exceeds a DB column or API constraint.
    #[error("numeric overflow in {field}: {detail}")]
    NumericOverflow { field: &'static str, detail: String },

    /// Caller-supplied inputs violate a batch contract (e.g. length mismatch).
    #[error("report pipeline contract violation: {detail}")]
    ContractViolation { detail: String },

    /// The complete catalog exceeded the deployment-proven report capacity.
    /// The caller must fail the entire report; truncation is forbidden.
    #[error("report resource capacity exceeded: catalog_visible={actual}, ceiling={ceiling}")]
    ResourceCapacityExceeded { actual: usize, ceiling: usize },

    /// An empty report was built but suppressed by `publish_empty_reports=false`.
    #[error("empty report suppressed: {reason}")]
    EmptyReportSuppressed { reason: String },
}
