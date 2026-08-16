//! Report generation pipeline errors.

use thiserror::Error;
use uuid::Uuid;

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

    /// A report workload dimension exceeded its deployment-proven capacity.
    /// The caller must fail the entire report; truncation is forbidden.
    #[error("report resource capacity exceeded: {resource}={actual}, ceiling={ceiling}")]
    ResourceCapacityExceeded {
        resource: &'static str,
        actual: usize,
        ceiling: usize,
    },

    /// Report diff operands belong to different authority scopes.
    #[error("reports are not comparable: {detail}")]
    IncomparableReports { detail: String },

    /// One represented Route lacks an atomically compatible serving artifact set.
    #[error("represented Route `{route}` is not ready: {detail}")]
    RouteReadiness { route: String, detail: String },

    /// A real positive account position cannot be represented by the pinned
    /// serving generation. Reports fail closed until that route is active.
    #[error(
        "positive open exposure {token_id} in market {market_id} requires inactive Route `{route}`"
    )]
    UnmodeledOpenExposure {
        route: String,
        market_id: String,
        token_id: String,
    },

    /// A report-bound history head changed revision or gained quarantine evidence.
    #[error("history window seal {seal_id} was invalidated: {detail}")]
    HistoryWindowInvalidated { seal_id: Uuid, detail: String },

    /// No immutable serving head existed at the report decision boundary.
    #[error("finalized execution-history serving head is unavailable: {detail}")]
    HistoryServingHeadUnavailable { detail: String },

    /// The promoted joint-scenario artifact is absent, malformed, or incompatible.
    #[error("portfolio scenario artifact contract failed: {detail}")]
    ScenarioArtifact { detail: String },

    /// `HiGHS` did not prove one lexicographic stage optimal.
    #[error("global portfolio optimization failed at {stage}: {detail}")]
    PortfolioOptimization { stage: &'static str, detail: String },

    /// Exact Decimal recomputation disagreed with the solver projection.
    #[error("global portfolio exact verification failed: {detail}")]
    PortfolioPostCheck { detail: String },
}
