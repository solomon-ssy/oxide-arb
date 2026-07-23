//! Execution-plane typed errors.

use thiserror::Error;

/// Failures in order-intent, admission, dispatch, capital, and reconciliation
/// execution flows.
#[derive(Debug, Error)]
pub enum ExecutionError {
    /// Report-only mode forbids new entry order submission.
    #[error("report-only mode forbids order submission")]
    ReportOnlyMode,

    /// Runtime mode or lifecycle preflight blocks the requested operation.
    #[error("mode preflight denied: {reason}")]
    ModePreflightDenied { reason: String },

    /// Operational kill-switch state blocks the requested execution operation.
    #[error("kill switch blocks execution: state={state}, operation={operation}")]
    KillSwitchBlocks { state: String, operation: String },

    /// Recommendation/report TTL invalidates an intent.
    #[error("recommendation expired: {reason}")]
    RecommendationExpired { reason: String },

    /// A persisted or configured execution timestamp/duration cannot be
    /// represented at the target boundary. Never substitute epoch/zero/MAX.
    #[error("execution time conversion failed for `{field}` value `{value}`: {detail}")]
    TimeConversion {
        field: &'static str,
        value: String,
        detail: String,
    },

    /// Intent state cannot accept the requested transition.
    #[error("order intent `{intent_id}` is not submittable from state `{state}`")]
    NotSubmittable { intent_id: String, state: String },

    /// Policy denied intent creation or approval.
    #[error("intent denied by policy: {reason}")]
    IntentDenied { reason: String },

    /// Admission engine denied the intent (terminal — do not retry without human action).
    #[error("admission denied: {reason}")]
    AdmissionDenied { reason: String },

    /// Admission deferred the intent (transient — retry later; intent stays submittable).
    #[error("admission deferred: {reason}")]
    AdmissionDeferred { reason: String },

    /// Approval has been invalidated by a newer state/config/market fact.
    #[error("approval invalidated: {reason}")]
    ApprovalInvalidated { reason: String },

    /// Capital reservation/allocation invariants could not be recovered.
    #[error("capital recovery failed: {reason}")]
    CapitalRecoveryFailed { reason: String },

    /// Reconciliation reached an ambiguous/unresolvable state.
    #[error("reconciliation unresolvable: {reason}")]
    ReconciliationUnresolvable { reason: String },

    /// Settlement redemption service invariant failed.
    #[error("settlement redeem invariant failed: {reason}")]
    SettlementRedeemInvariant { reason: String },

    /// A risk-increasing hold-to-resolution entry has no verified recovery path.
    #[error("automatic settlement recovery is unavailable: {reason}")]
    SettlementRecoveryUnavailable { reason: String },

    /// Runtime-mode transition is not allowed.
    #[error("mode transition forbidden: {reason}")]
    ModeTransitionForbidden { reason: String },

    /// Operator resolve targeted a reconciliation row that is not blocking.
    #[error("reconciliation `{reconciliation_id}` is not operator-resolvable (result={result})")]
    ReconciliationNotResolvable {
        reconciliation_id: String,
        result: String,
    },

    /// Operator resolve payload is invalid for the chosen terminal result.
    #[error("reconciliation resolve invalid: {detail}")]
    ReconciliationResolveInvalid { detail: String },
}
