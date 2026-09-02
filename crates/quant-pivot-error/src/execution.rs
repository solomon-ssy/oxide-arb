//! Execution-plane typed errors.

use thiserror::Error;

use crate::api::ClobFundingDeficit;

/// Failures in order-intent, admission, dispatch, capital, and reconciliation
/// execution flows.
#[derive(Debug, Error)]
pub enum ExecutionError {
    /// Entry-authorization preflight blocks policy-automatic authorization.
    #[error("entry authorization preflight denied: {reason}")]
    AuthorizationPreflightDenied { reason: String },

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

    /// A valid live conditional-token funding snapshot cannot authorize an exit
    /// yet. No WAL row or venue POST has been created, so retry is safe.
    #[error(
        "exit funding deferred ({deficit}): required={required}, balance={balance}, allowance={allowance}"
    )]
    ExitFundingDeferred {
        deficit: ClobFundingDeficit,
        required: String,
        balance: String,
        allowance: String,
    },

    /// Current venue tick rules cannot represent the governed SELL hard
    /// minimum without exceeding the valid price range. No WAL/POST occurred.
    #[error("exit price deferred: {reason}")]
    ExitPriceDeferred { reason: String },

    /// Approval has been invalidated by a newer state/config/market fact.
    #[error("approval invalidated: {reason}")]
    ApprovalInvalidated { reason: String },

    /// Capital reservation/allocation invariants could not be recovered.
    #[error("capital recovery failed: {reason}")]
    CapitalRecoveryFailed { reason: String },

    /// Reconciliation reached an ambiguous/unresolvable state.
    #[error("reconciliation unresolvable: {reason}")]
    ReconciliationUnresolvable { reason: String },

    /// A finalized accepted chain event cannot be projected into account truth.
    #[error("account chain execution projection failed: {reason}")]
    AccountChainProjection { reason: String },

    /// Account pause, evidence, allocation, or manifest invariants did not converge.
    #[error("account recovery failed: {reason}")]
    AccountRecovery { reason: String },

    /// The canonical finalized resolution source could not be read or verified.
    #[error("outcome reconciliation source failed: {reason}")]
    OutcomeReconciliationSource { reason: String },

    /// Cross-store outcome source/cursor/fact invariants did not hold.
    #[error("outcome reconciliation invariant failed: {reason}")]
    OutcomeReconciliationInvariant { reason: String },

    /// Settlement redemption service invariant failed.
    #[error("settlement redeem invariant failed: {reason}")]
    SettlementRedeemInvariant { reason: String },

    /// A risk-increasing hold-to-resolution entry has no verified recovery path.
    #[error("automatic settlement recovery is unavailable: {reason}")]
    SettlementRecoveryUnavailable { reason: String },

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
