//! Feedback-cycle orchestration and immutable evidence errors.

use thiserror::Error;

use crate::{hashing::CanonicalDigestError, rbac::RbacError, storage::StorageError};

/// Stable failures raised by feedback-cycle contracts and orchestration.
///
/// Persistence transport failures remain [`crate::storage::StorageError`].
/// These variants describe semantic corruption or rejected lifecycle actions
/// and therefore fail closed before a repository mutates durable state.
#[derive(Debug, Error)]
pub enum FeedbackError {
    #[error("invalid feedback-cycle identity: {detail}")]
    InvalidCycleIdentity { detail: String },

    #[error("invalid feedback-cycle state: {detail}")]
    InvalidCycleState { detail: String },

    #[error("illegal feedback-cycle transition from {from} to {to}")]
    IllegalCycleTransition { from: String, to: String },

    #[error("feedback-cycle generation mismatch: expected {expected}, got {actual}")]
    StaleCycleGeneration { expected: i64, actual: i64 },

    #[error("invalid feedback stage event: {detail}")]
    InvalidStageEvent { detail: String },

    #[error("invalid feedback research-job identity: {detail}")]
    InvalidJobIdentity { detail: String },

    #[error("invalid research-job enqueue contract: {detail}")]
    InvalidJobContract { detail: String },

    #[error("invalid feedback-coordinator configuration: {detail}")]
    InvalidCoordinatorConfig { detail: String },

    #[error("invalid feedback-coordinator state: {detail}")]
    InvalidCoordinatorState { detail: String },

    #[error("invalid drift report: {detail}")]
    InvalidDriftReport { detail: String },

    #[error("invalid evaluation-use evidence: {detail}")]
    InvalidEvaluationUse { detail: String },

    #[error("evaluation holdout has already been consumed: {semantic_use_hash}")]
    EvaluationReuse { semantic_use_hash: String },

    #[error("invalid feedback comparison contract: {detail}")]
    InvalidComparisonContract { detail: String },

    #[error("invalid feedback comparison evidence: {detail}")]
    InvalidComparisonEvidence { detail: String },

    #[error("feedback comparison same-window mismatch: {detail}")]
    SameWindowMismatch { detail: String },

    #[error("invalid promotion permit: {detail}")]
    InvalidPromotionPermit { detail: String },

    #[error("promotion permit conflict: {detail}")]
    PromotionPermitConflict { detail: String },

    #[error("invalid shared model-route evidence: {detail}")]
    InvalidModelRouteEvidence { detail: String },

    #[error("invalid promotion preflight: {detail}")]
    InvalidPromotionPreflight { detail: String },

    #[error("model-route promotion conflict: {detail}")]
    PromotionTransactionConflict { detail: String },

    #[error("invalid model-route bootstrap preflight: {detail}")]
    InvalidBootstrapPreflight { detail: String },

    #[error("model-route bootstrap conflict: {detail}")]
    BootstrapTransactionConflict { detail: String },

    #[error("model-route shadow slot {route} is occupied by binding {binding_id}")]
    ShadowOccupied { route: String, binding_id: String },

    #[error(
        "model-route shadow memory budget exceeded: active={active_bytes}, requested={requested_bytes}, limit={limit_bytes}"
    )]
    ShadowMemoryBudgetExceeded {
        active_bytes: u64,
        requested_bytes: u64,
        limit_bytes: u64,
    },

    #[error("model-route shadow-binding conflict: {detail}")]
    ShadowBindingConflict { detail: String },

    #[error("model-route runtime convergence conflict: {detail}")]
    ModelRouteConvergenceConflict { detail: String },

    #[error(transparent)]
    Hash(#[from] CanonicalDigestError),
}

/// Failures from the transaction-owning promotion-permit repository.
///
/// The repository crosses persistence, domain-integrity, and authorization
/// boundaries in one atomic operation, so callers must retain all three typed
/// error families instead of flattening them into a storage conflict.
#[derive(Debug, Error)]
pub enum PromotionPermitCommandError {
    #[error(transparent)]
    Contract(#[from] FeedbackError),

    #[error(transparent)]
    Authorization(#[from] RbacError),

    #[error(transparent)]
    Storage(#[from] StorageError),
}

/// Failures from a transaction-owned feedback trigger or cancellation.
///
/// Governed cycle mutations authorize the actor and append immutable evidence
/// in the same transaction, so callers must retain contract, authorization,
/// and persistence failures as distinct typed causes.
#[derive(Debug, Error)]
pub enum FeedbackCycleCommandError {
    #[error(transparent)]
    Contract(#[from] FeedbackError),

    #[error(transparent)]
    Authorization(#[from] RbacError),

    #[error(transparent)]
    Storage(#[from] StorageError),
}

/// Failures from the atomic model-route promotion transaction.
#[derive(Debug, Error)]
pub enum PromotionCommitError {
    #[error(transparent)]
    Contract(#[from] FeedbackError),

    #[error(transparent)]
    Authorization(#[from] RbacError),

    #[error(transparent)]
    Storage(#[from] StorageError),
}

/// Failures from the atomic first-champion model-route transaction.
#[derive(Debug, Error)]
pub enum RouteBootstrapCommitError {
    #[error(transparent)]
    Contract(#[from] FeedbackError),

    #[error(transparent)]
    Authorization(#[from] RbacError),

    #[error(transparent)]
    Storage(#[from] StorageError),
}
