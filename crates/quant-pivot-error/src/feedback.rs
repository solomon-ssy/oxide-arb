//! Feedback-cycle orchestration and immutable evidence errors.

use thiserror::Error;

use crate::hashing::CanonicalDigestError;

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

    #[error("invalid drift report: {detail}")]
    InvalidDriftReport { detail: String },

    #[error("invalid evaluation-use evidence: {detail}")]
    InvalidEvaluationUse { detail: String },

    #[error("evaluation holdout has already been consumed: {semantic_use_hash}")]
    EvaluationReuse { semantic_use_hash: String },

    #[error(transparent)]
    Hash(#[from] CanonicalDigestError),
}
