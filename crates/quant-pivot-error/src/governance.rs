//! Model-governance errors (publish / rollback / dataset promotion).
//!
//! Money-critical governance refusals are dedicated variants so callers (and the
//! audit trail) can distinguish a blocked publish from an infrastructure fault.
//! Folds into [`crate::QuantError`] via `#[from]`.

use thiserror::Error;

/// Failure modes of the offline governance closure.
#[derive(Debug, Error)]
pub enum GovernanceError {
    /// A governance count cannot be represented by its durable/API type.
    #[error("governance numeric overflow in {field}: {detail}")]
    NumericOverflow {
        /// The count being converted.
        field: &'static str,
        /// The checked-conversion failure.
        detail: String,
    },

    /// A market-linkage outcome could not be projected into its durable JSON
    /// payload. The row must not be appended with partial provenance.
    #[error("market-linkage payload serialization failed: {detail}")]
    LinkagePayloadSerialization {
        /// The serialization failure.
        detail: String,
    },

    /// A quality gate did not clear; the model / dataset may not advance.
    #[error("quality gate failed for {entity} `{id}`: {failures}")]
    QualityGateFailed {
        /// The gated entity kind (e.g. `model_version`, `training_dataset`).
        entity: &'static str,
        /// The gated entity id.
        id: String,
        /// Rendered hard-failure summary.
        failures: String,
    },

    /// Shadow stability was not established over the required window.
    #[error("shadow stability not established: {detail}")]
    ShadowNotStable {
        /// Context describing the missing / insufficient stability.
        detail: String,
    },

    /// A governance action requested an illegal lifecycle transition.
    #[error("illegal governance transition: {detail}")]
    IllegalTransition {
        /// Context describing the rejected transition.
        detail: String,
    },

    /// A required governance entity (version / dataset / predecessor) was absent.
    #[error("governance target not found: {entity} `{id}`")]
    NotFound {
        /// The missing entity kind.
        entity: &'static str,
        /// The missing entity id.
        id: String,
    },
}
