//! Role-based access-control (RBAC) domain errors.
//!
//! Persistence failures (`NotFound`, `Duplicate`, …) surface as
//! [`crate::storage::StorageError`] via repository ports. This enum covers
//! Casbin policy parsing and structural assignment validation only.

use thiserror::Error;

/// Errors raised by RBAC permission resolution and assignment validation.
#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum RbacError {
    /// An authenticated actor does not currently hold the complete governed
    /// authority required for a mutation.
    #[error(
        "permission denied for actor {actor_user_id} acting as {acting_role}: \
         {resource}:{operation}"
    )]
    PermissionDenied {
        /// Authenticated user identity.
        actor_user_id: String,
        /// Explicit acting-role code.
        acting_role: String,
        /// Governed resource token.
        resource: &'static str,
        /// Governed operation token.
        operation: &'static str,
    },

    /// A Casbin policy string could not be mapped back to a known
    /// `ResourceType` / `Operation` pair.
    #[error("unknown permission: {resource}:{operation}")]
    UnknownPermission {
        /// The resource token that failed to parse.
        resource: String,
        /// The operation token that failed to parse.
        operation: String,
    },

    /// A role/permission assignment request is structurally invalid (e.g. an
    /// operation not allowed for the target resource).
    #[error("invalid assignment: {detail}")]
    InvalidAssignment {
        /// Human-readable explanation of why the assignment was rejected.
        detail: String,
    },
}
