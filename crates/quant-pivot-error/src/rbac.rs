//! Role-based access-control (RBAC) domain errors.
//!
//! These cover the user / role / menu / permission management surface and the
//! Casbin policy reverse-lookup path (`ResourceType` / `Operation` parsing).

use thiserror::Error;

/// Errors raised by RBAC management and permission resolution.
#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum RbacError {
    /// A referenced RBAC entity does not exist.
    #[error("{entity} not found: {id}")]
    NotFound {
        /// Logical entity name (e.g. `user`, `role`, `menu`).
        entity: &'static str,
        /// The identifier or natural key that was looked up.
        id: String,
    },

    /// A unique constraint would be violated (e.g. duplicate username/role code).
    #[error("{entity} already exists: {key}")]
    Duplicate {
        /// Logical entity name.
        entity: &'static str,
        /// The conflicting natural key.
        key: String,
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
