//! Password and credential security errors.

use thiserror::Error;

/// Failure modes for password hashing primitives (argon2id).
///
/// Verification never surfaces an error (it is fail-closed and returns
/// `false`); this type is only produced by the hashing path where a structured
/// failure must be reported to the caller.
#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum PasswordError {
    /// The argon2id hashing routine failed to produce a PHC string.
    #[error("password hashing failed: {0}")]
    HashFailed(String),
}
