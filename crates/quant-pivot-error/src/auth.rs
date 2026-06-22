//! Authentication errors (JWT / bearer credentials).
//!
//! These cover the web-layer authentication surface: bearer-token extraction,
//! JWT decoding/validation, the Redis-backed token blacklist, and password
//! credential verification at login. They are deliberately coarse and never
//! leak which specific check failed to the client — the web layer maps every
//! variant except [`AuthError::BlacklistUnavailable`] to HTTP 401 with a single
//! generic message, defeating user-enumeration and timing oracles.

use thiserror::Error;

/// Failure modes for authentication and session management.
#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum AuthError {
    /// No `Authorization: Bearer <token>` header was present on a protected
    /// route.
    #[error("missing bearer token")]
    MissingToken,

    /// The token was malformed, signed with the wrong key, or failed a claim
    /// check other than expiry.
    #[error("invalid token")]
    InvalidToken,

    /// The token's `exp` claim is in the past.
    #[error("token expired")]
    ExpiredToken,

    /// The token's `jti` is present in the revocation blacklist (logged out or
    /// rotated).
    #[error("token revoked")]
    Blacklisted,

    /// An access token was supplied where a refresh token was required (or vice
    /// versa).
    #[error("wrong token type: expected {expected}")]
    WrongTokenType {
        /// The token type the endpoint required.
        expected: &'static str,
    },

    /// Username/password verification failed, or the account is not active.
    ///
    /// Intentionally indistinguishable from "user does not exist" to prevent
    /// account enumeration.
    #[error("invalid credentials")]
    InvalidCredentials,

    /// The token blacklist store (Redis) could not be reached. Authentication
    /// **fails closed**: a request cannot be authenticated while revocation
    /// status is unknown.
    #[error("token store unavailable")]
    BlacklistUnavailable,
}
