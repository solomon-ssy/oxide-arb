//! Web-layer error type and HTTP mapping.
//!
//! [`WebError`] is the single error surfaced by handlers, extractors, and
//! middleware. Its `ResponseError` implementation renders the *same* envelope
//! as [`crate::response::WebResponse`] (with `data: null`), so success and
//! error payloads are structurally identical.
//!
//! Domain errors are funneled in via `From` conversions:
//!
//! - [`AuthError`] — every variant maps to `401` except a token-store outage,
//!   which is `503`. The client-facing message is deliberately generic so it
//!   cannot be used as an authentication oracle.
//! - [`StorageError`] — `NotFound` → 404, `Conflict` → 409, transport/timeout
//!   failures → 503, everything else → 500 (details are logged, not leaked).
//! - [`RbacError`] — `NotFound` → 404, `Duplicate` → 409, structural errors →
//!   400.
//! - [`GovernanceError`] / [`RegistryError`] — governance invariants map to
//!   400 (malformed request), 403 (risk-expanding change without approval), 409
//!   (state / concurrency / idempotency conflict), and 500 (hashing / audit-chain
//!   integrity). `RegistryError` recurses into its inner governance/storage error.

use actix_web::{HttpResponse, ResponseError, http::StatusCode};
use oxide_arb_error::{
    auth::AuthError,
    control::{GovernanceError, RegistryError},
    rbac::RbacError,
    storage::StorageError,
};
use oxide_arb_models::domain::{DailySeriesRangeError, RuntimeControlError, WindowQueryError};
use thiserror::Error;

use crate::response::WebResponse;

/// The unified error returned across the web layer.
#[derive(Debug, Error)]
pub enum WebError {
    /// Authentication failed or was absent (HTTP 401).
    #[error("unauthorized: {0}")]
    Unauthorized(String),

    /// Authenticated but not permitted (HTTP 403).
    #[error("forbidden")]
    Forbidden,

    /// The requested resource does not exist (HTTP 404).
    #[error("not found: {0}")]
    NotFound(String),

    /// Malformed request / failed validation (HTTP 400).
    #[error("bad request: {0}")]
    BadRequest(String),

    /// A uniqueness or state conflict (HTTP 409).
    #[error("conflict: {0}")]
    Conflict(String),

    /// An unexpected server-side failure (HTTP 500). The detail is logged but
    /// never returned to the client.
    #[error("internal error: {0}")]
    Internal(String),

    /// A dependency (DB / Redis) is unavailable (HTTP 503).
    #[error("service unavailable: {0}")]
    ServiceUnavailable(String),
}

impl WebError {
    /// HTTP status code for this error.
    const fn status(&self) -> StatusCode {
        match self {
            Self::Unauthorized(_) => StatusCode::UNAUTHORIZED,
            Self::Forbidden => StatusCode::FORBIDDEN,
            Self::NotFound(_) => StatusCode::NOT_FOUND,
            Self::BadRequest(_) => StatusCode::BAD_REQUEST,
            Self::Conflict(_) => StatusCode::CONFLICT,
            Self::Internal(_) => StatusCode::INTERNAL_SERVER_ERROR,
            Self::ServiceUnavailable(_) => StatusCode::SERVICE_UNAVAILABLE,
        }
    }

    /// Client-safe message. Internal failures are masked so implementation
    /// details never reach the wire.
    fn client_message(&self) -> String {
        match self {
            Self::Unauthorized(msg)
            | Self::NotFound(msg)
            | Self::BadRequest(msg)
            | Self::Conflict(msg)
            | Self::ServiceUnavailable(msg) => msg.clone(),
            Self::Forbidden => "forbidden".to_owned(),
            Self::Internal(_) => "internal error".to_owned(),
        }
    }
}

impl ResponseError for WebError {
    fn status_code(&self) -> StatusCode {
        self.status()
    }

    fn error_response(&self) -> HttpResponse {
        let status = self.status();
        if let Self::Internal(detail) = self {
            tracing::error!(%detail, "web handler internal error");
        }
        let envelope = WebResponse::<()> {
            code: status.as_u16(),
            message: self.client_message(),
            data: None,
        };
        HttpResponse::build(status).json(envelope)
    }
}

impl From<AuthError> for WebError {
    fn from(error: AuthError) -> Self {
        match error {
            // Fail-closed: the token revocation store is unreachable, so we
            // cannot prove a token is still valid — treat as transient outage.
            AuthError::BlacklistUnavailable => {
                Self::ServiceUnavailable("authentication temporarily unavailable".to_owned())
            }
            AuthError::InvalidCredentials => Self::Unauthorized("invalid credentials".to_owned()),
            // All token-shaped failures collapse to one generic message so the
            // client cannot distinguish missing / malformed / expired / revoked.
            AuthError::MissingToken
            | AuthError::InvalidToken
            | AuthError::ExpiredToken
            | AuthError::Blacklisted
            | AuthError::WrongTokenType { .. } => Self::Unauthorized("unauthorized".to_owned()),
        }
    }
}

impl From<StorageError> for WebError {
    fn from(error: StorageError) -> Self {
        match error {
            StorageError::NotFound { entity, id } => {
                Self::NotFound(format!("{entity} not found: {id}"))
            }
            StorageError::Conflict(detail) => Self::Conflict(detail),
            StorageError::Connection(_)
            | StorageError::Redis(_)
            | StorageError::RedisPool(_)
            | StorageError::Timeout { .. } => {
                Self::ServiceUnavailable("storage temporarily unavailable".to_owned())
            }
            other => Self::Internal(other.to_string()),
        }
    }
}

impl From<RbacError> for WebError {
    fn from(error: RbacError) -> Self {
        match error {
            RbacError::NotFound { entity, id } => {
                Self::NotFound(format!("{entity} not found: {id}"))
            }
            RbacError::Duplicate { entity, key } => {
                Self::Conflict(format!("{entity} already exists: {key}"))
            }
            RbacError::UnknownPermission { .. } | RbacError::InvalidAssignment { .. } => {
                Self::BadRequest(error.to_string())
            }
        }
    }
}

impl From<GovernanceError> for WebError {
    fn from(error: GovernanceError) -> Self {
        match error {
            // 403 — a risk-expanding change requires explicit risk-owner approval.
            GovernanceError::RiskExpansionNotApproved => Self::Forbidden,
            // 400 — malformed or incomplete governance request.
            GovernanceError::MissingReason
            | GovernanceError::MissingField { .. }
            | GovernanceError::InvalidPublicationWindow
            | GovernanceError::FactorValue(_) => Self::BadRequest(error.to_string()),
            // 409 — a governance invariant or optimistic-concurrency conflict.
            GovernanceError::EmptyPublication
            | GovernanceError::FactorSetMismatch
            | GovernanceError::PublicationHashMismatch { .. }
            | GovernanceError::FactorNotReadyForPublication { .. }
            | GovernanceError::IdempotencyConflict { .. }
            | GovernanceError::RollbackTargetMissing
            | GovernanceError::PublicationLockConflict => Self::Conflict(error.to_string()),
            // 500 — hashing / audit-chain integrity failure (logged, masked).
            GovernanceError::CanonicalDigest(_) | GovernanceError::AuditChain(_) => {
                Self::Internal(error.to_string())
            }
        }
    }
}

impl From<RegistryError> for WebError {
    fn from(error: RegistryError) -> Self {
        match error {
            RegistryError::Governance(inner) => inner.into(),
            RegistryError::Storage(inner) => inner.into(),
            // A canonical-digest failure is a server-side integrity fault.
            RegistryError::CanonicalDigest(inner) => Self::Internal(inner.to_string()),
        }
    }
}

impl From<WindowQueryError> for WebError {
    fn from(error: WindowQueryError) -> Self {
        // Both variants are caller-supplied window misuse → 400.
        Self::BadRequest(error.to_string())
    }
}

impl From<DailySeriesRangeError> for WebError {
    fn from(error: DailySeriesRangeError) -> Self {
        // Caller-supplied `days` out of range → 400.
        Self::BadRequest(error.to_string())
    }
}

impl From<RuntimeControlError> for WebError {
    fn from(error: RuntimeControlError) -> Self {
        match error {
            // 409 — the requested transition is invalid in the current state
            // (e.g. entering Live without credentials / a metrics refresher).
            RuntimeControlError::Precondition(_) | RuntimeControlError::BlockingTrades { .. } => {
                Self::Conflict(error.to_string())
            }
            // 503 — the trading loop did not quiesce, or post-commit activation
            // could not establish a safe state. The system stays halted.
            RuntimeControlError::QuiesceTimeout { .. } | RuntimeControlError::Activation(_) => {
                Self::ServiceUnavailable(error.to_string())
            }
            // 500 — an underlying engine operation failed (logged, masked).
            RuntimeControlError::Engine(_) => Self::Internal(error.to_string()),
        }
    }
}
