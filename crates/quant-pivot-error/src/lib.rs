//! Unified error types for the quant-pivot platform.
//!
//! Errors are organized into domain-specific sub-modules. The root
//! [`QuantError`] enum composes them via `#[from]` so that any sub-error
//! can propagate through `?` in functions returning [`QuantResult`].
//!
//! # Architecture
//!
//! ```text
//! ApiError ─────────┐
//! WsError ──────────┤
//! RpcError ─────────┤
//! StorageError──────┤
//! SigningError──────┼──► QuantError
//! ConfigError───────┤
//! MarketError───────┤
//! ReportError───────┤
//! ExecutionError────┤
//! InfraError────────┤
//! ControlError──────┤
//! FeedbackError─────┤
//! ResearchError─────┤
//! GovernanceError───┤
//! SeedError ────────┘
//! ```

use quant_pivot_allocator as _;

pub mod account;
pub mod api;
pub mod auth;
pub mod config;
pub mod config_validation;
pub mod control;
pub mod execution;
pub mod feedback;
pub mod governance;
pub mod hashing;
pub mod infra;
pub mod market;
pub mod query;
pub mod rbac;
pub mod report;
pub mod research;
pub mod rpc;
pub mod security;
pub mod seed;
pub mod signing;
pub mod storage;
pub mod ws;

use account::AccountError;
use api::ApiError;
use auth::AuthError;
use config::ConfigError;
use config_validation::{ConfigValidationError, ConfigValidationReport};
use control::ControlError;
use execution::ExecutionError;
use feedback::{FeedbackError, PromotionCommitError, PromotionPermitCommandError};
use governance::GovernanceError;
use hashing::CanonicalDigestError;
use infra::InfraError;
use market::MarketError;
use rbac::RbacError;
use report::ReportError;
use research::ResearchError;
use rpc::RpcError;
use sea_orm::{DbErr, TransactionError};
use security::PasswordError;
use seed::SeedError;
use signing::SigningError;
use storage::StorageError;
use thiserror::Error;
use ws::WsError;

/// Convenience alias used throughout the workspace.
pub type QuantResult<T> = Result<T, QuantError>;

/// Top-level error enum covering every known failure mode.
///
/// Each variant wraps a domain-specific sub-error via `#[from]`,
/// enabling ergonomic `?` propagation from any subsystem.
#[derive(Debug, Error)]
pub enum QuantError {
    // ── Account capital (venue snapshot for report sizing) ───────────────
    #[error(transparent)]
    Account(#[from] AccountError),

    // ── API / Network ───────────────────────────────────────────────────
    #[error(transparent)]
    Api(#[from] ApiError),

    #[error(transparent)]
    WebSocket(#[from] WsError),

    #[error(transparent)]
    Rpc(#[from] RpcError),

    // ── Persistence ─────────────────────────────────────────────────────
    #[error(transparent)]
    Storage(#[from] StorageError),

    // ── Security ────────────────────────────────────────────────────────
    #[error(transparent)]
    Signing(#[from] SigningError),

    #[error(transparent)]
    Password(#[from] PasswordError),

    // ── Access control (RBAC) ────────────────────────────────────────────
    #[error(transparent)]
    Rbac(#[from] RbacError),

    // ── Authentication (JWT / sessions) ──────────────────────────────────
    #[error(transparent)]
    Auth(#[from] AuthError),

    // ── Configuration ───────────────────────────────────────────────────
    #[error(transparent)]
    Config(#[from] ConfigError),

    // ── Market catalog ──────────────────────────────────────────────────
    #[error(transparent)]
    Market(#[from] MarketError),

    #[error(transparent)]
    Seed(#[from] SeedError),

    // ── Research plane ──────────────────────────────────────────────────
    #[error(transparent)]
    Research(#[from] ResearchError),

    // ── Feedback-cycle orchestration ────────────────────────────────────
    #[error(transparent)]
    Feedback(#[from] FeedbackError),

    // ── Model governance (publish / rollback / dataset promotion) ───────
    #[error(transparent)]
    Governance(#[from] GovernanceError),

    // ── Canonical hashing / content addressing ──────────────────────────
    #[error(transparent)]
    Hashing(#[from] CanonicalDigestError),

    // ── Report generation pipeline ──────────────────────────────────────
    #[error(transparent)]
    Report(#[from] ReportError),

    // ── Execution plane ─────────────────────────────────────────────────
    #[error(transparent)]
    Execution(#[from] ExecutionError),

    // ── Process bootstrap / observability ─────────────────────────────────
    #[error(transparent)]
    Infra(#[from] InfraError),

    // ── Runtime control plane ───────────────────────────────────────────
    #[error(transparent)]
    Control(#[from] ControlError),
}

impl QuantError {
    /// Stable, queryable failure taxonomy code (the sub-error family name).
    ///
    /// Used for observability surfaces that need a coarse, append-only label —
    /// e.g. a durable failed `ReportRun` projection — without leaking the
    /// human-readable message.
    #[must_use]
    pub const fn code(&self) -> &'static str {
        match self {
            Self::Account(_) => "account",
            Self::Api(_) => "api",
            Self::WebSocket(_) => "websocket",
            Self::Rpc(_) => "rpc",
            Self::Storage(_) => "storage",
            Self::Signing(_) => "signing",
            Self::Password(_) => "password",
            Self::Rbac(_) => "rbac",
            Self::Auth(_) => "auth",
            Self::Config(_) => "config",
            Self::Market(_) => "market",
            Self::Seed(_) => "seed",
            Self::Research(_) => "research",
            Self::Feedback(_) => "feedback",
            Self::Governance(_) => "governance",
            Self::Hashing(_) => "hashing",
            Self::Report(_) => "report",
            Self::Execution(_) => "execution",
            Self::Infra(_) => "infra",
            Self::Control(_) => "control",
        }
    }

    /// Shorthand config error from a string message (used by the deploy-config loader).
    pub fn config(msg: impl Into<String>) -> Self {
        Self::Config(
            ConfigValidationReport::single_error(ConfigValidationError::InvalidValue {
                field: "config",
                detail: msg.into(),
            })
            .into(),
        )
    }
}

// ── Bridge: sea_orm::TransactionError → QuantError ───────────────────────────

impl From<DbErr> for QuantError {
    fn from(e: DbErr) -> Self {
        Self::Storage(StorageError::Database(e))
    }
}

impl From<TransactionError<Self>> for QuantError {
    fn from(e: TransactionError<Self>) -> Self {
        match e {
            TransactionError::Connection(db_err) => Self::Storage(StorageError::Database(db_err)),
            TransactionError::Transaction(oxide_err) => oxide_err,
        }
    }
}

impl From<PromotionPermitCommandError> for QuantError {
    fn from(error: PromotionPermitCommandError) -> Self {
        match error {
            PromotionPermitCommandError::Contract(error) => Self::Feedback(error),
            PromotionPermitCommandError::Authorization(error) => Self::Rbac(error),
            PromotionPermitCommandError::Storage(error) => Self::Storage(error),
        }
    }
}

impl From<PromotionCommitError> for QuantError {
    fn from(error: PromotionCommitError) -> Self {
        match error {
            PromotionCommitError::Contract(error) => Self::Feedback(error),
            PromotionCommitError::Storage(error) => Self::Storage(error),
        }
    }
}

#[cfg(test)]
mod tests {
    use rust_decimal_macros::dec;

    use super::{
        ApiError, ConfigError, ConfigValidationError, ConfigValidationReport, ControlError, DbErr,
        ExecutionError, FeedbackError, InfraError, MarketError, QuantError, QuantResult,
        ReportError, TransactionError, WsError,
    };

    #[test]
    fn api_error_propagates_via() {
        let api_err = ApiError::Timeout {
            operation: "get_book".into(),
            elapsed_ms: 5000,
        };
        let oxide_err: QuantError = api_err.into();
        assert!(matches!(oxide_err, QuantError::Api(_)));
    }

    #[test]
    fn ws_error_propagates() {
        let ws_err = WsError::PingTimeout {
            shard_id: 2,
            deadline_ms: 10000,
        };
        let oxide_err: QuantError = ws_err.into();
        assert!(matches!(oxide_err, QuantError::WebSocket(_)));
    }

    #[test]
    fn storage_error_wraps_err() {
        let db_err = DbErr::Custom("test db error".into());
        let oxide_err: QuantError = db_err.into();
        assert!(matches!(oxide_err, QuantError::Storage(_)));
    }

    #[test]
    fn config_error_propagates() {
        let cfg_err = ConfigError::from(ConfigValidationReport::single_error(
            ConfigValidationError::InvalidKellyFraction(dec!(1.5)),
        ));
        let oxide_err: QuantError = cfg_err.into();
        assert!(matches!(oxide_err, QuantError::Config(_)));
    }

    #[test]
    fn result_alias_works() {
        let ok: QuantResult<i32> = Ok(42);
        assert!(matches!(ok, Ok(42)));

        let err: QuantResult<i32> = Err(ApiError::Timeout {
            operation: "result_alias_test".to_owned(),
            elapsed_ms: 1,
        }
        .into());
        assert!(err.is_err());
    }

    #[test]
    fn feedback_error_propagates() {
        let error: QuantError = FeedbackError::StaleCycleGeneration {
            expected: 4,
            actual: 3,
        }
        .into();
        assert_eq!(error.code(), "feedback");
        assert!(matches!(error, QuantError::Feedback(_)));
    }

    #[test]
    fn error_is_send_sync() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<QuantError>();
    }

    #[test]
    fn transaction_error_connection_converts() {
        let tx_err =
            TransactionError::<QuantError>::Connection(DbErr::Custom("conn failed".into()));
        let oxide_err: QuantError = tx_err.into();
        assert!(matches!(oxide_err, QuantError::Storage(_)));
    }

    #[test]
    fn market_error_propagates() {
        let market_err = MarketError::InvalidTokenPair {
            market_id: "0xabc".into(),
            reason: "missing NO leg".into(),
        };
        let oxide_err: QuantError = market_err.into();
        assert!(matches!(oxide_err, QuantError::Market(_)));
    }

    #[test]
    fn report_error_propagates() {
        let err = ReportError::InvariantViolation {
            stage: "compose",
            detail: "missing feature vector".into(),
        };
        let oxide_err: QuantError = err.into();
        assert!(matches!(oxide_err, QuantError::Report(_)));
    }

    #[test]
    fn execution_error_propagates_code() {
        let err = ExecutionError::ReportOnlyMode;
        let oxide_err: QuantError = err.into();
        assert!(matches!(oxide_err, QuantError::Execution(_)));
        assert_eq!(oxide_err.code(), "execution");
    }

    #[test]
    fn infra_error_propagates() {
        let err = InfraError::ChannelClosed {
            name: "pipeline_events",
        };
        let oxide_err: QuantError = err.into();
        assert!(matches!(oxide_err, QuantError::Infra(_)));
    }

    #[test]
    fn control_error_propagates() {
        let err = ControlError::Precondition("catalog not ready".into());
        let oxide_err: QuantError = err.into();
        assert!(matches!(oxide_err, QuantError::Control(_)));
    }

    #[test]
    fn api_error_retryable() {
        let rate_limited = ApiError::RateLimited {
            retry_after_ms: 1000,
            bucket: "orders".into(),
        };
        assert!(rate_limited.is_retryable());
        assert_eq!(rate_limited.retry_after_ms(), Some(1000));

        let gamma_5xx = ApiError::Gamma {
            endpoint: "/events".into(),
            status: 500,
            body: "error".into(),
            retry_after_ms: None,
        };
        assert!(gamma_5xx.is_retryable());

        let gamma_4xx = ApiError::Gamma {
            endpoint: "/events".into(),
            status: 404,
            body: "not found".into(),
            retry_after_ms: None,
        };
        assert!(!gamma_4xx.is_retryable());
    }
}
