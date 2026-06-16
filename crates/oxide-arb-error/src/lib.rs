//! Unified error types for the oxide-arb platform.
//!
//! Errors are organized into domain-specific sub-modules. The root
//! [`OxideError`] enum composes them via `#[from]` so that any sub-error
//! can propagate through `?` in functions returning [`OxideResult`].
//!
//! # Architecture (ng-gateway pattern)
//!
//! ```text
//! AlgoError ────────┐
//! ApiError ─────────┤
//! WsError ──────────┤
//! StorageError──────┤
//! SigningError───────┼──► OxideError
//! RpcError ─────────┤
//! ConfigError───────┤
//! MarketError────────┤
//! TradingError──────┤
//! ReservationError──┘
//! ```

pub mod algorithm;
pub mod api;
pub mod auth;
pub mod config;
pub mod config_validation;
pub mod control;
pub mod fee;
pub mod market;
pub mod rbac;
pub mod redeem;
pub mod reservation;
pub mod rpc;
pub mod security;
pub mod seed;
pub mod signing;
pub mod storage;
pub mod trading;
pub mod ws;

pub use config_validation::{ConfigValidationError, ConfigValidationReport, ConfigWarning};

use thiserror::Error;

/// Convenience alias used throughout the workspace.
pub type OxideResult<T> = Result<T, OxideError>;

/// Top-level error enum covering every known failure mode.
///
/// Each variant wraps a domain-specific sub-error via `#[from]`,
/// enabling ergonomic `?` propagation from any subsystem.
#[derive(Debug, Error)]
pub enum OxideError {
    // ── Algorithm ─────────────────────────────────────────────────────────
    #[error(transparent)]
    Algorithm(#[from] algorithm::AlgoError),

    // ── API / Network ───────────────────────────────────────────────────
    #[error(transparent)]
    Api(#[from] api::ApiError),

    #[error(transparent)]
    WebSocket(#[from] ws::WsError),

    #[error(transparent)]
    Rpc(#[from] rpc::RpcError),

    // ── Persistence ─────────────────────────────────────────────────────
    #[error(transparent)]
    Storage(#[from] storage::StorageError),

    // ── Security ────────────────────────────────────────────────────────
    #[error(transparent)]
    Signing(#[from] signing::SigningError),

    #[error(transparent)]
    Password(#[from] security::PasswordError),

    // ── Access control (RBAC) ────────────────────────────────────────────
    #[error(transparent)]
    Rbac(#[from] rbac::RbacError),

    // ── Authentication (JWT / sessions) ──────────────────────────────────
    #[error(transparent)]
    Auth(#[from] auth::AuthError),

    // ── Configuration ───────────────────────────────────────────────────
    #[error(transparent)]
    Config(#[from] config::ConfigError),

    #[error(transparent)]
    Control(#[from] control::GovernanceError),

    #[error(transparent)]
    Materialization(#[from] control::MaterializationError),

    #[error("control-factor snapshot build failed: {0}")]
    SnapshotBuild(#[from] control::SnapshotBuildError),

    // ── Market catalog ──────────────────────────────────────────────────
    #[error(transparent)]
    Market(#[from] market::MarketError),

    // ── Trading ─────────────────────────────────────────────────────────
    #[error(transparent)]
    Trading(#[from] trading::TradingError),

    // ── Reservation ──────────────────────────────────────────────────────
    #[error(transparent)]
    Reservation(#[from] reservation::ReservationError),

    #[error(transparent)]
    Redeem(#[from] redeem::RedeemError),

    #[error(transparent)]
    FeeQuote(#[from] fee::FeeQuoteError),

    #[error(transparent)]
    Seed(#[from] seed::SeedError),

    // ── General ─────────────────────────────────────────────────────────
    #[error("Internal error: {0}")]
    Internal(String),

    #[error("Not implemented: {0}")]
    NotImplemented(String),
}

// ── Convenience constructors for the String-accepting variants of OxideError ─

impl OxideError {
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

// ── Bridge: sea_orm::TransactionError → OxideError ───────────────────────────

impl From<sea_orm::DbErr> for OxideError {
    fn from(e: sea_orm::DbErr) -> Self {
        Self::Storage(storage::StorageError::Database(e))
    }
}

impl From<sea_orm::TransactionError<Self>> for OxideError {
    fn from(e: sea_orm::TransactionError<Self>) -> Self {
        match e {
            sea_orm::TransactionError::Connection(db_err) => {
                Self::Storage(storage::StorageError::Database(db_err))
            }
            sea_orm::TransactionError::Transaction(oxide_err) => oxide_err,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal_macros::dec;

    #[test]
    fn api_error_propagates_via_from() {
        let api_err = api::ApiError::Timeout {
            operation: "get_book".into(),
            elapsed_ms: 5000,
        };
        let oxide_err: OxideError = api_err.into();
        assert!(matches!(oxide_err, OxideError::Api(_)));
    }

    #[test]
    fn ws_error_propagates() {
        let ws_err = ws::WsError::PingTimeout {
            shard_id: 2,
            deadline_ms: 10000,
        };
        let oxide_err: OxideError = ws_err.into();
        assert!(matches!(oxide_err, OxideError::WebSocket(_)));
    }

    #[test]
    fn storage_error_wraps_db_err() {
        let db_err = sea_orm::DbErr::Custom("test db error".into());
        let oxide_err: OxideError = db_err.into();
        assert!(matches!(oxide_err, OxideError::Storage(_)));
    }

    #[test]
    fn config_error_propagates() {
        let cfg_err = config::ConfigError::from(ConfigValidationReport::single_error(
            ConfigValidationError::InvalidKellyFraction(dec!(1.5)),
        ));
        let oxide_err: OxideError = cfg_err.into();
        assert!(matches!(oxide_err, OxideError::Config(_)));
    }

    #[test]
    fn trading_error_propagates() {
        let t_err = trading::TradingError::CircuitBreakerOpen {
            level: 3,
            reason: "drawdown exceeded".into(),
        };
        let oxide_err: OxideError = t_err.into();
        assert!(matches!(oxide_err, OxideError::Trading(_)));
    }

    #[test]
    fn result_alias_works() {
        let ok: OxideResult<i32> = Ok(42);
        assert!(matches!(ok, Ok(42)));

        let err: OxideResult<i32> = Err(OxideError::Internal("test".into()));
        assert!(err.is_err());
    }

    #[test]
    fn error_is_send_sync() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<OxideError>();
    }

    #[test]
    fn transaction_error_connection_converts() {
        let tx_err = sea_orm::TransactionError::<OxideError>::Connection(sea_orm::DbErr::Custom(
            "conn failed".into(),
        ));
        let oxide_err: OxideError = tx_err.into();
        assert!(matches!(oxide_err, OxideError::Storage(_)));
    }

    #[test]
    fn market_error_propagates() {
        let market_err = market::MarketError::InvalidTokenPair {
            market_id: "0xabc".into(),
        };
        let oxide_err: OxideError = market_err.into();
        assert!(matches!(oxide_err, OxideError::Market(_)));
    }

    #[test]
    fn api_error_retryable() {
        let rate_limited = api::ApiError::RateLimited {
            retry_after_ms: 1000,
            bucket: "orders".into(),
        };
        assert!(rate_limited.is_retryable());
        assert_eq!(rate_limited.retry_after_ms(), Some(1000));

        let gamma_5xx = api::ApiError::Gamma {
            endpoint: "/events".into(),
            status: 500,
            body: "error".into(),
        };
        assert!(gamma_5xx.is_retryable());

        let gamma_4xx = api::ApiError::Gamma {
            endpoint: "/events".into(),
            status: 404,
            body: "not found".into(),
        };
        assert!(!gamma_4xx.is_retryable());
    }
}
