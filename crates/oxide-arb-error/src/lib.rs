//! Unified error types for the oxide-arb platform.
//!
//! All crates in the workspace converge their errors into [`OxideError`].
//! This crate contains zero business logic — only error enum variants and
//! automatic conversions.

use thiserror::Error;

/// Convenience alias used throughout the workspace.
pub type OxideResult<T> = Result<T, OxideError>;

/// Top-level error enum covering every known failure mode.
///
/// Variants are grouped by subsystem so that callers can pattern-match at
/// the granularity they need.
#[derive(Debug, Error)]
pub enum OxideError {
    // ── Infrastructure ──────────────────────────────────────────────────
    #[error("Configuration error: {0}")]
    Config(String),

    #[error("Database error: {0}")]
    Database(#[from] sea_orm::DbErr),

    #[error("Database transaction error: {0}")]
    Transaction(String),

    #[error("ClickHouse error: {0}")]
    ClickHouse(String),

    #[error("Cache error: {0}")]
    Cache(String),

    // ── Network ─────────────────────────────────────────────────────────
    #[error("HTTP error: {0}")]
    Http(String),

    #[error("WebSocket error: {0}")]
    WebSocket(String),

    #[error("Timeout: {0}")]
    Timeout(String),

    // ── Trading ─────────────────────────────────────────────────────────
    #[error("Execution error: {0}")]
    Execution(String),

    #[error("Signing error: {0}")]
    Signing(String),

    #[error("Validation error: {0}")]
    Validation(String),

    // ── Risk ────────────────────────────────────────────────────────────
    #[error("Risk denial: {0}")]
    RiskDenial(String),

    #[error("Circuit breaker open: level {level}, reason: {reason}")]
    CircuitBreakerOpen { level: u8, reason: String },

    // ── Data ────────────────────────────────────────────────────────────
    #[error("Market not found: {0}")]
    MarketNotFound(String),

    #[error("Stale data: {0}")]
    StaleData(String),

    // ── General ─────────────────────────────────────────────────────────
    #[error("Internal error: {0}")]
    Internal(String),

    #[error("Not implemented: {0}")]
    NotImplemented(String),
}

impl From<sea_orm::TransactionError<Self>> for OxideError {
    fn from(e: sea_orm::TransactionError<Self>) -> Self {
        match e {
            sea_orm::TransactionError::Connection(db_err) => Self::Database(db_err),
            sea_orm::TransactionError::Transaction(oxide_err) => oxide_err,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn config_error_displays_message() {
        let err = OxideError::Config("missing field".into());
        assert_eq!(err.to_string(), "Configuration error: missing field");
    }

    #[test]
    fn circuit_breaker_displays_level_and_reason() {
        let err = OxideError::CircuitBreakerOpen {
            level: 3,
            reason: "drawdown exceeded".into(),
        };
        assert_eq!(
            err.to_string(),
            "Circuit breaker open: level 3, reason: drawdown exceeded"
        );
    }

    #[test]
    fn result_alias_works() {
        let ok: OxideResult<i32> = Ok(42);
        assert_eq!(ok.unwrap(), 42);

        let err: OxideResult<i32> = Err(OxideError::Internal("test".into()));
        assert!(err.is_err());
    }

    #[test]
    fn error_is_send_sync() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<OxideError>();
    }

    #[test]
    fn db_error_converts() {
        let db_err = sea_orm::DbErr::Custom("test db error".into());
        let oxide_err: OxideError = db_err.into();
        assert!(matches!(oxide_err, OxideError::Database(_)));
    }

    #[test]
    fn transaction_error_connection_converts() {
        let tx_err = sea_orm::TransactionError::<OxideError>::Connection(sea_orm::DbErr::Custom(
            "conn failed".into(),
        ));
        let oxide_err: OxideError = tx_err.into();
        assert!(matches!(oxide_err, OxideError::Database(_)));
    }

    #[test]
    fn transaction_error_inner_converts() {
        let tx_err = sea_orm::TransactionError::<OxideError>::Transaction(OxideError::Execution(
            "order rejected".into(),
        ));
        let oxide_err: OxideError = tx_err.into();
        assert!(matches!(oxide_err, OxideError::Execution(_)));
    }
}
