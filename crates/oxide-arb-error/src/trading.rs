//! Trading execution and risk management errors.

use thiserror::Error;

/// Errors from the trading pipeline (execution, validation, risk).
#[derive(Debug, Error)]
pub enum TradingError {
    #[error("Execution failed: {0}")]
    Execution(String),

    #[error("Order validation failed: {0}")]
    Validation(String),

    #[error("Risk denial: {0}")]
    RiskDenial(String),

    #[error("Circuit breaker open: level {level}, reason: {reason}")]
    CircuitBreakerOpen { level: u8, reason: String },

    #[error("Market not found: {0}")]
    MarketNotFound(String),

    #[error("Position limit reached: {0}")]
    PositionLimit(String),

    #[error(
        "Blocking trades unresolved: {count} durable row(s) must be reconciled before resuming"
    )]
    BlockingTradesUnresolved { count: u32 },
}
