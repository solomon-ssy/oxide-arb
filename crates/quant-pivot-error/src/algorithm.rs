//! Algorithm-layer error types for the endgame detection and calibration pipeline.

use thiserror::Error;

/// Errors originating from the `quant-pivot-algorithm` crate.
#[derive(Debug, Error)]
pub enum AlgoError {
    /// Calibration lookup or aggregation failed.
    #[error("Calibration lookup failed: {reason}")]
    CalibrationLookup { reason: String },

    /// Orderbook walk produced no fills (insufficient liquidity above threshold).
    #[error("Orderbook walk failed: no liquidity above threshold {threshold} for token {token_id}")]
    InsufficientLiquidity { token_id: String, threshold: String },

    /// Fee estimation produced an unexpected result.
    #[error("Fee estimation failed: {0}")]
    FeeEstimation(String),

    /// Confidence fusion output was out of expected bounds before clamping.
    #[error("Confidence fusion out of bounds: raw={raw}, floor={floor}, ceiling={ceiling}")]
    FusionOutOfBounds {
        raw: String,
        floor: String,
        ceiling: String,
    },

    /// An injected data source (`CalibrationDataSource`) returned an error.
    #[error("Calibration data source error: {0}")]
    DataSource(String),

    /// Invalid algorithm configuration detected at runtime.
    #[error("Invalid algorithm configuration: {0}")]
    InvalidConfig(String),
}
