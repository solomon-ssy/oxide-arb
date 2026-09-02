//! Finalized exchange-history reconstruction failures.

use thiserror::Error;

/// Fail-closed failures in extraction, independent attestation, projection,
/// and accepted-frontier advancement.
#[derive(Debug, Error)]
pub enum ExchangeHistoryError {
    #[error("HyperSync extraction failed: {detail}")]
    Extraction { detail: String },
    #[error("independent archive attestation failed: {detail}")]
    Attestation { detail: String },
    #[error(
        "both history providers failed (HyperSync extractor: {extractor}; independent attestor: {attestor})"
    )]
    ProviderFailures { extractor: String, attestor: String },
    #[error("history providers disagree for block range {from_block}..={to_block}")]
    ProviderMismatch { from_block: u64, to_block: u64 },
    #[error("accepted history has a parent-hash discontinuity at block {block}")]
    ParentDiscontinuity { block: u64 },
    #[error("accepted exchange events cannot be projected: {detail}")]
    Projection { detail: String },
    #[error("exchange-history frontier cannot be represented in durable storage")]
    FrontierOverflow,
    #[error("exchange-history time boundary is invalid")]
    InvalidTime,
}
