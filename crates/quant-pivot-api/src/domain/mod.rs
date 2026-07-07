//! Provider-agnostic domain data-source contracts (Phase 11.2.2).
//!
//! External vertical feature sources (Binance klines, Chainlink aggregators)
//! implement [`DomainDataSource`] and emit normalized [`DomainObservation`]
//! rows. Ingest orchestration lives in `quant-pivot-core`; this crate owns only
//! the fetch boundary.

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use quant_pivot_error::QuantResult;
use quant_pivot_models::{
    domain::DomainObservation,
    enums::domain::DomainFamily,
    types::{DomainInstrumentKey, DomainSourceId},
};

/// One fetch window for a single instrument stream.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DomainFetchRequest {
    /// Canonical instrument key (`BINANCE:…` / `CHAINLINK:…`).
    pub instrument_key: DomainInstrumentKey,
    /// Exclusive lower bound (cursor resume point).
    pub from_exclusive: DateTime<Utc>,
    /// Inclusive upper bound (safe head / now).
    pub to_inclusive: DateTime<Utc>,
    /// Whether this tick is bootstrapping historical depth.
    pub bootstrap: bool,
}

/// One external domain feature source.
#[async_trait]
pub trait DomainDataSource: Send + Sync {
    /// Vertical family this source serves.
    fn family(&self) -> DomainFamily;

    /// Stable source identifier (`binance`, `chainlink`, …).
    fn source_id(&self) -> DomainSourceId;

    /// Fetch observations for one instrument over `[from_exclusive, to_inclusive]`.
    ///
    /// Returns an empty vector when there is nothing new — never fabricated rows.
    ///
    /// # Errors
    ///
    /// Propagates transport / decode failures.
    async fn fetch(&self, request: DomainFetchRequest) -> QuantResult<Vec<DomainObservation>>;
}
