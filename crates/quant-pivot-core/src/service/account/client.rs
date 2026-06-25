//! Venue account client façade.
//!
//! Hides the CLOB / Data API split (and all SDK raw types) behind one trait the
//! account provider depends on.

use std::sync::Arc;

use async_trait::async_trait;
use quant_pivot_api::{
    clob::ClobClient,
    data_api::{DataApiClient, VenuePosition},
};
use quant_pivot_error::QuantResult;
use quant_pivot_models::types::Usd;

/// Read-only venue account façade for report sizing.
#[async_trait]
pub trait PolymarketAccountClient: Send + Sync {
    /// On-exchange USDC collateral (CLOB, private-key L2 read credential).
    async fn available_collateral(&self) -> QuantResult<Usd>;
    /// Open positions for a proxy/funder address (Data API, keyless).
    async fn positions(&self, funder: &str) -> QuantResult<Vec<VenuePosition>>;
}

/// Production façade backed by the CLOB client (collateral) and Data API client
/// (positions).
pub struct VenuePolymarketAccountClient {
    clob: Arc<ClobClient>,
    data_api: Arc<DataApiClient>,
}

impl VenuePolymarketAccountClient {
    #[must_use]
    pub const fn new(clob: Arc<ClobClient>, data_api: Arc<DataApiClient>) -> Self {
        Self { clob, data_api }
    }
}

#[async_trait]
impl PolymarketAccountClient for VenuePolymarketAccountClient {
    async fn available_collateral(&self) -> QuantResult<Usd> {
        Ok(self.clob.collateral_balance().await?)
    }

    async fn positions(&self, funder: &str) -> QuantResult<Vec<VenuePosition>> {
        Ok(self.data_api.positions(funder).await?)
    }
}
