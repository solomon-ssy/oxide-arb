//! SDK-free venue read façade for reconciliation evidence (Phase 05.5).
//!
//! Mirrors [`PolymarketOrderClient`](crate::execution::PolymarketOrderClient):
//! it wraps [`ClobClient`] (rate limiting + retry + SDK mapping already inside)
//! behind a venue-neutral boundary so the reconciliation engine never touches
//! `polymarket_client_sdk_v2`. The only types crossing the trait are the
//! already-SDK-free [`OpenOrder`] / [`ClobTrade`] projections and project value
//! types.

use std::sync::Arc;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use quant_pivot_api::clob::{ClobClient, ClobTrade, OpenOrder};
use quant_pivot_error::QuantResult;
use quant_pivot_models::types::{Shares, TokenId, Usd};

/// Read-only venue access used by the reconciliation evidence collector.
#[async_trait]
pub trait VenueReconciliationReader: Send + Sync {
    /// All resting open orders for the authenticated account (evidence #1 —
    /// CLOB order status: presence means the order is still working).
    async fn open_orders(&self) -> QuantResult<Vec<OpenOrder>>;

    /// Account trades for `token_id` at or after `after` (the order's submit
    /// time), used to derive the realized fill for one venue order (evidence #2).
    async fn trades_for(
        &self,
        token_id: &TokenId,
        after: DateTime<Utc>,
    ) -> QuantResult<Vec<ClobTrade>>;

    /// Current conditional-token (outcome share) balance for `token_id`
    /// (evidence #3 — absolute corroboration that shares were received).
    async fn token_balance(&self, token_id: &TokenId) -> QuantResult<Shares>;

    /// Current USDC.e collateral balance (evidence #4 — absolute corroboration
    /// that collateral was spent).
    async fn collateral_balance(&self) -> QuantResult<Usd>;
}

/// [`VenueReconciliationReader`] backed by the shared authenticated [`ClobClient`].
pub struct ClobReconciliationReader {
    clob: Arc<ClobClient>,
}

impl ClobReconciliationReader {
    #[must_use]
    pub const fn new(clob: Arc<ClobClient>) -> Self {
        Self { clob }
    }
}

#[async_trait]
impl VenueReconciliationReader for ClobReconciliationReader {
    async fn open_orders(&self) -> QuantResult<Vec<OpenOrder>> {
        Ok(self.clob.get_open_orders().await?)
    }

    async fn trades_for(
        &self,
        token_id: &TokenId,
        after: DateTime<Utc>,
    ) -> QuantResult<Vec<ClobTrade>> {
        Ok(self
            .clob
            .get_trades(None, Some(token_id), Some(after.timestamp()))
            .await?)
    }

    async fn token_balance(&self, token_id: &TokenId) -> QuantResult<Shares> {
        Ok(self.clob.token_balance(token_id).await?)
    }

    async fn collateral_balance(&self) -> QuantResult<Usd> {
        Ok(self.clob.collateral_balance().await?)
    }
}
