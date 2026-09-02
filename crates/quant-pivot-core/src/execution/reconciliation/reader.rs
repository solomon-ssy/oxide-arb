//! SDK-free venue read façade for reconciliation evidence.
//!
//! Mirrors [`PolymarketOrderClient`](crate::execution::PolymarketOrderClient):
//! it wraps [`ClobClient`] (rate limiting + retry + SDK mapping already inside)
//! behind a venue-neutral boundary so the reconciliation engine never touches
//! `polymarket_client_sdk_v2`. The only types crossing the trait are the
//! already-SDK-free [`ClobOrder`] / [`ClobTrade`] projections and project value
//! types.

use std::sync::Arc;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use quant_pivot_api::clob::{ClobClient, ClobOrder, ClobTrade};
use quant_pivot_error::QuantResult;
use quant_pivot_models::types::{OrderId, Shares, TokenId, Usd, VenueTradeId};

/// Read-only venue access used by the reconciliation evidence collector.
#[async_trait]
pub trait VenueReconciliationReader: Send + Sync {
    /// Exact authenticated order lookup by its durable venue identity.
    async fn order(&self, order_id: &OrderId) -> QuantResult<ClobOrder>;

    /// Exact authenticated trade lookup by globally unique venue trade ID.
    async fn trade(&self, trade_id: &VenueTradeId) -> QuantResult<Option<ClobTrade>>;

    /// Bounded account-history discovery used only when placement persisted no
    /// order, trade, or chain identity at all.
    async fn discover_trades(
        &self,
        token_id: &TokenId,
        after: DateTime<Utc>,
        before: DateTime<Utc>,
    ) -> QuantResult<Vec<ClobTrade>>;

    /// Current account-wide conditional-token balance for `token_id` (evidence
    /// #3, diagnostic only; exact trades prove directional receipt/debit).
    async fn token_balance(&self, token_id: &TokenId) -> QuantResult<Shares>;

    /// Current pUSD collateral balance (evidence #4 — absolute corroboration
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
    async fn order(&self, order_id: &OrderId) -> QuantResult<ClobOrder> {
        Ok(self.clob.get_order(order_id).await?)
    }

    async fn trade(&self, trade_id: &VenueTradeId) -> QuantResult<Option<ClobTrade>> {
        Ok(self.clob.get_trade(trade_id).await?)
    }

    async fn discover_trades(
        &self,
        token_id: &TokenId,
        after: DateTime<Utc>,
        before: DateTime<Utc>,
    ) -> QuantResult<Vec<ClobTrade>> {
        Ok(self
            .clob
            .get_trades(
                None,
                Some(token_id),
                Some(after.timestamp()),
                Some(before.timestamp()),
            )
            .await?)
    }

    async fn token_balance(&self, token_id: &TokenId) -> QuantResult<Shares> {
        Ok(self.clob.token_balance(token_id).await?)
    }

    async fn collateral_balance(&self) -> QuantResult<Usd> {
        Ok(self.clob.collateral_balance().await?)
    }
}
