//! Venue evidence collection in fixed order (Phase 05.5, parent doc §11).
//!
//! For one reconcilable order the collector gathers, in the immutable order
//! 1→5, one [`ReconciliationEvidence`] per source: CLOB order status → CLOB
//! trades → token balance → account balance → book context. (`OperatorNote`,
//! #6, is appended only on a human resolve, never by the machine.) The high-
//! confidence sources (status + trades) decide; balances corroborate.

use std::sync::Arc;

use async_trait::async_trait;
use chrono::{DateTime, Duration, Utc};
use quant_pivot_error::QuantResult;
use quant_pivot_models::{
    domain::ExecutionOrderInfo,
    enums::execution::ReconciliationEvidenceKind,
    types::{Price, ReconciliationEvidence, Shares, TokenId, Usd},
};

use super::{ReconcileFacts, VenuePresence, VenueReconciliationReader};
use crate::pipeline::book_store::BookStore;

/// Evidence chain + structured facts produced for one reconcilable order.
pub struct CollectedReconciliation {
    /// Ordered evidence (kinds 1→5) recorded on the reconciliation summary row.
    pub evidence: Vec<ReconciliationEvidence>,
    /// Decision facts derived from the same observations.
    pub facts: ReconcileFacts,
}

/// Collects the fixed-order venue evidence for one reconcilable order.
#[async_trait]
pub trait EvidenceCollector: Send + Sync {
    async fn collect(
        &self,
        order: &ExecutionOrderInfo,
        now: DateTime<Utc>,
        stale_after: Duration,
    ) -> QuantResult<CollectedReconciliation>;
}

/// [`EvidenceCollector`] backed by the venue reader + the in-memory book store.
pub struct VenueEvidenceCollector {
    reader: Arc<dyn VenueReconciliationReader>,
    book_store: Arc<BookStore>,
}

impl VenueEvidenceCollector {
    #[must_use]
    pub const fn new(
        reader: Arc<dyn VenueReconciliationReader>,
        book_store: Arc<BookStore>,
    ) -> Self {
        Self { reader, book_store }
    }

    /// Evidence #5 — the current published book snapshot for price sanity
    /// (best effort: records version + timestamp, or that none is published).
    fn book_context_evidence(
        &self,
        token_id: &TokenId,
        now: DateTime<Utc>,
    ) -> ReconciliationEvidence {
        let detail = self.book_store.load(token_id).map_or_else(
            || "no published book snapshot".to_owned(),
            |snapshot| {
                format!(
                    "book version={} ts_ms={}",
                    snapshot.version, snapshot.timestamp_ms
                )
            },
        );
        ReconciliationEvidence {
            kind: ReconciliationEvidenceKind::BookContext,
            observed_at: now,
            detail,
            venue_ref: Some(token_id.to_string()),
            shares: None,
            price: None,
        }
    }
}

#[async_trait]
impl EvidenceCollector for VenueEvidenceCollector {
    async fn collect(
        &self,
        order: &ExecutionOrderInfo,
        now: DateTime<Utc>,
        stale_after: Duration,
    ) -> QuantResult<CollectedReconciliation> {
        let token_id = &order.token_id;
        let submitted_at = order.submitted_at.unwrap_or(order.created_at);
        let attributable = order.venue_order_id.is_some();
        let mut evidence = Vec::with_capacity(5);

        // 1 — CLOB order status: presence in open orders means still working.
        let open_orders = self.reader.open_orders().await?;
        let still_open = order
            .venue_order_id
            .as_ref()
            .is_some_and(|id| open_orders.iter().any(|o| &o.order_id == id));
        evidence.push(ReconciliationEvidence {
            kind: ReconciliationEvidenceKind::ClobOrderStatus,
            observed_at: now,
            detail: format!(
                "open_orders={}; still_open={still_open}; attributable={attributable}",
                open_orders.len()
            ),
            venue_ref: order.venue_order_id.as_ref().map(ToString::to_string),
            shares: None,
            price: None,
        });

        // 2 — CLOB trades: the realized fill attributed to this venue order.
        let trades = self.reader.trades_for(token_id, submitted_at).await?;
        let mut filled_shares = Shares::ZERO;
        let mut filled_cost = Usd::ZERO;
        for trade in trades
            .iter()
            .filter(|t| order.venue_order_id.as_ref() == Some(&t.order_id))
        {
            filled_shares += trade.size;
            filled_cost += trade.size * trade.price;
        }
        let avg_price = if filled_shares.is_positive() {
            Some(Price::new(filled_cost.inner() / filled_shares.inner()))
        } else {
            None
        };
        evidence.push(ReconciliationEvidence {
            kind: ReconciliationEvidenceKind::ClobTrades,
            observed_at: now,
            detail: format!(
                "matched_trades={}; filled_shares={filled_shares}",
                trades.len()
            ),
            venue_ref: order.venue_order_id.as_ref().map(ToString::to_string),
            shares: Some(filled_shares),
            price: avg_price,
        });

        // 3 — Token balance: absolute corroboration that shares were received.
        let token_balance = self.reader.token_balance(token_id).await?;
        evidence.push(ReconciliationEvidence {
            kind: ReconciliationEvidenceKind::TokenBalanceDelta,
            observed_at: now,
            detail: format!("token_balance={token_balance} (absolute)"),
            venue_ref: Some(token_id.to_string()),
            shares: Some(token_balance),
            price: None,
        });

        // 4 — Account balance: absolute corroboration that collateral was spent.
        let collateral = self.reader.collateral_balance().await?;
        evidence.push(ReconciliationEvidence {
            kind: ReconciliationEvidenceKind::AccountBalanceDelta,
            observed_at: now,
            detail: format!("collateral_balance={collateral} (absolute)"),
            venue_ref: None,
            shares: None,
            price: None,
        });

        // 5 — Book context: price sanity around the submission (best effort).
        evidence.push(self.book_context_evidence(token_id, now));

        let gtd_expired = order.gtd_expiration_at.is_some_and(|expiry| now >= expiry);
        let past_stale_deadline = now - submitted_at > stale_after;
        let presence = if !attributable {
            VenuePresence::Unattributable
        } else if still_open {
            VenuePresence::Resting
        } else {
            VenuePresence::Settled
        };

        Ok(CollectedReconciliation {
            evidence,
            facts: ReconcileFacts {
                order_shares: order.shares,
                presence,
                filled_shares,
                avg_price,
                token_balance,
                past_stale_deadline,
                gtd_expired,
            },
        })
    }
}
