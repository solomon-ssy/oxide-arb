//! Venue evidence collection in deterministic reconciliation order.
//!
//! For one reconcilable order the collector gathers, in the immutable order
//! 1→5, one [`ReconciliationEvidence`] per source: CLOB order status → CLOB
//! trades → token balance → account balance → book context. (`OperatorNote`,
//! #6, is appended only on a human resolve, never by the machine.) The high-
//! confidence sources (status + trades) decide; balances corroborate.

use std::sync::Arc;

use async_trait::async_trait;
use chrono::{DateTime, Duration, Utc};
use quant_pivot_api::clob::ClobTrade;
use quant_pivot_error::{QuantResult, execution::ExecutionError};
use quant_pivot_models::{
    domain::quant::ExecutionOrderInfo,
    enums::{execution::ReconciliationEvidenceKind, fee::FeeLiquidityRole},
    types::{FeeEvidence, Price, ReconciliationEvidence, Shares, TokenId, Usd},
};
use quant_pivot_research::execution_semantics::{LiquidityRole, PitFeeSchedule};

use super::{ReconcileFacts, VenuePresence, VenueReconciliationReader};
use crate::ingest::book_store::BookStore;

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
        let last_known = self.book_store.load_last_known_by_id(token_id);
        let detail = last_known.snapshot.map_or_else(
            || format!("no book snapshot ({:?})", last_known.availability),
            |snapshot| {
                format!(
                    "book version={} ts_ms={} availability={:?}",
                    snapshot.version, snapshot.timestamp_ms, last_known.availability
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
            fee_evidence: None,
        }
    }
}

fn authenticated_fee_evidence(
    order: &ExecutionOrderInfo,
    trade: &ClobTrade,
) -> QuantResult<FeeEvidence> {
    let prepared = &order.prepared_order_json.fee_schedule;
    let role = match trade.trader_side {
        FeeLiquidityRole::Maker => LiquidityRole::Maker,
        FeeLiquidityRole::Taker => LiquidityRole::Taker,
    };
    let schedule = PitFeeSchedule {
        schedule_hash: prepared.schedule_hash,
        effective_at: prepared.effective_at,
        available_at: prepared.available_at,
        platform_rate: trade.fee_rate_bps.to_fraction(),
        exponent: prepared.exponent,
        taker_only: prepared.taker_only,
        builder_maker_fee_bps: prepared.builder_maker_fee_bps,
        builder_taker_fee_bps: prepared.builder_taker_fee_bps,
        builder_attribution: prepared.builder_attribution,
    };
    let reconstructed_fee = schedule
        .fee(role, trade.price, trade.size, trade.matched_at)
        .map_err(|error| ExecutionError::ReconciliationUnresolvable {
            reason: format!(
                "authenticated trade {} fee reconstruction failed: {error:?}",
                trade.trade_id
            ),
        })?;
    Ok(FeeEvidence::AuthenticatedTradeReconstructed {
        trade_id: trade.trade_id.clone(),
        order_id: trade.order_id.clone(),
        liquidity_role: trade.trader_side,
        fee_rate_bps: trade.fee_rate_bps,
        reconstructed_fee,
        transaction_hash: trade.tx_hash.clone(),
        matched_at: trade.matched_at,
        maker_order_ids: trade
            .maker_orders
            .iter()
            .map(|maker| maker.order_id.clone())
            .collect(),
    })
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
        let mut evidence = Vec::with_capacity(8);

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
            fee_evidence: None,
        });

        // 2 — CLOB trades: the realized fill attributed to this venue order.
        let trades = self.reader.trades_for(token_id, submitted_at).await?;
        let matched_trades = trades
            .iter()
            .filter(|trade| order.venue_order_id.as_ref() == Some(&trade.order_id))
            .collect::<Vec<_>>();
        let mut filled_shares = Shares::ZERO;
        let mut filled_cost = Usd::ZERO;
        for trade in &matched_trades {
            filled_shares += trade.size;
            filled_cost += trade.size * trade.price;
            evidence.push(ReconciliationEvidence {
                kind: ReconciliationEvidenceKind::ClobTrades,
                observed_at: now,
                detail: format!(
                    "trade_id={}; role={:?}; matched_at={}; tx_hash={}",
                    trade.trade_id, trade.trader_side, trade.matched_at, trade.tx_hash
                ),
                venue_ref: Some(trade.order_id.to_string()),
                shares: Some(trade.size),
                price: Some(trade.price),
                fee_evidence: Some(authenticated_fee_evidence(order, trade)?),
            });
        }
        let avg_price = if filled_shares.is_positive() {
            Some(Price::new(filled_cost.inner() / filled_shares.inner()))
        } else {
            None
        };
        if matched_trades.is_empty() {
            evidence.push(ReconciliationEvidence {
                kind: ReconciliationEvidenceKind::ClobTrades,
                observed_at: now,
                detail: "matched_trades=0; filled_shares=0".to_owned(),
                venue_ref: order.venue_order_id.as_ref().map(ToString::to_string),
                shares: Some(Shares::ZERO),
                price: None,
                fee_evidence: None,
            });
        }

        // 3 — Token balance: absolute corroboration that shares were received.
        let token_balance = self.reader.token_balance(token_id).await?;
        evidence.push(ReconciliationEvidence {
            kind: ReconciliationEvidenceKind::TokenBalanceDelta,
            observed_at: now,
            detail: format!("token_balance={token_balance} (absolute)"),
            venue_ref: Some(token_id.to_string()),
            shares: Some(token_balance),
            price: None,
            fee_evidence: None,
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
            fee_evidence: None,
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
