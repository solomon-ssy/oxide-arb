//! Phase 05.5 — reconciliation evidence collection + decision (in-memory).
//!
//! Exercises the read-side pipeline (`VenueEvidenceCollector` → `decide`) with a
//! stub `VenueReconciliationReader`, no DB or venue. Service orchestration tests
//! live in `phase_05_5_reconciliation_service.rs`; write-side correction
//! (`apply_reconciliation`: capital/position/WORM/idempotency) is covered by the
//! repository integration tests in `pg_execution_submission.rs`.

use std::sync::Arc;

use async_trait::async_trait;
use chrono::{Duration, Utc};
use quant_pivot_api::clob::{ClobTrade, OpenOrder};
use quant_pivot_core::{
    execution::{
        EvidenceCollector, VenueEvidenceCollector, VenuePresence, VenueReconciliationReader, decide,
    },
    observability::metrics_hub::MetricsHub,
    pipeline::book_store::BookStore,
};
use quant_pivot_error::QuantResult;
use quant_pivot_models::{
    domain::ExecutionOrderInfo,
    enums::{
        common::Side,
        execution::{
            ExecutionOrderPhase, OrderTypeKind, ReconciliationEvidenceKind, ReconciliationResult,
        },
        quant::ExecutionOrderState,
    },
    types::{ExecutionOrderId, MarketId, OrderId, OrderIntentId, Price, Shares, TokenId, Usd},
};
use rust_decimal_macros::dec;

const VENUE_ORDER_ID: &str = "venue-1";
const TOKEN: &str = "token-1";

/// Canned venue reads for one order.
struct StubReader {
    open_orders: Vec<OpenOrder>,
    trades: Vec<ClobTrade>,
    token_balance: Shares,
    collateral: Usd,
}

#[async_trait]
impl VenueReconciliationReader for StubReader {
    async fn open_orders(&self) -> QuantResult<Vec<OpenOrder>> {
        Ok(self.open_orders.clone())
    }

    async fn trades_for(
        &self,
        _token_id: &TokenId,
        _after: chrono::DateTime<Utc>,
    ) -> QuantResult<Vec<ClobTrade>> {
        Ok(self.trades.clone())
    }

    async fn token_balance(&self, _token_id: &TokenId) -> QuantResult<Shares> {
        Ok(self.token_balance)
    }

    async fn collateral_balance(&self) -> QuantResult<Usd> {
        Ok(self.collateral)
    }
}

fn order() -> ExecutionOrderInfo {
    let now = Utc::now();
    ExecutionOrderInfo {
        execution_order_id: ExecutionOrderId::from_v7(),
        order_intent_id: OrderIntentId::from_v7(),
        order_phase: ExecutionOrderPhase::Entry,
        market_id: MarketId::new("0xmarket"),
        token_id: TokenId::new(TOKEN),
        side: Side::Buy,
        order_type: OrderTypeKind::Gtc,
        price: Price::new(dec!(0.6)),
        shares: Shares::new(dec!(100)),
        cost_usd: Usd::new(dec!(60)),
        venue_order_id: Some(OrderId::new(VENUE_ORDER_ID)),
        venue_status: None,
        state: ExecutionOrderState::Ambiguous,
        submitted_at: Some(now - Duration::seconds(30)),
        filled_at: None,
        cancelled_at: None,
        gtd_expiration_at: None,
        error_message: None,
        created_at: now - Duration::seconds(30),
        updated_at: now,
    }
}

fn trade(shares: &str, price: &str) -> ClobTrade {
    ClobTrade {
        trade_id: "t1".to_owned(),
        order_id: OrderId::new(VENUE_ORDER_ID),
        market_id: MarketId::new("0xmarket"),
        token_id: TokenId::new(TOKEN),
        side: Side::Buy,
        size: Shares::new(shares.parse().unwrap()),
        price: Price::new(price.parse().unwrap()),
        tx_hash: "0xtx".to_owned(),
        matched_at: Utc::now(),
    }
}

fn collector(reader: StubReader) -> VenueEvidenceCollector {
    let metrics = Arc::new(MetricsHub::new());
    let book_store = Arc::new(BookStore::new(metrics));
    VenueEvidenceCollector::new(
        Arc::new(reader) as Arc<dyn VenueReconciliationReader>,
        book_store,
    )
}

#[tokio::test]
async fn recon_evidence_collected_in_fixed_order() {
    let reader = StubReader {
        open_orders: Vec::new(),
        trades: vec![trade("100", "0.6")],
        token_balance: Shares::new(dec!(100)),
        collateral: Usd::new(dec!(9000)),
    };
    let collected = collector(reader)
        .collect(&order(), Utc::now(), Duration::seconds(300))
        .await
        .expect("collect");

    let kinds: Vec<_> = collected.evidence.iter().map(|e| e.kind).collect();
    assert_eq!(
        kinds,
        vec![
            ReconciliationEvidenceKind::ClobOrderStatus,
            ReconciliationEvidenceKind::ClobTrades,
            ReconciliationEvidenceKind::TokenBalanceDelta,
            ReconciliationEvidenceKind::AccountBalanceDelta,
            ReconciliationEvidenceKind::BookContext,
        ],
        "evidence must be collected in the fixed parent-doc §11 order",
    );
}

#[tokio::test]
async fn recon_filled_when_status_and_trades_agree() {
    let reader = StubReader {
        open_orders: Vec::new(),
        trades: vec![trade("100", "0.6")],
        token_balance: Shares::new(dec!(100)),
        collateral: Usd::new(dec!(9000)),
    };
    let collected = collector(reader)
        .collect(&order(), Utc::now(), Duration::seconds(300))
        .await
        .expect("collect");
    assert_eq!(collected.facts.presence, VenuePresence::Settled);
    assert_eq!(collected.facts.filled_shares, Shares::new(dec!(100)));
    assert_eq!(
        decide(&collected.facts).result,
        ReconciliationResult::Filled
    );
}

#[tokio::test]
async fn recon_not_filled_when_no_trades_and_not_open() {
    let reader = StubReader {
        open_orders: Vec::new(),
        trades: Vec::new(),
        token_balance: Shares::ZERO,
        collateral: Usd::new(dec!(9000)),
    };
    let collected = collector(reader)
        .collect(&order(), Utc::now(), Duration::seconds(300))
        .await
        .expect("collect");
    // Zero fill, not resting, not GTD-expired → cancelled (capital released).
    assert_eq!(
        decide(&collected.facts).result,
        ReconciliationResult::Cancelled
    );
}

#[tokio::test]
async fn recon_still_open_before_deadline_stays_pending() {
    let reader = StubReader {
        open_orders: vec![OpenOrder {
            order_id: OrderId::new(VENUE_ORDER_ID),
            token_id: TokenId::new(TOKEN),
            side: Side::Buy,
            price: Price::new(dec!(0.6)),
            size: Shares::new(dec!(100)),
            filled: Shares::ZERO,
        }],
        trades: Vec::new(),
        token_balance: Shares::ZERO,
        collateral: Usd::new(dec!(9000)),
    };
    let collected = collector(reader)
        .collect(&order(), Utc::now(), Duration::seconds(300))
        .await
        .expect("collect");
    assert_eq!(collected.facts.presence, VenuePresence::Resting);
    assert_eq!(
        decide(&collected.facts).result,
        ReconciliationResult::Pending
    );
}

#[tokio::test]
async fn recon_conflicting_token_balance_is_unresolvable() {
    // Trades claim a full fill, but the account holds none of the token: a hard
    // contradiction the engine must never resolve as filled.
    let reader = StubReader {
        open_orders: Vec::new(),
        trades: vec![trade("100", "0.6")],
        token_balance: Shares::ZERO,
        collateral: Usd::new(dec!(9000)),
    };
    let collected = collector(reader)
        .collect(&order(), Utc::now(), Duration::seconds(300))
        .await
        .expect("collect");
    assert_eq!(
        decide(&collected.facts).result,
        ReconciliationResult::Unresolvable
    );
}
