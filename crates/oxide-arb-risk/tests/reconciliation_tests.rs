//! `LedgerReconciler` edge-case tests.
//!
//! Exercises the reconciliation engine with mock metrics and balance queries
//! to verify Ok / Warning / Critical classification and mismatch detection.

use oxide_arb_error::OxideResult;
use oxide_arb_models::{
    domain::position::PositionInfo,
    enums::{
        common::{
            ExecutionMode, PositionStatus, RedeemResolutionSource, RedeemStatus,
            SettlementAccountingStatus, Side,
        },
        risk::ReconciliationStatus,
    },
    types::{MarketId, PositionId, Price, Shares, TokenId, TradeId, Usd},
};
use oxide_arb_risk::{
    reconciliation::LedgerReconciler,
    traits::{BalanceQuerier, RiskMetrics},
    types::ReconciliationMismatch,
};
use rust_decimal_macros::dec;

// ── Mock Metrics ────────────────────────────────────────────────────────────

struct MockReconMetrics {
    balance: Usd,
    total_exposure: Usd,
    market_exposures: Vec<(MarketId, Usd)>,
    positions: Vec<PositionInfo>,
    reserved: Usd,
}

impl Default for MockReconMetrics {
    fn default() -> Self {
        Self {
            balance: Usd::new(dec!(1000)),
            total_exposure: Usd::new(dec!(100)),
            market_exposures: vec![],
            positions: vec![],
            reserved: Usd::ZERO,
        }
    }
}

impl RiskMetrics for MockReconMetrics {
    fn total_exposure(&self) -> Usd {
        self.total_exposure
    }
    fn market_exposure(&self, market_id: &MarketId) -> Usd {
        self.market_exposures
            .iter()
            .find(|(m, _)| m == market_id)
            .map_or(Usd::ZERO, |(_, v)| *v)
    }
    fn open_position_count(&self) -> usize {
        self.positions.len()
    }
    fn open_positions(&self) -> Vec<PositionInfo> {
        self.positions.clone()
    }
    fn cash_balance(&self) -> Usd {
        self.balance
    }

    fn position_mark_value(&self) -> Usd {
        Usd::ZERO
    }

    fn equity(&self) -> Usd {
        self.balance
    }
    fn active_reservation_count(&self) -> usize {
        0
    }
    fn reserved_usd(&self) -> Usd {
        self.reserved
    }
    fn open_directional_count(&self, _side: Side) -> usize {
        0
    }
    fn daily_directional_trades(&self, _side: Side) -> u32 {
        0
    }
    fn consecutive_market_misses(&self, _market_id: &MarketId) -> u32 {
        0
    }
    fn record_trade_outcome(&self, _side: Side, _market_id: &MarketId, _was_miss: bool) {}
    fn ws_disconnect_secs(&self) -> u64 {
        0
    }
    fn api_error_count(&self) -> u64 {
        0
    }
    fn api_request_count(&self) -> u64 {
        0
    }

    fn metrics_age_secs(&self) -> u64 {
        0
    }

    fn is_stale(&self) -> bool {
        false
    }

    fn is_authoritative(&self) -> bool {
        true
    }
}

// ── Mock Balance Querier ────────────────────────────────────────────────────

struct MockQuerier {
    available: Usd,
    locked: Usd,
    positions: Vec<(MarketId, Usd)>,
}

impl Default for MockQuerier {
    fn default() -> Self {
        Self {
            available: Usd::new(dec!(1000)),
            locked: Usd::ZERO,
            positions: vec![],
        }
    }
}

#[async_trait::async_trait]
impl BalanceQuerier for MockQuerier {
    async fn query_balance(&self) -> OxideResult<(Usd, Usd)> {
        Ok((self.available, self.locked))
    }
    async fn query_positions(&self) -> OxideResult<Vec<(MarketId, Usd)>> {
        Ok(self.positions.clone())
    }
}

// ── Tests ───────────────────────────────────────────────────────────────────

#[tokio::test]
async fn reconciliation_ok_when_everything_matches() {
    let metrics = MockReconMetrics::default();
    let querier = MockQuerier::default();
    let reconciler = LedgerReconciler::new(dec!(1));

    let report = reconciler.reconcile(&metrics, &querier).await.unwrap();
    assert_eq!(report.status, ReconciliationStatus::Ok);
    assert!(report.mismatches.is_empty());
}

#[tokio::test]
async fn reconciliation_warning_when_balance_drifts_within_10x_tolerance() {
    let metrics = MockReconMetrics {
        balance: Usd::new(dec!(1000)),
        ..Default::default()
    };
    // External is off by 5 — above tolerance(1) but below 10x threshold(10)
    let querier = MockQuerier {
        available: Usd::new(dec!(995)),
        ..Default::default()
    };
    let reconciler = LedgerReconciler::new(dec!(1));

    let report = reconciler.reconcile(&metrics, &querier).await.unwrap();
    assert_eq!(report.status, ReconciliationStatus::Warning);
    assert!(!report.mismatches.is_empty());
}

#[tokio::test]
async fn reconciliation_critical_when_balance_drifts_beyond_10x_tolerance() {
    let metrics = MockReconMetrics {
        balance: Usd::new(dec!(1000)),
        ..Default::default()
    };
    // External is off by 15 — above 10x threshold(10)
    let querier = MockQuerier {
        available: Usd::new(dec!(985)),
        ..Default::default()
    };
    let reconciler = LedgerReconciler::new(dec!(1));

    let report = reconciler.reconcile(&metrics, &querier).await.unwrap();
    assert_eq!(report.status, ReconciliationStatus::Critical);
    assert!(!report.mismatches.is_empty());
}

#[tokio::test]
async fn reconciliation_detects_position_drift() {
    let market = MarketId::new("0xdrift_market");
    let metrics = MockReconMetrics {
        market_exposures: vec![(market.clone(), Usd::new(dec!(50)))],
        ..Default::default()
    };
    // External shows 40 for the same market — 10 drift, above tolerance of 1
    let querier = MockQuerier {
        positions: vec![(market, Usd::new(dec!(40)))],
        ..Default::default()
    };
    let reconciler = LedgerReconciler::new(dec!(1));

    let report = reconciler.reconcile(&metrics, &querier).await.unwrap();
    assert!(
        report
            .mismatches
            .iter()
            .any(|m| matches!(m, ReconciliationMismatch::PositionDrift { .. })),
        "expected PositionDrift mismatch, got: {:?}",
        report.mismatches
    );
}

#[tokio::test]
async fn reconciliation_detects_internal_markets_not_present_externally() {
    let market = MarketId::new("0xorphan_market");
    let metrics = MockReconMetrics {
        market_exposures: vec![(market.clone(), Usd::new(dec!(50)))],
        positions: vec![PositionInfo {
            position_id: PositionId::from_v7(),
            trade_id: TradeId::from_v7(),
            market_id: market,
            token_id: TokenId::new("tok"),
            side: Side::Buy,
            execution_mode: ExecutionMode::Live,
            shares: Shares::new(dec!(10)),
            avg_entry_price: Price::new(dec!(5)),
            total_cost_usd: Usd::new(dec!(50)),
            total_fees_usd: Usd::ZERO,
            unrealized_pnl: Usd::ZERO,
            realized_pnl: Usd::ZERO,
            status: PositionStatus::Open,
            opened_at: chrono::Utc::now(),
            closed_at: None,
            settled_at: None,
            winning_token_id: None,
            settlement_payout_usd: None,
            redeem_tx_hash: None,
            redeem_status: RedeemStatus::NotRequired,
            redeem_attempts: 0,
            oracle_verdict: None,
            settlement_trigger: None,
            settlement_accounting_status: SettlementAccountingStatus::Pending,
            settlement_accounting_error: None,
            settlement_accounted_at: None,
            redeem_terminal_reason: None,
            redeem_neg_risk: false,
            redeem_route: "standard_ctf".into(),
            redeem_holder_address: None,
            redeem_resolution: RedeemResolutionSource::ClassStandard,
            redeem_gas_limit: 500_000,
        }],
        ..Default::default()
    };
    // External has no positions at all
    let querier = MockQuerier::default();
    let reconciler = LedgerReconciler::new(dec!(1));

    let report = reconciler.reconcile(&metrics, &querier).await.unwrap();
    assert!(
        report.mismatches.iter().any(|m| matches!(
            m,
            ReconciliationMismatch::PositionDrift {
                external,
                ..
            } if *external == Usd::ZERO
        )),
        "expected PositionDrift with zero external, got: {:?}",
        report.mismatches
    );
}

#[tokio::test]
async fn reconciliation_skips_position_drift_when_external_positions_are_unknown() {
    let market = MarketId::new("0xunknown_external_positions");
    let metrics = MockReconMetrics {
        balance: Usd::new(dec!(1000)),
        total_exposure: Usd::new(dec!(50)),
        market_exposures: vec![(market.clone(), Usd::new(dec!(50)))],
        positions: vec![PositionInfo {
            position_id: PositionId::from_v7(),
            trade_id: TradeId::from_v7(),
            market_id: market,
            token_id: TokenId::new("tok"),
            side: Side::Buy,
            execution_mode: ExecutionMode::Live,
            shares: Shares::new(dec!(10)),
            avg_entry_price: Price::new(dec!(5)),
            total_cost_usd: Usd::new(dec!(50)),
            total_fees_usd: Usd::ZERO,
            unrealized_pnl: Usd::ZERO,
            realized_pnl: Usd::ZERO,
            status: PositionStatus::Open,
            opened_at: chrono::Utc::now(),
            closed_at: None,
            settled_at: None,
            winning_token_id: None,
            settlement_payout_usd: None,
            redeem_tx_hash: None,
            redeem_status: RedeemStatus::NotRequired,
            redeem_attempts: 0,
            oracle_verdict: None,
            settlement_trigger: None,
            settlement_accounting_status: SettlementAccountingStatus::Pending,
            settlement_accounting_error: None,
            settlement_accounted_at: None,
            redeem_terminal_reason: None,
            redeem_neg_risk: false,
            redeem_route: "standard_ctf".into(),
            redeem_holder_address: None,
            redeem_resolution: RedeemResolutionSource::ClassStandard,
            redeem_gas_limit: 500_000,
        }],
        reserved: Usd::ZERO,
    };
    let reconciler = LedgerReconciler::new(dec!(1));

    let report = reconciler.reconcile_fetched(
        &metrics,
        metrics.cash_balance(),
        metrics.cash_balance(),
        Usd::ZERO,
        None,
    );

    assert_eq!(report.status, ReconciliationStatus::Ok);
    assert!(
        report
            .mismatches
            .iter()
            .all(|m| !matches!(m, ReconciliationMismatch::PositionDrift { .. })),
        "external positions are unknown, not empty: {:?}",
        report.mismatches
    );
}
