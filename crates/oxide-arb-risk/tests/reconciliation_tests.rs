//! `LedgerReconciler` edge-case tests.
//!
//! Exercises the reconciliation engine with mock metrics and balance queries
//! to verify Ok / Warning / Critical classification and mismatch detection.

use oxide_arb_models::domain::position::PositionInfo;
use oxide_arb_models::enums::common::Side;
use oxide_arb_models::types::{MarketId, Usd};
use oxide_arb_risk::reconciliation::LedgerReconciler;
use oxide_arb_risk::traits::{BalanceQuerier, RiskMetrics};
use oxide_arb_risk::types::ReconciliationStatus;
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
    fn cached_balance(&self) -> Usd {
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
    fn ws_disconnect_secs(&self) -> u64 {
        0
    }
    fn api_error_count(&self) -> u64 {
        0
    }
    fn api_request_count(&self) -> u64 {
        0
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
    async fn query_balance(&self) -> oxide_arb_error::OxideResult<(Usd, Usd)> {
        Ok((self.available, self.locked))
    }
    async fn query_positions(&self) -> oxide_arb_error::OxideResult<Vec<(MarketId, Usd)>> {
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
        report.mismatches.iter().any(|m| matches!(
            m,
            oxide_arb_risk::types::ReconciliationMismatch::PositionDrift { .. }
        )),
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
            market_id: market,
            token_id: oxide_arb_models::types::TokenId::new("tok"),
            side: Side::Buy,
            size: oxide_arb_models::types::Shares::new(dec!(10)),
            avg_entry_price: oxide_arb_models::types::Price::new(dec!(5)),
            cost_basis: Usd::new(dec!(50)),
            updated_at: chrono::Utc::now(),
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
            oxide_arb_risk::types::ReconciliationMismatch::PositionDrift {
                external,
                ..
            } if *external == Usd::ZERO
        )),
        "expected PositionDrift with zero external, got: {:?}",
        report.mismatches
    );
}
