//! In-memory [`RiskMetrics`] for integration tests and harnesses.

use oxide_arb_models::domain::position::PositionInfo;
use oxide_arb_models::enums::common::Side;
use oxide_arb_models::types::{MarketId, Usd};
use oxide_arb_risk::traits::RiskMetrics;
use rust_decimal_macros::dec;

/// Zero-friction metrics snapshot with healthy defaults for execution tests.
pub struct TestRiskMetrics;

impl RiskMetrics for TestRiskMetrics {
    fn total_exposure(&self) -> Usd {
        Usd::new(dec!(100))
    }

    fn market_exposure(&self, _: &MarketId) -> Usd {
        Usd::ZERO
    }

    fn open_position_count(&self) -> usize {
        0
    }

    fn open_positions(&self) -> Vec<PositionInfo> {
        Vec::new()
    }

    fn cached_balance(&self) -> Usd {
        Usd::new(dec!(5000))
    }

    fn active_reservation_count(&self) -> usize {
        0
    }

    fn reserved_usd(&self) -> Usd {
        Usd::ZERO
    }

    fn open_directional_count(&self, _: Side) -> usize {
        0
    }

    fn daily_directional_trades(&self, _: Side) -> u32 {
        0
    }

    fn consecutive_market_misses(&self, _: &MarketId) -> u32 {
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
