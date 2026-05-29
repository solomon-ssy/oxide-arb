//! Configurable mock metrics for risk engine integration tests.

use oxide_arb_models::{
    domain::position::PositionInfo,
    enums::common::Side,
    types::{MarketId, Usd},
};
use oxide_arb_risk::traits::RiskMetrics;
use rust_decimal_macros::dec;

/// Mock [`RiskMetrics`] with overridable fields for deterministic tests.
#[derive(Debug, Clone)]
pub struct MockMetrics {
    pub balance: Usd,
    pub total_exposure: Usd,
    pub market_exposure: Usd,
    pub open_position_count: usize,
    pub open_directional_count: usize,
    pub daily_directional_trades: u32,
    pub consecutive_misses: u32,
    pub ws_disconnect_secs: u64,
    pub reserved_usd: Usd,
    pub active_reservation_count: usize,
    pub api_error_count: u64,
    pub api_request_count: u64,
}

impl Default for MockMetrics {
    fn default() -> Self {
        Self::healthy()
    }
}

impl MockMetrics {
    #[must_use]
    pub const fn healthy() -> Self {
        Self {
            balance: Usd::new(dec!(5000)),
            total_exposure: Usd::new(dec!(100)),
            market_exposure: Usd::ZERO,
            open_position_count: 0,
            open_directional_count: 0,
            daily_directional_trades: 0,
            consecutive_misses: 0,
            ws_disconnect_secs: 0,
            reserved_usd: Usd::ZERO,
            active_reservation_count: 0,
            api_error_count: 0,
            api_request_count: 0,
        }
    }
}

impl RiskMetrics for MockMetrics {
    fn total_exposure(&self) -> Usd {
        self.total_exposure
    }

    fn market_exposure(&self, _market_id: &MarketId) -> Usd {
        self.market_exposure
    }

    fn open_position_count(&self) -> usize {
        self.open_position_count
    }

    fn open_positions(&self) -> Vec<PositionInfo> {
        Vec::new()
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
        self.active_reservation_count
    }

    fn reserved_usd(&self) -> Usd {
        self.reserved_usd
    }

    fn open_directional_count(&self, _side: Side) -> usize {
        self.open_directional_count
    }

    fn daily_directional_trades(&self, _side: Side) -> u32 {
        self.daily_directional_trades
    }

    fn consecutive_market_misses(&self, _market_id: &MarketId) -> u32 {
        self.consecutive_misses
    }

    fn record_trade_outcome(&self, _side: Side, _market_id: &MarketId, _was_miss: bool) {}

    fn ws_disconnect_secs(&self) -> u64 {
        self.ws_disconnect_secs
    }

    fn api_error_count(&self) -> u64 {
        self.api_error_count
    }

    fn api_request_count(&self) -> u64 {
        self.api_request_count
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
