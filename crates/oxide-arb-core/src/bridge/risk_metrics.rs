//! Thin `RiskMetrics` trait adapter over shared state and live reservation/WS probes.

use std::sync::Arc;

use crate::exposure::in_memory::InMemoryExposureReservation;
use crate::service::risk_metrics_refresh::RiskMetricsState;
use oxide_arb_api::ws::ClobWsManager;
use oxide_arb_models::domain::position::PositionInfo;
use oxide_arb_models::enums::common::Side;
use oxide_arb_models::types::{MarketId, Usd};
use oxide_arb_risk::traits::RiskMetrics;

pub struct CoreRiskMetrics {
    state: Arc<RiskMetricsState>,
    exposure: Arc<InMemoryExposureReservation>,
    ws_manager: Arc<ClobWsManager>,
}

impl CoreRiskMetrics {
    pub const fn new(
        state: Arc<RiskMetricsState>,
        exposure: Arc<InMemoryExposureReservation>,
        ws_manager: Arc<ClobWsManager>,
    ) -> Self {
        Self {
            state,
            exposure,
            ws_manager,
        }
    }

    pub const fn state(&self) -> &Arc<RiskMetricsState> {
        &self.state
    }
}

impl RiskMetrics for CoreRiskMetrics {
    fn total_exposure(&self) -> Usd {
        self.state.total_position_exposure() + self.exposure.total_reserved_usd_sync()
    }

    fn market_exposure(&self, market_id: &MarketId) -> Usd {
        self.state.market_position_exposure(market_id)
            + self.exposure.per_market_reserved_sync(market_id)
    }

    fn open_position_count(&self) -> usize {
        self.state.open_position_count()
    }

    fn open_positions(&self) -> Vec<PositionInfo> {
        self.state.open_positions()
    }

    fn cached_balance(&self) -> Usd {
        self.state.cached_balance()
    }

    fn active_reservation_count(&self) -> usize {
        self.exposure.active_count_sync()
    }

    fn reserved_usd(&self) -> Usd {
        self.exposure.total_reserved_usd_sync()
    }

    fn open_directional_count(&self, side: Side) -> usize {
        self.state.open_directional_count(side)
    }

    fn daily_directional_trades(&self, side: Side) -> u32 {
        self.state.daily_directional_trades(side)
    }

    fn consecutive_market_misses(&self, market_id: &MarketId) -> u32 {
        self.state.consecutive_market_misses(market_id)
    }

    fn ws_disconnect_secs(&self) -> u64 {
        self.ws_manager
            .last_message_age_ms()
            .map_or(u64::MAX, |ms| ms / 1000)
    }

    fn api_error_count(&self) -> u64 {
        self.state.api_tracker.errors_in_window()
    }

    fn api_request_count(&self) -> u64 {
        self.state.api_tracker.requests_in_window()
    }
}
