//! Thin `RiskMetrics` trait adapter over shared state and live reservation/WS probes.

use std::sync::Arc;

use crate::exposure::in_memory::InMemoryExposureReservation;
use crate::service::risk_metrics::RiskMetricsState;
use oxide_arb_api::ws::ClobWsManager;
use oxide_arb_models::domain::position::PositionInfo;
use oxide_arb_models::enums::common::Side;
use oxide_arb_models::types::{MarketId, Usd};
use oxide_arb_risk::traits::{RiskMetrics, RiskMetricsSnapshot};

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
    #[inline]
    fn total_exposure(&self) -> Usd {
        self.state.total_position_exposure() + self.exposure.total_reserved_usd_sync()
    }

    #[inline]
    fn market_exposure(&self, market_id: &MarketId) -> Usd {
        self.state.market_position_exposure(market_id)
            + self.exposure.per_market_reserved_sync(market_id)
    }

    #[inline]
    fn open_position_count(&self) -> usize {
        self.state.open_position_count()
    }

    fn open_positions(&self) -> Vec<PositionInfo> {
        self.state.open_positions()
    }

    #[inline]
    fn cached_balance(&self) -> Usd {
        self.state.cached_balance()
    }

    #[inline]
    fn active_reservation_count(&self) -> usize {
        self.exposure.active_count_sync()
    }

    #[inline]
    fn reserved_usd(&self) -> Usd {
        self.exposure.total_reserved_usd_sync()
    }

    #[inline]
    fn open_directional_count(&self, side: Side) -> usize {
        self.state.open_directional_count(side)
    }

    #[inline]
    fn daily_directional_trades(&self, side: Side) -> u32 {
        self.state.daily_directional_trades(side)
    }

    #[inline]
    fn consecutive_market_misses(&self, market_id: &MarketId) -> u32 {
        self.state.consecutive_market_misses(market_id)
    }

    #[inline]
    fn ws_disconnect_secs(&self) -> u64 {
        self.ws_manager
            .last_message_age_ms()
            .map_or(u64::MAX, |ms| ms / 1000)
    }

    #[inline]
    fn api_error_count(&self) -> u64 {
        self.state.api_tracker.errors_in_window()
    }

    #[inline]
    fn api_request_count(&self) -> u64 {
        self.state.api_tracker.requests_in_window()
    }

    fn snapshot_for(&self, market_id: &MarketId) -> RiskMetricsSnapshot {
        let snap = self.state.load_metrics_snapshot(market_id);
        let reserved = self.exposure.total_reserved_usd_sync();
        let market_reserved = self.exposure.per_market_reserved_sync(market_id);
        RiskMetricsSnapshot {
            cached_balance: snap.cached_balance,
            total_exposure: snap.total_position_exposure + reserved,
            market_exposure: snap.market_position_exposure + market_reserved,
            open_position_count: snap.open_position_count,
            active_reservation_count: self.exposure.active_count_sync(),
            reserved_usd: reserved,
            open_directional_count_buy: snap.open_buy_count,
            open_directional_count_sell: snap.open_sell_count,
            daily_directional_trades_buy: snap.daily_buy_trades,
            daily_directional_trades_sell: snap.daily_sell_trades,
            consecutive_market_misses: snap.consecutive_market_misses,
            ws_disconnect_secs: self
                .ws_manager
                .last_message_age_ms()
                .map_or(u64::MAX, |ms| ms / 1000),
            api_error_count: self.state.api_tracker.errors_in_window(),
            api_request_count: self.state.api_tracker.requests_in_window(),
        }
    }
}
