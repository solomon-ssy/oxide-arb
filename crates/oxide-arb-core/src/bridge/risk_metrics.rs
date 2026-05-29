//! Thin `RiskMetrics` trait adapter over shared state and live reservation/WS probes.

use crate::{
    exposure::in_memory::InMemoryExposureReservation, service::risk_metrics::RiskMetricsState,
};
use oxide_arb_api::ws::ClobWsManager;
use oxide_arb_models::{
    domain::position::PositionInfo,
    enums::common::{ExecutionMode, Side},
    types::{MarketId, Usd},
};
use oxide_arb_risk::traits::{RiskMetrics, RiskMetricsSnapshot};
use std::{
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};

pub struct CoreRiskMetrics {
    state: Arc<RiskMetricsState>,
    exposure: Arc<InMemoryExposureReservation>,
    ws_manager: Arc<ClobWsManager>,
    execution_mode: ExecutionMode,
}

impl CoreRiskMetrics {
    pub const fn new(
        state: Arc<RiskMetricsState>,
        exposure: Arc<InMemoryExposureReservation>,
        ws_manager: Arc<ClobWsManager>,
        execution_mode: ExecutionMode,
    ) -> Self {
        Self {
            state,
            exposure,
            ws_manager,
            execution_mode,
        }
    }

    pub const fn state(&self) -> &Arc<RiskMetricsState> {
        &self.state
    }

    fn build_snapshot(&self, market_id: &MarketId) -> RiskMetricsSnapshot {
        let snap = self.state.load_metrics_snapshot(market_id);
        let (reserved, market_reserved, active_count) =
            self.exposure.reservation_snapshot_sync(market_id);
        let ws_disconnect_secs = self.ws_disconnect_secs_for_mode();
        RiskMetricsSnapshot {
            version: self.state.metrics_version(),
            cash_balance: snap.cash_balance,
            position_mark_value: snap.position_mark_value,
            equity: snap.equity,
            total_exposure: snap.total_position_exposure + reserved,
            market_exposure: snap.market_position_exposure + market_reserved,
            open_position_count: snap.open_position_count,
            active_reservation_count: active_count,
            reserved_usd: reserved,
            open_directional_count_buy: snap.open_buy_count,
            open_directional_count_sell: snap.open_sell_count,
            daily_directional_trades_buy: snap.daily_buy_trades,
            daily_directional_trades_sell: snap.daily_sell_trades,
            consecutive_market_misses: snap.consecutive_market_misses,
            ws_disconnect_secs,
            api_error_count: self.state.api_tracker.errors_in_window(),
            api_request_count: self.state.api_tracker.requests_in_window(),
            is_stale: self.is_stale_for_mode(),
            metrics_age_secs: self.metrics_age_secs_for_mode(),
            is_authoritative: self.is_authoritative_for_mode(),
        }
    }

    fn ws_disconnect_secs_for_mode(&self) -> u64 {
        self.ws_manager.last_message_age_ms().map_or_else(
            || match self.execution_mode {
                ExecutionMode::Live => u64::MAX,
                ExecutionMode::DryRun | ExecutionMode::Paper => 0,
            },
            |ms| ms / 1000,
        )
    }

    fn metrics_age_secs_for_mode(&self) -> u64 {
        match self.execution_mode {
            ExecutionMode::DryRun | ExecutionMode::Paper => 0,
            ExecutionMode::Live => {
                now_ms().saturating_sub(self.state.last_successful_refresh_ms()) / 1000
            }
        }
    }

    fn is_authoritative_for_mode(&self) -> bool {
        match self.execution_mode {
            ExecutionMode::DryRun | ExecutionMode::Paper => true,
            ExecutionMode::Live => {
                self.state.source()
                    == crate::service::risk_metrics::RiskMetricsSource::AuthoritativeClob
            }
        }
    }

    fn is_stale_for_mode(&self) -> bool {
        match self.execution_mode {
            ExecutionMode::DryRun | ExecutionMode::Paper => false,
            ExecutionMode::Live => self.state.is_stale(),
        }
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
    fn cash_balance(&self) -> Usd {
        self.state.cash_balance()
    }

    #[inline]
    fn position_mark_value(&self) -> Usd {
        self.state.position_mark_value()
    }

    #[inline]
    fn equity(&self) -> Usd {
        self.state.equity()
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
        self.ws_disconnect_secs_for_mode()
    }

    #[inline]
    fn api_error_count(&self) -> u64 {
        self.state.api_tracker.errors_in_window()
    }

    #[inline]
    fn api_request_count(&self) -> u64 {
        self.state.api_tracker.requests_in_window()
    }

    #[inline]
    fn metrics_age_secs(&self) -> u64 {
        self.metrics_age_secs_for_mode()
    }

    #[inline]
    fn is_stale(&self) -> bool {
        self.is_stale_for_mode()
    }

    #[inline]
    fn is_authoritative(&self) -> bool {
        self.is_authoritative_for_mode()
    }

    fn snapshot_for(&self, market_id: &MarketId) -> RiskMetricsSnapshot {
        for _ in 0..2 {
            let v1 = self.state.metrics_version();
            let snap = self.build_snapshot(market_id);
            let v2 = self.state.metrics_version();
            if v1 == v2 {
                return snap;
            }
        }
        self.build_snapshot(market_id)
    }
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .ok()
        .and_then(|d| u64::try_from(d.as_millis()).ok())
        .unwrap_or(0)
}
