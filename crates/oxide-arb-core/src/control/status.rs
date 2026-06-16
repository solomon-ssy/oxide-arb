//! Live [`SystemStatus`] assembly shared by the runtime control port and the
//! WebSocket status publisher.

use crate::control::mode_transition::CoreRuntimeControlDeps;
use chrono::Utc;
use oxide_arb_models::{
    domain::{CatalogStatusPort, SystemBalanceSource, SystemBalanceView, SystemStatus},
    enums::common::ExecutionMode,
    types::Usd,
};
use oxide_arb_risk::traits::RiskMetrics;
use std::{
    sync::Arc,
    time::{Duration, Instant},
};
use tokio::sync::{Notify, futures::Notified};

/// Default interval between periodic `SystemStatusChanged` pushes.
pub const SYSTEM_STATUS_BROADCAST_INTERVAL: Duration = Duration::from_secs(5);

/// Build the aggregate system status snapshot from live subsystem handles.
pub fn build_system_status(deps: &CoreRuntimeControlDeps, started_at: Instant) -> SystemStatus {
    let metrics = deps.metrics.as_ref();
    let snapshot = deps.risk_engine.snapshot(metrics);
    SystemStatus {
        execution_mode: deps.execution_mode.current(),
        breaker_state: snapshot.breaker_state,
        uptime_secs: started_at.elapsed().as_secs(),
        active_markets: u32::try_from(deps.market_registry.active_markets().len())
            .unwrap_or(u32::MAX),
        open_positions: u32::try_from(metrics.open_position_count()).unwrap_or(u32::MAX),
        pending_reservations: u32::try_from(deps.exposure.active_count_sync()).unwrap_or(u32::MAX),
        total_exposure: snapshot.total_exposure,
        daily_pnl: snapshot.daily_pnl,
        catalog: deps.catalog.catalog_state(),
        checked_at: Utc::now(),
    }
}

/// Build the operator money-state snapshot from live risk metrics and runtime caps.
pub fn build_system_balance(deps: &CoreRuntimeControlDeps) -> SystemBalanceView {
    let metrics = deps.metrics.as_ref();
    let mode = deps.execution_mode.current();
    let runtime = deps.runtime_config.current();
    let bankroll_cap = Usd::new(runtime.risk.bankroll_usd);
    let reserve_balance = Usd::new(runtime.risk.reserve_balance_usd);
    let cash_balance = metrics.cash_balance();
    let position_mark_value = metrics.position_mark_value();
    let equity = metrics.equity();
    let reserved = metrics.reserved_usd();
    let available_dynamic = (equity - reserve_balance - reserved).max(Usd::ZERO);
    let available_before_potential_loss =
        Usd::new(bankroll_cap.inner().min(available_dynamic.inner())).max(Usd::ZERO);

    SystemBalanceView {
        execution_mode: mode,
        source: balance_source(mode, metrics.is_authoritative()),
        cash_balance_usd: cash_balance,
        position_mark_value_usd: position_mark_value,
        equity_usd: equity,
        bankroll_cap_usd: bankroll_cap,
        reserve_balance_usd: reserve_balance,
        reserved_usd: reserved,
        total_exposure_usd: metrics.total_exposure(),
        available_before_potential_loss_usd: available_before_potential_loss,
        open_position_count: u32::try_from(metrics.open_position_count()).unwrap_or(u32::MAX),
        active_reservation_count: u32::try_from(metrics.active_reservation_count())
            .unwrap_or(u32::MAX),
        metrics_age_secs: metrics.metrics_age_secs(),
        is_authoritative: metrics.is_authoritative(),
        is_stale: metrics.is_stale(),
        checked_at: Utc::now(),
    }
}

const fn balance_source(mode: ExecutionMode, is_authoritative: bool) -> SystemBalanceSource {
    match (mode, is_authoritative) {
        (ExecutionMode::Live, true) => SystemBalanceSource::AuthoritativeClob,
        (ExecutionMode::DryRun, true) => SystemBalanceSource::SimulatedDryRun,
        (ExecutionMode::Paper, true) => SystemBalanceSource::SimulatedPaper,
        _ => SystemBalanceSource::NonAuthoritative,
    }
}

/// Shared nudge handle for immediate status broadcasts outside the periodic tick.
#[derive(Clone, Default)]
pub struct SystemStatusNudge {
    inner: Arc<Notify>,
}

impl SystemStatusNudge {
    /// Signal the broadcaster to publish a fresh snapshot now.
    pub fn nudge(&self) {
        self.inner.notify_waiters();
    }

    pub(crate) fn wait_notified(&self) -> Notified<'_> {
        self.inner.notified()
    }
}
