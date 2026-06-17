//! Live [`SystemStatus`] assembly shared by the runtime control port and the
//! WebSocket status publisher.

use crate::{
    control::{factor_snapshot::FactorSnapshotStore, mode_transition::CoreRuntimeControlDeps},
    service::runtime_lifecycle::{
        LatestUnhealthySubsystems, LifecycleSnapshot, evaluate_lifecycle, lifecycle_inputs,
    },
};
use chrono::Utc;
use oxide_arb_models::{
    domain::{CatalogStatusPort, SystemBalanceSource, SystemBalanceView, SystemStatus},
    enums::common::ExecutionMode,
    types::{MarketId, Usd},
};
use oxide_arb_risk::{engine::RiskEngine, traits::RiskMetrics};
use std::{
    sync::Arc,
    time::{Duration, Instant},
};
use tokio::sync::{Notify, futures::Notified};

/// Default interval between periodic `SystemStatusChanged` pushes.
pub const SYSTEM_STATUS_BROADCAST_INTERVAL: Duration = Duration::from_secs(5);

/// Collect lifecycle inputs shared by system status and health checks.
#[must_use]
pub fn lifecycle_snapshot(deps: &CoreRuntimeControlDeps) -> LifecycleSnapshot {
    lifecycle_snapshot_from_parts(
        deps.risk_engine.as_ref(),
        deps.metrics.as_ref(),
        deps.execution_mode.current(),
        deps.factor_store.as_ref(),
        deps.unhealthy_subsystems.as_ref(),
    )
}

/// Build a lifecycle snapshot from subsystem handles (health checker path).
#[must_use]
pub fn lifecycle_snapshot_from_parts(
    risk_engine: &RiskEngine,
    metrics: &dyn RiskMetrics,
    mode: ExecutionMode,
    factor_store: &FactorSnapshotStore,
    unhealthy_subsystems: &LatestUnhealthySubsystems,
) -> LifecycleSnapshot {
    let snapshot = risk_engine.snapshot(metrics);
    let published = factor_store.published();
    LifecycleSnapshot {
        unhealthy: unhealthy_subsystems.snapshot(),
        breaker_state: snapshot.breaker_state,
        control_factor_snapshot_expired: published
            .publication_id
            .as_ref()
            .is_some_and(|_| published.is_expired_at(Utc::now())),
        control_factor_live_warn: mode == ExecutionMode::Live && published.publication_id.is_none(),
    }
}

/// Evaluate operator lifecycle from a precomputed snapshot.
#[must_use]
pub fn evaluate_operational_lifecycle(
    deps: &CoreRuntimeControlDeps,
    snap: &LifecycleSnapshot,
) -> (
    oxide_arb_models::domain::OperationalPhase,
    oxide_arb_models::domain::MarketDataConnectivity,
) {
    evaluate_lifecycle(&lifecycle_inputs(
        deps.catalog.as_ref(),
        deps.ws_manager.as_ref(),
        snap,
    ))
}

/// Build the aggregate system status snapshot from live subsystem handles.
pub fn build_system_status(deps: &CoreRuntimeControlDeps, started_at: Instant) -> SystemStatus {
    let metrics = deps.metrics.as_ref();
    let snapshot = deps.risk_engine.snapshot(metrics);
    let published = deps.factor_store.published();
    let mode = deps.execution_mode.current();
    let lifecycle = lifecycle_snapshot(deps);
    let (operational_phase, market_data) = evaluate_operational_lifecycle(deps, &lifecycle);
    let snapshot_expired = lifecycle.control_factor_snapshot_expired;
    let live_warn = lifecycle.control_factor_live_warn;
    SystemStatus {
        execution_mode: mode,
        breaker_state: snapshot.breaker_state,
        uptime_secs: started_at.elapsed().as_secs(),
        active_markets: u32::try_from(deps.market_registry.active_markets().len())
            .unwrap_or(u32::MAX),
        open_positions: u32::try_from(metrics.open_position_count()).unwrap_or(u32::MAX),
        pending_reservations: u32::try_from(deps.exposure.active_count_sync()).unwrap_or(u32::MAX),
        total_exposure: snapshot.total_exposure,
        daily_pnl: snapshot.daily_pnl,
        catalog: deps.catalog.catalog_state(),
        operational_phase,
        market_data,
        control_factor_publication_id: published.publication_id.as_ref().map(ToString::to_string),
        control_factor_snapshot_expired: snapshot_expired,
        control_factor_live_warn: live_warn,
        checked_at: Utc::now(),
    }
}

/// Build the operator money-state snapshot from live risk metrics and runtime caps.
pub fn build_system_balance(deps: &CoreRuntimeControlDeps) -> SystemBalanceView {
    let metrics = deps.metrics.as_ref();
    let mode = deps.execution_mode.current();
    let runtime = deps.runtime_config.current();
    let integrity = deps.trade_integrity.load();
    let bankroll_cap = Usd::new(runtime.risk.bankroll_usd);
    let reserve_balance = Usd::new(runtime.risk.reserve_balance_usd);
    let portfolio_market = MarketId::new("0x0");
    SystemBalanceView {
        execution_mode: mode,
        source: balance_source(mode, metrics.is_authoritative()),
        cash_balance_usd: metrics.cash_balance(),
        position_mark_value_usd: metrics.position_mark_value(),
        equity_usd: metrics.equity(),
        bankroll_cap_usd: bankroll_cap,
        reserve_balance_usd: reserve_balance,
        reserved_usd: metrics.reserved_usd(),
        total_exposure_usd: metrics.total_exposure(),
        available_for_sizing_usd: deps.risk_engine.available_bankroll_for_sizing(metrics),
        potential_loss_usd: deps.risk_engine.total_potential_loss_usd(),
        blocking_trade_count: integrity.blocking_count,
        needs_reconcile_count: integrity.needs_reconcile_count,
        max_total_exposure_usd: Usd::new(runtime.risk.max_total_exposure_usd),
        max_single_market_exposure_usd: Usd::new(runtime.risk.max_single_market_exposure_usd),
        max_total_exposure_pct: runtime.risk.max_total_exposure_pct,
        binding_exposure_limit: deps.risk_engine.binding_exposure_limit(
            metrics,
            Usd::ZERO,
            &portfolio_market,
        ),
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
