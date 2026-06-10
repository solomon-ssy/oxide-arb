//! Startup recovery and in-memory state projection management.
//!
//! `recover_state` reconstructs all risk engine subsystems from persisted
//! snapshots and validates invariants before allowing trading.

use crate::{
    accounting::{DailyAccounting, HourlyAccounting, WeeklyAccounting},
    circuit_breaker::CircuitBreaker,
    clock::Clock,
    position::PotentialLossLedger,
    traits::RiskMetrics,
    types::{PeriodStats, StateVersion},
};
use chrono::Timelike;
use num_traits::ToPrimitive;
use oxide_arb_error::{OxideError, OxideResult};
use oxide_arb_models::{
    domain::{blacklist::BlacklistInfo, potential_loss::PotentialLossInfo, risk::RiskEngineState},
    runtime_config::RiskConfig,
    types::Usd,
};
use std::sync::Arc;

/// Fully recovered state ready to initialize a `RiskEngine`.
pub struct RecoveredState {
    pub breaker: CircuitBreaker,
    pub daily: DailyAccounting,
    pub weekly: WeeklyAccounting,
    pub hourly: HourlyAccounting,
    pub potential_loss: PotentialLossLedger,
    pub blacklist_entries: Vec<BlacklistInfo>,
    pub drawdown_hwm: Usd,
    pub state_version: StateVersion,
}

/// Reconstruct risk engine state from persisted snapshot.
///
/// Validates invariants and returns `Err` if any critical state is
/// corrupt or inconsistent (fail-closed).
pub fn recover_state(
    config: &RiskConfig,
    snapshot: &RiskEngineState,
    blacklist_entries: Vec<BlacklistInfo>,
    potential_loss_entries: Vec<PotentialLossInfo>,
    metrics: &dyn RiskMetrics,
    clock: &Arc<dyn Clock>,
) -> OxideResult<RecoveredState> {
    let today = clock.today();

    let breaker =
        CircuitBreaker::from_snapshot(config.circuit_breaker.clone(), Arc::clone(clock), snapshot)?;

    let snapshot_date = snapshot.snapshot_at.date_naive();
    let mut daily = if snapshot_date <= today {
        DailyAccounting::from_snapshot(
            snapshot_date,
            PeriodStats {
                loss: snapshot.daily_loss_usd,
                pnl: snapshot.daily_pnl,
                fees: snapshot.daily_fee_usd,
                trade_count: ToPrimitive::to_u32(&snapshot.daily_trade_count).unwrap_or(0),
                success_count: ToPrimitive::to_u32(&snapshot.daily_success_count).unwrap_or(0),
                miss_count: ToPrimitive::to_u32(&snapshot.daily_miss_count).unwrap_or(0),
                ..PeriodStats::default()
            },
            Usd::new(config.daily_budget_usd),
            snapshot.daily_budget_spent,
            Arc::clone(clock),
        )
    } else {
        return Err(OxideError::Internal(format!(
            "snapshot date {snapshot_date} is in the future (today: {today})",
        )));
    };

    let mut weekly = WeeklyAccounting::from_snapshot(
        snapshot.snapshot_at.date_naive(),
        PeriodStats {
            loss: snapshot.weekly_loss_usd,
            trade_count: ToPrimitive::to_u32(&snapshot.weekly_trade_count).unwrap_or(0),
            ..PeriodStats::default()
        },
        Arc::clone(clock),
    );

    let mut hourly = HourlyAccounting::from_snapshot(
        snapshot.snapshot_at.hour(),
        snapshot.snapshot_at.date_naive(),
        PeriodStats {
            loss: snapshot.hourly_loss_usd,
            fees: snapshot.hourly_fee_usd,
            trade_count: ToPrimitive::to_u32(&snapshot.hourly_trade_count).unwrap_or(0),
            success_count: ToPrimitive::to_u32(&snapshot.hourly_success_count).unwrap_or(0),
            miss_count: ToPrimitive::to_u32(&snapshot.hourly_miss_count).unwrap_or(0),
            ..PeriodStats::default()
        },
        Arc::clone(clock),
    );

    let _ = daily.maybe_rollover();
    let _ = weekly.maybe_rollover();
    let _ = hourly.maybe_rollover();

    if snapshot.daily_loss_usd.is_negative() {
        return Err(OxideError::Internal(
            "snapshot daily_loss is negative".into(),
        ));
    }
    if snapshot.weekly_loss_usd.is_negative() {
        return Err(OxideError::Internal(
            "snapshot weekly_loss is negative".into(),
        ));
    }
    if snapshot.hourly_loss_usd.is_negative() {
        return Err(OxideError::Internal(
            "snapshot hourly_loss is negative".into(),
        ));
    }

    let potential_loss = PotentialLossLedger::from_entries(potential_loss_entries);
    let active_potential_loss = potential_loss.active_count();
    let open_positions = metrics.open_positions();
    if active_potential_loss > 0 && open_positions.is_empty() {
        return Err(OxideError::Internal(format!(
            "recovery invariant failed: {active_potential_loss} active potential-loss entries but no open positions"
        )));
    }
    if active_potential_loss > open_positions.len() {
        return Err(OxideError::Internal(format!(
            "recovery invariant failed: {active_potential_loss} active potential-loss entries exceed {} open positions",
            open_positions.len()
        )));
    }

    let drawdown_hwm = if snapshot.hwm_equity.is_positive() {
        snapshot.hwm_equity
    } else {
        metrics.equity()
    };

    Ok(RecoveredState {
        breaker,
        daily,
        weekly,
        hourly,
        potential_loss,
        blacklist_entries,
        drawdown_hwm,
        state_version: StateVersion::new(0),
    })
}
