//! Startup recovery and in-memory state projection management.
//!
//! `recover_state` reconstructs all risk engine subsystems from persisted
//! snapshots and validates invariants before allowing trading.

use crate::accounting::{DailyAccounting, HourlyAccounting, WeeklyAccounting};
use crate::circuit_breaker::CircuitBreaker;
use crate::position::{PositionTracker, PotentialLossLedger};
use crate::traits::RiskMetrics;
use crate::types::{PeriodStats, StateVersion};
use chrono::{Timelike, Utc};
use oxide_arb_error::{OxideError, OxideResult};
use oxide_arb_models::config::RiskConfig;
use oxide_arb_models::domain::blacklist::BlacklistEntry;
use oxide_arb_models::domain::potential_loss::PotentialLossEntry;
use oxide_arb_models::domain::risk::RiskEngineSnapshot;
use oxide_arb_models::types::Usd;

/// Fully recovered state ready to initialize a `RiskEngine`.
pub struct RecoveredState {
    pub breaker: CircuitBreaker,
    pub daily: DailyAccounting,
    pub weekly: WeeklyAccounting,
    pub hourly: HourlyAccounting,
    pub position_tracker: PositionTracker,
    pub potential_loss: PotentialLossLedger,
    pub blacklist_entries: Vec<BlacklistEntry>,
    pub drawdown_hwm: Usd,
    pub state_version: StateVersion,
}

/// Reconstruct risk engine state from persisted snapshot.
///
/// Validates invariants and returns `Err` if any critical state is
/// corrupt or inconsistent (fail-closed).
pub fn recover_state(
    config: &RiskConfig,
    snapshot: &RiskEngineSnapshot,
    blacklist_entries: Vec<BlacklistEntry>,
    potential_loss_entries: Vec<PotentialLossEntry>,
    metrics: &dyn RiskMetrics,
) -> OxideResult<RecoveredState> {
    let today = Utc::now().date_naive();

    let breaker = CircuitBreaker::from_snapshot(config.circuit_breaker.clone(), snapshot)?;

    let snapshot_date = snapshot.snapshot_at.date_naive();
    let daily = if snapshot_date <= today {
        DailyAccounting::from_snapshot(
            snapshot_date,
            PeriodStats {
                loss: snapshot.daily_loss,
                pnl: snapshot.daily_pnl,
                trade_count: snapshot.daily_trade_count,
                success_count: snapshot.daily_success_count,
                miss_count: snapshot.daily_miss_count,
                ..PeriodStats::default()
            },
            Usd::new(config.daily_budget_usd),
            snapshot.daily_budget_spent,
        )
    } else {
        return Err(OxideError::Internal(format!(
            "snapshot date {snapshot_date} is in the future (today: {today})",
        )));
    };

    let weekly = WeeklyAccounting::from_snapshot(
        snapshot.snapshot_at.date_naive(),
        PeriodStats {
            loss: snapshot.weekly_loss,
            trade_count: snapshot.weekly_trade_count,
            ..PeriodStats::default()
        },
    );

    let hourly = HourlyAccounting::from_snapshot(
        snapshot.snapshot_at.hour(),
        snapshot.snapshot_at.date_naive(),
        PeriodStats {
            loss: snapshot.hourly_loss,
            trade_count: snapshot.hourly_trade_count,
            success_count: snapshot.hourly_success_count,
            miss_count: snapshot.hourly_miss_count,
            ..PeriodStats::default()
        },
    );

    if snapshot.daily_loss.is_negative() {
        return Err(OxideError::Internal(
            "snapshot daily_loss is negative".into(),
        ));
    }
    if snapshot.weekly_loss.is_negative() {
        return Err(OxideError::Internal(
            "snapshot weekly_loss is negative".into(),
        ));
    }
    if snapshot.hourly_loss.is_negative() {
        return Err(OxideError::Internal(
            "snapshot hourly_loss is negative".into(),
        ));
    }

    let mut position_tracker = PositionTracker::new();
    position_tracker.refresh(metrics);

    let potential_loss = PotentialLossLedger::from_entries(potential_loss_entries);

    let drawdown_hwm = if snapshot.hwm_equity.is_positive() {
        snapshot.hwm_equity
    } else {
        metrics.cached_balance()
    };

    Ok(RecoveredState {
        breaker,
        daily,
        weekly,
        hourly,
        position_tracker,
        potential_loss,
        blacklist_entries,
        drawdown_hwm,
        state_version: StateVersion::new(0),
    })
}
