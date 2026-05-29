//! State recovery edge-case tests.
//!
//! Exercises `state_store::recover_state()` with valid and invalid snapshots
//! to verify fail-closed behaviour on corrupt or inconsistent data.

mod support;

use chrono::{Duration, Utc};
use oxide_arb_error::OxideError;
use oxide_arb_models::{
    config::RiskConfig,
    domain::risk::RiskEngineState,
    enums::risk::{BreakerStateName, CircuitBreakerLevel},
    types::Usd,
};
use oxide_arb_risk::{clock::utc_clock, state_store};
use rust_decimal_macros::dec;
use support::MockMetrics;

// ── Helpers ─────────────────────────────────────────────────────────────────

fn default_snapshot() -> RiskEngineState {
    let now = Utc::now();
    RiskEngineState {
        breaker_state: BreakerStateName::Closed,
        breaker_level: None,
        is_halted: false,
        halt_reason: None,
        cooldown_until: None,
        total_exposure: Usd::ZERO,
        hourly_loss_usd: Usd::ZERO,
        hourly_fee_usd: Usd::ZERO,
        hourly_trade_count: 0,
        hourly_success_count: 0,
        hourly_miss_count: 0,
        hourly_window_start: now,
        daily_pnl: Usd::ZERO,
        daily_loss_usd: Usd::ZERO,
        daily_fee_usd: Usd::ZERO,
        daily_budget_spent: Usd::ZERO,
        daily_trade_count: 0,
        daily_success_count: 0,
        daily_miss_count: 0,
        daily_window_start: now.date_naive(),
        weekly_loss_usd: Usd::ZERO,
        weekly_trade_count: 0,
        weekly_window_start: now.date_naive(),
        consecutive_misses: 0,
        cooldown_multiplier: 0,
        hwm_equity: Usd::new(dec!(5000)),
        last_emergency_at: None,
        last_emergency_reason: None,
        snapshot_at: now,
    }
}

fn default_config() -> RiskConfig {
    RiskConfig {
        max_total_exposure_usd: dec!(5000),
        max_single_market_exposure_usd: dec!(500),
        max_single_bet_usd: dec!(25),
        max_open_positions: 5,
        max_daily_loss_usd: dec!(75),
        max_weekly_loss_usd: dec!(120),
        daily_budget_usd: dec!(200),
        min_balance_usd: dec!(50),
        reserve_balance_usd: dec!(100),
        min_trade_usd: dec!(1),
        ..RiskConfig::default()
    }
}

fn try_recover(snapshot: &RiskEngineState) -> Result<state_store::RecoveredState, OxideError> {
    let config = default_config();
    let clock = utc_clock();
    let metrics = MockMetrics::healthy();
    state_store::recover_state(&config, snapshot, vec![], vec![], &metrics, &clock)
}

// ── Tests ───────────────────────────────────────────────────────────────────

#[test]
fn recover_closed_breaker_succeeds() {
    let snap = default_snapshot();
    let result = try_recover(&snap);
    assert!(
        result.is_ok(),
        "closed breaker should recover: {}",
        result.err().map_or_else(String::new, |e| e.to_string())
    );
}

#[test]
fn recover_restores_fee_windows() {
    let mut snap = default_snapshot();
    snap.daily_fee_usd = Usd::new(dec!(3.25));
    snap.hourly_fee_usd = Usd::new(dec!(1.50));

    let recovered = try_recover(&snap).expect("state should recover");

    assert_eq!(recovered.daily.fees(), Usd::new(dec!(3.25)));
    assert_eq!(recovered.hourly.fees(), Usd::new(dec!(1.50)));
}

#[test]
fn recover_open_breaker_without_level_fails() {
    let mut snap = default_snapshot();
    snap.breaker_state = BreakerStateName::Open;
    snap.breaker_level = None;
    snap.halt_reason = Some("test".into());
    snap.cooldown_until = Some(Utc::now() + Duration::minutes(5));

    let result = try_recover(&snap);
    assert!(result.is_err(), "Open without level should fail");
}

#[test]
fn recover_open_breaker_without_reason_fails() {
    let mut snap = default_snapshot();
    snap.breaker_state = BreakerStateName::Open;
    snap.breaker_level = Some(CircuitBreakerLevel::Session);
    snap.halt_reason = None;
    snap.cooldown_until = Some(Utc::now() + Duration::minutes(5));

    let result = try_recover(&snap);
    assert!(result.is_err(), "Open without reason should fail");
}

#[test]
fn recover_open_breaker_without_cooling_until_fails() {
    let mut snap = default_snapshot();
    snap.breaker_state = BreakerStateName::Open;
    snap.breaker_level = Some(CircuitBreakerLevel::Session);
    snap.halt_reason = Some("test".into());
    snap.cooldown_until = None;

    let result = try_recover(&snap);
    assert!(result.is_err(), "Open without cooling_until should fail");
}

#[test]
fn recover_future_snapshot_date_fails() {
    let mut snap = default_snapshot();
    snap.snapshot_at = Utc::now() + Duration::days(1);

    let result = try_recover(&snap);
    assert!(result.is_err(), "future snapshot date should fail");
}

#[test]
fn recover_negative_daily_loss_fails() {
    let mut snap = default_snapshot();
    snap.daily_loss_usd = Usd::new(dec!(-10));

    let result = try_recover(&snap);
    assert!(result.is_err(), "negative daily loss should fail");
}

#[test]
fn recover_negative_weekly_loss_fails() {
    let mut snap = default_snapshot();
    snap.weekly_loss_usd = Usd::new(dec!(-5));

    let result = try_recover(&snap);
    assert!(result.is_err(), "negative weekly loss should fail");
}

#[test]
fn recover_negative_hourly_loss_fails() {
    let mut snap = default_snapshot();
    snap.hourly_loss_usd = Usd::new(dec!(-1));

    let result = try_recover(&snap);
    assert!(result.is_err(), "negative hourly loss should fail");
}

#[test]
fn recover_half_open_without_level_fails() {
    let mut snap = default_snapshot();
    snap.breaker_state = BreakerStateName::HalfOpen;
    snap.breaker_level = None;

    let result = try_recover(&snap);
    assert!(result.is_err(), "HalfOpen without level should fail");
}
