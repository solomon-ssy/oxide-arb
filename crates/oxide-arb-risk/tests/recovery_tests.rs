//! State recovery edge-case tests.
//!
//! Exercises `state_store::recover_state()` with valid and invalid snapshots
//! to verify fail-closed behaviour on corrupt or inconsistent data.

use chrono::{Duration, Utc};
use oxide_arb_error::OxideError;
use oxide_arb_models::config::RiskConfig;
use oxide_arb_models::domain::position::PositionInfo;
use oxide_arb_models::domain::risk::RiskEngineSnapshot;
use oxide_arb_models::enums::common::Side;
use oxide_arb_models::enums::risk::{BreakerStateName, CircuitBreakerLevel};
use oxide_arb_models::types::{MarketId, Usd};
use oxide_arb_risk::clock::utc_clock;
use oxide_arb_risk::state_store;
use oxide_arb_risk::traits::RiskMetrics;
use rust_decimal_macros::dec;

// ── Mock Metrics ────────────────────────────────────────────────────────────

struct MockMetrics;

impl RiskMetrics for MockMetrics {
    fn total_exposure(&self) -> Usd {
        Usd::ZERO
    }
    fn market_exposure(&self, _market_id: &MarketId) -> Usd {
        Usd::ZERO
    }
    fn open_position_count(&self) -> usize {
        0
    }
    fn open_positions(&self) -> Vec<PositionInfo> {
        vec![]
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
    fn open_directional_count(&self, _side: Side) -> usize {
        0
    }
    fn daily_directional_trades(&self, _side: Side) -> u32 {
        0
    }
    fn consecutive_market_misses(&self, _market_id: &MarketId) -> u32 {
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

// ── Helpers ─────────────────────────────────────────────────────────────────

fn default_snapshot() -> RiskEngineSnapshot {
    RiskEngineSnapshot {
        breaker_state: BreakerStateName::Closed,
        breaker_level: None,
        breaker_reason: None,
        cooling_until: None,
        total_exposure: Usd::ZERO,
        daily_pnl: Usd::ZERO,
        daily_loss: Usd::ZERO,
        weekly_loss: Usd::ZERO,
        hourly_loss: Usd::ZERO,
        hourly_trade_count: 0,
        hourly_success_count: 0,
        hourly_miss_count: 0,
        consecutive_misses: 0,
        l2_trip_count: 0,
        daily_budget_spent: Usd::ZERO,
        daily_trade_count: 0,
        daily_success_count: 0,
        daily_miss_count: 0,
        weekly_trade_count: 0,
        hwm_equity: Usd::new(dec!(5000)),
        snapshot_at: Utc::now(),
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

fn try_recover(snapshot: &RiskEngineSnapshot) -> Result<state_store::RecoveredState, OxideError> {
    let config = default_config();
    let clock = utc_clock();
    state_store::recover_state(&config, snapshot, vec![], vec![], &MockMetrics, &clock)
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
fn recover_open_breaker_without_level_fails() {
    let mut snap = default_snapshot();
    snap.breaker_state = BreakerStateName::Open;
    snap.breaker_level = None;
    snap.breaker_reason = Some("test".into());
    snap.cooling_until = Some(Utc::now() + Duration::minutes(5));

    let result = try_recover(&snap);
    assert!(result.is_err(), "Open without level should fail");
}

#[test]
fn recover_open_breaker_without_reason_fails() {
    let mut snap = default_snapshot();
    snap.breaker_state = BreakerStateName::Open;
    snap.breaker_level = Some(CircuitBreakerLevel::Session);
    snap.breaker_reason = None;
    snap.cooling_until = Some(Utc::now() + Duration::minutes(5));

    let result = try_recover(&snap);
    assert!(result.is_err(), "Open without reason should fail");
}

#[test]
fn recover_open_breaker_without_cooling_until_fails() {
    let mut snap = default_snapshot();
    snap.breaker_state = BreakerStateName::Open;
    snap.breaker_level = Some(CircuitBreakerLevel::Session);
    snap.breaker_reason = Some("test".into());
    snap.cooling_until = None;

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
    snap.daily_loss = Usd::new(dec!(-10));

    let result = try_recover(&snap);
    assert!(result.is_err(), "negative daily loss should fail");
}

#[test]
fn recover_negative_weekly_loss_fails() {
    let mut snap = default_snapshot();
    snap.weekly_loss = Usd::new(dec!(-5));

    let result = try_recover(&snap);
    assert!(result.is_err(), "negative weekly loss should fail");
}

#[test]
fn recover_negative_hourly_loss_fails() {
    let mut snap = default_snapshot();
    snap.hourly_loss = Usd::new(dec!(-1));

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
