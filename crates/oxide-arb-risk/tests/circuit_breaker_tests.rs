//! Circuit breaker FSM tests.
//!
//! Validates all edges of the 5-state FSM (`Closed`, `Open`, `HalfOpen`,
//! `Recovered`, `Halted`) plus L2 exponential cooldown, level overwrite
//! semantics, Halted manual-ack semantics, and fail-closed snapshot recovery.

use chrono::{Duration, Utc};
use oxide_arb_models::{
    domain::risk::RiskEngineState,
    enums::risk::{BreakerStateName, CircuitBreakerLevel},
    runtime_config::CircuitBreakerConfig,
    types::Usd,
};
use oxide_arb_risk::{circuit_breaker::CircuitBreaker, clock::utc_clock, types::BreakerState};
use rust_decimal_macros::dec;
use std::thread::sleep;
use std::time::Duration as StdTimeDuration;

const fn test_config() -> CircuitBreakerConfig {
    CircuitBreakerConfig {
        l1_cooldown_secs: 60,
        l2_cooldown_secs: 900,
        l3_cooldown_secs: 3600,
        l4_cooldown_secs: 7200,
        half_open_probes: 2,
        recovery_observation_secs: 300,
        max_cooldown_secs: 14400,
    }
}

fn base_snapshot() -> RiskEngineState {
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
        total_realized_pnl: Usd::ZERO,
        last_emergency_at: None,
        last_emergency_reason: None,
        snapshot_at: now,
    }
}

// ── Edge 1: Closed → Open ───────────────────────────────────────────────────

#[test]
fn trip_from_closed_transitions_to_open() {
    let mut cb = CircuitBreaker::new(test_config(), utc_clock());
    assert!(matches!(cb.state(), BreakerState::Closed));
    assert!(cb.allows_trading());

    cb.trip(CircuitBreakerLevel::Trade, "test reason".into());

    assert!(matches!(cb.state(), BreakerState::Open { .. }));
    assert!(!cb.allows_trading());
    if let BreakerState::Open { level, reason, .. } = cb.state() {
        assert_eq!(*level, CircuitBreakerLevel::Trade);
        assert_eq!(reason, "test reason");
    }
}

// ── Edge 2: Open → HalfOpen (cooldown expires) ─────────────────────────────

#[test]
fn tick_past_cooldown_transitions_open_to_half_open() {
    let config = CircuitBreakerConfig {
        l1_cooldown_secs: 0, // immediate expiry for test
        ..test_config()
    };
    let mut cb = CircuitBreaker::new(config, utc_clock());
    cb.trip(CircuitBreakerLevel::Trade, "test".into());
    assert!(!cb.allows_trading());

    sleep(StdTimeDuration::from_millis(10));
    let transitioned = cb.tick();

    assert!(transitioned);
    assert!(matches!(cb.state(), BreakerState::HalfOpen { .. }));
    assert!(cb.allows_trading());
    assert!(cb.is_probe_mode());
}

// ── Edge 3: HalfOpen → Recovered (probes pass) ─────────────────────────────

#[test]
fn successful_probes_transition_half_open_to_recovered() {
    let config = CircuitBreakerConfig {
        l1_cooldown_secs: 0,
        half_open_probes: 2,
        ..test_config()
    };
    let mut cb = CircuitBreaker::new(config, utc_clock());
    cb.trip(CircuitBreakerLevel::Trade, "test".into());
    sleep(StdTimeDuration::from_millis(10));
    let _ = cb.tick();
    assert!(cb.is_probe_mode());

    cb.on_trade_result(true);
    assert!(cb.is_probe_mode()); // still HalfOpen, need 2 probes

    cb.on_trade_result(true);
    assert!(matches!(cb.state(), BreakerState::Recovered { .. }));
    assert!(!cb.is_probe_mode());
}

// ── Edge 4: HalfOpen → Open (probe fails) ──────────────────────────────────

#[test]
fn failed_probe_transitions_half_open_back_to_open() {
    let config = CircuitBreakerConfig {
        l1_cooldown_secs: 0,
        half_open_probes: 2,
        ..test_config()
    };
    let mut cb = CircuitBreaker::new(config, utc_clock());
    cb.trip(CircuitBreakerLevel::Trade, "test".into());
    sleep(StdTimeDuration::from_millis(10));
    let _ = cb.tick();
    assert!(cb.is_probe_mode());

    cb.on_trade_result(false);

    assert!(matches!(cb.state(), BreakerState::Open { .. }));
    assert!(!cb.allows_trading());
}

// ── Edge 5: Recovered → Closed (observation period expires) ────────────────

#[test]
fn tick_past_observation_period_transitions_recovered_to_closed() {
    let config = CircuitBreakerConfig {
        l1_cooldown_secs: 0, // Trade level uses this
        l2_cooldown_secs: 0, // Session level uses this
        half_open_probes: 1,
        recovery_observation_secs: 0, // immediate for test
        ..test_config()
    };
    let mut cb = CircuitBreaker::new(config, utc_clock());
    cb.trip(CircuitBreakerLevel::Trade, "test".into());

    sleep(StdTimeDuration::from_millis(10));
    let _ = cb.tick(); // Open → HalfOpen
    cb.on_trade_result(true); // HalfOpen → Recovered

    sleep(StdTimeDuration::from_millis(10));
    let transitioned = cb.tick(); // Recovered → Closed

    assert!(transitioned);
    assert!(matches!(cb.state(), BreakerState::Closed));
}

// ── Edge 6: reset() → Closed (operator intervention) ───────────────────────

#[test]
fn reset_from_any_state_returns_to_closed() {
    let mut cb = CircuitBreaker::new(test_config(), utc_clock());
    cb.trip(CircuitBreakerLevel::System, "emergency".into());
    assert!(matches!(cb.state(), BreakerState::Open { .. }));

    cb.reset("operator override");

    assert!(matches!(cb.state(), BreakerState::Closed));
    assert!(cb.allows_trading());
    assert_eq!(cb.l2_trip_count(), 0);
}

// ── L2 exponential cooldown ────────────────────────────────────────────────

#[test]
fn l2_exponential_cooldown_increases() {
    let config = test_config(); // l2_cooldown_secs = 900
    let mut cb = CircuitBreaker::new(config, utc_clock());

    // 1st trip: count=0 → cooldown = 900 * 2^0 = 900, then count→1
    cb.trip(CircuitBreakerLevel::Session, "miss 1".into());
    assert_eq!(cb.l2_trip_count(), 1);
    if let BreakerState::Open {
        cooldown_until,
        tripped_at,
        ..
    } = cb.state()
    {
        let secs = (*cooldown_until - *tripped_at).num_seconds();
        assert_eq!(secs, 900);
    } else {
        panic!("expected Open state");
    }

    // 2nd trip: count=1 → cooldown = 900 * 2^0 = 900, then count→2
    cb.trip(CircuitBreakerLevel::Session, "miss 2".into());
    assert_eq!(cb.l2_trip_count(), 2);
    if let BreakerState::Open {
        cooldown_until,
        tripped_at,
        ..
    } = cb.state()
    {
        let secs = (*cooldown_until - *tripped_at).num_seconds();
        assert_eq!(secs, 900);
    } else {
        panic!("expected Open state");
    }

    // 3rd trip: count=2 → cooldown = 900 * 2^1 = 1800, then count→3
    cb.trip(CircuitBreakerLevel::Session, "miss 3".into());
    assert_eq!(cb.l2_trip_count(), 3);
    if let BreakerState::Open {
        cooldown_until,
        tripped_at,
        ..
    } = cb.state()
    {
        let secs = (*cooldown_until - *tripped_at).num_seconds();
        assert_eq!(secs, 1800);
    } else {
        panic!("expected Open state");
    }
}

#[test]
fn l2_cooldown_capped_at_max() {
    let config = CircuitBreakerConfig {
        l2_cooldown_secs: 10000,
        max_cooldown_secs: 14400,
        ..test_config()
    };
    let mut cb = CircuitBreaker::new(config, utc_clock());

    cb.trip(CircuitBreakerLevel::Session, "t1".into()); // count 0→1
    cb.trip(CircuitBreakerLevel::Session, "t2".into()); // count 1→2
    cb.trip(CircuitBreakerLevel::Session, "t3".into()); // count 2→3
    cb.trip(CircuitBreakerLevel::Session, "t4".into()); // count 3→4

    cb.trip(CircuitBreakerLevel::Session, "t5".into()); // count 4→5

    if let BreakerState::Open {
        cooldown_until,
        tripped_at,
        ..
    } = cb.state()
    {
        let secs = (*cooldown_until - *tripped_at).num_seconds();
        assert_eq!(
            secs, 14400,
            "cooldown should be capped at max_cooldown_secs"
        );
    } else {
        panic!("expected Open state");
    }
}

// ── Level overwrite semantics ──────────────────────────────────────────────

#[test]
fn higher_level_overwrites_lower() {
    let mut cb = CircuitBreaker::new(test_config(), utc_clock());
    cb.trip(CircuitBreakerLevel::Trade, "L1 trip".into());

    if let BreakerState::Open { level, .. } = cb.state() {
        assert_eq!(*level, CircuitBreakerLevel::Trade);
    }

    cb.trip(CircuitBreakerLevel::Daily, "L3 override".into());

    if let BreakerState::Open { level, reason, .. } = cb.state() {
        assert_eq!(*level, CircuitBreakerLevel::Daily);
        assert_eq!(reason, "L3 override");
    } else {
        panic!("expected Open state");
    }
}

#[test]
fn lower_level_does_not_overwrite_higher() {
    let mut cb = CircuitBreaker::new(test_config(), utc_clock());
    cb.trip(CircuitBreakerLevel::Daily, "L3 trip".into());

    cb.trip(CircuitBreakerLevel::Trade, "L1 attempt".into());

    if let BreakerState::Open { level, reason, .. } = cb.state() {
        assert_eq!(*level, CircuitBreakerLevel::Daily);
        assert_eq!(reason, "L3 trip");
    } else {
        panic!("expected Open state");
    }
}

// ── Snapshot recovery: fail-closed ─────────────────────────────────────────

#[test]
fn from_snapshot_open_missing_level_returns_error() {
    let config = test_config();
    let snapshot = RiskEngineState {
        breaker_state: BreakerStateName::Open,
        breaker_level: None, // missing!
        halt_reason: Some("test".into()),
        cooldown_until: Some(Utc::now() + Duration::seconds(300)),
        ..base_snapshot()
    };

    let result = CircuitBreaker::from_snapshot(config, utc_clock(), &snapshot);
    assert!(
        result.is_err(),
        "should fail-closed when Open missing level"
    );
}

#[test]
fn from_snapshot_open_missing_reason_returns_error() {
    let config = test_config();
    let snapshot = RiskEngineState {
        breaker_state: BreakerStateName::Open,
        breaker_level: Some(CircuitBreakerLevel::Session),
        halt_reason: None, // missing!
        cooldown_until: Some(Utc::now() + Duration::seconds(300)),
        ..base_snapshot()
    };

    let result = CircuitBreaker::from_snapshot(config, utc_clock(), &snapshot);
    assert!(
        result.is_err(),
        "should fail-closed when Open missing reason"
    );
}

#[test]
fn from_snapshot_open_missing_cooldown_until_returns_error() {
    let config = test_config();
    let snapshot = RiskEngineState {
        breaker_state: BreakerStateName::Open,
        breaker_level: Some(CircuitBreakerLevel::Session),
        halt_reason: Some("test".into()),
        cooldown_until: None, // missing!
        ..base_snapshot()
    };

    let result = CircuitBreaker::from_snapshot(config, utc_clock(), &snapshot);
    assert!(
        result.is_err(),
        "should fail-closed when Open missing cooldown_until"
    );
}

#[test]
fn from_snapshot_closed_restores_successfully() {
    let config = test_config();
    let snapshot = base_snapshot();

    let cb = CircuitBreaker::from_snapshot(config, utc_clock(), &snapshot).unwrap();
    assert!(matches!(cb.state(), BreakerState::Closed));
    assert!(cb.allows_trading());
}

// ── Halted state: L3/L4 manual ack only ─────────────────────────────────

#[test]
fn fsm_halt_system_blocks_allows_trading() {
    let mut cb = CircuitBreaker::new(test_config(), utc_clock());
    cb.halt(CircuitBreakerLevel::System, "balance critical".into());

    assert!(matches!(cb.state(), BreakerState::Halted { .. }));
    assert!(!cb.allows_trading());
    assert!(!cb.is_probe_mode());
    assert!(cb.state().is_halted());
}

#[test]
fn fsm_halted_never_auto_transitions_on_tick() {
    let config = CircuitBreakerConfig {
        l3_cooldown_secs: 0,
        l4_cooldown_secs: 0,
        ..test_config()
    };
    let mut cb = CircuitBreaker::new(config, utc_clock());
    cb.halt(CircuitBreakerLevel::Daily, "daily loss cap".into());

    sleep(StdTimeDuration::from_millis(10));
    let transitioned = cb.tick();

    assert!(!transitioned, "Halted must never auto-transition via tick");
    assert!(matches!(cb.state(), BreakerState::Halted { .. }));
}

#[test]
fn fsm_halted_daily_requires_acknowledge_and_resume() {
    let mut cb = CircuitBreaker::new(test_config(), utc_clock());
    cb.halt(CircuitBreakerLevel::Daily, "daily loss cap breached".into());
    assert!(!cb.allows_trading());

    let resumed = cb.acknowledge_and_resume("operator reviewed");
    assert!(resumed);
    assert!(matches!(cb.state(), BreakerState::Closed));
    assert!(cb.allows_trading());
    assert_eq!(cb.l2_trip_count(), 0);
}

#[test]
fn fsm_acknowledge_and_resume_noop_when_not_halted() {
    let mut cb = CircuitBreaker::new(test_config(), utc_clock());
    cb.trip(CircuitBreakerLevel::Session, "session trip".into());

    let resumed = cb.acknowledge_and_resume("attempted");
    assert!(!resumed, "acknowledge should be no-op when not Halted");
    assert!(matches!(cb.state(), BreakerState::Open { .. }));
}

#[test]
fn fsm_session_trip_does_not_use_halt() {
    let mut cb = CircuitBreaker::new(test_config(), utc_clock());
    cb.trip(CircuitBreakerLevel::Session, "session miss".into());

    assert!(
        matches!(cb.state(), BreakerState::Open { .. }),
        "Session trip should produce Open, not Halted"
    );
}

#[test]
fn fsm_halt_does_not_downgrade_existing_halted() {
    let mut cb = CircuitBreaker::new(test_config(), utc_clock());
    cb.halt(CircuitBreakerLevel::System, "system emergency".into());

    cb.halt(CircuitBreakerLevel::Daily, "daily cap".into());

    if let BreakerState::Halted { level, reason, .. } = cb.state() {
        assert_eq!(
            *level,
            CircuitBreakerLevel::System,
            "L4 must not be downgraded to L3"
        );
        assert_eq!(reason, "system emergency");
    } else {
        panic!("expected Halted state");
    }
}

#[test]
fn fsm_trip_is_noop_when_halted() {
    let mut cb = CircuitBreaker::new(test_config(), utc_clock());
    cb.halt(CircuitBreakerLevel::System, "system halt".into());

    cb.trip(CircuitBreakerLevel::Session, "should be ignored".into());

    assert!(
        matches!(cb.state(), BreakerState::Halted { .. }),
        "trip() must not override Halted state"
    );
}

#[test]
fn fsm_higher_session_trip_refreshes_cooldown() {
    let mut cb = CircuitBreaker::new(test_config(), utc_clock());
    cb.trip(CircuitBreakerLevel::Trade, "trade issue".into());

    let first_cooldown = if let BreakerState::Open { cooldown_until, .. } = cb.state() {
        *cooldown_until
    } else {
        panic!("expected Open");
    };

    sleep(StdTimeDuration::from_millis(5));
    cb.trip(CircuitBreakerLevel::Session, "session issue".into());

    if let BreakerState::Open {
        cooldown_until,
        level,
        ..
    } = cb.state()
    {
        assert_eq!(*level, CircuitBreakerLevel::Session);
        assert!(
            *cooldown_until > first_cooldown,
            "cooldown should be refreshed"
        );
    } else {
        panic!("expected Open");
    }
}

// ── Snapshot recovery: Halted ───────────────────────────────────────────

#[test]
fn from_snapshot_halted_restores_successfully() {
    let config = test_config();
    let snapshot = RiskEngineState {
        breaker_state: BreakerStateName::Halted,
        breaker_level: Some(CircuitBreakerLevel::Daily),
        is_halted: true,
        halt_reason: Some("daily loss cap".into()),
        ..base_snapshot()
    };

    let cb = CircuitBreaker::from_snapshot(config, utc_clock(), &snapshot).unwrap();
    assert!(matches!(cb.state(), BreakerState::Halted { .. }));
    assert!(!cb.allows_trading());
}

#[test]
fn from_snapshot_halted_missing_level_returns_error() {
    let config = test_config();
    let snapshot = RiskEngineState {
        breaker_state: BreakerStateName::Halted,
        breaker_level: None,
        is_halted: true,
        halt_reason: Some("test".into()),
        ..base_snapshot()
    };

    let result = CircuitBreaker::from_snapshot(config, utc_clock(), &snapshot);
    assert!(
        result.is_err(),
        "should fail-closed when Halted missing level"
    );
}

#[test]
fn from_snapshot_halted_missing_reason_returns_error() {
    let config = test_config();
    let snapshot = RiskEngineState {
        breaker_state: BreakerStateName::Halted,
        breaker_level: Some(CircuitBreakerLevel::System),
        is_halted: true,
        halt_reason: None,
        ..base_snapshot()
    };

    let result = CircuitBreaker::from_snapshot(config, utc_clock(), &snapshot);
    assert!(
        result.is_err(),
        "should fail-closed when Halted missing reason"
    );
}
