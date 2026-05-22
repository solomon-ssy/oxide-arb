//! Circuit breaker FSM tests.
//!
//! Validates all 6 edges of the 4-state FSM plus L2 exponential cooldown,
//! level overwrite semantics, and fail-closed snapshot recovery.

use chrono::{Duration, Utc};
use oxide_arb_models::config::CircuitBreakerConfig;
use oxide_arb_models::domain::risk::RiskEngineSnapshot;
use oxide_arb_models::enums::risk::{BreakerStateName, CircuitBreakerLevel};
use oxide_arb_models::types::Usd;
use oxide_arb_risk::circuit_breaker::CircuitBreaker;
use oxide_arb_risk::types::BreakerState;
use rust_decimal_macros::dec;

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

fn base_snapshot() -> RiskEngineSnapshot {
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

// ── Edge 1: Closed → Open ───────────────────────────────────────────────────

#[test]
fn trip_from_closed_transitions_to_open() {
    let mut cb = CircuitBreaker::new(test_config());
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
    let mut cb = CircuitBreaker::new(config);
    cb.trip(CircuitBreakerLevel::Trade, "test".into());
    assert!(!cb.allows_trading());

    std::thread::sleep(std::time::Duration::from_millis(10));
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
    let mut cb = CircuitBreaker::new(config);
    cb.trip(CircuitBreakerLevel::Trade, "test".into());
    std::thread::sleep(std::time::Duration::from_millis(10));
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
    let mut cb = CircuitBreaker::new(config);
    cb.trip(CircuitBreakerLevel::Trade, "test".into());
    std::thread::sleep(std::time::Duration::from_millis(10));
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
    let mut cb = CircuitBreaker::new(config);
    cb.trip(CircuitBreakerLevel::Trade, "test".into());

    std::thread::sleep(std::time::Duration::from_millis(10));
    let _ = cb.tick(); // Open → HalfOpen
    cb.on_trade_result(true); // HalfOpen → Recovered

    std::thread::sleep(std::time::Duration::from_millis(10));
    let transitioned = cb.tick(); // Recovered → Closed

    assert!(transitioned);
    assert!(matches!(cb.state(), BreakerState::Closed));
}

// ── Edge 6: reset() → Closed (operator intervention) ───────────────────────

#[test]
fn reset_from_any_state_returns_to_closed() {
    let mut cb = CircuitBreaker::new(test_config());
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
    let mut cb = CircuitBreaker::new(config);

    // cooldown_for_level is evaluated BEFORE l2_trip_count is incremented.
    // Formula: base * 2^(count.saturating_sub(1)), computed BEFORE ++count.

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
    let mut cb = CircuitBreaker::new(config);

    // Trip multiple times in succession to accumulate l2_trip_count.
    // Eventually the exponential would exceed max, but cap should apply.
    cb.trip(CircuitBreakerLevel::Session, "t1".into()); // count 0→1
    cb.trip(CircuitBreakerLevel::Session, "t2".into()); // count 1→2
    cb.trip(CircuitBreakerLevel::Session, "t3".into()); // count 2→3
    cb.trip(CircuitBreakerLevel::Session, "t4".into()); // count 3→4

    // At this point count=4 before computation:
    // cooldown = 10000 * 2^(4-1) = 10000 * 8 = 80000 → capped at 14400
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
    let mut cb = CircuitBreaker::new(test_config());
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
    let mut cb = CircuitBreaker::new(test_config());
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
    let snapshot = RiskEngineSnapshot {
        breaker_state: BreakerStateName::Open,
        breaker_level: None, // missing!
        breaker_reason: Some("test".into()),
        cooling_until: Some(Utc::now() + Duration::seconds(300)),
        ..base_snapshot()
    };

    let result = CircuitBreaker::from_snapshot(config, &snapshot);
    assert!(
        result.is_err(),
        "should fail-closed when Open missing level"
    );
}

#[test]
fn from_snapshot_open_missing_reason_returns_error() {
    let config = test_config();
    let snapshot = RiskEngineSnapshot {
        breaker_state: BreakerStateName::Open,
        breaker_level: Some(CircuitBreakerLevel::Session),
        breaker_reason: None, // missing!
        cooling_until: Some(Utc::now() + Duration::seconds(300)),
        ..base_snapshot()
    };

    let result = CircuitBreaker::from_snapshot(config, &snapshot);
    assert!(
        result.is_err(),
        "should fail-closed when Open missing reason"
    );
}

#[test]
fn from_snapshot_open_missing_cooling_until_returns_error() {
    let config = test_config();
    let snapshot = RiskEngineSnapshot {
        breaker_state: BreakerStateName::Open,
        breaker_level: Some(CircuitBreakerLevel::Session),
        breaker_reason: Some("test".into()),
        cooling_until: None, // missing!
        ..base_snapshot()
    };

    let result = CircuitBreaker::from_snapshot(config, &snapshot);
    assert!(
        result.is_err(),
        "should fail-closed when Open missing cooling_until"
    );
}

#[test]
fn from_snapshot_closed_restores_successfully() {
    let config = test_config();
    let snapshot = base_snapshot();

    let cb = CircuitBreaker::from_snapshot(config, &snapshot).unwrap();
    assert!(matches!(cb.state(), BreakerState::Closed));
    assert!(cb.allows_trading());
}
