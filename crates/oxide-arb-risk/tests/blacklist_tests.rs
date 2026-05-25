//! Blacklist manager tests.
//!
//! Validates TTL eviction, scope ordering, auto-blacklist, and GC.

use oxide_arb_models::config::RiskConfig;
use oxide_arb_models::enums::blacklist::BlacklistCheckResult;
use oxide_arb_models::enums::risk::{BlacklistReason, BlacklistScope};
use oxide_arb_models::types::MarketId;
use oxide_arb_risk::blacklist::BlacklistManager;
use oxide_arb_risk::clock::utc_clock;
use std::time::Duration;

fn test_config() -> RiskConfig {
    RiskConfig {
        market_miss_blacklist_count: 3,
        market_miss_blacklist_duration_secs: 3600,
        permanent_blacklist_markets: vec![],
        permanent_blacklist_tokens: vec![],
        ..RiskConfig::default()
    }
}

// ── Clear market passes ────────────────────────────────────────────────────

#[test]
fn clear_market_passes_check() {
    let config = test_config();
    let mgr = BlacklistManager::new(&config, utc_clock());
    let market_id = MarketId::new("0xclear_market");

    let result = mgr.check(&market_id, BlacklistScope::TradingPath);
    assert!(matches!(result, BlacklistCheckResult::Clear));
}

// ── Blacklisted market is blocked ──────────────────────────────────────────

#[test]
fn blacklisted_market_is_blocked() {
    let config = test_config();
    let mgr = BlacklistManager::new(&config, utc_clock());
    let market_id = MarketId::new("0xblocked_market");

    mgr.add_temporary(
        market_id.clone(),
        None,
        BlacklistScope::Full,
        BlacklistReason::ConsecutiveFokFailures,
        Duration::from_secs(3600),
        3,
    );

    let result = mgr.check(&market_id, BlacklistScope::TradingPath);
    assert!(
        matches!(result, BlacklistCheckResult::Blocked { .. }),
        "blacklisted market should be blocked"
    );
}

// ── Expired entry returns clear ────────────────────────────────────────────

#[test]
fn expired_entry_returns_clear() {
    let config = test_config();
    let mgr = BlacklistManager::new(&config, utc_clock());
    let market_id = MarketId::new("0xexpired_market");

    // Add with very short TTL
    mgr.add_temporary(
        market_id.clone(),
        None,
        BlacklistScope::TradingPath,
        BlacklistReason::ConsecutiveFokFailures,
        Duration::from_millis(1),
        3,
    );

    // Wait for expiry
    std::thread::sleep(std::time::Duration::from_millis(10));

    let result = mgr.check(&market_id, BlacklistScope::TradingPath);
    assert!(
        matches!(result, BlacklistCheckResult::Clear),
        "expired entry should be lazily evicted and return Clear"
    );
}

// ── Permanent entry never expires ──────────────────────────────────────────

#[test]
fn permanent_entry_never_expires() {
    let config = test_config();
    let mgr = BlacklistManager::new(&config, utc_clock());
    let market_id = MarketId::new("0xperm_market");

    mgr.add_permanent(market_id.clone(), BlacklistReason::Manual);

    // Even after "time passes" the permanent entry stays
    let result = mgr.check(&market_id, BlacklistScope::TradingPath);
    assert!(matches!(result, BlacklistCheckResult::Blocked { .. }));
}

// ── GC removes expired but keeps permanent ─────────────────────────────────

#[test]
fn gc_removes_expired_keeps_permanent() {
    let config = test_config();
    let mgr = BlacklistManager::new(&config, utc_clock());

    let perm_market = MarketId::new("0xperm");
    let temp_market = MarketId::new("0xtemp");

    mgr.add_permanent(perm_market.clone(), BlacklistReason::Manual);
    mgr.add_temporary(
        temp_market.clone(),
        None,
        BlacklistScope::TradingPath,
        BlacklistReason::ConsecutiveFokFailures,
        Duration::from_millis(1),
        2,
    );

    std::thread::sleep(std::time::Duration::from_millis(10));

    let evicted = mgr.gc();
    assert_eq!(evicted, 1, "should evict 1 expired entry");

    // Permanent remains
    assert!(matches!(
        mgr.check(&perm_market, BlacklistScope::TradingPath),
        BlacklistCheckResult::Blocked { .. }
    ));

    // Temp was GC'd
    assert!(matches!(
        mgr.check(&temp_market, BlacklistScope::TradingPath),
        BlacklistCheckResult::Clear
    ));
}

// ── Scope ordering ─────────────────────────────────────────────────────────

#[test]
fn scope_ordering_lower_scope_does_not_block_higher_required() {
    let config = test_config();
    let mgr = BlacklistManager::new(&config, utc_clock());
    let market_id = MarketId::new("0xscope_test");

    // Blacklist at DataPath scope only
    mgr.add_temporary(
        market_id.clone(),
        None,
        BlacklistScope::DataPath,
        BlacklistReason::ConsecutiveFokFailures,
        Duration::from_secs(3600),
        2,
    );

    // Checking at TradingPath should NOT be blocked (DataPath < TradingPath)
    let result = mgr.check(&market_id, BlacklistScope::TradingPath);
    assert!(
        matches!(result, BlacklistCheckResult::Clear),
        "DataPath scope should not block TradingPath check"
    );

    // But checking at DataPath should be blocked
    let result = mgr.check(&market_id, BlacklistScope::DataPath);
    assert!(matches!(result, BlacklistCheckResult::Blocked { .. }));
}

// ── Auto-blacklist at threshold ────────────────────────────────────────────

#[test]
fn auto_blacklist_at_threshold() {
    let config = RiskConfig {
        market_miss_blacklist_count: 3,
        market_miss_blacklist_duration_secs: 3600,
        ..test_config()
    };
    let mgr = BlacklistManager::new(&config, utc_clock());
    let market_id = MarketId::new("0xauto_bl");

    // Below threshold — no blacklist
    let result = mgr.maybe_auto_blacklist(&market_id, 2);
    assert!(result.is_none());

    // At threshold — should blacklist
    let result = mgr.maybe_auto_blacklist(&market_id, 3);
    assert!(result.is_some(), "should auto-blacklist at threshold");

    // Verify it's actually blacklisted now
    let check = mgr.check(&market_id, BlacklistScope::TradingPath);
    assert!(matches!(check, BlacklistCheckResult::Blocked { .. }));
}

// ── Scope upgrade on re-blacklist ──────────────────────────────────────────

#[test]
fn scope_upgrade_on_re_blacklist() {
    let config = test_config();
    let mgr = BlacklistManager::new(&config, utc_clock());
    let market_id = MarketId::new("0xupgrade");

    // First blacklist at TradingPath
    mgr.add_temporary(
        market_id.clone(),
        None,
        BlacklistScope::TradingPath,
        BlacklistReason::ConsecutiveFokFailures,
        Duration::from_secs(3600),
        2,
    );

    // Re-blacklist at Full scope — should upgrade
    mgr.add_temporary(
        market_id.clone(),
        None,
        BlacklistScope::Full,
        BlacklistReason::ConsecutiveFokFailures,
        Duration::from_secs(7200),
        4,
    );

    // Check at Full scope should be blocked
    let result = mgr.check(&market_id, BlacklistScope::Full);
    assert!(
        matches!(
            result,
            BlacklistCheckResult::Blocked {
                scope: BlacklistScope::Full,
                ..
            }
        ),
        "scope should have been upgraded to Full"
    );
}

// ── Permanent blacklist from config ────────────────────────────────────────

#[test]
fn permanent_blacklist_from_config_is_loaded() {
    let config = RiskConfig {
        permanent_blacklist_markets: vec![
            "0xperm_from_config_1".to_string(),
            "0xperm_from_config_2".to_string(),
        ],
        ..test_config()
    };
    let mgr = BlacklistManager::new(&config, utc_clock());

    let m1 = MarketId::new("0xperm_from_config_1");
    let m2 = MarketId::new("0xperm_from_config_2");
    let m3 = MarketId::new("0xnot_blacklisted");

    assert!(matches!(
        mgr.check(&m1, BlacklistScope::Full),
        BlacklistCheckResult::Blocked { .. }
    ));
    assert!(matches!(
        mgr.check(&m2, BlacklistScope::Full),
        BlacklistCheckResult::Blocked { .. }
    ));
    assert!(matches!(
        mgr.check(&m3, BlacklistScope::Full),
        BlacklistCheckResult::Clear
    ));
}
