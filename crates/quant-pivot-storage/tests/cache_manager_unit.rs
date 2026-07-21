//! `CacheManager` policy unit tests (no Docker required).
//!
//! Backend failures are produced deterministically with a lazy deadpool pool
//! pointing at an unreachable Redis endpoint: connections are only attempted
//! on first use, so construction succeeds and every L2 operation fails.

use std::collections::HashMap;

use deadpool_redis::{Config, Runtime};
use prometheus::Registry;
use quant_pivot_models::{
    config::{CacheConfig, DomainCacheConfig},
    types::MarketId,
};
use quant_pivot_storage::cache::{CacheKey, CacheManager, MokaBackend, RedisBackend, TieredCache};

#[derive(
    bitcode::Encode,
    bitcode::Decode,
    serde::Serialize,
    serde::Deserialize,
    Debug,
    PartialEq,
    Eq,
    Clone,
)]
struct Stub {
    id: String,
}

fn stub() -> Stub {
    Stub {
        id: "0xstub".into(),
    }
}

fn market_key() -> CacheKey {
    CacheKey::MarketInfo {
        market_id: MarketId::new("0xstub"),
    }
}

/// Manager over an unreachable Redis L2 (lazy pool — no connection at build).
fn unreachable_manager(config: &CacheConfig) -> CacheManager {
    let pool = Config::from_url("redis://127.0.0.1:1")
        .create_pool(Some(Runtime::Tokio1))
        .expect("lazy pool");
    CacheManager::new(
        TieredCache::new(MokaBackend::new(16), RedisBackend::new(pool, "test:")),
        config,
    )
}

fn config(fail_open: bool) -> CacheConfig {
    CacheConfig {
        operation_timeout_ms: 1_000,
        fail_open,
        ..CacheConfig::default()
    }
}

#[tokio::test]
async fn noop_manager_skips_all_operations() {
    let manager = CacheManager::noop();
    assert!(manager.is_noop());

    let missing: Option<Stub> = manager.get(&market_key()).await;
    assert!(missing.is_none());
    manager.set(&market_key(), &stub()).await.expect("noop set");
    manager
        .set_json(&market_key(), &stub())
        .await
        .expect("noop set_json");
    manager.invalidate(&market_key()).await;
    manager
        .register_metrics(&Registry::new())
        .expect("noop registers nothing");
}

#[tokio::test]
async fn disabled_config_builds_noop() {
    let manager = unreachable_manager(&CacheConfig {
        disabled: true,
        ..CacheConfig::default()
    });
    assert!(manager.is_noop());
}

#[tokio::test]
async fn get_is_fail_open_on_backend_error() {
    let manager = unreachable_manager(&config(true));
    let missing: Option<Stub> = manager.get(&market_key()).await;
    assert!(missing.is_none(), "backend error must degrade to a miss");
}

#[tokio::test]
async fn get_json_is_fail_open_on_backend_error() {
    let manager = unreachable_manager(&config(true));
    let missing: Option<Stub> = manager.get_json(&market_key()).await;
    assert!(missing.is_none(), "backend error must degrade to a miss");
}

#[tokio::test]
async fn get_is_fail_open_even_when_writes_fail_closed() {
    // Reads never propagate cache errors regardless of the fail_open policy:
    // callers must always be able to fall through to the source of truth.
    let manager = unreachable_manager(&config(false));
    let missing: Option<Stub> = manager.get(&market_key()).await;
    assert!(missing.is_none());
}

#[tokio::test]
async fn set_fail_open_swallows_backend_error() {
    let manager = unreachable_manager(&config(true));
    manager
        .set(&market_key(), &stub())
        .await
        .expect("fail-open set must swallow backend errors");
    manager
        .set_json(&market_key(), &stub())
        .await
        .expect("fail-open set_json must swallow backend errors");
}

#[tokio::test]
async fn set_fail_closed_propagates_backend_error() {
    let manager = unreachable_manager(&config(false));
    assert!(manager.set(&market_key(), &stub()).await.is_err());
    assert!(manager.set_json(&market_key(), &stub()).await.is_err());
}

#[tokio::test]
async fn disabled_domain_skips_operations_entirely() {
    let mut cfg = config(false);
    cfg.domains = HashMap::from([(
        "market".to_owned(),
        DomainCacheConfig {
            timeout_ms: None,
            fail_open: None,
            disabled: true,
        },
    )]);
    let manager = unreachable_manager(&cfg);

    // Even with fail-closed writes and an unreachable backend, a disabled
    // domain is skipped before any backend call.
    manager
        .set(&market_key(), &stub())
        .await
        .expect("disabled domain set is a no-op");
    let missing: Option<Stub> = manager.get(&market_key()).await;
    assert!(missing.is_none());
}

#[tokio::test]
async fn invalidate_is_fail_open_on_backend_error() {
    let manager = unreachable_manager(&config(false));
    // Must not panic or propagate.
    manager.invalidate(&market_key()).await;
}

#[tokio::test]
async fn register_metrics_registers_cache_counters() {
    let manager = unreachable_manager(&config(true));
    let registry = Registry::new();
    manager
        .register_metrics(&registry)
        .expect("first registration succeeds");
    // Re-registering the same collectors must fail — proves the counters were
    // actually registered (vecs with no recorded label values gather empty).
    assert!(manager.register_metrics(&registry).is_err());
}
