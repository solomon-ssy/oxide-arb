//! `TieredCache` system contracts backed by Redis.

use quant_pivot_models::{
    config::RedisConfig,
    types::{EventId, MarketId},
};
use quant_pivot_storage::cache::{
    CacheBackend, CacheKey, MokaBackend, RedisBackend, TieredCache, connect_pool,
};
use quant_pivot_system_tests::resources::fresh_redis_config;

fn test_redis_config(scope: &str) -> RedisConfig {
    RedisConfig {
        pool_size: 5,
        timeout_ms: 5000,
        ..fresh_redis_config(scope)
    }
}

async fn setup_redis(scope: &str) -> RedisConfig {
    let config = test_redis_config(scope);
    connect_pool(&config).await.expect("Redis readiness");
    config
}

/// Connect a shared pool and wrap an L2 backend over it (composition-root shape).
async fn redis_backend(config: &RedisConfig) -> RedisBackend {
    let pool = connect_pool(config).await.expect("redis pool");
    RedisBackend::new(pool, &config.key_prefix)
}

#[derive(bitcode::Encode, bitcode::Decode, Debug, PartialEq, Eq, Clone)]
struct CachedMarketStub {
    market_id: String,
    question: String,
}

pub async fn tiered_l2_hit_l1() {
    let redis_cfg = setup_redis("tiered_backfill").await;
    let key = CacheKey::MarketInfo {
        market_id: MarketId::new("0xbackfill"),
    };
    let value = CachedMarketStub {
        market_id: "0xbackfill".into(),
        question: "Will backfill work?".into(),
    };

    let writer = TieredCache::new(MokaBackend::new(100), redis_backend(&redis_cfg).await);
    writer.set(&key, &value).await.unwrap();

    let reader = TieredCache::new(MokaBackend::new(100), redis_backend(&redis_cfg).await);

    let first: Option<CachedMarketStub> = reader.get(&key).await.unwrap();
    assert_eq!(first.as_ref(), Some(&value));

    // Remove L2 entry; L1 on `reader` should still serve the backfilled value.
    let l2_only = redis_backend(&redis_cfg).await;
    l2_only.delete(&key.as_str()).await.unwrap();
    assert!(
        l2_only.get(&key.as_str()).await.unwrap().is_none(),
        "L2 key should be deleted"
    );

    let second: Option<CachedMarketStub> = reader.get(&key).await.unwrap();
    assert_eq!(
        second,
        Some(value),
        "L1 backfill should survive L2 eviction"
    );
}

pub async fn tiered_both_returns_none() {
    let redis_cfg = setup_redis("tiered_miss").await;
    let key = CacheKey::EventInfo {
        event_id: EventId::new("evt-missing"),
    };

    let cache = TieredCache::new(MokaBackend::new(100), redis_backend(&redis_cfg).await);

    let missing: Option<CachedMarketStub> = cache.get(&key).await.unwrap();
    assert!(missing.is_none());
}

pub async fn tiered_set_populates_levels() {
    let redis_cfg = setup_redis("tiered_set").await;
    let key = CacheKey::MarketMetadata {
        market_id: MarketId::new("fee-metadata-market"),
    };
    let value = CachedMarketStub {
        market_id: "fee".into(),
        question: "params".into(),
    };

    let cache = TieredCache::new(MokaBackend::new(100), redis_backend(&redis_cfg).await);
    cache.set(&key, &value).await.unwrap();

    let l2 = redis_backend(&redis_cfg).await;
    let raw = l2.get(&key.as_str()).await.unwrap();
    assert!(
        raw.is_some(),
        "L2 should contain bitcode-encoded value after set"
    );
}
