//! `TieredCache` integration tests (requires Docker for Redis).

use oxide_arb_models::{
    config::RedisConfig,
    enums::common::MarketCategory,
    types::{EventId, MarketId},
};
use oxide_arb_storage::cache::{CacheBackend, CacheKey, MokaBackend, RedisBackend, TieredCache};

fn test_redis_config(port: u16) -> RedisConfig {
    RedisConfig {
        url: format!("redis://localhost:{port}"),
        pool_size: 5,
        timeout_ms: 5000,
        key_prefix: "tiered-test:".into(),
    }
}

async fn setup_redis() -> (
    u16,
    testcontainers::ContainerAsync<testcontainers_modules::redis::Redis>,
) {
    use testcontainers::runners::AsyncRunner;
    let container = testcontainers_modules::redis::Redis::default()
        .start()
        .await
        .expect("Redis container");
    let port = container.get_host_port_ipv4(6379).await.expect("port");
    (port, container)
}

#[derive(bitcode::Encode, bitcode::Decode, Debug, PartialEq, Eq, Clone)]
struct CachedMarketStub {
    market_id: String,
    question: String,
}

#[tokio::test]
#[ignore = "requires Docker"]
async fn tiered_l2_hit_backfills_l1() {
    let (port, _container) = setup_redis().await;
    let redis_cfg = test_redis_config(port);
    let key = CacheKey::MarketInfo {
        market_id: MarketId::new("0xbackfill"),
    };
    let value = CachedMarketStub {
        market_id: "0xbackfill".into(),
        question: "Will backfill work?".into(),
    };

    let writer = TieredCache::new(
        MokaBackend::new(100),
        RedisBackend::new(&redis_cfg).await.unwrap(),
    );
    writer.set(&key, &value).await.unwrap();

    let reader = TieredCache::new(
        MokaBackend::new(100),
        RedisBackend::new(&redis_cfg).await.unwrap(),
    );

    let first: Option<CachedMarketStub> = reader.get(&key).await.unwrap();
    assert_eq!(first.as_ref(), Some(&value));

    // Remove L2 entry; L1 on `reader` should still serve the backfilled value.
    let l2_only = RedisBackend::new(&redis_cfg).await.unwrap();
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

#[tokio::test]
#[ignore = "requires Docker"]
async fn tiered_both_miss_returns_none() {
    let (port, _container) = setup_redis().await;
    let redis_cfg = test_redis_config(port);
    let key = CacheKey::EventInfo {
        event_id: EventId::new("evt-missing"),
    };

    let cache = TieredCache::new(
        MokaBackend::new(100),
        RedisBackend::new(&redis_cfg).await.unwrap(),
    );

    let missing: Option<CachedMarketStub> = cache.get(&key).await.unwrap();
    assert!(missing.is_none());
}

#[tokio::test]
#[ignore = "requires Docker"]
async fn tiered_set_populates_both_levels() {
    let (port, _container) = setup_redis().await;
    let redis_cfg = test_redis_config(port);
    let key = CacheKey::FeeParams {
        category: MarketCategory::Sports,
    };
    let value = CachedMarketStub {
        market_id: "fee".into(),
        question: "params".into(),
    };

    let cache = TieredCache::new(
        MokaBackend::new(100),
        RedisBackend::new(&redis_cfg).await.unwrap(),
    );
    cache.set(&key, &value).await.unwrap();

    let l2 = RedisBackend::new(&redis_cfg).await.unwrap();
    let raw = l2.get(&key.as_str()).await.unwrap();
    assert!(
        raw.is_some(),
        "L2 should contain bitcode-encoded value after set"
    );
}
