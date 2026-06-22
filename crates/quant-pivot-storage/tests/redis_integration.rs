//! Redis cache backend integration tests (requires Docker).

use quant_pivot_models::config::RedisConfig;
use quant_pivot_storage::cache::{CacheBackend, RedisBackend, connect_pool};
use std::time::Duration;
use testcontainers::{ImageExt, runners::AsyncRunner};

fn test_redis_config(port: u16) -> RedisConfig {
    RedisConfig {
        host: "127.0.0.1".into(),
        port,
        pool_size: 5,
        timeout_ms: 5000,
        key_prefix: "test:".into(),
        ..RedisConfig::default()
    }
}

async fn setup_redis() -> (
    RedisBackend,
    testcontainers::ContainerAsync<testcontainers_modules::redis::Redis>,
) {
    let container = testcontainers_modules::redis::Redis::default()
        .with_tag("7-alpine")
        .start()
        .await
        .expect("Redis container");
    let port = container.get_host_port_ipv4(6379).await.expect("port");
    let config = test_redis_config(port);
    let pool = connect_pool(&config).await.expect("connect");
    let backend = RedisBackend::new(pool, &config.key_prefix);
    (backend, container)
}

#[tokio::test]
#[ignore = "requires Docker"]
async fn redis_set_get_roundtrip() {
    let (backend, _container) = setup_redis().await;
    backend
        .set("k1", b"v1", Duration::from_secs(60))
        .await
        .unwrap();
    let val = backend.get("k1").await.unwrap();
    assert_eq!(val, Some(b"v1".to_vec()));
}

#[tokio::test]
#[ignore = "requires Docker"]
async fn redis_get_missing_returns_none() {
    let (backend, _container) = setup_redis().await;
    let val = backend.get("nonexistent").await.unwrap();
    assert_eq!(val, None);
}

#[tokio::test]
#[ignore = "requires Docker"]
async fn redis_delete_removes_entry() {
    let (backend, _container) = setup_redis().await;
    backend
        .set("del_me", b"data", Duration::from_secs(60))
        .await
        .unwrap();
    let removed = backend.delete("del_me").await.unwrap();
    assert!(removed);
    let val = backend.get("del_me").await.unwrap();
    assert_eq!(val, None);
}

#[tokio::test]
#[ignore = "requires Docker"]
async fn redis_mget_mset() {
    let (backend, _container) = setup_redis().await;
    backend
        .mset(
            &[("a", b"1"), ("b", b"2"), ("c", b"3")],
            Duration::from_secs(60),
        )
        .await
        .unwrap();

    let results = backend.mget(&["a", "missing", "c"]).await.unwrap();
    assert_eq!(results[0], Some(b"1".to_vec()));
    assert_eq!(results[1], None);
    assert_eq!(results[2], Some(b"3".to_vec()));
}

#[tokio::test]
#[ignore = "requires Docker"]
async fn redis_health_check() {
    let (backend, _container) = setup_redis().await;
    backend.health_check().await.expect("health check");
}
