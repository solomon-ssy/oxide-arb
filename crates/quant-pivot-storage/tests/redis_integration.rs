//! Redis cache backend integration tests (requires Docker).

use quant_pivot_models::config::RedisConfig;
use quant_pivot_storage::cache::{
    CacheBackend, RedisBackend, connect_pool, count_preproduction_namespace,
    unlink_preproduction_namespace,
};
use std::{
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};
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
        .set("k1", b"v1", Duration::from_mins(1))
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
        .set("del_me", b"data", Duration::from_mins(1))
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
            Duration::from_mins(1),
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

#[tokio::test]
#[ignore = "requires Docker"]
async fn preproduction_cleanup_is_namespace_exact() {
    let container = testcontainers_modules::redis::Redis::default()
        .with_tag("7-alpine")
        .start()
        .await
        .expect("Redis container");
    let port = container.get_host_port_ipv4(6379).await.expect("port");
    let config = RedisConfig {
        host: "127.0.0.1".to_owned(),
        port,
        database: 0,
        key_prefix: "qp:".to_owned(),
        ..RedisConfig::default()
    };
    let pool = connect_pool(&config).await.expect("connect reset Redis");
    let mut connection = pool.get().await.expect("Redis connection");
    for key in ["qp:first", "qp:second", "outside:preserved"] {
        redis::cmd("SET")
            .arg(key)
            .arg("value")
            .query_async::<()>(&mut connection)
            .await
            .expect("seed reset key");
    }
    drop(connection);

    assert_eq!(
        count_preproduction_namespace(&config)
            .await
            .expect("count reset namespace"),
        2
    );
    assert_eq!(
        unlink_preproduction_namespace(&config)
            .await
            .expect("unlink reset namespace"),
        2
    );
    assert_eq!(
        count_preproduction_namespace(&config)
            .await
            .expect("recount reset namespace"),
        0
    );
    let mut connection = pool.get().await.expect("Redis verification connection");
    let preserved = redis::cmd("GET")
        .arg("outside:preserved")
        .query_async::<Option<String>>(&mut connection)
        .await
        .expect("read preserved key");
    assert_eq!(preserved.as_deref(), Some("value"));
}

#[tokio::test]
#[ignore = "requires Docker"]
async fn preproduction_cleanup_fails_closed_with_a_concurrent_writer() {
    let container = testcontainers_modules::redis::Redis::default()
        .with_tag("7-alpine")
        .start()
        .await
        .expect("Redis container");
    let port = container.get_host_port_ipv4(6379).await.expect("port");
    let config = RedisConfig {
        host: "127.0.0.1".to_owned(),
        port,
        database: 0,
        key_prefix: "qp:".to_owned(),
        ..RedisConfig::default()
    };
    let pool = connect_pool(&config).await.expect("connect reset Redis");
    let keep_writing = Arc::new(AtomicBool::new(true));
    let writer_flag = Arc::clone(&keep_writing);
    let writer_pool = pool.clone();
    let writer = tokio::spawn(async move {
        let mut connection = writer_pool.get().await.expect("writer connection");
        while writer_flag.load(Ordering::Acquire) {
            let mut pipeline = redis::pipe();
            for index in 0..256 {
                pipeline
                    .cmd("SET")
                    .arg(format!("qp:concurrent:{index}"))
                    .arg("value")
                    .ignore();
            }
            pipeline
                .query_async::<()>(&mut connection)
                .await
                .expect("write concurrent reset keys");
        }
    });
    while count_preproduction_namespace(&config)
        .await
        .expect("wait for concurrent keys")
        < 256
    {
        tokio::task::yield_now().await;
    }

    let result = unlink_preproduction_namespace(&config).await;
    keep_writing.store(false, Ordering::Release);
    writer.await.expect("join concurrent writer");
    assert!(
        result.is_err(),
        "a live writer must prevent reset from claiming an empty namespace"
    );
    unlink_preproduction_namespace(&config)
        .await
        .expect("clean namespace after writer stops");
    assert_eq!(
        count_preproduction_namespace(&config)
            .await
            .expect("verify namespace after writer stops"),
        0
    );
}
