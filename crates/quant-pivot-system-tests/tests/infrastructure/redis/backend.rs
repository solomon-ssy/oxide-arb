//! Redis cache backend system contracts.

use std::{
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

use quant_pivot_models::config::RedisConfig;
use quant_pivot_storage::cache::{
    CacheBackend, RedisBackend, connect_pool, count_preproduction_namespace,
    unlink_preproduction_namespace,
};
use quant_pivot_system_tests::resources::fresh_redis_config;
fn test_redis_config(scope: &str) -> RedisConfig {
    RedisConfig {
        pool_size: 5,
        timeout_ms: 5000,
        ..fresh_redis_config(scope)
    }
}

async fn setup_redis() -> (RedisBackend, ()) {
    let config = test_redis_config("redis_backend");
    let pool = connect_pool(&config).await.expect("connect");
    let backend = RedisBackend::new(pool, &config.key_prefix);
    (backend, ())
}

pub async fn redis_set_get_roundtrip() {
    let (backend, _container) = setup_redis().await;
    backend
        .set("k1", b"v1", Duration::from_mins(1))
        .await
        .unwrap();
    let val = backend.get("k1").await.unwrap();
    assert_eq!(val, Some(b"v1".to_vec()));
}

pub async fn redis_get_missing_returns_none() {
    let (backend, _container) = setup_redis().await;
    let val = backend.get("nonexistent").await.unwrap();
    assert_eq!(val, None);
}

pub async fn redis_delete_removes_entry() {
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

pub async fn redis_mget_mset() {
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

pub async fn redis_health_check() {
    let (backend, _container) = setup_redis().await;
    backend.health_check().await.expect("health check");
}

pub async fn preproduction_cleanup_is_namespace_exact() {
    let mut config = fresh_redis_config("redis_cleanup_exact");
    config.database = 0;
    "qp:".clone_into(&mut config.key_prefix);
    let pool = connect_pool(&config).await.expect("connect reset Redis");
    let mut connection = pool.get().await.expect("Redis connection");
    let first = format!("{}first", config.key_prefix);
    let second = format!("{}second", config.key_prefix);
    let outside = format!("outside:{}", config.key_prefix);
    for key in [&first, &second, &outside] {
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
        .arg(&outside)
        .query_async::<Option<String>>(&mut connection)
        .await
        .expect("read preserved key");
    assert_eq!(preserved.as_deref(), Some("value"));
}

pub async fn preproduction_cleanup_fails_closed_with_a_concurrent_writer() {
    let mut config = fresh_redis_config("redis_cleanup_concurrent");
    config.database = 0;
    "qp:".clone_into(&mut config.key_prefix);
    let pool = connect_pool(&config).await.expect("connect reset Redis");
    let keep_writing = Arc::new(AtomicBool::new(true));
    let writer_flag = Arc::clone(&keep_writing);
    let writer_pool = pool.clone();
    let writer_prefix = config.key_prefix.clone();
    let writer = tokio::spawn(async move {
        let mut connection = writer_pool.get().await.expect("writer connection");
        while writer_flag.load(Ordering::Acquire) {
            let mut pipeline = redis::pipe();
            for index in 0..256 {
                pipeline
                    .cmd("SET")
                    .arg(format!("{writer_prefix}concurrent:{index}"))
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
