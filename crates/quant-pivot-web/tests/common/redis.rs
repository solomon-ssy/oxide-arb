//! Redis testcontainer bring-up for the JWT revocation blacklist.

use quant_pivot_models::config::RedisConfig;
use testcontainers::{ContainerAsync, ImageExt, runners::AsyncRunner};
use testcontainers_modules::redis::Redis;

/// Redis settings tuned for integration tests (larger pool + longer wait).
#[must_use]
pub fn test_redis_config(port: u16) -> RedisConfig {
    RedisConfig {
        host: "127.0.0.1".into(),
        port,
        pool_size: 16,
        timeout_ms: 5_000,
        key_prefix: "oarb:test:".to_owned(),
        ..RedisConfig::default()
    }
}

/// Start Redis and return its host port plus the container guard.
pub async fn setup_redis() -> (u16, ContainerAsync<Redis>) {
    let container = Redis::default()
        .with_tag("7-alpine")
        .start()
        .await
        .expect("start Redis container");
    let port = container
        .get_host_port_ipv4(6379)
        .await
        .expect("Redis host port");
    (port, container)
}
