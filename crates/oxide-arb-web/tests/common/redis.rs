//! Redis testcontainer bring-up for the JWT revocation blacklist.

use oxide_arb_models::config::RedisConfig;
use testcontainers::{ContainerAsync, runners::AsyncRunner};
use testcontainers_modules::redis::Redis;

/// Redis settings tuned for integration tests (larger pool + longer wait).
#[must_use]
pub fn test_redis_config(url: &str) -> RedisConfig {
    RedisConfig {
        url: url.to_owned(),
        pool_size: 16,
        timeout_ms: 5_000,
        key_prefix: "oarb:test:".to_owned(),
    }
}

/// Start Redis and return its connection URL plus the container guard.
pub async fn setup_redis() -> (String, ContainerAsync<Redis>) {
    let container = Redis::default()
        .start()
        .await
        .expect("start Redis container");
    let port = container
        .get_host_port_ipv4(6379)
        .await
        .expect("Redis host port");
    let url = format!("redis://127.0.0.1:{port}");
    (url, container)
}
