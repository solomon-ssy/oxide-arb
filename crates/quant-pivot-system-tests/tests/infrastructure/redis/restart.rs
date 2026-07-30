//! Redis restart semantics for the shared pool and recoverable cache state.

use std::{
    net::{Ipv4Addr, TcpListener},
    time::{Duration, Instant},
};

use quant_pivot_models::config::RedisConfig;
use quant_pivot_storage::cache::{CacheBackend, RedisBackend, RedisPool, connect_pool};
use testcontainers::{ContainerAsync, GenericImage, ImageExt, core::WaitFor, runners::AsyncRunner};
use tokio::time::sleep;

const REDIS_PORT: u16 = 6379;

pub async fn cache_restart_recovers() {
    let fixture = RedisRestartFixture::start().await;
    fixture.assert_pre_restart().await;
    fixture.restart().await;
    fixture.assert_state_loss().await;
    fixture.assert_new_write().await;
}

struct RedisRestartFixture {
    container: ContainerAsync<GenericImage>,
    config: RedisConfig,
    pool: RedisPool,
    cache: RedisBackend,
}

impl RedisRestartFixture {
    async fn start() -> Self {
        let host_port = TcpListener::bind((Ipv4Addr::LOCALHOST, 0))
            .expect("reserve an ephemeral Redis host port")
            .local_addr()
            .expect("resolve the ephemeral Redis host port")
            .port();
        let container = GenericImage::new("redis", "7-alpine")
            .with_wait_for(WaitFor::message_on_stdout("Ready to accept connections"))
            .with_mapped_port(host_port, REDIS_PORT.into())
            .with_cmd(["redis-server", "--save", "", "--appendonly", "no"])
            .start()
            .await
            .expect("start disposable non-persistent Redis");
        let port = container
            .get_host_port_ipv4(REDIS_PORT)
            .await
            .expect("resolve disposable Redis port");
        let config = RedisConfig {
            host: "127.0.0.1".to_owned(),
            port,
            key_prefix: "quant-pivot:w4-e01:restart:".to_owned(),
            pool_size: 4,
            timeout_ms: 500,
            connect_timeout_ms: 20_000,
            ..RedisConfig::default()
        };
        let pool = connect_pool(&config)
            .await
            .expect("connect disposable Redis");
        let cache = RedisBackend::new(pool.clone(), &config.key_prefix);
        cache
            .set("restart-cache", b"warm", Duration::from_mins(2))
            .await
            .expect("seed disposable cache");

        Self {
            container,
            config,
            pool,
            cache,
        }
    }

    async fn assert_pre_restart(&self) {
        assert_eq!(
            self.cache
                .get("restart-cache")
                .await
                .expect("read cache before restart"),
            Some(b"warm".to_vec()),
            "the restart fixture must begin with observable volatile state"
        );
    }

    async fn restart(&self) {
        self.container
            .stop_with_timeout(Some(0))
            .await
            .expect("stop disposable Redis");
        assert!(
            self.cache.health_check().await.is_err(),
            "the shared pool must expose the Redis outage"
        );

        self.container
            .start()
            .await
            .expect("restart disposable Redis");
        let restarted_port = self
            .container
            .get_host_port_ipv4(REDIS_PORT)
            .await
            .expect("resolve Redis port after restart");
        assert_eq!(
            restarted_port, self.config.port,
            "Docker must preserve the published Redis endpoint across restart"
        );
        let restart_running = self
            .container
            .is_running()
            .await
            .expect("inspect Redis state after restart");
        let restart_exit_code = self
            .container
            .exit_code()
            .await
            .expect("inspect Redis exit code after restart");
        assert!(
            restart_running,
            "Redis container exited immediately after restart with code {restart_exit_code:?}"
        );
        let _fresh_pool = connect_pool(&self.config)
            .await
            .expect("connect a fresh pool after Redis restart");
        let recovery_deadline = Instant::now() + Duration::from_secs(20);
        loop {
            match self.cache.health_check().await {
                Ok(()) => break,
                Err(error) if Instant::now() >= recovery_deadline => {
                    panic!(
                        "shared Redis pool did not recover after restart: {error}; status={:?}",
                        self.pool.status()
                    );
                }
                Err(_) => sleep(Duration::from_millis(50)).await,
            }
        }
    }

    async fn assert_state_loss(&self) {
        assert_eq!(
            self.cache
                .get("restart-cache")
                .await
                .expect("read cache after restart"),
            None,
            "cache state is intentionally recoverable rather than authoritative"
        );
    }

    async fn assert_new_write(&self) {
        self.cache
            .set("restart-cache", b"recovered", Duration::from_mins(2))
            .await
            .expect("write cache after Redis restart");
        assert_eq!(
            self.cache
                .get("restart-cache")
                .await
                .expect("read cache after Redis restart"),
            Some(b"recovered".to_vec()),
            "the recovered shared pool must accept new cache state"
        );
    }
}
