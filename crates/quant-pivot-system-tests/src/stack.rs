//! Disposable `PostgreSQL`, Redis, and `ClickHouse` stack for system suites.

use std::{
    net::{Ipv4Addr, TcpListener},
    time::Duration,
};

use anyhow::{Context, Result};
use quant_pivot_migration::apply as apply_postgres_migrations;
use quant_pivot_models::{
    config::{ClickHouseConfig, PostgresConfig, RedisConfig},
    security::hash_password,
};
use quant_pivot_storage::{
    cache::{RedisBackend, connect_pool},
    clickhouse::{ClickHousePool, ClickHouseSchemaStatus, apply_online_schema_migrations},
    postgres::{
        PostgresPool,
        migration::{PostgresSchemaStatus, finalize_schema_deployment},
    },
};
use testcontainers::{
    ContainerAsync, GenericImage, ImageExt,
    core::{WaitFor, wait::HttpWaitStrategy},
    runners::AsyncRunner,
};
use testcontainers_modules::{postgres::Postgres, redis::Redis};

const POSTGRES_DATABASE: &str = "quant_pivot_system";
const POSTGRES_IMAGE_TAG: &str = "16";
const REDIS_IMAGE_TAG: &str = "7-alpine";
/// Exact `ClickHouse` image used by disposable schema and production-stack gates.
pub const CLICKHOUSE_IMAGE_TAG: &str = "26.5";
/// Fixed credential installed only in disposable system-test databases.
pub const BOOTSTRAP_ADMIN_PASSWORD: &str = "system-test-bootstrap-admin";

/// One disposable, schema-complete infrastructure stack owned by a system suite.
///
/// Pool fields precede container guards so clients are dropped before their
/// backing containers when the stack leaves scope.
pub struct SystemStack {
    pub postgres_config: PostgresConfig,
    pub postgres: PostgresPool,
    pub postgres_schema: PostgresSchemaStatus,
    pub redis_config: RedisConfig,
    pub redis: RedisBackend,
    pub clickhouse_config: ClickHouseConfig,
    pub clickhouse: ClickHousePool,
    pub clickhouse_schema: ClickHouseSchemaStatus,
    _postgres_container: ContainerAsync<Postgres>,
    pub(crate) redis_container: ContainerAsync<Redis>,
    _clickhouse_container: ContainerAsync<GenericImage>,
}

impl SystemStack {
    /// Start each service within its own readiness budget, deploy both database
    /// schemas, and fail before returning if any contract is incomplete.
    pub async fn start() -> Result<Self> {
        // Docker Desktop can report individual containers ready while host port
        // forwarding for concurrently started siblings is still settling.
        // Sequential startup keeps the disposable harness deterministic without
        // weakening any service-specific readiness deadline.
        let postgres = start_postgres().await?;
        let redis = start_redis().await?;
        let clickhouse = start_clickhouse().await?;
        Ok(Self {
            postgres_config: postgres.0,
            postgres: postgres.1,
            postgres_schema: postgres.2,
            redis_config: redis.0,
            redis: redis.1,
            clickhouse_config: clickhouse.0,
            clickhouse: clickhouse.1,
            clickhouse_schema: clickhouse.2,
            _postgres_container: postgres.3,
            redis_container: redis.2,
            _clickhouse_container: clickhouse.3,
        })
    }

    /// Remove every disposable service before the owning runtime exits.
    ///
    /// `ContainerAsync` falls back to an asynchronous drop task, which is not
    /// sufficient for short-lived launchers: the Tokio runtime can terminate
    /// before that task reaches Docker. System and browser fixtures therefore
    /// close pools first and await container removal explicitly.
    pub async fn shutdown(self) -> Result<()> {
        let Self {
            postgres,
            redis,
            clickhouse,
            _postgres_container: postgres_container,
            redis_container,
            _clickhouse_container: clickhouse_container,
            ..
        } = self;
        drop((postgres, redis, clickhouse));

        tokio::try_join!(
            async {
                postgres_container
                    .rm()
                    .await
                    .context("remove PostgreSQL system-test container")
            },
            async {
                redis_container
                    .rm()
                    .await
                    .context("remove Redis system-test container")
            },
            async {
                clickhouse_container
                    .rm()
                    .await
                    .context("remove ClickHouse system-test container")
            },
        )?;
        Ok(())
    }
}

async fn start_postgres() -> Result<(
    PostgresConfig,
    PostgresPool,
    PostgresSchemaStatus,
    ContainerAsync<Postgres>,
)> {
    let container = Postgres::default()
        .with_db_name(POSTGRES_DATABASE)
        .with_user("postgres")
        .with_password("postgres")
        .with_tag(POSTGRES_IMAGE_TAG)
        .start()
        .await
        .context("start PostgreSQL system-test container")?;
    let port = container
        .get_host_port_ipv4(5432)
        .await
        .context("resolve PostgreSQL system-test port")?;
    let config = PostgresConfig {
        host: "127.0.0.1".to_owned(),
        port,
        user: "postgres".to_owned(),
        password: "postgres".into(),
        database: POSTGRES_DATABASE.to_owned(),
        schema: "public".to_owned(),
        max_connections: 15,
        min_connections: 1,
        connect_timeout_secs: 20,
        acquire_timeout_secs: 30,
        application_name: "quant-pivot-system-tests".to_owned(),
        verify_session_params: true,
        ..PostgresConfig::default()
    };
    let pool = PostgresPool::connect(&config)
        .await
        .context("connect PostgreSQL system-test pool")?;
    apply_postgres_migrations(&config, pool.connection())
        .await
        .context("apply PostgreSQL system-test migrations")?;
    let password_hash = hash_password(BOOTSTRAP_ADMIN_PASSWORD)
        .context("hash PostgreSQL system-test bootstrap credential")?;
    let status = finalize_schema_deployment(pool.connection(), &password_hash)
        .await
        .context("finalize PostgreSQL system-test schema")?;
    pool.health_check()
        .await
        .context("verify PostgreSQL system-test readiness")?;
    Ok((config, pool, status, container))
}

async fn start_redis() -> Result<(RedisConfig, RedisBackend, ContainerAsync<Redis>)> {
    let host_port = TcpListener::bind((Ipv4Addr::LOCALHOST, 0))
        .context("reserve Redis system-test host port")?
        .local_addr()
        .context("resolve Redis system-test host port")?
        .port();
    let container = Redis::default()
        .with_tag(REDIS_IMAGE_TAG)
        .with_mapped_port(host_port, 6379.into())
        // SystemStack owns volatile cache/session state. Disabling both Redis
        // persistence mechanisms makes a process restart deterministically
        // model complete state loss instead of depending on the default RDB
        // save cadence and the test's wall-clock duration.
        .with_cmd(["redis-server", "--save", "", "--appendonly", "no"])
        .start()
        .await
        .context("start Redis system-test container")?;
    let port = container
        .get_host_port_ipv4(6379)
        .await
        .context("resolve Redis system-test port")?;
    let config = RedisConfig {
        host: "127.0.0.1".to_owned(),
        port,
        key_prefix: "quant-pivot:system:".to_owned(),
        pool_size: 8,
        timeout_ms: 5_000,
        connect_timeout_ms: 20_000,
        ..RedisConfig::default()
    };
    let pool = connect_pool(&config)
        .await
        .context("connect Redis system-test pool")?;
    let backend = RedisBackend::new(pool, &config.key_prefix);
    backend
        .health_check()
        .await
        .context("verify Redis system-test readiness")?;
    Ok((config, backend, container))
}

async fn start_clickhouse() -> Result<(
    ClickHouseConfig,
    ClickHousePool,
    ClickHouseSchemaStatus,
    ContainerAsync<GenericImage>,
)> {
    let container = GenericImage::new("clickhouse/clickhouse-server", CLICKHOUSE_IMAGE_TAG)
        .with_exposed_port(8123.into())
        .with_wait_for(WaitFor::http(
            HttpWaitStrategy::new("/ping")
                .with_port(8123.into())
                .with_expected_status_code(200u16),
        ))
        .with_env_var("CLICKHOUSE_SKIP_USER_SETUP", "1")
        .with_startup_timeout(Duration::from_mins(2))
        .start()
        .await
        .context("start ClickHouse system-test container")?;
    let port = container
        .get_host_port_ipv4(8123)
        .await
        .context("resolve ClickHouse system-test port")?;
    let config = ClickHouseConfig {
        deployment_id: "system-test".to_owned(),
        cluster_id: "testcontainer".to_owned(),
        url: format!("http://127.0.0.1:{port}"),
        database: "quant_pivot_system".to_owned(),
        user: "default".to_owned(),
        password: "".into(),
        batch_size: 100,
        flush_interval_secs: 1,
        max_concurrent_inserts: 4,
    };
    let status = apply_online_schema_migrations(&config)
        .await
        .context("apply ClickHouse system-test migrations")?;
    let pool = ClickHousePool::connect(&config)
        .await
        .context("connect ClickHouse system-test pool")?;
    let verified = pool
        .verify_schema()
        .await
        .context("verify ClickHouse system-test schema")?;
    if status != verified {
        anyhow::bail!(
            "ClickHouse migration status differs from runtime verification: applied={status:?}, verified={verified:?}"
        );
    }
    Ok((config, pool, verified, container))
}
