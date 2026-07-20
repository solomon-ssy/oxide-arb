//! Testcontainers Postgres pool + migration bootstrap for integration tests.

use quant_pivot_migration::apply as apply_postgres_migrations;
use quant_pivot_models::{config::PostgresConfig, security::hash_password};
use quant_pivot_storage::postgres::{PostgresPool, migration::finalize_schema_deployment};
use std::{
    sync::{Arc, LazyLock},
    time::Duration,
};
use testcontainers::{ContainerAsync, ImageExt, runners::AsyncRunner};
use testcontainers_modules::postgres::Postgres;
use tokio::{
    sync::{OwnedSemaphorePermit, Semaphore},
    time::sleep,
};
use tracing::warn;

/// Bound active `PostgreSQL` testcontainers per test process. Unbounded libtest
/// parallelism can starve Docker Desktop during container readiness and surface
/// as an unrelated `SeaORM` maintenance-pool timeout.
const POSTGRES_CONTAINER_CONCURRENCY: usize = 2;
const POSTGRES_CONTAINER_START_ATTEMPTS: usize = 3;
const POSTGRES_CONTAINER_RETRY_DELAY: Duration = Duration::from_millis(250);
static POSTGRES_CONTAINER_SLOTS: LazyLock<Arc<Semaphore>> =
    LazyLock::new(|| Arc::new(Semaphore::new(POSTGRES_CONTAINER_CONCURRENCY)));

/// `PostgreSQL` testcontainer whose lifetime also owns one bounded Docker slot.
pub struct TestPostgresContainer {
    _container: ContainerAsync<Postgres>,
    _permit: OwnedSemaphorePermit,
}

/// Build a [`PostgresConfig`] aimed at a local testcontainer port.
#[must_use]
pub fn test_pg_config(port: u16) -> PostgresConfig {
    PostgresConfig {
        host: "localhost".into(),
        port,
        user: "postgres".into(),
        password: "postgres".into(),
        database: "test_quant_pivot".into(),
        schema: "public".into(),
        max_connections: 15,
        min_connections: 1,
        // A single disposable-container attempt is bounded independently from
        // the outer whole-container retry in `setup_pg`.
        connect_timeout_secs: 20,
        idle_timeout_secs: 300,
        acquire_timeout_secs: 30,
        max_lifetime_secs: 1800,
        statement_timeout_ms: 30_000,
        idle_in_transaction_timeout_ms: 60_000,
        lock_timeout_ms: 5_000,
        work_mem: "16MB".into(),
        verify_session_params: false,
        statement_cache_capacity: 100,
        application_name: "quant-pivot-test".into(),
    }
}

/// Start Postgres 16, run migrations, and return a pool plus the container handle.
///
/// Keep the returned container alive for the duration of the test so the pool
/// stays connected.
pub async fn setup_pg() -> (PostgresPool, TestPostgresContainer) {
    let permit = Arc::clone(&POSTGRES_CONTAINER_SLOTS)
        .acquire_owned()
        .await
        .expect("PostgreSQL testcontainer semaphore must remain open");

    for attempt in 1..=POSTGRES_CONTAINER_START_ATTEMPTS {
        match start_ready_postgres().await {
            Ok((pool, container)) => {
                return (
                    pool,
                    TestPostgresContainer {
                        _container: container,
                        _permit: permit,
                    },
                );
            }
            Err(error) if attempt < POSTGRES_CONTAINER_START_ATTEMPTS => {
                warn!(
                    attempt,
                    max_attempts = POSTGRES_CONTAINER_START_ATTEMPTS,
                    error = %error,
                    "discarding failed PostgreSQL testcontainer startup"
                );
                sleep(POSTGRES_CONTAINER_RETRY_DELAY).await;
            }
            Err(error) => {
                panic!(
                    "PostgreSQL testcontainer failed after {POSTGRES_CONTAINER_START_ATTEMPTS} complete startup attempts: {error}"
                );
            }
        }
    }

    unreachable!("positive PostgreSQL startup-attempt count exhausts through return or panic")
}

async fn start_ready_postgres() -> Result<(PostgresPool, ContainerAsync<Postgres>), String> {
    let container = Postgres::default()
        .with_db_name("test_quant_pivot")
        .with_user("postgres")
        .with_password("postgres")
        .with_tag("16")
        .start()
        .await
        .map_err(|error| format!("start container: {error}"))?;

    let port = container
        .get_host_port_ipv4(5432)
        .await
        .map_err(|error| format!("resolve PostgreSQL host port: {error}"))?;
    let config = test_pg_config(port);
    let pool = PostgresPool::connect(&config)
        .await
        .map_err(|error| format!("connect and ensure test database: {error}"))?;
    apply_postgres_migrations(&config, pool.connection())
        .await
        .map_err(|error| format!("apply test migrations: {error}"))?;
    let bootstrap_admin_password_hash = hash_password("admin")
        .map_err(|error| format!("hash test bootstrap admin password: {error}"))?;
    finalize_schema_deployment(pool.connection(), &bootstrap_admin_password_hash)
        .await
        .map_err(|error| format!("finalize test schema deployment: {error}"))?;

    Ok((pool, container))
}
