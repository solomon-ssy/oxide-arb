//! Testcontainers Postgres pool + migration bootstrap for integration tests.

use quant_pivot_migration::apply as apply_postgres_migrations;
use quant_pivot_models::{
    config::{PostgresConfig, SchemaMigrationConfig},
    security::hash_password,
};
use quant_pivot_storage::postgres::{PostgresPool, migration::finalize_schema_deployment};
use sea_orm::ConnectionTrait;
use testcontainers::{ContainerAsync, ImageExt, runners::AsyncRunner};
use testcontainers_modules::postgres::Postgres;

pub const TEST_RUNTIME_ROLE: &str = "quant_pivot_test_runtime";

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
        migration: SchemaMigrationConfig {
            user: "quant_pivot_test_migrator".into(),
        },
        max_connections: 15,
        min_connections: 1,
        connect_timeout_secs: 10,
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
pub async fn setup_pg() -> (PostgresPool, ContainerAsync<Postgres>) {
    let container = Postgres::default()
        .with_db_name("test_quant_pivot")
        .with_user("postgres")
        .with_password("postgres")
        .with_tag("16")
        .start()
        .await
        .expect("PG container");

    let port = container.get_host_port_ipv4(5432).await.expect("port");
    let config = test_pg_config(port);
    let pool = PostgresPool::connect(&config).await.expect("connect");
    pool.connection()
        .execute_unprepared(&format!("CREATE ROLE {TEST_RUNTIME_ROLE} NOLOGIN"))
        .await
        .expect("create runtime role");
    apply_postgres_migrations(pool.connection())
        .await
        .expect("apply migrations");
    let bootstrap_admin_password_hash =
        hash_password("admin").expect("hash test bootstrap admin password");
    finalize_schema_deployment(
        pool.connection(),
        TEST_RUNTIME_ROLE,
        &bootstrap_admin_password_hash,
    )
    .await
    .expect("finalize schema deployment");

    (pool, container)
}
