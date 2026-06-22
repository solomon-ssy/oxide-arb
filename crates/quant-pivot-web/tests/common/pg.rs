//! Postgres testcontainer bring-up: starts a container, connects a pool, and
//! runs every migration (which also seeds the bootstrap `admin` user, the six
//! built-in roles, and the `g(admin, super_admin)` grant).

use oxide_arb_models::config::PostgresConfig;
use oxide_arb_storage::postgres::{
    PostgresPool,
    migration::{Migrator, MigratorTrait},
};
use testcontainers::{ContainerAsync, ImageExt, runners::AsyncRunner};
use testcontainers_modules::postgres::Postgres;

fn test_pg_config(port: u16) -> PostgresConfig {
    PostgresConfig {
        host: "localhost".into(),
        port,
        user: "postgres".into(),
        password: "postgres".into(),
        database: "test_oxide_arb".into(),
        schema: "public".into(),
        max_connections: 5,
        min_connections: 1,
        connect_timeout_secs: 10,
        idle_timeout_secs: 300,
        acquire_timeout_secs: 10,
        max_lifetime_secs: 1800,
        statement_timeout_ms: 30_000,
        idle_in_transaction_timeout_ms: 60_000,
        lock_timeout_ms: 5_000,
        work_mem: "16MB".into(),
        verify_session_params: false,
        statement_cache_capacity: 100,
        application_name: "oxide-arb-web-test".into(),
    }
}

/// Start Postgres, connect, and migrate (incl. RBAC seed). Returns the pool and
/// the container guard (kept alive for the test's lifetime).
pub async fn setup_pg() -> (PostgresPool, ContainerAsync<Postgres>) {
    let container = Postgres::default()
        .with_db_name("test_oxide_arb")
        .with_user("postgres")
        .with_password("postgres")
        .with_tag("16")
        .start()
        .await
        .expect("start PG container");

    let port = container
        .get_host_port_ipv4(5432)
        .await
        .expect("PG host port");
    let pool = PostgresPool::connect(&test_pg_config(port))
        .await
        .expect("connect PG pool");
    Migrator::up(pool.connection(), None)
        .await
        .expect("run migrations");

    (pool, container)
}
