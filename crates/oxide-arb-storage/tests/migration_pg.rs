//! `PostgreSQL` migration integration tests (requires Docker).

use chrono::Utc;
use oxide_arb_models::{
    config::PostgresConfig,
    entities::{risk_state, runtime_config},
    enums::runtime_config::RuntimeConfigKey,
};
use oxide_arb_storage::postgres::{
    PostgresPool,
    migration::{Migrator, MigratorTrait},
};
use sea_orm::{ConnectionTrait, EntityTrait};
use std::time::Duration;
use testcontainers::runners::AsyncRunner;

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
        application_name: "oxide-arb-test".into(),
    }
}

async fn setup_pool() -> (
    PostgresPool,
    testcontainers::ContainerAsync<testcontainers_modules::postgres::Postgres>,
) {
    let pg_container = testcontainers_modules::postgres::Postgres::default()
        .with_db_name("test_oxide_arb")
        .with_user("postgres")
        .with_password("postgres")
        .start()
        .await
        .expect("Failed to start PostgreSQL container");

    let port = pg_container
        .get_host_port_ipv4(5432)
        .await
        .expect("Failed to get PG port");

    let config = test_pg_config(port);
    let pool = PostgresPool::connect(&config)
        .await
        .expect("Failed to connect");

    (pool, pg_container)
}

#[tokio::test]
#[ignore = "requires Docker"]
async fn migrations_up_down_idempotent() {
    let (pool, _container) = setup_pool().await;

    Migrator::up(pool.connection(), None)
        .await
        .expect("Migration up failed");

    pool.health_check()
        .await
        .expect("Health check failed after migrations");

    Migrator::up(pool.connection(), None)
        .await
        .expect("Second migration up should be idempotent");

    Migrator::down(pool.connection(), None)
        .await
        .expect("Migration down failed");
}

#[tokio::test]
#[ignore = "requires Docker"]
async fn bootstrap_applies_risk_engine_state() {
    let (pool, _container) = setup_pool().await;
    let db = pool.connection();

    Migrator::up(db, None).await.expect("Migration up failed");

    let row = risk_state::Entity::find_by_id(1)
        .one(db)
        .await
        .expect("Failed to query risk_engine_state")
        .expect("risk_engine_state id=1 should exist after bootstrap");

    assert_eq!(row.id, 1);
    assert!(!row.is_halted);
    assert_eq!(row.consecutive_misses, 0);
    assert_eq!(row.cooldown_multiplier, 1);
}

#[tokio::test]
#[ignore = "requires Docker"]
async fn bootstrap_idempotent() {
    let (pool, _container) = setup_pool().await;
    let db = pool.connection();

    Migrator::up(db, None).await.expect("First up failed");
    Migrator::up(db, None)
        .await
        .expect("Second up (idempotent) failed");

    let rows: Vec<risk_state::Model> = risk_state::Entity::find()
        .all(db)
        .await
        .expect("Failed to query risk_engine_state");
    assert_eq!(
        rows.len(),
        1,
        "should still have exactly one risk state row"
    );
}

#[tokio::test]
#[ignore = "requires Docker"]
async fn runtime_config_all_keys_seeded() {
    let (pool, _container) = setup_pool().await;
    let db = pool.connection();

    Migrator::up(db, None).await.expect("Migration up failed");

    let rows: Vec<runtime_config::Model> = runtime_config::Entity::find()
        .all(db)
        .await
        .expect("Failed to query runtime_config");

    let expected_count = 14; // one per RuntimeConfigKey variant
    assert_eq!(
        rows.len(),
        expected_count,
        "should have one row per RuntimeConfigKey variant"
    );
}

#[tokio::test]
#[ignore = "requires Docker"]
async fn runtime_config_no_clobber() {
    let (pool, _container) = setup_pool().await;
    let db = pool.connection();

    Migrator::up(db, None).await.expect("Migration up failed");

    // Manually update a config value
    let custom_value = serde_json::json!(99999.0);
    let typed_key = RuntimeConfigKey::MaxPortfolioExposureUsd;
    let key = typed_key.as_str();
    db.execute(sea_orm::Statement::from_sql_and_values(
        db.get_database_backend(),
        "UPDATE runtime_config SET value = $1 WHERE key = $2",
        [custom_value.clone().into(), key.into()],
    ))
    .await
    .expect("Failed to update runtime_config");

    // Re-run migrations — should not overwrite the custom value
    Migrator::up(db, None)
        .await
        .expect("Second up (no-clobber) failed");

    let row = runtime_config::Entity::find_by_id(typed_key)
        .one(db)
        .await
        .expect("Failed to query runtime_config")
        .expect("key should still exist");

    assert_eq!(
        row.value, custom_value,
        "operator-modified value must survive re-migration"
    );
}

#[tokio::test]
#[ignore = "requires Docker"]
async fn updated_at_trigger_fires_on_update() {
    let (pool, _container) = setup_pool().await;
    let db = pool.connection();

    Migrator::up(db, None).await.expect("Migration up failed");

    // Record the bootstrap row's updated_at
    let before = risk_state::Entity::find_by_id(1)
        .one(db)
        .await
        .expect("query")
        .expect("row");
    let old_updated_at = before.updated_at;

    // Sleep briefly to ensure timestamp differs
    tokio::time::sleep(Duration::from_millis(50)).await;

    // Update a column WITHOUT manually setting updated_at
    db.execute(sea_orm::Statement::from_sql_and_values(
        db.get_database_backend(),
        "UPDATE risk_engine_state SET is_halted = $1 WHERE id = $2",
        [true.into(), 1_i32.into()],
    ))
    .await
    .expect("raw UPDATE");

    let after = risk_state::Entity::find_by_id(1)
        .one(db)
        .await
        .expect("query")
        .expect("row");

    assert!(
        after.updated_at > old_updated_at,
        "trigger must auto-refresh updated_at on UPDATE \
         (before={old_updated_at}, after={})",
        after.updated_at
    );
    assert!(after.is_halted, "the actual column should have changed too");
}

#[tokio::test]
#[ignore = "requires Docker"]
async fn db_defaults_fill_notset_timestamps_on_insert() {
    let (pool, _container) = setup_pool().await;
    let db = pool.connection();

    Migrator::up(db, None).await.expect("Migration up failed");

    let before_insert = Utc::now();

    let typed_key = RuntimeConfigKey::DryRunMode;
    let key = typed_key.as_str();

    db.execute(sea_orm::Statement::from_sql_and_values(
        db.get_database_backend(),
        "DELETE FROM runtime_config WHERE key = $1",
        [key.into()],
    ))
    .await
    .expect("delete seeded row");

    // Insert a runtime_config row with updated_at deliberately omitted (DB default).
    db.execute(sea_orm::Statement::from_sql_and_values(
        db.get_database_backend(),
        "INSERT INTO runtime_config (key, value, updated_by) VALUES ($1, $2, $3)",
        [key.into(), serde_json::json!(42).into(), "test".into()],
    ))
    .await
    .expect("raw INSERT");

    let row = runtime_config::Entity::find_by_id(typed_key)
        .one(db)
        .await
        .expect("query")
        .expect("row");

    assert!(
        row.updated_at >= before_insert,
        "DB column default must fill updated_at on INSERT"
    );
}
