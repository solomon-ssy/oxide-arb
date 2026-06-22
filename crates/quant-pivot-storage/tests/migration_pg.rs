//! `PostgreSQL` migration integration tests (requires Docker).

use chrono::{DateTime, Utc};
use quant_pivot_models::{
    config::PostgresConfig,
    entities::{blacklist_entry, risk_state, runtime_config_version},
    enums::runtime_config::RuntimeConfigVersionSource,
    schema::{catalog, trigger::TriggerKind},
    types::{AuditEventId, MarketId, OperationLogId, RuntimeConfigVersionId},
};
use quant_pivot_storage::postgres::{
    PostgresPool,
    migration::{Migrator, MigratorTrait},
};
use sea_orm::{ConnectionTrait, EntityTrait};
use std::time::Duration;
use testcontainers::{ImageExt, runners::AsyncRunner};
use tokio::time::sleep;

#[test]
fn updated_at_trigger_catalog_is_complete() {
    let mut tables = catalog::tables()
        .into_iter()
        .flat_map(|spec| (spec.triggers)())
        .filter(|trigger| trigger.kind == TriggerKind::UpdatedAt)
        .map(|trigger| (trigger.table_name)())
        .collect::<Vec<_>>();
    tables.sort();

    assert_eq!(
        tables,
        vec![
            "blacklist_entry",
            "control_factor_materialization_run",
            "control_factor_publication",
            "control_factor_value",
            "endgame_calibration_bucket",
            "event",
            "market",
            "menu",
            "risk_engine_state",
            "role",
            "system_runtime_state",
            "trade",
            "user",
        ]
    );
}

#[test]
fn append_only_trigger_catalog_is_complete() {
    let mut tables = catalog::tables()
        .into_iter()
        .flat_map(|spec| (spec.triggers)())
        .filter(|trigger| trigger.kind == TriggerKind::AppendOnly)
        .map(|trigger| (trigger.table_name)())
        .collect::<Vec<_>>();
    tables.sort();

    assert_eq!(
        tables,
        vec![
            "balance_snapshot",
            "control_factor_audit_event",
            "control_factor_shadow_decision",
            "emergency_snapshot",
            "market_pit_snapshot",
            "operation_log",
            "risk_audit_event",
        ],
        "every lifecycle=audit table must be append-only (WORM)"
    );
}

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
        application_name: "quant-pivot-test".into(),
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
        .with_tag("16")
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
async fn runtime_config_uses_versioned_tables() {
    let (pool, _container) = setup_pool().await;
    let db = pool.connection();

    Migrator::up(db, None).await.expect("Migration up failed");

    let old_table = db
        .query_one(sea_orm::Statement::from_string(
            db.get_database_backend(),
            "SELECT to_regclass('public.runtime_config')",
        ))
        .await
        .expect("query old runtime_config table")
        .expect("old table regclass row");
    let old_table_name: Option<String> = old_table.try_get_by_index(0).expect("read regclass");
    assert!(
        old_table_name.is_none(),
        "old runtime_config table must not exist"
    );

    let version_count: i64 = db
        .query_one(sea_orm::Statement::from_string(
            db.get_database_backend(),
            "SELECT COUNT(*) FROM runtime_config_version",
        ))
        .await
        .expect("query runtime_config_version")
        .expect("version count row")
        .try_get_by_index(0)
        .expect("read version count");
    assert_eq!(
        version_count, 0,
        "Phase 5.1 baseline creates schema, not mutable defaults"
    );
}

#[tokio::test]
#[ignore = "requires Docker"]
async fn seed_application_records_catalog_seeds() {
    let (pool, _container) = setup_pool().await;
    let db = pool.connection();

    Migrator::up(db, None).await.expect("Migration up failed");

    let row = db
        .query_one(sea_orm::Statement::from_string(
            db.get_database_backend(),
            "SELECT COUNT(*) FROM seed_application",
        ))
        .await
        .expect("query seed ledger")
        .expect("seed ledger row");
    let count: i64 = row.try_get_by_index(0).expect("read seed ledger count");

    assert_eq!(
        count, 8,
        "risk state seed + system runtime state seed + 6 RBAC seeds \
         (roles/menus/admin_user/user_role/role_menu/casbin)"
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

    let before_insert: DateTime<Utc> = db
        .query_one(sea_orm::Statement::from_string(
            db.get_database_backend(),
            "SELECT statement_timestamp()",
        ))
        .await
        .expect("query db timestamp")
        .expect("timestamp row")
        .try_get_by_index(0)
        .expect("read db timestamp");

    let version_id = RuntimeConfigVersionId::from_v7();

    // Insert a runtime_config_version row with created_at deliberately omitted.
    db.execute(sea_orm::Statement::from_sql_and_values(
        db.get_database_backend(),
        "INSERT INTO runtime_config_version \
         (runtime_config_version_id, config_hash, schema_version, config_json, source, created_by, reason) \
         VALUES ($1, $2, $3, $4, $5, $6, $7)",
        [
            version_id.clone().into(),
            "hash:test".into(),
            1_i32.into(),
            serde_json::json!({"schema_version": 1}).into(),
            RuntimeConfigVersionSource::Bootstrap.as_str().into(),
            "test".into(),
            "test default timestamp".into(),
        ],
    ))
    .await
    .expect("raw INSERT");

    let row = runtime_config_version::Entity::find_by_id(version_id)
        .one(db)
        .await
        .expect("query")
        .expect("row");

    assert!(
        row.created_at >= before_insert,
        "DB column default must fill created_at on INSERT"
    );
}

#[tokio::test]
#[ignore = "requires Docker"]
async fn blacklist_updated_at_trigger_fires_on_update() {
    let (pool, _container) = setup_pool().await;
    let db = pool.connection();

    Migrator::up(db, None).await.expect("Migration up failed");

    let market_id = MarketId::new("blacklist-trigger-market");

    db.execute(sea_orm::Statement::from_sql_and_values(
        db.get_database_backend(),
        "INSERT INTO blacklist_entry (market_id, token_id, scope, reason) VALUES ($1, $2, $3, $4)",
        [
            market_id.as_str().into(),
            sea_orm::Value::String(None),
            "full".into(),
            "manual".into(),
        ],
    ))
    .await
    .expect("raw INSERT");

    let before = blacklist_entry::Entity::find_by_id(market_id.clone())
        .one(db)
        .await
        .expect("query")
        .expect("row");

    sleep(Duration::from_millis(5)).await;

    db.execute(sea_orm::Statement::from_sql_and_values(
        db.get_database_backend(),
        "UPDATE blacklist_entry SET miss_count = $1 WHERE market_id = $2",
        [1.into(), market_id.as_str().into()],
    ))
    .await
    .expect("raw UPDATE");

    let after = blacklist_entry::Entity::find_by_id(market_id)
        .one(db)
        .await
        .expect("query")
        .expect("row");

    assert!(
        after.updated_at > before.updated_at,
        "blacklist_entry updated_at trigger must auto-refresh on UPDATE \
         (before={}, after={})",
        before.updated_at,
        after.updated_at
    );
    assert_eq!(after.miss_count, 1);
}

/// Count helper for raw `SELECT COUNT(*)` queries.
async fn count_rows(db: &impl ConnectionTrait, sql: &str) -> i64 {
    db.query_one(sea_orm::Statement::from_string(
        db.get_database_backend(),
        sql.to_owned(),
    ))
    .await
    .expect("count query")
    .expect("count row")
    .try_get_by_index(0)
    .expect("read count")
}

#[tokio::test]
#[ignore = "requires Docker"]
async fn rbac_tables_are_created() {
    let (pool, _container) = setup_pool().await;
    let db = pool.connection();

    Migrator::up(db, None).await.expect("Migration up failed");

    for table in [
        "user",
        "role",
        "menu",
        "user_role",
        "role_menu",
        "casbin_rule",
        "operation_log",
    ] {
        let regclass = db
            .query_one(sea_orm::Statement::from_string(
                db.get_database_backend(),
                format!("SELECT to_regclass('public.\"{table}\"')::text"),
            ))
            .await
            .expect("regclass query")
            .expect("regclass row");
        let name: Option<String> = regclass.try_get_by_index(0).expect("read regclass");
        assert!(name.is_some(), "table `{table}` must exist after migration");
    }
}

#[tokio::test]
#[ignore = "requires Docker"]
async fn operation_log_is_append_only() {
    let (pool, _container) = setup_pool().await;
    let db = pool.connection();

    Migrator::up(db, None).await.expect("Migration up failed");

    let log_id = OperationLogId::from_v7();

    db.execute(sea_orm::Statement::from_sql_and_values(
        db.get_database_backend(),
        "INSERT INTO operation_log \
         (id, request_id, category, action, http_method, http_path, http_status, outcome, latency_ms) \
         VALUES ($1, 'req-1', 'system', 'test.action', 'GET', '/x', 200, 'success', 5)",
        [log_id.clone().into()],
    ))
    .await
    .expect("append-only table still accepts INSERT");

    let update = db
        .execute(sea_orm::Statement::from_sql_and_values(
            db.get_database_backend(),
            "UPDATE operation_log SET action = 'tampered' WHERE id = $1",
            [log_id.clone().into()],
        ))
        .await;
    assert!(update.is_err(), "UPDATE on operation_log must be rejected");

    let delete = db
        .execute(sea_orm::Statement::from_sql_and_values(
            db.get_database_backend(),
            "DELETE FROM operation_log WHERE id = $1",
            [log_id.clone().into()],
        ))
        .await;
    assert!(delete.is_err(), "DELETE on operation_log must be rejected");

    let remaining = count_rows(db, "SELECT COUNT(*) FROM operation_log").await;
    assert_eq!(remaining, 1, "row must survive rejected mutations");
}

#[tokio::test]
#[ignore = "requires Docker"]
async fn control_factor_audit_event_is_append_only() {
    let (pool, _container) = setup_pool().await;
    let db = pool.connection();

    Migrator::up(db, None).await.expect("Migration up failed");

    let event_id = AuditEventId::from_v7();

    db.execute(sea_orm::Statement::from_sql_and_values(
        db.get_database_backend(),
        "INSERT INTO control_factor_audit_event \
         (event_id, sequence, event_type, actor, actor_role, resource_type, resource_id, \
          request_id, reason, diff, event_hash) \
         VALUES ($1, 1, 'factor_created', 'op', 'operator', 'factor', 'cf_1', \
                 'req-1', 'r', '{}'::jsonb, 'blake3:test')",
        [event_id.clone().into()],
    ))
    .await
    .expect("audit insert");

    let update = db
        .execute(sea_orm::Statement::from_sql_and_values(
            db.get_database_backend(),
            "UPDATE control_factor_audit_event SET reason = 'tampered' WHERE event_id = $1",
            [event_id.clone().into()],
        ))
        .await;
    assert!(update.is_err(), "UPDATE on audit chain must be rejected");

    let delete = db
        .execute(sea_orm::Statement::from_sql_and_values(
            db.get_database_backend(),
            "DELETE FROM control_factor_audit_event WHERE event_id = $1",
            [event_id.clone().into()],
        ))
        .await;
    assert!(delete.is_err(), "DELETE on audit chain must be rejected");
}

#[tokio::test]
#[ignore = "requires Docker"]
async fn rbac_seeds_are_idempotent() {
    let (pool, _container) = setup_pool().await;
    let db = pool.connection();

    Migrator::up(db, None).await.expect("First up failed");

    let admin_hash_before: String = db
        .query_one(sea_orm::Statement::from_string(
            db.get_database_backend(),
            "SELECT password_hash FROM \"user\" WHERE username = 'admin'".to_owned(),
        ))
        .await
        .expect("query admin")
        .expect("admin row")
        .try_get_by_index(0)
        .expect("read hash");

    Migrator::up(db, None).await.expect("Second up failed");

    assert_eq!(count_rows(db, "SELECT COUNT(*) FROM role").await, 6);
    assert_eq!(
        count_rows(db, "SELECT COUNT(*) FROM \"user\" WHERE username = 'admin'").await,
        1
    );

    let admin_hash_after: String = db
        .query_one(sea_orm::Statement::from_string(
            db.get_database_backend(),
            "SELECT password_hash FROM \"user\" WHERE username = 'admin'".to_owned(),
        ))
        .await
        .expect("query admin")
        .expect("admin row")
        .try_get_by_index(0)
        .expect("read hash");

    assert_eq!(
        admin_hash_before, admin_hash_after,
        "re-migration must not overwrite the admin password hash"
    );
}
