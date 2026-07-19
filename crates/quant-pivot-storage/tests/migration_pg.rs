//! Audited `PostgreSQL` `SeaORM` migration and schema verification tests (requires Docker).

use quant_pivot_error::storage::StorageError;
use quant_pivot_migration::{
    apply as apply_postgres_migrations, migrations::Migrator as PostgresMigrator,
    plan as plan_postgres_migrations,
};
use quant_pivot_models::security::hash_password;
use quant_pivot_storage::postgres::migration::{
    PostgresSchemaStatus, finalize_schema_deployment as finalize_schema_deployment_with_hash,
    verify_schema,
};
use quant_pivot_test_support::pg::{TEST_RUNTIME_ROLE, test_pg_config};
use sea_orm::{ConnectionTrait, Database, DbBackend, Statement};
use sea_orm_migration::MigratorTrait;
use testcontainers::{ImageExt, runners::AsyncRunner};
use testcontainers_modules::postgres::Postgres;

async fn setup_empty_pg() -> (
    sea_orm::DatabaseConnection,
    testcontainers::ContainerAsync<Postgres>,
    u16,
) {
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
    let url = config.try_connection_url().expect("PostgreSQL URL");
    let db = Database::connect(url).await.expect("connect");
    db.execute_unprepared(&format!(
        "CREATE ROLE {TEST_RUNTIME_ROLE} LOGIN PASSWORD 'quant-pivot-test-runtime'"
    ))
    .await
    .expect("create runtime role");
    (db, container, port)
}

async fn relation_exists(db: &sea_orm::DatabaseConnection, relation_name: &str) -> bool {
    db.query_one_raw(Statement::from_sql_and_values(
        DbBackend::Postgres,
        "SELECT 1 FROM pg_class WHERE relname = $1",
        [relation_name.into()],
    ))
    .await
    .expect("query pg_class")
    .is_some()
}

async fn finalize_schema_deployment(
    db: &sea_orm::DatabaseConnection,
    runtime_role: &str,
) -> Result<PostgresSchemaStatus, StorageError> {
    let bootstrap_admin_password_hash =
        hash_password("admin").expect("hash test bootstrap admin password");
    finalize_schema_deployment_with_hash(db, runtime_role, &bootstrap_admin_password_hash).await
}

async fn assert_manifest_drift(db: &sea_orm::DatabaseConnection, section: &str) {
    let error = verify_schema(db)
        .await
        .expect_err("schema drift must fail verification");
    assert!(error.to_string().contains("semantic schema manifest drift"));
    assert!(error.to_string().contains(section));
}

#[tokio::test]
#[ignore = "requires Docker"]
async fn migration_plan_is_read_only_on_empty_database() {
    let (db, _container, _port) = setup_empty_pg().await;

    let plan = plan_postgres_migrations(&db)
        .await
        .expect("plan empty database");

    assert!(!plan.migration_ledger_exists);
    assert!(plan.applied_versions.is_empty());
    assert_eq!(plan.pending_migrations.len(), 1);
    assert!(!relation_exists(&db, "seaql_migrations").await);
    assert!(!relation_exists(&db, "schema_migration_audit").await);
}

#[tokio::test]
#[ignore = "requires Docker"]
async fn immutable_baseline_is_idempotent_and_drift_is_rejected() {
    let (db, _container, _port) = setup_empty_pg().await;

    apply_postgres_migrations(&db)
        .await
        .expect("apply baseline");
    let status = finalize_schema_deployment(&db, TEST_RUNTIME_ROLE)
        .await
        .expect("finalize baseline");
    assert_eq!(status.current_version, "m00000000_000001_bootstrap");
    assert_eq!(status.migration_count, 1);
    assert!(relation_exists(&db, "quant_report_schedule_state").await);
    assert!(relation_exists(&db, "seaql_migrations").await);
    assert!(relation_exists(&db, "schema_migration_audit").await);
    assert!(!relation_exists(&db, "_sqlx_migrations").await);

    apply_postgres_migrations(&db)
        .await
        .expect("reapply migrations");
    let repeated = finalize_schema_deployment(&db, TEST_RUNTIME_ROLE)
        .await
        .expect("refinalize baseline");
    assert_eq!(repeated, status);

    db.execute_raw(Statement::from_string(
        DbBackend::Postgres,
        "DROP TABLE quant_report_schedule_state",
    ))
    .await
    .expect("inject schema drift");
    assert_manifest_drift(&db, "tables").await;
}

#[tokio::test]
#[ignore = "requires Docker"]
async fn boot_baseline_rejects_a_nonempty_unknown_schema() {
    let (db, _container, _port) = setup_empty_pg().await;
    db.execute_unprepared("CREATE TABLE legacy_schema_marker (id bigint PRIMARY KEY)")
        .await
        .expect("seed unknown legacy schema");

    let error = apply_postgres_migrations(&db)
        .await
        .expect_err("boot migration must reject a nonempty target");
    assert!(
        error
            .to_string()
            .contains("boot migration requires an empty public schema")
    );
}

#[tokio::test]
#[ignore = "requires Docker"]
async fn immutable_migrations_round_trip_on_empty_database() {
    let (db, _container, _port) = setup_empty_pg().await;
    apply_postgres_migrations(&db)
        .await
        .expect("apply migrations");

    PostgresMigrator::down(&db, None)
        .await
        .expect("roll back all migrations");
    assert!(!relation_exists(&db, "quant_report_schedule_state").await);

    PostgresMigrator::up(&db, None)
        .await
        .expect("reapply all migrations");
    finalize_schema_deployment(&db, TEST_RUNTIME_ROLE)
        .await
        .expect("finalize reapplied schema");
    assert!(relation_exists(&db, "quant_report_schedule_state").await);
}

#[tokio::test]
#[ignore = "requires Docker"]
async fn legacy_sqlx_ledger_is_forbidden() {
    let (db, _container, _port) = setup_empty_pg().await;
    apply_postgres_migrations(&db)
        .await
        .expect("apply baseline");
    finalize_schema_deployment(&db, TEST_RUNTIME_ROLE)
        .await
        .expect("finalize baseline");
    db.execute_raw(Statement::from_string(
        DbBackend::Postgres,
        "CREATE TABLE _sqlx_migrations (version bigint PRIMARY KEY)",
    ))
    .await
    .expect("inject forbidden ledger");
    let error = verify_schema(&db)
        .await
        .expect_err("legacy migration ledger must fail verification");
    assert!(error.to_string().contains("_sqlx_migrations"));
}

#[tokio::test]
#[ignore = "requires Docker"]
async fn migration_artifact_checksum_tamper_is_rejected() {
    let (db, _container, _port) = setup_empty_pg().await;
    apply_postgres_migrations(&db)
        .await
        .expect("apply baseline");
    finalize_schema_deployment(&db, TEST_RUNTIME_ROLE)
        .await
        .expect("finalize baseline");

    db.execute_raw(Statement::from_string(
        DbBackend::Postgres,
        "UPDATE schema_migration_audit SET checksum = repeat('0', 64)",
    ))
    .await
    .expect("tamper checksum audit");
    let error = verify_schema(&db)
        .await
        .expect_err("migration checksum tamper must fail verification");
    assert!(error.to_string().contains("immutable deploy manifest"));
}

#[tokio::test]
#[ignore = "requires Docker"]
async fn unknown_future_native_migration_is_rejected() {
    let (db, _container, _port) = setup_empty_pg().await;
    apply_postgres_migrations(&db)
        .await
        .expect("apply baseline");
    finalize_schema_deployment(&db, TEST_RUNTIME_ROLE)
        .await
        .expect("finalize baseline");

    db.execute_unprepared(
        "INSERT INTO seaql_migrations (version, applied_at) \
         VALUES ('m20990101_000001_future', 0)",
    )
    .await
    .expect("inject unknown future migration");
    let error = plan_postgres_migrations(&db)
        .await
        .expect_err("unknown future migration must fail planning");
    assert!(
        error
            .to_string()
            .contains("unknown to this deploy artifact")
    );
}

#[tokio::test]
#[ignore = "requires Docker"]
async fn native_enum_drift_is_rejected() {
    let (db, _container, _port) = setup_empty_pg().await;
    apply_postgres_migrations(&db)
        .await
        .expect("apply baseline");
    finalize_schema_deployment(&db, TEST_RUNTIME_ROLE)
        .await
        .expect("finalize baseline");

    db.execute_unprepared("ALTER TYPE qp_recommendation_status ADD VALUE 'forbidden_test_label'")
        .await
        .expect("inject enum drift");
    assert_manifest_drift(&db, "enums").await;
}

#[tokio::test]
#[ignore = "requires Docker"]
async fn column_definition_drift_is_rejected() {
    let (db, _container, _port) = setup_empty_pg().await;
    apply_postgres_migrations(&db)
        .await
        .expect("apply baseline");
    finalize_schema_deployment(&db, TEST_RUNTIME_ROLE)
        .await
        .expect("finalize baseline");

    db.execute_unprepared(
        "ALTER TABLE quant_report_schedule_state ALTER COLUMN enabled DROP NOT NULL",
    )
    .await
    .expect("inject column drift");
    assert_manifest_drift(&db, "columns").await;
}

#[tokio::test]
#[ignore = "requires Docker"]
async fn index_definition_drift_is_rejected() {
    let (db, _container, _port) = setup_empty_pg().await;
    apply_postgres_migrations(&db)
        .await
        .expect("apply baseline");
    finalize_schema_deployment(&db, TEST_RUNTIME_ROLE)
        .await
        .expect("finalize baseline");

    db.execute_unprepared("DROP INDEX idx_quant_report_schedule_state_due")
        .await
        .expect("inject index drift");
    assert_manifest_drift(&db, "indexes").await;
}

#[tokio::test]
#[ignore = "requires Docker"]
async fn constraint_definition_drift_is_rejected() {
    let (db, _container, _port) = setup_empty_pg().await;
    apply_postgres_migrations(&db)
        .await
        .expect("apply baseline");
    finalize_schema_deployment(&db, TEST_RUNTIME_ROLE)
        .await
        .expect("finalize baseline");

    db.execute_unprepared(
        "ALTER TABLE quant_report_schedule_state \
         DROP CONSTRAINT \"fk-quant_report_schedule_state-decision_policy_snapshot_id\"",
    )
    .await
    .expect("inject constraint drift");
    assert_manifest_drift(&db, "constraints").await;
}

#[tokio::test]
#[ignore = "requires Docker"]
async fn trigger_definition_drift_is_rejected() {
    let (db, _container, _port) = setup_empty_pg().await;
    apply_postgres_migrations(&db)
        .await
        .expect("apply baseline");
    finalize_schema_deployment(&db, TEST_RUNTIME_ROLE)
        .await
        .expect("finalize baseline");

    db.execute_unprepared(
        "DROP TRIGGER trg_quant_report_schedule_state_updated_at \
         ON quant_report_schedule_state",
    )
    .await
    .expect("inject trigger drift");
    assert_manifest_drift(&db, "triggers").await;
}

#[tokio::test]
#[ignore = "requires Docker"]
async fn grant_drift_is_rejected() {
    let (db, _container, _port) = setup_empty_pg().await;
    apply_postgres_migrations(&db)
        .await
        .expect("apply baseline");
    finalize_schema_deployment(&db, TEST_RUNTIME_ROLE)
        .await
        .expect("finalize baseline");

    db.execute_unprepared(&format!(
        "GRANT TRUNCATE ON quant_report_schedule_state TO {TEST_RUNTIME_ROLE}"
    ))
    .await
    .expect("inject grant drift");
    assert_manifest_drift(&db, "grants").await;
}

#[tokio::test]
#[ignore = "requires Docker"]
async fn runtime_identity_verifies_but_cannot_execute_ddl() {
    let (db, _container, port) = setup_empty_pg().await;
    apply_postgres_migrations(&db)
        .await
        .expect("apply baseline");
    finalize_schema_deployment(&db, TEST_RUNTIME_ROLE)
        .await
        .expect("finalize baseline");

    let runtime_url = format!(
        "postgres://{TEST_RUNTIME_ROLE}:quant-pivot-test-runtime@localhost:{port}/test_quant_pivot"
    );
    let runtime = Database::connect(runtime_url)
        .await
        .expect("connect runtime identity");
    verify_schema(&runtime)
        .await
        .expect("runtime identity verifies exact schema");

    let ddl_error = runtime
        .execute_unprepared("CREATE TABLE forbidden_runtime_ddl (id bigint PRIMARY KEY)")
        .await
        .expect_err("runtime DDL must be denied");
    assert!(ddl_error.to_string().contains("permission denied"));
    let ledger_error = runtime
        .execute_unprepared("DELETE FROM schema_migration_audit")
        .await
        .expect_err("runtime migration-ledger mutation must be denied");
    assert!(ledger_error.to_string().contains("permission denied"));
}
