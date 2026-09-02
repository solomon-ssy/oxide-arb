//! Audited `PostgreSQL` migration and schema-verification system contracts.

use crate::infrastructure_removal_catalog::POSTGRES_REMOVED_OBJECTS_QUERY;
use quant_pivot_error::storage::StorageError;
use quant_pivot_migration::{
    apply as apply_postgres_migrations, migrations::Migrator as PostgresMigrator,
    plan as plan_postgres_migrations,
};
use quant_pivot_models::{config::PostgresConfig, security::hash_password};
use quant_pivot_storage::postgres::{
    PostgresPool,
    migration::{
        PostgresSchemaStatus, finalize_schema_deployment as finalize_schema_deployment_with_hash,
        verify_schema,
    },
};
use quant_pivot_system_tests::resources::fresh_postgres_config;
use sea_orm::{ConnectionTrait, Database, DatabaseConnection, DbBackend, Statement};
use sea_orm_migration::MigratorTrait;

const GRANT_DRIFT_ROLE: &str = "quant_pivot_grant_drift";

async fn setup_empty_pg() -> (DatabaseConnection, (), PostgresConfig) {
    let config = fresh_postgres_config("schema_migration");
    let pool = PostgresPool::connect(&config)
        .await
        .expect("create isolated PostgreSQL database");
    let url = config.try_connection_url().expect("PostgreSQL URL");
    let db = Database::connect(url).await.expect("connect");
    drop(pool);
    db.execute_unprepared(&format!(
        "DO $$ BEGIN CREATE ROLE {GRANT_DRIFT_ROLE} NOLOGIN; \
         EXCEPTION WHEN duplicate_object THEN NULL; END $$"
    ))
    .await
    .expect("create runtime role");
    (db, (), config)
}

async fn relation_exists(db: &DatabaseConnection, relation_name: &str) -> bool {
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
    db: &DatabaseConnection,
) -> Result<PostgresSchemaStatus, StorageError> {
    let bootstrap_admin_password_hash =
        hash_password("admin").expect("hash test bootstrap admin password");
    finalize_schema_deployment_with_hash(db, &bootstrap_admin_password_hash).await
}

async fn assert_manifest_drift(db: &DatabaseConnection, section: &str) {
    let error = verify_schema(db)
        .await
        .expect_err("schema drift must fail verification");
    assert!(error.to_string().contains("semantic schema manifest drift"));
    assert!(error.to_string().contains(section));
}

pub async fn migration_plan_empty_database() {
    let (db, _container, _config) = setup_empty_pg().await;

    let plan = plan_postgres_migrations(&db)
        .await
        .expect("plan empty database");

    assert!(!plan.migration_ledger_exists);
    assert!(plan.applied_versions.is_empty());
    assert_eq!(plan.pending_migrations.len(), 1);
    assert!(!relation_exists(&db, "seaql_migrations").await);
    assert!(!relation_exists(&db, "schema_migration_audit").await);
}

pub async fn immutable_baseline_idempotent_rejected() {
    let (db, _container, config) = setup_empty_pg().await;

    apply_postgres_migrations(&config, &db)
        .await
        .expect("apply baseline");
    let status = finalize_schema_deployment(&db)
        .await
        .expect("finalize baseline");
    assert_eq!(status.current_version, "m00000000_000001_bootstrap");
    assert_eq!(status.migration_count, 1);
    assert!(relation_exists(&db, "quant_report_schedule_state").await);
    assert!(relation_exists(&db, "seaql_migrations").await);
    assert!(relation_exists(&db, "schema_migration_audit").await);
    assert!(!relation_exists(&db, "_sqlx_migrations").await);

    let lease_owner_type = db
        .query_one_raw(Statement::from_string(
            DbBackend::Postgres,
            "SELECT pg_catalog.format_type(a.atttypid, a.atttypmod) AS column_type \
             FROM pg_catalog.pg_attribute AS a \
             JOIN pg_catalog.pg_class AS c ON c.oid = a.attrelid \
             JOIN pg_catalog.pg_namespace AS n ON n.oid = c.relnamespace \
             WHERE n.nspname = 'public' \
               AND c.relname = 'quant_research_job' \
               AND a.attname = 'lease_owner' \
               AND NOT a.attisdropped",
        ))
        .await
        .expect("inspect research-job lease owner type")
        .expect("research-job lease owner column");
    assert_eq!(
        lease_owner_type
            .try_get::<String>("", "column_type")
            .expect("research-job lease owner type"),
        "uuid"
    );

    apply_postgres_migrations(&config, &db)
        .await
        .expect("reapply migrations");
    let repeated = finalize_schema_deployment(&db)
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

pub async fn removed_schema_absent() {
    let (db, _container, config) = setup_empty_pg().await;
    apply_postgres_migrations(&config, &db)
        .await
        .expect("apply baseline");
    finalize_schema_deployment(&db)
        .await
        .expect("finalize baseline");

    let residues = db
        .query_all_raw(Statement::from_string(
            DbBackend::Postgres,
            POSTGRES_REMOVED_OBJECTS_QUERY,
        ))
        .await
        .expect("inspect removed PostgreSQL objects");
    let residues = residues
        .iter()
        .map(|row| {
            let kind = row
                .try_get::<String>("", "object_kind")
                .expect("removed object kind");
            let name = row
                .try_get::<String>("", "object_name")
                .expect("removed object name");
            format!("{kind}:{name}")
        })
        .collect::<Vec<_>>();
    assert!(
        residues.is_empty(),
        "fresh PostgreSQL catalog retained removed objects: {residues:?}"
    );
}

pub async fn boot_rejects_unknown_schema() {
    let (db, _container, config) = setup_empty_pg().await;
    db.execute_unprepared("CREATE TABLE legacy_schema_marker (id bigint PRIMARY KEY)")
        .await
        .expect("seed unknown legacy schema");

    let error = apply_postgres_migrations(&config, &db)
        .await
        .expect_err("boot migration must reject a nonempty target");
    assert!(
        error
            .to_string()
            .contains("boot migration requires an empty public schema")
    );
}

pub async fn bootstrap_down_rejected() {
    let (db, _container, config) = setup_empty_pg().await;
    apply_postgres_migrations(&config, &db)
        .await
        .expect("apply migrations");
    finalize_schema_deployment(&db)
        .await
        .expect("finalize bootstrap");
    let before = verify_schema(&db)
        .await
        .expect("verify bootstrap before down");

    let error = PostgresMigrator::down(&db, Some(1))
        .await
        .expect_err("fresh bootstrap must reject schema rollback");
    assert!(
        error
            .to_string()
            .contains("fresh-bootstrap schema has no down path")
    );
    let after = verify_schema(&db)
        .await
        .expect("verify bootstrap after down");
    assert_eq!(after, before);
    assert_eq!(after.migration_count, 1);
    assert!(relation_exists(&db, "quant_report_schedule_state").await);
    assert!(relation_exists(&db, "seaql_migrations").await);
    assert!(relation_exists(&db, "schema_migration_audit").await);
}

pub async fn legacy_sqlx_ledger_forbidden() {
    let (db, _container, config) = setup_empty_pg().await;
    apply_postgres_migrations(&config, &db)
        .await
        .expect("apply baseline");
    finalize_schema_deployment(&db)
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

pub async fn migration_artifact_checksum_rejected() {
    let (db, _container, config) = setup_empty_pg().await;
    apply_postgres_migrations(&config, &db)
        .await
        .expect("apply baseline");
    finalize_schema_deployment(&db)
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

pub async fn unknown_future_native_rejected() {
    let (db, _container, config) = setup_empty_pg().await;
    apply_postgres_migrations(&config, &db)
        .await
        .expect("apply baseline");
    finalize_schema_deployment(&db)
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

pub async fn native_enum_drift_rejected() {
    let (db, _container, config) = setup_empty_pg().await;
    apply_postgres_migrations(&config, &db)
        .await
        .expect("apply baseline");
    finalize_schema_deployment(&db)
        .await
        .expect("finalize baseline");

    db.execute_unprepared("ALTER TYPE qp_recommendation_status ADD VALUE 'forbidden_test_label'")
        .await
        .expect("inject enum drift");
    assert_manifest_drift(&db, "enums").await;
}

pub async fn column_definition_drift_rejected() {
    let (db, _container, config) = setup_empty_pg().await;
    apply_postgres_migrations(&config, &db)
        .await
        .expect("apply baseline");
    finalize_schema_deployment(&db)
        .await
        .expect("finalize baseline");

    db.execute_unprepared(
        "ALTER TABLE quant_report_schedule_state ALTER COLUMN enabled DROP NOT NULL",
    )
    .await
    .expect("inject column drift");
    assert_manifest_drift(&db, "columns").await;
}

pub async fn index_definition_drift_rejected() {
    let (db, _container, config) = setup_empty_pg().await;
    apply_postgres_migrations(&config, &db)
        .await
        .expect("apply baseline");
    finalize_schema_deployment(&db)
        .await
        .expect("finalize baseline");

    db.execute_unprepared("DROP INDEX idx_quant_report_schedule_state_due")
        .await
        .expect("inject index drift");
    assert_manifest_drift(&db, "indexes").await;
}

pub async fn constraint_definition_drift_rejected() {
    let (db, _container, config) = setup_empty_pg().await;
    apply_postgres_migrations(&config, &db)
        .await
        .expect("apply baseline");
    finalize_schema_deployment(&db)
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

pub async fn trigger_definition_drift_rejected() {
    let (db, _container, config) = setup_empty_pg().await;
    apply_postgres_migrations(&config, &db)
        .await
        .expect("apply baseline");
    finalize_schema_deployment(&db)
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

pub async fn grant_drift_is_rejected() {
    let (db, _container, config) = setup_empty_pg().await;
    apply_postgres_migrations(&config, &db)
        .await
        .expect("apply baseline");
    finalize_schema_deployment(&db)
        .await
        .expect("finalize baseline");

    db.execute_unprepared(&format!(
        "GRANT TRUNCATE ON quant_report_schedule_state TO {GRANT_DRIFT_ROLE}"
    ))
    .await
    .expect("inject grant drift");
    assert_manifest_drift(&db, "grants").await;
}
