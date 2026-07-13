//! Postgres native enum migration lane (requires Docker).

use quant_pivot_models::schema::pg_enum;
use quant_pivot_storage::postgres::migration::{Migrator, SchemaRunner};
use quant_pivot_test_support::pg::test_pg_config;
use sea_orm::{ConnectionTrait, Database, DbBackend, Statement};
use sea_orm_migration::{MigratorTrait, SchemaManager};
use testcontainers::{ImageExt, runners::AsyncRunner};
use testcontainers_modules::postgres::Postgres;

async fn setup_empty_pg() -> (
    sea_orm::DatabaseConnection,
    testcontainers::ContainerAsync<Postgres>,
) {
    let container = Postgres::default()
        .with_db_name("test_oxide_arb")
        .with_user("postgres")
        .with_password("postgres")
        .with_tag("16")
        .start()
        .await
        .expect("PG container");

    let port = container.get_host_port_ipv4(5432).await.expect("port");
    let config = test_pg_config(port);
    let url = format!(
        "postgres://{}:{}@{}:{}/{}",
        config.user, config.password, config.host, config.port, config.database
    );
    let db = Database::connect(url).await.expect("connect");

    (db, container)
}

async fn enum_type_exists(db: &sea_orm::DatabaseConnection, type_name: &str) -> bool {
    let sql = format!("SELECT 1 FROM pg_type WHERE typname = '{type_name}' AND typtype = 'e'");
    let rows = db
        .query_all(Statement::from_string(DbBackend::Postgres, sql))
        .await
        .expect("query pg_type");
    !rows.is_empty()
}

async fn relation_exists(db: &sea_orm::DatabaseConnection, relation_name: &str) -> bool {
    let rows = db
        .query_all(Statement::from_sql_and_values(
            DbBackend::Postgres,
            "SELECT 1 FROM pg_class WHERE relname = $1",
            [relation_name.into()],
        ))
        .await
        .expect("query pg_class");
    !rows.is_empty()
}

async fn column_exists(
    db: &sea_orm::DatabaseConnection,
    table_name: &str,
    column_name: &str,
) -> bool {
    let rows = db
        .query_all(Statement::from_sql_and_values(
            DbBackend::Postgres,
            "SELECT 1 FROM information_schema.columns \
             WHERE table_schema = 'public' AND table_name = $1 AND column_name = $2",
            [table_name.into(), column_name.into()],
        ))
        .await
        .expect("query information_schema.columns");
    !rows.is_empty()
}

#[tokio::test]
#[ignore = "requires Docker"]
async fn create_schema_registers_all_qp_enums() {
    let (db, _container) = setup_empty_pg().await;
    let manager = SchemaManager::new(&db);

    SchemaRunner::new(&manager)
        .create_schema()
        .await
        .expect("create_schema");

    assert_eq!(
        Migrator::migrations().len(),
        3,
        "pre-production bootstrap must stay a clean initial schema/index/seed lane"
    );

    for spec in pg_enum::specs() {
        assert!(
            enum_type_exists(&db, spec.type_name).await,
            "missing Postgres enum type `{}` after create_schema",
            spec.type_name
        );
    }

    // Sample enum casts (no seed data required).
    db.execute(Statement::from_string(
        DbBackend::Postgres,
        "SELECT 'active'::qp_market_status, 'report_only'::qp_quant_runtime_mode",
    ))
    .await
    .expect("enum cast");

    db.execute(Statement::from_string(
        DbBackend::Postgres,
        "SELECT ARRAY['politics'::qp_market_category, 'crypto'::qp_market_category]",
    ))
    .await
    .expect("enum array cast");

    for table in [
        "catalog_sync_batch",
        "event_catalog_version",
        "market_catalog_version",
        "quant_feature_parity_run",
        "quant_feature_parity_state",
    ] {
        assert!(
            relation_exists(&db, table).await,
            "fresh schema is missing Phase 11.6 table `{table}`"
        );
    }
    for column in ["manifest_hash", "manifest_json", "artifact_bytes_hash"] {
        assert!(
            column_exists(&db, "quant_training_dataset", column).await,
            "fresh training dataset schema is missing `{column}`"
        );
    }

    SchemaRunner::new(&manager)
        .create_indexes()
        .await
        .expect("create_indexes");
    assert!(
        relation_exists(&db, "uq_quant_feature_parity_run_full_window").await,
        "fresh schema is missing the active-window parity uniqueness gate"
    );

    SchemaRunner::new(&manager)
        .drop_schema()
        .await
        .expect("drop_schema");

    for spec in pg_enum::specs() {
        assert!(
            !enum_type_exists(&db, spec.type_name).await,
            "orphan Postgres enum `{}` after drop_schema",
            spec.type_name
        );
    }
}
