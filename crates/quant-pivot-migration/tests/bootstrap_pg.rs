//! Fresh-install acceptance for the single `PostgreSQL` boot migration.

use quant_pivot_migration::{apply, expected_migrations, plan, verify};
use sea_orm::Database;
use sea_orm_migration::SchemaManager;
use testcontainers::{ImageExt, runners::AsyncRunner};
use testcontainers_modules::postgres::Postgres;

#[tokio::test]
#[ignore = "requires Docker"]
async fn empty_postgres_bootstraps_once_and_verifies() {
    let container = Postgres::default()
        .with_db_name("quant_pivot_boot")
        .with_user("postgres")
        .with_password("postgres")
        .with_tag("16")
        .start()
        .await
        .expect("start isolated PostgreSQL");
    let port = container
        .get_host_port_ipv4(5432)
        .await
        .expect("resolve PostgreSQL port");
    let database_url = format!("postgres://postgres:postgres@127.0.0.1:{port}/quant_pivot_boot");
    let db = Database::connect(database_url)
        .await
        .expect("connect isolated PostgreSQL");

    let initial = plan(&db).await.expect("plan empty PostgreSQL");
    assert!(!initial.migration_ledger_exists);
    assert!(initial.applied_versions.is_empty());
    assert_eq!(initial.pending_migrations, expected_migrations());

    apply(&db).await.expect("apply boot migration");
    verify(&db).await.expect("verify boot migration");
    let schema = SchemaManager::new(&db);
    for table in [
        "policy_revision",
        "policy_approval",
        "policy_activation",
        "decision_policy_snapshot",
        "system_production_baseline",
    ] {
        assert!(schema.has_table(table).await.expect("inspect boot table"));
    }

    apply(&db)
        .await
        .expect("reapply boot migration idempotently");
    verify(&db).await.expect("reverify boot migration");
    let complete = plan(&db).await.expect("plan complete PostgreSQL");
    assert!(complete.pending_migrations.is_empty());
}
