//! Fresh-install and schema-mutation `PostgreSQL` system contracts.

use std::time::Duration;

use quant_pivot_migration::{
    acquire_schema_mutation_lease, apply, expected_migrations, inspect_preproduction_postgres,
    plan, release_schema_mutation_lease, reset_preproduction_postgres, verify,
};
use quant_pivot_models::config::PostgresConfig;
use quant_pivot_storage::postgres::PostgresPool;
use quant_pivot_system_tests::resources::fresh_postgres_config;
use sea_orm::Database;
use sea_orm_migration::SchemaManager;
use sqlx::PgPool;

async fn create_empty_postgres(scope: &str) -> PostgresConfig {
    let config = fresh_postgres_config(scope);
    let pool = PostgresPool::connect(&config)
        .await
        .expect("create isolated PostgreSQL database");
    drop(pool);
    config
}

pub async fn empty_postgres_bootstraps_once_and_verifies() {
    let config = PostgresConfig {
        min_connections: 1,
        max_connections: 2,
        verify_session_params: false,
        ..create_empty_postgres("migration_boot").await
    };
    let db = Database::connect(config.try_connection_url().expect("build PostgreSQL URL"))
        .await
        .expect("connect isolated PostgreSQL");

    let initial = plan(&db).await.expect("plan empty PostgreSQL");
    assert!(!initial.migration_ledger_exists);
    assert!(initial.applied_versions.is_empty());
    assert_eq!(initial.pending_migrations, expected_migrations());

    apply(&config, &db).await.expect("apply boot migration");
    verify(&db).await.expect("verify boot migration");
    let schema = SchemaManager::new(&db);
    for table in [
        "policy_revision",
        "policy_approval",
        "policy_activation",
        "decision_policy_snapshot",
        "system_runtime_control",
        "system_runtime_control_transition",
    ] {
        assert!(schema.has_table(table).await.expect("inspect boot table"));
    }

    apply(&config, &db)
        .await
        .expect("reapply boot migration idempotently");
    verify(&db).await.expect("reverify boot migration");
    let complete = plan(&db).await.expect("plan complete PostgreSQL");
    assert!(complete.pending_migrations.is_empty());
}

pub async fn schema_mutation_lease_is_exclusive_and_cancels_after_session_loss() {
    let config = PostgresConfig {
        min_connections: 1,
        max_connections: 2,
        verify_session_params: false,
        ..create_empty_postgres("schema_mutation_lease").await
    };

    let lease = acquire_schema_mutation_lease(&config)
        .await
        .expect("acquire first schema mutation lease");
    let competing = match acquire_schema_mutation_lease(&config).await {
        Ok(lease) => {
            release_schema_mutation_lease(lease)
                .await
                .expect("release unexpected competing lease");
            panic!("second schema mutation lease must fail closed");
        }
        Err(error) => error,
    };
    assert!(competing.to_string().contains("already holds"));

    let maintenance_url = config
        .try_connection_url_with_database("postgres")
        .expect("build maintenance URL");
    let admin = PgPool::connect(&maintenance_url)
        .await
        .expect("connect lease-loss administrator");
    let terminated = sqlx::query_scalar::<_, bool>("SELECT pg_terminate_backend($1)")
        .bind(lease.backend_pid())
        .fetch_one(&admin)
        .await
        .expect("terminate schema mutation session");
    assert!(terminated);
    tokio::time::timeout(Duration::from_secs(8), lease.cancelled())
        .await
        .expect("heartbeat must signal lease loss");
    assert!(lease.ensure_active().is_err());
    assert!(release_schema_mutation_lease(lease).await.is_err());
    admin.close().await;
}

pub async fn reset_rejects_unknown_sessions_and_never_forces_them_closed() {
    let base_config = fresh_postgres_config("schema_mutation_reset");
    let maintenance_url = base_config
        .try_connection_url_with_database("postgres")
        .expect("build PostgreSQL maintenance URL");
    let maintenance = PgPool::connect(&maintenance_url)
        .await
        .expect("connect reset administrator");
    sqlx::query("CREATE ROLE quant_pivot LOGIN CREATEDB PASSWORD 'quant_pivot'")
        .execute(&maintenance)
        .await
        .expect("create exact preproduction owner");
    sqlx::query("CREATE DATABASE quant_pivot OWNER quant_pivot")
        .execute(&maintenance)
        .await
        .expect("create exact preproduction database");
    maintenance.close().await;
    let config = PostgresConfig {
        user: "quant_pivot".to_owned(),
        password: "quant_pivot".into(),
        database: "quant_pivot".to_owned(),
        min_connections: 1,
        max_connections: 2,
        verify_session_params: false,
        ..base_config
    };
    let target_url = config
        .try_connection_url()
        .expect("build PostgreSQL target URL");
    let unknown_session = PgPool::connect(&target_url)
        .await
        .expect("open unknown target session");
    let lease = acquire_schema_mutation_lease(&config)
        .await
        .expect("acquire reset schema mutation lease");

    let error = reset_preproduction_postgres(&config, &lease)
        .await
        .expect_err("an unknown session must deny reset");
    assert!(error.to_string().contains("connections remain"));
    sqlx::query("SELECT 1")
        .execute(&unknown_session)
        .await
        .expect("denied reset must not terminate the unknown session");
    unknown_session.close().await;

    reset_preproduction_postgres(&config, &lease)
        .await
        .expect("reset exact disposable database after sessions stop");
    let inventory = inspect_preproduction_postgres(&config)
        .await
        .expect("inspect recreated disposable database");
    assert!(inventory.database_exists);
    assert_eq!(inventory.object_count, 0);
    release_schema_mutation_lease(lease)
        .await
        .expect("release reset schema mutation lease");
}
