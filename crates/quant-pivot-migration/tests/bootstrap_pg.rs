//! Fresh-install and canonical lifecycle-lease acceptance for `PostgreSQL`.

use std::time::Duration;

use quant_pivot_migration::{
    acquire_lifecycle_lease, apply, expected_migrations, inspect_preproduction_postgres, plan,
    release_lifecycle_lease, reset_preproduction_postgres, verify,
};
use quant_pivot_models::config::PostgresConfig;
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
    let config = PostgresConfig {
        host: "127.0.0.1".to_owned(),
        port,
        user: "postgres".to_owned(),
        password: "postgres".into(),
        database: "quant_pivot_boot".to_owned(),
        min_connections: 1,
        max_connections: 2,
        verify_session_params: false,
        ..PostgresConfig::default()
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
        "system_production_baseline",
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

#[tokio::test]
#[ignore = "requires Docker"]
async fn lifecycle_lease_is_exclusive_and_cancels_after_session_loss() {
    let container = Postgres::default()
        .with_db_name("quant_pivot_lease")
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
    let config = PostgresConfig {
        host: "127.0.0.1".to_owned(),
        port,
        user: "postgres".to_owned(),
        password: "postgres".into(),
        database: "quant_pivot_lease".to_owned(),
        min_connections: 1,
        max_connections: 2,
        verify_session_params: false,
        ..PostgresConfig::default()
    };

    let lease = acquire_lifecycle_lease(&config)
        .await
        .expect("acquire first lifecycle lease");
    let competing = match acquire_lifecycle_lease(&config).await {
        Ok(lease) => {
            release_lifecycle_lease(lease)
                .await
                .expect("release unexpected competing lease");
            panic!("second lifecycle lease must fail closed");
        }
        Err(error) => error,
    };
    assert!(competing.to_string().contains("already holds"));

    let maintenance_url = config
        .try_connection_url_with_database("postgres")
        .expect("build maintenance URL");
    let admin = sqlx::PgPool::connect(&maintenance_url)
        .await
        .expect("connect lease-loss administrator");
    let terminated = sqlx::query_scalar::<_, bool>("SELECT pg_terminate_backend($1)")
        .bind(lease.backend_pid())
        .fetch_one(&admin)
        .await
        .expect("terminate lifecycle session");
    assert!(terminated);
    tokio::time::timeout(Duration::from_secs(8), lease.cancelled())
        .await
        .expect("heartbeat must signal lease loss");
    assert!(lease.ensure_active().is_err());
    assert!(release_lifecycle_lease(lease).await.is_err());
    admin.close().await;
}

#[tokio::test]
#[ignore = "requires Docker"]
async fn reset_rejects_unknown_sessions_and_never_forces_them_closed() {
    let container = Postgres::default()
        .with_db_name("quant_pivot")
        .with_user("quant_pivot")
        .with_password("postgres")
        .with_tag("16")
        .start()
        .await
        .expect("start isolated PostgreSQL");
    let port = container
        .get_host_port_ipv4(5432)
        .await
        .expect("resolve PostgreSQL port");
    let config = PostgresConfig {
        host: "127.0.0.1".to_owned(),
        port,
        user: "quant_pivot".to_owned(),
        password: "postgres".into(),
        database: "quant_pivot".to_owned(),
        min_connections: 1,
        max_connections: 2,
        verify_session_params: false,
        ..PostgresConfig::default()
    };
    let target_url = config
        .try_connection_url()
        .expect("build PostgreSQL target URL");
    let unknown_session = sqlx::PgPool::connect(&target_url)
        .await
        .expect("open unknown target session");
    let lease = acquire_lifecycle_lease(&config)
        .await
        .expect("acquire reset lifecycle lease");

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
    release_lifecycle_lease(lease)
        .await
        .expect("release reset lifecycle lease");
}
