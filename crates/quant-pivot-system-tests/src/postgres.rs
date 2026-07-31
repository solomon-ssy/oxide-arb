//! One-container `PostgreSQL` fixture for repository system contracts.

use std::{
    future::Future,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
};

use anyhow::{Context, Result, anyhow};
use chrono::{DateTime, Utc};
use quant_pivot_migration::apply as apply_postgres_migrations;
use quant_pivot_models::{config::PostgresConfig, security::hash_password};
use quant_pivot_storage::postgres::{PostgresPool, migration::finalize_schema_deployment};
use sea_orm::{ConnectionTrait, DatabaseConnection, DbBackend, Statement};
use testcontainers::{ContainerAsync, ImageExt, runners::AsyncRunner};
use testcontainers_modules::postgres::Postgres;
use tokio::sync::{Mutex, OwnedMutexGuard, Semaphore};

use crate::stack::BOOTSTRAP_ADMIN_PASSWORD;

const MAINTENANCE_DATABASE: &str = "postgres";
const MAX_PARALLEL_POSTGRES_SUITES: usize = 4;
const TEMPLATE_DATABASE: &str = "quant_pivot_repository_template";
const POSTGRES_IMAGE_TAG: &str = "16";
static POSTGRES_SUITE_LIMIT: Semaphore = Semaphore::const_new(MAX_PARALLEL_POSTGRES_SUITES);

tokio::task_local! {
    static ACTIVE_SUITE: Arc<PostgresSuite>;
}

/// Process-local `PostgreSQL` server shared by every repository scenario.
///
/// Scenarios are serialized and receive a database cloned from one immutable,
/// schema-complete template. This preserves both migration-owned boot rows and
/// catalog seeds without deploying the schema for every assertion.
struct PostgresSuite {
    maintenance_config: PostgresConfig,
    maintenance: PostgresPool,
    scenario_lock: Arc<Mutex<()>>,
    next_database: AtomicU64,
    _container: ContainerAsync<Postgres>,
}

impl PostgresSuite {
    async fn start() -> Result<Self> {
        let container = Postgres::default()
            .with_db_name(MAINTENANCE_DATABASE)
            .with_user("postgres")
            .with_password("postgres")
            .with_tag(POSTGRES_IMAGE_TAG)
            .start()
            .await
            .context("start repository PostgreSQL system-test container")?;
        let port = container
            .get_host_port_ipv4(5432)
            .await
            .context("resolve repository PostgreSQL system-test port")?;
        let config = PostgresConfig {
            host: "127.0.0.1".to_owned(),
            port,
            user: "postgres".to_owned(),
            password: "postgres".into(),
            database: MAINTENANCE_DATABASE.to_owned(),
            schema: "public".to_owned(),
            max_connections: 20,
            min_connections: 1,
            connect_timeout_secs: 20,
            acquire_timeout_secs: 30,
            application_name: "quant-pivot-repository-system-tests".to_owned(),
            verify_session_params: true,
            ..PostgresConfig::default()
        };
        let maintenance = PostgresPool::connect_existing(&config)
            .await
            .context("connect repository PostgreSQL maintenance pool")?;

        let mut template_config = config.clone();
        TEMPLATE_DATABASE.clone_into(&mut template_config.database);
        let template = PostgresPool::connect(&template_config)
            .await
            .context("create repository PostgreSQL template database")?;
        apply_postgres_migrations(&template_config, template.connection())
            .await
            .context("apply repository PostgreSQL template migrations")?;
        reseed(template.connection())
            .await
            .context("seed repository PostgreSQL template database")?;
        drop(template);
        terminate_database_connections(&maintenance, TEMPLATE_DATABASE).await?;

        Ok(Self {
            maintenance_config: config,
            maintenance,
            scenario_lock: Arc::new(Mutex::new(())),
            next_database: AtomicU64::new(1),
            _container: container,
        })
    }

    async fn checkout(self: &Arc<Self>) -> Result<(PostgresPool, ScenarioDatabase)> {
        let permit = Arc::clone(&self.scenario_lock).lock_owned().await;
        let sequence = self.next_database.fetch_add(1, Ordering::Relaxed);
        let database = format!("quant_pivot_repository_case_{sequence:03}");
        let statement =
            format!("CREATE DATABASE \"{database}\" WITH TEMPLATE \"{TEMPLATE_DATABASE}\"");
        self.maintenance
            .connection()
            .execute_raw(Statement::from_string(DbBackend::Postgres, statement))
            .await
            .with_context(|| format!("clone repository scenario database `{database}`"))?;
        let mut scenario_config = self.maintenance_config.clone();
        scenario_config.database = database;
        let pool = PostgresPool::connect_existing(&scenario_config)
            .await
            .context("connect isolated repository scenario pool")?;
        Ok((pool, ScenarioDatabase { _permit: permit }))
    }
}

/// Serial guard retaining exclusive access to the shared scenario database.
pub struct ScenarioDatabase {
    _permit: OwnedMutexGuard<()>,
}

/// Database-clock boundary for fixtures that participate in persisted timelines.
pub trait PostgresClock {
    async fn statement_time(&self) -> DateTime<Utc>;
}

impl PostgresClock for DatabaseConnection {
    async fn statement_time(&self) -> DateTime<Utc> {
        self.query_one_raw(Statement::from_string(
            DbBackend::Postgres,
            "SELECT statement_timestamp() AS statement_time",
        ))
        .await
        .expect("read database statement time")
        .expect("database statement time row")
        .try_get::<DateTime<Utc>>("", "statement_time")
        .expect("decode database statement time")
    }
}

/// Run all repository contracts against one disposable `PostgreSQL` server.
pub async fn with_postgres_suite<F>(future: F) -> Result<F::Output>
where
    F: Future,
{
    let _suite_permit = POSTGRES_SUITE_LIMIT
        .acquire()
        .await
        .context("acquire bounded PostgreSQL suite capacity")?;
    let suite = Arc::new(PostgresSuite::start().await?);
    Ok(ACTIVE_SUITE.scope(suite, future).await)
}

/// Run one sequential repository scenario in its own Tokio task while
/// preserving the active suite task-local.
///
/// The aggregate repository runner contains hundreds of independently boxed
/// scenarios. Starting each scenario at a task root prevents their deep
/// database/contract futures from inheriting the aggregate runner's poll stack;
/// the suite mutex still serializes database checkout exactly as before.
pub async fn run_suite_task<F>(future: F) -> Result<F::Output>
where
    F: Future + Send + 'static,
    F::Output: Send + 'static,
{
    let suite = ACTIVE_SUITE
        .try_with(Arc::clone)
        .context("repository scenario task requires an active PostgreSQL suite")?;
    match tokio::spawn(ACTIVE_SUITE.scope(suite, future)).await {
        Ok(output) => Ok(output),
        Err(error) if error.is_panic() => Err(anyhow!(
            "isolated repository scenario task panicked: {error}"
        )),
        Err(error) => Err(error).context("join isolated repository scenario task"),
    }
}

/// Reset and connect the database for one sequential repository scenario.
///
/// This intentionally preserves the former fixture signature so scenario
/// bodies stay behavior-identical while their infrastructure ownership moves
/// into the system-test crate.
pub async fn setup_pg() -> (PostgresPool, ScenarioDatabase) {
    let suite = ACTIVE_SUITE
        .try_with(Arc::clone)
        .expect("repository scenario must run inside with_postgres_suite");
    suite
        .checkout()
        .await
        .expect("reset shared repository PostgreSQL scenario")
}

async fn reseed(db: &DatabaseConnection) -> Result<()> {
    let password_hash = hash_password(BOOTSTRAP_ADMIN_PASSWORD)
        .context("hash repository PostgreSQL bootstrap credential")?;
    finalize_schema_deployment(db, &password_hash)
        .await
        .context("finalize repository PostgreSQL schema")?;
    Ok(())
}

async fn terminate_database_connections(pool: &PostgresPool, database: &str) -> Result<()> {
    pool.connection()
        .execute_raw(Statement::from_sql_and_values(
            DbBackend::Postgres,
            "SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname = $1 AND pid <> pg_backend_pid()",
            [database.into()],
        ))
        .await
        .with_context(|| format!("close PostgreSQL connections to template `{database}`"))?;
    Ok(())
}
