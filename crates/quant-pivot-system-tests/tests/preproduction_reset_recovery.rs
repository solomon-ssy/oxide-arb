//! Cross-system disposable Fresh Boot, recovery, and backup/restore rehearsal.

use std::{
    env, fs,
    fs::Permissions,
    os::unix::fs::PermissionsExt,
    path::{Path, PathBuf},
    process::Output,
    time::Duration,
};

use chrono::{Duration as ChronoDuration, Utc};
use quant_pivot_migration::inspect_preproduction_postgres;
use quant_pivot_models::config::{ClickHouseConfig, DeployConfig, PostgresConfig};
use quant_pivot_repository::{postgres::PgPolicyRepository, traits::PolicyRepository};
use quant_pivot_storage::{
    cache::{CacheBackend, RedisBackend, connect_pool, count_preproduction_namespace},
    clickhouse::{
        ClickHousePool, active_preproduction_query_count, database_object_count,
        verify_schema as verify_clickhouse_schema,
    },
    postgres::{
        PostgresPool,
        migration::{
            inspect_schema_manifest as inspect_postgres_schema_manifest,
            verify_schema as verify_postgres_schema,
        },
    },
};
use sea_orm::{ConnectionTrait, DatabaseBackend, Statement};
use serde::Deserialize;
use testcontainers::{
    ContainerAsync, GenericImage, ImageExt,
    core::{CmdWaitFor, ExecCommand, WaitFor, wait::HttpWaitStrategy},
    runners::AsyncRunner,
};
use testcontainers_modules::postgres::Postgres;
use tokio::process::Command;
use uuid::Uuid;

const POSTGRES_PASSWORD: &str = "w9-postgres-test-secret";
const POSTGRES_FAULT_PASSWORD: &str = "w9-postgres-fault-test-secret";
const REDIS_PASSWORD: &str = "w9-redis-test-secret";
const BOOTSTRAP_ADMIN_PASSWORD: &str = "w9-bootstrap-admin-test-secret";
const BOOTSTRAP_ADMIN_PASSWORD_ENV: &str = "QUANT_PIVOT_BOOTSTRAP__ADMIN_PASSWORD_FILE";
const JOURNAL_FILE_NAME: &str = "active-operation.json";
const CLEAN_BOOTSTRAP_CONFIRMATION: &str = "DELETE_ALL_PREPRODUCTION_DATA_AND_REBOOTSTRAP";

#[derive(Debug, Deserialize)]
struct ResetFailure {
    failed_stage: String,
}

#[derive(Debug, Deserialize)]
struct ResetJournal {
    operation_id: Uuid,
    nonce: String,
    stage: String,
    failure: Option<ResetFailure>,
}

struct DisposableDirectory {
    path: PathBuf,
}

impl DisposableDirectory {
    fn new() -> Self {
        let path = env::temp_dir().join(format!("quant-pivot-w9-{}", Uuid::now_v7()));
        fs::create_dir(&path).expect("create W9 disposable directory");
        Self { path }
    }

    fn path(&self) -> &Path {
        &self.path
    }
}

impl Drop for DisposableDirectory {
    fn drop(&mut self) {
        if let Err(error) = fs::remove_dir_all(&self.path) {
            eprintln!(
                "failed to remove W9 disposable directory {}: {error}",
                self.path.display()
            );
        }
    }
}

#[tokio::test]
async fn clean_recovers_restores_backups() {
    let workspace = DisposableDirectory::new();
    let postgres = Postgres::default()
        .with_db_name("postgres")
        .with_user("w9_cluster_admin")
        .with_password(POSTGRES_FAULT_PASSWORD)
        .with_tag("16")
        .start()
        .await
        .expect("start disposable PostgreSQL");
    let postgres_port = postgres
        .get_host_port_ipv4(5432)
        .await
        .expect("resolve PostgreSQL port");
    configure_disposable_postgres_user(&postgres).await;
    let clickhouse = GenericImage::new("clickhouse/clickhouse-server", "26.5")
        .with_exposed_port(8123.into())
        .with_wait_for(WaitFor::http(
            HttpWaitStrategy::new("/ping")
                .with_port(8123.into())
                .with_expected_status_code(200u16),
        ))
        .with_env_var("CLICKHOUSE_SKIP_USER_SETUP", "1")
        .with_startup_timeout(Duration::from_mins(2))
        .start()
        .await
        .expect("start disposable ClickHouse");
    let clickhouse_port = clickhouse
        .get_host_port_ipv4(8123)
        .await
        .expect("resolve ClickHouse port");
    create_disposable_clickhouse_user(clickhouse_port).await;
    let redis = GenericImage::new("redis", "7-alpine")
        .with_exposed_port(6379.into())
        .with_wait_for(WaitFor::message_on_stdout("Ready to accept connections"))
        .start()
        .await
        .expect("start disposable Redis");
    let redis_port = redis
        .get_host_port_ipv4(6379)
        .await
        .expect("resolve Redis port");
    configure_disposable_redis_user(&redis).await;

    write_disposable_config(workspace.path(), postgres_port, clickhouse_port, redis_port);
    let bootstrap_password_file = workspace.path().join("bootstrap-admin-password");
    fs::write(&bootstrap_password_file, BOOTSTRAP_ADMIN_PASSWORD)
        .expect("write bootstrap admin password");
    fs::set_permissions(&bootstrap_password_file, Permissions::from_mode(0o600))
        .expect("restrict bootstrap admin password file");
    let journal_file = workspace.path().join(JOURNAL_FILE_NAME);
    let deploy = load_deploy(workspace.path());

    assert_standard_fresh_deployment(workspace.path(), &bootstrap_password_file, &deploy).await;
    let first_operation =
        full_reset_cycle(workspace.path(), &journal_file, &bootstrap_password_file).await;
    assert_eq!(read_journal(&journal_file).operation_id, first_operation);

    assert_reset_rejects_owners(&deploy, workspace.path(), &journal_file).await;

    seed_partial_reset_markers(&deploy).await;
    let confirmation_operation = plan_reset(workspace.path(), &journal_file).await;
    let confirmation_journal = read_journal(&journal_file);
    let wrong_confirmation = run_xtask(
        reset_apply_args(
            workspace.path(),
            &journal_file,
            &confirmation_journal.nonce,
            "DELETE_ONLY_L2",
        ),
        Some(&bootstrap_password_file),
    )
    .await;
    assert!(!wrong_confirmation.status.success());
    assert_output_redacted(&wrong_confirmation);
    assert!(
        String::from_utf8_lossy(&wrong_confirmation.stderr).contains(CLEAN_BOOTSTRAP_CONFIRMATION)
    );
    assert!(postgres_marker_exists(&deploy.db.postgres).await);
    assert!(clickhouse_marker_exists(&deploy).await);
    assert_redis_markers_preserved(&deploy).await;
    let confirmed_reset =
        apply_planned_reset(workspace.path(), &journal_file, &bootstrap_password_file).await;
    assert_success(&confirmed_reset, "confirmed clean bootstrap apply");
    assert_eq!(
        read_journal(&journal_file).operation_id,
        confirmation_operation
    );
    assert_clean_recovery_state(&deploy).await;

    seed_partial_reset_markers(&deploy).await;
    let postgres_failed_operation = plan_reset(workspace.path(), &journal_file).await;
    set_postgres_create_allowed(&postgres, false).await;
    let postgres_failed_output =
        apply_planned_reset(workspace.path(), &journal_file, &bootstrap_password_file).await;
    set_postgres_create_allowed(&postgres, true).await;
    assert_failed_apply(&postgres_failed_output, "PostgreSQL");
    assert_failed_journal(&journal_file, postgres_failed_operation, "applying");
    assert_postgres_failure_state(&deploy).await;

    let postgres_recovered_operation =
        full_reset_cycle(workspace.path(), &journal_file, &bootstrap_password_file).await;
    assert_ne!(postgres_recovered_operation, first_operation);
    assert_ne!(postgres_recovered_operation, postgres_failed_operation);
    assert_clean_recovery_state(&deploy).await;

    seed_partial_reset_markers(&deploy).await;
    let clickhouse_failed_operation = plan_reset(workspace.path(), &journal_file).await;
    set_clickhouse_drop_limit(&deploy, 1).await;
    let clickhouse_failed_output =
        apply_planned_reset(workspace.path(), &journal_file, &bootstrap_password_file).await;
    set_clickhouse_drop_limit(&deploy, 0).await;
    assert_failed_apply(&clickhouse_failed_output, "ClickHouse");
    assert_failed_journal(&journal_file, clickhouse_failed_operation, "postgres_reset");
    assert_clickhouse_failure_state(&deploy).await;

    let clickhouse_recovered_operation =
        full_reset_cycle(workspace.path(), &journal_file, &bootstrap_password_file).await;
    assert_ne!(clickhouse_recovered_operation, clickhouse_failed_operation);
    assert_clean_recovery_state(&deploy).await;

    seed_partial_reset_markers(&deploy).await;
    let redis_failed_operation = plan_reset(workspace.path(), &journal_file).await;
    set_redis_unlink_allowed(&redis, false).await;
    let redis_failed_output =
        apply_planned_reset(workspace.path(), &journal_file, &bootstrap_password_file).await;
    set_redis_unlink_allowed(&redis, true).await;
    assert_failed_apply(&redis_failed_output, "Redis");
    assert_failed_journal(&journal_file, redis_failed_operation, "clickhouse_reset");
    assert_redis_failure_state(&deploy).await;

    let redis_recovered_operation =
        full_reset_cycle(workspace.path(), &journal_file, &bootstrap_password_file).await;
    assert_ne!(redis_recovered_operation, redis_failed_operation);
    assert_clean_recovery_state(&deploy).await;

    verify_postgres_backup_restore(&postgres, &deploy.db.postgres).await;
    verify_clickhouse_backup_restore(&deploy).await;
}

async fn assert_standard_fresh_deployment(
    config_dir: &Path,
    bootstrap_password_file: &Path,
    deploy: &DeployConfig,
) {
    let output = run_xtask(
        vec![
            "postgres-schema".to_owned(),
            "apply".to_owned(),
            "--config-dir".to_owned(),
            path_text(config_dir),
        ],
        Some(bootstrap_password_file),
    )
    .await;
    assert_success(&output, "standard fresh PostgreSQL deployment");
    assert_output_redacted(&output);
    assert!(
        String::from_utf8_lossy(&output.stdout).contains("Policy bundle deployed:"),
        "deploy output must identify the canonical boot policy bundle"
    );

    let postgres = PostgresPool::connect_existing(&deploy.db.postgres)
        .await
        .expect("connect standard fresh PostgreSQL deployment");
    verify_postgres_schema(postgres.connection())
        .await
        .expect("verify standard fresh PostgreSQL schema");
    PgPolicyRepository::new(postgres.connection().clone())
        .load_current_bundle()
        .await
        .expect("load standard fresh policy bundle")
        .expect("standard fresh deployment must create an active policy bundle");
    let boot_facts = postgres
        .connection()
        .query_one_raw(Statement::from_string(
            DatabaseBackend::Postgres,
            "SELECT \
                (SELECT COUNT(*) FROM research_profile_artifact) AS profile_count, \
                (SELECT settlement_write_policy::text \
                   FROM system_runtime_control WHERE id = 1) AS settlement_write_policy",
        ))
        .await
        .expect("query standard fresh deployment facts")
        .expect("standard fresh deployment facts row");
    assert!(
        boot_facts
            .try_get::<i64>("", "profile_count")
            .expect("decode built-in research profile count")
            > 0,
        "standard fresh deployment must seed immutable research profiles"
    );
    assert_eq!(
        boot_facts
            .try_get::<String>("", "settlement_write_policy")
            .expect("decode default settlement write policy"),
        "disabled"
    );
    postgres.close().await;
}

async fn full_reset_cycle(
    config_dir: &Path,
    journal_file: &Path,
    bootstrap_password_file: &Path,
) -> Uuid {
    let operation_id = plan_reset(config_dir, journal_file).await;
    let journal = read_journal(journal_file);
    let apply = run_xtask(
        reset_apply_args(
            config_dir,
            journal_file,
            &journal.nonce,
            CLEAN_BOOTSTRAP_CONFIRMATION,
        ),
        Some(bootstrap_password_file),
    )
    .await;
    assert_success(&apply, "preproduction reset apply");
    assert_output_redacted(&apply);
    let completed = read_journal(journal_file);
    assert_eq!(completed.operation_id, operation_id);
    assert_eq!(completed.stage, "completed");
    assert!(completed.failure.is_none());
    assert_eq!(
        fs::metadata(journal_file)
            .expect("journal metadata")
            .permissions()
            .mode()
            & 0o777,
        0o600
    );

    let verify = run_xtask(
        vec![
            "preproduction-reset".to_owned(),
            "verify".to_owned(),
            "--config-dir".to_owned(),
            path_text(config_dir),
            "--journal-file".to_owned(),
            path_text(journal_file),
            "--operation-id".to_owned(),
            operation_id.to_string(),
        ],
        None,
    )
    .await;
    assert_success(&verify, "preproduction reset verify");
    assert_output_redacted(&verify);
    operation_id
}

async fn apply_planned_reset(
    config_dir: &Path,
    journal_file: &Path,
    bootstrap_password_file: &Path,
) -> Output {
    let journal = read_journal(journal_file);
    tokio::time::timeout(
        Duration::from_mins(2),
        run_xtask(
            reset_apply_args(
                config_dir,
                journal_file,
                &journal.nonce,
                CLEAN_BOOTSTRAP_CONFIRMATION,
            ),
            Some(bootstrap_password_file),
        ),
    )
    .await
    .expect("injected reset apply timeout")
}

fn assert_failed_apply(output: &Output, expected_system: &str) {
    assert!(!output.status.success(), "injected apply must fail");
    assert_output_redacted(output);
    assert!(
        String::from_utf8_lossy(&output.stderr).contains(expected_system),
        "the injected failure must originate at the {expected_system} stage\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
}

fn assert_failed_journal(path: &Path, operation_id: Uuid, failed_stage: &str) {
    let failed = read_journal(path);
    assert_eq!(failed.operation_id, operation_id);
    assert_eq!(failed.stage, "failed");
    assert_eq!(
        failed.failure.expect("failed reset metadata").failed_stage,
        failed_stage
    );
}

async fn plan_reset(config_dir: &Path, journal_file: &Path) -> Uuid {
    let plan = run_xtask(reset_plan_args(config_dir, journal_file), None).await;
    assert_success(&plan, "preproduction reset plan");
    assert_output_redacted(&plan);
    read_journal(journal_file).operation_id
}

fn reset_plan_args(config_dir: &Path, journal_file: &Path) -> Vec<String> {
    vec![
        "preproduction-reset".to_owned(),
        "plan".to_owned(),
        "--config-dir".to_owned(),
        path_text(config_dir),
        "--journal-file".to_owned(),
        path_text(journal_file),
    ]
}

fn reset_apply_args(
    config_dir: &Path,
    journal_file: &Path,
    nonce: &str,
    confirmation: &str,
) -> Vec<String> {
    vec![
        "preproduction-reset".to_owned(),
        "apply".to_owned(),
        "--config-dir".to_owned(),
        path_text(config_dir),
        "--journal-file".to_owned(),
        path_text(journal_file),
        "--confirm-nonce".to_owned(),
        nonce.to_owned(),
        "--confirm".to_owned(),
        confirmation.to_owned(),
    ]
}

async fn run_xtask(args: Vec<String>, bootstrap_password_file: Option<&Path>) -> Output {
    let workspace_root = Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(Path::parent)
        .expect("system-test crate must live under workspace crates/");
    let mut command = Command::new("cargo");
    command
        .current_dir(workspace_root)
        .args(["run", "--quiet", "-p", "quant-pivot-xtask", "--"])
        .args(args)
        .kill_on_drop(true);
    if let Some(path) = bootstrap_password_file {
        command.env(BOOTSTRAP_ADMIN_PASSWORD_ENV, path);
    }
    command.output().await.expect("execute quant-pivot-xtask")
}

fn assert_success(output: &Output, operation: &str) {
    assert!(
        output.status.success(),
        "{operation} failed\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
}

fn assert_output_redacted(output: &Output) {
    for secret in [POSTGRES_PASSWORD, REDIS_PASSWORD, BOOTSTRAP_ADMIN_PASSWORD] {
        assert!(
            !output
                .stdout
                .windows(secret.len())
                .any(|part| part == secret.as_bytes())
        );
        assert!(
            !output
                .stderr
                .windows(secret.len())
                .any(|part| part == secret.as_bytes())
        );
    }
}

fn read_journal(path: &Path) -> ResetJournal {
    serde_json::from_slice(&fs::read(path).expect("read reset journal"))
        .expect("decode reset journal")
}

async fn seed_partial_reset_markers(deploy: &DeployConfig) {
    let postgres = PostgresPool::connect_existing(&deploy.db.postgres)
        .await
        .expect("connect PostgreSQL marker target");
    postgres
        .connection()
        .execute_unprepared("CREATE TABLE w9_reset_marker (value bigint PRIMARY KEY)")
        .await
        .expect("create PostgreSQL reset marker");
    postgres.close().await;

    let clickhouse = ClickHousePool::connect(&deploy.db.clickhouse)
        .await
        .expect("connect ClickHouse marker target");
    clickhouse
        .client()
        .query("CREATE TABLE w9_reset_marker (value UInt8) ENGINE = TinyLog")
        .execute()
        .await
        .expect("create ClickHouse reset marker");
    clickhouse
        .client()
        .query("INSERT INTO w9_reset_marker SELECT number % 256 FROM numbers(4096)")
        .execute()
        .await
        .expect("populate ClickHouse reset marker");

    let redis_pool = connect_pool(&deploy.cache.redis)
        .await
        .expect("connect Redis marker target");
    let qp = RedisBackend::new(redis_pool.clone(), "qp:");
    qp.set("w9-marker", b"owned", Duration::from_mins(10))
        .await
        .expect("seed qp:* marker");
    let foreign = RedisBackend::new(redis_pool, "foreign:");
    foreign
        .set("w9-marker", b"preserve", Duration::from_mins(10))
        .await
        .expect("seed foreign Redis marker");
}

async fn set_clickhouse_drop_limit(deploy: &DeployConfig, bytes: u64) {
    ClickHousePool::from_config(&deploy.db.clickhouse)
        .client()
        .query(&format!(
            "ALTER USER quant_pivot SETTINGS max_table_size_to_drop = {bytes}"
        ))
        .execute()
        .await
        .expect("set disposable ClickHouse drop-size limit");
}

async fn create_disposable_clickhouse_user(port: u16) {
    let admin = ClickHouseConfig {
        deployment_id: "w9-disposable-bootstrap".to_owned(),
        cluster_id: "testcontainer".to_owned(),
        url: format!("http://127.0.0.1:{port}"),
        database: "default".to_owned(),
        user: "default".to_owned(),
        password: "".into(),
        ..ClickHouseConfig::default()
    };
    let client = ClickHousePool::from_config(&admin).client().clone();
    client
        .query("CREATE USER quant_pivot")
        .execute()
        .await
        .expect("create disposable ClickHouse user");
    client
        .query("GRANT CURRENT GRANTS ON *.* TO quant_pivot")
        .execute()
        .await
        .expect("grant disposable ClickHouse user");
}

async fn configure_disposable_postgres_user(container: &ContainerAsync<Postgres>) {
    container
        .exec(
            ExecCommand::new([
                "psql",
                "-U",
                "w9_cluster_admin",
                "-d",
                "postgres",
                "-v",
                "ON_ERROR_STOP=1",
                "-c",
                "CREATE ROLE quant_pivot LOGIN CREATEDB PASSWORD 'w9-postgres-test-secret'",
            ])
            .with_env_vars([("PGPASSWORD", POSTGRES_FAULT_PASSWORD)])
            .with_cmd_ready_condition(CmdWaitFor::exit_code(0)),
        )
        .await
        .expect("configure disposable PostgreSQL application user");
}

async fn set_postgres_create_allowed(container: &ContainerAsync<Postgres>, allowed: bool) {
    let attributes = if allowed { "CREATEDB" } else { "NOCREATEDB" };
    let statement = format!("ALTER ROLE quant_pivot {attributes}");
    let mut execution = container
        .exec(
            ExecCommand::new([
                "psql",
                "-U",
                "w9_cluster_admin",
                "-d",
                "postgres",
                "-v",
                "ON_ERROR_STOP=1",
                "-c",
                statement.as_str(),
            ])
            .with_env_vars([("PGPASSWORD", POSTGRES_FAULT_PASSWORD)]),
        )
        .await
        .expect("execute disposable PostgreSQL role alteration");
    let stderr = execution
        .stderr_to_vec()
        .await
        .expect("read PostgreSQL role alteration stderr");
    assert_eq!(
        execution
            .exit_code()
            .await
            .expect("PostgreSQL role alteration exit code"),
        Some(0),
        "set disposable PostgreSQL role attributes: {}",
        String::from_utf8_lossy(&stderr)
    );
}

async fn configure_disposable_redis_user(container: &ContainerAsync<GenericImage>) {
    let password = format!(">{REDIS_PASSWORD}");
    container
        .exec(
            ExecCommand::new([
                "redis-cli",
                "ACL",
                "SETUSER",
                "quant_pivot",
                "on",
                password.as_str(),
                "~*",
                "+@all",
            ])
            .with_cmd_ready_condition(CmdWaitFor::exit_code(0)),
        )
        .await
        .expect("configure disposable Redis user");
}

async fn set_redis_unlink_allowed(container: &ContainerAsync<GenericImage>, allowed: bool) {
    let permission = if allowed { "+unlink" } else { "-unlink" };
    container
        .exec(
            ExecCommand::new(["redis-cli", "ACL", "SETUSER", "quant_pivot", permission])
                .with_cmd_ready_condition(CmdWaitFor::exit_code(0)),
        )
        .await
        .expect("set disposable Redis UNLINK permission");
}

async fn assert_postgres_failure_state(deploy: &DeployConfig) {
    let inventory = inspect_preproduction_postgres(&deploy.db.postgres)
        .await
        .expect("inspect PostgreSQL failure state");
    assert!(!inventory.database_exists);
    assert!(clickhouse_marker_exists(deploy).await);
    assert_redis_markers_preserved(deploy).await;
}

async fn assert_clickhouse_failure_state(deploy: &DeployConfig) {
    assert!(!postgres_marker_exists(&deploy.db.postgres).await);
    assert!(
        database_object_count(&deploy.db.clickhouse)
            .await
            .expect("count partial ClickHouse objects")
            > 0
    );
    assert_redis_markers_preserved(deploy).await;
}

async fn assert_redis_failure_state(deploy: &DeployConfig) {
    assert!(!postgres_marker_exists(&deploy.db.postgres).await);
    assert!(!clickhouse_marker_exists(deploy).await);
    assert_redis_markers_preserved(deploy).await;
}

async fn assert_redis_markers_preserved(deploy: &DeployConfig) {
    assert_eq!(
        count_preproduction_namespace(&deploy.cache.redis)
            .await
            .expect("count qp:* after partial failure"),
        1
    );
    assert_eq!(
        foreign_redis_marker(deploy).await.as_deref(),
        Some(b"preserve".as_slice())
    );
}

async fn assert_clean_recovery_state(deploy: &DeployConfig) {
    assert!(!postgres_marker_exists(&deploy.db.postgres).await);
    assert!(!clickhouse_marker_exists(deploy).await);
    assert_eq!(
        count_preproduction_namespace(&deploy.cache.redis)
            .await
            .expect("count qp:* after recovery"),
        0
    );
    assert_eq!(
        foreign_redis_marker(deploy).await.as_deref(),
        Some(b"preserve".as_slice())
    );

    let clickhouse = ClickHousePool::connect(&deploy.db.clickhouse)
        .await
        .expect("connect clean ClickHouse target");
    assert_eq!(
        clickhouse
            .client()
            .query("SELECT count() FROM quant_book_l2_ledger")
            .fetch_one::<u64>()
            .await
            .expect("count clean L2 ledger"),
        0
    );
    let now = Utc::now();
    let latency = clickhouse
        .observe_book_latency(now - ChronoDuration::hours(1), now)
        .await
        .expect("observe empty clean-bootstrap L2 readiness");
    assert_eq!(latency.event_count, 0);

    let postgres = PostgresPool::connect_existing(&deploy.db.postgres)
        .await
        .expect("connect clean PostgreSQL target");
    let evidence = postgres
        .connection()
        .query_one_raw(Statement::from_string(
            DatabaseBackend::Postgres,
            "SELECT (SELECT COUNT(*) FROM quant_source_slice) + \
                    (SELECT COUNT(*) FROM quant_training_dataset) + \
                    (SELECT COUNT(*) FROM quant_backtest_report) + \
                    (SELECT COUNT(*) FROM quant_feature_parity_run) + \
                    (SELECT COUNT(*) FROM quant_trade_policy_validation) + \
                    (SELECT COUNT(*) FROM quant_research_readiness_evidence) AS evidence_count",
        ))
        .await
        .expect("count invalidated research evidence")
        .expect("research evidence count row");
    assert_eq!(
        evidence
            .try_get::<i64>("", "evidence_count")
            .expect("decode research evidence count"),
        0
    );
    postgres.close().await;
}

async fn assert_reset_rejects_owners(
    deploy: &DeployConfig,
    config_dir: &Path,
    journal_file: &Path,
) {
    let postgres = PostgresPool::connect_existing(&deploy.db.postgres)
        .await
        .expect("hold project PostgreSQL connection");
    postgres
        .connection()
        .execute_unprepared("SELECT 1")
        .await
        .expect("activate held PostgreSQL connection");
    let postgres_denial = run_xtask(reset_plan_args(config_dir, journal_file), None).await;
    assert!(!postgres_denial.status.success());
    assert_output_redacted(&postgres_denial);
    assert!(
        String::from_utf8_lossy(&postgres_denial.stderr)
            .contains("PostgreSQL target connections remain")
    );
    postgres.close().await;

    let clickhouse = ClickHousePool::from_config(&deploy.db.clickhouse);
    let query_client = clickhouse.client().clone();
    let active_query_id = format!("reset-preflight-{}", Uuid::now_v7());
    let task_query_id = active_query_id.clone();
    let active_query = tokio::spawn(async move {
        query_client
            .query(
                "SELECT sleepEachRow(0.02) FROM numbers(1000000) \
                 SETTINGS max_block_size = 1",
            )
            .with_setting("query_id", task_query_id)
            .fetch_all::<u8>()
            .await
    });
    let mut observed = false;
    for _ in 0..100 {
        if active_preproduction_query_count(&deploy.db.clickhouse)
            .await
            .expect("inspect active ClickHouse query")
            != 0
        {
            observed = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert!(
        observed,
        "long-running project query must become observable"
    );
    let clickhouse_denial = run_xtask(reset_plan_args(config_dir, journal_file), None).await;
    assert!(!clickhouse_denial.status.success());
    assert_output_redacted(&clickhouse_denial);
    let clickhouse_stderr = String::from_utf8_lossy(&clickhouse_denial.stderr);
    assert!(
        clickhouse_stderr.contains("active ClickHouse project queries"),
        "unexpected ClickHouse reset denial:\n{clickhouse_stderr}"
    );
    clickhouse
        .client()
        .query("KILL QUERY WHERE query_id = ? SYNC")
        .bind(&active_query_id)
        .execute()
        .await
        .expect("kill active ClickHouse preflight fixture");
    active_query.abort();
    let _ = active_query.await;
    let mut drained = false;
    for _ in 0..250 {
        if active_preproduction_query_count(&deploy.db.clickhouse)
            .await
            .expect("inspect cancelled ClickHouse query")
            == 0
        {
            drained = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert!(
        drained,
        "cancelled ClickHouse query must leave system.processes"
    );
}

async fn postgres_marker_exists(config: &PostgresConfig) -> bool {
    let pool = PostgresPool::connect_existing(config)
        .await
        .expect("connect PostgreSQL marker probe");
    let row = pool
        .connection()
        .query_one_raw(Statement::from_string(
            DatabaseBackend::Postgres,
            "SELECT to_regclass('public.w9_reset_marker') IS NOT NULL AS exists",
        ))
        .await
        .expect("query PostgreSQL marker")
        .expect("PostgreSQL marker result");
    let exists = row
        .try_get::<bool>("", "exists")
        .expect("decode marker result");
    pool.close().await;
    exists
}

async fn clickhouse_marker_exists(deploy: &DeployConfig) -> bool {
    let pool = ClickHousePool::from_config(&deploy.db.clickhouse);
    pool.client()
        .query(
            "SELECT count() FROM system.tables \
             WHERE database = 'quant_pivot' AND name = 'w9_reset_marker'",
        )
        .fetch_one::<u64>()
        .await
        .expect("query ClickHouse marker")
        == 1
}

async fn foreign_redis_marker(deploy: &DeployConfig) -> Option<Vec<u8>> {
    let pool = connect_pool(&deploy.cache.redis)
        .await
        .expect("connect Redis foreign marker probe");
    RedisBackend::new(pool, "foreign:")
        .get("w9-marker")
        .await
        .expect("read foreign Redis marker")
}

async fn verify_postgres_backup_restore(
    container: &ContainerAsync<Postgres>,
    source_config: &PostgresConfig,
) {
    let command = "pg_dump -U quant_pivot -Fc -f /tmp/w9.dump quant_pivot && \
                   createdb -U quant_pivot -T template0 quant_pivot_restore && \
                   pg_restore -U quant_pivot --exit-on-error -d quant_pivot_restore /tmp/w9.dump && \
                   vacuumdb -U quant_pivot --analyze quant_pivot_restore";
    let execution = container
        .exec(
            ExecCommand::new(["sh", "-c", command])
                .with_env_vars([("PGPASSWORD", POSTGRES_PASSWORD)])
                .with_cmd_ready_condition(CmdWaitFor::exit_code(0)),
        )
        .await
        .expect("execute PostgreSQL backup/restore");
    assert_eq!(
        execution
            .exit_code()
            .await
            .expect("PostgreSQL backup exit code"),
        Some(0)
    );

    let source = PostgresPool::connect_existing(source_config)
        .await
        .expect("connect PostgreSQL backup source");
    let mut restore_config = source_config.clone();
    "quant_pivot_restore".clone_into(&mut restore_config.database);
    let restored = PostgresPool::connect_existing(&restore_config)
        .await
        .expect("connect PostgreSQL restored target");
    let source_manifest = inspect_postgres_schema_manifest(source.connection())
        .await
        .expect("inspect PostgreSQL backup source");
    let restored_manifest = inspect_postgres_schema_manifest(restored.connection())
        .await
        .expect("inspect PostgreSQL restored target");
    if source_manifest["constraints"] != restored_manifest["constraints"] {
        let source_constraints = source_manifest["constraints"]
            .as_array()
            .expect("source constraints array");
        let restored_constraints = restored_manifest["constraints"]
            .as_array()
            .expect("restored constraints array");
        let differences = source_constraints
            .iter()
            .zip(restored_constraints)
            .filter(|(source, restored)| source != restored)
            .map(|(source, restored)| {
                (
                    source["name"].as_str().unwrap_or("<missing>"),
                    &source["definition"],
                    &restored["definition"],
                )
            })
            .collect::<Vec<_>>();
        panic!(
            "PostgreSQL restored constraints differ: source_count={} restored_count={} differences={differences:?}",
            source_constraints.len(),
            restored_constraints.len()
        );
    }
    let source_status = verify_postgres_schema(source.connection())
        .await
        .expect("verify PostgreSQL backup source");
    let restored_status = verify_postgres_schema(restored.connection())
        .await
        .expect("verify PostgreSQL restored target");
    assert_eq!(source_status, restored_status);
    let source_bundle = PgPolicyRepository::new(source.connection().clone())
        .load_current_bundle()
        .await
        .expect("load source policy bundle")
        .expect("source policy bundle");
    let restored_bundle = PgPolicyRepository::new(restored.connection().clone())
        .load_current_bundle()
        .await
        .expect("load restored policy bundle")
        .expect("restored policy bundle");
    assert_eq!(source_bundle.generation, restored_bundle.generation);
    assert_eq!(source_bundle.snapshot_hash, restored_bundle.snapshot_hash);
    source.close().await;
    restored.close().await;
}

async fn verify_clickhouse_backup_restore(deploy: &DeployConfig) {
    let source = ClickHousePool::connect(&deploy.db.clickhouse)
        .await
        .expect("connect ClickHouse backup source");
    source
        .client()
        .query("BACKUP DATABASE quant_pivot TO File('w9-disposable.zip')")
        .execute()
        .await
        .expect("backup ClickHouse database");
    source
        .client()
        .query(
            "RESTORE DATABASE quant_pivot AS quant_pivot_restore \
             FROM File('w9-disposable.zip')",
        )
        .execute()
        .await
        .expect("restore ClickHouse database");

    let source_status = verify_clickhouse_schema(&deploy.db.clickhouse)
        .await
        .expect("verify ClickHouse backup source");
    let mut restore_config = deploy.db.clickhouse.clone();
    "quant_pivot_restore".clone_into(&mut restore_config.database);
    let restored_status = verify_clickhouse_schema(&restore_config)
        .await
        .expect("verify ClickHouse restored target");
    assert_eq!(source_status, restored_status);
}

fn write_disposable_config(
    directory: &Path,
    postgres_port: u16,
    clickhouse_port: u16,
    redis_port: u16,
) {
    let contents = format!(
        r#"
[deployment]
environment = "w9-disposable"

[db.postgres]
host = "127.0.0.1"
port = {postgres_port}
user = "quant_pivot"
password = "{POSTGRES_PASSWORD}"
database = "quant_pivot"
min_connections = 1
max_connections = 4
verify_session_params = false

[db.clickhouse]
deployment_id = "w9-disposable"
cluster_id = "testcontainer"
url = "http://127.0.0.1:{clickhouse_port}"
database = "quant_pivot"
user = "quant_pivot"
password = ""

[cache.redis]
host = "127.0.0.1"
port = {redis_port}
user = "quant_pivot"
password = "{REDIS_PASSWORD}"
database = 0
key_prefix = "qp:"
"#
    );
    fs::write(directory.join("quant-pivot.local.toml"), contents)
        .expect("write disposable deploy config");
}

fn load_deploy(directory: &Path) -> DeployConfig {
    DeployConfig::load_for_migration(&path_text(directory)).expect("load disposable deploy config")
}

fn path_text(path: &Path) -> String {
    path.to_str()
        .expect("disposable path must be valid UTF-8")
        .to_owned()
}
