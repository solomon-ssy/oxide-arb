use anyhow::{Context, Result, bail};
use clap::{Args, Parser, Subcommand};
use quant_pivot_migration::{apply as apply_postgres_migrations, plan as plan_postgres_migrations};
use quant_pivot_models::{config::DeployConfig, security::hash_password};
use quant_pivot_storage::{
    clickhouse::{
        apply_offline_schema_migrations, apply_online_schema_migrations, plan_schema,
        render_schema_manifest as render_clickhouse_schema_manifest, verify_schema,
    },
    postgres::{
        PostgresPool,
        migration::{
            finalize_schema_deployment, inspect_schema_manifest, render_schema_manifest,
            verify_schema as verify_postgres_schema,
        },
    },
};
use rustls::crypto::aws_lc_rs;
use std::{
    env, fs,
    path::PathBuf,
    process::{Command, ExitCode, Stdio},
};
use zeroize::Zeroizing;

const POSTGRES_MIGRATION_PASSWORD_ENV: &str = "QUANT_PIVOT_MIGRATION__POSTGRES_PASSWORD";
const CLICKHOUSE_MIGRATION_PASSWORD_ENV: &str = "QUANT_PIVOT_MIGRATION__CLICKHOUSE_PASSWORD";
const BOOTSTRAP_ADMIN_PASSWORD_FILE_ENV: &str = "QUANT_PIVOT_BOOTSTRAP__ADMIN_PASSWORD_FILE";

const DOCKER_SUITES: &[(&str, &str)] = &[
    ("quant-pivot-storage", "migration_pg"),
    ("quant-pivot-repository", "pg_catalog_ledger"),
    ("quant-pivot-repository", "pg_feature_parity"),
    ("quant-pivot-repository", "pg_bootstrap"),
    ("quant-pivot-repository", "pg_account_capital"),
    ("quant-pivot-repository", "pg_attribution"),
    ("quant-pivot-repository", "pg_backtest_path_set"),
    ("quant-pivot-repository", "pg_basis_alert"),
    ("quant-pivot-repository", "pg_calibration_artifact"),
    ("quant-pivot-repository", "pg_entry_condition_evaluation"),
    ("quant-pivot-repository", "pg_equity_snapshot"),
    ("quant-pivot-repository", "pg_factor_revision"),
    ("quant-pivot-repository", "pg_market_selection"),
    ("quant-pivot-repository", "pg_market_linkage"),
    ("quant-pivot-repository", "pg_market_page"),
    ("quant-pivot-repository", "pg_governance"),
    ("quant-pivot-repository", "pg_rbac"),
    ("quant-pivot-repository", "pg_report_scheduler"),
    ("quant-pivot-repository", "pg_training_dataset"),
    ("quant-pivot-repository", "pg_model_registry"),
    ("quant-pivot-repository", "pg_research_job"),
    ("quant-pivot-repository", "pg_research_readiness"),
    ("quant-pivot-repository", "pg_trade_policy_trial"),
    ("quant-pivot-repository", "pg_backtest_report"),
    ("quant-pivot-repository", "pg_comparison_report"),
    ("quant-pivot-repository", "pg_domain_projection"),
    ("quant-pivot-repository", "pg_execution_submission"),
    ("quant-pivot-repository", "portfolio_optimizer_meta"),
    ("quant-pivot-repository", "ch_fact_read_pit"),
    ("quant-pivot-storage", "redis_integration"),
    ("quant-pivot-storage", "clickhouse_integration"),
    ("quant-pivot-storage", "cache_tiered_integration"),
    ("quant-pivot-core", "health_checker"),
    ("quant-pivot-core", "market_selection_e2e"),
    ("quant-pivot-core", "bootstrap_registry_e2e"),
    ("quant-pivot-core", "equity_snapshot"),
    ("quant-pivot-core", "factor_plane_e2e"),
    ("quant-pivot-core", "governance_e2e"),
    ("quant-pivot-core", "model_train_backtest_e2e"),
    ("quant-pivot-core", "report_pipeline_e2e"),
    ("quant-pivot-web", "web"),
];

const NETWORK_SUITES: &[(&str, &str)] = &[
    ("quant-pivot-api", "http_gamma_wiremock"),
    ("quant-pivot-api", "http_data_api_wiremock"),
];

const UI_E2E_SERVER_TEST: &str = "ui_e2e_server_impl::serve_protected_ui_e2e";

#[derive(Parser)]
#[command(name = "quant-pivot-xtask")]
#[command(about = "Task runner for quant-pivot", long_about = None)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Plan, apply online-safe migrations, or verify the `ClickHouse` schema.
    #[command(name = "clickhouse-schema")]
    ClickHouseSchema {
        #[command(subcommand)]
        command: ClickHouseSchemaCommand,
    },
    /// Plan/apply/verify immutable migrations or generate their semantic manifest.
    #[command(name = "postgres-schema")]
    PostgresSchema {
        #[command(subcommand)]
        command: PostgresSchemaCommand,
    },
    /// Run the default workspace tests, Docker suites, and network suites.
    #[command(name = "test-full")]
    Full,
    /// Run testcontainers-based integration tests (requires Docker daemon).
    #[command(name = "test-docker")]
    Docker,
    /// Run network-shaped API tests that are ignored by default.
    #[command(name = "test-network")]
    Network,
    /// Derive the report hard ceiling from the recent 30-day catalog-visible p99.
    #[command(name = "report-capacity-ceiling")]
    ReportCapacityCeiling {
        #[arg(long)]
        catalog_visible_p99: u64,
    },
}

#[derive(Subcommand)]
enum ClickHouseSchemaCommand {
    /// Print pending migrations without changing the target database.
    Plan(ConfigDirArgs),
    /// Create the database and apply pending online-safe migrations.
    ApplyOnline(ConfigDirArgs),
    /// Apply pending offline migrations after proving destructive source tables are empty.
    ApplyOffline(ConfigDirArgs),
    /// Verify the migration ledger and runtime schema contract read-only.
    Verify(ConfigDirArgs),
    /// Generate the normalized SHOW CREATE semantic manifest after migrations.
    Manifest(ConfigDirArgs),
}

#[derive(Subcommand)]
enum PostgresSchemaCommand {
    /// Print pending migrations without mutating the target database.
    Plan(ConfigDirArgs),
    /// Apply pending migrations and versioned catalog seeds with deploy credentials.
    Apply(ConfigDirArgs),
    /// Verify checksums, seeds, and the runtime schema contract read-only.
    Verify(ConfigDirArgs),
    /// Generate the normalized `pg_catalog` manifest after applying migrations.
    Manifest(ConfigDirArgs),
    /// Regenerate only the immutable compiled migration-artifact manifest.
    MigrationManifest,
}

#[derive(Args)]
struct ConfigDirArgs {
    /// Directory containing quant-pivot.toml.
    #[arg(long, env = "QUANT_PIVOT_CONFIG_DIR", default_value = "config")]
    config_dir: PathBuf,
}

#[tokio::main]
async fn main() -> ExitCode {
    if aws_lc_rs::default_provider().install_default().is_err() {
        eprintln!("rustls crypto provider already installed");
    }
    match run().await {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("error: {error:#}");
            ExitCode::FAILURE
        }
    }
}

async fn run() -> Result<()> {
    match Cli::parse().command {
        Commands::ClickHouseSchema { command } => clickhouse_schema(command).await,
        Commands::PostgresSchema { command } => postgres_schema(command).await,
        Commands::Full => test_full(),
        Commands::Docker => test_docker(),
        Commands::Network => test_network(),
        Commands::ReportCapacityCeiling {
            catalog_visible_p99,
        } => {
            println!("{}", report_capacity_ceiling(catalog_visible_p99)?);
            Ok(())
        }
    }
}

async fn postgres_schema(command: PostgresSchemaCommand) -> Result<()> {
    let args = match &command {
        PostgresSchemaCommand::Plan(args)
        | PostgresSchemaCommand::Apply(args)
        | PostgresSchemaCommand::Verify(args)
        | PostgresSchemaCommand::Manifest(args) => Some(args),
        PostgresSchemaCommand::MigrationManifest => None,
    };
    let Some(args) = args else {
        write_postgres_migration_manifest()?;
        return Ok(());
    };
    let config_dir = args
        .config_dir
        .to_str()
        .context("PostgreSQL schema config directory is not valid UTF-8")?;
    let deploy = DeployConfig::load(config_dir).context("load deploy config")?;
    match command {
        PostgresSchemaCommand::Plan(_) => {
            let password = migration_password(POSTGRES_MIGRATION_PASSWORD_ENV)?;
            let config = deploy.db.postgres.migration_connection(&password);
            let pool = PostgresPool::connect_existing(&config)
                .await
                .context("connect PostgreSQL for migration plan")?;
            let plan = plan_postgres_migrations(pool.connection())
                .await
                .context("plan PostgreSQL schema")?;
            println!("migration_ledger_exists={}", plan.migration_ledger_exists);
            println!("applied_versions={:?}", plan.applied_versions);
            for migration in plan.pending_migrations {
                println!(
                    "pending_migration version={} checksum={}",
                    migration.version, migration.checksum
                );
            }
            pool.close().await;
            Ok(())
        }
        PostgresSchemaCommand::Apply(_) => {
            let password = migration_password(POSTGRES_MIGRATION_PASSWORD_ENV)?;
            let bootstrap_admin_password_hash = bootstrap_admin_password_hash()?;
            let pool = PostgresPool::connect_migration(&deploy.db.postgres, &password)
                .await
                .context("connect PostgreSQL migration identity")?;
            apply_postgres_migrations(pool.connection())
                .await
                .context("apply audited SeaORM PostgreSQL migrations")?;
            let status = finalize_schema_deployment(
                pool.connection(),
                &deploy.db.postgres.user,
                &bootstrap_admin_password_hash,
            )
            .await
            .context("finalize PostgreSQL schema deployment")?;
            println!(
                "PostgreSQL schema deployed: version={}, migrations={}, tables={}, indexes={}",
                status.current_version,
                status.migration_count,
                status.required_table_count,
                status.required_index_count
            );
            pool.close().await;
            Ok(())
        }
        PostgresSchemaCommand::Verify(_) => {
            let pool = PostgresPool::connect_existing(&deploy.db.postgres)
                .await
                .context("connect PostgreSQL runtime identity")?;
            let status = verify_postgres_schema(pool.connection())
                .await
                .context("verify PostgreSQL schema")?;
            println!(
                "PostgreSQL schema verified: version={}, migrations={}, tables={}, indexes={}",
                status.current_version,
                status.migration_count,
                status.required_table_count,
                status.required_index_count
            );
            pool.close().await;
            Ok(())
        }
        PostgresSchemaCommand::Manifest(_) => {
            let password = migration_password(POSTGRES_MIGRATION_PASSWORD_ENV)?;
            let pool = PostgresPool::connect_migration(&deploy.db.postgres, &password)
                .await
                .context("connect PostgreSQL migration identity")?;
            let manifest = inspect_schema_manifest(pool.connection())
                .await
                .context("inspect PostgreSQL semantic manifest")?;
            let rendered =
                render_schema_manifest(&manifest).context("render PostgreSQL semantic manifest")?;
            let path = workspace_root()?
                .join("schema")
                .join("postgres")
                .join("manifest.json");
            fs::create_dir_all(path.parent().context("manifest path has no parent")?)
                .context("create PostgreSQL manifest directory")?;
            fs::write(&path, rendered).with_context(|| format!("write {}", path.display()))?;
            let migration_path = write_postgres_migration_manifest()?;
            println!("generated {}", path.display());
            println!("generated {}", migration_path.display());
            pool.close().await;
            Ok(())
        }
        PostgresSchemaCommand::MigrationManifest => Ok(()),
    }
}

fn write_postgres_migration_manifest() -> Result<PathBuf> {
    let path = workspace_root()?
        .join("schema")
        .join("postgres")
        .join("migrations.json");
    fs::create_dir_all(
        path.parent()
            .context("migration manifest path has no parent")?,
    )
    .context("create PostgreSQL migration manifest directory")?;
    let manifest =
        quant_pivot_migration::render_manifest().context("render PostgreSQL migration manifest")?;
    fs::write(&path, manifest).with_context(|| format!("write {}", path.display()))?;
    println!("generated {}", path.display());
    Ok(path)
}

async fn clickhouse_schema(command: ClickHouseSchemaCommand) -> Result<()> {
    let config_dir = match &command {
        ClickHouseSchemaCommand::Plan(args)
        | ClickHouseSchemaCommand::ApplyOnline(args)
        | ClickHouseSchemaCommand::ApplyOffline(args)
        | ClickHouseSchemaCommand::Verify(args)
        | ClickHouseSchemaCommand::Manifest(args) => &args.config_dir,
    };
    let config_dir = config_dir
        .to_str()
        .context("ClickHouse schema config directory is not valid UTF-8")?;
    let deploy = DeployConfig::load(config_dir).context("load deploy config")?;
    let config = &deploy.db.clickhouse;
    match command {
        ClickHouseSchemaCommand::Plan(_) => {
            let password = migration_password(CLICKHOUSE_MIGRATION_PASSWORD_ENV)?;
            let migration_config = config.migration_connection(&password);
            let plan = plan_schema(&migration_config)
                .await
                .context("plan ClickHouse schema")?;
            println!("database_exists={}", plan.database_exists);
            println!("migration_ledger_exists={}", plan.migration_ledger_exists);
            println!("applied_versions={:?}", plan.applied_versions);
            if plan.pending_migrations.is_empty() {
                println!("pending_migrations=[]");
            } else {
                for migration in plan.pending_migrations {
                    println!(
                        "pending_migration version={} name={} safety={:?} checksum={}",
                        migration.version, migration.name, migration.safety, migration.checksum
                    );
                }
            }
            Ok(())
        }
        ClickHouseSchemaCommand::ApplyOnline(_) => {
            let password = migration_password(CLICKHOUSE_MIGRATION_PASSWORD_ENV)?;
            let migration_config = config.migration_connection(&password);
            let status = apply_online_schema_migrations(&migration_config)
                .await
                .context("deploy ClickHouse schema")?;
            println!(
                "ClickHouse schema deployed: version={}, required_objects={}",
                status.current_version, status.required_object_count
            );
            Ok(())
        }
        ClickHouseSchemaCommand::ApplyOffline(_) => {
            let password = migration_password(CLICKHOUSE_MIGRATION_PASSWORD_ENV)?;
            let migration_config = config.migration_connection(&password);
            let status = apply_offline_schema_migrations(&migration_config)
                .await
                .context("deploy offline ClickHouse schema")?;
            println!(
                "ClickHouse offline schema deployed: version={}, required_objects={}",
                status.current_version, status.required_object_count
            );
            Ok(())
        }
        ClickHouseSchemaCommand::Verify(_) => {
            let status = verify_schema(config)
                .await
                .context("verify ClickHouse schema")?;
            println!(
                "ClickHouse schema verified: version={}, required_objects={}",
                status.current_version, status.required_object_count
            );
            Ok(())
        }
        ClickHouseSchemaCommand::Manifest(_) => {
            let password = migration_password(CLICKHOUSE_MIGRATION_PASSWORD_ENV)?;
            let migration_config = config.migration_connection(&password);
            let rendered = render_clickhouse_schema_manifest(&migration_config)
                .await
                .context("render ClickHouse semantic schema manifest")?;
            let path = workspace_root()?
                .join("schema")
                .join("clickhouse")
                .join("manifest.json");
            fs::create_dir_all(path.parent().context("manifest path has no parent")?)
                .context("create ClickHouse manifest directory")?;
            fs::write(&path, rendered).with_context(|| format!("write {}", path.display()))?;
            println!("generated {}", path.display());
            Ok(())
        }
    }
}

fn migration_password(variable: &'static str) -> Result<Zeroizing<String>> {
    let password = env::var(variable).with_context(|| {
        format!("deployment secret `{variable}` is required for schema commands")
    })?;
    if password.is_empty() {
        bail!("deployment secret `{variable}` must not be empty");
    }
    Ok(Zeroizing::new(password))
}

fn bootstrap_admin_password_hash() -> Result<Zeroizing<String>> {
    let path = env::var_os(BOOTSTRAP_ADMIN_PASSWORD_FILE_ENV)
        .map(PathBuf::from)
        .with_context(|| {
            format!(
                "deployment secret file `{BOOTSTRAP_ADMIN_PASSWORD_FILE_ENV}` is required for PostgreSQL schema apply"
            )
        })?;
    let metadata = fs::metadata(&path)
        .with_context(|| format!("inspect bootstrap admin password file {}", path.display()))?;
    if !metadata.is_file() {
        bail!(
            "bootstrap admin password path {} is not a regular file",
            path.display()
        );
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;

        if metadata.permissions().mode() & 0o077 != 0 {
            bail!(
                "bootstrap admin password file {} must not be accessible by group or others",
                path.display()
            );
        }
    }

    let password = fs::read_to_string(&path)
        .with_context(|| format!("read bootstrap admin password file {}", path.display()))?;
    let password = Zeroizing::new(password.trim_end_matches(['\r', '\n']).to_owned());
    let character_count = password.chars().count();
    if !(16..=256).contains(&character_count) {
        bail!("bootstrap admin password must contain 16..=256 characters");
    }
    if password.eq_ignore_ascii_case("admin") || password.as_str() == "quant-pivot" {
        bail!("bootstrap admin password is a forbidden template value");
    }
    hash_password(password.as_str())
        .map(Zeroizing::new)
        .context("hash bootstrap admin password with Argon2id")
}

fn report_capacity_ceiling(catalog_visible_p99: u64) -> Result<u64> {
    const MINIMUM: u64 = 100_000;
    const QUANTUM: u64 = 1_000;

    let doubled = catalog_visible_p99
        .checked_mul(2)
        .context("catalog-visible p99 overflow while applying the 2x runway")?;
    let raw = doubled.max(MINIMUM);
    raw.checked_add(QUANTUM - 1)
        .map(|value| value / QUANTUM * QUANTUM)
        .context("catalog-visible report capacity ceiling overflow")
}

fn workspace_root() -> Result<PathBuf> {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(|crates| crates.parent())
        .context("quant-pivot-xtask must live under crates/")
        .map(PathBuf::from)
}

fn ensure_docker_daemon() -> Result<()> {
    let status = Command::new("docker")
        .arg("info")
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .context("failed to run `docker info`")?;
    if status.success() {
        Ok(())
    } else {
        bail!("Docker daemon is not running (required for testcontainers)");
    }
}

fn test_full() -> Result<()> {
    test_workspace()?;
    test_docker()?;
    test_network()?;

    eprintln!("All tests passed.");
    Ok(())
}

fn test_workspace() -> Result<()> {
    let root = workspace_root()?;

    eprintln!("== workspace :: cargo test --workspace ==");
    let status = Command::new("cargo")
        .current_dir(&root)
        .args(["test", "--workspace"])
        .status()
        .context("failed to run workspace test suite")?;
    if !status.success() {
        bail!("workspace test suite failed");
    }

    Ok(())
}

fn test_docker() -> Result<()> {
    ensure_docker_daemon()?;
    run_ignored_suites(DOCKER_SUITES, "docker")?;

    eprintln!("All Docker integration tests passed.");
    Ok(())
}

fn test_network() -> Result<()> {
    run_ignored_suites(NETWORK_SUITES, "network")?;

    eprintln!("All network integration tests passed.");
    Ok(())
}

fn run_ignored_suites(suites: &[(&str, &str)], label: &str) -> Result<()> {
    let root = workspace_root()?;

    for (pkg, test) in suites {
        eprintln!("== {pkg} :: {test} ==");
        let mut command = Command::new("cargo");
        command.current_dir(&root).args([
            "test",
            "-p",
            pkg,
            "--test",
            test,
            "--",
            "--ignored",
            "--test-threads=1",
        ]);
        if (*pkg, *test) == ("quant-pivot-web", "web") {
            command.args(["--skip", UI_E2E_SERVER_TEST]);
        }
        let status = command
            .status()
            .with_context(|| format!("failed to run {label} suite {pkg}::{test}"))?;
        if !status.success() {
            bail!("{label} suite failed: {pkg}::{test}");
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use std::{collections::BTreeSet, fs, path::Path};

    use super::{DOCKER_SUITES, report_capacity_ceiling, workspace_root};

    const DOCKER_IGNORE_MARKER: &str = "#[ignore = \"requires Docker\"]";

    #[test]
    fn report_capacity_ceiling_enforces_floor_double_runway_and_rounding() {
        assert_eq!(report_capacity_ceiling(1).expect("ceiling"), 100_000);
        assert_eq!(report_capacity_ceiling(50_001).expect("ceiling"), 101_000);
        assert_eq!(report_capacity_ceiling(120_000).expect("ceiling"), 240_000);
    }

    #[test]
    fn docker_suite_registry_matches_ignored_integration_targets() {
        let root = workspace_root().expect("workspace root");
        let packages = DOCKER_SUITES
            .iter()
            .map(|(package, _)| *package)
            .collect::<BTreeSet<_>>();
        let mut discovered = BTreeSet::new();

        for package in packages {
            let tests_dir = root.join("crates").join(package).join("tests");
            for entry in fs::read_dir(&tests_dir).expect("integration test directory") {
                let path = entry.expect("integration test entry").path();
                if path.extension().and_then(|value| value.to_str()) != Some("rs") {
                    continue;
                }
                let target = path
                    .file_stem()
                    .and_then(|value| value.to_str())
                    .expect("UTF-8 integration target");
                let module_dir = tests_dir.join(target);
                if path_contains(&path, DOCKER_IGNORE_MARKER)
                    || path_contains(&module_dir, DOCKER_IGNORE_MARKER)
                {
                    discovered.insert((package.to_owned(), target.to_owned()));
                }
            }
        }

        let registered = DOCKER_SUITES
            .iter()
            .map(|(package, target)| ((*package).to_owned(), (*target).to_owned()))
            .collect::<BTreeSet<_>>();
        assert_eq!(registered, discovered);
    }

    fn path_contains(path: &Path, marker: &str) -> bool {
        if path.is_file() {
            return fs::read_to_string(path).is_ok_and(|contents| contents.contains(marker));
        }
        if !path.is_dir() {
            return false;
        }
        fs::read_dir(path)
            .expect("integration module directory")
            .filter_map(Result::ok)
            .any(|entry| path_contains(&entry.path(), marker))
    }
}
