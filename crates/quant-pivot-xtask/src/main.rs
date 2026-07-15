use anyhow::{Context, Result, bail};
use clap::{Args, Parser, Subcommand};
use quant_pivot_models::config::DeployConfig;
use quant_pivot_storage::clickhouse::{apply_online_schema_migrations, plan_schema, verify_schema};
use rustls::crypto::aws_lc_rs;
use std::{
    path::PathBuf,
    process::{Command, ExitCode, Stdio},
};

const DOCKER_SUITES: &[(&str, &str)] = &[
    ("quant-pivot-storage", "migration_pg"),
    ("quant-pivot-repository", "pg_account_capital"),
    ("quant-pivot-repository", "pg_attribution"),
    ("quant-pivot-repository", "pg_equity_snapshot"),
    ("quant-pivot-repository", "pg_market_selection"),
    ("quant-pivot-repository", "pg_market_page"),
    ("quant-pivot-repository", "pg_market_upsert"),
    ("quant-pivot-repository", "pg_governance"),
    ("quant-pivot-repository", "pg_rbac"),
    ("quant-pivot-repository", "pg_training_dataset"),
    ("quant-pivot-repository", "pg_model_registry"),
    ("quant-pivot-repository", "pg_research_job"),
    ("quant-pivot-repository", "pg_research_readiness"),
    ("quant-pivot-repository", "pg_trade_policy_trial"),
    ("quant-pivot-repository", "pg_backtest_report"),
    ("quant-pivot-repository", "pg_comparison_report"),
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
    ("quant-pivot-api", "http_clob_wiremock"),
    ("quant-pivot-api", "http_data_api_wiremock"),
];

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
    /// Verify the migration ledger and runtime schema contract read-only.
    Verify(ConfigDirArgs),
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

async fn clickhouse_schema(command: ClickHouseSchemaCommand) -> Result<()> {
    let config_dir = match &command {
        ClickHouseSchemaCommand::Plan(args)
        | ClickHouseSchemaCommand::ApplyOnline(args)
        | ClickHouseSchemaCommand::Verify(args) => &args.config_dir,
    };
    let config_dir = config_dir
        .to_str()
        .context("ClickHouse schema config directory is not valid UTF-8")?;
    let deploy = DeployConfig::load(config_dir).context("load deploy config")?;
    let config = &deploy.db.clickhouse;
    let migration_config = config.migration_connection();
    match command {
        ClickHouseSchemaCommand::Plan(_) => {
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
            let status = apply_online_schema_migrations(&migration_config)
                .await
                .context("deploy ClickHouse schema")?;
            println!(
                "ClickHouse schema deployed: version={}, required_objects={}",
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
    }
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
        let status = Command::new("cargo")
            .current_dir(&root)
            .args([
                "test",
                "-p",
                pkg,
                "--test",
                test,
                "--",
                "--ignored",
                "--test-threads=1",
            ])
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
    use super::report_capacity_ceiling;

    #[test]
    fn report_capacity_ceiling_enforces_floor_double_runway_and_rounding() {
        assert_eq!(report_capacity_ceiling(1).expect("ceiling"), 100_000);
        assert_eq!(report_capacity_ceiling(50_001).expect("ceiling"), 101_000);
        assert_eq!(report_capacity_ceiling(120_000).expect("ceiling"), 240_000);
    }
}
