use anyhow::{Context, Result, bail};
use clap::{Parser, Subcommand};
use std::{
    path::PathBuf,
    process::{Command, ExitCode, Stdio},
};

const DOCKER_SUITES: &[(&str, &str)] = &[
    ("oxide-arb-repository", "pg_repository"),
    ("oxide-arb-repository", "ch_timeseries"),
    ("oxide-arb-storage", "migration_pg"),
    ("oxide-arb-storage", "redis_integration"),
    ("oxide-arb-storage", "clickhouse_integration"),
    ("oxide-arb-storage", "cache_tiered_integration"),
    ("oxide-arb-core", "gamma_service_sync"),
];

const NETWORK_SUITES: &[(&str, &str)] = &[
    ("oxide-arb-api", "http_gamma_wiremock"),
    ("oxide-arb-api", "http_clob_wiremock"),
    ("oxide-arb-api", "clob_live_path_wiremock"),
];

#[derive(Parser)]
#[command(name = "oxide-arb-xtask")]
#[command(about = "Task runner for oxide-arb", long_about = None)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Run the default workspace tests, Docker suites, and network suites.
    #[command(name = "test-full")]
    Full,
    /// Run testcontainers-based integration tests (requires Docker daemon).
    #[command(name = "test-docker")]
    Docker,
    /// Run network-shaped API tests that are ignored by default.
    #[command(name = "test-network")]
    Network,
}

fn main() -> ExitCode {
    match run() {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("error: {error:#}");
            ExitCode::FAILURE
        }
    }
}

fn run() -> Result<()> {
    match Cli::parse().command {
        Commands::Full => test_full(),
        Commands::Docker => test_docker(),
        Commands::Network => test_network(),
    }
}

fn workspace_root() -> Result<PathBuf> {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(|crates| crates.parent())
        .context("oxide-arb-xtask must live under crates/")
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
