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

#[derive(Parser)]
#[command(name = "oxide-arb-xtask")]
#[command(about = "Task runner for oxide-arb", long_about = None)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Run testcontainers-based integration tests (requires Docker daemon).
    TestDocker,
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
        Commands::TestDocker => test_docker(),
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

fn test_docker() -> Result<()> {
    ensure_docker_daemon()?;
    let root = workspace_root()?;

    for (pkg, test) in DOCKER_SUITES {
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
            .with_context(|| format!("failed to run docker suite {pkg}::{test}"))?;
        if !status.success() {
            bail!("docker suite failed: {pkg}::{test}");
        }
    }

    eprintln!("All Docker integration tests passed.");
    Ok(())
}
