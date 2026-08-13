use std::{env, error::Error, path::PathBuf, sync::Arc};

use clap::Parser;
use quant_pivot_allocator as _;
use quant_pivot_compute::ComputeExecutor;
use quant_pivot_core::app::bootstrap;
use quant_pivot_models::{
    config::{DeployConfig, DeployConfigLoadRequest, ObservabilityConfig},
    types::DeploymentEnvironment,
};
use rustls::crypto::aws_lc_rs;
use tokio::runtime::Builder;
use tracing_subscriber::EnvFilter;

const TOKIO_WORKER_THREADS: usize = 3;
const TOKIO_MAX_BLOCKING_THREADS: usize = 4;

#[derive(Parser)]
#[command(
    name = "quant-pivot",
    about = "Polymarket quantitative report and execution platform"
)]
struct Cli {
    /// Absolute path to the single deploy configuration file.
    #[arg(long, value_name = "ABSOLUTE_PATH")]
    config_file: PathBuf,
    /// Environment identity that must exactly match `[deployment].environment`.
    #[arg(long, value_name = "ENVIRONMENT")]
    expected_environment: DeploymentEnvironment,
}

/// SDK internals are capped at error: recoverable transport failures and the
/// create-then-derive API-key flow otherwise emit misleading WARN events.
/// Application wrappers propagate terminal failures, while connectivity is
/// aggregated by the core `HealthChecker`.
const SDK_LOG_DIRECTIVE: &str = "polymarket_client_sdk_v2=error";

/// Initialize tracing from `[observability]`.
///
/// The configured `log_level` is the default filter; a set `RUST_LOG`
/// environment variable overrides it entirely. Unless the chosen filter
/// already mentions the Polymarket SDK, its internal channel is capped at
/// `error` (see [`SDK_LOG_DIRECTIVE`]). `log_json` switches the formatter to
/// structured JSON lines for log aggregation.
fn init_tracing(observability: &ObservabilityConfig) {
    let base = env::var(EnvFilter::DEFAULT_ENV)
        .ok()
        .filter(|value| !value.trim().is_empty())
        .unwrap_or_else(|| observability.log_level.clone());
    let directives = if base.contains("polymarket_client_sdk_v2") {
        base
    } else {
        format!("{base},{SDK_LOG_DIRECTIVE}")
    };
    let filter = EnvFilter::new(directives);
    if observability.log_json {
        tracing_subscriber::fmt()
            .with_env_filter(filter)
            .with_target(true)
            .json()
            .init();
    } else {
        tracing_subscriber::fmt()
            .with_env_filter(filter)
            .with_target(true)
            .init();
    }
}

/// Install the process-wide rustls `CryptoProvider` before any TLS usage.
///
/// The dependency tree enables both rustls crypto backends — `ring` (via
/// sea-orm/sqlx) and `aws-lc-rs` (via reqwest/hyper-rustls) — so rustls
/// cannot pick a default automatically and would panic on the first TLS
/// handshake that relies on the process default (e.g. CLOB websocket
/// connects). `Err` only means a provider is already installed, which is
/// equally fine.
fn init_crypto_provider() {
    if aws_lc_rs::default_provider().install_default().is_err() {
        tracing::debug!("rustls crypto provider already installed");
    }
}

fn main() -> Result<(), Box<dyn Error>> {
    let cli = Cli::parse();
    let request = DeployConfigLoadRequest::new(cli.config_file, cli.expected_environment);
    let deploy = Arc::new(DeployConfig::load(&request)?);
    init_tracing(&deploy.observability);
    init_crypto_provider();
    let compute = Arc::new(ComputeExecutor::new()?);
    let runtime = Builder::new_multi_thread()
        .worker_threads(TOKIO_WORKER_THREADS)
        .max_blocking_threads(TOKIO_MAX_BLOCKING_THREADS)
        .thread_name("quant-tokio")
        .enable_all()
        .build()?;
    tracing::info!(
        config_file = %request.config_file.display(),
        expected_environment = request.expected_environment.as_str(),
        "deploy config loaded"
    );
    runtime.block_on(bootstrap::run(deploy, compute))?;
    Ok(())
}
