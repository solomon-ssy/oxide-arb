use clap::Parser;
use oxide_arb_core::app::bootstrap;
use oxide_arb_models::config::{DeployConfig, ObservabilityConfig};
use std::{error::Error, sync::Arc};
use tracing_subscriber::EnvFilter;

#[derive(Parser)]
#[command(name = "oxide-arb", about = "Endgame arbitrage engine")]
struct Cli {
    /// Directory containing oxide-arb.toml
    #[arg(long, env = "OXIDE_ARB_CONFIG_DIR", default_value = "config")]
    config_dir: String,
}

/// Initialize tracing from `[observability]`.
///
/// The configured `log_level` is the default filter; a set `RUST_LOG`
/// environment variable overrides it entirely. `log_json` switches the
/// formatter to structured JSON lines for log aggregation.
fn init_tracing(observability: &ObservabilityConfig) {
    let filter = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::new(observability.log_level.clone()));
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

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let cli = Cli::parse();
    let deploy = Arc::new(DeployConfig::load(&cli.config_dir)?);
    init_tracing(&deploy.observability);
    tracing::info!(config_dir = %cli.config_dir, "deploy config loaded");
    bootstrap::run(deploy).await?;
    Ok(())
}
