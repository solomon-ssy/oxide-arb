use clap::Parser;
use oxide_arb_core::app::bootstrap;
use oxide_arb_models::config::{DeployConfig, ObservabilityConfig};
use rustls::crypto::aws_lc_rs;
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

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let cli = Cli::parse();
    let deploy = Arc::new(DeployConfig::load(&cli.config_dir)?);
    init_tracing(&deploy.observability);
    init_crypto_provider();
    tracing::info!(config_dir = %cli.config_dir, "deploy config loaded");
    bootstrap::run(deploy).await?;
    Ok(())
}
