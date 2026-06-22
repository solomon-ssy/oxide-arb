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

/// SDK WebSocket channel default: the SDK WARNs on every failed reconnect
/// attempt, which multiplies across shards into log storms during outages.
/// Shard connectivity is aggregated by the core `HealthChecker` instead.
const SDK_WS_LOG_DIRECTIVE: &str = "polymarket_client_sdk_v2::ws=error";

/// Initialize tracing from `[observability]`.
///
/// The configured `log_level` is the default filter; a set `RUST_LOG`
/// environment variable overrides it entirely. Unless the chosen filter
/// already mentions the Polymarket SDK, its WS channel is capped at `error`
/// (see [`SDK_WS_LOG_DIRECTIVE`]). `log_json` switches the formatter to
/// structured JSON lines for log aggregation.
fn init_tracing(observability: &ObservabilityConfig) {
    let base = std::env::var(EnvFilter::DEFAULT_ENV)
        .ok()
        .filter(|value| !value.trim().is_empty())
        .unwrap_or_else(|| observability.log_level.clone());
    let directives = if base.contains("polymarket_client_sdk_v2") {
        base
    } else {
        format!("{base},{SDK_WS_LOG_DIRECTIVE}")
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
