use clap::Parser;
use oxide_arb_core::app::bootstrap;
use tracing_subscriber::EnvFilter;

#[derive(Parser)]
#[command(name = "oxide-arb", about = "Endgame arbitrage engine")]
struct Cli {
    /// Directory containing oxide-arb.toml
    #[arg(long, env = "OXIDE_ARB_CONFIG_DIR", default_value = "config")]
    config_dir: String,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env())
        .with_target(true)
        .init();

    let cli = Cli::parse();
    bootstrap::run(&cli.config_dir).await?;
    Ok(())
}
