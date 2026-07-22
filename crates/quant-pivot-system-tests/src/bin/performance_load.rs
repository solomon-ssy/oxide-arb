use std::{error::Error, path::PathBuf};

use clap::Parser;
use quant_pivot_allocator as _;
use quant_pivot_system_tests::performance::{PerformanceProfile, run_profile};
use tokio::runtime::Builder;

#[derive(Parser)]
#[command(name = "performance-load")]
#[command(about = "Run the production Gamma/CLOB/ClickHouse performance stack")]
struct Cli {
    #[arg(long, value_enum, default_value_t = PerformanceProfile::Full)]
    profile: PerformanceProfile,
    #[arg(long, default_value = "target/performance-evidence")]
    output: PathBuf,
}

fn main() -> Result<(), Box<dyn Error>> {
    let cli = Cli::parse();
    Builder::new_multi_thread()
        .worker_threads(3)
        .max_blocking_threads(4)
        .thread_name("quant-perf-tokio")
        .enable_all()
        .build()?
        .block_on(async move {
            for path in run_profile(cli.profile, &cli.output).await? {
                println!("performance evidence: {}", path.display());
            }
            Ok(())
        })
}
