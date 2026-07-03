//! Seed Postgres + `ClickHouse` demo data for Phase 10.4 execution-plane UI validation.
//!
//! ```bash
//! cargo run -p quant-pivot-test-support --bin seed-ui-demo -- --config-dir config
//! ```

use std::sync::Arc;

use anyhow::{Context, Result};
use clap::Parser;
use quant_pivot_models::config::DeployConfig;
use quant_pivot_storage::{clickhouse::ClickHousePool, postgres::PostgresPool};
use quant_pivot_test_support::ui_demo_seed::{UiDemoSeedSummary, seed_ui_demo_ck, seed_ui_demo_pg};
use tracing_subscriber::EnvFilter;

#[derive(Parser)]
#[command(name = "seed-ui-demo")]
#[command(about = "Seed Postgres + ClickHouse demo data for execution-plane UI")]
struct Cli {
    /// Directory containing quant-pivot.toml (+ optional quant-pivot.local.toml)
    #[arg(long, env = "QUANT_PIVOT_CONFIG_DIR", default_value = "config")]
    config_dir: String,

    /// Skip `ClickHouse` fact inserts
    #[arg(long)]
    pg_only: bool,
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env().add_directive("info".parse()?))
        .init();

    let cli = Cli::parse();
    let deploy = DeployConfig::load(&cli.config_dir).context("load deploy config")?;
    let funder = deploy
        .quant
        .account
        .funder
        .clone()
        .context("config [quant.account].funder is required for settlement redeem demo rows")?;

    let pg = PostgresPool::connect(&deploy.db.postgres)
        .await
        .context("connect postgres")?;
    let db = pg.connection().clone();

    tracing::info!(funder = %funder, "seeding Postgres ui-demo fixtures");
    let summary = seed_ui_demo_pg(&db, &funder).await;
    tracing::info!(
        reports = summary.reports,
        intents = summary.intents,
        execution_orders = summary.execution_orders,
        positions = summary.positions,
        reconciliations = summary.reconciliations,
        settlement_redeems = summary.settlement_redeems,
        attributions = summary.attributions,
        "Postgres seed complete"
    );

    if cli.pg_only {
        print_summary(&summary);
        return Ok(());
    }

    let ch = ClickHousePool::connect(&deploy.db.clickhouse)
        .await
        .context("connect clickhouse")?;
    let rows = seed_ui_demo_ck(Arc::new(ch), &summary)
        .await
        .context("seed clickhouse facts")?;
    tracing::info!(rows, "ClickHouse seed complete");

    print_summary(&summary);
    Ok(())
}

fn print_summary(summary: &UiDemoSeedSummary) {
    eprintln!("\n=== ui-demo seed summary ===");
    eprintln!("reports:            {}", summary.reports);
    eprintln!("intents:            {}", summary.intents);
    eprintln!("execution_orders:   {}", summary.execution_orders);
    eprintln!("positions:          {}", summary.positions);
    eprintln!("reconciliations:    {}", summary.reconciliations);
    eprintln!("settlement_redeems: {}", summary.settlement_redeems);
    eprintln!("attributions:       {}", summary.attributions);
    eprintln!("\nFilter tips:");
    eprintln!("  - Reports trigger_key prefix: ui-demo:");
    eprintln!("  - Market ids prefix:          ui-demo-");
    eprintln!("  - Settlement batch market:    {SETTLE_MARKET}");
    if let Some(id) = &summary.actionable_recommendation_id {
        eprintln!("\n10.3 create-intent (no blocking intent):");
        eprintln!("  recommendation_id: {id}");
        eprintln!("  open: /quant/recommendations/{id}");
    }
    if let (Some(base), Some(current)) = (
        &summary.diff_base_report_id,
        &summary.diff_current_report_id,
    ) {
        eprintln!("\n10.3 report diff pair:");
        eprintln!("  baseline (older):  {base}");
        eprintln!("  compare (current): {current}");
        eprintln!("  open: /quant/reports/{current} → Diff tab");
    }
    eprintln!("\nSample intent ids:");
    for record in summary
        .records
        .iter()
        .filter(|r| r.intent_id.is_some())
        .take(5)
    {
        eprintln!(
            "  [{}] {}",
            record.slug,
            record.intent_id.as_ref().expect("intent")
        );
    }
}

const SETTLE_MARKET: &str = "ui-demo-settle-mkt";
