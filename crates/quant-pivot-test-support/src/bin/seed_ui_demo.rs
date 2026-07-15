//! Seed Postgres + `ClickHouse` demo data for Phase 10.4/10.5 UI validation.
//!
//! ```bash
//! cargo run -p quant-pivot-test-support --bin seed-ui-demo -- --config-dir config
//! ```

use std::sync::Arc;

use anyhow::{Context, Result};
use clap::Parser;
use quant_pivot_models::config::DeployConfig;
use quant_pivot_research::artifact::build_artifact_store;
use quant_pivot_storage::{
    clickhouse::{ClickHousePool, apply_online_schema_migrations, verify_schema},
    postgres::PostgresPool,
};
use quant_pivot_test_support::{
    research_ui_seed::ResearchUiSeedSummary,
    ui_demo_seed::{UiDemoSeedSummary, seed_ui_demo_ck, seed_ui_demo_pg},
};
use tracing_subscriber::EnvFilter;

#[derive(Parser)]
#[command(name = "seed-ui-demo")]
#[command(about = "Seed Postgres + ClickHouse demo data for execution + research UI")]
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
    let model_artifact_store = build_artifact_store(&deploy.research.artifact_store)
        .context("build research artifact store")?;

    tracing::info!(funder = %funder, "seeding Postgres ui-demo fixtures");
    let summary = seed_ui_demo_pg(&db, &funder, &model_artifact_store).await;
    tracing::info!(
        reports = summary.reports,
        intents = summary.intents,
        execution_orders = summary.execution_orders,
        positions = summary.positions,
        reconciliations = summary.reconciliations,
        settlement_redeems = summary.settlement_redeems,
        attributions = summary.attributions,
        research_datasets = summary.research.datasets,
        research_models = summary.research.model_versions,
        research_backtests = summary.research.backtest_reports,
        research_comparisons = summary.research.comparison_reports,
        research_factors = summary.research.factors,
        research_skipped = summary.research.skipped,
        "Postgres seed complete"
    );

    if cli.pg_only {
        print_summary(&summary);
        return Ok(());
    }

    if deploy.db.clickhouse.migration.auto_apply_online {
        apply_online_schema_migrations(&deploy.db.clickhouse.migration_connection())
            .await
            .context("apply ClickHouse online-safe schema migrations")?;
    } else {
        verify_schema(&deploy.db.clickhouse)
            .await
            .context("verify ClickHouse schema")?;
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
    print_research_summary(&summary.research);
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

fn print_research_summary(research: &ResearchUiSeedSummary) {
    eprintln!("\n--- 10.5 research catalog ---");
    if research.skipped {
        eprintln!("(skipped — fixtures already present)");
    }
    eprintln!("datasets:           {}", research.datasets);
    eprintln!("model_specs:        {}", research.model_specs);
    eprintln!("model_versions:     {}", research.model_versions);
    eprintln!("backtest_reports:   {}", research.backtest_reports);
    eprintln!("comparison_reports: {}", research.comparison_reports);
    eprintln!("factors:            {}", research.factors);
    eprintln!("\n10.5 deep links:");
    if let Some(id) = &research.dataset_ready_id {
        eprintln!("  trainable dataset (ready):  {id}");
        eprintln!("  open: /research/datasets?open={id}");
    }
    if let Some(id) = &research.candidate_model_version_id {
        eprintln!("  candidate model (publish):  {id}");
        eprintln!("  open: /research/models?open={id}");
    }
    if let Some(id) = &research.shadow_model_version_id {
        eprintln!("  shadow model (publish):     {id}");
    }
    if let Some(id) = &research.retired_model_version_id {
        eprintln!("  retired model (rollback):   {id}");
    }
    if let Some(id) = &research.candidate_backtest_report_id {
        eprintln!("  candidate backtest:         {id}");
        eprintln!("  open: /research/backtests?open={id}");
    }
    if let Some(id) = &research.comparison_report_id {
        eprintln!("  comparison detail:          {id}");
        eprintln!("  open: /research/comparisons/{id}");
    }
    if let Some(id) = &research.draft_factor_id {
        eprintln!("  draft factor (publish):     {id}");
        eprintln!("  open: /research/factors?open={id}");
    }
    if let Some(id) = &research.published_factor_id {
        eprintln!("  published factor (retire):  {id}");
    }
    if let Some(id) = &research.retired_factor_id {
        eprintln!("  retired factor:             {id}");
    }
    eprintln!("\nResearch filter tips:");
    eprintln!("  - Dataset parquet prefix:   file:///tmp/ui-demo-research-");
    eprintln!("  - Secondary model spec:     ui-demo-research-spec-secondary");
    eprintln!("  - Factor name prefix:       ui-demo-research-");
}
