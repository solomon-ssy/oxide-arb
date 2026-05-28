//! Application bootstrap — load config and run lifecycle.

use crate::app::{AppContext, periodic_services, task_registry::AppRunner};
use oxide_arb_error::OxideResult;
use oxide_arb_models::config::Settings;
use oxide_arb_repository::postgres::PgRiskAuditRepository;
use std::sync::Arc;
use tokio_util::sync::CancellationToken;

/// Load settings, build subsystems, register tasks, and run until shutdown.
pub async fn run(config_dir: &str) -> OxideResult<()> {
    let settings = Arc::new(Settings::new(config_dir)?);
    let mode = settings.execution.execution_mode;

    let shutdown = CancellationToken::new();
    let ctx = AppContext::build(settings, shutdown.clone()).await?;
    periodic_services::ensure_live_metrics_ready(mode, ctx.risk.metrics_refresh.as_deref()).await?;
    ctx.queue_runtime_tasks();
    ctx.queue_market_settlement_task();
    ctx.queue_risk_decision_audit_drain(Arc::new(PgRiskAuditRepository::new(
        ctx.infra.pg.connection().clone(),
    )));
    ctx.queue_periodic_services();

    let mut runner = AppRunner::for_mode(shutdown, mode);
    runner.absorb_pending_queue(&ctx.pending_tasks);

    tracing::info!(
        mode = ?mode,
        config_dir,
        tasks = runner.registry_len(),
        "oxide-arb starting",
    );

    runner.run().await
}
