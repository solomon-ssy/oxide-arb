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

    let shutdown = CancellationToken::new();
    let ctx = AppContext::build(settings, shutdown.clone()).await?;
    // Effective mode after restoring the persisted operational state.
    let mode = ctx.execution_mode.current();
    periodic_services::ensure_live_metrics_ready(mode, ctx.risk.metrics_refresh.as_deref()).await?;
    ctx.queue_runtime_tasks();
    ctx.queue_market_settlement_task();
    ctx.queue_risk_decision_audit_drain(Arc::new(PgRiskAuditRepository::new(
        ctx.infra.pg.connection().clone(),
    )));
    ctx.queue_periodic_services();
    // Web + governance control plane: HTTP/WS server (stage-0 ingress),
    // operation-log writer, enqueue-only scheduler, and the execute worker.
    ctx.queue_web_services().await?;
    ctx.queue_control_factor_scheduler();
    ctx.queue_materialization_execute_worker();

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
