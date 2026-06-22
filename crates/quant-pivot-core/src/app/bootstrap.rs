//! Application bootstrap — build subsystems and run the lifecycle.

use crate::app::{AppContext, task_registry::AppRunner};
use oxide_arb_error::OxideResult;
use oxide_arb_models::config::DeployConfig;
use oxide_arb_repository::postgres::PgRiskAuditRepository;
use std::sync::Arc;
use tokio_util::sync::CancellationToken;

/// Build subsystems from the loaded deploy config, register tasks, and run
/// until shutdown. The runtime config is seeded from Postgres during build.
pub async fn run(deploy: Arc<DeployConfig>) -> OxideResult<()> {
    let shutdown = CancellationToken::new();
    let ctx = AppContext::build(deploy, shutdown.clone()).await?;
    if let Some(execution) = ctx.trading.execution.as_ref() {
        ctx.trade_integrity
            .boot_rehydrate(&execution.capital_manager)
            .await?;
    }
    ctx.ensure_live_ready().await?;
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

    let mode = ctx.execution_mode.current();
    let mut runner = AppRunner::for_mode(shutdown, mode);
    runner.absorb_pending_queue(&ctx.pending_tasks);

    tracing::info!(
        mode = ?mode,
        tasks = runner.registry_len(),
        "oxide-arb starting",
    );

    let result = runner.run().await;

    // Close the shared Redis pool (cache L2 + JWT revocation) only after every
    // shutdown stage has drained — earlier stages may still read/write Redis.
    ctx.infra.redis.close();
    tracing::info!("shared Redis pool closed");

    result
}
