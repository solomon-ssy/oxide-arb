//! Application bootstrap — build subsystems and run the lifecycle.

use crate::app::{AppContext, task_registry::AppRunner};
use quant_pivot_error::QuantResult;
use quant_pivot_models::config::DeployConfig;
use std::sync::Arc;
use tokio_util::sync::CancellationToken;

pub async fn run(deploy: Arc<DeployConfig>) -> QuantResult<()> {
    let shutdown = CancellationToken::new();
    let ctx = AppContext::build(deploy, shutdown.clone()).await?;

    // Crash recovery (in-flight orders → reconciliation) is owned by the
    // execution dispatcher worker, which runs it fail-closed as its first action
    // before any submission (see `register_execution_dispatcher`).
    let mut runner = AppRunner::for_quant_mode(shutdown.clone(), ctx.runtime_mode().current());
    ctx.register_runtime_tasks(&mut runner);
    ctx.register_periodic_services(&mut runner);
    ctx.register_report_scheduler(&mut runner);
    ctx.register_report_expire_sweep(&mut runner);
    ctx.register_recommendation_deadline_scheduler(&mut runner);
    ctx.register_recommendation_expire_sweep(&mut runner);
    ctx.register_execution_dispatcher(&mut runner);
    ctx.register_reconciliation_worker(&mut runner);
    ctx.register_exit_monitor_worker(&mut runner);
    let order_intents = ctx.register_execution_services(&mut runner);
    ctx.register_web_services(&mut runner, order_intents)
        .await?;
    ctx.register_fact_writer_tasks(&mut runner);

    tracing::info!(
        mode = ?ctx.runtime_mode().current(),
        tasks = runner.registry_len(),
        "quant-pivot starting",
    );

    let result = runner.run().await;
    ctx.infra.redis.close();
    tracing::info!("shared Redis pool closed");
    result
}
