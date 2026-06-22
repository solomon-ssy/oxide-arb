//! Application bootstrap — build subsystems and run the lifecycle.

use crate::app::{AppContext, task_registry::AppRunner};
use quant_pivot_error::QuantResult;
use quant_pivot_models::config::DeployConfig;
use std::sync::Arc;
use tokio_util::sync::CancellationToken;

pub async fn run(deploy: Arc<DeployConfig>) -> QuantResult<()> {
    let shutdown = CancellationToken::new();
    let ctx = AppContext::build(deploy, shutdown.clone()).await?;

    let mut runner = AppRunner::for_quant_mode(shutdown.clone(), ctx.runtime_mode().current());
    ctx.register_runtime_tasks(&mut runner);
    ctx.register_periodic_services(&mut runner);
    ctx.register_web_services(&mut runner).await?;

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
