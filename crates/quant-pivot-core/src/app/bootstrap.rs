//! Application bootstrap — build subsystems and run the lifecycle.

use crate::app::{
    AppContext, ports::research_job::CoreResearchJobPort, research_job::ResearchJobEngine,
    task_registry::AppRunner,
};
use quant_pivot_error::QuantResult;
use quant_pivot_models::{config::DeployConfig, domain::ResearchJobPort};
use quant_pivot_repository::traits::{
    ResearchJobRepository, RuntimeConfigVersionRepository, TrainingDatasetRepository,
};
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
    ctx.register_equity_snapshot_worker(&mut runner);
    ctx.register_report_scheduler(&mut runner);
    ctx.register_report_expire_sweep(&mut runner);
    ctx.register_recommendation_deadline_scheduler(&mut runner);
    ctx.register_recommendation_expire_sweep(&mut runner);
    ctx.register_entry_condition_worker(&mut runner);
    ctx.register_execution_dispatcher(&mut runner);
    ctx.register_reconciliation_worker(&mut runner);
    ctx.register_exit_monitor_worker(&mut runner);
    ctx.register_settlement_redeem_worker(&mut runner);
    ctx.register_attribution_worker(&mut runner);

    // Durable async research-job engine: the enqueue port (HTTP) and the worker
    // (execution + crash recovery) share one engine so cancellation tokens, the
    // ledger, and the boot epoch id are common.
    let job_engine = ResearchJobEngine::new(
        Arc::clone(&ctx.infra.repos.research_job) as Arc<dyn ResearchJobRepository>,
        ctx.events.clone(),
    );
    let research_jobs: Arc<dyn ResearchJobPort> = Arc::new(CoreResearchJobPort::new(
        job_engine.clone(),
        Arc::clone(&ctx.infra.repos.training_dataset) as Arc<dyn TrainingDatasetRepository>,
        Arc::clone(&ctx.infra.repos.runtime_config) as Arc<dyn RuntimeConfigVersionRepository>,
        ctx.config.quant.research_jobs.max_recovery_attempts,
    ));
    ctx.register_research_job_worker(&mut runner, job_engine)?;
    ctx.register_research_readiness_evidence_worker(&mut runner)?;
    ctx.register_feature_parity_scheduler(&mut runner);

    let order_intents = ctx.register_execution_services(&mut runner);
    ctx.register_web_services(&mut runner, order_intents, research_jobs)
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
