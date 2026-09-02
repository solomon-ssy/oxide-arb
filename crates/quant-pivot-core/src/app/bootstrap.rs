//! Application bootstrap — build subsystems and run the lifecycle.

use std::sync::Arc;

use quant_pivot_compute::ComputeExecutor;
use quant_pivot_error::{QuantResult, storage::StorageError};
use quant_pivot_models::{
    config::DeployConfig,
    domain::{
        data_plane::ExchangeHistoryStage, ports::ResearchJobPort,
        quant::AccountRecoveryIncidentInfo,
    },
};
use quant_pivot_repository::traits::{
    PolicyRepository, ResearchJobRepository, TrainingDatasetRepository,
};
use tokio_util::sync::CancellationToken;

use crate::app::{
    AppContext,
    ports::research_job::CoreResearchJobPort,
    research_job::ResearchJobEngine,
    task_registry::{AppRunner, ShutdownBudget, ShutdownStage},
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum StartupExecutionScope {
    EntryEnabled,
    RecoveryOnly,
}

impl From<Option<&AccountRecoveryIncidentInfo>> for StartupExecutionScope {
    fn from(active_recovery: Option<&AccountRecoveryIncidentInfo>) -> Self {
        if active_recovery.is_some() {
            Self::RecoveryOnly
        } else {
            Self::EntryEnabled
        }
    }
}

pub async fn run(deploy: Arc<DeployConfig>, compute: Arc<ComputeExecutor>) -> QuantResult<()> {
    let shutdown = CancellationToken::new();
    let mut ctx = AppContext::build(deploy, shutdown.clone(), compute).await?;

    // Recovery-only startup gate: finalized account executions are projected,
    // associated, and any unknown external execution latches ExitOnly before
    // an entry worker is registered.
    while ctx.execution.account_chain_projector.project_pass().await? > 0 {}
    let active_recovery = ctx
        .execution
        .account_chain_projector
        .active_recovery()
        .await?;

    // Crash recovery (in-flight orders → reconciliation) is owned by the
    // execution dispatcher worker, which runs it fail-closed as its first action
    // before any submission (see `register_execution_dispatcher`).
    let mut runner = AppRunner::new(shutdown.clone());
    ctx.register_runtime_control_sync(&mut runner);
    // Historical projection is identity-strict. Complete the active + closed
    // Gamma baseline before the history worker can observe any chain log.
    ctx.data
        .exchange_history_progress
        .set_stage(ExchangeHistoryStage::IdentitySync);
    ctx.data.gamma_service.sync().await?;
    ctx.register_runtime_tasks(&mut runner).await?;
    ctx.register_periodic_services(&mut runner);
    ctx.register_equity_snapshot_worker(&mut runner);
    ctx.register_venue_incentive_worker(&mut runner);
    ctx.register_report_coordinator(&mut runner);
    ctx.register_report_expire_sweep(&mut runner);
    ctx.register_recommendation_deadline_scheduler(&mut runner);
    ctx.register_recommendation_expire_sweep(&mut runner);
    ctx.register_entry_condition_worker(&mut runner);
    match StartupExecutionScope::from(active_recovery.as_ref()) {
        StartupExecutionScope::EntryEnabled => ctx.register_execution_dispatcher(&mut runner),
        StartupExecutionScope::RecoveryOnly => {
            ctx.register_execution_recovery(&mut runner);
            if let Some(incident) = active_recovery.as_ref() {
                tracing::warn!(
                    recovery_incident_id = %incident.account_recovery_incident_id,
                    "recovery-only startup: entry submission worker is disabled until a governed restart",
                );
            }
        }
    }
    ctx.register_reconciliation_worker(&mut runner);
    ctx.register_settlement_workers(&mut runner);
    ctx.register_exit_monitor_worker(&mut runner);
    ctx.register_outcome_reconciliation_worker(&mut runner);

    // Durable async research-job engine: the enqueue port (HTTP) and the worker
    // (execution + crash recovery) share one engine so cancellation tokens, the
    // ledger, and the boot epoch id are common.
    let job_engine = ResearchJobEngine::new(
        Arc::clone(&ctx.infra.repos.research_job) as Arc<dyn ResearchJobRepository>,
        ctx.events.clone(),
    );
    let core_research_jobs = Arc::new(CoreResearchJobPort::new(
        job_engine.clone(),
        Arc::clone(&ctx.infra.repos.training_dataset) as Arc<dyn TrainingDatasetRepository>,
        Arc::clone(&ctx.infra.repos.runtime_config) as Arc<dyn PolicyRepository>,
        ctx.config.quant.research_jobs.max_recovery_attempts,
    ));
    let research_jobs: Arc<dyn ResearchJobPort> =
        Arc::<CoreResearchJobPort>::clone(&core_research_jobs);
    let feedback_wake = job_engine.feedback_wake();
    ctx.register_research_runtime(&mut runner, job_engine)?;
    ctx.register_fresh_boot(&mut runner, core_research_jobs);
    ctx.register_readiness_worker(&mut runner)?;
    ctx.register_feature_parity_scheduler(&mut runner);

    let order_intents = ctx.register_execution_services(&mut runner);
    ctx.register_web_services(&mut runner, order_intents, research_jobs, feedback_wake)
        .await?;
    ctx.register_fact_writer_tasks(&mut runner);
    ctx.governance.register_policy_reconciler(&mut runner)?;

    tracing::info!(
        mode = ?ctx.runtime_controls().entry_authorization_policy(),
        tasks = runner.registry_len(),
        "quant-pivot starting",
    );

    let result = runner.run().await;
    ctx.research.runtime_registry.shutdown().await;
    let close_budget = ShutdownBudget::execution().stage(ShutdownStage::DbClose);
    let postgres_result = tokio::time::timeout(close_budget, ctx.infra.pg.close())
        .await
        .unwrap_or_else(|_| {
            Err(StorageError::Timeout {
                operation: "postgres_pool.close".to_owned(),
                duration: close_budget,
            })
        });
    match &postgres_result {
        Ok(()) => tracing::info!("shared PostgreSQL pool closed"),
        Err(error) => tracing::error!(%error, "shared PostgreSQL pool close failed"),
    }
    ctx.infra.redis.close();
    drop(ctx);
    tracing::info!("shared Redis pool closed");
    // Cleanup always runs. Preserve the original runtime failure when both
    // fail, with the independent close failure already emitted above.
    result?;
    postgres_result?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use chrono::Utc;
    use quant_pivot_models::{
        domain::quant::AccountRecoveryIncidentInfo,
        enums::execution::{AccountRecoveryIncidentKind, AccountRecoveryIncidentStatus},
        types::{AccountRecoveryIncidentId, ExecutionAccountId},
    };

    use super::StartupExecutionScope;

    #[test]
    fn recovery_disables_entry_workers() {
        let now = Utc::now();
        let incident = AccountRecoveryIncidentInfo {
            account_recovery_incident_id: AccountRecoveryIncidentId::from_v7(),
            execution_account_id: ExecutionAccountId::from_v7(),
            kind: AccountRecoveryIncidentKind::UnknownExternalExecution,
            status: AccountRecoveryIncidentStatus::Open,
            trigger_chain_execution_id: None,
            reason: "test recovery".to_owned(),
            opened_at: now,
            seal_hash: None,
            sealed_by: None,
            sealed_at: None,
            revision: 0,
            created_at: now,
            updated_at: now,
        };

        assert_eq!(
            StartupExecutionScope::from(Some(&incident)),
            StartupExecutionScope::RecoveryOnly,
        );
        assert_eq!(
            StartupExecutionScope::from(None),
            StartupExecutionScope::EntryEnabled,
        );
    }
}
