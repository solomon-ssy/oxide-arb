//! Runtime owner for orthogonal recommendation outcome reconciliation.

use std::{sync::Arc, time::Duration};

use chrono::Utc;
use quant_pivot_error::QuantResult;

use super::AppContext;
use crate::{
    app::{task_id::TaskId, task_registry::AppRunner},
    execution::{
        OutcomeReconciliationPassConfig, OutcomeReconciliationPassSummary,
        OutcomeReconciliationService,
    },
    infra::periodic_task::PeriodicTask,
};

/// Executes both outcome lanes without allowing one lane to starve the other.
pub struct OutcomeReconciliationWorker {
    service: Arc<OutcomeReconciliationService>,
}

impl OutcomeReconciliationWorker {
    #[must_use]
    pub const fn new(service: Arc<OutcomeReconciliationService>) -> Self {
        Self { service }
    }

    /// Execute one bounded pass, preserving fail-closed error semantics.
    ///
    /// Execution truth is attempted first because it does not depend on the
    /// external resolution source. Resolution is always attempted afterwards,
    /// even when execution reconciliation fails.
    pub async fn run_once(&self, config: OutcomeReconciliationPassConfig) -> QuantResult<()> {
        let execution = self.service.run_execution_pass(config).await;
        if let Ok(summary) = &execution {
            (*summary).log_execution_summary();
        }

        let resolution = self.service.run_resolution_pass(config).await;
        if let Ok(summary) = &resolution {
            (*summary).log_resolution_summary();
        }

        match (execution, resolution) {
            (Ok(_), Ok(_)) => Ok(()),
            (Err(error), Ok(_)) | (Ok(_), Err(error)) => Err(error),
            (Err(execution_error), Err(resolution_error)) => {
                tracing::warn!(
                    error = %resolution_error,
                    "resolution reconciliation also failed after execution reconciliation failure"
                );
                Err(execution_error)
            }
        }
    }
}

impl AppContext {
    /// Register outcome reconciliation in every runtime mode.
    pub fn register_outcome_reconciliation_worker(&self, runner: &mut AppRunner) {
        let worker = Arc::new(OutcomeReconciliationWorker::new(Arc::clone(
            &self.execution.outcome_reconciliation,
        )));
        let config = self.runtime_config();
        runner.spawn(
            TaskId::OutcomeReconciliationWorker,
            move |token| async move {
                let cadence_config = Arc::clone(&config);
                let pass_config = Arc::clone(&config);
                let _ = PeriodicTask::run(
                    "outcome-reconciliation-worker",
                    move || {
                        let secs = cadence_config
                            .current()
                            .operational_control
                            .outcome_reconciliation
                            .sweep_secs
                            .max(1);
                        Duration::from_secs(secs)
                    },
                    0.1,
                    false,
                    token,
                    move || {
                        let worker = Arc::clone(&worker);
                        let snapshot = pass_config.current();
                        async move {
                            let policy = &snapshot.operational_control.outcome_reconciliation;
                            if !policy.enabled {
                                return Ok(());
                            }
                            worker
                                .run_once(OutcomeReconciliationPassConfig {
                                    pass_started_at: Utc::now(),
                                    candidate_batch_size: policy.candidate_batch_size,
                                    source_block_span: policy.source_block_span,
                                })
                                .await
                        }
                    },
                )
                .await;
            },
        );
    }
}

impl OutcomeReconciliationPassSummary {
    fn log_execution_summary(self) {
        if self.execution_candidates > 0 {
            tracing::info!(
                candidates = self.execution_candidates,
                inserted = self.execution_inserted,
                existing = self.execution_existing,
                deferred = self.execution_deferred,
                "execution outcome reconciliation completed"
            );
        }
    }
}

impl OutcomeReconciliationPassSummary {
    fn log_resolution_summary(self) {
        let observed_work =
            self.source_scans + self.source_observations + self.resolution_candidates;
        if observed_work > 0 || self.cursor_conflicted {
            tracing::info!(
                source_scans = self.source_scans,
                source_observations = self.source_observations,
                source_unknown_markets = self.source_unknown_markets,
                source_facts_written = self.source_facts_written,
                source_facts_recovered = self.source_facts_recovered,
                cursor_advanced = self.cursor_advanced,
                cursor_conflicted = self.cursor_conflicted,
                candidates = self.resolution_candidates,
                inserted = self.resolution_inserted,
                existing = self.resolution_existing,
                "resolution outcome reconciliation completed"
            );
        }
    }
}
