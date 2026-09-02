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

/// Executes all orthogonal outcome lanes without allowing one to starve another.
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
    /// Account truth runs first, economic horizon work second, and external
    /// resolution last. Every lane runs even when an earlier lane fails.
    pub async fn run_once(&self, config: OutcomeReconciliationPassConfig) -> QuantResult<()> {
        let execution = self.service.run_execution_pass(config).await;
        if let Ok(summary) = &execution {
            (*summary).log_execution_summary();
        }
        let economic = self.service.run_economic_pass(config).await;
        if let Ok(summary) = &economic {
            (*summary).log_economic_summary();
        }
        let resolution = self.service.run_resolution_pass(config).await;
        if let Ok(summary) = &resolution {
            (*summary).log_resolution_summary();
        }
        let mut first_error = execution.err();
        if let Err(error) = economic {
            if first_error.is_none() {
                first_error = Some(error);
            } else {
                tracing::warn!(%error, "economic outcome reconciliation also failed");
            }
        }
        if let Err(error) = resolution {
            if first_error.is_none() {
                first_error = Some(error);
            } else {
                tracing::warn!(%error, "resolution reconciliation also failed");
            }
        }
        first_error.map_or(Ok(()), Err)
    }
}

impl AppContext {
    /// Register outcome reconciliation independently of entry authorization.
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
                            .operations_policy
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
                            let policy = &snapshot.operations_policy.outcome_reconciliation;
                            if !policy.enabled {
                                return Ok(());
                            }
                            worker
                                .run_once(OutcomeReconciliationPassConfig {
                                    pass_started_at: Utc::now(),
                                    sweep_secs: policy.sweep_secs,
                                    candidate_batch_size: policy.candidate_batch_size,
                                    source_block_span: policy.source_block_span,
                                    economic_source_lateness_secs: policy
                                        .economic_source_lateness_secs,
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
    fn log_economic_summary(self) {
        if self.economic_candidates > 0 {
            tracing::info!(
                candidates = self.economic_candidates,
                inserted = self.economic_inserted,
                existing = self.economic_existing,
                deferred = self.economic_deferred,
                capacity_deferred = self.economic_capacity_deferred,
                censored = self.economic_censored,
                claim_lost = self.economic_claim_lost,
                "recommendation economic outcome reconciliation completed"
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
                deferred = self.resolution_deferred,
                "resolution outcome reconciliation completed"
            );
        }
    }
}
