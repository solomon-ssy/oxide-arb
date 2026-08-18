//! Reconciliation worker wiring.
//!
//! Registers the periodic sweep that resolves in-flight orders against
//! Polymarket venue truth. The cadence is read from runtime-config
//! (`execution.reconciliation.interval_secs`) on every tick, so activation
//! changes take effect without a restart. The worker runs in **all** runtime
//! authorization policies and gates internally on
//! `execution.reconciliation.enabled`.

use std::{sync::Arc, time::Duration};

use chrono::Utc;

use super::AppContext;
use crate::app::{task_id::TaskId, task_registry::AppRunner};

impl AppContext {
    /// Register the reconciliation sweep (`TaskId::ReconciliationWorker`).
    pub fn register_reconciliation_worker(&self, runner: &mut AppRunner) {
        let service = Arc::clone(&self.execution.reconciliation);
        let account_chain_projector = Arc::clone(&self.execution.account_chain_projector);
        let breaker = Arc::clone(&self.execution.breaker);
        let recovery = Arc::clone(&self.governance.execution_recovery);
        let config = self.runtime_config();
        let terms_drift_wake = self.data.terms_drift_wake.clone();
        runner.spawn(TaskId::ReconciliationWorker, move |token| async move {
            let mut first_pass = true;
            loop {
                let terms_change = if first_pass {
                    first_pass = false;
                    false
                } else {
                    let interval_secs = config
                        .current()
                        .execution_risk
                        .reconciliation
                        .interval_secs
                        .max(1);
                    tokio::select! {
                        () = token.cancelled() => break,
                        () = tokio::time::sleep(Duration::from_secs(interval_secs)) => false,
                        () = terms_drift_wake.notified() => true,
                    }
                };
                let now = Utc::now();
                if let Err(error) = account_chain_projector.project_pass().await {
                    breaker
                        .trip_kill_switch("account_chain_projection", &error.to_string())
                        .await;
                    tracing::error!(%error, "account chain projection pass failed");
                    continue;
                }
                let result = if terms_change {
                    let mut market_ids = terms_drift_wake
                        .take_markets()
                        .into_iter()
                        .collect::<Vec<_>>();
                    market_ids.sort();
                    service.reconcile_terms_changes(now, &market_ids).await
                } else {
                    service.reconcile_pass(now).await
                };
                if let Err(error) = result {
                    tracing::error!(%error, terms_change, "reconciliation worker pass failed");
                    continue;
                }
                let _ = recovery.refresh().await;
            }
        });
    }
}
