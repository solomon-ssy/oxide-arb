//! Reconciliation worker wiring.
//!
//! Registers the periodic sweep that resolves in-flight orders against
//! Polymarket venue truth. The cadence is read from runtime-config
//! (`execution.reconciliation.interval_secs`) on every tick, so activation
//! changes take effect without a restart. The worker runs in **all** runtime
//! modes — in-flight money must be reconciled regardless of the current mode —
//! and gates internally on `execution.reconciliation.enabled`.

use std::{sync::Arc, time::Duration};

use chrono::Utc;

use super::AppContext;
use crate::{
    app::{task_id::TaskId, task_registry::AppRunner},
    infra::periodic_task::PeriodicTask,
};

impl AppContext {
    /// Register the reconciliation sweep (`TaskId::ReconciliationWorker`).
    pub fn register_reconciliation_worker(&self, runner: &mut AppRunner) {
        let service = Arc::clone(&self.execution.reconciliation);
        let recovery = Arc::clone(&self.governance.execution_recovery);
        let config = self.runtime_config();
        runner.spawn(TaskId::ReconciliationWorker, move |token| async move {
            let _ = PeriodicTask::run(
                "reconciliation-worker",
                move || {
                    let secs = config
                        .current()
                        .execution_risk
                        .reconciliation
                        .interval_secs
                        .max(1);
                    Duration::from_secs(secs)
                },
                0.0,
                true,
                token,
                move || {
                    let service = Arc::clone(&service);
                    let recovery = Arc::clone(&recovery);
                    async move {
                        service.reconcile_pass(Utc::now()).await?;
                        let _ = recovery.refresh().await;
                        Ok(())
                    }
                },
            )
            .await;
        });
    }
}
