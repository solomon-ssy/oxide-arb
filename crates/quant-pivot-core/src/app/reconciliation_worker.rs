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
use quant_pivot_error::{QuantError, execution::ExecutionError};

use super::AppContext;
use crate::app::{task_id::TaskId, task_registry::AppRunner};

impl AppContext {
    /// Register the reconciliation sweep (`TaskId::ReconciliationWorker`).
    pub fn register_reconciliation_worker(&self, runner: &mut AppRunner) {
        let service = Arc::clone(&self.execution.reconciliation);
        let fee_settlement = Arc::clone(&self.execution.fee_settlement);
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
                if let Err(error) = fee_settlement.settle_pass(now).await {
                    if matches!(
                        &error,
                        QuantError::Execution(ExecutionError::ReconciliationUnresolvable { .. })
                    ) {
                        breaker
                            .trip_kill_switch("on_chain_fee_reconciliation", &error.to_string())
                            .await;
                    }
                    tracing::error!(%error, "on-chain fee reconciliation pass failed");
                    continue;
                }
                let _ = recovery.refresh().await;
            }
        });
    }
}
