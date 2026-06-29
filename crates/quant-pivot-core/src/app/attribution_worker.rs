//! Recommendation-attribution worker wiring (Phase 05.7).
//!
//! The sweep runs in all runtime modes because final attribution is a ledger
//! closeout concern, not an order-submission capability. Runtime config gates
//! the pass with `execution.attribution.enabled`.

use std::{sync::Arc, time::Duration};

use chrono::Utc;

use super::AppContext;
use crate::{
    app::{task_id::TaskId, task_registry::AppRunner},
    infra::periodic_task::PeriodicTask,
};

impl AppContext {
    /// Register the final-attribution sweep (`TaskId::AttributionWorker`).
    pub fn register_attribution_worker(&self, runner: &mut AppRunner) {
        let service = Arc::clone(&self.execution.attribution);
        let config = self.runtime_config();
        runner.spawn(TaskId::AttributionWorker, move |token| async move {
            let cadence_config = Arc::clone(&config);
            let pass_config = Arc::clone(&config);
            let _ = PeriodicTask::run(
                "attribution-worker",
                move || {
                    let secs = cadence_config
                        .current()
                        .execution
                        .attribution
                        .sweep_secs
                        .max(1);
                    Duration::from_secs(secs)
                },
                0.0,
                false,
                token,
                move || {
                    let service = Arc::clone(&service);
                    let snapshot = pass_config.current();
                    async move {
                        if !snapshot.execution.attribution.enabled {
                            return Ok(());
                        }
                        let summary = service
                            .run_pass(Utc::now(), snapshot.execution.attribution.batch_size)
                            .await?;
                        if summary.written > 0 || summary.skipped > 0 {
                            tracing::info!(
                                considered = summary.considered,
                                written = summary.written,
                                skipped = summary.skipped,
                                "attribution sweep completed",
                            );
                        }
                        Ok(())
                    }
                },
            )
            .await;
        });
    }
}
