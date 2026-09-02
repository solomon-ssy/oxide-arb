//! Exit-monitor worker wiring.
//!
//! Registers the periodic sweep that scans every open position lot and drives
//! the deterministic exit priority ladder (TP / SL / trailing / time / signal /
//! partial / emergency), submitting exit orders, routing to manual review, or
//! holding. The cadence is read from runtime-config
//! (`execution.exit_monitor.monitor_secs`) on every tick, so activation changes
//! take effect without a restart. The worker runs independently of entry authorization —
//! open positions must always be monitored for exit — and gates internally on
//! `execution.exit_monitor.enabled`. Each successful pass publishes the
//! exit-monitor health heartbeat that gates admission `#20`.

use std::{sync::Arc, time::Duration};

use chrono::Utc;

use super::AppContext;
use crate::{
    app::{task_id::TaskId, task_registry::AppRunner},
    infra::periodic_task::PeriodicTask,
};

impl AppContext {
    /// Register the exit-monitor sweep (`TaskId::ExitMonitor`).
    pub fn register_exit_monitor_worker(&self, runner: &mut AppRunner) {
        let service = Arc::clone(&self.execution.exit_monitor);
        let config = self.runtime_config();
        runner.spawn(TaskId::ExitMonitor, move |token| async move {
            let _ = PeriodicTask::run(
                "exit-monitor-worker",
                move || {
                    let secs = config
                        .current()
                        .execution_risk
                        .exit_monitor
                        .monitor_secs
                        .max(1);
                    Duration::from_secs(secs)
                },
                0.0,
                false,
                token,
                move || {
                    let service = Arc::clone(&service);
                    async move {
                        service.run_pass(Utc::now()).await?;
                        Ok(())
                    }
                },
            )
            .await;
        });
    }
}
