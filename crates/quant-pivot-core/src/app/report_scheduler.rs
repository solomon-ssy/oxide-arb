//! Report scheduler task registration (04.3).
//!
//! Registers two Execution-stage tasks:
//! - `ReportGenerator`: rebuilds jobs from the active config, then runs the
//!   cron/interval/ad-hoc scheduler until shutdown (graceful in-flight drain).
//! - `ReportExpireSweep`: a decoupled `PeriodicTask` that expires reports past
//!   their TTL, independent of the fire schedule.

use std::{sync::Arc, time::Duration};

use chrono::Utc;

use super::AppContext;
use crate::{
    app::{task_id::TaskId, task_registry::AppRunner},
    infra::periodic_task::PeriodicTask,
};

/// Max reports expired per sweep pass (bounds one transaction burst).
const EXPIRE_SWEEP_BATCH: u64 = 256;

impl AppContext {
    /// Register the single report fire scheduler (`TaskId::ReportGenerator`).
    ///
    /// On start it rebuilds jobs from the active runtime-config snapshot (no
    /// scheduler persistence: runtime-config is the schedule truth source), then
    /// runs until the Execution-stage shutdown token cancels it.
    pub fn register_report_scheduler(&self, runner: &mut AppRunner) {
        let scheduler = self.report_scheduler();
        let reports = self.runtime_config().current().reports.clone();
        runner.spawn(TaskId::ReportGenerator, move |token| async move {
            if let Err(error) = scheduler.sync_from_config(&reports).await {
                tracing::error!(%error, "initial report schedule sync failed");
            }
            if let Err(error) = scheduler.run(token).await {
                tracing::error!(%error, "report scheduler exited with error");
            }
        });
    }

    /// Register the report TTL expire sweep (`TaskId::ReportExpireSweep`).
    pub fn register_report_expire_sweep(&self, runner: &mut AppRunner) {
        let lifecycle = self.report_lifecycle();
        let runtime_config = self.runtime_config();
        let metrics = Arc::clone(&self.infra.metrics);
        let sweep_secs = self.config.quant.workers.report_expire_sweep_secs;
        runner.spawn(TaskId::ReportExpireSweep, move |token| async move {
            let _ = PeriodicTask::run(
                "report-expire-sweep",
                move || Duration::from_secs(sweep_secs),
                0.0,
                true,
                token,
                move || {
                    let lifecycle = Arc::clone(&lifecycle);
                    let runtime_config = Arc::clone(&runtime_config);
                    let metrics = Arc::clone(&metrics);
                    async move {
                        let ttl_secs = runtime_config.current().reports.report_ttl_secs;
                        // ttl_secs == 0 disables TTL expiry (never anchor at now).
                        if ttl_secs == 0 {
                            return Ok(());
                        }
                        let swept = lifecycle
                            .expire_due_reports(Utc::now(), ttl_secs, EXPIRE_SWEEP_BATCH)
                            .await?;
                        if swept > 0 {
                            metrics.inc_report_expire_swept(u64::from(swept));
                        }
                        Ok(())
                    }
                },
            )
            .await;
        });
    }
}
