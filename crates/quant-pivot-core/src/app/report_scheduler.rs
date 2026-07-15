//! Report scheduler task registration (04.3).
//!
//! Registers the Execution-stage report-lifecycle tasks:
//! - `ReportGenerator`: rebuilds jobs from the active config, then runs the
//!   cron/interval/ad-hoc scheduler until shutdown (graceful in-flight drain).
//! - `RecommendationDeadlineScheduler`: precise per-recommendation TTL wakes
//!   (`DelayQueue` on the data-driven `valid_until`) that expire recommendations
//!   and cascade their reserved capital.
//! - `RecommendationExpireSweep`: the durable poll backstop for the above.
//! - `ReportExpireSweep`: rolls reports up to `Expired` once all their
//!   recommendations are terminal (and finalizes empty reports).

use std::{sync::Arc, time::Duration};

use chrono::Utc;
use quant_pivot_repository::traits::RecommendationRepository;

use super::AppContext;
use crate::{
    app::{task_id::TaskId, task_registry::AppRunner},
    infra::{deadline_scheduler, periodic_task::PeriodicTask},
};

/// Max rows expired per sweep pass (bounds one transaction burst).
const EXPIRE_SWEEP_BATCH: u64 = 256;

impl AppContext {
    /// Register the durable schedule coordinator and global report build worker.
    pub fn register_report_coordinator(&self, runner: &mut AppRunner) {
        let coordinator = self.report_coordinator();
        runner.spawn(TaskId::ReportGenerator, move |token| async move {
            if let Err(error) = Box::pin(coordinator.run(token)).await {
                tracing::error!(%error, "report coordinator exited with error");
            }
        });
    }

    /// Register the report roll-up backstop (`TaskId::ReportExpireSweep`): rolls
    /// reports up to `Expired` once all their recommendations are terminal, and
    /// finalizes empty reports past their roll-up `valid_until`. The per-record
    /// truth is the recommendation expiry; this is the durable poll backstop.
    pub fn register_report_expire_sweep(&self, runner: &mut AppRunner) {
        let lifecycle = self.report_lifecycle();
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
                    let metrics = Arc::clone(&metrics);
                    async move {
                        let rolled = lifecycle
                            .expire_due_reports(Utc::now(), EXPIRE_SWEEP_BATCH)
                            .await?;
                        if rolled > 0 {
                            metrics.inc_report_expire_swept(u64::from(rolled));
                        }
                        Ok(())
                    }
                },
            )
            .await;
        });
    }

    /// Register the precise per-recommendation TTL deadline scheduler
    /// (`TaskId::RecommendationDeadlineScheduler`). Fires expiry exactly at each
    /// recommendation's data-driven `valid_until`; the DB is the source of truth
    /// (every fire re-checks it) and `RecommendationExpireSweep` is the backstop.
    pub fn register_recommendation_deadline_scheduler(&self, runner: &mut AppRunner) {
        let lifecycle = self.report_lifecycle();
        let recommendations: Arc<dyn RecommendationRepository> =
            Arc::clone(&self.infra.repos.recommendation) as Arc<dyn RecommendationRepository>;
        let reconcile = Duration::from_secs(self.config.quant.workers.report_expire_sweep_secs);
        runner.spawn(
            TaskId::RecommendationDeadlineScheduler,
            move |token| async move {
                deadline_scheduler::run(
                    "recommendation-deadline-scheduler",
                    reconcile,
                    token,
                    move |horizon| {
                        let recommendations = Arc::clone(&recommendations);
                        async move {
                            recommendations
                                .upcoming_expirations(horizon, EXPIRE_SWEEP_BATCH)
                                .await
                                .map_err(Into::into)
                        }
                    },
                    move || {
                        let lifecycle = Arc::clone(&lifecycle);
                        async move {
                            lifecycle
                                .expire_due_recommendations(Utc::now(), EXPIRE_SWEEP_BATCH)
                                .await
                        }
                    },
                )
                .await;
            },
        );
    }

    /// Register the per-recommendation TTL expiry poll backstop
    /// (`TaskId::RecommendationExpireSweep`).
    pub fn register_recommendation_expire_sweep(&self, runner: &mut AppRunner) {
        let lifecycle = self.report_lifecycle();
        let sweep_secs = self.config.quant.workers.report_expire_sweep_secs;
        runner.spawn(TaskId::RecommendationExpireSweep, move |token| async move {
            let _ = PeriodicTask::run(
                "recommendation-expire-sweep",
                move || Duration::from_secs(sweep_secs),
                0.0,
                true,
                token,
                move || {
                    let lifecycle = Arc::clone(&lifecycle);
                    async move {
                        lifecycle
                            .expire_due_recommendations(Utc::now(), EXPIRE_SWEEP_BATCH)
                            .await?;
                        Ok(())
                    }
                },
            )
            .await;
        });
    }
}
