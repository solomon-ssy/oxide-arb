//! Periodic background services (Gamma catalog sync, data-quality refresh).

use super::AppContext;
use crate::{
    app::{task_id::TaskId, task_registry::AppRunner},
    infra::periodic_task::PeriodicTask,
    pipeline::data_quality::DataQualityService,
};
use quant_pivot_error::QuantResult;
use std::{sync::Arc, time::Duration};

/// Interval between data-quality snapshot refreshes into Prometheus.
const DATA_QUALITY_REFRESH_SECS: u64 = 5;

impl AppContext {
    pub fn register_periodic_services(&self, runner: &mut AppRunner) {
        let gamma = Arc::clone(&self.data.gamma_service);
        let interval_secs = self.config.market_data.gamma.full_sync_interval_secs;
        runner.spawn(TaskId::GammaSync, move |token| async move {
            let _ = PeriodicTask::run(
                "gamma-sync",
                move || Duration::from_secs(interval_secs),
                0.0,
                false,
                token,
                move || {
                    let gamma = Arc::clone(&gamma);
                    async move { gamma.sync().await }
                },
            )
            .await;
        });

        let data_quality = Arc::clone(&self.data_quality);
        let metrics = Arc::clone(&self.infra.metrics);
        runner.spawn(TaskId::DataQualityRefresh, move |token| async move {
            let _ = PeriodicTask::run(
                "data-quality-refresh",
                || Duration::from_secs(DATA_QUALITY_REFRESH_SECS),
                0.0,
                true,
                token,
                move || {
                    let data_quality = Arc::clone(&data_quality);
                    let metrics = Arc::clone(&metrics);
                    async move {
                        let snapshot = data_quality.snapshot();
                        metrics.set_data_quality_tokens("fresh", snapshot.fresh);
                        metrics.set_data_quality_tokens("acceptable", snapshot.acceptable);
                        metrics.set_data_quality_tokens("degraded", snapshot.degraded);
                        metrics.set_data_quality_tokens("stale", snapshot.stale);
                        metrics.set_data_quality_tokens("insufficient", snapshot.insufficient);
                        metrics.set_fact_lag_worst_ms(data_quality.take_worst_fact_lag_ms());
                        QuantResult::Ok(())
                    }
                },
            )
            .await;
        });
    }
}
