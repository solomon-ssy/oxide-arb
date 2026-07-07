//! Periodic background services (Gamma catalog sync, data-quality refresh).

use super::AppContext;
use crate::{
    app::{task_id::TaskId, task_registry::AppRunner},
    infra::periodic_task::PeriodicTask,
    ingest::data_quality::DataQualityService,
    service::equity::EquitySnapshotService,
};
use quant_pivot_error::{QuantError, QuantResult};
use quant_pivot_models::{domain::RuntimeConfigPort, types::Usd};
use quant_pivot_repository::traits::{EquitySnapshotRepository, PositionRepository};
use rust_decimal::Decimal;
use std::str::FromStr;
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

        let data_quality = Arc::clone(&self.data.data_quality);
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
                        metrics.set_ingest_pipeline_lag_worst_ms(
                            data_quality.take_worst_ingest_lag_ms(),
                        );
                        QuantResult::Ok(())
                    }
                },
            )
            .await;
        });
    }

    pub fn register_equity_snapshot_worker(&self, runner: &mut AppRunner) {
        let account_factory = Arc::clone(&self.account.provider_factory);
        let runtime_config = Arc::clone(&self.governance.applicator);
        let equity_service = Arc::new(EquitySnapshotService::new(
            Arc::clone(&self.infra.repos.equity_snapshot) as Arc<dyn EquitySnapshotRepository>,
            Arc::clone(&self.infra.repos.position) as Arc<dyn PositionRepository>,
        ));
        let interval_secs = self.config.quant.workers.equity_snapshot_secs;
        runner.spawn(TaskId::EquitySnapshotWorker, move |token| async move {
            let _ = PeriodicTask::run(
                "equity-snapshot-worker",
                move || Duration::from_secs(interval_secs),
                0.0,
                true,
                token,
                move || {
                    let account_factory = Arc::clone(&account_factory);
                    let runtime_config = Arc::clone(&runtime_config);
                    let equity_service = Arc::clone(&equity_service);
                    async move {
                        let budget_cap = parse_budget_cap(
                            &runtime_config
                                .current()
                                .portfolio
                                .budget
                                .total_budget_usd
                                .value,
                        )?;
                        let as_of = chrono::Utc::now();
                        let account = account_factory.create(budget_cap)?.snapshot(as_of).await?;
                        equity_service.record_history_snapshot(&account).await?;
                        Ok(())
                    }
                },
            )
            .await;
        });
    }
}

fn parse_budget_cap(raw: &str) -> QuantResult<Usd> {
    let value = Decimal::from_str(raw.trim()).map_err(|error| {
        QuantError::config(format!(
            "invalid portfolio.budget.total_budget_usd `{raw}`: {error}"
        ))
    })?;
    Ok(Usd::new(value))
}
