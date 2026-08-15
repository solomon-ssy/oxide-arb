//! Periodic background services (Gamma catalog sync, data-quality refresh).

use std::{sync::Arc, time::Duration};

use chrono::{DateTime, Utc};
use quant_pivot_error::QuantResult;
use quant_pivot_models::{
    domain::ports::PolicySnapshotPort, enums::system::CapabilityId, types::Usd,
};
use quant_pivot_repository::traits::{EquitySnapshotRepository, PositionRepository};

use super::AppContext;
use crate::{
    app::{capability_gate::wait_for_capability, task_id::TaskId, task_registry::AppRunner},
    infra::periodic_task::PeriodicTask,
    ingest::data_quality::DataQualityService,
    service::{equity::EquitySnapshotService, feature_integrity::AutomaticFullParityOutcome},
};

/// Interval between data-quality snapshot refreshes into Prometheus.
const DATA_QUALITY_REFRESH_SECS: u64 = 5;

impl AppContext {
    pub fn register_periodic_services(&self, runner: &mut AppRunner) {
        let gamma = Arc::clone(&self.data.gamma_service);
        let linkage_gamma = Arc::clone(&gamma);
        runner.spawn(TaskId::CatalogLinkageResolver, move |token| async move {
            linkage_gamma.run_linkage_resolver(token).await;
        });
        let interval_secs = self.config.market_data.gamma.reconcile_interval_secs;
        runner.spawn(TaskId::GammaSync, move |token| async move {
            let _ = PeriodicTask::run(
                "gamma-sync",
                move || Duration::from_secs(interval_secs),
                0.0,
                true,
                token,
                move || {
                    let gamma = Arc::clone(&gamma);
                    async move {
                        gamma.sync().await?;
                        QuantResult::Ok(())
                    }
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
                        metrics.set_worst_ingest_lag(data_quality.take_worst_lag_ms());
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
            self.account.execution_account.execution_account_id,
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
                        let budget_cap = Usd::new(
                            runtime_config
                                .current()
                                .execution_risk
                                .portfolio
                                .budget
                                .total_budget_usd
                                .value,
                        );
                        let as_of = Utc::now();
                        let account = account_factory.create(budget_cap)?.snapshot(as_of).await?;
                        equity_service.record_history_snapshot(&account).await?;
                        Ok(())
                    }
                },
            )
            .await;
        });
    }

    pub fn register_feature_parity_scheduler(&self, runner: &mut AppRunner) {
        let coordinator = Arc::clone(&self.report.feature_parity);
        let capabilities = Arc::clone(&self.governance.capabilities);
        runner.spawn(TaskId::FeatureParityScheduler, move |token| async move {
            if !wait_for_capability(
                Arc::clone(&capabilities),
                CapabilityId::AutomaticParityEligible,
                &token,
            )
            .await
            {
                return;
            }
            let _ = PeriodicTask::run(
                "feature-parity-scheduler",
                || next_utc_hour_delay(Utc::now()),
                0.0,
                true,
                token,
                move || {
                    let coordinator = Arc::clone(&coordinator);
                    let capabilities = Arc::clone(&capabilities);
                    async move {
                        if !capabilities
                            .capability_snapshot()
                            .get(CapabilityId::AutomaticParityEligible)
                            .enabled
                        {
                            return QuantResult::Ok(());
                        }
                        match coordinator.ensure_automatic_full(Utc::now()).await? {
                            AutomaticFullParityOutcome::Enqueued { run_id, job_id } => {
                                tracing::info!(%run_id, %job_id, "enqueued automatic 24-hour feature parity replay");
                            }
                            AutomaticFullParityOutcome::Existing(run_id) => {
                                tracing::debug!(%run_id, "automatic feature parity window already queued");
                            }
                            AutomaticFullParityOutcome::NotEligible { reason } => {
                                tracing::debug!(reason = reason.as_str(), "automatic feature parity is waiting for serving evidence");
                            }
                            AutomaticFullParityOutcome::NotDue => {}
                        }
                        QuantResult::Ok(())
                    }
                },
            )
            .await;
        });
    }
}

const fn next_utc_hour_delay(now: DateTime<Utc>) -> Duration {
    let elapsed = now.timestamp().rem_euclid(60 * 60) as u64;
    let nanos = now.timestamp_subsec_nanos();
    let seconds = 60 * 60 - elapsed;
    if nanos == 0 {
        Duration::from_secs(seconds)
    } else {
        Duration::new(seconds - 1, 1_000_000_000 - nanos)
    }
}
