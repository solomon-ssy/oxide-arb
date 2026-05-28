//! Periodic background services — risk ticks, catalog sync, health, metrics refresh.

use super::{AppContext, task_id::TaskId};
use crate::{
    infra::{
        debounced_writer::DebouncedWriter, health_checker::HealthChecker,
        periodic_task::PeriodicTask,
    },
    observability::{
        metrics_hub::MetricsHub,
        report_generator::{ReportGenerator, previous_utc_day, previous_utc_week_start},
    },
    service::risk_metrics::RiskMetricsRefreshService,
};
use chrono::Datelike;
use oxide_arb_algorithm::calibration::{CalibrationUpdater, ResolutionCalibrator};
use oxide_arb_error::{OxideError, OxideResult};
use oxide_arb_models::{domain::risk::UpsertRiskEngineState, enums::common::ExecutionMode};
use oxide_arb_repository::traits::RiskStateRepository;
use oxide_arb_risk::{engine::RiskEngine, traits::RiskMetrics};
use std::{sync::Arc, time::Duration};

const RISK_TICK_INTERVAL_SECS: u64 = 5;
const EXPOSURE_GC_INTERVAL_SECS: u64 = 30;
const HEALTH_CHECK_INTERVAL_SECS: u64 = 30;
const PERIODIC_JITTER_PCT: f64 = 0.1;

impl AppContext {
    /// Register all periodic background services into the pending task queue.
    ///
    /// Call after `queue_runtime_tasks()` in bootstrap. Startup sync for gamma,
    /// calibration, and Live metrics refresh must complete in `AppContext::build`
    /// before this runs.
    pub fn queue_periodic_services(&self) {
        self.queue_gamma_sync();
        self.queue_risk_tick();
        self.queue_risk_state_debouncer();
        self.queue_exposure_gc();
        self.queue_calibration_updater();
        self.queue_risk_metrics_refresh();
        self.queue_market_settlement_retry();
        self.queue_report_generator();
        self.queue_health_checker();
    }
}

impl AppContext {
    fn queue_gamma_sync(&self) {
        let gamma_service = Arc::clone(&self.data.gamma_service);
        let interval_secs = self
            .config
            .market_data
            .gamma
            .full_sync_interval_secs
            .max(60);

        self.pending_tasks
            .push(TaskId::GammaSync, move |shutdown| async move {
                if let Err(error) = PeriodicTask::run(
                    TaskId::GammaSync.static_name(),
                    Duration::from_secs(interval_secs),
                    PERIODIC_JITTER_PCT,
                    true,
                    shutdown,
                    || {
                        let gamma_service = Arc::clone(&gamma_service);
                        async move { gamma_service.sync().await }
                    },
                )
                .await
                {
                    tracing::error!(%error, "gamma sync periodic task exited");
                }
            });
    }

    fn queue_risk_tick(&self) {
        let risk_engine = Arc::clone(&self.risk.engine);
        let risk_metrics = Arc::clone(&self.risk.metrics);

        self.pending_tasks
            .push(TaskId::RiskTick, move |shutdown| async move {
                if let Err(error) = PeriodicTask::run(
                    TaskId::RiskTick.static_name(),
                    Duration::from_secs(RISK_TICK_INTERVAL_SECS),
                    0.0,
                    false,
                    shutdown,
                    || {
                        let risk_engine = Arc::clone(&risk_engine);
                        let risk_metrics = Arc::clone(&risk_metrics);
                        async move { run_risk_tick(&risk_engine, risk_metrics.as_ref()).await }
                    },
                )
                .await
                {
                    tracing::error!(%error, "risk tick periodic task exited");
                }
            });
    }

    fn queue_exposure_gc(&self) {
        let exposure = Arc::clone(&self.risk.exposure);
        let metrics = Arc::clone(&self.infra.metrics);

        self.pending_tasks
            .push(TaskId::ExposureGc, move |shutdown| async move {
                if let Err(error) = PeriodicTask::run(
                    TaskId::ExposureGc.static_name(),
                    Duration::from_secs(EXPOSURE_GC_INTERVAL_SECS),
                    0.0,
                    false,
                    shutdown,
                    || {
                        let exposure = Arc::clone(&exposure);
                        let metrics = Arc::clone(&metrics);
                        async move {
                            let expired = exposure.gc_expired();
                            if expired > 0 {
                                tracing::info!(expired, "exposure GC cleaned expired reservations");
                                metrics.exposure_gc_cleaned.inc_by(u64::from(expired));
                            }
                            Ok(())
                        }
                    },
                )
                .await
                {
                    tracing::error!(%error, "exposure GC periodic task exited");
                }
            });
    }

    fn queue_risk_state_debouncer(&self) {
        let risk_engine = Arc::clone(&self.risk.engine);
        let risk_metrics = Arc::clone(&self.risk.metrics);
        let risk_state_repo = Arc::clone(&self.infra.risk_state_repo);
        let shutdown = self.shutdown.clone();
        let (writer, worker) = DebouncedWriter::new(
            TaskId::RiskStateDebouncer.static_name(),
            Duration::from_secs(60),
            move |state: UpsertRiskEngineState| {
                let repo = Arc::clone(&risk_state_repo);
                Box::pin(async move { repo.upsert(state).await.map_err(Into::into) })
            },
            shutdown,
        );

        writer.update(UpsertRiskEngineState::from(
            &risk_engine.snapshot(risk_metrics.as_ref()),
        ));

        self.pending_tasks
            .push(TaskId::RiskStateDebouncer, move |shutdown| async move {
                let mut interval = tokio::time::interval(Duration::from_secs(60));
                interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
                tokio::pin!(worker);

                loop {
                    tokio::select! {
                        () = shutdown.cancelled() => {
                            return;
                        }
                        result = &mut worker => {
                            if let Err(error) = result {
                                tracing::error!(%error, "risk state debouncer exited with error");
                            }
                            return;
                        }
                        _ = interval.tick() => {
                            writer.update(UpsertRiskEngineState::from(
                                &risk_engine.snapshot(risk_metrics.as_ref()),
                            ));
                        }
                    }
                }
            });
    }

    fn queue_calibration_updater(&self) {
        let updater = Arc::clone(&self.trading.calibration_updater);
        let metrics = Arc::clone(&self.infra.metrics);
        let calibrator = Arc::clone(&self.trading.calibrator);
        let interval_secs = self
            .config
            .detection
            .calibration
            .refresh_interval_secs
            .max(300);

        self.pending_tasks
            .push(TaskId::CalibrationUpdater, move |shutdown| async move {
                if let Err(error) = PeriodicTask::run(
                    TaskId::CalibrationUpdater.static_name(),
                    Duration::from_secs(interval_secs),
                    PERIODIC_JITTER_PCT,
                    true,
                    shutdown,
                    || {
                        let updater = Arc::clone(&updater);
                        let metrics = Arc::clone(&metrics);
                        let calibrator = Arc::clone(&calibrator);
                        async move { run_calibration_tick(&updater, &metrics, &calibrator).await }
                    },
                )
                .await
                {
                    tracing::error!(%error, "calibration updater periodic task exited");
                }
            });
    }

    fn queue_risk_metrics_refresh(&self) {
        let Some(refresher) = self.risk.metrics_refresh.as_ref() else {
            tracing::info!("risk metrics refresh skipped — ClobClient unavailable");
            return;
        };
        let refresher = Arc::clone(refresher);
        let interval_secs = self.config.risk.metrics_refresh_interval_secs.max(1);

        self.pending_tasks
            .push(TaskId::RiskMetricsRefresh, move |shutdown| async move {
                if let Err(error) = PeriodicTask::run(
                    TaskId::RiskMetricsRefresh.static_name(),
                    Duration::from_secs(interval_secs),
                    PERIODIC_JITTER_PCT,
                    true,
                    shutdown,
                    || {
                        let refresher = Arc::clone(&refresher);
                        async move {
                            refresher.refresh().await.map_err(|error| {
                                tracing::warn!(%error, "risk metrics refresh failed");
                                error
                            })
                        }
                    },
                )
                .await
                {
                    tracing::error!(%error, "risk metrics refresh periodic task exited");
                }
            });
    }

    fn queue_market_settlement_retry(&self) {
        let settlement = Arc::clone(&self.settlement.service);
        let interval_secs = self.config.settlement.lifecycle.retry_interval_secs.max(10);

        self.pending_tasks
            .push(TaskId::MarketSettlementRetry, move |shutdown| async move {
                if let Err(error) = PeriodicTask::run(
                    TaskId::MarketSettlementRetry.static_name(),
                    Duration::from_secs(interval_secs),
                    PERIODIC_JITTER_PCT,
                    true,
                    shutdown,
                    || {
                        let settlement = Arc::clone(&settlement);
                        async move { settlement.retry_pending().await }
                    },
                )
                .await
                {
                    tracing::error!(%error, "market settlement retry task exited");
                }
            });
    }

    fn queue_health_checker(&self) {
        let pg = Arc::clone(&self.infra.pg);
        let ch = Arc::clone(&self.infra.ch);
        let ws = Arc::clone(&self.trading.ws_manager);
        let metrics = Arc::clone(&self.infra.metrics);

        self.pending_tasks
            .push(TaskId::HealthChecker, move |shutdown| async move {
                let checker = HealthChecker::new(pg, ch, ws);
                if let Err(error) = PeriodicTask::run(
                    TaskId::HealthChecker.static_name(),
                    Duration::from_secs(HEALTH_CHECK_INTERVAL_SECS),
                    0.0,
                    false,
                    shutdown,
                    || {
                        let checker_ref = &checker;
                        let metrics_ref = &metrics;
                        async move {
                            let report = checker_ref.check_all().await;
                            if !report.overall_healthy {
                                tracing::warn!(
                                    checks = ?report.checks.iter()
                                        .filter(|c| !c.healthy)
                                        .map(|c| c.name.as_str())
                                        .collect::<Vec<_>>(),
                                    "health check detected unhealthy subsystems"
                                );
                                metrics_ref.health_check_failures.inc();
                            }
                            Ok(())
                        }
                    },
                )
                .await
                {
                    tracing::error!(%error, "health checker periodic task exited");
                }
            });
    }

    fn queue_report_generator(&self) {
        let generator = Arc::new(ReportGenerator::new(
            Arc::clone(&self.infra.trade_repo),
            Arc::clone(&self.infra.position_repo),
            Arc::clone(&self.infra.report_repo),
            Arc::clone(&self.risk.engine),
            self.risk.metrics.clone(),
            Arc::clone(&self.infra.alerts),
        ));

        self.pending_tasks
            .push(TaskId::ReportGenerator, move |shutdown| async move {
                if let Err(error) = PeriodicTask::run(
                    TaskId::ReportGenerator.static_name(),
                    Duration::from_secs(3600),
                    PERIODIC_JITTER_PCT,
                    false,
                    shutdown,
                    || {
                        let generator = Arc::clone(&generator);
                        async move {
                            let now = chrono::Utc::now();
                            generator.generate_daily(previous_utc_day(now)).await?;
                            if now.weekday() == chrono::Weekday::Mon {
                                generator
                                    .generate_weekly(previous_utc_week_start(now))
                                    .await?;
                            }
                            Ok(())
                        }
                    },
                )
                .await
                {
                    tracing::error!(%error, "report generator periodic task exited");
                }
            });
    }
}

async fn run_risk_tick(
    risk_engine: &RiskEngine,
    risk_metrics: &dyn RiskMetrics,
) -> OxideResult<()> {
    risk_engine
        .tick(risk_metrics)
        .await
        .map(|_| ())
        .map_err(|error| {
            tracing::error!(%error, "risk tick failed — engine may halt");
            error
        })
}

async fn run_calibration_tick(
    updater: &CalibrationUpdater,
    metrics: &MetricsHub,
    calibrator: &ResolutionCalibrator,
) -> OxideResult<()> {
    match updater.tick().await {
        Ok(stats) => {
            metrics.calibration_update_total.inc();
            if stats.resolved > 0 {
                metrics
                    .calibration_resolved
                    .inc_by(u64::from(stats.resolved));
            }
            metrics
                .calibration_bucket_count
                .set(i64::try_from(calibrator.bucket_count()).unwrap_or(i64::MAX));
            tracing::debug!(
                total_unresolved = stats.total_unresolved,
                resolved = stats.resolved,
                gamma_miss = stats.gamma_miss,
                buckets = calibrator.bucket_count(),
                "calibration update completed"
            );
            Ok(())
        }
        Err(error) => {
            tracing::error!(%error, "calibration refresh failed");
            Err(OxideError::Algorithm(error))
        }
    }
}

/// Live-mode gate: metrics snapshot must be fresh before trading loops start.
pub async fn ensure_live_metrics_ready(
    mode: ExecutionMode,
    refresher: Option<&RiskMetricsRefreshService>,
) -> OxideResult<()> {
    if mode != ExecutionMode::Live {
        return Ok(());
    }
    let Some(refresher) = refresher else {
        return Err(OxideError::Internal(
            "Live mode requires ClobClient for risk metrics refresh".into(),
        ));
    };
    refresher.refresh().await.map_err(|error| {
        tracing::error!(
            %error,
            "Live startup metrics refresh failed — refusing to start"
        );
        error
    })
}

/// Best-effort calibration resolution tick at startup (does not block boot).
pub async fn run_calibration_startup_tick(
    updater: &CalibrationUpdater,
    metrics: &MetricsHub,
    calibrator: &ResolutionCalibrator,
) {
    if let Err(error) = run_calibration_tick(updater, metrics, calibrator).await {
        tracing::warn!(
            %error,
            "calibration startup tick failed — continuing with loaded buckets"
        );
    }
}
