//! Periodic background services — risk ticks, catalog sync, health, metrics refresh.

use super::{AppContext, task_id::TaskId};
use crate::{
    bridge::execution_mode::ExecutionModeHandle,
    control::status::SystemStatusNudge,
    infra::{debounced_writer::DebouncedWriter, periodic_task::PeriodicTask},
    observability::{
        balance_fact_writer::{BalanceFactObservation, BalanceFactWriter},
        book_fact_writer::BookFactWriter,
        metrics_hub::MetricsHub,
        report_generator::{ReportGenerator, previous_utc_day, previous_utc_week_start},
    },
    pipeline::{book_store::BookStore, market_registry::MarketRegistry},
    service::{
        catalog_readiness::CatalogReadiness, gamma::GammaService,
        risk_metrics::RiskMetricsRefreshService,
    },
};
use chrono::Datelike;
use oxide_arb_algorithm::calibration::{CalibrationUpdater, ResolutionCalibrator};
use oxide_arb_api::{
    clob::ClobClient,
    ctf::client::CtfRedeemClient,
    infra::retry::{RetryController, RetryDecision, RetryPolicy},
};
use oxide_arb_error::{OxideError, OxideResult};
use oxide_arb_models::{
    domain::risk::UpsertRiskEngineState,
    enums::clickhouse::ChSnapshotReason,
    enums::common::ExecutionMode,
    types::{MarketId, Shares, Usd},
};
use oxide_arb_repository::traits::RiskStateRepository;
use oxide_arb_risk::{engine::RiskEngine, traits::RiskMetrics};
use prometheus::IntGauge;
use rust_decimal::Decimal;
use std::{collections::HashMap, sync::Arc, time::Duration};
use tokio_util::sync::CancellationToken;

const RISK_TICK_INTERVAL_SECS: u64 = 5;
const HEALTH_CHECK_INTERVAL_SECS: u64 = 30;
const BOOK_SNAPSHOT_PUBLISH_INTERVAL_SECS: u64 = 60;
const PERIODIC_JITTER_PCT: f64 = 0.1;

struct LedgerReconciliationDeps<'a> {
    /// Live execution-mode handle, read per tick: external-balance
    /// reconciliation only makes sense against real funds, so every
    /// non-Live tick is skipped and a runtime switch to Live re-arms the
    /// task without a restart.
    execution_mode: &'a ExecutionModeHandle,
    risk_engine: &'a RiskEngine,
    risk_metrics: &'a dyn RiskMetrics,
    clob_client: &'a ClobClient,
    ctf_redeem: &'a CtfRedeemClient,
    balance_fact_writer: &'a BalanceFactWriter,
    holder_address: &'a str,
}

impl AppContext {
    /// Register all periodic background services into the pending task queue.
    ///
    /// Call after `queue_runtime_tasks()` in bootstrap. Calibration startup
    /// and the Live metrics refresh still complete in `AppContext::build`;
    /// the first Gamma catalog sync runs as the warmup phase of the
    /// `GammaSync` task (see [`Self::queue_gamma_sync`]).
    pub fn queue_periodic_services(&self) {
        self.queue_gamma_sync();
        self.queue_risk_tick();
        self.queue_risk_state_debouncer();
        self.queue_exposure_gc();
        self.queue_calibration_updater();
        self.queue_risk_metrics_refresh();
        self.queue_ledger_reconciliation();
        self.queue_book_snapshot_publisher();
        self.queue_market_settlement_retry();
        self.queue_report_generator();
        self.queue_health_checker();
    }
}

impl AppContext {
    /// Catalog warmup + periodic Gamma sync.
    ///
    /// Phase 1 (warmup): retry the first sync indefinitely with exponential
    /// backoff — a Polymarket outage delays detection but never blocks or
    /// kills the process. The first success flips [`CatalogReadiness`] to
    /// `Ready`, unlocking the scanner. Phase 2: the regular periodic cadence.
    fn queue_gamma_sync(&self) {
        let gamma_service = Arc::clone(&self.data.gamma_service);
        let catalog = Arc::clone(&self.data.catalog);
        let market_registry = Arc::clone(&self.data.market_registry);
        let catalog_ready_gauge = self.infra.metrics.catalog_ready.clone();
        let status_nudge = self.system_status_nudge.clone();
        let interval_secs = self
            .config
            .market_data
            .gamma
            .full_sync_interval_secs
            .max(60);

        self.pending_tasks
            .push(TaskId::GammaSync, move |shutdown| async move {
                let deps = CatalogSyncDeps {
                    gamma_service,
                    catalog,
                    market_registry,
                    catalog_ready_gauge,
                    status_nudge,
                };
                if !warmup_catalog(&deps, &shutdown).await {
                    return;
                }
                if let Err(error) = PeriodicTask::run(
                    TaskId::GammaSync.static_name(),
                    move || Duration::from_secs(interval_secs),
                    PERIODIC_JITTER_PCT,
                    true,
                    shutdown,
                    || {
                        let deps = deps.clone();
                        async move {
                            deps.gamma_service.sync().await?;
                            deps.mark_ready();
                            Ok(())
                        }
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
                    || Duration::from_secs(RISK_TICK_INTERVAL_SECS),
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
        let runtime = Arc::clone(&self.runtime_config);

        self.pending_tasks
            .push(TaskId::ExposureGc, move |shutdown| async move {
                let interval_runtime = Arc::clone(&runtime);
                if let Err(error) = PeriodicTask::run(
                    TaskId::ExposureGc.static_name(),
                    move || {
                        Duration::from_secs(
                            interval_runtime
                                .load()
                                .risk
                                .reservation_gc_interval_secs
                                .max(1),
                        )
                    },
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

        self.pending_tasks
            .push(TaskId::CalibrationUpdater, move |shutdown| async move {
                let interval_updater = Arc::clone(&updater);
                if let Err(error) = PeriodicTask::run(
                    TaskId::CalibrationUpdater.static_name(),
                    move || Duration::from_secs(interval_updater.refresh_interval_secs().max(300)),
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
        let refresher = Arc::clone(&self.risk.metrics_refresh);
        let runtime = Arc::clone(&self.runtime_config);

        self.pending_tasks
            .push(TaskId::RiskMetricsRefresh, move |shutdown| async move {
                let interval_runtime = Arc::clone(&runtime);
                if let Err(error) = PeriodicTask::run(
                    TaskId::RiskMetricsRefresh.static_name(),
                    move || {
                        Duration::from_secs(
                            interval_runtime
                                .load()
                                .risk
                                .metrics_refresh_interval_secs
                                .max(1),
                        )
                    },
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

    fn queue_ledger_reconciliation(&self) {
        let Some(clob_client) = self.trading.clob_client.as_ref() else {
            tracing::info!("ledger reconciliation skipped — ClobClient unavailable");
            return;
        };
        let Some(ctf_redeem) = self.trading.ctf_redeem.as_ref() else {
            tracing::info!("ledger reconciliation skipped — CTF redeem client unavailable");
            return;
        };
        let clob_client = Arc::clone(clob_client);
        let ctf_redeem = Arc::clone(ctf_redeem);
        let execution_mode = self.execution_mode.clone();
        let risk_engine = Arc::clone(&self.risk.engine);
        let risk_metrics = Arc::clone(&self.risk.metrics);
        let balance_fact_writer = Arc::clone(&self.infra.balance_fact_writer);
        let holder_address = self.infra.holder_address.clone();
        let runtime = Arc::clone(&self.runtime_config);

        self.pending_tasks
            .push(TaskId::LedgerReconciliation, move |shutdown| async move {
                let interval_runtime = Arc::clone(&runtime);
                if let Err(error) = PeriodicTask::run(
                    TaskId::LedgerReconciliation.static_name(),
                    move || {
                        Duration::from_secs(
                            interval_runtime
                                .load()
                                .risk
                                .reconciliation_interval_secs
                                .max(30),
                        )
                    },
                    PERIODIC_JITTER_PCT,
                    true,
                    shutdown,
                    || {
                        let clob_client = Arc::clone(&clob_client);
                        let ctf_redeem = Arc::clone(&ctf_redeem);
                        let execution_mode = execution_mode.clone();
                        let risk_engine = Arc::clone(&risk_engine);
                        let risk_metrics = Arc::clone(&risk_metrics);
                        let balance_fact_writer = Arc::clone(&balance_fact_writer);
                        let holder_address = holder_address.clone();
                        async move {
                            run_ledger_reconciliation(LedgerReconciliationDeps {
                                execution_mode: &execution_mode,
                                risk_engine: &risk_engine,
                                risk_metrics: risk_metrics.as_ref(),
                                clob_client: &clob_client,
                                ctf_redeem: &ctf_redeem,
                                balance_fact_writer: &balance_fact_writer,
                                holder_address: &holder_address,
                            })
                            .await
                        }
                    },
                )
                .await
                {
                    tracing::error!(%error, "ledger reconciliation periodic task exited");
                }
            });
    }

    fn queue_market_settlement_retry(&self) {
        let settlement = Arc::clone(&self.settlement.service);
        let runtime = Arc::clone(&self.runtime_config);

        self.pending_tasks
            .push(TaskId::MarketSettlementRetry, move |shutdown| async move {
                if let Err(error) = PeriodicTask::run(
                    TaskId::MarketSettlementRetry.static_name(),
                    move || {
                        Duration::from_secs(
                            runtime
                                .load()
                                .settlement
                                .lifecycle
                                .retry_interval_secs
                                .max(10),
                        )
                    },
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

    fn queue_book_snapshot_publisher(&self) {
        let book_store = Arc::clone(&self.data.book_store);
        let market_registry = Arc::clone(&self.data.market_registry);
        let writer = Arc::clone(&self.infra.book_fact_writer);

        self.pending_tasks
            .push(TaskId::BookSnapshotPublisher, move |shutdown| async move {
                publish_book_snapshots(
                    &book_store,
                    &market_registry,
                    &writer,
                    ChSnapshotReason::Startup,
                );
                if let Err(error) = PeriodicTask::run(
                    TaskId::BookSnapshotPublisher.static_name(),
                    || Duration::from_secs(BOOK_SNAPSHOT_PUBLISH_INTERVAL_SECS),
                    PERIODIC_JITTER_PCT,
                    false,
                    shutdown,
                    || {
                        let book_store = Arc::clone(&book_store);
                        let market_registry = Arc::clone(&market_registry);
                        let writer = Arc::clone(&writer);
                        async move {
                            publish_book_snapshots(
                                &book_store,
                                &market_registry,
                                &writer,
                                ChSnapshotReason::Periodic,
                            );
                            Ok(())
                        }
                    },
                )
                .await
                {
                    tracing::error!(%error, "book snapshot publisher task exited");
                }
            });
    }

    fn queue_health_checker(&self) {
        let checker = Arc::clone(&self.health_checker);
        let metrics = Arc::clone(&self.infra.metrics);
        let alerts = Arc::clone(&self.infra.alerts);
        let nudge = self.system_status_nudge.clone();
        let ws = Arc::clone(&self.trading.ws_manager);

        self.pending_tasks
            .push(TaskId::HealthChecker, move |shutdown| async move {
                if let Err(error) = PeriodicTask::run(
                    TaskId::HealthChecker.static_name(),
                    || Duration::from_secs(HEALTH_CHECK_INTERVAL_SECS),
                    0.0,
                    true,
                    shutdown,
                    || {
                        let checker = Arc::clone(&checker);
                        let metrics = Arc::clone(&metrics);
                        let alerts = Arc::clone(&alerts);
                        let nudge = nudge.clone();
                        let ws = Arc::clone(&ws);
                        async move {
                            let shards = ws.shard_health();
                            if shards.disconnected > 0 {
                                tracing::warn!(%shards, "WS shard connectivity degraded");
                            }
                            let report = checker.check_all_and_notify(&alerts, &nudge).await;
                            if !report.overall_healthy {
                                let unhealthy = report
                                    .checks
                                    .iter()
                                    .filter(|check| {
                                        check.counts_toward_overall() && !check.is_healthy()
                                    })
                                    .map(|check| check.name.as_str())
                                    .collect::<Vec<_>>();
                                tracing::warn!(
                                    checks = ?unhealthy,
                                    "health check detected unhealthy subsystems"
                                );
                                metrics.health_check_failures.inc();
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
        // `Arc<CoreRiskMetrics>` unsizes to `Arc<dyn RiskMetrics>` at the
        // argument; the binding keeps `Arc::clone` inference on the concrete type.
        let risk_metrics = Arc::clone(&self.risk.metrics);
        let generator = Arc::new(ReportGenerator::new(
            Arc::clone(&self.infra.trade_repo),
            Arc::clone(&self.infra.position_repo),
            Arc::clone(&self.infra.report_repo),
            Arc::clone(&self.risk.engine),
            risk_metrics,
            Arc::clone(&self.infra.alerts),
        ));

        self.pending_tasks
            .push(TaskId::ReportGenerator, move |shutdown| async move {
                if let Err(error) = PeriodicTask::run(
                    TaskId::ReportGenerator.static_name(),
                    || Duration::from_secs(3600),
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

/// Shared handles for catalog warmup and the periodic Gamma cadence.
#[derive(Clone)]
struct CatalogSyncDeps {
    gamma_service: Arc<GammaService>,
    catalog: Arc<CatalogReadiness>,
    market_registry: Arc<MarketRegistry>,
    catalog_ready_gauge: IntGauge,
    status_nudge: SystemStatusNudge,
}

impl CatalogSyncDeps {
    /// Refresh the readiness snapshot (markets + timestamp) after a successful sync.
    fn mark_ready(&self) {
        let markets = u64::try_from(self.market_registry.market_count()).unwrap_or(u64::MAX);
        self.catalog.mark_ready(markets, chrono::Utc::now());
        self.catalog_ready_gauge.set(1);
        self.status_nudge.nudge();
    }
}

/// First-sync retry loop (unlimited attempts, exponential backoff + jitter,
/// 60 s ceiling). Returns `false` when shutdown was requested before success.
async fn warmup_catalog(deps: &CatalogSyncDeps, shutdown: &CancellationToken) -> bool {
    let mut controller = RetryController::new(&RetryPolicy {
        max_attempts: None,
        initial_interval_ms: 1_000,
        max_interval_ms: 60_000,
        randomization_factor: 0.25,
        multiplier: 2.0,
        max_elapsed_time_ms: None,
    });

    loop {
        if shutdown.is_cancelled() {
            return false;
        }
        match deps.gamma_service.sync().await {
            Ok(()) => {
                deps.mark_ready();
                tracing::info!(
                    markets = deps.market_registry.market_count(),
                    attempts = controller.retries_used(),
                    "catalog warmup complete — detection unlocked"
                );
                return true;
            }
            Err(error) => match controller.on_failure() {
                RetryDecision::RetryAfter(delay) => {
                    tracing::warn!(
                        %error,
                        attempt = controller.retries_used(),
                        backoff_ms = delay.as_millis(),
                        "catalog warmup sync failed — retrying"
                    );
                    tokio::select! {
                        () = tokio::time::sleep(delay) => {}
                        () = shutdown.cancelled() => return false,
                    }
                }
                RetryDecision::Exhausted => {
                    // Unreachable with max_attempts = None; fail loudly if the
                    // policy is ever tightened without revisiting this loop.
                    tracing::error!(%error, "catalog warmup retry budget exhausted");
                    return false;
                }
            },
        }
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

/// Whether the external-ledger reconciliation tick should run for `mode`.
///
/// External-balance reconciliation compares the internal ledger against real
/// venue funds; in DryRun/Paper the ledger tracks simulated money, so any
/// comparison with the CLOB balance is meaningless and must never trip the
/// breaker.
#[must_use]
pub const fn ledger_reconciliation_enabled(mode: ExecutionMode) -> bool {
    matches!(mode, ExecutionMode::Live)
}

async fn run_ledger_reconciliation(deps: LedgerReconciliationDeps<'_>) -> OxideResult<()> {
    // The gate re-evaluates per tick so a governed switch to Live re-arms
    // reconciliation without a restart.
    let mode = deps.execution_mode.current();
    if !ledger_reconciliation_enabled(mode) {
        tracing::debug!(%mode, "ledger reconciliation skipped — external balance reconciliation is Live-only");
        return Ok(());
    }

    let observed_at = chrono::Utc::now();
    let external_available = deps
        .clob_client
        .collateral_balance()
        .await
        .map_err(OxideError::from)?;
    // In Live, CLOB collateral is the only authoritative cash truth. Runtime
    // `bankroll_usd` remains a sizing cap and simulated-mode baseline; it must
    // never be reused as a Live cash ledger.
    let internal_cash = deps.risk_metrics.cash_balance();
    let reserved = deps.risk_metrics.reserved_usd();
    let external_positions =
        fetch_external_position_values(deps.risk_metrics, deps.ctf_redeem, deps.holder_address)
            .await?;

    let report = deps.risk_engine.reconciler().reconcile_fetched(
        deps.risk_metrics,
        internal_cash,
        external_available,
        Usd::ZERO,
        Some(&external_positions),
    );
    deps.balance_fact_writer
        .write_observation(BalanceFactObservation {
            holder_address: deps.holder_address.to_owned(),
            internal_available_usd: internal_cash,
            internal_reserved_usd: reserved,
            external_available_usd: external_available,
            external_locked_usd: Usd::ZERO,
            block_number: None,
            reconciliation_report_id: None,
            observed_at,
        })
        .await?;
    deps.risk_engine
        .on_reconciliation_result(&report, deps.risk_metrics)
        .await
}

async fn fetch_external_position_values(
    risk_metrics: &dyn RiskMetrics,
    ctf_redeem: &CtfRedeemClient,
    holder_address: &str,
) -> OxideResult<Vec<(MarketId, Usd)>> {
    let mut by_market = HashMap::<MarketId, Usd>::new();
    for position in risk_metrics.open_positions() {
        let chain_shares = ctf_redeem
            .position_balance(holder_address, &position.token_id)
            .await
            .map_err(OxideError::from)?;
        if position.shares <= Shares::ZERO {
            continue;
        }
        let ratio = (chain_shares.inner() / position.shares.inner()).min(Decimal::ONE);
        let external_value = position.total_cost_usd * ratio;
        *by_market
            .entry(position.market_id.clone())
            .or_insert(Usd::ZERO) += external_value;
    }
    Ok(by_market.into_iter().collect())
}

fn publish_book_snapshots(
    book_store: &BookStore,
    market_registry: &MarketRegistry,
    writer: &BookFactWriter,
    reason: ChSnapshotReason,
) {
    for (token_id, snapshot) in book_store.published_snapshots() {
        if snapshot.timestamp_ms == 0 {
            continue;
        }
        writer.write_published_snapshot(
            &token_id,
            market_registry.market_for_token(&token_id),
            reason,
            &snapshot,
        );
    }
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
///
/// Simulated modes hydrate their derived ledger snapshot during wiring; only
/// Live defers to this gate because the CLOB fetch must succeed (fail-closed)
/// before any trading loop spins up.
pub async fn ensure_live_metrics_ready(
    mode: ExecutionMode,
    refresher: &RiskMetricsRefreshService,
) -> OxideResult<()> {
    if mode != ExecutionMode::Live {
        return Ok(());
    }
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
