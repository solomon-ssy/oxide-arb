//! Application context — system composition root and lifecycle manager.
//!
//! `AppContext` owns all subsystem instances and orchestrates startup,
//! run, and graceful shutdown. The struct is decomposed into four bundles
//! to avoid a 40+ field god struct.

pub mod bootstrap;
pub mod build;
pub mod lifecycle;
pub mod periodic_services;
pub mod task_id;
pub mod task_registry;

use crate::{
    app::{task_id::TaskId, task_registry::PendingTaskQueue},
    bridge::{
        CoreOpportunityPipeline, potential_loss_store::CorePotentialLossStore,
        risk_metrics::CoreRiskMetrics, trading_gate::TradingGate,
    },
    control::{
        ControlFactorRegistry, factor_refresher::FactorRefresher, factor_shadow::ShadowWriterTask,
        factor_snapshot::FactorSnapshotStore,
    },
    detection::{
        coalescer::Coalescer,
        funnel::Funnel,
        scanner::Scanner,
        scanner_task::{ScannerTask, ScannerTaskDeps},
    },
    execution::{
        capital_manager::CapitalManager,
        execution_pipeline::ExecutionPipeline,
        fsm::ExecutionFSM,
        heartbeat::HeartbeatTask,
        market_inflight::MarketInFlightRegistry,
        runner::ExecutionRunner,
        settlement::{
            dedup::SettlementDedup, service::MarketSettlementService, task::MarketSettlementTask,
        },
    },
    exposure::in_memory::InMemoryExposureReservation,
    infra::{
        risk_decision_audit_buffer::RiskDecisionAuditBuffer,
        risk_decision_audit_drain::spawn_risk_decision_audit_drain,
    },
    observability::{
        alert_dispatcher::AlertDispatcher, balance_fact_writer::BalanceFactWriter,
        book_fact_writer::BookFactWriter, execution_audit::ExecutionAuditWriter,
        metrics_hub::MetricsHub,
    },
    pipeline::{
        book_store::BookStore, data_pipeline::DataPipeline, market_cache::MarketCache,
        market_registry::MarketRegistry,
    },
    post_trade::{
        consumer::PostTradeConsumer,
        relay::{PostTradeRelay, PostTradeRelayDeps},
    },
    service::{
        gamma::GammaService,
        risk_metrics::{RiskMetricsRefreshService, RiskMetricsState},
    },
};
use flume::Receiver;
use oxide_arb_algorithm::calibration::{CalibrationUpdater, ResolutionCalibrator};
use oxide_arb_api::{clob::ClobClient, ws::ClobWsManager};
use oxide_arb_models::{
    config::Settings,
    domain::settlement::MarketSettlementRequest,
    enums::common::ExecutionMode,
    types::{MarketId, MicroScore, TokenId},
};
use oxide_arb_repository::{
    clickhouse::ChTimeseriesRepository,
    postgres::{
        PgCalibrationRepository, PgFactDataRepository, PgPositionRepository, PgReportRepository,
        PgRiskAuditRepository, PgRiskStateRepository, PgTradeRepository,
    },
    traits::{PositionRepository, TradeRepository},
};
use oxide_arb_risk::{audit::RiskAuditEvent, engine::RiskEngine};
use oxide_arb_storage::{cache::TieredCache, clickhouse::ClickHousePool, postgres::PostgresPool};
use parking_lot::Mutex;
use std::{sync::Arc, time::Duration};
use tokio::sync::Notify;
use tokio_util::sync::CancellationToken;

/// Max trades claimed per post-trade relay drain iteration.
const POST_TRADE_RELAY_BATCH_SIZE: u64 = 128;
/// Infrastructure subsystem: storage, metrics, alerts.
pub struct InfraBundle {
    pub pg: Arc<PostgresPool>,
    pub ch: Arc<ClickHousePool>,
    pub cache: Arc<TieredCache>,
    pub metrics: Arc<MetricsHub>,
    pub alerts: Arc<AlertDispatcher>,
    pub risk_decision_audit: Arc<RiskDecisionAuditBuffer>,
    pub risk_decision_audit_rx: Mutex<Option<Receiver<RiskAuditEvent>>>,
    pub trade_repo: Arc<PgTradeRepository>,
    pub position_repo: Arc<PgPositionRepository>,
    pub report_repo: Arc<PgReportRepository>,
    pub fact_data_repo: Arc<PgFactDataRepository>,
    pub calibration_repo: Arc<PgCalibrationRepository>,
    pub risk_state_repo: Arc<PgRiskStateRepository>,
    pub timeseries: Arc<ChTimeseriesRepository>,
    pub audit_writer: Arc<ExecutionAuditWriter>,
    pub balance_fact_writer: Arc<BalanceFactWriter>,
    pub book_fact_writer: Arc<BookFactWriter>,
    pub holder_address: String,
}

/// Data pipeline subsystem: WS event loop, order books, market metadata.
pub struct DataBundle {
    pub book_store: Arc<BookStore>,
    pub market_registry: Arc<MarketRegistry>,
    pub market_cache: Arc<MarketCache>,
    pub data_pipeline: Arc<DataPipeline>,
    pub gamma_service: Arc<GammaService>,
}

/// Risk management subsystem.
pub struct RiskBundle {
    pub engine: Arc<RiskEngine>,
    pub metrics: Arc<CoreRiskMetrics>,
    pub metrics_state: Arc<RiskMetricsState>,
    pub exposure: Arc<InMemoryExposureReservation>,
    pub potential_loss_store: Arc<CorePotentialLossStore>,
    pub metrics_refresh: Option<Arc<RiskMetricsRefreshService>>,
}

/// Execution subsystem wired after opportunity detection.
pub struct ExecutionBundle {
    pub pipeline: Arc<ExecutionPipeline>,
    pub market_inflight: Arc<MarketInFlightRegistry>,
    pub trading_gate: TradingGate,
    pub capital_manager: Arc<CapitalManager>,
    /// Shared with the pipeline; rung after each `*_observed` write to wake the relay.
    pub relay_notify: Arc<Notify>,
}

impl ExecutionBundle {
    pub fn new(
        pipeline: Arc<ExecutionPipeline>,
        market_inflight: Arc<MarketInFlightRegistry>,
        risk_engine: Arc<RiskEngine>,
        fsm: Arc<ExecutionFSM>,
        capital_manager: Arc<CapitalManager>,
        relay_notify: Arc<Notify>,
    ) -> Self {
        Self {
            pipeline,
            market_inflight,
            trading_gate: TradingGate::new(risk_engine, fsm),
            capital_manager,
            relay_notify,
        }
    }
}

/// Trading subsystem: detection, execution, algorithm.
pub struct TradingBundle {
    pub opportunity_pipeline: Arc<CoreOpportunityPipeline>,
    pub calibrator: Arc<ResolutionCalibrator>,
    pub calibration_updater: Arc<CalibrationUpdater>,
    pub scanner: Arc<Scanner>,
    pub coalescer: Arc<Coalescer>,
    pub funnel: Arc<Funnel>,
    pub fsm: Arc<ExecutionFSM>,
    pub execution: Option<ExecutionBundle>,
    pub clob_client: Option<Arc<ClobClient>>,
    pub ws_manager: Arc<ClobWsManager>,
}

/// Live control-factor subsystem (Phase 5.6): snapshot store, refresher, and
/// the shadow-decision writer drain task.
pub struct ControlFactorBundle {
    pub store: Arc<FactorSnapshotStore>,
    pub refresher: Arc<FactorRefresher>,
    /// Governance registry wired to the refresher notify handle (publish/rollback
    /// wake the snapshot reload without waiting for the periodic poll).
    pub registry: Arc<ControlFactorRegistry>,
    pub shadow_writer_task: Mutex<Option<ShadowWriterTask>>,
}

/// Market settlement subsystem.
pub struct SettlementBundle {
    pub service: Arc<MarketSettlementService>,
    pub dedup: Arc<SettlementDedup>,
    settlement_rx: Mutex<Option<Receiver<MarketSettlementRequest>>>,
}

/// One-shot channel receivers consumed when registering runtime tasks.
pub struct RuntimeChannels {
    pub coalescer_token_rx: Mutex<Option<flume::Receiver<TokenId>>>,
    pub scanner_market_rx: Mutex<Option<flume::Receiver<MarketId>>>,
    pub execution_runners: Mutex<Option<Vec<ExecutionRunner>>>,
}

/// System composition root — owns all subsystems.
///
/// Decomposed into four bundles (`InfraBundle`, `DataBundle`,
/// `RiskBundle`, `TradingBundle`) to avoid a 40+ flat-field struct.
pub struct AppContext {
    pub config: Arc<Settings>,
    pub infra: InfraBundle,
    pub data: DataBundle,
    pub risk: RiskBundle,
    pub trading: TradingBundle,
    pub control: ControlFactorBundle,
    pub settlement: SettlementBundle,
    pub runtime: RuntimeChannels,
    pub shutdown: CancellationToken,
    pub pending_tasks: PendingTaskQueue,
}

impl AppContext {
    /// Queue the background task that drains pre-trade audit events to Postgres.
    ///
    /// Consumes `infra.risk_decision_audit_rx` — call at most once before `AppRunner::run`.
    pub fn queue_risk_decision_audit_drain(&self, repo: Arc<PgRiskAuditRepository>) {
        let Some(rx) = self.infra.risk_decision_audit_rx.lock().take() else {
            tracing::warn!("risk decision audit drain already registered or rx unavailable");
            return;
        };

        self.pending_tasks
            .push(TaskId::RiskAuditBatch, move |shutdown| async move {
                if let Err(error) = spawn_risk_decision_audit_drain(rx, repo, shutdown).await {
                    tracing::error!(%error, "risk decision audit drain exited with error");
                }
            });
    }

    /// Queue market settlement worker consuming resolved-market events.
    pub fn queue_market_settlement_task(&self) {
        let Some(rx) = self.settlement.settlement_rx.lock().take() else {
            tracing::warn!("market settlement task already registered or rx unavailable");
            return;
        };
        let service = Arc::clone(&self.settlement.service);
        let dedup = Arc::clone(&self.settlement.dedup);
        let metrics = Arc::clone(&self.infra.metrics);

        self.pending_tasks
            .push(TaskId::MarketSettlement, move |shutdown| async move {
                let task = MarketSettlementTask::new(rx, service, dedup, metrics, shutdown);
                if let Err(error) = task.run().await {
                    tracing::error!(%error, "market settlement task exited with error");
                }
            });
    }

    /// Queue the durable post-trade relay (notify-woken + periodic crash-recovery poll).
    pub fn queue_post_trade_relay(&self) {
        let Some(exec) = &self.trading.execution else {
            tracing::warn!("post-trade relay skipped — execution bundle not configured");
            return;
        };

        let trade_repo = Arc::clone(&self.infra.trade_repo);
        let trade_repo: Arc<dyn TradeRepository> = trade_repo;
        let position_repo = Arc::clone(&self.infra.position_repo);
        let position_repo: Arc<dyn PositionRepository> = position_repo;
        let calibration_repo = Arc::clone(&self.infra.calibration_repo);
        let consumer = PostTradeConsumer {
            risk_engine: Arc::clone(&self.risk.engine),
            risk_metrics: Arc::clone(&self.risk.metrics),
            fsm: Arc::clone(&self.trading.fsm),
            trade_repo: Arc::clone(&trade_repo),
            position_repo,
            calibration_repo,
            audit_writer: Arc::clone(&self.infra.audit_writer),
            metrics_state: Arc::clone(&self.risk.metrics_state),
            metrics_refresh: self.risk.metrics_refresh.clone(),
            metrics: Arc::clone(&self.infra.metrics),
            execution_mode: self.config.execution.execution_mode,
        };
        let relay = PostTradeRelay::new(PostTradeRelayDeps {
            consumer,
            trade_repo,
            notify: Arc::clone(&exec.relay_notify),
            capital_manager: Arc::clone(&exec.capital_manager),
            batch_size: POST_TRADE_RELAY_BATCH_SIZE,
            poll_interval: Duration::from_secs(
                self.config
                    .execution
                    .timeout
                    .trade_confirm_poll_interval_secs
                    .max(1),
            ),
            stale_submitted_after: Duration::from_secs(
                self.config.execution.timeout.trade_confirm_timeout_secs,
            ),
            metrics: Arc::clone(&self.infra.metrics),
        });

        self.pending_tasks
            .push(TaskId::PostTradeRelay, move |shutdown| async move {
                if let Err(error) = relay.run(shutdown).await {
                    tracing::error!(%error, "post-trade relay exited with error");
                }
            });
    }

    /// Queue venue heartbeat probe (Live mode only).
    pub fn queue_execution_heartbeat(&self, interval_secs: u64, execution_mode: ExecutionMode) {
        let Some(clob_client) = self.trading.clob_client.as_ref() else {
            tracing::info!("execution heartbeat skipped — ClobClient unavailable");
            return;
        };
        let clob_client = Arc::clone(clob_client);
        let risk_engine = Arc::clone(&self.risk.engine);
        let fsm = Arc::clone(&self.trading.fsm);

        self.pending_tasks
            .push(TaskId::ExecutionHeartbeat, move |shutdown| async move {
                let task = HeartbeatTask::new(
                    clob_client,
                    risk_engine,
                    fsm,
                    interval_secs,
                    shutdown,
                    execution_mode,
                );
                if let Err(error) = task.run().await {
                    tracing::error!(%error, "execution heartbeat exited with error");
                }
            });
    }

    /// Queue one registered task per execution shard.
    pub fn queue_execution_runners(&self, runners: Vec<ExecutionRunner>) {
        if runners.is_empty() {
            tracing::warn!("no execution runners to queue");
            return;
        }

        for (index, runner) in runners.into_iter().enumerate() {
            let shard = u8::try_from(index).unwrap_or(u8::MAX);
            self.pending_tasks.push(
                TaskId::ExecutionRunner { shard },
                move |_token| async move {
                    if let Err(error) = runner.run().await {
                        tracing::error!(shard, %error, "execution runner exited with error");
                    }
                },
            );
        }
    }

    /// Register the core trading loop tasks (WS → books → detect → execute).
    pub fn queue_runtime_tasks(&self) {
        let mode = self.config.execution.execution_mode;

        let data_pipeline = Arc::clone(&self.data.data_pipeline);
        self.pending_tasks
            .push(TaskId::DataPipeline, move |_token| async move {
                if let Err(error) = data_pipeline.run().await {
                    tracing::error!(%error, "data pipeline exited with error");
                }
            });

        let coalescer = Arc::clone(&self.trading.coalescer);
        let token_rx = self.runtime.coalescer_token_rx.lock().take();
        self.pending_tasks
            .push(TaskId::Coalescer, move |_token| async move {
                if let Err(error) = coalescer.run_with_ingress(token_rx).await {
                    tracing::error!(%error, "coalescer exited with error");
                }
            });

        let Some(market_rx) = self.runtime.scanner_market_rx.lock().take() else {
            tracing::warn!("scanner task skipped — market channel unavailable");
            return;
        };
        let scanner_task = ScannerTask::new(ScannerTaskDeps {
            rx: market_rx,
            scanner: Arc::clone(&self.trading.scanner),
            market_cache: Arc::clone(&self.data.market_cache),
            funnel: Arc::clone(&self.trading.funnel),
            dispatch_immediate_threshold: MicroScore::try_from_decimal(
                self.config
                    .execution
                    .endgame_latency
                    .dispatch_immediate_threshold,
            )
            .unwrap_or(MicroScore::ZERO),
            shutdown: self.shutdown.clone(),
        });
        self.pending_tasks
            .push(TaskId::Scanner, move |_token| async move {
                if let Err(error) = scanner_task.run().await {
                    tracing::error!(%error, "scanner task exited with error");
                }
            });

        let funnel = Arc::clone(&self.trading.funnel);
        let shutdown = self.shutdown.clone();
        self.pending_tasks
            .push(TaskId::Funnel, move |_token| async move {
                if let Err(error) = funnel.run(shutdown).await {
                    tracing::error!(%error, "funnel exited with error");
                }
            });

        // Live control-factor refresher (periodic + notify) and shadow writer.
        let refresher = Arc::clone(&self.control.refresher);
        self.pending_tasks
            .push(TaskId::FactorRefresher, move |token| async move {
                refresher.run(token).await;
            });
        let shadow_task = self.control.shadow_writer_task.lock().take();
        if let Some(shadow_task) = shadow_task {
            self.pending_tasks
                .push(TaskId::ShadowDecisionWriter, move |token| async move {
                    shadow_task.run(token).await;
                });
        }

        if self.trading.execution.is_some() {
            let runners = self.runtime.execution_runners.lock().take();
            if let Some(runners) = runners {
                self.queue_execution_runners(runners);
            }
            self.queue_post_trade_relay();
            self.queue_execution_heartbeat(30, mode);
        }
    }
}
