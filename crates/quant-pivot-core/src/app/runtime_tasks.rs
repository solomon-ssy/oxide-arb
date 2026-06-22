//! Core trading-loop task registration for [`super::AppContext`].

use super::{AppContext, POST_TRADE_RELAY_BATCH_SIZE, task_id::TaskId};
use crate::{
    detection::scanner_task::{ScannerTask, ScannerTaskDeps},
    execution::{
        heartbeat::{HeartbeatTask, HeartbeatTaskConfig},
        runner::ExecutionRunner,
        settlement::task::MarketSettlementTask,
    },
    infra::risk_decision_audit_drain::spawn_risk_decision_audit_drain,
    post_trade::{
        consumer::PostTradeConsumer,
        reconciliation::{ReconciliationWorker, ReconciliationWorkerDeps},
        relay::{PostTradeRelay, PostTradeRelayDeps},
    },
};
use oxide_arb_error::OxideResult;
use oxide_arb_models::enums::common::ExecutionMode;
use oxide_arb_repository::{
    postgres::PgRiskAuditRepository,
    traits::{PositionRepository, TradeRepository},
};
use std::sync::Arc;

impl AppContext {
    /// Live-mode gate: metrics must be fresh and venue subsystems must be wired.
    pub async fn ensure_live_ready(&self) -> OxideResult<()> {
        let mode = self.execution_mode.current();
        if mode != ExecutionMode::Live {
            return Ok(());
        }
        self.risk.metrics_refresh.refresh().await.map_err(|error| {
            tracing::error!(
                %error,
                "Live startup metrics refresh failed — refusing to start"
            );
            error
        })?;
        self.ensure_live_subsystems(mode)
    }

    /// Fatal when Live is requested but venue/reconciliation subsystems are missing.
    pub fn ensure_live_subsystems(&self, mode: ExecutionMode) -> OxideResult<()> {
        use oxide_arb_error::OxideError;

        if mode != ExecutionMode::Live {
            return Ok(());
        }
        if self.trading.clob_client.is_none() {
            return Err(OxideError::Internal(
                "Live mode requires ClobClient — check keystore configuration".into(),
            ));
        }
        if self.trading.ctf_redeem.is_none() {
            return Err(OxideError::Internal(
                "Live mode requires CtfRedeemClient — check keystore/RPC configuration".into(),
            ));
        }
        if self.infra.holder_address == "unavailable" {
            return Err(OxideError::Internal(
                "Live mode requires a configured holder address".into(),
            ));
        }
        if self.trading.execution.is_none() {
            return Err(OxideError::Internal(
                "Live mode requires the execution bundle to be wired".into(),
            ));
        }
        Ok(())
    }

    /// Clone the non-blocking event publisher for a producer (e.g. the web
    /// governance handlers or a periodic service).
    #[must_use]
    pub fn event_publisher(&self) -> oxide_arb_models::domain::CoreEventPublisher {
        self.events.clone()
    }

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
            capital_manager: Arc::clone(&exec.capital_manager),
            trade_repo: Arc::clone(&trade_repo),
            position_repo,
            calibration_repo,
            audit_writer: Arc::clone(&self.infra.audit_writer),
            metrics_state: Arc::clone(&self.risk.metrics_state),
            metrics_refresh: Arc::clone(&self.risk.metrics_refresh),
            metrics: Arc::clone(&self.infra.metrics),
            events: self.event_publisher(),
            market_registry: Arc::clone(&self.data.market_registry),
            runtime_config: Arc::clone(&self.runtime_config),
        };
        let relay = PostTradeRelay::new(PostTradeRelayDeps {
            consumer,
            trade_repo,
            notify: Arc::clone(&exec.relay_notify),
            capital_manager: Arc::clone(&exec.capital_manager),
            batch_size: POST_TRADE_RELAY_BATCH_SIZE,
            runtime: Arc::clone(&self.runtime_config),
            metrics: Arc::clone(&self.infra.metrics),
        });

        self.pending_tasks
            .push(TaskId::PostTradeRelay, move |shutdown| async move {
                if let Err(error) = relay.run(shutdown).await {
                    tracing::error!(%error, "post-trade relay exited with error");
                }
            });
    }

    /// Queue the unknown-outcome reconciliation worker.
    pub fn queue_reconciliation_worker(&self) {
        let Some(exec) = &self.trading.execution else {
            tracing::error!("reconciliation worker unavailable — execution bundle not configured");
            return;
        };
        let Some(clob_client) = self.trading.clob_client.as_ref() else {
            tracing::error!("reconciliation worker unavailable — ClobClient missing");
            return;
        };
        let Some(ctf_redeem) = self.trading.ctf_redeem.as_ref() else {
            tracing::error!("reconciliation worker unavailable — CTF redeem client missing");
            return;
        };

        let trade_repo = Arc::clone(&self.infra.trade_repo);
        let trade_repo: Arc<dyn TradeRepository> = trade_repo;
        let worker = ReconciliationWorker::new(ReconciliationWorkerDeps {
            trade_repo,
            clob_client: Arc::clone(clob_client),
            ctf_redeem: Arc::clone(ctf_redeem),
            fee_calculator: Arc::clone(&self.infra.fee_calculator),
            holder_address: self.infra.holder_address.clone(),
            capital_manager: Arc::clone(&exec.capital_manager),
            fsm: Arc::clone(&self.trading.fsm),
            metrics_state: Arc::clone(&self.risk.metrics_state),
            runtime_config: Arc::clone(&self.runtime_config),
            reconcile_notify: Arc::clone(&exec.reconcile_notify),
            relay_notify: Arc::clone(&exec.relay_notify),
        });

        self.pending_tasks
            .push(TaskId::ReconciliationWorker, move |shutdown| async move {
                worker.run(shutdown).await;
            });
    }

    /// Queue venue heartbeat probe (self-gates on the live mode each tick).
    pub fn queue_execution_heartbeat(&self, interval_secs: u64) {
        let Some(clob_client) = self.trading.clob_client.as_ref() else {
            tracing::info!("execution heartbeat skipped — ClobClient unavailable");
            return;
        };
        let clob_client = Arc::clone(clob_client);
        let risk_engine = Arc::clone(&self.risk.engine);
        let fsm = Arc::clone(&self.trading.fsm);
        let integrity = Arc::clone(&self.trade_integrity);
        let factor_store = Arc::clone(&self.control.store);
        let alerts = Arc::clone(&self.infra.alerts);
        let metrics = Arc::clone(&self.infra.metrics);
        let mode = self.execution_mode.clone();

        self.pending_tasks
            .push(TaskId::ExecutionHeartbeat, move |shutdown| async move {
                let task = HeartbeatTask::new(HeartbeatTaskConfig {
                    clob_client,
                    risk_engine,
                    fsm,
                    integrity,
                    factor_store,
                    alerts,
                    metrics,
                    interval_secs,
                    shutdown,
                    mode,
                });
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
            runtime: Arc::clone(&self.runtime_config),
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
            self.queue_reconciliation_worker();
            self.queue_execution_heartbeat(30);
        }
    }
}
