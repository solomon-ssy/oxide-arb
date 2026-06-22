//! Execution loop wiring (pipeline, funnel, data pipeline).

use super::super::{ExecutionBundle, ExecutionBundleDeps};
use super::types::{
    BuildRisk, BuildTrading, DetectionStack, ExecutionLoopParts, TradingBuildInput,
};
use crate::{
    detection::funnel::Funnel,
    execution::{
        capital_manager::CapitalManager,
        dispatcher::Dispatcher,
        execution_pipeline::{ExecutionPipeline, ExecutionPipelineDeps},
        fok_strategy::FokOrderStrategy,
        market_inflight::MarketInFlightRegistry,
        plan_builder::PlanBuilder,
        port::ExecutionPort,
        runner::ExecutionRunnerPool,
        validator::Validator,
    },
    pipeline::data_pipeline::{DataPipeline, DataPipelineDeps},
    pipeline::event_source::PipelineEventSource,
    trade_integrity::TradeIntegrityStore,
};
use oxide_arb_models::domain::settlement::MarketSettlementRequest;
use oxide_arb_repository::traits::TradeRepository;
use std::sync::{Arc, atomic::AtomicU32};
use std::time::Duration;
use tokio::sync::Notify;
use tokio_util::sync::CancellationToken;

use super::types::ExecutionLoop;

struct PipelineOwnedParts {
    capital: Arc<CapitalManager>,
    market_inflight: Arc<MarketInFlightRegistry>,
    validator: Arc<Validator>,
    order_strategy: Arc<FokOrderStrategy>,
    relay_notify: Arc<Notify>,
    reconcile_notify: Arc<Notify>,
    trade_integrity: Arc<TradeIntegrityStore>,
}

struct PipelineContext<'a> {
    execution_mode: crate::bridge::execution_mode::ExecutionModeHandle,
    input: &'a TradingBuildInput<'a>,
    risk: &'a BuildRisk,
    detection: &'a DetectionStack,
    owned: PipelineOwnedParts,
}

impl BuildTrading {
    pub(super) fn wire(
        input: &TradingBuildInput<'_>,
        risk: &BuildRisk,
        detection: DetectionStack,
        shutdown: CancellationToken,
    ) -> Self {
        let execution = ExecutionLoop::wire(input, risk, &detection, shutdown);
        Self::assembled(detection, execution)
    }
}

impl ExecutionLoop {
    fn wire(
        input: &TradingBuildInput<'_>,
        risk: &BuildRisk,
        detection: &DetectionStack,
        shutdown: CancellationToken,
    ) -> Self {
        let deploy = input.wiring().deploy();
        let runtime = input.wiring().runtime();
        let infra = input.infra();
        let clients = input.clients();
        let mode_handle = input.execution_mode().clone();
        let relay_notify = Arc::new(Notify::new());
        let reconcile_notify = Arc::new(Notify::new());
        let (settlement_tx, settlement_rx) =
            flume::bounded(deploy.settlement.lifecycle.channel_capacity);
        let capital = Arc::new(CapitalManager::new(
            Arc::clone(risk.exposure()),
            &runtime.risk.exposure_reservation_config(),
        ));
        let market_inflight = Arc::new(MarketInFlightRegistry::new());
        let validator = Arc::new(Validator::new(
            Arc::clone(detection.book_store()),
            detection.staleness().clone(),
            &runtime.execution,
            Arc::clone(infra.metrics()),
        ));
        let order_strategy = Arc::new(FokOrderStrategy::new(
            mode_handle.clone(),
            clients.clob_client().map(Arc::clone),
            Arc::clone(clients.fee_calculator()),
            runtime.execution.timeout.dispatcher_timeout_ms,
            Arc::clone(infra.metrics()),
        ));
        let trade_integrity = Arc::new(TradeIntegrityStore::new(
            {
                let repo = Arc::clone(&infra.persistence().trade_repo);
                repo as Arc<dyn TradeRepository>
            },
            Arc::clone(risk.exposure()),
            Arc::clone(risk.fsm()),
            Arc::clone(infra.runtime_store()),
            Arc::clone(infra.alerts()),
        ));
        let execution_pipeline = Self::build_pipeline(PipelineContext {
            execution_mode: mode_handle,
            input,
            risk,
            detection,
            owned: PipelineOwnedParts {
                capital: Arc::clone(&capital),
                market_inflight: Arc::clone(&market_inflight),
                validator: Arc::clone(&validator),
                order_strategy: Arc::clone(&order_strategy),
                relay_notify: Arc::clone(&relay_notify),
                reconcile_notify: Arc::clone(&reconcile_notify),
                trade_integrity: Arc::clone(&trade_integrity),
            },
        });

        let inflight = Arc::new(AtomicU32::new(0));
        let pipeline_port = Arc::clone(&execution_pipeline);
        let pipeline_port: Arc<dyn ExecutionPort> = pipeline_port;
        let (runner_pool, execution_runners) = ExecutionRunnerPool::new(
            deploy.execution.book_apply.shard_count,
            &pipeline_port,
            &shutdown,
            &inflight,
            infra.metrics(),
        );
        let funnel = Arc::new(Funnel::with_backpressure(
            runner_pool.shard_senders().to_vec(),
            runtime.execution.funnel.max_queue_size,
            Duration::from_millis(runtime.execution.funnel.min_dispatch_interval_ms),
            Arc::clone(infra.metrics()),
            Some(Arc::clone(risk.backpressure())),
        ));

        let data_pipeline =
            Self::build_data_pipeline(input, detection, risk, settlement_tx, shutdown);

        let execution = ExecutionBundle::new(ExecutionBundleDeps {
            pipeline: execution_pipeline,
            market_inflight,
            capital_manager: Arc::clone(&capital),
            relay_notify,
            reconcile_notify,
        });

        Self::assembled(ExecutionLoopParts {
            funnel,
            validator,
            order_strategy,
            data_pipeline,
            execution,
            settlement_rx,
            execution_runners,
            trade_integrity,
        })
    }

    fn build_pipeline(ctx: PipelineContext<'_>) -> Arc<ExecutionPipeline> {
        let PipelineContext {
            execution_mode,
            input,
            risk,
            detection,
            owned,
        } = ctx;
        let infra = input.infra();
        let clients = input.clients();
        Arc::new(ExecutionPipeline::new(ExecutionPipelineDeps {
            validator: Arc::clone(&owned.validator),
            plan_builder: PlanBuilder::new(
                Arc::clone(clients.fee_calculator()),
                Arc::clone(detection.market_registry()),
            ),
            dispatcher: Dispatcher::new(
                execution_mode.clone(),
                Arc::clone(detection.book_store()),
                Arc::clone(clients.fee_calculator()),
                Arc::clone(infra.metrics()),
            ),
            order_strategy: Arc::clone(&owned.order_strategy),
            capital_manager: Arc::clone(&owned.capital),
            risk_engine: Arc::clone(risk.engine()),
            risk_metrics: Arc::clone(risk.metrics()),
            fsm: Arc::clone(risk.fsm()),
            market_inflight: Arc::clone(&owned.market_inflight),
            metrics: Arc::clone(infra.metrics()),
            mode: execution_mode,
            trade_repo: Arc::clone(&infra.persistence().trade_repo),
            audit_writer: Arc::clone(&infra.persistence().audit_writer),
            book_decision_context_writer: Arc::clone(
                &infra.persistence().book_decision_context_writer,
            ),
            relay_notify: Arc::clone(&owned.relay_notify),
            reconcile_notify: Arc::clone(&owned.reconcile_notify),
            metrics_state: Arc::clone(risk.metrics_state()),
            runtime_config: Arc::clone(infra.runtime_store()),
            factors: Arc::clone(infra.factor_store()),
            shadow_writer: Some(infra.shadow_writer().clone()),
            ctf_redeem: clients.ctf_redeem().map(Arc::clone),
            holder_address: clients.holder_address().to_owned(),
            trade_integrity: Arc::clone(&owned.trade_integrity),
        }))
    }

    fn build_data_pipeline(
        input: &TradingBuildInput<'_>,
        detection: &DetectionStack,
        risk: &BuildRisk,
        settlement_tx: flume::Sender<MarketSettlementRequest>,
        shutdown: CancellationToken,
    ) -> Arc<DataPipeline> {
        let deploy = input.wiring().deploy();
        let infra = input.infra();
        let clients = input.clients();
        Arc::new(DataPipeline::new(DataPipelineDeps {
            event_source: Arc::clone(clients.ws_manager()) as Arc<dyn PipelineEventSource>,
            book_store: Arc::clone(detection.book_store()),
            market_registry: Arc::clone(detection.market_registry()),
            coalescer_tx: detection.token_tx().clone(),
            settlement_tx,
            metrics: Arc::clone(infra.metrics()),
            alerts: Arc::clone(infra.alerts()),
            backpressure: Arc::clone(risk.backpressure()),
            book_fact_writer: Some(Arc::clone(&infra.persistence().book_fact_writer)),
            book_shard_count: deploy.execution.book_apply.shard_count,
            book_channel_capacity: deploy.execution.book_apply.channel_capacity,
            shutdown,
            status_nudge: input.lifecycle().system_status_nudge().clone(),
        }))
    }
}
