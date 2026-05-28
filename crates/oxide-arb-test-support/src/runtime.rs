//! Full runtime harness for e2e tests — no Postgres/ClickHouse/Redis required.

use crate::mocks::MockTradeRepository;
use crate::{
    mock_event::{MockEventInject, MockEventSource},
    persistence::test_persistence,
    risk::{TestRiskMetrics, TestRiskPersistence, test_risk_config},
};
use chrono::Utc;
use oxide_arb_algorithm::{
    calibration::ResolutionCalibrator, cooldown::InMemoryEmissionCooldown,
    endgame::EndgameDetector, pipeline::OpportunityPipeline, scorer::EndgameScorer,
};
use oxide_arb_api::{clob::ClobClient, fees::FeeCalculator, ws::ClobWsManager};
use oxide_arb_core::{
    bridge::{
        CoreOpportunityPipeline, fee_estimator::CoreFeeEstimator, risk_audit_sink::new_audit_sink,
        risk_metrics::CoreRiskMetrics,
    },
    detection::{
        coalescer::Coalescer,
        funnel::Funnel,
        scanner::Scanner,
        scanner_task::{ScannerTask, ScannerTaskDeps},
    },
    execution::{
        capital_manager::CapitalManager,
        dispatcher::Dispatcher,
        execution_pipeline::{ExecutionPipeline, ExecutionPipelineDeps},
        fsm::ExecutionFSM,
        market_inflight::MarketInFlightRegistry,
        plan_builder::PlanBuilder,
        port::ExecutionPort,
        runner::{ExecutionRunner, ExecutionRunnerPool},
        tiered_strategy::OrderStrategy,
        validator::Validator,
    },
    exposure::in_memory::InMemoryExposureReservation,
    observability::{
        alert_dispatcher::AlertDispatcher, backpressure::BackpressurePolicy,
        metrics_hub::MetricsHub,
    },
    outbox::in_memory::InMemoryEventStore,
    pipeline::{
        book_store::BookStore,
        data_pipeline::{DataPipeline, DataPipelineDeps},
        event_source::PipelineEventSource,
        market_cache::MarketCache,
        market_registry::MarketRegistry,
        staleness_classifier::StalenessClassifier,
    },
    service::risk_metrics::{ApiHealthTracker, RiskMetricsState},
};
use oxide_arb_error::OxideResult;
use oxide_arb_models::{
    config::{ExposureReservationConfig, Settings},
    domain::{
        book::BookLevel,
        execution::PostTradeJob,
        market::{MarketRegistryInfo, TokenInfo},
        pipeline::{
            BookSideData, BookSnapshotCmd, IngressTrace, PipelineEvent, PriceDeltaCmd,
            PriceLevelDelta,
        },
    },
    enums::{
        common::{ExecutionMode, MarketCategory, TickSize},
        market::MarketStatus,
    },
    types::{EventId, MarketId, MicroScore, MicroUsd, Price, Shares, TokenId, Usd},
};
use oxide_arb_risk::{
    audit_sink::AuditSink, builder::RiskEngineBuilder, clock::utc_clock, engine::RiskEngine,
    traits::RiskPersistence,
};
use rust_decimal_macros::dec;
use std::{
    sync::{Arc, atomic::AtomicU32},
    time::{Duration, Instant},
};
use tokio_util::sync::CancellationToken;

// ── Build dependencies ──────────────────────────────────────────────────

/// Injectable dependencies for [`assemble_test_runtime`].
pub struct TestBuildDeps {
    pub persistence: Arc<dyn RiskPersistence>,
    pub event_source: Arc<dyn PipelineEventSource>,
    pub clob: Option<Arc<ClobClient>>,
    pub execution_mode: ExecutionMode,
}

/// Wired runtime graph consumed by [`RuntimeHarness`].
pub struct TestRuntime {
    pub metrics: Arc<MetricsHub>,
    pub fsm: Arc<ExecutionFSM>,
    pub book_store: Arc<BookStore>,
    pub market_registry: Arc<MarketRegistry>,
    pub market_cache: Arc<MarketCache>,
    pub data_pipeline: Arc<DataPipeline>,
    pub coalescer: Arc<Coalescer>,
    pub funnel: Arc<Funnel>,
    pub pipeline: Arc<ExecutionPipeline<MockTradeRepository>>,
    pub market_inflight: Arc<MarketInFlightRegistry>,
    pub token_rx: flume::Receiver<TokenId>,
    pub market_rx_tap: flume::Receiver<MarketId>,
    pub post_trade_rx: flume::Receiver<PostTradeJob>,
    pub scanner_task: ScannerTask,
    pub execution_runners: Vec<ExecutionRunner>,
}

// ── Internal build helpers ──────────────────────────────────────────────

struct TestInfra {
    metrics: Arc<MetricsHub>,
    fsm: Arc<ExecutionFSM>,
    backpressure: Arc<BackpressurePolicy>,
    fee_calculator: Arc<FeeCalculator>,
    engine: Arc<RiskEngine>,
    risk_metrics: Arc<CoreRiskMetrics>,
    exposure: Arc<InMemoryExposureReservation>,
}

fn build_test_infra(
    settings: &Settings,
    persistence: Arc<dyn RiskPersistence>,
    ws_manager: &Arc<ClobWsManager>,
) -> OxideResult<TestInfra> {
    let metrics = Arc::new(MetricsHub::new());
    let alerts = Arc::new(AlertDispatcher::new(None, None, None, 60));
    let (risk_decision_audit, _audit_rx) = new_audit_sink(4096);

    let fee_calculator = Arc::new(FeeCalculator::from_config(&settings.polymarket.fees));
    let exposure = Arc::new(InMemoryExposureReservation::new(
        ExposureReservationConfig::default(),
    ));
    let api_tracker = Arc::new(ApiHealthTracker::new(Duration::from_secs(60)));
    let metrics_state = Arc::new(RiskMetricsState::new(api_tracker));
    metrics_state.seed_test_snapshot(Usd::new(dec!(5000)));
    let risk_metrics = Arc::new(CoreRiskMetrics::new(
        Arc::clone(&metrics_state),
        Arc::clone(&exposure),
        Arc::clone(ws_manager),
    ));

    let audit_sink: Arc<dyn AuditSink> = risk_decision_audit;
    let engine: Arc<RiskEngine> = Arc::new(
        RiskEngineBuilder::new()
            .config(test_risk_config())
            .persistence(persistence)
            .initial_equity(Usd::new(dec!(5000)))
            .clock(utc_clock())
            .audit_sink(audit_sink)
            .build(&TestRiskMetrics)?,
    );

    let fsm = Arc::new(ExecutionFSM::new(Arc::clone(&metrics)));
    let post_trade_spill = Arc::new(InMemoryEventStore::new());
    let backpressure = Arc::new(BackpressurePolicy::new(
        Arc::clone(&metrics),
        Some(alerts),
        post_trade_spill,
        settings.execution.book_apply.shard_count,
    ));

    Ok(TestInfra {
        metrics,
        fsm,
        backpressure,
        fee_calculator,
        engine,
        risk_metrics,
        exposure,
    })
}

fn build_test_scanner(
    settings: &Settings,
    infra: &TestInfra,
    book_store: &Arc<BookStore>,
    market_cache: &Arc<MarketCache>,
) -> Arc<Scanner> {
    let calibrator = Arc::new(ResolutionCalibrator::empty(
        settings.detection.calibration.clone(),
    ));
    let detector = EndgameDetector::new(
        &settings.detection.endgame,
        &settings.detection.calibration,
        Arc::clone(&calibrator),
        CoreFeeEstimator(Arc::clone(&infra.fee_calculator)),
    );
    let scorer = EndgameScorer::new(
        settings.detection.endgame.scorer.clone(),
        &settings.detection.endgame.fill_probability,
        settings.detection.endgame.settlement_window_hours,
    );
    let cooldown = InMemoryEmissionCooldown::new(&settings.detection.endgame.emission_cooldown);
    let min_profit = MicroUsd::try_from_decimal(settings.detection.min_profit_threshold_usd)
        .unwrap_or(MicroUsd::ZERO);
    let opportunity_pipeline: Arc<CoreOpportunityPipeline> = Arc::new(OpportunityPipeline::new(
        detector,
        scorer,
        cooldown,
        min_profit,
        &settings.detection.endgame.scorer,
    ));
    let staleness = StalenessClassifier::new(&settings.market_data);

    Arc::new(Scanner::new(
        opportunity_pipeline,
        Arc::clone(book_store),
        Arc::clone(market_cache),
        staleness,
        Arc::clone(&infra.metrics),
        None,
    ))
}

fn spawn_market_forwarder(
    coalescer_market_rx: flume::Receiver<MarketId>,
    scanner_market_tx: flume::Sender<MarketId>,
    market_tap_tx: flume::Sender<MarketId>,
) {
    tokio::spawn(async move {
        while let Ok(market_id) = coalescer_market_rx.recv_async().await {
            let _ = scanner_market_tx.send(market_id.clone());
            let _ = market_tap_tx.try_send(market_id);
        }
    });
}

struct TestExecutionGraph {
    pipeline: Arc<ExecutionPipeline<MockTradeRepository>>,
    market_inflight: Arc<MarketInFlightRegistry>,
    post_trade_rx: flume::Receiver<PostTradeJob>,
    funnel: Arc<Funnel>,
    execution_runners: Vec<ExecutionRunner>,
}

fn build_test_execution_graph(
    settings: &Settings,
    infra: &TestInfra,
    book_store: &Arc<BookStore>,
    market_registry: &Arc<MarketRegistry>,
    mode: ExecutionMode,
    clob: Option<Arc<ClobClient>>,
    shutdown: &CancellationToken,
) -> TestExecutionGraph {
    let persistence = test_persistence(shutdown.clone());
    let (outcome_tx, post_trade_rx) = flume::bounded(1024);
    let capital = Arc::new(CapitalManager::new(
        Arc::clone(&infra.exposure),
        ExposureReservationConfig::default(),
    ));
    let market_inflight = Arc::new(MarketInFlightRegistry::new());
    let pipeline = Arc::new(ExecutionPipeline::new(ExecutionPipelineDeps {
        validator: Validator::new(
            Arc::clone(book_store),
            StalenessClassifier::new(&settings.market_data),
            settings.execution.timeout.max_validation_slippage_bps,
            settings.execution.endgame_latency.max_book_to_order_ms,
            Arc::clone(&infra.metrics),
        ),
        plan_builder: PlanBuilder::new(
            Arc::clone(&infra.fee_calculator),
            Arc::clone(market_registry),
        ),
        dispatcher: Dispatcher::new(
            mode,
            match mode {
                ExecutionMode::Paper => Some(Arc::clone(book_store)),
                ExecutionMode::DryRun | ExecutionMode::Live => None,
            },
            Arc::clone(&infra.fee_calculator),
            Arc::clone(&infra.metrics),
        ),
        order_strategy: OrderStrategy::new(
            mode,
            clob,
            Arc::clone(&infra.fee_calculator),
            settings.execution.timeout.dispatcher_timeout_ms,
            Arc::clone(&infra.metrics),
        ),
        capital_manager: capital,
        risk_engine: Arc::clone(&infra.engine),
        risk_metrics: Arc::clone(&infra.risk_metrics),
        fsm: Arc::clone(&infra.fsm),
        market_inflight: Arc::clone(&market_inflight),
        metrics: Arc::clone(&infra.metrics),
        execution_mode: mode,
        trade_repo: Arc::clone(&persistence.trade_repo),
        audit_writer: Arc::clone(&persistence.audit_writer),
        outcome_tx,
        backpressure: Arc::clone(&infra.backpressure),
    }));
    let _persistence = persistence;

    let inflight = Arc::new(AtomicU32::new(0));
    let pipeline_port: Arc<dyn ExecutionPort> = pipeline.clone();
    let (runner_pool, execution_runners) = ExecutionRunnerPool::new(
        settings.execution.book_apply.shard_count,
        &pipeline_port,
        shutdown,
        &inflight,
        &infra.metrics,
    );
    let funnel = Arc::new(Funnel::with_backpressure(
        runner_pool.shard_senders().to_vec(),
        settings.execution.funnel.max_queue_size,
        Duration::from_millis(settings.execution.funnel.min_dispatch_interval_ms),
        Arc::clone(&infra.metrics),
        Some(Arc::clone(&infra.backpressure)),
    ));

    TestExecutionGraph {
        pipeline,
        market_inflight,
        post_trade_rx,
        funnel,
        execution_runners,
    }
}

pub fn assemble_test_runtime(
    settings: &Arc<Settings>,
    deps: TestBuildDeps,
    shutdown: CancellationToken,
) -> OxideResult<TestRuntime> {
    let mode = deps.execution_mode;
    settings.ensure_valid_for_mode(mode)?;

    let ws_manager = Arc::new(ClobWsManager::new(
        &settings.polymarket,
        &settings.market_data.websocket,
        shutdown.clone(),
        None,
        None,
    ));
    ws_manager.seed_test_connectivity();

    let infra = build_test_infra(settings, deps.persistence, &ws_manager)?;

    let book_store = Arc::new(BookStore::new(Arc::clone(&infra.metrics)));
    let market_registry = Arc::new(MarketRegistry::new());
    let market_cache = Arc::new(MarketCache::new(Arc::clone(&market_registry)));
    let scanner = build_test_scanner(settings, &infra, &book_store, &market_cache);

    let (token_tx, token_rx) = flume::bounded(8192);
    let (coalescer_market_tx, coalescer_market_rx) = flume::bounded::<MarketId>(512);
    let (scanner_market_tx, scanner_market_rx) = flume::bounded::<MarketId>(512);
    let (market_tap_tx, market_rx_tap) = flume::bounded::<MarketId>(512);
    spawn_market_forwarder(coalescer_market_rx, scanner_market_tx, market_tap_tx);

    let coalescer = Arc::new(Coalescer::new(
        Arc::clone(&market_registry),
        Duration::from_millis(settings.execution.coalescer.coalesce_window_ms),
        coalescer_market_tx,
        Arc::clone(&infra.metrics),
        shutdown.clone(),
    ));

    let execution = build_test_execution_graph(
        settings,
        &infra,
        &book_store,
        &market_registry,
        mode,
        deps.clob,
        &shutdown,
    );

    let data_pipeline = Arc::new(DataPipeline::new(DataPipelineDeps {
        event_source: deps.event_source,
        book_store: Arc::clone(&book_store),
        market_registry: Arc::clone(&market_registry),
        coalescer_tx: token_tx,
        metrics: Arc::clone(&infra.metrics),
        backpressure: Arc::clone(&infra.backpressure),
        book_shard_count: settings.execution.book_apply.shard_count,
        book_channel_capacity: settings.execution.book_apply.channel_capacity,
        shutdown: shutdown.clone(),
    }));

    let scanner_task = ScannerTask::new(ScannerTaskDeps {
        rx: scanner_market_rx,
        scanner,
        market_cache: Arc::clone(&market_cache),
        funnel: Arc::clone(&execution.funnel),
        dispatch_immediate_threshold: MicroScore::try_from_decimal(
            settings
                .execution
                .endgame_latency
                .dispatch_immediate_threshold,
        )
        .unwrap_or(MicroScore::ZERO),
        shutdown,
        metrics: Arc::clone(&infra.metrics),
    });

    Ok(TestRuntime {
        metrics: infra.metrics,
        fsm: infra.fsm,
        book_store,
        market_registry,
        market_cache,
        data_pipeline,
        coalescer,
        funnel: execution.funnel,
        pipeline: execution.pipeline,
        market_inflight: execution.market_inflight,
        execution_runners: execution.execution_runners,
        token_rx,
        market_rx_tap,
        post_trade_rx: execution.post_trade_rx,
        scanner_task,
    })
}

// ── RuntimeHarness ──────────────────────────────────────────────────────

pub struct RuntimeHarness {
    shutdown: CancellationToken,
    metrics: Arc<MetricsHub>,
    fsm: Arc<ExecutionFSM>,
    book_store: Arc<BookStore>,
    pipeline: Arc<ExecutionPipeline<MockTradeRepository>>,
    market_inflight: Arc<MarketInFlightRegistry>,
    inject: MockEventInject,
    market_rx_tap: flume::Receiver<MarketId>,
    post_trade_rx: flume::Receiver<PostTradeJob>,
    started: bool,
    inner: Option<HarnessInner>,
}

struct HarnessInner {
    data_pipeline: Arc<DataPipeline>,
    coalescer: Arc<Coalescer>,
    funnel: Arc<Funnel>,
    token_rx: flume::Receiver<TokenId>,
    scanner_task: ScannerTask,
    execution_runners: Vec<ExecutionRunner>,
    market_registry: Arc<MarketRegistry>,
    market_cache: Arc<MarketCache>,
}

impl RuntimeHarness {
    pub fn build() -> Self {
        Self::build_with_mode(ExecutionMode::Paper)
    }

    pub fn build_with_mode(mode: ExecutionMode) -> Self {
        let settings =
            Arc::new(Settings::new("nonexistent_dir_for_test").expect("default settings"));
        let shutdown = CancellationToken::new();
        let (source, inject) = MockEventSource::paired(8192);
        let event_source: Arc<dyn PipelineEventSource> = Arc::new(source);
        let persistence: Arc<dyn RiskPersistence> = Arc::new(TestRiskPersistence::new());

        let runtime = assemble_test_runtime(
            &settings,
            TestBuildDeps {
                persistence,
                event_source,
                clob: None,
                execution_mode: mode,
            },
            shutdown.clone(),
        )
        .expect("test runtime");

        let inner = HarnessInner {
            data_pipeline: runtime.data_pipeline,
            coalescer: runtime.coalescer,
            funnel: runtime.funnel,
            token_rx: runtime.token_rx,
            scanner_task: runtime.scanner_task,
            execution_runners: runtime.execution_runners,
            market_registry: runtime.market_registry,
            market_cache: runtime.market_cache,
        };

        Self {
            shutdown,
            metrics: runtime.metrics,
            fsm: runtime.fsm,
            book_store: runtime.book_store,
            pipeline: runtime.pipeline,
            market_inflight: runtime.market_inflight,
            inject,
            market_rx_tap: runtime.market_rx_tap,
            post_trade_rx: runtime.post_trade_rx,
            started: false,
            inner: Some(inner),
        }
    }

    pub fn register_fixture_market(&self) {
        let yes = TokenId::new("yes-token");
        let no = TokenId::new("no-token");
        let Some(inner) = &self.inner else {
            return;
        };
        inner.market_registry.register_market(MarketRegistryInfo {
            market_id: MarketId::new("0xtest_market"),
            event_id: EventId::new("test_event"),
            token_yes: yes.clone(),
            token_no: no.clone(),
            question: "Q".into(),
            slug: "q".into(),
            category: MarketCategory::Politics,
            status: MarketStatus::Active,
            neg_risk: false,
            tick_size: TickSize::Hundredth,
            tokens: vec![
                TokenInfo {
                    token_id: yes,
                    outcome: "Yes".into(),
                    neg_risk: false,
                },
                TokenInfo {
                    token_id: no,
                    outcome: "No".into(),
                    neg_risk: false,
                },
            ],
            best_bid: None,
            best_ask: None,
            depth_usd: None,
            min_order_size: dec!(1),
            volume_24h: Usd::ZERO,
            created_at: Utc::now(),
            updated_at: Utc::now(),
        });
        inner.market_cache.rebuild();
    }

    pub fn inject_fixture_books(&self) {
        let yes = TokenId::new("yes-token");
        let no = TokenId::new("no-token");
        self.inject_book_snapshot(
            &yes,
            vec![],
            vec![BookLevel::from_decimal_unchecked(
                Price::new(dec!(0.92)),
                Shares::new(dec!(1000)),
            )],
        );
        self.inject_book_snapshot(
            &no,
            vec![BookLevel::from_decimal_unchecked(
                Price::new(dec!(0.07)),
                Shares::new(dec!(1000)),
            )],
            vec![BookLevel::from_decimal_unchecked(
                Price::new(dec!(0.08)),
                Shares::new(dec!(1000)),
            )],
        );
    }

    pub fn register_endgame_market(&self, market_id: &str) {
        let Some(inner) = &self.inner else {
            return;
        };
        let yes = TokenId::new(format!("{market_id}-yes"));
        let no = TokenId::new(format!("{market_id}-no"));
        inner.market_registry.register_market(MarketRegistryInfo {
            market_id: MarketId::new(market_id),
            event_id: EventId::new("evt"),
            token_yes: yes.clone(),
            token_no: no.clone(),
            question: "Q".into(),
            slug: "q".into(),
            category: MarketCategory::Politics,
            status: MarketStatus::Active,
            neg_risk: false,
            tick_size: TickSize::Hundredth,
            tokens: vec![
                TokenInfo {
                    token_id: yes,
                    outcome: "Yes".into(),
                    neg_risk: false,
                },
                TokenInfo {
                    token_id: no,
                    outcome: "No".into(),
                    neg_risk: false,
                },
            ],
            best_bid: None,
            best_ask: None,
            depth_usd: None,
            min_order_size: dec!(1),
            volume_24h: Usd::ZERO,
            created_at: Utc::now(),
            updated_at: Utc::now(),
        });
        inner.market_cache.rebuild();
    }

    pub fn inject_price_delta(&self, token: &TokenId, changes: &[PriceLevelDelta]) {
        let cmd = PriceDeltaCmd {
            asset_id: token.clone(),
            changes: Arc::from(changes),
            timestamp_ms: u64::try_from(Utc::now().timestamp_millis()).unwrap_or(0),
            trace: IngressTrace::new(Instant::now(), 0),
        };
        self.inject.send(PipelineEvent::PriceDelta(cmd));
    }

    pub fn inject_book_snapshot(
        &self,
        token: &TokenId,
        bids: Vec<BookLevel>,
        asks: Vec<BookLevel>,
    ) {
        let cmd = BookSnapshotCmd {
            asset_id: token.clone(),
            bids: BookSideData::from_levels(Arc::from(bids)),
            asks: BookSideData::from_levels(Arc::from(asks)),
            timestamp_ms: u64::try_from(Utc::now().timestamp_millis()).unwrap_or(0),
            trace: IngressTrace::new(Instant::now(), 0),
        };
        self.inject.send(PipelineEvent::BookSnapshot(cmd));
    }

    pub fn inject_endgame_pair(&self, market_id: &str) {
        let yes = TokenId::new(format!("{market_id}-yes"));
        let no = TokenId::new(format!("{market_id}-no"));
        self.inject_book_snapshot(
            &yes,
            vec![],
            vec![BookLevel::from_decimal_unchecked(
                Price::new(dec!(0.92)),
                Shares::new(dec!(1000)),
            )],
        );
        self.inject_book_snapshot(
            &no,
            vec![BookLevel::from_decimal_unchecked(
                Price::new(dec!(0.07)),
                Shares::new(dec!(1000)),
            )],
            vec![BookLevel::from_decimal_unchecked(
                Price::new(dec!(0.08)),
                Shares::new(dec!(1000)),
            )],
        );
    }

    pub fn start(&mut self) {
        assert!(!self.started, "harness already started");
        self.started = true;
        let inner = self.inner.take().expect("harness inner");

        let data_pipeline = Arc::clone(&inner.data_pipeline);
        let shutdown = self.shutdown.clone();
        tokio::spawn(async move {
            if let Err(error) = data_pipeline.run().await {
                tracing::error!(%error, "data pipeline stopped");
            }
            shutdown.cancel();
        });

        let coalescer = Arc::clone(&inner.coalescer);
        let token_rx = inner.token_rx;
        let shutdown = self.shutdown.clone();
        tokio::spawn(async move {
            if let Err(error) = coalescer.run_with_ingress(Some(token_rx)).await {
                tracing::error!(%error, "coalescer stopped");
            }
            shutdown.cancel();
        });

        let scanner_task = inner.scanner_task;
        let shutdown = self.shutdown.clone();
        tokio::spawn(async move {
            if let Err(error) = scanner_task.run().await {
                tracing::error!(%error, "scanner stopped");
            }
            shutdown.cancel();
        });

        let funnel = Arc::clone(&inner.funnel);
        let shutdown = self.shutdown.clone();
        tokio::spawn(async move {
            if let Err(error) = funnel.run(shutdown).await {
                tracing::error!(%error, "funnel stopped");
            }
        });

        for runner in inner.execution_runners {
            let shutdown = self.shutdown.clone();
            tokio::spawn(async move {
                if let Err(error) = runner.run().await {
                    tracing::error!(%error, "execution runner stopped");
                }
                shutdown.cancel();
            });
        }
    }

    pub async fn run_until<F>(&self, mut pred: F, timeout: Duration) -> bool
    where
        F: FnMut(&MetricsHub) -> bool,
    {
        let deadline = Instant::now() + timeout;
        while Instant::now() < deadline {
            if pred(self.metrics()) {
                return true;
            }
            if self.post_trade_rx.try_recv().is_ok() {
                return true;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        pred(self.metrics())
    }

    pub fn metrics(&self) -> &MetricsHub {
        &self.metrics
    }

    pub fn fsm(&self) -> &ExecutionFSM {
        &self.fsm
    }

    pub fn try_recv_post_trade(&self) -> Option<PostTradeJob> {
        self.post_trade_rx.try_recv().ok()
    }

    pub const fn pipeline(&self) -> &Arc<ExecutionPipeline<MockTradeRepository>> {
        &self.pipeline
    }

    pub const fn book_store(&self) -> &Arc<BookStore> {
        &self.book_store
    }

    pub const fn market_inflight(&self) -> &Arc<MarketInFlightRegistry> {
        &self.market_inflight
    }

    pub const fn market_rx_tap(&self) -> &flume::Receiver<MarketId> {
        &self.market_rx_tap
    }
}
