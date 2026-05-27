//! In-process runtime harness for PR-8 e2e tests.

#[path = "test_util/book_store_seed.rs"]
pub mod book_store_seed;
#[path = "test_util/mock_event_source.rs"]
mod mock_event_source;
#[path = "test_util/risk_config.rs"]
mod risk_config;
#[path = "test_util/risk_metrics.rs"]
mod risk_metrics;
#[path = "test_util/risk_persistence.rs"]
mod risk_persistence;
#[path = "runtime_build.rs"]
mod runtime_build;
#[path = "test_util/scored_opportunity.rs"]
pub mod scored_opportunity;

use std::sync::Arc;
use std::time::{Duration, Instant};

use chrono::Utc;
use mock_event_source::{MockEventInject, MockEventSource};
use oxide_arb_core::detection::coalescer::Coalescer;
use oxide_arb_core::detection::funnel::Funnel;
use oxide_arb_core::detection::scanner_task::ScannerTask;
use oxide_arb_core::execution::execution_pipeline::{ExecutionPipeline, PostTradeJob};
use oxide_arb_core::execution::fsm::ExecutionFSM;
use oxide_arb_core::execution::market_inflight::MarketInFlightRegistry;
use oxide_arb_core::execution::runner::ExecutionRunner;
use oxide_arb_core::observability::metrics_hub::MetricsHub;
use oxide_arb_core::pipeline::book_store::BookStore;
use oxide_arb_core::pipeline::data_pipeline::DataPipeline;
use oxide_arb_core::pipeline::event_source::PipelineEventSource;
use oxide_arb_core::pipeline::market_cache::MarketCache;
use oxide_arb_core::pipeline::market_registry::MarketRegistry;
use oxide_arb_models::config::Settings;
use oxide_arb_models::domain::book::BookLevel;
use oxide_arb_models::domain::market::{MarketRegistryInfo, TokenInfo};
use oxide_arb_models::domain::pipeline::{
    BookSideData, BookSnapshotCmd, IngressTrace, PipelineEvent, PriceDeltaCmd, PriceLevelDelta,
};
use oxide_arb_models::enums::common::ExecutionMode;
use oxide_arb_models::enums::market::MarketStatus;
use oxide_arb_models::enums::{MarketCategory, TickSize};
use oxide_arb_models::types::{EventId, MarketId, Price, Shares, TokenId};
use oxide_arb_risk::traits::RiskPersistence;
use risk_persistence::TestRiskPersistence;
use runtime_build::{TestBuildDeps, assemble_test_runtime};
use rust_decimal_macros::dec;
use tokio_util::sync::CancellationToken;

pub struct RuntimeHarness {
    shutdown: CancellationToken,
    metrics: Arc<MetricsHub>,
    fsm: Arc<ExecutionFSM>,
    book_store: Arc<BookStore>,
    pipeline: Arc<ExecutionPipeline>,
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
            event_id: oxide_arb_models::types::EventId::new("test_event"),
            token_yes: yes.clone(),
            token_no: no.clone(),
            question: "Q".into(),
            slug: "q".into(),
            category: oxide_arb_models::enums::common::MarketCategory::Politics,
            status: MarketStatus::Active,
            neg_risk: false,
            tick_size: oxide_arb_models::enums::common::TickSize::Hundredth,
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
            volume_24h: oxide_arb_models::types::Usd::ZERO,
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
            volume_24h: oxide_arb_models::types::Usd::ZERO,
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

    pub const fn pipeline(&self) -> &Arc<ExecutionPipeline> {
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
