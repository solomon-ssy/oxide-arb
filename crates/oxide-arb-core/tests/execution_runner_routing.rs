//! Execution runner shard routing and parallel dispatch tests.

use async_trait::async_trait;
use oxide_arb_algorithm::scorer::ScoredOpportunity;
use oxide_arb_core::{
    execution::{
        port::ExecutionPort,
        runner::{DEFAULT_EXECUTION_SHARD_COUNT, ExecutionRunnerPool},
    },
    observability::metrics_hub::MetricsHub,
};
use oxide_arb_models::{
    domain::execution::ExecutionResult, enums::execution::ExecutionOutcomeSummary, types::MarketId,
};
use oxide_arb_test_support::fixtures::sample_scored;
use parking_lot::Mutex;
use std::{
    sync::{
        Arc,
        atomic::{AtomicU32, Ordering},
    },
    time::Duration,
};
use tokio_util::sync::CancellationToken;

struct CountingPipeline {
    inflight: Arc<AtomicU32>,
    max_inflight: Arc<AtomicU32>,
    executed: Arc<Mutex<Vec<MarketId>>>,
}

impl CountingPipeline {
    fn new() -> Self {
        Self {
            inflight: Arc::new(AtomicU32::new(0)),
            max_inflight: Arc::new(AtomicU32::new(0)),
            executed: Arc::new(Mutex::new(Vec::new())),
        }
    }

    fn record_inflight(&self) {
        let current = self.inflight.fetch_add(1, Ordering::SeqCst) + 1;
        loop {
            let prev = self.max_inflight.load(Ordering::SeqCst);
            if current <= prev {
                break;
            }
            if self
                .max_inflight
                .compare_exchange(prev, current, Ordering::SeqCst, Ordering::SeqCst)
                .is_ok()
            {
                break;
            }
        }
    }
}

#[async_trait]
impl ExecutionPort for CountingPipeline {
    async fn execute(&self, scored: Arc<ScoredOpportunity>) -> ExecutionResult {
        self.record_inflight();
        self.executed
            .lock()
            .push(scored.opportunity.market_id.clone());
        tokio::time::sleep(Duration::from_millis(10)).await;
        self.inflight.fetch_sub(1, Ordering::SeqCst);
        ExecutionResult::completed(ExecutionOutcomeSummary::Miss)
    }
}

fn scored_for_market(id: &str) -> Arc<ScoredOpportunity> {
    let mut scored = sample_scored();
    Arc::make_mut(&mut scored).opportunity = Arc::new({
        let mut opp = (*scored.opportunity).clone();
        opp.market_id = MarketId::new(id);
        opp
    });
    scored
}

#[tokio::test]
async fn multi_market_parallel_execution() {
    let metrics = Arc::new(MetricsHub::new());
    let shutdown = CancellationToken::new();
    let inflight = Arc::new(AtomicU32::new(0));
    let pipeline = Arc::new(CountingPipeline::new());
    let pipeline_port: Arc<dyn ExecutionPort> = pipeline.clone();

    let (pool, runners) = ExecutionRunnerPool::new(
        DEFAULT_EXECUTION_SHARD_COUNT,
        &pipeline_port,
        &shutdown,
        &inflight,
        &metrics,
    );

    let runner_handles: Vec<_> = runners
        .into_iter()
        .map(|runner| tokio::spawn(async move { runner.run().await }))
        .collect();

    let markets = ["m0", "m1", "m2", "m3"];
    for (idx, market) in markets.iter().enumerate() {
        let scored = scored_for_market(market);
        pool.shard_senders()[idx % DEFAULT_EXECUTION_SHARD_COUNT]
            .send_async(scored)
            .await
            .expect("dispatch to runner shard");
    }

    tokio::time::sleep(Duration::from_millis(100)).await;
    shutdown.cancel();

    for handle in runner_handles {
        let _ = handle.await;
    }

    assert!(
        pipeline.max_inflight.load(Ordering::SeqCst) >= 2,
        "expected parallel execution across shards, max_inflight={}",
        pipeline.max_inflight.load(Ordering::SeqCst)
    );

    let executed = pipeline.executed.lock().clone();
    assert_eq!(executed.len(), 4);
    for market in markets {
        assert!(
            executed.iter().any(|m| m.as_str() == market),
            "missing execution for {market}"
        );
    }
}

#[tokio::test]
async fn runner_drains_until_shutdown() {
    let metrics = Arc::new(MetricsHub::new());
    let shutdown = CancellationToken::new();
    let inflight = Arc::new(AtomicU32::new(0));
    let pipeline = Arc::new(CountingPipeline::new());
    let pipeline_port: Arc<dyn ExecutionPort> = pipeline.clone();

    let (pool, runners) =
        ExecutionRunnerPool::new(1, &pipeline_port, &shutdown, &inflight, &metrics);

    let runner = runners.into_iter().next().expect("one runner");
    let handle = tokio::spawn(async move { runner.run().await });

    pool.shard_senders()[0]
        .send_async(scored_for_market("solo"))
        .await
        .expect("send");

    tokio::time::sleep(Duration::from_millis(30)).await;
    shutdown.cancel();
    handle.await.expect("join").expect("run ok");

    assert_eq!(pipeline.executed.lock().len(), 1);
}
