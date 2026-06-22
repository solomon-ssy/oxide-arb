use crate::{execution::port::ExecutionPort, observability::metrics_hub::MetricsHub};
use oxide_arb_algorithm::scorer::ScoredOpportunity;
use oxide_arb_error::OxideError;
use std::sync::{
    Arc,
    atomic::{AtomicU32, Ordering},
};
use tokio_util::sync::CancellationToken;

/// Default number of execution shards (one runner per shard).
pub const DEFAULT_EXECUTION_SHARD_COUNT: usize = 4;

pub struct ExecutionRunner {
    rx: flume::Receiver<Arc<ScoredOpportunity>>,
    pipeline: Arc<dyn ExecutionPort>,
    shutdown: CancellationToken,
    inflight: Arc<AtomicU32>,
    metrics: Arc<MetricsHub>,
}

impl ExecutionRunner {
    pub const fn new(
        rx: flume::Receiver<Arc<ScoredOpportunity>>,
        pipeline: Arc<dyn ExecutionPort>,
        shutdown: CancellationToken,
        inflight: Arc<AtomicU32>,
        metrics: Arc<MetricsHub>,
    ) -> Self {
        Self {
            rx,
            pipeline,
            shutdown,
            inflight,
            metrics,
        }
    }

    pub async fn run(&self) -> Result<(), OxideError> {
        loop {
            tokio::select! {
                biased;
                () = self.shutdown.cancelled() => {
                    tracing::info!("execution runner shutting down");
                    return Ok(());
                }
                scored = self.rx.recv_async() => {
                    if let Ok(scored) = scored {
                        self.inflight.fetch_add(1, Ordering::AcqRel);
                        self.metrics
                            .active_tasks
                            .set(i64::from(self.inflight.load(Ordering::Relaxed)));
                        let result = self.pipeline.execute(scored).await;
                        self.inflight.fetch_sub(1, Ordering::AcqRel);
                        self.metrics
                            .active_tasks
                            .set(i64::from(self.inflight.load(Ordering::Relaxed)));
                        if result.is_rejected() {
                            tracing::debug!(
                                stage = ?result.rejection_stage,
                                reason = ?result.rejection_reason,
                                "execution rejected"
                            );
                        }
                    } else {
                        tracing::warn!("execution queue channel closed");
                        return Ok(());
                    }
                }
            }
        }
    }

    pub fn inflight_count(&self) -> u32 {
        self.inflight.load(Ordering::Acquire)
    }
}

/// Pool of N shard runners; funnel dispatches directly to [`Self::shard_senders`].
pub struct ExecutionRunnerPool {
    shard_txs: Vec<flume::Sender<Arc<ScoredOpportunity>>>,
}

impl ExecutionRunnerPool {
    pub fn new(
        shard_count: usize,
        pipeline: &Arc<dyn ExecutionPort>,
        shutdown: &CancellationToken,
        inflight: &Arc<AtomicU32>,
        metrics: &Arc<MetricsHub>,
    ) -> (Self, Vec<ExecutionRunner>) {
        let shard_count = shard_count.max(1);
        let mut shard_txs = Vec::with_capacity(shard_count);
        let mut runners = Vec::with_capacity(shard_count);

        for _ in 0..shard_count {
            let (tx, rx) = flume::bounded(256);
            shard_txs.push(tx);
            runners.push(ExecutionRunner::new(
                rx,
                Arc::clone(pipeline),
                shutdown.clone(),
                Arc::clone(inflight),
                Arc::clone(metrics),
            ));
        }

        (Self { shard_txs }, runners)
    }

    #[must_use]
    pub fn shard_senders(&self) -> &[flume::Sender<Arc<ScoredOpportunity>>] {
        &self.shard_txs
    }
}
