use std::cmp::Ordering;
use std::collections::BinaryHeap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use num_traits::ToPrimitive;
use oxide_arb_algorithm::scorer::ScoredOpportunity;
use oxide_arb_error::OxideError;
use tokio_util::sync::CancellationToken;

use crate::observability::metrics_hub::MetricsHub;

struct ScoredEntry {
    scored: ScoredOpportunity,
    received_at: Instant,
}

impl PartialEq for ScoredEntry {
    fn eq(&self, other: &Self) -> bool {
        self.scored.score == other.scored.score
    }
}

impl Eq for ScoredEntry {}

impl PartialOrd for ScoredEntry {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for ScoredEntry {
    fn cmp(&self, other: &Self) -> Ordering {
        self.scored
            .score
            .partial_cmp(&other.scored.score)
            .unwrap_or(Ordering::Equal)
    }
}

/// Priority queue that rate-limits opportunity dispatch to the execution pipeline.
///
/// Scored opportunities are submitted by the scanner. The funnel holds them in a
/// max-heap (by score) and dispatches the best one at each `min_dispatch_interval`
/// tick. When the queue is full, the lowest-scored entry is evicted if the incoming
/// opportunity has a higher score.
pub struct Funnel {
    queue: parking_lot::Mutex<BinaryHeap<ScoredEntry>>,
    tx: flume::Sender<ScoredOpportunity>,
    max_queue_size: usize,
    min_dispatch_interval: Duration,
    metrics: Arc<MetricsHub>,
}

impl Funnel {
    pub const fn new(
        tx: flume::Sender<ScoredOpportunity>,
        max_queue_size: usize,
        min_dispatch_interval: Duration,
        metrics: Arc<MetricsHub>,
    ) -> Self {
        Self {
            queue: parking_lot::Mutex::new(BinaryHeap::new()),
            tx,
            max_queue_size,
            min_dispatch_interval,
            metrics,
        }
    }

    /// Submit a scored opportunity for rate-limited dispatch.
    pub fn submit(&self, scored: ScoredOpportunity) {
        let mut queue = self.queue.lock();
        if queue.len() >= self.max_queue_size {
            if let Some(min) = queue.peek() {
                if scored.score > min.scored.score {
                    queue.pop();
                } else {
                    self.metrics.funnel_dropped.inc();
                    return;
                }
            }
        }
        queue.push(ScoredEntry {
            scored,
            received_at: Instant::now(),
        });
        self.metrics.funnel_enqueued.inc();
        drop(queue);
    }

    /// Dispatch loop: pops the highest-scored entry every interval tick.
    pub async fn run(&self, shutdown: CancellationToken) -> Result<(), OxideError> {
        loop {
            tokio::select! {
                biased;
                () = shutdown.cancelled() => return Ok(()),
                () = tokio::time::sleep(self.min_dispatch_interval) => {
                    let entry = {
                        let mut queue = self.queue.lock();
                        queue.pop()
                    };
                    if let Some(entry) = entry {
                        let age_ms = ToPrimitive::to_u64(
                            &entry.received_at.elapsed().as_millis(),
                        )
                        .unwrap_or(u64::MAX);
                        self.metrics.funnel_dispatch_age_ms.observe(f64::from(
                            ToPrimitive::to_u32(&age_ms.min(u64::from(u32::MAX)))
                                .unwrap_or(u32::MAX),
                        ));
                        let _ = self.tx.send_async(entry.scored).await;
                        self.metrics.funnel_dispatched.inc();
                    }
                }
            }
        }
    }
}
