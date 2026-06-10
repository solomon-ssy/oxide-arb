use crate::{
    infra::sharding::shard_index,
    observability::{
        backpressure::BackpressurePolicy,
        latency::{observe_scan_to_dispatch, stamp_dispatch_started},
        metrics_hub::MetricsHub,
    },
};
use num_traits::ToPrimitive;
use oxide_arb_algorithm::scorer::ScoredOpportunity;
use oxide_arb_error::OxideError;
use oxide_arb_models::{runtime_config::FunnelConfig, types::MicroScore};
use std::{
    cmp::{Ordering, Reverse},
    collections::{BinaryHeap, HashMap},
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering as AtomicOrdering},
    },
    time::{Duration, Instant},
};
use tokio_util::sync::CancellationToken;

#[derive(Clone)]
struct ScoredEntry {
    received_at: Instant,
    scored: Arc<ScoredOpportunity>,
}

#[derive(Clone, Copy, Eq, PartialEq)]
struct HeapKey {
    score: MicroScore,
    received_at: Instant,
    id: u64,
}

impl PartialOrd for HeapKey {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for HeapKey {
    fn cmp(&self, other: &Self) -> Ordering {
        match self.score.cmp(&other.score) {
            Ordering::Equal => other.received_at.cmp(&self.received_at),
            non_eq => non_eq,
        }
    }
}

enum SubmitOutcome {
    Enqueued,
    EnqueuedEvicted,
    Dropped,
}

/// Bounded priority queue with O(log n) submit/evict via lazy dual-heaps.
struct FunnelQueue {
    entries: HashMap<u64, ScoredEntry>,
    max_heap: BinaryHeap<HeapKey>,
    min_heap: BinaryHeap<Reverse<HeapKey>>,
    next_id: u64,
    max_size: usize,
}

impl FunnelQueue {
    fn new(max_size: usize) -> Self {
        Self {
            entries: HashMap::new(),
            max_heap: BinaryHeap::new(),
            min_heap: BinaryHeap::new(),
            next_id: 0,
            max_size,
        }
    }

    fn len(&self) -> usize {
        self.entries.len()
    }

    fn push_heaps(&mut self, key: &HeapKey) {
        self.max_heap.push(*key);
        self.min_heap.push(Reverse(*key));
    }

    fn peek_min_score(&mut self) -> Option<MicroScore> {
        while let Some(Reverse(key)) = self.min_heap.peek() {
            if self.entries.contains_key(&key.id) {
                return Some(key.score);
            }
            self.min_heap.pop();
        }
        None
    }

    fn evict_min(&mut self) -> bool {
        while let Some(Reverse(key)) = self.min_heap.pop() {
            if self.entries.remove(&key.id).is_some() {
                return true;
            }
        }
        false
    }

    fn submit(&mut self, scored: Arc<ScoredOpportunity>, score: MicroScore) -> SubmitOutcome {
        let evicted = if self.entries.len() >= self.max_size {
            let Some(min_score) = self.peek_min_score() else {
                return SubmitOutcome::Dropped;
            };
            if score <= min_score {
                return SubmitOutcome::Dropped;
            }
            self.evict_min();
            true
        } else {
            false
        };

        let id = self.next_id;
        self.next_id += 1;
        let received_at = Instant::now();
        self.entries.insert(
            id,
            ScoredEntry {
                received_at,
                scored,
            },
        );
        self.push_heaps(&HeapKey {
            score,
            received_at,
            id,
        });

        if evicted {
            SubmitOutcome::EnqueuedEvicted
        } else {
            SubmitOutcome::Enqueued
        }
    }

    fn pop_best(&mut self) -> Option<ScoredEntry> {
        while let Some(key) = self.max_heap.pop() {
            if let Some(entry) = self.entries.remove(&key.id) {
                return Some(entry);
            }
        }
        None
    }

    /// Resize the queue; evicts lowest-scored entries when shrinking.
    /// Returns the number of evicted entries.
    fn set_max_size(&mut self, max_size: usize) -> usize {
        self.max_size = max_size;
        let mut evicted = 0;
        while self.entries.len() > self.max_size {
            if !self.evict_min() {
                break;
            }
            evicted += 1;
        }
        evicted
    }

    fn evict_lowest(&mut self) -> bool {
        self.evict_min()
    }
}

/// Result of a fast-lane dispatch attempt.
pub enum FastLaneDispatch {
    Dispatched,
    Backpressure(Arc<ScoredOpportunity>),
}

/// High-score opportunities should use [`Self::try_dispatch_immediate`] (fast lane).
/// This funnel only sweeps lower-priority backlog on a fixed interval.
///
/// Queue capacity and sweep cadence are hot-reloadable via [`Self::reload`].
pub struct Funnel {
    queue: parking_lot::Mutex<FunnelQueue>,
    shard_txs: Vec<flume::Sender<Arc<ScoredOpportunity>>>,
    min_dispatch_interval_ms: AtomicU64,
    metrics: Arc<MetricsHub>,
    backpressure: Option<Arc<BackpressurePolicy>>,
}

impl Funnel {
    pub fn new(
        shard_txs: Vec<flume::Sender<Arc<ScoredOpportunity>>>,
        max_queue_size: usize,
        min_dispatch_interval: Duration,
        metrics: Arc<MetricsHub>,
    ) -> Self {
        Self::with_backpressure(
            shard_txs,
            max_queue_size,
            min_dispatch_interval,
            metrics,
            None,
        )
    }

    pub fn with_backpressure(
        shard_txs: Vec<flume::Sender<Arc<ScoredOpportunity>>>,
        max_queue_size: usize,
        min_dispatch_interval: Duration,
        metrics: Arc<MetricsHub>,
        backpressure: Option<Arc<BackpressurePolicy>>,
    ) -> Self {
        Self {
            queue: parking_lot::Mutex::new(FunnelQueue::new(max_queue_size)),
            shard_txs,
            min_dispatch_interval_ms: AtomicU64::new(
                u64::try_from(min_dispatch_interval.as_millis()).unwrap_or(u64::MAX),
            ),
            metrics,
            backpressure,
        }
    }

    /// Hot-reload funnel parameters (runtime-config activation).
    ///
    /// A shrunken queue capacity evicts the lowest-scored entries immediately
    /// (counted as execution-shard evictions); the sweep interval applies from
    /// the next dispatch tick.
    pub fn reload(&self, config: &FunnelConfig) {
        self.min_dispatch_interval_ms
            .store(config.min_dispatch_interval_ms, AtomicOrdering::Relaxed);
        let evicted = {
            let mut queue = self.queue.lock();
            queue.set_max_size(config.max_queue_size)
        };
        for _ in 0..evicted {
            self.record_execution_shard_evict();
        }
        if evicted > 0 {
            self.metrics
                .funnel_queue_depth
                .set(ToPrimitive::to_i64(&self.queue.lock().len()).unwrap_or(i64::MAX));
        }
    }

    /// Submit a scored opportunity for rate-limited sweep dispatch.
    pub fn submit(&self, scored: Arc<ScoredOpportunity>) {
        let score = scored.score;
        let mut queue = self.queue.lock();
        match queue.submit(scored, score) {
            SubmitOutcome::Enqueued => {
                self.metrics.funnel_enqueued.inc();
            }
            SubmitOutcome::EnqueuedEvicted => {
                self.metrics.funnel_enqueued.inc();
                self.record_execution_shard_evict();
            }
            SubmitOutcome::Dropped => {
                self.metrics.funnel_dropped.inc();
            }
        }
        self.metrics
            .funnel_queue_depth
            .set(ToPrimitive::to_i64(&queue.len()).unwrap_or(i64::MAX));
        drop(queue);
    }

    /// Fast lane: dispatch immediately to the execution shard (bypass funnel timer).
    ///
    /// Dispatches immediately on success; returns the opportunity on channel backpressure.
    pub fn try_dispatch_immediate(&self, scored: Arc<ScoredOpportunity>) -> FastLaneDispatch {
        let scored = stamp_dispatch_started(scored);
        observe_scan_to_dispatch(&scored.trace, &self.metrics);
        let market_id = &scored.opportunity.market_id;
        let idx = shard_index(market_id.as_str(), self.shard_txs.len());
        match self.shard_txs[idx].try_send(scored) {
            Ok(()) => {
                self.metrics.funnel_fast_lane_dispatched.inc();
                FastLaneDispatch::Dispatched
            }
            Err(error) => FastLaneDispatch::Backpressure(error.into_inner()),
        }
    }

    /// Dispatch loop: pops the highest-scored entry every interval tick and routes to shard.
    ///
    /// The sweep interval is read per tick so a runtime-config activation takes
    /// effect on the next dispatch.
    pub async fn run(&self, shutdown: CancellationToken) -> Result<(), OxideError> {
        loop {
            let interval = Duration::from_millis(
                self.min_dispatch_interval_ms
                    .load(AtomicOrdering::Relaxed)
                    .max(1),
            );
            tokio::select! {
                biased;
                () = shutdown.cancelled() => return Ok(()),
                () = tokio::time::sleep(interval) => {
                    let entry = {
                        let mut queue = self.queue.lock();
                        let entry = queue.pop_best();
                        self.metrics
                            .funnel_queue_depth
                            .set(ToPrimitive::to_i64(&queue.len()).unwrap_or(i64::MAX));
                        entry
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
                        let market_id = &entry.scored.opportunity.market_id;
                        let idx = shard_index(market_id.as_str(), self.shard_txs.len());
                        match self.shard_txs[idx].send_async(Arc::clone(&entry.scored)).await {
                            Ok(()) => {
                                self.metrics.funnel_dispatched.inc();
                            }
                            Err(error) => {
                                let mut scored = error.into_inner();
                                self.evict_lowest_from_queue();
                                match self.shard_txs[idx].try_send(Arc::clone(&scored)) {
                                    Ok(()) => {
                                        self.metrics.funnel_dispatched.inc();
                                    }
                                    Err(retry_err) => {
                                        scored = retry_err.into_inner();
                                        self.submit(scored);
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    fn evict_lowest_from_queue(&self) {
        let mut queue = self.queue.lock();
        if queue.evict_lowest() {
            drop(queue);
            self.record_execution_shard_evict();
            self.metrics
                .funnel_queue_depth
                .set(ToPrimitive::to_i64(&self.queue.lock().len()).unwrap_or(i64::MAX));
        }
    }

    fn record_execution_shard_evict(&self) {
        if let Some(bp) = &self.backpressure {
            bp.on_execution_shard_evict();
        } else {
            self.metrics.execution_shard_evicted_total.inc();
            self.metrics
                .backpressure_events
                .with_label_values(&["execution_shard", "evict"])
                .inc();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;
    use oxide_arb_models::{
        domain::{
            calibration,
            latency::LatencyTrace,
            opportunity::{EndgameMeta, Opportunity},
        },
        enums::{
            calibration::{DurationBucket, PriceZone},
            common::{MarketCategory, Side, StalenessLevel},
            opportunity::PayoutModel,
        },
        types::{
            Bps, EventId, MarketId, MicroProb, MicroScore, OpportunityId, Price, Shares, TokenId,
            Usd,
        },
    };
    use rust_decimal_macros::dec;
    use std::{thread::sleep, time::Duration};
    fn sample_scored(market: &str, score: MicroScore) -> Arc<ScoredOpportunity> {
        Arc::new(ScoredOpportunity {
            opportunity: Arc::new(Opportunity {
                opportunity_id: OpportunityId::from_v7(),
                market_id: MarketId::new(market),
                event_id: EventId::new("e"),
                token_id: TokenId::new("t"),
                side: Side::Buy,
                payout_model: PayoutModel::DirectionalSettlement {
                    projected_payout_if_correct: Usd::ZERO,
                    expected_payout: Usd::ZERO,
                    predicted_side: Side::Buy,
                },
                shares: Shares::ZERO,
                entry_price: Price::new(dec!(0.95)),
                total_cost: Usd::ZERO,
                total_fees: Usd::ZERO,
                net_profit: Usd::new(dec!(1)),
                expected_net_profit: Usd::new(dec!(1)),
                edge_bps: Bps::ZERO,
                resolution_adjust: dec!(1),
                depth_used_pct: dec!(1),
                staleness: StalenessLevel::Fresh,
                category: MarketCategory::Other,
                meta: EndgameMeta {
                    predicted_yes: true,
                    confidence: dec!(0.9),
                    convergence_duration_secs: 0,
                    price_zone: PriceZone::Z95,
                    duration_bucket: DurationBucket::Short,
                    settlement_deadline: None,
                },
                calibration: calibration::CalibrationSnapshot {
                    bucket_key: calibration::BucketKey {
                        category: MarketCategory::Other,
                        price_zone: PriceZone::Z95,
                        duration_bucket: DurationBucket::Short,
                    },
                    posterior_mean: dec!(0.9),
                    sample_size: 10,
                    alpha_prior: dec!(1),
                    beta_prior: dec!(1),
                    fallback_tier: 1,
                    fused_probability: dec!(0.9),
                },
                detected_at: Utc::now(),
            }),
            token_yes: TokenId::new("y"),
            token_no: TokenId::new("n"),
            score,
            fill_probability: MicroProb::ONE,
            urgency_factor: MicroProb::ONE,
            category_weight: MicroProb::ONE,
            staleness_discount: MicroProb::ONE,
            book_yes_version: 1,
            book_no_version: 1,
            applied_factors: Arc::from([]),
            trace: Arc::new(LatencyTrace::default()),
        })
    }

    #[test]
    fn heap_evicts_lowest_on_overflow() {
        let mut q = FunnelQueue::new(2);
        assert!(matches!(
            q.submit(
                sample_scored("a", MicroScore::from_micro(1_000_000)),
                MicroScore::from_micro(1_000_000)
            ),
            SubmitOutcome::Enqueued
        ));
        assert!(matches!(
            q.submit(
                sample_scored("b", MicroScore::from_micro(2_000_000)),
                MicroScore::from_micro(2_000_000)
            ),
            SubmitOutcome::Enqueued
        ));
        assert!(matches!(
            q.submit(
                sample_scored("c", MicroScore::from_micro(500_000)),
                MicroScore::from_micro(500_000)
            ),
            SubmitOutcome::Dropped
        ));
        assert!(matches!(
            q.submit(
                sample_scored("d", MicroScore::from_micro(3_000_000)),
                MicroScore::from_micro(3_000_000)
            ),
            SubmitOutcome::EnqueuedEvicted
        ));
        let best = q.pop_best().unwrap();
        assert_eq!(best.scored.score, MicroScore::from_micro(3_000_000));
        let second = q.pop_best().unwrap();
        assert_eq!(second.scored.score, MicroScore::from_micro(2_000_000));
        assert!(q.pop_best().is_none());
    }

    #[test]
    fn pop_best_score_order() {
        let mut q = FunnelQueue::new(3);
        q.submit(
            sample_scored("a", MicroScore::from_micro(1_000_000)),
            MicroScore::from_micro(1_000_000),
        );
        q.submit(
            sample_scored("b", MicroScore::from_micro(5_000_000)),
            MicroScore::from_micro(5_000_000),
        );
        q.submit(
            sample_scored("c", MicroScore::from_micro(3_000_000)),
            MicroScore::from_micro(3_000_000),
        );
        assert_eq!(
            q.pop_best().unwrap().scored.score,
            MicroScore::from_micro(5_000_000)
        );
        assert_eq!(
            q.pop_best().unwrap().scored.score,
            MicroScore::from_micro(3_000_000)
        );
        assert_eq!(
            q.pop_best().unwrap().scored.score,
            MicroScore::from_micro(1_000_000)
        );
    }

    #[test]
    fn tie_break_earlier_received_wins() {
        let mut q = FunnelQueue::new(10);
        q.submit(
            sample_scored("early", MicroScore::from_micro(5_000_000)),
            MicroScore::from_micro(5_000_000),
        );
        sleep(Duration::from_millis(2));
        q.submit(
            sample_scored("late", MicroScore::from_micro(5_000_000)),
            MicroScore::from_micro(5_000_000),
        );
        let first = q.pop_best().unwrap();
        assert_eq!(first.scored.opportunity.market_id.as_str(), "early");
    }
}
