use std::cmp::Ordering;
use std::collections::BinaryHeap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use num_traits::ToPrimitive;
use oxide_arb_algorithm::scorer::ScoredOpportunity;
use oxide_arb_error::OxideError;
use rust_decimal::Decimal;
use tokio_util::sync::CancellationToken;

use crate::execution::runner::shard_index;
use crate::observability::metrics_hub::MetricsHub;

struct ScoredEntry {
    score: Decimal,
    received_at: Instant,
    scored: ScoredOpportunity,
}

/// Bounded priority queue: dispatch highest score, evict lowest when full.
struct FunnelQueue {
    entries: Vec<ScoredEntry>,
    max_size: usize,
}

impl FunnelQueue {
    const fn new(max_size: usize) -> Self {
        Self {
            entries: Vec::new(),
            max_size,
        }
    }

    fn len(&self) -> usize {
        self.entries.len()
    }

    fn submit(&mut self, scored: ScoredOpportunity, score: Decimal) -> bool {
        if self.entries.len() >= self.max_size {
            let Some(min_idx) = self.min_score_index() else {
                return false;
            };
            if score <= self.entries[min_idx].score {
                return false;
            }
            self.entries.swap_remove(min_idx);
        }
        self.entries.push(ScoredEntry {
            score,
            received_at: Instant::now(),
            scored,
        });
        true
    }

    /// Pop highest-scored entry using a max-heap over indices (O(n log n), n ≤ `max_size`).
    fn pop_best(&mut self) -> Option<ScoredEntry> {
        if self.entries.is_empty() {
            return None;
        }
        let mut heap: BinaryHeap<(Decimal, usize)> = self
            .entries
            .iter()
            .enumerate()
            .map(|(i, e)| (e.score, i))
            .collect();
        let (_, max_idx) = heap.pop()?;
        Some(self.entries.swap_remove(max_idx))
    }

    fn min_score_index(&self) -> Option<usize> {
        self.entries
            .iter()
            .enumerate()
            .min_by(|(_, a), (_, b)| a.score.partial_cmp(&b.score).unwrap_or(Ordering::Equal))
            .map(|(i, _)| i)
    }
}

/// Priority queue that rate-limits opportunity dispatch to execution shards.
///
/// Result of a fast-lane dispatch attempt.
pub enum FastLaneDispatch {
    Dispatched,
    Backpressure(ScoredOpportunity),
}

/// High-score opportunities should use [`Self::try_dispatch_immediate`] (fast lane).
/// This funnel only sweeps lower-priority backlog on a fixed interval.
pub struct Funnel {
    queue: parking_lot::Mutex<FunnelQueue>,
    shard_txs: Vec<flume::Sender<ScoredOpportunity>>,
    min_dispatch_interval: Duration,
    metrics: Arc<MetricsHub>,
}

impl Funnel {
    pub const fn new(
        shard_txs: Vec<flume::Sender<ScoredOpportunity>>,
        max_queue_size: usize,
        min_dispatch_interval: Duration,
        metrics: Arc<MetricsHub>,
    ) -> Self {
        Self {
            queue: parking_lot::Mutex::new(FunnelQueue::new(max_queue_size)),
            shard_txs,
            min_dispatch_interval,
            metrics,
        }
    }

    /// Submit a scored opportunity for rate-limited sweep dispatch.
    pub fn submit(&self, scored: ScoredOpportunity) {
        let score = scored.score;
        let mut queue = self.queue.lock();
        if queue.submit(scored, score) {
            self.metrics.funnel_enqueued.inc();
            self.metrics
                .funnel_queue_depth
                .set(ToPrimitive::to_i64(&queue.len()).unwrap_or(i64::MAX));
        } else {
            self.metrics.funnel_dropped.inc();
        }
        drop(queue);
    }

    /// Fast lane: dispatch immediately to the execution shard (bypass funnel timer).
    ///
    /// Dispatches immediately on success; returns the opportunity on channel backpressure.
    pub fn try_dispatch_immediate(&self, scored: ScoredOpportunity) -> FastLaneDispatch {
        let market_id = &scored.opportunity.market_id;
        let idx = shard_index(market_id.as_str(), self.shard_txs.len());
        match self.shard_txs[idx].try_send(scored) {
            Ok(()) => {
                self.metrics.funnel_fast_lane_dispatched.inc();
                FastLaneDispatch::Dispatched
            }
            Err(e) => FastLaneDispatch::Backpressure(e.into_inner()),
        }
    }

    /// Dispatch loop: pops the highest-scored entry every interval tick and routes to shard.
    pub async fn run(&self, shutdown: CancellationToken) -> Result<(), OxideError> {
        loop {
            tokio::select! {
                biased;
                () = shutdown.cancelled() => return Ok(()),
                () = tokio::time::sleep(self.min_dispatch_interval) => {
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
                        if let Err(e) = self.shard_txs[idx].send_async(entry.scored).await {
                            tracing::warn!(error = %e, shard = idx, "execution shard channel closed");
                            return Ok(());
                        }
                        self.metrics.funnel_dispatched.inc();
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;
    use oxide_arb_models::{
        domain::opportunity::{EndgameMeta, Opportunity},
        enums::{
            calibration::{DurationBucket, PriceZone},
            common::{MarketCategory, Side, StalenessLevel},
            opportunity::PayoutModel,
        },
        types::{Bps, EventId, MarketId, OpportunityId, Price, Shares, TokenId, Usd},
    };
    use rust_decimal_macros::dec;

    fn sample_scored(market: &str, score: Decimal) -> ScoredOpportunity {
        ScoredOpportunity {
            opportunity: Arc::new(Opportunity {
                opportunity_id: OpportunityId::new_v7(),
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
                calibration: oxide_arb_models::domain::calibration::CalibrationSnapshot {
                    bucket_key: oxide_arb_models::domain::calibration::BucketKey {
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
            fill_probability: dec!(1),
            urgency_factor: dec!(1),
            category_weight: dec!(1),
            staleness_discount: dec!(1),
            book_yes_version: 1,
            book_no_version: 1,
        }
    }

    #[test]
    fn queue_evicts_lowest_when_full() {
        let mut q = FunnelQueue::new(2);
        assert!(q.submit(sample_scored("a", dec!(1)), dec!(1)));
        assert!(q.submit(sample_scored("b", dec!(2)), dec!(2)));
        assert!(!q.submit(sample_scored("c", dec!(0.5)), dec!(0.5)));
        assert!(q.submit(sample_scored("d", dec!(3)), dec!(3)));
        let best = q.pop_best().unwrap();
        assert_eq!(best.score, dec!(3));
    }

    #[test]
    fn pop_best_returns_highest() {
        let mut q = FunnelQueue::new(3);
        q.submit(sample_scored("a", dec!(1)), dec!(1));
        q.submit(sample_scored("b", dec!(5)), dec!(5));
        q.submit(sample_scored("c", dec!(3)), dec!(3));
        assert_eq!(q.pop_best().unwrap().score, dec!(5));
    }
}
