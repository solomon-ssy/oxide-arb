use std::cmp::Ordering;
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

    fn pop_best(&mut self) -> Option<ScoredEntry> {
        let max_idx = self.max_score_index()?;
        Some(self.entries.swap_remove(max_idx))
    }

    fn min_score_index(&self) -> Option<usize> {
        self.entries
            .iter()
            .enumerate()
            .min_by(|(_, a), (_, b)| a.score.partial_cmp(&b.score).unwrap_or(Ordering::Equal))
            .map(|(i, _)| i)
    }

    fn max_score_index(&self) -> Option<usize> {
        self.entries
            .iter()
            .enumerate()
            .max_by(|(_, a), (_, b)| a.score.partial_cmp(&b.score).unwrap_or(Ordering::Equal))
            .map(|(i, _)| i)
    }
}

/// Priority queue that rate-limits opportunity dispatch to execution shards.
///
/// Scored opportunities are submitted by the scanner. The funnel holds them in a
/// score-ordered buffer and dispatches the best one at each `min_dispatch_interval`
/// tick directly to the shard channel for the opportunity's market. When the queue
/// is full, the lowest-scored entry is evicted if the incoming opportunity has a
/// higher score.
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

    /// Submit a scored opportunity for rate-limited dispatch.
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
                        let idx = shard_index(market_id, self.shard_txs.len());
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
    use oxide_arb_models::domain::calibration::{BucketKey, CalibrationSnapshot};
    use oxide_arb_models::domain::opportunity::{EndgameMeta, Opportunity};
    use oxide_arb_models::enums::calibration::{DurationBucket, PriceZone};
    use oxide_arb_models::enums::common::{MarketCategory, Side, StalenessLevel};
    use oxide_arb_models::enums::opportunity::PayoutModel;
    use oxide_arb_models::types::{Bps, MarketId, OpportunityId, Price, Shares, Usd};
    use rust_decimal_macros::dec;
    use std::sync::Arc;

    fn sample_scored(score: Decimal) -> ScoredOpportunity {
        ScoredOpportunity {
            opportunity: Arc::new(Opportunity {
                opportunity_id: OpportunityId::pending(),
                market_id: MarketId::new("m1"),
                event_id: "e1".into(),
                token_id: "t1".into(),
                side: Side::Buy,
                payout_model: PayoutModel::DirectionalSettlement {
                    projected_payout_if_correct: Usd::ZERO,
                    expected_payout: Usd::ZERO,
                    predicted_side: Side::Buy,
                },
                shares: Shares::ZERO,
                entry_price: Price::ZERO,
                total_cost: Usd::ZERO,
                total_fees: Usd::ZERO,
                net_profit: Usd::ZERO,
                expected_net_profit: Usd::ZERO,
                edge_bps: Bps::default(),
                resolution_adjust: dec!(0.5),
                depth_used_pct: dec!(10),
                staleness: StalenessLevel::Fresh,
                category: MarketCategory::Politics,
                meta: EndgameMeta {
                    predicted_yes: true,
                    confidence: dec!(0.95),
                    convergence_duration_secs: 60,
                    price_zone: PriceZone::Z99,
                    duration_bucket: DurationBucket::Short,
                    settlement_deadline: None,
                },
                calibration: CalibrationSnapshot {
                    bucket_key: BucketKey {
                        category: MarketCategory::Politics,
                        price_zone: PriceZone::Z99,
                        duration_bucket: DurationBucket::Short,
                    },
                    posterior_mean: dec!(0.95),
                    sample_size: 10,
                    alpha_prior: dec!(1),
                    beta_prior: dec!(1),
                    fallback_tier: 1,
                    fused_probability: dec!(0.95),
                },
                detected_at: Utc::now(),
            }),
            score,
            fill_probability: dec!(1),
            urgency_factor: dec!(1),
            category_weight: dec!(1),
            staleness_discount: dec!(1),
        }
    }

    #[test]
    fn evicts_lowest_when_full_and_incoming_higher() {
        let mut q = FunnelQueue::new(2);
        assert!(q.submit(sample_scored(dec!(1)), dec!(1)));
        assert!(q.submit(sample_scored(dec!(2)), dec!(2)));
        assert!(!q.submit(sample_scored(dec!(0.5)), dec!(0.5)));
        assert!(q.submit(sample_scored(dec!(3)), dec!(3)));

        let first = q.pop_best().unwrap();
        assert_eq!(first.score, dec!(3));
        let second = q.pop_best().unwrap();
        assert_eq!(second.score, dec!(2));
        assert!(q.pop_best().is_none());
    }

    #[test]
    fn drops_incoming_when_lower_than_queue_min() {
        let mut q = FunnelQueue::new(2);
        assert!(q.submit(sample_scored(dec!(5)), dec!(5)));
        assert!(q.submit(sample_scored(dec!(4)), dec!(4)));
        assert!(!q.submit(sample_scored(dec!(3)), dec!(3)));
        assert_eq!(q.len(), 2);
    }

    #[test]
    fn accepts_incoming_when_score_between_min_and_max() {
        let mut q = FunnelQueue::new(2);
        assert!(q.submit(sample_scored(dec!(5)), dec!(5)));
        assert!(q.submit(sample_scored(dec!(3)), dec!(3)));
        assert!(q.submit(sample_scored(dec!(4)), dec!(4)));
        assert_eq!(q.len(), 2);
        let best = q.pop_best().unwrap();
        assert_eq!(best.score, dec!(5));
        let second = q.pop_best().unwrap();
        assert_eq!(second.score, dec!(4));
    }
}
