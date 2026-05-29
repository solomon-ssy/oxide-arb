//! In-memory outbox buffer for synchronous post-trade spill (PR-1).
//!
//! Production flusher integration lands in PR-8; until then spilled jobs are
//! replayed directly by the execution outcome drain.

use crate::{observability::metrics_hub::MetricsHub, outbox::event_store::EventStore};
use chrono::Utc;
use oxide_arb_error::OxideError;
use oxide_arb_models::{
    domain::{execution::PostTradeJob, outbox::OutboxEventInfo},
    enums::outbox::{OutboxAggregateType, OutboxEventType},
    types::{AggregateId, OutboxEventId},
};
use parking_lot::Mutex;
use std::{collections::VecDeque, sync::Arc};

/// Synchronous spill buffer backing post-trade backpressure.
pub struct InMemoryEventStore {
    post_trade_jobs: Mutex<VecDeque<PostTradeJob>>,
    outbox_events: Mutex<VecDeque<OutboxEventInfo>>,
}

impl InMemoryEventStore {
    pub const fn new() -> Self {
        Self {
            post_trade_jobs: Mutex::new(VecDeque::new()),
            outbox_events: Mutex::new(VecDeque::new()),
        }
    }

    /// Spill a post-trade job into the in-memory outbox (never drops).
    pub fn enqueue_sync_post_trade(
        &self,
        job: PostTradeJob,
        metrics: &MetricsHub,
    ) -> Result<(), OxideError> {
        let mut jobs = self.post_trade_jobs.lock();
        jobs.push_back(job);

        metrics
            .outbox_pending
            .set(i64::try_from(jobs.len()).unwrap_or(i64::MAX));
        drop(jobs);

        Ok(())
    }

    /// Pop one spilled job for replay (FIFO).
    pub fn try_pop_post_trade(&self) -> Option<PostTradeJob> {
        self.post_trade_jobs.lock().pop_front()
    }

    /// Drain all spilled jobs in FIFO order (shutdown path).
    pub fn drain_post_trade_jobs(&self) -> Vec<PostTradeJob> {
        self.post_trade_jobs.lock().drain(..).collect()
    }

    pub fn pending_post_trade_count(&self) -> usize {
        self.post_trade_jobs.lock().len()
    }
}

#[async_trait::async_trait]
impl EventStore for InMemoryEventStore {
    async fn append(
        &self,
        aggregate_type: OutboxAggregateType,
        aggregate_id: AggregateId,
        event_type: OutboxEventType,
        payload: &serde_json::Value,
    ) -> Result<OutboxEventInfo, OxideError> {
        let event = OutboxEventInfo {
            event_id: OutboxEventId::generate(),
            aggregate_type,
            aggregate_id,
            event_type,
            payload: payload.clone(),
            publish_attempts: 0,
            published_at: None,
            last_error: None,
            dead_letter_reason: None,
            created_at: Utc::now(),
        };
        self.outbox_events.lock().push_back(event.clone());
        Ok(event)
    }

    async fn fetch_pending(&self, limit: usize) -> Result<Vec<OutboxEventInfo>, OxideError> {
        Ok(self
            .outbox_events
            .lock()
            .iter()
            .take(limit)
            .cloned()
            .collect())
    }

    async fn mark_published(&self, event_id: &OutboxEventId) -> Result<(), OxideError> {
        self.outbox_events
            .lock()
            .retain(|event| &event.event_id != event_id);
        Ok(())
    }

    async fn record_failure(
        &self,
        event: &OutboxEventInfo,
        reason: &str,
    ) -> Result<(), OxideError> {
        for stored in self.outbox_events.lock().iter_mut() {
            if stored.event_id == event.event_id {
                stored.publish_attempts = stored.publish_attempts.saturating_add(1);
                stored.last_error = Some(reason.to_owned());
            }
        }
        Ok(())
    }

    async fn mark_dead_letter(
        &self,
        event_id: &OutboxEventId,
        reason: &str,
    ) -> Result<(), OxideError> {
        for stored in self.outbox_events.lock().iter_mut() {
            if &stored.event_id == event_id {
                stored.dead_letter_reason = Some(reason.to_owned());
            }
        }
        Ok(())
    }

    async fn dead_letter_count(&self) -> Result<u64, OxideError> {
        Ok(self
            .outbox_events
            .lock()
            .iter()
            .filter(|event| event.dead_letter_reason.is_some())
            .count()
            .try_into()
            .unwrap_or(u64::MAX))
    }
}

impl Default for InMemoryEventStore {
    fn default() -> Self {
        Self::new()
    }
}

/// Shared handle used across backpressure sites and the outcome drain.
pub type SharedInMemoryEventStore = Arc<InMemoryEventStore>;

#[cfg(test)]
mod tests {
    use super::*;
    use oxide_arb_models::{
        domain::scored_snapshot::ScoredOpportunitySnapshot,
        enums::{
            calibration::{DurationBucket, PriceZone},
            common::{ExecutionMode, MarketCategory, Side, StalenessLevel},
            execution::ExecutionOutcome,
        },
        types::{EventId, ExecutionId, MarketId, OpportunityId, Price, Shares, TokenId, TradeId},
    };
    use rust_decimal_macros::dec;

    fn sample_job(id: &str) -> PostTradeJob {
        PostTradeJob {
            trade_id: TradeId::new(id),
            execution_id: ExecutionId::generate(),
            opportunity_id: OpportunityId::new_v7(),
            market_id: MarketId::new("m1"),
            event_id: EventId::new("e1"),
            token_id: TokenId::new("t1"),
            side: Side::Buy,
            plan_shares: Shares::new(dec!(10)),
            entry_price: Price::new(dec!(0.5)),
            execution_mode: ExecutionMode::Paper,
            edge_bps: None,
            detected_profit: None,
            detected_at: chrono::Utc::now(),
            category: MarketCategory::Politics,
            scored_snapshot: ScoredOpportunitySnapshot {
                resolution_prob: 0.0,
                confidence: 0.0,
                convergence_secs: 0,
                price_zone: PriceZone::Z97,
                duration_bucket: DurationBucket::Medium,
                depth_used_pct: 0.0,
                staleness: StalenessLevel::Fresh,
            },
            outcome: ExecutionOutcome::Miss {
                reason: "test".into(),
                execution_mode: ExecutionMode::Paper,
            },
        }
    }

    #[test]
    fn spill_fifo_ordering() {
        let store = InMemoryEventStore::new();
        let metrics = MetricsHub::new();

        store
            .enqueue_sync_post_trade(sample_job("a"), &metrics)
            .expect("enqueue a");
        store
            .enqueue_sync_post_trade(sample_job("b"), &metrics)
            .expect("enqueue b");
        store
            .enqueue_sync_post_trade(sample_job("c"), &metrics)
            .expect("enqueue c");

        assert_eq!(store.try_pop_post_trade().unwrap().trade_id.as_str(), "a");
        assert_eq!(store.try_pop_post_trade().unwrap().trade_id.as_str(), "b");
        assert_eq!(store.try_pop_post_trade().unwrap().trade_id.as_str(), "c");
        assert!(store.try_pop_post_trade().is_none());
    }

    #[test]
    fn drain_preserves_fifo() {
        let store = InMemoryEventStore::new();
        let metrics = MetricsHub::new();
        for id in ["x", "y", "z"] {
            store
                .enqueue_sync_post_trade(sample_job(id), &metrics)
                .expect("enqueue");
        }
        let drained = store.drain_post_trade_jobs();
        assert_eq!(
            drained
                .iter()
                .map(|j| j.trade_id.as_str())
                .collect::<Vec<_>>(),
            vec!["x", "y", "z"]
        );
    }
}
