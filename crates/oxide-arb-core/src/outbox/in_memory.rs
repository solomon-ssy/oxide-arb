//! In-memory outbox buffer for synchronous post-trade spill (PR-1).
//!
//! Production flusher integration lands in PR-8; until then spilled jobs are
//! replayed directly by the execution outcome drain.

use std::sync::Arc;

use chrono::Utc;
use oxide_arb_error::OxideError;
use oxide_arb_models::domain::outbox::OutboxEventInfo;
use oxide_arb_models::enums::outbox::{OutboxAggregateType, OutboxEventType};
use oxide_arb_models::types::{AggregateId, OutboxEventId};
use parking_lot::Mutex;
use serde_json::json;

use crate::execution::execution_pipeline::PostTradeJob;
use crate::observability::metrics_hub::MetricsHub;

/// Synchronous spill buffer backing post-trade backpressure.
pub struct InMemoryEventStore {
    events: Mutex<Vec<OutboxEventInfo>>,
    post_trade_jobs: Mutex<Vec<PostTradeJob>>,
}

impl InMemoryEventStore {
    pub const fn new() -> Self {
        Self {
            events: Mutex::new(Vec::new()),
            post_trade_jobs: Mutex::new(Vec::new()),
        }
    }

    /// Spill a post-trade job into the in-memory outbox (never drops).
    pub fn enqueue_sync_post_trade(
        &self,
        job: PostTradeJob,
        metrics: &MetricsHub,
    ) -> Result<(), OxideError> {
        let payload = serde_json::to_value(&job)
            .map_err(|e| OxideError::Internal(format!("post-trade spill serialize failed: {e}")))?;

        let event = OutboxEventInfo {
            event_id: OutboxEventId::generate(),
            aggregate_type: OutboxAggregateType::Trade,
            aggregate_id: AggregateId::new(job.trade_id.as_str()),
            event_type: OutboxEventType::Lifecycle,
            payload: json!({
                "kind": "post_trade_spill",
                "job": payload,
            }),
            publish_attempts: 0,
            published_at: None,
            last_error: None,
            dead_letter_reason: None,
            created_at: Utc::now(),
        };

        self.events.lock().push(event);
        self.post_trade_jobs.lock().push(job);

        metrics
            .outbox_pending
            .set(i64::try_from(self.events.lock().len()).unwrap_or(i64::MAX));

        Ok(())
    }

    /// Pop one spilled job for replay (FIFO).
    pub fn try_pop_post_trade(&self) -> Option<PostTradeJob> {
        self.post_trade_jobs.lock().pop()
    }

    /// Drain all spilled jobs (shutdown path).
    pub fn drain_post_trade_jobs(&self) -> Vec<PostTradeJob> {
        std::mem::take(&mut *self.post_trade_jobs.lock())
    }

    pub fn pending_post_trade_count(&self) -> usize {
        self.post_trade_jobs.lock().len()
    }

    #[cfg(test)]
    pub fn pending_event_count(&self) -> usize {
        self.events.lock().len()
    }
}

impl Default for InMemoryEventStore {
    fn default() -> Self {
        Self::new()
    }
}

/// Shared handle used across backpressure sites and the outcome drain.
pub type SharedInMemoryEventStore = Arc<InMemoryEventStore>;
