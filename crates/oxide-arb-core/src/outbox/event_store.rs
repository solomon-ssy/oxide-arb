use chrono::Utc;
use oxide_arb_error::OxideError;
use oxide_arb_models::{
    domain::outbox::{NewOutboxEventWithId, OutboxEventInfo, UpdateOutboxEvent},
    enums::outbox::{OutboxAggregateType, OutboxEventType},
    types::{AggregateId, OutboxEventId},
};
use oxide_arb_repository::{postgres::PgOutboxRepository, traits::OutboxRepository};
use std::sync::Arc;

#[async_trait::async_trait]
pub trait EventStore: Send + Sync + 'static {
    async fn append(
        &self,
        aggregate_type: OutboxAggregateType,
        aggregate_id: AggregateId,
        event_type: OutboxEventType,
        payload: &serde_json::Value,
    ) -> Result<OutboxEventInfo, OxideError>;

    async fn fetch_pending(&self, limit: usize) -> Result<Vec<OutboxEventInfo>, OxideError>;

    async fn mark_published(&self, event_id: &OutboxEventId) -> Result<(), OxideError>;

    async fn record_failure(&self, event: &OutboxEventInfo, reason: &str)
    -> Result<(), OxideError>;

    async fn mark_dead_letter(
        &self,
        event_id: &OutboxEventId,
        reason: &str,
    ) -> Result<(), OxideError>;

    async fn dead_letter_count(&self) -> Result<u64, OxideError>;
}

pub struct PgEventStore {
    repo: Arc<PgOutboxRepository>,
}

impl PgEventStore {
    pub const fn new(repo: Arc<PgOutboxRepository>) -> Self {
        Self { repo }
    }
}

#[async_trait::async_trait]
impl EventStore for PgEventStore {
    async fn append(
        &self,
        aggregate_type: OutboxAggregateType,
        aggregate_id: AggregateId,
        event_type: OutboxEventType,
        payload: &serde_json::Value,
    ) -> Result<OutboxEventInfo, OxideError> {
        let event = NewOutboxEventWithId {
            event_id: OutboxEventId::generate(),
            aggregate_type,
            aggregate_id,
            event_type,
            payload: payload.clone(),
        };
        Ok(self.repo.create(event).await?)
    }

    async fn fetch_pending(&self, limit: usize) -> Result<Vec<OutboxEventInfo>, OxideError> {
        Ok(self.repo.fetch_pending(limit).await?)
    }

    async fn mark_published(&self, event_id: &OutboxEventId) -> Result<(), OxideError> {
        Ok(self
            .repo
            .update(
                event_id,
                UpdateOutboxEvent {
                    published_at: Some(Utc::now()),
                    ..Default::default()
                },
            )
            .await?)
    }

    async fn record_failure(
        &self,
        event: &OutboxEventInfo,
        reason: &str,
    ) -> Result<(), OxideError> {
        Ok(self
            .repo
            .update(
                &event.event_id,
                UpdateOutboxEvent {
                    publish_attempts: Some(event.publish_attempts.saturating_add(1)),
                    last_error: Some(Some(reason.to_owned())),
                    ..Default::default()
                },
            )
            .await?)
    }

    async fn mark_dead_letter(
        &self,
        event_id: &OutboxEventId,
        reason: &str,
    ) -> Result<(), OxideError> {
        Ok(self
            .repo
            .update(
                event_id,
                UpdateOutboxEvent {
                    dead_letter_reason: Some(Some(reason.to_owned())),
                    ..Default::default()
                },
            )
            .await?)
    }

    async fn dead_letter_count(&self) -> Result<u64, OxideError> {
        Ok(self.repo.dead_letter_count().await?)
    }
}
