//! Outbox event domain DTOs.

use crate::{
    enums::outbox::{OutboxAggregateType, OutboxEventType},
    types::{AggregateId, OutboxEventId},
};
use chrono::{DateTime, Utc};
use sea_orm::{DeriveIntoActiveModel, DerivePartialModel, FromQueryResult};
use serde::{Deserialize, Serialize};

// ── Read ──────────────────────────────────────────────────────────────

/// DB row projection for outbox events.
#[derive(Debug, Clone, Serialize, Deserialize, DerivePartialModel, FromQueryResult)]
#[sea_orm(entity = "crate::entities::outbox_event::Entity")]
pub struct OutboxEventInfo {
    pub event_id: OutboxEventId,
    pub aggregate_type: OutboxAggregateType,
    pub aggregate_id: AggregateId,
    pub event_type: OutboxEventType,
    pub payload: serde_json::Value,
    pub publish_attempts: i32,
    pub published_at: Option<DateTime<Utc>>,
    pub last_error: Option<String>,
    pub dead_letter_reason: Option<String>,
    pub created_at: DateTime<Utc>,
}

info_from_model!(OutboxEventInfo, crate::entities::outbox_event::Model, {
    event_id, aggregate_type, aggregate_id, event_type, payload,
    publish_attempts, published_at, last_error, dead_letter_reason,
    created_at,
});

// ── Write ─────────────────────────────────────────────────────────────

/// Fields required to create a new outbox event with a caller-assigned ID.
#[derive(Debug, Clone, DeriveIntoActiveModel)]
#[sea_orm(active_model = "super::super::entities::outbox_event::ActiveModel")]
pub struct NewOutboxEventWithId {
    pub event_id: OutboxEventId,
    pub aggregate_type: OutboxAggregateType,
    pub aggregate_id: AggregateId,
    pub event_type: OutboxEventType,
    pub payload: serde_json::Value,
}

/// Partial update for outbox events (publish status changes).
#[derive(Debug, Clone, Default)]
pub struct UpdateOutboxEvent {
    pub published_at: Option<DateTime<Utc>>,
    pub publish_attempts: Option<i32>,
    pub last_error: Option<Option<String>>,
    pub dead_letter_reason: Option<Option<String>>,
}
