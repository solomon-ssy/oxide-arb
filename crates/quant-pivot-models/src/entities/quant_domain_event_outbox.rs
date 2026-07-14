//! Durable Postgres outbox for `ClickHouse` domain events.

use chrono::{DateTime, Utc};
use sea_orm::entity::prelude::*;
use uuid::Uuid;

use crate::{domain::DomainEventEnvelope, types::DomainEventId};

#[derive(Clone, Debug, PartialEq, Eq, DeriveEntityModel)]
#[sea_orm(table_name = "quant_domain_event_outbox")]
pub struct Model {
    #[sea_orm(primary_key, auto_increment = false)]
    pub event_id: DomainEventId,
    #[sea_orm(column_type = "JsonBinary")]
    pub envelope_json: DomainEventEnvelope,
    pub published_at: Option<DateTime<Utc>>,
    pub publish_attempts: i32,
    pub claim_owner: Option<Uuid>,
    pub lease_expires_at: Option<DateTime<Utc>>,
    pub last_error: Option<String>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Copy, Clone, Debug, EnumIter, DeriveRelation)]
pub enum Relation {}

impl ActiveModelBehavior for ActiveModel {}
