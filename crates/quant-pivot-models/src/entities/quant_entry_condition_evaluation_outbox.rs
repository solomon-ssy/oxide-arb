//! Durable outbox for authoritative and observed entry-condition evaluations.

use chrono::{DateTime, Utc};
use sea_orm::entity::prelude::*;

use crate::{
    clickhouse::EntryConditionEvaluationEventRow,
    types::{ContentHash, EntryConditionEvaluationOutboxId, WorkerId},
};

#[sea_orm::model]
#[derive(Clone, Debug, PartialEq, Eq, DeriveEntityModel)]
#[sea_orm(table_name = "quant_entry_condition_evaluation_outbox")]
pub struct Model {
    #[sea_orm(primary_key, auto_increment = false)]
    pub outbox_id: EntryConditionEvaluationOutboxId,
    pub evaluation_id: ContentHash,
    #[sea_orm(column_type = "JsonBinary")]
    pub event_json: EntryConditionEvaluationEventRow,
    pub published_at: Option<DateTime<Utc>>,
    pub publish_attempts: i32,
    pub claim_owner: Option<WorkerId>,
    pub lease_expires_at: Option<DateTime<Utc>>,
    pub last_error: Option<String>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

impl ActiveModelBehavior for ActiveModel {}
