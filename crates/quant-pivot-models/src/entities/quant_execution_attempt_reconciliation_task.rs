//! Durable retry and lease state for execution-attempt reconciliation.

use chrono::{DateTime, Utc};
use sea_orm::entity::prelude::*;

use super::quant_order_intent;
use crate::{
    enums::quant::OutcomeReconciliationTaskStatus,
    types::{OrderIntentId, WorkerId},
};

#[sea_orm::model]
#[derive(Clone, Debug, PartialEq, Eq, DeriveEntityModel)]
#[sea_orm(table_name = "quant_execution_attempt_reconciliation_task")]
pub struct Model {
    #[sea_orm(primary_key, auto_increment = false)]
    pub order_intent_id: OrderIntentId,
    pub ready_at: DateTime<Utc>,
    pub status: OutcomeReconciliationTaskStatus,
    pub attempt_count: i32,
    pub claim_owner: Option<WorkerId>,
    pub lease_expires_at: Option<DateTime<Utc>>,
    pub next_attempt_at: Option<DateTime<Utc>>,
    #[sea_orm(column_type = "Text", nullable)]
    pub last_error: Option<String>,
    pub completed_at: Option<DateTime<Utc>>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,

    #[sea_orm(
        belongs_to,
        relation_enum = "OrderIntent",
        from = "order_intent_id",
        to = "order_intent_id"
    )]
    pub order_intent: BelongsTo<quant_order_intent::Entity>,
}

impl ActiveModelBehavior for ActiveModel {}
