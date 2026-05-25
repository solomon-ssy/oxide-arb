//! `risk_audit_events` table entity.

use crate::enums::risk::RiskAuditEventType;
use crate::types::{OpportunityId, TradeId};
use chrono::{DateTime, Utc};
use sea_orm::entity::prelude::*;

#[derive(Clone, Debug, PartialEq, Eq, DeriveEntityModel)]
#[sea_orm(table_name = "risk_audit_event")]
pub struct Model {
    #[sea_orm(primary_key)]
    pub id: i64,
    pub event_type: RiskAuditEventType,
    pub opportunity_id: Option<OpportunityId>,
    pub trade_id: Option<TradeId>,
    #[sea_orm(column_type = "JsonBinary")]
    pub payload: serde_json::Value,
    pub created_at: DateTime<Utc>,
}

#[derive(Copy, Clone, Debug, EnumIter, DeriveRelation)]
pub enum Relation {}

impl ActiveModelBehavior for ActiveModel {}
