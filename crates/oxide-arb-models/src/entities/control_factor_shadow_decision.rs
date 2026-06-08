//! `control_factor_shadow_decision` table entity.

use crate::{
    enums::fact::ShadowDecisionType,
    types::{EventId, FactorPublicationId, MarketId, OpportunityId, ShadowDecisionId},
};
use chrono::{DateTime, Utc};
use sea_orm::entity::prelude::*;

#[derive(Clone, Debug, PartialEq, Eq, DeriveEntityModel)]
#[sea_orm(table_name = "control_factor_shadow_decision")]
pub struct Model {
    #[sea_orm(primary_key, auto_increment = false)]
    pub shadow_decision_id: ShadowDecisionId,
    pub publication_id: FactorPublicationId,
    pub opportunity_id: OpportunityId,
    pub event_id: EventId,
    pub market_id: MarketId,
    pub decision_type: ShadowDecisionType,
    #[sea_orm(column_type = "JsonBinary")]
    pub baseline_decision: Json,
    #[sea_orm(column_type = "JsonBinary")]
    pub shadow_decision: Json,
    #[sea_orm(column_type = "JsonBinary")]
    pub delta: Json,
    #[sea_orm(column_type = "JsonBinary")]
    pub affected_factor_ids: Json,
    pub decided_at: DateTime<Utc>,
    pub created_at: DateTime<Utc>,
}

#[derive(Copy, Clone, Debug, EnumIter, DeriveRelation)]
pub enum Relation {}

impl ActiveModelBehavior for ActiveModel {}
