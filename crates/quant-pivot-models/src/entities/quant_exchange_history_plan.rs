//! `quant_exchange_history_plan` immutable fresh-boot block plan.

use chrono::{DateTime, Utc};
use sea_orm::entity::prelude::*;

use crate::types::{ContentHash, EvmBlockHash};

#[sea_orm::model]
#[derive(Clone, Debug, PartialEq, Eq, DeriveEntityModel)]
#[sea_orm(table_name = "quant_exchange_history_plan")]
pub struct Model {
    #[sea_orm(primary_key, auto_increment = false)]
    pub plan_id: Uuid,
    #[sea_orm(unique)]
    pub chain_id: i64,
    pub policy_hash: ContentHash,
    pub bootstrap_profile_set_hash: ContentHash,
    pub finalized_anchor_block: i64,
    pub finalized_anchor_hash: EvmBlockHash,
    pub finalized_anchor_timestamp: i64,
    pub activation_from_block: i64,
    pub activation_through_block: i64,
    pub crypto_required_from_block: i64,
    pub weather_required_from_block: i64,
    pub retention_from_block: i64,
    pub retention_through_block: i64,
    pub created_at: DateTime<Utc>,
}

impl ActiveModelBehavior for ActiveModel {}
