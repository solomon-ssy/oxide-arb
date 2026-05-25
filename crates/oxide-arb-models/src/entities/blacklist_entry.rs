//! `blacklist_entries` table entity.

use crate::enums::risk::{BlacklistReason, BlacklistScope};
use crate::types::{MarketId, TokenId};
use chrono::{DateTime, Utc};
use sea_orm::entity::prelude::*;

#[derive(Clone, Debug, PartialEq, Eq, DeriveEntityModel)]
#[sea_orm(table_name = "blacklist_entry")]
pub struct Model {
    #[sea_orm(primary_key, auto_increment = false)]
    pub market_id: MarketId,
    pub token_id: Option<TokenId>,
    pub scope: BlacklistScope,
    pub reason: BlacklistReason,
    pub expires_at: Option<DateTime<Utc>>,
    pub miss_count: i32,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Copy, Clone, Debug, EnumIter, DeriveRelation)]
pub enum Relation {}

impl ActiveModelBehavior for ActiveModel {}
