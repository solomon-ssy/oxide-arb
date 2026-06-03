//! `token_balance_snapshot` table entity.

use crate::{
    enums::{common::Side, fact::BalanceSnapshotSource},
    types::{MarketId, Shares, TokenBalanceSnapshotId, TokenId},
};
use chrono::{DateTime, Utc};
use sea_orm::entity::prelude::*;

#[derive(Clone, Debug, PartialEq, Eq, DeriveEntityModel)]
#[sea_orm(table_name = "token_balance_snapshot")]
pub struct Model {
    #[sea_orm(primary_key, auto_increment = false)]
    pub token_balance_snapshot_id: TokenBalanceSnapshotId,
    #[sea_orm(column_type = "Text")]
    pub holder_address: String,
    pub market_id: MarketId,
    pub token_id: TokenId,
    pub side: Side,
    pub internal_shares: Shares,
    pub external_shares: Option<Shares>,
    pub drift_shares: Option<Shares>,
    pub source: BalanceSnapshotSource,
    pub block_number: Option<i64>,
    pub reconciliation_report_id: Option<i64>,
    pub observed_at: DateTime<Utc>,
    pub created_at: DateTime<Utc>,
}

#[derive(Copy, Clone, Debug, EnumIter, DeriveRelation)]
pub enum Relation {}

impl ActiveModelBehavior for ActiveModel {}
