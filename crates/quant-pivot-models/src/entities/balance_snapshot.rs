//! `balance_snapshot` table entity.

use crate::{
    enums::fact::BalanceSnapshotSource,
    types::{BalanceSnapshotId, Usd},
};
use chrono::{DateTime, Utc};
use sea_orm::entity::prelude::*;

#[derive(Clone, Debug, PartialEq, Eq, DeriveEntityModel)]
#[sea_orm(table_name = "balance_snapshot")]
pub struct Model {
    #[sea_orm(primary_key, auto_increment = false)]
    pub balance_snapshot_id: BalanceSnapshotId,
    #[sea_orm(column_type = "Text")]
    pub holder_address: String,
    pub internal_available_usd: Usd,
    pub internal_reserved_usd: Usd,
    pub external_available_usd: Usd,
    pub external_locked_usd: Usd,
    pub drift_usd: Usd,
    pub source: BalanceSnapshotSource,
    pub block_number: Option<i64>,
    pub reconciliation_report_id: Option<i64>,
    pub observed_at: DateTime<Utc>,
    pub created_at: DateTime<Utc>,
}

#[derive(Copy, Clone, Debug, EnumIter, DeriveRelation)]
pub enum Relation {}

impl ActiveModelBehavior for ActiveModel {}
