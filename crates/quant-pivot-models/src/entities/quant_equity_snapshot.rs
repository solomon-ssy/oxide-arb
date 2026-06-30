//! `quant_equity_snapshot` table entity.

use crate::{
    enums::quant::AccountSource,
    types::{AccountSnapshotId, EquitySnapshotId, Usd},
};
use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use sea_orm::entity::prelude::*;

#[derive(Clone, Debug, PartialEq, Eq, DeriveEntityModel)]
#[sea_orm(table_name = "quant_equity_snapshot")]
pub struct Model {
    #[sea_orm(primary_key, auto_increment = false)]
    pub equity_snapshot_id: EquitySnapshotId,
    pub as_of: DateTime<Utc>,
    pub source: AccountSource,
    pub venue_net_liquidation_usd: Usd,
    pub capital_base_usd: Usd,
    pub available_usd: Usd,
    pub reserved_usd: Usd,
    pub realized_pnl_cumulative_usd: Usd,
    pub unrealized_pnl_usd: Usd,
    pub high_water_mark_usd: Usd,
    pub drawdown_pct: Decimal,
    pub account_snapshot_ref: Option<AccountSnapshotId>,
    pub created_at: DateTime<Utc>,
}

#[derive(Copy, Clone, Debug, EnumIter, DeriveRelation)]
pub enum Relation {
    #[sea_orm(
        belongs_to = "super::quant_account_snapshot::Entity",
        from = "Column::AccountSnapshotRef",
        to = "super::quant_account_snapshot::Column::AccountSnapshotId"
    )]
    AccountSnapshot,
    #[sea_orm(has_many = "super::quant_recommendation_report::Entity")]
    RecommendationReport,
}

impl Related<super::quant_account_snapshot::Entity> for Entity {
    fn to() -> RelationDef {
        Relation::AccountSnapshot.def()
    }
}

impl Related<super::quant_recommendation_report::Entity> for Entity {
    fn to() -> RelationDef {
        Relation::RecommendationReport.def()
    }
}

impl ActiveModelBehavior for ActiveModel {}
