//! `quant_account_snapshot` table entity.

use crate::{
    enums::quant::AccountSource,
    types::{AccountPositions, AccountSnapshotId, ExposureBreakdown, Usd},
};
use chrono::{DateTime, Utc};
use sea_orm::entity::prelude::*;

#[derive(Clone, Debug, PartialEq, Eq, DeriveEntityModel)]
#[sea_orm(table_name = "quant_account_snapshot")]
pub struct Model {
    #[sea_orm(primary_key, auto_increment = false)]
    pub account_snapshot_id: AccountSnapshotId,
    pub as_of: DateTime<Utc>,
    pub source: AccountSource,
    pub venue_net_liquidation_usd: Usd,
    pub capital_base_usd: Usd,
    pub available_usd: Usd,
    pub reserved_usd: Usd,
    #[sea_orm(column_type = "JsonBinary")]
    pub positions_json: AccountPositions,
    #[sea_orm(column_type = "JsonBinary")]
    pub exposures_json: ExposureBreakdown,
    pub created_at: DateTime<Utc>,
}

#[derive(Copy, Clone, Debug, EnumIter, DeriveRelation)]
pub enum Relation {
    #[sea_orm(has_many = "super::quant_recommendation_report::Entity")]
    RecommendationReport,
    #[sea_orm(has_many = "super::quant_equity_snapshot::Entity")]
    EquitySnapshot,
}

impl Related<super::quant_recommendation_report::Entity> for Entity {
    fn to() -> RelationDef {
        Relation::RecommendationReport.def()
    }
}

impl Related<super::quant_equity_snapshot::Entity> for Entity {
    fn to() -> RelationDef {
        Relation::EquitySnapshot.def()
    }
}

impl ActiveModelBehavior for ActiveModel {}
