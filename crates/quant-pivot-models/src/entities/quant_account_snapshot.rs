//! `quant_account_snapshot` table entity.

use chrono::{DateTime, Utc};
use sea_orm::entity::prelude::*;

use super::{quant_equity_snapshot, quant_recommendation_report};
use crate::{
    enums::quant::AccountSource,
    types::{AccountPositions, AccountSnapshotId, ExposureBreakdown, Usd},
};

#[sea_orm::model]
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

    #[sea_orm(has_many, relation_enum = "RecommendationReport")]
    pub recommendation_report: HasMany<quant_recommendation_report::Entity>,
    #[sea_orm(has_many, relation_enum = "EquitySnapshot")]
    pub equity_snapshot: HasMany<quant_equity_snapshot::Entity>,
}

impl ActiveModelBehavior for ActiveModel {}
