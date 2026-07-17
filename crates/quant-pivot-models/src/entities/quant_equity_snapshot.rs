//! `quant_equity_snapshot` table entity.

use crate::{
    enums::quant::AccountSource,
    types::{AccountSnapshotId, EquitySnapshotId, Usd},
};
use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use sea_orm::entity::prelude::*;

#[sea_orm::model]
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

    #[sea_orm(
        belongs_to,
        relation_enum = "AccountSnapshot",
        from = "account_snapshot_ref",
        to = "account_snapshot_id"
    )]
    pub account_snapshot: BelongsTo<Option<super::quant_account_snapshot::Entity>>,
    #[sea_orm(has_many, relation_enum = "RecommendationReport")]
    pub recommendation_report: HasMany<super::quant_recommendation_report::Entity>,
}

impl ActiveModelBehavior for ActiveModel {}
