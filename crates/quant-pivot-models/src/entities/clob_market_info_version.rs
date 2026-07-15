//! Append-only `clob_market_info_version` entity.

use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use sea_orm::entity::prelude::*;

use crate::{
    enums::common::TickSize,
    types::{ClobFeeDetails, ClobMarketInfoVersionId, ClobTokenSet, ContentHash, MarketId},
};

#[derive(Clone, Debug, PartialEq, Eq, DeriveEntityModel)]
#[sea_orm(table_name = "clob_market_info_version")]
pub struct Model {
    #[sea_orm(primary_key, auto_increment = false)]
    pub version_id: ClobMarketInfoVersionId,
    pub market_id: MarketId,
    #[sea_orm(column_type = "JsonBinary")]
    pub tokens_json: ClobTokenSet,
    pub tick_size: TickSize,
    pub minimum_order_size: Decimal,
    pub neg_risk: bool,
    pub taker_order_delay_enabled: bool,
    pub minimum_order_age_secs: Option<i64>,
    pub blockaid_check_enabled: bool,
    #[sea_orm(column_type = "JsonBinary")]
    pub fee_details_json: ClobFeeDetails,
    pub builder_maker_fee_rate_bps: i32,
    pub builder_taker_fee_rate_bps: i32,
    pub effective_at: DateTime<Utc>,
    pub available_at: DateTime<Utc>,
    pub payload_hash: ContentHash,
    #[sea_orm(column_type = "JsonBinary")]
    pub raw_payload: Json,
    pub created_at: DateTime<Utc>,
}

#[derive(Copy, Clone, Debug, EnumIter, DeriveRelation)]
pub enum Relation {
    #[sea_orm(
        belongs_to = "super::market::Entity",
        from = "Column::MarketId",
        to = "super::market::Column::MarketId"
    )]
    Market,
}

impl Related<super::market::Entity> for Entity {
    fn to() -> RelationDef {
        Relation::Market.def()
    }
}

impl ActiveModelBehavior for ActiveModel {}
