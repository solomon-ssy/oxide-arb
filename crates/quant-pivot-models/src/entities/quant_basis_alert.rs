//! `quant_basis_alert` table entity.

use crate::types::{BasisAlertId, Bps, MarketId};
use chrono::{DateTime, Utc};
use sea_orm::entity::prelude::*;

#[sea_orm::model]
#[derive(Clone, Debug, PartialEq, Eq, DeriveEntityModel)]
#[sea_orm(table_name = "quant_basis_alert")]
pub struct Model {
    #[sea_orm(primary_key, auto_increment = false)]
    pub alert_id: BasisAlertId,
    pub market_id: MarketId,
    pub instrument_key: String,
    pub oracle_instrument_key: String,
    pub basis_bps: Bps,
    pub threshold_bps: Bps,
    pub as_of: DateTime<Utc>,
    pub acknowledged: bool,
    pub acknowledged_at: Option<DateTime<Utc>>,
    pub acknowledged_by: Option<String>,
    pub created_at: DateTime<Utc>,

    #[sea_orm(
        belongs_to,
        relation_enum = "Market",
        from = "market_id",
        to = "market_id"
    )]
    pub market: BelongsTo<super::market::Entity>,
}

impl ActiveModelBehavior for ActiveModel {}
