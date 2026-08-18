//! `quant_venue_incentive_event` append-only venue-incentive ledger entity.

use chrono::{DateTime, NaiveDate, Utc};
use sea_orm::entity::prelude::*;

use super::{quant_clob_trade_observation, quant_execution_account};
use crate::{
    enums::fee::{VenueIncentiveKind, VenueIncentiveStage},
    types::{
        ClobTradeObservationId, ContentHash, EvmTransactionHash, ExecutionAccountId, MarketId, Usd,
        VenueIncentiveEventId,
    },
};

#[sea_orm::model]
#[derive(Clone, Debug, PartialEq, Eq, DeriveEntityModel)]
#[sea_orm(table_name = "quant_venue_incentive_event")]
pub struct Model {
    #[sea_orm(primary_key, auto_increment = false)]
    pub venue_incentive_event_id: VenueIncentiveEventId,
    pub execution_account_id: ExecutionAccountId,
    pub clob_trade_observation_id: Option<ClobTradeObservationId>,
    pub market_id: Option<MarketId>,
    pub kind: VenueIncentiveKind,
    pub stage: VenueIncentiveStage,
    pub program_date: NaiveDate,
    pub amount_usd: Usd,
    pub source_terms_hash: Option<ContentHash>,
    #[sea_orm(column_type = "Text")]
    pub source_partition: String,
    #[sea_orm(column_type = "Text", unique)]
    pub source_identity: String,
    pub transaction_hash: Option<EvmTransactionHash>,
    pub observed_at: DateTime<Utc>,
    pub available_at: DateTime<Utc>,
    pub evidence_hash: ContentHash,
    pub created_at: DateTime<Utc>,

    #[sea_orm(
        belongs_to,
        relation_enum = "ExecutionAccount",
        from = "execution_account_id",
        to = "execution_account_id"
    )]
    pub execution_account: BelongsTo<quant_execution_account::Entity>,
    #[sea_orm(
        belongs_to,
        relation_enum = "ClobTradeObservation",
        from = "clob_trade_observation_id",
        to = "clob_trade_observation_id"
    )]
    pub clob_trade_observation: BelongsTo<Option<quant_clob_trade_observation::Entity>>,
}

impl ActiveModelBehavior for ActiveModel {}
