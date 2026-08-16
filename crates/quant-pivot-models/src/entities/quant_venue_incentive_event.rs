//! `quant_venue_incentive_event` append-only venue-incentive ledger entity.

use chrono::{DateTime, NaiveDate, Utc};
use sea_orm::entity::prelude::*;

use super::{quant_execution_account, quant_execution_fill};
use crate::{
    enums::fee::{VenueIncentiveKind, VenueIncentiveStage},
    types::{
        ContentHash, EvmTransactionHash, ExecutionAccountId, ExecutionFillId, MarketId, Usd,
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
    pub execution_fill_id: Option<ExecutionFillId>,
    pub market_id: Option<MarketId>,
    pub kind: VenueIncentiveKind,
    pub stage: VenueIncentiveStage,
    pub program_date: NaiveDate,
    pub amount_usd: Usd,
    pub source_schedule_hash: Option<ContentHash>,
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
        relation_enum = "ExecutionFill",
        from = "execution_fill_id",
        to = "execution_fill_id"
    )]
    pub execution_fill: BelongsTo<Option<quant_execution_fill::Entity>>,
}

impl ActiveModelBehavior for ActiveModel {}
