//! Immutable raw resolution observations received from the finalized source.

use chrono::{DateTime, Utc};
use sea_orm::entity::prelude::*;

use super::quant_resolution_observation_projection;
use crate::types::{
    ArtifactUri, ContentHash, DomainInstrumentKey, DomainSourceId, EvmAddress, EvmBlockHash,
    EvmTransactionHash, EvmUint256, MarketId, PayoutRatio, ResolutionObservationId,
};

#[sea_orm::model]
#[derive(Clone, Debug, PartialEq, Eq, DeriveEntityModel)]
#[sea_orm(table_name = "quant_resolution_observation_inbox")]
pub struct Model {
    #[sea_orm(primary_key, auto_increment = false)]
    pub resolution_observation_id: ResolutionObservationId,
    pub source_checkpoint_hash: ContentHash,
    pub source_id: DomainSourceId,
    pub instrument_key: DomainInstrumentKey,
    pub market_id: MarketId,
    pub denominator: EvmUint256,
    pub yes_numerator: EvmUint256,
    pub no_numerator: EvmUint256,
    pub yes_payout_ratio: PayoutRatio,
    pub no_payout_ratio: PayoutRatio,
    pub oracle: EvmAddress,
    #[sea_orm(column_type = "Text")]
    pub question_id: String,
    pub transaction_hash: EvmTransactionHash,
    pub block_number: i64,
    pub block_hash: EvmBlockHash,
    pub log_index: i64,
    pub resolved_at: DateTime<Utc>,
    pub raw_payload_hash: ContentHash,
    pub raw_uri: ArtifactUri,
    pub provider_revision: EvmBlockHash,
    pub available_at: DateTime<Utc>,
    pub created_at: DateTime<Utc>,

    #[sea_orm(has_one)]
    pub projection: HasOne<quant_resolution_observation_projection::Entity>,
}

impl ActiveModelBehavior for ActiveModel {}
