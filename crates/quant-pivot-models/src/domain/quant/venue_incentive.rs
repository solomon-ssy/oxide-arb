//! Append-only venue-incentive lifecycle persistence contracts.

use chrono::{DateTime, NaiveDate, Utc};
use sea_orm::{DeriveIntoActiveModel, DerivePartialModel};
use serde::{Deserialize, Serialize};

use crate::{
    entities::quant_venue_incentive_event,
    enums::fee::{VenueIncentiveKind, VenueIncentiveStage},
    types::{
        ContentHash, EvmTransactionHash, ExecutionAccountId, ExecutionFillId, MarketId, Usd,
        VenueIncentiveEventId,
    },
};

#[derive(Debug, Clone, Serialize, Deserialize, DerivePartialModel)]
#[sea_orm(entity = "quant_venue_incentive_event::Entity")]
pub struct VenueIncentiveEventInfo {
    pub venue_incentive_event_id: VenueIncentiveEventId,
    pub execution_account_id: ExecutionAccountId,
    pub execution_fill_id: Option<ExecutionFillId>,
    pub market_id: Option<MarketId>,
    pub kind: VenueIncentiveKind,
    pub stage: VenueIncentiveStage,
    pub program_date: NaiveDate,
    pub amount_usd: Usd,
    pub source_schedule_hash: Option<ContentHash>,
    pub source_partition: String,
    pub source_identity: String,
    pub transaction_hash: Option<EvmTransactionHash>,
    pub observed_at: DateTime<Utc>,
    pub available_at: DateTime<Utc>,
    pub evidence_hash: ContentHash,
    pub created_at: DateTime<Utc>,
}

info_from_model!(
    VenueIncentiveEventInfo,
    quant_venue_incentive_event::Model,
    {
        venue_incentive_event_id,
        execution_account_id,
        execution_fill_id,
        market_id,
        kind,
        stage,
        program_date,
        amount_usd,
        source_schedule_hash,
        source_partition,
        source_identity,
        transaction_hash,
        observed_at,
        available_at,
        evidence_hash,
        created_at,
    }
);

#[derive(Debug, Clone, Serialize, Deserialize, DeriveIntoActiveModel)]
#[sea_orm(active_model = "quant_venue_incentive_event::ActiveModel")]
pub struct NewVenueIncentiveEvent {
    pub venue_incentive_event_id: VenueIncentiveEventId,
    pub execution_account_id: ExecutionAccountId,
    pub execution_fill_id: Option<ExecutionFillId>,
    pub market_id: Option<MarketId>,
    pub kind: VenueIncentiveKind,
    pub stage: VenueIncentiveStage,
    pub program_date: NaiveDate,
    pub amount_usd: Usd,
    pub source_schedule_hash: Option<ContentHash>,
    pub source_partition: String,
    pub source_identity: String,
    pub transaction_hash: Option<EvmTransactionHash>,
    pub observed_at: DateTime<Utc>,
    pub available_at: DateTime<Utc>,
    pub evidence_hash: ContentHash,
}

/// Cumulative, point-in-time incentive reconciliation. Estimated accrual and
/// venue award are valuation facts; only wallet credits are account cash.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct VenueIncentiveReconciliation {
    pub as_of: DateTime<Utc>,
    pub estimated_maker_accrual_usd: Usd,
    pub venue_awarded_maker_usd: Usd,
    pub wallet_credited_maker_usd: Usd,
    pub wallet_credited_taker_usd: Usd,
}

impl VenueIncentiveReconciliation {
    #[must_use]
    pub fn estimate_to_award_delta(self) -> Usd {
        self.venue_awarded_maker_usd - self.estimated_maker_accrual_usd
    }

    #[must_use]
    pub fn award_to_credit_delta(self) -> Usd {
        self.wallet_credited_maker_usd - self.venue_awarded_maker_usd
    }

    #[must_use]
    pub fn wallet_credit_total(self) -> Usd {
        self.wallet_credited_maker_usd + self.wallet_credited_taker_usd
    }
}
