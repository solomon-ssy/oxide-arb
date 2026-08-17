//! Operator-facing venue incentive reconciliation contracts.

use chrono::{DateTime, NaiveDate, Utc};
use quant_pivot_macros::NormalizePageQuery;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::{
    domain::{pagination::PageRequest, quant::VenueIncentiveEventInfo},
    enums::fee::{VenueIncentiveKind, VenueIncentiveStage},
    types::{
        ContentHash, EvmTransactionHash, ExecutionFillId, MarketId, Usd, VenueIncentiveEventId,
    },
};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum IncentiveReconciliationHealth {
    Healthy,
    Stale,
    Incomplete,
    Unavailable,
}

/// Account-level incentive attribution and upstream scan health.
#[derive(Debug, Clone, Serialize, JsonSchema)]
pub struct IncentiveReconciliationView {
    pub as_of: DateTime<Utc>,
    pub estimated_maker_accrual_usd: Usd,
    pub venue_reported_maker_accrual_usd: Usd,
    pub wallet_credited_maker_usd: Usd,
    pub wallet_credited_taker_usd: Usd,
    pub estimate_to_reported_delta_usd: Usd,
    pub reported_to_credit_delta_usd: Usd,
    pub last_success_at: Option<DateTime<Utc>>,
    pub oldest_incomplete_date: Option<NaiveDate>,
    pub incomplete_day_count: u32,
    pub health: IncentiveReconciliationHealth,
    pub payout_threshold_usd: Usd,
    pub below_payout_threshold_program_dates: Vec<NaiveDate>,
    pub overdue_program_dates: Vec<NaiveDate>,
}

/// One immutable incentive ledger event, including zero-amount retractions.
#[derive(Debug, Clone, Serialize, JsonSchema)]
pub struct VenueIncentiveEventView {
    pub venue_incentive_event_id: VenueIncentiveEventId,
    pub execution_fill_id: Option<ExecutionFillId>,
    pub market_id: Option<MarketId>,
    pub kind: VenueIncentiveKind,
    pub stage: VenueIncentiveStage,
    pub program_date: NaiveDate,
    pub amount_usd: Usd,
    pub source_terms_hash: Option<ContentHash>,
    pub source_partition: String,
    pub source_identity: String,
    pub transaction_hash: Option<EvmTransactionHash>,
    pub observed_at: DateTime<Utc>,
    pub available_at: DateTime<Utc>,
    pub evidence_hash: ContentHash,
    pub created_at: DateTime<Utc>,
}

impl From<VenueIncentiveEventInfo> for VenueIncentiveEventView {
    fn from(info: VenueIncentiveEventInfo) -> Self {
        Self {
            venue_incentive_event_id: info.venue_incentive_event_id,
            execution_fill_id: info.execution_fill_id,
            market_id: info.market_id,
            kind: info.kind,
            stage: info.stage,
            program_date: info.program_date,
            amount_usd: info.amount_usd,
            source_terms_hash: info.source_terms_hash,
            source_partition: info.source_partition,
            source_identity: info.source_identity,
            transaction_hash: info.transaction_hash,
            observed_at: info.observed_at,
            available_at: info.available_at,
            evidence_hash: info.evidence_hash,
            created_at: info.created_at,
        }
    }
}

#[derive(Debug, Clone, Default, Deserialize, NormalizePageQuery)]
pub struct VenueIncentiveEventListQuery {
    pub kind: Option<VenueIncentiveKind>,
    pub stage: Option<VenueIncentiveStage>,
    pub program_date: Option<NaiveDate>,
    #[normalize_page]
    #[serde(flatten)]
    pub page: PageRequest,
}
