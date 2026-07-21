//! Capital-allocation ledger persistence DTOs.

use chrono::{DateTime, Utc};
use sea_orm::{DeriveIntoActiveModel, DerivePartialModel};
use serde::{Deserialize, Serialize};

use crate::{
    entities::quant_capital_allocation,
    enums::execution::CapitalAllocationState,
    types::{CapitalAllocationId, OrderIntentId, RecommendationId, Usd},
};

/// Persisted intent-level capital allocation state.
#[derive(Debug, Clone, Serialize, Deserialize, DerivePartialModel)]
#[sea_orm(entity = "crate::entities::quant_capital_allocation::Entity")]
pub struct CapitalAllocationInfo {
    pub capital_allocation_id: CapitalAllocationId,
    pub order_intent_id: OrderIntentId,
    pub recommendation_id: RecommendationId,
    pub state: CapitalAllocationState,
    pub planned_usd: Usd,
    pub allocated_usd: Usd,
    pub locked_usd: Usd,
    pub spent_usd: Usd,
    pub released_usd: Usd,
    pub reason: String,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

info_from_model!(
    CapitalAllocationInfo,
    quant_capital_allocation::Model,
    {
        capital_allocation_id,
        order_intent_id,
        recommendation_id,
        state,
        planned_usd,
        allocated_usd,
        locked_usd,
        spent_usd,
        released_usd,
        reason,
        created_at,
        updated_at,
    }
);

/// Insert payload for `quant_capital_allocation`.
#[derive(Debug, Clone, Serialize, Deserialize, DeriveIntoActiveModel)]
#[sea_orm(active_model = "crate::entities::quant_capital_allocation::ActiveModel")]
pub struct NewCapitalAllocation {
    pub capital_allocation_id: CapitalAllocationId,
    pub order_intent_id: OrderIntentId,
    pub recommendation_id: RecommendationId,
    pub state: CapitalAllocationState,
    pub planned_usd: Usd,
    pub allocated_usd: Usd,
    pub locked_usd: Usd,
    pub spent_usd: Usd,
    pub released_usd: Usd,
    pub reason: String,
}

/// Complete state-machine write intent for a capital-allocation transition.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CapitalAllocationPatch {
    pub state: CapitalAllocationState,
    pub allocated_usd: Usd,
    pub locked_usd: Usd,
    pub spent_usd: Usd,
    pub released_usd: Usd,
    pub reason: String,
}
