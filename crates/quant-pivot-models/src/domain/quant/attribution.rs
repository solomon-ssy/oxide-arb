//! Recommendation attribution persistence DTOs.

use crate::{
    entities::quant_recommendation_attribution,
    enums::quant::RecommendationAttributionOutcome,
    types::{AttributionDetail, EntryOutcome, ExitOutcome, RecommendationId, Usd},
};
use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use sea_orm::{DeriveIntoActiveModel, DerivePartialModel, FromQueryResult};
use serde::{Deserialize, Serialize};

/// Recommendation outcome and label feedback row.
#[derive(Debug, Clone, Serialize, Deserialize, DerivePartialModel, FromQueryResult)]
#[sea_orm(entity = "crate::entities::quant_recommendation_attribution::Entity")]
pub struct RecommendationAttributionInfo {
    pub recommendation_id: RecommendationId,
    pub outcome: RecommendationAttributionOutcome,
    pub entry_outcome_json: EntryOutcome,
    pub exit_outcome_json: ExitOutcome,
    pub realized_pnl_usd: Option<Usd>,
    pub max_adverse_excursion_bps: Option<Decimal>,
    pub max_favorable_excursion_bps: Option<Decimal>,
    pub label_available_at: Option<DateTime<Utc>>,
    pub attribution_json: AttributionDetail,
    pub created_at: DateTime<Utc>,
}

info_from_model!(
    RecommendationAttributionInfo,
    quant_recommendation_attribution::Model,
    {
        recommendation_id,
        outcome,
        entry_outcome_json,
        exit_outcome_json,
        realized_pnl_usd,
        max_adverse_excursion_bps,
        max_favorable_excursion_bps,
        label_available_at,
        attribution_json,
        created_at,
    }
);

/// Insert payload for `quant_recommendation_attribution`.
#[derive(Debug, Clone, Serialize, Deserialize, DeriveIntoActiveModel)]
#[sea_orm(active_model = "crate::entities::quant_recommendation_attribution::ActiveModel")]
pub struct NewRecommendationAttribution {
    pub recommendation_id: RecommendationId,
    pub outcome: RecommendationAttributionOutcome,
    pub entry_outcome_json: EntryOutcome,
    pub exit_outcome_json: ExitOutcome,
    pub realized_pnl_usd: Option<Usd>,
    pub max_adverse_excursion_bps: Option<Decimal>,
    pub max_favorable_excursion_bps: Option<Decimal>,
    pub label_available_at: Option<DateTime<Utc>>,
    pub attribution_json: AttributionDetail,
}

/// Result of an idempotent final attribution write.
#[derive(Debug, Clone)]
pub enum InsertFinalOutcome {
    /// A new WORM attribution row was inserted and the recommendation advanced.
    Written(Box<RecommendationAttributionInfo>),
    /// Another writer already persisted the final row for this recommendation.
    AlreadyExists,
}
