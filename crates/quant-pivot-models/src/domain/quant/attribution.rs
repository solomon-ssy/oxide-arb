//! Recommendation attribution persistence DTOs.

use crate::{
    enums::quant::RecommendationOutcome,
    types::{RecommendationId, Usd},
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
    pub outcome: RecommendationOutcome,
    pub entry_outcome_json: serde_json::Value,
    pub exit_outcome_json: serde_json::Value,
    pub realized_pnl_usd: Option<Usd>,
    pub max_adverse_excursion_bps: Option<Decimal>,
    pub max_favorable_excursion_bps: Option<Decimal>,
    pub label_available_at: Option<DateTime<Utc>>,
    pub attribution_json: serde_json::Value,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

info_from_model!(
    RecommendationAttributionInfo,
    crate::entities::quant_recommendation_attribution::Model,
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
        updated_at,
    }
);

/// Insert payload for `quant_recommendation_attribution`.
#[derive(Debug, Clone, Serialize, Deserialize, DeriveIntoActiveModel)]
#[sea_orm(active_model = "crate::entities::quant_recommendation_attribution::ActiveModel")]
pub struct NewRecommendationAttribution {
    pub recommendation_id: RecommendationId,
    pub outcome: RecommendationOutcome,
    pub entry_outcome_json: serde_json::Value,
    pub exit_outcome_json: serde_json::Value,
    pub realized_pnl_usd: Option<Usd>,
    pub max_adverse_excursion_bps: Option<Decimal>,
    pub max_favorable_excursion_bps: Option<Decimal>,
    pub label_available_at: Option<DateTime<Utc>>,
    pub attribution_json: serde_json::Value,
}

/// Runtime attribution result before persistence.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RecommendationAttributionModel {
    pub attribution: NewRecommendationAttribution,
}
