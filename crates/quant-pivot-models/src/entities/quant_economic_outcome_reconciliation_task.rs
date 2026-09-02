//! Durable lease/retry state for recommendation economic-horizon reconciliation.

use chrono::{DateTime, Utc};
use sea_orm::entity::prelude::*;

use super::quant_recommendation;
use crate::{
    enums::quant::OutcomeReconciliationTaskStatus,
    types::{ContentHash, RecommendationId, WorkerId},
};

#[sea_orm::model]
#[derive(Clone, Debug, PartialEq, Eq, DeriveEntityModel)]
#[sea_orm(table_name = "quant_economic_outcome_reconciliation_task")]
pub struct Model {
    #[sea_orm(primary_key, auto_increment = false)]
    pub recommendation_id: RecommendationId,
    pub horizon_at: DateTime<Utc>,
    pub replay_until: Option<DateTime<Utc>>,
    #[sea_orm(column_type = "Text", nullable)]
    pub resolution_outcome_hash: Option<ContentHash>,
    pub source_cutoff_at: Option<DateTime<Utc>>,
    pub status: OutcomeReconciliationTaskStatus,
    pub attempt_count: i32,
    pub claim_owner: Option<WorkerId>,
    pub lease_expires_at: Option<DateTime<Utc>>,
    pub next_attempt_at: Option<DateTime<Utc>>,
    #[sea_orm(column_type = "Text", nullable)]
    pub last_error: Option<String>,
    pub completed_at: Option<DateTime<Utc>>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,

    #[sea_orm(
        belongs_to,
        relation_enum = "Recommendation",
        from = "recommendation_id",
        to = "recommendation_id"
    )]
    pub recommendation: BelongsTo<quant_recommendation::Entity>,
}

impl ActiveModelBehavior for ActiveModel {}
