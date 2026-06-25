//! Postgres-backed reserved-capital reader.

use std::collections::HashMap;

use crate::traits::ReservedCapitalRepository;
use quant_pivot_error::storage::StorageError;
use quant_pivot_models::{
    entities::{quant_order_intent, quant_recommendation},
    enums::quant::OrderIntentStatus,
    types::{RecommendationId, Usd},
};
use sea_orm::{ColumnTrait, DatabaseConnection, EntityTrait, QueryFilter};

/// Order-intent statuses that lock capital (not yet spent or terminal).
const LOCKED_STATUSES: [OrderIntentStatus; 5] = [
    OrderIntentStatus::PendingApproval,
    OrderIntentStatus::Approved,
    OrderIntentStatus::ApprovedByPolicy,
    OrderIntentStatus::Submitted,
    OrderIntentStatus::PartiallyFilled,
];

/// Postgres-backed reserved-capital reader.
pub struct PgReservedCapitalRepository {
    db: DatabaseConnection,
}

impl PgReservedCapitalRepository {
    pub const fn new(db: DatabaseConnection) -> Self {
        Self { db }
    }
}

#[async_trait::async_trait]
impl ReservedCapitalRepository for PgReservedCapitalRepository {
    async fn sum_locked_usd(&self) -> Result<Usd, StorageError> {
        let locked = quant_order_intent::Entity::find()
            .filter(quant_order_intent::Column::Status.is_in(LOCKED_STATUSES))
            .all(&self.db)
            .await
            .map_err(StorageError::from)?;
        if locked.is_empty() {
            return Ok(Usd::ZERO);
        }

        let recommendation_ids = locked
            .iter()
            .map(|intent| intent.recommendation_id.clone())
            .collect::<Vec<_>>();
        let recommendations = quant_recommendation::Entity::find()
            .filter(quant_recommendation::Column::RecommendationId.is_in(recommendation_ids))
            .all(&self.db)
            .await
            .map_err(StorageError::from)?;

        let suggested_by_id = recommendations
            .into_iter()
            .map(|row| (row.recommendation_id, row.sizing_plan.suggested_usd))
            .collect::<HashMap<RecommendationId, Usd>>();

        // Sum per locked intent so multiple intents on one recommendation each
        // reserve their suggested size.
        let total = locked
            .iter()
            .filter_map(|intent| suggested_by_id.get(&intent.recommendation_id).copied())
            .sum();
        Ok(total)
    }
}
