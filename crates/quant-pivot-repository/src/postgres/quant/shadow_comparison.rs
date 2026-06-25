//! Postgres-backed shadow-comparison ledger repository (append-only).

use crate::traits::ShadowComparisonRepository;
use chrono::{DateTime, Utc};
use quant_pivot_error::storage::StorageError;
use quant_pivot_models::{
    domain::{NewShadowComparison, ShadowComparisonInfo, ShadowStabilitySummary},
    entities::quant_shadow_comparison,
    types::{ModelVersionId, Probability},
};
use rust_decimal::Decimal;
use sea_orm::{
    ColumnTrait, DatabaseConnection, EntityTrait, IntoActiveModel, QueryFilter, QueryOrder,
};

/// Postgres-backed shadow-comparison ledger repository.
pub struct PgShadowComparisonRepository {
    db: DatabaseConnection,
}

impl PgShadowComparisonRepository {
    pub const fn new(db: DatabaseConnection) -> Self {
        Self { db }
    }
}

#[async_trait::async_trait]
impl ShadowComparisonRepository for PgShadowComparisonRepository {
    async fn create(
        &self,
        comparison: NewShadowComparison,
    ) -> Result<ShadowComparisonInfo, StorageError> {
        quant_shadow_comparison::Entity::insert(comparison.into_active_model())
            .exec_with_returning(&self.db)
            .await
            .map_err(StorageError::from)
            .map(Into::into)
    }

    async fn summary(
        &self,
        shadow_model_version_id: &ModelVersionId,
        since: DateTime<Utc>,
    ) -> Result<ShadowStabilitySummary, StorageError> {
        let rows = quant_shadow_comparison::Entity::find()
            .filter(
                quant_shadow_comparison::Column::ShadowModelVersionId
                    .eq(shadow_model_version_id.clone()),
            )
            .filter(quant_shadow_comparison::Column::AsOf.gte(since))
            .order_by_asc(quant_shadow_comparison::Column::AsOf)
            .all(&self.db)
            .await
            .map_err(StorageError::from)?;

        let sample_count = u64::try_from(rows.len()).unwrap_or(u64::MAX);
        let window_start = rows.first().map(|row| row.as_of);
        let window_end = rows.last().map(|row| row.as_of);
        let any_hard_divergence = rows.iter().any(|row| row.hard_divergence);
        let mean_topn_overlap = if rows.is_empty() {
            Probability::new(Decimal::ZERO)
        } else {
            let sum: Decimal = rows.iter().map(|row| row.topn_overlap.inner()).sum();
            Probability::new(sum / Decimal::from(rows.len() as u64))
        };

        Ok(ShadowStabilitySummary {
            shadow_model_version_id: shadow_model_version_id.clone(),
            sample_count,
            window_start,
            window_end,
            mean_topn_overlap,
            any_hard_divergence,
        })
    }
}
