//! Postgres-backed shadow-comparison ledger repository (append-only).

use crate::{postgres::primitives, traits::ShadowComparisonRepository};
use chrono::{DateTime, Utc};
use quant_pivot_error::storage::StorageError;
use quant_pivot_models::{
    domain::{NewShadowComparison, ShadowComparisonInfo, ShadowStabilitySummary},
    entities::quant_shadow_comparison,
    enums::quant::ModelWeightSource,
    types::{ModelVersionId, Probability},
};
use rust_decimal::Decimal;
use sea_orm::{
    ColumnTrait, DatabaseConnection, EntityTrait, ExprTrait, FromQueryResult, IntoActiveModel,
    QueryFilter, QuerySelect, sea_query::Expr,
};
use std::cmp;

/// Postgres-backed shadow-comparison ledger repository.
pub struct PgShadowComparisonRepository {
    db: DatabaseConnection,
}

impl PgShadowComparisonRepository {
    pub const fn new(db: DatabaseConnection) -> Self {
        Self { db }
    }
}

#[derive(Debug, FromQueryResult)]
struct ShadowSummaryRow {
    sample_count: i64,
    window_start: Option<DateTime<Utc>>,
    window_end: Option<DateTime<Utc>>,
    mean_topn_overlap: Option<Decimal>,
    any_hard_divergence: Option<bool>,
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
        let row = quant_shadow_comparison::Entity::find()
            .filter(
                quant_shadow_comparison::Column::ShadowModelVersionId
                    .eq(shadow_model_version_id.clone()),
            )
            .filter(quant_shadow_comparison::Column::WeightSource.eq(ModelWeightSource::Artifact))
            .filter(quant_shadow_comparison::Column::DecisionAt.gte(since))
            .select_only()
            .column_as(
                Expr::col(quant_shadow_comparison::Column::ShadowComparisonId).count(),
                "sample_count",
            )
            .column_as(
                Expr::col(quant_shadow_comparison::Column::DecisionAt).min(),
                "window_start",
            )
            .column_as(
                Expr::col(quant_shadow_comparison::Column::DecisionAt).max(),
                "window_end",
            )
            .column_as(
                Expr::col(quant_shadow_comparison::Column::TopnOverlap).avg(),
                "mean_topn_overlap",
            )
            .column_as(
                primitives::bool_or(quant_shadow_comparison::Column::HardDivergence),
                "any_hard_divergence",
            )
            .into_model::<ShadowSummaryRow>()
            .one(&self.db)
            .await
            .map_err(StorageError::from)?;

        let Some(row) = row else {
            return Ok(ShadowStabilitySummary {
                shadow_model_version_id: shadow_model_version_id.clone(),
                sample_count: 0,
                window_start: None,
                window_end: None,
                mean_topn_overlap: Probability::new(Decimal::ZERO),
                any_hard_divergence: false,
            });
        };

        let sample_count = u64::try_from(cmp::max(row.sample_count, 0)).unwrap_or(u64::MAX);
        Ok(ShadowStabilitySummary {
            shadow_model_version_id: shadow_model_version_id.clone(),
            sample_count,
            window_start: row.window_start,
            window_end: row.window_end,
            mean_topn_overlap: Probability::new(row.mean_topn_overlap.unwrap_or(Decimal::ZERO)),
            any_hard_divergence: row.any_hard_divergence.unwrap_or(false),
        })
    }
}
