//! Postgres-backed shadow-comparison ledger repository (append-only).

use std::cmp;

use chrono::{DateTime, Utc};
use quant_pivot_error::storage::StorageError;
use quant_pivot_models::{
    domain::quant::{
        NewShadowComparison, ShadowComparisonInfo, ShadowObservationQuery, ShadowObservationWindow,
        ShadowStabilitySummary,
    },
    entities::quant_shadow_comparison::{Column, Entity},
    enums::quant::ModelWeightSource,
    types::{ModelVersionId, Probability},
};
use rust_decimal::Decimal;
use sea_orm::{
    ColumnTrait, DatabaseConnection, EntityTrait, ExprTrait, FromQueryResult, IntoActiveModel,
    QueryFilter, QuerySelect, TryInsertResult,
    sea_query::{Expr, OnConflict},
};

use crate::{
    postgres::primitives,
    traits::{ShadowComparisonRepository, ShadowComparisonWriteOutcome},
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

#[derive(Debug, FromQueryResult)]
struct ShadowSummaryRow {
    sample_count: i64,
    window_start: Option<DateTime<Utc>>,
    window_end: Option<DateTime<Utc>>,
    mean_topn_decision_overlap: Option<Decimal>,
    any_hard_divergence: Option<bool>,
}

#[async_trait::async_trait]
impl ShadowComparisonRepository for PgShadowComparisonRepository {
    async fn create(
        &self,
        comparison: NewShadowComparison,
    ) -> Result<ShadowComparisonWriteOutcome, StorageError> {
        let comparison_hash = comparison.comparison_hash;
        let result = Entity::insert(comparison.clone().into_active_model())
            .on_conflict(
                OnConflict::column(Column::ComparisonHash)
                    .do_nothing()
                    .to_owned(),
            )
            .try_insert()
            .exec_without_returning(&self.db)
            .await
            .map_err(StorageError::from)?;
        let inserted = match result {
            TryInsertResult::Inserted(1) => true,
            TryInsertResult::Conflicted | TryInsertResult::Inserted(0) => false,
            TryInsertResult::Inserted(rows) => {
                return Err(StorageError::invariant_violation(
                    Some("quant_shadow_comparison"),
                    format!("single shadow-comparison insert affected {rows} rows"),
                ));
            }
            TryInsertResult::Empty => {
                return Err(StorageError::invariant_violation(
                    Some("quant_shadow_comparison"),
                    "non-empty shadow-comparison insert produced no statement",
                ));
            }
        };
        let stored = Entity::find()
            .filter(Column::ComparisonHash.eq(comparison_hash))
            .one(&self.db)
            .await
            .map_err(StorageError::from)?
            .map(ShadowComparisonInfo::from)
            .ok_or_else(|| StorageError::not_found("quant_shadow_comparison", comparison_hash))?;
        if !stored.matches_new(&comparison) {
            return Err(StorageError::state_conflict(
                "quant_shadow_comparison",
                Some(comparison_hash),
                "content-addressed shadow replay differs from the stored observation",
            ));
        }
        Ok(if inserted {
            ShadowComparisonWriteOutcome::Inserted(stored)
        } else {
            ShadowComparisonWriteOutcome::AlreadyPresent(stored)
        })
    }

    async fn summary(
        &self,
        candidate_model_version_id: &ModelVersionId,
        since: DateTime<Utc>,
    ) -> Result<ShadowStabilitySummary, StorageError> {
        let row = Entity::find()
            .filter(Column::CandidateModelVersionId.eq(*candidate_model_version_id))
            .filter(Column::WeightSource.eq(ModelWeightSource::Artifact))
            .filter(Column::DecisionAt.gte(since))
            .select_only()
            .column_as(
                Expr::col(Column::ShadowComparisonId).count(),
                "sample_count",
            )
            .column_as(Expr::col(Column::DecisionAt).min(), "window_start")
            .column_as(Expr::col(Column::DecisionAt).max(), "window_end")
            .column_as(
                Expr::col(Column::TopnDecisionOverlap).avg(),
                "mean_topn_decision_overlap",
            )
            .column_as(
                primitives::bool_or(Column::HardDivergence),
                "any_hard_divergence",
            )
            .into_model::<ShadowSummaryRow>()
            .one(&self.db)
            .await
            .map_err(StorageError::from)?;

        let Some(row) = row else {
            return Ok(ShadowStabilitySummary {
                candidate_model_version_id: *candidate_model_version_id,
                sample_count: 0,
                window_start: None,
                window_end: None,
                mean_topn_decision_overlap: Probability::new(Decimal::ZERO),
                any_hard_divergence: false,
            });
        };

        let sample_count = u64::try_from(cmp::max(row.sample_count, 0)).unwrap_or(u64::MAX);
        Ok(ShadowStabilitySummary {
            candidate_model_version_id: *candidate_model_version_id,
            sample_count,
            window_start: row.window_start,
            window_end: row.window_end,
            mean_topn_decision_overlap: Probability::new(
                row.mean_topn_decision_overlap.unwrap_or(Decimal::ZERO),
            ),
            any_hard_divergence: row.any_hard_divergence.unwrap_or(false),
        })
    }

    async fn observation_window(
        &self,
        query: &ShadowObservationQuery,
    ) -> Result<ShadowObservationWindow, StorageError> {
        if query.window_start >= query.window_end
            || query.champion_model_version_id == query.candidate_model_version_id
            || query.champion_serving_contract_hash == query.candidate_serving_contract_hash
        {
            return Err(StorageError::invariant_violation(
                Some("quant_shadow_comparison"),
                "F10 observation query has an invalid window or aliased subjects",
            ));
        }
        let row = Entity::find()
            .filter(Column::ChampionModelVersionId.eq(query.champion_model_version_id))
            .filter(Column::CandidateModelVersionId.eq(query.candidate_model_version_id))
            .filter(Column::ChampionServingContractHash.eq(query.champion_serving_contract_hash))
            .filter(Column::CandidateServingContractHash.eq(query.candidate_serving_contract_hash))
            .filter(
                Column::ResearchProfileArtifactId.eq(query.research_profile_artifact_id.clone()),
            )
            .filter(Column::CategoryScope.eq(query.category_scope))
            .filter(Column::DecisionPolicySnapshotId.eq(query.decision_policy_snapshot_id))
            .filter(Column::DecisionPolicySnapshotHash.eq(query.decision_policy_snapshot_hash))
            .filter(Column::PolicyBundleGeneration.eq(query.policy_bundle_generation))
            .filter(Column::WeightSource.eq(ModelWeightSource::Artifact))
            .filter(Column::DecisionAt.gte(query.window_start))
            .filter(Column::DecisionAt.lt(query.window_end))
            .filter(Column::CreatedAt.lt(query.window_end))
            .select_only()
            .column_as(
                Expr::col(Column::ShadowComparisonId).count(),
                "sample_count",
            )
            .column_as(Expr::col(Column::DecisionAt).min(), "window_start")
            .column_as(Expr::col(Column::DecisionAt).max(), "window_end")
            .column_as(
                Expr::col(Column::TopnDecisionOverlap).avg(),
                "mean_topn_decision_overlap",
            )
            .column_as(
                primitives::bool_or(Column::HardDivergence),
                "any_hard_divergence",
            )
            .into_model::<ShadowSummaryRow>()
            .one(&self.db)
            .await
            .map_err(StorageError::from)?
            .ok_or_else(|| {
                StorageError::invariant_violation(
                    Some("quant_shadow_comparison"),
                    "aggregate query returned no row",
                )
            })?;
        let sample_count = u64::try_from(row.sample_count).map_err(|error| {
            StorageError::invariant_violation(
                Some("quant_shadow_comparison"),
                format!("aggregate sample count is invalid: {error}"),
            )
        })?;
        if sample_count == 0 {
            if row.window_start.is_some()
                || row.window_end.is_some()
                || row.mean_topn_decision_overlap.is_some()
                || row.any_hard_divergence.is_some()
            {
                return Err(StorageError::invariant_violation(
                    Some("quant_shadow_comparison"),
                    "empty aggregate returned synthetic observation values",
                ));
            }
            return Ok(ShadowObservationWindow {
                sample_count,
                first_decision_at: None,
                last_decision_at: None,
                mean_topn_decision_overlap: None,
                any_hard_divergence: false,
            });
        }
        let first_decision_at = row.window_start.ok_or_else(|| {
            StorageError::invariant_violation(
                Some("quant_shadow_comparison"),
                "non-empty aggregate has no first decision time",
            )
        })?;
        let last_decision_at = row.window_end.ok_or_else(|| {
            StorageError::invariant_violation(
                Some("quant_shadow_comparison"),
                "non-empty aggregate has no last decision time",
            )
        })?;
        let mean_topn_decision_overlap = row.mean_topn_decision_overlap.ok_or_else(|| {
            StorageError::invariant_violation(
                Some("quant_shadow_comparison"),
                "non-empty aggregate has no mean decision overlap",
            )
        })?;
        Ok(ShadowObservationWindow {
            sample_count,
            first_decision_at: Some(first_decision_at),
            last_decision_at: Some(last_decision_at),
            mean_topn_decision_overlap: Some(Probability::new(mean_topn_decision_overlap)),
            any_hard_divergence: row.any_hard_divergence.ok_or_else(|| {
                StorageError::invariant_violation(
                    Some("quant_shadow_comparison"),
                    "non-empty aggregate has no divergence aggregate",
                )
            })?,
        })
    }
}
