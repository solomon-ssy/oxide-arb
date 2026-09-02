//! Point-in-time feedback-cohort aggregate reader.

use std::{collections::HashMap, fmt::Display};

use quant_pivot_error::storage::{StorageError, entity::QUANT_RECOMMENDATION};
use quant_pivot_models::{
    domain::quant::{
        FeedbackCohortCandidate, FeedbackCohortPage, FeedbackCohortPageQuery,
        FeedbackRecommendationContext, RecommendationEconomicOutcomeInfo,
        RecommendationExecutionRollupInfo, RecommendationInfo, RecommendationReportInfo,
        RecommendationResolutionOutcomeInfo, ReportRouteRunInfo,
    },
    entities::{
        quant_recommendation::{
            Column as QuantRecommendationColumn, Entity as QuantRecommendationEntity,
        },
        quant_recommendation_economic_outcome::{
            Column as EconomicOutcomeColumn, Entity as EconomicOutcomeEntity,
        },
        quant_recommendation_execution_rollup::{
            Column as QuantRecommendationExecutionRollupColumn,
            Entity as QuantRecommendationExecutionRollupEntity,
        },
        quant_recommendation_report::{
            Column as QuantRecommendationReportColumn, Entity as QuantRecommendationReportEntity,
        },
        quant_recommendation_resolution_outcome::{
            Column as QuantRecommendationResolutionOutcomeColumn,
            Entity as QuantRecommendationResolutionOutcomeEntity,
        },
        quant_report_route_run::{
            Column as QuantReportRouteRunColumn, Entity as QuantReportRouteRunEntity,
        },
    },
    types::RecommendationId,
};
use sea_orm::{
    AccessMode, ColumnTrait, Condition, DatabaseConnection, DatabaseTransaction, EntityTrait,
    IsolationLevel, QueryFilter, QueryOrder, QuerySelect, TransactionTrait,
};

use crate::traits::FeedbackCohortRepository;

/// PostgreSQL-backed PIT feedback-cohort aggregate reader.
pub struct PgFeedbackCohortRepository {
    db: DatabaseConnection,
}

impl PgFeedbackCohortRepository {
    pub const fn new(db: DatabaseConnection) -> Self {
        Self { db }
    }
}

#[async_trait::async_trait]
impl FeedbackCohortRepository for PgFeedbackCohortRepository {
    async fn list_page(
        &self,
        query: FeedbackCohortPageQuery,
    ) -> Result<FeedbackCohortPage, StorageError> {
        let transaction = self
            .db
            .begin_with_config(
                Some(IsolationLevel::RepeatableRead),
                Some(AccessMode::ReadOnly),
            )
            .await
            .map_err(StorageError::from)?;
        let page = Self::load_page(&transaction, &query).await?;
        transaction.commit().await.map_err(StorageError::from)?;
        Ok(page)
    }
}

impl PgFeedbackCohortRepository {
    async fn load_page(
        transaction: &DatabaseTransaction,
        query: &FeedbackCohortPageQuery,
    ) -> Result<FeedbackCohortPage, StorageError> {
        let cohort = query.cohort();
        let mut contexts = Self::load_contexts(transaction, query).await?;
        let limit = usize::try_from(query.limit()).map_err(|error| {
            StorageError::invariant_violation(
                Some(QUANT_RECOMMENDATION),
                format!("convert bounded feedback page limit: {error}"),
            )
        })?;
        let has_more = contexts.len() > limit;
        contexts.truncate(limit);

        let recommendation_ids = contexts
            .iter()
            .map(FeedbackRecommendationContext::recommendation_id)
            .collect::<Vec<_>>();
        let mut resolution_outcomes =
            Self::load_resolution_outcomes(transaction, query, &recommendation_ids).await?;
        let mut execution_rollups =
            Self::load_execution_rollups(transaction, query, &recommendation_ids).await?;
        let mut economic_outcomes =
            Self::load_economic_outcomes(transaction, query, &recommendation_ids).await?;
        let candidates = contexts
            .into_iter()
            .map(|context| {
                let recommendation_id = context.recommendation_id();
                FeedbackCohortCandidate::try_new(
                    cohort,
                    context,
                    resolution_outcomes.remove(&recommendation_id),
                    execution_rollups.remove(&recommendation_id),
                    economic_outcomes.remove(&recommendation_id),
                )
                .map_err(feedback_contract_error)
            })
            .collect::<Result<Vec<_>, _>>()?;
        FeedbackCohortPage::try_new(cohort, candidates, has_more).map_err(feedback_contract_error)
    }
}

impl PgFeedbackCohortRepository {
    async fn load_economic_outcomes(
        transaction: &DatabaseTransaction,
        query: &FeedbackCohortPageQuery,
        recommendation_ids: &[RecommendationId],
    ) -> Result<HashMap<RecommendationId, RecommendationEconomicOutcomeInfo>, StorageError> {
        if recommendation_ids.is_empty() || !query.cohort().requires_economic_outcome() {
            return Ok(HashMap::new());
        }
        EconomicOutcomeEntity::find()
            .filter(
                EconomicOutcomeColumn::RecommendationId.is_in(recommendation_ids.iter().copied()),
            )
            .filter(EconomicOutcomeColumn::AvailableAt.lte(query.snapshot().truth_cutoff()))
            .all(transaction)
            .await
            .map_err(StorageError::from)?
            .into_iter()
            .map(|row| {
                let outcome = RecommendationEconomicOutcomeInfo::from(row);
                outcome.verify().map_err(feedback_contract_error)?;
                Ok((outcome.recommendation_id, outcome))
            })
            .collect()
    }
}

impl PgFeedbackCohortRepository {
    async fn load_contexts(
        transaction: &DatabaseTransaction,
        query: &FeedbackCohortPageQuery,
    ) -> Result<Vec<FeedbackRecommendationContext>, StorageError> {
        let snapshot = query.snapshot();
        let window = snapshot.decision_window();
        let profile_artifact_id = window.profile_ref().artifact_id();
        let keyset = query.after().map(|cursor| {
            Condition::any()
                .add(QuantRecommendationColumn::CreatedAt.gt(cursor.available_at()))
                .add(
                    Condition::all()
                        .add(QuantRecommendationColumn::CreatedAt.eq(cursor.available_at()))
                        .add(
                            QuantRecommendationColumn::RecommendationId
                                .gt(cursor.recommendation_id()),
                        ),
                )
        });
        let condition = Condition::all()
            .add(
                QuantReportRouteRunColumn::ResearchProfileArtifactId
                    .eq(profile_artifact_id.clone()),
            )
            .add(QuantRecommendationColumn::CreatedAt.lte(window.cutoff()))
            .add(QuantRecommendationReportColumn::DecisionAt.gte(window.window_start()))
            .add(QuantRecommendationReportColumn::DecisionAt.lte(window.cutoff()))
            .add(QuantRecommendationReportColumn::CreatedAt.lte(window.cutoff()))
            .add_option(keyset);
        let fetch_limit = u64::from(query.limit()) + 1;
        let rows = QuantRecommendationEntity::find()
            .inner_join(QuantReportRouteRunEntity)
            .find_also_related(QuantRecommendationReportEntity)
            .filter(condition)
            .order_by_asc(QuantRecommendationColumn::CreatedAt)
            .order_by_asc(QuantRecommendationColumn::RecommendationId)
            .limit(fetch_limit)
            .all(transaction)
            .await
            .map_err(StorageError::from)?;
        let route_run_ids = rows
            .iter()
            .map(|(recommendation, _)| recommendation.report_route_run_id)
            .collect::<Vec<_>>();
        let route_runs = QuantReportRouteRunEntity::find()
            .filter(QuantReportRouteRunColumn::ReportRouteRunId.is_in(route_run_ids))
            .all(transaction)
            .await
            .map_err(StorageError::from)?
            .into_iter()
            .map(|route_run| (route_run.report_route_run_id, route_run))
            .collect::<HashMap<_, _>>();
        rows.into_iter()
            .map(|(recommendation, report)| {
                let report = report.ok_or_else(|| {
                    StorageError::invariant_violation(
                        Some(QUANT_RECOMMENDATION),
                        "recommendation lost its required report relation",
                    )
                })?;
                let route_run = route_runs
                    .get(&recommendation.report_route_run_id)
                    .ok_or_else(|| {
                        StorageError::invariant_violation(
                            Some(QUANT_RECOMMENDATION),
                            "recommendation lost its required Route-run relation",
                        )
                    })?;
                let route_run_info = ReportRouteRunInfo::from(route_run.clone());
                FeedbackRecommendationContext::try_from_report(
                    &RecommendationInfo::from(recommendation),
                    &RecommendationReportInfo::from(report),
                    &route_run_info,
                )
                .map_err(feedback_contract_error)
            })
            .collect()
    }
}

impl PgFeedbackCohortRepository {
    async fn load_resolution_outcomes(
        transaction: &DatabaseTransaction,
        query: &FeedbackCohortPageQuery,
        recommendation_ids: &[RecommendationId],
    ) -> Result<HashMap<RecommendationId, RecommendationResolutionOutcomeInfo>, StorageError> {
        if recommendation_ids.is_empty() || !query.cohort().requires_resolution() {
            return Ok(HashMap::new());
        }
        let rows = QuantRecommendationResolutionOutcomeEntity::find()
            .filter(
                QuantRecommendationResolutionOutcomeColumn::RecommendationId
                    .is_in(recommendation_ids.iter().copied()),
            )
            .filter(
                QuantRecommendationResolutionOutcomeColumn::AvailableAt
                    .lte(query.snapshot().truth_cutoff()),
            )
            .all(transaction)
            .await
            .map_err(StorageError::from)?;
        rows.into_iter()
            .map(|row| {
                let outcome = RecommendationResolutionOutcomeInfo::from(row);
                outcome.validate().map_err(feedback_contract_error)?;
                Ok((outcome.recommendation_id, outcome))
            })
            .collect()
    }
}

impl PgFeedbackCohortRepository {
    async fn load_execution_rollups(
        transaction: &DatabaseTransaction,
        query: &FeedbackCohortPageQuery,
        recommendation_ids: &[RecommendationId],
    ) -> Result<HashMap<RecommendationId, RecommendationExecutionRollupInfo>, StorageError> {
        if recommendation_ids.is_empty() || !query.cohort().requires_execution() {
            return Ok(HashMap::new());
        }
        let rows = QuantRecommendationExecutionRollupEntity::find()
            .filter(
                QuantRecommendationExecutionRollupColumn::RecommendationId
                    .is_in(recommendation_ids.iter().copied()),
            )
            .filter(
                QuantRecommendationExecutionRollupColumn::AvailableAt
                    .lte(query.snapshot().truth_cutoff()),
            )
            .all(transaction)
            .await
            .map_err(StorageError::from)?;
        rows.into_iter()
            .map(|row| {
                let rollup = RecommendationExecutionRollupInfo::from(row);
                rollup.validate().map_err(feedback_contract_error)?;
                Ok((rollup.recommendation_id, rollup))
            })
            .collect()
    }
}

fn feedback_contract_error(error: impl Display) -> StorageError {
    StorageError::invariant_violation(Some(QUANT_RECOMMENDATION), error)
}
