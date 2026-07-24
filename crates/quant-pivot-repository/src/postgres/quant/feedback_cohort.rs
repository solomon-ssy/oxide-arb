//! Point-in-time feedback-cohort aggregate reader.

use std::{collections::HashMap, fmt::Display};

use chrono::{DateTime, Utc};
use quant_pivot_error::storage::{StorageError, entity::QUANT_RECOMMENDATION};
use quant_pivot_models::{
    domain::quant::{
        FeedbackCohortCandidate, FeedbackCohortPage, FeedbackCohortPageQuery,
        FeedbackExecutionAttempt, FeedbackRecommendationContext,
        RecommendationExecutionOutcomeInfo, RecommendationInfo, RecommendationReportInfo,
        RecommendationResolutionOutcomeInfo,
    },
    entities::{
        quant_execution_order::{
            Column as QuantExecutionOrderColumn, Entity as QuantExecutionOrderEntity,
            Relation as QuantExecutionOrderRelation,
        },
        quant_order_intent::Column as QuantOrderIntentColumn,
        quant_recommendation::{
            Column as QuantRecommendationColumn, Entity as QuantRecommendationEntity,
        },
        quant_recommendation_execution_outcome::{
            Column as QuantRecommendationExecutionOutcomeColumn,
            Entity as QuantRecommendationExecutionOutcomeEntity,
        },
        quant_recommendation_report::{
            Column as QuantRecommendationReportColumn, Entity as QuantRecommendationReportEntity,
        },
        quant_recommendation_resolution_outcome::{
            Column as QuantRecommendationResolutionOutcomeColumn,
            Entity as QuantRecommendationResolutionOutcomeEntity,
        },
    },
    enums::{
        execution::ExecutionOrderPhase,
        quant::{FeedbackCohort, QuantRuntimeMode},
    },
    types::{
        DecisionPolicySnapshotId, ExecutionOrderId, MarketId, ModelVersionId, OrderIntentId,
        RecommendationId, ResearchProfileArtifactId, TokenId,
    },
};
use sea_orm::{
    AccessMode, ColumnTrait, Condition, DatabaseConnection, DatabaseTransaction, EntityTrait,
    FromQueryResult, IsolationLevel, JoinType, QueryFilter, QueryOrder, QuerySelect, RelationTrait,
    TransactionTrait,
};

use crate::{postgres::error::invariant_violation, traits::FeedbackCohortRepository};

/// PostgreSQL-backed PIT feedback-cohort aggregate reader.
pub struct PgFeedbackCohortRepository {
    db: DatabaseConnection,
}

impl PgFeedbackCohortRepository {
    pub const fn new(db: DatabaseConnection) -> Self {
        Self { db }
    }
}

#[derive(Debug, FromQueryResult)]
struct SubmittedAttemptRow {
    recommendation_id: RecommendationId,
    order_intent_id: OrderIntentId,
    entry_execution_order_id: ExecutionOrderId,
    submitted_at: DateTime<Utc>,
    research_profile_artifact_id: ResearchProfileArtifactId,
    decision_policy_snapshot_id: DecisionPolicySnapshotId,
    model_version_id: ModelVersionId,
    runtime_mode: QuantRuntimeMode,
    market_id: MarketId,
    token_id: TokenId,
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
        let page = load_page(&transaction, &query).await?;
        transaction.commit().await.map_err(StorageError::from)?;
        Ok(page)
    }
}

async fn load_page(
    transaction: &DatabaseTransaction,
    query: &FeedbackCohortPageQuery,
) -> Result<FeedbackCohortPage, StorageError> {
    let cohort = query.cohort();
    let mut contexts = load_contexts(transaction, query).await?;
    let limit = usize::try_from(query.limit()).map_err(|error| {
        invariant_violation(
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
        load_resolution_outcomes(transaction, query, &recommendation_ids).await?;
    let mut execution_outcomes =
        load_execution_outcomes(transaction, query, &recommendation_ids).await?;
    let mut execution_attempts = load_execution_attempts(transaction, query, &contexts).await?;
    let candidates = contexts
        .into_iter()
        .map(|context| {
            let recommendation_id = context.recommendation_id();
            let execution_attempt = cohort_uses_execution(cohort).then(|| {
                execution_attempts
                    .remove(&recommendation_id)
                    .unwrap_or(FeedbackExecutionAttempt::NotAttempted)
            });
            FeedbackCohortCandidate::try_new(
                cohort,
                context,
                execution_attempt,
                resolution_outcomes.remove(&recommendation_id),
                execution_outcomes.remove(&recommendation_id),
            )
            .map_err(feedback_contract_error)
        })
        .collect::<Result<Vec<_>, _>>()?;
    FeedbackCohortPage::try_new(cohort, candidates, has_more).map_err(feedback_contract_error)
}

async fn load_contexts(
    transaction: &DatabaseTransaction,
    query: &FeedbackCohortPageQuery,
) -> Result<Vec<FeedbackRecommendationContext>, StorageError> {
    let window = query.window();
    let profile_artifact_id = window.profile_ref().artifact_id();
    let keyset = query.after().map(|cursor| {
        Condition::any()
            .add(QuantRecommendationColumn::CreatedAt.gt(cursor.available_at()))
            .add(
                Condition::all()
                    .add(QuantRecommendationColumn::CreatedAt.eq(cursor.available_at()))
                    .add(
                        QuantRecommendationColumn::RecommendationId.gt(cursor.recommendation_id()),
                    ),
            )
    });
    let condition = Condition::all()
        .add(QuantRecommendationColumn::ResearchProfileArtifactId.eq(profile_artifact_id.clone()))
        .add(QuantRecommendationColumn::CreatedAt.gte(window.window_start()))
        .add(QuantRecommendationColumn::CreatedAt.lte(window.cutoff()))
        .add(QuantRecommendationReportColumn::ResearchProfileArtifactId.eq(profile_artifact_id))
        .add(QuantRecommendationReportColumn::DecisionAt.gte(window.window_start()))
        .add(QuantRecommendationReportColumn::DecisionAt.lte(window.cutoff()))
        .add(QuantRecommendationReportColumn::CreatedAt.lte(window.cutoff()))
        .add_option(keyset);
    let fetch_limit = u64::from(query.limit()) + 1;
    let rows = QuantRecommendationEntity::find()
        .find_also_related(QuantRecommendationReportEntity)
        .filter(condition)
        .order_by_asc(QuantRecommendationColumn::CreatedAt)
        .order_by_asc(QuantRecommendationColumn::RecommendationId)
        .limit(fetch_limit)
        .all(transaction)
        .await
        .map_err(StorageError::from)?;
    rows.into_iter()
        .map(|(recommendation, report)| {
            let report = report.ok_or_else(|| {
                invariant_violation(
                    Some(QUANT_RECOMMENDATION),
                    "recommendation lost its required report relation",
                )
            })?;
            FeedbackRecommendationContext::try_from_report(
                &RecommendationInfo::from(recommendation),
                &RecommendationReportInfo::from(report),
            )
            .map_err(feedback_contract_error)
        })
        .collect()
}

async fn load_resolution_outcomes(
    transaction: &DatabaseTransaction,
    query: &FeedbackCohortPageQuery,
    recommendation_ids: &[RecommendationId],
) -> Result<HashMap<RecommendationId, RecommendationResolutionOutcomeInfo>, StorageError> {
    if recommendation_ids.is_empty() || !cohort_uses_resolution(query.cohort()) {
        return Ok(HashMap::new());
    }
    let rows = QuantRecommendationResolutionOutcomeEntity::find()
        .filter(
            QuantRecommendationResolutionOutcomeColumn::RecommendationId
                .is_in(recommendation_ids.iter().copied()),
        )
        .filter(
            QuantRecommendationResolutionOutcomeColumn::AvailableAt.lte(query.window().cutoff()),
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

async fn load_execution_outcomes(
    transaction: &DatabaseTransaction,
    query: &FeedbackCohortPageQuery,
    recommendation_ids: &[RecommendationId],
) -> Result<HashMap<RecommendationId, RecommendationExecutionOutcomeInfo>, StorageError> {
    if recommendation_ids.is_empty() || !cohort_uses_execution(query.cohort()) {
        return Ok(HashMap::new());
    }
    let rows = QuantRecommendationExecutionOutcomeEntity::find()
        .filter(
            QuantRecommendationExecutionOutcomeColumn::RecommendationId
                .is_in(recommendation_ids.iter().copied()),
        )
        .filter(QuantRecommendationExecutionOutcomeColumn::AvailableAt.lte(query.window().cutoff()))
        .all(transaction)
        .await
        .map_err(StorageError::from)?;
    rows.into_iter()
        .map(|row| {
            let outcome = RecommendationExecutionOutcomeInfo::from(row);
            outcome.validate().map_err(feedback_contract_error)?;
            Ok((outcome.recommendation_id, outcome))
        })
        .collect()
}

async fn load_execution_attempts(
    transaction: &DatabaseTransaction,
    query: &FeedbackCohortPageQuery,
    contexts: &[FeedbackRecommendationContext],
) -> Result<HashMap<RecommendationId, FeedbackExecutionAttempt>, StorageError> {
    if contexts.is_empty() || !cohort_uses_execution(query.cohort()) {
        return Ok(HashMap::new());
    }
    let recommendation_ids = contexts
        .iter()
        .map(FeedbackRecommendationContext::recommendation_id)
        .collect::<Vec<_>>();
    let rows = QuantExecutionOrderEntity::find()
        .select_only()
        .column_as(
            QuantOrderIntentColumn::RecommendationId,
            "recommendation_id",
        )
        .column_as(QuantOrderIntentColumn::OrderIntentId, "order_intent_id")
        .column_as(
            QuantExecutionOrderColumn::ExecutionOrderId,
            "entry_execution_order_id",
        )
        .column_as(QuantExecutionOrderColumn::SubmittedAt, "submitted_at")
        .column_as(
            QuantOrderIntentColumn::ResearchProfileArtifactId,
            "research_profile_artifact_id",
        )
        .column_as(
            QuantOrderIntentColumn::DecisionPolicySnapshotId,
            "decision_policy_snapshot_id",
        )
        .column_as(QuantOrderIntentColumn::ModelVersionId, "model_version_id")
        .column_as(QuantOrderIntentColumn::RuntimeMode, "runtime_mode")
        .column_as(QuantExecutionOrderColumn::MarketId, "market_id")
        .column_as(QuantExecutionOrderColumn::TokenId, "token_id")
        .join(
            JoinType::InnerJoin,
            QuantExecutionOrderRelation::OrderIntent.def(),
        )
        .filter(QuantOrderIntentColumn::RecommendationId.is_in(recommendation_ids.iter().copied()))
        .filter(QuantExecutionOrderColumn::OrderPhase.eq(ExecutionOrderPhase::Entry))
        .filter(QuantExecutionOrderColumn::SubmittedAt.is_not_null())
        .filter(QuantExecutionOrderColumn::SubmittedAt.lte(query.window().cutoff()))
        .order_by_asc(QuantOrderIntentColumn::RecommendationId)
        .order_by_asc(QuantExecutionOrderColumn::ExecutionOrderId)
        .into_model::<SubmittedAttemptRow>()
        .all(transaction)
        .await
        .map_err(StorageError::from)?;
    let contexts = contexts
        .iter()
        .map(|context| (context.recommendation_id(), context))
        .collect::<HashMap<_, _>>();
    let mut attempts = HashMap::with_capacity(rows.len());
    for row in rows {
        let context = contexts.get(&row.recommendation_id).ok_or_else(|| {
            invariant_violation(
                Some(QUANT_RECOMMENDATION),
                "submitted attempt escaped the bounded recommendation page",
            )
        })?;
        validate_submitted_attempt(&row, context)?;
        if attempts
            .insert(
                row.recommendation_id,
                FeedbackExecutionAttempt::Submitted {
                    order_intent_id: row.order_intent_id,
                    entry_execution_order_id: row.entry_execution_order_id,
                    submitted_at: row.submitted_at,
                },
            )
            .is_some()
        {
            return Err(invariant_violation(
                Some(QUANT_RECOMMENDATION),
                format!(
                    "recommendation {} contains multiple submitted entry orders",
                    row.recommendation_id
                ),
            ));
        }
    }
    Ok(attempts)
}

fn validate_submitted_attempt(
    row: &SubmittedAttemptRow,
    context: &FeedbackRecommendationContext,
) -> Result<(), StorageError> {
    let expected_profile = context.profile_ref().artifact_id();
    let identity_matches = row.research_profile_artifact_id == expected_profile
        && row.decision_policy_snapshot_id == context.decision_policy_snapshot_id()
        && row.model_version_id == context.model_version_id()
        && row.runtime_mode == context.runtime_mode()
        && &row.market_id == context.market_id()
        && &row.token_id == context.token_id();
    if !identity_matches {
        return Err(invariant_violation(
            Some(QUANT_RECOMMENDATION),
            format!(
                "submitted attempt lineage does not match recommendation {}",
                row.recommendation_id
            ),
        ));
    }
    Ok(())
}

const fn cohort_uses_resolution(cohort: FeedbackCohort) -> bool {
    matches!(
        cohort,
        FeedbackCohort::ModelLearning | FeedbackCohort::PolicyEvaluation
    )
}

const fn cohort_uses_execution(cohort: FeedbackCohort) -> bool {
    matches!(
        cohort,
        FeedbackCohort::ExecutionLearning | FeedbackCohort::PolicyEvaluation
    )
}

fn feedback_contract_error(error: impl Display) -> StorageError {
    invariant_violation(Some(QUANT_RECOMMENDATION), error)
}
