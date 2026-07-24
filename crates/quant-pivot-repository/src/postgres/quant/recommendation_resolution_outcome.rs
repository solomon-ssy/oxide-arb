//! `PostgreSQL` WORM recommendation-resolution outcome repository.

use chrono::{TimeZone, Utc};
use quant_pivot_error::storage::{
    StorageError,
    entity::{QUANT_RECOMMENDATION, QUANT_RECOMMENDATION_RESOLUTION_OUTCOME},
};
use quant_pivot_models::{
    clickhouse::MarketResolutionRow,
    domain::quant::{
        InsertResolutionOutcomeResult, NewRecommendationResolutionOutcome,
        RecommendationResolutionOutcomeInfo, RecommendationResolutionOutcomePage,
        RecommendationResolutionOutcomePageQuery, RecommendationResolutionReconciliationCandidate,
    },
    entities::{
        market::Column as MarketColumn,
        quant_recommendation::{
            Column as QuantRecommendationColumn, Entity as QuantRecommendationEntity,
            Model as QuantRecommendationModel, Relation as QuantRecommendationRelation,
        },
        quant_recommendation_resolution_outcome::{
            Column, Entity as QuantRecommendationResolutionOutcomeEntity,
            Model as QuantRecommendationResolutionOutcomeModel,
        },
    },
    enums::market::MarketStatus,
    types::{MarketId, RecommendationId, SchemaVersion},
};
use sea_orm::{
    ActiveValue, ColumnTrait, Condition, DatabaseConnection, DatabaseTransaction, DbErr,
    EntityTrait, IntoActiveModel, JoinType, QueryFilter, QueryOrder, QuerySelect, RelationTrait,
    TransactionTrait, sea_query::OnConflict,
};

use crate::{
    postgres::{
        error::{invariant_violation, not_found, state_conflict},
        primitives::statement_timestamp,
    },
    traits::RecommendationResolutionOutcomeRepository,
};

/// PostgreSQL-backed immutable recommendation-resolution outcome repository.
pub struct PgRecommendationResolutionOutcomeRepository {
    db: DatabaseConnection,
}

impl PgRecommendationResolutionOutcomeRepository {
    pub const fn new(db: DatabaseConnection) -> Self {
        Self { db }
    }
}

#[async_trait::async_trait]
impl RecommendationResolutionOutcomeRepository for PgRecommendationResolutionOutcomeRepository {
    async fn reconcile_fact(
        &self,
        recommendation_id: &RecommendationId,
        fact: &MarketResolutionRow,
    ) -> Result<InsertResolutionOutcomeResult, StorageError> {
        fact.validate().map_err(|error| {
            invariant_violation(Some(QUANT_RECOMMENDATION_RESOLUTION_OUTCOME), error)
        })?;
        let transaction = self.db.begin().await.map_err(StorageError::from)?;
        let recommendation = QuantRecommendationEntity::find_by_id(*recommendation_id)
            .lock_shared()
            .one(&transaction)
            .await
            .map_err(StorageError::from)?
            .ok_or_else(|| not_found(QUANT_RECOMMENDATION, recommendation_id))?;
        let outcome = derive_outcome(&recommendation, fact)?;
        let result = insert_derived(&transaction, outcome).await?;
        transaction.commit().await.map_err(StorageError::from)?;
        Ok(result)
    }

    async fn find_by_recommendation(
        &self,
        recommendation_id: &RecommendationId,
    ) -> Result<Option<RecommendationResolutionOutcomeInfo>, StorageError> {
        QuantRecommendationResolutionOutcomeEntity::find_by_id(*recommendation_id)
            .one(&self.db)
            .await
            .map_err(StorageError::from)?
            .map(validated_info)
            .transpose()
    }

    async fn list_reconciliation_candidates(
        &self,
        after: Option<RecommendationId>,
        limit: u64,
    ) -> Result<Vec<RecommendationResolutionReconciliationCandidate>, StorageError> {
        if limit == 0 {
            return Ok(Vec::new());
        }
        let rows = QuantRecommendationEntity::find()
            .select_only()
            .column(QuantRecommendationColumn::RecommendationId)
            .column(QuantRecommendationColumn::MarketId)
            .join(
                JoinType::InnerJoin,
                QuantRecommendationRelation::Market.def(),
            )
            .join(
                JoinType::LeftJoin,
                QuantRecommendationRelation::ResolutionOutcome.def(),
            )
            .filter(MarketColumn::Status.eq(MarketStatus::Settled))
            .filter(Column::RecommendationId.is_null())
            .filter(after.map_or_else(Condition::all, |cursor| {
                Condition::all().add(QuantRecommendationColumn::RecommendationId.gt(cursor))
            }))
            .order_by_asc(QuantRecommendationColumn::RecommendationId)
            .limit(limit)
            .into_tuple::<(RecommendationId, MarketId)>()
            .all(&self.db)
            .await
            .map_err(StorageError::from)?;
        Ok(rows
            .into_iter()
            .map(
                |(recommendation_id, market_id)| RecommendationResolutionReconciliationCandidate {
                    recommendation_id,
                    market_id,
                },
            )
            .collect())
    }

    async fn list_available_page(
        &self,
        query: RecommendationResolutionOutcomePageQuery,
    ) -> Result<RecommendationResolutionOutcomePage, StorageError> {
        query.validate().map_err(|error| {
            invariant_violation(Some(QUANT_RECOMMENDATION_RESOLUTION_OUTCOME), error)
        })?;
        let keyset = query.after.map(|cursor| {
            Condition::any()
                .add(Column::AvailableAt.gt(cursor.available_at))
                .add(
                    Condition::all()
                        .add(Column::AvailableAt.eq(cursor.available_at))
                        .add(Column::RecommendationId.gt(cursor.recommendation_id)),
                )
        });
        let condition = Condition::all()
            .add(Column::AvailableAt.gte(query.available_from))
            .add(Column::AvailableAt.lte(query.available_through))
            .add_option(keyset);
        let outcomes = QuantRecommendationResolutionOutcomeEntity::find()
            .filter(condition)
            .order_by_asc(Column::AvailableAt)
            .order_by_asc(Column::RecommendationId)
            .limit(u64::from(query.limit))
            .all(&self.db)
            .await
            .map_err(StorageError::from)?
            .into_iter()
            .map(validated_info)
            .collect::<Result<Vec<_>, _>>()?;
        Ok(RecommendationResolutionOutcomePage::new(outcomes))
    }
}

fn derive_outcome(
    recommendation: &QuantRecommendationModel,
    fact: &MarketResolutionRow,
) -> Result<NewRecommendationResolutionOutcome, StorageError> {
    if recommendation.market_id != fact.market_id {
        return Err(invariant_violation(
            Some(QUANT_RECOMMENDATION_RESOLUTION_OUTCOME),
            format!(
                "recommendation {} is bound to market {}, not resolution market {}",
                recommendation.recommendation_id, recommendation.market_id, fact.market_id
            ),
        ));
    }
    let token_payout_ratio = fact.payout_for(&recommendation.token_id).map_err(|error| {
        invariant_violation(Some(QUANT_RECOMMENDATION_RESOLUTION_OUTCOME), error)
    })?;
    let resolution_kind = fact.resolution_kind().map_err(|error| {
        invariant_violation(Some(QUANT_RECOMMENDATION_RESOLUTION_OUTCOME), error)
    })?;
    let resolved_at = Utc
        .timestamp_millis_opt(fact.resolved_at)
        .single()
        .ok_or_else(|| {
            invariant_violation(
                Some(QUANT_RECOMMENDATION_RESOLUTION_OUTCOME),
                format!(
                    "resolution fact timestamp {} is outside the UTC millisecond range",
                    fact.resolved_at
                ),
            )
        })?;
    let source_observed_at = Utc
        .timestamp_millis_opt(fact.observed_at)
        .single()
        .ok_or_else(|| {
            invariant_violation(
                Some(QUANT_RECOMMENDATION_RESOLUTION_OUTCOME),
                format!(
                    "resolution observation timestamp {} is outside the UTC millisecond range",
                    fact.observed_at
                ),
            )
        })?;
    let resolution_fact_log_index = i64::try_from(fact.source_log_index).map_err(|_| {
        invariant_violation(
            Some(QUANT_RECOMMENDATION_RESOLUTION_OUTCOME),
            format!(
                "resolution fact log index {} exceeds PostgreSQL BIGINT",
                fact.source_log_index
            ),
        )
    })?;
    Ok(NewRecommendationResolutionOutcome {
        recommendation_id: recommendation.recommendation_id,
        market_id: recommendation.market_id.clone(),
        token_id: recommendation.token_id.clone(),
        resolution_kind,
        token_payout_ratio,
        resolved_at,
        source_observed_at,
        source_checkpoint_hash: fact.source_checkpoint_hash,
        resolution_fact_hash: fact.resolution_fact_hash,
        resolution_fact_log_index,
        resolution_fact_schema_version: SchemaVersion::FIRST,
    })
}

async fn insert_derived(
    transaction: &DatabaseTransaction,
    outcome: NewRecommendationResolutionOutcome,
) -> Result<InsertResolutionOutcomeResult, StorageError> {
    validate_new(&outcome)?;
    let available_at = statement_timestamp(transaction).await?;
    let outcome_hash = outcome
        .expected_outcome_hash(available_at)
        .map_err(|error| {
            invariant_violation(Some(QUANT_RECOMMENDATION_RESOLUTION_OUTCOME), error)
        })?;
    let mut active_outcome = outcome.clone().into_active_model();
    active_outcome.available_at = ActiveValue::Set(available_at);
    active_outcome.outcome_hash = ActiveValue::Set(outcome_hash);

    let inserted = match QuantRecommendationResolutionOutcomeEntity::insert(active_outcome)
        .on_conflict(
            OnConflict::column(Column::RecommendationId)
                .do_nothing()
                .to_owned(),
        )
        .exec_with_returning(transaction)
        .await
    {
        Ok(row) => Some(row),
        Err(DbErr::RecordNotFound(_)) => None,
        Err(error) => return Err(StorageError::from(error)),
    };
    let (row, was_inserted) = match inserted {
        Some(row) => (row, true),
        None => (
            QuantRecommendationResolutionOutcomeEntity::find_by_id(outcome.recommendation_id)
                .one(transaction)
                .await
                .map_err(StorageError::from)?
                .ok_or_else(|| {
                    invariant_violation(
                        Some(QUANT_RECOMMENDATION_RESOLUTION_OUTCOME),
                        "outcome conflict completed without an observable stored row",
                    )
                })?,
            false,
        ),
    };
    let stored = validated_info(row)?;
    if !stored.has_same_derivation(&outcome) {
        return Err(state_conflict(
            QUANT_RECOMMENDATION_RESOLUTION_OUTCOME,
            Some(outcome.recommendation_id),
            "recommendation id is already bound to different immutable resolution content",
        ));
    }
    if was_inserted {
        Ok(InsertResolutionOutcomeResult::Inserted(stored))
    } else {
        Ok(InsertResolutionOutcomeResult::AlreadyPresent(stored))
    }
}

fn validate_new(outcome: &NewRecommendationResolutionOutcome) -> Result<(), StorageError> {
    outcome
        .validate()
        .map_err(|error| invariant_violation(Some(QUANT_RECOMMENDATION_RESOLUTION_OUTCOME), error))
}

fn validated_info(
    row: QuantRecommendationResolutionOutcomeModel,
) -> Result<RecommendationResolutionOutcomeInfo, StorageError> {
    let outcome: RecommendationResolutionOutcomeInfo = row.into();
    outcome.validate().map_err(|error| {
        invariant_violation(
            Some(QUANT_RECOMMENDATION_RESOLUTION_OUTCOME),
            format!("stored outcome failed integrity validation: {error}"),
        )
    })?;
    Ok(outcome)
}
