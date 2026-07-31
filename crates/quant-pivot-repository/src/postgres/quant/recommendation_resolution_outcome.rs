//! `PostgreSQL` WORM recommendation-resolution outcome and durable queue.

use std::{collections::HashMap, fmt::Display};

use chrono::{DateTime, Duration, TimeZone, Utc};
use quant_pivot_error::storage::{
    StorageError,
    entity::{
        QUANT_RECOMMENDATION, QUANT_RECOMMENDATION_RESOLUTION_OUTCOME,
        QUANT_RESOLUTION_OUTCOME_RECONCILIATION_TASK,
    },
};
use quant_pivot_models::{
    clickhouse::MarketResolutionRow,
    domain::quant::{
        InsertResolutionOutcomeResult, NewRecommendationResolutionOutcome, OutcomeTaskSettlement,
        RecommendationResolutionOutcomeInfo, RecommendationResolutionOutcomePage,
        RecommendationResolutionOutcomePageQuery, RecommendationResolutionReconciliationCandidate,
        ResolutionOutcomeTaskClaim,
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
        quant_resolution_outcome_reconciliation_task::{
            ActiveModel as TaskActiveModel, Column as TaskColumn, Entity as TaskEntity,
        },
    },
    enums::{market::MarketStatus, quant::OutcomeReconciliationTaskStatus},
    types::{MarketId, RecommendationId, SchemaVersion, WorkerId},
};
use sea_orm::{
    ActiveModelTrait, ActiveValue, ColumnTrait, Condition, DatabaseConnection, DatabaseTransaction,
    DbErr, EntityTrait, IntoActiveModel, JoinType, QueryFilter, QueryOrder, QuerySelect,
    RelationTrait, TransactionTrait,
    sea_query::{LockBehavior, LockType, OnConflict},
};

use crate::{
    postgres::primitives::statement_timestamp, traits::RecommendationResolutionOutcomeRepository,
};

const MAX_ERROR_CHARS: usize = 4_096;
const MAX_LEASE_SECS: u64 = 3_600;
const MAX_QUEUE_BATCH: u64 = 4_096;
const MAX_RETRY_SECS: u64 = 86_400;

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
            StorageError::invariant_violation(Some(QUANT_RECOMMENDATION_RESOLUTION_OUTCOME), error)
        })?;
        let transaction = self.db.begin().await.map_err(StorageError::from)?;
        let recommendation = QuantRecommendationEntity::find_by_id(*recommendation_id)
            .lock_shared()
            .one(&transaction)
            .await
            .map_err(StorageError::from)?
            .ok_or_else(|| StorageError::not_found(QUANT_RECOMMENDATION, recommendation_id))?;
        let outcome = derive_outcome(&recommendation, fact)?;
        let result = Self::insert_derived(&transaction, outcome).await?;
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
            .map(Self::validated_info)
            .transpose()
    }

    async fn source_history_start(
        &self,
        available_through: DateTime<Utc>,
    ) -> Result<Option<DateTime<Utc>>, StorageError> {
        QuantRecommendationEntity::find()
            .select_only()
            .column_as(
                QuantRecommendationColumn::CreatedAt.min(),
                "source_history_start",
            )
            .filter(QuantRecommendationColumn::CreatedAt.lte(available_through))
            .into_tuple::<Option<DateTime<Utc>>>()
            .one(&self.db)
            .await
            .map_err(StorageError::from)
            .map(Option::flatten)
    }

    async fn claim_reconciliation(
        &self,
        available_through: DateTime<Utc>,
        worker_id: WorkerId,
        lease_secs: u64,
        limit: u64,
    ) -> Result<Vec<ResolutionOutcomeTaskClaim>, StorageError> {
        let lease = validate_claim(lease_secs, limit)?;
        let transaction = self.db.begin().await.map_err(StorageError::from)?;
        Self::materialize_tasks(&transaction, available_through, limit).await?;
        let now = statement_timestamp(&transaction).await?;
        let due = Condition::any()
            .add(TaskColumn::Status.eq(OutcomeReconciliationTaskStatus::Pending))
            .add(
                Condition::all()
                    .add(TaskColumn::Status.eq(OutcomeReconciliationTaskStatus::Retrying))
                    .add(TaskColumn::NextAttemptAt.lte(now)),
            )
            .add(
                Condition::all()
                    .add(TaskColumn::Status.eq(OutcomeReconciliationTaskStatus::Delivering))
                    .add(TaskColumn::LeaseExpiresAt.lte(now)),
            );
        let rows = TaskEntity::find()
            .filter(TaskColumn::ReadyAt.lte(available_through))
            .filter(due)
            .order_by_asc(TaskColumn::ReadyAt)
            .order_by_asc(TaskColumn::RecommendationId)
            .limit(limit)
            .lock_with_behavior(LockType::Update, LockBehavior::SkipLocked)
            .all(&transaction)
            .await
            .map_err(StorageError::from)?;
        let recommendation_ids = rows
            .iter()
            .map(|row| row.recommendation_id)
            .collect::<Vec<_>>();
        let markets = QuantRecommendationEntity::find()
            .select_only()
            .column(QuantRecommendationColumn::RecommendationId)
            .column(QuantRecommendationColumn::MarketId)
            .filter(QuantRecommendationColumn::RecommendationId.is_in(recommendation_ids))
            .into_tuple::<(RecommendationId, MarketId)>()
            .all(&transaction)
            .await
            .map_err(StorageError::from)?
            .into_iter()
            .collect::<HashMap<_, _>>();
        let mut claims = Vec::with_capacity(rows.len());
        for row in rows {
            let attempt_count = row
                .attempt_count
                .checked_add(1)
                .ok_or_else(|| queue_invariant("resolution reconciliation count overflow"))?;
            let mut active = row.clone().into_active_model();
            active.status = ActiveValue::Set(OutcomeReconciliationTaskStatus::Delivering);
            active.attempt_count = ActiveValue::Set(attempt_count);
            active.claim_owner = ActiveValue::Set(Some(worker_id));
            active.lease_expires_at = ActiveValue::Set(Some(now + lease));
            active.next_attempt_at = ActiveValue::Set(None);
            active.updated_at = ActiveValue::Set(now);
            active
                .update(&transaction)
                .await
                .map_err(StorageError::from)?;
            let market_id = markets
                .get(&row.recommendation_id)
                .cloned()
                .ok_or_else(|| queue_invariant("resolution task lost its recommendation"))?;
            claims.push(ResolutionOutcomeTaskClaim {
                candidate: RecommendationResolutionReconciliationCandidate {
                    recommendation_id: row.recommendation_id,
                    market_id,
                },
                attempt_count,
            });
        }
        transaction.commit().await.map_err(StorageError::from)?;
        Ok(claims)
    }

    async fn settle_reconciliation(
        &self,
        recommendation_id: RecommendationId,
        worker_id: WorkerId,
        settlement: OutcomeTaskSettlement,
    ) -> Result<(), StorageError> {
        let transaction = self.db.begin().await.map_err(StorageError::from)?;
        let row = TaskEntity::find_by_id(recommendation_id)
            .lock_exclusive()
            .one(&transaction)
            .await
            .map_err(StorageError::from)?
            .ok_or_else(|| {
                StorageError::not_found(
                    QUANT_RESOLUTION_OUTCOME_RECONCILIATION_TASK,
                    recommendation_id,
                )
            })?;
        if row.status != OutcomeReconciliationTaskStatus::Delivering
            || row.claim_owner != Some(worker_id)
        {
            return Err(StorageError::state_conflict(
                QUANT_RESOLUTION_OUTCOME_RECONCILIATION_TASK,
                Some(recommendation_id),
                "resolution task is not leased by the settling worker",
            ));
        }
        let now = statement_timestamp(&transaction).await?;
        let mut active = row.into_active_model();
        active.claim_owner = ActiveValue::Set(None);
        active.lease_expires_at = ActiveValue::Set(None);
        active.updated_at = ActiveValue::Set(now);
        match settlement {
            OutcomeTaskSettlement::Completed => {
                active.status = ActiveValue::Set(OutcomeReconciliationTaskStatus::Completed);
                active.next_attempt_at = ActiveValue::Set(None);
                active.last_error = ActiveValue::Set(None);
                active.completed_at = ActiveValue::Set(Some(now));
            }
            OutcomeTaskSettlement::RetryAfter { delay_secs, error } => {
                let delay = validate_retry(delay_secs, &error)?;
                active.status = ActiveValue::Set(OutcomeReconciliationTaskStatus::Retrying);
                active.next_attempt_at = ActiveValue::Set(Some(now + delay));
                active.last_error = ActiveValue::Set(Some(error));
                active.completed_at = ActiveValue::Set(None);
            }
        }
        active
            .update(&transaction)
            .await
            .map_err(StorageError::from)?;
        transaction.commit().await.map_err(StorageError::from)
    }

    async fn list_available_page(
        &self,
        query: RecommendationResolutionOutcomePageQuery,
    ) -> Result<RecommendationResolutionOutcomePage, StorageError> {
        query.validate().map_err(|error| {
            StorageError::invariant_violation(Some(QUANT_RECOMMENDATION_RESOLUTION_OUTCOME), error)
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
            .map(Self::validated_info)
            .collect::<Result<Vec<_>, _>>()?;
        Ok(RecommendationResolutionOutcomePage::new(outcomes))
    }
}

impl PgRecommendationResolutionOutcomeRepository {
    async fn materialize_tasks(
        transaction: &DatabaseTransaction,
        available_through: DateTime<Utc>,
        limit: u64,
    ) -> Result<u64, StorageError> {
        let recommendation_ids = QuantRecommendationEntity::find()
            .select_only()
            .column(QuantRecommendationColumn::RecommendationId)
            .join(
                JoinType::InnerJoin,
                QuantRecommendationRelation::Market.def(),
            )
            .join(
                JoinType::LeftJoin,
                QuantRecommendationRelation::ResolutionOutcome.def(),
            )
            .join(
                JoinType::LeftJoin,
                QuantRecommendationRelation::ResolutionOutcomeReconciliationTask.def(),
            )
            .filter(MarketColumn::Status.eq(MarketStatus::Settled))
            .filter(Column::RecommendationId.is_null())
            .filter(TaskColumn::RecommendationId.is_null())
            .filter(QuantRecommendationColumn::CreatedAt.lte(available_through))
            .order_by_asc(QuantRecommendationColumn::RecommendationId)
            .limit(limit)
            .into_tuple::<RecommendationId>()
            .all(transaction)
            .await
            .map_err(StorageError::from)?;
        let inserted = u64::try_from(recommendation_ids.len())
            .map_err(|error| queue_invariant(format!("task batch size overflow: {error}")))?;
        let now = statement_timestamp(transaction).await?;
        for recommendation_id in recommendation_ids {
            TaskEntity::insert(TaskActiveModel {
                recommendation_id: ActiveValue::Set(recommendation_id),
                ready_at: ActiveValue::Set(available_through),
                status: ActiveValue::Set(OutcomeReconciliationTaskStatus::Pending),
                attempt_count: ActiveValue::Set(0),
                claim_owner: ActiveValue::Set(None),
                lease_expires_at: ActiveValue::Set(None),
                next_attempt_at: ActiveValue::Set(None),
                last_error: ActiveValue::Set(None),
                completed_at: ActiveValue::Set(None),
                created_at: ActiveValue::Set(now),
                updated_at: ActiveValue::Set(now),
            })
            .on_conflict(
                OnConflict::column(TaskColumn::RecommendationId)
                    .do_nothing()
                    .to_owned(),
            )
            .exec_without_returning(transaction)
            .await
            .map_err(StorageError::from)?;
        }
        Ok(inserted)
    }
}

fn validate_claim(lease_secs: u64, limit: u64) -> Result<Duration, StorageError> {
    if lease_secs == 0 || lease_secs > MAX_LEASE_SECS {
        return Err(queue_invariant(format!(
            "lease_secs must be within 1..={MAX_LEASE_SECS}"
        )));
    }
    if limit == 0 || limit > MAX_QUEUE_BATCH {
        return Err(queue_invariant(format!(
            "claim limit must be within 1..={MAX_QUEUE_BATCH}"
        )));
    }
    let seconds = i64::try_from(lease_secs)
        .map_err(|error| queue_invariant(format!("lease seconds overflow: {error}")))?;
    Ok(Duration::seconds(seconds))
}

fn validate_retry(delay_secs: u64, error: &str) -> Result<Duration, StorageError> {
    if delay_secs == 0
        || delay_secs > MAX_RETRY_SECS
        || error.trim().is_empty()
        || error.chars().count() > MAX_ERROR_CHARS
    {
        return Err(queue_invariant(format!(
            "retry delay must be within 1..={MAX_RETRY_SECS} and error within 1..={MAX_ERROR_CHARS} characters"
        )));
    }
    let seconds = i64::try_from(delay_secs)
        .map_err(|error| queue_invariant(format!("retry delay overflow: {error}")))?;
    Ok(Duration::seconds(seconds))
}

fn queue_invariant(detail: impl Display) -> StorageError {
    StorageError::invariant_violation(Some(QUANT_RESOLUTION_OUTCOME_RECONCILIATION_TASK), detail)
}

fn derive_outcome(
    recommendation: &QuantRecommendationModel,
    fact: &MarketResolutionRow,
) -> Result<NewRecommendationResolutionOutcome, StorageError> {
    if recommendation.market_id != fact.market_id {
        return Err(StorageError::invariant_violation(
            Some(QUANT_RECOMMENDATION_RESOLUTION_OUTCOME),
            format!(
                "recommendation {} is bound to market {}, not resolution market {}",
                recommendation.recommendation_id, recommendation.market_id, fact.market_id
            ),
        ));
    }
    let token_payout_ratio = fact.payout_for(&recommendation.token_id).map_err(|error| {
        StorageError::invariant_violation(Some(QUANT_RECOMMENDATION_RESOLUTION_OUTCOME), error)
    })?;
    let resolution_kind = fact.resolution_kind().map_err(|error| {
        StorageError::invariant_violation(Some(QUANT_RECOMMENDATION_RESOLUTION_OUTCOME), error)
    })?;
    let resolved_at = Utc
        .timestamp_millis_opt(fact.resolved_at)
        .single()
        .ok_or_else(|| {
            StorageError::invariant_violation(
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
            StorageError::invariant_violation(
                Some(QUANT_RECOMMENDATION_RESOLUTION_OUTCOME),
                format!(
                    "resolution observation timestamp {} is outside the UTC millisecond range",
                    fact.observed_at
                ),
            )
        })?;
    let resolution_fact_log_index = i64::try_from(fact.source_log_index).map_err(|_| {
        StorageError::invariant_violation(
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

impl PgRecommendationResolutionOutcomeRepository {
    async fn insert_derived(
        transaction: &DatabaseTransaction,
        outcome: NewRecommendationResolutionOutcome,
    ) -> Result<InsertResolutionOutcomeResult, StorageError> {
        validate_new(&outcome)?;
        let available_at = statement_timestamp(transaction).await?;
        let outcome_hash = outcome
            .expected_outcome_hash(available_at)
            .map_err(|error| {
                StorageError::invariant_violation(
                    Some(QUANT_RECOMMENDATION_RESOLUTION_OUTCOME),
                    error,
                )
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
                        StorageError::invariant_violation(
                            Some(QUANT_RECOMMENDATION_RESOLUTION_OUTCOME),
                            "outcome conflict completed without an observable stored row",
                        )
                    })?,
                false,
            ),
        };
        let stored = Self::validated_info(row)?;
        if !stored.has_same_derivation(&outcome) {
            return Err(StorageError::state_conflict(
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
}

fn validate_new(outcome: &NewRecommendationResolutionOutcome) -> Result<(), StorageError> {
    outcome.validate().map_err(|error| {
        StorageError::invariant_violation(Some(QUANT_RECOMMENDATION_RESOLUTION_OUTCOME), error)
    })
}

impl PgRecommendationResolutionOutcomeRepository {
    fn validated_info(
        row: QuantRecommendationResolutionOutcomeModel,
    ) -> Result<RecommendationResolutionOutcomeInfo, StorageError> {
        let outcome: RecommendationResolutionOutcomeInfo = row.into();
        outcome.validate().map_err(|error| {
            StorageError::invariant_violation(
                Some(QUANT_RECOMMENDATION_RESOLUTION_OUTCOME),
                format!("stored outcome failed integrity validation: {error}"),
            )
        })?;
        Ok(outcome)
    }
}
