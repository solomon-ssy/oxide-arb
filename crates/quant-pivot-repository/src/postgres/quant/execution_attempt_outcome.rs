//! `PostgreSQL` WORM execution-attempt outcome repository.

use std::{collections::HashMap, fmt::Display};

use chrono::{DateTime, Duration, Utc};
use quant_pivot_error::storage::{
    StorageError,
    entity::{
        QUANT_EXECUTION_ATTEMPT_OUTCOME, QUANT_EXECUTION_ATTEMPT_RECONCILIATION_TASK,
        QUANT_ORDER_INTENT, QUANT_RECOMMENDATION,
    },
};
use quant_pivot_models::{
    domain::quant::{
        AccountExecutionFeeFact, ExecutionAttemptBarrier, ExecutionAttemptDeferredReason,
        ExecutionAttemptDerivation, ExecutionAttemptOutcomeInfo,
        ExecutionAttemptReconciliationCandidate, ExecutionAttemptReconciliationError,
        ExecutionAttemptReconciliationResult, ExecutionAttemptSourceGraph,
        ExecutionAttemptTaskClaim, ExecutionOrderInfo, NewExecutionAttemptOutcome, OrderIntentInfo,
        OutcomeTaskSettlement, StrategyPositionLot, settlement::SettlementRedeemLotInfo,
    },
    entities::{
        quant_account_chain_execution::{
            Column as AccountExecutionColumn, Entity as AccountExecutionEntity,
        },
        quant_account_execution_association::{
            Column as AssociationColumn, Entity as AssociationEntity,
        },
        quant_execution_attempt_outcome::{
            Column, Entity as QuantExecutionAttemptOutcomeEntity,
            Model as QuantExecutionAttemptOutcomeModel,
        },
        quant_execution_attempt_reconciliation_task::{
            ActiveModel as TaskActiveModel, Column as TaskColumn, Entity as TaskEntity,
        },
        quant_execution_order::{
            Column as QuantExecutionOrderColumn, Entity as QuantExecutionOrderEntity,
            Relation as QuantExecutionOrderRelation,
        },
        quant_order_intent::{
            Column as QuantOrderIntentColumn, Entity as QuantOrderIntentEntity,
            Relation as QuantOrderIntentRelation,
        },
        quant_recommendation::Entity as QuantRecommendationEntity,
        quant_reconciliation::{
            Column as QuantReconciliationColumn, Entity as QuantReconciliationEntity,
        },
        quant_settlement_redeem_lot::{
            Column as QuantSettlementRedeemLotColumn, Entity as QuantSettlementRedeemLotEntity,
        },
        quant_strategy_position_lot::{
            Column as QuantPositionColumn, Entity as QuantPositionEntity,
        },
    },
    enums::{
        execution::{ExecutionOrderPhase, PositionLedgerState, ReconciliationResult},
        quant::{ExecutionOrderState, OutcomeReconciliationTaskStatus},
    },
    types::{OrderIntentId, RecommendationId, WorkerId},
};
use sea_orm::{
    ActiveModelTrait, ActiveValue, ColumnTrait, Condition, DatabaseConnection, DatabaseTransaction,
    DbErr, EntityTrait, IntoActiveModel, IsolationLevel, JoinType, PaginatorTrait, QueryFilter,
    QueryOrder, QuerySelect, RelationTrait, TransactionTrait,
    sea_query::{LockBehavior, LockType, OnConflict},
};

use crate::{
    postgres::primitives::{self, statement_timestamp},
    traits::ExecutionAttemptOutcomeRepository,
};

const MAX_ERROR_CHARS: usize = 4_096;
const MAX_LEASE_SECS: u64 = 3_600;
const MAX_QUEUE_BATCH: u64 = 4_096;
const MAX_RETRY_SECS: u64 = 86_400;

/// PostgreSQL-backed immutable execution-attempt outcome repository.
pub struct PgExecutionAttemptOutcomeRepository {
    db: DatabaseConnection,
}

impl PgExecutionAttemptOutcomeRepository {
    pub const fn new(db: DatabaseConnection) -> Self {
        Self { db }
    }
}

#[async_trait::async_trait]
impl ExecutionAttemptOutcomeRepository for PgExecutionAttemptOutcomeRepository {
    async fn reconcile_intent(
        &self,
        order_intent_id: &OrderIntentId,
        available_through: DateTime<Utc>,
    ) -> Result<ExecutionAttemptReconciliationResult, StorageError> {
        let transaction = self.db.begin().await.map_err(StorageError::from)?;
        let graph = Self::load_source_graph(&transaction, order_intent_id).await?;
        let source_observed_at = graph.source_observed_at();
        if source_observed_at > available_through {
            transaction.commit().await.map_err(StorageError::from)?;
            return Ok(ExecutionAttemptReconciliationResult::Deferred(
                ExecutionAttemptDeferredReason::SourceAvailableAfterCutoff,
            ));
        }
        let outcome = match graph.derive().map_err(|error| source_graph_error(&error))? {
            ExecutionAttemptDerivation::Ready(outcome) => *outcome,
            ExecutionAttemptDerivation::Deferred(reason) => {
                transaction.commit().await.map_err(StorageError::from)?;
                return Ok(ExecutionAttemptReconciliationResult::Deferred(reason));
            }
        };
        validate_new(&outcome)?;
        let inserted = Self::insert_derived(&transaction, outcome, source_observed_at).await?;
        transaction.commit().await.map_err(StorageError::from)?;
        Ok(inserted)
    }

    async fn find_by_intent(
        &self,
        order_intent_id: &OrderIntentId,
    ) -> Result<Option<ExecutionAttemptOutcomeInfo>, StorageError> {
        QuantExecutionAttemptOutcomeEntity::find_by_id(*order_intent_id)
            .one(&self.db)
            .await
            .map_err(StorageError::from)?
            .map(Self::validated_info)
            .transpose()
    }

    async fn list_by_recommendations(
        &self,
        recommendation_ids: &[RecommendationId],
        cutoff: DateTime<Utc>,
    ) -> Result<Vec<ExecutionAttemptOutcomeInfo>, StorageError> {
        if recommendation_ids.is_empty() {
            return Ok(Vec::new());
        }
        QuantExecutionAttemptOutcomeEntity::find()
            .filter(Column::RecommendationId.is_in(recommendation_ids.iter().copied()))
            .filter(Column::AvailableAt.lte(cutoff))
            .order_by_asc(Column::RecommendationId)
            .order_by_asc(Column::TerminalAt)
            .order_by_asc(Column::OrderIntentId)
            .all(&self.db)
            .await
            .map_err(StorageError::from)?
            .into_iter()
            .map(Self::validated_info)
            .collect()
    }

    async fn claim_reconciliation(
        &self,
        available_through: DateTime<Utc>,
        worker_id: WorkerId,
        lease_secs: u64,
        limit: u64,
    ) -> Result<Vec<ExecutionAttemptTaskClaim>, StorageError> {
        let lease = validate_claim(lease_secs, limit)?;
        let transaction = self.db.begin().await.map_err(StorageError::from)?;
        Self::materialize_tasks(&transaction, available_through, limit).await?;
        let now = primitives::statement_timestamp(&transaction).await?;
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
            .order_by_asc(TaskColumn::OrderIntentId)
            .limit(limit)
            .lock_with_behavior(LockType::Update, LockBehavior::SkipLocked)
            .all(&transaction)
            .await
            .map_err(StorageError::from)?;
        let intent_ids = rows
            .iter()
            .map(|row| row.order_intent_id)
            .collect::<Vec<_>>();
        let recommendations = QuantOrderIntentEntity::find()
            .select_only()
            .column(QuantOrderIntentColumn::OrderIntentId)
            .column(QuantOrderIntentColumn::RecommendationId)
            .filter(QuantOrderIntentColumn::OrderIntentId.is_in(intent_ids))
            .into_tuple::<(OrderIntentId, RecommendationId)>()
            .all(&transaction)
            .await
            .map_err(StorageError::from)?
            .into_iter()
            .collect::<HashMap<_, _>>();
        let mut claims = Vec::with_capacity(rows.len());
        for row in rows {
            let attempt_count = row.attempt_count.checked_add(1).ok_or_else(|| {
                queue_invariant("execution-attempt reconciliation count overflow")
            })?;
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
            let recommendation_id = recommendations
                .get(&row.order_intent_id)
                .copied()
                .ok_or_else(|| queue_invariant("task lost its order-intent relation"))?;
            claims.push(ExecutionAttemptTaskClaim {
                candidate: ExecutionAttemptReconciliationCandidate {
                    order_intent_id: row.order_intent_id,
                    recommendation_id,
                },
                attempt_count,
            });
        }
        transaction.commit().await.map_err(StorageError::from)?;
        Ok(claims)
    }

    async fn settle_reconciliation(
        &self,
        order_intent_id: OrderIntentId,
        worker_id: WorkerId,
        settlement: OutcomeTaskSettlement,
    ) -> Result<(), StorageError> {
        let transaction = self.db.begin().await.map_err(StorageError::from)?;
        let row = TaskEntity::find_by_id(order_intent_id)
            .lock_exclusive()
            .one(&transaction)
            .await
            .map_err(StorageError::from)?
            .ok_or_else(|| {
                StorageError::not_found(
                    QUANT_EXECUTION_ATTEMPT_RECONCILIATION_TASK,
                    order_intent_id,
                )
            })?;
        if row.status != OutcomeReconciliationTaskStatus::Delivering
            || row.claim_owner != Some(worker_id)
        {
            return Err(StorageError::state_conflict(
                QUANT_EXECUTION_ATTEMPT_RECONCILIATION_TASK,
                Some(order_intent_id),
                "reconciliation task is not leased by the settling worker",
            ));
        }
        let now = primitives::statement_timestamp(&transaction).await?;
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

    async fn barrier(
        &self,
        cutoff: DateTime<Utc>,
    ) -> Result<ExecutionAttemptBarrier, StorageError> {
        let transaction = self
            .db
            .begin_with_config(Some(IsolationLevel::RepeatableRead), None)
            .await
            .map_err(StorageError::from)?;
        loop {
            let inserted = Self::materialize_tasks(&transaction, cutoff, MAX_QUEUE_BATCH).await?;
            if inserted < MAX_QUEUE_BATCH {
                break;
            }
        }
        let unsealed = TaskEntity::find()
            .filter(TaskColumn::ReadyAt.lte(cutoff))
            .filter(TaskColumn::Status.ne(OutcomeReconciliationTaskStatus::Completed));
        let eligible_unsealed_count = unsealed
            .clone()
            .count(&transaction)
            .await
            .map_err(StorageError::from)?;
        let oldest_unsealed_at = unsealed
            .order_by_asc(TaskColumn::ReadyAt)
            .one(&transaction)
            .await
            .map_err(StorageError::from)?
            .map(|task| task.ready_at);
        transaction.commit().await.map_err(StorageError::from)?;
        Ok(ExecutionAttemptBarrier {
            cutoff,
            eligible_unsealed_count,
            sealed_through: oldest_unsealed_at.unwrap_or(cutoff),
        })
    }
}

impl PgExecutionAttemptOutcomeRepository {
    async fn materialize_tasks(
        transaction: &DatabaseTransaction,
        available_through: DateTime<Utc>,
        limit: u64,
    ) -> Result<u64, StorageError> {
        let source_terminal = Condition::any()
            .add(QuantReconciliationColumn::Result.is_in([
                ReconciliationResult::NotFilled,
                ReconciliationResult::Cancelled,
            ]))
            .add(
                QuantPositionColumn::State
                    .is_in([PositionLedgerState::Closed, PositionLedgerState::Settled]),
            );
        let rows = QuantOrderIntentEntity::find()
            .select_only()
            .column(QuantOrderIntentColumn::OrderIntentId)
            .column(QuantOrderIntentColumn::RecommendationId)
            .join(
                JoinType::InnerJoin,
                QuantOrderIntentRelation::ExecutionOrder.def(),
            )
            .join(
                JoinType::InnerJoin,
                QuantExecutionOrderRelation::Reconciliation.def(),
            )
            .join(JoinType::LeftJoin, QuantOrderIntentRelation::Position.def())
            .join(
                JoinType::LeftJoin,
                QuantOrderIntentRelation::ExecutionOutcome.def(),
            )
            .join(
                JoinType::LeftJoin,
                QuantOrderIntentRelation::ExecutionReconciliationTask.def(),
            )
            .filter(QuantExecutionOrderColumn::OrderPhase.eq(ExecutionOrderPhase::Entry))
            .filter(QuantExecutionOrderColumn::SubmittedAt.is_not_null())
            .filter(QuantExecutionOrderColumn::State.is_in([
                ExecutionOrderState::Filled,
                ExecutionOrderState::PartiallyFilled,
                ExecutionOrderState::Cancelled,
                ExecutionOrderState::Failed,
            ]))
            .filter(QuantReconciliationColumn::ResolvedAt.is_not_null())
            .filter(QuantReconciliationColumn::Result.is_in([
                ReconciliationResult::Filled,
                ReconciliationResult::NotFilled,
                ReconciliationResult::PartiallyFilled,
                ReconciliationResult::Cancelled,
            ]))
            .filter(source_terminal)
            .filter(Column::OrderIntentId.is_null())
            .filter(TaskColumn::OrderIntentId.is_null())
            .filter(QuantOrderIntentColumn::CreatedAt.lte(available_through))
            .filter(QuantOrderIntentColumn::UpdatedAt.lte(available_through))
            .filter(QuantExecutionOrderColumn::UpdatedAt.lte(available_through))
            .filter(QuantReconciliationColumn::UpdatedAt.lte(available_through))
            .filter(
                Condition::any()
                    .add(QuantPositionColumn::UpdatedAt.is_null())
                    .add(QuantPositionColumn::UpdatedAt.lte(available_through)),
            )
            .order_by_asc(QuantOrderIntentColumn::OrderIntentId)
            .distinct()
            .limit(limit)
            .into_tuple::<(OrderIntentId, RecommendationId)>()
            .all(transaction)
            .await
            .map_err(StorageError::from)?;
        let inserted = u64::try_from(rows.len())
            .map_err(|error| queue_invariant(format!("task batch size overflow: {error}")))?;
        let now = primitives::statement_timestamp(transaction).await?;
        let ready_at = primitives::queue_ready_at(available_through, now);
        for (order_intent_id, _) in rows {
            TaskEntity::insert(TaskActiveModel {
                order_intent_id: ActiveValue::Set(order_intent_id),
                ready_at: ActiveValue::Set(ready_at),
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
                OnConflict::column(TaskColumn::OrderIntentId)
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

impl PgExecutionAttemptOutcomeRepository {
    async fn load_source_graph(
        transaction: &DatabaseTransaction,
        order_intent_id: &OrderIntentId,
    ) -> Result<ExecutionAttemptSourceGraph, StorageError> {
        let intent = QuantOrderIntentEntity::find_by_id(*order_intent_id)
            .lock_shared()
            .one(transaction)
            .await
            .map_err(StorageError::from)?
            .ok_or_else(|| StorageError::not_found(QUANT_ORDER_INTENT, order_intent_id))?;
        let recommendation = QuantRecommendationEntity::find_by_id(intent.recommendation_id)
            .lock_shared()
            .one(transaction)
            .await
            .map_err(StorageError::from)?
            .ok_or_else(|| {
                StorageError::not_found(QUANT_RECOMMENDATION, intent.recommendation_id)
            })?;
        let orders: Vec<ExecutionOrderInfo> = QuantExecutionOrderEntity::find()
            .filter(QuantExecutionOrderColumn::OrderIntentId.eq(*order_intent_id))
            .order_by_asc(QuantExecutionOrderColumn::CreatedAt)
            .order_by_asc(QuantExecutionOrderColumn::ExecutionOrderId)
            .lock_shared()
            .all(transaction)
            .await
            .map_err(StorageError::from)?
            .into_iter()
            .map(Into::into)
            .collect();
        let execution_order_ids = orders
            .iter()
            .map(|order| order.execution_order_id)
            .collect::<Vec<_>>();
        let associations = AssociationEntity::find()
            .filter(AssociationColumn::ExecutionOrderId.is_in(execution_order_ids))
            .lock_shared()
            .all(transaction)
            .await
            .map_err(StorageError::from)?;
        let order_by_execution = associations
            .iter()
            .filter_map(|association| {
                association
                    .execution_order_id
                    .map(|order_id| (association.account_chain_execution_id, order_id))
            })
            .collect::<HashMap<_, _>>();
        let account_execution_fees = if order_by_execution.is_empty() {
            Vec::new()
        } else {
            AccountExecutionEntity::find()
                .filter(
                    AccountExecutionColumn::AccountChainExecutionId
                        .is_in(order_by_execution.keys().copied()),
                )
                .lock_shared()
                .all(transaction)
                .await
                .map_err(StorageError::from)?
                .into_iter()
                .map(|execution| {
                    let execution_order_id = order_by_execution
                        .get(&execution.account_chain_execution_id)
                        .copied()
                        .ok_or_else(|| {
                            source_invariant("account execution association disappeared")
                        })?;
                    let exact_fee_usd = execution.exact_fee_usd.ok_or_else(|| {
                        source_invariant("system-associated account execution has no exact fee")
                    })?;
                    Ok(AccountExecutionFeeFact {
                        account_chain_execution_id: execution.account_chain_execution_id,
                        execution_order_id,
                        exact_fee_usd,
                        source_event_hash: execution.source_event_hash,
                        available_at: execution.available_at,
                    })
                })
                .collect::<Result<Vec<_>, StorageError>>()?
        };
        let reconciliations = QuantReconciliationEntity::find()
            .filter(QuantReconciliationColumn::OrderIntentId.eq(*order_intent_id))
            .order_by_asc(QuantReconciliationColumn::CreatedAt)
            .order_by_asc(QuantReconciliationColumn::ReconciliationId)
            .lock_shared()
            .all(transaction)
            .await
            .map_err(StorageError::from)?
            .into_iter()
            .map(Into::into)
            .collect();
        let position: Option<StrategyPositionLot> = QuantPositionEntity::find()
            .filter(QuantPositionColumn::OrderIntentId.eq(*order_intent_id))
            .lock_shared()
            .one(transaction)
            .await
            .map_err(StorageError::from)?
            .map(Into::into);
        let settlement_lots = QuantSettlementRedeemLotEntity::find()
            .filter(QuantSettlementRedeemLotColumn::OrderIntentId.eq(*order_intent_id))
            .order_by_asc(QuantSettlementRedeemLotColumn::CreatedAt)
            .order_by_asc(QuantSettlementRedeemLotColumn::SettlementRedeemLotId)
            .lock_shared()
            .all(transaction)
            .await
            .map_err(StorageError::from)?;
        if settlement_lots.len() > 1 {
            return Err(source_invariant(
                "one execution intent cannot have multiple settlement outcome lots",
            ));
        }
        Ok(ExecutionAttemptSourceGraph {
            recommendation_id: recommendation.recommendation_id,
            market_id: recommendation.market_id,
            token_id: recommendation.token_id,
            intent: OrderIntentInfo::from(intent),
            orders,
            reconciliations,
            account_execution_fees,
            position,
            settlement_lot: settlement_lots
                .into_iter()
                .next()
                .map(SettlementRedeemLotInfo::from),
        })
    }
}

impl PgExecutionAttemptOutcomeRepository {
    async fn insert_derived(
        transaction: &DatabaseTransaction,
        outcome: NewExecutionAttemptOutcome,
        source_observed_at: DateTime<Utc>,
    ) -> Result<ExecutionAttemptReconciliationResult, StorageError> {
        let available_at = statement_timestamp(transaction).await?;
        let outcome_hash = outcome
            .expected_outcome_hash(source_observed_at, available_at)
            .map_err(|error| {
                StorageError::invariant_violation(Some(QUANT_EXECUTION_ATTEMPT_OUTCOME), error)
            })?;
        let mut active_outcome = outcome.clone().into_active_model();
        active_outcome.source_observed_at = ActiveValue::Set(source_observed_at);
        active_outcome.available_at = ActiveValue::Set(available_at);
        active_outcome.outcome_hash = ActiveValue::Set(outcome_hash);
        let inserted = match QuantExecutionAttemptOutcomeEntity::insert(active_outcome)
            .on_conflict(
                OnConflict::column(Column::OrderIntentId)
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
                QuantExecutionAttemptOutcomeEntity::find_by_id(outcome.order_intent_id)
                    .one(transaction)
                    .await
                    .map_err(StorageError::from)?
                    .ok_or_else(|| {
                        source_invariant(
                            "outcome conflict completed without an observable stored row",
                        )
                    })?,
                false,
            ),
        };
        let stored = Self::validated_info(row)?;
        if !stored.has_same_derivation(&outcome) {
            return Err(StorageError::state_conflict(
                QUANT_EXECUTION_ATTEMPT_OUTCOME,
                Some(outcome.order_intent_id),
                "order intent is already bound to different immutable execution content",
            ));
        }
        if was_inserted {
            Ok(ExecutionAttemptReconciliationResult::Inserted(stored))
        } else {
            Ok(ExecutionAttemptReconciliationResult::AlreadyPresent(stored))
        }
    }
}

fn source_graph_error(error: &ExecutionAttemptReconciliationError) -> StorageError {
    StorageError::invariant_violation(Some(QUANT_EXECUTION_ATTEMPT_OUTCOME), error.to_string())
}

fn validate_new(outcome: &NewExecutionAttemptOutcome) -> Result<(), StorageError> {
    outcome.validate().map_err(|error| {
        StorageError::invariant_violation(Some(QUANT_EXECUTION_ATTEMPT_OUTCOME), error)
    })
}

impl PgExecutionAttemptOutcomeRepository {
    fn validated_info(
        row: QuantExecutionAttemptOutcomeModel,
    ) -> Result<ExecutionAttemptOutcomeInfo, StorageError> {
        let outcome: ExecutionAttemptOutcomeInfo = row.into();
        outcome.validate().map_err(|error| {
            StorageError::invariant_violation(
                Some(QUANT_EXECUTION_ATTEMPT_OUTCOME),
                format!("stored outcome failed integrity validation: {error}"),
            )
        })?;
        Ok(outcome)
    }
}

fn source_invariant(detail: &'static str) -> StorageError {
    StorageError::invariant_violation(Some(QUANT_EXECUTION_ATTEMPT_OUTCOME), detail.to_owned())
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
    StorageError::invariant_violation(Some(QUANT_EXECUTION_ATTEMPT_RECONCILIATION_TASK), detail)
}
