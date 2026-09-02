//! `PostgreSQL` final recommendation execution rollup.

use std::{
    collections::{HashMap, HashSet},
    fmt::Display,
};

use chrono::{DateTime, Duration, Utc};
use quant_pivot_error::storage::{
    StorageError,
    entity::{
        QUANT_EXECUTION_ATTEMPT_OUTCOME, QUANT_EXECUTION_ROLLUP_RECONCILIATION_TASK,
        QUANT_RECOMMENDATION, QUANT_RECOMMENDATION_EXECUTION_ROLLUP,
    },
};
use quant_pivot_models::{
    domain::quant::{
        ExecutionAttemptOutcomeInfo, ExecutionRollupBarrier, ExecutionRollupDeferredReason,
        ExecutionRollupReconciliationResult, ExecutionRollupTaskClaim,
        NewRecommendationExecutionRollup, NewRecommendationExecutionRollupAttempt,
        OutcomeTaskSettlement, RecommendationExecutionRollupAttemptInfo,
        RecommendationExecutionRollupInfo,
    },
    entities::{
        quant_execution_attempt_outcome::{
            Column as AttemptColumn, Entity as AttemptEntity, Model as AttemptModel,
        },
        quant_execution_order::{
            Column as OrderColumn, Entity as OrderEntity, Model as OrderModel,
        },
        quant_execution_rollup_reconciliation_task::{
            ActiveModel as TaskActiveModel, Column as TaskColumn, Entity as TaskEntity,
        },
        quant_order_intent::{
            Column as IntentColumn, Entity as IntentEntity, Model as IntentModel,
        },
        quant_recommendation::{
            Column as RecommendationColumn, Entity as RecommendationEntity,
            Relation as RecommendationRelation,
        },
        quant_recommendation_execution_rollup::{
            ActiveModel as RollupActiveModel, Column as RollupColumn, Entity as RollupEntity,
            Model as RollupModel,
        },
        quant_recommendation_execution_rollup_attempt::{
            ActiveModel as BindingActiveModel, Column as BindingColumn, Entity as BindingEntity,
        },
        quant_strategy_position_lot::{
            Column as PositionColumn, Entity as PositionEntity, Model as PositionModel,
        },
    },
    enums::{
        execution::{ExecutionOrderPhase, PositionLedgerState},
        quant::{OutcomeReconciliationTaskStatus, RecommendationStatus},
    },
    types::{ContentHash, OrderIntentId, RecommendationId, WorkerId},
};
use sea_orm::{
    ActiveModelTrait,
    ActiveValue::Set,
    ColumnTrait, Condition, DatabaseConnection, DatabaseTransaction, DbErr, EntityTrait,
    IntoActiveModel, IsolationLevel, JoinType, PaginatorTrait, QueryFilter, QueryOrder,
    QuerySelect, RelationTrait, TransactionTrait,
    sea_query::{LockBehavior, LockType, OnConflict},
};

use crate::{postgres::primitives, traits::RecommendationExecutionRollupRepository};

const MAX_ERROR_CHARS: usize = 4_096;
const MAX_LEASE_SECS: u64 = 3_600;
const MAX_QUEUE_BATCH: u64 = 4_096;
const MAX_RETRY_SECS: u64 = 86_400;

/// `PostgreSQL` owner for final recommendation-level execution truth.
pub struct PgRecommendationExecutionRollupRepository {
    db: DatabaseConnection,
}

impl PgRecommendationExecutionRollupRepository {
    #[must_use]
    pub const fn new(db: DatabaseConnection) -> Self {
        Self { db }
    }

    fn rollup_info(row: RollupModel) -> Result<RecommendationExecutionRollupInfo, StorageError> {
        let info: RecommendationExecutionRollupInfo = row.into();
        info.validate().map_err(|error| {
            StorageError::invariant_violation(
                Some(QUANT_RECOMMENDATION_EXECUTION_ROLLUP),
                format!("stored rollup failed validation: {error}"),
            )
        })?;
        Ok(info)
    }

    fn attempt_info(row: AttemptModel) -> Result<ExecutionAttemptOutcomeInfo, StorageError> {
        let info: ExecutionAttemptOutcomeInfo = row.into();
        info.validate().map_err(|error| {
            StorageError::invariant_violation(
                Some(QUANT_EXECUTION_ATTEMPT_OUTCOME),
                format!("stored attempt outcome failed validation: {error}"),
            )
        })?;
        Ok(info)
    }
}

struct ExecutionRollupGraph {
    recommendation_id: RecommendationId,
    terminal_at: DateTime<Utc>,
    source_observed_at: DateTime<Utc>,
    intents: Vec<IntentModel>,
    attempts: Vec<ExecutionAttemptOutcomeInfo>,
}

impl PgRecommendationExecutionRollupRepository {
    async fn load_graph(
        transaction: &DatabaseTransaction,
        recommendation_id: RecommendationId,
        cutoff: DateTime<Utc>,
    ) -> Result<Result<ExecutionRollupGraph, ExecutionRollupDeferredReason>, StorageError> {
        let recommendation = RecommendationEntity::find_by_id(recommendation_id)
            .lock_shared()
            .one(transaction)
            .await
            .map_err(StorageError::from)?
            .ok_or_else(|| StorageError::not_found(QUANT_RECOMMENDATION, recommendation_id))?;
        if !recommendation.status.completes_report_rollup() {
            return Ok(Err(
                ExecutionRollupDeferredReason::RecommendationAuthorityOpen,
            ));
        }
        if recommendation.status_changed_at > cutoff {
            return Ok(Err(
                ExecutionRollupDeferredReason::SourceAvailableAfterCutoff,
            ));
        }
        let intents = IntentEntity::find()
            .filter(IntentColumn::RecommendationId.eq(recommendation_id))
            .order_by_asc(IntentColumn::CreatedAt)
            .order_by_asc(IntentColumn::OrderIntentId)
            .lock_shared()
            .all(transaction)
            .await
            .map_err(StorageError::from)?;
        if intents.iter().any(|intent| intent.updated_at > cutoff) {
            return Ok(Err(
                ExecutionRollupDeferredReason::SourceAvailableAfterCutoff,
            ));
        }
        let intent_ids = intents
            .iter()
            .map(|intent| intent.order_intent_id)
            .collect::<Vec<_>>();
        let orders = if intent_ids.is_empty() {
            Vec::new()
        } else {
            OrderEntity::find()
                .filter(OrderColumn::OrderIntentId.is_in(intent_ids.iter().copied()))
                .order_by_asc(OrderColumn::CreatedAt)
                .order_by_asc(OrderColumn::ExecutionOrderId)
                .lock_shared()
                .all(transaction)
                .await
                .map_err(StorageError::from)?
        };
        if orders
            .iter()
            .any(|order| !order.state.is_terminal() || order.updated_at > cutoff)
        {
            return Ok(Err(ExecutionRollupDeferredReason::OrderNotTerminal));
        }
        let positions = if intent_ids.is_empty() {
            Vec::new()
        } else {
            PositionEntity::find()
                .filter(PositionColumn::OrderIntentId.is_in(intent_ids.iter().copied()))
                .order_by_asc(PositionColumn::OpenedAt)
                .order_by_asc(PositionColumn::StrategyPositionLotId)
                .lock_shared()
                .all(transaction)
                .await
                .map_err(StorageError::from)?
        };
        if positions.iter().any(|position| {
            !matches!(
                position.state,
                PositionLedgerState::Closed | PositionLedgerState::Settled
            ) || position.updated_at > cutoff
        }) {
            return Ok(Err(ExecutionRollupDeferredReason::PositionNotTerminal));
        }
        let attempt_rows = AttemptEntity::find()
            .filter(AttemptColumn::RecommendationId.eq(recommendation_id))
            .order_by_asc(AttemptColumn::TerminalAt)
            .order_by_asc(AttemptColumn::OrderIntentId)
            .lock_shared()
            .all(transaction)
            .await
            .map_err(StorageError::from)?;
        let attempts = attempt_rows
            .into_iter()
            .map(Self::attempt_info)
            .collect::<Result<Vec<_>, _>>()?;
        if attempts.iter().any(|attempt| attempt.available_at > cutoff) {
            return Ok(Err(
                ExecutionRollupDeferredReason::SourceAvailableAfterCutoff,
            ));
        }
        Self::validate_membership(&intents, &orders, &attempts)?;
        let submitted = submitted_intents(&orders)?;
        let attempts_by_intent = attempts
            .iter()
            .map(|attempt| (attempt.order_intent_id, attempt))
            .collect::<HashMap<_, _>>();
        for intent in &intents {
            if submitted.contains(&intent.order_intent_id) {
                if !attempts_by_intent.contains_key(&intent.order_intent_id) {
                    return Ok(Err(ExecutionRollupDeferredReason::AttemptOutcomeMissing));
                }
            } else if attempts_by_intent.contains_key(&intent.order_intent_id) {
                return Err(StorageError::invariant_violation(
                    Some(QUANT_RECOMMENDATION_EXECUTION_ROLLUP),
                    "unsubmitted intent unexpectedly has an attempt outcome",
                ));
            } else if !intent.status.is_unsubmitted_terminal() {
                return Ok(Err(ExecutionRollupDeferredReason::IntentStillOpen));
            }
        }
        let terminal_at = graph_terminal_at(
            recommendation.status_changed_at,
            &intents,
            &orders,
            &positions,
            &attempts,
        );
        let source_observed_at =
            graph_observed_at(terminal_at, &intents, &orders, &positions, &attempts);
        if source_observed_at > cutoff {
            return Ok(Err(
                ExecutionRollupDeferredReason::SourceAvailableAfterCutoff,
            ));
        }
        Ok(Ok(ExecutionRollupGraph {
            recommendation_id,
            terminal_at,
            source_observed_at,
            intents,
            attempts,
        }))
    }

    fn validate_membership(
        intents: &[IntentModel],
        orders: &[OrderModel],
        attempts: &[ExecutionAttemptOutcomeInfo],
    ) -> Result<(), StorageError> {
        let intent_ids = intents
            .iter()
            .map(|intent| intent.order_intent_id)
            .collect::<HashSet<_>>();
        if orders
            .iter()
            .any(|order| !intent_ids.contains(&order.order_intent_id))
            || attempts
                .iter()
                .any(|attempt| !intent_ids.contains(&attempt.order_intent_id))
        {
            return Err(StorageError::invariant_violation(
                Some(QUANT_RECOMMENDATION_EXECUTION_ROLLUP),
                "execution rollup graph contains a foreign intent member",
            ));
        }
        Ok(())
    }

    async fn insert_rollup(
        transaction: &DatabaseTransaction,
        graph: ExecutionRollupGraph,
    ) -> Result<ExecutionRollupReconciliationResult, StorageError> {
        let seal = NewRecommendationExecutionRollup::aggregate(
            graph.recommendation_id,
            graph.intents.len(),
            graph.terminal_at,
            graph.source_observed_at,
            graph.attempts,
        )
        .map_err(|error| {
            StorageError::invariant_violation(
                Some(QUANT_RECOMMENDATION_EXECUTION_ROLLUP),
                error.to_string(),
            )
        })?;
        let available_at = primitives::statement_timestamp(transaction).await?;
        let rollup_hash = seal
            .rollup
            .expected_rollup_hash(available_at)
            .map_err(|error| {
                StorageError::invariant_violation(
                    Some(QUANT_RECOMMENDATION_EXECUTION_ROLLUP),
                    error.to_string(),
                )
            })?;
        let inserted =
            match RollupEntity::insert(rollup_active(&seal.rollup, rollup_hash, available_at))
                .on_conflict(
                    OnConflict::column(RollupColumn::RecommendationId)
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
        if let Some(row) = inserted {
            for binding in &seal.bindings {
                BindingEntity::insert(binding_active(binding, available_at))
                    .exec_without_returning(transaction)
                    .await
                    .map_err(StorageError::from)?;
            }
            return Self::rollup_info(row).map(ExecutionRollupReconciliationResult::Inserted);
        }
        let stored = RollupEntity::find_by_id(seal.rollup.recommendation_id)
            .one(transaction)
            .await
            .map_err(StorageError::from)?
            .ok_or_else(|| {
                StorageError::invariant_violation(
                    Some(QUANT_RECOMMENDATION_EXECUTION_ROLLUP),
                    "rollup conflict completed without a durable row",
                )
            })
            .and_then(Self::rollup_info)?;
        let bindings = BindingEntity::find()
            .filter(BindingColumn::RecommendationId.eq(seal.rollup.recommendation_id))
            .order_by_asc(BindingColumn::Sequence)
            .all(transaction)
            .await
            .map_err(StorageError::from)?
            .into_iter()
            .map(RecommendationExecutionRollupAttemptInfo::from)
            .collect::<Vec<_>>();
        if stored.as_new() != seal.rollup || !bindings_match(&bindings, &seal.bindings) {
            return Err(StorageError::state_conflict(
                QUANT_RECOMMENDATION_EXECUTION_ROLLUP,
                Some(seal.rollup.recommendation_id),
                "recommendation is already bound to a different final execution graph",
            ));
        }
        Ok(ExecutionRollupReconciliationResult::AlreadyPresent(stored))
    }
}

#[async_trait::async_trait]
impl RecommendationExecutionRollupRepository for PgRecommendationExecutionRollupRepository {
    async fn reconcile_recommendation(
        &self,
        recommendation_id: RecommendationId,
        available_through: DateTime<Utc>,
    ) -> Result<ExecutionRollupReconciliationResult, StorageError> {
        let transaction = self.db.begin().await.map_err(StorageError::from)?;
        let graph =
            match Self::load_graph(&transaction, recommendation_id, available_through).await? {
                Ok(graph) => graph,
                Err(reason) => {
                    transaction.commit().await.map_err(StorageError::from)?;
                    return Ok(ExecutionRollupReconciliationResult::Deferred(reason));
                }
            };
        let result = Self::insert_rollup(&transaction, graph).await?;
        transaction.commit().await.map_err(StorageError::from)?;
        Ok(result)
    }

    async fn find_by_recommendation(
        &self,
        recommendation_id: RecommendationId,
    ) -> Result<Option<RecommendationExecutionRollupInfo>, StorageError> {
        RollupEntity::find_by_id(recommendation_id)
            .one(&self.db)
            .await
            .map_err(StorageError::from)?
            .map(Self::rollup_info)
            .transpose()
    }

    async fn claim_reconciliation(
        &self,
        available_through: DateTime<Utc>,
        worker_id: WorkerId,
        lease_secs: u64,
        limit: u64,
    ) -> Result<Vec<ExecutionRollupTaskClaim>, StorageError> {
        let lease = validate_rollup_claim(lease_secs, limit)?;
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
            .order_by_asc(TaskColumn::RecommendationId)
            .limit(limit)
            .lock_with_behavior(LockType::Update, LockBehavior::SkipLocked)
            .all(&transaction)
            .await
            .map_err(StorageError::from)?;
        let mut claims = Vec::with_capacity(rows.len());
        for row in rows {
            let attempt_count = row
                .attempt_count
                .checked_add(1)
                .ok_or_else(|| rollup_queue_invariant("rollup reconciliation count overflow"))?;
            let mut active = row.clone().into_active_model();
            active.status = Set(OutcomeReconciliationTaskStatus::Delivering);
            active.attempt_count = Set(attempt_count);
            active.claim_owner = Set(Some(worker_id));
            active.lease_expires_at = Set(Some(now + lease));
            active.next_attempt_at = Set(None);
            active.updated_at = Set(now);
            active
                .update(&transaction)
                .await
                .map_err(StorageError::from)?;
            claims.push(ExecutionRollupTaskClaim {
                recommendation_id: row.recommendation_id,
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
                    QUANT_EXECUTION_ROLLUP_RECONCILIATION_TASK,
                    recommendation_id,
                )
            })?;
        if row.status != OutcomeReconciliationTaskStatus::Delivering
            || row.claim_owner != Some(worker_id)
        {
            return Err(StorageError::state_conflict(
                QUANT_EXECUTION_ROLLUP_RECONCILIATION_TASK,
                Some(recommendation_id),
                "rollup task is not leased by the settling worker",
            ));
        }
        let now = primitives::statement_timestamp(&transaction).await?;
        let mut active = row.into_active_model();
        active.claim_owner = Set(None);
        active.lease_expires_at = Set(None);
        active.updated_at = Set(now);
        match settlement {
            OutcomeTaskSettlement::Completed => {
                active.status = Set(OutcomeReconciliationTaskStatus::Completed);
                active.next_attempt_at = Set(None);
                active.last_error = Set(None);
                active.completed_at = Set(Some(now));
            }
            OutcomeTaskSettlement::RetryAfter { delay_secs, error } => {
                let delay = validate_rollup_retry(delay_secs, &error)?;
                active.status = Set(OutcomeReconciliationTaskStatus::Retrying);
                active.next_attempt_at = Set(Some(now + delay));
                active.last_error = Set(Some(error));
                active.completed_at = Set(None);
            }
        }
        active
            .update(&transaction)
            .await
            .map_err(StorageError::from)?;
        transaction.commit().await.map_err(StorageError::from)
    }

    async fn barrier(&self, cutoff: DateTime<Utc>) -> Result<ExecutionRollupBarrier, StorageError> {
        let transaction = self
            .db
            .begin_with_config(Some(IsolationLevel::RepeatableRead), None)
            .await
            .map_err(StorageError::from)?;
        let unsealed = RecommendationEntity::find()
            .join(
                JoinType::LeftJoin,
                RecommendationRelation::ExecutionRollup.def(),
            )
            .filter(
                RecommendationColumn::Status.is_in(RecommendationStatus::REPORT_ROLLUP_COMPLETE),
            )
            .filter(RecommendationColumn::StatusChangedAt.lte(cutoff))
            .filter(RollupColumn::RecommendationId.is_null());
        let eligible_unsealed_count = unsealed
            .clone()
            .count(&transaction)
            .await
            .map_err(StorageError::from)?;
        let oldest = unsealed
            .order_by_asc(RecommendationColumn::StatusChangedAt)
            .one(&transaction)
            .await
            .map_err(StorageError::from)?
            .map(|row| row.status_changed_at);
        transaction.commit().await.map_err(StorageError::from)?;
        Ok(ExecutionRollupBarrier {
            cutoff,
            eligible_unsealed_count,
            sealed_through: oldest.unwrap_or(cutoff),
        })
    }
}

impl PgRecommendationExecutionRollupRepository {
    async fn materialize_tasks(
        transaction: &DatabaseTransaction,
        available_through: DateTime<Utc>,
        limit: u64,
    ) -> Result<(), StorageError> {
        let rows = RecommendationEntity::find()
            .select_only()
            .column(RecommendationColumn::RecommendationId)
            .join(
                JoinType::LeftJoin,
                RecommendationRelation::ExecutionRollup.def(),
            )
            .join(
                JoinType::LeftJoin,
                RecommendationRelation::ExecutionRollupReconciliationTask.def(),
            )
            .filter(
                RecommendationColumn::Status.is_in(RecommendationStatus::REPORT_ROLLUP_COMPLETE),
            )
            .filter(RecommendationColumn::StatusChangedAt.lte(available_through))
            .filter(RollupColumn::RecommendationId.is_null())
            .filter(TaskColumn::RecommendationId.is_null())
            .order_by_asc(RecommendationColumn::RecommendationId)
            .limit(limit)
            .into_tuple::<RecommendationId>()
            .all(transaction)
            .await
            .map_err(StorageError::from)?;
        let now = primitives::statement_timestamp(transaction).await?;
        let ready_at = primitives::queue_ready_at(available_through, now);
        for recommendation_id in rows {
            TaskEntity::insert(TaskActiveModel {
                recommendation_id: Set(recommendation_id),
                ready_at: Set(ready_at),
                status: Set(OutcomeReconciliationTaskStatus::Pending),
                attempt_count: Set(0),
                claim_owner: Set(None),
                lease_expires_at: Set(None),
                next_attempt_at: Set(None),
                last_error: Set(None),
                completed_at: Set(None),
                created_at: Set(now),
                updated_at: Set(now),
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
        Ok(())
    }
}

fn submitted_intents(orders: &[OrderModel]) -> Result<HashSet<OrderIntentId>, StorageError> {
    let mut submitted = HashSet::new();
    for order in orders.iter().filter(|order| {
        order.order_phase == ExecutionOrderPhase::Entry && order.submitted_at.is_some()
    }) {
        if !submitted.insert(order.order_intent_id) {
            return Err(StorageError::invariant_violation(
                Some(QUANT_RECOMMENDATION_EXECUTION_ROLLUP),
                "one intent contains multiple submitted entry orders",
            ));
        }
    }
    Ok(submitted)
}

fn graph_terminal_at(
    recommendation_terminal_at: DateTime<Utc>,
    intents: &[IntentModel],
    orders: &[OrderModel],
    positions: &[PositionModel],
    attempts: &[ExecutionAttemptOutcomeInfo],
) -> DateTime<Utc> {
    intents
        .iter()
        .map(|intent| intent.updated_at)
        .chain(orders.iter().map(|order| order.updated_at))
        .chain(positions.iter().map(|position| position.updated_at))
        .chain(attempts.iter().map(|attempt| attempt.terminal_at))
        .fold(recommendation_terminal_at, DateTime::max)
}

fn graph_observed_at(
    terminal_at: DateTime<Utc>,
    intents: &[IntentModel],
    orders: &[OrderModel],
    positions: &[PositionModel],
    attempts: &[ExecutionAttemptOutcomeInfo],
) -> DateTime<Utc> {
    intents
        .iter()
        .map(|intent| intent.updated_at)
        .chain(orders.iter().map(|order| order.updated_at))
        .chain(positions.iter().map(|position| position.updated_at))
        .chain(attempts.iter().map(|attempt| attempt.available_at))
        .fold(terminal_at, DateTime::max)
}

const fn rollup_active(
    rollup: &NewRecommendationExecutionRollup,
    rollup_hash: ContentHash,
    available_at: DateTime<Utc>,
) -> RollupActiveModel {
    RollupActiveModel {
        recommendation_id: Set(rollup.recommendation_id),
        intent_count: Set(rollup.intent_count),
        attempt_count: Set(rollup.attempt_count),
        unfilled_attempt_count: Set(rollup.unfilled_attempt_count),
        partially_filled_attempt_count: Set(rollup.partially_filled_attempt_count),
        fully_filled_attempt_count: Set(rollup.fully_filled_attempt_count),
        total_requested_shares: Set(rollup.total_requested_shares),
        total_filled_shares: Set(rollup.total_filled_shares),
        total_entry_fee_usd: Set(rollup.total_entry_fee_usd),
        total_exit_fee_usd: Set(rollup.total_exit_fee_usd),
        total_settlement_payout_usd: Set(rollup.total_settlement_payout_usd),
        total_realized_pnl_usd: Set(rollup.total_realized_pnl_usd),
        first_attempt_terminal_at: Set(rollup.first_attempt_terminal_at),
        last_attempt_terminal_at: Set(rollup.last_attempt_terminal_at),
        terminal_at: Set(rollup.terminal_at),
        source_observed_at: Set(rollup.source_observed_at),
        available_at: Set(available_at),
        attempt_set_hash: Set(rollup.attempt_set_hash),
        rollup_hash: Set(rollup_hash),
        created_at: Set(available_at),
    }
}

const fn binding_active(
    binding: &NewRecommendationExecutionRollupAttempt,
    created_at: DateTime<Utc>,
) -> BindingActiveModel {
    BindingActiveModel {
        recommendation_id: Set(binding.recommendation_id),
        sequence: Set(binding.sequence),
        order_intent_id: Set(binding.order_intent_id),
        attempt_outcome_hash: Set(binding.attempt_outcome_hash),
        terminal_at: Set(binding.terminal_at),
        created_at: Set(created_at),
    }
}

fn bindings_match(
    stored: &[RecommendationExecutionRollupAttemptInfo],
    expected: &[NewRecommendationExecutionRollupAttempt],
) -> bool {
    stored.len() == expected.len()
        && stored.iter().zip(expected).all(|(stored, expected)| {
            stored.recommendation_id == expected.recommendation_id
                && stored.sequence == expected.sequence
                && stored.order_intent_id == expected.order_intent_id
                && stored.attempt_outcome_hash == expected.attempt_outcome_hash
                && stored.terminal_at == expected.terminal_at
        })
}

fn validate_rollup_claim(lease_secs: u64, limit: u64) -> Result<Duration, StorageError> {
    if lease_secs == 0 || lease_secs > MAX_LEASE_SECS {
        return Err(rollup_queue_invariant(format!(
            "lease_secs must be within 1..={MAX_LEASE_SECS}"
        )));
    }
    if limit == 0 || limit > MAX_QUEUE_BATCH {
        return Err(rollup_queue_invariant(format!(
            "claim limit must be within 1..={MAX_QUEUE_BATCH}"
        )));
    }
    let seconds = i64::try_from(lease_secs)
        .map_err(|error| rollup_queue_invariant(format!("lease seconds overflow: {error}")))?;
    Ok(Duration::seconds(seconds))
}

fn validate_rollup_retry(delay_secs: u64, error: &str) -> Result<Duration, StorageError> {
    if delay_secs == 0
        || delay_secs > MAX_RETRY_SECS
        || error.trim().is_empty()
        || error.chars().count() > MAX_ERROR_CHARS
    {
        return Err(rollup_queue_invariant(format!(
            "retry delay must be within 1..={MAX_RETRY_SECS} and error within 1..={MAX_ERROR_CHARS} characters"
        )));
    }
    let seconds = i64::try_from(delay_secs)
        .map_err(|error| rollup_queue_invariant(format!("retry delay overflow: {error}")))?;
    Ok(Duration::seconds(seconds))
}

fn rollup_queue_invariant(detail: impl Display) -> StorageError {
    StorageError::invariant_violation(Some(QUANT_EXECUTION_ROLLUP_RECONCILIATION_TASK), detail)
}
