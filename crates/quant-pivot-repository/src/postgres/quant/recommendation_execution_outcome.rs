//! `PostgreSQL` WORM recommendation-execution outcome repository.

use chrono::{DateTime, Utc};
use quant_pivot_error::storage::{
    StorageError,
    entity::{QUANT_ORDER_INTENT, QUANT_RECOMMENDATION, QUANT_RECOMMENDATION_EXECUTION_OUTCOME},
};
use quant_pivot_models::{
    domain::quant::{
        ExecutionOutcomeDeferredReason, ExecutionOutcomeDerivation,
        ExecutionOutcomeReconciliationError, ExecutionOutcomeReconciliationResult,
        ExecutionOutcomeSourceGraph, NewRecommendationExecutionOutcome, OrderIntentInfo,
        PositionInfo, RecommendationExecutionOutcomeInfo,
        RecommendationExecutionReconciliationCandidate, settlement::SettlementRedeemLotInfo,
    },
    entities::{
        quant_execution_order::{
            Column as QuantExecutionOrderColumn, Entity as QuantExecutionOrderEntity,
            Relation as QuantExecutionOrderRelation,
        },
        quant_order_intent::{
            Column as QuantOrderIntentColumn, Entity as QuantOrderIntentEntity,
            Relation as QuantOrderIntentRelation,
        },
        quant_position::{Column as QuantPositionColumn, Entity as QuantPositionEntity},
        quant_recommendation::Entity as QuantRecommendationEntity,
        quant_recommendation_execution_outcome::{
            Column, Entity as QuantRecommendationExecutionOutcomeEntity,
            Model as QuantRecommendationExecutionOutcomeModel,
        },
        quant_reconciliation::{
            Column as QuantReconciliationColumn, Entity as QuantReconciliationEntity,
        },
        quant_settlement_redeem_lot::{
            Column as QuantSettlementRedeemLotColumn, Entity as QuantSettlementRedeemLotEntity,
        },
    },
    enums::{
        execution::{ExecutionOrderPhase, PositionLedgerState, ReconciliationResult},
        quant::ExecutionOrderState,
    },
    types::{OrderIntentId, RecommendationId},
};
use sea_orm::{
    ActiveValue, ColumnTrait, Condition, DatabaseConnection, DatabaseTransaction, DbErr,
    EntityTrait, IntoActiveModel, JoinType, QueryFilter, QueryOrder, QuerySelect, RelationTrait,
    TransactionTrait, sea_query::OnConflict,
};

use crate::{
    postgres::primitives::statement_timestamp, traits::RecommendationExecutionOutcomeRepository,
};

/// PostgreSQL-backed immutable recommendation-execution outcome repository.
pub struct PgRecommendationExecutionOutcomeRepository {
    db: DatabaseConnection,
}

impl PgRecommendationExecutionOutcomeRepository {
    pub const fn new(db: DatabaseConnection) -> Self {
        Self { db }
    }
}

#[async_trait::async_trait]
impl RecommendationExecutionOutcomeRepository for PgRecommendationExecutionOutcomeRepository {
    async fn reconcile_intent(
        &self,
        order_intent_id: &OrderIntentId,
        available_through: DateTime<Utc>,
    ) -> Result<ExecutionOutcomeReconciliationResult, StorageError> {
        let transaction = self.db.begin().await.map_err(StorageError::from)?;
        let graph = Self::load_source_graph(&transaction, order_intent_id).await?;
        let source_observed_at = graph.source_observed_at();
        if source_observed_at > available_through {
            transaction.commit().await.map_err(StorageError::from)?;
            return Ok(ExecutionOutcomeReconciliationResult::Deferred(
                ExecutionOutcomeDeferredReason::SourceAvailableAfterCutoff,
            ));
        }
        let outcome = match graph.derive().map_err(|error| source_graph_error(&error))? {
            ExecutionOutcomeDerivation::Ready(outcome) => *outcome,
            ExecutionOutcomeDerivation::Deferred(reason) => {
                transaction.commit().await.map_err(StorageError::from)?;
                return Ok(ExecutionOutcomeReconciliationResult::Deferred(reason));
            }
        };
        validate_new(&outcome)?;
        let inserted = Self::insert_derived(&transaction, outcome, source_observed_at).await?;
        transaction.commit().await.map_err(StorageError::from)?;
        Ok(inserted)
    }

    async fn find_by_recommendation(
        &self,
        recommendation_id: &RecommendationId,
    ) -> Result<Option<RecommendationExecutionOutcomeInfo>, StorageError> {
        QuantRecommendationExecutionOutcomeEntity::find_by_id(*recommendation_id)
            .one(&self.db)
            .await
            .map_err(StorageError::from)?
            .map(Self::validated_info)
            .transpose()
    }

    async fn list_reconciliation_candidates(
        &self,
        available_through: DateTime<Utc>,
        after: Option<OrderIntentId>,
        limit: u64,
    ) -> Result<Vec<RecommendationExecutionReconciliationCandidate>, StorageError> {
        if limit == 0 {
            return Ok(Vec::new());
        }
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
            .filter(QuantOrderIntentColumn::CreatedAt.lte(available_through))
            .filter(after.map_or_else(Condition::all, |cursor| {
                Condition::all().add(QuantOrderIntentColumn::OrderIntentId.gt(cursor))
            }))
            .order_by_asc(QuantOrderIntentColumn::OrderIntentId)
            .distinct()
            .limit(limit)
            .into_tuple::<(OrderIntentId, RecommendationId)>()
            .all(&self.db)
            .await
            .map_err(StorageError::from)?;
        Ok(rows
            .into_iter()
            .map(|(order_intent_id, recommendation_id)| {
                RecommendationExecutionReconciliationCandidate {
                    order_intent_id,
                    recommendation_id,
                }
            })
            .collect())
    }
}

impl PgRecommendationExecutionOutcomeRepository {
    async fn load_source_graph(
        transaction: &DatabaseTransaction,
        order_intent_id: &OrderIntentId,
    ) -> Result<ExecutionOutcomeSourceGraph, StorageError> {
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
        let orders = QuantExecutionOrderEntity::find()
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
        let position: Option<PositionInfo> = QuantPositionEntity::find()
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
        Ok(ExecutionOutcomeSourceGraph {
            recommendation_id: recommendation.recommendation_id,
            market_id: recommendation.market_id,
            token_id: recommendation.token_id,
            intent: OrderIntentInfo::from(intent),
            orders,
            reconciliations,
            position,
            settlement_lot: settlement_lots
                .into_iter()
                .next()
                .map(SettlementRedeemLotInfo::from),
        })
    }
}

impl PgRecommendationExecutionOutcomeRepository {
    async fn insert_derived(
        transaction: &DatabaseTransaction,
        outcome: NewRecommendationExecutionOutcome,
        source_observed_at: DateTime<Utc>,
    ) -> Result<ExecutionOutcomeReconciliationResult, StorageError> {
        let available_at = statement_timestamp(transaction).await?;
        let outcome_hash = outcome
            .expected_outcome_hash(source_observed_at, available_at)
            .map_err(|error| {
                StorageError::invariant_violation(
                    Some(QUANT_RECOMMENDATION_EXECUTION_OUTCOME),
                    error,
                )
            })?;
        let mut active_outcome = outcome.clone().into_active_model();
        active_outcome.source_observed_at = ActiveValue::Set(source_observed_at);
        active_outcome.available_at = ActiveValue::Set(available_at);
        active_outcome.outcome_hash = ActiveValue::Set(outcome_hash);
        let inserted = match QuantRecommendationExecutionOutcomeEntity::insert(active_outcome)
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
                QuantRecommendationExecutionOutcomeEntity::find_by_id(outcome.recommendation_id)
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
                QUANT_RECOMMENDATION_EXECUTION_OUTCOME,
                Some(outcome.recommendation_id),
                "recommendation id is already bound to different immutable execution content",
            ));
        }
        if was_inserted {
            Ok(ExecutionOutcomeReconciliationResult::Inserted(stored))
        } else {
            Ok(ExecutionOutcomeReconciliationResult::AlreadyPresent(stored))
        }
    }
}

fn source_graph_error(error: &ExecutionOutcomeReconciliationError) -> StorageError {
    StorageError::invariant_violation(
        Some(QUANT_RECOMMENDATION_EXECUTION_OUTCOME),
        error.to_string(),
    )
}

fn validate_new(outcome: &NewRecommendationExecutionOutcome) -> Result<(), StorageError> {
    outcome.validate().map_err(|error| {
        StorageError::invariant_violation(Some(QUANT_RECOMMENDATION_EXECUTION_OUTCOME), error)
    })
}

impl PgRecommendationExecutionOutcomeRepository {
    fn validated_info(
        row: QuantRecommendationExecutionOutcomeModel,
    ) -> Result<RecommendationExecutionOutcomeInfo, StorageError> {
        let outcome: RecommendationExecutionOutcomeInfo = row.into();
        outcome.validate().map_err(|error| {
            StorageError::invariant_violation(
                Some(QUANT_RECOMMENDATION_EXECUTION_OUTCOME),
                format!("stored outcome failed integrity validation: {error}"),
            )
        })?;
        Ok(outcome)
    }
}

fn source_invariant(detail: &'static str) -> StorageError {
    StorageError::invariant_violation(
        Some(QUANT_RECOMMENDATION_EXECUTION_OUTCOME),
        detail.to_owned(),
    )
}
