//! Postgres-backed recommendation attribution repository.

use chrono::{DateTime, Utc};
use quant_pivot_error::storage::{StorageError, entity::QUANT_RECOMMENDATION};
use quant_pivot_models::{
    domain::quant::{
        InsertFinalOutcome, NewRecommendationAttribution, RecommendationAttributionInfo,
    },
    entities::{
        quant_execution_order::{
            Column as QuantExecutionOrderColumn, Entity as QuantExecutionOrderEntity,
            Relation as QuantExecutionOrderRelation,
        },
        quant_order_intent::{Column as QuantOrderIntentColumn, Entity as QuantOrderIntentEntity},
        quant_position::{Column as QuantPositionColumn, Entity as QuantPositionEntity, Relation},
        quant_recommendation::{Column as QuantRecommendationColumn, Entity},
        quant_recommendation_attribution::{
            Column, Entity as QuantRecommendationAttributionEntity,
        },
        quant_reconciliation::{
            Column as QuantReconciliationColumn, Entity as QuantReconciliationEntity,
            Relation as QuantReconciliationRelation,
        },
    },
    enums::{
        execution::{PositionLedgerState, ReconciliationResult},
        quant::{ExecutionOrderState, OrderIntentStatus, RecommendationStatus},
    },
    types::RecommendationId,
};
use sea_orm::{
    ActiveModelTrait, ColumnTrait, DatabaseConnection, DbErr, EntityTrait, ExprTrait,
    IntoActiveModel, Iterable, JoinType, QueryFilter, QueryOrder, QuerySelect, QueryTrait,
    RelationTrait, Set, TransactionTrait,
    sea_query::{Expr, OnConflict, SelectStatement, SimpleExpr},
};

use crate::{postgres::error::not_found, traits::AttributionRepository};

/// Postgres-backed recommendation attribution repository.
pub struct PgAttributionRepository {
    db: DatabaseConnection,
}

impl PgAttributionRepository {
    pub const fn new(db: DatabaseConnection) -> Self {
        Self { db }
    }
}

#[async_trait::async_trait]
impl AttributionRepository for PgAttributionRepository {
    async fn insert_final_and_mark_attributed(
        &self,
        attribution: NewRecommendationAttribution,
    ) -> Result<InsertFinalOutcome, StorageError> {
        let txn = self.db.begin().await.map_err(StorageError::from)?;
        let recommendation_id = attribution.recommendation_id;

        let recommendation_row = Entity::find_by_id(recommendation_id)
            .one(&txn)
            .await
            .map_err(StorageError::from)?
            .ok_or_else(|| not_found(QUANT_RECOMMENDATION, recommendation_id))?;
        if !recommendation_row.status.eligible_for_attribution() {
            txn.rollback().await.map_err(StorageError::from)?;
            return Err(StorageError::InvariantViolation {
                entity: Some(QUANT_RECOMMENDATION),
                detail: format!(
                    "cannot attribute recommendation {recommendation_id} in status {:?}",
                    recommendation_row.status
                ),
            });
        }

        let on_conflict = OnConflict::column(Column::RecommendationId)
            .do_nothing()
            .to_owned();

        let row =
            match QuantRecommendationAttributionEntity::insert(attribution.into_active_model())
                .on_conflict(on_conflict)
                .exec_with_returning(&txn)
                .await
            {
                Ok(row) => row,
                Err(DbErr::RecordNotFound(_)) => {
                    txn.rollback().await.map_err(StorageError::from)?;
                    return Ok(InsertFinalOutcome::AlreadyExists);
                }
                Err(err) => return Err(StorageError::from(err)),
            };

        let mut recommendation = recommendation_row.into_active_model();
        recommendation.status = Set(RecommendationStatus::Attributed);
        recommendation
            .update(&txn)
            .await
            .map_err(StorageError::from)?;

        txn.commit().await.map_err(StorageError::from)?;
        Ok(InsertFinalOutcome::Written(Box::new(row.into())))
    }

    async fn find_by_recommendation(
        &self,
        recommendation_id: &RecommendationId,
    ) -> Result<Option<RecommendationAttributionInfo>, StorageError> {
        QuantRecommendationAttributionEntity::find()
            .filter(Column::RecommendationId.eq(*recommendation_id))
            .one(&self.db)
            .await
            .map_err(StorageError::from)
            .map(|row| row.map(Into::into))
    }

    async fn find_label_available_between(
        &self,
        window_start: DateTime<Utc>,
        window_end: DateTime<Utc>,
        limit: u64,
    ) -> Result<Vec<RecommendationAttributionInfo>, StorageError> {
        if limit == 0 {
            return Ok(Vec::new());
        }
        QuantRecommendationAttributionEntity::find()
            .filter(
                Column::LabelAvailableAt
                    .gte(window_start)
                    .and(Column::LabelAvailableAt.lt(window_end)),
            )
            .order_by_asc(Column::LabelAvailableAt)
            .limit(limit)
            .all(&self.db)
            .await
            .map_err(StorageError::from)
            .map(|rows| rows.into_iter().map(Into::into).collect())
    }
}

// ── Expired-path ledger guards ────────────────────────────────────────

/// SQL filters: expired recommendations whose execution ledger has settled.
#[must_use]
pub fn expired_ledger_filters() -> Vec<SimpleExpr> {
    vec![
        exists_non_terminal_intent().not(),
        exists_open_position().not(),
        exists_blocking_order().not(),
        exists_blocking_recon().not(),
    ]
}

/// Returns `true` when final attribution for this recommendation must defer.
pub async fn blocks_attribution(
    db: &DatabaseConnection,
    recommendation_id: &RecommendationId,
) -> Result<bool, DbErr> {
    if QuantOrderIntentEntity::find()
        .filter(QuantOrderIntentColumn::RecommendationId.eq(*recommendation_id))
        .filter(QuantOrderIntentColumn::Status.is_in(non_terminal_intents()))
        .limit(1)
        .one(db)
        .await?
        .is_some()
    {
        return Ok(true);
    }

    if QuantPositionEntity::find()
        .join(JoinType::InnerJoin, Relation::OrderIntent.def())
        .filter(QuantOrderIntentColumn::RecommendationId.eq(*recommendation_id))
        .filter(QuantPositionColumn::State.is_in(blocking_positions()))
        .limit(1)
        .one(db)
        .await?
        .is_some()
    {
        return Ok(true);
    }

    if QuantExecutionOrderEntity::find()
        .join(
            JoinType::InnerJoin,
            QuantExecutionOrderRelation::OrderIntent.def(),
        )
        .filter(QuantOrderIntentColumn::RecommendationId.eq(*recommendation_id))
        .filter(QuantExecutionOrderColumn::State.is_in(blocking_orders()))
        .limit(1)
        .one(db)
        .await?
        .is_some()
    {
        return Ok(true);
    }

    if QuantReconciliationEntity::find()
        .join(
            JoinType::InnerJoin,
            QuantReconciliationRelation::OrderIntent.def(),
        )
        .filter(QuantOrderIntentColumn::RecommendationId.eq(*recommendation_id))
        .filter(QuantReconciliationColumn::Result.is_in(blocking_recons()))
        .limit(1)
        .one(db)
        .await?
        .is_some()
    {
        return Ok(true);
    }

    Ok(false)
}

fn terminal_intents() -> Vec<OrderIntentStatus> {
    OrderIntentStatus::UNFILLED_TERMINAL
        .iter()
        .chain(OrderIntentStatus::FILLED_TERMINAL.iter())
        .copied()
        .collect()
}

/// Complement of [`terminal_intents`]. Prefer `is_in` over `is_not_in` for Postgres
/// native enums: [`SeaORM`](sea_orm)'s raw `sea_query` path binds `is_not_in` as text
/// (`<>`), which Postgres rejects for `qp_order_intent_status`.
fn non_terminal_intents() -> Vec<OrderIntentStatus> {
    let terminal = terminal_intents();
    OrderIntentStatus::iter()
        .filter(|status| !terminal.contains(status))
        .collect()
}

fn blocking_orders() -> Vec<ExecutionOrderState> {
    vec![
        ExecutionOrderState::Submitted,
        ExecutionOrderState::CancelRequested,
        ExecutionOrderState::Ambiguous,
    ]
}

fn blocking_recons() -> Vec<ReconciliationResult> {
    vec![
        ReconciliationResult::Pending,
        ReconciliationResult::Unresolvable,
    ]
}

fn blocking_positions() -> Vec<PositionLedgerState> {
    vec![PositionLedgerState::Open, PositionLedgerState::Closing]
}

fn exists_non_terminal_intent() -> SimpleExpr {
    exists(
        QuantOrderIntentEntity::find()
            .select_only()
            .column(QuantOrderIntentColumn::OrderIntentId)
            .filter(recommendation_matches_intent())
            .filter(QuantOrderIntentColumn::Status.is_in(non_terminal_intents()))
            .into_query(),
    )
}

fn exists_open_position() -> SimpleExpr {
    exists(
        QuantPositionEntity::find()
            .select_only()
            .column(QuantPositionColumn::PositionId)
            .join(JoinType::InnerJoin, Relation::OrderIntent.def())
            .filter(recommendation_matches_intent())
            .filter(QuantPositionColumn::State.is_in(blocking_positions()))
            .into_query(),
    )
}

fn exists_blocking_order() -> SimpleExpr {
    exists(
        QuantExecutionOrderEntity::find()
            .select_only()
            .column(QuantExecutionOrderColumn::ExecutionOrderId)
            .join(
                JoinType::InnerJoin,
                QuantExecutionOrderRelation::OrderIntent.def(),
            )
            .filter(recommendation_matches_intent())
            .filter(QuantExecutionOrderColumn::State.is_in(blocking_orders()))
            .into_query(),
    )
}

fn exists_blocking_recon() -> SimpleExpr {
    exists(
        QuantReconciliationEntity::find()
            .select_only()
            .column(QuantReconciliationColumn::ReconciliationId)
            .join(
                JoinType::InnerJoin,
                QuantReconciliationRelation::OrderIntent.def(),
            )
            .filter(recommendation_matches_intent())
            .filter(QuantReconciliationColumn::Result.is_in(blocking_recons()))
            .into_query(),
    )
}

fn recommendation_matches_intent() -> SimpleExpr {
    Expr::col((
        QuantOrderIntentEntity,
        QuantOrderIntentColumn::RecommendationId,
    ))
    .equals((Entity, QuantRecommendationColumn::RecommendationId))
}

fn exists(sub_query: SelectStatement) -> SimpleExpr {
    Expr::exists(sub_query)
}
