//! Postgres-backed recommendation attribution repository.

use crate::{postgres::error::not_found, traits::AttributionRepository};
use chrono::{DateTime, Utc};
use quant_pivot_error::storage::{StorageError, entity};
use quant_pivot_models::{
    domain::{InsertFinalOutcome, NewRecommendationAttribution, RecommendationAttributionInfo},
    entities::{
        quant_execution_order, quant_order_intent, quant_position, quant_recommendation,
        quant_recommendation_attribution, quant_reconciliation,
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
        let recommendation_id = attribution.recommendation_id.clone();

        let recommendation_row =
            quant_recommendation::Entity::find_by_id(recommendation_id.clone())
                .one(&txn)
                .await
                .map_err(StorageError::from)?
                .ok_or_else(|| not_found(entity::QUANT_RECOMMENDATION, &recommendation_id))?;
        if !recommendation_row.status.eligible_for_attribution() {
            txn.rollback().await.map_err(StorageError::from)?;
            return Err(StorageError::InvariantViolation {
                entity: Some(entity::QUANT_RECOMMENDATION),
                detail: format!(
                    "cannot attribute recommendation {recommendation_id} in status {:?}",
                    recommendation_row.status
                ),
            });
        }

        let on_conflict =
            OnConflict::column(quant_recommendation_attribution::Column::RecommendationId)
                .do_nothing()
                .to_owned();

        let row =
            match quant_recommendation_attribution::Entity::insert(attribution.into_active_model())
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
        quant_recommendation_attribution::Entity::find()
            .filter(
                quant_recommendation_attribution::Column::RecommendationId
                    .eq(recommendation_id.clone()),
            )
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
        quant_recommendation_attribution::Entity::find()
            .filter(
                quant_recommendation_attribution::Column::LabelAvailableAt
                    .gte(window_start)
                    .and(quant_recommendation_attribution::Column::LabelAvailableAt.lt(window_end)),
            )
            .order_by_asc(quant_recommendation_attribution::Column::LabelAvailableAt)
            .limit(limit)
            .all(&self.db)
            .await
            .map_err(StorageError::from)
            .map(|rows| rows.into_iter().map(Into::into).collect())
    }
}

// ── Expired-path ledger guards (05.7) ────────────────────────────────────────

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
) -> Result<bool, sea_orm::DbErr> {
    if quant_order_intent::Entity::find()
        .filter(quant_order_intent::Column::RecommendationId.eq(recommendation_id.clone()))
        .filter(quant_order_intent::Column::Status.is_in(non_terminal_intents()))
        .limit(1)
        .one(db)
        .await?
        .is_some()
    {
        return Ok(true);
    }

    if quant_position::Entity::find()
        .join(
            JoinType::InnerJoin,
            quant_position::Relation::OrderIntent.def(),
        )
        .filter(quant_order_intent::Column::RecommendationId.eq(recommendation_id.clone()))
        .filter(quant_position::Column::State.is_in(blocking_positions()))
        .limit(1)
        .one(db)
        .await?
        .is_some()
    {
        return Ok(true);
    }

    if quant_execution_order::Entity::find()
        .join(
            JoinType::InnerJoin,
            quant_execution_order::Relation::OrderIntent.def(),
        )
        .filter(quant_order_intent::Column::RecommendationId.eq(recommendation_id.clone()))
        .filter(quant_execution_order::Column::State.is_in(blocking_orders()))
        .limit(1)
        .one(db)
        .await?
        .is_some()
    {
        return Ok(true);
    }

    if quant_reconciliation::Entity::find()
        .join(
            JoinType::InnerJoin,
            quant_reconciliation::Relation::OrderIntent.def(),
        )
        .filter(quant_order_intent::Column::RecommendationId.eq(recommendation_id.clone()))
        .filter(quant_reconciliation::Column::Result.is_in(blocking_recons()))
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
        quant_order_intent::Entity::find()
            .select_only()
            .column(quant_order_intent::Column::OrderIntentId)
            .filter(recommendation_matches_intent())
            .filter(quant_order_intent::Column::Status.is_in(non_terminal_intents()))
            .into_query(),
    )
}

fn exists_open_position() -> SimpleExpr {
    exists(
        quant_position::Entity::find()
            .select_only()
            .column(quant_position::Column::PositionId)
            .join(
                JoinType::InnerJoin,
                quant_position::Relation::OrderIntent.def(),
            )
            .filter(recommendation_matches_intent())
            .filter(quant_position::Column::State.is_in(blocking_positions()))
            .into_query(),
    )
}

fn exists_blocking_order() -> SimpleExpr {
    exists(
        quant_execution_order::Entity::find()
            .select_only()
            .column(quant_execution_order::Column::ExecutionOrderId)
            .join(
                JoinType::InnerJoin,
                quant_execution_order::Relation::OrderIntent.def(),
            )
            .filter(recommendation_matches_intent())
            .filter(quant_execution_order::Column::State.is_in(blocking_orders()))
            .into_query(),
    )
}

fn exists_blocking_recon() -> SimpleExpr {
    exists(
        quant_reconciliation::Entity::find()
            .select_only()
            .column(quant_reconciliation::Column::ReconciliationId)
            .join(
                JoinType::InnerJoin,
                quant_reconciliation::Relation::OrderIntent.def(),
            )
            .filter(recommendation_matches_intent())
            .filter(quant_reconciliation::Column::Result.is_in(blocking_recons()))
            .into_query(),
    )
}

fn recommendation_matches_intent() -> SimpleExpr {
    Expr::col((
        quant_order_intent::Entity,
        quant_order_intent::Column::RecommendationId,
    ))
    .equals((
        quant_recommendation::Entity,
        quant_recommendation::Column::RecommendationId,
    ))
}

fn exists(sub_query: SelectStatement) -> SimpleExpr {
    Expr::exists(sub_query)
}
