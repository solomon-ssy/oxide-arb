//! Postgres-backed capital-allocation repository.

use crate::traits::{CapitalAllocationRepository, ReservedCapitalRepository};
use quant_pivot_error::storage::StorageError;
use quant_pivot_models::{
    domain::{CapitalAllocationInfo, CapitalAllocationPatch, NewCapitalAllocation},
    entities::quant_capital_allocation,
    enums::execution::CapitalAllocationState,
    types::{CapitalAllocationId, OrderIntentId, Usd},
};
use rust_decimal::Decimal;
use sea_orm::{
    ActiveModelTrait, ActiveValue, ColumnTrait, DatabaseConnection, EntityTrait, FromQueryResult,
    IntoActiveModel, PaginatorTrait, QueryFilter, QuerySelect, sea_query::Expr,
};

/// Allocation rows included in the reserved-capital aggregate.
///
/// Excludes terminal rows (`Spent`, `Released`) and pre-reserve `Planned`.
const RESERVED_STATES: [CapitalAllocationState; 3] = [
    CapitalAllocationState::Allocated,
    CapitalAllocationState::Locked,
    CapitalAllocationState::Impaired,
];

/// Postgres-backed capital-allocation repository.
pub struct PgCapitalAllocationRepository {
    db: DatabaseConnection,
}

impl PgCapitalAllocationRepository {
    pub const fn new(db: DatabaseConnection) -> Self {
        Self { db }
    }
}

#[derive(Debug, FromQueryResult)]
struct LockedCapitalSum {
    total: Option<Decimal>,
}

#[async_trait::async_trait]
impl CapitalAllocationRepository for PgCapitalAllocationRepository {
    async fn create(
        &self,
        allocation: NewCapitalAllocation,
    ) -> Result<CapitalAllocationInfo, StorageError> {
        validate_non_negative(
            allocation.allocated_usd,
            allocation.locked_usd,
            allocation.spent_usd,
            allocation.released_usd,
        )?;
        quant_capital_allocation::Entity::insert(allocation.into_active_model())
            .exec_with_returning(&self.db)
            .await
            .map_err(StorageError::from)
            .map(Into::into)
    }

    async fn find_by_intent(
        &self,
        order_intent_id: &OrderIntentId,
    ) -> Result<Option<CapitalAllocationInfo>, StorageError> {
        quant_capital_allocation::Entity::find()
            .filter(quant_capital_allocation::Column::OrderIntentId.eq(order_intent_id.clone()))
            .one(&self.db)
            .await
            .map_err(StorageError::from)
            .map(|row| row.map(Into::into))
    }

    async fn transition(
        &self,
        capital_allocation_id: &CapitalAllocationId,
        patch: CapitalAllocationPatch,
    ) -> Result<CapitalAllocationInfo, StorageError> {
        validate_non_negative(
            patch.allocated_usd,
            patch.locked_usd,
            patch.spent_usd,
            patch.released_usd,
        )?;

        let Some(row) = quant_capital_allocation::Entity::find_by_id(capital_allocation_id.clone())
            .one(&self.db)
            .await
            .map_err(StorageError::from)?
        else {
            return Err(StorageError::Conflict(format!(
                "capital allocation not found: {capital_allocation_id}"
            )));
        };

        let (state, reason) = if patch.state == CapitalAllocationState::Impaired
            || capital_invariant_ok(
                row.planned_usd,
                patch.allocated_usd,
                patch.locked_usd,
                patch.spent_usd,
                patch.released_usd,
            ) {
            (patch.state, patch.reason)
        } else {
            (
                CapitalAllocationState::Impaired,
                format!("impaired: {}", patch.reason),
            )
        };

        let mut active = row.into_active_model();
        active.state = ActiveValue::Set(state);
        active.allocated_usd = ActiveValue::Set(patch.allocated_usd);
        active.locked_usd = ActiveValue::Set(patch.locked_usd);
        active.spent_usd = ActiveValue::Set(patch.spent_usd);
        active.released_usd = ActiveValue::Set(patch.released_usd);
        active.reason = ActiveValue::Set(reason);
        active
            .update(&self.db)
            .await
            .map_err(StorageError::from)
            .map(Into::into)
    }

    async fn sum_reserved_usd(&self) -> Result<Usd, StorageError> {
        sum_reserved_usd(&self.db).await
    }

    async fn has_impaired(&self) -> Result<bool, StorageError> {
        quant_capital_allocation::Entity::find()
            .filter(quant_capital_allocation::Column::State.eq(CapitalAllocationState::Impaired))
            .count(&self.db)
            .await
            .map_err(StorageError::from)
            .map(|count| count > 0)
    }
}

#[async_trait::async_trait]
impl ReservedCapitalRepository for PgCapitalAllocationRepository {
    async fn sum_reserved_usd(&self) -> Result<Usd, StorageError> {
        sum_reserved_usd(&self.db).await
    }
}

/// Sum net reserved USD across in-flight capital allocations.
///
/// For each row in [`RESERVED_STATES`], contributes:
///
/// ```text
/// GREATEST(GREATEST(allocated_usd, locked_usd) - spent_usd - released_usd, 0)
/// ```
///
/// - **`Allocated` / `Locked`**: counts intent-reserved budget not yet spent or released.
/// - **`Impaired`**: still included (fail-closed) until manually resolved — corrupted
///   invariants must not free budget for new entries.
/// - **`Planned` / `Spent` / `Released`**: excluded.
pub async fn sum_reserved_usd(db: &DatabaseConnection) -> Result<Usd, StorageError> {
    let row = quant_capital_allocation::Entity::find()
        .filter(quant_capital_allocation::Column::State.is_in(RESERVED_STATES))
        .select_only()
        .column_as(
            Expr::cust(
                "SUM(GREATEST(GREATEST(allocated_usd, locked_usd) - spent_usd - released_usd, 0))",
            ),
            "total",
        )
        .into_model::<LockedCapitalSum>()
        .one(db)
        .await
        .map_err(StorageError::from)?;

    let total = row.and_then(|row| row.total).unwrap_or(Decimal::ZERO);
    Ok(Usd::new(total))
}

fn validate_non_negative(
    allocated_usd: Usd,
    locked_usd: Usd,
    spent_usd: Usd,
    released_usd: Usd,
) -> Result<(), StorageError> {
    if allocated_usd.is_negative()
        || locked_usd.is_negative()
        || spent_usd.is_negative()
        || released_usd.is_negative()
    {
        return Err(StorageError::Conflict(
            "capital allocation amounts must be non-negative".to_owned(),
        ));
    }
    Ok(())
}

fn capital_invariant_ok(
    planned_usd: Usd,
    allocated_usd: Usd,
    locked_usd: Usd,
    spent_usd: Usd,
    released_usd: Usd,
) -> bool {
    let reserve_basis = allocated_usd.max(locked_usd);
    planned_usd >= allocated_usd
        && allocated_usd >= locked_usd
        && spent_usd + released_usd <= reserve_basis
}
