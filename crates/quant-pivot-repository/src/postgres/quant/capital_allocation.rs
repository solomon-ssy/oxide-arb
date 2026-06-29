//! Postgres-backed capital-allocation repository.

use crate::{
    postgres::error,
    traits::{CapitalAllocationRepository, ReservedCapitalRepository},
};
use quant_pivot_error::storage::{StorageError, entity};
use quant_pivot_models::{
    domain::{CapitalAllocationInfo, CapitalReconcileSettlement, CapitalSettlement},
    entities::quant_capital_allocation,
    enums::execution::CapitalAllocationState,
    types::{OrderIntentId, Usd},
};
use rust_decimal::Decimal;
use sea_orm::{
    ActiveModelTrait, ActiveValue, ColumnTrait, ConnectionTrait, DatabaseConnection, EntityTrait,
    FromQueryResult, IntoActiveModel, PaginatorTrait, QueryFilter, QuerySelect, sea_query::Expr,
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

/// Reject negative capital amounts before any write (shared with the
/// order-intent composite transaction).
pub fn validate_non_negative(
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
        return Err(error::invariant_violation(
            Some(entity::QUANT_CAPITAL_ALLOCATION),
            "capital allocation amounts must be non-negative",
        ));
    }
    Ok(())
}

/// Whether a capital row satisfies the FSM money invariant:
/// `planned ≥ allocated ≥ locked` and `spent + released ≤ max(allocated, locked)`.
///
/// Shared with the order-intent composite transaction; a violation forces the
/// row to `Impaired` rather than freeing budget for new entries.
#[must_use]
pub fn capital_invariant_ok(
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

/// Load the (single) capital allocation row for an intent, failing if absent.
///
/// Shared by every money-moving composite transaction (order-intent +
/// execution-submission), always called against the transaction connection.
pub async fn load_capital(
    db: &impl ConnectionTrait,
    intent_id: &OrderIntentId,
) -> Result<quant_capital_allocation::Model, StorageError> {
    quant_capital_allocation::Entity::find()
        .filter(quant_capital_allocation::Column::OrderIntentId.eq(intent_id.clone()))
        .one(db)
        .await
        .map_err(StorageError::from)?
        .ok_or_else(|| error::not_found(entity::QUANT_CAPITAL_ALLOCATION, intent_id))
}

/// Release an intent's still-reserved capital in full (`Allocated`/`Locked` →
/// `Released`). A broken invariant forces `Impaired` (fail-closed: never free
/// corrupted budget). Used by reject / cancel / expire / invalidate / admission
/// deny paths.
pub async fn release_capital(
    db: &impl ConnectionTrait,
    intent_id: &OrderIntentId,
    reason: String,
) -> Result<(), StorageError> {
    let cap = load_capital(db, intent_id).await?;
    let released_usd = cap.allocated_usd.max(cap.locked_usd);
    validate_non_negative(
        cap.allocated_usd,
        cap.locked_usd,
        cap.spent_usd,
        released_usd,
    )?;
    let (state, reason) = if capital_invariant_ok(
        cap.planned_usd,
        cap.allocated_usd,
        cap.locked_usd,
        cap.spent_usd,
        released_usd,
    ) {
        (CapitalAllocationState::Released, reason)
    } else {
        (
            CapitalAllocationState::Impaired,
            format!("impaired: {reason}"),
        )
    };
    let mut active = cap.into_active_model();
    active.state = ActiveValue::Set(state);
    active.released_usd = ActiveValue::Set(released_usd);
    active.reason = ActiveValue::Set(reason);
    active.update(db).await.map_err(StorageError::from)?;
    Ok(())
}

/// Lock an intent's reserved capital for submission (`Allocated` → `Locked`,
/// `locked_usd = allocated_usd`). Fail-closed to `Impaired` on a broken
/// invariant. Called inside the entry-order write-ahead transaction.
pub async fn lock_capital(
    db: &impl ConnectionTrait,
    intent_id: &OrderIntentId,
    reason: String,
) -> Result<(), StorageError> {
    let cap = load_capital(db, intent_id).await?;
    if cap.state != CapitalAllocationState::Allocated {
        return Err(error::state_conflict(
            entity::QUANT_CAPITAL_ALLOCATION,
            Some(intent_id),
            format!(
                "capital must be allocated to lock, got {}",
                cap.state.as_str()
            ),
        ));
    }
    let locked_usd = cap.allocated_usd;
    let ok = validate_non_negative(
        cap.allocated_usd,
        locked_usd,
        cap.spent_usd,
        cap.released_usd,
    )
    .is_ok()
        && capital_invariant_ok(
            cap.planned_usd,
            cap.allocated_usd,
            locked_usd,
            cap.spent_usd,
            cap.released_usd,
        );
    let mut active = cap.into_active_model();
    if ok {
        active.state = ActiveValue::Set(CapitalAllocationState::Locked);
        active.locked_usd = ActiveValue::Set(locked_usd);
        active.reason = ActiveValue::Set(reason);
    } else {
        active.state = ActiveValue::Set(CapitalAllocationState::Impaired);
        active.reason = ActiveValue::Set(format!("impaired: {reason}"));
    }
    active.update(db).await.map_err(StorageError::from)?;
    Ok(())
}

/// Settle locked capital against a venue outcome ([`CapitalSettlement`]).
///
/// - `SettleFull` → `Spent`, unspent locked remainder released.
/// - `SettlePartial` → stays `Locked`, `spent` increased (remaining exposure
///   still reserved).
/// - `ReleaseAll` → `Released`.
/// - `Hold` → untouched (Ambiguous / resting `Open`; never frees capital that
///   may already be spent on the venue).
///
/// Any computed write that would violate the money invariant forces `Impaired`
/// with amounts left intact (fail-closed: never free corrupted budget).
pub async fn settle_capital(
    db: &impl ConnectionTrait,
    intent_id: &OrderIntentId,
    settlement: &CapitalSettlement,
    reason: String,
) -> Result<(), StorageError> {
    if matches!(settlement, CapitalSettlement::Hold) {
        return Ok(());
    }
    let cap = load_capital(db, intent_id).await?;
    let basis = cap.allocated_usd.max(cap.locked_usd);
    let (state, locked_usd, spent_usd, released_usd) = match settlement {
        CapitalSettlement::Hold => return Ok(()),
        CapitalSettlement::ReleaseAll => (
            CapitalAllocationState::Released,
            cap.locked_usd,
            cap.spent_usd,
            basis - cap.spent_usd,
        ),
        CapitalSettlement::SettleFull { spent_usd } => (
            CapitalAllocationState::Spent,
            cap.locked_usd,
            *spent_usd,
            basis - *spent_usd,
        ),
        CapitalSettlement::SettlePartial { spent_usd } => (
            CapitalAllocationState::Locked,
            cap.locked_usd,
            *spent_usd,
            cap.released_usd,
        ),
    };
    let ok = validate_non_negative(cap.allocated_usd, locked_usd, spent_usd, released_usd).is_ok()
        && capital_invariant_ok(
            cap.planned_usd,
            cap.allocated_usd,
            locked_usd,
            spent_usd,
            released_usd,
        );
    let mut active = cap.into_active_model();
    if ok {
        active.state = ActiveValue::Set(state);
        active.locked_usd = ActiveValue::Set(locked_usd);
        active.spent_usd = ActiveValue::Set(spent_usd);
        active.released_usd = ActiveValue::Set(released_usd);
        active.reason = ActiveValue::Set(reason);
    } else {
        active.state = ActiveValue::Set(CapitalAllocationState::Impaired);
        active.reason = ActiveValue::Set(format!("impaired: {reason}"));
    }
    active.update(db).await.map_err(StorageError::from)?;
    Ok(())
}

/// Complete a fully-exited lot's capital lifecycle (`Spent -> Released`,
/// Phase 05.6).
///
/// Called only when the position lot is fully exited. The persisted amounts are
/// left intact (`spent_usd` stays the realized entry cost; `released_usd` keeps
/// the entry-time unspent remainder) — flipping the state to `Released` is the
/// lifecycle/audit completion. A `Spent` lot already does not count toward the
/// reserved aggregate, so this never changes new-entry budget. Idempotent: a row
/// already `Released` is a no-op; any other state is a fail-closed conflict
/// (never un-settle money).
pub async fn complete_exit_capital(
    db: &impl ConnectionTrait,
    intent_id: &OrderIntentId,
    reason: String,
) -> Result<(), StorageError> {
    let cap = load_capital(db, intent_id).await?;
    match cap.state {
        CapitalAllocationState::Released => Ok(()),
        CapitalAllocationState::Spent => {
            let mut active = cap.into_active_model();
            active.state = ActiveValue::Set(CapitalAllocationState::Released);
            active.reason = ActiveValue::Set(reason);
            active.update(db).await.map_err(StorageError::from)?;
            Ok(())
        }
        other => Err(error::state_conflict(
            entity::QUANT_CAPITAL_ALLOCATION,
            Some(intent_id),
            format!("cannot complete exit capital from {}", other.as_str()),
        )),
    }
}

/// Apply a reconciliation verdict to an intent's capital allocation
/// ([`CapitalReconcileSettlement`], Phase 05.5).
///
/// State-guarded and **idempotent**: only a row still `Locked` (or `Impaired`,
/// for an operator override of an unresolvable) is moved; a row already
/// `Spent`/`Released` is left untouched, so re-running reconciliation never
/// double-counts capital. Fail-closed: a verdict that contradicts terminal
/// capital (e.g. a "filled" verdict on a `Released` row) forces `Impaired`
/// rather than rewriting settled money, and any computed write breaking the
/// money invariant likewise forces `Impaired` with amounts intact.
pub async fn reconcile_capital(
    db: &impl ConnectionTrait,
    intent_id: &OrderIntentId,
    settlement: &CapitalReconcileSettlement,
    reason: String,
) -> Result<(), StorageError> {
    use CapitalAllocationState as State;
    use CapitalReconcileSettlement as Recon;

    if matches!(settlement, Recon::Hold) {
        return Ok(());
    }
    let cap = load_capital(db, intent_id).await?;
    let basis = cap.allocated_usd.max(cap.locked_usd);

    // (target_state, spent, released, contradiction). `contradiction` forces
    // `Impaired` while preserving the persisted amounts.
    let (state, spent_usd, released_usd, contradiction) = match settlement {
        Recon::Hold => return Ok(()),
        Recon::Settle { spent_usd } => match cap.state {
            State::Spent => return Ok(()),
            State::Locked | State::Impaired => {
                (State::Spent, *spent_usd, basis - *spent_usd, false)
            }
            _ => (State::Impaired, cap.spent_usd, cap.released_usd, true),
        },
        Recon::Release => match cap.state {
            State::Released => return Ok(()),
            State::Locked | State::Impaired => {
                (State::Released, cap.spent_usd, basis - cap.spent_usd, false)
            }
            _ => (State::Impaired, cap.spent_usd, cap.released_usd, true),
        },
        Recon::Impair => match cap.state {
            State::Locked => (State::Impaired, cap.spent_usd, cap.released_usd, false),
            // Already impaired, or terminal (Spent/Released): never un-settle.
            _ => return Ok(()),
        },
    };

    let locked_usd = cap.locked_usd;
    let invariant_ok =
        validate_non_negative(cap.allocated_usd, locked_usd, spent_usd, released_usd).is_ok()
            && capital_invariant_ok(
                cap.planned_usd,
                cap.allocated_usd,
                locked_usd,
                spent_usd,
                released_usd,
            );

    let mut active = cap.into_active_model();
    if contradiction || !invariant_ok {
        active.state = ActiveValue::Set(CapitalAllocationState::Impaired);
        active.reason = ActiveValue::Set(format!("impaired (reconcile): {reason}"));
    } else {
        active.state = ActiveValue::Set(state);
        active.locked_usd = ActiveValue::Set(locked_usd);
        active.spent_usd = ActiveValue::Set(spent_usd);
        active.released_usd = ActiveValue::Set(released_usd);
        active.reason = ActiveValue::Set(reason);
    }
    active.update(db).await.map_err(StorageError::from)?;
    Ok(())
}
