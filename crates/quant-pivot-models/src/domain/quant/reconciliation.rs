//! Execution-order reconciliation persistence DTOs.

use chrono::{DateTime, Utc};
use sea_orm::{DeriveIntoActiveModel, DerivePartialModel};
use serde::{Deserialize, Serialize};

use crate::{
    domain::{
        patch::{NullablePatch, Patch},
        quant::{PositionExit, PositionFill},
    },
    entities::quant_reconciliation,
    enums::{
        execution::{ExitState, ReconciliationResult, VenueOrderStatus},
        quant::{ExecutionOrderState, OrderIntentStatus},
    },
    types::{
        ExecutionOrderId, OrderId, OrderIntentId, Price, ReconciliationEvidence,
        ReconciliationEvidenceChain, ReconciliationId, Shares, Usd,
    },
};

/// Persisted reconciliation summary for one execution order.
#[derive(Debug, Clone, Serialize, Deserialize, DerivePartialModel)]
#[sea_orm(entity = "crate::entities::quant_reconciliation::Entity")]
pub struct ReconciliationInfo {
    pub reconciliation_id: ReconciliationId,
    pub execution_order_id: ExecutionOrderId,
    pub order_intent_id: OrderIntentId,
    pub result: ReconciliationResult,
    pub evidence_json: ReconciliationEvidenceChain,
    pub venue_filled_shares: Option<Shares>,
    pub venue_avg_price: Option<Price>,
    pub expected_cash_delta_usd: Option<Usd>,
    pub venue_cash_delta_usd: Option<Usd>,
    pub realized_pnl_usd: Option<Usd>,
    pub expected_fee_usd: Option<Usd>,
    pub observed_fee_usd: Option<Usd>,
    pub fee_delta_usd: Option<Usd>,
    pub resolved_by: Option<String>,
    pub resolved_at: Option<DateTime<Utc>>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

info_from_model!(
    ReconciliationInfo,
    quant_reconciliation::Model,
    {
        reconciliation_id,
        execution_order_id,
        order_intent_id,
        result,
        evidence_json,
        venue_filled_shares,
        venue_avg_price,
        expected_cash_delta_usd,
        venue_cash_delta_usd,
        realized_pnl_usd,
        expected_fee_usd,
        observed_fee_usd,
        fee_delta_usd,
        resolved_by,
        resolved_at,
        created_at,
        updated_at,
    }
);

/// Insert payload for `quant_reconciliation`.
#[derive(Debug, Clone, Serialize, Deserialize, DeriveIntoActiveModel)]
#[sea_orm(active_model = "crate::entities::quant_reconciliation::ActiveModel")]
pub struct NewReconciliation {
    pub reconciliation_id: ReconciliationId,
    pub execution_order_id: ExecutionOrderId,
    pub order_intent_id: OrderIntentId,
    pub result: ReconciliationResult,
    pub evidence_json: ReconciliationEvidenceChain,
    pub venue_filled_shares: Option<Shares>,
    pub venue_avg_price: Option<Price>,
    pub expected_cash_delta_usd: Option<Usd>,
    pub venue_cash_delta_usd: Option<Usd>,
    pub realized_pnl_usd: Option<Usd>,
    pub expected_fee_usd: Option<Usd>,
    pub observed_fee_usd: Option<Usd>,
    pub fee_delta_usd: Option<Usd>,
    pub resolved_by: Option<String>,
    pub resolved_at: Option<DateTime<Utc>>,
}

/// Controlled update payload for resolving or reclassifying a reconciliation.
#[derive(Debug, Clone, Default)]
pub struct ReconciliationPatch {
    pub result: Patch<ReconciliationResult>,
    pub venue_filled_shares: NullablePatch<Shares>,
    pub venue_avg_price: NullablePatch<Price>,
    pub expected_cash_delta_usd: NullablePatch<Usd>,
    pub venue_cash_delta_usd: NullablePatch<Usd>,
    pub realized_pnl_usd: NullablePatch<Usd>,
    pub expected_fee_usd: NullablePatch<Usd>,
    pub observed_fee_usd: NullablePatch<Usd>,
    pub fee_delta_usd: NullablePatch<Usd>,
    pub resolved_by: NullablePatch<String>,
    pub resolved_at: NullablePatch<DateTime<Utc>>,
}

/// Append-only-at-the-repository evidence write intent.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AppendReconciliationEvidence {
    pub evidence: ReconciliationEvidence,
}

/// How a reconciliation verdict settles the order's capital allocation.
///
/// Distinct from [`CapitalSettlement`](super::CapitalSettlement): the correction
/// is **state-guarded and idempotent** — it only moves capital out of `Locked`
/// (or `Impaired`, for an operator override), never re-touching a row already
/// `Spent`/`Released`.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum CapitalReconcileSettlement {
    /// Venue-confirmed fill: `Locked|Impaired -> Spent` with `spent_usd`
    /// (fill cost + fee); any unspent locked remainder is released.
    Settle { spent_usd: Usd },
    /// Venue-confirmed non-fill: `Locked|Impaired -> Released`.
    Release,
    /// Unresolvable verdict: `Locked -> Impaired` (capital frozen pending an
    /// operator). Fail-closed: never freed while truth is unknown.
    Impair,
    /// No capital change (order still open / not yet terminal).
    Hold,
}

/// Atomic ledger correction applied by `apply_reconciliation` in one txn.
///
/// Built by the reconciliation service from venue truth and applied across
/// execution order + capital + position + intent + the reconciliation summary
/// row. Idempotency is guaranteed by the order's terminal-state guard inside the
/// repository.
#[derive(Debug, Clone)]
pub struct ReconciliationLedgerWrite {
    /// Target execution-order state (terminal, or unchanged for `Unresolvable`).
    pub order_state: ExecutionOrderState,
    /// Target order-intent status (kept consistent with `order_state`).
    pub intent_status: OrderIntentStatus,
    /// Raw venue order status, when known.
    pub venue_status: Option<VenueOrderStatus>,
    /// Venue-assigned order id, when one is known (e.g. on an `Open` order with
    /// no submit-time recon row).
    pub venue_order_id: Option<OrderId>,
    /// When the order filled, when known.
    pub filled_at: Option<DateTime<Utc>>,
    /// When the order was cancelled/expired, when known.
    pub cancelled_at: Option<DateTime<Utc>>,
    /// Failure / unresolvable detail recorded on the order row.
    pub error_message: Option<String>,
    /// State-guarded capital correction.
    pub capital: CapitalReconcileSettlement,
    /// Position upsert (present only when correcting an **entry** order into a
    /// confirmed fill, applied exactly once as the order leaves a non-filled
    /// state).
    pub fill: Option<PositionFill>,
    /// Position-lot exit (present only when correcting an **exit** order into a
    /// confirmed (partial) fill). When set, the repo applies `apply_exit` (not
    /// `apply_fill`); on `exit_fully` it completes the capital `Spent ->
    /// Released`, and the entry intent's `status` is left unchanged.
    pub exit: Option<PositionExit>,
    /// Whether the reconciled exit fully closes the lot (`Spent -> Released`).
    pub exit_fully: bool,
    /// Exit-FSM state to set on the intent for an exit-order reconciliation.
    pub exit_state: Option<ExitState>,
    /// Revert the lot `Closing -> Open` (a reconciled exit non-fill / cancel).
    pub revert_lot: bool,
    /// Reconciliation verdict to persist on the summary row.
    pub result: ReconciliationResult,
    /// Full evidence chain collected this pass (replaces the row's chain;
    /// callers carry forward prior evidence to keep the row append-only).
    pub evidence: ReconciliationEvidenceChain,
    /// Venue-confirmed total filled shares, when known.
    pub venue_filled_shares: Option<Shares>,
    /// Venue-confirmed average fill price, when known.
    pub venue_avg_price: Option<Price>,
    /// Cash movement predicted from the frozen decision-time execution plan.
    pub expected_cash_delta_usd: Option<Usd>,
    /// Cash movement reconstructed from venue-observed fills and fees.
    pub venue_cash_delta_usd: Option<Usd>,
    /// Realized `PnL` for exit fills only.
    pub realized_pnl_usd: Option<Usd>,
    /// Fee recomputed from the frozen decision-time schedule.
    pub expected_fee_usd: Option<Usd>,
    /// Strongest authenticated/on-chain venue fee observation.
    pub observed_fee_usd: Option<Usd>,
    /// Signed `observed_fee_usd - expected_fee_usd` when both are known.
    pub fee_delta_usd: Option<Usd>,
    /// Who resolved (machine `system:reconciliation_worker` or an operator).
    /// `None` while the verdict is non-terminal or `Unresolvable`.
    pub resolved_by: Option<String>,
    /// When the reconciliation reached a terminal, actioned state. `None` for
    /// in-progress (`Pending`) or unresolved `Unresolvable` rows.
    pub resolved_at: Option<DateTime<Utc>>,
}
