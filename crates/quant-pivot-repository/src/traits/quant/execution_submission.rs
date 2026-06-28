use chrono::{DateTime, Utc};
use quant_pivot_error::storage::StorageError;
use quant_pivot_models::{
    domain::{
        ExecutionOrderInfo, NewExecutionOrder, OrderIntentInfo, ReconciliationLedgerWrite,
        SubmissionLedgerWrite,
    },
    types::{ExecutionOrderId, OrderIntentId},
};

/// Cross-table execution-submission transactions (Phase 05.4 — real money).
///
/// Each method owns exactly one Postgres transaction spanning the execution
/// order, order intent, capital allocation, position ledger, recommendation, and
/// reconciliation tables, so a submission's money state can never partially
/// apply. Network I/O (venue sign + post) happens **between**
/// [`create_entry_order_and_lock_capital`](Self::create_entry_order_and_lock_capital)
/// and [`record_submission_result`](Self::record_submission_result), never
/// inside a transaction (no DB lock is held across venue calls).
#[async_trait::async_trait]
pub trait ExecutionSubmissionRepository: Send + Sync {
    /// Row-lock the intent, verify it is submittable at `now`
    /// (`Approved`/`ApprovedByPolicy`, not expired), and claim it by
    /// transitioning to `AdmissionPending`. A concurrent claimer that wins the
    /// row lock first leaves the loser observing `AdmissionPending` (not
    /// submittable) — this is the double-submit guard.
    async fn claim_for_submission(
        &self,
        intent_id: &OrderIntentId,
        now: DateTime<Utc>,
    ) -> Result<OrderIntentInfo, StorageError>;

    /// Release a claim on transient admission defer: `AdmissionPending ->
    /// Approved` (semi-auto) or `ApprovedByPolicy` (auto), so the dispatcher may
    /// retry later. No-op (returns the current row) if the intent has since left
    /// `AdmissionPending`.
    async fn revert_claim(
        &self,
        intent_id: &OrderIntentId,
    ) -> Result<OrderIntentInfo, StorageError>;

    /// Terminal admission deny: `AdmissionPending -> AdmissionRejected`, record
    /// the trace reference, and release the reserved capital — all in one txn.
    async fn reject_admission(
        &self,
        intent_id: &OrderIntentId,
        status_reason: String,
        admission_trace_ref: Option<String>,
    ) -> Result<OrderIntentInfo, StorageError>;

    /// Write-ahead the submission in one txn: insert the execution order
    /// (`state = Submitted`, the durable intent-to-submit marker), lock the
    /// capital (`Allocated -> Locked`), advance the intent (`AdmissionPending ->
    /// Submitted`), and advance the recommendation (`-> Executed`). The intent
    /// row is re-locked and its `AdmissionPending` claim re-verified inside this
    /// txn.
    async fn create_entry_order_and_lock_capital(
        &self,
        order: NewExecutionOrder,
    ) -> Result<ExecutionOrderInfo, StorageError>;

    /// Apply the venue outcome in one txn: advance the entry state, settle the
    /// capital (`Locked -> Spent`/`Released`, partial, or hold), upsert the
    /// position on a fill, advance the intent, and enqueue reconciliation.
    /// `Ambiguous` outcomes hold capital and enqueue recon (fail-closed).
    async fn record_submission_result(
        &self,
        execution_order_id: &ExecutionOrderId,
        write: SubmissionLedgerWrite,
    ) -> Result<ExecutionOrderInfo, StorageError>;

    /// Boot recovery: in-flight orders (`Submitted` / `Ambiguous`) with no
    /// terminal resolution, handed to reconciliation after a process crash.
    async fn recover_dangling(&self, limit: u64) -> Result<Vec<ExecutionOrderInfo>, StorageError>;

    /// Apply a reconciliation verdict in one txn (Phase 05.5): advance the entry
    /// order to its terminal state, correct the capital (state-guarded and
    /// idempotent), upsert the position on a confirmed fill (applied exactly
    /// once as the order leaves a non-filled state), advance the intent, and
    /// upsert the reconciliation summary row (appending the freshly-collected
    /// evidence — append-only / WORM). An order already in a terminal state is
    /// returned unchanged (idempotent no-op), so repeated reconciliation can
    /// never double-count capital or shares.
    async fn apply_reconciliation(
        &self,
        execution_order_id: &ExecutionOrderId,
        write: ReconciliationLedgerWrite,
    ) -> Result<ExecutionOrderInfo, StorageError>;
}
