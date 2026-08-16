use chrono::{DateTime, Utc};
use quant_pivot_error::storage::StorageError;
use quant_pivot_models::{
    domain::quant::{
        EntryConditionClaim, EntryConditionInstanceInfo, ExecutionIdentityEnrichment,
        ExecutionOrderIdentityRefs, ExecutionOrderInfo, ExitLedgerWrite, NewExecutionOrder,
        OrderIntentInfo, PendingExecutionFeeSettlement, ReconciliationLedgerWrite,
        SubmissionLedgerWrite,
    },
    enums::execution::ExitReason,
    types::{
        ExecutionOrderId, ExitReinferenceObservation, FeatureParityStateId, FeeMeasurement,
        OrderIntentId, PendingScaleOut, Price,
    },
};

/// Money-critical cross-table execution-submission transactions.
///
/// Each method owns exactly one Postgres transaction spanning the execution
/// order, order intent, capital allocation, position ledger, recommendation, and
/// reconciliation tables, so a submission's money state can never partially
/// apply. Network I/O (venue sign + post) happens **between**
/// [`create_entry_order`](Self::create_entry_order)
/// and [`record_submission_result`](Self::record_submission_result), never
/// inside a transaction (no DB lock is held across venue calls).
#[async_trait::async_trait]
pub trait ExecutionSubmissionRepository: Send + Sync {
    /// Row-lock the intent and its exact condition revision, verify both are
    /// claimable, then atomically transition the intent to `AdmissionPending`
    /// and the condition to `Consumed`. A concurrent claimer observes neither
    /// half of the transition.
    async fn claim_for_submission(
        &self,
        claim: EntryConditionClaim,
    ) -> Result<(OrderIntentInfo, EntryConditionInstanceInfo), StorageError>;

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
    /// txn. The exact clear parity generation is verified under the global
    /// parity advisory lock in the same transaction.
    async fn create_entry_order(
        &self,
        order: NewExecutionOrder,
        feature_parity_state_id: &FeatureParityStateId,
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

    /// Load the complete typed venue identity graph for reconciliation.
    async fn load_identity_refs(
        &self,
        execution_order_id: &ExecutionOrderId,
    ) -> Result<ExecutionOrderIdentityRefs, StorageError>;

    /// Atomically enrich exact order/trade observations. A trade ID already
    /// owned by another order is a hard conflict; every non-zero hash is also
    /// retained in the append-only transaction-ref ledger.
    async fn enrich_identity_refs(
        &self,
        execution_order_id: &ExecutionOrderId,
        enrichment: ExecutionIdentityEnrichment,
    ) -> Result<ExecutionOrderIdentityRefs, StorageError>;

    /// Write-ahead an exit (Sell) order in one txn: insert the exit
    /// execution order (`order_phase = Exit`, `state = Submitted`), mark the
    /// per-intent position lot `Open -> Closing`, and advance the intent's exit
    /// FSM to `OrderSubmitted` recording `exit_reason`. No capital change — the
    /// lot's capital is already `Spent` from entry.
    async fn create_exit_order(
        &self,
        order: NewExecutionOrder,
        exit_reason: ExitReason,
        pending_scale_out: Option<PendingScaleOut>,
    ) -> Result<ExecutionOrderInfo, StorageError>;

    /// Route a lot to manual exit handling (`exit_state = ManualRequired`,
    /// recording `exit_reason`). Fail-closed path for data-stale / market-abnormal
    /// / emergency-manual / auto-exit-frozen decisions.
    async fn mark_exit_manual(
        &self,
        intent_id: &OrderIntentId,
        reason: ExitReason,
    ) -> Result<(), StorageError>;

    /// Persist a `Hold` tick's monitoring bookkeeping: advance `next_check_at`,
    /// the trailing `peak_mark_price` (when present), and `last_signal_recheck_at`
    /// (when a re-inference ran). Promotes `NotStarted -> Monitoring` on first
    /// scan; never downgrades an in-flight / partially-exited state.
    async fn touch_exit_monitor(
        &self,
        intent_id: &OrderIntentId,
        next_check_at: DateTime<Utc>,
        peak_mark_price: Option<Price>,
        last_signal_recheck_at: Option<DateTime<Utc>>,
        latest_reinference: Option<ExitReinferenceObservation>,
    ) -> Result<(), StorageError>;

    /// Apply the exit venue outcome in one txn: advance the exit
    /// order state, reduce/close the position lot on a (partial) fill (exact
    /// average-cost realized `PnL`), complete the capital (`Spent -> Released`) on
    /// full exit, advance the intent's exit FSM, revert `Closing -> Open` on a
    /// failed/cancelled attempt, and enqueue reconciliation. `Ambiguous` exits
    /// hold the position and enqueue recon (fail-closed). Idempotent: a terminal
    /// exit order is returned unchanged.
    async fn record_exit_result(
        &self,
        execution_order_id: &ExecutionOrderId,
        write: ExitLedgerWrite,
    ) -> Result<ExecutionOrderInfo, StorageError>;

    /// Boot recovery: in-flight orders (`Submitted` / `Ambiguous`) with no
    /// terminal resolution, handed to reconciliation after a process crash.
    async fn recover_dangling(&self, limit: u64) -> Result<Vec<ExecutionOrderInfo>, StorageError>;

    /// Authenticated fills whose exact V2 fee has not yet arrived from the
    /// finalized chain projection.
    async fn unsettled_fills(
        &self,
        limit: u64,
    ) -> Result<Vec<PendingExecutionFeeSettlement>, StorageError>;

    /// Idempotently append exact chain-settled fee measurements. Existing
    /// expected/derived rows are immutable and remain available for variance
    /// analysis.
    async fn record_fee_settlements(
        &self,
        measurements: Vec<FeeMeasurement>,
    ) -> Result<(), StorageError>;

    /// Apply a reconciliation verdict in one txn: advance the entry
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
