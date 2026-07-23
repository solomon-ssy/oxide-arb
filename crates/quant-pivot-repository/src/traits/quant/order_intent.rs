use chrono::{DateTime, Utc};
use quant_pivot_error::storage::StorageError;
use quant_pivot_models::{
    domain::{
        api::OrderIntentListQuery,
        governance::NewOperationLog,
        pagination::Paginated,
        quant::{
            ApproveOrderIntent, ApproveOrderIntentOutcome, NewCapitalAllocation, NewOrderIntent,
            OrderIntentInfo,
        },
    },
    enums::{execution::ApprovalInvalidation, quant::OrderIntentStatus},
    types::{EntryOrderSpec, OrderIntentId, RecommendationId, RecommendationReportId, Usd},
};

/// Governed order-intent persistence port.
///
/// Every money-moving mutation is **atomic over the intent FSM and the capital
/// allocation FSM in one Postgres transaction** — an intent never exists without
/// its reservation and vice versa. Background-origin terminal transitions
/// (`expire` / `invalidate`) additionally write their operation-log row inside
/// the same transaction (HTTP-origin mutations are audited by the web
/// middleware, so they take no log row).
#[async_trait::async_trait]
pub trait OrderIntentRepository: Send + Sync {
    /// Create an intent (`PendingApproval` or `ApprovedByPolicy`) and reserve its
    /// capital (`Allocated`) atomically.
    async fn create_with_allocation(
        &self,
        intent: NewOrderIntent,
        allocation: NewCapitalAllocation,
    ) -> Result<OrderIntentInfo, StorageError>;

    /// Approve a `PendingApproval` intent. Re-reads recommendation, report,
    /// runtime config, and kill-switch state **inside the same transaction**
    /// (after row lock) before transitioning. When `entry_override` /
    /// `allocated_override` are present the entry is narrowed and the reserved
    /// capital shrunk to match. Returns [`ApproveOrderIntentOutcome::Invalidated`]
    /// when a governed fact changed (capital released, no operation-log row).
    async fn approve(
        &self,
        intent_id: &OrderIntentId,
        approval: ApproveOrderIntent,
        entry_override: Option<EntryOrderSpec>,
        allocated_override: Option<Usd>,
        now: DateTime<Utc>,
    ) -> Result<ApproveOrderIntentOutcome, StorageError>;

    /// Reject a `PendingApproval` intent and release its capital atomically.
    async fn reject(
        &self,
        intent_id: &OrderIntentId,
        reason: String,
        rejected_at: DateTime<Utc>,
        operation_log: NewOperationLog,
    ) -> Result<OrderIntentInfo, StorageError>;

    /// Cancel a not-yet-submitted intent and release its capital atomically.
    async fn cancel(
        &self,
        intent_id: &OrderIntentId,
        reason: String,
        cancelled_at: DateTime<Utc>,
        operation_log: NewOperationLog,
    ) -> Result<OrderIntentInfo, StorageError>;

    /// Expire a due intent: release its capital and write the operation log in
    /// the same transaction (background origin).
    async fn expire(
        &self,
        intent_id: &OrderIntentId,
        expired_at: DateTime<Utc>,
        operation_log: NewOperationLog,
    ) -> Result<OrderIntentInfo, StorageError>;

    /// Invalidate an intent for a changed governed fact: release its capital and
    /// write the operation log in the same transaction (background origin).
    async fn invalidate(
        &self,
        intent_id: &OrderIntentId,
        reason: ApprovalInvalidation,
        invalidated_at: DateTime<Utc>,
        operation_log: NewOperationLog,
    ) -> Result<OrderIntentInfo, StorageError>;

    /// Load one intent by id.
    async fn find_by_id(
        &self,
        intent_id: &OrderIntentId,
    ) -> Result<Option<OrderIntentInfo>, StorageError>;

    /// Batch-load intents by id (chunked `IN` lists). Missing ids are omitted.
    async fn find_by_ids(
        &self,
        intent_ids: &[OrderIntentId],
    ) -> Result<Vec<OrderIntentInfo>, StorageError>;

    /// Page intents filtered by status / mode / recommendation / `created_at`.
    async fn page(
        &self,
        query: OrderIntentListQuery,
    ) -> Result<Paginated<OrderIntentInfo>, StorageError>;

    /// Intents past `expires_at` still in an expirable status (sweep input).
    async fn find_expired(&self, now: DateTime<Utc>) -> Result<Vec<OrderIntentInfo>, StorageError>;

    /// Ids + `expires_at` deadlines of expirable intents due at or before
    /// `before`, earliest deadline first, capped at `limit`. Feeds the deadline
    /// scheduler's bounded look-ahead horizon (the DB stays the source of truth).
    async fn upcoming_expirations(
        &self,
        before: DateTime<Utc>,
        limit: u64,
    ) -> Result<Vec<(OrderIntentId, DateTime<Utc>)>, StorageError>;

    /// Blocking intent for a recommendation, if any (create dedup — includes in-flight submission).
    async fn find_active_by_recommendation(
        &self,
        recommendation_id: &RecommendationId,
    ) -> Result<Option<OrderIntentInfo>, StorageError>;

    /// All active (pre-submission) intents for a recommendation — the
    /// per-recommendation expiry cascade (release reserved capital).
    async fn find_active_intents_by_recommendation(
        &self,
        recommendation_id: &RecommendationId,
    ) -> Result<Vec<OrderIntentInfo>, StorageError>;

    /// Active (pre-submission) intents for a report — report-termination cascade.
    async fn find_active_by_report(
        &self,
        report_id: &RecommendationReportId,
    ) -> Result<Vec<OrderIntentInfo>, StorageError>;

    /// Blocking intents for a report's recommendations (create dedup + outbound
    /// view assembly — includes in-flight submission statuses).
    async fn find_blocking_by_report(
        &self,
        report_id: &RecommendationReportId,
    ) -> Result<Vec<OrderIntentInfo>, StorageError>;

    /// Count intents currently open (capital-holding or in-flight, i.e.
    /// [`OrderIntentStatus::OPEN`]). Feeds the admission concurrency cap (`#21`).
    async fn count_open(&self) -> Result<u64, StorageError>;

    /// Terminal / near-terminal intents whose parent recommendation has not yet
    /// received a final attribution row. Used by the attribution worker; the
    /// builder still re-checks execution / position state before writing WORM.
    async fn find_attribution_candidates(
        &self,
        statuses: Vec<OrderIntentStatus>,
        limit: u64,
    ) -> Result<Vec<OrderIntentInfo>, StorageError>;
}
