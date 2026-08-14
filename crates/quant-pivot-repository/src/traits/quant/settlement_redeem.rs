use chrono::{DateTime, Utc};
use quant_pivot_error::storage::StorageError;
use quant_pivot_models::{
    domain::{
        api::settlement_redeem::{SettlementRedeemListQuery, SettlementRedeemSummary},
        pagination::Paginated,
        quant::{
            settlement::{
                ApproveSettlementAuthorization, BeginSettlementDispatch, ConfirmSettlementRedeem,
                NewSettlementRedeem, PersistPreparedSettlementSubmission,
                PersistSettlementPreflight, RecordEoaSettlementBroadcast,
                RecordRelayerSettlementAcceptance, RecordRelayerSettlementChainHash,
                RequireSettlementReconciliation, RevokeSettlementAuthorization,
                ScheduleSettlementRetry, ScheduleSettlementWork, SettlementChainSubmissionInfo,
                SettlementRedeemInfo, SettlementRedeemLotInfo, SettlementSubmissionOutcome,
                SettlementWorkClaim, StageSettlementAuthorization,
            },
            settlement_inventory::{
                MarkSettlementInventoryAbsent, NewSettlementInventoryLot,
                RefreshSettlementInventory, SettlementDiscoveryCandidate,
                SettlementInventoryLotInfo,
            },
        },
    },
    types::{
        ExecutionAccountId, MarketId, SettlementChainSubmissionId, SettlementRedeemId, WorkerId,
    },
};

#[async_trait::async_trait]
pub trait SettlementRedeemRepository: Send + Sync {
    async fn find_by_id(
        &self,
        settlement_redeem_id: &SettlementRedeemId,
    ) -> Result<Option<SettlementRedeemInfo>, StorageError>;

    async fn find_by_market_account(
        &self,
        market_id: &MarketId,
        execution_account_id: &ExecutionAccountId,
    ) -> Result<Option<SettlementRedeemInfo>, StorageError>;

    /// Page cases with the contributor count of each exact current inventory.
    async fn page(
        &self,
        query: SettlementRedeemListQuery,
    ) -> Result<Paginated<SettlementRedeemSummary>, StorageError>;

    async fn list_lots_by_redeem(
        &self,
        settlement_redeem_id: &SettlementRedeemId,
    ) -> Result<Vec<SettlementRedeemLotInfo>, StorageError>;

    async fn find_submission_by_id(
        &self,
        submission_id: &SettlementChainSubmissionId,
    ) -> Result<Option<SettlementChainSubmissionInfo>, StorageError>;

    async fn list_submissions_by_redeem(
        &self,
        settlement_redeem_id: &SettlementRedeemId,
    ) -> Result<Vec<SettlementChainSubmissionInfo>, StorageError>;

    /// Resolved `PostgreSQL` market/account groups with open lots and no case.
    async fn find_discovery_candidates(
        &self,
        limit: u64,
    ) -> Result<Vec<SettlementDiscoveryCandidate>, StorageError>;

    /// Current durable inventory for one exact market/account pair, including an existing case.
    async fn load_inventory_candidate(
        &self,
        market_id: &MarketId,
        execution_account_id: &ExecutionAccountId,
    ) -> Result<Option<SettlementDiscoveryCandidate>, StorageError>;

    /// Pre-submission cases whose inventory digest must be revalidated.
    async fn list_refreshable_inventory_cases(
        &self,
        limit: u64,
    ) -> Result<Vec<SettlementRedeemInfo>, StorageError>;

    /// Atomically create a case and its first immutable inventory snapshot.
    async fn insert_discovered_case(
        &self,
        redeem: NewSettlementRedeem,
        lots: Vec<NewSettlementInventoryLot>,
    ) -> Result<SettlementRedeemInfo, StorageError>;

    /// Append a new snapshot and atomically move an unsubmitted case to its new digest.
    async fn refresh_discovered_inventory(
        &self,
        command: RefreshSettlementInventory,
    ) -> Result<SettlementRedeemInfo, StorageError>;

    async fn mark_inventory_absent(
        &self,
        command: MarkSettlementInventoryAbsent,
    ) -> Result<SettlementRedeemInfo, StorageError>;

    async fn list_current_inventory(
        &self,
        settlement_redeem_id: &SettlementRedeemId,
    ) -> Result<Vec<SettlementInventoryLotInfo>, StorageError>;

    /// Count non-terminal orders that can still change one account's market inventory.
    async fn count_unsettled_execution_orders(
        &self,
        market_id: &MarketId,
        execution_account_id: &ExecutionAccountId,
    ) -> Result<u64, StorageError>;

    /// Claim the oldest case with an active durable submission. This path is
    /// independent from runtime mode, kill switch and current deployment.
    async fn claim_next_recovery(
        &self,
        owner: &WorkerId,
        now: DateTime<Utc>,
        lease_expires_at: DateTime<Utc>,
    ) -> Result<Option<SettlementWorkClaim>, StorageError>;

    /// Claim one unresolved case for signer-free capability, balance and
    /// simulation preflight. This never claims a case with durable submission.
    async fn claim_next_preflight(
        &self,
        owner: &WorkerId,
        now: DateTime<Utc>,
        lease_expires_at: DateTime<Utc>,
    ) -> Result<Option<SettlementWorkClaim>, StorageError>;

    /// Claim the oldest ready case that has no active submission. Admission is
    /// evaluated by the service after this mutually-exclusive database claim.
    async fn claim_next_new_submission(
        &self,
        owner: &WorkerId,
        now: DateTime<Utc>,
        lease_expires_at: DateTime<Utc>,
    ) -> Result<Option<SettlementWorkClaim>, StorageError>;

    async fn renew_claim(
        &self,
        settlement_redeem_id: &SettlementRedeemId,
        owner: &WorkerId,
        now: DateTime<Utc>,
        lease_expires_at: DateTime<Utc>,
    ) -> Result<bool, StorageError>;

    async fn release_claim(
        &self,
        settlement_redeem_id: &SettlementRedeemId,
        owner: &WorkerId,
    ) -> Result<bool, StorageError>;

    async fn persist_preflight(
        &self,
        command: PersistSettlementPreflight,
    ) -> Result<SettlementRedeemInfo, StorageError>;

    async fn schedule_retry(
        &self,
        command: ScheduleSettlementRetry,
    ) -> Result<SettlementRedeemInfo, StorageError>;

    async fn schedule_work(
        &self,
        command: ScheduleSettlementWork,
    ) -> Result<SettlementRedeemInfo, StorageError>;

    async fn stage_authorization(
        &self,
        command: StageSettlementAuthorization,
    ) -> Result<SettlementRedeemInfo, StorageError>;

    async fn approve_authorization(
        &self,
        command: ApproveSettlementAuthorization,
    ) -> Result<SettlementRedeemInfo, StorageError>;

    async fn revoke_authorization(
        &self,
        command: RevokeSettlementAuthorization,
    ) -> Result<SettlementRedeemInfo, StorageError>;

    /// Persist a signed envelope before any network dispatch. Approved
    /// `SemiAuto` authorization is consumed in the same transaction.
    async fn persist_prepared_submission(
        &self,
        command: PersistPreparedSettlementSubmission,
    ) -> Result<SettlementSubmissionOutcome, StorageError>;

    /// Compare-and-swap the exact durable target/digest/calldata/envelope into
    /// `Dispatching`; only after this succeeds may transport be invoked.
    async fn begin_dispatch(
        &self,
        command: BeginSettlementDispatch,
    ) -> Result<SettlementSubmissionOutcome, StorageError>;

    async fn record_eoa_broadcast(
        &self,
        command: RecordEoaSettlementBroadcast,
    ) -> Result<SettlementSubmissionOutcome, StorageError>;

    async fn record_relayer_acceptance(
        &self,
        command: RecordRelayerSettlementAcceptance,
    ) -> Result<SettlementSubmissionOutcome, StorageError>;

    async fn record_relayer_chain_hash(
        &self,
        command: RecordRelayerSettlementChainHash,
    ) -> Result<SettlementSubmissionOutcome, StorageError>;

    async fn confirm(
        &self,
        write: ConfirmSettlementRedeem,
    ) -> Result<SettlementRedeemInfo, StorageError>;

    async fn require_reconciliation(
        &self,
        write: RequireSettlementReconciliation,
    ) -> Result<SettlementRedeemInfo, StorageError>;
}
