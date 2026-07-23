use chrono::{DateTime, Utc};
use quant_pivot_error::storage::StorageError;
use quant_pivot_models::{
    domain::{
        api::settlement_redeem::SettlementGovernedActionListQuery,
        pagination::Paginated,
        quant::{
            settlement::SettlementChainSubmissionInfo,
            settlement_governance::{
                AdvanceSettlementExternalCursor, BeginGovernedActionDispatch,
                ConfirmSettlementGovernedAction, FailSettlementGovernedAction,
                NewSettlementExternalCursor, NewSettlementGovernedAction,
                PersistExternalSettlementScan, PersistPreparedGovernedActionSubmission,
                RecordGovernedActionEoaBroadcast, RecordGovernedActionRelayerAcceptance,
                RecordGovernedActionRelayerChainHash, RequireGovernedActionReconciliation,
                RevokeSettlementGovernedAction, ScheduleGovernedActionRetry,
                ScheduleGovernedActionWork, SettlementExternalCursorInfo,
                SettlementGovernedActionInfo, SettlementGovernedActionWorkClaim,
            },
        },
    },
    enums::settlement::SettlementRoute,
    types::{
        ContentHash, EvmAddress, ExecutionAccountId, SettlementExternalCursorId,
        SettlementGovernedActionId, SettlementRedeemId, WorkerId,
    },
};

#[async_trait::async_trait]
pub trait SettlementGovernanceRepository: Send + Sync {
    async fn create_action(
        &self,
        action: NewSettlementGovernedAction,
    ) -> Result<SettlementGovernedActionInfo, StorageError>;

    async fn find_action(
        &self,
        action_id: &SettlementGovernedActionId,
    ) -> Result<Option<SettlementGovernedActionInfo>, StorageError>;

    async fn page_actions(
        &self,
        query: SettlementGovernedActionListQuery,
    ) -> Result<Paginated<SettlementGovernedActionInfo>, StorageError>;

    async fn revoke_action(
        &self,
        command: RevokeSettlementGovernedAction,
    ) -> Result<SettlementGovernedActionInfo, StorageError>;

    async fn find_submission_by_action(
        &self,
        action_id: &SettlementGovernedActionId,
    ) -> Result<Option<SettlementChainSubmissionInfo>, StorageError>;

    async fn claim_next_action(
        &self,
        execution_account_id: &ExecutionAccountId,
        owner: &WorkerId,
        now: DateTime<Utc>,
        lease_expires_at: DateTime<Utc>,
    ) -> Result<Option<SettlementGovernedActionWorkClaim>, StorageError>;

    async fn renew_action_claim(
        &self,
        action_id: &SettlementGovernedActionId,
        owner: &WorkerId,
        now: DateTime<Utc>,
        lease_expires_at: DateTime<Utc>,
    ) -> Result<bool, StorageError>;

    async fn release_action_claim(
        &self,
        action_id: &SettlementGovernedActionId,
        owner: &WorkerId,
    ) -> Result<bool, StorageError>;

    async fn persist_prepared_action_submission(
        &self,
        command: PersistPreparedGovernedActionSubmission,
    ) -> Result<SettlementChainSubmissionInfo, StorageError>;

    async fn begin_action_dispatch(
        &self,
        command: BeginGovernedActionDispatch,
    ) -> Result<SettlementChainSubmissionInfo, StorageError>;

    async fn record_action_eoa_broadcast(
        &self,
        command: RecordGovernedActionEoaBroadcast,
    ) -> Result<SettlementChainSubmissionInfo, StorageError>;

    async fn record_action_relayer_acceptance(
        &self,
        command: RecordGovernedActionRelayerAcceptance,
    ) -> Result<SettlementChainSubmissionInfo, StorageError>;

    async fn record_action_relayer_chain_hash(
        &self,
        command: RecordGovernedActionRelayerChainHash,
    ) -> Result<SettlementChainSubmissionInfo, StorageError>;

    async fn schedule_action_retry(
        &self,
        command: ScheduleGovernedActionRetry,
    ) -> Result<SettlementGovernedActionInfo, StorageError>;

    async fn schedule_action_work(
        &self,
        command: ScheduleGovernedActionWork,
    ) -> Result<SettlementGovernedActionInfo, StorageError>;

    async fn fail_action(
        &self,
        command: FailSettlementGovernedAction,
    ) -> Result<SettlementGovernedActionInfo, StorageError>;

    async fn confirm_action(
        &self,
        command: ConfirmSettlementGovernedAction,
    ) -> Result<SettlementGovernedActionInfo, StorageError>;

    async fn require_action_reconciliation(
        &self,
        command: RequireGovernedActionReconciliation,
    ) -> Result<SettlementGovernedActionInfo, StorageError>;

    async fn find_authorized_canary(
        &self,
        settlement_redeem_id: &SettlementRedeemId,
        authorization_digest: ContentHash,
        route: SettlementRoute,
        target_adapter: &EvmAddress,
        deployment_digest: ContentHash,
        now: DateTime<Utc>,
    ) -> Result<Option<SettlementGovernedActionInfo>, StorageError>;

    async fn has_confirmed_canary(
        &self,
        execution_account_id: &ExecutionAccountId,
        route: SettlementRoute,
        deployment_digest: ContentHash,
    ) -> Result<bool, StorageError>;
}

#[async_trait::async_trait]
pub trait SettlementExternalCursorRepository: Send + Sync {
    async fn ensure_cursor(
        &self,
        cursor: NewSettlementExternalCursor,
    ) -> Result<SettlementExternalCursorInfo, StorageError>;

    async fn find_cursor(
        &self,
        cursor_id: &SettlementExternalCursorId,
    ) -> Result<Option<SettlementExternalCursorInfo>, StorageError>;

    async fn advance_cursor(
        &self,
        command: AdvanceSettlementExternalCursor,
    ) -> Result<SettlementExternalCursorInfo, StorageError>;

    async fn persist_scan(
        &self,
        command: PersistExternalSettlementScan,
    ) -> Result<SettlementExternalCursorInfo, StorageError>;
}
