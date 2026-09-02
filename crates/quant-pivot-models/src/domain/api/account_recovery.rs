//! Strict break-glass account-recovery API contract.

use chrono::{DateTime, Utc};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use validator::Validate;

use crate::{
    domain::quant::{
        AccountPauseOperationInfo, AccountRecoveryAssessment, AccountRecoveryAssessmentInput,
        AccountRecoveryIncidentInfo, AccountRecoveryManifestInfo, AccountRecoverySellAllocation,
    },
    enums::{
        execution::{AccountPauseOperationKind, AccountPauseOperationState},
        settlement::SettlementSubmissionKind,
    },
    types::{
        AccountPauseOperationId, AccountRecoveryManifestId, ContentHash, EvmAddress, EvmBlockHash,
        EvmTransactionHash,
    },
};

#[derive(Debug, Clone, Serialize, JsonSchema)]
pub struct AccountPauseOperationView {
    pub account_pause_operation_id: AccountPauseOperationId,
    pub exchange_address: EvmAddress,
    pub operation_kind: AccountPauseOperationKind,
    pub state: AccountPauseOperationState,
    pub submission_kind: SettlementSubmissionKind,
    pub requested_block: i64,
    pub effective_block: Option<i64>,
    pub transaction_hash: Option<EvmTransactionHash>,
    pub confirmation_block_number: Option<i64>,
    pub confirmation_block_hash: Option<EvmBlockHash>,
    pub failure_detail: Option<String>,
    pub dispatched_at: Option<DateTime<Utc>>,
    pub confirmed_at: Option<DateTime<Utc>>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

impl From<AccountPauseOperationInfo> for AccountPauseOperationView {
    fn from(info: AccountPauseOperationInfo) -> Self {
        Self {
            account_pause_operation_id: info.account_pause_operation_id,
            exchange_address: info.exchange_address,
            operation_kind: info.operation_kind,
            state: info.state,
            submission_kind: info.submission_kind,
            requested_block: info.requested_block,
            effective_block: info.effective_block,
            transaction_hash: info.transaction_hash,
            confirmation_block_number: info.confirmation_block_number,
            confirmation_block_hash: info.confirmation_block_hash,
            failure_detail: info.failure_detail,
            dispatched_at: info.dispatched_at,
            confirmed_at: info.confirmed_at,
            created_at: info.created_at,
            updated_at: info.updated_at,
        }
    }
}

#[derive(Debug, Clone, Serialize, JsonSchema)]
pub struct AccountRecoveryManifestView {
    pub account_recovery_manifest_id: AccountRecoveryManifestId,
    pub attempt_no: i32,
    pub observed_at: DateTime<Utc>,
    pub finalized_block_number: i64,
    pub finalized_block_hash: EvmBlockHash,
    pub converged: bool,
    pub input: AccountRecoveryAssessmentInput,
    pub assessment: AccountRecoveryAssessment,
    pub evidence_hash: ContentHash,
    pub created_at: DateTime<Utc>,
}

impl From<AccountRecoveryManifestInfo> for AccountRecoveryManifestView {
    fn from(info: AccountRecoveryManifestInfo) -> Self {
        Self {
            account_recovery_manifest_id: info.account_recovery_manifest_id,
            attempt_no: info.attempt_no,
            observed_at: info.observed_at,
            finalized_block_number: info.finalized_block_number,
            finalized_block_hash: info.finalized_block_hash,
            converged: info.converged,
            input: info.input_json,
            assessment: info.assessment_json,
            evidence_hash: info.evidence_hash,
            created_at: info.created_at,
        }
    }
}

#[derive(Debug, Clone, Serialize, JsonSchema)]
pub struct AccountRecoveryIncidentView {
    pub incident: AccountRecoveryIncidentInfo,
    pub latest_manifest: Option<AccountRecoveryManifestView>,
    pub pause_operations: Vec<AccountPauseOperationView>,
}

#[derive(Debug, Clone, Deserialize, Validate, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ReconcileAccountRecoveryRequest {
    #[validate(range(min = 0))]
    pub expected_revision: i64,
    pub sell_allocations: Vec<AccountRecoverySellAllocation>,
    #[validate(length(min = 1, max = 1024))]
    pub reason: String,
}

#[derive(Debug, Clone, Deserialize, Validate, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct SealAccountRecoveryRequest {
    #[validate(range(min = 0))]
    pub expected_revision: i64,
    pub account_recovery_manifest_id: AccountRecoveryManifestId,
    #[validate(length(min = 1, max = 1024))]
    pub reason: String,
}

#[derive(Debug, Clone, Deserialize, Validate, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct FinalizeAccountRecoveryRequest {
    #[validate(range(min = 0))]
    pub expected_revision: i64,
    #[validate(length(min = 1, max = 1024))]
    pub reason: String,
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::ReconcileAccountRecoveryRequest;

    #[test]
    fn reconcile_requires_allocations() {
        let missing = serde_json::from_value::<ReconcileAccountRecoveryRequest>(json!({
            "expected_revision": 1,
            "reason": "operator supplied exact recovery evidence",
        }));
        assert!(missing.is_err());

        let explicit_empty = serde_json::from_value::<ReconcileAccountRecoveryRequest>(json!({
            "expected_revision": 1,
            "sell_allocations": [],
            "reason": "no external sells require lot allocation",
        }));
        assert!(explicit_empty.is_ok());
    }
}
