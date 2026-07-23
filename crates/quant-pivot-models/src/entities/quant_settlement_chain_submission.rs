//! `quant_settlement_chain_submission` table entity.

use chrono::{DateTime, Utc};
use sea_orm::entity::prelude::*;

use super::{quant_settlement_governed_action, quant_settlement_redeem};
use crate::{
    enums::settlement::{
        SettlementFailureCode, SettlementRoute, SettlementSubmissionKind,
        SettlementSubmissionPurpose, SettlementSubmissionState,
    },
    types::{
        ContentHash, EvmAddress, EvmBlockHash, EvmCalldataHash, EvmCodeHash, EvmTransactionHash,
        EvmUint256, RelayerTransactionId, SettlementChainSubmissionId, SettlementEvidenceVersion,
        SettlementGovernedActionId, SettlementRedeemId,
        settlement_payload::{SettlementChainReceiptEvidence, SettlementFailureHistory},
    },
};

#[sea_orm::model]
#[derive(Clone, Debug, PartialEq, Eq, DeriveEntityModel)]
#[sea_orm(table_name = "quant_settlement_chain_submission")]
pub struct Model {
    #[sea_orm(primary_key, auto_increment = false)]
    pub settlement_chain_submission_id: SettlementChainSubmissionId,
    pub settlement_redeem_id: Option<SettlementRedeemId>,
    pub settlement_governed_action_id: Option<SettlementGovernedActionId>,
    pub canary_action_id: Option<SettlementGovernedActionId>,
    pub purpose: SettlementSubmissionPurpose,
    pub kind: SettlementSubmissionKind,
    pub state: SettlementSubmissionState,
    pub route: SettlementRoute,
    pub target_adapter: EvmAddress,
    pub target_code_hash: EvmCodeHash,
    pub conditional_tokens: EvmAddress,
    pub collateral_token: EvmAddress,
    pub usdce: EvmAddress,
    pub call_target: EvmAddress,
    pub deployment_digest: ContentHash,
    pub deployment_evidence_version: SettlementEvidenceVersion,
    pub verified_block_number: i64,
    pub verified_block_hash: EvmBlockHash,
    pub prepared_block_number: Option<i64>,
    pub prepared_block_hash: Option<EvmBlockHash>,
    pub calldata_hash: EvmCalldataHash,
    pub calldata: Vec<u8>,
    pub signed_envelope: Option<Vec<u8>>,
    pub signed_envelope_hash: Option<ContentHash>,
    pub prepared_nonce: Option<EvmUint256>,
    pub gas_limit: Option<EvmUint256>,
    pub relayer_transaction_id: Option<RelayerTransactionId>,
    pub transaction_hash: Option<EvmTransactionHash>,
    pub failure_code: Option<SettlementFailureCode>,
    #[sea_orm(column_type = "JsonBinary")]
    pub failure_history_json: SettlementFailureHistory,
    #[sea_orm(column_type = "JsonBinary", nullable)]
    pub receipt_evidence_json: Option<SettlementChainReceiptEvidence>,
    pub attempt_ordinal: i32,
    #[sea_orm(column_type = "Text", nullable)]
    pub last_error: Option<String>,
    pub dispatched_at: Option<DateTime<Utc>>,
    pub chain_hash_observed_at: Option<DateTime<Utc>>,
    pub confirmed_at: Option<DateTime<Utc>>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,

    #[sea_orm(
        belongs_to,
        relation_enum = "SettlementRedeem",
        from = "settlement_redeem_id",
        to = "settlement_redeem_id"
    )]
    pub settlement_redeem: BelongsTo<Option<quant_settlement_redeem::Entity>>,
    #[sea_orm(
        belongs_to,
        relation_enum = "GovernedAction",
        from = "settlement_governed_action_id",
        to = "settlement_governed_action_id"
    )]
    pub governed_action: BelongsTo<Option<quant_settlement_governed_action::Entity>>,
    #[sea_orm(
        belongs_to,
        relation_enum = "CanaryAction",
        from = "canary_action_id",
        to = "settlement_governed_action_id"
    )]
    pub canary_action: BelongsTo<Option<quant_settlement_governed_action::Entity>>,
}

impl ActiveModelBehavior for ActiveModel {}
