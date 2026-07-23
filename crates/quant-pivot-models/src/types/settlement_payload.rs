//! Strongly typed settlement evidence persisted as canonical JSONB documents.

use chrono::{DateTime, Utc};
use sea_orm::FromJsonQueryResult;
use serde::{Deserialize, Serialize};

use crate::{
    domain::quant::settlement_readiness::{
        SettlementDeploymentEvidence, SettlementReadinessReason,
    },
    enums::{quant::ExecutionWalletKind, settlement::SettlementFailureCode},
    types::{
        EvmAddress, EvmBlockHash, EvmCalldataHash, EvmTransactionHash, EvmUint256, Shares, TokenId,
        Usd,
    },
};

/// Closed deployment-readiness reasons and non-blocking advisories captured
/// together at one canonical Polygon block.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize, FromJsonQueryResult)]
#[serde(deny_unknown_fields)]
pub struct SettlementReadinessEvidence {
    pub reasons: Vec<SettlementReadinessReason>,
    pub advisories: Vec<SettlementDeploymentEvidence>,
}

/// CTF payout vector frozen before a redemption is prepared.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, FromJsonQueryResult)]
#[serde(deny_unknown_fields)]
pub struct SettlementPayoutVector {
    /// CTF denominator. Zero means the condition is not redeemable.
    pub denominator: EvmUint256,
    /// YES payout numerator.
    pub yes: EvmUint256,
    /// NO payout numerator.
    pub no: EvmUint256,
}

impl SettlementPayoutVector {
    /// Unresolved vector used before a fresh chain preflight freezes payout semantics.
    #[must_use]
    pub fn unresolved() -> Self {
        Self {
            denominator: EvmUint256::zero(),
            yes: EvmUint256::zero(),
            no: EvmUint256::zero(),
        }
    }
}

/// Token-balance evidence for one fixed binary outcome side.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SettlementTokenBalance {
    pub token_id: TokenId,
    pub raw_balance: EvmUint256,
    pub shares: Shares,
}

/// YES/NO balance evidence captured at one chain observation boundary.
///
/// Binary route index sets are a contract invariant (`YES = 1`, `NO = 2`) and
/// are deliberately not persisted as row-configurable data.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, FromJsonQueryResult)]
#[serde(deny_unknown_fields)]
pub struct SettlementBalanceEvidence {
    pub yes: SettlementTokenBalance,
    pub no: SettlementTokenBalance,
}

/// One typed, sanitized failure observation retained for an attempt.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SettlementFailureEvidence {
    pub code: SettlementFailureCode,
    pub detail: String,
    pub observed_at: DateTime<Utc>,
}

/// Append-ordered failure history for one durable submission identity.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize, FromJsonQueryResult)]
#[serde(deny_unknown_fields)]
pub struct SettlementFailureHistory {
    pub entries: Vec<SettlementFailureEvidence>,
}

/// Exact pUSD mint log emitted while wrapping redeemed USDC.e.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SettlementPusdMintEvidence {
    pub token: EvmAddress,
    pub from: EvmAddress,
    pub to: EvmAddress,
    pub raw_amount: EvmUint256,
    pub amount_usd: Usd,
    pub log_index: u64,
}

/// Exact `Wrapped` log emitted by pUSD for the adapter redemption payout.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SettlementWrappedPayoutEvidence {
    pub collateral_token: EvmAddress,
    pub caller: EvmAddress,
    pub asset: EvmAddress,
    pub to: EvmAddress,
    pub raw_amount: EvmUint256,
    pub amount_usd: Usd,
    pub log_index: u64,
}

/// Exact outer and inner call identity proven from the mined transaction input.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SettlementMinedCallEvidence {
    pub wallet_kind: ExecutionWalletKind,
    pub outer_sender: EvmAddress,
    pub outer_target: EvmAddress,
    pub outer_calldata_hash: EvmCalldataHash,
    pub inner_target: EvmAddress,
    pub inner_calldata_hash: EvmCalldataHash,
}

/// Receipt and business-log proof retained after chain observation.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, FromJsonQueryResult)]
#[serde(deny_unknown_fields)]
pub struct SettlementReceiptEvidence {
    pub chain_id: u64,
    pub transaction_hash: EvmTransactionHash,
    pub block_number: u64,
    pub block_hash: EvmBlockHash,
    pub finalized_block_number: u64,
    pub finalized_block_hash: EvmBlockHash,
    pub call: SettlementMinedCallEvidence,
    pub receipt_success: bool,
    pub pusd_mint: SettlementPusdMintEvidence,
    pub wrapped_payout: SettlementWrappedPayoutEvidence,
    pub canonical_checked_at: DateTime<Utc>,
    pub observed_at: DateTime<Utc>,
}

/// Finalized proof for an ERC-1155 operator approval or revocation.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SettlementOperatorApprovalReceiptEvidence {
    pub chain_id: u64,
    pub transaction_hash: EvmTransactionHash,
    pub block_number: u64,
    pub block_hash: EvmBlockHash,
    pub finalized_block_number: u64,
    pub finalized_block_hash: EvmBlockHash,
    pub call: SettlementMinedCallEvidence,
    pub receipt_success: bool,
    pub desired_approval: bool,
    pub operator_approved: bool,
    pub canonical_checked_at: DateTime<Utc>,
    pub observed_at: DateTime<Utc>,
}

/// Purpose-specific chain proof for one confirmed submission.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, FromJsonQueryResult)]
#[serde(tag = "kind", content = "evidence", rename_all = "snake_case")]
pub enum SettlementChainReceiptEvidence {
    Redeem(Box<SettlementReceiptEvidence>),
    OperatorApproval(Box<SettlementOperatorApprovalReceiptEvidence>),
}
