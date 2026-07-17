//! Strong-typed JSONB payloads for settlement redemption evidence.

use sea_orm::FromJsonQueryResult;
use serde::{Deserialize, Serialize};

/// CTF payout vector observed before a redemption transaction is sent.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, FromJsonQueryResult)]
pub struct SettlementPayoutVector {
    /// CTF denominator. Zero means the condition is not redeemable.
    pub denominator: String,
    /// YES payout numerator.
    pub yes: String,
    /// NO payout numerator.
    pub no: String,
}

/// Token-balance evidence for a single ERC-1155 outcome token.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SettlementTokenBalance {
    /// Outcome token id.
    pub token_id: String,
    /// CTF binary index set (`1` for YES, `2` for NO).
    pub index_set: u8,
    /// Raw on-chain ERC-1155 balance.
    pub raw_balance: String,
    /// Decimal shares after collateral-scale normalization.
    pub shares: String,
}

/// Balance evidence captured around the redemption transaction.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, FromJsonQueryResult)]
pub struct SettlementBalanceEvidence {
    /// YES token balance.
    pub yes: SettlementTokenBalance,
    /// NO token balance.
    pub no: SettlementTokenBalance,
}

/// Index sets submitted to the CTF redemption call.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, FromJsonQueryResult)]
pub struct SettlementRedeemIndexSets {
    /// Ordered CTF index sets.
    pub index_sets: Vec<u8>,
}
