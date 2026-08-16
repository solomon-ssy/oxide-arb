//! V2 `FeeCharged` event ABI and decoder.

use alloy::{
    primitives::{Address, B256, U256},
    rpc::types::Log,
    sol,
    sol_types::SolEvent,
};

sol! {
    #[derive(Debug)]
    event FeeCharged(address indexed receiver, uint256 amount);
}

/// Topic hash for V2 `FeeCharged` logs.
pub const FEE_CHARGED_TOPIC: B256 = FeeCharged::SIGNATURE_HASH;

/// Decoded V2 fee transfer from an RPC log.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DecodedFeeChargedV2 {
    pub receiver: Address,
    pub amount: U256,
}

/// Decode a V2 `FeeCharged` log, returning `None` when topic/data do not match.
#[must_use]
pub fn decode_log(log: &Log) -> Option<DecodedFeeChargedV2> {
    let decoded = FeeCharged::decode_log(log.as_ref()).ok()?;
    Some(DecodedFeeChargedV2 {
        receiver: decoded.receiver,
        amount: decoded.amount,
    })
}
