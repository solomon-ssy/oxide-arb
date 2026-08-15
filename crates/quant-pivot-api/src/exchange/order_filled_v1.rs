//! V1 `OrderFilled` event ABI and decoder.

use alloy::{
    primitives::{Address, B256, U256},
    rpc::types::Log,
    sol,
    sol_types::SolEvent,
};

sol! {
    #[derive(Debug)]
    event OrderFilled(
        bytes32 indexed orderHash,
        address indexed maker,
        address indexed taker,
        uint256 makerAssetId,
        uint256 takerAssetId,
        uint256 makerAmountFilled,
        uint256 takerAmountFilled,
        uint256 fee
    );
}

/// Topic hash for V1 `OrderFilled` logs.
pub const ORDER_FILLED_TOPIC: B256 = OrderFilled::SIGNATURE_HASH;

/// Decoded V1 fill leg from an RPC log.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DecodedOrderFilledV1 {
    pub order_hash: B256,
    pub maker: Address,
    pub taker: Address,
    pub maker_asset_id: U256,
    pub taker_asset_id: U256,
    pub maker_amount_filled: U256,
    pub taker_amount_filled: U256,
    pub fee: U256,
}

/// Decode a V1 `OrderFilled` log, returning `None` when topic/data do not match.
#[must_use]
pub fn decode_log(log: &Log) -> Option<DecodedOrderFilledV1> {
    let decoded = OrderFilled::decode_log(log.as_ref()).ok()?;
    Some(DecodedOrderFilledV1 {
        order_hash: decoded.orderHash,
        maker: decoded.maker,
        taker: decoded.taker,
        maker_asset_id: decoded.makerAssetId,
        taker_asset_id: decoded.takerAssetId,
        maker_amount_filled: decoded.makerAmountFilled,
        taker_amount_filled: decoded.takerAmountFilled,
        fee: decoded.fee,
    })
}
