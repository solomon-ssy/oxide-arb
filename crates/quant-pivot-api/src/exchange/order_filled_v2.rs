//! V2 `OrderFilled` event ABI and decoder.

use alloy::{
    primitives::{Address, B256, U256},
    rpc::types::Log,
    sol,
    sol_types::SolEvent,
};

sol! {
    #[derive(Debug)]
    event OrderFilledV2(
        bytes32 indexed orderHash,
        address indexed maker,
        address indexed taker,
        uint8 side,
        uint256 tokenId,
        uint256 makerAmountFilled,
        uint256 takerAmountFilled,
        uint256 fee,
        bytes32 builder,
        bytes32 metadata
    );
}

/// Topic hash for V2 `OrderFilled` logs.
pub const ORDER_FILLED_TOPIC: B256 = OrderFilledV2::SIGNATURE_HASH;

/// Decoded V2 fill leg from an RPC log.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DecodedOrderFilledV2 {
    pub order_hash: B256,
    pub maker: Address,
    pub taker: Address,
    pub side: u8,
    pub token_id: U256,
    pub maker_amount_filled: U256,
    pub taker_amount_filled: U256,
    pub fee: U256,
    pub builder: B256,
    pub metadata: B256,
}

/// Decode a V2 `OrderFilled` log, returning `None` when topic/data do not match.
#[must_use]
pub fn decode_log(log: &Log) -> Option<DecodedOrderFilledV2> {
    let decoded = OrderFilledV2::decode_log(log.as_ref()).ok()?;
    Some(DecodedOrderFilledV2 {
        order_hash: decoded.orderHash,
        maker: decoded.maker,
        taker: decoded.taker,
        side: decoded.side,
        token_id: decoded.tokenId,
        maker_amount_filled: decoded.makerAmountFilled,
        taker_amount_filled: decoded.takerAmountFilled,
        fee: decoded.fee,
        builder: decoded.builder,
        metadata: decoded.metadata,
    })
}
