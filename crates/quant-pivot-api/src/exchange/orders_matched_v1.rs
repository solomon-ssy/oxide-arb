//! V1 `OrdersMatched` event ABI and decoder.

use alloy::{
    primitives::{Address, B256, U256},
    rpc::types::Log,
    sol,
    sol_types::SolEvent,
};

sol! {
    #[derive(Debug)]
    event OrdersMatched(
        bytes32 indexed takerOrderHash,
        address indexed takerOrderMaker,
        uint256 makerAssetId,
        uint256 takerAssetId,
        uint256 makerAmountFilled,
        uint256 takerAmountFilled
    );
}

pub const ORDERS_MATCHED_TOPIC: B256 = OrdersMatched::SIGNATURE_HASH;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DecodedOrdersMatchedV1 {
    pub taker_order_hash: B256,
    pub taker_order_maker: Address,
    pub maker_asset_id: U256,
    pub taker_asset_id: U256,
    pub maker_amount_filled: U256,
    pub taker_amount_filled: U256,
}

#[must_use]
pub fn decode_log(log: &Log) -> Option<DecodedOrdersMatchedV1> {
    let decoded = OrdersMatched::decode_log(log.as_ref()).ok()?;
    Some(DecodedOrdersMatchedV1 {
        taker_order_hash: decoded.takerOrderHash,
        taker_order_maker: decoded.takerOrderMaker,
        maker_asset_id: decoded.makerAssetId,
        taker_asset_id: decoded.takerAssetId,
        maker_amount_filled: decoded.makerAmountFilled,
        taker_amount_filled: decoded.takerAmountFilled,
    })
}
