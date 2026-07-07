//! Addresses that must never appear as trade-tape participants.

use alloy::primitives::{Address, address};
use std::sync::LazyLock;

use super::constants::{
    CTF_EXCHANGE_V1, CTF_EXCHANGE_V2, NEG_RISK_EXCHANGE_V1, NEG_RISK_EXCHANGE_V2,
};

/// Polymarket CTF contract on Polygon.
pub const CTF_CONTRACT: Address = address!("0x4D97DCd97eC945f40cF65F87097ACe5EA0476045");

static DENYLIST: LazyLock<Vec<Address>> = LazyLock::new(|| {
    vec![
        Address::ZERO,
        CTF_CONTRACT,
        CTF_EXCHANGE_V1.address,
        NEG_RISK_EXCHANGE_V1.address,
        CTF_EXCHANGE_V2.address,
        NEG_RISK_EXCHANGE_V2.address,
    ]
});

/// Whether `address` may be recorded as a human trade-tape participant.
#[must_use]
pub fn is_human_participant(address: Address) -> bool {
    !DENYLIST.contains(&address)
}
