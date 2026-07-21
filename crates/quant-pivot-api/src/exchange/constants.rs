//! Polymarket exchange contract addresses and bootstrap blocks.

use alloy::primitives::{Address, B256, address};

use super::{
    order_filled_v1::ORDER_FILLED_TOPIC,
    order_filled_v2::ORDER_FILLED_TOPIC as ORDER_FILLED_V2_TOPIC,
};

/// Exchange contract generation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ExchangeVersion {
    V1,
    V2,
}

impl ExchangeVersion {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::V1 => "v1",
            Self::V2 => "v2",
        }
    }
}

/// One exchange contract tracked by the on-chain trade-tape worker.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ExchangeContract {
    pub key: &'static str,
    pub version: ExchangeVersion,
    pub address: Address,
    pub topic: B256,
    pub bootstrap_block: u64,
}

/// CTF Exchange V1 (`0x4bFb…982E`).
pub const CTF_EXCHANGE_V1: ExchangeContract = ExchangeContract {
    key: "ctf_v1",
    version: ExchangeVersion::V1,
    address: address!("0x4bFb41d5B3570DeFd03C39a9A4D8dE6Bd8B8982E"),
    topic: ORDER_FILLED_TOPIC,
    bootstrap_block: 57_000_000,
};

/// `NegRisk` CTF Exchange V1 (`0xC5d5…f80a`).
pub const NEG_RISK_EXCHANGE_V1: ExchangeContract = ExchangeContract {
    key: "neg_risk_v1",
    version: ExchangeVersion::V1,
    address: address!("0xC5d563A36AE78145C45a50134d48A1215220f80a"),
    topic: ORDER_FILLED_TOPIC,
    bootstrap_block: 57_000_000,
};

/// CTF Exchange V2 (`0xE111…996B`).
pub const CTF_EXCHANGE_V2: ExchangeContract = ExchangeContract {
    key: "ctf_v2",
    version: ExchangeVersion::V2,
    address: address!("0xE111180000d2663C0091e4f400237545B87B996B"),
    topic: ORDER_FILLED_V2_TOPIC,
    bootstrap_block: 84_902_353,
};

/// `NegRisk` CTF Exchange V2 (`0xe222…0F59`).
pub const NEG_RISK_EXCHANGE_V2: ExchangeContract = ExchangeContract {
    key: "neg_risk_v2",
    version: ExchangeVersion::V2,
    address: address!("0xe2222d279d744050d28e00520010520000310F59"),
    topic: ORDER_FILLED_V2_TOPIC,
    bootstrap_block: 84_902_353,
};

/// All exchange contracts scanned by the on-chain ingest worker.
pub const EXCHANGE_CONTRACTS: [ExchangeContract; 4] = [
    CTF_EXCHANGE_V1,
    NEG_RISK_EXCHANGE_V1,
    CTF_EXCHANGE_V2,
    NEG_RISK_EXCHANGE_V2,
];
