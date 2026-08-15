//! Polymarket exchange contract addresses and bootstrap blocks.

use alloy::primitives::{Address, B256, address};

use super::{
    order_filled_v1::ORDER_FILLED_TOPIC,
    order_filled_v2::ORDER_FILLED_TOPIC as ORDER_FILLED_V2_TOPIC,
    orders_matched_v1::ORDERS_MATCHED_TOPIC,
    orders_matched_v2::ORDERS_MATCHED_TOPIC as ORDERS_MATCHED_V2_TOPIC,
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

/// One exchange contract tracked by finalized exchange-history reconstruction.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ExchangeContract {
    pub key: &'static str,
    pub version: ExchangeVersion,
    pub address: Address,
    pub order_filled_topic: B256,
    pub orders_matched_topic: B256,
    pub first_valid_block: u64,
    pub first_valid_block_hash: &'static str,
    pub last_valid_block: Option<u64>,
    pub last_valid_block_hash: Option<&'static str>,
    pub boundary_evidence: &'static str,
    pub bytecode_blake3: &'static str,
}

const V1_LAST_VALID_BLOCK: u64 = 86_127_097;
const V1_LAST_VALID_BLOCK_HASH: &str =
    "0x9f7ba2ebd4b4a1e8654910daafcf7c85f56de1985a4c3fb1d14f4ccf1ab5b49c";
const V2_PRODUCTION_BLOCK: u64 = 86_129_648;
const V2_PRODUCTION_BLOCK_HASH: &str =
    "0xc4020af2b4a94afc8462b27278f5604822599634fe1df84f4fdd0f1dc2972cc3";
const V2_MIGRATION_EVIDENCE: &str = "https://docs.polymarket.com/v2-migration";

/// CTF Exchange V1 (`0x4bFb…982E`).
pub const CTF_EXCHANGE_V1: ExchangeContract = ExchangeContract {
    key: "ctf_v1",
    version: ExchangeVersion::V1,
    address: address!("0x4bFb41d5B3570DeFd03C39a9A4D8dE6Bd8B8982E"),
    order_filled_topic: ORDER_FILLED_TOPIC,
    orders_matched_topic: ORDERS_MATCHED_TOPIC,
    first_valid_block: 33_605_403,
    first_valid_block_hash: "0x98c891768cc94e5893b0ff0e200de741019fad7a859bef36da69b919dbbd2e06",
    last_valid_block: Some(V1_LAST_VALID_BLOCK),
    last_valid_block_hash: Some(V1_LAST_VALID_BLOCK_HASH),
    boundary_evidence: V2_MIGRATION_EVIDENCE,
    bytecode_blake3: "3b26550db97de5f02126313415dfe8a1c9e826684abef30b09367a337bb90627",
};

/// `NegRisk` CTF Exchange V1 (`0xC5d5…f80a`).
pub const NEG_RISK_EXCHANGE_V1: ExchangeContract = ExchangeContract {
    key: "neg_risk_v1",
    version: ExchangeVersion::V1,
    address: address!("0xC5d563A36AE78145C45a50134d48A1215220f80a"),
    order_filled_topic: ORDER_FILLED_TOPIC,
    orders_matched_topic: ORDERS_MATCHED_TOPIC,
    first_valid_block: 50_505_492,
    first_valid_block_hash: "0x21753e94027482e39bf0972ecdbc495e59ee3bcd21da6d07dd17a7cbb01d4d3d",
    last_valid_block: Some(V1_LAST_VALID_BLOCK),
    last_valid_block_hash: Some(V1_LAST_VALID_BLOCK_HASH),
    boundary_evidence: V2_MIGRATION_EVIDENCE,
    bytecode_blake3: "175d3a70971f16f8ce67750b0cbd72a55c56ac2fe446e940d04c6b5333f3d36d",
};

/// CTF Exchange V2 (`0xE111…996B`).
pub const CTF_EXCHANGE_V2: ExchangeContract = ExchangeContract {
    key: "ctf_v2",
    version: ExchangeVersion::V2,
    address: address!("0xE111180000d2663C0091e4f400237545B87B996B"),
    order_filled_topic: ORDER_FILLED_V2_TOPIC,
    orders_matched_topic: ORDERS_MATCHED_V2_TOPIC,
    first_valid_block: V2_PRODUCTION_BLOCK,
    first_valid_block_hash: V2_PRODUCTION_BLOCK_HASH,
    last_valid_block: None,
    last_valid_block_hash: None,
    boundary_evidence: V2_MIGRATION_EVIDENCE,
    bytecode_blake3: "27a1986cfd1796b79bf2ae14758a90a4a53e43a0aac40929719bee2095f84799",
};

/// `NegRisk` CTF Exchange V2 (`0xe222…0F59`).
pub const NEG_RISK_EXCHANGE_V2: ExchangeContract = ExchangeContract {
    key: "neg_risk_v2",
    version: ExchangeVersion::V2,
    address: address!("0xe2222d279d744050d28e00520010520000310F59"),
    order_filled_topic: ORDER_FILLED_V2_TOPIC,
    orders_matched_topic: ORDERS_MATCHED_V2_TOPIC,
    first_valid_block: V2_PRODUCTION_BLOCK,
    first_valid_block_hash: V2_PRODUCTION_BLOCK_HASH,
    last_valid_block: None,
    last_valid_block_hash: None,
    boundary_evidence: V2_MIGRATION_EVIDENCE,
    bytecode_blake3: "ea2cd04f602fa8289b3aaa225778e286f20b468e501e2f3cd73c2755734de282",
};

/// All exchange contracts scanned by finalized exchange-history reconstruction.
pub const EXCHANGE_CONTRACTS: [ExchangeContract; 4] = [
    CTF_EXCHANGE_V1,
    NEG_RISK_EXCHANGE_V1,
    CTF_EXCHANGE_V2,
    NEG_RISK_EXCHANGE_V2,
];

#[cfg(test)]
mod tests {
    use super::{CTF_EXCHANGE_V1, CTF_EXCHANGE_V2, NEG_RISK_EXCHANGE_V1, NEG_RISK_EXCHANGE_V2};

    #[test]
    fn generations_have_sealed_gap() {
        for (v1, v2) in [
            (CTF_EXCHANGE_V1, CTF_EXCHANGE_V2),
            (NEG_RISK_EXCHANGE_V1, NEG_RISK_EXCHANGE_V2),
        ] {
            let last_v1 = v1.last_valid_block.expect("V1 must have an inclusive end");
            assert!(last_v1 < v2.first_valid_block);
            assert!(v1.last_valid_block_hash.is_some());
            assert_eq!(v2.last_valid_block, None);
            assert_eq!(v2.last_valid_block_hash, None);
            assert!(v1.first_valid_block_hash.starts_with("0x"));
            assert!(v2.first_valid_block_hash.starts_with("0x"));
            assert_eq!(v1.first_valid_block_hash.len(), 66);
            assert_eq!(v2.first_valid_block_hash.len(), 66);
        }
    }
}
