//! Polymarket exchange contract addresses and bootstrap blocks.

use alloy::primitives::{Address, B256, address};

use super::{
    fee_charged_v2::FEE_CHARGED_TOPIC, order_filled_v2::ORDER_FILLED_TOPIC,
    orders_matched_v2::ORDERS_MATCHED_TOPIC,
};

/// One exchange contract tracked by finalized exchange-history reconstruction.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ExchangeContract {
    pub key: &'static str,
    pub address: Address,
    pub order_filled_topic: B256,
    pub orders_matched_topic: B256,
    pub fee_charged_topic: B256,
    pub first_valid_block: u64,
    pub first_valid_block_hash: &'static str,
    pub last_valid_block: Option<u64>,
    pub last_valid_block_hash: Option<&'static str>,
    pub boundary_evidence: &'static str,
    pub bytecode_blake3: &'static str,
}

const V2_PRODUCTION_BLOCK: u64 = 86_129_648;
const V2_PRODUCTION_BLOCK_HASH: &str =
    "0xc4020af2b4a94afc8462b27278f5604822599634fe1df84f4fdd0f1dc2972cc3";
const V2_MIGRATION_EVIDENCE: &str = "https://docs.polymarket.com/v2-migration";

/// CTF Exchange V2 (`0xE111…996B`).
pub const CTF_EXCHANGE_V2: ExchangeContract = ExchangeContract {
    key: "ctf_v2",
    address: address!("0xE111180000d2663C0091e4f400237545B87B996B"),
    order_filled_topic: ORDER_FILLED_TOPIC,
    orders_matched_topic: ORDERS_MATCHED_TOPIC,
    fee_charged_topic: FEE_CHARGED_TOPIC,
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
    address: address!("0xe2222d279d744050d28e00520010520000310F59"),
    order_filled_topic: ORDER_FILLED_TOPIC,
    orders_matched_topic: ORDERS_MATCHED_TOPIC,
    fee_charged_topic: FEE_CHARGED_TOPIC,
    first_valid_block: V2_PRODUCTION_BLOCK,
    first_valid_block_hash: V2_PRODUCTION_BLOCK_HASH,
    last_valid_block: None,
    last_valid_block_hash: None,
    boundary_evidence: V2_MIGRATION_EVIDENCE,
    bytecode_blake3: "ea2cd04f602fa8289b3aaa225778e286f20b468e501e2f3cd73c2755734de282",
};

/// All exchange contracts scanned by finalized exchange-history reconstruction.
pub const EXCHANGE_CONTRACTS: [ExchangeContract; 2] = [CTF_EXCHANGE_V2, NEG_RISK_EXCHANGE_V2];

#[cfg(test)]
mod tests {
    use super::{CTF_EXCHANGE_V2, NEG_RISK_EXCHANGE_V2};

    #[test]
    fn v2_contracts_open_ended() {
        for v2 in [CTF_EXCHANGE_V2, NEG_RISK_EXCHANGE_V2] {
            assert_eq!(v2.last_valid_block, None);
            assert_eq!(v2.last_valid_block_hash, None);
            assert!(v2.first_valid_block_hash.starts_with("0x"));
            assert_eq!(v2.first_valid_block_hash.len(), 66);
        }
    }
}
