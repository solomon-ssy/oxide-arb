//! On-chain CTF oracle against a known resolved market on Polygon mainnet.
//!
//! # Polygon RPC (Alchemy)
//!
//! Use any Polygon **mainnet** HTTPS endpoint. Alchemy is fully supported:
//!
//! ```text
//! https://polygon-mainnet.g.alchemy.com/v2/<YOUR_API_KEY>
//! ```
//!
//! Configure via (highest precedence first):
//!
//! 1. `QUANT_PIVOT__POLYMARKET__ONCHAIN__RPC_URL` — overrides TOML at runtime
//! 2. `[polymarket.onchain].rpc_url` in `config/quant-pivot.toml`
//!
//! Create the key in [Alchemy Dashboard](https://dashboard.alchemy.com/) → Apps →
//! Polygon → copy the HTTPS URL. No special contract allowlist is required for
//! `eth_call` view methods used here (`payoutNumerators`, `payoutDenominator`).
//!
//! # Resolved market fixture
//!
//! `condition_id` is the Polymarket **condition_id** (32-byte hex, `0x` + 64 hex chars),
//! not a CLOB decimal `token_id`. Find one from:
//!
//! - Gamma API: `GET /markets?closed=true` on a settled market
//! - Polygonscan logs on `CTF_ADDRESS` (`0x4D97…6045`) for `PayoutRedemption`
//! - Polymarket UI → market → developer tools / condition id in API payloads
//!
//! Set **`QUANT_PIVOT_TEST_RESOLVED_CONDITION_ID`** to that value. There is no
//! baked-in default — placeholders rot when markets delist.

use quant_pivot_api::oracle::{CtfOracleSource, OracleSource};
use quant_pivot_models::{
    config::{OnchainConfig, settlement::SettlementContractsSection},
    types::MarketId,
};
use std::env::var;

fn onchain_from_env_or_config() -> OnchainConfig {
    if let Ok(url) = var("QUANT_PIVOT__POLYMARKET__ONCHAIN__RPC_URL") {
        return OnchainConfig {
            rpc_url: url,
            ..OnchainConfig::default()
        };
    }
    if let Ok(url) = var("QUANT_PIVOT_TEST_POLYGON_RPC_URL") {
        return OnchainConfig {
            rpc_url: url,
            ..OnchainConfig::default()
        };
    }
    OnchainConfig::default()
}

fn require_resolved_condition_id() -> String {
    var("QUANT_PIVOT_TEST_RESOLVED_CONDITION_ID").unwrap_or_else(|_| {
        panic!(
            "set QUANT_PIVOT_TEST_RESOLVED_CONDITION_ID to a settled market condition_id \
             (0x + 64 hex). Also set QUANT_PIVOT__POLYMARKET__ONCHAIN__RPC_URL or \
             QUANT_PIVOT_TEST_POLYGON_RPC_URL to your Alchemy Polygon mainnet URL."
        )
    })
}

#[tokio::test]
#[ignore = "requires Polygon mainnet RPC + QUANT_PIVOT_TEST_RESOLVED_CONDITION_ID"]
async fn ctf_oracle_reads_resolved_payout() {
    let condition_id = require_resolved_condition_id();
    assert!(
        condition_id.starts_with("0x") && condition_id.len() == 66,
        "condition_id must be 32-byte hex: {condition_id}"
    );

    let onchain = onchain_from_env_or_config();
    let contracts = SettlementContractsSection::default();
    let source = CtfOracleSource::new(onchain.rpc_url, &contracts.ctf_address).expect("ctf source");

    let market_id = MarketId::new(&condition_id);
    let vote = source
        .query_resolution(&market_id, &condition_id)
        .await
        .expect("RPC query")
        .unwrap_or_else(|| {
            panic!(
                "CTF returned unresolved for {condition_id} — pick a market with \
                 payoutDenominator > 0 on-chain"
            )
        });

    assert!(vote.confidence > rust_decimal::Decimal::ZERO);
    assert_eq!(vote.source_id, "ctf_onchain");
}
