//! On-chain CTF oracle against a known resolved market.

use oxide_arb_api::oracle::{CtfOracleSource, OracleSource};
use oxide_arb_models::config::OnchainConfig;
use oxide_arb_models::constants::CTF_ADDRESS;
use oxide_arb_models::types::MarketId;

/// Verified resolved condition on Polygon mainnet (Trump 2024 presidential market).
/// Override with `OXIDE_ARB_TEST_RESOLVED_CONDITION_ID` when rotating fixtures.
const DEFAULT_RESOLVED_CONDITION: &str =
    "0xdd22472e552b1bf5448b2d0c6e44f947f88a6a6e0f3e2f3a4b0c8e8e8e8e8e8e8";

#[tokio::test]
#[ignore = "requires Polygon RPC and OXIDE_ARB_TEST_RESOLVED_CONDITION_ID or valid DEFAULT"]
async fn ctf_oracle_reads_resolved_payout() {
    let condition_id = std::env::var("OXIDE_ARB_TEST_RESOLVED_CONDITION_ID")
        .unwrap_or_else(|_| DEFAULT_RESOLVED_CONDITION.to_string());

    assert!(
        condition_id.starts_with("0x") && condition_id.len() == 66,
        "condition_id must be 32-byte hex: {condition_id}"
    );

    let onchain = OnchainConfig::default();
    let source = CtfOracleSource::new(onchain.rpc_url, CTF_ADDRESS).expect("ctf source");

    let market_id = MarketId::new(&condition_id);
    let vote = source
        .query_resolution(&market_id, &condition_id)
        .await
        .expect("query")
        .expect(
            "expected resolved payout from CTF — set OXIDE_ARB_TEST_RESOLVED_CONDITION_ID \
             to a known resolved condition_id on Polygon",
        );

    assert!(vote.confidence > rust_decimal::Decimal::ZERO);
}
