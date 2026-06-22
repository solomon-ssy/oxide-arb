//! Live CLOB connect probe: `private_key` only (no configured L2 trio).
//!
//! Run once to verify SDK `authenticate()` derives credentials at connect time:
//! `OXIDE_ARB_TEST_PRIVATE_KEY=0x... cargo test -p oxide-arb-api --test clob_connect_private_key_only -- --ignored --nocapture`

use oxide_arb_api::{clob::ClobClient, keystore::Keystore};
use oxide_arb_models::config::{KeySource, KeysConfig, PolymarketConfig};
use std::env::var;

#[tokio::test]
#[ignore = "requires OXIDE_ARB_TEST_PRIVATE_KEY and outbound network"]
async fn clob_connect_succeeds_with_private_key_only() {
    let private_key = var("OXIDE_ARB_TEST_PRIVATE_KEY").expect("OXIDE_ARB_TEST_PRIVATE_KEY");
    let ks = Keystore::from_config(&KeysConfig {
        source: KeySource::Env,
        private_key: Some(private_key),
        keystore_path: None,
    })
    .expect("keystore from private_key");

    let client = ClobClient::connect(ks.signer_arc(), &PolymarketConfig::default())
        .await
        .expect("ClobClient::connect must succeed without configured L2 trio");

    let balance = client.collateral_balance().await;
    match balance {
        Ok(usd) => println!("collateral_balance: {usd}"),
        Err(e) => {
            let msg = e.to_string();
            assert!(
                !msg.contains("invalid signature") && !msg.contains("Unauthorized"),
                "auth failure after connect: {msg}"
            );
            println!("collateral_balance: {msg} (non-auth error acceptable)");
        }
    }
}
