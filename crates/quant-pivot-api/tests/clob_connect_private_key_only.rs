//! Live CLOB connect probe: `private_key` only (no configured L2 trio).
//!
//! Run once to verify SDK `authenticate()` derives credentials at connect time:
//! `QUANT_PIVOT_TEST_PRIVATE_KEY_FILE=/run/credentials/polymarket-private-key cargo test -p quant-pivot-api --test clob_connect_private_key_only -- --ignored --nocapture`

use quant_pivot_api::{clob::ClobClient, keystore::Keystore, wallet::WalletTopology};
use quant_pivot_models::config::{KeysConfig, PolymarketConfig};

#[path = "credential_file.rs"]
mod credential_file;

#[tokio::test]
#[ignore = "requires permission-checked private-key credential file and outbound network"]
async fn clob_connect_succeeds_with_private_key_only() {
    let private_key = credential_file::required_private_key()
        .expect("permission-checked private-key credential file");
    let ks = Keystore::from_config(&KeysConfig {
        private_key: Some(private_key.into()),
    })
    .expect("keystore from private_key");

    let topology = WalletTopology::eoa(ks.address());
    let client = ClobClient::connect(ks.signer_arc(), &PolymarketConfig::default(), &topology)
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
