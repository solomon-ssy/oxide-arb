//! CLOB authentication: sign + optional live order probe.

use oxide_arb_api::clob::ClobClient;
use oxide_arb_api::keystore::Keystore;
use oxide_arb_models::config::{KeySource, KeysConfig, PolymarketConfig};
use oxide_arb_models::domain::order::{OrderRequest, OrderStatus};
use oxide_arb_models::enums::common::{OrderType, Side};
use oxide_arb_models::types::{MarketId, Price, Shares, TokenId, Usd};
use rust_decimal_macros::dec;
use std::sync::Arc;

fn test_keystore() -> Option<Keystore> {
    let key = std::env::var("OXIDE_ARB_TEST_PRIVATE_KEY").ok()?;
    Keystore::from_config(&KeysConfig {
        source: KeySource::Env,
        private_key: Some(key),
        polymarket_api_key: std::env::var("OXIDE_ARB_TEST_API_KEY").ok(),
        polymarket_api_secret: std::env::var("OXIDE_ARB_TEST_API_SECRET").ok(),
        polymarket_passphrase: std::env::var("OXIDE_ARB_TEST_PASSPHRASE").ok(),
        keystore_path: None,
    })
    .ok()
}

#[tokio::test]
#[ignore = "requires OXIDE_ARB_TEST_PRIVATE_KEY and network"]
async fn derive_l2_credentials_from_signer() {
    let ks = test_keystore().expect("OXIDE_ARB_TEST_PRIVATE_KEY");
    let creds = ks
        .derive_l2_credentials(&PolymarketConfig::default())
        .await
        .expect("derive L2");
    assert!(!creds.api_key.is_empty());
    assert!(!creds.api_secret.is_empty());
}

#[tokio::test]
#[ignore = "requires credentials; posts FOK far from market (expect miss/reject, not auth error)"]
async fn fok_order_sign_and_submit() {
    let ks = test_keystore().expect("credentials");
    let token_id =
        std::env::var("OXIDE_ARB_TEST_TOKEN_ID").expect("OXIDE_ARB_TEST_TOKEN_ID decimal token id");
    let market_id = std::env::var("OXIDE_ARB_TEST_MARKET_ID").unwrap_or_else(|_| "0x0".into());

    let client = ClobClient::connect(Arc::new(ks.signer().clone()), &PolymarketConfig::default())
        .await
        .expect("connect");

    let req = OrderRequest {
        market_id: MarketId::new(market_id),
        token_id: TokenId::new(token_id),
        side: Side::Buy,
        shares: Shares::new(dec!(5)),
        price: Price::new(dec!(0.01)),
        order_type: OrderType::Fok,
        neg_risk: false,
    };

    let resp = client.place_order(&req).await;
    match resp {
        Ok(r) => {
            assert!(
                matches!(
                    r.status,
                    OrderStatus::Filled | OrderStatus::Rejected | OrderStatus::Cancelled
                ),
                "unexpected status: {:?}",
                r.status
            );
        }
        Err(e) => {
            let msg = e.to_string();
            assert!(
                !msg.contains("invalid signature") && !msg.contains("Unauthorized"),
                "auth failure: {msg}"
            );
        }
    }
}
