//! CLOB authentication: sign + optional live order probe.

use quant_pivot_api::{clob::ClobClient, keystore::Keystore, wallet::WalletTopology};
use quant_pivot_models::{
    config::{KeysConfig, PolymarketConfig},
    domain::order::OrderRequest,
    enums::{
        common::{OrderType, Side},
        execution::VenueOrderStatus,
    },
    types::{MarketId, Price, TokenId, Usd, VenueOrderAmount},
};
use rust_decimal_macros::dec;
use std::env::var;

fn test_keystore() -> Option<Keystore> {
    let key = var("QUANT_PIVOT_TEST_PRIVATE_KEY").ok()?;
    Keystore::from_config(&KeysConfig {
        private_key: Some(key),
    })
    .ok()
}

#[tokio::test]
#[ignore = "requires credentials; posts FOK far from market (expect miss/reject, not auth error)"]
async fn fok_order_sign_and_submit() {
    let ks = test_keystore().expect("QUANT_PIVOT_TEST_PRIVATE_KEY");
    let token_id =
        var("QUANT_PIVOT_TEST_TOKEN_ID").expect("QUANT_PIVOT_TEST_TOKEN_ID decimal token id");
    let market_id = var("QUANT_PIVOT_TEST_MARKET_ID").unwrap_or_else(|_| "0x0".into());

    let topology = WalletTopology::eoa(ks.address());
    let client = ClobClient::connect(ks.signer_arc(), &PolymarketConfig::default(), &topology)
        .await
        .expect("connect");

    let req = OrderRequest {
        market_id: MarketId::new(market_id),
        token_id: TokenId::new(token_id),
        side: Side::Buy,
        amount: VenueOrderAmount::GrossUsd(Usd::new(dec!(5))),
        price: Price::new(dec!(0.01)),
        order_type: OrderType::Fok,
        post_only: false,
    };

    let resp = client.place_order(&req).await;
    match resp {
        Ok(r) => {
            assert!(
                matches!(
                    r.status,
                    VenueOrderStatus::Filled
                        | VenueOrderStatus::Rejected
                        | VenueOrderStatus::Cancelled
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
