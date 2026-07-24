//! Deterministic CLOB SDK contract-test support.

use std::{str::FromStr as _, sync::Arc, time::Duration};

use alloy::{
    primitives::Address,
    signers::{Signer as _, local::LocalSigner},
};
use polymarket_client_sdk_v2::{
    POLYGON,
    auth::{Normal, state::Authenticated},
    clob::{Client as SdkClient, Config as SdkConfig, types::SignatureType},
};
use quant_pivot_models::{
    domain::order::OrderRequest,
    enums::common::{OrderType, Side},
    types::{MarketId, Price, Shares, TokenId, Usd, VenueOrderAmount},
};
use reqwest::Client;
use rust_decimal_macros::dec;
use wiremock::{
    Mock, MockServer, ResponseTemplate,
    matchers::{method, path, query_param},
};

use super::super::RateLimiter;
use crate::{clob::ClobClient, keystore::OrderSigner, ws::BookLevelRejectHook};

/// Publicly known Anvil/Hardhat test key #0.
const PRIVATE_KEY: &str = "0xac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80";
const API_KEY: &str = "00000000-0000-0000-0000-000000000000";
const PASSPHRASE: &str = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
const SECRET: &str = "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=";

pub fn test_token_id() -> TokenId {
    TokenId::new("15871154585880608648532107628464183779895785213830018178010423617714102767076")
}

pub async fn mount_derive_api_key(server: &MockServer) {
    Mock::given(method("GET"))
        .and(path("/auth/derive-api-key"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "apiKey": API_KEY,
            "passphrase": PASSPHRASE,
            "secret": SECRET
        })))
        .mount(server)
        .await;

    Mock::given(method("GET"))
        .and(path("/time"))
        .respond_with(ResponseTemplate::new(200).set_body_string("1000000"))
        .mount(server)
        .await;
}

pub async fn mount_clob_requirements(server: &MockServer, token_id: &TokenId) {
    Mock::given(method("GET"))
        .and(path("/version"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({ "version": 2 })))
        .mount(server)
        .await;

    Mock::given(method("GET"))
        .and(path("/neg-risk"))
        .respond_with(
            ResponseTemplate::new(200).set_body_json(serde_json::json!({ "neg_risk": false })),
        )
        .mount(server)
        .await;

    Mock::given(method("GET"))
        .and(path("/fee-rate"))
        .respond_with(
            ResponseTemplate::new(200).set_body_json(serde_json::json!({ "base_fee": 0 })),
        )
        .mount(server)
        .await;

    Mock::given(method("GET"))
        .and(path("/tick-size"))
        .and(query_param("token_id", token_id.as_str()))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_json(serde_json::json!({ "minimum_tick_size": "0.01" })),
        )
        .mount(server)
        .await;
}

pub async fn mount_post_order(server: &MockServer, body: &str, expected_calls: u64) {
    let mut mock = Mock::given(method("POST"))
        .and(path("/order"))
        .respond_with(ResponseTemplate::new(200).set_body_string(body));
    if expected_calls > 0 {
        mock = mock.expect(expected_calls);
    }
    mock.mount(server).await;
}

pub fn test_signer() -> Arc<OrderSigner> {
    let bytes = hex::decode(PRIVATE_KEY.trim_start_matches("0x")).expect("test key hex");
    Arc::new(
        OrderSigner::from_bytes(&bytes)
            .expect("signer")
            .with_chain_id(Some(POLYGON)),
    )
}

pub async fn test_sdk_client(server: &MockServer) -> Arc<SdkClient<Authenticated<Normal>>> {
    let signer = LocalSigner::from_str(PRIVATE_KEY)
        .expect("local signer")
        .with_chain_id(Some(POLYGON));

    let config = SdkConfig::builder().build();
    let mut client = SdkClient::new(&server.uri(), config)
        .expect("sdk client")
        .authentication_builder(&signer)
        .authenticate()
        .await
        .expect("authenticate");
    client
        .stop_heartbeats()
        .await
        .expect("stop test heartbeat task");

    Arc::new(client)
}

pub async fn test_clob_client(server: &MockServer) -> ClobClient {
    clob_client_order_timeout(server, Duration::from_secs(15)).await
}

pub async fn deposit_wallet_clob_client(
    server: &MockServer,
    deposit_wallet: Address,
) -> ClobClient {
    let signer = LocalSigner::from_str(PRIVATE_KEY)
        .expect("local signer")
        .with_chain_id(Some(POLYGON));
    let config = SdkConfig::builder().build();
    let mut sdk = SdkClient::new(&server.uri(), config)
        .expect("sdk client")
        .authentication_builder(&signer)
        .signature_type(SignatureType::Poly1271)
        .funder(deposit_wallet)
        .authenticate()
        .await
        .expect("authenticate Deposit Wallet client");
    sdk.stop_heartbeats()
        .await
        .expect("stop test heartbeat task");
    ClobClient {
        sdk: Arc::new(sdk),
        http: Client::new(),
        clob_base_url: server.uri(),
        signer: test_signer(),
        order_post_timeout: Duration::from_secs(15),
        rate_limiter: RateLimiter::new(),
        on_book_level_rejected: None::<BookLevelRejectHook>,
    }
}

pub async fn clob_client_order_timeout(
    server: &MockServer,
    order_post_timeout: Duration,
) -> ClobClient {
    let sdk = test_sdk_client(server).await;
    ClobClient {
        sdk,
        http: Client::new(),
        clob_base_url: server.uri(),
        signer: test_signer(),
        order_post_timeout,
        rate_limiter: RateLimiter::new(),
        on_book_level_rejected: None::<BookLevelRejectHook>,
    }
}

pub fn test_order_request(order_type: OrderType) -> OrderRequest {
    OrderRequest {
        market_id: MarketId::new("0xtest"),
        token_id: test_token_id(),
        side: Side::Buy,
        amount: match order_type {
            OrderType::Fok | OrderType::Fak => VenueOrderAmount::GrossUsd(Usd::new(dec!(100))),
            OrderType::Gtc | OrderType::Gtd { expiration: _ } => {
                VenueOrderAmount::Shares(Shares::new(dec!(100)))
            }
        },
        price: Price::new(dec!(0.92)),
        order_type,
        post_only: matches!(order_type, OrderType::Gtc | OrderType::Gtd { .. }),
    }
}
