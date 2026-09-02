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
    contract_config,
    types::U256,
};
use quant_pivot_models::{
    domain::order::OrderRequest,
    enums::common::{OrderType, Side, TickSize},
    hashing::CanonicalDigest,
    types::{ContentHash, EvmAddress, MarketId, Price, Shares, TokenId, Usd, VenueOrderAmount},
};
use reqwest::Client;
use rust_decimal_macros::dec;
use serde_json::{Map, Value};
use tokio::sync::Mutex;
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
pub const TEST_CONDITION_ID: &str =
    "0x0000000000000000000000000000000000000000000000000000000000000001";
const MAX_UINT256: &str =
    "115792089237316195423570985008687907853269984665640564039457584007913129639935";

pub fn test_token_id() -> TokenId {
    TokenId::new("15871154585880608648532107628464183779895785213830018178010423617714102767076")
}

pub fn complete_market_info_payload(neg_risk: bool) -> Value {
    serde_json::json!({
        "c": TEST_CONDITION_ID,
        "t": [
            { "t": test_token_id().as_str(), "o": "Yes" },
            { "t": "2", "o": "No" }
        ],
        "mts": "0.01",
        "mos": "5",
        "nr": neg_risk,
        "itode": false,
        "ibce": false,
        "oas": 0,
        "fd": { "r": "0.25", "e": 2, "to": true },
        "mbf": 0,
        "tbf": 0,
        "rfqe": false
    })
}

pub fn test_market_info_hash(neg_risk: bool) -> ContentHash {
    CanonicalDigest::content_hash_json(&complete_market_info_payload(neg_risk))
        .expect("market info hash")
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
    mount_clob_balance_requirements(server, token_id, "10000000000").await;
}

pub async fn mount_clob_balance_requirements(
    server: &MockServer,
    token_id: &TokenId,
    balance: &str,
) {
    mount_clob_route_requirements(server, token_id, false, balance, Some(MAX_UINT256)).await;
}

pub async fn mount_clob_route_requirements(
    server: &MockServer,
    token_id: &TokenId,
    neg_risk: bool,
    balance: &str,
    allowance: Option<&str>,
) {
    let wire_token_id = U256::from_str(token_id.as_str()).expect("test token uint256");
    Mock::given(method("GET"))
        .and(path("/version"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({ "version": 2 })))
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
        .and(path("/book"))
        .and(query_param("token_id", token_id.as_str()))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "market": TEST_CONDITION_ID,
            "asset_id": wire_token_id,
            "timestamp": "1705322096000",
            "hash": "book-hash",
            "bids": [{ "price": "0.91", "size": "100" }],
            "asks": [{ "price": "0.92", "size": "100" }],
            "min_order_size": "5",
            "neg_risk": neg_risk,
            "tick_size": "0.01"
        })))
        .mount(server)
        .await;

    Mock::given(method("GET"))
        .and(path(format!("/clob-markets/{TEST_CONDITION_ID}")))
        .respond_with(
            ResponseTemplate::new(200).set_body_json(complete_market_info_payload(neg_risk)),
        )
        .mount(server)
        .await;

    let spender = contract_config(POLYGON, neg_risk)
        .and_then(|config| config.exchange_v2)
        .expect("test V2 spender");
    let mut allowances = Map::new();
    if let Some(allowance) = allowance {
        allowances.insert(spender.to_string(), serde_json::json!(allowance));
    }
    Mock::given(method("GET"))
        .and(path("/balance-allowance"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "balance": balance,
            "allowances": allowances
        })))
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

pub fn unbound_test_signer() -> Arc<OrderSigner> {
    let bytes = hex::decode(PRIVATE_KEY.trim_start_matches("0x")).expect("test key hex");
    Arc::new(OrderSigner::from_bytes(&bytes).expect("unbound signer"))
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
    funder_clob_client(server, SignatureType::Poly1271, deposit_wallet).await
}

pub async fn funder_clob_client(
    server: &MockServer,
    signature_type: SignatureType,
    funder: Address,
) -> ClobClient {
    let signer = LocalSigner::from_str(PRIVATE_KEY)
        .expect("local signer")
        .with_chain_id(Some(POLYGON));
    let config = SdkConfig::builder().build();
    let mut sdk = SdkClient::new(&server.uri(), config)
        .expect("sdk client")
        .authentication_builder(&signer)
        .signature_type(signature_type)
        .funder(funder)
        .authenticate()
        .await
        .expect("authenticate funder client");
    sdk.stop_heartbeats()
        .await
        .expect("stop test heartbeat task");
    ClobClient {
        sdk: Arc::new(sdk),
        http: Client::new(),
        clob_base_url: server.uri(),
        maker_address: EvmAddress::parse(format!("{funder:#x}")).expect("funder maker"),
        chain_id: POLYGON,
        signature_type,
        signer: test_signer(),
        sdk_rule_lock: Mutex::new(()),
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
        maker_address: EvmAddress::parse(format!("{:#x}", test_signer().address()))
            .expect("EOA maker"),
        chain_id: POLYGON,
        signature_type: SignatureType::Eoa,
        signer: test_signer(),
        sdk_rule_lock: Mutex::new(()),
        order_post_timeout,
        rate_limiter: RateLimiter::new(),
        on_book_level_rejected: None::<BookLevelRejectHook>,
    }
}

pub fn test_order_request(order_type: OrderType) -> OrderRequest {
    OrderRequest {
        market_id: MarketId::new(TEST_CONDITION_ID),
        token_id: test_token_id(),
        expected_tick_size: TickSize::Hundredth,
        expected_minimum_order_size: Shares::new(dec!(5)),
        expected_neg_risk: false,
        expected_clob_market_info_payload_hash: test_market_info_hash(false),
        side: Side::Buy,
        amount: match order_type {
            OrderType::Fok | OrderType::Fak => VenueOrderAmount::PrincipalUsd(Usd::new(dec!(100))),
            OrderType::Gtc | OrderType::Gtd { expiration: _ } => {
                VenueOrderAmount::Shares(Shares::new(dec!(100)))
            }
        },
        expected_fee: Usd::new(dec!(1)),
        price: Price::new(dec!(0.92)),
        order_type,
        post_only: matches!(order_type, OrderType::Gtc | OrderType::Gtd { .. }),
    }
}
