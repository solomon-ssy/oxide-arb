//! Official-SDK order metadata and FAK wire semantics over a local CLOB stub.

use std::time::Duration;

use alloy::primitives::Address;
use chrono::{NaiveDate, Utc};
use polymarket_client_sdk_v2::clob::types::SignatureType;
use quant_pivot_error::api::ApiError;
use quant_pivot_models::{
    config::PolymarketConfig,
    enums::{
        common::{OrderType, TickSize},
        execution::{VenueOrderStatus, VenueTradeStatus},
    },
    types::{EvmAddress, EvmTransactionHash, MarketId, OrderId, VenueTradeId},
};
use rust_decimal_macros::dec;
use serde_json::Value;
use wiremock::{
    Mock, MockServer, ResponseTemplate,
    matchers::{method, path, query_param},
};

use super::support::{
    clob_client_order_timeout, deposit_wallet_clob_client, funder_clob_client,
    mount_clob_balance_requirements, mount_clob_requirements, mount_derive_api_key,
    mount_post_order, test_clob_client, test_order_request, test_signer, test_token_id,
};
use crate::{
    clob::{ClobClient, OrderSubmissionStage},
    wallet::WalletTopology,
};

const CONDITION_ID: &str = "0x0000000000000000000000000000000000000000000000000000000000000001";
const TRANSACTION_HASH_1: &str =
    "0x1111111111111111111111111111111111111111111111111111111111111111";
const TRANSACTION_HASH_2: &str =
    "0x2222222222222222222222222222222222222222222222222222222222222222";

fn complete_market_info_payload() -> Value {
    serde_json::json!({
        "c": CONDITION_ID,
        "t": [
            { "t": test_token_id().as_str(), "o": "Yes" },
            { "t": "2", "o": "No" }
        ],
        "mts": "0.01",
        "mos": "5",
        "nr": false,
        "itode": false,
        "ibce": false,
        "oas": 0,
        "fd": { "r": "0.25", "e": 2, "to": true },
        "mbf": 0,
        "tbf": 0,
        "rfqe": false
    })
}

fn trade_payload(id: &str, status: &str, transaction_hash: &str) -> Value {
    serde_json::json!({
        "id": id,
        "taker_order_id": "venue-fak-async",
        "market": CONDITION_ID,
        "asset_id": test_token_id().as_str(),
        "side": "BUY",
        "size": "50",
        "fee_rate_bps": "0",
        "price": "1",
        "status": status,
        "match_time": "1705322096",
        "last_update": "1705322130",
        "outcome": "YES",
        "bucket_index": 0,
        "owner": "ffffffff-ffff-ffff-ffff-ffffffffffff",
        "maker_address": "0x2222222222222222222222222222222222222222",
        "maker_orders": [],
        "transaction_hash": transaction_hash,
        "trader_side": "TAKER"
    })
}

fn trades_page(trades: &[Value]) -> Value {
    serde_json::json!({
        "count": trades.len(),
        "data": trades,
        "limit": 100,
        "next_cursor": "LTE="
    })
}

fn order_payload(order_id: &str, status: &str, trade_ids: &[&str]) -> Value {
    serde_json::json!({
        "id": order_id,
        "status": status,
        "owner": "ffffffff-ffff-ffff-ffff-ffffffffffff",
        "maker_address": "0x2222222222222222222222222222222222222222",
        "market": CONDITION_ID,
        "asset_id": test_token_id().as_str(),
        "side": "BUY",
        "original_size": "50",
        "size_matched": "50",
        "price": "1",
        "associate_trades": trade_ids,
        "outcome": "YES",
        "created_at": 1_705_322_096,
        "expiration": "1705322396",
        "order_type": "FAK"
    })
}

#[tokio::test]
async fn connect_rejects_v1_protocol() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/version"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "version": 1
        })))
        .expect(1)
        .mount(&server)
        .await;
    let signer = test_signer();
    let topology = WalletTopology::eoa(signer.address());
    let config = PolymarketConfig {
        clob_base_url: server.uri(),
        ..PolymarketConfig::default()
    };

    let Err(error) = ClobClient::connect(signer, &config, &topology).await else {
        panic!("CLOB V1 must be rejected before authentication");
    };

    assert!(matches!(
        error,
        ApiError::Clob {
            code,
            retryable: false,
            ..
        } if code == "unsupported_protocol_version"
    ));
    let requests = server.received_requests().await.expect("request ledger");
    assert_eq!(
        requests
            .iter()
            .map(|request| request.url.path())
            .collect::<Vec<_>>(),
        vec!["/version"]
    );
}

#[tokio::test]
async fn sdk_metadata_matches_endpoints() {
    let server = MockServer::start().await;
    mount_derive_api_key(&server).await;
    let token_id = test_token_id();
    mount_clob_requirements(&server, &token_id).await;
    let client = test_clob_client(&server).await;

    let metadata = client.order_metadata(&token_id).await.expect("metadata");

    assert_eq!(metadata.tick_size, TickSize::Hundredth);
    assert!(!metadata.neg_risk);
}

#[tokio::test]
async fn market_info_validates_observation() {
    let server = MockServer::start().await;
    mount_derive_api_key(&server).await;
    Mock::given(method("GET"))
        .and(path(format!("/clob-markets/{CONDITION_ID}")))
        .respond_with(ResponseTemplate::new(200).set_body_json(complete_market_info_payload()))
        .expect(1)
        .mount(&server)
        .await;
    let client = test_clob_client(&server).await;

    let observation = client
        .clob_market_info_version(&MarketId::new(CONDITION_ID))
        .await
        .expect("complete V2 market info");

    assert_eq!(observation.fee_details.rate, dec!(0.25));
    assert_eq!(observation.fee_details.exponent, 2);
    assert!(observation.fee_details.taker_only);
    assert_eq!(observation.builder_maker_fee_rate_bps, 0);
    assert_eq!(observation.builder_taker_fee_rate_bps, 0);
}

#[tokio::test]
async fn missing_fee_never_defaults() {
    let server = MockServer::start().await;
    mount_derive_api_key(&server).await;
    let mut payload = complete_market_info_payload();
    payload.as_object_mut().expect("test object").remove("fd");
    Mock::given(method("GET"))
        .and(path(format!("/clob-markets/{CONDITION_ID}")))
        .respond_with(ResponseTemplate::new(200).set_body_json(payload))
        .expect(1)
        .mount(&server)
        .await;
    let client = test_clob_client(&server).await;

    let error = client
        .clob_market_info_version(&MarketId::new(CONDITION_ID))
        .await
        .expect_err("missing fd must be rejected");

    assert!(error.to_string().contains("CLOB market-info contract"));
}

#[tokio::test]
async fn fak_uses_never_retried() {
    let server = MockServer::start().await;
    mount_derive_api_key(&server).await;
    let token_id = test_token_id();
    mount_clob_requirements(&server, &token_id).await;
    mount_post_order(
        &server,
        r#"{
            "errorMsg":"",
            "makingAmount":"50",
            "orderID":"venue-fak-1",
            "status":"matched",
            "success":true,
            "takingAmount":"40",
            "transactionHashes":[]
        }"#,
        1,
    )
    .await;
    let client = test_clob_client(&server).await;

    let response = client
        .place_order(&test_order_request(OrderType::Fak))
        .await
        .expect("FAK response");

    assert_eq!(response.status, VenueOrderStatus::PartiallyFilled);
    let requests = server.received_requests().await.expect("request ledger");
    let body = requests
        .iter()
        .find(|request| request.url.path() == "/order")
        .map(|request| serde_json::from_slice::<Value>(&request.body).expect("V2 order JSON"))
        .expect("order request");
    assert_eq!(body["orderType"].as_str(), Some("FAK"));
    let order = body["order"].as_object().expect("V2 signed order object");
    for field in ["timestamp", "metadata", "builder"] {
        assert!(order.contains_key(field), "V2 order must contain {field}");
    }
    for field in ["nonce", "feeRateBps", "taker"] {
        assert!(
            !order.contains_key(field),
            "V2 order must not contain V1 field {field}"
        );
    }
}

#[tokio::test]
async fn buy_rechecks_pusd_requirement() {
    let server = MockServer::start().await;
    mount_derive_api_key(&server).await;
    let token_id = test_token_id();
    mount_clob_balance_requirements(&server, &token_id, "100").await;
    let client = test_clob_client(&server).await;

    let error = client
        .place_order(&test_order_request(OrderType::Fak))
        .await
        .expect_err("principal plus fee exceeds live pUSD collateral");

    assert_eq!(error.stage, OrderSubmissionStage::Prepare);
    assert!(matches!(
        error.source,
        ApiError::Clob {
            code,
            retryable: false,
            ..
        } if code == "insufficient_pusd_balance"
    ));
    assert!(
        server
            .received_requests()
            .await
            .expect("request ledger")
            .iter()
            .all(|request| request.url.path() != "/order")
    );
}

#[tokio::test]
async fn deposit_wallet_uses_identity() {
    let server = MockServer::start().await;
    mount_derive_api_key(&server).await;
    let token_id = test_token_id();
    mount_clob_requirements(&server, &token_id).await;
    mount_post_order(
        &server,
        r#"{
            "errorMsg":"",
            "makingAmount":"50",
            "orderID":"venue-poly1271-1",
            "status":"matched",
            "success":true,
            "takingAmount":"40",
            "transactionHashes":[]
        }"#,
        1,
    )
    .await;
    let deposit_wallet = Address::repeat_byte(0x42);
    let client = deposit_wallet_clob_client(&server, deposit_wallet).await;

    client
        .place_order(&test_order_request(OrderType::Fak))
        .await
        .expect("POLY_1271 order response");

    let requests = server.received_requests().await.expect("request ledger");
    let payload: Value = requests
        .iter()
        .find(|request| request.url.path() == "/order")
        .map(|request| serde_json::from_slice(&request.body).expect("order JSON"))
        .expect("order request");
    let order = &payload["order"];
    let wallet = format!("{deposit_wallet:#x}");
    assert_eq!(order["maker"].as_str(), Some(wallet.as_str()));
    assert_eq!(order["signer"].as_str(), Some(wallet.as_str()));
    assert_eq!(order["signatureType"].as_u64(), Some(3));
    assert!(
        order["signature"]
            .as_str()
            .is_some_and(|signature| signature.len() > 132),
        "POLY_1271 order signature must be ERC-7739 wrapped, not a 65-byte ECDSA signature"
    );
}

async fn assert_maker_identity(signature_type: Option<SignatureType>, funder: Address) {
    let server = MockServer::start().await;
    mount_derive_api_key(&server).await;
    let token_id = test_token_id();
    mount_clob_requirements(&server, &token_id).await;
    mount_post_order(
        &server,
        r#"{
            "errorMsg":"",
            "makingAmount":"50",
            "orderID":"venue-maker-identity",
            "status":"matched",
            "success":true,
            "takingAmount":"40",
            "transactionHashes":[]
        }"#,
        1,
    )
    .await;
    let client = match signature_type {
        Some(signature_type) => funder_clob_client(&server, signature_type, funder).await,
        None => test_clob_client(&server).await,
    };
    let maker = client.maker_address().clone();
    let date = NaiveDate::from_ymd_opt(2026, 8, 15).expect("fixture date");
    Mock::given(method("GET"))
        .and(path("/rebates/current"))
        .and(query_param("date", "2026-08-15"))
        .and(query_param("maker_address", maker.as_str()))
        .respond_with(
            ResponseTemplate::new(200).set_body_json(serde_json::json!([{
                "date": "2026-08-15",
                "condition_id": CONDITION_ID,
                "asset_address": format!("0x{}", "3".repeat(40)),
                "maker_address": maker.as_str(),
                "rebated_fees_usdc": "0.25"
            }])),
        )
        .expect(1)
        .mount(&server)
        .await;

    client
        .place_order(&test_order_request(OrderType::Fak))
        .await
        .expect("place identity order");
    let awards = client
        .maker_rebate_awards(date)
        .await
        .expect("read maker awards");
    let requests = server.received_requests().await.expect("request ledger");
    let order: Value = requests
        .iter()
        .find(|request| request.url.path() == "/order")
        .map(|request| serde_json::from_slice(&request.body).expect("order JSON"))
        .expect("order request");
    let signed_order_maker = order["order"]["maker"]
        .as_str()
        .and_then(|value| EvmAddress::parse(value.to_ascii_lowercase()).ok())
        .expect("signed order maker");

    assert_eq!(awards[0].maker_address, signed_order_maker);
    assert_eq!(&signed_order_maker, client.maker_address());
}

#[tokio::test]
async fn rebate_matches_order_maker() {
    assert_maker_identity(None, test_signer().address()).await;
    assert_maker_identity(Some(SignatureType::Proxy), Address::repeat_byte(0x41)).await;
    assert_maker_identity(Some(SignatureType::GnosisSafe), Address::repeat_byte(0x42)).await;
}

#[tokio::test]
async fn post_order_preserves_identities() {
    let server = MockServer::start().await;
    mount_derive_api_key(&server).await;
    let token_id = test_token_id();
    mount_clob_requirements(&server, &token_id).await;
    mount_post_order(
        &server,
        &serde_json::json!({
            "errorMsg": "",
            "makingAmount": "100",
            "orderID": "venue-fak-immediate",
            "status": "matched",
            "success": true,
            "takingAmount": "100",
            "tradeIDs": ["trade-1", "trade-2"],
            "transactionHashes": [TRANSACTION_HASH_1, TRANSACTION_HASH_2]
        })
        .to_string(),
        1,
    )
    .await;
    let client = test_clob_client(&server).await;

    let response = client
        .place_order(&test_order_request(OrderType::Fak))
        .await
        .expect("immediate placement response");

    assert_eq!(
        response
            .trade_ids
            .iter()
            .map(VenueTradeId::as_str)
            .collect::<Vec<_>>(),
        vec!["trade-1", "trade-2"]
    );
    assert_eq!(
        response
            .transaction_hashes
            .iter()
            .map(EvmTransactionHash::as_str)
            .collect::<Vec<_>>(),
        vec![TRANSACTION_HASH_1, TRANSACTION_HASH_2]
    );
}

#[tokio::test]
async fn post_order_preserves_hashes() {
    let server = MockServer::start().await;
    mount_derive_api_key(&server).await;
    let token_id = test_token_id();
    mount_clob_requirements(&server, &token_id).await;
    mount_post_order(
        &server,
        r#"{
            "errorMsg":"",
            "makingAmount":"100",
            "orderID":"venue-fak-async",
            "status":"matched",
            "success":true,
            "takingAmount":"100",
            "tradeIDs":["trade-1","trade-2"]
        }"#,
        1,
    )
    .await;
    Mock::given(method("GET"))
        .and(path("/data/trades"))
        .and(query_param("id", "trade-1"))
        .respond_with(
            ResponseTemplate::new(200).set_body_json(trades_page(&[trade_payload(
                "trade-1",
                "MINED",
                TRANSACTION_HASH_1,
            )])),
        )
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/data/trades"))
        .and(query_param("id", "trade-2"))
        .respond_with(
            ResponseTemplate::new(200).set_body_json(trades_page(&[trade_payload(
                "trade-2",
                "CONFIRMED",
                TRANSACTION_HASH_2,
            )])),
        )
        .mount(&server)
        .await;
    let client = test_clob_client(&server).await;

    let response = client
        .place_order(&test_order_request(OrderType::Fak))
        .await
        .expect("asynchronous placement response");

    assert_eq!(response.trade_ids.len(), 2);
    assert_eq!(
        response
            .transaction_hashes
            .iter()
            .map(EvmTransactionHash::as_str)
            .collect::<Vec<_>>(),
        vec![TRANSACTION_HASH_1, TRANSACTION_HASH_2]
    );
}

#[tokio::test]
async fn exact_reads_preserve_status() {
    let server = MockServer::start().await;
    mount_derive_api_key(&server).await;
    Mock::given(method("GET"))
        .and(path("/data/order/venue-restart"))
        .respond_with(ResponseTemplate::new(200).set_body_json(order_payload(
            "venue-restart",
            "MATCHED",
            &["trade-restart"],
        )))
        .expect(1)
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/data/trades"))
        .and(query_param("id", "trade-restart"))
        .respond_with(
            ResponseTemplate::new(200).set_body_json(trades_page(&[trade_payload(
                "trade-restart",
                "CONFIRMED",
                TRANSACTION_HASH_1,
            )])),
        )
        .expect(1)
        .mount(&server)
        .await;
    let client = test_clob_client(&server).await;

    let order = client
        .get_order(&OrderId::new("venue-restart"))
        .await
        .expect("exact order");
    let trade = client
        .get_trade(&VenueTradeId::new("trade-restart"))
        .await
        .expect("exact trade")
        .expect("trade exists");

    assert!(!order.is_working);
    assert_eq!(
        order.associated_trade_ids,
        vec![VenueTradeId::new("trade-restart")]
    );
    assert_eq!(trade.trade_id, VenueTradeId::new("trade-restart"));
    assert_eq!(trade.status, VenueTradeStatus::Confirmed);
    assert_eq!(
        trade
            .transaction_hash
            .as_ref()
            .map(EvmTransactionHash::as_str),
        Some(TRANSACTION_HASH_1)
    );
}

#[tokio::test]
async fn unresolved_keeps_zero_absent() {
    let server = MockServer::start().await;
    mount_derive_api_key(&server).await;
    Mock::given(method("GET"))
        .and(path("/data/trades"))
        .and(query_param("id", "trade-matched"))
        .respond_with(
            ResponseTemplate::new(200).set_body_json(trades_page(&[trade_payload(
                "trade-matched",
                "MATCHED",
                "",
            )])),
        )
        .expect(1)
        .mount(&server)
        .await;
    let client = test_clob_client(&server).await;

    let trade = client
        .get_trade(&VenueTradeId::new("trade-matched"))
        .await
        .expect("exact trade")
        .expect("trade exists");

    assert_eq!(trade.status, VenueTradeStatus::Matched);
    assert!(trade.transaction_hash.is_none());
}

#[tokio::test]
async fn sdk_returns_without_hash() {
    let server = MockServer::start().await;
    mount_derive_api_key(&server).await;
    let token_id = test_token_id();
    mount_clob_requirements(&server, &token_id).await;
    mount_post_order(
        &server,
        r#"{
            "errorMsg":"",
            "makingAmount":"100",
            "orderID":"venue-fak-pending",
            "status":"matched",
            "success":true,
            "takingAmount":"100",
            "tradeIDs":["trade-pending"]
        }"#,
        1,
    )
    .await;
    Mock::given(method("GET"))
        .and(path("/data/trades"))
        .and(query_param("id", "trade-pending"))
        .respond_with(ResponseTemplate::new(200).set_body_json(trades_page(&[])))
        .mount(&server)
        .await;
    let client = clob_client_order_timeout(&server, Duration::from_secs(35)).await;

    let response = client
        .place_order(&test_order_request(OrderType::Fak))
        .await
        .expect("poll exhaustion remains an accepted placement response");

    assert_eq!(response.trade_ids[0].as_str(), "trade-pending");
    assert!(response.transaction_hashes.is_empty());
}

#[tokio::test]
async fn order_type_attempted_failure() {
    let server = MockServer::start().await;
    mount_derive_api_key(&server).await;
    let token_id = test_token_id();
    mount_clob_requirements(&server, &token_id).await;
    Mock::given(method("POST"))
        .and(path("/order"))
        .respond_with(ResponseTemplate::new(503).set_body_string("venue unavailable"))
        .expect(4)
        .mount(&server)
        .await;
    let client = test_clob_client(&server).await;

    let expiration = u64::try_from(Utc::now().timestamp()).expect("positive timestamp") + 300;
    for order_type in [
        OrderType::Fok,
        OrderType::Fak,
        OrderType::Gtc,
        OrderType::Gtd { expiration },
    ] {
        assert!(
            client
                .place_order(&test_order_request(order_type))
                .await
                .is_err()
        );
    }
}

#[tokio::test]
async fn order_type_attempted_timeout() {
    let server = MockServer::start().await;
    mount_derive_api_key(&server).await;
    let token_id = test_token_id();
    mount_clob_requirements(&server, &token_id).await;
    Mock::given(method("POST"))
        .and(path("/order"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_delay(Duration::from_millis(200))
                .set_body_string(r#"{"success":true}"#),
        )
        .expect(4)
        .mount(&server)
        .await;
    let client = clob_client_order_timeout(&server, Duration::from_millis(25)).await;

    let expiration = u64::try_from(Utc::now().timestamp()).expect("positive timestamp") + 300;
    for order_type in [
        OrderType::Fok,
        OrderType::Fak,
        OrderType::Gtc,
        OrderType::Gtd { expiration },
    ] {
        let error = client
            .place_order(&test_order_request(order_type))
            .await
            .expect_err("delayed response must time out");
        assert_eq!(error.stage, OrderSubmissionStage::Post);
        assert!(matches!(error.source, ApiError::Timeout { .. }));
    }
}
