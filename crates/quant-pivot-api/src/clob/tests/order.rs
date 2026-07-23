//! Official-SDK order metadata and FAK wire semantics over a local CLOB stub.

use std::time::Duration;

use alloy::primitives::Address;
use chrono::Utc;
use quant_pivot_error::api::ApiError;
use quant_pivot_models::{
    enums::{
        common::{OrderType, TickSize},
        execution::{VenueOrderStatus, VenueTradeStatus},
    },
    types::{EvmTransactionHash, MarketId, OrderId, VenueTradeId},
};
use rust_decimal_macros::dec;
use serde_json::Value;
use wiremock::{
    Mock, MockServer, ResponseTemplate,
    matchers::{method, path, query_param},
};

use super::support::{
    mount_clob_requirements, mount_derive_api_key, mount_post_order, test_clob_client,
    test_clob_client_with_order_timeout, test_deposit_wallet_clob_client, test_order_request,
    test_token_id,
};
use crate::clob::OrderSubmissionStage;

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
async fn sdk_metadata_matches_tick_and_negrisk_endpoints() {
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
async fn market_info_capture_sdk_validates_one_http_observation() {
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
async fn missing_market_info_fee_details_never_falls_back_to_zero() {
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
async fn fak_buy_uses_usd_amount_and_is_never_retried() {
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
        .map(|request| String::from_utf8_lossy(&request.body))
        .expect("order request");
    assert!(body.contains("\"orderType\":\"FAK\""));
}

#[tokio::test]
async fn deposit_wallet_order_uses_poly1271_maker_and_signer_wire_identity() {
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
    let client = test_deposit_wallet_clob_client(&server, deposit_wallet).await;

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

#[tokio::test]
async fn post_order_preserves_all_immediate_trade_and_transaction_identities() {
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
async fn post_order_preserves_trade_ids_and_sdk_backfilled_hashes() {
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
async fn exact_order_and_trade_reads_preserve_restart_identity_and_status() {
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
async fn unresolved_exact_trade_keeps_zero_hash_absent() {
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
async fn sdk_poll_exhaustion_returns_durable_trade_identity_without_hash() {
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
    let client = test_clob_client_with_order_timeout(&server, Duration::from_secs(35)).await;

    let response = client
        .place_order(&test_order_request(OrderType::Fak))
        .await
        .expect("poll exhaustion remains an accepted placement response");

    assert_eq!(response.trade_ids[0].as_str(), "trade-pending");
    assert!(response.transaction_hashes.is_empty());
}

#[tokio::test]
async fn every_order_type_is_attempted_once_on_ambiguous_http_failure() {
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
async fn every_order_type_is_attempted_once_on_post_timeout() {
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
    let client = test_clob_client_with_order_timeout(&server, Duration::from_millis(25)).await;

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
