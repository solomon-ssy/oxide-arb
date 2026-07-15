//! Official-SDK order metadata and FAK wire semantics over a local CLOB stub.

mod clob_wiremock;

use clob_wiremock::{
    mount_clob_requirements, mount_derive_api_key, mount_post_order, test_clob_client,
    test_clob_client_with_order_timeout, test_order_request, test_token_id,
};
use quant_pivot_api::clob::OrderSubmissionStage;
use quant_pivot_error::api::ApiError;
use quant_pivot_models::{
    enums::{
        common::{OrderType, TickSize},
        execution::VenueOrderStatus,
    },
    types::MarketId,
};
use rust_decimal_macros::dec;
use std::time::Duration;
use wiremock::{
    Mock, MockServer, ResponseTemplate,
    matchers::{method, path},
};

const CONDITION_ID: &str = "0x0000000000000000000000000000000000000000000000000000000000000001";

fn complete_market_info_payload() -> serde_json::Value {
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

    let expiration =
        u64::try_from(chrono::Utc::now().timestamp()).expect("positive timestamp") + 300;
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

    let expiration =
        u64::try_from(chrono::Utc::now().timestamp()).expect("positive timestamp") + 300;
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
