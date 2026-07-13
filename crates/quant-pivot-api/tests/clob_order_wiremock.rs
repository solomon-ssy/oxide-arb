//! Official-SDK order metadata and FAK wire semantics over a local CLOB stub.

mod clob_wiremock;

use clob_wiremock::{
    mount_clob_requirements, mount_derive_api_key, mount_post_order, test_clob_client,
    test_clob_client_with_order_timeout, test_order_request, test_token_id,
};
use quant_pivot_api::clob::OrderSubmissionStage;
use quant_pivot_error::api::ApiError;
use quant_pivot_models::enums::{
    common::{OrderType, TickSize},
    execution::VenueOrderStatus,
};
use std::time::Duration;
use wiremock::{
    Mock, MockServer, ResponseTemplate,
    matchers::{method, path},
};

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
