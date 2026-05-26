//! Wiremock-backed CLOB live-path tests (no network).

mod clob_wiremock;

use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};

use chrono::Utc;
use oxide_arb_api::infra::retry::RetryPolicy;
use oxide_arb_core::execution::clob_outcome::map_order_response;
use oxide_arb_models::domain::execution::ExecutionPlan;
use oxide_arb_models::enums::common::{ExecutionMode, OrderType, Side};
use oxide_arb_models::enums::execution::ExecutionOutcome;
use oxide_arb_models::enums::order::OrderStatus;
use oxide_arb_models::types::{
    EventId, ExecutionId, MarketId, OpportunityId, Price, ReservationId, Shares, Usd,
};
use rust_decimal_macros::dec;
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

use clob_wiremock::{
    mount_clob_requirements, mount_derive_api_key, mount_post_order, test_clob_client,
    test_order_request, test_token_id,
};

#[tokio::test]
async fn live_fok_fill() {
    let server = MockServer::start().await;
    mount_derive_api_key(&server).await;
    mount_clob_requirements(&server, &test_token_id()).await;
    mount_post_order(
        &server,
        r#"{
            "success": true,
            "orderID": "0xfill",
            "status": "matched",
            "makingAmount": "100",
            "takingAmount": "92",
            "transactionHashes": ["0x0000000000000000000000000000000000000000000000000000000000000001"]
        }"#,
        1,
    )
    .await;

    let clob = test_clob_client(&server).await;
    let resp = clob
        .place_order(&test_order_request(OrderType::Fok))
        .await
        .expect("place order");

    assert_eq!(resp.status, OrderStatus::Filled);
    assert_eq!(resp.filled_shares, Shares::new(dec!(100)));
}

#[tokio::test]
async fn live_fok_miss() {
    let server = MockServer::start().await;
    mount_derive_api_key(&server).await;
    mount_clob_requirements(&server, &test_token_id()).await;
    mount_post_order(
        &server,
        r#"{
            "success": true,
            "orderID": "0xmiss",
            "status": "live",
            "makingAmount": "0",
            "takingAmount": "0"
        }"#,
        1,
    )
    .await;

    let clob = test_clob_client(&server).await;
    let resp = clob
        .place_order(&test_order_request(OrderType::Fok))
        .await
        .expect("place order");

    assert_eq!(resp.status, OrderStatus::Rejected);

    let plan = sample_plan();
    let outcome = map_order_response(resp, &plan, ExecutionMode::Live, std::time::Instant::now());
    assert!(matches!(outcome, ExecutionOutcome::Miss { .. }));
}

#[tokio::test]
async fn live_partial_fill() {
    let server = MockServer::start().await;
    mount_derive_api_key(&server).await;
    mount_clob_requirements(&server, &test_token_id()).await;
    mount_post_order(
        &server,
        r#"{
            "success": true,
            "orderID": "0xpartial",
            "status": "matched",
            "makingAmount": "40",
            "takingAmount": "20"
        }"#,
        1,
    )
    .await;

    let clob = test_clob_client(&server).await;
    let req = test_order_request(OrderType::Fok);
    let resp = clob.place_order(&req).await.expect("place order");

    assert_eq!(resp.status, OrderStatus::PartiallyFilled);
    assert_eq!(resp.filled_shares, Shares::new(dec!(40)));
    assert!(resp.filled_shares.inner() < req.shares.inner());
}

#[tokio::test]
async fn live_fok_429_no_retry() {
    let server = MockServer::start().await;
    mount_derive_api_key(&server).await;
    mount_clob_requirements(&server, &test_token_id()).await;

    Mock::given(method("POST"))
        .and(path("/order"))
        .respond_with(
            ResponseTemplate::new(429)
                .insert_header("retry-after", "0")
                .set_body_string("rate limited"),
        )
        .expect(1)
        .mount(&server)
        .await;

    let attempts = Arc::new(AtomicU32::new(0));
    let clob = test_clob_client(&server).await;

    let policy = RetryPolicy::for_order_type(OrderType::Fok);
    assert_eq!(policy.max_attempts, Some(0));

    let result = clob.place_order(&test_order_request(OrderType::Fok)).await;
    attempts.fetch_add(1, Ordering::SeqCst);
    assert!(result.is_err(), "FOK must not retry after 429");
}

#[tokio::test]
async fn live_gtc_429_retries() {
    let server = MockServer::start().await;
    mount_derive_api_key(&server).await;
    mount_clob_requirements(&server, &test_token_id()).await;

    Mock::given(method("POST"))
        .and(path("/order"))
        .respond_with(
            ResponseTemplate::new(429)
                .insert_header("retry-after", "0")
                .set_body_string("rate limited"),
        )
        .up_to_n_times(1)
        .expect(1)
        .mount(&server)
        .await;

    Mock::given(method("POST"))
        .and(path("/order"))
        .respond_with(ResponseTemplate::new(200).set_body_string(
            r#"{
                "success": true,
                "orderID": "0xgtc",
                "status": "live",
                "makingAmount": "100",
                "takingAmount": "50"
            }"#,
        ))
        .mount(&server)
        .await;

    let clob = test_clob_client(&server).await;
    let resp = clob
        .place_order(&test_order_request(OrderType::Gtc))
        .await
        .expect("GTC should retry after 429");

    assert_eq!(resp.status, OrderStatus::Filled);
}

fn sample_plan() -> ExecutionPlan {
    ExecutionPlan {
        execution_id: ExecutionId::generate(),
        opportunity_id: OpportunityId::new_v7(),
        market_id: MarketId::new("m1"),
        event_id: EventId::new("e1"),
        token_id: test_token_id(),
        side: Side::Buy,
        shares: Shares::new(dec!(100)),
        limit_price: Price::new(dec!(0.5)),
        estimated_cost: Usd::new(dec!(50)),
        estimated_fee: oxide_arb_models::types::Usd::ZERO,
        neg_risk: false,
        reservation_id: ReservationId::new_id(),
        detected_at: Utc::now(),
        planned_at: Utc::now(),
    }
}
