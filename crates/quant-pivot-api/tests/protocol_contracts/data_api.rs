//! Data API HTTP protocol contracts over a deterministic local server.

use quant_pivot_api::data_api::DataApiClient;
use quant_pivot_error::api::ApiError;
use quant_pivot_models::{config::DataApiConfig, types::EvmAddress};
use rust_decimal_macros::dec;
use serde_json::Value;
use wiremock::{
    Mock, MockServer, ResponseTemplate,
    matchers::{method, path, query_param},
};

use super::support::{fast_http_client, fast_retry_policy};

const FUNDER: &str = "0x56687bf447db6ffa42ffe2204a05edaa20f55839";

/// OpenAPI-shaped position fixture with tiered fields (camelCase wire).
fn position_fixture() -> Value {
    serde_json::json!({
        "proxyWallet": FUNDER,
        "asset": "30114730931285",
        "conditionId": "0xdd22472e552920b8438158ea7238bfadfa4f736aa4cee91a6b86c39ead110917",
        "size": 100.0,
        "avgPrice": 0.01,
        "initialValue": 1.0,
        "curPrice": 0.02,
        "currentValue": 2.0,
        "cashPnl": 1.0,
        "percentPnl": 100.0,
        "totalBought": 100.0,
        "realizedPnl": 0.0,
        "percentRealizedPnl": 0.0,
        "redeemable": true,
        "mergeable": false,
        "negativeRisk": false,
        "outcome": "Yes",
        "outcomeIndex": 0,
        "title": "ignored UI metadata"
    })
}

fn data_api_client(server: &MockServer, page_size: u32) -> DataApiClient {
    DataApiClient::new(DataApiConfig {
        base_url: server.uri(),
        page_size,
        size_threshold: 1,
    })
    .with_http_client(fast_http_client())
    .with_retry_policy(fast_retry_policy())
}

#[tokio::test]
async fn positions_deserializes_openapi_fixture() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/positions"))
        .and(query_param("user", FUNDER))
        .respond_with(
            ResponseTemplate::new(200).set_body_json(serde_json::json!([position_fixture()])),
        )
        .mount(&server)
        .await;

    let positions = data_api_client(&server, 500)
        .positions(FUNDER)
        .await
        .expect("positions");

    assert_eq!(positions.len(), 1);
    let position = &positions[0];
    assert_eq!(position.proxy_wallet.as_deref(), Some(FUNDER));
    assert_eq!(position.asset, "30114730931285");
    assert_eq!(position.size, dec!(100));
    assert_eq!(position.initial_value, dec!(1));
    assert_eq!(position.cash_pnl, dec!(1));
    assert_eq!(position.percent_pnl, dec!(100));
    assert!(position.redeemable);
    assert!(!position.mergeable);
    assert!(!position.negative_risk);
}

#[tokio::test]
async fn positions_paginates_until_page() {
    let server = MockServer::start().await;
    let page_size = 2_u32;

    Mock::given(method("GET"))
        .and(path("/positions"))
        .and(query_param("offset", "0"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_json(serde_json::json!([position_fixture(), position_fixture()])),
        )
        .mount(&server)
        .await;

    Mock::given(method("GET"))
        .and(path("/positions"))
        .and(query_param("offset", "2"))
        .respond_with(
            ResponseTemplate::new(200).set_body_json(serde_json::json!([position_fixture()])),
        )
        .mount(&server)
        .await;

    let positions = data_api_client(&server, page_size)
        .positions(FUNDER)
        .await
        .expect("positions");

    assert_eq!(positions.len(), 3);
}

#[tokio::test]
async fn positions_query_params() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/positions"))
        .and(query_param("user", FUNDER))
        .and(query_param("limit", "500"))
        .and(query_param("offset", "0"))
        .and(query_param("sizeThreshold", "1"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!([])))
        .expect(1)
        .mount(&server)
        .await;

    data_api_client(&server, 500)
        .positions(FUNDER)
        .await
        .expect("positions");
}

#[tokio::test]
async fn positions_retries_transient_429() {
    let server = MockServer::start().await;

    Mock::given(method("GET"))
        .and(path("/positions"))
        .respond_with(ResponseTemplate::new(429).set_body_string("rate limited"))
        .up_to_n_times(1)
        .expect(1)
        .mount(&server)
        .await;

    Mock::given(method("GET"))
        .and(path("/positions"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!([])))
        .mount(&server)
        .await;

    let positions = data_api_client(&server, 500)
        .positions(FUNDER)
        .await
        .expect("positions after retry");

    assert!(positions.is_empty());
}

#[tokio::test]
async fn positions_error_surfaces_error() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/positions"))
        .respond_with(ResponseTemplate::new(400).set_body_string("bad request"))
        .mount(&server)
        .await;

    let error = data_api_client(&server, 500)
        .positions(FUNDER)
        .await
        .expect_err("400 should fail");

    match error {
        ApiError::Http {
            status: 400,
            retryable: false,
            ..
        } => {}
        other => panic!("expected non-retryable Http error, got {other:?}"),
    }
}

#[tokio::test]
async fn incentives_paginate_all_pages() {
    let server = MockServer::start().await;
    let wallet = EvmAddress::parse(FUNDER).expect("fixture funder");
    let market = format!("0x{}", "2".repeat(64));
    let activity = |timestamp: i64, kind: &str, hash_seed: char| {
        serde_json::json!({
            "proxyWallet": FUNDER,
            "timestamp": timestamp,
            "conditionId": market.clone(),
            "type": kind,
            "usdcSize": "0.75",
            "transactionHash": format!("0x{}", hash_seed.to_string().repeat(64))
        })
    };
    Mock::given(method("GET"))
        .and(path("/activity"))
        .and(query_param("offset", "0"))
        .and(query_param("limit", "2"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!([
            activity(101, "MAKER_REBATE", '3'),
            activity(102, "TAKER_REBATE", '4')
        ])))
        .expect(1)
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/activity"))
        .and(query_param("offset", "2"))
        .and(query_param("limit", "2"))
        .respond_with(
            ResponseTemplate::new(200).set_body_json(serde_json::json!([activity(
                103,
                "MAKER_REBATE",
                '5'
            )])),
        )
        .expect(1)
        .mount(&server)
        .await;

    let credits = data_api_client(&server, 2)
        .incentive_credits(&wallet, 100, 200)
        .await
        .expect("paginated incentive activity");

    assert_eq!(credits.len(), 3);
    assert_eq!(credits[0].occurred_at.timestamp(), 101);
    assert_eq!(credits[2].occurred_at.timestamp(), 103);
}
