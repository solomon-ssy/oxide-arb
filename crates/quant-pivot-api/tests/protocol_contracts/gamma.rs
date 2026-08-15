//! Gamma HTTP protocol contracts over a deterministic local server.

use quant_pivot_api::gamma::GammaClient;
use quant_pivot_models::{
    config::GammaConfig,
    enums::common::MarketCategory,
    types::{MarketId, TokenId},
};
use serde_json::Value;
use wiremock::{
    Mock, MockServer, ResponseTemplate,
    matchers::{method, path, query_param},
};

use super::support::{fast_http_client, fast_retry_policy};

/// Standard `GET /markets?condition_ids=` payload: array with one market
/// embedding its parent event (id + tags).
fn market_by_condition_body() -> Value {
    serde_json::json!([{
        "conditionId": "0xabc",
        "question": "Test?",
        "active": true,
        "closed": false,
        "feesEnabled": true,
        "clobTokenIds": ["1", "2"],
        "outcomes": ["Team A", "Team B"],
        "events": [{
            "id": "evt-42",
            "tags": [{ "label": "Sports", "slug": "sports" }]
        }]
    }])
}

#[tokio::test]
async fn gamma_get_deserializes_mock() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/markets"))
        .and(query_param("condition_ids", "0xabc"))
        .respond_with(ResponseTemplate::new(200).set_body_json(market_by_condition_body()))
        .mount(&server)
        .await;

    let client = GammaClient::new(GammaConfig {
        base_url: server.uri(),
        ..GammaConfig::default()
    })
    .with_http_client(fast_http_client())
    .with_retry_policy(fast_retry_policy());

    let market = client
        .get_market(&MarketId::new("0xabc"))
        .await
        .expect("get_market");

    assert_eq!(market.market_id.as_str(), "0xabc");
    assert_eq!(market.event_id.as_str(), "evt-42");
    assert_eq!(market.tokens.len(), 2);
    // Positional pair for non-Yes/No binary outcomes.
    assert_eq!(market.token_yes.as_str(), "1");
    assert_eq!(market.token_no.as_str(), "2");
    assert!(market.categories.contains(MarketCategory::Sports));
}

#[tokio::test]
async fn gamma_get_unknown_error() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/markets"))
        .and(query_param("condition_ids", "0xmissing"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!([])))
        .mount(&server)
        .await;

    let client = GammaClient::new(GammaConfig {
        base_url: server.uri(),
        ..GammaConfig::default()
    })
    .with_http_client(fast_http_client())
    .with_retry_policy(fast_retry_policy());

    let result = client.get_market(&MarketId::new("0xmissing")).await;
    assert!(result.is_err(), "empty array must be a NotFound error");
}

#[tokio::test]
async fn gamma_resolution_derives_prices() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/markets"))
        .and(query_param("condition_ids", "0xdone"))
        .respond_with(
            ResponseTemplate::new(200).set_body_json(serde_json::json!([{
                "conditionId": "0xdone",
                "question": "Over/Under 21.5?",
                "active": false,
                "closed": true,
                "umaResolutionStatus": "resolved",
                "closedTime": "2026-06-11 04:05:01+00",
                "clobTokenIds": ["111", "222"],
                "outcomes": ["Over", "Under"],
                "outcomePrices": ["0", "1"],
                "events": [{ "id": "evt-9" }]
            }])),
        )
        .mount(&server)
        .await;

    let client = GammaClient::new(GammaConfig {
        base_url: server.uri(),
        ..GammaConfig::default()
    })
    .with_http_client(fast_http_client())
    .with_retry_policy(fast_retry_policy());

    let resolution = client
        .get_resolution_status(&MarketId::new("0xdone"))
        .await
        .expect("resolution probe")
        .expect("settled market yields Some");

    assert!(resolution.resolved);
    assert_eq!(
        resolution.winning_token_id.as_ref().map(TokenId::as_str),
        Some("222")
    );
    assert_eq!(resolution.winning_outcome.as_deref(), Some("Under"));
    assert!(resolution.resolved_at.is_some(), "closedTime must parse");
}

#[tokio::test]
async fn gamma_resolution_open_none() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/markets"))
        .and(query_param("condition_ids", "0xopen"))
        .respond_with(
            ResponseTemplate::new(200).set_body_json(serde_json::json!([{
                "conditionId": "0xopen",
                "question": "Still trading?",
                "active": true,
                "closed": false,
                "clobTokenIds": ["1", "2"],
                "outcomes": ["Yes", "No"],
                "events": [{ "id": "evt-1" }]
            }])),
        )
        .mount(&server)
        .await;

    let client = GammaClient::new(GammaConfig {
        base_url: server.uri(),
        ..GammaConfig::default()
    })
    .with_http_client(fast_http_client())
    .with_retry_policy(fast_retry_policy());

    let resolution = client
        .get_resolution_status(&MarketId::new("0xopen"))
        .await
        .expect("resolution probe");
    assert!(resolution.is_none());
}

#[tokio::test]
async fn gamma_retries_on_429() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/markets"))
        .and(query_param("condition_ids", "0xabc"))
        .respond_with(
            ResponseTemplate::new(429)
                .insert_header("retry-after", "0")
                .set_body_string("rate limited"),
        )
        .up_to_n_times(1)
        .expect(1)
        .mount(&server)
        .await;

    Mock::given(method("GET"))
        .and(path("/markets"))
        .and(query_param("condition_ids", "0xabc"))
        .respond_with(ResponseTemplate::new(200).set_body_json(market_by_condition_body()))
        .mount(&server)
        .await;

    let client = GammaClient::new(GammaConfig {
        base_url: server.uri(),
        ..GammaConfig::default()
    })
    .with_http_client(fast_http_client())
    .with_retry_policy(fast_retry_policy());

    let result = client.get_market(&MarketId::new("0xabc")).await;

    assert!(result.is_ok(), "expected retry success: {result:?}");
}

#[tokio::test]
async fn gamma_full_sync_cursor() {
    let server = MockServer::start().await;

    Mock::given(method("GET"))
        .and(path("/events/keyset"))
        .and(query_param("active", "true"))
        .and(query_param("closed", "false"))
        .and(query_param("limit", "100"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "events": [{
                "id": "evt-page-1",
                "title": "Page one",
                "slug": "page-one",
                "markets": [{
                    "conditionId": "0xpage1",
                    "question": "Page one?",
                    "active": true,
                    "closed": false,
                    "clobTokenIds": ["11", "12"],
                    "outcomes": ["Yes", "No"]
                }]
            }],
            "next_cursor": "cursor-page-2"
        })))
        .up_to_n_times(1)
        .expect(1)
        .mount(&server)
        .await;

    Mock::given(method("GET"))
        .and(path("/events/keyset"))
        .and(query_param("active", "false"))
        .and(query_param("closed", "true"))
        .and(query_param("limit", "100"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "events": [{
                "id": "evt-historical",
                "title": "Historical",
                "slug": "historical",
                "markets": []
            }]
        })))
        .expect(1)
        .mount(&server)
        .await;

    Mock::given(method("GET"))
        .and(path("/events/keyset"))
        .and(query_param("active", "true"))
        .and(query_param("closed", "false"))
        .and(query_param("limit", "100"))
        .and(query_param("after_cursor", "cursor-page-2"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "events": [{
                "id": "evt-page-2",
                "title": "Page two",
                "slug": "page-two",
                "markets": [{
                    "conditionId": "0xpage2",
                    "question": "Page two?",
                    "active": true,
                    "closed": false,
                    "clobTokenIds": ["21", "22"],
                    "outcomes": ["Yes", "No"]
                }]
            }]
        })))
        .mount(&server)
        .await;

    let client = GammaClient::new(GammaConfig {
        base_url: server.uri(),
        ..GammaConfig::default()
    })
    .with_http_client(fast_http_client());

    let events = client.full_sync().await.expect("full_sync");
    assert_eq!(events.len(), 3);
    assert_eq!(events[0].slug, "historical");
    assert_eq!(events[1].slug, "page-one");
    assert_eq!(events[2].slug, "page-two");
}

#[tokio::test]
async fn gamma_invalid_without_body() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/events/keyset"))
        .and(query_param("active", "true"))
        .and(query_param("closed", "false"))
        .respond_with(
            ResponseTemplate::new(200)
                .insert_header("content-type", "text/html")
                .set_body_string("<html>temporary edge failure</html>"),
        )
        .up_to_n_times(1)
        .expect(1)
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/events/keyset"))
        .and(query_param("active", "true"))
        .and(query_param("closed", "false"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "events": [{ "id": "evt-recovered", "markets": [] }]
        })))
        .expect(1)
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/events/keyset"))
        .and(query_param("active", "false"))
        .and(query_param("closed", "true"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "events": []
        })))
        .expect(1)
        .mount(&server)
        .await;

    let client = GammaClient::new(GammaConfig {
        base_url: server.uri(),
        ..GammaConfig::default()
    })
    .with_http_client(fast_http_client())
    .with_retry_policy(fast_retry_policy());

    let events = client.full_sync().await.expect("syntax failure is retried");
    assert_eq!(events.len(), 1);
    assert_eq!(events[0].event_id.as_str(), "evt-recovered");
}

#[tokio::test]
async fn gamma_never_retries_drift() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/events/keyset"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "events": [{ "markets": [] }]
        })))
        .expect(1)
        .mount(&server)
        .await;

    let client = GammaClient::new(GammaConfig {
        base_url: server.uri(),
        ..GammaConfig::default()
    })
    .with_http_client(fast_http_client())
    .with_retry_policy(fast_retry_policy());

    let error = client
        .full_sync()
        .await
        .expect_err("missing event id is permanent contract drift");
    let rendered = error.to_string();
    assert!(rendered.contains("body_hash=blake3:"));
    assert!(!rendered.contains("markets"));
}

#[tokio::test]
async fn gamma_rejects_unbounded_continuation() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/events/keyset"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "events": [{ "id": "evt-1", "markets": [] }],
            "next_cursor": "unexpected-page-2"
        })))
        .expect(1)
        .mount(&server)
        .await;

    let client = GammaClient::new(GammaConfig {
        base_url: server.uri(),
        max_keyset_pages: 1,
        max_keyset_requests: 1,
        ..GammaConfig::default()
    })
    .with_http_client(fast_http_client())
    .with_retry_policy(fast_retry_policy());

    let error = client
        .full_sync()
        .await
        .expect_err("continuation beyond configured page budget must fail");
    assert!(error.to_string().contains("page budget exhausted"));
}

#[tokio::test]
async fn gamma_token_uses_paginator() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/events/keyset"))
        .and(query_param("limit", "50"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "events": [{ "id": "evt-empty", "markets": [] }],
            "next_cursor": "token-page"
        })))
        .up_to_n_times(1)
        .expect(1)
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/events/keyset"))
        .and(query_param("limit", "50"))
        .and(query_param("after_cursor", "token-page"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "events": [{
                "id": "evt-token",
                "title": "Token event",
                "slug": "token-event",
                "markets": [{
                    "conditionId": "0xtoken",
                    "question": "Token market?",
                    "active": true,
                    "closed": false,
                    "clobTokenIds": ["101", "102"],
                    "outcomes": ["Yes", "No"]
                }]
            }]
        })))
        .expect(1)
        .mount(&server)
        .await;

    let client = GammaClient::new(GammaConfig {
        base_url: server.uri(),
        ..GammaConfig::default()
    })
    .with_http_client(fast_http_client())
    .with_retry_policy(fast_retry_policy());

    let token = client
        .discover_active_token()
        .await
        .expect("token from second page");
    assert_eq!(token.as_str(), "101");
}
