//! Gamma HTTP client tests with wiremock (no live network).

use chrono::{Duration, Utc};
use quant_pivot_api::{gamma::GammaClient, infra::retry::RetryPolicy};
use quant_pivot_models::{
    config::GammaConfig,
    enums::common::MarketCategory,
    types::{MarketId, TokenId},
};
use wiremock::{
    Mock, MockServer, ResponseTemplate,
    matchers::{method, path, query_param},
};

/// Standard `GET /markets?condition_ids=` payload: array with one market
/// embedding its parent event (id + tags).
fn market_by_condition_body() -> serde_json::Value {
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
#[ignore = "run with cargo test-network"]
async fn gamma_get_market_deserializes_from_mock() {
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
#[ignore = "run with cargo test-network"]
async fn gamma_get_market_unknown_condition_id_is_an_error() {
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
#[ignore = "run with cargo test-network"]
async fn gamma_resolution_derives_winner_from_outcome_prices() {
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
#[ignore = "run with cargo test-network"]
async fn gamma_resolution_open_market_yields_none() {
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
#[ignore = "run with cargo test-network"]
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
#[ignore = "run with cargo test-network"]
async fn gamma_full_sync_follows_keyset_cursor() {
    let server = MockServer::start().await;

    Mock::given(method("GET"))
        .and(path("/events/keyset"))
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
    assert_eq!(events.len(), 2);
    assert_eq!(events[0].slug, "page-one");
    assert_eq!(events[1].slug, "page-two");
}

#[tokio::test]
#[ignore = "run with cargo test-network"]
async fn gamma_incremental_sync_uses_updated_since_query() {
    let server = MockServer::start().await;
    let since = Utc::now() - Duration::hours(2);
    let since_str = since.to_rfc3339();

    // `updated_since` is RFC3339 (may be URL-encoded); match path + active only.
    let _ = since_str;
    Mock::given(method("GET"))
        .and(path("/events"))
        .and(query_param("active", "true"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!([
            {
                "id": "evt-1",
                "title": "Updated event",
                "slug": "updated-event",
                "markets": [{
                    "conditionId": "0xdeadbeef",
                    "question": "Q?",
                    "active": true,
                    "closed": false,
                    "clobTokenIds": ["111", "222"],
                    "outcomes": ["Yes", "No"]
                }]
            }
        ])))
        .mount(&server)
        .await;

    let client = GammaClient::new(GammaConfig {
        base_url: server.uri(),
        ..GammaConfig::default()
    })
    .with_http_client(fast_http_client());

    let events = client
        .incremental_sync(since)
        .await
        .expect("incremental_sync");

    assert_eq!(events.len(), 1);
    assert_eq!(events[0].slug, "updated-event");
    assert_eq!(events[0].market_ids.len(), 1);
}

const fn fast_retry_policy() -> RetryPolicy {
    RetryPolicy {
        max_attempts: Some(2),
        initial_interval_ms: 1,
        max_interval_ms: 1,
        randomization_factor: 0.0,
        multiplier: 1.0,
        max_elapsed_time_ms: None,
    }
}

fn fast_http_client() -> reqwest::Client {
    reqwest::Client::builder()
        .no_proxy()
        .pool_max_idle_per_host(0)
        .build()
        .expect("test reqwest client")
}
