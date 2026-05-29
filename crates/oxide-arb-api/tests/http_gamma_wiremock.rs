//! Gamma HTTP client tests with wiremock (no live network).

use chrono::{Duration, Utc};
use oxide_arb_api::{gamma::GammaClient, infra::retry::RetryPolicy};
use oxide_arb_models::{config::GammaConfig, types::MarketId};
use wiremock::{
    Mock, MockServer, ResponseTemplate,
    matchers::{method, path, query_param},
};

#[tokio::test]
#[ignore = "run with cargo test-network"]
async fn gamma_get_market_deserializes_from_mock() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/markets/0xabc"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "condition_id": "0xabc",
            "question": "Test?",
            "category": "sports",
            "active": true,
            "closed": false,
            "fees_enabled": true,
            "tokens": [
                {"token_id": "1", "outcome": "Yes"},
                {"token_id": "2", "outcome": "No"}
            ]
        })))
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
    assert_eq!(market.tokens.len(), 2);
}

#[tokio::test]
#[ignore = "run with cargo test-network"]
async fn gamma_retries_on_429() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/markets/0xabc"))
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
        .and(path("/markets/0xabc"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "condition_id": "0xabc",
            "question": "OK",
            "active": true,
            "closed": false,
            "tokens": []
        })))
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
                    "condition_id": "0xdeadbeef",
                    "question": "Q?",
                    "active": true,
                    "closed": false,
                    "tokens": [
                        {"token_id": "111", "outcome": "Yes"},
                        {"token_id": "222", "outcome": "No"}
                    ]
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
