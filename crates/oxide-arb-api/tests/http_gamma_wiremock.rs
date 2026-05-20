//! Gamma HTTP client tests with wiremock (no live network).

use oxide_arb_api::gamma::GammaClient;
use oxide_arb_models::config::GammaConfig;
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

#[tokio::test]
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
    });

    let market = client
        .get_market(&oxide_arb_models::types::MarketId::new("0xabc"))
        .await
        .expect("get_market");

    assert_eq!(market.market_id.as_str(), "0xabc");
    assert_eq!(market.tokens.len(), 2);
}

#[tokio::test]
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
    });

    let result = client
        .get_market(&oxide_arb_models::types::MarketId::new("0xabc"))
        .await;

    assert!(result.is_ok(), "expected retry success: {result:?}");
}
