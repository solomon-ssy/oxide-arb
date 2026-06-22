//! CLOB-shaped HTTP 429 retry: wiremock HTTP → domain error → retry policy.

use oxide_arb_api::infra::retry::{RetryPolicy, retry_with_policy};
use oxide_arb_error::api::ApiError;
use oxide_arb_models::enums::common::OrderType;
use reqwest::StatusCode;
use std::sync::{
    Arc,
    atomic::{AtomicU32, Ordering},
};
use wiremock::{
    Mock, MockServer, ResponseTemplate,
    matchers::{method, path},
};

#[tokio::test]
#[ignore = "run with cargo test-network"]
async fn clob_post_order_429_retries_over_http() {
    let server = MockServer::start().await;

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
        .respond_with(ResponseTemplate::new(200).set_body_string(r#"{"success":true}"#))
        .mount(&server)
        .await;

    let attempts = Arc::new(AtomicU32::new(0));
    let policy = fast_retry_policy();
    let url = format!("{}/order", server.uri());
    let http = fast_http_client();

    let result = retry_with_policy(&policy, || {
        let url = url.clone();
        let http = http.clone();
        let attempts = Arc::clone(&attempts);
        async move {
            let n = attempts.fetch_add(1, Ordering::SeqCst);
            let resp = http.post(&url).send().await.expect("wiremock request");

            if resp.status() == StatusCode::TOO_MANY_REQUESTS {
                return Err(ApiError::RateLimited {
                    retry_after_ms: 1,
                    bucket: "POST /order".into(),
                });
            }

            if !resp.status().is_success() {
                return Err(ApiError::Http {
                    method: "POST",
                    url: url.clone(),
                    status: resp.status().as_u16(),
                    body: resp.text().await.unwrap_or_default(),
                    retryable: false,
                });
            }

            assert_eq!(n, 1, "expected one retry after initial 429");
            Ok(resp.text().await.unwrap_or_default())
        }
    })
    .await;

    assert!(result.unwrap().contains("success"));
    assert!(attempts.load(Ordering::SeqCst) >= 2);
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

#[test]
fn fok_orders_use_no_retry_policy() {
    let policy = RetryPolicy::for_order_type(OrderType::Fok);
    assert_eq!(policy.max_attempts, Some(0));

    let gtc = RetryPolicy::for_order_type(OrderType::Gtc);
    assert_eq!(gtc.max_attempts, Some(3));
}
