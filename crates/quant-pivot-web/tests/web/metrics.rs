//! Prometheus `/metrics` integration tests.

use actix_web::{http::StatusCode, test::TestRequest};

use crate::harness;

#[actix_web::test]
#[ignore = "requires Docker"]
async fn metrics_endpoint_returns_prometheus_text() {
    let env = harness::TestEnv::start().await;

    let req = TestRequest::get().uri("/metrics");
    let res = harness::call(&env.state, req).await;

    assert_eq!(res.status, StatusCode::OK);
    let content_type = res
        .header("content-type")
        .expect("content-type header")
        .to_owned();
    assert!(
        content_type.contains("text/plain"),
        "expected prometheus text content-type, got {content_type}"
    );
    let body = String::from_utf8_lossy(res.body_bytes());
    assert!(
        body.contains("quant_pivot_") || body.is_empty(),
        "body should be valid prometheus text exposition"
    );
}
