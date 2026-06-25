//! Readiness probe integration tests.

use actix_web::{http::StatusCode, test::TestRequest};

use crate::harness::{self, TestEnv};

#[actix_web::test]
#[ignore = "requires Docker"]
async fn ready_returns_ok_when_postgres_and_redis_are_up() {
    let env = TestEnv::start().await;

    let req = TestRequest::get().uri("/ready");
    let res = harness::call(&env.state, req).await;

    assert_eq!(res.status, StatusCode::OK);
    let body = res.json();
    assert_eq!(body["data"]["status"], "ready");
    let checks = body["data"]["checks"].as_array().expect("checks");
    assert_eq!(checks.len(), 3, "postgresql, redis, catalog");
    let required = ["postgresql", "redis"];
    for name in required {
        let check = checks
            .iter()
            .find(|c| c["name"] == name)
            .unwrap_or_else(|| panic!("missing {name} check"));
        assert_eq!(check["ok"], true);
    }
}

#[actix_web::test]
#[ignore = "requires Docker"]
async fn ready_returns_service_unavailable_when_redis_is_down() {
    let mut env = TestEnv::start().await;
    drop(env.take_redis().expect("redis container"));

    tokio::time::sleep(std::time::Duration::from_millis(200)).await;

    let req = TestRequest::get().uri("/ready");
    let res = harness::call(&env.state, req).await;

    assert_eq!(res.status, StatusCode::SERVICE_UNAVAILABLE);
    let body = res.json();
    assert_eq!(body["data"]["status"], "not_ready");
    let redis = body["data"]["checks"]
        .as_array()
        .expect("checks")
        .iter()
        .find(|check| check["name"] == "redis")
        .expect("redis check");
    assert_eq!(redis["ok"], false);
}
