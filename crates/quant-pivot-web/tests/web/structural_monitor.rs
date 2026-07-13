//! Neg-risk structural-drift monitor route tests (Phase 11.2.1).

use actix_web::{
    http::{StatusCode, header::AUTHORIZATION},
    test::TestRequest,
};
use serde_json::json;

use crate::harness::{self, API_VERSION, TestEnv};

fn bearer(token: &str) -> (actix_web::http::header::HeaderName, String) {
    (AUTHORIZATION, format!("Bearer {token}"))
}

async fn admin_token(env: &TestEnv) -> String {
    let req = TestRequest::post()
        .uri("/api/auth/login")
        .insert_header(API_VERSION)
        .set_json(json!({ "username": "admin", "password": "admin" }));
    let res = harness::call(&env.state, req).await;
    assert_eq!(res.status, StatusCode::OK);
    res.json()["data"]["access_token"]
        .as_str()
        .expect("access_token")
        .to_owned()
}

#[tokio::test]
async fn negrisk_events_returns_ok_envelope() {
    let env = TestEnv::start().await;
    let admin = admin_token(&env).await;
    let req = TestRequest::get()
        .uri("/api/quant/structural/negrisk-events")
        .insert_header(API_VERSION)
        .insert_header(bearer(&admin));
    let res = harness::call(&env.state, req).await;
    assert_eq!(res.status, StatusCode::OK);
    assert_eq!(res.json()["data"], json!([]));
}

#[tokio::test]
async fn trade_tape_coverage_returns_ok_envelope() {
    let env = TestEnv::start().await;
    let admin = admin_token(&env).await;
    let req = TestRequest::get()
        .uri("/api/quant/structural/trade-tape/coverage")
        .insert_header(API_VERSION)
        .insert_header(bearer(&admin));
    let res = harness::call(&env.state, req).await;
    assert_eq!(res.status, StatusCode::OK);
    let body = res.json();
    assert!(body["data"]["decision_at"].is_string());
    assert!(body["data"]["knowledge_cutoff"].is_string());
    assert!(body["data"]["source_health"].is_array());
}

#[tokio::test]
async fn participant_concentration_returns_ok_envelope() {
    let env = TestEnv::start().await;
    let admin = admin_token(&env).await;
    let req = TestRequest::get()
        .uri("/api/quant/structural/participant-concentration")
        .insert_header(API_VERSION)
        .insert_header(bearer(&admin));
    let res = harness::call(&env.state, req).await;
    assert_eq!(res.status, StatusCode::OK);
    let body = res.json();
    assert!(body["data"]["decision_at"].is_string());
    assert!(body["data"]["knowledge_cutoff"].is_string());
    assert!(body["data"]["markets"].is_array());
}
