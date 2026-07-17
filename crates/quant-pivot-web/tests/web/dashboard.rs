//! Dashboard overview aggregate integration tests.

use actix_web::{
    http::{StatusCode, header::AUTHORIZATION},
    test::TestRequest,
};
use serde_json::json;

use crate::{
    client,
    harness::{self, API_VERSION, TestEnv},
};

fn bearer(token: &str) -> (actix_web::http::header::HeaderName, String) {
    (AUTHORIZATION, format!("Bearer {token}"))
}

async fn admin_token(env: &TestEnv) -> String {
    let request = TestRequest::post()
        .uri("/api/auth/login")
        .insert_header(API_VERSION)
        .set_json(json!({ "username": "admin", "password": "admin" }));
    let response = harness::call(&env.state, request).await;
    assert_eq!(response.status, StatusCode::OK);
    response.json()["data"]["access_token"]
        .as_str()
        .expect("access token")
        .to_owned()
}

#[tokio::test]
async fn overview_is_private_single_revision_and_section_tagged() {
    let env = TestEnv::start().await;
    let token = admin_token(&env).await;
    let request = TestRequest::get()
        .uri("/api/dashboard/overview?window=24h")
        .insert_header(API_VERSION)
        .insert_header(bearer(&token));
    let response = harness::call(&env.state, request).await;
    assert_eq!(response.status, StatusCode::OK);
    assert_eq!(response.header("cache-control"), Some("private, no-store"));
    let payload = response.json();
    let data = &payload["data"];
    assert!(data["revision"].is_string());
    assert_eq!(data["window"], "24h");
    for section in [
        "authority",
        "account",
        "equity_curve",
        "latest_report",
        "report_lifecycle",
        "exposures",
        "data_quality",
        "research_readiness",
        "subsystem_health",
        "action_inbox",
    ] {
        assert!(
            data[section]["state"].is_string(),
            "missing state for {section}"
        );
    }
}

#[tokio::test]
async fn overview_rejects_unknown_window() {
    let env = TestEnv::start().await;
    let token = admin_token(&env).await;
    let request = TestRequest::get()
        .uri("/api/dashboard/overview?window=365d")
        .insert_header(API_VERSION)
        .insert_header(bearer(&token));
    let response = harness::call(&env.state, request).await;
    assert_eq!(response.status, StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn sections_are_server_cropped_for_authenticated_user_without_roles() {
    let env = TestEnv::start().await;
    let admin = admin_token(&env).await;
    let created = client::post(
        &env,
        "/api/users",
        &admin,
        json!({
            "username": "dashboard-no-role",
            "password": "password123",
            "nickname": "Dashboard No Role"
        }),
    )
    .await;
    assert_eq!(created.status, StatusCode::OK);
    let token = client::login(&env, "dashboard-no-role", "password123").await;
    let response = client::get(&env, "/api/dashboard/overview", &token).await;
    assert_eq!(response.status, StatusCode::OK);
    let data = &response.json()["data"];
    for section in [
        "authority",
        "account",
        "equity_curve",
        "latest_report",
        "report_lifecycle",
        "exposures",
        "data_quality",
        "research_readiness",
        "subsystem_health",
        "action_inbox",
    ] {
        assert_eq!(data[section]["state"], "forbidden", "section {section}");
    }
}
