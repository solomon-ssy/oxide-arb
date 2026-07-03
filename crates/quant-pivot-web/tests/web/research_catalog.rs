//! Research catalog read API tests (datasets / models / model-specs / backtests
//! / comparisons / factors). The harness wires a no-op catalog port returning
//! empty pages, so these exercise route registration, RBAC wiring, and query
//! deserialization rather than repository behavior (covered by pg tests).

use actix_web::{
    http::{StatusCode, header::AUTHORIZATION},
    test::TestRequest,
};
use quant_pivot_models::types::FactorDefinitionId;
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

async fn assert_empty_page(env: &TestEnv, admin: &str, uri: &str) {
    let req = TestRequest::get()
        .uri(uri)
        .insert_header(API_VERSION)
        .insert_header(bearer(admin));
    let res = harness::call(&env.state, req).await;
    assert_eq!(res.status, StatusCode::OK, "GET {uri}");
    assert_eq!(res.json()["data"]["total"], json!(0), "{uri} total");
    assert_eq!(res.json()["data"]["items"], json!([]), "{uri} items");
}

#[tokio::test]
async fn research_catalogs_return_paginated_envelopes() {
    let env = TestEnv::start().await;
    let admin = admin_token(&env).await;

    assert_empty_page(
        &env,
        &admin,
        "/api/research/training-datasets?page=1&size=10",
    )
    .await;
    assert_empty_page(&env, &admin, "/api/research/models?page=1&size=10").await;
    assert_empty_page(&env, &admin, "/api/research/model-specs?page=1&size=10").await;
    assert_empty_page(
        &env,
        &admin,
        "/api/research/backtest-reports?page=1&size=10",
    )
    .await;
    assert_empty_page(
        &env,
        &admin,
        "/api/research/comparison-reports?page=1&size=10",
    )
    .await;
    assert_empty_page(&env, &admin, "/api/research/factors?page=1&size=10").await;
}

#[tokio::test]
async fn research_catalog_filters_deserialize() {
    let env = TestEnv::start().await;
    let admin = admin_token(&env).await;

    // Enum + pagination filters must deserialize from the query string.
    assert_empty_page(
        &env,
        &admin,
        "/api/research/training-datasets?status=ready&page=1&size=5",
    )
    .await;
    assert_empty_page(
        &env,
        &admin,
        "/api/research/models?publication_status=published&page=1&size=5",
    )
    .await;
    assert_empty_page(
        &env,
        &admin,
        "/api/research/factors?factor_family=liquidity&status=published",
    )
    .await;
}

#[tokio::test]
async fn factor_detail_not_found_returns_404() {
    let env = TestEnv::start().await;
    let admin = admin_token(&env).await;
    let id = FactorDefinitionId::from_v7();
    let req = TestRequest::get()
        .uri(&format!("/api/research/factors/{id}"))
        .insert_header(API_VERSION)
        .insert_header(bearer(&admin));
    let res = harness::call(&env.state, req).await;
    assert_eq!(res.status, StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn training_dataset_plan_and_build_match_post_handlers_not_id_route() {
    let env = TestEnv::start().await;
    let admin = admin_token(&env).await;
    let body = json!({
        "model_spec_id": "019f2964-d6e3-7711-aa6c-1dbb87257e9e",
        "runtime_config_version_id": "019f2964-d5f4-7740-aa85-9a65e487aab2",
        "window_start": "2026-07-02T00:00:00.000+08:00",
        "window_end": "2026-07-05T23:59:59.000+08:00",
        "sample_interval_secs": 60,
        "horizons_secs": [3600],
        "source_delay_secs": 1,
        "reason": "route registration regression",
    });

    for path in [
        "/api/research/training-datasets/plan",
        "/api/research/training-datasets/build",
    ] {
        let req = TestRequest::post()
            .uri(path)
            .insert_header(API_VERSION)
            .insert_header(bearer(&admin))
            .insert_header(("X-Acting-Role", "quant"))
            .set_json(&body);
        let res = harness::call(&env.state, req).await;
        assert_ne!(
            res.status,
            StatusCode::METHOD_NOT_ALLOWED,
            "POST {path} must not match GET-only {{id}} route"
        );
        assert_eq!(
            res.status,
            StatusCode::NOT_IMPLEMENTED,
            "POST {path} should reach the handler (mock port)"
        );
    }
}

#[tokio::test]
async fn research_catalog_requires_authentication() {
    let env = TestEnv::start().await;
    let req = TestRequest::get()
        .uri("/api/research/training-datasets")
        .insert_header(API_VERSION);
    let res = harness::call(&env.state, req).await;
    assert_eq!(res.status, StatusCode::UNAUTHORIZED);
}
