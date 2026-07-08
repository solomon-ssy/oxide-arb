//! Unified calibration-artifact admin route tests (Phase 11.3).
//!
//! Exercises route registration, RBAC wiring, request validation, and the
//! paginated envelope — repository behavior is covered by pg tests.

use actix_web::{
    http::{StatusCode, header::AUTHORIZATION},
    test::TestRequest,
};
use quant_pivot_models::types::{CalibrationArtifactId, ModelVersionId, TrainingDatasetId};
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
async fn calibration_artifacts_list_returns_paginated_envelope() {
    let env = TestEnv::start().await;
    let admin = admin_token(&env).await;
    let req = TestRequest::get()
        .uri("/api/research/calibration-artifacts?page=1&size=10")
        .insert_header(API_VERSION)
        .insert_header(bearer(&admin));
    let res = harness::call(&env.state, req).await;
    assert_eq!(res.status, StatusCode::OK);
    assert_eq!(res.json()["data"]["total"], json!(0));
    assert_eq!(res.json()["data"]["items"], json!([]));
}

#[tokio::test]
async fn calibration_artifact_detail_not_found_returns_404() {
    let env = TestEnv::start().await;
    let admin = admin_token(&env).await;
    let id = CalibrationArtifactId::from_v7();
    let req = TestRequest::get()
        .uri(&format!("/api/research/calibration-artifacts/{id}"))
        .insert_header(API_VERSION)
        .insert_header(bearer(&admin));
    let res = harness::call(&env.state, req).await;
    assert_eq!(res.status, StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn calibration_fit_preflight_requires_dataset_id_query_param() {
    let env = TestEnv::start().await;
    let admin = admin_token(&env).await;
    let model_version_id = ModelVersionId::from_v7();
    let req = TestRequest::get()
        .uri(&format!(
            "/api/research/models/{model_version_id}/calibration-fit-preflight"
        ))
        .insert_header(API_VERSION)
        .insert_header(bearer(&admin));
    let res = harness::call(&env.state, req).await;
    assert_eq!(
        res.status,
        StatusCode::BAD_REQUEST,
        "calibration_dataset_id is a required query param"
    );
}

#[tokio::test]
async fn calibration_fit_preflight_route_is_wired_to_the_service() {
    let env = TestEnv::start().await;
    let admin = admin_token(&env).await;
    let model_version_id = ModelVersionId::from_v7();
    let dataset_id = TrainingDatasetId::from_v7();
    let req = TestRequest::get()
        .uri(&format!(
            "/api/research/models/{model_version_id}/calibration-fit-preflight?calibration_dataset_id={dataset_id}"
        ))
        .insert_header(API_VERSION)
        .insert_header(bearer(&admin));
    let res = harness::call(&env.state, req).await;
    // The harness wires a mock `ModelCalibrationFitPort` that always returns
    // `NotImplemented` — this test only asserts the route/query wiring
    // reaches the port (501), never a 404 (unregistered route) or 400
    // (query deserialization failure), proving the endpoint is correctly
    // registered end to end.
    assert_eq!(res.status, StatusCode::NOT_IMPLEMENTED);
}

#[tokio::test]
async fn calibration_artifact_fit_bias_table_rejects_inverted_window() {
    let env = TestEnv::start().await;
    let admin = admin_token(&env).await;
    let req = TestRequest::post()
        .uri("/api/research/calibration-artifacts/fit-bias-table")
        .insert_header(API_VERSION)
        .insert_header(bearer(&admin))
        .set_json(json!({
            "window_start": "2026-07-05T00:00:00.000Z",
            "window_end": "2026-07-02T00:00:00.000Z",
            "reason": "integration test inverted window"
        }));
    let res = harness::call(&env.state, req).await;
    assert_eq!(
        res.status,
        StatusCode::BAD_REQUEST,
        "inverted fit window must fail validation before enqueue"
    );
}
