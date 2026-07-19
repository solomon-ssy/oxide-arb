//! Research catalog read API tests (datasets / models / model-specs / backtests
//! / comparisons / factors). The harness wires a no-op catalog port returning
//! empty pages, so these exercise route registration, RBAC wiring, and query
//! deserialization rather than repository behavior (covered by pg tests).

use actix_web::{
    http::{StatusCode, header::AUTHORIZATION},
    test::TestRequest,
};
use quant_pivot_models::types::{FactorDefinitionId, FeatureParityRunId, TradePolicyArtifactId};
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
async fn feature_integrity_capture_and_data_quality_filters_reach_the_handler() {
    let env = TestEnv::start().await;
    let admin = admin_token(&env).await;

    for stage in ["capture", "data_quality"] {
        let req = TestRequest::get()
            .uri(&format!(
                "/api/research/feature-integrity/events?stage={stage}&page=1&size=5"
            ))
            .insert_header(API_VERSION)
            .insert_header(bearer(&admin));
        let res = harness::call(&env.state, req).await;
        assert_eq!(
            res.status,
            StatusCode::OK,
            "stage `{stage}` must deserialize and reach the feature-integrity port"
        );
        assert_eq!(res.json()["data"]["items"], json!([]));
    }
}

#[tokio::test]
async fn feature_integrity_summary_runs_and_governed_actions_reach_their_handlers() {
    let env = TestEnv::start().await;
    let admin = admin_token(&env).await;

    for uri in [
        "/api/research/feature-integrity/summary",
        "/api/research/feature-integrity/runs?kind=full&status=passed&page=1&size=5",
    ] {
        let req = TestRequest::get()
            .uri(uri)
            .insert_header(API_VERSION)
            .insert_header(bearer(&admin));
        let res = harness::call(&env.state, req).await;
        assert_eq!(
            res.status,
            StatusCode::NOT_IMPLEMENTED,
            "GET {uri} must deserialize and reach the feature-integrity port"
        );
    }

    let full = TestRequest::post()
        .uri("/api/research/feature-integrity/runs/full")
        .insert_header(API_VERSION)
        .insert_header(bearer(&admin))
        .insert_header(("X-Acting-Role", "super_admin"))
        .insert_header(("X-Request-Id", "feature-parity-full-1"))
        .set_json(json!({
            "window_start": "2026-07-11T00:00:00Z",
            "window_end": "2026-07-12T00:00:00Z",
            "reason": "pre-publish deterministic full replay"
        }));
    let full = harness::call(&env.state, full).await;
    assert_eq!(
        full.status,
        StatusCode::NOT_IMPLEMENTED,
        "a governed full-run request must reach the feature-integrity port"
    );

    let parity_run_id = FeatureParityRunId::from_v7();
    let acknowledge = TestRequest::post()
        .uri("/api/research/feature-integrity/latch/acknowledge")
        .insert_header(API_VERSION)
        .insert_header(bearer(&admin))
        .insert_header(("X-Acting-Role", "super_admin"))
        .insert_header(("X-Request-Id", "feature-parity-ack-1"))
        .set_json(json!({
            "parity_run_id": parity_run_id,
            "reason": "causal recovery proof reviewed by risk owner"
        }));
    let acknowledge = harness::call(&env.state, acknowledge).await;
    assert_eq!(
        acknowledge.status,
        StatusCode::NOT_IMPLEMENTED,
        "a governed latch acknowledgement must reach the feature-integrity port"
    );
}

#[tokio::test]
async fn feature_contract_returns_hash_bound_active_catalog() {
    let env = TestEnv::start().await;
    let admin = admin_token(&env).await;
    let req = TestRequest::get()
        .uri("/api/research/feature-contract")
        .insert_header(API_VERSION)
        .insert_header(bearer(&admin));
    let res = harness::call(&env.state, req).await;

    assert_eq!(res.status, StatusCode::OK);
    assert_eq!(res.json()["data"]["feature_schema_version"], json!(6));
    assert_eq!(res.json()["data"]["features"][0]["name"], json!("book.mid"));
    assert_eq!(
        res.json()["data"]["features"][0]["value_kind"],
        json!("probability")
    );
    assert_eq!(
        res.json()["data"]["features"][0]["compute_revision"],
        json!(1)
    );
    assert!(
        res.json()["data"]["feature_schema_hash"]
            .as_str()
            .is_some_and(|hash| hash.starts_with("blake3:"))
    );
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
async fn trade_policy_source_slice_object_query_reaches_the_typed_handler() {
    let env = TestEnv::start().await;
    let admin = admin_token(&env).await;
    let id = TradePolicyArtifactId::from_v7();
    let req = TestRequest::get()
        .uri(&format!(
            "/api/research/trade-policies/{id}/source-slice/objects?kind=l2_event&page=1&size=100"
        ))
        .insert_header(API_VERSION)
        .insert_header(bearer(&admin));
    let res = harness::call(&env.state, req).await;
    assert_eq!(res.status, StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn training_dataset_plan_rejects_inverted_window() {
    let env = TestEnv::start().await;
    let admin = admin_token(&env).await;
    let req = TestRequest::post()
        .uri("/api/research/training-datasets/plan")
        .insert_header(API_VERSION)
        .insert_header(bearer(&admin))
        .insert_header(("X-Acting-Role", "quant"))
        .set_json(json!({
            "model_spec_id": "019f2964-d6e3-7711-aa6c-1dbb87257e9e",
            "profile_ref": {
                "id": "pooled_1h_control",
                "version": 1,
                "content_hash": format!("blake3:{}", "1".repeat(64)),
            },
            "decision_policy_snapshot_id": "019f2964-d5f4-7740-aa85-9a65e487aab2",
            "window_start": "2026-07-05T00:00:00.000Z",
            "window_end": "2026-07-02T00:00:00.000Z",
            "pit_cutoff": "2026-07-06T00:00:00.000Z",
            "sample_interval_secs": 60,
            "horizons_secs": [3600],
            "knowledge_lag_secs": 1,
            "reason": "inverted window validation"
        }));
    let res = harness::call(&env.state, req).await;
    assert_eq!(res.status, StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn training_dataset_plan_and_build_match_post_handlers_not_id_route() {
    let env = TestEnv::start().await;
    let admin = admin_token(&env).await;
    let body = json!({
        "model_spec_id": "019f2964-d6e3-7711-aa6c-1dbb87257e9e",
        "profile_ref": {
            "id": "pooled_1h_control",
            "version": 1,
            "content_hash": format!("blake3:{}", "1".repeat(64)),
        },
        "decision_policy_snapshot_id": "019f2964-d5f4-7740-aa85-9a65e487aab2",
        "window_start": "2026-07-02T00:00:00.000+08:00",
        "window_end": "2026-07-05T23:59:59.000+08:00",
        "pit_cutoff": "2026-07-07T00:00:00.000+08:00",
        "sample_interval_secs": 60,
        "horizons_secs": [3600],
        "knowledge_lag_secs": 1,
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

    let req = TestRequest::get()
        .uri("/api/research/feature-contract")
        .insert_header(API_VERSION);
    let res = harness::call(&env.state, req).await;
    assert_eq!(res.status, StatusCode::UNAUTHORIZED);

    let req = TestRequest::get()
        .uri("/api/research/feature-integrity/summary")
        .insert_header(API_VERSION);
    let res = harness::call(&env.state, req).await;
    assert_eq!(res.status, StatusCode::UNAUTHORIZED);

    for uri in [
        "/api/research/feature-integrity/runs/full",
        "/api/research/feature-integrity/latch/acknowledge",
    ] {
        let req = TestRequest::post()
            .uri(uri)
            .insert_header(API_VERSION)
            .insert_header(("X-Acting-Role", "risk_owner"))
            .set_json(json!({ "reason": "unauthenticated governed action" }));
        let res = harness::call(&env.state, req).await;
        assert_eq!(res.status, StatusCode::UNAUTHORIZED, "POST {uri}");
    }
}
