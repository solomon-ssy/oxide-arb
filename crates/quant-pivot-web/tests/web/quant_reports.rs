//! Phase 04.4 report API integration tests (real Postgres + Redis via Docker).
//!
//! Report rows are seeded into Postgres via [`seed_fixture_published_report`];
//! the HTTP surface reads through real [`CoreQuantReportPort`]. Postgres also
//! backs the RBAC graph so analyst / operator / viewer authorization paths are
//! exercised end-to-end.

use actix_web::{
    http::{StatusCode, header::AUTHORIZATION},
    test::TestRequest,
};
use serde_json::{Value, json};

use quant_pivot_models::{
    runtime_config::{ReportsConfig, RuntimeConfig},
    types::RecommendationReportId,
};
use quant_pivot_test_support::report_pipeline_harness::seed_fixture_published_report;

use crate::{
    client,
    harness::{self, API_VERSION, TestEnv},
};

fn bearer(token: &str) -> (actix_web::http::header::HeaderName, String) {
    (AUTHORIZATION, format!("Bearer {token}"))
}

async fn login(env: &TestEnv, username: &str, password: &str) -> String {
    let req = TestRequest::post()
        .uri("/api/auth/login")
        .insert_header(API_VERSION)
        .set_json(json!({ "username": username, "password": password }));
    let res = harness::call(&env.state, req).await;
    assert_eq!(res.status, StatusCode::OK, "login for {username}");
    res.json()["data"]["access_token"]
        .as_str()
        .expect("access_token")
        .to_owned()
}

async fn get(env: &TestEnv, uri: &str, token: &str) -> harness::Resp {
    let req = TestRequest::get()
        .uri(uri)
        .insert_header(API_VERSION)
        .insert_header(bearer(token));
    harness::call(&env.state, req).await
}

async fn post(
    env: &TestEnv,
    uri: &str,
    token: &str,
    headers: &[(&'static str, &str)],
    body: Value,
) -> harness::Resp {
    let mut req = TestRequest::post()
        .uri(uri)
        .insert_header(API_VERSION)
        .insert_header(bearer(token))
        .set_json(body);
    for (name, value) in headers {
        req = req.insert_header((*name, *value));
    }
    harness::call(&env.state, req).await
}

async fn put(env: &TestEnv, uri: &str, token: &str, body: Value) -> harness::Resp {
    let req = TestRequest::put()
        .uri(uri)
        .insert_header(API_VERSION)
        .insert_header(bearer(token))
        .set_json(body);
    harness::call(&env.state, req).await
}

/// Resolve a built-in role id by code (as admin).
async fn role_id(env: &TestEnv, admin: &str, code: &str) -> String {
    let res = get(env, "/api/roles", admin).await;
    assert_eq!(res.status, StatusCode::OK, "list roles");
    res.json()["data"]
        .as_array()
        .expect("roles array")
        .iter()
        .find(|role| role["code"] == json!(code))
        .unwrap_or_else(|| panic!("seeded role {code} not found"))["id"]
        .as_str()
        .expect("role id")
        .to_owned()
}

/// Create a user holding the seeded role `code`, returning a fresh access token.
async fn user_with_role(env: &TestEnv, admin: &str, username: &str, code: &str) -> String {
    let res = post(
        env,
        "/api/users",
        admin,
        &[],
        json!({ "username": username, "password": "password123", "nickname": username }),
    )
    .await;
    assert_eq!(res.status, StatusCode::OK, "create user {username}");
    let user_id = res.json()["data"]["id"]
        .as_str()
        .expect("user id")
        .to_owned();
    let rid = role_id(env, admin, code).await;
    assert_eq!(
        put(
            env,
            &format!("/api/users/{user_id}/roles"),
            admin,
            json!({ "role_ids": [rid] }),
        )
        .await
        .status,
        StatusCode::OK
    );
    login(env, username, "password123").await
}

/// Activate a runtime config with ad-hoc report generation enabled.
async fn enable_ad_hoc(env: &TestEnv) {
    let cfg = RuntimeConfig {
        reports: ReportsConfig {
            ad_hoc_report_enabled: true,
            ..ReportsConfig::default()
        },
        ..RuntimeConfig::default()
    };
    env.state
        .runtime_config_apply
        .apply(cfg)
        .await
        .expect("apply runtime config");
}

#[actix_web::test]
#[ignore = "requires Docker"]
async fn reports_list_paginated_view() {
    let env = TestEnv::start().await;
    let admin = login(&env, "admin", "admin").await;
    seed_fixture_published_report(
        &env.db,
        RecommendationReportId::from_v7(),
        &env.fixture_report_ctx(),
    )
    .await;

    let res = get(&env, "/api/quant/reports?page=1&size=10", &admin).await;
    assert_eq!(res.status, StatusCode::OK);
    let data = &res.json()["data"];
    assert_eq!(data["total"], json!(1));
    let row = &data["items"][0];
    assert_eq!(row["status"], json!("published"));
    assert_eq!(row["published_recommendation_count"], json!(2));
    assert_eq!(row["total_suggested_usd"], json!("500"));
    // List rows never embed the full summary object.
    assert!(row.get("summary").is_none());
}

#[actix_web::test]
#[ignore = "requires Docker"]
async fn report_detail_recommendations_and_evidence_views() {
    let env = TestEnv::start().await;
    let admin = login(&env, "admin", "admin").await;
    let report_id = RecommendationReportId::from_v7();
    seed_fixture_published_report(&env.db, report_id.clone(), &env.fixture_report_ctx()).await;

    let detail = get(&env, &format!("/api/quant/reports/{report_id}"), &admin).await;
    assert_eq!(detail.status, StatusCode::OK);
    assert_eq!(detail.json()["data"]["account_source"], json!("polymarket"));
    assert!(
        detail.json()["data"]["capital_base_usd"]
            .as_str()
            .is_some_and(|value| value.starts_with("10000")),
        "capital_base_usd: {:?}",
        detail.json()["data"]["capital_base_usd"]
    );
    assert!(detail.json()["data"]["summary"].is_object());

    let recs = get(
        &env,
        &format!("/api/quant/reports/{report_id}/recommendations"),
        &admin,
    )
    .await;
    assert_eq!(recs.status, StatusCode::OK);
    let items = recs.json()["data"]
        .as_array()
        .expect("recommendations")
        .clone();
    assert_eq!(items.len(), 2);
    assert!(items[0]["entry_plan"].is_object());
    assert!(items[0]["sizing_plan"].is_object());
    assert!(items[0]["exit_plan"].is_object());
    assert!(items[0]["execution_eligibility"].is_object());
    let rec_id = items[0]["recommendation_id"]
        .as_str()
        .expect("rec id")
        .to_owned();

    let one = get(
        &env,
        &format!("/api/quant/recommendations/{rec_id}"),
        &admin,
    )
    .await;
    assert_eq!(one.status, StatusCode::OK);

    let evidence = get(
        &env,
        &format!("/api/quant/recommendations/{rec_id}/evidence"),
        &admin,
    )
    .await;
    assert_eq!(evidence.status, StatusCode::OK);
    assert!(evidence.json()["data"]["signal_candidate_id"].is_string());
    assert!(evidence.json()["data"]["model_run_id"].is_string());
}

#[actix_web::test]
#[ignore = "requires Docker"]
async fn latest_returns_most_recent_published() {
    let env = TestEnv::start().await;
    let admin = login(&env, "admin", "admin").await;
    let report_id = RecommendationReportId::from_v7();
    seed_fixture_published_report(&env.db, report_id.clone(), &env.fixture_report_ctx()).await;

    let res = get(&env, "/api/quant/reports/latest", &admin).await;
    assert_eq!(res.status, StatusCode::OK);
    assert_eq!(
        res.json()["data"]["recommendation_report_id"],
        json!(report_id.to_string())
    );
}

#[actix_web::test]
#[ignore = "requires Docker"]
async fn report_diff_view_shape() {
    let env = TestEnv::start().await;
    let admin = login(&env, "admin", "admin").await;
    let base = RecommendationReportId::from_v7();
    seed_fixture_published_report(&env.db, base.clone(), &env.fixture_report_ctx()).await;
    let compare = RecommendationReportId::from_v7();
    seed_fixture_published_report(&env.db, compare.clone(), &env.fixture_report_ctx()).await;

    let res = get(
        &env,
        &format!("/api/quant/reports/{base}/diff/{compare}"),
        &admin,
    )
    .await;
    assert_eq!(res.status, StatusCode::OK);
    let data = &res.json()["data"];
    // Distinct fixture ids per report → markets differ → all added/removed.
    assert!(data["added"].is_array());
    assert!(data["removed"].is_array());
    assert!(data["retained"].is_array());
    assert!(data["base_eligibility"].is_object());
    assert!(data["total_suggested_usd_delta"].is_string());
}

#[actix_web::test]
#[ignore = "requires Docker"]
async fn run_report_gated_by_ad_hoc_enabled_and_rbac() {
    let env = TestEnv::start().await;
    let admin = login(&env, "admin", "admin").await;

    // Disabled by default → 409 even for super_admin.
    let disabled = post(
        &env,
        "/api/quant/reports/run",
        &admin,
        &[("X-Request-Id", "req-run-1")],
        json!({ "request_id": "adhoc-1", "reason": "manual refresh" }),
    )
    .await;
    assert_eq!(disabled.status, StatusCode::CONFLICT);

    enable_ad_hoc(&env).await;

    // analyst may enqueue → 202 with correlation handles.
    let analyst = user_with_role(&env, &admin, "analyst1", "analyst").await;
    let accepted = post(
        &env,
        "/api/quant/reports/run",
        &analyst,
        &[("X-Acting-Role", "analyst"), ("X-Request-Id", "req-run-2")],
        json!({ "request_id": "adhoc-2", "reason": "analyst refresh" }),
    )
    .await;
    assert_eq!(accepted.status, StatusCode::ACCEPTED);
    assert_eq!(accepted.json()["code"], json!(202));
    assert_eq!(
        accepted.json()["data"]["trigger_key"],
        json!("ad_hoc:adhoc-2")
    );
    {
        let enqueued = env.ad_hoc_enqueued.lock().unwrap();
        assert_eq!(enqueued.len(), 1);
        assert_eq!(enqueued[0].request_id, "adhoc-2");
        drop(enqueued);
    }

    // viewer may not enqueue → 403.
    let viewer = user_with_role(&env, &admin, "viewer1", "viewer").await;
    let denied = post(
        &env,
        "/api/quant/reports/run",
        &viewer,
        &[("X-Acting-Role", "viewer"), ("X-Request-Id", "req-run-3")],
        json!({ "request_id": "adhoc-3", "reason": "viewer refresh" }),
    )
    .await;
    assert_eq!(denied.status, StatusCode::FORBIDDEN);
}

#[actix_web::test]
#[ignore = "requires Docker"]
async fn revoke_writes_oplog_and_returns_detail() {
    let env = TestEnv::start().await;
    let admin = login(&env, "admin", "admin").await;
    let report_id = RecommendationReportId::from_v7();
    seed_fixture_published_report(&env.db, report_id.clone(), &env.fixture_report_ctx()).await;

    let res = post(
        &env,
        &format!("/api/quant/reports/{report_id}/revoke"),
        &admin,
        &[
            ("X-Acting-Role", "super_admin"),
            ("X-Request-Id", "req-revoke-1"),
        ],
        json!({ "reason": "stale recommendation" }),
    )
    .await;
    assert_eq!(res.status, StatusCode::OK);
    assert_eq!(res.json()["data"]["status"], json!("revoked"));

    let detail = get(&env, &format!("/api/quant/reports/{report_id}"), &admin).await;
    assert_eq!(detail.json()["data"]["status"], json!("revoked"));

    let logs = client::wait_for_oplog(&env, &admin, "req-revoke-1").await;
    assert!(
        logs.iter()
            .any(|row| row["action"] == json!("quant.report.revoke")
                && row["category"] == json!("quant_report")),
        "revoke must write a quant_report operation log: {logs:?}"
    );
}

#[actix_web::test]
#[ignore = "requires Docker"]
async fn revoke_forbidden_for_analyst() {
    let env = TestEnv::start().await;
    let admin = login(&env, "admin", "admin").await;
    let report_id = RecommendationReportId::from_v7();
    seed_fixture_published_report(&env.db, report_id.clone(), &env.fixture_report_ctx()).await;
    let analyst = user_with_role(&env, &admin, "analyst2", "analyst").await;

    let res = post(
        &env,
        &format!("/api/quant/reports/{report_id}/revoke"),
        &analyst,
        &[
            ("X-Acting-Role", "analyst"),
            ("X-Request-Id", "req-revoke-2"),
        ],
        json!({ "reason": "should be denied" }),
    )
    .await;
    assert_eq!(res.status, StatusCode::FORBIDDEN);
}

#[actix_web::test]
#[ignore = "requires Docker"]
async fn create_intent_returns_501_phase5() {
    let env = TestEnv::start().await;
    let admin = login(&env, "admin", "admin").await;
    let report_id = RecommendationReportId::from_v7();
    seed_fixture_published_report(&env.db, report_id.clone(), &env.fixture_report_ctx()).await;
    let recs = get(
        &env,
        &format!("/api/quant/reports/{report_id}/recommendations"),
        &admin,
    )
    .await;
    let rec_id = recs.json()["data"][0]["recommendation_id"]
        .as_str()
        .expect("rec id")
        .to_owned();

    let res = post(
        &env,
        &format!("/api/quant/recommendations/{rec_id}/create-intent"),
        &admin,
        &[("X-Request-Id", "req-intent-1")],
        json!({}),
    )
    .await;
    assert_eq!(res.status, StatusCode::NOT_IMPLEMENTED);
    assert_eq!(res.json()["code"], json!(501));
}
