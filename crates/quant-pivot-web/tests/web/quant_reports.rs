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
use chrono::Utc;
use serde_json::{Value, json};

use quant_pivot_models::{
    enums::quant::{RecommendationReportStatus, ReportFactDeliveryStatus, ReportRunTerminalReason},
    runtime_config::{ReportsConfig, RuntimeConfig},
    types::{POOLED_1H_CONTROL_PROFILE_ID, RecommendationReportId, builtin_research_profiles},
};
use quant_pivot_repository::{
    postgres::PgRecommendationReportRepository,
    traits::{RecommendationReportRepository, ReportRunRepository},
};
use quant_pivot_test_support::report_pipeline_harness::{
    seed_fixture_prepared_report, seed_fixture_published_report,
    seed_fixture_published_report_with_profile,
};
use uuid::Uuid;

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

    // runtime_mode filter narrows the result set (fixture runs report_only).
    let matched = get(
        &env,
        "/api/quant/reports?page=1&size=10&runtime_mode=report_only",
        &admin,
    )
    .await;
    assert_eq!(matched.status, StatusCode::OK);
    assert_eq!(matched.json()["data"]["total"], json!(1));

    let filtered = get(
        &env,
        "/api/quant/reports?page=1&size=10&runtime_mode=auto_execution",
        &admin,
    )
    .await;
    assert_eq!(filtered.status, StatusCode::OK);
    assert_eq!(filtered.json()["data"]["total"], json!(0));
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

    let diagnostics = get(
        &env,
        &format!("/api/quant/reports/{report_id}/diagnostics"),
        &admin,
    )
    .await;
    assert_eq!(diagnostics.status, StatusCode::OK);
    assert_eq!(
        diagnostics.json()["data"]["evidence_complete"],
        json!(false)
    );
    assert_eq!(diagnostics.json()["data"]["subject"], json!("model_run"));
    assert_eq!(
        diagnostics.json()["data"]["stage_ceiling"],
        json!("prediction")
    );
    assert!(diagnostics.json()["data"]["decision_boundary"].is_object());
    assert_eq!(
        diagnostics.json()["data"]["feature_state_counts"],
        Value::Null
    );
    assert_eq!(
        diagnostics.json()["data"]["feature_cell_count"],
        Value::Null
    );
    assert_eq!(diagnostics.json()["data"]["model_input_count"], Value::Null);

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
    // Enriched governance facts: parent report status + blocking intent id.
    assert_eq!(items[0]["report_status"], json!("published"));
    assert_eq!(items[0]["active_order_intent_id"], Value::Null);
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
    assert_eq!(one.json()["data"]["report_status"], json!("published"));
    assert_eq!(one.json()["data"]["active_order_intent_id"], Value::Null);

    let evidence = get(
        &env,
        &format!("/api/quant/recommendations/{rec_id}/evidence"),
        &admin,
    )
    .await;
    assert_eq!(evidence.status, StatusCode::OK);
    assert!(evidence.json()["data"]["signal_candidate_id"].is_string());
    assert!(evidence.json()["data"]["model_run_id"].is_string());
    assert_eq!(evidence.json()["data"]["evidence_complete"], json!(false));
    assert!(evidence.json()["data"]["feature_cells"].is_array());
    assert!(evidence.json()["data"]["model_inputs"].is_array());
}

#[actix_web::test]
#[ignore = "requires Docker"]
async fn current_returns_published_report_in_exact_scope() {
    let env = TestEnv::start().await;
    let admin = login(&env, "admin", "admin").await;
    let report_id = RecommendationReportId::from_v7();
    let report =
        seed_fixture_published_report(&env.db, report_id.clone(), &env.fixture_report_ctx()).await;

    let res = get(
        &env,
        &format!(
            "/api/quant/reports/current?profile_id={}&kind=top_n",
            report.profile_id
        ),
        &admin,
    )
    .await;
    assert_eq!(res.status, StatusCode::OK);
    assert_eq!(
        res.json()["data"]["recommendation_report_id"],
        json!(report_id.to_string())
    );
}

#[actix_web::test]
#[ignore = "requires Docker"]
async fn legacy_latest_route_is_absent() {
    let env = TestEnv::start().await;
    let admin = login(&env, "admin", "admin").await;

    let res = get(&env, "/api/quant/reports/latest", &admin).await;
    assert_eq!(res.status, StatusCode::NOT_FOUND);
}

#[actix_web::test]
#[ignore = "requires Docker"]
async fn cross_scope_report_diff_is_bad_request() {
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
    let retained = data["retained"].as_array().expect("retained deltas");
    assert_eq!(retained.len(), 2);
    assert!(retained[0]["base"].is_object());
    assert!(retained[0]["compare"].is_object());
    assert!(retained[0]["changed_fields"].is_array());

    let other_profile = builtin_research_profiles()
        .expect("built-in profiles")
        .into_iter()
        .find(|profile| profile.profile_ref.id == POOLED_1H_CONTROL_PROFILE_ID)
        .expect("pooled control profile")
        .profile_ref;
    let other_scope = RecommendationReportId::from_v7();
    seed_fixture_published_report_with_profile(
        &env.db,
        other_scope.clone(),
        &env.fixture_report_ctx(),
        other_profile,
    )
    .await;
    let rejected = get(
        &env,
        &format!("/api/quant/reports/{base}/diff/{other_scope}"),
        &admin,
    )
    .await;
    assert_eq!(rejected.status, StatusCode::BAD_REQUEST);
}

#[actix_web::test]
#[ignore = "requires Docker"]
async fn report_run_routes_enforce_rbac_idempotency_and_conflicts() {
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
    let run = env
        .report_runs
        .find_by_trigger_key("ad_hoc:adhoc-2")
        .await
        .expect("load durable report run")
        .expect("ad-hoc run row");
    assert_eq!(run.request_id.as_deref(), Some("adhoc-2"));

    // Exact replay is an HTTP 200 and returns the same durable identity.
    let replay = post(
        &env,
        "/api/quant/reports/run",
        &analyst,
        &[("X-Acting-Role", "analyst"), ("X-Request-Id", "req-run-2b")],
        json!({ "request_id": "adhoc-2", "reason": "network replay" }),
    )
    .await;
    assert_eq!(replay.status, StatusCode::OK);
    assert_eq!(
        replay.json()["data"]["report_run_id"],
        json!(run.report_run_id.to_string())
    );

    let list = get(
        &env,
        "/api/quant/report-runs?page=1&size=10&trigger_kind=ad_hoc",
        &analyst,
    )
    .await;
    assert_eq!(list.status, StatusCode::OK);
    assert_eq!(list.json()["data"]["total"], json!(1));
    let detail = get(
        &env,
        &format!("/api/quant/report-runs/{}", run.report_run_id),
        &analyst,
    )
    .await;
    assert_eq!(detail.status, StatusCode::OK);
    assert_eq!(detail.json()["data"]["status"], json!("queued"));

    let invalid_retry = post(
        &env,
        &format!("/api/quant/report-runs/{}/retry", run.report_run_id),
        &analyst,
        &[
            ("X-Acting-Role", "analyst"),
            ("X-Request-Id", "req-retry-1"),
        ],
        json!({ "request_id": "retry-1", "reason": "retry queued run" }),
    )
    .await;
    assert_eq!(invalid_retry.status, StatusCode::CONFLICT);

    env.report_runs
        .skip_queued_run(
            &run.report_run_id,
            ReportRunTerminalReason::QueueExpired,
            Utc::now(),
        )
        .await
        .expect("terminalize ad-hoc fixture");
    let retried = post(
        &env,
        &format!("/api/quant/report-runs/{}/retry", run.report_run_id),
        &analyst,
        &[
            ("X-Acting-Role", "analyst"),
            ("X-Request-Id", "req-retry-2"),
        ],
        json!({ "request_id": "retry-2", "reason": "operator retry" }),
    )
    .await;
    assert_eq!(retried.status, StatusCode::ACCEPTED);
    assert_eq!(
        retried.json()["data"]["retry_of_run_id"],
        json!(run.report_run_id.to_string())
    );
    let retry_run_id = retried.json()["data"]["report_run_id"]
        .as_str()
        .expect("retry run id")
        .to_owned();
    let retry_replay = post(
        &env,
        &format!("/api/quant/report-runs/{}/retry", run.report_run_id),
        &analyst,
        &[
            ("X-Acting-Role", "analyst"),
            ("X-Request-Id", "req-retry-3"),
        ],
        json!({ "request_id": "retry-2", "reason": "retry response replay" }),
    )
    .await;
    assert_eq!(retry_replay.status, StatusCode::OK);
    assert_eq!(
        retry_replay.json()["data"]["report_run_id"],
        json!(retry_run_id)
    );

    let health = get(&env, "/api/quant/report-schedules/health", &analyst).await;
    assert_eq!(health.status, StatusCode::OK);
    assert!(health.json()["data"]["observed_at"].is_string());
    assert!(health.json()["data"]["queued_run_count"].is_number());
    assert!(health.json()["data"]["prepared_report_count"].is_number());
    assert!(health.json()["data"]["current_reports"].is_array());
    let gaps = get(
        &env,
        "/api/quant/report-schedule-gaps?page=1&size=10",
        &analyst,
    )
    .await;
    assert_eq!(gaps.status, StatusCode::OK);
    assert!(gaps.json()["data"]["items"].is_array());

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
async fn publication_retry_reuses_bundle_and_obsoletes_when_stale() {
    let env = TestEnv::start().await;
    let admin = login(&env, "admin", "admin").await;
    let report_id = RecommendationReportId::from_v7();
    seed_fixture_published_report(&env.db, report_id.clone(), &env.fixture_report_ctx()).await;

    let timeline = get(
        &env,
        &format!("/api/quant/reports/{report_id}/timeline?page=1&size=20"),
        &admin,
    )
    .await;
    assert_eq!(timeline.status, StatusCode::OK);
    assert!(
        timeline.json()["data"]["total"]
            .as_u64()
            .is_some_and(|n| n >= 1)
    );
    assert!(
        timeline.json()["data"]["items"]
            .as_array()
            .expect("timeline items")
            .iter()
            .all(|row| row["resource_id"] == json!(report_id.to_string()))
    );

    let already_verified = post(
        &env,
        &format!("/api/quant/reports/{report_id}/publication/retry"),
        &admin,
        &[
            ("X-Acting-Role", "super_admin"),
            ("X-Request-Id", "pub-retry-1"),
        ],
        json!({ "request_id": "publication-retry-1", "reason": "must be rejected" }),
    )
    .await;
    assert_eq!(already_verified.status, StatusCode::CONFLICT);

    let failed_report_id = RecommendationReportId::from_v7();
    seed_fixture_prepared_report(&env.db, failed_report_id.clone(), &env.fixture_report_ctx())
        .await;
    let repository = PgRecommendationReportRepository::new(env.db.clone());
    let delivery_worker = Uuid::now_v7();
    repository
        .claim_fact_delivery(delivery_worker, 60)
        .await
        .expect("claim prepared delivery")
        .expect("prepared delivery is pending");
    let failed = repository
        .fail_fact_delivery(
            &failed_report_id,
            delivery_worker,
            ReportFactDeliveryStatus::Failed,
            "injected terminal ClickHouse failure",
        )
        .await
        .expect("terminalize delivery")
        .into_applied()
        .expect("failure settlement must retain its claim");

    let retried = post(
        &env,
        &format!("/api/quant/reports/{failed_report_id}/publication/retry"),
        &admin,
        &[
            ("X-Acting-Role", "super_admin"),
            ("X-Request-Id", "pub-retry-2"),
        ],
        json!({ "request_id": "publication-retry-2", "reason": "ClickHouse recovered" }),
    )
    .await;
    assert_eq!(retried.status, StatusCode::ACCEPTED);
    assert_eq!(retried.json()["data"]["status"], json!("retrying"));
    assert!(retried.json()["data"]["last_error"].is_string());

    let retry_worker = Uuid::now_v7();
    let retry_claim = repository
        .claim_fact_delivery(retry_worker, 60)
        .await
        .expect("claim governed publication retry")
        .expect("retry became immediately due");
    assert_eq!(retry_claim.recommendation_report_id, failed_report_id);
    assert_eq!(retry_claim.bundle_hash, failed.bundle_hash);
    let stale = repository
        .verify_and_publish_report(&failed_report_id, retry_worker, Utc::now())
        .await
        .expect("verify retried immutable bundle")
        .into_applied()
        .expect("publication retry claim must remain held");
    assert_eq!(stale.report.status, RecommendationReportStatus::Obsolete);
    assert_eq!(stale.delivery.status, ReportFactDeliveryStatus::Cancelled);

    let viewer = user_with_role(&env, &admin, "viewerretry", "viewer").await;
    let denied = post(
        &env,
        &format!("/api/quant/reports/{failed_report_id}/publication/retry"),
        &viewer,
        &[("X-Acting-Role", "viewer"), ("X-Request-Id", "pub-retry-3")],
        json!({ "request_id": "publication-retry-3", "reason": "forbidden retry" }),
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
async fn legacy_create_intent_route_is_removed() {
    // The 501 stub was deleted in Phase 05.2; intent creation now lives at
    // `POST /api/quant/intents`. The old sub-route is unregistered → fail-closed 403.
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
    assert_eq!(res.status, StatusCode::FORBIDDEN);
}
