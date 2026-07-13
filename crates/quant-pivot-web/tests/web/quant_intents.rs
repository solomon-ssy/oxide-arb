//! Phase 05.2 order-intent API integration tests (real Postgres + Redis).
//!
//! These exercise the RBAC matrix and routing of the governed intent surface
//! (`/api/quant/intents`) end-to-end through the real `CoreOrderIntentService`.
//! The full create / approve / reject / expire money closure is covered by the
//! repository integration tests (atomic intent + capital transactions) and the
//! mode-gate / projection / re-check unit tests.

use actix_web::{
    http::{StatusCode, header::AUTHORIZATION},
    test::TestRequest,
};
use quant_pivot_models::types::{OrderIntentId, RecommendationId};
use serde_json::{Value, json};

use crate::harness::{self, API_VERSION, TestEnv};

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

async fn put(env: &TestEnv, uri: &str, token: &str, body: Value) -> harness::Resp {
    let req = TestRequest::put()
        .uri(uri)
        .insert_header(API_VERSION)
        .insert_header(bearer(token))
        .set_json(body);
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

#[actix_web::test]
#[ignore = "requires Docker"]
async fn create_intent_requires_operator_acting_role() {
    let env = TestEnv::start().await;
    let admin = login(&env, "admin", "admin").await;
    let operator = user_with_role(&env, &admin, "op_create", "operator").await;
    let viewer = user_with_role(&env, &admin, "vw_create", "viewer").await;
    let body = json!({ "recommendation_id": RecommendationId::from_v7(), "reason": "manual" });

    // Operator with the acting role passes RBAC; the random recommendation is
    // not found, proving the handler ran (404, not 403/400).
    let allowed = post(
        &env,
        "/api/quant/intents",
        &operator,
        &[("X-Acting-Role", "operator"), ("X-Request-Id", "ci-1")],
        body.clone(),
    )
    .await;
    assert_eq!(allowed.status, StatusCode::NOT_FOUND);

    // Missing acting role on a governed route → 400.
    let no_role = post(
        &env,
        "/api/quant/intents",
        &operator,
        &[("X-Request-Id", "ci-2")],
        body.clone(),
    )
    .await;
    assert_eq!(no_role.status, StatusCode::BAD_REQUEST);

    // Viewer is not permitted to create → 403.
    let denied = post(
        &env,
        "/api/quant/intents",
        &viewer,
        &[("X-Acting-Role", "viewer"), ("X-Request-Id", "ci-3")],
        body,
    )
    .await;
    assert_eq!(denied.status, StatusCode::FORBIDDEN);
}

#[actix_web::test]
#[ignore = "requires Docker"]
async fn risk_owner_may_reject_but_not_approve() {
    let env = TestEnv::start().await;
    let admin = login(&env, "admin", "admin").await;
    let risk_owner = user_with_role(&env, &admin, "ro_intent", "risk_owner").await;
    let intent_id = OrderIntentId::from_v7();

    // risk_owner holds OrderIntent:Approve? No — approve is operator-only → 403.
    let approve = post(
        &env,
        &format!("/api/quant/intents/{intent_id}/approve"),
        &risk_owner,
        &[("X-Acting-Role", "risk_owner"), ("X-Request-Id", "ro-1")],
        json!({ "reason": "looks fine" }),
    )
    .await;
    assert_eq!(approve.status, StatusCode::FORBIDDEN);

    // risk_owner holds OrderIntent:Reject → RBAC passes; the random intent is
    // not found, proving the handler ran (404, not 403).
    let reject = post(
        &env,
        &format!("/api/quant/intents/{intent_id}/reject"),
        &risk_owner,
        &[("X-Acting-Role", "risk_owner"), ("X-Request-Id", "ro-2")],
        json!({ "reason": "risk veto" }),
    )
    .await;
    assert_eq!(reject.status, StatusCode::NOT_FOUND);
}

#[actix_web::test]
#[ignore = "requires Docker"]
async fn intents_list_is_readable_by_viewer() {
    let env = TestEnv::start().await;
    let admin = login(&env, "admin", "admin").await;
    let viewer = user_with_role(&env, &admin, "vw_list", "viewer").await;

    let res = get(&env, "/api/quant/intents?page=1&size=10", &viewer).await;
    assert_eq!(res.status, StatusCode::OK);
    assert_eq!(res.json()["data"]["total"], json!(0));
}
