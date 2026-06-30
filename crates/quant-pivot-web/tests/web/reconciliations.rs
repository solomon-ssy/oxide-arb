//! Reconciliation API RBAC integration tests.

use actix_web::{
    http::{StatusCode, header::AUTHORIZATION},
    test::TestRequest,
};
use quant_pivot_models::types::ReconciliationId;
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
async fn list_reconciliations_requires_read_permission() {
    let env = TestEnv::start().await;
    let admin = login(&env, "admin", "admin").await;
    let operator = user_with_role(&env, &admin, "op_recon_list", "operator").await;
    let viewer = user_with_role(&env, &admin, "vw_recon_list", "viewer").await;

    assert_eq!(
        get(&env, "/api/quant/reconciliations", &operator)
            .await
            .status,
        StatusCode::OK
    );
    assert_eq!(
        get(&env, "/api/quant/reconciliations", &viewer)
            .await
            .status,
        StatusCode::OK
    );
}

#[actix_web::test]
#[ignore = "requires Docker"]
async fn resolve_reconciliation_requires_resolve_permission_and_acting_role() {
    let env = TestEnv::start().await;
    let admin = login(&env, "admin", "admin").await;
    let operator = user_with_role(&env, &admin, "op_recon_resolve", "operator").await;
    let viewer = user_with_role(&env, &admin, "vw_recon_resolve", "viewer").await;
    let reconciliation_id = ReconciliationId::from_v7();
    let uri = format!("/api/quant/reconciliations/{reconciliation_id}/resolve");
    let body = json!({
        "result": "filled",
        "filled_shares": "10",
        "avg_price": "0.55",
        "reason": "operator confirmed fill on venue"
    });

    // Operator with acting role passes RBAC; mock port rejects as not resolvable (409).
    let allowed = post(
        &env,
        &uri,
        &operator,
        &[("X-Acting-Role", "operator"), ("X-Request-Id", "recon-1")],
        body.clone(),
    )
    .await;
    assert_eq!(
        allowed.status,
        StatusCode::CONFLICT,
        "handler must run past RBAC before mock port rejects"
    );

    let no_acting_role = post(
        &env,
        &uri,
        &operator,
        &[("X-Request-Id", "recon-2")],
        body.clone(),
    )
    .await;
    assert_eq!(no_acting_role.status, StatusCode::BAD_REQUEST);

    let denied = post(
        &env,
        &uri,
        &viewer,
        &[("X-Acting-Role", "viewer"), ("X-Request-Id", "recon-3")],
        body,
    )
    .await;
    assert_eq!(denied.status, StatusCode::FORBIDDEN);
}
