//! Authentication integration tests.

use actix_web::http::{StatusCode, header::AUTHORIZATION};
use quant_pivot_models::domain::UserInfo;
use serde_json::json;

use crate::{
    auth_helpers::{expired_access_token, kill_redis},
    harness::{self, API_VERSION, TestEnv},
};

/// Authenticate as the seeded `admin` and return `(access, refresh)`.
async fn login_admin(env: &TestEnv) -> (String, String) {
    let req = actix_web::test::TestRequest::post()
        .uri("/api/auth/login")
        .insert_header(API_VERSION)
        .set_json(json!({ "username": "admin", "password": "admin" }));
    let res = harness::call(&env.state, req).await;
    assert_eq!(res.status, StatusCode::OK, "admin login should succeed");

    let body = res.json();
    let data = &body["data"];
    (
        data["access_token"]
            .as_str()
            .expect("access_token")
            .to_owned(),
        data["refresh_token"]
            .as_str()
            .expect("refresh_token")
            .to_owned(),
    )
}

fn bearer(token: &str) -> (actix_web::http::header::HeaderName, String) {
    (AUTHORIZATION, format!("Bearer {token}"))
}

async fn fetch_admin(env: &TestEnv) -> UserInfo {
    env.state
        .users
        .find_by_username("admin")
        .await
        .expect("query admin")
        .expect("admin must be seeded")
}

#[actix_web::test]
#[ignore = "requires Docker"]
async fn login_success_issues_token_pair_in_unified_envelope() {
    let env = TestEnv::start().await;

    let req = actix_web::test::TestRequest::post()
        .uri("/api/auth/login")
        .insert_header(API_VERSION)
        .set_json(json!({ "username": "admin", "password": "admin" }));
    let res = harness::call(&env.state, req).await;

    assert_eq!(res.status, StatusCode::OK);
    let body = res.json();
    assert_eq!(body["code"], 200);
    assert_eq!(body["message"], "ok");
    let data = &body["data"];
    assert!(!data["access_token"].as_str().unwrap().is_empty());
    assert!(!data["refresh_token"].as_str().unwrap().is_empty());
    assert_eq!(data["token_type"], "Bearer");
    assert_eq!(data["expires_in"], 900);
}

#[actix_web::test]
#[ignore = "requires Docker"]
async fn login_wrong_password_is_unauthorized_with_generic_message() {
    let env = TestEnv::start().await;

    let req = actix_web::test::TestRequest::post()
        .uri("/api/auth/login")
        .insert_header(API_VERSION)
        .set_json(json!({ "username": "admin", "password": "wrong-password" }));
    let res = harness::call(&env.state, req).await;

    assert_eq!(res.status, StatusCode::UNAUTHORIZED);
    let body = res.json();
    assert_eq!(body["code"], 401);
    assert_eq!(body["message"], "invalid credentials");
    assert!(body["data"].is_null(), "error envelope carries null data");
}

#[actix_web::test]
#[ignore = "requires Docker"]
async fn login_unknown_user_is_indistinguishable_from_wrong_password() {
    let env = TestEnv::start().await;

    let req = actix_web::test::TestRequest::post()
        .uri("/api/auth/login")
        .insert_header(API_VERSION)
        .set_json(json!({ "username": "ghost-user", "password": "whatever" }));
    let res = harness::call(&env.state, req).await;

    assert_eq!(res.status, StatusCode::UNAUTHORIZED);
    // Identical to the wrong-password response → no account enumeration.
    assert_eq!(res.json()["message"], "invalid credentials");
}

#[actix_web::test]
#[ignore = "requires Docker"]
async fn me_returns_user_roles_and_menus_without_credentials() {
    let env = TestEnv::start().await;
    let (access, _refresh) = login_admin(&env).await;

    let req = actix_web::test::TestRequest::get()
        .uri("/api/auth/me")
        .insert_header(API_VERSION)
        .insert_header(bearer(&access));
    let res = harness::call(&env.state, req).await;

    assert_eq!(res.status, StatusCode::OK);
    let body = res.json();
    let data = &body["data"];
    assert_eq!(data["user"]["username"], "admin");

    let roles = data["roles"].as_array().expect("roles array");
    assert!(
        roles.iter().any(|role| role["code"] == "super_admin"),
        "admin must carry the super_admin role"
    );
    assert!(
        !data["menus"].as_array().expect("menus array").is_empty(),
        "super_admin must see a non-empty menu tree"
    );

    // Hard invariant: the credential hash must never reach the wire.
    let raw = serde_json::to_string(&body).unwrap();
    assert!(
        !raw.contains("password"),
        "response must not leak password material"
    );
}

#[actix_web::test]
#[ignore = "requires Docker"]
async fn refresh_rotates_pair_and_old_refresh_replay_is_rejected() {
    let env = TestEnv::start().await;
    let (_access, refresh) = login_admin(&env).await;

    // First rotation succeeds and yields a brand-new pair.
    let req = actix_web::test::TestRequest::post()
        .uri("/api/auth/refresh")
        .insert_header(API_VERSION)
        .set_json(json!({ "refresh_token": refresh }));
    let res = harness::call(&env.state, req).await;
    assert_eq!(res.status, StatusCode::OK);
    let new_refresh = res.json()["data"]["refresh_token"]
        .as_str()
        .unwrap()
        .to_owned();
    assert_ne!(new_refresh, refresh, "refresh token must rotate");

    // Replaying the consumed refresh token is rejected (it was revoked).
    let replay = actix_web::test::TestRequest::post()
        .uri("/api/auth/refresh")
        .insert_header(API_VERSION)
        .set_json(json!({ "refresh_token": refresh }));
    assert_eq!(
        harness::call(&env.state, replay).await.status,
        StatusCode::UNAUTHORIZED
    );

    // The freshly issued refresh token still works.
    let next = actix_web::test::TestRequest::post()
        .uri("/api/auth/refresh")
        .insert_header(API_VERSION)
        .set_json(json!({ "refresh_token": new_refresh }));
    assert_eq!(harness::call(&env.state, next).await.status, StatusCode::OK);
}

#[actix_web::test]
#[ignore = "requires Docker"]
async fn logout_revokes_both_access_and_refresh_tokens() {
    let env = TestEnv::start().await;
    let (access, refresh) = login_admin(&env).await;

    let logout = actix_web::test::TestRequest::post()
        .uri("/api/auth/logout")
        .insert_header(API_VERSION)
        .insert_header(bearer(&access))
        .set_json(json!({ "refresh_token": refresh }));
    assert_eq!(
        harness::call(&env.state, logout).await.status,
        StatusCode::OK
    );

    // The revoked access token can no longer reach a protected route.
    let me = actix_web::test::TestRequest::get()
        .uri("/api/auth/me")
        .insert_header(API_VERSION)
        .insert_header(bearer(&access));
    assert_eq!(
        harness::call(&env.state, me).await.status,
        StatusCode::UNAUTHORIZED
    );

    // The revoked refresh token can no longer be rotated.
    let refresh_req = actix_web::test::TestRequest::post()
        .uri("/api/auth/refresh")
        .insert_header(API_VERSION)
        .set_json(json!({ "refresh_token": refresh }));
    assert_eq!(
        harness::call(&env.state, refresh_req).await.status,
        StatusCode::UNAUTHORIZED
    );
}

#[actix_web::test]
#[ignore = "requires Docker"]
async fn authn_rejects_missing_token() {
    let env = TestEnv::start().await;

    let req = actix_web::test::TestRequest::get()
        .uri("/api/auth/me")
        .insert_header(API_VERSION);
    let res = harness::call(&env.state, req).await;

    assert_eq!(res.status, StatusCode::UNAUTHORIZED);
    let body = res.json();
    assert_eq!(body["code"], 401);
    assert!(body["data"].is_null());
}

#[actix_web::test]
#[ignore = "requires Docker"]
async fn authn_rejects_malformed_token() {
    let env = TestEnv::start().await;

    let req = actix_web::test::TestRequest::get()
        .uri("/api/auth/me")
        .insert_header(API_VERSION)
        .insert_header(bearer("not-a-real-jwt"));
    assert_eq!(
        harness::call(&env.state, req).await.status,
        StatusCode::UNAUTHORIZED
    );
}

#[actix_web::test]
#[ignore = "requires Docker"]
async fn authn_rejects_refresh_token_used_as_access() {
    let env = TestEnv::start().await;
    let (_access, refresh) = login_admin(&env).await;

    // Presenting a (valid) refresh token where an access token is required.
    let req = actix_web::test::TestRequest::get()
        .uri("/api/auth/me")
        .insert_header(API_VERSION)
        .insert_header(bearer(&refresh));
    assert_eq!(
        harness::call(&env.state, req).await.status,
        StatusCode::UNAUTHORIZED
    );
}

#[actix_web::test]
#[ignore = "requires Docker"]
async fn authn_rejects_expired_token() {
    let env = TestEnv::start().await;
    let admin = fetch_admin(&env).await;
    let expired = expired_access_token(&admin);

    let req = actix_web::test::TestRequest::get()
        .uri("/api/auth/me")
        .insert_header(API_VERSION)
        .insert_header(bearer(&expired));
    assert_eq!(
        harness::call(&env.state, req).await.status,
        StatusCode::UNAUTHORIZED
    );
}

#[actix_web::test]
#[ignore = "requires Docker"]
async fn versioned_route_requires_matching_version_header() {
    let env = TestEnv::start().await;
    let credentials = json!({ "username": "admin", "password": "admin" });

    // Missing version header → the v1 scope does not match → 404.
    let no_header = actix_web::test::TestRequest::post()
        .uri("/api/auth/login")
        .set_json(&credentials);
    assert_eq!(
        harness::call(&env.state, no_header).await.status,
        StatusCode::NOT_FOUND
    );

    // Unsupported version → still no match → 404.
    let wrong_version = actix_web::test::TestRequest::post()
        .uri("/api/auth/login")
        .insert_header(("Accept-Api-Version", "v2"))
        .set_json(&credentials);
    assert_eq!(
        harness::call(&env.state, wrong_version).await.status,
        StatusCode::NOT_FOUND
    );
}

#[actix_web::test]
#[ignore = "requires Docker"]
async fn health_probe_is_unversioned_and_echoes_request_id() {
    let env = TestEnv::start().await;

    // Probes are infra concerns: reachable without a version header.
    let supplied = actix_web::test::TestRequest::get()
        .uri("/health")
        .insert_header(("X-Request-Id", "trace-abc-123"));
    let res = harness::call(&env.state, supplied).await;
    assert_eq!(res.status, StatusCode::OK);
    assert_eq!(
        res.header("x-request-id"),
        Some("trace-abc-123"),
        "an inbound request id must be echoed verbatim"
    );
    assert_eq!(res.json()["data"]["status"], "ok");

    // Without an inbound id, the middleware generates one.
    let generated = actix_web::test::TestRequest::get().uri("/ready");
    let res = harness::call(&env.state, generated).await;
    assert!(
        res.header("x-request-id").is_some_and(|id| !id.is_empty()),
        "a request id must always be present on the response"
    );
}

#[actix_web::test]
#[ignore = "requires Docker"]
async fn authn_fails_closed_when_revocation_store_is_unavailable() {
    let mut env = TestEnv::start().await;
    // Obtain a valid access token while Redis is up (login never touches Redis).
    let (access, _refresh) = login_admin(&env).await;

    // Now the revocation store goes dark.
    kill_redis(&mut env).await;

    // The token is structurally valid, but its revocation status is unknown, so
    // authn must refuse to authenticate — 503, never a silent allow.
    let req = actix_web::test::TestRequest::get()
        .uri("/api/auth/me")
        .insert_header(API_VERSION)
        .insert_header(bearer(&access));
    let res = harness::call(&env.state, req).await;

    assert_eq!(res.status, StatusCode::SERVICE_UNAVAILABLE);
    let body = res.json();
    assert_eq!(body["code"], 503);
    assert!(body["data"].is_null());
}
