//! Authentication integration tests.

use actix_web::{
    cookie::Cookie,
    http::{
        StatusCode,
        header::{AUTHORIZATION, COOKIE, SET_COOKIE},
    },
};
use jsonwebtoken::{Algorithm, decode_header};
use quant_pivot_models::domain::UserInfo;
use serde_json::json;

use crate::{
    auth_helpers::{expired_access_token, kill_redis},
    harness::{self, API_VERSION, TestEnv},
};

const REFRESH_COOKIE_NAME: &str = "qp_refresh";
const SAME_ORIGIN: (&str, &str) = ("Origin", "http://localhost:8080");

/// Authenticate as the seeded `admin` and return `(access, refresh-cookie value)`.
async fn login_admin(env: &TestEnv) -> (String, String) {
    let req = actix_web::test::TestRequest::post()
        .uri("/api/auth/login")
        .insert_header(API_VERSION)
        .set_json(json!({ "username": "admin", "password": "admin" }));
    let res = harness::call(&env.state, req).await;
    assert_eq!(res.status, StatusCode::OK, "admin login should succeed");

    let body = res.json();
    let data = &body["data"];
    let refresh = refresh_cookie_value(&res);
    (
        data["access_token"]
            .as_str()
            .expect("access_token")
            .to_owned(),
        refresh,
    )
}

fn refresh_cookie_value(response: &harness::Resp) -> String {
    let set_cookie = response
        .headers
        .get(SET_COOKIE)
        .and_then(|value| value.to_str().ok())
        .expect("refresh Set-Cookie header");
    let cookie = Cookie::parse(set_cookie.to_owned()).expect("valid refresh cookie");
    assert_eq!(cookie.name(), REFRESH_COOKIE_NAME);
    cookie.value().to_owned()
}

fn refresh_cookie_header(value: &str) -> (actix_web::http::header::HeaderName, String) {
    (COOKIE, format!("{REFRESH_COOKIE_NAME}={value}"))
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
async fn login_returns_access_token_and_hardened_refresh_cookie() {
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
    let access_token = data["access_token"].as_str().expect("access token");
    let header = decode_header(access_token).expect("signed JWT header");
    assert_eq!(header.alg, Algorithm::HS256);
    assert_eq!(header.typ.as_deref(), Some("at+jwt"));
    assert!(header.kid.is_none());
    assert!(data.get("refresh_token").is_none());
    assert_eq!(data["token_type"], "Bearer");
    assert_eq!(data["expires_in"], 900);
    let set_cookie = res.header("set-cookie").expect("refresh cookie");
    assert!(set_cookie.contains("HttpOnly"));
    assert!(set_cookie.contains("Secure"));
    assert!(set_cookie.contains("SameSite=Strict"));
    assert!(set_cookie.contains("Path=/api/auth"));
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

    // First rotation succeeds and returns a new cookie, never a body token.
    let req = actix_web::test::TestRequest::post()
        .uri("/api/auth/refresh")
        .insert_header(API_VERSION)
        .insert_header(SAME_ORIGIN)
        .insert_header(refresh_cookie_header(&refresh));
    let res = harness::call(&env.state, req).await;
    assert_eq!(res.status, StatusCode::OK);
    assert!(res.json()["data"].get("refresh_token").is_none());
    let new_refresh = refresh_cookie_value(&res);
    assert_ne!(new_refresh, refresh, "refresh token must rotate");

    // Normal forward rotation remains valid.
    let next = actix_web::test::TestRequest::post()
        .uri("/api/auth/refresh")
        .insert_header(API_VERSION)
        .insert_header(SAME_ORIGIN)
        .insert_header(refresh_cookie_header(&new_refresh));
    let next_res = harness::call(&env.state, next).await;
    assert_eq!(next_res.status, StatusCode::OK);
    let latest_refresh = refresh_cookie_value(&next_res);

    // Replaying any consumed token terminates the entire rotation family.
    let replay = actix_web::test::TestRequest::post()
        .uri("/api/auth/refresh")
        .insert_header(API_VERSION)
        .insert_header(SAME_ORIGIN)
        .insert_header(refresh_cookie_header(&refresh));
    assert_eq!(
        harness::call(&env.state, replay).await.status,
        StatusCode::UNAUTHORIZED
    );

    // Even the newest token is rejected after family replay detection.
    let family_rejected = actix_web::test::TestRequest::post()
        .uri("/api/auth/refresh")
        .insert_header(API_VERSION)
        .insert_header(SAME_ORIGIN)
        .insert_header(refresh_cookie_header(&latest_refresh));
    assert_eq!(
        harness::call(&env.state, family_rejected).await.status,
        StatusCode::UNAUTHORIZED
    );
}

#[actix_web::test]
#[ignore = "requires Docker"]
async fn refresh_rejects_missing_or_cross_site_browser_origin() {
    let env = TestEnv::start().await;
    let (_access, refresh) = login_admin(&env).await;

    let missing_origin = actix_web::test::TestRequest::post()
        .uri("/api/auth/refresh")
        .insert_header(API_VERSION)
        .insert_header(refresh_cookie_header(&refresh));
    assert_eq!(
        harness::call(&env.state, missing_origin).await.status,
        StatusCode::UNAUTHORIZED
    );

    let cross_site = actix_web::test::TestRequest::post()
        .uri("/api/auth/refresh")
        .insert_header(API_VERSION)
        .insert_header(SAME_ORIGIN)
        .insert_header(("Sec-Fetch-Site", "cross-site"))
        .insert_header(refresh_cookie_header(&refresh));
    assert_eq!(
        harness::call(&env.state, cross_site).await.status,
        StatusCode::UNAUTHORIZED
    );
}

#[actix_web::test]
#[ignore = "requires Docker"]
async fn concurrent_refresh_replay_revokes_the_entire_session_family() {
    let env = TestEnv::start().await;
    let (_access, refresh) = login_admin(&env).await;

    let first = actix_web::test::TestRequest::post()
        .uri("/api/auth/refresh")
        .insert_header(API_VERSION)
        .insert_header(SAME_ORIGIN)
        .insert_header(refresh_cookie_header(&refresh));
    let second = actix_web::test::TestRequest::post()
        .uri("/api/auth/refresh")
        .insert_header(API_VERSION)
        .insert_header(SAME_ORIGIN)
        .insert_header(refresh_cookie_header(&refresh));

    let (first_response, second_response) = tokio::join!(
        harness::call(&env.state, first),
        harness::call(&env.state, second),
    );
    let responses: [_; 2] = (first_response, second_response).into();
    assert_eq!(
        responses
            .iter()
            .filter(|response| response.status == StatusCode::OK)
            .count(),
        1,
        "exactly one refresh CAS may succeed"
    );
    assert_eq!(
        responses
            .iter()
            .filter(|response| response.status == StatusCode::UNAUTHORIZED)
            .count(),
        1,
        "the competing refresh must be classified as replay"
    );

    let issued_access = responses
        .iter()
        .find(|response| response.status == StatusCode::OK)
        .and_then(|response| {
            response.json()["data"]["access_token"]
                .as_str()
                .map(str::to_owned)
        })
        .expect("successful refresh access token");
    let me = actix_web::test::TestRequest::get()
        .uri("/api/auth/me")
        .insert_header(API_VERSION)
        .insert_header(bearer(&issued_access));
    assert_eq!(
        harness::call(&env.state, me).await.status,
        StatusCode::UNAUTHORIZED,
        "replay revocation must invalidate every token in the family"
    );
}

#[actix_web::test]
#[ignore = "requires Docker"]
async fn logout_revokes_both_access_and_refresh_tokens() {
    let env = TestEnv::start().await;
    let (access, refresh) = login_admin(&env).await;

    let logout = actix_web::test::TestRequest::post()
        .uri("/api/auth/logout")
        .insert_header(API_VERSION)
        .insert_header(SAME_ORIGIN)
        .insert_header(bearer(&access))
        .insert_header(refresh_cookie_header(&refresh));
    let logout_res = harness::call(&env.state, logout).await;
    assert_eq!(logout_res.status, StatusCode::OK);
    assert!(
        logout_res
            .header("set-cookie")
            .expect("expired refresh cookie")
            .contains("Max-Age=0")
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
        .insert_header(SAME_ORIGIN)
        .insert_header(refresh_cookie_header(&refresh));
    assert_eq!(
        harness::call(&env.state, refresh_req).await.status,
        StatusCode::UNAUTHORIZED
    );
}

#[actix_web::test]
#[ignore = "requires Docker"]
async fn disabling_user_revokes_every_session_family_for_the_subject() {
    let env = TestEnv::start().await;
    let (operator_access, _operator_refresh) = login_admin(&env).await;
    let (other_access, other_refresh) = login_admin(&env).await;
    let admin = fetch_admin(&env).await;

    let disable = actix_web::test::TestRequest::put()
        .uri(&format!("/api/users/{}/status", admin.id))
        .insert_header(API_VERSION)
        .insert_header(bearer(&operator_access))
        .set_json(json!({ "status": "disabled" }));
    assert_eq!(
        harness::call(&env.state, disable).await.status,
        StatusCode::OK
    );

    let me = actix_web::test::TestRequest::get()
        .uri("/api/auth/me")
        .insert_header(API_VERSION)
        .insert_header(bearer(&other_access));
    assert_eq!(
        harness::call(&env.state, me).await.status,
        StatusCode::UNAUTHORIZED,
        "every access token for a disabled subject must fail immediately"
    );

    let refresh = actix_web::test::TestRequest::post()
        .uri("/api/auth/refresh")
        .insert_header(API_VERSION)
        .insert_header(SAME_ORIGIN)
        .insert_header(refresh_cookie_header(&other_refresh));
    assert_eq!(
        harness::call(&env.state, refresh).await.status,
        StatusCode::UNAUTHORIZED,
        "every refresh family for a disabled subject must be revoked"
    );
}

#[actix_web::test]
#[ignore = "requires Docker"]
async fn password_change_revokes_all_sessions_and_requires_the_new_secret() {
    let env = TestEnv::start().await;
    let (operator_access, _operator_refresh) = login_admin(&env).await;
    let (other_access, other_refresh) = login_admin(&env).await;
    let admin = fetch_admin(&env).await;

    let change = actix_web::test::TestRequest::put()
        .uri(&format!("/api/users/{}/password", admin.id))
        .insert_header(API_VERSION)
        .insert_header(bearer(&operator_access))
        .set_json(json!({ "password": "new-production-password-2026" }));
    assert_eq!(
        harness::call(&env.state, change).await.status,
        StatusCode::OK
    );

    let me = actix_web::test::TestRequest::get()
        .uri("/api/auth/me")
        .insert_header(API_VERSION)
        .insert_header(bearer(&other_access));
    assert_eq!(
        harness::call(&env.state, me).await.status,
        StatusCode::UNAUTHORIZED
    );

    let refresh = actix_web::test::TestRequest::post()
        .uri("/api/auth/refresh")
        .insert_header(API_VERSION)
        .insert_header(SAME_ORIGIN)
        .insert_header(refresh_cookie_header(&other_refresh));
    assert_eq!(
        harness::call(&env.state, refresh).await.status,
        StatusCode::UNAUTHORIZED
    );

    let old_secret = actix_web::test::TestRequest::post()
        .uri("/api/auth/login")
        .insert_header(API_VERSION)
        .set_json(json!({ "username": "admin", "password": "admin" }));
    assert_eq!(
        harness::call(&env.state, old_secret).await.status,
        StatusCode::UNAUTHORIZED
    );

    let new_secret = actix_web::test::TestRequest::post()
        .uri("/api/auth/login")
        .insert_header(API_VERSION)
        .set_json(json!({
            "username": "admin",
            "password": "new-production-password-2026"
        }));
    assert_eq!(
        harness::call(&env.state, new_secret).await.status,
        StatusCode::OK
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
