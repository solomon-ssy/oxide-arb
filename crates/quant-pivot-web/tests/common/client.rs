//! Shared HTTP client helpers for the governance / operation-log suites.
//!
//! Thin wrappers over [`crate::harness::call`] that attach the API version, a
//! bearer token, and optional governance headers (`X-Acting-Role`,
//! `X-Request-Id`), plus a poller for the asynchronously-written operation log.

use std::time::Duration;

use actix_web::{
    http::{StatusCode, header::AUTHORIZATION},
    test::TestRequest,
};
use serde_json::{Value, json};

use crate::harness::{self, API_VERSION, Resp, TestEnv};

/// Bearer authorization header pair.
fn bearer(token: &str) -> (actix_web::http::header::HeaderName, String) {
    (AUTHORIZATION, format!("Bearer {token}"))
}

/// Log in and return the access token (asserts success).
pub async fn login(env: &TestEnv, username: &str, password: &str) -> String {
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

/// Authenticated `GET`.
pub async fn get(env: &TestEnv, uri: &str, token: &str) -> Resp {
    let req = TestRequest::get()
        .uri(uri)
        .insert_header(API_VERSION)
        .insert_header(bearer(token));
    harness::call(&env.state, req).await
}

/// Authenticated `POST` with extra headers (e.g. `X-Acting-Role`, `X-Request-Id`).
pub async fn post_with(
    env: &TestEnv,
    uri: &str,
    token: &str,
    headers: &[(&'static str, &str)],
    body: Value,
) -> Resp {
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

/// Poll the (asynchronously-written) operation log for rows correlated with
/// `request_id`, returning them once present (or empty after the timeout).
pub async fn wait_for_oplog(env: &TestEnv, admin: &str, request_id: &str) -> Vec<Value> {
    for _ in 0..40 {
        let res = get(
            env,
            &format!("/api/operation-logs?request_id={request_id}"),
            admin,
        )
        .await;
        if res.status == StatusCode::OK {
            let items = res.json()["data"]["items"]
                .as_array()
                .cloned()
                .unwrap_or_default();
            if !items.is_empty() {
                return items;
            }
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    Vec::new()
}
