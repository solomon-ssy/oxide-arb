//! Operation-log (track-two audit) integration tests.
//!
//! Verifies the `OperationAudit` middleware records mutating requests and auth
//! events into the append-only `operation_log` — asynchronously, redacted, and
//! never blocking the response — while leaving reads unrecorded.

use actix_web::{http::StatusCode, test::TestRequest};
use serde_json::json;

use crate::{
    client,
    harness::{self, API_VERSION, TestEnv},
};

const REQUEST_ID: &str = "x-request-id";

#[actix_web::test]
#[ignore = "requires Docker"]
async fn mutating_request_is_recorded_with_envelope() {
    let env = TestEnv::start().await;
    let admin = client::login(&env, "admin", "admin").await;

    let res = client::post_with(
        &env,
        "/api/users",
        &admin,
        &[(REQUEST_ID, "oplog-create-user")],
        json!({ "username": "audited1", "password": "password123", "nickname": "Audited" }),
    )
    .await;
    assert_eq!(res.status, StatusCode::OK);

    let rows = client::wait_for_oplog(&env, &admin, "oplog-create-user").await;
    assert_eq!(rows.len(), 1, "exactly one row for the create request");
    let row = &rows[0];
    assert_eq!(row["action"], "user.create");
    assert_eq!(row["category"], "rbac");
    assert_eq!(row["resource_type"], "user");
    assert_eq!(row["http_method"], "POST");
    assert_eq!(row["http_status"], 200);
    assert_eq!(row["outcome"], "success");
    assert_eq!(row["actor_username"], "admin");
    // The redacted detail summary carries the username, never a password.
    assert_eq!(row["detail"]["username"], "audited1");
    assert!(!row.to_string().contains("password123"));
}

#[actix_web::test]
#[ignore = "requires Docker"]
async fn read_request_is_not_recorded() {
    let env = TestEnv::start().await;
    let admin = client::login(&env, "admin", "admin").await;

    let res = client::get(&env, "/api/users", &admin).await;
    assert_eq!(res.status, StatusCode::OK);
    // The GET above carried no explicit request id; issue one we can query for.
    let req = TestRequest::get()
        .uri("/api/users")
        .insert_header(API_VERSION)
        .insert_header((
            actix_web::http::header::AUTHORIZATION,
            format!("Bearer {admin}"),
        ))
        .insert_header((REQUEST_ID, "oplog-read"));
    assert_eq!(harness::call(&env.state, req).await.status, StatusCode::OK);

    // Give the writer ample time; the read must still leave no trace.
    tokio::time::sleep(std::time::Duration::from_millis(400)).await;
    let res = client::get(&env, "/api/operation-logs?request_id=oplog-read", &admin).await;
    assert_eq!(res.status, StatusCode::OK);
    let items = res.json()["data"]["items"].as_array().cloned().unwrap();
    assert!(
        items.is_empty(),
        "GET reads must not be recorded: {items:?}"
    );
}

#[actix_web::test]
#[ignore = "requires Docker"]
async fn failed_login_is_recorded_denied_without_leaking_password() {
    let env = TestEnv::start().await;
    let admin = client::login(&env, "admin", "admin").await;

    // Wrong password for an existing account.
    let req = TestRequest::post()
        .uri("/api/auth/login")
        .insert_header(API_VERSION)
        .insert_header((REQUEST_ID, "oplog-login-fail"))
        .set_json(json!({ "username": "admin", "password": "definitely-wrong" }));
    let res = harness::call(&env.state, req).await;
    assert_eq!(res.status, StatusCode::UNAUTHORIZED);

    let rows = client::wait_for_oplog(&env, &admin, "oplog-login-fail").await;
    assert_eq!(rows.len(), 1);
    let row = &rows[0];
    assert_eq!(row["action"], "auth.login");
    assert_eq!(row["category"], "auth");
    assert_eq!(row["outcome"], "denied");
    assert_eq!(row["http_status"], 401);
    // The attempted username is attributed; the password never appears anywhere.
    assert_eq!(row["actor_username"], "admin");
    assert!(
        !row.to_string().contains("definitely-wrong"),
        "operation log must not leak the attempted password"
    );
}

#[actix_web::test]
#[ignore = "requires Docker"]
async fn successful_login_is_recorded() {
    let env = TestEnv::start().await;
    let admin = client::login(&env, "admin", "admin").await;

    let req = TestRequest::post()
        .uri("/api/auth/login")
        .insert_header(API_VERSION)
        .insert_header((REQUEST_ID, "oplog-login-ok"))
        .set_json(json!({ "username": "admin", "password": "admin" }));
    assert_eq!(harness::call(&env.state, req).await.status, StatusCode::OK);

    let rows = client::wait_for_oplog(&env, &admin, "oplog-login-ok").await;
    assert_eq!(rows.len(), 1);
    let row = &rows[0];
    assert_eq!(row["action"], "auth.login");
    assert_eq!(row["outcome"], "success");
    assert_eq!(row["actor_username"], "admin");
    // A resolved login also stamps the stable actor id.
    assert!(row["actor_user_id"].is_string());
}
