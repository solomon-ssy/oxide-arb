//! Governance dual-track audit integration tests.
//!
//! Drives the hash chain through governed runtime-config mutations (which need no
//! materialized factors) and asserts the two audit tracks: the chain verifies,
//! and the general operation-log row hard-links the exact chain event it
//! produced (`governance_audit_event_id`). Also covers the super-admin acting-
//! role attribution. The control-factor publish/rollback/reject state machine and
//! tamper detection are covered at the repository layer (`pg_repository`).

use actix_web::http::StatusCode;
use serde_json::json;

use crate::{client, harness::TestEnv};

const REQUEST_ID: &str = "x-request-id";
const ACTING_ROLE: &str = "x-acting-role";

#[actix_web::test]
#[ignore = "requires Docker"]
async fn governed_change_hard_links_operation_log_to_audit_chain() {
    let env = TestEnv::start().await;
    let admin = client::login(&env, "admin", "admin").await;

    // A governed runtime-config create appends exactly one chain event.
    let res = client::post_with(
        &env,
        "/api/runtime-config/versions",
        &admin,
        &[(REQUEST_ID, "dual-track-1")],
        json!({ "config_json": { "mode": "live" }, "reason": "dual track" }),
    )
    .await;
    assert_eq!(res.status, StatusCode::OK);
    let version_id = res.json()["data"]["runtime_config_version_id"]
        .as_str()
        .expect("version id")
        .to_owned();

    // Track one: the hash chain verifies and contains the create event.
    let audit = client::get(&env, "/api/control-factors/audit", &admin).await;
    assert_eq!(audit.status, StatusCode::OK);
    assert_eq!(audit.json()["data"]["verified"], json!(true));
    assert_eq!(audit.json()["data"]["broken_at"], json!(null));
    let events = audit.json()["data"]["events"]
        .as_array()
        .expect("events")
        .clone();
    let event = events
        .iter()
        .find(|event| event["resource_id"] == json!(version_id))
        .expect("chain event for the created version");
    let event_id = event["event_id"].as_str().expect("event id").to_owned();

    // Track two: the operation-log row links the exact chain event id.
    let rows = client::wait_for_oplog(&env, &admin, "dual-track-1").await;
    assert_eq!(rows.len(), 1);
    let row = &rows[0];
    assert_eq!(row["action"], "runtime_config.create_version");
    assert_eq!(row["category"], "runtime_config");
    assert_eq!(row["governance_audit_event_id"], json!(event_id));
}

#[actix_web::test]
#[ignore = "requires Docker"]
async fn audit_endpoint_reports_a_verified_chain() {
    let env = TestEnv::start().await;
    let admin = client::login(&env, "admin", "admin").await;

    let create = client::post(
        &env,
        "/api/runtime-config/versions",
        &admin,
        json!({ "config_json": { "mode": "live" }, "reason": "seed" }),
    )
    .await;
    let version_id = create.json()["data"]["runtime_config_version_id"]
        .as_str()
        .expect("version id")
        .to_owned();
    client::post(
        &env,
        &format!("/api/runtime-config/versions/{version_id}/activate"),
        &admin,
        json!({ "reason": "activate" }),
    )
    .await;

    let audit = client::get(&env, "/api/control-factors/audit", &admin).await;
    assert_eq!(audit.status, StatusCode::OK);
    assert_eq!(audit.json()["data"]["verified"], json!(true));
    // create + activate both chained.
    let events = audit.json()["data"]["events"]
        .as_array()
        .expect("events")
        .len();
    assert!(
        events >= 2,
        "expected at least two chained events, got {events}"
    );
}

#[actix_web::test]
#[ignore = "requires Docker"]
async fn super_admin_governed_change_is_attributed_to_super_admin() {
    let env = TestEnv::start().await;
    let admin = client::login(&env, "admin", "admin").await;

    // No X-Acting-Role: the super-admin bypass attributes the literal role.
    let res = client::post_with(
        &env,
        "/api/runtime-config/versions",
        &admin,
        &[(REQUEST_ID, "super-admin-attr")],
        json!({ "config_json": { "mode": "live" }, "reason": "by super admin" }),
    )
    .await;
    assert_eq!(res.status, StatusCode::OK);

    let rows = client::wait_for_oplog(&env, &admin, "super-admin-attr").await;
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0]["acting_role"], "super_admin");

    // An explicit X-Acting-Role the admin does not actually hold is ignored on
    // the bypass and still recorded as super_admin.
    let res = client::post_with(
        &env,
        "/api/runtime-config/versions",
        &admin,
        &[
            (REQUEST_ID, "super-admin-attr-2"),
            (ACTING_ROLE, "risk_owner"),
        ],
        json!({ "config_json": { "mode": "shadow" }, "reason": "still super admin" }),
    )
    .await;
    assert_eq!(res.status, StatusCode::OK);
    let rows = client::wait_for_oplog(&env, &admin, "super-admin-attr-2").await;
    assert_eq!(rows[0]["acting_role"], "super_admin");
}
