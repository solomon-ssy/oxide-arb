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

use crate::{
    client,
    harness::TestEnv,
    headers::{ACTING_ROLE, REQUEST_ID},
};

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

#[actix_web::test]
#[ignore = "requires Docker"]
async fn shadow_publication_via_http_appends_verified_audit_chain() {
    let env = TestEnv::start().await;
    let admin = client::login(&env, "admin", "admin").await;
    let repo = oxide_arb_repository::postgres::PgControlFactorRepository::new(env.db.clone());
    let factor_id = crate::control_factor_fixture::seed_candidate_factor(&repo).await;
    let expires = chrono::Utc::now() + chrono::Duration::days(1);

    let res = client::post_with(
        &env,
        "/api/control-factors/publications/shadow",
        &admin,
        &[(REQUEST_ID, "gov-shadow-http")],
        json!({
            "factor_ids": [factor_id.to_string()],
            "idempotency_key": "web-shadow-http-1",
            "expires_at": expires,
            "reason": "shadow via http"
        }),
    )
    .await;
    assert_eq!(res.status, StatusCode::OK, "shadow publication");

    let audit = client::get(&env, "/api/control-factors/audit", &admin).await;
    assert_eq!(audit.status, StatusCode::OK);
    assert_eq!(audit.json()["data"]["verified"], json!(true));

    let rows = client::wait_for_oplog(&env, &admin, "gov-shadow-http").await;
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0]["action"], "control_factor.shadow");
}

#[actix_web::test]
#[ignore = "requires Docker"]
async fn reject_factor_via_http_appends_verified_audit_chain() {
    let env = TestEnv::start().await;
    let admin = client::login(&env, "admin", "admin").await;
    let repo = oxide_arb_repository::postgres::PgControlFactorRepository::new(env.db.clone());
    let factor_id = crate::control_factor_fixture::seed_candidate_factor(&repo).await;

    let res = client::post_with(
        &env,
        &format!("/api/control-factors/{factor_id}/reject"),
        &admin,
        &[(REQUEST_ID, "gov-reject-http")],
        json!({ "reason": "reject via http" }),
    )
    .await;
    assert_eq!(res.status, StatusCode::OK, "reject factor");

    let audit = client::get(&env, "/api/control-factors/audit", &admin).await;
    assert_eq!(audit.json()["data"]["verified"], json!(true));

    let rows = client::wait_for_oplog(&env, &admin, "gov-reject-http").await;
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0]["action"], "control_factor.reject");
}

#[actix_web::test]
#[ignore = "requires Docker"]
async fn publish_and_rollback_via_http_restore_target_publication() {
    let env = TestEnv::start().await;
    let admin = client::login(&env, "admin", "admin").await;
    let repo = oxide_arb_repository::postgres::PgControlFactorRepository::new(env.db.clone());
    let genesis_factor = crate::control_factor_fixture::seed_candidate_factor(&repo).await;
    let successor_factor = crate::control_factor_fixture::seed_candidate_factor(&repo).await;
    let expires = chrono::Utc::now() + chrono::Duration::days(1);

    // Shadow staging is required before a Published promotion.
    let shadow_genesis = client::post_with(
        &env,
        "/api/control-factors/publications/shadow",
        &admin,
        &[(REQUEST_ID, "gov-shadow-genesis")],
        json!({
            "factor_ids": [genesis_factor.to_string()],
            "idempotency_key": "web-shadow-genesis",
            "expires_at": expires,
            "reason": "shadow genesis"
        }),
    )
    .await;
    assert_eq!(shadow_genesis.status, StatusCode::OK);

    let publish_genesis = client::post_with(
        &env,
        "/api/control-factors/publications/publish",
        &admin,
        &[(REQUEST_ID, "gov-publish-genesis")],
        json!({
            "factor_ids": [genesis_factor.to_string()],
            "idempotency_key": "web-publish-genesis",
            "expires_at": expires,
            "manual_risk_expansion_approval": true,
            "reason": "publish genesis"
        }),
    )
    .await;
    assert_eq!(publish_genesis.status, StatusCode::OK);
    let genesis_pub_id = publish_genesis.json()["data"]["publication_id"]
        .as_str()
        .expect("genesis publication id")
        .to_owned();

    let shadow_successor = client::post_with(
        &env,
        "/api/control-factors/publications/shadow",
        &admin,
        &[(REQUEST_ID, "gov-shadow-successor")],
        json!({
            "factor_ids": [successor_factor.to_string()],
            "idempotency_key": "web-shadow-successor",
            "expires_at": expires,
            "reason": "shadow successor"
        }),
    )
    .await;
    assert_eq!(shadow_successor.status, StatusCode::OK);

    let publish_successor = client::post_with(
        &env,
        "/api/control-factors/publications/publish",
        &admin,
        &[(REQUEST_ID, "gov-publish-successor")],
        json!({
            "factor_ids": [successor_factor.to_string()],
            "idempotency_key": "web-publish-successor",
            "expires_at": expires,
            "manual_risk_expansion_approval": true,
            "reason": "publish successor"
        }),
    )
    .await;
    assert_eq!(publish_successor.status, StatusCode::OK);
    let successor_pub_id = publish_successor.json()["data"]["publication_id"]
        .as_str()
        .expect("successor publication id")
        .to_owned();

    let rollback = client::post_with(
        &env,
        &format!("/api/control-factors/publications/{successor_pub_id}/rollback"),
        &admin,
        &[(REQUEST_ID, "gov-rollback-http")],
        json!({
            "target_publication_id": genesis_pub_id,
            "reason": "rollback via http"
        }),
    )
    .await;
    assert_eq!(rollback.status, StatusCode::OK);
    assert_eq!(
        rollback.json()["data"]["publication_id"].as_str(),
        Some(genesis_pub_id.as_str())
    );

    let audit = client::get(&env, "/api/control-factors/audit", &admin).await;
    assert_eq!(audit.json()["data"]["verified"], json!(true));

    let rows = client::wait_for_oplog(&env, &admin, "gov-rollback-http").await;
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0]["action"], "publication.rollback");
}
