//! Governance versioned runtime-config integration tests.
//!
//! Verifies configuration changes flow only through immutable, audited versions
//! (create → activate → rollback), that there is no bare in-place mutation
//! route, and that governed mutations enforce the acting-role contract for
//! non-super-admin callers.

use actix_web::{http::StatusCode, test::TestRequest};
use serde_json::json;

use crate::{
    client,
    harness::{self, API_VERSION, TestEnv},
    headers::ACTING_ROLE,
};

/// Create a runtime-config version as admin and return its id.
async fn create_version(env: &TestEnv, admin: &str, mode: &str) -> String {
    let res = client::post(
        env,
        "/api/runtime-config/versions",
        admin,
        json!({ "config_json": { "mode": mode }, "reason": format!("set {mode}") }),
    )
    .await;
    assert_eq!(res.status, StatusCode::OK, "create version {mode}");
    res.json()["data"]["runtime_config_version_id"]
        .as_str()
        .expect("version id")
        .to_owned()
}

#[actix_web::test]
#[ignore = "requires Docker"]
async fn create_activate_and_read_current() {
    let env = TestEnv::start().await;
    let admin = client::login(&env, "admin", "admin").await;

    let version_id = create_version(&env, &admin, "live").await;

    let activate = client::post(
        &env,
        &format!("/api/runtime-config/versions/{version_id}/activate"),
        &admin,
        json!({ "reason": "go live" }),
    )
    .await;
    assert_eq!(activate.status, StatusCode::OK);

    let current = client::get(&env, "/api/runtime-config", &admin).await;
    assert_eq!(current.status, StatusCode::OK);
    assert_eq!(
        current.json()["data"]["runtime_config_version_id"],
        json!(version_id)
    );
    assert_eq!(current.json()["data"]["config_json"]["mode"], "live");

    let versions = client::get(&env, "/api/runtime-config/versions", &admin).await;
    assert_eq!(versions.status, StatusCode::OK);
    let listed = versions.json()["data"]
        .as_array()
        .expect("versions array")
        .iter()
        .any(|v| v["runtime_config_version_id"] == json!(version_id));
    assert!(listed, "created version must appear in the catalog");
}

#[actix_web::test]
#[ignore = "requires Docker"]
async fn rollback_restores_previous_version() {
    let env = TestEnv::start().await;
    let admin = client::login(&env, "admin", "admin").await;

    let v1 = create_version(&env, &admin, "conservative").await;
    client::post(
        &env,
        &format!("/api/runtime-config/versions/{v1}/activate"),
        &admin,
        json!({ "reason": "v1" }),
    )
    .await;
    let v2 = create_version(&env, &admin, "aggressive").await;
    client::post(
        &env,
        &format!("/api/runtime-config/versions/{v2}/activate"),
        &admin,
        json!({ "reason": "v2" }),
    )
    .await;
    assert_eq!(
        client::get(&env, "/api/runtime-config", &admin)
            .await
            .json()["data"]["runtime_config_version_id"],
        json!(v2)
    );

    let rollback = client::post(
        &env,
        &format!("/api/runtime-config/versions/{v1}/rollback"),
        &admin,
        json!({ "reason": "revert to conservative" }),
    )
    .await;
    assert_eq!(rollback.status, StatusCode::OK);
    assert_eq!(rollback.json()["data"]["activation_kind"], "rollback");
    assert_eq!(
        client::get(&env, "/api/runtime-config", &admin)
            .await
            .json()["data"]["runtime_config_version_id"],
        json!(v1)
    );
}

#[actix_web::test]
#[ignore = "requires Docker"]
async fn create_version_requires_reason() {
    let env = TestEnv::start().await;
    let admin = client::login(&env, "admin", "admin").await;

    let res = client::post(
        &env,
        "/api/runtime-config/versions",
        &admin,
        json!({ "config_json": { "mode": "live" }, "reason": "" }),
    )
    .await;
    assert_eq!(res.status, StatusCode::BAD_REQUEST);
}

#[actix_web::test]
#[ignore = "requires Docker"]
async fn no_bare_patch_config_route_exists() {
    let env = TestEnv::start().await;
    let admin = client::login(&env, "admin", "admin").await;

    // Legacy in-place hot reload is gone. There is no actix handler for
    // `/api/config`, but authenticated requests still enter the protected scope;
    // authz fails closed when `match_pattern()` is missing → 403 (not 404).
    let patch = TestRequest::patch()
        .uri("/api/config")
        .insert_header(API_VERSION)
        .insert_header((
            actix_web::http::header::AUTHORIZATION,
            format!("Bearer {admin}"),
        ))
        .set_json(json!({ "anything": true }));
    assert_eq!(
        harness::call(&env.state, patch).await.status,
        StatusCode::FORBIDDEN
    );

    // Same fail-closed behaviour for a non-privileged actor (not super_admin bypass).
    let role_id = client::create_role(&env, &admin, "config_probe").await;
    client::grant_permissions(
        &env,
        &admin,
        &role_id,
        json!([{ "resource": "runtime_config", "operation": "read" }]),
    )
    .await;
    let user_id = client::create_user(&env, &admin, "cfg_probe", "password123").await;
    client::assign_roles(&env, &admin, &user_id, &[&role_id]).await;
    let reader = client::login(&env, "cfg_probe", "password123").await;

    let patch = TestRequest::patch()
        .uri("/api/config")
        .insert_header(API_VERSION)
        .insert_header((
            actix_web::http::header::AUTHORIZATION,
            format!("Bearer {reader}"),
        ))
        .set_json(json!({ "anything": true }));
    assert_eq!(
        harness::call(&env.state, patch).await.status,
        StatusCode::FORBIDDEN
    );

    // The governed replacement path remains reachable (catalog list, no activation required).
    assert_eq!(
        client::get(&env, "/api/runtime-config/versions", &reader)
            .await
            .status,
        StatusCode::OK
    );
}

#[actix_web::test]
#[ignore = "requires Docker"]
async fn governed_create_enforces_acting_role_for_non_super_admin() {
    let env = TestEnv::start().await;
    let admin = client::login(&env, "admin", "admin").await;

    // A role that may create runtime-config versions.
    let role_id = client::create_role(&env, &admin, "config_author").await;
    client::grant_permissions(
        &env,
        &admin,
        &role_id,
        json!([{ "resource": "runtime_config", "operation": "create" }]),
    )
    .await;
    let user_id = client::create_user(&env, &admin, "cfg1", "password123").await;
    client::assign_roles(&env, &admin, &user_id, &[&role_id]).await;
    let author = client::login(&env, "cfg1", "password123").await;

    let body = json!({ "config_json": { "mode": "live" }, "reason": "governed" });

    // Missing X-Acting-Role → 400.
    assert_eq!(
        client::post(&env, "/api/runtime-config/versions", &author, body.clone())
            .await
            .status,
        StatusCode::BAD_REQUEST
    );
    // Acting as a role the caller does not hold → 403.
    assert_eq!(
        client::post_with(
            &env,
            "/api/runtime-config/versions",
            &author,
            &[(ACTING_ROLE, "super_admin")],
            body.clone(),
        )
        .await
        .status,
        StatusCode::FORBIDDEN
    );
    // Acting as the held, permitted role → 200.
    assert_eq!(
        client::post_with(
            &env,
            "/api/runtime-config/versions",
            &author,
            &[(ACTING_ROLE, "config_author")],
            body,
        )
        .await
        .status,
        StatusCode::OK
    );
}
