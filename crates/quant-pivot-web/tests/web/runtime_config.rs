//! Governance versioned runtime-config integration tests.
//!
//! Verifies configuration changes flow only through immutable, audited
//! versions (typed create → preflight → activate → live apply → rollback),
//! that invalid documents are rejected fail-closed, that sensitive
//! notification credentials are masked on every read, and that governed
//! mutations enforce the acting-role contract for non-super-admin callers.

use actix_web::{http::StatusCode, test::TestRequest};
use oxide_arb_models::{
    domain::RuntimeConfigPort, runtime_config::RuntimeConfig, types::RuntimeConfigVersionId,
};
use rust_decimal_macros::dec;
use serde_json::json;

use crate::{
    client,
    harness::{self, API_VERSION, TestEnv},
    headers::ACTING_ROLE,
};

/// Create a runtime-config version with the given daily-loss cap (a valid
/// partial document — defaults fill the rest) and return its id. The weekly
/// cap is raised alongside so the daily ≤ weekly cross-check holds for every
/// value the tests use.
async fn create_version(env: &TestEnv, admin: &str, max_daily_loss_usd: &str) -> String {
    let res = client::post(
        env,
        "/api/runtime-config/versions",
        admin,
        json!({
            "config_json": {
                "risk": {
                    "max_daily_loss_usd": max_daily_loss_usd,
                    "max_weekly_loss_usd": "1000",
                }
            },
            "reason": format!("daily loss cap {max_daily_loss_usd}"),
        }),
    )
    .await;
    assert_eq!(
        res.status,
        StatusCode::OK,
        "create version {max_daily_loss_usd}"
    );
    res.json()["data"]["runtime_config_version_id"]
        .as_str()
        .expect("version id")
        .to_owned()
}

#[actix_web::test]
#[ignore = "requires Docker"]
async fn create_activate_applies_to_live_config() {
    let env = TestEnv::start().await;
    let admin = client::login(&env, "admin", "admin").await;

    let version_id = create_version(&env, &admin, "150").await;

    let activate = client::post(
        &env,
        &format!("/api/runtime-config/versions/{version_id}/activate"),
        &admin,
        json!({ "reason": "raise the daily loss cap" }),
    )
    .await;
    assert_eq!(activate.status, StatusCode::OK);

    // GET /runtime-config returns the *applied* live snapshot, proving the
    // activation propagated through the apply port, not just the DB.
    let current = client::get(&env, "/api/runtime-config", &admin).await;
    assert_eq!(current.status, StatusCode::OK);
    assert_eq!(
        current.json()["data"]["version"]["runtime_config_version_id"],
        json!(version_id)
    );
    assert_eq!(
        current.json()["data"]["config"]["risk"]["max_daily_loss_usd"],
        json!("150")
    );

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

    let v1 = create_version(&env, &admin, "60").await;
    client::post(
        &env,
        &format!("/api/runtime-config/versions/{v1}/activate"),
        &admin,
        json!({ "reason": "v1" }),
    )
    .await;
    let v2 = create_version(&env, &admin, "200").await;
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
            .json()["data"]["version"]["runtime_config_version_id"],
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
    let current = client::get(&env, "/api/runtime-config", &admin).await;
    assert_eq!(
        current.json()["data"]["version"]["runtime_config_version_id"],
        json!(v1)
    );
    assert_eq!(
        current.json()["data"]["config"]["risk"]["max_daily_loss_usd"],
        json!("60"),
        "rollback must re-apply the previous config to the live system"
    );
}

#[actix_web::test]
#[ignore = "requires Docker"]
async fn activate_rejects_exposure_tightening_below_reserved_capital() {
    let env = TestEnv::start().await;
    let admin = client::login(&env, "admin", "admin").await;

    // A version that tightens the total exposure ceiling to 600 USD. Creating
    // it succeeds — versions are validated semantically, not against live
    // money state.
    let res = client::post(
        &env,
        "/api/runtime-config/versions",
        &admin,
        json!({
            "config_json": { "risk": { "max_total_exposure_usd": "600" } },
            "reason": "tighten exposure ceiling",
        }),
    )
    .await;
    assert_eq!(res.status, StatusCode::OK);
    let version_id = res.json()["data"]["runtime_config_version_id"]
        .as_str()
        .expect("version id")
        .to_owned();

    // 700 USD is already committed in-flight: activation must fail closed.
    env.runtime_config_apply.set_reserved(dec!(700), dec!(400));
    let activate = client::post(
        &env,
        &format!("/api/runtime-config/versions/{version_id}/activate"),
        &admin,
        json!({ "reason": "tighten below reserved" }),
    )
    .await;
    assert_eq!(
        activate.status,
        StatusCode::CONFLICT,
        "preflight precondition failure must map to 409: {}",
        activate.json()
    );

    // The live system keeps running on the previous configuration.
    let current = client::get(&env, "/api/runtime-config", &admin).await;
    assert_eq!(
        current.json()["data"]["config"]["risk"]["max_total_exposure_usd"],
        json!("5000"),
        "rejected activation must not touch the live config"
    );

    // Once the committed capital unwinds, the same version activates cleanly.
    env.runtime_config_apply.set_reserved(dec!(0), dec!(0));
    let retry = client::post(
        &env,
        &format!("/api/runtime-config/versions/{version_id}/activate"),
        &admin,
        json!({ "reason": "reservations unwound" }),
    )
    .await;
    assert_eq!(retry.status, StatusCode::OK);
    let current = client::get(&env, "/api/runtime-config", &admin).await;
    assert_eq!(
        current.json()["data"]["config"]["risk"]["max_total_exposure_usd"],
        json!("600")
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
        json!({ "config_json": {}, "reason": "" }),
    )
    .await;
    assert_eq!(res.status, StatusCode::BAD_REQUEST);
}

#[actix_web::test]
#[ignore = "requires Docker"]
async fn create_version_rejects_unknown_and_invalid_fields() {
    let env = TestEnv::start().await;
    let admin = client::login(&env, "admin", "admin").await;

    // Unknown section (legacy/typo'd document) → typed parse failure → 400.
    let unknown = client::post(
        &env,
        "/api/runtime-config/versions",
        &admin,
        json!({ "config_json": { "treasury": {} }, "reason": "legacy" }),
    )
    .await;
    assert_eq!(unknown.status, StatusCode::BAD_REQUEST);

    // Semantically invalid (hourly cap above daily cap) → validation → 400.
    let invalid = client::post(
        &env,
        "/api/runtime-config/versions",
        &admin,
        json!({
            "config_json": {
                "risk": { "max_hourly_loss_usd": "500", "max_daily_loss_usd": "75" }
            },
            "reason": "inverted caps",
        }),
    )
    .await;
    assert_eq!(invalid.status, StatusCode::BAD_REQUEST);
}

#[actix_web::test]
#[ignore = "requires Docker"]
async fn notification_credentials_are_masked_on_read() {
    let env = TestEnv::start().await;
    let admin = client::login(&env, "admin", "admin").await;

    let res = client::post(
        &env,
        "/api/runtime-config/versions",
        &admin,
        json!({
            "config_json": {
                "notification": {
                    "telegram": { "enabled": true, "bot_token": "secret-token", "chat_id": "42" }
                }
            },
            "reason": "rotate telegram token",
        }),
    )
    .await;
    assert_eq!(res.status, StatusCode::OK);
    assert_eq!(
        res.json()["data"]["config_json"]["notification"]["telegram"]["bot_token"],
        json!("***"),
        "bot token must never round-trip in plaintext"
    );

    let versions = client::get(&env, "/api/runtime-config/versions", &admin).await;
    for version in versions.json()["data"].as_array().expect("versions") {
        let token = &version["config_json"]["notification"]["telegram"]["bot_token"];
        assert_ne!(token, &json!("secret-token"), "catalog leaks bot token");
    }
}

#[actix_web::test]
#[ignore = "requires Docker"]
async fn masked_credentials_round_trip_to_current_plaintext_on_create() {
    let env = TestEnv::start().await;
    let admin = client::login(&env, "admin", "admin").await;

    let seed = client::post(
        &env,
        "/api/runtime-config/versions",
        &admin,
        json!({
            "config_json": {
                "notification": {
                    "telegram": { "enabled": true, "bot_token": "seed-token", "chat_id": "42" },
                    "webhook": { "enabled": true, "url": "https://hooks.example/seed" }
                }
            },
            "reason": "seed notification credentials",
        }),
    )
    .await;
    assert_eq!(seed.status, StatusCode::OK);
    let seed_id = seed.json()["data"]["runtime_config_version_id"]
        .as_str()
        .expect("seed version id")
        .to_owned();
    let activate_seed = client::post(
        &env,
        &format!("/api/runtime-config/versions/{seed_id}/activate"),
        &admin,
        json!({ "reason": "activate credentials" }),
    )
    .await;
    assert_eq!(activate_seed.status, StatusCode::OK);

    let current = client::get(&env, "/api/runtime-config", &admin).await;
    let mut masked_config = current.json()["data"]["config"].clone();
    masked_config["risk"]["max_daily_loss_usd"] = json!("175");
    masked_config["risk"]["max_weekly_loss_usd"] = json!("1000");

    let derived = client::post(
        &env,
        "/api/runtime-config/versions",
        &admin,
        json!({
            "config_json": masked_config,
            "reason": "derive from masked UI document",
        }),
    )
    .await;
    assert_eq!(derived.status, StatusCode::OK);
    let derived_id = derived.json()["data"]["runtime_config_version_id"]
        .as_str()
        .expect("derived version id")
        .to_owned();
    let derived_version_id: RuntimeConfigVersionId =
        derived_id.parse().expect("runtime config version id");
    let stored = env
        .state
        .runtime_config
        .load_version(&derived_version_id)
        .await
        .expect("load derived version")
        .expect("derived version exists");
    let stored_config =
        RuntimeConfig::from_json(&stored.config_json).expect("stored config parses");

    assert_eq!(stored_config.notification.telegram.bot_token, "seed-token");
    assert_eq!(
        stored_config.notification.webhook.url,
        "https://hooks.example/seed"
    );
    assert_eq!(stored_config.risk.max_daily_loss_usd, dec!(175));
}

#[actix_web::test]
#[ignore = "requires Docker"]
async fn schema_endpoint_describes_money_critical_fields() {
    let env = TestEnv::start().await;
    let admin = client::login(&env, "admin", "admin").await;

    let res = client::get(&env, "/api/runtime-config/schema", &admin).await;
    assert_eq!(res.status, StatusCode::OK);
    let data = &res.json()["data"];
    let groups = data["groups"].as_array().expect("schema groups");
    let fields = data["fields"].as_array().expect("schema fields");
    assert!(!groups.is_empty());
    assert!(!fields.is_empty());
    let daily_loss = fields
        .iter()
        .find(|f| f["path"] == "risk.max_daily_loss_usd")
        .expect("risk.max_daily_loss_usd in schema");
    assert_eq!(daily_loss["money_critical"], json!(true));
    assert_eq!(daily_loss["label"]["kind"], json!("localized"));
    assert!(
        daily_loss["label"]["locales"]["en-US"]
            .as_str()
            .is_some_and(|s| !s.is_empty())
    );
    let standard_route = fields
        .iter()
        .find(|f| f["path"] == "settlement.redeem.standard.route")
        .expect("standard redeem route in schema");
    let neg_risk_route = fields
        .iter()
        .find(|f| f["path"] == "settlement.redeem.neg_risk.route")
        .expect("neg-risk redeem route in schema");
    let standard_enum_items = standard_route["enum_items"]
        .as_array()
        .expect("standard redeem route enum_items");
    let neg_risk_enum_items = neg_risk_route["enum_items"]
        .as_array()
        .expect("neg-risk redeem route enum_items");
    assert_eq!(standard_enum_items.len(), 2);
    assert_eq!(neg_risk_enum_items.len(), 2);
    assert!(
        fields.iter().all(|f| f["path"] != "schema_version"),
        "schema_version must not appear in preferences fields"
    );
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

/// A live apply failure after the durable activation committed must not leave
/// the active version pointing at a config the live system never adopted: the
/// handler compensates with an automatic durable rollback.
#[actix_web::test]
#[ignore = "requires Docker"]
async fn apply_failure_auto_reverts_durable_activation() {
    let env = TestEnv::start().await;
    let admin = client::login(&env, "admin", "admin").await;

    let v1 = create_version(&env, &admin, "60").await;
    let activate_v1 = client::post(
        &env,
        &format!("/api/runtime-config/versions/{v1}/activate"),
        &admin,
        json!({ "reason": "baseline" }),
    )
    .await;
    assert_eq!(activate_v1.status, StatusCode::OK);

    // The next apply fails *after* preflight passed and the durable
    // activation was written — the split-brain window.
    let v2 = create_version(&env, &admin, "200").await;
    env.runtime_config_apply.fail_next_apply();
    let activate_v2 = client::post(
        &env,
        &format!("/api/runtime-config/versions/{v2}/activate"),
        &admin,
        json!({ "reason": "will not propagate" }),
    )
    .await;
    assert_eq!(activate_v2.status, StatusCode::CONFLICT);
    assert!(
        activate_v2.json()["message"]
            .as_str()
            .expect("conflict message")
            .contains("automatically reverted"),
        "operator must learn the durable state was compensated: {}",
        activate_v2.json()
    );

    // Durable active version and live config agree on v1 again.
    let current = client::get(&env, "/api/runtime-config", &admin).await;
    assert_eq!(
        current.json()["data"]["version"]["runtime_config_version_id"],
        json!(v1),
        "durable activation must be auto-reverted to the previous version"
    );
    assert_eq!(
        current.json()["data"]["config"]["risk"]["max_daily_loss_usd"],
        json!("60"),
        "live system keeps the previous configuration"
    );

    // The failed version is intact and activates cleanly once apply recovers.
    let retry = client::post(
        &env,
        &format!("/api/runtime-config/versions/{v2}/activate"),
        &admin,
        json!({ "reason": "apply recovered" }),
    )
    .await;
    assert_eq!(retry.status, StatusCode::OK);
    let current = client::get(&env, "/api/runtime-config", &admin).await;
    assert_eq!(
        current.json()["data"]["config"]["risk"]["max_daily_loss_usd"],
        json!("200")
    );
}

/// `schema_version` other than the current schema must be rejected at the HTTP boundary —
/// there is no migration chain, so an unknown document shape is fail-closed.
#[actix_web::test]
#[ignore = "requires Docker"]
async fn schema_version_other_than_1_is_rejected() {
    let env = TestEnv::start().await;
    let admin = client::login(&env, "admin", "admin").await;

    let res = client::post(
        &env,
        "/api/runtime-config/versions",
        &admin,
        json!({
            "config_json": { "schema_version": 1 },
            "reason": "future schema",
        }),
    )
    .await;
    assert_eq!(res.status, StatusCode::BAD_REQUEST);
}

/// `webhook.url` is as sensitive as `bot_token`: both must be masked on the
/// live snapshot (`GET /runtime-config`) after activation, not only on the
/// version catalog.
#[actix_web::test]
#[ignore = "requires Docker"]
async fn webhook_url_and_bot_token_are_masked_on_live_snapshot() {
    let env = TestEnv::start().await;
    let admin = client::login(&env, "admin", "admin").await;

    let res = client::post(
        &env,
        "/api/runtime-config/versions",
        &admin,
        json!({
            "config_json": {
                "notification": {
                    "telegram": { "enabled": true, "bot_token": "tg-secret", "chat_id": "42" },
                    "webhook": { "enabled": true, "url": "https://hooks.example/secret-path" }
                }
            },
            "reason": "rotate notification credentials",
        }),
    )
    .await;
    assert_eq!(res.status, StatusCode::OK);
    assert_eq!(
        res.json()["data"]["config_json"]["notification"]["webhook"]["url"],
        json!("***"),
        "webhook url must never round-trip in plaintext"
    );
    let version_id = res.json()["data"]["runtime_config_version_id"]
        .as_str()
        .expect("version id")
        .to_owned();

    let activate = client::post(
        &env,
        &format!("/api/runtime-config/versions/{version_id}/activate"),
        &admin,
        json!({ "reason": "apply credentials" }),
    )
    .await;
    assert_eq!(activate.status, StatusCode::OK);

    let current = client::get(&env, "/api/runtime-config", &admin).await;
    let notification = &current.json()["data"]["config"]["notification"];
    assert_eq!(notification["telegram"]["bot_token"], json!("***"));
    assert_eq!(notification["webhook"]["url"], json!("***"));
    assert_eq!(
        notification["telegram"]["chat_id"],
        json!("42"),
        "non-sensitive notification fields stay readable"
    );
}

/// The deploy-config sibling surface is read-only and masked: key material is
/// presence flags only, Redis credentials are masked, and the JWT secret is
/// never echoed.
#[actix_web::test]
#[ignore = "requires Docker"]
async fn deploy_config_endpoint_is_masked_read_only() {
    let env = TestEnv::start().await;
    let admin = client::login(&env, "admin", "admin").await;

    let res = client::get(&env, "/api/system/deploy-config", &admin).await;
    assert_eq!(res.status, StatusCode::OK);
    // The harness deploy config carries a non-empty Redis secret; it must not
    // leak anywhere in the serialized response.
    let raw = String::from_utf8_lossy(res.body_bytes());
    assert!(
        !raw.contains("harness-redis-secret"),
        "configured redis secret must never be echoed in the response"
    );
    let data = res.json()["data"].clone();

    let keys = data["keys"].as_object().expect("keys section");
    for (name, value) in keys {
        if name != "source" {
            assert!(
                value.is_boolean(),
                "keys.{name} must be a presence flag, got {value}"
            );
        }
    }
    assert_eq!(data["cache"]["redis"]["password"], json!("***"));
    assert!(
        data["cache"]["redis"]["host"].is_string(),
        "non-sensitive redis fields stay readable"
    );
    let jwt_secret = data["web"]["jwt"]["secret"].as_str().expect("jwt secret");
    assert!(
        jwt_secret.is_empty() || jwt_secret == "***",
        "jwt secret must never be echoed: {jwt_secret}"
    );
    assert!(
        data["db"]["postgres"]["host"].is_string(),
        "non-sensitive deploy fields stay readable"
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

    let body = json!({ "config_json": {}, "reason": "governed" });

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

/// Activate and rollback are governed exactly like create: RBAC denies a
/// reader outright, the acting-role header is mandatory for non-super-admin
/// callers, and the held + permitted role succeeds.
#[actix_web::test]
#[ignore = "requires Docker"]
async fn governed_activate_and_rollback_enforce_acting_role_and_rbac() {
    let env = TestEnv::start().await;
    let admin = client::login(&env, "admin", "admin").await;

    // Two versions; v1 active so v2's activation has a rollback target.
    let v1 = create_version(&env, &admin, "60").await;
    client::post(
        &env,
        &format!("/api/runtime-config/versions/{v1}/activate"),
        &admin,
        json!({ "reason": "baseline" }),
    )
    .await;
    let v2 = create_version(&env, &admin, "200").await;

    // A reader may list but holds no activate/rollback permission.
    let reader_role = client::create_role(&env, &admin, "config_reader").await;
    client::grant_permissions(
        &env,
        &admin,
        &reader_role,
        json!([{ "resource": "runtime_config", "operation": "read" }]),
    )
    .await;
    let reader_id = client::create_user(&env, &admin, "cfg_reader", "password123").await;
    client::assign_roles(&env, &admin, &reader_id, &[&reader_role]).await;
    let reader = client::login(&env, "cfg_reader", "password123").await;

    let body = json!({ "reason": "governed transition" });
    assert_eq!(
        client::post_with(
            &env,
            &format!("/api/runtime-config/versions/{v2}/activate"),
            &reader,
            &[(ACTING_ROLE, "config_reader")],
            body.clone(),
        )
        .await
        .status,
        StatusCode::FORBIDDEN,
        "read-only role must not activate"
    );

    // An operator role holding activate + rollback.
    let operator_role = client::create_role(&env, &admin, "config_operator").await;
    client::grant_permissions(
        &env,
        &admin,
        &operator_role,
        json!([
            { "resource": "runtime_config", "operation": "activate" },
            { "resource": "runtime_config", "operation": "rollback" }
        ]),
    )
    .await;
    let operator_id = client::create_user(&env, &admin, "cfg_op", "password123").await;
    client::assign_roles(&env, &admin, &operator_id, &[&operator_role]).await;
    let operator = client::login(&env, "cfg_op", "password123").await;

    // Missing X-Acting-Role → 400.
    assert_eq!(
        client::post(
            &env,
            &format!("/api/runtime-config/versions/{v2}/activate"),
            &operator,
            body.clone(),
        )
        .await
        .status,
        StatusCode::BAD_REQUEST
    );
    // Acting as a role the caller does not hold → 403.
    assert_eq!(
        client::post_with(
            &env,
            &format!("/api/runtime-config/versions/{v2}/activate"),
            &operator,
            &[(ACTING_ROLE, "super_admin")],
            body.clone(),
        )
        .await
        .status,
        StatusCode::FORBIDDEN
    );
    // Held + permitted role → activate and rollback both succeed.
    assert_eq!(
        client::post_with(
            &env,
            &format!("/api/runtime-config/versions/{v2}/activate"),
            &operator,
            &[(ACTING_ROLE, "config_operator")],
            body.clone(),
        )
        .await
        .status,
        StatusCode::OK
    );
    assert_eq!(
        client::post_with(
            &env,
            &format!("/api/runtime-config/versions/{v1}/rollback"),
            &operator,
            &[(ACTING_ROLE, "config_operator")],
            body,
        )
        .await
        .status,
        StatusCode::OK
    );
}

#[actix_web::test]
#[ignore = "requires Docker"]
async fn sparse_patch_updates_only_changed_leaf() {
    let env = TestEnv::start().await;
    let admin = client::login(&env, "admin", "admin").await;

    let seed = client::post(
        &env,
        "/api/runtime-config/versions",
        &admin,
        json!({
            "config_json": {
                "risk": {
                    "max_daily_loss_usd": "100",
                    "max_weekly_loss_usd": "1000",
                },
                "notification": {
                    "telegram": { "bot_token": "seed-token", "chat_id": "1" }
                }
            },
            "reason": "seed",
        }),
    )
    .await;
    assert_eq!(seed.status, StatusCode::OK);
    let seed_id = seed.json()["data"]["runtime_config_version_id"]
        .as_str()
        .expect("seed version id")
        .to_owned();
    let activate_seed = client::post(
        &env,
        &format!("/api/runtime-config/versions/{seed_id}/activate"),
        &admin,
        json!({ "reason": "activate seed" }),
    )
    .await;
    assert_eq!(activate_seed.status, StatusCode::OK, "activate seed");
    assert_eq!(
        env.runtime_config_apply
            .current()
            .notification
            .telegram
            .bot_token,
        "seed-token",
        "live config must carry credentials after activate"
    );

    let patched = client::post(
        &env,
        "/api/runtime-config/versions",
        &admin,
        json!({
            "config_patch": {
                "risk.max_daily_loss_usd": "120"
            },
            "reason": "patch daily loss only",
        }),
    )
    .await;
    assert_eq!(patched.status, StatusCode::OK);
    let patched_config = patched.json()["data"]["config_json"].clone();
    assert_eq!(patched_config["risk"]["max_daily_loss_usd"], json!("120"));
    assert_eq!(
        patched_config["notification"]["telegram"]["bot_token"],
        json!("***")
    );
}

#[actix_web::test]
#[ignore = "requires Docker"]
async fn patch_and_json_are_mutually_exclusive() {
    let env = TestEnv::start().await;
    let admin = client::login(&env, "admin", "admin").await;
    let res = client::post(
        &env,
        "/api/runtime-config/versions",
        &admin,
        json!({
            "config_patch": { "risk.max_daily_loss_usd": "50" },
            "config_json": { "risk": { "max_daily_loss_usd": "50" } },
            "reason": "invalid",
        }),
    )
    .await;
    assert_eq!(res.status, StatusCode::BAD_REQUEST);
}
