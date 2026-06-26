//! Phase 0 route regression tests.

use actix_web::http::StatusCode;
use serde_json::json;

use crate::{client, harness::TestEnv};

#[actix_web::test]
#[ignore = "requires Docker"]
async fn legacy_opportunity_route_returns_not_found() {
    let env = TestEnv::start().await;
    let admin = client::login(&env, "admin", "admin").await;
    let res = client::get(&env, "/api/health", &admin).await;
    assert_eq!(res.status, StatusCode::FORBIDDEN);
}

#[actix_web::test]
#[ignore = "requires Docker"]
async fn legacy_trades_route_returns_not_found() {
    let env = TestEnv::start().await;
    let admin = client::login(&env, "admin", "admin").await;
    let res = client::get(&env, "/api/trades", &admin).await;
    assert_eq!(res.status, StatusCode::FORBIDDEN);
}

#[actix_web::test]
#[ignore = "requires Docker"]
async fn quant_mode_route_is_available() {
    let env = TestEnv::start().await;
    let admin = client::login(&env, "admin", "admin").await;
    let res = client::get(&env, "/api/system/quant-mode", &admin).await;
    assert_eq!(res.status, StatusCode::OK);
    assert_eq!(res.json()["data"]["mode"].as_str(), Some("report_only"));
}

#[actix_web::test]
#[ignore = "requires Docker"]
async fn kill_switch_status_route_is_available() {
    let env = TestEnv::start().await;
    let admin = client::login(&env, "admin", "admin").await;
    let res = client::get(&env, "/api/system/kill-switch", &admin).await;
    assert_eq!(res.status, StatusCode::OK);
    assert_eq!(res.json()["data"]["state"].as_str(), Some("closed"));
}

#[actix_web::test]
#[ignore = "requires Docker"]
async fn set_kill_switch_requires_acting_role() {
    let env = TestEnv::start().await;
    let admin = client::login(&env, "admin", "admin").await;
    // Governed (ActingRoleGoverned) write without `X-Acting-Role` is rejected.
    let res = client::post_with(
        &env,
        "/api/system/kill-switch",
        &admin,
        &[],
        json!({ "state": "execution_halted", "reason": "manual halt", "ack": false }),
    )
    .await;
    assert_eq!(res.status, StatusCode::FORBIDDEN);
}

#[actix_web::test]
#[ignore = "requires Docker"]
async fn set_kill_switch_with_halt_role_updates_state() {
    let env = TestEnv::start().await;
    let admin = client::login(&env, "admin", "admin").await;
    let res = client::post_with(
        &env,
        "/api/system/kill-switch",
        &admin,
        &[("X-Acting-Role", "admin")],
        json!({ "state": "execution_halted", "reason": "manual halt", "ack": false }),
    )
    .await;
    assert_eq!(res.status, StatusCode::OK);
    assert_eq!(
        res.json()["data"]["state"].as_str(),
        Some("execution_halted")
    );

    let status = client::get(&env, "/api/system/kill-switch", &admin).await;
    assert_eq!(
        status.json()["data"]["state"].as_str(),
        Some("execution_halted")
    );
}

#[actix_web::test]
#[ignore = "requires Docker"]
async fn system_status_exposes_kill_switch_projection() {
    let env = TestEnv::start().await;
    let admin = client::login(&env, "admin", "admin").await;
    let res = client::get(&env, "/api/system/status", &admin).await;
    assert_eq!(res.status, StatusCode::OK);
    // The execution_emergency placeholder was replaced by a real kill_switch view.
    assert!(res.json()["data"]["kill_switch"]["state"].is_string());
    assert!(res.json()["data"].get("execution_emergency").is_none());
}
