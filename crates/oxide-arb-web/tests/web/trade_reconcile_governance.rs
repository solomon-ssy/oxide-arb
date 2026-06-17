//! Governed trade reconciliation integration tests (`ActingRoleGoverned` on
//! `POST /api/trades/{trade_id}/reconcile`).

use actix_web::http::StatusCode;
use oxide_arb_models::types::TradeId;
use serde_json::json;

use crate::{
    client,
    harness::TestEnv,
    headers::{ACTING_ROLE, REQUEST_ID},
};

const TRADE_RECON_ROLE: &str = "trade_recon_op";

async fn operator_token(env: &TestEnv, admin: &str) -> String {
    let role_id = client::create_role(env, admin, TRADE_RECON_ROLE).await;
    client::grant_permissions(
        env,
        admin,
        &role_id,
        json!([
            { "resource": "trade", "operation": "read" },
            { "resource": "trade", "operation": "update" },
        ]),
    )
    .await;
    let user_id = client::create_user(env, admin, "traderecon1", "traderecon1-pass").await;
    client::assign_roles(env, admin, &user_id, &[&role_id]).await;
    client::login(env, "traderecon1", "traderecon1-pass").await
}

async fn viewer_token(env: &TestEnv, admin: &str) -> String {
    let role_id = client::create_role(env, admin, "trade_viewer").await;
    client::grant_permissions(
        env,
        admin,
        &role_id,
        json!([{ "resource": "trade", "operation": "read" }]),
    )
    .await;
    let user_id = client::create_user(env, admin, "tradeview1", "tradeview1-pass").await;
    client::assign_roles(env, admin, &user_id, &[&role_id]).await;
    client::login(env, "tradeview1", "tradeview1-pass").await
}

#[actix_web::test]
#[ignore = "requires Docker"]
async fn reconciliation_list_requires_trade_read() {
    let env = TestEnv::start().await;
    let admin = client::login(&env, "admin", "admin").await;
    let viewer = viewer_token(&env, &admin).await;

    let res = client::get(&env, "/api/trades/reconciliation", &viewer).await;
    assert_eq!(res.status, StatusCode::OK);
    assert!(res.json()["data"]["items"].is_array());
}

#[actix_web::test]
#[ignore = "requires Docker"]
async fn reconcile_trade_enforces_acting_role_and_trade_update() {
    let env = TestEnv::start().await;
    let admin = client::login(&env, "admin", "admin").await;
    let operator = operator_token(&env, &admin).await;
    let viewer = viewer_token(&env, &admin).await;
    let trade_id = TradeId::from_v7();
    let path = format!("/api/trades/{trade_id}/reconcile");
    let body = json!({
        "resolution": "unresolvable",
        "note": "venue evidence exhausted after manual review",
    });

    assert_eq!(
        client::post(&env, &path, &viewer, body.clone())
            .await
            .status,
        StatusCode::FORBIDDEN,
        "viewer lacks trade:update"
    );

    assert_eq!(
        client::post(&env, &path, &operator, body.clone())
            .await
            .status,
        StatusCode::BAD_REQUEST,
        "missing X-Acting-Role"
    );

    assert_eq!(
        client::post_with(
            &env,
            &path,
            &operator,
            &[(ACTING_ROLE, "super_admin")],
            body.clone(),
        )
        .await
        .status,
        StatusCode::FORBIDDEN,
        "acting as unheld role"
    );

    let res = client::post_with(
        &env,
        &path,
        &operator,
        &[
            (REQUEST_ID, "trade-reconcile-gov"),
            (ACTING_ROLE, TRADE_RECON_ROLE),
        ],
        body,
    )
    .await;
    assert_eq!(
        res.status,
        StatusCode::CONFLICT,
        "harness control double returns false when trade is not pending"
    );

    let rows = client::wait_for_oplog(&env, &admin, "trade-reconcile-gov").await;
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0]["acting_role"], TRADE_RECON_ROLE);
    assert_eq!(rows[0]["action"], "trade.reconcile");
}
