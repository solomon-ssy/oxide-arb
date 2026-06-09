//! Governed risk-control integration tests (`ActingRoleGoverned` on circuit-
//! breaker reset and blacklist mutations).

use actix_web::http::StatusCode;
use serde_json::json;

use crate::{
    client,
    harness::TestEnv,
    headers::{ACTING_ROLE, REQUEST_ID},
};

const RISK_GOV_ROLE: &str = "risk_gov_op";
const TEST_MARKET_ID: &str = "0x1111111111111111111111111111111111111111111111111111111111111111";

async fn operator_token(env: &TestEnv, admin: &str) -> String {
    let role_id = client::create_role(env, admin, RISK_GOV_ROLE).await;
    client::grant_permissions(
        env,
        admin,
        &role_id,
        json!([
            { "resource": "risk", "operation": "reset" },
            { "resource": "blacklist", "operation": "create" },
            { "resource": "blacklist", "operation": "delete" },
        ]),
    )
    .await;
    let user_id = client::create_user(env, admin, "riskgov1", "riskgov1-pass").await;
    client::assign_roles(env, admin, &user_id, &[&role_id]).await;
    client::login(env, "riskgov1", "riskgov1-pass").await
}

#[actix_web::test]
#[ignore = "requires Docker"]
async fn circuit_breaker_reset_enforces_acting_role() {
    let env = TestEnv::start().await;
    let admin = client::login(&env, "admin", "admin").await;
    let operator = operator_token(&env, &admin).await;
    let body = json!({ "reason": "governed reset smoke" });

    assert_eq!(
        client::post(
            &env,
            "/api/risk/circuit-breaker/reset",
            &operator,
            body.clone()
        )
        .await
        .status,
        StatusCode::BAD_REQUEST,
        "missing X-Acting-Role"
    );
    assert_eq!(
        client::post_with(
            &env,
            "/api/risk/circuit-breaker/reset",
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
        "/api/risk/circuit-breaker/reset",
        &operator,
        &[(REQUEST_ID, "cb-reset-gov"), (ACTING_ROLE, RISK_GOV_ROLE)],
        body,
    )
    .await;
    assert_eq!(res.status, StatusCode::OK);

    let rows = client::wait_for_oplog(&env, &admin, "cb-reset-gov").await;
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0]["acting_role"], RISK_GOV_ROLE);
}

#[actix_web::test]
#[ignore = "requires Docker"]
async fn blacklist_mutations_enforce_acting_role() {
    let env = TestEnv::start().await;
    let admin = client::login(&env, "admin", "admin").await;
    let operator = operator_token(&env, &admin).await;

    let add_body = json!({
        "market_id": TEST_MARKET_ID,
        "reason": "manual"
    });
    assert_eq!(
        client::post(&env, "/api/risk/blacklist", &operator, add_body.clone())
            .await
            .status,
        StatusCode::BAD_REQUEST
    );
    let add = client::post_with(
        &env,
        "/api/risk/blacklist",
        &operator,
        &[(ACTING_ROLE, RISK_GOV_ROLE)],
        add_body,
    )
    .await;
    assert_eq!(add.status, StatusCode::OK);

    let remove_body = json!({ "reason": "governed removal" });
    let remove_path = format!("/api/risk/blacklist/{TEST_MARKET_ID}/remove");
    assert_eq!(
        client::post(&env, &remove_path, &operator, remove_body.clone())
            .await
            .status,
        StatusCode::BAD_REQUEST
    );
    let remove = client::post_with(
        &env,
        &remove_path,
        &operator,
        &[(REQUEST_ID, "bl-remove-gov"), (ACTING_ROLE, RISK_GOV_ROLE)],
        remove_body,
    )
    .await;
    assert_eq!(remove.status, StatusCode::OK);

    let rows = client::wait_for_oplog(&env, &admin, "bl-remove-gov").await;
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0]["acting_role"], RISK_GOV_ROLE);
}
