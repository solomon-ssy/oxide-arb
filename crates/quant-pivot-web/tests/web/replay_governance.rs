//! Governed replay enqueue integration tests (`ActingRoleGoverned` on
//! `POST /replay`).

use actix_web::http::StatusCode;
use serde_json::json;

use crate::{
    client,
    harness::TestEnv,
    headers::{ACTING_ROLE, REQUEST_ID},
};

const REPLAY_GOV_ROLE: &str = "replay_gov_op";

async fn replay_operator_token(env: &TestEnv, admin: &str) -> String {
    let role_id = client::create_role(env, admin, REPLAY_GOV_ROLE).await;
    client::grant_permissions(
        env,
        admin,
        &role_id,
        json!([{ "resource": "replay", "operation": "create" }]),
    )
    .await;
    let user_id = client::create_user(env, admin, "replaygov1", "replaygov1-pass").await;
    client::assign_roles(env, admin, &user_id, &[&role_id]).await;
    client::login(env, "replaygov1", "replaygov1-pass").await
}

fn replay_body() -> serde_json::Value {
    json!({
        "from": "2026-06-01T00:00:00Z",
        "to": "2026-06-02T00:00:00Z",
        "requested_factor_types": ["execution_quality"],
        "reason": "governed replay smoke"
    })
}

#[actix_web::test]
#[ignore = "requires Docker"]
async fn replay_enqueue_enforces_acting_role_for_non_super_admin() {
    let env = TestEnv::start().await;
    let admin = client::login(&env, "admin", "admin").await;
    let operator = replay_operator_token(&env, &admin).await;
    let body = replay_body();

    assert_eq!(
        client::post(&env, "/api/replay", &operator, body.clone())
            .await
            .status,
        StatusCode::BAD_REQUEST,
        "missing X-Acting-Role"
    );
    assert_eq!(
        client::post_with(
            &env,
            "/api/replay",
            &operator,
            &[(ACTING_ROLE, "super_admin")],
            body.clone(),
        )
        .await
        .status,
        StatusCode::FORBIDDEN,
        "acting as unheld role"
    );

    // Handler reaches the replay port (mock returns 500-class engine error) only
    // after authz passes — we assert not 400/403.
    let res = client::post_with(
        &env,
        "/api/replay",
        &operator,
        &[
            (REQUEST_ID, "replay-enqueue-gov"),
            (ACTING_ROLE, REPLAY_GOV_ROLE),
        ],
        body,
    )
    .await;
    assert!(
        !matches!(res.status, StatusCode::BAD_REQUEST | StatusCode::FORBIDDEN),
        "authz must pass before handler; got {}",
        res.status
    );

    let rows = client::wait_for_oplog(&env, &admin, "replay-enqueue-gov").await;
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0]["acting_role"], REPLAY_GOV_ROLE);
}
