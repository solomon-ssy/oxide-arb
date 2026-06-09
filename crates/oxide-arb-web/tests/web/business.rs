//! Phase 6.6c business read-route + authz integration tests.
//!
//! Validates that the markets / trades / pnl / analytics / replay routes are
//! registered, reachable, and correctly authorized against the seeded RBAC
//! matrix (read for every role; `Replay:Create` only for operator-class roles).

use actix_web::http::StatusCode;
use serde_json::json;
use uuid::Uuid;

use crate::{client, harness::TestEnv, headers::ACTING_ROLE};

#[actix_web::test]
#[ignore = "requires Docker"]
async fn business_read_routes_are_registered_and_authorized() {
    let env = TestEnv::start().await;
    let admin = client::login(&env, "admin", "admin").await;

    // Paginated list endpoints succeed on an empty database (empty page).
    let markets = client::get(&env, "/api/markets", &admin).await;
    assert_eq!(markets.status, StatusCode::OK, "GET /markets");
    assert!(
        markets.json()["data"]["items"].is_array(),
        "markets page has items array"
    );

    assert_eq!(
        client::get(&env, "/api/trades", &admin).await.status,
        StatusCode::OK,
        "GET /trades"
    );
    assert_eq!(
        client::get(&env, "/api/pnl/live", &admin).await.status,
        StatusCode::OK,
        "GET /pnl/live"
    );
    assert_eq!(
        client::get(&env, "/api/analytics/edge-distribution", &admin)
            .await
            .status,
        StatusCode::OK,
        "GET /analytics/edge-distribution"
    );

    // No report seeded yet — route registered + handler reached → 404.
    assert_eq!(
        client::get(&env, "/api/pnl/daily", &admin).await.status,
        StatusCode::NOT_FOUND,
        "GET /pnl/daily with no report"
    );

    // Unknown replay run → 404.
    let run = Uuid::now_v7();
    assert_eq!(
        client::get(&env, &format!("/api/replay/{run}"), &admin)
            .await
            .status,
        StatusCode::NOT_FOUND,
        "GET /replay/{{unknown}}"
    );
}

#[actix_web::test]
#[ignore = "requires Docker"]
async fn read_role_can_read_but_cannot_enqueue_replay() {
    let env = TestEnv::start().await;
    let admin = client::login(&env, "admin", "admin").await;

    let role_id = client::create_role(&env, &admin, "biz_reader").await;
    client::grant_permissions(
        &env,
        &admin,
        &role_id,
        json!([
            { "resource": "market", "operation": "read" },
            { "resource": "replay", "operation": "read" },
        ]),
    )
    .await;
    let user_id = client::create_user(&env, &admin, "bizreader", "bizreader-pass").await;
    client::assign_roles(&env, &admin, &user_id, &[&role_id]).await;
    let reader = client::login(&env, "bizreader", "bizreader-pass").await;

    // Granted read passes.
    assert_eq!(
        client::get(&env, "/api/markets", &reader).await.status,
        StatusCode::OK,
        "reader GET /markets"
    );

    let replay_body = json!({
        "from": "2026-06-01T00:00:00Z",
        "to": "2026-06-02T00:00:00Z",
        "requested_factor_types": ["execution_quality"],
        "reason": "authz smoke"
    });

    // Governed endpoint: missing `X-Acting-Role` → 400.
    assert_eq!(
        client::post(&env, "/api/replay", &reader, replay_body.clone())
            .await
            .status,
        StatusCode::BAD_REQUEST,
        "reader POST /replay without acting role"
    );

    // `Replay:Create` is not granted — authz denies even with acting role.
    let res = client::post_with(
        &env,
        "/api/replay",
        &reader,
        &[(ACTING_ROLE, "biz_reader")],
        replay_body,
    )
    .await;
    assert_eq!(
        res.status,
        StatusCode::FORBIDDEN,
        "reader POST /replay must be forbidden"
    );
}
