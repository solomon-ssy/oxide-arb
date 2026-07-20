//! Account snapshot + live venue read API tests.

use actix_web::{
    http::{StatusCode, header::AUTHORIZATION},
    test::TestRequest,
};
use chrono::Utc;
use quant_pivot_models::{
    domain::{NewAccountSnapshot, NewEquitySnapshot},
    enums::quant::AccountSource,
    types::{AccountPositions, AccountSnapshotId, EquitySnapshotId, ExposureBreakdown, Usd},
};
use quant_pivot_repository::{
    postgres::{PgAccountSnapshotRepository, PgEquitySnapshotRepository},
    traits::{AccountSnapshotRepository, EquitySnapshotRepository},
};
use rust_decimal_macros::dec;
use serde_json::json;

use crate::harness::{self, API_VERSION, TestEnv};

fn new_equity_snapshot(id: EquitySnapshotId, capital: Usd) -> NewEquitySnapshot {
    NewEquitySnapshot {
        equity_snapshot_id: id,
        as_of: Utc::now(),
        source: AccountSource::Polymarket,
        venue_net_liquidation_usd: capital,
        capital_base_usd: capital,
        available_usd: capital,
        reserved_usd: Usd::ZERO,
        realized_pnl_cumulative_usd: Usd::ZERO,
        unrealized_pnl_usd: Usd::ZERO,
        high_water_mark_usd: capital,
        drawdown_pct: dec!(0),
        account_snapshot_ref: None,
    }
}

fn bearer(token: &str) -> (actix_web::http::header::HeaderName, String) {
    (AUTHORIZATION, format!("Bearer {token}"))
}

async fn admin_token(env: &TestEnv) -> String {
    let req = TestRequest::post()
        .uri("/api/auth/login")
        .insert_header(API_VERSION)
        .set_json(json!({ "username": "admin", "password": "admin" }));
    let res = harness::call(&env.state, req).await;
    assert_eq!(res.status, StatusCode::OK);
    res.json()["data"]["access_token"]
        .as_str()
        .expect("access_token")
        .to_owned()
}

#[tokio::test]
async fn account_snapshot_not_found_returns_404() {
    let env = TestEnv::start().await;
    let admin = admin_token(&env).await;
    let id = AccountSnapshotId::from_v7();
    let req = TestRequest::get()
        .uri(&format!("/api/quant/account/snapshots/{id}"))
        .insert_header(API_VERSION)
        .insert_header(bearer(&admin));
    let res = harness::call(&env.state, req).await;
    assert_eq!(res.status, StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn account_live_returns_venue_snapshot() {
    let env = TestEnv::start().await;
    let admin = admin_token(&env).await;
    let req = TestRequest::get()
        .uri("/api/quant/account/live")
        .insert_header(API_VERSION)
        .insert_header(bearer(&admin));
    let res = harness::call(&env.state, req).await;
    assert_eq!(res.status, StatusCode::OK);
    assert!(res.json()["data"]["fetched_at"].is_string());
    assert_eq!(
        res.json()["data"]["source"],
        serde_json::json!("polymarket")
    );
}

#[tokio::test]
async fn account_snapshot_round_trips_persisted_row() {
    let env = TestEnv::start().await;
    let admin = admin_token(&env).await;
    let repo = PgAccountSnapshotRepository::new(env.db.clone());
    let snapshot_id = AccountSnapshotId::from_v7();
    let as_of = Utc::now();
    repo.create(NewAccountSnapshot {
        account_snapshot_id: snapshot_id.clone(),
        as_of,
        source: AccountSource::Polymarket,
        venue_net_liquidation_usd: Usd::new(dec!(10000)),
        capital_base_usd: Usd::new(dec!(10000)),
        available_usd: Usd::new(dec!(8000)),
        reserved_usd: Usd::new(dec!(2000)),
        positions_json: AccountPositions(Vec::new()),
        exposures_json: ExposureBreakdown::default(),
    })
    .await
    .expect("insert snapshot");

    let req = TestRequest::get()
        .uri(&format!("/api/quant/account/snapshots/{snapshot_id}"))
        .insert_header(API_VERSION)
        .insert_header(bearer(&admin));
    let res = harness::call(&env.state, req).await;
    assert_eq!(res.status, StatusCode::OK);
    assert_eq!(
        res.json()["data"]["capital_base_usd"],
        serde_json::json!("10000")
    );
    assert_eq!(
        res.json()["data"]["source"],
        serde_json::json!("polymarket")
    );
}

#[actix_web::test]
#[ignore = "requires Docker"]
async fn equity_snapshot_not_found_returns_404() {
    let env = TestEnv::start().await;
    let admin = admin_token(&env).await;
    let id = EquitySnapshotId::from_v7();
    let req = TestRequest::get()
        .uri(&format!("/api/quant/account/equity-snapshots/{id}"))
        .insert_header(API_VERSION)
        .insert_header(bearer(&admin));
    let res = harness::call(&env.state, req).await;
    assert_eq!(res.status, StatusCode::NOT_FOUND);
}

#[actix_web::test]
#[ignore = "requires Docker"]
async fn latest_equity_snapshot_not_found_returns_404() {
    let env = TestEnv::start().await;
    let admin = admin_token(&env).await;
    let req = TestRequest::get()
        .uri("/api/quant/account/equity-snapshots/latest")
        .insert_header(API_VERSION)
        .insert_header(bearer(&admin));
    let res = harness::call(&env.state, req).await;
    assert_eq!(res.status, StatusCode::NOT_FOUND);
}

#[actix_web::test]
#[ignore = "requires Docker"]
async fn equity_snapshot_round_trips_persisted_row() {
    let env = TestEnv::start().await;
    let admin = admin_token(&env).await;
    let repo = PgEquitySnapshotRepository::new(env.db.clone());
    let snapshot_id = EquitySnapshotId::from_v7();
    repo.create(new_equity_snapshot(
        snapshot_id.clone(),
        Usd::new(dec!(12000)),
    ))
    .await
    .expect("insert equity snapshot");

    let by_id = TestRequest::get()
        .uri(&format!(
            "/api/quant/account/equity-snapshots/{snapshot_id}"
        ))
        .insert_header(API_VERSION)
        .insert_header(bearer(&admin));
    let res = harness::call(&env.state, by_id).await;
    assert_eq!(res.status, StatusCode::OK);
    assert_eq!(
        res.json()["data"]["capital_base_usd"],
        serde_json::json!("12000")
    );

    let latest = TestRequest::get()
        .uri("/api/quant/account/equity-snapshots/latest")
        .insert_header(API_VERSION)
        .insert_header(bearer(&admin));
    let res = harness::call(&env.state, latest).await;
    assert_eq!(res.status, StatusCode::OK);
    assert_eq!(
        res.json()["data"]["equity_snapshot_id"],
        serde_json::json!(snapshot_id.to_string())
    );

    let list = TestRequest::get()
        .uri("/api/quant/account/equity-snapshots?page=1&size=10")
        .insert_header(API_VERSION)
        .insert_header(bearer(&admin));
    let res = harness::call(&env.state, list).await;
    assert_eq!(res.status, StatusCode::OK);
    assert_eq!(res.json()["data"]["total"], json!(1));
}
