//! Account snapshot + live venue read API tests.

use actix_web::{
    http::{StatusCode, header::AUTHORIZATION},
    test::TestRequest,
};
use chrono::Utc;
use quant_pivot_models::{
    domain::NewAccountSnapshot,
    enums::quant::AccountSource,
    types::{AccountPositions, AccountSnapshotId, ExposureBreakdown, Usd},
};
use quant_pivot_repository::{
    postgres::PgAccountSnapshotRepository, traits::AccountSnapshotRepository,
};
use rust_decimal_macros::dec;
use serde_json::json;

use crate::harness::{self, API_VERSION, TestEnv};

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
        serde_json::json!("10000.00000000")
    );
    assert_eq!(
        res.json()["data"]["source"],
        serde_json::json!("polymarket")
    );
}
