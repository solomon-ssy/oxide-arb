//! Phase 0 route regression tests.

use actix_web::http::StatusCode;

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
