//! Phase 6.6c business read-route + authz integration tests.
//!
//! Validates that the markets / trades / pnl / analytics / replay routes are
//! registered, reachable, and correctly authorized against the seeded RBAC
//! matrix (read for every role; `Replay:Create` only for operator-class roles).

use actix_web::http::StatusCode;
use chrono::{NaiveDate, Utc};
use oxide_arb_models::{
    domain::{DailyReport, ReportRiskSummary, ReportTradeStats, SettledPnlStats, WeeklyReport},
    enums::report::ReportSchemaVersion,
    types::Usd,
};
use rust_decimal::Decimal;
use serde_json::json;
use uuid::Uuid;

use crate::{
    client,
    harness::TestEnv,
    headers::{ACTING_ROLE, REQUEST_ID},
};

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
    let balance = client::get(&env, "/api/system/balance", &admin).await;
    assert_eq!(balance.status, StatusCode::OK, "GET /system/balance");
    assert_eq!(
        balance.json()["data"]["source"],
        "simulated_dry_run",
        "system balance exposes the single money-state source"
    );
    assert_eq!(
        client::get(&env, "/api/analytics/edge-distribution", &admin)
            .await
            .status,
        StatusCode::OK,
        "GET /analytics/edge-distribution"
    );

    // Analytics report endpoints are chart-friendly on a fresh deployment.
    assert_eq!(
        client::get(&env, "/api/analytics/daily", &admin)
            .await
            .status,
        StatusCode::OK,
        "GET /analytics/daily with no report"
    );
    assert_eq!(
        client::get(&env, "/api/analytics/weekly", &admin)
            .await
            .status,
        StatusCode::OK,
        "GET /analytics/weekly with no report"
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
async fn system_mode_switch_is_governed_and_audited() {
    let env = TestEnv::start().await;
    let admin = client::login(&env, "admin", "admin").await;

    let bypass = client::post(
        &env,
        "/api/system/mode",
        &admin,
        json!({
            "mode": "paper",
            "reason": "paper smoke"
        }),
    )
    .await;
    assert_eq!(bypass.status, StatusCode::OK);
    assert_eq!(bypass.json()["data"]["from"], "dry_run");
    assert_eq!(bypass.json()["data"]["to"], "paper");

    let res = client::post_with(
        &env,
        "/api/system/mode",
        &admin,
        &[
            (ACTING_ROLE, "super_admin"),
            (REQUEST_ID, "system-mode-test"),
        ],
        json!({
            "mode": "dry_run",
            "reason": "back to dry run"
        }),
    )
    .await;
    assert_eq!(res.status, StatusCode::OK);
    assert_eq!(res.json()["data"]["from"], "paper");
    assert_eq!(res.json()["data"]["to"], "dry_run");

    let rows = client::wait_for_oplog(&env, &admin, "system-mode-test").await;
    assert!(
        rows.iter().any(|row| row["category"] == "system"
            && row["detail"]["target_mode"] == "dry_run"
            && row["acting_role"] == "super_admin"),
        "system.switch_mode operation log missing: {rows:?}"
    );
}

/// Minimal daily report payload carrying only the values the series projects.
fn daily_report_payload(date: NaiveDate, pnl: i64) -> serde_json::Value {
    let pnl = Usd::new(Decimal::from(pnl));
    let report = DailyReport {
        date,
        schema_version: ReportSchemaVersion::V1,
        generated_at: Utc::now(),
        period_start: date,
        period_end: date,
        settled_pnl: SettledPnlStats {
            realized_pnl: pnl,
            total_payout: Usd::ZERO,
            total_cost: Usd::ZERO,
            total_fees: Usd::ZERO,
            settled_position_count: 0,
            winning_position_count: 0,
            losing_position_count: 0,
            unsettled_position_count: 0,
            failed_accounting_count: 0,
            largest_single_profit: Usd::ZERO,
            largest_single_loss: Usd::ZERO,
        },
        execution: ReportTradeStats {
            trade_count: 0,
            success_count: 0,
            miss_count: 0,
            failed_count: 0,
            total_fill_cost: Usd::ZERO,
            total_fill_fees: Usd::ZERO,
            fill_expected_pnl: Usd::ZERO,
        },
        risk: ReportRiskSummary {
            daily_pnl: pnl,
            daily_loss: Usd::ZERO,
            weekly_loss: Usd::ZERO,
            total_exposure: Usd::ZERO,
            open_position_count: 0,
        },
        total_pnl: pnl,
        total_fees_paid: Usd::ZERO,
        total_gas_paid: Usd::ZERO,
        trade_count: 0,
        success_count: 0,
        miss_count: 0,
        largest_single_loss: Usd::ZERO,
        largest_single_profit: Usd::ZERO,
    };
    serde_json::to_value(&report).expect("daily report serializes")
}

/// Minimal weekly report payload carrying the latest summary card values.
fn weekly_report_payload(week_start: NaiveDate, week_end: NaiveDate) -> serde_json::Value {
    let report = WeeklyReport {
        week_start,
        week_end,
        schema_version: ReportSchemaVersion::V1,
        generated_at: Utc::now(),
        settled_pnl: SettledPnlStats {
            realized_pnl: Usd::ZERO,
            total_payout: Usd::ZERO,
            total_cost: Usd::ZERO,
            total_fees: Usd::ZERO,
            settled_position_count: 0,
            winning_position_count: 0,
            losing_position_count: 0,
            unsettled_position_count: 0,
            failed_accounting_count: 0,
            largest_single_profit: Usd::ZERO,
            largest_single_loss: Usd::ZERO,
        },
        execution: ReportTradeStats {
            trade_count: 0,
            success_count: 0,
            miss_count: 0,
            failed_count: 0,
            total_fill_cost: Usd::ZERO,
            total_fill_fees: Usd::ZERO,
            fill_expected_pnl: Usd::ZERO,
        },
        risk: ReportRiskSummary {
            daily_pnl: Usd::ZERO,
            daily_loss: Usd::ZERO,
            weekly_loss: Usd::ZERO,
            total_exposure: Usd::ZERO,
            open_position_count: 0,
        },
        daily_reports: Vec::new(),
    };
    serde_json::to_value(&report).expect("weekly report serializes")
}

#[actix_web::test]
#[ignore = "requires Docker"]
async fn analytics_reports_are_windowed_and_empty_safe() {
    let env = TestEnv::start().await;
    let admin = client::login(&env, "admin", "admin").await;
    let day = |d: u32| NaiveDate::from_ymd_opt(2026, 6, d).expect("valid date");

    let empty_daily = client::get(&env, "/api/analytics/daily", &admin).await;
    assert_eq!(empty_daily.status, StatusCode::OK, "empty daily is 200");
    assert_eq!(
        empty_daily.json()["data"]["points"]
            .as_array()
            .expect("daily points array")
            .len(),
        0,
        "no reports -> empty daily points"
    );

    let empty_weekly = client::get(&env, "/api/analytics/weekly", &admin).await;
    assert_eq!(empty_weekly.status, StatusCode::OK, "empty weekly is 200");
    assert!(empty_weekly.json()["data"].is_null(), "no report -> null");

    for (date, pnl) in [(day(3), -2), (day(1), 10), (day(2), 5)] {
        env.state
            .reports
            .save_daily(date, daily_report_payload(date, pnl))
            .await
            .expect("seed daily report");
    }
    env.state
        .reports
        .save_weekly(day(1), day(7), weekly_report_payload(day(1), day(7)))
        .await
        .expect("seed weekly report");

    let daily = client::get(
        &env,
        "/api/analytics/daily?from=2026-06-01T00:00:00Z&to=2026-06-03T23:59:59Z",
        &admin,
    )
    .await;
    assert_eq!(daily.status, StatusCode::OK, "GET /analytics/daily");
    let rows = daily.json()["data"]["points"]
        .as_array()
        .cloned()
        .expect("daily points");
    assert_eq!(rows.len(), 3, "window includes all seeded reports");
    let dates: Vec<&str> = rows
        .iter()
        .map(|row| row["date"].as_str().expect("date"))
        .collect();
    assert_eq!(
        dates,
        vec!["2026-06-01", "2026-06-02", "2026-06-03"],
        "daily reports are oldest first"
    );
    assert_eq!(rows[0]["daily_pnl"], "10");
    assert_eq!(rows[2]["cumulative_pnl"], "13");

    let weekly = client::get(&env, "/api/analytics/weekly", &admin).await;
    assert_eq!(weekly.status, StatusCode::OK, "GET /analytics/weekly");
    assert_eq!(weekly.json()["data"]["week_start"], "2026-06-01");
}

#[actix_web::test]
#[ignore = "requires Docker"]
async fn pnl_daily_series_is_ascending_bounded_and_accumulated() {
    let env = TestEnv::start().await;
    let admin = client::login(&env, "admin", "admin").await;

    // Empty database → 200 with an empty series, not a 404.
    let empty = client::get(&env, "/api/pnl/daily-series", &admin).await;
    assert_eq!(empty.status, StatusCode::OK, "empty series is 200");
    assert_eq!(
        empty.json()["data"]["points"]
            .as_array()
            .expect("points array")
            .len(),
        0,
        "no reports → empty points"
    );

    // Seed three daily reports out of order; the series must sort ascending.
    let day = |d: u32| NaiveDate::from_ymd_opt(2026, 6, d).expect("valid date");
    for (date, pnl) in [(day(3), -2), (day(1), 10), (day(2), 5)] {
        env.state
            .reports
            .save_daily(date, daily_report_payload(date, pnl))
            .await
            .expect("seed daily report");
    }

    let series = client::get(&env, "/api/pnl/daily-series?days=7", &admin).await;
    assert_eq!(series.status, StatusCode::OK, "GET /pnl/daily-series");
    let points = series.json()["data"]["points"]
        .as_array()
        .cloned()
        .expect("points array");
    assert_eq!(points.len(), 3, "all seeded days included");
    let dates: Vec<&str> = points
        .iter()
        .map(|p| p["date"].as_str().expect("date"))
        .collect();
    assert_eq!(
        dates,
        vec!["2026-06-01", "2026-06-02", "2026-06-03"],
        "ascending by date"
    );
    let cumulative: Vec<&str> = points
        .iter()
        .map(|p| p["total_pnl"].as_str().expect("total_pnl"))
        .collect();
    assert_eq!(cumulative, vec!["10", "15", "13"], "running window total");
    assert_eq!(points[2]["daily_pnl"], "-2", "per-day settled pnl");

    // `days` truncates to the most recent N reports, still ascending.
    let truncated = client::get(&env, "/api/pnl/daily-series?days=2", &admin).await;
    let truncated_points = truncated.json()["data"]["points"]
        .as_array()
        .cloned()
        .expect("points array");
    assert_eq!(truncated_points.len(), 2, "days=2 keeps two newest days");
    assert_eq!(truncated_points[0]["date"], "2026-06-02");
    assert_eq!(truncated_points[1]["date"], "2026-06-03");

    // Out-of-range `days` is rejected at the boundary.
    for invalid in ["0", "91"] {
        assert_eq!(
            client::get(
                &env,
                &format!("/api/pnl/daily-series?days={invalid}"),
                &admin
            )
            .await
            .status,
            StatusCode::BAD_REQUEST,
            "days={invalid} must be 400"
        );
    }
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
