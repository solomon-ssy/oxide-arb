//! Strategy-capital equity snapshot persistence system contracts.

use chrono::{DateTime, Duration as ChronoDuration, Utc};
use quant_pivot_models::{
    domain::quant::{NewEquitySnapshot, capital_drawdown},
    enums::quant::AccountSource,
    types::{AccountSnapshotId, EquitySnapshotId, Usd},
};
use quant_pivot_repository::{
    postgres::{PgEquitySnapshotRepository, PgExecutionSubmissionRepository, PgPositionRepository},
    traits::{EquitySnapshotRepository, PositionRepository},
};
use quant_pivot_system_tests::{
    postgres::setup_pg,
    support::execution_pg_seed::{
        close_position_full, enable_entry_admission_for_test, seed_approved_intent,
        seed_report_fixture,
    },
};
use rust_decimal_macros::dec;

fn new_equity_snapshot(
    as_of: DateTime<Utc>,
    capital_base_usd: Usd,
    high_water_mark_usd: Usd,
    account_snapshot_ref: Option<AccountSnapshotId>,
) -> NewEquitySnapshot {
    NewEquitySnapshot {
        equity_snapshot_id: EquitySnapshotId::from_v7(),
        as_of,
        source: AccountSource::Polymarket,
        venue_net_liquidation_usd: capital_base_usd,
        capital_base_usd,
        available_usd: capital_base_usd,
        reserved_usd: Usd::ZERO,
        realized_pnl_cumulative_usd: Usd::ZERO,
        unrealized_pnl_usd: Usd::ZERO,
        high_water_mark_usd,
        drawdown_pct: capital_drawdown(capital_base_usd, high_water_mark_usd),
        account_snapshot_ref,
    }
}

pub async fn equity_snapshot_repo_create_latest_hwm() {
    let (pool, _container) = setup_pg().await;
    let db = pool.connection().clone();
    let repo = PgEquitySnapshotRepository::new(db);

    let first = repo
        .create(new_equity_snapshot(
            Utc::now() - ChronoDuration::hours(2),
            Usd::new(dec!(10000)),
            Usd::new(dec!(10000)),
            None,
        ))
        .await
        .expect("first snapshot");
    assert_eq!(first.high_water_mark_usd, Usd::new(dec!(10000)));

    let latest = repo.latest().await.expect("latest").expect("row");
    assert_eq!(latest.equity_snapshot_id, first.equity_snapshot_id);
}

pub async fn high_water_mark_is_monotonic_max() {
    let (pool, _container) = setup_pg().await;
    let db = pool.connection().clone();
    let repo = PgEquitySnapshotRepository::new(db);

    repo.create(new_equity_snapshot(
        Utc::now() - ChronoDuration::hours(2),
        Usd::new(dec!(10000)),
        Usd::new(dec!(10000)),
        None,
    ))
    .await
    .expect("peak snapshot");

    let trough = repo
        .create(new_equity_snapshot(
            Utc::now() - ChronoDuration::hours(1),
            Usd::new(dec!(9000)),
            Usd::new(dec!(9000)),
            None,
        ))
        .await
        .expect("trough snapshot");
    assert_eq!(trough.high_water_mark_usd, Usd::new(dec!(10000)));
    assert_eq!(trough.drawdown_pct, dec!(0.1));

    let recovery = repo
        .create(new_equity_snapshot(
            Utc::now(),
            Usd::new(dec!(11000)),
            Usd::new(dec!(11000)),
            None,
        ))
        .await
        .expect("recovery snapshot");
    assert_eq!(recovery.high_water_mark_usd, Usd::new(dec!(11000)));
    assert_eq!(recovery.drawdown_pct, dec!(0));
}

pub async fn drawdown_pct_is_hwm_minus_equity_over_hwm() {
    let (pool, _container) = setup_pg().await;
    let db = pool.connection().clone();
    let repo = PgEquitySnapshotRepository::new(db);

    repo.create(new_equity_snapshot(
        Utc::now() - ChronoDuration::minutes(30),
        Usd::new(dec!(12500)),
        Usd::new(dec!(12500)),
        None,
    ))
    .await
    .expect("peak snapshot");

    let snapshot = repo
        .create(new_equity_snapshot(
            Utc::now(),
            Usd::new(dec!(10000)),
            Usd::new(dec!(10000)),
            None,
        ))
        .await
        .expect("drawdown snapshot");

    assert_eq!(snapshot.drawdown_pct, dec!(0.2));
}

pub async fn realized_pnl_cumulative_matches_position_ledger_sum() {
    let (pool, _container) = setup_pg().await;
    let db = pool.connection().clone();
    let positions = PgPositionRepository::new(db.clone());
    let submission = PgExecutionSubmissionRepository::new(db.clone());

    let ids = seed_report_fixture(&db).await;
    enable_entry_admission_for_test(&db, "pg-equity-snapshot-it-operator").await;
    let intent_id = seed_approved_intent(&db, &ids).await;
    close_position_full(&db, &submission, &ids, &intent_id, None).await;

    let cumulative = positions
        .realized_pnl_cumulative_usd()
        .await
        .expect("cumulative pnl");
    assert_eq!(cumulative, Usd::new(dec!(-5)));

    let open_lots = positions.find_open_lots().await.expect("open lots");
    assert!(open_lots.is_empty());
}
