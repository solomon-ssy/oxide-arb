//! Equity snapshot service integration tests (Postgres + testcontainers).

use std::sync::Arc;

use chrono::{Duration as ChronoDuration, Utc};
use quant_pivot_core::service::equity::{DrawdownProvider, EquitySnapshotService};
use quant_pivot_models::{
    domain::NewEquitySnapshot,
    enums::quant::AccountSource,
    types::{EquitySnapshotId, Usd},
};
use quant_pivot_repository::{
    postgres::{PgEquitySnapshotRepository, PgPositionRepository},
    traits::EquitySnapshotRepository,
};
use quant_pivot_research::portfolio::{AccountSnapshot, DrawdownState};
use quant_pivot_test_support::pg::setup_pg;
use rust_decimal_macros::dec;

fn account_snapshot(capital_base_usd: Usd, as_of: chrono::DateTime<Utc>) -> AccountSnapshot {
    AccountSnapshot::new(
        as_of,
        AccountSource::Polymarket,
        capital_base_usd,
        capital_base_usd,
        capital_base_usd,
        Usd::ZERO,
        Vec::new(),
    )
}

async fn seed_peak(db: &sea_orm::DatabaseConnection, capital: Usd, as_of: chrono::DateTime<Utc>) {
    let repo = PgEquitySnapshotRepository::new(db.clone());
    repo.create(NewEquitySnapshot {
        equity_snapshot_id: EquitySnapshotId::from_v7(),
        as_of,
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
    })
    .await
    .expect("seed peak equity snapshot");
}

#[tokio::test]
#[ignore = "requires Docker"]
async fn new_account_no_history_is_neutral_drawdown() {
    let (pool, _container) = setup_pg().await;
    let db = pool.connection().clone();
    let service = EquitySnapshotService::new(
        Arc::new(PgEquitySnapshotRepository::new(db.clone())),
        Arc::new(PgPositionRepository::new(db)),
    );
    let as_of = Utc::now();
    let account = account_snapshot(Usd::new(dec!(10000)), as_of);

    let resolution = service
        .resolve_drawdown_for_sizing(&account, DrawdownState::neutral())
        .await
        .expect("resolve drawdown");

    assert_eq!(resolution.drawdown_state.current_drawdown, dec!(0));
    assert_eq!(resolution.high_water_mark_usd, Usd::new(dec!(10000)));
}

#[tokio::test]
#[ignore = "requires Docker"]
async fn resolve_drawdown_re_read_picks_up_concurrent_history() {
    let (pool, _container) = setup_pg().await;
    let db = pool.connection().clone();
    let equity_repo =
        Arc::new(PgEquitySnapshotRepository::new(db.clone())) as Arc<dyn EquitySnapshotRepository>;
    let service = EquitySnapshotService::new(
        Arc::clone(&equity_repo),
        Arc::new(PgPositionRepository::new(db.clone())),
    );

    let as_of = Utc::now();
    let account = account_snapshot(Usd::new(dec!(8000)), as_of);
    let first = service
        .resolve_drawdown_for_sizing(&account, DrawdownState::neutral())
        .await
        .expect("first resolve");
    assert_eq!(first.drawdown_state.current_drawdown, dec!(0));

    // Another report commits equity history while this build is still running.
    seed_peak(&db, Usd::new(dec!(10000)), as_of - ChronoDuration::hours(1)).await;

    let second = service
        .resolve_drawdown_for_sizing(&account, first.drawdown_state)
        .await
        .expect("second resolve");
    assert_eq!(second.drawdown_state.current_drawdown, dec!(0.2));
}

#[tokio::test]
#[ignore = "requires Docker"]
async fn equity_snapshot_records_real_equity_and_pnl() {
    let (pool, _container) = setup_pg().await;
    let db = pool.connection().clone();
    let service = EquitySnapshotService::new(
        Arc::new(PgEquitySnapshotRepository::new(db.clone())),
        Arc::new(PgPositionRepository::new(db.clone())),
    );

    let as_of = Utc::now();
    let account = account_snapshot(Usd::new(dec!(9000)), as_of);
    let info = service
        .record_history_snapshot(&account)
        .await
        .expect("record worker snapshot");

    assert_eq!(info.capital_base_usd, Usd::new(dec!(9000)));
    assert_eq!(info.realized_pnl_cumulative_usd, Usd::ZERO);
    assert_eq!(info.drawdown_pct, dec!(0));
}
