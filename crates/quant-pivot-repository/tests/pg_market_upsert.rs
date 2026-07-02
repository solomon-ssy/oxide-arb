//! Market catalog upsert integration tests (Postgres + testcontainers).
//!
//! Regression coverage for native enum columns on `ON CONFLICT DO UPDATE`, especially
//! nullable `fee_source` populated after the initial insert.

use chrono::Utc;
use quant_pivot_models::enums::common::MarketCategory;
use quant_pivot_models::enums::fee::FeeSource;
use quant_pivot_repository::postgres::PgEventRepository;
use quant_pivot_repository::traits::EventRepository;
use quant_pivot_repository::{postgres::PgMarketRepository, traits::MarketRepository};
use quant_pivot_test_support::{
    catalog_fixtures::{make_event, make_market},
    pg::setup_pg,
};
use rust_decimal::Decimal;

#[tokio::test]
#[ignore = "requires Docker"]
async fn market_upsert_conflict_updates_fee_source_enum() {
    let (pool, _container) = setup_pg().await;
    let db = pool.connection().clone();
    let event_repo = PgEventRepository::new(db.clone());
    let market_repo = PgMarketRepository::new(db);

    let event_id = "evt-fee-upsert";
    let market_id = "0xfeeupsertmarket";
    event_repo
        .upsert(make_event(
            event_id,
            "Fee Upsert Event",
            "fee-upsert-event",
            MarketCategory::Politics,
        ))
        .await
        .expect("seed event");

    let mut initial = make_market(
        market_id,
        event_id,
        "Will fee upsert work?",
        "fee-upsert-market",
        MarketCategory::Politics,
        None,
    );
    initial.fee_source = None;
    market_repo
        .upsert(initial)
        .await
        .expect("initial market insert");

    let mut updated = make_market(
        market_id,
        event_id,
        "Will fee upsert work?",
        "fee-upsert-market",
        MarketCategory::Politics,
        None,
    );
    updated.fee_rate = Some(Decimal::new(2, 2));
    updated.fee_exponent = Some(Decimal::ONE);
    updated.fee_taker_only = Some(true);
    updated.fee_source = Some(FeeSource::GammaFeeSchedule);
    updated.fee_observed_at = Some(Utc::now());

    market_repo
        .upsert_batch(vec![updated])
        .await
        .expect("conflict upsert with fee_source must cast enum");

    let persisted = market_repo
        .find_by_id(&quant_pivot_models::types::MarketId::new(market_id))
        .await
        .expect("reload market")
        .expect("market row");
    assert_eq!(persisted.fee_source, Some(FeeSource::GammaFeeSchedule));
    assert_eq!(persisted.fee_rate, Some(Decimal::new(2, 2)));
}
