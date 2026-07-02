//! Market catalog upsert integration tests (Postgres + testcontainers).
//!
//! Regression coverage for native enum columns on `ON CONFLICT DO UPDATE`, especially
//! nullable `fee_source` populated after the initial insert.

use chrono::Utc;
use quant_pivot_models::enums::common::MarketCategory;
use quant_pivot_models::enums::fee::FeeSource;
use quant_pivot_models::types::MarketId;
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
        .find_by_id(&MarketId::new(market_id))
        .await
        .expect("reload market")
        .expect("market row");
    assert_eq!(persisted.fee_source, Some(FeeSource::GammaFeeSchedule));
    assert_eq!(persisted.fee_rate, Some(Decimal::new(2, 2)));
}

#[tokio::test]
#[ignore = "requires Docker"]
async fn market_upsert_insert_with_fee_source_on_first_insert() {
    let (pool, _container) = setup_pg().await;
    let db = pool.connection().clone();
    let event_repo = PgEventRepository::new(db.clone());
    let market_repo = PgMarketRepository::new(db);

    let event_id = "evt-fee-insert";
    let market_id = "0xfeeinsertmarket";
    event_repo
        .upsert(make_event(
            event_id,
            "Fee Insert Event",
            "fee-insert-event",
            MarketCategory::Politics,
        ))
        .await
        .expect("seed event");

    let mut initial = make_market(
        market_id,
        event_id,
        "Will fee insert work?",
        "fee-insert-market",
        MarketCategory::Politics,
        None,
    );
    initial.fee_rate = Some(Decimal::new(2, 2));
    initial.fee_exponent = Some(Decimal::ONE);
    initial.fee_taker_only = Some(true);
    initial.fee_source = Some(FeeSource::GammaFeeSchedule);
    initial.fee_observed_at = Some(Utc::now());

    market_repo
        .upsert_batch(vec![initial])
        .await
        .expect("first insert with fee_source must cast enum");
}

#[tokio::test]
#[ignore = "requires Docker"]
async fn market_upsert_mixed_fee_source_in_same_batch() {
    let (pool, _container) = setup_pg().await;
    let db = pool.connection().clone();
    let event_repo = PgEventRepository::new(db.clone());
    let market_repo = PgMarketRepository::new(db);

    let event_id = "evt-fee-mixed-batch";
    event_repo
        .upsert(make_event(
            event_id,
            "Mixed Fee Batch",
            "fee-mixed-batch",
            MarketCategory::Politics,
        ))
        .await
        .expect("seed event");

    let with_fee = {
        let mut market = make_market(
            "0xfeebatchwith",
            event_id,
            "With fee?",
            "fee-batch-with",
            MarketCategory::Politics,
            None,
        );
        market.fee_source = Some(FeeSource::GammaFeeSchedule);
        market.fee_observed_at = Some(Utc::now());
        market
    };
    let without_fee = make_market(
        "0xfeebatchwithout",
        event_id,
        "Without fee?",
        "fee-batch-without",
        MarketCategory::Politics,
        None,
    );

    market_repo
        .upsert_batch(vec![with_fee, without_fee])
        .await
        .expect("mixed fee_source rows in one upsert batch");
}
