//! Basis-cross-check alert ledger integration tests (Postgres + testcontainers).
//!
//! Validates the real SQL: append-only inserts, per-market `latest_for_market`
//! ordering (the cooldown gate's read path), and paginated filtering — none of
//! which a mock repository can prove (11.2.2 remediation R6).

use chrono::{Duration as ChronoDuration, TimeZone, Utc};
use quant_pivot_models::{
    domain::{
        BasisAlertListQuery, NewBasisAlert, UpsertEvent, UpsertMarket, pagination::PageRequest,
    },
    enums::{
        common::{CategorySet, MarketCategory, TickSize},
        market::{EventStatus, MarketStatus},
    },
    types::{BasisAlertId, Bps, EventId, MarketId, TokenId},
};
use quant_pivot_repository::{
    postgres::{PgBasisAlertRepository, PgEventRepository, PgMarketRepository},
    traits::{BasisAlertRepository, EventRepository, MarketRepository},
};
use quant_pivot_test_support::pg::setup_pg;
use rust_decimal_macros::dec;

async fn seed_market(db: &sea_orm::DatabaseConnection, market_id: &str) {
    let events = PgEventRepository::new(db.clone());
    events
        .upsert(UpsertEvent {
            event_id: EventId::new("evt-basis-alert"),
            title: "Bitcoin up or down".to_owned(),
            slug: "btc-updown".to_owned(),
            series_slug: None,
            status: EventStatus::Active,
            tags: vec!["crypto".to_owned()].into(),
            neg_risk: false,
            catalog_market_ids: Vec::new().into(),
            end_date: None,
            raw_gamma: None,
        })
        .await
        .expect("seed event");

    let markets = PgMarketRepository::new(db.clone());
    markets
        .upsert(UpsertMarket {
            market_id: MarketId::new(market_id),
            event_id: EventId::new("evt-basis-alert"),
            question: "Will BTC be up?".to_owned(),
            slug: "btc-updown-5m-1".to_owned(),
            description: None,
            categories: CategorySet::from(MarketCategory::Crypto),
            status: MarketStatus::Active,
            outcome: None,
            yes_token_id: TokenId::new("111"),
            no_token_id: TokenId::new("222"),
            tick_size: TickSize::Hundredth,
            neg_risk: false,
            end_date: None,
            resolved_at: None,
            fees_enabled: true,
            fee_rate: None,
            fee_exponent: None,
            fee_taker_only: None,
            fee_rebate_rate: None,
            fee_source: None,
            fee_observed_at: None,
        })
        .await
        .expect("seed market");
}

fn alert(market_id: &str, basis: i64, as_of: chrono::DateTime<Utc>) -> NewBasisAlert {
    NewBasisAlert {
        alert_id: BasisAlertId::from_v7(),
        market_id: MarketId::new(market_id),
        instrument_key: "BINANCE:BTCUSDT:1m".to_owned(),
        oracle_instrument_key: "CHAINLINK:BTC-USD".to_owned(),
        basis_bps: Bps::new(dec!(1) * rust_decimal::Decimal::from(basis)),
        threshold_bps: Bps::new(dec!(50)),
        as_of,
    }
}

#[tokio::test]
#[ignore = "requires Docker"]
async fn record_persists_and_round_trips() {
    let (pool, _container) = setup_pg().await;
    let db = pool.connection().clone();
    seed_market(&db, "0xbasis1").await;

    let repo = PgBasisAlertRepository::new(db.clone());
    let as_of = Utc.with_ymd_and_hms(2026, 7, 1, 12, 0, 0).unwrap();
    let recorded = repo
        .record(alert("0xbasis1", 75, as_of))
        .await
        .expect("record");

    assert_eq!(recorded.market_id, MarketId::new("0xbasis1"));
    assert_eq!(recorded.basis_bps.inner(), dec!(75));
    assert_eq!(recorded.threshold_bps.inner(), dec!(50));
    assert_eq!(recorded.as_of, as_of);
}

#[tokio::test]
#[ignore = "requires Docker"]
async fn latest_for_market_picks_the_newest_as_of() {
    let (pool, _container) = setup_pg().await;
    let db = pool.connection().clone();
    seed_market(&db, "0xbasis2").await;

    let repo = PgBasisAlertRepository::new(db.clone());
    let earlier = Utc.with_ymd_and_hms(2026, 7, 1, 12, 0, 0).unwrap();
    let later = earlier + ChronoDuration::minutes(10);

    // Insert out of order to prove the query orders by `as_of`, not insertion order.
    repo.record(alert("0xbasis2", 90, later))
        .await
        .expect("record later");
    repo.record(alert("0xbasis2", 60, earlier))
        .await
        .expect("record earlier");

    let latest = repo
        .latest_for_market(&MarketId::new("0xbasis2"))
        .await
        .expect("query")
        .expect("some alert");
    assert_eq!(
        latest.as_of, later,
        "must pick the newest as_of, not last-inserted"
    );
    assert_eq!(latest.basis_bps.inner(), dec!(90));

    // A market with no alerts at all returns `None` (cooldown gate treats
    // this as "never alerted" — no suppression).
    assert!(
        repo.latest_for_market(&MarketId::new("0xbasis-unknown"))
            .await
            .expect("query")
            .is_none()
    );
}

#[tokio::test]
#[ignore = "requires Docker"]
async fn page_filters_by_market_and_time_range() {
    let (pool, _container) = setup_pg().await;
    let db = pool.connection().clone();
    seed_market(&db, "0xbasis3").await;

    let repo = PgBasisAlertRepository::new(db.clone());
    let t0 = Utc.with_ymd_and_hms(2026, 7, 1, 12, 0, 0).unwrap();
    repo.record(alert("0xbasis3", 60, t0)).await.expect("a");
    repo.record(alert("0xbasis3", 70, t0 + ChronoDuration::hours(1)))
        .await
        .expect("b");
    repo.record(alert("0xbasis3", 80, t0 + ChronoDuration::hours(2)))
        .await
        .expect("c");

    let page = repo
        .page(BasisAlertListQuery {
            market_id: Some(MarketId::new("0xbasis3")),
            from: Some(t0 + ChronoDuration::minutes(30)),
            to: None,
            page: PageRequest::default(),
        })
        .await
        .expect("page");
    assert_eq!(page.items.len(), 2, "excludes the alert before `from`");
    // Newest first.
    assert_eq!(page.items[0].basis_bps.inner(), dec!(80));
    assert_eq!(page.items[1].basis_bps.inner(), dec!(70));
}
