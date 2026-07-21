//! Market catalog page-query filter persistence system contract.

use quant_pivot_models::{
    domain::{api::MarketPageQuery, pagination::PageRequest},
    enums::common::MarketCategory,
    types::EventId,
};
use quant_pivot_repository::{
    postgres::{PgEventRepository, PgMarketRepository},
    traits::{EventRepository, MarketRepository},
};
use quant_pivot_system_tests::{
    postgres::setup_pg,
    support::catalog_fixtures::{make_event, make_market},
};

pub async fn market_page_filters_by_event_id_and_category() {
    let (pool, _container) = setup_pg().await;
    let db = pool.connection().clone();
    let event_repo = PgEventRepository::new(db.clone());
    let market_repo = PgMarketRepository::new(db);

    event_repo
        .upsert(make_event(
            "evt-page-a",
            "Event A",
            "event-a",
            MarketCategory::Politics,
        ))
        .await
        .expect("seed event a");
    event_repo
        .upsert(make_event(
            "evt-page-b",
            "Event B",
            "event-b",
            MarketCategory::Sports,
        ))
        .await
        .expect("seed event b");

    market_repo
        .upsert(make_market(
            "0xpagepolitics",
            "evt-page-a",
            "Politics market?",
            "politics-market",
            MarketCategory::Politics,
            None,
        ))
        .await
        .expect("seed politics market");
    market_repo
        .upsert(make_market(
            "0xpagesports",
            "evt-page-b",
            "Sports market?",
            "sports-market",
            MarketCategory::Sports,
            None,
        ))
        .await
        .expect("seed sports market");

    let by_event = market_repo
        .page(MarketPageQuery {
            event_id: Some(EventId::new("evt-page-a")),
            page: PageRequest::new(1, 20),
            ..Default::default()
        })
        .await
        .expect("page by event");
    assert_eq!(by_event.total, 1);
    assert_eq!(by_event.items[0].market_id.as_str(), "0xpagepolitics");

    let by_category = market_repo
        .page(MarketPageQuery {
            category: Some(MarketCategory::Sports),
            page: PageRequest::new(1, 20),
            ..Default::default()
        })
        .await
        .expect("page by category");
    assert_eq!(by_category.total, 1);
    assert_eq!(by_category.items[0].market_id.as_str(), "0xpagesports");

    let by_keyword = market_repo
        .page(MarketPageQuery {
            keyword: Some("Sports".into()),
            page: PageRequest::new(1, 20),
            ..Default::default()
        })
        .await
        .expect("page by keyword");
    assert_eq!(by_keyword.total, 1);
    assert_eq!(by_keyword.items[0].market_id.as_str(), "0xpagesports");

    let by_keyword_case_insensitive = market_repo
        .page(MarketPageQuery {
            keyword: Some("sports".into()),
            page: PageRequest::new(1, 20),
            ..Default::default()
        })
        .await
        .expect("page by keyword case-insensitive");
    assert_eq!(by_keyword_case_insensitive.total, 1);
    assert_eq!(
        by_keyword_case_insensitive.items[0].market_id.as_str(),
        "0xpagesports"
    );
}
