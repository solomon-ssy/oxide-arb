//! Market selection snapshot repository integration tests.
//!
//! Requires Docker (testcontainers Postgres). Exercises the single-transaction
//! `create_snapshot` write path against the real schema (including the member
//! foreign keys to `market` / `event`) plus the `find_by_id` / `list_members`
//! read paths.

use chrono::Utc;
use quant_pivot_models::{
    domain::{NewMarketSelection, NewMarketSelectionMember},
    entities::quant_market_selection::{SelectionExcludedMarketIds, SelectionIncludedMarketIds},
    enums::{common::MarketCategory, market::MarketStatus},
    types::{
        ContentHash, EventId, MarketId, MarketSelectionId, RuntimeConfigVersionId,
        SelectionExclusionSummary, TokenId, Usd,
    },
};
use quant_pivot_repository::{
    postgres::{PgEventRepository, PgMarketRepository, PgMarketSelectionRepository},
    traits::{EventRepository, MarketRepository, MarketSelectionRepository},
};
use quant_pivot_test_support::{
    catalog_fixtures::{make_event, make_market},
    pg::setup_pg,
};
use rust_decimal::Decimal;

#[tokio::test]
#[ignore = "requires Docker"]
async fn create_snapshot_then_find_and_list_members() {
    let (pool, _container) = setup_pg().await;
    let db = pool.connection().clone();

    let event_repo = PgEventRepository::new(db.clone());
    let market_repo = PgMarketRepository::new(db.clone());
    let selection_repo = PgMarketSelectionRepository::new(db.clone());

    // The member rows carry FKs to `event` and `market`, so seed both first.
    let event_id = "evt-selection-1";
    let market_id = "0xselectionmarket";
    event_repo
        .upsert(make_event(
            event_id,
            "Selection Event",
            "selection-event",
            MarketCategory::Sports,
        ))
        .await
        .expect("seed event");
    market_repo
        .upsert(make_market(
            market_id,
            event_id,
            "Will it happen?",
            "will-it-happen",
            MarketCategory::Sports,
            None,
        ))
        .await
        .expect("seed market");

    let selection_id = MarketSelectionId::from_v7();
    let snapshot = NewMarketSelection {
        market_selection_id: selection_id.clone(),
        as_of: Utc::now(),
        runtime_config_version_id: RuntimeConfigVersionId::from_v7(),
        selector_hash: ContentHash::parse(format!("blake3:{}", "a".repeat(64)))
            .expect("valid content hash"),
        market_count: 1,
        included_market_ids: SelectionIncludedMarketIds(vec![market_id.to_owned()]),
        excluded_market_ids: SelectionExcludedMarketIds(vec!["0xexcluded".to_owned()]),
        exclusion_summary: SelectionExclusionSummary {
            stale_book_count: 1,
            insufficient_liquidity_count: 0,
            excluded_by_operator_count: 0,
            other_count: 0,
        },
    };
    let member = NewMarketSelectionMember {
        market_selection_id: selection_id.clone(),
        market_id: MarketId::new(market_id),
        event_id: EventId::new(event_id),
        category: MarketCategory::Sports,
        status: MarketStatus::Active,
        primary_token_id: TokenId::new("12345"),
        secondary_token_id: Some(TokenId::new("67890")),
        liquidity_usd: Some(Usd::new(Decimal::from(10_000))),
        volume_24h_usd: Some(Usd::new(Decimal::from(5_000))),
    };

    let info = selection_repo
        .create_snapshot(snapshot, vec![member])
        .await
        .expect("create snapshot");
    assert_eq!(info.market_selection_id, selection_id);
    assert_eq!(info.market_count, 1);
    assert_eq!(info.included_market_ids.0, vec![market_id.to_owned()]);
    assert_eq!(info.exclusion_summary.stale_book_count, 1);

    let found = selection_repo
        .find_by_id(&selection_id)
        .await
        .expect("find by id")
        .expect("snapshot present");
    assert_eq!(found.market_selection_id, selection_id);
    assert_eq!(found.selector_hash, info.selector_hash);

    let members = selection_repo
        .list_members(&selection_id)
        .await
        .expect("list members");
    assert_eq!(members.len(), 1);
    assert_eq!(members[0].market_id.as_str(), market_id);
    assert_eq!(members[0].category, MarketCategory::Sports);
    assert_eq!(
        members[0].secondary_token_id.as_ref().map(TokenId::as_str),
        Some("67890")
    );

    // Unknown snapshot id resolves to `None`, not an error.
    let missing = selection_repo
        .find_by_id(&MarketSelectionId::from_v7())
        .await
        .expect("find missing");
    assert!(missing.is_none());
}
