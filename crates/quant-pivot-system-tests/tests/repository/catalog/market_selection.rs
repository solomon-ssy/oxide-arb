//! Market selection snapshot persistence system contract.
//!
//! Requires Docker (testcontainers Postgres). Exercises the single-transaction
//! `create_snapshot` write path against the real schema (including the member
//! foreign keys to `market` / `event`) plus the `find_by_id` / `list_members`
//! read paths.

use chrono::Utc;
use quant_pivot_models::{
    domain::quant::{NewMarketSelection, NewMarketSelectionMember},
    enums::{common::MarketCategory, market::MarketStatus},
    types::{
        ContentHash, DecisionPolicySnapshotId, EventId, MarketId, MarketSelectionId,
        SelectionExclusionSummary, TokenId, Usd,
    },
};
use quant_pivot_repository::{
    postgres::{PgEventRepository, PgMarketRepository, PgMarketSelectionRepository},
    traits::{EventRepository, MarketRepository, MarketSelectionRepository},
};
use quant_pivot_system_tests::{
    postgres::setup_pg,
    support::{
        SelectorFixture,
        catalog_fixtures::{make_event, make_market},
    },
};
use rust_decimal::Decimal;

pub async fn create_snapshot_find_members() {
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
    let selector_hash =
        ContentHash::parse(&format!("blake3:{}", "a".repeat(64))).expect("valid content hash");
    let snapshot = NewMarketSelection {
        market_selection_id: selection_id,
        decision_at: Utc::now(),
        decision_policy_snapshot_id: DecisionPolicySnapshotId::from_v7(),
        selector_hash,
        selector_evidence: SelectorFixture::evidence(selector_hash),
        market_count: 1,
        exclusion_summary: SelectionExclusionSummary {
            stale_book_count: 1,
            insufficient_liquidity_count: 0,
            excluded_by_operator_count: 0,
            other_count: 0,
        },
    };
    let member = NewMarketSelectionMember {
        market_selection_id: selection_id,
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
    assert_eq!(info.exclusion_summary.stale_book_count, 1);

    let found = selection_repo
        .find_by_id(&selection_id)
        .await
        .expect("find by id")
        .expect("snapshot present");
    assert_eq!(found.market_selection_id, selection_id);
    assert_eq!(found.selector_hash, info.selector_hash);
    assert_eq!(found.selector_evidence, info.selector_evidence);
    assert_eq!(found.selector_evidence.selector_hash, found.selector_hash);

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

    let invalid_hash = ContentHash::from_bytes([0xff; 32]);
    let invalid = NewMarketSelection {
        market_selection_id: MarketSelectionId::from_v7(),
        decision_at: Utc::now(),
        decision_policy_snapshot_id: DecisionPolicySnapshotId::from_v7(),
        selector_hash: invalid_hash,
        selector_evidence: SelectorFixture::evidence(selector_hash),
        market_count: 0,
        exclusion_summary: SelectionExclusionSummary::default(),
    };
    let error = selection_repo
        .create_snapshot(invalid, Vec::new())
        .await
        .expect_err("selector evidence with a different root must fail closed");
    assert!(
        error
            .to_string()
            .contains("ck_quant_market_selection_evidence"),
        "unexpected selector evidence constraint error: {error}"
    );
}
