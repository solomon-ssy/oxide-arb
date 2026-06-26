//! End-to-end market selection: provider → selector → mapper → Postgres.

use std::sync::Arc;

use chrono::{Duration, Utc};
use quant_pivot_core::{
    observability::{fact_lag::FactLagTracker, metrics_hub::MetricsHub},
    pipeline::{
        book_store::BookStore, market_candidate_provider::MarketCandidateProvider,
        market_registry::MarketRegistry,
    },
    service::market_selection::map_snapshot_to_model,
};
use quant_pivot_models::{
    domain::market::{MarketRegistryInfo, TokenInfo, book::BookLevel},
    enums::{
        common::{CategorySet, MarketCategory, TickSize},
        market::MarketStatus,
    },
    runtime_config::{DataQualityConfig, FeaturesConfig, SelectionConfig},
    types::{EventId, MarketId, Price, RuntimeConfigVersionId, Shares, TokenId, Usd},
};
use quant_pivot_repository::{
    postgres::{PgEventRepository, PgMarketRepository, PgMarketSelectionRepository},
    traits::{EventRepository, MarketRepository, MarketSelectionRepository},
};
use quant_pivot_research::selection::{
    ConfiguredMarketSelector, MarketSelectionBuildRequest, MarketSelector, ModelFeatureRequirements,
};
use quant_pivot_test_support::{
    catalog_fixtures::{make_event, make_market},
    pg::setup_pg,
};
use rust_decimal::Decimal;
use sea_orm::DatabaseConnection;

struct E2eCatalog {
    event_id: &'static str,
    market_id: &'static str,
    yes_token: &'static str,
    no_token: &'static str,
}

const CATALOG: E2eCatalog = E2eCatalog {
    event_id: "evt-selection-e2e",
    market_id: "0xselectione2e",
    yes_token: "11111",
    no_token: "22222",
};

fn registry_market(catalog: &E2eCatalog) -> MarketRegistryInfo {
    MarketRegistryInfo {
        market_id: MarketId::new(catalog.market_id),
        event_id: EventId::new(catalog.event_id),
        token_yes: TokenId::new(catalog.yes_token),
        token_no: TokenId::new(catalog.no_token),
        question: "Will it happen?".into(),
        slug: "will-it-happen".into(),
        categories: CategorySet::from(MarketCategory::Sports),
        status: MarketStatus::Active,
        outcome: None,
        neg_risk: false,
        tick_size: TickSize::Hundredth,
        tokens: vec![
            TokenInfo {
                token_id: TokenId::new(catalog.yes_token),
                outcome: "Yes".into(),
                neg_risk: false,
            },
            TokenInfo {
                token_id: TokenId::new(catalog.no_token),
                outcome: "No".into(),
                neg_risk: false,
            },
        ],
        best_bid: None,
        best_ask: None,
        depth_usd: None,
        min_order_size: Decimal::ONE,
        liquidity_usd: Some(Usd::new(Decimal::from(20_000))),
        volume_24h: Some(Usd::new(Decimal::from(8_000))),
        fee_schedule: None,
        end_date: Some(Utc::now() + Duration::days(7)),
        resolved_at: None,
        created_at: Utc::now(),
        updated_at: Utc::now(),
    }
}

async fn seed_catalog(db: &DatabaseConnection, catalog: &E2eCatalog) {
    let event_repo = PgEventRepository::new(db.clone());
    let market_repo = PgMarketRepository::new(db.clone());
    event_repo
        .upsert(make_event(
            catalog.event_id,
            "Selection E2E",
            "selection-e2e",
            MarketCategory::Sports,
        ))
        .await
        .expect("seed event");
    market_repo
        .upsert(make_market(
            catalog.market_id,
            catalog.event_id,
            "Will it happen?",
            "will-it-happen",
            MarketCategory::Sports,
            Some(Utc::now() + Duration::days(7)),
        ))
        .await
        .expect("seed market");
}

fn wire_live_book(registry: &MarketRegistry, book_store: &BookStore, catalog: &E2eCatalog) {
    registry.register_market(registry_market(catalog));
    let yes = TokenId::new(catalog.yes_token);
    book_store.apply_snapshot(
        &yes,
        Arc::from([BookLevel::from_decimal_unchecked(
            Price::new(Decimal::new(49, 2)),
            Shares::new(Decimal::from(100)),
        )]),
        Arc::from([BookLevel::from_decimal_unchecked(
            Price::new(Decimal::new(51, 2)),
            Shares::new(Decimal::from(100)),
        )]),
        u64::try_from(Utc::now().timestamp_millis()).unwrap_or(0),
        None,
    );
}

#[tokio::test]
async fn provider_selector_mapper_persist_round_trip() {
    let (pool, _container) = setup_pg().await;
    let db = pool.connection().clone();
    seed_catalog(&db, &CATALOG).await;

    let registry = Arc::new(MarketRegistry::new());
    let book_store = Arc::new(BookStore::new(Arc::new(MetricsHub::new())));
    wire_live_book(&registry, &book_store, &CATALOG);

    let provider = MarketCandidateProvider::new(
        Arc::clone(&registry),
        Arc::clone(&book_store),
        Arc::new(FactLagTracker::new()),
    );
    let selector = ConfiguredMarketSelector::new();
    let selection_repo = PgMarketSelectionRepository::new(db);

    let as_of = Utc::now();
    let candidates = provider.candidates(as_of);
    assert_eq!(candidates.len(), 1);
    assert_eq!(
        candidates[0].volume_24h_usd,
        Some(Usd::new(Decimal::from(8_000)))
    );

    let request = MarketSelectionBuildRequest {
        as_of,
        runtime_config_version_id: RuntimeConfigVersionId::from_v7(),
        selection: SelectionConfig {
            enabled_categories: vec![MarketCategory::Sports],
            max_selection_size: 10,
            ..SelectionConfig::default()
        },
        data_quality: DataQualityConfig::default(),
        features: FeaturesConfig::default(),
        model_requirements: ModelFeatureRequirements::default(),
        source_delay_secs: 0,
    };

    let snapshot = selector
        .build_snapshot(request, candidates.clone())
        .await
        .expect("build snapshot");
    assert_eq!(snapshot.included.len(), 1);
    assert_eq!(snapshot.included[0].market_id.as_str(), CATALOG.market_id);

    let model = map_snapshot_to_model(&snapshot, &candidates).expect("map snapshot");
    let persisted = selection_repo
        .create_snapshot(model.snapshot, model.members)
        .await
        .expect("persist snapshot");

    assert_eq!(persisted.market_selection_id, snapshot.market_selection_id);
    assert_eq!(persisted.market_count, 1);

    let members = selection_repo
        .list_members(&snapshot.market_selection_id)
        .await
        .expect("list members");
    assert_eq!(members.len(), 1);
    assert_eq!(members[0].market_id.as_str(), CATALOG.market_id);
    assert_eq!(members[0].reason, "selected");
    assert_eq!(
        members[0].volume_24h_usd,
        Some(Usd::new(Decimal::from(8_000)))
    );
}
