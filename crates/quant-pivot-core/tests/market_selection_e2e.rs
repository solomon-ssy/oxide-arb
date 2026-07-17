//! End-to-end market selection: provider → selector → mapper → Postgres.

use std::sync::Arc;

use async_trait::async_trait;
use chrono::{Duration, Utc};
use quant_pivot_core::{
    ingest::{book_store::BookStore, market_registry::MarketRegistry},
    observability::metrics_hub::MetricsHub,
    prefetch::market_candidates::MarketCandidateProvider,
    service::market_selection::map_snapshot_to_model,
};
use quant_pivot_error::storage::StorageError;
use quant_pivot_models::{
    clickhouse::{
        BookL2CheckpointRow, BookMicrostructureRow, DomainObservationRow, MarketResolutionRow,
        MidPriceBucketRow, TradeTapeRow,
    },
    domain::{
        DecisionClock,
        market::{EventRegistryInfo, MarketRegistryInfo, TokenInfo, book::BookLevel},
    },
    enums::{
        catalog::CatalogFilterReasonSet,
        common::{CategorySet, MarketCategory, TickSize},
        market::{EventStatus, MarketStatus},
    },
    runtime_config::{DataQualityConfig, DomainConfig, FeaturesConfig, SelectionConfig},
    types::{
        DomainInstrumentKey, EventId, MarketId, Price, RuntimeConfigVersionId, Shares, TokenId, Usd,
    },
};
use quant_pivot_repository::{
    postgres::{PgEventRepository, PgMarketRepository, PgMarketSelectionRepository},
    traits::{
        EventRepository, MarketRepository, MarketSelectionRepository, QuantFactReadRepository,
    },
};
use quant_pivot_research::{
    pit::PointInTimeSnapshotSource,
    selection::{
        ConfiguredMarketSelector, MarketSelectionBuildRequest, MarketSelector,
        ModelFeatureRequirements,
    },
};
use quant_pivot_test_support::{
    catalog_fixtures::{make_event, make_market},
    pg::setup_pg,
    pit::InMemoryDecisionSnapshotSource,
    report_pipeline_harness::EmptyLinkageRepo,
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
        description: None,
        categories: CategorySet::from(MarketCategory::Sports),
        status: MarketStatus::Active,
        filter_reasons: CatalogFilterReasonSet::default(),
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
        start_date: Some(Utc::now()),
        end_date: Some(Utc::now() + Duration::days(7)),
        resolved_at: None,
        created_at: Some(Utc::now()),
        updated_at: Utc::now(),
    }
}

struct EmptyFactRead;

#[async_trait]
impl QuantFactReadRepository for EmptyFactRead {
    async fn microstructure_window(
        &self,
        _token_ids: Vec<TokenId>,
        _from_ms: i64,
        _to_ms: i64,
        _decision_at_ms: i64,
    ) -> Result<Vec<BookMicrostructureRow>, StorageError> {
        Ok(Vec::new())
    }

    async fn microstructure_series(
        &self,
        _token_ids: Vec<TokenId>,
        _from_ms: i64,
        _to_ms: i64,
        _available_by_ms: i64,
        _minute: bool,
    ) -> Result<Vec<BookMicrostructureRow>, StorageError> {
        Ok(Vec::new())
    }

    async fn trade_tape_window_by_market(
        &self,
        _market_ids: Vec<MarketId>,
        _from_ms: i64,
        _to_ms: i64,
        _decision_at_ms: i64,
    ) -> Result<Vec<TradeTapeRow>, StorageError> {
        Ok(Vec::new())
    }

    async fn last_trades(
        &self,
        _token_ids: Vec<TokenId>,
        _from_ms: i64,
        _to_ms: i64,
        _limit: u64,
    ) -> Result<Vec<TradeTapeRow>, StorageError> {
        Ok(Vec::new())
    }

    async fn mid_price_series(
        &self,
        _token_ids: Vec<TokenId>,
        _from_ms: i64,
        _to_ms: i64,
        _decision_at_ms: i64,
        _bucket_secs: u32,
    ) -> Result<Vec<MidPriceBucketRow>, StorageError> {
        Ok(Vec::new())
    }

    async fn book_checkpoint_at(
        &self,
        _token_id: &TokenId,
        _as_of_ms: i64,
        _decision_at_ms: i64,
    ) -> Result<Option<BookL2CheckpointRow>, StorageError> {
        Ok(None)
    }

    async fn book_checkpoints_between(
        &self,
        _token_ids: Vec<TokenId>,
        _from_ms: i64,
        _to_ms: i64,
        _available_by_ms: i64,
    ) -> Result<Vec<BookL2CheckpointRow>, StorageError> {
        Ok(Vec::new())
    }

    async fn resolution_at(
        &self,
        _market_id: &MarketId,
        _source_cutoff_ms: i64,
        _decision_at_ms: i64,
    ) -> Result<Option<MarketResolutionRow>, StorageError> {
        Ok(None)
    }

    async fn resolutions_between(
        &self,
        _market_ids: Vec<MarketId>,
        _from_ms: i64,
        _to_ms: i64,
        _decision_at_ms: i64,
    ) -> Result<Vec<MarketResolutionRow>, StorageError> {
        Ok(Vec::new())
    }

    async fn domain_observations_between(
        &self,
        _instrument_keys: Vec<DomainInstrumentKey>,
        _from_ms: i64,
        _to_ms: i64,
        _publish_cutoff_ms: i64,
        _decision_at_ms: i64,
    ) -> Result<Vec<DomainObservationRow>, StorageError> {
        Ok(Vec::new())
    }

    async fn domain_observation_at(
        &self,
        _instrument_key: &DomainInstrumentKey,
        _metric: &str,
        _as_of_ms: i64,
        _decision_at_ms: i64,
    ) -> Result<Option<DomainObservationRow>, StorageError> {
        Ok(None)
    }

    async fn observed_markets_between(
        &self,
        _from_ms: i64,
        _to_ms: i64,
        _decision_at_ms: i64,
    ) -> Result<Vec<MarketId>, StorageError> {
        Ok(Vec::new())
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
    let market = registry_market(catalog);
    registry.register_event(EventRegistryInfo {
        event_id: market.event_id.clone(),
        title: "Selection E2E".to_owned(),
        slug: "selection-e2e".to_owned(),
        series_slug: None,
        status: EventStatus::Active,
        market_ids: vec![market.market_id.clone()],
        categories: CategorySet::from(MarketCategory::Sports),
        tags: vec![MarketCategory::Sports.as_str().to_owned()],
        neg_risk: false,
        end_date: market.end_date,
        created_at: Utc::now() - Duration::days(1),
        updated_at: market.updated_at,
    });
    registry.register_market(market);
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
        u64::try_from(Utc::now().timestamp_millis())
            .expect("test book timestamp must be non-negative"),
        None,
    );
}

#[tokio::test]
#[ignore = "requires Docker"]
async fn provider_selector_mapper_persist_round_trip() {
    let (pool, _container) = setup_pg().await;
    let db = pool.connection().clone();
    seed_catalog(&db, &CATALOG).await;

    let registry = Arc::new(MarketRegistry::new());
    let book_store = Arc::new(BookStore::new(Arc::new(MetricsHub::new())));
    wire_live_book(&registry, &book_store, &CATALOG);

    let pit_source: Arc<dyn PointInTimeSnapshotSource> = Arc::new(
        InMemoryDecisionSnapshotSource::freeze(registry.as_ref(), book_store.as_ref()),
    );
    let provider = MarketCandidateProvider::new(
        pit_source,
        Arc::new(EmptyLinkageRepo),
        Arc::new(EmptyFactRead),
    );
    let selector = ConfiguredMarketSelector::new();
    let selection_repo = PgMarketSelectionRepository::new(db);

    let as_of = Utc::now();
    let domain = DomainConfig::default();
    let candidates = provider
        .candidates(
            &DecisionClock::new(0)
                .boundary(as_of)
                .expect("decision boundary"),
            &domain,
        )
        .await
        .expect("candidates")
        .candidates;
    assert_eq!(candidates.len(), 1);
    assert_eq!(
        candidates[0].volume_24h_usd,
        Some(Usd::new(Decimal::from(8_000)))
    );

    let request = MarketSelectionBuildRequest {
        decision_at: as_of,
        runtime_config_version_id: RuntimeConfigVersionId::from_v7(),
        selection: SelectionConfig {
            enabled_categories: vec![MarketCategory::Sports],
            ..SelectionConfig::default()
        },
        data_quality: DataQualityConfig::default(),
        features: FeaturesConfig::default(),
        model_requirements: ModelFeatureRequirements::default(),
        knowledge_lag_secs: 0,
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
    assert_eq!(
        members[0].volume_24h_usd,
        Some(Usd::new(Decimal::from(8_000)))
    );
}
