//! End-to-end feature plane: provider → selector → pipeline → Postgres.

use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

use async_trait::async_trait;
use chrono::{Duration as ChronoDuration, Utc};
use quant_pivot_core::{
    observability::{
        fact_lag::IngestPipelineLagTracker, feature_fact_writer::FeatureEventWriter,
        metrics_hub::MetricsHub,
    },
    pipeline::{
        book_store::BookStore, feature_window_provider::FeatureWindowProvider,
        market_candidate_provider::MarketCandidateProvider, market_registry::MarketRegistry,
        point_in_time::LiveBookDataSource,
    },
    service::feature_pipeline::{FeaturePipelineRequest, FeaturePipelineService},
};
use quant_pivot_error::storage::StorageError;
use quant_pivot_models::{
    clickhouse::{
        BookMicrostructureRow, BookSnapshotRow, MarketResolutionRow, MidPriceBucketRow,
        TickEventRow,
    },
    domain::{
        FeatureVectorInfo, NewFeatureVector,
        market::{MarketRegistryInfo, TokenInfo, book::BookLevel},
    },
    enums::{
        common::{CategorySet, MarketCategory, TickSize},
        market::MarketStatus,
    },
    runtime_config::{DataQualityConfig, FeaturesConfig, SelectionConfig},
    types::{
        EventId, FeatureVectorId, MarketId, Price, RuntimeConfigVersionId, Shares, TokenId, Usd,
    },
};
use quant_pivot_repository::{
    postgres::{PgEventRepository, PgFeatureRepository, PgMarketRepository},
    traits::{EventRepository, FeatureRepository, MarketRepository, QuantFactReadRepository},
};
use quant_pivot_research::{
    features::{PitView, names},
    hashing::ResearchHasher,
    selection::{
        ConfiguredMarketSelector, MarketSelectionBuildRequest, MarketSelector,
        ModelFeatureRequirements, SelectedMarket,
    },
};
use quant_pivot_storage::write::{AsyncWriter, AsyncWriterConfig, AsyncWriterObservability};
use quant_pivot_test_support::{
    catalog_fixtures::{make_event, make_market},
    pg::setup_pg,
    ws::WsShardHealth,
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
    event_id: "evt-feature-e2e",
    market_id: "0xfeaturee2e",
    yes_token: "33333",
    no_token: "44444",
};

fn registry_market(catalog: &E2eCatalog) -> MarketRegistryInfo {
    MarketRegistryInfo {
        market_id: MarketId::new(catalog.market_id),
        event_id: EventId::new(catalog.event_id),
        token_yes: TokenId::new(catalog.yes_token),
        token_no: TokenId::new(catalog.no_token),
        question: "Feature E2E?".into(),
        slug: "feature-e2e".into(),
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
        liquidity_usd: Some(Usd::new(Decimal::from(25_000))),
        volume_24h: Some(Usd::new(Decimal::from(9_000))),
        fee_schedule: None,
        end_date: Some(Utc::now() + ChronoDuration::days(5)),
        resolved_at: None,
        created_at: Utc::now() - ChronoDuration::days(2),
        updated_at: Utc::now(),
    }
}

async fn seed_catalog(db: &DatabaseConnection, catalog: &E2eCatalog) {
    let event_repo = PgEventRepository::new(db.clone());
    let market_repo = PgMarketRepository::new(db.clone());
    event_repo
        .upsert(make_event(
            catalog.event_id,
            "Feature E2E",
            "feature-e2e",
            MarketCategory::Sports,
        ))
        .await
        .expect("seed event");
    market_repo
        .upsert(make_market(
            catalog.market_id,
            catalog.event_id,
            "Feature E2E?",
            "feature-e2e",
            MarketCategory::Sports,
            Some(Utc::now() + ChronoDuration::days(5)),
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
            Price::new(Decimal::new(47, 2)),
            Shares::new(Decimal::from(120)),
        )]),
        Arc::from([BookLevel::from_decimal_unchecked(
            Price::new(Decimal::new(53, 2)),
            Shares::new(Decimal::from(120)),
        )]),
        u64::try_from(Utc::now().timestamp_millis()).unwrap_or(0),
        None,
    );
}

struct EmptyFactRead;

#[async_trait]
impl QuantFactReadRepository for EmptyFactRead {
    async fn microstructure_window(
        &self,
        _token_ids: Vec<TokenId>,
        _from_ms: i64,
        _to_ms: i64,
    ) -> Result<Vec<BookMicrostructureRow>, StorageError> {
        Ok(Vec::new())
    }

    async fn microstructure_series(
        &self,
        _token_ids: Vec<TokenId>,
        _from_ms: i64,
        _to_ms: i64,
        _minute: bool,
    ) -> Result<Vec<BookMicrostructureRow>, StorageError> {
        Ok(Vec::new())
    }

    async fn last_trades(
        &self,
        _token_ids: Vec<TokenId>,
        _from_ms: i64,
        _to_ms: i64,
        _limit: u64,
    ) -> Result<Vec<TickEventRow>, StorageError> {
        Ok(Vec::new())
    }

    async fn mid_price_series(
        &self,
        _token_ids: Vec<TokenId>,
        _from_ms: i64,
        _to_ms: i64,
        _bucket_secs: u32,
    ) -> Result<Vec<MidPriceBucketRow>, StorageError> {
        Ok(Vec::new())
    }

    async fn book_snapshot_at(
        &self,
        _token_id: &TokenId,
        _as_of_ms: i64,
    ) -> Result<Option<BookSnapshotRow>, StorageError> {
        Ok(None)
    }

    async fn book_snapshots_between(
        &self,
        _token_ids: Vec<TokenId>,
        _from_ms: i64,
        _to_ms: i64,
    ) -> Result<Vec<BookSnapshotRow>, StorageError> {
        Ok(Vec::new())
    }

    async fn resolution_at(
        &self,
        _market_id: &MarketId,
        _as_of_ms: i64,
    ) -> Result<Option<MarketResolutionRow>, StorageError> {
        Ok(None)
    }

    async fn resolutions_between(
        &self,
        _market_ids: Vec<MarketId>,
        _from_ms: i64,
        _to_ms: i64,
    ) -> Result<Vec<MarketResolutionRow>, StorageError> {
        Ok(Vec::new())
    }

    async fn observed_markets_between(
        &self,
        _from_ms: i64,
        _to_ms: i64,
    ) -> Result<Vec<MarketId>, StorageError> {
        Ok(Vec::new())
    }
}

/// A feature repository that records how many rows it was asked to persist, so a
/// test can prove that rejected vectors never reach persistence.
struct RecordingFeatureRepo {
    persisted_rows: AtomicUsize,
}

#[async_trait]
impl FeatureRepository for RecordingFeatureRepo {
    async fn create(&self, _vector: NewFeatureVector) -> Result<FeatureVectorInfo, StorageError> {
        unreachable!("the pipeline only uses create_batch")
    }

    async fn create_batch(
        &self,
        vectors: Vec<NewFeatureVector>,
    ) -> Result<Vec<FeatureVectorInfo>, StorageError> {
        self.persisted_rows
            .fetch_add(vectors.len(), Ordering::Relaxed);
        // The partition test only ever feeds an empty accepted set; returning an
        // empty projection is sufficient (and asserted by the row counter).
        Ok(Vec::new())
    }

    async fn find_by_id(
        &self,
        _id: &FeatureVectorId,
    ) -> Result<Option<FeatureVectorInfo>, StorageError> {
        Ok(None)
    }
}

#[tokio::test]
async fn insufficient_vectors_are_partitioned_and_not_persisted() {
    // A market whose token has no book and whose metadata is unavailable: every
    // critical feature is missing, so the vector is `Insufficient`.
    let registry = Arc::new(MarketRegistry::new());
    let book_store = Arc::new(BookStore::new(Arc::new(MetricsHub::new())));
    let live_pit = LiveBookDataSource::new(Arc::clone(&book_store), Arc::clone(&registry));

    let features = FeaturesConfig::default();
    let as_of = Utc::now();
    let market = SelectedMarket {
        market_id: MarketId::new("0xno-data"),
        event_id: EventId::new("evt-no-data"),
        category: MarketCategory::Sports,
        primary_token_id: TokenId::new("token-no-book"),
        secondary_token_id: None,
        liquidity_usd: None,
        volume_24h_usd: None,
        source_refs: Vec::new(),
    };

    let repo = Arc::new(RecordingFeatureRepo {
        persisted_rows: AtomicUsize::new(0),
    });
    let (writer, _worker) = AsyncWriter::new(
        AsyncWriterConfig::new("e2e-reject-events").capacity(64),
        |_| Box::pin(async { Ok(()) }),
        prometheus::IntCounter::new("e2e_reject_drops", "drops").expect("counter"),
        AsyncWriterObservability::default(),
    );
    let event_writer = Arc::new(FeatureEventWriter::new(Arc::new(writer)));
    let window_provider = FeatureWindowProvider::new(Arc::new(EmptyFactRead));
    let pipeline = FeaturePipelineService::new(
        window_provider,
        Arc::clone(&repo) as Arc<dyn FeatureRepository>,
        event_writer,
    );

    let included = vec![market];
    let result = pipeline
        .run(FeaturePipelineRequest {
            included: &included,
            as_of,
            features: &features,
            data_quality: &DataQualityConfig::default(),
            model_requirements: &ModelFeatureRequirements::default(),
            source_delay_secs: 0,
            pit: PitView::Live(&live_pit),
            runtime_config_version_id: RuntimeConfigVersionId::from_v7(),
            liquidity_cap_usd: Usd::new(rust_decimal::Decimal::from(10_000)),
        })
        .await
        .expect("pipeline");

    assert!(
        result.accepted.is_empty(),
        "the bad vector must not be accepted"
    );
    assert_eq!(result.persisted.len(), 0);
    assert_eq!(result.rejected.len(), 1);
    assert_eq!(result.rejected[0].market_id.as_str(), "0xno-data");
    assert!(
        !result.rejected[0].missing_required.is_empty(),
        "rejection must report the missing critical features"
    );
    assert_eq!(
        repo.persisted_rows
            .load(std::sync::atomic::Ordering::Relaxed),
        0,
        "rejected vectors must never reach persistence"
    );
}

#[tokio::test]
async fn create_feature_vector_then_find() {
    let (pool, _container) = setup_pg().await;
    let db = pool.connection().clone();
    seed_catalog(&db, &CATALOG).await;

    let registry = Arc::new(MarketRegistry::new());
    let book_store = Arc::new(BookStore::new(Arc::new(MetricsHub::new())));
    wire_live_book(&registry, &book_store, &CATALOG);

    let provider = MarketCandidateProvider::new(
        Arc::clone(&registry),
        Arc::clone(&book_store),
        WsShardHealth::operational(),
        Arc::new(IngestPipelineLagTracker::new()),
    );
    let selector = ConfiguredMarketSelector::new();
    let features = FeaturesConfig::default();
    let as_of = Utc::now();

    let request = MarketSelectionBuildRequest {
        as_of,
        runtime_config_version_id: RuntimeConfigVersionId::from_v7(),
        selection: SelectionConfig {
            enabled_categories: vec![MarketCategory::Sports],
            max_selection_size: 10,
            ..SelectionConfig::default()
        },
        data_quality: DataQualityConfig::default(),
        features: features.clone(),
        model_requirements: ModelFeatureRequirements {
            required_features: vec![names::book::BEST_BID],
        },
        source_delay_secs: 0,
    };

    let candidates = provider.candidates(as_of);
    let snapshot = selector
        .build_snapshot(request, candidates)
        .await
        .expect("selection");
    assert_eq!(snapshot.included.len(), 1);

    let live_pit = LiveBookDataSource::new(Arc::clone(&book_store), Arc::clone(&registry));
    let feature_repo = Arc::new(PgFeatureRepository::new(db.clone())) as Arc<dyn FeatureRepository>;

    let (writer, _worker) = AsyncWriter::new(
        AsyncWriterConfig::new("e2e-feature-events").capacity(512),
        |_| Box::pin(async { Ok(()) }),
        prometheus::IntCounter::new("e2e_drops", "drops").expect("counter"),
        AsyncWriterObservability::default(),
    );
    let event_writer = Arc::new(FeatureEventWriter::new(Arc::new(writer)));

    let window_provider = FeatureWindowProvider::new(Arc::new(EmptyFactRead));
    let pipeline = FeaturePipelineService::new(
        window_provider,
        Arc::clone(&feature_repo),
        Arc::clone(&event_writer),
    );

    let result = pipeline
        .run(FeaturePipelineRequest {
            included: &snapshot.included,
            as_of,
            features: &features,
            data_quality: &DataQualityConfig::default(),
            model_requirements: &ModelFeatureRequirements::default(),
            source_delay_secs: 0,
            pit: PitView::Live(&live_pit),
            runtime_config_version_id: RuntimeConfigVersionId::from_v7(),
            liquidity_cap_usd: Usd::new(rust_decimal::Decimal::from(10_000)),
        })
        .await
        .expect("pipeline");

    assert_eq!(result.accepted.len(), 1);
    assert!(result.rejected.is_empty());
    let vector = &result.accepted[0];
    assert!(vector.values.contains_key(&names::book::BEST_BID));

    let expected_hash = ResearchHasher::feature_vector(vector).expect("hash");
    let persisted = &result.persisted[0];
    assert_eq!(persisted.feature_hash, expected_hash);

    let loaded = feature_repo
        .find_by_id(&persisted.feature_vector_id)
        .await
        .expect("find")
        .expect("row");
    assert_eq!(loaded.feature_hash, expected_hash);
    assert_eq!(loaded.market_id.as_str(), CATALOG.market_id);

    let mapped = vector.try_to_new().expect("map");
    assert_eq!(mapped.feature_hash, expected_hash);
}
