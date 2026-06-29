//! End-to-end factor plane: feature pipeline → factor pipeline → Postgres + CH.

use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

use async_trait::async_trait;
use chrono::{Duration as ChronoDuration, Utc};
use quant_pivot_core::{
    observability::{
        factor_fact_writer::FactorEventWriter, feature_fact_writer::FeatureEventWriter,
        metrics_hub::MetricsHub,
    },
    pipeline::{
        book_store::BookStore, feature_window_provider::FeatureWindowProvider,
        market_registry::MarketRegistry, point_in_time::LiveBookDataSource,
    },
    service::{
        factor_pipeline::{FactorPipelineRequest, FactorPipelineService},
        feature_pipeline::{FeaturePipelineRequest, FeaturePipelineService},
    },
};
use quant_pivot_error::storage::StorageError;
use quant_pivot_models::{
    clickhouse::{
        BookMicrostructureRow, BookSnapshotRow, MarketResolutionRow, QuantFactorEventRow,
    },
    domain::{
        NewModelRun,
        market::{MarketRegistryInfo, TokenInfo, book::BookLevel},
    },
    enums::{
        common::{CategorySet, MarketCategory, TickSize},
        factor::{FactorDefinitionScope, FactorFamily},
        market::MarketStatus,
        quant::{ModelRunKind, ModelRunStatus},
    },
    runtime_config::{DataQualityConfig, FactorsConfig, FeaturesConfig},
    types::{
        ContentHash, EventId, FeatureVectorId, MarketId, ModelRunId, Price, RuntimeConfigVersionId,
        Shares, TokenId, Usd,
    },
};
use quant_pivot_repository::{
    postgres::{
        PgEventRepository, PgFactorRepository, PgFeatureRepository, PgMarketRepository,
        PgModelRunRepository,
    },
    traits::{
        EventRepository, FactorRepository, FeatureRepository, MarketRepository, ModelRunRepository,
        QuantFactReadRepository,
    },
};
use quant_pivot_research::{
    factors::{FactorEngine, factor_definition_id, factor_events},
    features::{FeatureVector, PitView},
    selection::{ModelFeatureRequirements, SelectedMarket},
};
use quant_pivot_storage::write::{AsyncWriter, AsyncWriterConfig, AsyncWriterObservability};
use quant_pivot_test_support::{
    catalog_fixtures::{make_event, make_market},
    factor_governance::publish_all_factor_definitions,
    pg::setup_pg,
};
use rust_decimal::Decimal;
use sea_orm::DatabaseConnection;
use tokio_util::sync::CancellationToken;

struct Catalog {
    event_id: &'static str,
    market_id: &'static str,
    yes_token: &'static str,
    no_token: &'static str,
}

const CATALOG: Catalog = Catalog {
    event_id: "evt-factor-e2e",
    market_id: "0xfactore2e",
    yes_token: "55555",
    no_token: "66666",
};

fn registry_market(catalog: &Catalog) -> MarketRegistryInfo {
    MarketRegistryInfo {
        market_id: MarketId::new(catalog.market_id),
        event_id: EventId::new(catalog.event_id),
        token_yes: TokenId::new(catalog.yes_token),
        token_no: TokenId::new(catalog.no_token),
        question: "Factor E2E?".into(),
        slug: "factor-e2e".into(),
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

async fn seed_catalog(db: &DatabaseConnection, catalog: &Catalog) {
    let event_repo = PgEventRepository::new(db.clone());
    let market_repo = PgMarketRepository::new(db.clone());
    event_repo
        .upsert(make_event(
            catalog.event_id,
            "Factor E2E",
            "factor-e2e",
            MarketCategory::Sports,
        ))
        .await
        .expect("seed event");
    market_repo
        .upsert(make_market(
            catalog.market_id,
            catalog.event_id,
            "Factor E2E?",
            "factor-e2e",
            MarketCategory::Sports,
            Some(Utc::now() + ChronoDuration::days(5)),
        ))
        .await
        .expect("seed market");
}

fn wire_live_book(registry: &MarketRegistry, book_store: &BookStore, catalog: &Catalog) {
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
            Shares::new(Decimal::from(80)),
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
}

fn noop_feature_writer() -> Arc<FeatureEventWriter> {
    let (writer, _worker) = AsyncWriter::new(
        AsyncWriterConfig::new("factor-e2e-feature-events").capacity(256),
        |_| Box::pin(async { Ok(()) }),
        prometheus::IntCounter::new("factor_e2e_feat_drops", "drops").expect("counter"),
        AsyncWriterObservability::default(),
    );
    Arc::new(FeatureEventWriter::new(Arc::new(writer)))
}

fn factors_config() -> FactorsConfig {
    FactorsConfig {
        enabled_factor_families: vec![
            FactorFamily::Liquidity,
            FactorFamily::Microstructure,
            FactorFamily::Resolution,
            FactorFamily::DataQuality,
        ],
        ..FactorsConfig::default()
    }
}

fn selected_market() -> SelectedMarket {
    SelectedMarket {
        market_id: MarketId::new(CATALOG.market_id),
        event_id: EventId::new(CATALOG.event_id),
        category: MarketCategory::Sports,
        primary_token_id: TokenId::new(CATALOG.yes_token),
        secondary_token_id: Some(TokenId::new(CATALOG.no_token)),
        liquidity_usd: Some(Usd::new(Decimal::from(25_000))),
        volume_24h_usd: Some(Usd::new(Decimal::from(9_000))),
        source_refs: Vec::new(),
    }
}

/// Run the feature plane and return the accepted vectors plus their persisted ids.
async fn build_features(db: &DatabaseConnection) -> (Vec<FeatureVector>, Vec<FeatureVectorId>) {
    let registry = Arc::new(MarketRegistry::new());
    let book_store = Arc::new(BookStore::new(Arc::new(MetricsHub::new())));
    wire_live_book(&registry, &book_store, &CATALOG);
    let live_pit = LiveBookDataSource::new(Arc::clone(&book_store), Arc::clone(&registry));

    let feature_repo = Arc::new(PgFeatureRepository::new(db.clone())) as Arc<dyn FeatureRepository>;
    let window_provider = FeatureWindowProvider::new(Arc::new(EmptyFactRead));
    let pipeline =
        FeaturePipelineService::new(window_provider, feature_repo, noop_feature_writer());

    let features = FeaturesConfig::default();
    let included = vec![selected_market()];
    let result = pipeline
        .run(FeaturePipelineRequest {
            included: &included,
            as_of: Utc::now(),
            features: &features,
            data_quality: &DataQualityConfig::default(),
            model_requirements: &ModelFeatureRequirements::default(),
            source_delay_secs: 0,
            pit: PitView::Live(&live_pit),
            runtime_config_version_id: RuntimeConfigVersionId::from_v7(),
            liquidity_cap_usd: Usd::new(Decimal::from(10_000)),
        })
        .await
        .expect("feature pipeline");

    assert_eq!(result.accepted.len(), 1, "the market must produce a vector");
    let ids = result
        .persisted
        .iter()
        .map(|info| info.feature_vector_id.clone())
        .collect();
    (result.accepted, ids)
}

#[tokio::test]
async fn create_definition_and_values_then_list_for_run() {
    let (pool, _container) = setup_pg().await;
    let db = pool.connection().clone();
    seed_catalog(&db, &CATALOG).await;

    let (vectors, feature_vector_ids) = build_features(&db).await;

    let factor_repo = Arc::new(PgFactorRepository::new(db.clone())) as Arc<dyn FactorRepository>;
    let (writer, _worker) = AsyncWriter::new(
        AsyncWriterConfig::new("factor-e2e-factor-events").capacity(256),
        |_| Box::pin(async { Ok(()) }),
        prometheus::IntCounter::new("factor_e2e_drops", "drops").expect("counter"),
        AsyncWriterObservability::default(),
    );
    let event_writer = Arc::new(FactorEventWriter::new(Arc::new(writer)));
    let service = FactorPipelineService::new(Arc::clone(&factor_repo), event_writer);

    let model_run_id = ModelRunId::from_v7();
    // The factor-value → model_run FK (added in 3.4) requires the owning run to
    // exist before any factor value is persisted.
    let model_run_repo = PgModelRunRepository::new(db.clone());
    model_run_repo
        .create(NewModelRun {
            model_run_id: model_run_id.clone(),
            run_kind: ModelRunKind::LiveInference,
            model_version_id: None,
            runtime_config_version_id: RuntimeConfigVersionId::from_v7(),
            market_selection_id: None,
            window_start: Utc::now(),
            window_end: Utc::now(),
            status: ModelRunStatus::Running,
            input_hash: ContentHash::parse(format!("blake3:{}", "0".repeat(64)))
                .expect("zero hash"),
            output_hash: None,
            metrics_json: serde_json::json!({}),
            error_code: None,
            error_message: None,
            started_at: Utc::now(),
            finished_at: None,
        })
        .await
        .expect("create owning model run");

    let factors = factors_config();
    let features = FeaturesConfig::default();
    publish_all_factor_definitions(factor_repo.as_ref(), &factors, &features)
        .await
        .expect("publish factor definitions");

    let result = service
        .run(FactorPipelineRequest {
            model_run_id: &model_run_id,
            vectors: &vectors,
            feature_vector_ids: &feature_vector_ids,
            factors: &factors,
            features: &features,
        })
        .await
        .expect("factor pipeline");

    assert!(
        !result.persisted.is_empty(),
        "an eligible market must persist factor values"
    );
    assert!(result.rejected.is_empty(), "no market should be rejected");

    let listed = factor_repo
        .list_values_for_run(&model_run_id)
        .await
        .expect("list values for run");
    assert_eq!(listed.len(), result.persisted.len());
    assert!(
        listed
            .iter()
            .all(|value| value.model_run_id == model_run_id),
        "listed values must all belong to the run"
    );

    // The data-quality definition is always registered and idempotent.
    let definition_id = factor_definition_id("data_quality");
    let definition = factor_repo
        .find_definition(&definition_id)
        .await
        .expect("find definition")
        .expect("definition row");
    assert_eq!(definition.name, "data_quality");
    assert_eq!(definition.scope, FactorDefinitionScope::Generic);
}

#[tokio::test]
#[ignore = "requires Docker"]
async fn unpublished_factor_definitions_block_pipeline() {
    let (pool, _container) = setup_pg().await;
    let db = pool.connection().clone();
    seed_catalog(&db, &CATALOG).await;

    let (vectors, feature_vector_ids) = build_features(&db).await;
    let factor_repo = Arc::new(PgFactorRepository::new(db.clone())) as Arc<dyn FactorRepository>;
    let (writer, _worker) = AsyncWriter::new(
        AsyncWriterConfig::new("factor-e2e-unpublished").capacity(256),
        |_| Box::pin(async { Ok(()) }),
        prometheus::IntCounter::new("factor_e2e_unpub_drops", "drops").expect("counter"),
        AsyncWriterObservability::default(),
    );
    let service = FactorPipelineService::new(
        Arc::clone(&factor_repo),
        Arc::new(FactorEventWriter::new(Arc::new(writer))),
    );

    let model_run_id = ModelRunId::from_v7();
    PgModelRunRepository::new(db.clone())
        .create(NewModelRun {
            model_run_id: model_run_id.clone(),
            run_kind: ModelRunKind::LiveInference,
            model_version_id: None,
            runtime_config_version_id: RuntimeConfigVersionId::from_v7(),
            market_selection_id: None,
            window_start: Utc::now(),
            window_end: Utc::now(),
            status: ModelRunStatus::Running,
            input_hash: ContentHash::parse(format!("blake3:{}", "1".repeat(64))).expect("hash"),
            output_hash: None,
            metrics_json: serde_json::json!({}),
            error_code: None,
            error_message: None,
            started_at: Utc::now(),
            finished_at: None,
        })
        .await
        .expect("create model run");

    let factors = factors_config();
    let features = FeaturesConfig::default();
    let error = service
        .run(FactorPipelineRequest {
            model_run_id: &model_run_id,
            vectors: &vectors,
            feature_vector_ids: &feature_vector_ids,
            factors: &factors,
            features: &features,
        })
        .await;
    let Err(error) = error else {
        panic!("draft definitions must block the factor plane");
    };
    assert!(
        error.to_string().contains("must be Published"),
        "unexpected error: {error}"
    );
}

#[tokio::test]
async fn factor_event_writer_batches() {
    // Build outcomes directly (no DB) and prove the writer drains the projected
    // rows through its async sink.
    let factors = factors_config();
    let features = FeaturesConfig::default();
    let engine = FactorEngine::new(&factors, &features);

    let as_of = Utc::now();
    let market_a = sample_vector("0xbatcha", Decimal::from(20_000), Decimal::from(120), as_of);
    let market_b = sample_vector("0xbatchb", Decimal::from(5_000), Decimal::from(300), as_of);
    let outcomes = engine
        .compute_all_batch(&[market_a, market_b], &factors)
        .expect("compute outcomes");

    let model_run_id = ModelRunId::from_v7();
    let rows = factor_events(&outcomes, &model_run_id, Utc::now().timestamp_millis());
    assert!(
        !rows.is_empty(),
        "projection must produce factor-event rows"
    );
    let expected = rows.len();

    let flushed = Arc::new(AtomicUsize::new(0));
    let sink = Arc::clone(&flushed);
    let (writer, worker) = AsyncWriter::new(
        AsyncWriterConfig::new("factor-e2e-batch").capacity(512),
        move |batch: Vec<QuantFactorEventRow>| {
            let sink = Arc::clone(&sink);
            Box::pin(async move {
                sink.fetch_add(batch.len(), Ordering::Relaxed);
                Ok(())
            })
        },
        prometheus::IntCounter::new("factor_e2e_batch_drops", "drops").expect("counter"),
        AsyncWriterObservability::default(),
    );
    let handle = tokio::spawn(worker.run(CancellationToken::new()));

    let event_writer = FactorEventWriter::new(Arc::new(writer));
    event_writer.write_batch(rows);
    // Drop the only producer so the worker drains the remainder and exits.
    drop(event_writer);
    handle.await.expect("worker join");

    assert_eq!(flushed.load(Ordering::Relaxed), expected);
}

fn sample_vector(
    market: &str,
    liquidity: Decimal,
    spread_bps: Decimal,
    as_of: chrono::DateTime<Utc>,
) -> FeatureVector {
    use std::collections::BTreeMap;

    use quant_pivot_models::{enums::quant::DataQualityStatus, types::SchemaVersion};
    use quant_pivot_research::features::{
        FeatureName, FeatureValue,
        names::{book, market},
    };

    let mut values: BTreeMap<FeatureName, FeatureValue> = BTreeMap::new();
    values.insert(
        book::VISIBLE_LIQUIDITY_USD,
        FeatureValue::Usd(Usd::new(liquidity)),
    );
    values.insert(book::SPREAD_BPS, FeatureValue::Bps(spread_bps));
    values.insert(
        book::DEPTH_IMBALANCE,
        FeatureValue::Decimal(Decimal::new(2, 1)),
    );
    values.insert(
        market::TIME_TO_RESOLUTION_SECS,
        FeatureValue::Count(172_800),
    );
    FeatureVector {
        market_id: MarketId::new(market),
        token_id: Some(TokenId::new("token")),
        as_of,
        schema_version: SchemaVersion::FIRST,
        values,
        substitutions: Vec::new(),
        data_quality: DataQualityStatus::Fresh,
        staleness_ms: 0,
        source_refs: Vec::new(),
    }
}
