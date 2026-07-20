//! End-to-end factor plane: feature pipeline → factor pipeline → Postgres + CH.

use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

use async_trait::async_trait;
use chrono::{Duration as ChronoDuration, Utc};
use quant_pivot_core::{
    governance::BiasTableApplicator,
    ingest::{book_store::BookStore, market_registry::MarketRegistry},
    observability::{
        factor_fact_writer::FactorEventWriter, feature_fact_writer::FeatureEventWriter,
        metrics_hub::MetricsHub,
    },
    prefetch::feature_window::FeatureWindowProvider,
    service::{
        factor_pipeline::{FactorPipelineRequest, FactorPipelineService},
        feature_pipeline::{FeaturePipelineDeps, FeaturePipelineRequest, FeaturePipelineService},
    },
};
use quant_pivot_error::storage::StorageError;
use quant_pivot_models::{
    clickhouse::{
        BookL2CheckpointRow, BookMicrostructureRow, DomainObservationRow, MarketResolutionRow,
        MidPriceBucketRow, QuantFactorEventRow, TradeTapeRow,
    },
    config::TradeTapeOnChainConfig,
    domain::{
        DecisionClock, NewModelRun,
        market::{EventRegistryInfo, MarketRegistryInfo, TokenInfo, book::BookLevel},
    },
    enums::{
        catalog::CatalogFilterReasonSet,
        common::{CategorySet, MarketCategory, TickSize},
        factor::{FactorDefinitionScope, FactorFamily},
        market::{EventStatus, MarketStatus},
        quant::{ModelRunKind, ModelRunStatus},
    },
    runtime_config::{DataQualityConfig, DomainConfig, FactorsConfig, FeaturesConfig},
    types::{
        ContentHash, DecisionPolicySnapshotId, DomainInstrumentKey, EventId, FeatureVectorId,
        MarketId, ModelRunId, Price, Shares, TokenId, Usd, stable_name::FactorName,
    },
};
use quant_pivot_repository::{
    postgres::{
        PgCalibrationArtifactRepository, PgEventRepository, PgFactorRepository,
        PgFeatureRepository, PgMarketRepository, PgModelRunRepository,
    },
    traits::{
        CalibrationArtifactRepository, EventRepository, FactorRepository, FeatureRepository,
        MarketRepository, ModelRunRepository, QuantFactReadRepository,
    },
};
use quant_pivot_research::{
    factors::{FactorEngine, factor_events},
    features::FeatureVector,
    selection::{ModelFeatureRequirements, SelectedMarket},
};
use quant_pivot_storage::write::{AsyncWriter, AsyncWriterConfig, AsyncWriterObservability};
use quant_pivot_test_support::{
    catalog_fixtures::{make_event, make_market},
    factor_governance::{publish_all_factor_definitions, register_all_factor_definitions},
    pg::setup_pg,
    report_pipeline_harness::{EmptyBasisAlertRepo, EmptyLinkageRepo},
    trade_tape_fixtures::live_trade_tape_block_cursor_repo,
};
use quant_pivot_test_support::{fact_sink::DiscardFactWriter, pit::InMemoryDecisionSnapshotSource};
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
        liquidity_usd: Some(Usd::new(Decimal::from(25_000))),
        volume_24h: Some(Usd::new(Decimal::from(9_000))),
        start_date: Some(Utc::now() - ChronoDuration::days(2)),
        end_date: Some(Utc::now() + ChronoDuration::days(5)),
        resolved_at: None,
        created_at: Some(Utc::now() - ChronoDuration::days(2)),
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
    let market = registry_market(catalog);
    registry.register_event(EventRegistryInfo {
        event_id: market.event_id.clone(),
        title: "Factor E2E".to_owned(),
        slug: "factor-e2e".to_owned(),
        series_slug: None,
        status: EventStatus::Active,
        market_ids: vec![market.market_id.clone()],
        categories: CategorySet::from(MarketCategory::Sports),
        tags: vec![MarketCategory::Sports.as_str().to_owned()],
        neg_risk: false,
        end_date: market.end_date,
        created_at: Utc::now() - ChronoDuration::days(2),
        updated_at: market.updated_at,
    });
    registry.register_market(market);
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
        u64::try_from(Utc::now().timestamp_millis())
            .expect("test book timestamp must be non-negative"),
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

fn noop_feature_writer() -> Arc<FeatureEventWriter> {
    Arc::new(FeatureEventWriter::new(Arc::new(DiscardFactWriter::new())))
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
    let live_pit = InMemoryDecisionSnapshotSource::freeze(registry.as_ref(), book_store.as_ref());

    let feature_repo = Arc::new(PgFeatureRepository::new(db.clone())) as Arc<dyn FeatureRepository>;
    let window_provider = FeatureWindowProvider::new(Arc::new(EmptyFactRead));
    let pipeline = FeaturePipelineService::new(FeaturePipelineDeps {
        window_provider,
        feature_repo,
        event_writer: noop_feature_writer(),
        market_registry: Arc::clone(&registry),
        block_cursor_repo: live_trade_tape_block_cursor_repo(),
        linkage_repo: Arc::new(EmptyLinkageRepo),
        basis_alert_repo: Arc::new(EmptyBasisAlertRepo),
        calibration_repo: Arc::new(PgCalibrationArtifactRepository::new(db.clone())),
        trade_tape_on_chain: TradeTapeOnChainConfig::default(),
    });

    let features = FeaturesConfig::default();
    let domain = DomainConfig::default();
    let included = vec![selected_market()];
    let result = pipeline
        .run(FeaturePipelineRequest {
            included: &included,
            boundary: DecisionClock::new(0)
                .boundary(Utc::now())
                .expect("decision boundary"),
            features: &features,
            domain: &domain,
            data_quality: &DataQualityConfig::default(),
            model_requirements: &ModelFeatureRequirements::default(),
            pit: &live_pit,
            decision_policy_snapshot_id: DecisionPolicySnapshotId::from_v7(),
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
    let bias_table = Arc::new(BiasTableApplicator::new(Arc::new(
        PgCalibrationArtifactRepository::new(db.clone()),
    )
        as Arc<dyn CalibrationArtifactRepository>));
    let service = FactorPipelineService::new(Arc::clone(&factor_repo), event_writer, bias_table);

    let model_run_id = ModelRunId::from_v7();
    // The factor-value → model_run FK (added in 3.4) requires the owning run to
    // exist before any factor value is persisted.
    let model_run_repo = PgModelRunRepository::new(db.clone());
    model_run_repo
        .create(NewModelRun {
            model_run_id: model_run_id.clone(),
            run_kind: ModelRunKind::LiveInference,
            model_version_id: None,
            decision_policy_snapshot_id: DecisionPolicySnapshotId::from_v7(),
            market_selection_id: None,
            window_start: Utc::now(),
            window_end: Utc::now(),
            status: ModelRunStatus::Running,
            input_hash: ContentHash::parse(format!("blake3:{}", "0".repeat(64)))
                .expect("zero hash"),
            output_hash: None,
            error_code: None,
            error_message: None,
            started_at: Utc::now(),
            finished_at: None,
        })
        .await
        .expect("create owning model run");

    let factors = factors_config();
    let features = FeaturesConfig::default();
    publish_all_factor_definitions(
        factor_repo.as_ref(),
        &factors,
        &features,
        &DomainConfig::default(),
    )
    .await
    .expect("publish factor definitions");

    let result = service
        .run(FactorPipelineRequest {
            model_run_id: &model_run_id,
            vectors: &vectors,
            feature_vector_ids: &feature_vector_ids,
            factors: &factors,
            features: &features,
            domain: &DomainConfig::default(),
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
    let definition_id = FactorEngine::new(&factors, &features, &DomainConfig::default(), None)
        .definition_identity(&FactorName::new("data_quality"))
        .expect("data-quality identity")
        .factor_definition_id;
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
    let bias_table = Arc::new(BiasTableApplicator::new(Arc::new(
        PgCalibrationArtifactRepository::new(db.clone()),
    )
        as Arc<dyn CalibrationArtifactRepository>));
    let service = FactorPipelineService::new(
        Arc::clone(&factor_repo),
        Arc::new(FactorEventWriter::new(Arc::new(writer))),
        bias_table,
    );

    let model_run_id = ModelRunId::from_v7();
    PgModelRunRepository::new(db.clone())
        .create(NewModelRun {
            model_run_id: model_run_id.clone(),
            run_kind: ModelRunKind::LiveInference,
            model_version_id: None,
            decision_policy_snapshot_id: DecisionPolicySnapshotId::from_v7(),
            market_selection_id: None,
            window_start: Utc::now(),
            window_end: Utc::now(),
            status: ModelRunStatus::Running,
            input_hash: ContentHash::parse(format!("blake3:{}", "1".repeat(64))).expect("hash"),
            output_hash: None,
            error_code: None,
            error_message: None,
            started_at: Utc::now(),
            finished_at: None,
        })
        .await
        .expect("create model run");

    let factors = factors_config();
    let features = FeaturesConfig::default();

    // Definitions are no longer auto-registered on the hot path: a fresh,
    // unregistered factor set must hard-block (never a silent pass).
    let unregistered = service
        .run(FactorPipelineRequest {
            model_run_id: &model_run_id,
            vectors: &vectors,
            feature_vector_ids: &feature_vector_ids,
            factors: &factors,
            features: &features,
            domain: &DomainConfig::default(),
        })
        .await;
    let Err(error) = unregistered else {
        panic!("unregistered definitions must block the factor plane");
    };
    assert!(
        error.to_string().contains("must be Published"),
        "unexpected error: {error}"
    );

    // Registered-but-Draft definitions must also block until published.
    register_all_factor_definitions(
        factor_repo.as_ref(),
        &factors,
        &features,
        &DomainConfig::default(),
    )
    .await
    .expect("register draft definitions");
    let draft = service
        .run(FactorPipelineRequest {
            model_run_id: &model_run_id,
            vectors: &vectors,
            feature_vector_ids: &feature_vector_ids,
            factors: &factors,
            features: &features,
            domain: &DomainConfig::default(),
        })
        .await;
    let Err(error) = draft else {
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
    let engine = FactorEngine::new(&factors, &features, &DomainConfig::disabled(), None);

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

    use quant_pivot_models::{
        enums::quant::DataQualityStatus,
        types::{SchemaVersion, stable_name::FeatureName},
    };
    use quant_pivot_research::features::{
        FeatureCell, FeatureStaleness, FeatureValue,
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
    let values = values
        .into_iter()
        .map(|(name, value)| {
            (
                name,
                FeatureCell::observed(value, None, FeatureStaleness::Unknown),
            )
        })
        .collect();
    FeatureVector {
        market_id: MarketId::new(market),
        token_id: Some(TokenId::new("token")),
        decision_at: as_of,
        generic_schema_version: SchemaVersion::FIRST,
        generic: values,
        domain: None,
        data_quality: DataQualityStatus::Fresh,
    }
}
