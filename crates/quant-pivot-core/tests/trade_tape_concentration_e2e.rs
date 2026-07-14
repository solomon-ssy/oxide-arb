//! End-to-end **structural** trade-tape participant concentration (Phase 11.2.1).
//!
//! # Contract
//!
//! Given a whale-heavy on-chain trade-tape fixture, the test locks three planes
//! on the **same Postgres catalog** and a **domain-disabled** runtime config
//! (this path is intentionally orthogonal to the crypto domain vertical):
//!
//! 1. **Feature plane** — `FeaturePipelineService` emits scored
//!    `struct.participant_gini` / `struct.participant_cr1_share` / `struct.participant_hhi`
//!    with whale-window numerics (CR1 ≥ 85%, Gini > 0).
//! 2. **Factor plane** — `FactorPipelineService` persists
//!    `struct.participant_concentration` whose raw value matches the canonical
//!    composite estimator applied to the feature numerics.
//! 3. **Monitor plane** — `CoreStructuralMonitor::participant_concentration`
//!    exposes the same composite as the research estimator (byte-identical at
//!    12 dp), proving online monitor ↔ offline feature/factor parity.
//!
//! # Explicitly out of scope
//!
//! - Crypto domain slice / linkage / `quant_domain_observation` (use
//!   `DomainConfig::disabled()` and structural-only feature families).
//! - Postgres persistence semantics of the feature repo itself (covered by
//!   repository tests); here PG is the real FK shell for the factor plane.

use std::sync::Arc;

use async_trait::async_trait;
use chrono::{DateTime, Duration as ChronoDuration, Utc};
use quant_pivot_core::{
    app::ports::structural_monitor::CoreStructuralMonitor,
    governance::BiasTableApplicator,
    ingest::{book_store::BookStore, market_registry::MarketRegistry},
    observability::{
        factor_fact_writer::FactorEventWriter, feature_fact_writer::FeatureEventWriter,
        metrics_hub::MetricsHub,
    },
    prefetch::feature_window::FeatureWindowProvider,
    service::{
        factor_pipeline::{FactorPipelineRequest, FactorPipelineService},
        feature_pipeline::{
            FeaturePipelineDeps, FeaturePipelineRequest, FeaturePipelineResult,
            FeaturePipelineService,
        },
    },
};
use quant_pivot_error::{control::ControlError, storage::StorageError};
use quant_pivot_models::domain::{DecisionClock, RuntimeConfigPort, StructuralMonitorPort};
use quant_pivot_models::runtime_config::FeatureFamily;
use quant_pivot_models::{
    clickhouse::{
        BookMicrostructureRow, BookSnapshotRow, DomainObservationRow, MarketResolutionRow,
        MidPriceBucketRow, TickEventRow, TradeTapeRow,
    },
    config::TradeTapeOnChainConfig,
    domain::{
        NewModelRun,
        market::{EventRegistryInfo, MarketRegistryInfo, TokenInfo, book::BookLevel},
    },
    enums::{
        common::{CategorySet, MarketCategory, TickSize},
        factor::FactorFamily,
        market::{EventStatus, MarketStatus},
        quant::{ModelRunKind, ModelRunStatus},
    },
    runtime_config::{
        DataQualityConfig, DomainConfig, FactorsConfig, FeaturesConfig, RuntimeConfig,
    },
    types::{
        ContentHash, DomainInstrumentKey, EventId, FeatureVectorId, MarketId, ModelRunId, Price,
        RuntimeConfigVersionId, Shares, TokenId, Usd,
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
    factors::{FactorEngine, names::STRUCT_PARTICIPANT_CONCENTRATION},
    features::{FeatureValue, FeatureVector, names::structural as structural_features},
    selection::{ModelFeatureRequirements, SelectedMarket},
    trade_tape::{
        ConcentrationCompositeWeights, participant_concentration::composite_concentration,
    },
};
use quant_pivot_storage::write::{AsyncWriter, AsyncWriterConfig, AsyncWriterObservability};
use quant_pivot_test_support::{
    catalog_fixtures::{make_event, make_market},
    factor_governance::publish_all_factor_definitions,
    pg::setup_pg,
    report_pipeline_harness::{EmptyBasisAlertRepo, EmptyLinkageRepo},
    trade_tape_fixtures::{
        ConfigurableFactRead, live_trade_tape_block_cursor_repo, whale_concentration_by_market,
    },
};
use quant_pivot_test_support::{fact_sink::DiscardFactWriter, pit::InMemoryDecisionSnapshotSource};
use rust_decimal::Decimal;
use sea_orm::DatabaseConnection;
use testcontainers::ContainerAsync;
use testcontainers_modules::postgres::Postgres;

struct Catalog {
    event_id: &'static str,
    market_id: &'static str,
    yes_token: &'static str,
    no_token: &'static str,
}

const CATALOG: Catalog = Catalog {
    event_id: "evt-tape-conc-e2e",
    market_id: "0xtapeconce2e",
    yes_token: "77771",
    no_token: "77772",
};

fn composite_weights() -> ConcentrationCompositeWeights {
    ConcentrationCompositeWeights {
        gini: Decimal::new(50, 2),
        cr1_share: Decimal::new(30, 2),
        hhi: Decimal::new(20, 2),
    }
}

struct FixedRuntimeConfig(Arc<RuntimeConfig>);

#[async_trait::async_trait]
impl RuntimeConfigPort for FixedRuntimeConfig {
    fn current(&self) -> Arc<RuntimeConfig> {
        Arc::clone(&self.0)
    }

    fn preflight(&self, _candidate: &RuntimeConfig) -> Result<(), ControlError> {
        Ok(())
    }

    async fn apply(&self, _config: RuntimeConfig) -> Result<(), ControlError> {
        Ok(())
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
    ) -> Result<Vec<TickEventRow>, StorageError> {
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

    async fn book_snapshot_at(
        &self,
        _token_id: &TokenId,
        _as_of_ms: i64,
        _decision_at_ms: i64,
    ) -> Result<Option<BookSnapshotRow>, StorageError> {
        Ok(None)
    }

    async fn book_snapshots_between(
        &self,
        _token_ids: Vec<TokenId>,
        _from_ms: i64,
        _to_ms: i64,
        _available_by_ms: i64,
    ) -> Result<Vec<BookSnapshotRow>, StorageError> {
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

fn registry_market(catalog: &Catalog) -> MarketRegistryInfo {
    MarketRegistryInfo {
        market_id: MarketId::new(catalog.market_id),
        event_id: EventId::new(catalog.event_id),
        token_yes: TokenId::new(catalog.yes_token),
        token_no: TokenId::new(catalog.no_token),
        question: "Trade tape concentration E2E?".into(),
        slug: "tape-conc-e2e".into(),
        description: None,
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
            "Tape Conc E2E",
            "tape-conc-e2e",
            MarketCategory::Sports,
        ))
        .await
        .expect("seed event");
    market_repo
        .upsert(make_market(
            catalog.market_id,
            catalog.event_id,
            "Trade tape concentration E2E?",
            "tape-conc-e2e",
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
        title: "Tape Conc E2E".to_owned(),
        slug: "tape-conc-e2e".to_owned(),
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
            Shares::new(Decimal::from(120)),
        )]),
        u64::try_from(Utc::now().timestamp_millis())
            .expect("test book timestamp must be non-negative"),
        None,
    );
}

fn noop_feature_writer() -> Arc<FeatureEventWriter> {
    Arc::new(FeatureEventWriter::new(Arc::new(DiscardFactWriter::new())))
}

fn noop_factor_writer() -> Arc<FactorEventWriter> {
    let (writer, _worker) = AsyncWriter::new(
        AsyncWriterConfig::new("tape-conc-factor-events").capacity(256),
        |_| Box::pin(async { Ok(()) }),
        prometheus::IntCounter::new("tape_conc_factor_drops", "drops").expect("counter"),
        AsyncWriterObservability::default(),
    );
    Arc::new(FactorEventWriter::new(Arc::new(writer)))
}

fn selected_market(catalog: &Catalog) -> SelectedMarket {
    SelectedMarket {
        market_id: MarketId::new(catalog.market_id),
        event_id: EventId::new(catalog.event_id),
        category: MarketCategory::Sports,
        primary_token_id: TokenId::new(catalog.yes_token),
        secondary_token_id: Some(TokenId::new(catalog.no_token)),
        liquidity_usd: Some(Usd::new(Decimal::from(25_000))),
        volume_24h_usd: Some(Usd::new(Decimal::from(9_000))),
        source_refs: Vec::new(),
    }
}

fn structural_factors_config() -> FactorsConfig {
    FactorsConfig {
        enabled_factor_families: vec![FactorFamily::Structural],
        ..FactorsConfig::default()
    }
}

fn structural_features_only_config() -> FeaturesConfig {
    FeaturesConfig {
        enabled_feature_families: vec![
            FeatureFamily::MarketMetadata,
            FeatureFamily::PriceBook,
            FeatureFamily::Structural,
        ],
        max_concurrent_market_resolves: 1,
        ..FeaturesConfig::default()
    }
}

struct WhaleTapeConcHarness {
    _container: ContainerAsync<Postgres>,
    db: DatabaseConnection,
    registry: Arc<MarketRegistry>,
    book_store: Arc<BookStore>,
    live_pit: InMemoryDecisionSnapshotSource,
    fact_read: Arc<dyn QuantFactReadRepository>,
    market_id: MarketId,
    as_of: DateTime<Utc>,
}

async fn whale_tape_conc_harness() -> WhaleTapeConcHarness {
    let (pool, container) = setup_pg().await;
    let db = pool.connection().clone();
    seed_catalog(&db, &CATALOG).await;

    let registry = Arc::new(MarketRegistry::new());
    let book_store = Arc::new(BookStore::new(Arc::new(MetricsHub::new())));
    wire_live_book(&registry, &book_store, &CATALOG);
    let live_pit = InMemoryDecisionSnapshotSource::freeze(registry.as_ref(), book_store.as_ref());

    let as_of = Utc::now();
    let event_time_ms = (as_of - ChronoDuration::seconds(60)).timestamp_millis();
    let market_id = MarketId::new(CATALOG.market_id);
    let token_id = TokenId::new(CATALOG.yes_token);
    let fact_read = Arc::new(ConfigurableFactRead::new(
        Arc::new(EmptyFactRead),
        whale_concentration_by_market(&market_id, &token_id, event_time_ms),
    )) as Arc<dyn QuantFactReadRepository>;

    WhaleTapeConcHarness {
        _container: container,
        db,
        registry,
        book_store,
        live_pit,
        fact_read,
        market_id,
        as_of,
    }
}

async fn run_whale_feature_pipeline(harness: &WhaleTapeConcHarness) -> FeaturePipelineResult {
    let features = structural_features_only_config();
    let feature_repo =
        Arc::new(PgFeatureRepository::new(harness.db.clone())) as Arc<dyn FeatureRepository>;
    let feature_pipeline = FeaturePipelineService::new(FeaturePipelineDeps {
        window_provider: FeatureWindowProvider::new(Arc::clone(&harness.fact_read)),
        feature_repo: Arc::clone(&feature_repo),
        event_writer: noop_feature_writer(),
        market_registry: Arc::clone(&harness.registry),
        block_cursor_repo: live_trade_tape_block_cursor_repo(),
        linkage_repo: Arc::new(EmptyLinkageRepo),
        basis_alert_repo: Arc::new(EmptyBasisAlertRepo),
        calibration_repo: Arc::new(PgCalibrationArtifactRepository::new(harness.db.clone())),
        trade_tape_on_chain: TradeTapeOnChainConfig::default(),
    });

    let domain = DomainConfig::disabled();
    let included = vec![selected_market(&CATALOG)];
    feature_pipeline
        .run(FeaturePipelineRequest {
            included: &included,
            boundary: DecisionClock::new(0)
                .boundary(harness.as_of)
                .expect("decision boundary"),
            features: &features,
            domain: &domain,
            data_quality: &DataQualityConfig::default(),
            model_requirements: &ModelFeatureRequirements::default(),
            pit: &harness.live_pit,
            runtime_config_version_id: RuntimeConfigVersionId::from_v7(),
            liquidity_cap_usd: Usd::new(Decimal::from(10_000)),
        })
        .await
        .expect("feature pipeline")
}

fn concentration_feature_decimals(vector: &FeatureVector) -> (Decimal, Decimal, Decimal) {
    let gini = vector
        .value(&structural_features::PARTICIPANT_GINI)
        .expect("participant gini feature");
    let cr1 = vector
        .value(&structural_features::PARTICIPANT_CR1_SHARE)
        .expect("participant cr1 feature");
    let hhi = vector
        .value(&structural_features::PARTICIPANT_HHI)
        .expect("participant hhi feature");
    match (gini, cr1, hhi) {
        (FeatureValue::Decimal(gini), FeatureValue::Decimal(cr1), FeatureValue::Decimal(hhi)) => {
            (*gini, *cr1, *hhi)
        }
        other => panic!("expected scored decimal concentration features, got {other:?}"),
    }
}

fn assert_whale_concentration_features(vector: &FeatureVector) -> Decimal {
    let (gini, cr1, hhi) = concentration_feature_decimals(vector);
    assert!(
        cr1 >= Decimal::new(85, 2),
        "whale window cr1 should reflect ~90% top share, got {cr1}"
    );
    assert!(
        gini > Decimal::ZERO,
        "gini must be positive for whale window"
    );
    assert!(hhi > Decimal::ZERO, "hhi must be positive for whale window");
    composite_concentration(gini, cr1, hhi, &composite_weights()).expect("composite from features")
}

async fn run_factor_round_and_assert_concentration(
    harness: &WhaleTapeConcHarness,
    feature_result: &FeaturePipelineResult,
    expected_composite: Decimal,
) {
    let features = structural_features_only_config();
    let factors = structural_factors_config();
    let domain = DomainConfig::disabled();

    let factor_repo =
        Arc::new(PgFactorRepository::new(harness.db.clone())) as Arc<dyn FactorRepository>;
    publish_all_factor_definitions(factor_repo.as_ref(), &factors, &features, &domain)
        .await
        .expect("publish factor definitions");

    let model_run_id = ModelRunId::from_v7();
    PgModelRunRepository::new(harness.db.clone())
        .create(NewModelRun {
            model_run_id: model_run_id.clone(),
            run_kind: ModelRunKind::LiveInference,
            model_version_id: None,
            runtime_config_version_id: RuntimeConfigVersionId::from_v7(),
            market_selection_id: None,
            window_start: harness.as_of,
            window_end: harness.as_of,
            status: ModelRunStatus::Running,
            input_hash: ContentHash::parse(format!("blake3:{}", "1".repeat(64))).expect("hash"),
            output_hash: None,
            metrics_json: serde_json::json!({}),
            error_code: None,
            error_message: None,
            started_at: harness.as_of,
            finished_at: None,
        })
        .await
        .expect("model run");

    let feature_vector_ids: Vec<FeatureVectorId> = feature_result
        .persisted
        .iter()
        .map(|row| row.feature_vector_id.clone())
        .collect();
    assert_eq!(
        feature_vector_ids.len(),
        feature_result.accepted.len(),
        "persisted feature rows must align 1:1 with accepted vectors"
    );

    let factor_service = FactorPipelineService::new(
        Arc::clone(&factor_repo),
        noop_factor_writer(),
        Arc::new(BiasTableApplicator::new(
            Arc::new(PgCalibrationArtifactRepository::new(harness.db.clone()))
                as Arc<dyn CalibrationArtifactRepository>,
        )),
    );
    let factor_result = factor_service
        .run(FactorPipelineRequest {
            model_run_id: &model_run_id,
            vectors: &feature_result.accepted,
            feature_vector_ids: &feature_vector_ids,
            factors: &factors,
            features: &features,
            domain: &domain,
        })
        .await
        .expect("factor pipeline");
    assert!(
        !factor_result.persisted.is_empty(),
        "factor values must persist"
    );
    assert!(
        factor_result.rejected.is_empty(),
        "whale fixture must not reject factor scoring"
    );

    let listed = factor_repo
        .list_values_for_run(&model_run_id)
        .await
        .expect("list values for run");
    assert_eq!(listed.len(), factor_result.persisted.len());

    let engine = FactorEngine::new(&factors, &features, &domain, None);
    let outcomes = engine
        .compute_all_batch(&feature_result.accepted, &factors)
        .expect("factor outcomes");
    let concentration = outcomes[0]
        .factors
        .iter()
        .find(|factor| factor.value.name == STRUCT_PARTICIPANT_CONCENTRATION)
        .expect("participant concentration factor outcome");
    let raw = concentration
        .value
        .raw_value
        .expect("participant concentration must score from whale trade tape");
    assert_eq!(
        raw.round_dp(12),
        expected_composite.round_dp(12),
        "factor composite must match feature-derived canonical estimator"
    );
}

async fn assert_monitor_concentration_matches_canonical(
    harness: &WhaleTapeConcHarness,
    expected_composite: Decimal,
) {
    let runtime = Arc::new(RuntimeConfig::default());
    let monitor = CoreStructuralMonitor::new(
        Arc::clone(&harness.registry),
        Arc::clone(&harness.book_store),
        Arc::new(FeatureWindowProvider::new(Arc::clone(&harness.fact_read))),
        live_trade_tape_block_cursor_repo(),
        Arc::new(FixedRuntimeConfig(runtime)),
        TradeTapeOnChainConfig::default(),
    );
    let summary = monitor
        .participant_concentration()
        .await
        .expect("monitor summary");
    let market_view = summary
        .markets
        .iter()
        .find(|view| view.market_id == harness.market_id)
        .expect("market concentration view");
    assert!(
        market_view.composite_raw.is_some(),
        "monitor must expose scored composite_raw"
    );
    assert!(
        market_view.gini.is_some() && market_view.cr1_share.is_some(),
        "monitor numerics must not be null when ingest route is healthy"
    );

    let monitor_composite = composite_concentration(
        market_view.gini.unwrap(),
        market_view.cr1_share.unwrap(),
        market_view.hhi.unwrap(),
        &composite_weights(),
    )
    .expect("composite");
    assert_eq!(
        market_view.composite_raw.unwrap().round_dp(12),
        monitor_composite.round_dp(12),
        "monitor composite must match canonical estimator"
    );
    assert_eq!(
        monitor_composite.round_dp(12),
        expected_composite.round_dp(12),
        "monitor must agree with feature/factor plane"
    );
}

#[tokio::test]
async fn whale_trade_tape_scores_feature_factor_and_monitor() {
    let harness = whale_tape_conc_harness().await;
    let feature_result = run_whale_feature_pipeline(&harness).await;

    assert_eq!(feature_result.accepted.len(), 1, "vector must be accepted");
    assert_eq!(
        feature_result.persisted.len(),
        1,
        "feature vector must persist to Postgres"
    );
    let expected_composite = assert_whale_concentration_features(&feature_result.accepted[0]);

    run_factor_round_and_assert_concentration(&harness, &feature_result, expected_composite).await;
    assert_monitor_concentration_matches_canonical(&harness, expected_composite).await;
}
