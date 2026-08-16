//! Finalized execution-participant concentration system contracts.
//!
//! # Contract
//!
//! Given a whale-heavy finalized-execution fixture, the test locks three planes
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
//!   `DomainConfig::disabled` and structural-only feature families).
//! - Postgres persistence semantics of the feature repo itself (covered by
//!   repository tests); here PG is the real FK shell for the factor plane.

use std::sync::Arc;

use async_trait::async_trait;
use chrono::{DateTime, Duration as ChronoDuration, Utc};
use prometheus::IntCounter;
use quant_pivot_compute::ComputeExecutor;
use quant_pivot_core::{
    app::ports::structural_monitor::CoreStructuralMonitor,
    ingest::{book_store::BookStore, data_plane_index::DataPlane, market_registry::MarketRegistry},
    observability::{
        factor_fact_writer::FactorEventWriter, feature_fact_writer::FeatureEventWriter,
        metrics_hub::MetricsHub,
    },
    prefetch::feature_window::FeatureWindowProvider,
    service::{
        factor_pipeline::{FactorExecutionPlane, FactorPipelineRequest, FactorPipelineService},
        feature_pipeline::{
            FeaturePipelineDeps, FeaturePipelineRequest, FeaturePipelineResult,
            FeaturePipelineService,
        },
    },
};
use quant_pivot_error::{control::ControlError, storage::StorageError};
use quant_pivot_models::{
    clickhouse::{
        BookL2LedgerRow, BookMicrostructureRow, DomainObservationRow, ExecutionParticipantFactRow,
        ExecutionParticipantRow, MarketExecutionRow, MarketResolutionRow, MidPriceBucketRow,
    },
    domain::{
        data_plane::{DecisionClock, HistorySealChunkRef},
        market::{
            EventRegistryInfo, MarketRegistryInfo, TokenInfo,
            book::{BookLevel, BookSnapshot},
        },
        ports::{PolicySnapshotPort, PreparedPolicySnapshot, StructuralMonitorPort},
        quant::NewModelRun,
    },
    enums::{
        catalog::CatalogFilterReasonSet,
        common::{CategorySet, MarketCategory, TickSize},
        factor::FactorFamily,
        market::{EventStatus, MarketStatus},
        quant::ModelRunKind,
    },
    runtime_config::{
        DataQualityConfig, DecisionPolicySnapshot, DomainConfig, FactorsConfig, FeatureFamily,
        FeaturesConfig,
    },
    types::{
        ContentHash, DecisionPolicySnapshotId, DomainInstrumentKey, EventId, FeatureValue,
        FeatureVectorId, MarketId, ModelRunId, Price, ResearchFeatureContract, Shares, TokenId,
        Usd,
    },
};
use quant_pivot_repository::{
    postgres::{
        PgCalibrationArtifactRepository, PgEventRepository, PgFactorRepository,
        PgFeatureRepository, PgMarketRepository, PgModelRunRepository,
    },
    traits::{
        EventRepository, FactorRepository, FeatureRepository, MarketRepository, ModelRunRepository,
        QuantFactReadRepository,
    },
};
use quant_pivot_research::{
    execution_history::{
        ConcentrationCompositeWeights, participant_concentration::composite_concentration,
    },
    factors::{FactorEngine, names::STRUCT_PARTICIPANT_CONCENTRATION},
    features::{
        FeatureVector,
        names::structural::{PARTICIPANT_CR1_SHARE, PARTICIPANT_GINI, PARTICIPANT_HHI},
    },
    selection::{ModelFeatureRequirements, SelectedMarket},
};
use quant_pivot_storage::write::{AsyncWriter, AsyncWriterConfig, AsyncWriterObservability};
use quant_pivot_system_tests::{
    postgres::{ScenarioDatabase, setup_pg},
    support::{
        catalog_fixtures::{make_event, make_market},
        execution_history_fixtures::{
            ConfigurableFactRead, live_activation_head, live_history_config, live_history_repo,
            unavailable_history_repo, whale_concentration_by_market,
        },
        fact_sink::DiscardFactWriter,
        factor_definitions::register_all_factor_definitions,
        pit::InMemoryDecisionSnapshotSource,
        publish_fresh_book,
        report_pipeline_harness::{EmptyBasisAlertRepo, EmptyLinkageRepo},
    },
};
use rust_decimal::Decimal;
use sea_orm::DatabaseConnection;

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

struct FixedRuntimeConfig(Arc<DecisionPolicySnapshot>);

#[async_trait::async_trait]
impl PolicySnapshotPort for FixedRuntimeConfig {
    fn current(&self) -> Arc<DecisionPolicySnapshot> {
        Arc::clone(&self.0)
    }

    async fn prepare(
        &self,
        config: DecisionPolicySnapshot,
    ) -> Result<PreparedPolicySnapshot, ControlError> {
        Ok(PreparedPolicySnapshot::new(Arc::new(config), || {}))
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

    async fn market_execution_window(
        &self,
        _market_ids: Vec<MarketId>,
        _history_chunks: Vec<HistorySealChunkRef>,
        _from_ms: i64,
        _to_ms: i64,
        _decision_at_ms: i64,
    ) -> Result<Vec<ExecutionParticipantFactRow>, StorageError> {
        Ok(Vec::new())
    }

    async fn last_executions(
        &self,
        _token_ids: Vec<TokenId>,
        _from_ms: i64,
        _to_ms: i64,
        _limit: u64,
    ) -> Result<Vec<MarketExecutionRow>, StorageError> {
        Ok(Vec::new())
    }

    async fn market_executions_between(
        &self,
        _market_ids: Vec<MarketId>,
        _history_chunks: Vec<HistorySealChunkRef>,
        _from_ms: i64,
        _to_ms: i64,
        _decision_at_ms: i64,
    ) -> Result<Vec<MarketExecutionRow>, StorageError> {
        Ok(Vec::new())
    }

    async fn execution_participants_between(
        &self,
        _market_ids: Vec<MarketId>,
        _history_chunks: Vec<HistorySealChunkRef>,
        _from_ms: i64,
        _to_ms: i64,
        _decision_at_ms: i64,
    ) -> Result<Vec<ExecutionParticipantRow>, StorageError> {
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

    async fn book_ledger_snapshot_at(
        &self,
        _token_id: &TokenId,
        _as_of_ms: i64,
        _decision_at_ms: i64,
    ) -> Result<Option<BookL2LedgerRow>, StorageError> {
        Ok(None)
    }

    async fn book_ledger_snapshots_between(
        &self,
        _token_ids: Vec<TokenId>,
        _from_ms: i64,
        _to_ms: i64,
        _available_by_ms: i64,
    ) -> Result<Vec<BookL2LedgerRow>, StorageError> {
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

impl Catalog {
    fn registry_market(&self) -> MarketRegistryInfo {
        MarketRegistryInfo {
            market_id: MarketId::new(self.market_id),
            event_id: EventId::new(self.event_id),
            token_yes: TokenId::new(self.yes_token),
            token_no: TokenId::new(self.no_token),
            question: "Execution history concentration E2E?".into(),
            slug: "tape-conc-e2e".into(),
            description: None,
            categories: CategorySet::from(MarketCategory::Sports),
            status: MarketStatus::Active,
            filter_reasons: CatalogFilterReasonSet::default(),
            outcome: None,
            neg_risk: false,
            tick_size: TickSize::Hundredth,
            tokens: vec![
                TokenInfo {
                    token_id: TokenId::new(self.yes_token),
                    outcome: "Yes".into(),
                    neg_risk: false,
                },
                TokenInfo {
                    token_id: TokenId::new(self.no_token),
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
            maker_rebate_schedule: None,
            start_date: Some(Utc::now() - ChronoDuration::days(2)),
            end_date: Some(Utc::now() + ChronoDuration::days(5)),
            resolved_at: None,
            created_at: Some(Utc::now() - ChronoDuration::days(2)),
            updated_at: Utc::now(),
        }
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
            "Execution history concentration E2E?",
            "tape-conc-e2e",
            MarketCategory::Sports,
            Some(Utc::now() + ChronoDuration::days(5)),
        ))
        .await
        .expect("seed market");
}

fn wire_live_book(registry: &MarketRegistry, book_store: &BookStore, catalog: &Catalog) {
    let market = (catalog).registry_market();
    registry.register_event(EventRegistryInfo {
        event_id: market.event_id.clone(),
        title: "Tape Conc E2E".to_owned(),
        slug: "tape-conc-e2e".to_owned(),
        series_slug: None,
        status: EventStatus::Active,
        market_ids: vec![market.market_id.clone()],
        categories: CategorySet::from(MarketCategory::Sports),
        tags: vec![MarketCategory::Sports.to_string()],
        neg_risk: false,
        end_date: market.end_date,
        created_at: Utc::now() - ChronoDuration::days(2),
        updated_at: market.updated_at,
    });
    registry.register_market(market);
    let yes = TokenId::new(catalog.yes_token);
    let timestamp_ms = u64::try_from(Utc::now().timestamp_millis())
        .expect("test book timestamp must be non-negative");
    publish_fresh_book(
        book_store,
        &yes,
        BookSnapshot::new(
            Arc::from([BookLevel::from_decimal_unchecked(
                Price::new(Decimal::new(47, 2)),
                Shares::new(Decimal::from(120)),
            )]),
            Arc::from([BookLevel::from_decimal_unchecked(
                Price::new(Decimal::new(53, 2)),
                Shares::new(Decimal::from(120)),
            )]),
            timestamp_ms,
            1,
        ),
        1,
    );
}

fn noop_feature_writer() -> Arc<FeatureEventWriter> {
    Arc::new(FeatureEventWriter::new(Arc::new(DiscardFactWriter::new())))
}

fn noop_factor_writer() -> Arc<FactorEventWriter> {
    let (writer, _worker) = AsyncWriter::new(
        AsyncWriterConfig::new("tape-conc-factor-events").capacity(256),
        |_| Box::pin(async { Ok(()) }),
        IntCounter::new("tape_conc_factor_drops", "drops").expect("counter"),
        AsyncWriterObservability::default(),
    );
    Arc::new(FactorEventWriter::new(Arc::new(writer)))
}

impl Catalog {
    fn selected_market(&self) -> SelectedMarket {
        SelectedMarket {
            market_id: MarketId::new(self.market_id),
            event_id: EventId::new(self.event_id),
            category: MarketCategory::Sports,
            primary_token_id: TokenId::new(self.yes_token),
            secondary_token_id: Some(TokenId::new(self.no_token)),
            liquidity_usd: Some(Usd::new(Decimal::from(25_000))),
            volume_24h_usd: Some(Usd::new(Decimal::from(9_000))),
            source_refs: Vec::new(),
        }
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
    _database: ScenarioDatabase,
    db: DatabaseConnection,
    registry: Arc<MarketRegistry>,
    book_store: Arc<BookStore>,
    live_pit: InMemoryDecisionSnapshotSource,
    fact_read: Arc<dyn QuantFactReadRepository>,
    market_id: MarketId,
    as_of: DateTime<Utc>,
}

impl WhaleTapeConcHarness {
    async fn fixture() -> Self {
        let (pool, database) = setup_pg().await;
        let db = pool.connection().clone();
        seed_catalog(&db, &CATALOG).await;

        let data_plane = Arc::new(DataPlane::new());
        let registry = Arc::new(MarketRegistry::new(Arc::clone(&data_plane)));
        let book_store = Arc::new(BookStore::new(data_plane, Arc::new(MetricsHub::new())));
        wire_live_book(&registry, &book_store, &CATALOG);
        let live_pit =
            InMemoryDecisionSnapshotSource::freeze(registry.as_ref(), book_store.as_ref());

        let as_of = Utc::now();
        let event_time_ms = (as_of - ChronoDuration::seconds(60)).timestamp_millis();
        let market_id = MarketId::new(CATALOG.market_id);
        let token_id = TokenId::new(CATALOG.yes_token);
        let fact_read = Arc::new(ConfigurableFactRead::new(
            Arc::new(EmptyFactRead),
            whale_concentration_by_market(&market_id, &token_id, event_time_ms),
        )) as Arc<dyn QuantFactReadRepository>;

        Self {
            _database: database,
            db,
            registry,
            book_store,
            live_pit,
            fact_read,
            market_id,
            as_of,
        }
    }
}

impl WhaleTapeConcHarness {
    async fn run_whale_feature_pipeline(&self) -> FeaturePipelineResult {
        let features = structural_features_only_config();
        let feature_repo =
            Arc::new(PgFeatureRepository::new(self.db.clone())) as Arc<dyn FeatureRepository>;
        let feature_pipeline = FeaturePipelineService::new(FeaturePipelineDeps {
            compute: Arc::new(ComputeExecutor::new().expect("test compute executor")),
            window_provider: FeatureWindowProvider::new(Arc::clone(&self.fact_read)),
            feature_repo: Arc::clone(&feature_repo),
            event_writer: noop_feature_writer(),
            exchange_history_repo: live_history_repo(),
            linkage_repo: Arc::new(EmptyLinkageRepo),
            basis_alert_repo: Arc::new(EmptyBasisAlertRepo),
            calibration_repo: Arc::new(PgCalibrationArtifactRepository::new(self.db.clone())),
            finalized_exchange_history: live_history_config(),
        });

        let domain = DomainConfig::disabled();
        let included = vec![CATALOG.selected_market()];
        let execution_history_seal = live_activation_head();
        feature_pipeline
            .run(FeaturePipelineRequest {
                included: &included,
                feature_contract: ResearchFeatureContract::FullL2,
                boundary: DecisionClock::new(0)
                    .boundary(self.as_of)
                    .expect("decision boundary"),
                features: &features,
                domain: &domain,
                data_quality: &DataQualityConfig::default(),
                model_requirements: &ModelFeatureRequirements::default(),
                pit: &self.live_pit,
                decision_policy_snapshot_id: DecisionPolicySnapshotId::from_v7(),
                liquidity_cap_usd: Usd::new(Decimal::from(10_000)),
                execution_history_seal: Some(&execution_history_seal),
            })
            .await
            .expect("feature pipeline")
    }

    async fn assert_monitor_without_head(&self) {
        let runtime = Arc::new(DecisionPolicySnapshot::default());
        let monitor = CoreStructuralMonitor::new(
            Arc::clone(&self.registry),
            Arc::clone(&self.book_store),
            Arc::new(FeatureWindowProvider::new(Arc::clone(&self.fact_read))),
            unavailable_history_repo(),
            Arc::new(FixedRuntimeConfig(runtime)),
            live_history_config(),
        );
        let summary = monitor
            .participant_concentration()
            .await
            .expect("an unwarmed serving head is an explicit unavailable monitor state");
        let market = summary
            .markets
            .iter()
            .find(|view| view.market_id == self.market_id)
            .expect("active market remains visible while finalized history is unavailable");
        assert_eq!(market.composite_raw, None);
        assert_eq!(
            market.missing_reason.as_deref(),
            Some("execution_history_unavailable")
        );
    }
}

fn concentration_feature_decimals(vector: &FeatureVector) -> (Decimal, Decimal, Decimal) {
    let gini = vector
        .value(&PARTICIPANT_GINI)
        .expect("participant gini feature");
    let cr1 = vector
        .value(&PARTICIPANT_CR1_SHARE)
        .expect("participant cr1 feature");
    let hhi = vector
        .value(&PARTICIPANT_HHI)
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
        cr1 >= Decimal::new(40, 2),
        "bilateral whale window cr1 should reflect the concentrated maker, got {cr1}"
    );
    assert!(
        gini > Decimal::ZERO,
        "gini must be positive for whale window"
    );
    assert!(hhi > Decimal::ZERO, "hhi must be positive for whale window");
    composite_concentration(gini, cr1, hhi, &composite_weights()).expect("composite from features")
}

async fn run_factor_round_concentration(
    harness: &WhaleTapeConcHarness,
    feature_result: &FeaturePipelineResult,
    expected_composite: Decimal,
) {
    let features = structural_features_only_config();
    let factors = structural_factors_config();
    let domain = DomainConfig::disabled();

    let factor_repo =
        Arc::new(PgFactorRepository::new(harness.db.clone())) as Arc<dyn FactorRepository>;
    register_all_factor_definitions(
        factor_repo.as_ref(),
        &factors,
        &features,
        &domain,
        ResearchFeatureContract::FullL2,
        None,
    )
    .await
    .expect("register immutable factor definitions");

    let model_run_id = ModelRunId::from_v7();
    PgModelRunRepository::new(harness.db.clone())
        .create(NewModelRun {
            model_run_id,
            run_kind: ModelRunKind::LiveInference,
            model_version_id: None,
            decision_policy_snapshot_id: DecisionPolicySnapshotId::from_v7(),
            market_selection_id: None,
            window_start: harness.as_of,
            window_end: harness.as_of,
            input_hash: ContentHash::parse(&format!("blake3:{}", "1".repeat(64))).expect("hash"),
        })
        .await
        .expect("model run");

    let feature_vector_ids: Vec<FeatureVectorId> = feature_result
        .persisted
        .iter()
        .map(|row| row.feature_vector_id)
        .collect();
    assert_eq!(
        feature_vector_ids.len(),
        feature_result.accepted.len(),
        "persisted feature rows must align 1:1 with accepted vectors"
    );

    let factor_service = FactorPipelineService::new(
        Arc::clone(&factor_repo),
        noop_factor_writer(),
        Arc::new(ComputeExecutor::new().expect("test compute executor")),
    );
    let factor_execution = FactorExecutionPlane::try_new(
        &factors,
        &features,
        &domain,
        ResearchFeatureContract::FullL2,
        None,
        None,
    )
    .expect("factor execution plane");
    let factor_result = factor_service
        .run(FactorPipelineRequest {
            model_run_id: &model_run_id,
            vectors: Arc::from(feature_result.accepted.clone()),
            feature_vector_ids: &feature_vector_ids,
            factor_execution: &factor_execution,
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
        .expect("participant concentration must score from finalized executions");
    assert_eq!(
        raw.round_dp(12),
        expected_composite.round_dp(12),
        "factor composite must match feature-derived canonical estimator"
    );
}

async fn assert_monitor_matches_canonical(
    harness: &WhaleTapeConcHarness,
    expected_composite: Decimal,
) {
    let runtime = Arc::new(DecisionPolicySnapshot::default());
    let monitor = CoreStructuralMonitor::new(
        Arc::clone(&harness.registry),
        Arc::clone(&harness.book_store),
        Arc::new(FeatureWindowProvider::new(Arc::clone(&harness.fact_read))),
        live_history_repo(),
        Arc::new(FixedRuntimeConfig(runtime)),
        live_history_config(),
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

pub async fn whale_execution_history_monitor() {
    let harness = WhaleTapeConcHarness::fixture().await;
    let feature_result = harness.run_whale_feature_pipeline().await;

    assert_eq!(feature_result.accepted.len(), 1, "vector must be accepted");
    assert_eq!(
        feature_result.persisted.len(),
        1,
        "feature vector must persist to Postgres"
    );
    let expected_composite = assert_whale_concentration_features(&feature_result.accepted[0]);

    run_factor_round_concentration(&harness, &feature_result, expected_composite).await;
    assert_monitor_matches_canonical(&harness, expected_composite).await;
    harness.assert_monitor_without_head().await;
    drop(harness);
}
