//! End-to-end model runtime: selection + features → [`ModelRunner`] →
//! [`SignalCandidate`]s + [`ModelRun`] lifecycle + `ClickHouse` facts, plus the
//! model-run repository finalize transitions.

use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

use async_trait::async_trait;
use chrono::{Duration as ChronoDuration, Utc};
use quant_pivot_core::{
    governance::{BiasTableApplicator, CoreCalibrationArtifactLoader, WeightOverlayApplicator},
    ingest::{book_store::BookStore, market_registry::MarketRegistry},
    observability::{
        factor_fact_writer::FactorEventWriter, feature_fact_writer::FeatureEventWriter,
        metrics_hub::MetricsHub, model_input_fact_writer::ModelInputEventWriter,
        serving_evidence::FeatureEvidenceCommitment,
        signal_candidate_fact_writer::SignalCandidateEventWriter,
    },
    prefetch::feature_window::FeatureWindowProvider,
    service::{
        factor_pipeline::FactorPipelineService,
        feature_pipeline::{FeaturePipelineDeps, FeaturePipelineRequest, FeaturePipelineService},
        model_runner::{
            InferenceAlertSink, ModelRunOutcome, ModelRunRequest, ModelRunner, ModelRunnerDeps,
        },
    },
};
use quant_pivot_error::storage::StorageError;
use quant_pivot_models::{
    clickhouse::{
        BookL2CheckpointRow, BookMicrostructureRow, DomainObservationRow, MarketResolutionRow,
        MidPriceBucketRow, TradeTapeRow,
    },
    config::TradeTapeOnChainConfig,
    domain::{
        DecisionBoundary, DecisionClock, NewModelRun, NewModelSpec, NewModelVersion,
        market::{EventRegistryInfo, MarketRegistryInfo, TokenInfo, book::BookLevel},
    },
    enums::{
        common::{CategorySet, MarketCategory, TickSize},
        factor::FactorFamily,
        market::{EventStatus, MarketStatus},
        model::ModelFamily,
        quant::{ModelRunErrorCode, ModelRunKind, ModelRunStatus, PublicationStatus},
    },
    runtime_config::{
        DataQualityConfig, DecimalString, DomainConfig, FactorCrossSectionConfig, FactorWeights,
        FactorsConfig, FeaturesConfig, ModelConfig, ModelVersionRef,
    },
    types::{
        ContentHash, DomainInstrumentKey, EventId, FeatureVectorId, MarketId, ModelInputContract,
        ModelRunId, ModelSpecId, ModelTrainingContract, ModelVersionId, Price,
        RuntimeConfigVersionId, SchemaVersion, Shares, TokenId, Usd,
    },
};
use quant_pivot_repository::{
    postgres::{
        PgCalibrationArtifactRepository, PgEventRepository, PgFactorRepository,
        PgFeatureRepository, PgMarketRepository, PgModelRegistryRepository, PgModelRunRepository,
        PgShadowComparisonRepository,
    },
    traits::{
        CalibrationArtifactRepository, EventRepository, FactorRepository, FeatureRepository,
        MarketRepository, ModelRegistryRepository, ModelRunRepository, QuantFactReadRepository,
        ShadowComparisonRepository,
    },
};
use quant_pivot_research::{
    artifact::{ArtifactStore, LocalArtifactStore},
    factors::{FactorEngine, FrozenReferenceQuantiles},
    features::{FeatureSchema, FeatureVector},
    hashing::ResearchHasher,
    model::{
        CalibrationArtifactLoader, DefaultModelRuntimeFactoryBuilder, FactorWeight, ModelArtifact,
        ModelArtifactHeader, ReturnModelSpec, ScoreMultiplierSpec, SubstitutionConfidenceRules,
        WeightedFactorModelArtifact, model_input_contract_hash,
    },
    selection::{ModelFeatureRequirements, SelectedMarket},
};
use quant_pivot_storage::write::{AsyncWriter, AsyncWriterConfig, AsyncWriterObservability};
use quant_pivot_test_support::{
    catalog_fixtures::{make_event, make_market},
    factor_governance::publish_all_factor_definitions,
    pg::setup_pg,
    report_pipeline_harness::{EmptyBasisAlertRepo, EmptyLinkageRepo},
    trade_tape_fixtures::live_trade_tape_block_cursor_repo,
};
use quant_pivot_test_support::{fact_sink::DiscardFactWriter, pit::InMemoryDecisionSnapshotSource};
use rust_decimal::Decimal;
use sea_orm::DatabaseConnection;

const EVENT_ID: &str = "evt-model-e2e";
const MARKET_ID: &str = "0xmodele2e";
const YES_TOKEN: &str = "77777";
const NO_TOKEN: &str = "88888";

/// Counts the critical alerts raised during a round.
struct CountingAlertSink {
    critical: Arc<AtomicUsize>,
}

impl InferenceAlertSink for CountingAlertSink {
    fn critical(&self, _title: String, _body: String) {
        self.critical.fetch_add(1, Ordering::Relaxed);
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

fn registry_market() -> MarketRegistryInfo {
    MarketRegistryInfo {
        market_id: MarketId::new(MARKET_ID),
        event_id: EventId::new(EVENT_ID),
        token_yes: TokenId::new(YES_TOKEN),
        token_no: TokenId::new(NO_TOKEN),
        question: "Model E2E?".into(),
        slug: "model-e2e".into(),
        description: None,
        categories: CategorySet::from(MarketCategory::Sports),
        status: MarketStatus::Active,
        outcome: None,
        neg_risk: false,
        tick_size: TickSize::Hundredth,
        tokens: vec![
            TokenInfo {
                token_id: TokenId::new(YES_TOKEN),
                outcome: "Yes".into(),
                neg_risk: false,
            },
            TokenInfo {
                token_id: TokenId::new(NO_TOKEN),
                outcome: "No".into(),
                neg_risk: false,
            },
        ],
        best_bid: None,
        best_ask: None,
        depth_usd: None,
        min_order_size: Decimal::ONE,
        liquidity_usd: Some(Usd::new(Decimal::from(60_000))),
        volume_24h: Some(Usd::new(Decimal::from(9_000))),
        fee_schedule: None,
        start_date: Some(Utc::now() - ChronoDuration::days(2)),
        end_date: Some(Utc::now() + ChronoDuration::days(2)),
        resolved_at: None,
        created_at: Some(Utc::now() - ChronoDuration::days(2)),
        updated_at: Utc::now(),
    }
}

fn register_runtime_event(registry: &MarketRegistry, market_ids: Vec<MarketId>) {
    registry.register_event(EventRegistryInfo {
        event_id: EventId::new(EVENT_ID),
        title: "Model E2E".to_owned(),
        slug: "model-e2e".to_owned(),
        series_slug: None,
        status: EventStatus::Active,
        market_ids,
        categories: CategorySet::from(MarketCategory::Sports),
        tags: vec![MarketCategory::Sports.as_str().to_owned()],
        neg_risk: false,
        end_date: Some(Utc::now() + ChronoDuration::days(2)),
        created_at: Utc::now() - ChronoDuration::days(2),
        updated_at: Utc::now(),
    });
}

async fn seed_catalog(db: &DatabaseConnection) {
    PgEventRepository::new(db.clone())
        .upsert(make_event(
            EVENT_ID,
            "Model E2E",
            "model-e2e",
            MarketCategory::Sports,
        ))
        .await
        .expect("seed event");
    PgMarketRepository::new(db.clone())
        .upsert(make_market(
            MARKET_ID,
            EVENT_ID,
            "Model E2E?",
            "model-e2e",
            MarketCategory::Sports,
            Some(Utc::now() + ChronoDuration::days(2)),
        ))
        .await
        .expect("seed market");
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
        market_id: MarketId::new(MARKET_ID),
        event_id: EventId::new(EVENT_ID),
        category: MarketCategory::Sports,
        primary_token_id: TokenId::new(YES_TOKEN),
        secondary_token_id: Some(TokenId::new(NO_TOKEN)),
        liquidity_usd: Some(Usd::new(Decimal::from(60_000))),
        volume_24h_usd: Some(Usd::new(Decimal::from(9_000))),
        source_refs: Vec::new(),
    }
}

fn noop_feature_writer() -> Arc<FeatureEventWriter> {
    Arc::new(FeatureEventWriter::new(Arc::new(DiscardFactWriter::new())))
}

fn noop_factor_writer() -> Arc<FactorEventWriter> {
    let (writer, _worker) = AsyncWriter::new(
        AsyncWriterConfig::new("model-e2e-factor").capacity(64),
        |_| Box::pin(async { Ok(()) }),
        prometheus::IntCounter::new("model_e2e_fac_drops", "d").expect("counter"),
        AsyncWriterObservability::default(),
    );
    Arc::new(FactorEventWriter::new(Arc::new(writer)))
}

fn noop_signal_writer() -> Arc<SignalCandidateEventWriter> {
    let (writer, _worker) = AsyncWriter::new(
        AsyncWriterConfig::new("model-e2e-signal").capacity(64),
        |_| Box::pin(async { Ok(()) }),
        prometheus::IntCounter::new("model_e2e_sig_drops", "d").expect("counter"),
        AsyncWriterObservability::default(),
    );
    Arc::new(SignalCandidateEventWriter::new(Arc::new(writer)))
}

fn noop_model_input_writer() -> Arc<ModelInputEventWriter> {
    Arc::new(ModelInputEventWriter::new(
        Arc::new(DiscardFactWriter::new()),
        Arc::new(DiscardFactWriter::new()),
    ))
}

/// Build + persist feature vectors for the seeded market via the live book.
async fn build_features(
    db: &DatabaseConnection,
) -> (
    Vec<FeatureVector>,
    Vec<FeatureVectorId>,
    FeatureEvidenceCommitment,
    DecisionBoundary,
) {
    let registry = Arc::new(MarketRegistry::new());
    let book_store = Arc::new(BookStore::new(Arc::new(MetricsHub::new())));
    let market = registry_market();
    register_runtime_event(&registry, vec![market.market_id.clone()]);
    registry.register_market(market);
    book_store.apply_snapshot(
        &TokenId::new(YES_TOKEN),
        Arc::from([BookLevel::from_decimal_unchecked(
            Price::new(Decimal::new(47, 2)),
            Shares::new(Decimal::from(150)),
        )]),
        Arc::from([BookLevel::from_decimal_unchecked(
            Price::new(Decimal::new(53, 2)),
            Shares::new(Decimal::from(140)),
        )]),
        u64::try_from(Utc::now().timestamp_millis())
            .expect("test book timestamp must be non-negative"),
        None,
    );
    let live_pit = InMemoryDecisionSnapshotSource::freeze(registry.as_ref(), book_store.as_ref());

    let feature_repo = Arc::new(PgFeatureRepository::new(db.clone())) as Arc<dyn FeatureRepository>;
    let pipeline = FeaturePipelineService::new(FeaturePipelineDeps {
        window_provider: FeatureWindowProvider::new(Arc::new(EmptyFactRead)),
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
    let boundary = DecisionClock::new(0)
        .boundary(Utc::now())
        .expect("decision boundary");
    let result = pipeline
        .run(FeaturePipelineRequest {
            included: &included,
            boundary: boundary.clone(),
            features: &features,
            domain: &domain,
            data_quality: &DataQualityConfig::default(),
            model_requirements: &ModelFeatureRequirements::default(),
            pit: &live_pit,
            runtime_config_version_id: RuntimeConfigVersionId::from_v7(),
            liquidity_cap_usd: Usd::new(rust_decimal::Decimal::from(10_000)),
        })
        .await
        .expect("feature pipeline");

    assert_eq!(result.accepted.len(), 1, "market must produce a vector");
    let ids = result
        .persisted
        .iter()
        .map(|info| info.feature_vector_id.clone())
        .collect();
    (
        result.accepted,
        ids,
        result.feature_evidence.expect("durable feature evidence"),
        boundary,
    )
}

/// Peer markets forming a real cross-section alongside the primary market, so
/// the cross-sectional factors (`liquidity_depth` rank, `spread_efficiency`
/// z-score) are *scored* rather than indeterminate. Overlay-reweighting tests
/// need a cross-section ≥ `cross_section.min_size` (5) with dispersion.
const PEER_COUNT: usize = 5;

fn peer_market_id(index: usize) -> String {
    format!("0xmodele2epeer{index}")
}

fn peer_yes(index: usize) -> String {
    format!("7000{index}")
}

fn peer_no(index: usize) -> String {
    format!("8000{index}")
}

fn peer_registry_market(index: usize) -> MarketRegistryInfo {
    let yes = peer_yes(index);
    let no = peer_no(index);
    let step = i64::try_from(index).expect("index fits i64");
    MarketRegistryInfo {
        market_id: MarketId::new(peer_market_id(index)),
        event_id: EventId::new(EVENT_ID),
        token_yes: TokenId::new(&yes),
        token_no: TokenId::new(&no),
        question: format!("Model E2E peer {index}?"),
        slug: format!("model-e2e-peer-{index}"),
        description: None,
        categories: CategorySet::from(MarketCategory::Sports),
        status: MarketStatus::Active,
        outcome: None,
        neg_risk: false,
        tick_size: TickSize::Hundredth,
        tokens: vec![
            TokenInfo {
                token_id: TokenId::new(&yes),
                outcome: "Yes".into(),
                neg_risk: false,
            },
            TokenInfo {
                token_id: TokenId::new(&no),
                outcome: "No".into(),
                neg_risk: false,
            },
        ],
        best_bid: None,
        best_ask: None,
        depth_usd: None,
        min_order_size: Decimal::ONE,
        liquidity_usd: Some(Usd::new(Decimal::from(40_000 + 8_000 * step))),
        volume_24h: Some(Usd::new(Decimal::from(9_000))),
        fee_schedule: None,
        start_date: Some(Utc::now() - ChronoDuration::days(2)),
        end_date: Some(Utc::now() + ChronoDuration::days(2)),
        resolved_at: None,
        created_at: Some(Utc::now() - ChronoDuration::days(2)),
        updated_at: Utc::now(),
    }
}

fn peer_selected_market(index: usize) -> SelectedMarket {
    let step = i64::try_from(index).expect("index fits i64");
    SelectedMarket {
        market_id: MarketId::new(peer_market_id(index)),
        event_id: EventId::new(EVENT_ID),
        category: MarketCategory::Sports,
        primary_token_id: TokenId::new(peer_yes(index)),
        secondary_token_id: Some(TokenId::new(peer_no(index))),
        liquidity_usd: Some(Usd::new(Decimal::from(40_000 + 8_000 * step))),
        volume_24h_usd: Some(Usd::new(Decimal::from(9_000))),
        source_refs: Vec::new(),
    }
}

/// Recover the `SelectedMarket` for a computed vector's market id (keeps the
/// selection index-aligned with the pipeline's accepted-vector order).
fn selected_for(market_id: &MarketId) -> SelectedMarket {
    let id = market_id.as_str();
    if id == MARKET_ID {
        return selected_market();
    }
    for index in 0..PEER_COUNT {
        if id == peer_market_id(index) {
            return peer_selected_market(index);
        }
    }
    panic!("unknown market id in cross-section: {id}");
}

/// Seed the peer markets (FK targets for their feature vectors / factor values).
async fn seed_peer_markets(db: &DatabaseConnection) {
    let repo = PgMarketRepository::new(db.clone());
    for index in 0..PEER_COUNT {
        repo.upsert(make_market(
            &peer_market_id(index),
            EVENT_ID,
            "Model E2E peer?",
            &format!("model-e2e-peer-{index}"),
            MarketCategory::Sports,
            Some(Utc::now() + ChronoDuration::days(2)),
        ))
        .await
        .expect("seed peer market");
    }
}

/// Apply one market's book snapshot to the store.
fn apply_book(
    book_store: &BookStore,
    token: &str,
    bid_px: Decimal,
    bid_shares: i64,
    ask_px: Decimal,
    ask_shares: i64,
    ts_ms: u64,
) {
    book_store.apply_snapshot(
        &TokenId::new(token),
        Arc::from([BookLevel::from_decimal_unchecked(
            Price::new(bid_px),
            Shares::new(Decimal::from(bid_shares)),
        )]),
        Arc::from([BookLevel::from_decimal_unchecked(
            Price::new(ask_px),
            Shares::new(Decimal::from(ask_shares)),
        )]),
        ts_ms,
        None,
    );
}

/// Build + persist feature vectors for the primary market plus dispersed peers,
/// yielding a real cross-section (order-aligned `vectors` / `ids` / `selection`).
async fn build_cross_section_features(
    db: &DatabaseConnection,
) -> (
    Vec<FeatureVector>,
    Vec<FeatureVectorId>,
    Vec<SelectedMarket>,
    FeatureEvidenceCommitment,
    DecisionBoundary,
) {
    let registry = Arc::new(MarketRegistry::new());
    let book_store = Arc::new(BookStore::new(Arc::new(MetricsHub::new())));
    let ts_ms = u64::try_from(Utc::now().timestamp_millis())
        .expect("test book timestamp must be non-negative");
    let primary = registry_market();
    let peers = (0..PEER_COUNT)
        .map(peer_registry_market)
        .collect::<Vec<_>>();
    let market_ids = std::iter::once(primary.market_id.clone())
        .chain(peers.iter().map(|market| market.market_id.clone()))
        .collect();
    register_runtime_event(&registry, market_ids);
    registry.register_market(primary);
    apply_book(
        &book_store,
        YES_TOKEN,
        Decimal::new(47, 2),
        150,
        Decimal::new(53, 2),
        140,
        ts_ms,
    );
    for (index, peer) in peers.into_iter().enumerate() {
        registry.register_market(peer);
        let step = i64::try_from(index).expect("index fits i64");
        // Dispersed prices / depths so the cross-section carries real variance.
        apply_book(
            &book_store,
            &peer_yes(index),
            Decimal::new(40 + 3 * step, 2),
            120 + 40 * step,
            Decimal::new(46 + 4 * step, 2),
            110 + 30 * step,
            ts_ms,
        );
    }
    let live_pit = InMemoryDecisionSnapshotSource::freeze(registry.as_ref(), book_store.as_ref());

    let feature_repo = Arc::new(PgFeatureRepository::new(db.clone())) as Arc<dyn FeatureRepository>;
    let pipeline = FeaturePipelineService::new(FeaturePipelineDeps {
        window_provider: FeatureWindowProvider::new(Arc::new(EmptyFactRead)),
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
    let included: Vec<SelectedMarket> = std::iter::once(selected_market())
        .chain((0..PEER_COUNT).map(peer_selected_market))
        .collect();
    let boundary = DecisionClock::new(0)
        .boundary(Utc::now())
        .expect("decision boundary");
    let result = pipeline
        .run(FeaturePipelineRequest {
            included: &included,
            boundary: boundary.clone(),
            features: &features,
            domain: &domain,
            data_quality: &DataQualityConfig::default(),
            model_requirements: &ModelFeatureRequirements::default(),
            pit: &live_pit,
            runtime_config_version_id: RuntimeConfigVersionId::from_v7(),
            liquidity_cap_usd: Usd::new(rust_decimal::Decimal::from(10_000)),
        })
        .await
        .expect("feature pipeline");

    assert_eq!(
        result.accepted.len(),
        1 + PEER_COUNT,
        "every cross-section market must produce a vector"
    );
    let ids = result
        .persisted
        .iter()
        .map(|info| info.feature_vector_id.clone())
        .collect();
    let selection = result
        .accepted
        .iter()
        .map(|vector| selected_for(&vector.market_id))
        .collect();
    (
        result.accepted,
        ids,
        selection,
        result.feature_evidence.expect("durable feature evidence"),
        boundary,
    )
}

/// Author a weighted artifact bound to the active schema, weighting every enabled
/// factor equally, and persist its bytes + registry rows. Returns the version id.
async fn publish_weighted_model(
    db: &DatabaseConnection,
    store: &Arc<dyn ArtifactStore>,
    factors: &FactorsConfig,
    features: &FeaturesConfig,
) -> ModelVersionId {
    let engine = FactorEngine::new(factors, features, &DomainConfig::default(), None);
    let factor_set = engine.factor_set();
    let count = factor_set.definitions.len();
    assert!(count > 0, "the factor set must be non-empty");
    let each = Decimal::ONE / Decimal::from(u64::try_from(count).expect("count"));
    let mut weights: Vec<FactorWeight> = factor_set
        .definitions
        .iter()
        .map(|spec| FactorWeight {
            factor: spec.name.clone(),
            weight: each,
        })
        .collect();
    // Make the set sum to exactly 1 by absorbing the rounding into the first weight.
    let tail: Decimal = weights.iter().skip(1).map(|w| w.weight).sum();
    weights[0].weight = Decimal::ONE - tail;

    let model_version_id = ModelVersionId::from_v7();
    let feature_schema_hash =
        ResearchHasher::feature_schema(&FeatureSchema::build(features).expect("feature schema"))
            .expect("feature hash");
    let factor_schema_hash = engine.factor_schema_hash().expect("factor hash");
    let input_contract = ModelInputContract::single_required("book.mid");
    let input_contract_hash =
        model_input_contract_hash(&input_contract).expect("input contract hash");

    let artifact = ModelArtifact::WeightedFactor(Box::new(WeightedFactorModelArtifact {
        header: ModelArtifactHeader {
            model_version_id: model_version_id.clone(),
            model_family: ModelFamily::WeightedFactor,
            feature_schema_hash,
            factor_schema_hash: factor_schema_hash.clone(),
            trade_policy_artifact_id: None,
            trade_policy_hash: None,
        },
        training_dataset_hash: factor_schema_hash.clone(),
        training_input_hash: factor_schema_hash,
        input_contract,
        input_contract_hash,
        weights,
        prediction_horizon_secs: 86_400,
        multipliers: ScoreMultiplierSpec::conservative(),
        substitution_confidence_rules: SubstitutionConfidenceRules::conservative(),
        return_model: ReturnModelSpec::heuristic_default(),
        factor_cross_section: FactorCrossSectionConfig::default(),
        frozen_reference_quantiles: FrozenReferenceQuantiles::empty(),
        objective_report: None,
        category_scope: None,
    }));
    artifact.validate().expect("artifact valid");
    let artifact_hash = artifact.content_hash().expect("hash");
    let key = ModelArtifact::artifact_key(&artifact_hash).expect("key");
    store
        .put(key, &artifact.to_bytes().expect("bytes"))
        .await
        .expect("store artifact");

    let registry = PgModelRegistryRepository::new(db.clone());
    let model_spec_id = ModelSpecId::from_v7();
    registry
        .create_model_spec(NewModelSpec {
            model_spec_id: model_spec_id.clone(),
            name: "weighted-e2e".to_owned(),
            model_family: ModelFamily::WeightedFactor,
            prediction_horizon_secs: 86_400,
            feature_schema_version: SchemaVersion::FIRST,
            label_schema_version: SchemaVersion::FIRST,
            spec_json: serde_json::json!({}),
            input_contract: ModelInputContract::single_required("book.mid"),
            training_contract: ModelTrainingContract::settlement_default(),
            status: PublicationStatus::Published,
        })
        .await
        .expect("create spec");
    registry
        .create_model_version(NewModelVersion {
            model_version_id: model_version_id.clone(),
            model_spec_id,
            version: 1,
            artifact_hash,
            training_dataset_id: None,
            trade_policy_artifact_id: None,
            trade_policy_hash: None,
            publish_path_set_id: None,
            metrics_json: serde_json::json!({}),
            training_objective_json: serde_json::json!({"kind": "not_trained"}),
            quality_gate_report: serde_json::json!({}),
            publication_status: PublicationStatus::Published,
            published_at: Some(Utc::now()),
            retired_at: None,
        })
        .await
        .expect("create version");

    model_version_id
}

/// Register a sibling candidate with the same weights as `published`, but a
/// distinct artifact header (content-addressed per version id).
async fn register_candidate_sibling(
    db: &DatabaseConnection,
    store: &Arc<dyn ArtifactStore>,
    published_id: &ModelVersionId,
) -> ModelVersionId {
    let registry = PgModelRegistryRepository::new(db.clone());
    let published = registry
        .find_model_version_by_id(published_id)
        .await
        .expect("find published")
        .expect("published row");
    let key = ModelArtifact::artifact_key(&published.artifact_hash).expect("artifact key");
    let bytes = store
        .get_by_key(&key)
        .await
        .expect("load published artifact bytes");
    let mut artifact = ModelArtifact::from_bytes(&bytes).expect("decode artifact");
    let candidate_id = ModelVersionId::from_v7();
    match &mut artifact {
        ModelArtifact::WeightedFactor(weighted) => {
            weighted.header.model_version_id = candidate_id.clone();
        }
        ModelArtifact::Classical(classical) => {
            classical.header.model_version_id = candidate_id.clone();
        }
        ModelArtifact::SellScorer(sell) => {
            sell.header.model_version_id = candidate_id.clone();
        }
    }
    artifact.validate().expect("candidate artifact valid");
    let artifact_hash = artifact.content_hash().expect("candidate hash");
    let candidate_key = ModelArtifact::artifact_key(&artifact_hash).expect("candidate key");
    store
        .put(
            candidate_key,
            &artifact.to_bytes().expect("candidate bytes"),
        )
        .await
        .expect("store candidate artifact");
    registry
        .create_model_version(NewModelVersion {
            model_version_id: candidate_id.clone(),
            model_spec_id: published.model_spec_id.clone(),
            version: published.version + 1,
            artifact_hash,
            training_dataset_id: published.training_dataset_id.clone(),
            trade_policy_artifact_id: published.trade_policy_artifact_id.clone(),
            trade_policy_hash: published.trade_policy_hash.clone(),
            publish_path_set_id: None,
            metrics_json: serde_json::json!({}),
            training_objective_json: serde_json::json!({"kind": "not_trained"}),
            quality_gate_report: serde_json::json!({}),
            publication_status: PublicationStatus::Candidate,
            published_at: None,
            retired_at: None,
        })
        .await
        .expect("create candidate sibling");
    candidate_id
}

/// Build a normalized overlay skewed toward the factor at `lead_index`.
fn factors_with_overlay_skew(
    base: &FactorsConfig,
    features: &FeaturesConfig,
    lead_index: usize,
) -> FactorsConfig {
    let engine = FactorEngine::new(base, features, &DomainConfig::default(), None);
    let definitions = &engine.factor_set().definitions;
    assert!(
        definitions.len() >= 2,
        "overlay test needs at least two factors"
    );
    let lead = Decimal::new(90, 2);
    let tail = (Decimal::ONE - lead)
        / Decimal::from(u64::try_from(definitions.len().saturating_sub(1)).expect("count"));
    let mut weights = FactorWeights::default();
    for (index, spec) in definitions.iter().enumerate() {
        let value = if index == lead_index { lead } else { tail };
        weights.weights.insert(
            spec.name.as_str().to_owned(),
            DecimalString::new(value.to_string()),
        );
    }
    let mut config = base.clone();
    config.factor_weights = weights;
    config
}

fn model_config(active: &ModelVersionId, shadow: Option<&str>) -> ModelConfig {
    ModelConfig {
        active_model_version_id: Some(ModelVersionRef {
            id: active.to_string(),
        }),
        shadow_model_version_id: shadow.map(|id| ModelVersionRef { id: id.to_owned() }),
        min_model_confidence: DecimalString::new("0.00"),
        candidate_score_floor: DecimalString::new("0.00"),
        ..ModelConfig::default()
    }
}

fn build_runner(
    db: &DatabaseConnection,
    store: Arc<dyn ArtifactStore>,
    critical: Arc<AtomicUsize>,
    weight_overlay: Arc<WeightOverlayApplicator>,
) -> ModelRunner {
    let factor_repo = Arc::new(PgFactorRepository::new(db.clone())) as Arc<dyn FactorRepository>;
    let calibration_repo = Arc::new(PgCalibrationArtifactRepository::new(db.clone()))
        as Arc<dyn CalibrationArtifactRepository>;
    let calibration_loader: Arc<dyn CalibrationArtifactLoader> = Arc::new(
        CoreCalibrationArtifactLoader::new(Arc::clone(&calibration_repo)),
    );
    let bias_table = Arc::new(BiasTableApplicator::new(calibration_repo));
    let factor_pipeline = Arc::new(FactorPipelineService::new(
        factor_repo,
        noop_factor_writer(),
        Arc::clone(&bias_table),
    ));
    let model_run_repo =
        Arc::new(PgModelRunRepository::new(db.clone())) as Arc<dyn ModelRunRepository>;
    let registry =
        Arc::new(PgModelRegistryRepository::new(db.clone())) as Arc<dyn ModelRegistryRepository>;
    let shadow_comparison_repo = Arc::new(PgShadowComparisonRepository::new(db.clone()))
        as Arc<dyn ShadowComparisonRepository>;
    ModelRunner::new(ModelRunnerDeps {
        model_run_repo,
        model_registry_repo: registry,
        shadow_comparison_repo,
        factory_builder: Arc::new(DefaultModelRuntimeFactoryBuilder::new(
            store,
            calibration_loader,
        )),
        factor_pipeline,
        signal_writer: noop_signal_writer(),
        model_input_writer: noop_model_input_writer(),
        alerts: Arc::new(CountingAlertSink { critical }),
        weight_overlay,
        bias_table,
    })
}

async fn publish_enabled_factors(
    db: &DatabaseConnection,
    factors: &FactorsConfig,
    features: &FeaturesConfig,
) {
    let repo = PgFactorRepository::new(db.clone());
    publish_all_factor_definitions(&repo, factors, features, &DomainConfig::default())
        .await
        .expect("publish factor definitions");
}

fn artifact_store() -> Arc<dyn ArtifactStore> {
    let root = std::env::temp_dir().join(format!(
        "qp_model_e2e_{}_{}",
        std::process::id(),
        Utc::now().timestamp_nanos_opt().unwrap_or_default()
    ));
    Arc::new(LocalArtifactStore::new(root))
}

#[tokio::test]
async fn online_loop_selection_to_signal_candidates() {
    let (pool, _container) = setup_pg().await;
    let db = pool.connection().clone();
    seed_catalog(&db).await;

    let factors = factors_config();
    let features = FeaturesConfig::default();
    let (vectors, ids, evidence, boundary) = build_features(&db).await;

    let store = artifact_store();
    let active = publish_weighted_model(&db, &store, &factors, &features).await;
    publish_enabled_factors(&db, &factors, &features).await;

    let critical = Arc::new(AtomicUsize::new(0));
    let runner = build_runner(
        &db,
        Arc::clone(&store),
        Arc::clone(&critical),
        Arc::new(WeightOverlayApplicator::new()),
    );

    let selection = vec![selected_market()];
    let outcome = runner
        .run(ModelRunRequest {
            runtime_config_version_id: RuntimeConfigVersionId::from_v7(),
            market_selection_id: None,
            selection: &selection,
            feature_vectors: &vectors,
            feature_vector_ids: &ids,
            feature_evidence: &evidence,
            features: &features,
            factors: &factors,
            domain: &DomainConfig::default(),
            model: &model_config(&active, None),
            top_n: 10,
            boundary,
        })
        .await
        .expect("inference round");

    assert!(outcome.emitted >= 1, "the market must produce a candidate");
    assert!(
        !outcome.accepted.is_empty(),
        "with zero floors the candidate must be accepted"
    );
    assert!(outcome.shadow.is_none(), "no shadow configured");
    assert_eq!(
        critical.load(Ordering::Relaxed),
        0,
        "no critical alert on success"
    );

    // The model run is finalized with an input + output hash.
    let model_run_repo = PgModelRunRepository::new(db.clone());
    let run = model_run_repo
        .find_by_id(&outcome.model_run_id)
        .await
        .expect("find run")
        .expect("run row");
    assert_eq!(run.status, ModelRunStatus::Succeeded);
    assert_eq!(run.run_kind, ModelRunKind::LiveInference);
    assert!(run.output_hash.is_some(), "output hash recorded");
    assert!(run.model_version_id.is_some(), "active version recorded");

    // Factor values are owned by the run (the new FK resolved).
    let factor_repo = PgFactorRepository::new(db.clone());
    let values = factor_repo
        .list_values_for_run(&outcome.model_run_id)
        .await
        .expect("list values");
    assert!(!values.is_empty(), "factor values persisted under the run");
}

#[tokio::test]
async fn inference_degradation_shadow_failure_keeps_active() {
    let (pool, _container) = setup_pg().await;
    let db = pool.connection().clone();
    seed_catalog(&db).await;

    let factors = factors_config();
    let features = FeaturesConfig::default();
    let (vectors, ids, evidence, boundary) = build_features(&db).await;

    let store = artifact_store();
    let active = publish_weighted_model(&db, &store, &factors, &features).await;
    publish_enabled_factors(&db, &factors, &features).await;

    let critical = Arc::new(AtomicUsize::new(0));
    let runner = build_runner(
        &db,
        Arc::clone(&store),
        Arc::clone(&critical),
        Arc::new(WeightOverlayApplicator::new()),
    );

    // Shadow points at a version that does not exist: the shadow path degrades,
    // the active result is unaffected, and no critical alert fires.
    let missing_shadow = ModelVersionId::from_v7().to_string();
    let selection = vec![selected_market()];
    let outcome = runner
        .run(ModelRunRequest {
            runtime_config_version_id: RuntimeConfigVersionId::from_v7(),
            market_selection_id: None,
            selection: &selection,
            feature_vectors: &vectors,
            feature_vector_ids: &ids,
            feature_evidence: &evidence,
            features: &features,
            factors: &factors,
            domain: &DomainConfig::default(),
            model: &model_config(&active, Some(&missing_shadow)),
            top_n: 10,
            boundary,
        })
        .await
        .expect("active round survives shadow failure");

    assert!(!outcome.accepted.is_empty(), "active candidates produced");
    let shadow = outcome.shadow.expect("shadow attempted");
    assert!(shadow.failure.is_some(), "shadow recorded a failure");
    assert!(
        shadow.model_run_id.is_none(),
        "early shadow failure before create_run must not expose a phantom run id"
    );
    assert_eq!(
        critical.load(Ordering::Relaxed),
        0,
        "a shadow failure must not raise a critical alert"
    );
}

struct OverlayRoundInput<'a> {
    runner: &'a ModelRunner,
    overlay: &'a WeightOverlayApplicator,
    base_factors: &'a FactorsConfig,
    features: &'a FeaturesConfig,
    model: &'a ModelConfig,
    selection: &'a [SelectedMarket],
    vectors: &'a [FeatureVector],
    ids: &'a [FeatureVectorId],
    evidence: &'a FeatureEvidenceCommitment,
    boundary: DecisionBoundary,
    skew: usize,
}

async fn run_overlay_round(input: OverlayRoundInput<'_>) -> ModelRunOutcome {
    let OverlayRoundInput {
        runner,
        overlay,
        base_factors,
        features,
        model,
        selection,
        vectors,
        ids,
        evidence,
        boundary,
        skew,
    } = input;
    overlay.reload(
        &factors_with_overlay_skew(base_factors, features, skew),
        model,
    );
    runner
        .run(ModelRunRequest {
            runtime_config_version_id: RuntimeConfigVersionId::from_v7(),
            market_selection_id: None,
            selection,
            feature_vectors: vectors,
            feature_vector_ids: ids,
            feature_evidence: evidence,
            features,
            factors: base_factors,
            domain: &DomainConfig::default(),
            model,
            top_n: 10,
            boundary,
        })
        .await
        .expect("overlay round")
}

#[tokio::test]
async fn hot_update_changes_candidate_weights_not_published_artifact() {
    let (pool, _container) = setup_pg().await;
    let db = pool.connection().clone();
    seed_catalog(&db).await;
    seed_peer_markets(&db).await;

    let base_factors = factors_config();
    let features = FeaturesConfig::default();
    // A real multi-market cross-section: the overlay reweights *scored*
    // cross-sectional factors, so shadow scores must shift when weights change
    // (a single-market cross-section would leave them all indeterminate).
    let (vectors, ids, selection, evidence, boundary) = build_cross_section_features(&db).await;

    let store = artifact_store();
    let published = publish_weighted_model(&db, &store, &base_factors, &features).await;
    publish_enabled_factors(&db, &base_factors, &features).await;
    let candidate = register_candidate_sibling(&db, &store, &published).await;

    let overlay = Arc::new(WeightOverlayApplicator::new());
    let critical = Arc::new(AtomicUsize::new(0));
    let runner = build_runner(
        &db,
        Arc::clone(&store),
        Arc::clone(&critical),
        Arc::clone(&overlay),
    );

    let model = model_config(&published, Some(&candidate.to_string()));
    let first = run_overlay_round(OverlayRoundInput {
        runner: &runner,
        overlay: &overlay,
        base_factors: &base_factors,
        features: &features,
        model: &model,
        selection: &selection,
        vectors: &vectors,
        ids: &ids,
        evidence: &evidence,
        boundary: boundary.clone(),
        skew: 0,
    })
    .await;
    let second = run_overlay_round(OverlayRoundInput {
        runner: &runner,
        overlay: &overlay,
        base_factors: &base_factors,
        features: &features,
        model: &model,
        selection: &selection,
        vectors: &vectors,
        ids: &ids,
        evidence: &evidence,
        boundary,
        skew: 1,
    })
    .await;

    let active_first = first.accepted[0].composite_score.inner();
    let active_second = second.accepted[0].composite_score.inner();
    assert_eq!(
        active_first, active_second,
        "published active must ignore config overlay changes"
    );

    let shadow_first = first
        .shadow
        .as_ref()
        .and_then(|outcome| outcome.diff.as_ref())
        .expect("shadow diff on first round")
        .mean_score_diff;
    let shadow_second = second
        .shadow
        .as_ref()
        .and_then(|outcome| outcome.diff.as_ref())
        .expect("shadow diff on second round")
        .mean_score_diff;
    assert_ne!(
        shadow_first, shadow_second,
        "candidate shadow scores must shift when overlay weights change"
    );

    let shadow_run_id = second
        .shadow
        .as_ref()
        .and_then(|outcome| outcome.model_run_id.clone())
        .expect("shadow run row");
    let shadow_run = PgModelRunRepository::new(db.clone())
        .find_by_id(&shadow_run_id)
        .await
        .expect("find shadow run")
        .expect("shadow run row");
    assert_eq!(
        shadow_run
            .metrics_json
            .get("weight_source")
            .and_then(|v| v.as_str()),
        Some("config_overlay"),
        "shadow candidate must record overlay provenance"
    );
}

#[tokio::test]
async fn inference_rejects_retired_active_model() {
    let (pool, _container) = setup_pg().await;
    let db = pool.connection().clone();
    seed_catalog(&db).await;

    let factors = factors_config();
    let features = FeaturesConfig::default();
    let (vectors, ids, evidence, boundary) = build_features(&db).await;

    let store = artifact_store();
    let active = publish_weighted_model(&db, &store, &factors, &features).await;
    publish_enabled_factors(&db, &factors, &features).await;
    PgModelRegistryRepository::new(db.clone())
        .retire_model_version(&active)
        .await
        .expect("retire published version");

    let critical = Arc::new(AtomicUsize::new(0));
    let runner = build_runner(
        &db,
        Arc::clone(&store),
        Arc::clone(&critical),
        Arc::new(WeightOverlayApplicator::new()),
    );

    let selection = [selected_market()];
    let result = runner
        .run(ModelRunRequest {
            runtime_config_version_id: RuntimeConfigVersionId::from_v7(),
            market_selection_id: None,
            selection: &selection,
            feature_vectors: &vectors,
            feature_vector_ids: &ids,
            feature_evidence: &evidence,
            features: &features,
            factors: &factors,
            domain: &DomainConfig::default(),
            model: &model_config(&active, None),
            top_n: 10,
            boundary,
        })
        .await;
    assert!(result.is_err(), "retired active model must fail load");
    let Err(err) = result else {
        panic!("retired active model must fail load");
    };
    assert!(
        err.to_string().contains("must be published"),
        "failure must cite publication status: {err}"
    );
}

#[tokio::test]
async fn model_run_create_find_succeed_fail() {
    let (pool, _container) = setup_pg().await;
    let db = pool.connection().clone();
    let repo = PgModelRunRepository::new(db.clone());

    let zero = ContentHash::parse(format!("blake3:{}", "0".repeat(64))).expect("hash");
    let new_run = |id: &ModelRunId| NewModelRun {
        model_run_id: id.clone(),
        run_kind: ModelRunKind::LiveInference,
        model_version_id: None,
        runtime_config_version_id: RuntimeConfigVersionId::from_v7(),
        market_selection_id: None,
        window_start: Utc::now(),
        window_end: Utc::now(),
        status: ModelRunStatus::Running,
        input_hash: zero.clone(),
        output_hash: None,
        metrics_json: serde_json::json!({}),
        error_code: None,
        error_message: None,
        started_at: Utc::now(),
        finished_at: None,
    };

    // create → find → succeed.
    let ok_id = ModelRunId::from_v7();
    repo.create(new_run(&ok_id)).await.expect("create");
    let found = repo.find_by_id(&ok_id).await.expect("find").expect("row");
    assert_eq!(found.status, ModelRunStatus::Running);
    let output = ContentHash::parse(format!("blake3:{}", "1".repeat(64))).expect("hash");
    let succeeded = repo
        .succeed(
            &ok_id,
            output.clone(),
            serde_json::json!({"ok": true}),
            Utc::now(),
            None,
        )
        .await
        .expect("succeed");
    assert_eq!(succeeded.status, ModelRunStatus::Succeeded);
    assert_eq!(succeeded.output_hash, Some(output));
    assert!(succeeded.finished_at.is_some());

    // A second succeed/fail on a terminal run is rejected.
    assert!(
        repo.fail(
            &ok_id,
            ModelRunErrorCode::ActiveInferenceFailed,
            "y".to_owned(),
            Utc::now(),
        )
        .await
        .is_err(),
        "finalizing a terminal run must be rejected"
    );

    // create → fail.
    let fail_id = ModelRunId::from_v7();
    repo.create(new_run(&fail_id)).await.expect("create");
    let failed = repo
        .fail(
            &fail_id,
            ModelRunErrorCode::ArtifactLoadFailed,
            "detail".to_owned(),
            Utc::now(),
        )
        .await
        .expect("fail");
    assert_eq!(failed.status, ModelRunStatus::Failed);
    assert_eq!(
        failed.error_code,
        Some(ModelRunErrorCode::ArtifactLoadFailed)
    );
}
