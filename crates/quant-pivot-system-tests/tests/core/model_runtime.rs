//! Model runtime system contracts: selection + features → [`ModelRunner`] →
//! [`SignalCandidate`]s + [`ModelRun`] lifecycle + `ClickHouse` facts, plus the
//! model-run repository finalize transitions.

use std::{
    collections::BTreeMap,
    env, iter,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
};

use async_trait::async_trait;
use chrono::{DateTime, Duration as ChronoDuration, Utc};
use prometheus::IntCounter;
use quant_pivot_compute::ComputeExecutor;
use quant_pivot_core::{
    ingest::{book_store::BookStore, data_plane_index::DataPlane, market_registry::MarketRegistry},
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
            ActiveModelRequirements, ActiveModelRequirementsRequest, InferenceAlertSink,
            ModelRunOutcome, ModelRunRequest, ModelRunner, ModelRunnerDeps,
        },
        research_readiness::EvidenceScopeIdentity,
    },
};
use quant_pivot_error::{QuantResult, storage::StorageError};
use quant_pivot_models::{
    clickhouse::{
        BookL2LedgerRow, BookMicrostructureRow, DomainObservationRow, MarketResolutionRow,
        MidPriceBucketRow, TradeTapeRow,
    },
    config::{ArtifactStoreDeployConfig, ClickHouseConfig, TradeTapeOnChainConfig},
    domain::{
        data_plane::{DecisionBoundary, DecisionClock},
        governance::DecisionPolicySnapshotInfo,
        market::{
            EventRegistryInfo, MarketRegistryInfo, TokenInfo,
            book::{BookLevel, BookSnapshot},
        },
        quant::{
            NewMarketSelection, NewMarketSelectionMember, NewModelRun, NewModelVersion,
            NewShadowComparison, ShadowObservationQuery, ShadowObservationWindow,
            ShadowStabilitySummary,
        },
    },
    entities::quant_model_run::Entity as ModelRunEntity,
    enums::{
        catalog::CatalogFilterReasonSet,
        common::{CategorySet, MarketCategory, TickSize},
        factor::FactorFamily,
        market::{EventStatus, MarketStatus},
        model::ModelFamily,
        quant::{DatasetPurpose, ModelRunErrorCode, ModelRunKind, ModelRunStatus},
        runtime_config::ConfigResourceKind,
    },
    runtime_config::{
        BuyModelRoute, BuyRouteBinding, DataQualityConfig, DecimalValue, DecisionPolicySnapshot,
        DomainConfig, FactorsConfig, FeaturesConfig, ModelBinding, ModelBindingSource, ModelConfig,
    },
    types::{
        ContentHash, DecisionPolicySnapshotId, DomainInstrumentKey, EventId, FeatureVectorId,
        FeedbackCycleId, MarketId, MarketSelectionId, ModelInputContract, ModelRunId, ModelSpecId,
        ModelTrainingContract, ModelVersionId, PolicyBundleGeneration, Price, SchemaVersion,
        SelectionExclusionSummary, Shares, TokenId, Usd, model_metrics::ModelVersionMetrics,
        model_training::ModelTrainingObjective,
    },
};
use quant_pivot_repository::{
    postgres::{
        PgCalibrationArtifactRepository, PgEventRepository, PgFactorRepository,
        PgFeatureRepository, PgMarketRepository, PgMarketSelectionRepository,
        PgModelRegistryRepository, PgModelRunRepository, PgPolicyRepository,
        PgShadowComparisonRepository,
    },
    traits::{
        EventRepository, FactorRepository, FeatureRepository, MarketRepository,
        MarketSelectionRepository, ModelRegistryRepository, ModelRunRepository, PolicyRepository,
        QuantFactReadRepository, ShadowComparisonRepository, ShadowComparisonWriteOutcome,
    },
};
use quant_pivot_research::{
    artifact::{ArtifactStore, LocalArtifactStore},
    factors::FactorEngine,
    features::{FeatureSchema, FeatureVector},
    hashing::ResearchHasher,
    model::ReturnModelSpec,
    selection::{ModelFeatureRequirements, SelectedMarket},
};
use quant_pivot_storage::write::{AsyncWriter, AsyncWriterConfig, AsyncWriterObservability};
use quant_pivot_system_tests::{
    postgres::setup_pg,
    support::{
        SelectorFixture,
        catalog_fixtures::{make_event, make_market},
        fact_sink::DiscardFactWriter,
        factor_definitions::register_all_factor_definitions,
        model_serving_fixtures::{
            ModelArtifactFixtureSeed, ModelDatasetLedgerFixture, ModelDatasetLedgerSeed,
            ModelPayloadFixture, ModelVersionFixture, SealedModelFixture,
        },
        model_serving_runtime::ModelServingRegistryFixture,
        model_spec_fixtures,
        pit::InMemoryDecisionSnapshotSource,
        policy_fixtures::{activate_policy_bundle, bootstrap_policy_bundle},
        publish_fresh_book,
        report_pipeline_harness::{EmptyBasisAlertRepo, EmptyLinkageRepo},
        trade_tape_fixtures::live_tape_cursor_repo,
    },
};
use rust_decimal::Decimal;
use sea_orm::{
    ActiveModelTrait, ActiveValue::Set, ConnectionTrait, DatabaseConnection, DbBackend,
    EntityTrait, IntoActiveModel, Statement, TryGetable,
};

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

    async fn market_tape_window(
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

struct RejectingShadowComparisonRepository;

#[async_trait]
impl ShadowComparisonRepository for RejectingShadowComparisonRepository {
    async fn create(
        &self,
        _comparison: NewShadowComparison,
    ) -> Result<ShadowComparisonWriteOutcome, StorageError> {
        Err(StorageError::invariant_violation(
            Some("quant_shadow_comparison"),
            "forced shadow comparison persistence failure",
        ))
    }

    async fn summary(
        &self,
        _candidate_model_version_id: &ModelVersionId,
        _since: DateTime<Utc>,
    ) -> Result<ShadowStabilitySummary, StorageError> {
        Err(StorageError::invariant_violation(
            Some("quant_shadow_comparison"),
            "summary is unavailable in the rejecting test repository",
        ))
    }

    async fn observation_window(
        &self,
        _query: &ShadowObservationQuery,
    ) -> Result<ShadowObservationWindow, StorageError> {
        Err(StorageError::invariant_violation(
            Some("quant_shadow_comparison"),
            "observation window is unavailable in the rejecting test repository",
        ))
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
        filter_reasons: CatalogFilterReasonSet::default(),
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
        tags: vec![MarketCategory::Sports.to_string()],
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

async fn seed_runtime_policy(
    db: &DatabaseConnection,
    factors: &FactorsConfig,
    features: &FeaturesConfig,
) -> DecisionPolicySnapshotId {
    let mut snapshot = DecisionPolicySnapshot::default();
    snapshot.profile_artifacts.scoring.definition = factors.clone();
    snapshot.profile_artifacts.features.definition = features.clone();
    snapshot.profile_artifacts.domain.definition = DomainConfig::default();
    bootstrap_policy_bundle(
        &PgPolicyRepository::new(db.clone()),
        &snapshot,
        "model-runtime-system-test",
        "model runtime immutable serving fixture",
    )
    .await
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
        IntCounter::new("model_e2e_fac_drops", "d").expect("counter"),
        AsyncWriterObservability::default(),
    );
    Arc::new(FactorEventWriter::new(Arc::new(writer)))
}

fn noop_signal_writer() -> Arc<SignalCandidateEventWriter> {
    let (writer, _worker) = AsyncWriter::new(
        AsyncWriterConfig::new("model-e2e-signal").capacity(64),
        |_| Box::pin(async { Ok(()) }),
        IntCounter::new("model_e2e_sig_drops", "d").expect("counter"),
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

/// Peer markets forming a real cross-section alongside the primary market, so
/// the cross-sectional factors (`liquidity_depth` rank, `spread_efficiency`
/// z-score) are *scored* rather than indeterminate. Immutable-head tests need a
/// cross-section ≥ `cross_section.min_size` (5) with dispersion.
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
        filter_reasons: CatalogFilterReasonSet::default(),
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
    let token_id = TokenId::new(token);
    publish_fresh_book(
        book_store,
        &token_id,
        BookSnapshot::new(
            Arc::from([BookLevel::from_decimal_unchecked(
                Price::new(bid_px),
                Shares::new(Decimal::from(bid_shares)),
            )]),
            Arc::from([BookLevel::from_decimal_unchecked(
                Price::new(ask_px),
                Shares::new(Decimal::from(ask_shares)),
            )]),
            ts_ms,
            1,
        ),
        1,
    );
}

/// Build + persist feature vectors for the primary market plus dispersed peers,
/// yielding a real cross-section (order-aligned `vectors` / `ids` / `selection`).
async fn build_cross_section_features(
    db: &DatabaseConnection,
    decision_policy_snapshot_id: DecisionPolicySnapshotId,
) -> (
    Vec<FeatureVector>,
    Vec<FeatureVectorId>,
    Vec<SelectedMarket>,
    FeatureEvidenceCommitment,
    DecisionBoundary,
) {
    let data_plane = Arc::new(DataPlane::new());
    let registry = Arc::new(MarketRegistry::new(Arc::clone(&data_plane)));
    let book_store = Arc::new(BookStore::new(data_plane, Arc::new(MetricsHub::new())));
    let ts_ms = u64::try_from(Utc::now().timestamp_millis())
        .expect("test book timestamp must be non-negative");
    let primary = registry_market();
    let peers = (0..PEER_COUNT)
        .map(peer_registry_market)
        .collect::<Vec<_>>();
    let market_ids = iter::once(primary.market_id.clone())
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
        compute: Arc::new(ComputeExecutor::new().expect("test compute executor")),
        window_provider: FeatureWindowProvider::new(Arc::new(EmptyFactRead)),
        feature_repo,
        event_writer: noop_feature_writer(),
        block_cursor_repo: live_tape_cursor_repo(),
        linkage_repo: Arc::new(EmptyLinkageRepo),
        basis_alert_repo: Arc::new(EmptyBasisAlertRepo),
        calibration_repo: Arc::new(PgCalibrationArtifactRepository::new(db.clone())),
        trade_tape_on_chain: TradeTapeOnChainConfig::default(),
    });

    let features = FeaturesConfig::default();
    let domain = DomainConfig::default();
    let included: Vec<SelectedMarket> = iter::once(selected_market())
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
            decision_policy_snapshot_id,
            liquidity_cap_usd: Usd::new(Decimal::from(10_000)),
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
        .map(|info| info.feature_vector_id)
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

async fn persist_live_selection(
    db: &DatabaseConnection,
    decision_policy_snapshot_id: DecisionPolicySnapshotId,
    decision_at: DateTime<Utc>,
    selection: &[SelectedMarket],
) -> MarketSelectionId {
    let market_selection_id = MarketSelectionId::from_v7();
    let selector_hash = ResearchHasher::canonical(&(
        "model-runtime-live-selection-v1",
        decision_policy_snapshot_id,
        decision_at,
        selection,
    ))
    .expect("hash exact live selection");
    let members = selection
        .iter()
        .map(|market| NewMarketSelectionMember {
            market_selection_id,
            market_id: market.market_id.clone(),
            event_id: market.event_id.clone(),
            category: market.category,
            status: MarketStatus::Active,
            primary_token_id: market.primary_token_id.clone(),
            secondary_token_id: market.secondary_token_id.clone(),
            liquidity_usd: market.liquidity_usd,
            volume_24h_usd: market.volume_24h_usd,
        })
        .collect::<Vec<_>>();
    let market_count = i32::try_from(members.len()).expect("selection count fits i32");
    PgMarketSelectionRepository::new(db.clone())
        .create_snapshot(
            NewMarketSelection {
                market_selection_id,
                decision_at,
                decision_policy_snapshot_id,
                selector_hash,
                selector_evidence: SelectorFixture::evidence(selector_hash),
                market_count,
                exclusion_summary: SelectionExclusionSummary::default(),
            },
            members,
        )
        .await
        .expect("persist exact live selection");
    market_selection_id
}

/// Author a weighted artifact bound to the active schema, weighting every enabled
/// factor equally, and persist its bytes + registry rows. Returns the version id.
async fn publish_weighted_model(
    db: &DatabaseConnection,
    store: &Arc<dyn ArtifactStore>,
    factors: &FactorsConfig,
    features: &FeaturesConfig,
    decision_policy_snapshot_id: DecisionPolicySnapshotId,
) -> ModelVersionId {
    let engine =
        FactorEngine::for_model_scope(factors, features, &DomainConfig::default(), None, None);
    let factor_plane = engine.serving_plane().expect("factor plane");

    let model_version_id = ModelVersionId::from_v7();
    let feature_schema_hash =
        ResearchHasher::feature_schema(&FeatureSchema::build(features).expect("feature schema"))
            .expect("feature hash");
    let input_contract = ModelInputContract::single_required("book.mid");
    let model_spec_id = ModelSpecId::from_v7();
    let spec = model_spec_fixtures::new_model_spec_fixture(
        model_spec_id,
        "weighted-e2e",
        ModelFamily::WeightedFactor,
        model_spec_fixtures::pooled_horizon_secs(),
        input_contract.clone(),
        ModelTrainingContract::outcome_default(),
    );
    let model_spec_definition_hash = spec.definition_hash;
    let registry = PgModelRegistryRepository::new(db.clone());
    registry.create_model_spec(spec).await.expect("create spec");
    let window_end = Utc::now() - ChronoDuration::days(2);
    let window_start = window_end - ChronoDuration::days(1);
    let dataset = Box::pin(ModelDatasetLedgerFixture::persist(
        db,
        store,
        ModelDatasetLedgerSeed {
            scope: format!("model-runtime-{model_version_id}"),
            model_spec_id,
            model_family: ModelFamily::WeightedFactor,
            model_spec_definition_hash,
            factor_serving_plane: factor_plane.clone(),
            feature_schema_version: SchemaVersion::FIRST,
            feature_schema_hash,
            decision_policy_snapshot_id,
            profile_ref: model_spec_fixtures::pooled_profile_ref(),
            prediction_horizon_secs: u64::try_from(model_spec_fixtures::pooled_horizon_secs())
                .expect("pooled horizon"),
            purpose: DatasetPurpose::Training,
            window_start,
            window_end,
            research_program_hash: ResearchHasher::canonical(&(
                "model-runtime-program-v1",
                model_spec_id,
                model_spec_definition_hash,
            ))
            .expect("research program hash"),
            sample_count: 32,
            decision_interval_secs: 1,
            trade_policy: None,
        },
    ))
    .await
    .expect("persist runtime model dataset");
    let payload = ModelPayloadFixture::weighted(
        factor_plane,
        &factors.factor_head,
        input_contract,
        ReturnModelSpec::heuristic_default(),
        factors.cross_section.clone(),
    )
    .expect("runtime weighted payload");
    let fixture = SealedModelFixture::seal(
        db,
        ModelArtifactFixtureSeed {
            model_version_id,
            training_dataset_id: dataset.training_dataset_id,
            payload,
            training_input_hash: ResearchHasher::canonical(&"model-runtime-training-input")
                .expect("training input hash"),
            category_scope: None,
            calibration: None,
            bias_table: None,
        },
    )
    .await
    .expect("seal runtime model fixture");
    fixture.store(store).await.expect("store runtime model");
    ModelVersionFixture::persist_route_candidate(
        db,
        model_version_fixture(&fixture, model_spec_id, 1, fixture.artifact_hash()),
    )
    .await
    .expect("publish exact runtime model fixture");

    model_version_id
}

fn model_version_fixture(
    fixture: &SealedModelFixture,
    model_spec_id: ModelSpecId,
    version: i32,
    artifact_hash: ContentHash,
) -> NewModelVersion {
    let serving_contract = fixture.serving_contract().clone();
    let bindings = serving_contract.bindings();
    let category_scope = bindings.model.category_scope;
    let profile_ref = bindings.model.profile_ref.clone();
    let training_dataset_id = bindings.dataset.manifest.training_dataset_id;
    let trade_policy = bindings
        .trade_policy
        .as_ref()
        .map(|binding| (binding.artifact_id, binding.content_hash));
    NewModelVersion {
        model_version_id: bindings.model.model_version_id,
        model_spec_id,
        version,
        artifact_hash,
        serving_contract,
        category_scope,
        profile_ref,
        training_dataset_id: Some(training_dataset_id),
        trade_policy_artifact_id: trade_policy.map(|binding| binding.0),
        trade_policy_hash: trade_policy.map(|binding| binding.1),
        derivation: NewModelVersion::training_derivation(),
        metrics: ModelVersionMetrics::not_measured("test fixture"),
        training_objective: ModelTrainingObjective::hand_authored("test fixture"),
    }
}

async fn seal_candidate(
    db: &DatabaseConnection,
    published_id: &ModelVersionId,
    factors: &FactorsConfig,
) -> (ModelSpecId, i32, SealedModelFixture) {
    let registry = PgModelRegistryRepository::new(db.clone());
    let published = registry
        .find_model_version(published_id)
        .await
        .expect("find published")
        .expect("published row");
    let candidate_id = ModelVersionId::from_v7();
    let contract = published
        .verified_serving_contract()
        .expect("published serving contract");
    let input_contract = registry
        .find_model_spec(&published.model_spec_id)
        .await
        .expect("candidate model spec lookup")
        .expect("candidate model spec")
        .input_contract;
    let plane = contract.bindings().factors.plane.clone();
    let payload = ModelPayloadFixture::weighted(
        &plane,
        &factors.factor_head,
        input_contract,
        ReturnModelSpec::heuristic_default(),
        factors.cross_section.clone(),
    )
    .expect("candidate weighted payload");
    let fixture = SealedModelFixture::seal(
        db,
        ModelArtifactFixtureSeed {
            model_version_id: candidate_id,
            training_dataset_id: contract.bindings().dataset.manifest.training_dataset_id,
            payload,
            training_input_hash: contract.bindings().transform.training_input_hash,
            category_scope: contract.bindings().model.category_scope,
            calibration: None,
            bias_table: contract.bindings().factors.bias_table.clone(),
        },
    )
    .await
    .expect("seal candidate sibling");
    (published.model_spec_id, published.version + 1, fixture)
}

/// Register a correctly sealed sibling candidate with a distinct estimator.
async fn register_candidate_sibling(
    db: &DatabaseConnection,
    store: &Arc<dyn ArtifactStore>,
    published_id: &ModelVersionId,
    factors: &FactorsConfig,
) -> ModelVersionId {
    let (model_spec_id, version, fixture) = seal_candidate(db, published_id, factors).await;
    fixture
        .store(store)
        .await
        .expect("store candidate artifact");
    PgModelRegistryRepository::new(db.clone())
        .create_model_version(model_version_fixture(
            &fixture,
            model_spec_id,
            version,
            fixture.artifact_hash(),
        ))
        .await
        .expect("create candidate sibling");
    fixture.serving_contract().bindings().model.model_version_id
}

/// Register a row whose scalar projections are coherent but whose artifact
/// points at another version's exact bytes.
async fn register_contract_drift(
    db: &DatabaseConnection,
    published_id: &ModelVersionId,
    factors: &FactorsConfig,
) -> ModelVersionId {
    let registry = PgModelRegistryRepository::new(db.clone());
    let published = registry
        .find_model_version(published_id)
        .await
        .expect("find published model")
        .expect("published model");
    let (model_spec_id, version, fixture) = seal_candidate(db, published_id, factors).await;
    let candidate_id = fixture.serving_contract().bindings().model.model_version_id;
    registry
        .create_model_version(model_version_fixture(
            &fixture,
            model_spec_id,
            version,
            published.artifact_hash,
        ))
        .await
        .expect("create contract-drift candidate");
    candidate_id
}

/// Build a governed alpha-head seed skewed toward one `OutcomeAlpha` factor.
fn factors_with_head_skew(
    base: &FactorsConfig,
    features: &FeaturesConfig,
    variant_index: usize,
) -> FactorsConfig {
    let engine =
        FactorEngine::for_model_scope(base, features, &DomainConfig::default(), None, None);
    let plane = engine.serving_plane().expect("factor plane");
    let alpha = plane
        .definitions()
        .iter()
        .filter(|revision| revision.definition().is_outcome_alpha())
        .collect::<Vec<_>>();
    let mut config = base.clone();
    if alpha.len() > 1 {
        assert!(
            variant_index < alpha.len(),
            "head variant must select an exact alpha revision"
        );
        let lead = Decimal::new(90, 2);
        let tail =
            (Decimal::ONE - lead) / Decimal::from(u64::try_from(alpha.len() - 1).expect("count"));
        config.factor_head.alpha_seed_weights.clear();
        for (index, revision) in alpha.into_iter().enumerate() {
            config.factor_head.alpha_seed_weights.insert(
                revision.factor_name().to_string(),
                DecimalValue::new(if index == variant_index { lead } else { tail }),
            );
        }
    } else {
        assert_eq!(
            alpha.len(),
            1,
            "weighted head fixture requires an OutcomeAlpha revision"
        );
        let deadband = match variant_index {
            0 => Decimal::new(1, 2),
            1 => Decimal::new(20, 2),
            other => panic!("unsupported single-alpha head variant {other}"),
        };
        config.factor_head.alpha_deadband = DecimalValue::new(deadband);
    }
    config
}

fn model_config(active: &ModelVersionId, shadow: Option<&ModelVersionId>) -> ModelConfig {
    ModelConfig {
        buy_routes: BTreeMap::from([(
            BuyModelRoute::Pooled,
            BuyRouteBinding {
                champion: ModelBinding::new(
                    *active,
                    ModelBindingSource::Bootstrap,
                    Utc::now(),
                    PolicyBundleGeneration::FIRST,
                    1,
                ),
                shadow: shadow.copied().map(|model_version_id| {
                    ModelBinding::new(
                        model_version_id,
                        ModelBindingSource::Feedback {
                            feedback_cycle_id: FeedbackCycleId::from_v7(),
                        },
                        Utc::now(),
                        PolicyBundleGeneration::FIRST,
                        2,
                    )
                }),
            },
        )]),
        ..ModelConfig::default()
    }
}

async fn activate_model_config(
    db: &DatabaseConnection,
    active: &ModelVersionId,
    shadow: Option<&ModelVersionId>,
) -> DecisionPolicySnapshotInfo {
    let repository = PgPolicyRepository::new(db.clone());
    let config = model_config(active, shadow);
    let snapshot_id = activate_policy_bundle(
        &repository,
        ConfigResourceKind::ModelRouting,
        "model-runtime-system-test",
        "pin exact model serving generation",
        move |snapshot| snapshot.model_routing.model = config,
    )
    .await;
    repository
        .load_snapshot(&snapshot_id)
        .await
        .expect("load activated model policy")
        .expect("activated model policy")
}

async fn resolve_active_model(
    runner: &ModelRunner,
    policy: &DecisionPolicySnapshotInfo,
    boundary: &DecisionBoundary,
) -> ActiveModelRequirements {
    runner
        .active_requirements(ActiveModelRequirementsRequest {
            policy,
            decision_at: boundary.decision_at(),
            route: BuyModelRoute::Pooled,
        })
        .await
        .expect("resolve exact active serving route")
}

async fn build_runner(
    db: &DatabaseConnection,
    store: Arc<dyn ArtifactStore>,
    critical: Arc<AtomicUsize>,
    shadow_comparison_repo: Option<Arc<dyn ShadowComparisonRepository>>,
) -> QuantResult<ModelRunner> {
    let factor_repo = Arc::new(PgFactorRepository::new(db.clone())) as Arc<dyn FactorRepository>;
    let factor_pipeline = Arc::new(FactorPipelineService::new(
        factor_repo,
        noop_factor_writer(),
        Arc::new(ComputeExecutor::new().expect("test compute executor")),
    ));
    let model_run_repo =
        Arc::new(PgModelRunRepository::new(db.clone())) as Arc<dyn ModelRunRepository>;
    let shadow_comparison_repo = shadow_comparison_repo.unwrap_or_else(|| {
        Arc::new(PgShadowComparisonRepository::new(db.clone()))
            as Arc<dyn ShadowComparisonRepository>
    });
    let evidence_scope = EvidenceScopeIdentity::from_config(
        &ClickHouseConfig::default(),
        &ArtifactStoreDeployConfig::default(),
    )
    .expect("model runtime evidence scope");
    let serving_generations = ModelServingRegistryFixture {
        db: db.clone(),
        artifact_store: store,
        evidence_scope,
        evidence_attestor: None,
    }
    .build_generation()
    .await?;
    Ok(ModelRunner::new(ModelRunnerDeps {
        model_run_repo,
        shadow_comparison_repo,
        serving_generations,
        factor_pipeline,
        signal_writer: noop_signal_writer(),
        model_input_writer: noop_model_input_writer(),
        alerts: Arc::new(CountingAlertSink { critical }),
    }))
}

async fn register_enabled_factors(
    db: &DatabaseConnection,
    factors: &FactorsConfig,
    features: &FeaturesConfig,
) {
    let repo = PgFactorRepository::new(db.clone());
    register_all_factor_definitions(&repo, factors, features, &DomainConfig::default())
        .await
        .expect("register immutable factor definitions");
}

fn artifact_store() -> Arc<dyn ArtifactStore> {
    let root = env::temp_dir().join(format!(
        "qp_model_e2e_{}_{}",
        std::process::id(),
        Utc::now().timestamp_nanos_opt().unwrap_or_default()
    ));
    Arc::new(LocalArtifactStore::new(root))
}

pub async fn online_loop_selection_candidates() {
    let (pool, _container) = setup_pg().await;
    let db = pool.connection().clone();
    seed_catalog(&db).await;
    seed_peer_markets(&db).await;

    let factors = factors_config();
    let features = FeaturesConfig::default();
    let training_policy_id = seed_runtime_policy(&db, &factors, &features).await;
    let store = artifact_store();
    let active = publish_weighted_model(&db, &store, &factors, &features, training_policy_id).await;
    register_enabled_factors(&db, &factors, &features).await;
    let policy = activate_model_config(&db, &active, None).await;
    let (vectors, ids, selection, evidence, boundary) =
        build_cross_section_features(&db, policy.decision_policy_snapshot_id).await;
    let market_selection_id = persist_live_selection(
        &db,
        policy.decision_policy_snapshot_id,
        boundary.decision_at(),
        &selection,
    )
    .await;

    let critical = Arc::new(AtomicUsize::new(0));
    let runner = build_runner(&db, Arc::clone(&store), Arc::clone(&critical), None)
        .await
        .expect("bootstrap serving generation");
    let active_model = resolve_active_model(&runner, &policy, &boundary).await;

    let rejected = Box::pin(runner.run(ModelRunRequest {
        decision_policy_snapshot_id: policy.decision_policy_snapshot_id,
        market_selection_id: None,
        selection: &selection,
        feature_vectors: &vectors,
        feature_vector_ids: &ids,
        feature_evidence: &evidence,
        serving: &active_model.serving,
        top_n: 10,
        boundary: boundary.clone(),
    }))
    .await;
    let Err(error) = rejected else {
        panic!("live inference without a persisted selection must fail closed");
    };
    assert!(
        error
            .to_string()
            .contains("requires an exact persisted market selection"),
        "unexpected missing-selection error: {error}"
    );
    assert!(
        ModelRunEntity::find()
            .all(&db)
            .await
            .expect("load model runs after rejected live inference")
            .is_empty(),
        "rejected live inference must not create a model-run side effect"
    );

    let outcome = Box::pin(runner.run(ModelRunRequest {
        decision_policy_snapshot_id: policy.decision_policy_snapshot_id,
        market_selection_id: Some(market_selection_id),
        selection: &selection,
        feature_vectors: &vectors,
        feature_vector_ids: &ids,
        feature_evidence: &evidence,
        serving: &active_model.serving,
        top_n: 10,
        boundary,
    }))
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

pub async fn generation_rejects_bad_shadow() {
    let (pool, _container) = setup_pg().await;
    let db = pool.connection().clone();
    seed_catalog(&db).await;
    seed_peer_markets(&db).await;

    let factors = factors_config();
    let features = FeaturesConfig::default();
    let training_policy_id = seed_runtime_policy(&db, &factors, &features).await;
    let store = artifact_store();
    let active = publish_weighted_model(&db, &store, &factors, &features, training_policy_id).await;
    register_enabled_factors(&db, &factors, &features).await;

    // The shadow row is internally coherent, but its artifact hash points at
    // another version's exact bytes. Deep registry verification must reject it
    // while preparing the complete generation, before any route is published or
    // model-run side effect can exist.
    let drifted_shadow = register_contract_drift(&db, &active, &factors).await;
    let _policy = activate_model_config(&db, &active, Some(&drifted_shadow)).await;
    let result = build_runner(&db, Arc::clone(&store), Arc::new(AtomicUsize::new(0)), None).await;
    let Err(error) = result else {
        panic!("invalid shadow must reject the complete serving generation");
    };
    assert!(
        error
            .to_string()
            .contains("exact persisted serving contract"),
        "generation must fail on exact contract drift, got: {error}"
    );
    assert!(
        ModelRunEntity::find()
            .all(&db)
            .await
            .expect("load model runs after rejected generation")
            .is_empty(),
        "a rejected generation must not create active or shadow runs"
    );
}

struct ShadowRoundInput<'a> {
    runner: &'a ModelRunner,
    active: &'a ActiveModelRequirements,
    selection: &'a [SelectedMarket],
    vectors: &'a [FeatureVector],
    ids: &'a [FeatureVectorId],
    evidence: &'a FeatureEvidenceCommitment,
    boundary: DecisionBoundary,
    decision_policy_snapshot_id: DecisionPolicySnapshotId,
}

async fn run_shadow_round(input: ShadowRoundInput<'_>) -> ModelRunOutcome {
    let ShadowRoundInput {
        runner,
        active,
        selection,
        vectors,
        ids,
        evidence,
        boundary,
        decision_policy_snapshot_id,
    } = input;
    Box::pin(runner.run_shadow_evaluation(ModelRunRequest {
        decision_policy_snapshot_id,
        market_selection_id: None,
        selection,
        feature_vectors: vectors,
        feature_vector_ids: ids,
        feature_evidence: evidence,
        serving: &active.serving,
        top_n: 10,
        boundary,
    }))
    .await
    .expect("model round")
}

pub async fn cached_planes_stay_stable() {
    let (pool, _container) = setup_pg().await;
    let db = pool.connection().clone();
    seed_catalog(&db).await;
    seed_peer_markets(&db).await;

    let base_factors = factors_config();
    let features = FeaturesConfig::default();
    let training_policy_id = seed_runtime_policy(&db, &base_factors, &features).await;
    let store = artifact_store();
    let published =
        publish_weighted_model(&db, &store, &base_factors, &features, training_policy_id).await;
    register_enabled_factors(&db, &base_factors, &features).await;
    let candidate_factors = factors_with_head_skew(&base_factors, &features, 0);
    let candidate = register_candidate_sibling(&db, &store, &published, &candidate_factors).await;
    register_enabled_factors(&db, &candidate_factors, &features).await;
    let policy = activate_model_config(&db, &published, Some(&candidate)).await;
    let (vectors, ids, selection, evidence, boundary) =
        build_cross_section_features(&db, policy.decision_policy_snapshot_id).await;
    let critical = Arc::new(AtomicUsize::new(0));
    let runner = build_runner(&db, Arc::clone(&store), Arc::clone(&critical), None)
        .await
        .expect("bootstrap active and shadow generation");
    let active = resolve_active_model(&runner, &policy, &boundary).await;
    let first = run_shadow_round(ShadowRoundInput {
        runner: &runner,
        active: &active,
        selection: &selection,
        vectors: &vectors,
        ids: &ids,
        evidence: &evidence,
        boundary: boundary.clone(),
        decision_policy_snapshot_id: policy.decision_policy_snapshot_id,
    })
    .await;
    let second = run_shadow_round(ShadowRoundInput {
        runner: &runner,
        active: &active,
        selection: &selection,
        vectors: &vectors,
        ids: &ids,
        evidence: &evidence,
        boundary,
        decision_policy_snapshot_id: policy.decision_policy_snapshot_id,
    })
    .await;

    let active_first = first.accepted[0].composite_score.inner();
    let active_second = second.accepted[0].composite_score.inner();
    assert_eq!(
        active_first, active_second,
        "published active must remain stable after a contract-cache hit"
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
    assert_eq!(
        shadow_first, shadow_second,
        "candidate shadow divergence must remain frozen after a contract-cache hit"
    );

    let shadow_run_id = second
        .shadow
        .as_ref()
        .and_then(|outcome| outcome.model_run_id)
        .expect("shadow run row");
    let shadow_run = PgModelRunRepository::new(db.clone())
        .find_by_id(&shadow_run_id)
        .await
        .expect("find shadow run")
        .expect("shadow run row");
    assert_eq!(shadow_run.run_kind, ModelRunKind::Shadow);
}

pub async fn shadow_persistence_degrades_only() {
    let (pool, _container) = setup_pg().await;
    let db = pool.connection().clone();
    seed_catalog(&db).await;
    seed_peer_markets(&db).await;

    let factors = factors_config();
    let features = FeaturesConfig::default();
    let training_policy_id = seed_runtime_policy(&db, &factors, &features).await;
    let store = artifact_store();
    let champion =
        publish_weighted_model(&db, &store, &factors, &features, training_policy_id).await;
    register_enabled_factors(&db, &factors, &features).await;
    let candidate_factors = factors_with_head_skew(&factors, &features, 0);
    let candidate = register_candidate_sibling(&db, &store, &champion, &candidate_factors).await;
    register_enabled_factors(&db, &candidate_factors, &features).await;
    let policy = activate_model_config(&db, &champion, Some(&candidate)).await;
    let (vectors, ids, selection, evidence, boundary) =
        build_cross_section_features(&db, policy.decision_policy_snapshot_id).await;
    let critical = Arc::new(AtomicUsize::new(0));
    let comparison_repo =
        Arc::new(RejectingShadowComparisonRepository) as Arc<dyn ShadowComparisonRepository>;
    let runner = build_runner(
        &db,
        Arc::clone(&store),
        Arc::clone(&critical),
        Some(comparison_repo),
    )
    .await
    .expect("bootstrap active and shadow generation");
    let active = resolve_active_model(&runner, &policy, &boundary).await;
    let outcome = run_shadow_round(ShadowRoundInput {
        runner: &runner,
        active: &active,
        selection: &selection,
        vectors: &vectors,
        ids: &ids,
        evidence: &evidence,
        boundary,
        decision_policy_snapshot_id: policy.decision_policy_snapshot_id,
    })
    .await;

    let active_run = PgModelRunRepository::new(db.clone())
        .find_by_id(&outcome.model_run_id)
        .await
        .expect("find active run")
        .expect("active run row");
    assert_eq!(active_run.status, ModelRunStatus::Succeeded);
    assert_eq!(active_run.run_kind, ModelRunKind::Shadow);
    assert!(!outcome.accepted.is_empty(), "active result remains usable");
    let shadow = outcome.shadow.expect("configured shadow outcome");
    assert_eq!(shadow.emitted, 0);
    assert!(shadow.diff.is_none());
    assert!(
        shadow
            .failure
            .as_deref()
            .is_some_and(|detail| detail.contains("forced shadow comparison persistence failure"))
    );
    let shadow_run = PgModelRunRepository::new(db)
        .find_by_id(&shadow.model_run_id.expect("shadow run id"))
        .await
        .expect("find shadow run")
        .expect("shadow run row");
    assert_eq!(shadow_run.status, ModelRunStatus::Failed);
    assert_eq!(
        shadow_run.error_code,
        Some(ModelRunErrorCode::ShadowInferenceFailed)
    );
    assert!(shadow_run.output_hash.is_none());
    assert_eq!(critical.load(Ordering::Relaxed), 0);
}

pub async fn generation_uses_route_authority() {
    let (pool, _container) = setup_pg().await;
    let db = pool.connection().clone();
    seed_catalog(&db).await;

    let factors = factors_config();
    let features = FeaturesConfig::default();
    let training_policy_id = seed_runtime_policy(&db, &factors, &features).await;
    let store = artifact_store();
    let active = publish_weighted_model(&db, &store, &factors, &features, training_policy_id).await;
    register_enabled_factors(&db, &factors, &features).await;
    let _policy = activate_model_config(&db, &active, None).await;
    let _runner = build_runner(&db, Arc::clone(&store), Arc::new(AtomicUsize::new(0)), None)
        .await
        .expect("the active route, not a global model status, owns serving authority");
    assert!(
        ModelRunEntity::find()
            .all(&db)
            .await
            .expect("load model runs after route generation bootstrap")
            .is_empty(),
        "generation bootstrap must not create an inference run"
    );
}

struct ModelRunLifecycleScenario {
    db: DatabaseConnection,
    repo: PgModelRunRepository,
    policy_id: DecisionPolicySnapshotId,
    input_hash: ContentHash,
    output_hash: ContentHash,
}

impl ModelRunLifecycleScenario {
    async fn initialize(db: DatabaseConnection) -> Self {
        let factors = factors_config();
        let features = FeaturesConfig::default();
        let policy_id = seed_runtime_policy(&db, &factors, &features).await;
        let repo = PgModelRunRepository::new(db.clone());
        let input_hash =
            ContentHash::parse(&format!("blake3:{}", "0".repeat(64))).expect("model input hash");
        let output_hash =
            ContentHash::parse(&format!("blake3:{}", "1".repeat(64))).expect("model output hash");
        Self {
            db,
            repo,
            policy_id,
            input_hash,
            output_hash,
        }
    }

    fn running(&self, model_run_id: ModelRunId) -> NewModelRun {
        NewModelRun {
            model_run_id,
            run_kind: ModelRunKind::LiveInference,
            model_version_id: None,
            decision_policy_snapshot_id: self.policy_id,
            market_selection_id: None,
            window_start: Utc::now(),
            window_end: Utc::now(),
            input_hash: self.input_hash,
        }
    }

    async fn database_timestamp(&self) -> DateTime<Utc> {
        let row = self
            .db
            .query_one_raw(Statement::from_string(
                DbBackend::Postgres,
                "SELECT statement_timestamp() AS observed_at",
            ))
            .await
            .expect("query model-run database clock")
            .expect("model-run database clock row");
        DateTime::<Utc>::try_get(&row, "", "observed_at").expect("decode model-run database clock")
    }

    async fn succeed_once(&self) {
        let model_run_id = ModelRunId::from_v7();
        let before_create = self.database_timestamp().await;
        let created = self
            .repo
            .create(self.running(model_run_id))
            .await
            .expect("create successful model run");
        let after_create = self.database_timestamp().await;
        assert_eq!(created.status, ModelRunStatus::Running);
        assert!(
            created.started_at >= before_create && created.started_at <= after_create,
            "run start must be sealed by the PostgreSQL statement clock"
        );
        let found = self
            .repo
            .find_by_id(&model_run_id)
            .await
            .expect("find running model run")
            .expect("running model run row");
        assert_eq!(found.status, ModelRunStatus::Running);

        let succeeded = self
            .repo
            .succeed(&model_run_id, self.output_hash, None)
            .await
            .expect("succeed model run");
        assert_eq!(succeeded.status, ModelRunStatus::Succeeded);
        assert_eq!(succeeded.output_hash, Some(self.output_hash));
        assert!(succeeded.finished_at.is_some());
        assert!(
            self.repo
                .fail(
                    &model_run_id,
                    ModelRunErrorCode::ActiveInferenceFailed,
                    "late active inference failure".to_owned(),
                )
                .await
                .is_err(),
            "finalizing a terminal run must be rejected"
        );
    }

    async fn reject_future_window(&self) {
        let model_run_id = ModelRunId::from_v7();
        let mut run = self.running(model_run_id);
        let future = Utc::now() + ChronoDuration::seconds(3);
        run.window_start = future;
        run.window_end = future;
        assert!(
            self.repo.create(run).await.is_err(),
            "a decision window beyond the bounded internal clock skew must fail closed"
        );
        assert!(
            self.repo
                .find_by_id(&model_run_id)
                .await
                .expect("query rejected future-window run")
                .is_none(),
            "a rejected future-window run must not leave a row"
        );
    }

    async fn fail_once(&self) {
        let model_run_id = ModelRunId::from_v7();
        self.repo
            .create(self.running(model_run_id))
            .await
            .expect("create failing model run");
        let failed = self
            .repo
            .fail(
                &model_run_id,
                ModelRunErrorCode::ArtifactLoadFailed,
                "artifact fixture failure".to_owned(),
            )
            .await
            .expect("fail model run");
        assert_eq!(failed.status, ModelRunStatus::Failed);
        assert_eq!(
            failed.error_code,
            Some(ModelRunErrorCode::ArtifactLoadFailed)
        );
    }

    async fn cancel_once(&self) {
        let model_run_id = ModelRunId::from_v7();
        self.repo
            .create(self.running(model_run_id))
            .await
            .expect("create cancellable model run");
        let cancelled = self
            .repo
            .cancel(&model_run_id, "operator cancelled fixture".to_owned())
            .await
            .expect("cancel model run");
        assert_eq!(cancelled.status, ModelRunStatus::Cancelled);
        assert_eq!(
            cancelled.error_code,
            Some(ModelRunErrorCode::CancelledByOperator)
        );
        assert!(cancelled.finished_at.is_some());
        assert!(
            self.repo
                .fail(
                    &model_run_id,
                    ModelRunErrorCode::TrainingFailed,
                    "late training failure".to_owned(),
                )
                .await
                .is_err(),
            "cancelled runs must remain terminal"
        );
    }

    async fn terminal_race(&self) {
        let model_run_id = ModelRunId::from_v7();
        self.repo
            .create(self.running(model_run_id))
            .await
            .expect("create terminal race");
        let (succeed_result, cancel_result) = tokio::join!(
            self.repo.succeed(&model_run_id, self.output_hash, None),
            self.repo
                .cancel(&model_run_id, "concurrent operator cancellation".to_owned(),),
        );
        let winner = match (succeed_result, cancel_result) {
            (Ok(winner), Err(StorageError::StateConflict { .. }))
            | (Err(StorageError::StateConflict { .. }), Ok(winner)) => winner,
            (succeed, cancel) => {
                panic!("exactly one terminal compare-and-set must win: {succeed:?}, {cancel:?}")
            }
        };
        let durable = self
            .repo
            .find_by_id(&model_run_id)
            .await
            .expect("reload terminal race")
            .expect("terminal race row");
        assert_eq!(durable.model_run_id, winner.model_run_id);
        assert_eq!(durable.run_kind, winner.run_kind);
        assert_eq!(durable.model_version_id, winner.model_version_id);
        assert_eq!(
            durable.decision_policy_snapshot_id,
            winner.decision_policy_snapshot_id
        );
        assert_eq!(durable.market_selection_id, winner.market_selection_id);
        assert_eq!(durable.window_start, winner.window_start);
        assert_eq!(durable.window_end, winner.window_end);
        assert_eq!(durable.status, winner.status);
        assert_eq!(durable.input_hash, winner.input_hash);
        assert_eq!(durable.output_hash, winner.output_hash);
        assert_eq!(durable.error_code, winner.error_code);
        assert_eq!(durable.error_message, winner.error_message);
        assert_eq!(durable.started_at, winner.started_at);
        assert_eq!(durable.finished_at, winner.finished_at);

        let row = ModelRunEntity::find_by_id(model_run_id)
            .one(&self.db)
            .await
            .expect("load terminal model")
            .expect("terminal model");
        let mut tampered = row.into_active_model();
        tampered.status = Set(ModelRunStatus::Failed);
        tampered.output_hash = Set(None);
        tampered.error_code = Set(Some(ModelRunErrorCode::ActiveInferenceFailed));
        tampered.error_message = Set(Some("late overwrite".to_owned()));
        assert!(
            tampered.update(&self.db).await.is_err(),
            "database lifecycle guard must reject terminal overwrite"
        );
        assert!(
            ModelRunEntity::delete_by_id(model_run_id)
                .exec(&self.db)
                .await
                .is_err(),
            "database lifecycle guard must reject audit-row deletion"
        );
    }
}

pub async fn model_run_create_fail() {
    let (pool, _container) = setup_pg().await;
    let scenario = ModelRunLifecycleScenario::initialize(pool.connection().clone()).await;
    scenario.succeed_once().await;
    scenario.reject_future_window().await;
    scenario.fail_once().await;
    scenario.cancel_once().await;
    scenario.terminal_race().await;
}
