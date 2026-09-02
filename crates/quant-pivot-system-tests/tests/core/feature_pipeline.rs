//! Feature pipeline system contracts across provider, selector, and `PostgreSQL` ports.

use std::{
    collections::HashSet,
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
};

use async_trait::async_trait;
use chrono::{DateTime, Duration as ChronoDuration, Utc};
use quant_pivot_compute::ComputeExecutor;
use quant_pivot_core::{
    ingest::{book_store::BookStore, data_plane_index::DataPlane, market_registry::MarketRegistry},
    observability::{feature_fact_writer::FeatureEventWriter, metrics_hub::MetricsHub},
    prefetch::{feature_window::FeatureWindowProvider, market_candidates::MarketCandidateProvider},
    service::feature_pipeline::{
        FeaturePipelineDeps, FeaturePipelineRequest, FeaturePipelineResult, FeaturePipelineService,
    },
};
use quant_pivot_error::storage::StorageError;
use quant_pivot_models::{
    clickhouse::{
        BookL2LedgerRow, BookMicrostructureRow, DomainObservationRow, ExecutionParticipantFactRow,
        ExecutionParticipantRow, MarketExecutionRow, MarketResolutionRow, MidPriceBucketRow,
        QuantFeatureEventRow,
    },
    domain::{
        api::CalibrationArtifactListQuery,
        data_plane::{DecisionClock, HistorySealChunkRef},
        market::{
            EventRegistryInfo, MarketMakerRebateEvidence, MarketRegistryInfo, TokenInfo,
            book::{BookLevel, BookSnapshot},
        },
        pagination::Paginated,
        quant::{
            CalibrationArtifactInfo, FeatureVectorInfo, ModelScoreCalibrationCommitOutcome,
            NewCalibrationArtifact, NewFeatureVector, VerifiedModelScoreCalibrationCommit,
        },
    },
    enums::{
        catalog::CatalogFilterReasonSet,
        clickhouse::ChFeatureCellState,
        common::{CategorySet, MarketCategory, TickSize},
        market::{EventStatus, MarketStatus},
        quant::DataQualityStatus,
    },
    runtime_config::{DataQualityConfig, DomainConfig, FeaturesConfig, SelectionConfig},
    types::{
        CalibrationArtifactId, ContentHash, DecisionPolicySnapshotId, DomainInstrumentKey, EventId,
        FeatureVectorId, MarketId, Price, ResearchFeatureContract, Shares, TokenId, Usd,
        calibration::PublishedWeatherStationLeadBias,
    },
};
use quant_pivot_repository::{
    postgres::{
        PgCalibrationArtifactRepository, PgEventRepository, PgFeatureRepository, PgMarketRepository,
    },
    traits::{
        CalibrationArtifactRepository, EventRepository, FeatureRepository, MarketRepository,
        QuantFactReadRepository,
    },
};
use quant_pivot_research::{
    features::{FeatureVector, names},
    hashing::ResearchHasher,
    pit::PointInTimeSnapshotSource,
    selection::{
        ConfiguredMarketSelector, MarketSelectionBuildRequest, MarketSelector,
        ModelFeatureRequirements, SelectedMarket,
    },
};
use quant_pivot_system_tests::{
    postgres::setup_pg,
    support::{
        catalog_fixtures::{make_event, make_market},
        execution_history_fixtures::{
            ConfigurableFactRead, live_activation_head, live_history_config, live_history_repo,
            whale_concentration_by_market,
        },
        fact_sink::RecordingFactWriter,
        pit::InMemoryDecisionSnapshotSource,
        publish_fresh_book,
        report_pipeline_harness::{EmptyBasisAlertRepo, EmptyLinkageRepo},
    },
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

impl E2eCatalog {
    fn registry_market(&self) -> MarketRegistryInfo {
        MarketRegistryInfo {
            market_id: MarketId::new(self.market_id),
            event_id: EventId::new(self.event_id),
            token_yes: TokenId::new(self.yes_token),
            token_no: TokenId::new(self.no_token),
            question: "Feature E2E?".into(),
            slug: "feature-e2e".into(),
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
            maker_rebate_evidence: MarketMakerRebateEvidence::source_unavailable(),
            start_date: Some(Utc::now() - ChronoDuration::days(2)),
            end_date: Some(Utc::now() + ChronoDuration::days(5)),
            resolved_at: None,
            created_at: Some(Utc::now() - ChronoDuration::days(2)),
            updated_at: Utc::now(),
        }
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
    let market = (catalog).registry_market();
    registry.register_event(EventRegistryInfo {
        event_id: market.event_id.clone(),
        title: "Feature E2E".to_owned(),
        slug: "feature-e2e".to_owned(),
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

struct EmptyFactRead;

struct EmptyCalibrationArtifactRepo;

#[async_trait]
impl CalibrationArtifactRepository for EmptyCalibrationArtifactRepo {
    async fn create(
        &self,
        _artifact: NewCalibrationArtifact,
    ) -> Result<CalibrationArtifactInfo, StorageError> {
        unimplemented!("feature-plane test does not create calibration artifacts")
    }

    async fn commit_model_score(
        &self,
        _commit: VerifiedModelScoreCalibrationCommit,
    ) -> Result<ModelScoreCalibrationCommitOutcome, StorageError> {
        unimplemented!("feature-plane test does not commit calibration artifacts")
    }

    async fn find_by_id(
        &self,
        _artifact_id: &CalibrationArtifactId,
    ) -> Result<Option<CalibrationArtifactInfo>, StorageError> {
        Ok(None)
    }

    async fn find_by_content_hash(
        &self,
        _content_hash: &ContentHash,
    ) -> Result<Option<CalibrationArtifactInfo>, StorageError> {
        Ok(None)
    }

    async fn page(
        &self,
        _query: CalibrationArtifactListQuery,
    ) -> Result<Paginated<CalibrationArtifactInfo>, StorageError> {
        unimplemented!("feature-plane test does not page calibration artifacts")
    }

    async fn published_weather_through(
        &self,
        _at: DateTime<Utc>,
    ) -> Result<Vec<PublishedWeatherStationLeadBias>, StorageError> {
        Ok(Vec::new())
    }

    async fn mark_active(
        &self,
        _artifact_id: &CalibrationArtifactId,
    ) -> Result<CalibrationArtifactInfo, StorageError> {
        unimplemented!("feature-plane test does not activate calibration artifacts")
    }
}

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

/// A feature repository that records audit persistence for rejected vectors.
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
        let created_at = Utc::now();
        Ok(vectors
            .into_iter()
            .map(|vector| FeatureVectorInfo {
                feature_vector_id: vector.feature_vector_id,
                market_id: vector.market_id,
                token_id: vector.token_id,
                decision_at: vector.decision_at,
                decision_boundary: vector.decision_boundary,
                feature_schema_version: vector.feature_schema_version,
                feature_hash: vector.feature_hash,
                data_quality: vector.data_quality,
                staleness_ms: vector.staleness_ms,
                payload: vector.payload,
                source_refs: vector.source_refs,
                decision_capture: vector.decision_capture,
                decision_capture_hash: vector.decision_capture_hash,
                created_at,
            })
            .collect())
    }

    async fn find_by_id(
        &self,
        _id: &FeatureVectorId,
    ) -> Result<Option<FeatureVectorInfo>, StorageError> {
        Ok(None)
    }

    async fn find_by_ids(
        &self,
        _ids: &[FeatureVectorId],
    ) -> Result<Vec<FeatureVectorInfo>, StorageError> {
        Ok(Vec::new())
    }
}

fn assert_feature_evidence(result: &FeaturePipelineResult) {
    let vector = &result.accepted[0];
    let persisted = &result.persisted[0];
    let evidence = result
        .feature_evidence
        .as_ref()
        .expect("accepted vector must have a serving evidence commitment");

    assert_eq!(
        evidence.expected_row_count(),
        u64::try_from(vector.value_count()).expect("feature cell count fits u64")
    );
    assert_eq!(
        evidence.feature_vector_ids(),
        std::slice::from_ref(&persisted.feature_vector_id)
    );
}

fn assert_emitted_feature_cells(
    flushed_events: &Mutex<Vec<QuantFeatureEventRow>>,
    vector: &FeatureVector,
    persisted: &FeatureVectorInfo,
) {
    let emitted = flushed_events.lock().expect("feature event sink");
    assert_eq!(
        emitted.len(),
        vector.value_count(),
        "every accepted FeatureCell must emit one stateful fact"
    );
    assert!(
        emitted
            .iter()
            .all(|row| row.feature_vector_id == persisted.feature_vector_id),
        "every emitted cell must bind to the Postgres-returned vector id"
    );
    let emitted_names: HashSet<_> = emitted
        .iter()
        .map(|row| row.feature_name.as_str())
        .collect();
    let vector_names: HashSet<_> = vector.iter_cells().map(|(name, _)| name.as_str()).collect();
    assert_eq!(emitted_names, vector_names);
    assert!(
        emitted
            .iter()
            .any(|row| row.cell_state == ChFeatureCellState::Missing),
        "missing cells must be explicit facts, not omitted rows"
    );
    drop(emitted);
}

pub async fn insufficient_vectors_audited_input() {
    // Catalog and book inputs are valid, but the required microstructure window
    // is absent. The model contract must reject the vector without inventing a
    // replacement value, while retaining its complete audit evidence.
    let data_plane = Arc::new(DataPlane::new());
    let registry = Arc::new(MarketRegistry::new(Arc::clone(&data_plane)));
    let book_store = Arc::new(BookStore::new(data_plane, Arc::new(MetricsHub::new())));
    wire_live_book(&registry, &book_store, &CATALOG);
    let live_pit = InMemoryDecisionSnapshotSource::freeze(registry.as_ref(), book_store.as_ref());

    let features = FeaturesConfig::default();
    let as_of = Utc::now();
    let market = SelectedMarket {
        market_id: MarketId::new(CATALOG.market_id),
        event_id: EventId::new(CATALOG.event_id),
        category: MarketCategory::Sports,
        primary_token_id: TokenId::new(CATALOG.yes_token),
        secondary_token_id: Some(TokenId::new(CATALOG.no_token)),
        liquidity_usd: Some(Usd::new(Decimal::from(25_000))),
        volume_24h_usd: Some(Usd::new(Decimal::from(9_000))),
        source_refs: Vec::new(),
    };

    let repo = Arc::new(RecordingFeatureRepo {
        persisted_rows: AtomicUsize::new(0),
    });
    let rejected_events = Arc::new(Mutex::new(Vec::<QuantFeatureEventRow>::new()));
    let event_writer = Arc::new(FeatureEventWriter::new(Arc::new(RecordingFactWriter::new(
        Arc::clone(&rejected_events),
    ))));
    let execution_history = whale_concentration_by_market(
        &MarketId::new(CATALOG.market_id),
        &TokenId::new(CATALOG.yes_token),
        (as_of - ChronoDuration::minutes(1)).timestamp_millis(),
    );
    let window_provider = FeatureWindowProvider::new(Arc::new(ConfigurableFactRead::new(
        Arc::new(EmptyFactRead),
        execution_history,
    )));
    let pipeline = FeaturePipelineService::new(FeaturePipelineDeps {
        compute: Arc::new(ComputeExecutor::new().expect("test compute executor")),
        window_provider,
        feature_repo: Arc::clone(&repo) as Arc<dyn FeatureRepository>,
        event_writer,
        exchange_history_repo: live_history_repo(),
        linkage_repo: Arc::new(EmptyLinkageRepo),
        basis_alert_repo: Arc::new(EmptyBasisAlertRepo),
        calibration_repo: Arc::new(EmptyCalibrationArtifactRepo),
        finalized_exchange_history: live_history_config(),
    });

    let domain = DomainConfig::default();
    let included = vec![market];
    let execution_history_seal = live_activation_head();
    let result = pipeline
        .run(FeaturePipelineRequest {
            included: &included,
            feature_contract: ResearchFeatureContract::FullL2,
            boundary: DecisionClock::new(0)
                .boundary(as_of)
                .expect("decision boundary"),
            features: &features,
            domain: &domain,
            data_quality: &DataQualityConfig::default(),
            model_requirements: &ModelFeatureRequirements::generic_only(vec![
                names::micro::QUOTE_UPDATE_RATE,
            ]),
            pit: &live_pit,
            decision_policy_snapshot_id: DecisionPolicySnapshotId::from_v7(),
            liquidity_cap_usd: Usd::new(Decimal::from(10_000)),
            execution_history_seal: Some(&execution_history_seal),
        })
        .await
        .expect("pipeline");

    assert!(
        result.accepted.is_empty(),
        "the bad vector must not be accepted"
    );
    assert_eq!(result.persisted.len(), 0);
    assert!(
        result.feature_evidence.is_none(),
        "rejected feature cells must not enter the serving evidence commitment"
    );
    assert_eq!(result.rejected.len(), 1);
    assert_eq!(result.rejected[0].market_id.as_str(), CATALOG.market_id);
    assert_eq!(
        result.rejected[0].data_quality,
        DataQualityStatus::Insufficient,
        "rejection must retain its aggregate data-quality evidence"
    );
    assert!(
        !result.rejected[0].missing_required.is_empty(),
        "rejection must report the missing required features"
    );
    assert_eq!(
        repo.persisted_rows
            .load(std::sync::atomic::Ordering::Relaxed),
        1,
        "rejected vectors must be retained as audit evidence"
    );
    assert!(
        !rejected_events
            .lock()
            .expect("rejected event sink mutex poisoned")
            .is_empty(),
        "rejected feature cells must be written to the durable audit fact stream"
    );
}

pub async fn create_feature_vector_find() {
    let (pool, _container) = setup_pg().await;
    let db = pool.connection().clone();
    seed_catalog(&db, &CATALOG).await;

    let data_plane = Arc::new(DataPlane::new());
    let registry = Arc::new(MarketRegistry::new(Arc::clone(&data_plane)));
    let book_store = Arc::new(BookStore::new(data_plane, Arc::new(MetricsHub::new())));
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
    let features = FeaturesConfig::default();
    let domain = DomainConfig::default();
    let as_of = Utc::now();

    let request = MarketSelectionBuildRequest {
        decision_at: as_of,
        decision_policy_snapshot_id: DecisionPolicySnapshotId::from_v7(),
        selection: SelectionConfig {
            enabled_categories: vec![MarketCategory::Sports],
            ..SelectionConfig::default()
        },
        data_quality: DataQualityConfig::default(),
        features: features.clone(),
        model_requirements: ModelFeatureRequirements::generic_only(vec![names::book::BEST_BID]),
        knowledge_lag_secs: 0,
        route_availability: None,
    };

    let boundary = DecisionClock::new(0)
        .boundary(as_of)
        .expect("decision boundary");
    let candidate_batch = provider
        .candidates(&boundary, &domain)
        .await
        .expect("candidates");
    let snapshot = selector
        .build_snapshot(request, candidate_batch.candidates)
        .await
        .expect("selection");
    assert_eq!(snapshot.included.len(), 1);

    let feature_repo = Arc::new(PgFeatureRepository::new(db.clone())) as Arc<dyn FeatureRepository>;

    let flushed_events = Arc::new(Mutex::new(Vec::<QuantFeatureEventRow>::new()));
    let event_writer = Arc::new(FeatureEventWriter::new(Arc::new(RecordingFactWriter::new(
        Arc::clone(&flushed_events),
    ))));

    let execution_history = whale_concentration_by_market(
        &MarketId::new(CATALOG.market_id),
        &TokenId::new(CATALOG.yes_token),
        (as_of - ChronoDuration::minutes(1)).timestamp_millis(),
    );
    let window_provider = FeatureWindowProvider::new(Arc::new(ConfigurableFactRead::new(
        Arc::new(EmptyFactRead),
        execution_history,
    )));
    let pipeline = FeaturePipelineService::new(FeaturePipelineDeps {
        compute: Arc::new(ComputeExecutor::new().expect("test compute executor")),
        window_provider,
        feature_repo: Arc::clone(&feature_repo),
        event_writer: Arc::clone(&event_writer),
        exchange_history_repo: live_history_repo(),
        linkage_repo: Arc::new(EmptyLinkageRepo),
        basis_alert_repo: Arc::new(EmptyBasisAlertRepo),
        calibration_repo: Arc::new(PgCalibrationArtifactRepository::new(db.clone())),
        finalized_exchange_history: live_history_config(),
    });

    let execution_history_seal = live_activation_head();
    let result = pipeline
        .run(FeaturePipelineRequest {
            included: &snapshot.included,
            feature_contract: ResearchFeatureContract::FullL2,
            boundary,
            features: &features,
            domain: &domain,
            data_quality: &DataQualityConfig::default(),
            model_requirements: &ModelFeatureRequirements::default(),
            pit: candidate_batch.snapshot_source.as_ref(),
            decision_policy_snapshot_id: DecisionPolicySnapshotId::from_v7(),
            liquidity_cap_usd: Usd::new(Decimal::from(10_000)),
            execution_history_seal: Some(&execution_history_seal),
        })
        .await
        .expect("pipeline");

    assert_eq!(result.accepted.len(), 1);
    assert!(result.rejected.is_empty());
    let vector = &result.accepted[0];
    assert!(vector.value(&names::book::BEST_BID).is_some());

    let expected_hash = ResearchHasher::feature_vector(vector).expect("hash");
    let persisted = &result.persisted[0];
    assert_eq!(persisted.feature_hash, expected_hash);
    assert_feature_evidence(&result);

    let loaded = feature_repo
        .find_by_id(&persisted.feature_vector_id)
        .await
        .expect("find")
        .expect("row");
    assert_eq!(loaded.feature_hash, expected_hash);
    assert_eq!(loaded.market_id.as_str(), CATALOG.market_id);

    let boundary = DecisionClock::new(0)
        .boundary(as_of)
        .expect("decision boundary");
    assert_eq!(loaded.decision_boundary, boundary);

    assert_emitted_feature_cells(&flushed_events, vector, persisted);
}
