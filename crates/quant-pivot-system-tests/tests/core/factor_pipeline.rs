//! Factor pipeline system contracts across feature, factor, `PostgreSQL`, and `ClickHouse` ports.

use std::{
    collections::BTreeMap,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use async_trait::async_trait;
use chrono::{DateTime, Duration as ChronoDuration, Utc};
use prometheus::IntCounter;
use quant_pivot_compute::ComputeExecutor;
use quant_pivot_core::{
    ingest::{book_store::BookStore, data_plane_index::DataPlane, market_registry::MarketRegistry},
    observability::{
        factor_fact_writer::FactorEventWriter, feature_fact_writer::FeatureEventWriter,
        metrics_hub::MetricsHub,
    },
    prefetch::feature_window::FeatureWindowProvider,
    service::{
        factor_pipeline::{FactorExecutionPlane, FactorPipelineRequest, FactorPipelineService},
        feature_pipeline::{FeaturePipelineDeps, FeaturePipelineRequest, FeaturePipelineService},
    },
};
use quant_pivot_error::storage::StorageError;
use quant_pivot_models::{
    clickhouse::{
        BookL2LedgerRow, BookMicrostructureRow, DomainObservationRow, ExecutionParticipantFactRow,
        ExecutionParticipantRow, MarketExecutionRow, MarketResolutionRow, MidPriceBucketRow,
        QuantFactorEventRow,
    },
    domain::{
        data_plane::DecisionClock,
        market::{
            EventRegistryInfo, MarketRegistryInfo, TokenInfo,
            book::{BookLevel, BookSnapshot},
        },
        quant::{FactorValueInfo, NewFactorValue, NewModelRun},
    },
    enums::{
        catalog::CatalogFilterReasonSet,
        common::{CategorySet, MarketCategory, TickSize},
        factor::{
            FactorDefinitionScope, FactorFamily, FactorNormalization, FactorValueState,
            NormalizationSource,
        },
        market::{EventStatus, MarketStatus},
        quant::{DataQualityStatus, FactorDirection, ModelRunErrorCode, ModelRunKind},
    },
    runtime_config::{
        DataQualityConfig, DecimalValue, DomainConfig, FactorsConfig, FeaturesConfig,
        PerFactorNormalization,
    },
    types::{
        ContentHash, DecisionPolicySnapshotId, DomainInstrumentKey, EventId, FactorDefinitionId,
        FactorValueId, FeatureCell, FeatureStaleness, FeatureValue, FeatureVectorId, MarketId,
        ModelRunId, ModelVersionId, Price, Probability, ResearchFeatureContract, SchemaVersion,
        Shares, TokenId, Usd,
        stable_name::{FactorName, FeatureName},
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
    factors::{FactorEngine, factor_events},
    features::{
        FeatureVector,
        names::{
            book::{DEPTH_IMBALANCE, SPREAD_BPS, VISIBLE_LIQUIDITY_USD},
            market::TIME_TO_RESOLUTION_SECS,
        },
    },
    selection::{ModelFeatureRequirements, SelectedMarket},
};
use quant_pivot_storage::write::{AsyncWriter, AsyncWriterConfig, AsyncWriterObservability};
use quant_pivot_system_tests::{
    postgres::setup_pg,
    support::{
        catalog_fixtures::{make_event, make_market},
        execution_history_fixtures::{
            ConfigurableFactRead, live_history_config, live_history_repo,
            whale_concentration_by_market,
        },
        execution_pg_seed::seed_shared_demo_infra,
        fact_sink::DiscardFactWriter,
        factor_definitions::register_all_factor_definitions,
        pit::InMemoryDecisionSnapshotSource,
        publish_fresh_book,
        report_pipeline_harness::{EmptyBasisAlertRepo, EmptyLinkageRepo},
    },
};
use rust_decimal::Decimal;
use sea_orm::{ConnectionTrait, DatabaseConnection, DbBackend, Statement, TryGetable};
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

impl Catalog {
    fn registry_market(&self) -> MarketRegistryInfo {
        MarketRegistryInfo {
            market_id: MarketId::new(self.market_id),
            event_id: EventId::new(self.event_id),
            token_yes: TokenId::new(self.yes_token),
            token_no: TokenId::new(self.no_token),
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
    let market = (catalog).registry_market();
    registry.register_event(EventRegistryInfo {
        event_id: market.event_id.clone(),
        title: "Factor E2E".to_owned(),
        slug: "factor-e2e".to_owned(),
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
                Shares::new(Decimal::from(80)),
            )]),
            timestamp_ms,
            1,
        ),
        1,
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

    async fn market_execution_window(
        &self,
        _market_ids: Vec<MarketId>,
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
        _from_ms: i64,
        _to_ms: i64,
        _decision_at_ms: i64,
    ) -> Result<Vec<MarketExecutionRow>, StorageError> {
        Ok(Vec::new())
    }

    async fn execution_participants_between(
        &self,
        _market_ids: Vec<MarketId>,
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

fn noop_feature_writer() -> Arc<FeatureEventWriter> {
    Arc::new(FeatureEventWriter::new(Arc::new(DiscardFactWriter::new())))
}

fn factors_config() -> FactorsConfig {
    let mut config = FactorsConfig {
        enabled_factor_families: vec![
            FactorFamily::Liquidity,
            FactorFamily::Microstructure,
            FactorFamily::Resolution,
            FactorFamily::DataQuality,
        ],
        ..FactorsConfig::default()
    };
    // This persistence contract intentionally owns one market. Give the two
    // required factors explicit semantic bounds so it exercises an eligible
    // value write without fabricating a cross-section or weakening the live
    // small-cross-section policy.
    for (name, max) in [
        ("liquidity_depth", Decimal::from(100_000)),
        ("spread_efficiency", Decimal::from(10_000)),
    ] {
        config.normalization.per_factor.insert(
            name.to_owned(),
            PerFactorNormalization {
                method: FactorNormalization::MinMax,
                winsor_p: None,
                clamp_sigma: None,
                min: Some(DecimalValue::new(Decimal::ZERO)),
                max: Some(DecimalValue::new(max)),
            },
        );
    }
    config
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
    let data_plane = Arc::new(DataPlane::new());
    let registry = Arc::new(MarketRegistry::new(Arc::clone(&data_plane)));
    let book_store = Arc::new(BookStore::new(data_plane, Arc::new(MetricsHub::new())));
    wire_live_book(&registry, &book_store, &CATALOG);
    let live_pit = InMemoryDecisionSnapshotSource::freeze(registry.as_ref(), book_store.as_ref());

    let decision_at = Utc::now();
    let market_id = MarketId::new(CATALOG.market_id);
    let token_id = TokenId::new(CATALOG.yes_token);
    let fact_read = Arc::new(ConfigurableFactRead::new(
        Arc::new(EmptyFactRead),
        whale_concentration_by_market(
            &market_id,
            &token_id,
            (decision_at - ChronoDuration::seconds(60)).timestamp_millis(),
        ),
    ));
    let feature_repo = Arc::new(PgFeatureRepository::new(db.clone())) as Arc<dyn FeatureRepository>;
    let window_provider = FeatureWindowProvider::new(fact_read);
    let pipeline = FeaturePipelineService::new(FeaturePipelineDeps {
        compute: Arc::new(ComputeExecutor::new().expect("test compute executor")),
        window_provider,
        feature_repo,
        event_writer: noop_feature_writer(),
        exchange_history_repo: live_history_repo(),
        linkage_repo: Arc::new(EmptyLinkageRepo),
        basis_alert_repo: Arc::new(EmptyBasisAlertRepo),
        calibration_repo: Arc::new(PgCalibrationArtifactRepository::new(db.clone())),
        finalized_exchange_history: live_history_config(),
    });

    let features = FeaturesConfig::default();
    let domain = DomainConfig::default();
    let included = vec![selected_market()];
    let result = pipeline
        .run(FeaturePipelineRequest {
            included: &included,
            feature_contract: ResearchFeatureContract::FullL2,
            boundary: DecisionClock::new(0)
                .boundary(decision_at)
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
        .map(|info| info.feature_vector_id)
        .collect();
    (result.accepted, ids)
}

struct FactorPipelineScenario {
    db: DatabaseConnection,
    factor_repo: Arc<dyn FactorRepository>,
    model_run_repo: PgModelRunRepository,
    model_version_id: ModelVersionId,
    model_run_id: ModelRunId,
    run_decision_at: DateTime<Utc>,
    factors: FactorsConfig,
    features: FeaturesConfig,
    listed: Vec<FactorValueInfo>,
    snapshot_definition_ids: Vec<FactorDefinitionId>,
}

impl FactorPipelineScenario {
    async fn initialize(db: DatabaseConnection) -> Self {
        seed_catalog(&db, &CATALOG).await;
        let (vectors, feature_vector_ids) = build_features(&db).await;
        let run_decision_at = vectors
            .first()
            .map(|vector| vector.decision_at)
            .expect("factor pipeline decision boundary");
        let model_version_id = seed_shared_demo_infra(&db).await.model_version_id;
        let factor_repo =
            Arc::new(PgFactorRepository::new(db.clone())) as Arc<dyn FactorRepository>;
        let model_run_repo = PgModelRunRepository::new(db.clone());
        let model_run_id = ModelRunId::from_v7();
        let factors = factors_config();
        let features = FeaturesConfig::default();

        let mut scenario = Self {
            db,
            factor_repo,
            model_run_repo,
            model_version_id,
            model_run_id,
            run_decision_at,
            factors,
            features,
            listed: Vec::new(),
            snapshot_definition_ids: Vec::new(),
        };
        scenario
            .model_run_repo
            .create(scenario.new_live_run(model_run_id, '0'))
            .await
            .expect("create owning model run");
        register_all_factor_definitions(
            scenario.factor_repo.as_ref(),
            &scenario.factors,
            &scenario.features,
            &DomainConfig::default(),
            ResearchFeatureContract::FullL2,
            None,
        )
        .await
        .expect("register immutable factor definitions");

        let (writer, _worker) = AsyncWriter::new(
            AsyncWriterConfig::new("factor-e2e-factor-events").capacity(256),
            |_| Box::pin(async { Ok(()) }),
            IntCounter::new("factor_e2e_drops", "drops").expect("counter"),
            AsyncWriterObservability::default(),
        );
        let event_writer = Arc::new(FactorEventWriter::new(Arc::new(writer)));
        let service = FactorPipelineService::new(
            Arc::clone(&scenario.factor_repo),
            event_writer,
            Arc::new(ComputeExecutor::new().expect("test compute executor")),
        );
        let domain = DomainConfig::default();
        let factor_execution = FactorExecutionPlane::try_new(
            &scenario.factors,
            &scenario.features,
            &domain,
            ResearchFeatureContract::FullL2,
            None,
            None,
        )
        .expect("factor execution plane");
        let result = service
            .run(FactorPipelineRequest {
                model_run_id: &scenario.model_run_id,
                vectors: Arc::from(vectors),
                feature_vector_ids: &feature_vector_ids,
                factor_execution: &factor_execution,
            })
            .await
            .expect("factor pipeline");
        assert!(
            !result.persisted.is_empty(),
            "an eligible market must persist factor values: outcomes={}, rejected={:?}",
            result.outcomes.len(),
            result
                .rejected
                .iter()
                .map(|rejected| format!("{}: {}", rejected.market_id, rejected.reason))
                .collect::<Vec<_>>()
        );
        assert!(result.rejected.is_empty(), "no market should be rejected");

        scenario.listed = scenario
            .factor_repo
            .list_values_for_run(&scenario.model_run_id)
            .await
            .expect("list values for run");
        assert_eq!(scenario.listed.len(), result.persisted.len());
        assert!(
            scenario
                .listed
                .iter()
                .all(|value| value.model_run_id == scenario.model_run_id),
            "listed values must all belong to the run"
        );
        scenario.snapshot_definition_ids = scenario
            .listed
            .iter()
            .take(2)
            .map(|value| value.factor_definition_id)
            .collect();
        assert_eq!(
            scenario.snapshot_definition_ids.len(),
            2,
            "snapshot visibility contract requires at least two factors"
        );
        scenario
    }

    fn new_live_run(&self, model_run_id: ModelRunId, hash_seed: char) -> NewModelRun {
        NewModelRun {
            model_run_id,
            run_kind: ModelRunKind::LiveInference,
            model_version_id: Some(self.model_version_id),
            decision_policy_snapshot_id: DecisionPolicySnapshotId::from_v7(),
            market_selection_id: None,
            window_start: self.run_decision_at,
            window_end: self.run_decision_at,
            input_hash: ContentHash::parse(&format!("blake3:{}", hash_seed.to_string().repeat(64)))
                .expect("fixture model input hash"),
        }
    }

    fn persisted_value(&self) -> &FactorValueInfo {
        self.listed
            .first()
            .expect("at least one persisted factor value")
    }

    fn scored_value(&self) -> &FactorValueInfo {
        self.listed
            .iter()
            .find(|value| value.value_state == FactorValueState::Scored)
            .expect("factor pipeline must produce a scored value")
    }

    fn value_for_run(&self, source: &FactorValueInfo, model_run_id: ModelRunId) -> NewFactorValue {
        assert_eq!(
            source.model_run_id, self.model_run_id,
            "factor fixture sources must belong to the canonical pipeline run"
        );
        NewFactorValue {
            factor_value_id: FactorValueId::from_v7(),
            factor_definition_id: source.factor_definition_id,
            feature_vector_id: source.feature_vector_id,
            model_run_id,
            market_id: source.market_id.clone(),
            decision_at: source.decision_at,
            value_state: source.value_state,
            raw_value: source.raw_value,
            normalized_score: source.normalized_score,
            normalization_source: source.normalization_source,
            indeterminate_reason: source.indeterminate_reason,
            direction: source.direction,
            confidence: source.confidence,
            explanation: source.explanation.clone(),
        }
    }
}

impl FactorPipelineScenario {
    async fn verify_value_plane(&self) -> FeatureVectorId {
        self.running_is_hidden().await;
        let alternate_vector_id = self.reject_value_lineage().await;
        self.reject_value_shape().await;
        alternate_vector_id
    }

    async fn running_is_hidden(&self) {
        let persisted = self.persisted_value();
        assert!(
            self.factor_repo
                .latest_snapshot_bundle(
                    &self.snapshot_definition_ids,
                    &persisted.market_id,
                    &self.model_version_id,
                    Utc::now(),
                )
                .await
                .expect("query Running factor snapshot")
                .is_none(),
            "Running model runs must be serving-invisible"
        );
    }

    async fn reject_value_lineage(&self) -> FeatureVectorId {
        let persisted = self.persisted_value();
        let duplicate = self
            .factor_repo
            .create_values(vec![self.value_for_run(persisted, self.model_run_id)])
            .await;
        assert!(
            matches!(duplicate, Err(StorageError::Duplicate { .. })),
            "the natural model-run/vector/definition key must reject duplicate facts"
        );

        let mut lineage_mismatch = self.value_for_run(persisted, self.model_run_id);
        lineage_mismatch.market_id = MarketId::new("0xfactor-lineage-mismatch");
        assert!(matches!(
            self.factor_repo.create_values(vec![lineage_mismatch]).await,
            Err(StorageError::InvariantViolation { .. })
        ));

        let alternate_vector_id = FeatureVectorId::from_v7();
        self.db
            .execute_raw(Statement::from_sql_and_values(
                DbBackend::Postgres,
                "INSERT INTO quant_feature_vector (
                    feature_vector_id, market_id, token_id, decision_at, decision_boundary,
                    feature_schema_version, feature_hash, data_quality, staleness_ms, payload,
                    source_refs, decision_capture, decision_capture_hash, created_at
                 )
                 SELECT $1, market_id, token_id, decision_at, decision_boundary,
                        feature_schema_version, feature_hash, data_quality, staleness_ms, payload,
                        source_refs, decision_capture, decision_capture_hash, created_at
                 FROM quant_feature_vector
                 WHERE feature_vector_id = $2",
                [
                    alternate_vector_id.as_uuid().into(),
                    persisted.feature_vector_id.as_uuid().into(),
                ],
            ))
            .await
            .expect("clone alternate feature vector");
        let mut alternate_binding = self.value_for_run(persisted, self.model_run_id);
        alternate_binding.feature_vector_id = alternate_vector_id;
        assert!(matches!(
            self.factor_repo
                .create_values(vec![alternate_binding])
                .await,
            Err(StorageError::InvariantViolation { .. })
        ));
        let raw_alternate_binding = self
            .db
            .execute_raw(Statement::from_sql_and_values(
                DbBackend::Postgres,
                "INSERT INTO quant_factor_value (
                    factor_value_id, factor_definition_id, feature_vector_id, model_run_id,
                    market_id, decision_at, value_state, raw_value, normalized_score,
                    normalization_source, indeterminate_reason, direction, confidence,
                    explanation, created_at
                 )
                 SELECT $1, factor_definition_id, $3, model_run_id,
                        market_id, decision_at, value_state, raw_value, normalized_score,
                        normalization_source, indeterminate_reason, direction, confidence,
                        explanation, created_at
                 FROM quant_factor_value
                 WHERE factor_value_id = $2",
                [
                    FactorValueId::from_v7().as_uuid().into(),
                    persisted.factor_value_id.as_uuid().into(),
                    alternate_vector_id.as_uuid().into(),
                ],
            ))
            .await;
        assert!(
            raw_alternate_binding.is_err(),
            "one run/market/decision factor plane must bind one exact feature vector"
        );
        alternate_vector_id
    }

    async fn reject_value_shape(&self) {
        let persisted = self.persisted_value();
        let malformed = NewFactorValue {
            factor_value_id: FactorValueId::from_v7(),
            factor_definition_id: persisted.factor_definition_id,
            feature_vector_id: persisted.feature_vector_id,
            model_run_id: persisted.model_run_id,
            market_id: persisted.market_id.clone(),
            decision_at: persisted.decision_at,
            value_state: FactorValueState::Scored,
            raw_value: None,
            normalized_score: Some(Probability::ZERO),
            normalization_source: Some(NormalizationSource::PerMarket),
            indeterminate_reason: None,
            direction: persisted.direction,
            confidence: persisted.confidence,
            explanation: persisted.explanation.clone(),
        };
        assert!(matches!(
            self.factor_repo.create_values(vec![malformed]).await,
            Err(StorageError::InvariantViolation { .. })
        ));

        let direction = if persisted.direction == FactorDirection::Neutral {
            FactorDirection::Positive
        } else {
            FactorDirection::Neutral
        };
        let direction_mismatch = NewFactorValue {
            factor_value_id: FactorValueId::from_v7(),
            factor_definition_id: persisted.factor_definition_id,
            feature_vector_id: persisted.feature_vector_id,
            model_run_id: persisted.model_run_id,
            market_id: persisted.market_id.clone(),
            decision_at: persisted.decision_at,
            value_state: persisted.value_state,
            raw_value: persisted.raw_value,
            normalized_score: persisted.normalized_score,
            normalization_source: persisted.normalization_source,
            indeterminate_reason: persisted.indeterminate_reason,
            direction,
            confidence: persisted.confidence,
            explanation: persisted.explanation.clone(),
        };
        assert!(matches!(
            self.factor_repo
                .create_values(vec![direction_mismatch])
                .await,
            Err(StorageError::InvariantViolation { .. })
        ));
    }
}

impl FactorPipelineScenario {
    async fn verify_run_bindings(&self, alternate_vector_id: FeatureVectorId) {
        self.reject_concurrent_binding(alternate_vector_id).await;
        self.reject_window_mismatch().await;
        self.terminal_runs_hidden().await;
    }

    async fn reject_concurrent_binding(&self, alternate_vector_id: FeatureVectorId) {
        let persisted = self.persisted_value();
        let concurrent_run_id = ModelRunId::from_v7();
        self.model_run_repo
            .create(self.new_live_run(concurrent_run_id, '3'))
            .await
            .expect("create concurrent-binding run");
        let concurrent_sources = self
            .listed
            .iter()
            .filter(|value| value.feature_vector_id == persisted.feature_vector_id)
            .take(2)
            .collect::<Vec<_>>();
        assert_eq!(
            concurrent_sources.len(),
            2,
            "concurrency contract requires two factors from one vector"
        );
        let left_value = self.value_for_run(concurrent_sources[0], concurrent_run_id);
        let mut right_value = self.value_for_run(concurrent_sources[1], concurrent_run_id);
        right_value.feature_vector_id = alternate_vector_id;
        let concurrent_results: [_; 2] = tokio::time::timeout(Duration::from_secs(10), async {
            tokio::join!(
                self.factor_repo.create_values(vec![left_value]),
                self.factor_repo.create_values(vec![right_value])
            )
        })
        .await
        .expect("concurrent factor binding must not deadlock")
        .into();
        assert_eq!(
            concurrent_results
                .iter()
                .filter(|result| result.is_ok())
                .count(),
            1
        );
        assert_eq!(
            concurrent_results
                .iter()
                .filter(|result| matches!(result, Err(StorageError::InvariantViolation { .. })))
                .count(),
            1
        );
        let concurrent_created_at = concurrent_results
            .iter()
            .find_map(|result| {
                result
                    .as_ref()
                    .ok()
                    .and_then(|values| values.first())
                    .map(|value| value.created_at)
            })
            .expect("one concurrent factor value");
        let premature_finish = self
            .db
            .execute_raw(Statement::from_sql_and_values(
                DbBackend::Postgres,
                "UPDATE quant_model_run
                 SET status = 'cancelled'::qp_model_run_status,
                     error_code = 'cancelled_by_operator'::qp_model_run_error_code,
                     error_message = 'forged premature factor run finalization',
                     finished_at = $2
                 WHERE model_run_id = $1",
                [
                    concurrent_run_id.as_uuid().into(),
                    (concurrent_created_at - ChronoDuration::microseconds(1)).into(),
                ],
            ))
            .await;
        assert!(
            premature_finish.is_err(),
            "the database must reject a terminal timestamp before an owned factor value"
        );
        self.model_run_repo
            .cancel(
                &concurrent_run_id,
                "concurrent factor binding negative completed".to_owned(),
            )
            .await
            .expect("cancel concurrent-binding run");
    }

    async fn reject_window_mismatch(&self) {
        let persisted = self.persisted_value();
        let model_run_id = ModelRunId::from_v7();
        let mut model_run = self.new_live_run(model_run_id, '3');
        let wrong_decision_at = DateTime::from_timestamp_micros(
            self.run_decision_at
                .timestamp_micros()
                .checked_add(1)
                .expect("wrong decision timestamp"),
        )
        .expect("wrong decision DateTime");
        model_run.window_start = wrong_decision_at;
        model_run.window_end = wrong_decision_at;
        self.model_run_repo
            .create(model_run)
            .await
            .expect("create wrong-window run");
        assert!(matches!(
            self.factor_repo
                .create_values(vec![self.value_for_run(persisted, model_run_id)])
                .await,
            Err(StorageError::InvariantViolation { .. })
        ));
        self.model_run_repo
            .cancel(
                &model_run_id,
                "factor run-lineage negative completed".to_owned(),
            )
            .await
            .expect("cancel wrong-window run");
    }

    async fn terminal_runs_hidden(&self) {
        self.failed_run_is_hidden().await;
        self.cancelled_run_is_hidden().await;
    }

    async fn persist_run_values(&self, model_run_id: ModelRunId) -> DateTime<Utc> {
        let values = self
            .factor_repo
            .create_values(
                self.listed
                    .iter()
                    .map(|value| self.value_for_run(value, model_run_id))
                    .collect(),
            )
            .await
            .expect("persist terminal-visibility factor values");
        values
            .iter()
            .map(|value| value.created_at)
            .max()
            .expect("terminal-visibility factor values")
    }

    async fn failed_run_is_hidden(&self) {
        let persisted = self.persisted_value();
        let model_run_id = ModelRunId::from_v7();
        self.model_run_repo
            .create(self.new_live_run(model_run_id, '3'))
            .await
            .expect("create failed-visibility run");
        let latest_value_at = self.persist_run_values(model_run_id).await;
        let failed = self
            .model_run_repo
            .fail(
                &model_run_id,
                ModelRunErrorCode::FactorPlaneFailed,
                "factor visibility test failure".to_owned(),
            )
            .await
            .expect("fail visibility run");
        let finished_at = failed.finished_at.expect("database-owned failure time");
        assert!(finished_at >= latest_value_at);
        assert!(
            self.factor_repo
                .latest_snapshot_bundle(
                    &self.snapshot_definition_ids,
                    &persisted.market_id,
                    &self.model_version_id,
                    finished_at,
                )
                .await
                .expect("query Failed factor snapshot")
                .is_none(),
            "Failed model runs must be serving-invisible"
        );
    }

    async fn cancelled_run_is_hidden(&self) {
        let persisted = self.persisted_value();
        let model_run_id = ModelRunId::from_v7();
        self.model_run_repo
            .create(self.new_live_run(model_run_id, '3'))
            .await
            .expect("create cancelled-visibility run");
        let latest_value_at = self.persist_run_values(model_run_id).await;
        let cancelled = self
            .model_run_repo
            .cancel(
                &model_run_id,
                "factor visibility test cancellation".to_owned(),
            )
            .await
            .expect("cancel visibility run");
        let finished_at = cancelled
            .finished_at
            .expect("database-owned cancellation time");
        assert!(finished_at >= latest_value_at);
        assert!(
            self.factor_repo
                .latest_snapshot_bundle(
                    &self.snapshot_definition_ids,
                    &persisted.market_id,
                    &self.model_version_id,
                    finished_at,
                )
                .await
                .expect("query Cancelled factor snapshot")
                .is_none(),
            "Cancelled model runs must be serving-invisible"
        );
    }
}

impl FactorPipelineScenario {
    async fn verify_raw_contracts(&self) {
        let model_run_id = ModelRunId::from_v7();
        self.model_run_repo
            .create(self.new_live_run(model_run_id, '3'))
            .await
            .expect("create raw-contract run");
        let sealed_created_at = self.seal_database_clock(model_run_id).await;
        self.reject_raw_shape(model_run_id).await;
        self.reject_raw_numeric(model_run_id).await;
        self.reject_future_decision(model_run_id, sealed_created_at)
            .await;
        self.ledger_is_immutable().await;
    }

    async fn seal_database_clock(&self, model_run_id: ModelRunId) -> DateTime<Utc> {
        let persisted = self.persisted_value();
        let clock_source = self
            .listed
            .iter()
            .find(|value| value.factor_definition_id != persisted.factor_definition_id)
            .expect("DB-clock source factor");
        let factor_value_id = FactorValueId::from_v7();
        let before_row = self
            .db
            .query_one_raw(Statement::from_string(
                DbBackend::Postgres,
                "SELECT statement_timestamp() AS observed_at",
            ))
            .await
            .expect("query database clock before factor insert")
            .expect("database clock row before factor insert");
        let before_insert =
            DateTime::<Utc>::try_get(&before_row, "", "observed_at").expect("decode DB clock");
        self.db
            .execute_raw(Statement::from_sql_and_values(
                DbBackend::Postgres,
                "INSERT INTO quant_factor_value (
                    factor_value_id, factor_definition_id, feature_vector_id, model_run_id,
                    market_id, decision_at, value_state, raw_value, normalized_score,
                    normalization_source, indeterminate_reason, direction, confidence,
                    explanation, created_at
                 )
                 SELECT $1, factor_definition_id, feature_vector_id, $3,
                        market_id, decision_at, value_state, raw_value, normalized_score,
                        normalization_source, indeterminate_reason, direction, confidence,
                        explanation, to_timestamp(0)
                 FROM quant_factor_value
                 WHERE factor_value_id = $2",
                [
                    factor_value_id.as_uuid().into(),
                    clock_source.factor_value_id.as_uuid().into(),
                    model_run_id.as_uuid().into(),
                ],
            ))
            .await
            .expect("insert DB-clock-sealed factor value");
        let row = self
            .db
            .query_one_raw(Statement::from_sql_and_values(
                DbBackend::Postgres,
                "SELECT created_at, statement_timestamp() AS observed_at
                 FROM quant_factor_value
                 WHERE factor_value_id = $1",
                [factor_value_id.as_uuid().into()],
            ))
            .await
            .expect("query sealed factor value")
            .expect("sealed factor value row");
        let sealed_created_at =
            DateTime::<Utc>::try_get(&row, "", "created_at").expect("decode DB clock");
        let after_insert =
            DateTime::<Utc>::try_get(&row, "", "observed_at").expect("decode DB observation clock");
        assert!(
            sealed_created_at >= before_insert && sealed_created_at <= after_insert,
            "factor value availability must be sealed by the insert statement clock"
        );
        sealed_created_at
    }

    async fn reject_raw_shape(&self, model_run_id: ModelRunId) {
        let persisted = self.persisted_value();
        let malformed_insert = self
            .db
            .execute_raw(Statement::from_sql_and_values(
                DbBackend::Postgres,
                "INSERT INTO quant_factor_value (
                    factor_value_id, factor_definition_id, feature_vector_id, model_run_id,
                    market_id, decision_at, value_state, raw_value, normalized_score,
                    normalization_source, indeterminate_reason, direction, confidence,
                    explanation, created_at
                 )
                 SELECT $1, factor_definition_id, feature_vector_id, $3,
                        market_id, decision_at, 'scored'::qp_factor_value_state, NULL,
                        0.5::numeric, 'per_market'::qp_normalization_source, NULL,
                        direction, confidence, explanation, created_at
                 FROM quant_factor_value
                 WHERE factor_value_id = $2",
                [
                    FactorValueId::from_v7().as_uuid().into(),
                    persisted.factor_value_id.as_uuid().into(),
                    model_run_id.as_uuid().into(),
                ],
            ))
            .await;
        assert!(
            malformed_insert.is_err(),
            "relational state-tuple contract must reject scored values without a raw value"
        );

        let malformed_explanation = self
            .db
            .execute_raw(Statement::from_sql_and_values(
                DbBackend::Postgres,
                "INSERT INTO quant_factor_value (
                    factor_value_id, factor_definition_id, feature_vector_id, model_run_id,
                    market_id, decision_at, value_state, raw_value, normalized_score,
                    normalization_source, indeterminate_reason, direction, confidence,
                    explanation, created_at
                 )
                 SELECT $1, factor_definition_id, feature_vector_id, $3,
                        market_id, decision_at, value_state, raw_value, normalized_score,
                        normalization_source, indeterminate_reason, direction, confidence,
                        '{\"drivers\": [null]}'::jsonb, created_at
                 FROM quant_factor_value
                 WHERE factor_value_id = $2",
                [
                    FactorValueId::from_v7().as_uuid().into(),
                    persisted.factor_value_id.as_uuid().into(),
                    model_run_id.as_uuid().into(),
                ],
            ))
            .await;
        assert!(
            malformed_explanation.is_err(),
            "relational explanation contract must reject missing and malformed members"
        );
    }

    async fn reject_raw_numeric(&self, model_run_id: ModelRunId) {
        let persisted = self.persisted_value();
        let raw_overflow = self
            .db
            .execute_raw(Statement::from_sql_and_values(
                DbBackend::Postgres,
                "INSERT INTO quant_factor_value (
                    factor_value_id, factor_definition_id, feature_vector_id, model_run_id,
                    market_id, decision_at, value_state, raw_value, normalized_score,
                    normalization_source, indeterminate_reason, direction, confidence,
                    explanation, created_at
                 )
                 SELECT $1, factor_definition_id, feature_vector_id, $3,
                        market_id, decision_at, 'scored'::qp_factor_value_state,
                        10000000000000000::numeric, 0.5::numeric,
                        'per_market'::qp_normalization_source, NULL, direction, confidence,
                        explanation, created_at
                 FROM quant_factor_value
                 WHERE factor_value_id = $2",
                [
                    FactorValueId::from_v7().as_uuid().into(),
                    persisted.factor_value_id.as_uuid().into(),
                    model_run_id.as_uuid().into(),
                ],
            ))
            .await;
        assert!(
            raw_overflow.is_err(),
            "numeric(28,12) must reject a raw factor value with seventeen integer digits"
        );

        let contribution_overflow = self
            .db
            .execute_raw(Statement::from_sql_and_values(
                DbBackend::Postgres,
                "INSERT INTO quant_factor_value (
                    factor_value_id, factor_definition_id, feature_vector_id, model_run_id,
                    market_id, decision_at, value_state, raw_value, normalized_score,
                    normalization_source, indeterminate_reason, direction, confidence,
                    explanation, created_at
                 )
                 SELECT $1, factor_definition_id, feature_vector_id, $3,
                        market_id, decision_at, value_state, raw_value, normalized_score,
                        normalization_source, indeterminate_reason, direction, confidence,
                        jsonb_build_object(
                            'headline', 'overflow',
                            'drivers', jsonb_build_array(jsonb_build_object(
                                'feature_name', 'book.mid',
                                'contribution', '79228162514264337593543950336'
                            ))
                        ),
                        created_at
                 FROM quant_factor_value
                 WHERE factor_value_id = $2",
                [
                    FactorValueId::from_v7().as_uuid().into(),
                    persisted.factor_value_id.as_uuid().into(),
                    model_run_id.as_uuid().into(),
                ],
            ))
            .await;
        assert!(
            contribution_overflow.is_err(),
            "factor explanations must reject contributions outside rust_decimal"
        );
    }

    async fn reject_future_decision(
        &self,
        model_run_id: ModelRunId,
        sealed_created_at: DateTime<Utc>,
    ) {
        let scored = self.scored_value();
        let future_decision = Utc::now() + ChronoDuration::hours(1);
        let future_insert = self
            .db
            .execute_raw(Statement::from_sql_and_values(
                DbBackend::Postgres,
                "INSERT INTO quant_factor_value (
                    factor_value_id, factor_definition_id, feature_vector_id, model_run_id,
                    market_id, decision_at, value_state, raw_value, normalized_score,
                    normalization_source, indeterminate_reason, direction, confidence,
                    explanation, created_at
                 )
                 SELECT $1, factor_definition_id, feature_vector_id, $3,
                        market_id, $4, value_state, raw_value, normalized_score,
                        normalization_source, indeterminate_reason, direction, confidence,
                        explanation, created_at
                 FROM quant_factor_value
                 WHERE factor_value_id = $2",
                [
                    FactorValueId::from_v7().as_uuid().into(),
                    scored.factor_value_id.as_uuid().into(),
                    model_run_id.as_uuid().into(),
                    future_decision.into(),
                ],
            ))
            .await;
        assert!(
            future_insert.is_err(),
            "raw future-decision values must fail model-run and feature-vector lineage"
        );

        let failed = self
            .model_run_repo
            .fail(
                &model_run_id,
                ModelRunErrorCode::FactorPlaneFailed,
                "raw-contract negative completed".to_owned(),
            )
            .await
            .expect("fail raw-contract run");
        let finished_at = failed
            .finished_at
            .expect("database-owned raw-contract finish time");
        assert!(finished_at >= sealed_created_at);
        assert!(
            self.factor_repo
                .latest_snapshot(
                    &scored.factor_definition_id,
                    &scored.market_id,
                    &self.model_version_id,
                    finished_at,
                )
                .await
                .expect("query before future decision")
                .is_none(),
            "a factor decision after available_by must remain invisible"
        );
        assert!(
            self.factor_repo
                .latest_snapshot(
                    &scored.factor_definition_id,
                    &scored.market_id,
                    &self.model_version_id,
                    future_decision,
                )
                .await
                .expect("query rejected future decision")
                .is_none(),
            "a rejected future-decision row must never enter serving history"
        );

        let late_insert = self
            .db
            .execute_raw(Statement::from_sql_and_values(
                DbBackend::Postgres,
                "INSERT INTO quant_factor_value (
                    factor_value_id, factor_definition_id, feature_vector_id, model_run_id,
                    market_id, decision_at, value_state, raw_value, normalized_score,
                    normalization_source, indeterminate_reason, direction, confidence,
                    explanation, created_at
                 )
                 SELECT $1, factor_definition_id, feature_vector_id, $3,
                        market_id, decision_at, value_state, raw_value, normalized_score,
                        normalization_source, indeterminate_reason, direction, confidence,
                        explanation, created_at
                 FROM quant_factor_value
                 WHERE factor_value_id = $2",
                [
                    FactorValueId::from_v7().as_uuid().into(),
                    scored.factor_value_id.as_uuid().into(),
                    model_run_id.as_uuid().into(),
                ],
            ))
            .await;
        assert!(
            late_insert.is_err(),
            "raw factor values must not be appended after run finalization"
        );
    }

    async fn ledger_is_immutable(&self) {
        let persisted = self.persisted_value();
        let update = self
            .db
            .execute_raw(Statement::from_sql_and_values(
                DbBackend::Postgres,
                "UPDATE quant_factor_value SET confidence = confidence WHERE factor_value_id = $1",
                [persisted.factor_value_id.as_uuid().into()],
            ))
            .await;
        assert!(update.is_err(), "factor-value ledger must reject UPDATE");
        let delete = self
            .db
            .execute_raw(Statement::from_sql_and_values(
                DbBackend::Postgres,
                "DELETE FROM quant_factor_value WHERE factor_value_id = $1",
                [persisted.factor_value_id.as_uuid().into()],
            ))
            .await;
        assert!(delete.is_err(), "factor-value ledger must reject DELETE");

        let vector_update = self
            .db
            .execute_raw(Statement::from_sql_and_values(
                DbBackend::Postgres,
                "UPDATE quant_feature_vector SET market_id = market_id WHERE feature_vector_id = $1",
                [persisted.feature_vector_id.as_uuid().into()],
            ))
            .await;
        assert!(
            vector_update.is_err(),
            "feature-vector ledger must reject UPDATE"
        );
        let vector_delete = self
            .db
            .execute_raw(Statement::from_sql_and_values(
                DbBackend::Postgres,
                "DELETE FROM quant_feature_vector WHERE feature_vector_id = $1",
                [persisted.feature_vector_id.as_uuid().into()],
            ))
            .await;
        assert!(
            vector_delete.is_err(),
            "feature-vector ledger must reject DELETE"
        );
    }
}

impl FactorPipelineScenario {
    async fn verify_serving_visibility(&self) {
        self.finish_visible_run().await;
        self.preserve_complete_snapshot().await;
        self.assert_definition_revision().await;
    }

    async fn finish_visible_run(&self) -> DateTime<Utc> {
        let persisted = self.persisted_value();
        let scored = self.scored_value();
        let latest_value_created_at = self
            .listed
            .iter()
            .map(|value| value.created_at)
            .max()
            .expect("persisted factor value availability");
        let succeeded = self
            .model_run_repo
            .succeed(
                &self.model_run_id,
                ContentHash::parse(&format!("blake3:{}", "4".repeat(64)))
                    .expect("model output hash"),
                None,
            )
            .await
            .expect("succeed visible factor run");
        let finished_at = succeeded
            .finished_at
            .expect("database-owned successful finish time");
        assert!(finished_at >= latest_value_created_at);
        let before_finish = finished_at - ChronoDuration::microseconds(1);
        assert!(
            self.factor_repo
                .latest_snapshot_bundle(
                    &self.snapshot_definition_ids,
                    &persisted.market_id,
                    &self.model_version_id,
                    before_finish,
                )
                .await
                .expect("query factor snapshot before run finish")
                .is_none(),
            "factor values must remain invisible until their owning run finishes"
        );
        let visible = self
            .factor_repo
            .latest_snapshot_bundle(
                &self.snapshot_definition_ids,
                &persisted.market_id,
                &self.model_version_id,
                finished_at,
            )
            .await
            .expect("query succeeded factor snapshot")
            .expect("Succeeded LiveInference run must be visible");
        assert_eq!(visible.model_run_id, self.model_run_id);
        assert_eq!(visible.feature_vector_id, persisted.feature_vector_id);
        assert_eq!(visible.available_at, finished_at);

        let scored_snapshot = self
            .factor_repo
            .latest_snapshot(
                &scored.factor_definition_id,
                &scored.market_id,
                &self.model_version_id,
                finished_at,
            )
            .await
            .expect("query succeeded scored factor snapshot")
            .expect("succeeded scored factor must be visible");
        assert_eq!(scored_snapshot.factor_value_id, scored.factor_value_id);
        assert_eq!(scored_snapshot.feature_vector_id, scored.feature_vector_id);
        assert_eq!(scored_snapshot.model_run_id, self.model_run_id);
        assert_eq!(scored_snapshot.available_at, finished_at);
        assert!(matches!(
            self.factor_repo
                .create_values(vec![self.value_for_run(persisted, self.model_run_id)])
                .await,
            Err(StorageError::StateConflict { .. })
        ));
        finished_at
    }

    async fn preserve_complete_snapshot(&self) {
        let persisted = self.persisted_value();
        let model_run_id = ModelRunId::from_v7();
        self.model_run_repo
            .create(self.new_live_run(model_run_id, '3'))
            .await
            .expect("create newer incomplete run");
        let source = self
            .listed
            .iter()
            .find(|value| {
                value.factor_definition_id
                    == *self
                        .snapshot_definition_ids
                        .first()
                        .expect("snapshot factor definition")
            })
            .expect("incomplete-run source value");
        let incomplete_values = self
            .factor_repo
            .create_values(vec![self.value_for_run(source, model_run_id)])
            .await
            .expect("persist newer incomplete run");
        let incomplete_value_created_at = incomplete_values
            .first()
            .map(|value| value.created_at)
            .expect("newer incomplete factor value");
        let succeeded = self
            .model_run_repo
            .succeed(
                &model_run_id,
                ContentHash::parse(&format!("blake3:{}", "5".repeat(64)))
                    .expect("incomplete model output hash"),
                None,
            )
            .await
            .expect("succeed newer incomplete run");
        let incomplete_finished_at = succeeded
            .finished_at
            .expect("database-owned incomplete-run finish time");
        assert!(incomplete_finished_at >= incomplete_value_created_at);
        let fallback = self
            .factor_repo
            .latest_snapshot_bundle(
                &self.snapshot_definition_ids,
                &persisted.market_id,
                &self.model_version_id,
                incomplete_finished_at,
            )
            .await
            .expect("query older complete factor snapshot")
            .expect("an older complete run must survive a newer incomplete run");
        assert_eq!(fallback.model_run_id, self.model_run_id);
    }

    async fn assert_definition_revision(&self) {
        let definition_id = FactorEngine::new(
            &self.factors,
            &self.features,
            &DomainConfig::default(),
            None,
        )
        .definition_ref(&FactorName::new("data_quality"))
        .expect("data-quality revision")
        .factor_definition_id();
        let definition = self
            .factor_repo
            .find_definition(&definition_id)
            .await
            .expect("find definition")
            .expect("definition row");
        assert_eq!(definition.name, "data_quality");
        assert_eq!(definition.scope, FactorDefinitionScope::Generic);
    }
}

pub async fn create_definition_values_run() {
    let (pool, _container) = setup_pg().await;
    let scenario = FactorPipelineScenario::initialize(pool.connection().clone()).await;
    let alternate_vector_id = scenario.verify_value_plane().await;
    scenario.verify_run_bindings(alternate_vector_id).await;
    scenario.verify_raw_contracts().await;
    scenario.verify_serving_visibility().await;
}

pub async fn unregistered_factor_definitions_pipeline() {
    let (pool, _container) = setup_pg().await;
    let db = pool.connection().clone();
    seed_catalog(&db, &CATALOG).await;

    let (vectors, feature_vector_ids) = build_features(&db).await;
    let factor_repo = Arc::new(PgFactorRepository::new(db.clone())) as Arc<dyn FactorRepository>;
    let (writer, _worker) = AsyncWriter::new(
        AsyncWriterConfig::new("factor-e2e-unregistered").capacity(256),
        |_| Box::pin(async { Ok(()) }),
        IntCounter::new("factor_e2e_unpub_drops", "drops").expect("counter"),
        AsyncWriterObservability::default(),
    );
    let service = FactorPipelineService::new(
        Arc::clone(&factor_repo),
        Arc::new(FactorEventWriter::new(Arc::new(writer))),
        Arc::new(ComputeExecutor::new().expect("test compute executor")),
    );

    let model_run_id = ModelRunId::from_v7();
    let run_decision_at = vectors
        .first()
        .map(|vector| vector.decision_at)
        .expect("factor pipeline decision boundary");
    PgModelRunRepository::new(db.clone())
        .create(NewModelRun {
            model_run_id,
            run_kind: ModelRunKind::LiveInference,
            model_version_id: None,
            decision_policy_snapshot_id: DecisionPolicySnapshotId::from_v7(),
            market_selection_id: None,
            window_start: run_decision_at,
            window_end: run_decision_at,
            input_hash: ContentHash::parse(&format!("blake3:{}", "1".repeat(64))).expect("hash"),
        })
        .await
        .expect("create model run");

    let factors = factors_config();
    let features = FeaturesConfig::default();
    let domain = DomainConfig::default();
    let factor_execution = FactorExecutionPlane::try_new(
        &factors,
        &features,
        &domain,
        ResearchFeatureContract::FullL2,
        None,
        None,
    )
    .expect("factor execution plane");

    // Definitions are no longer auto-registered on the hot path: a fresh,
    // unregistered factor set must hard-block (never a silent pass).
    let unregistered = service
        .run(FactorPipelineRequest {
            model_run_id: &model_run_id,
            vectors: Arc::from(vectors.clone()),
            feature_vector_ids: &feature_vector_ids,
            factor_execution: &factor_execution,
        })
        .await;
    let Err(error) = unregistered else {
        panic!("unregistered definitions must block the factor plane");
    };
    assert!(
        error
            .to_string()
            .contains("were not registered immutably during training"),
        "unexpected error: {error}"
    );

    register_all_factor_definitions(
        factor_repo.as_ref(),
        &factors,
        &features,
        &domain,
        ResearchFeatureContract::FullL2,
        None,
    )
    .await
    .expect("register immutable definitions");
    service
        .run(FactorPipelineRequest {
            model_run_id: &model_run_id,
            vectors: Arc::from(vectors),
            feature_vector_ids: &feature_vector_ids,
            factor_execution: &factor_execution,
        })
        .await
        .expect("registered immutable definitions must allow factor compute");
}

pub async fn factor_event_writer_batches() {
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
        IntCounter::new("factor_e2e_batch_drops", "drops").expect("counter"),
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
    as_of: DateTime<Utc>,
) -> FeatureVector {
    let mut values: BTreeMap<FeatureName, FeatureValue> = BTreeMap::new();
    values.insert(
        VISIBLE_LIQUIDITY_USD,
        FeatureValue::Usd(Usd::new(liquidity)),
    );
    values.insert(SPREAD_BPS, FeatureValue::Bps(spread_bps));
    values.insert(DEPTH_IMBALANCE, FeatureValue::Decimal(Decimal::new(2, 1)));
    values.insert(TIME_TO_RESOLUTION_SECS, FeatureValue::Count(172_800));
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
