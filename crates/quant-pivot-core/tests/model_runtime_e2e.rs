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
    governance::WeightOverlayApplicator,
    observability::{
        factor_fact_writer::FactorEventWriter, feature_fact_writer::FeatureEventWriter,
        metrics_hub::MetricsHub, signal_candidate_fact_writer::SignalCandidateEventWriter,
    },
    pipeline::{
        book_store::BookStore, feature_window_provider::FeatureWindowProvider,
        market_registry::MarketRegistry, point_in_time::LiveBookDataSource,
    },
    service::{
        factor_pipeline::FactorPipelineService,
        feature_pipeline::{FeaturePipelineRequest, FeaturePipelineService},
        model_runner::{InferenceAlertSink, ModelRunRequest, ModelRunner, ModelRunnerDeps},
    },
};
use quant_pivot_error::storage::StorageError;
use quant_pivot_models::{
    clickhouse::{BookMicrostructureRow, BookSnapshotRow, MarketResolutionRow},
    domain::{
        NewModelRun, NewModelSpec, NewModelVersion,
        market::{MarketRegistryInfo, TokenInfo, book::BookLevel},
    },
    enums::{
        common::{CategorySet, MarketCategory, TickSize},
        factor::FactorFamily,
        market::MarketStatus,
        model::ModelFamily,
        quant::{ModelRunErrorCode, ModelRunKind, ModelRunStatus, PublicationStatus},
    },
    runtime_config::{
        DataQualityConfig, DecimalString, FactorWeights, FactorsConfig, FeaturesConfig,
        ModelConfig, ModelVersionRef,
    },
    types::{
        ContentHash, EventId, FeatureVectorId, MarketId, ModelRunId, ModelSpecId, ModelVersionId,
        Price, RuntimeConfigVersionId, SchemaVersion, Shares, TokenId, Usd,
    },
};
use quant_pivot_repository::{
    postgres::{
        PgEventRepository, PgFactorRepository, PgFeatureRepository, PgMarketRepository,
        PgModelRegistryRepository, PgModelRunRepository, PgShadowComparisonRepository,
    },
    traits::{
        EventRepository, FactorRepository, FeatureRepository, MarketRepository,
        ModelRegistryRepository, ModelRunRepository, QuantFactReadRepository,
        ShadowComparisonRepository,
    },
};
use quant_pivot_research::{
    artifact::{ArtifactStore, LocalArtifactStore},
    factors::FactorEngine,
    features::{FeatureSchema, FeatureVector, PitView},
    hashing::ResearchHasher,
    model::{
        DefaultModelRuntimeFactoryBuilder, FactorWeight, ModelArtifact, ModelArtifactHeader,
        ReturnModelSpec, ScoreMultiplierSpec, SubstitutionConfidenceRules,
        WeightedFactorModelArtifact,
    },
    selection::{ModelFeatureRequirements, SelectedMarket},
};
use quant_pivot_storage::write::{AsyncWriter, AsyncWriterConfig, AsyncWriterObservability};
use quant_pivot_test_support::{
    catalog_fixtures::{make_event, make_market},
    pg::setup_pg,
};
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

fn registry_market() -> MarketRegistryInfo {
    MarketRegistryInfo {
        market_id: MarketId::new(MARKET_ID),
        event_id: EventId::new(EVENT_ID),
        token_yes: TokenId::new(YES_TOKEN),
        token_no: TokenId::new(NO_TOKEN),
        question: "Model E2E?".into(),
        slug: "model-e2e".into(),
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
        end_date: Some(Utc::now() + ChronoDuration::days(2)),
        resolved_at: None,
        created_at: Utc::now() - ChronoDuration::days(2),
        updated_at: Utc::now(),
    }
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
    let (writer, _worker) = AsyncWriter::new(
        AsyncWriterConfig::new("model-e2e-feature").capacity(64),
        |_| Box::pin(async { Ok(()) }),
        prometheus::IntCounter::new("model_e2e_feat_drops", "d").expect("counter"),
        AsyncWriterObservability::default(),
    );
    Arc::new(FeatureEventWriter::new(Arc::new(writer)))
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

/// Build + persist feature vectors for the seeded market via the live book.
async fn build_features(db: &DatabaseConnection) -> (Vec<FeatureVector>, Vec<FeatureVectorId>) {
    let registry = Arc::new(MarketRegistry::new());
    let book_store = Arc::new(BookStore::new(Arc::new(MetricsHub::new())));
    registry.register_market(registry_market());
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
        u64::try_from(Utc::now().timestamp_millis()).unwrap_or(0),
        None,
    );
    let live_pit = LiveBookDataSource::new(Arc::clone(&book_store), Arc::clone(&registry));

    let feature_repo = Arc::new(PgFeatureRepository::new(db.clone())) as Arc<dyn FeatureRepository>;
    let pipeline = FeaturePipelineService::new(
        FeatureWindowProvider::new(Arc::new(EmptyFactRead)),
        feature_repo,
        noop_feature_writer(),
    );

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
        })
        .await
        .expect("feature pipeline");

    assert_eq!(result.accepted.len(), 1, "market must produce a vector");
    let ids = result
        .persisted
        .iter()
        .map(|info| info.feature_vector_id.clone())
        .collect();
    (result.accepted, ids)
}

/// Author a weighted artifact bound to the active schema, weighting every enabled
/// factor equally, and persist its bytes + registry rows. Returns the version id.
async fn publish_weighted_model(
    db: &DatabaseConnection,
    store: &Arc<dyn ArtifactStore>,
    factors: &FactorsConfig,
    features: &FeaturesConfig,
) -> ModelVersionId {
    let engine = FactorEngine::new(factors, features);
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
        ResearchHasher::feature_schema(&FeatureSchema::build(features)).expect("feature hash");
    let factor_schema_hash = engine.factor_schema_hash().expect("factor hash");

    let artifact = ModelArtifact::WeightedFactor(Box::new(WeightedFactorModelArtifact {
        header: ModelArtifactHeader {
            model_version_id: model_version_id.clone(),
            model_family: ModelFamily::WeightedFactor,
            feature_schema_hash,
            factor_schema_hash,
        },
        weights,
        prediction_horizon_secs: 86_400,
        multipliers: ScoreMultiplierSpec::conservative(),
        substitution_confidence_rules: SubstitutionConfidenceRules::conservative(),
        return_model: ReturnModelSpec::heuristic_default(),
        required_features: Vec::new(),
        objective_report: None,
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
            metrics_json: serde_json::json!({}),
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
            metrics_json: serde_json::json!({}),
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
    let engine = FactorEngine::new(base, features);
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
    let factor_pipeline = Arc::new(FactorPipelineService::new(
        factor_repo,
        noop_factor_writer(),
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
        factory_builder: Arc::new(DefaultModelRuntimeFactoryBuilder::new(store)),
        factor_pipeline,
        signal_writer: noop_signal_writer(),
        alerts: Arc::new(CountingAlertSink { critical }),
        weight_overlay,
    })
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
    let (vectors, ids) = build_features(&db).await;

    let store = artifact_store();
    let active = publish_weighted_model(&db, &store, &factors, &features).await;

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
            features: &features,
            factors: &factors,
            model: &model_config(&active, None),
            top_n: 10,
            as_of: Utc::now(),
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
    let (vectors, ids) = build_features(&db).await;

    let store = artifact_store();
    let active = publish_weighted_model(&db, &store, &factors, &features).await;

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
            features: &features,
            factors: &factors,
            model: &model_config(&active, Some(&missing_shadow)),
            top_n: 10,
            as_of: Utc::now(),
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

#[tokio::test]
async fn hot_update_changes_candidate_weights_not_published_artifact() {
    let (pool, _container) = setup_pg().await;
    let db = pool.connection().clone();
    seed_catalog(&db).await;

    let base_factors = factors_config();
    let features = FeaturesConfig::default();
    let (vectors, ids) = build_features(&db).await;

    let store = artifact_store();
    let published = publish_weighted_model(&db, &store, &base_factors, &features).await;
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
    let selection = vec![selected_market()];
    let as_of = Utc::now();

    overlay.reload(
        &factors_with_overlay_skew(&base_factors, &features, 0),
        &model,
    );
    let first = runner
        .run(ModelRunRequest {
            runtime_config_version_id: RuntimeConfigVersionId::from_v7(),
            market_selection_id: None,
            selection: &selection,
            feature_vectors: &vectors,
            feature_vector_ids: &ids,
            features: &features,
            factors: &base_factors,
            model: &model,
            top_n: 10,
            as_of,
        })
        .await
        .expect("first round");

    overlay.reload(
        &factors_with_overlay_skew(&base_factors, &features, 1),
        &model,
    );
    let second = runner
        .run(ModelRunRequest {
            runtime_config_version_id: RuntimeConfigVersionId::from_v7(),
            market_selection_id: None,
            selection: &selection,
            feature_vectors: &vectors,
            feature_vector_ids: &ids,
            features: &features,
            factors: &base_factors,
            model: &model,
            top_n: 10,
            as_of,
        })
        .await
        .expect("second round");

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
    let (vectors, ids) = build_features(&db).await;

    let store = artifact_store();
    let active = publish_weighted_model(&db, &store, &factors, &features).await;
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
            features: &features,
            factors: &factors,
            model: &model_config(&active, None),
            top_n: 10,
            as_of: Utc::now(),
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
