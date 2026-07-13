//! End-to-end trainer + backtest (Phase 3.6): train a weighted model from a
//! frozen dataset, register a Candidate, replay its exact frozen input, and
//! fit a calibrated child version — all without ever touching a live `BookStore`.
//!
//! Frozen Parquet is the sole source of the selected market, `FeatureCell`,
//! factor, and label bytes consumed by both training and backtest.

use std::{collections::BTreeMap, sync::Arc};

use chrono::{Duration as ChronoDuration, TimeZone, Utc};
use tokio_util::sync::CancellationToken;

use quant_pivot_core::{
    app::ports::cpcv_backtest::{CoreCpcvBacktestPort, CoreCpcvBacktestPortDeps},
    governance::CoreCalibrationArtifactLoader,
    service::{
        backtest::{BacktestInput, BacktestService, BacktestServiceDeps},
        historical_replay::ReplayConfig,
        model_training::{
            ModelTrainerConfig, ModelTrainerService, ModelTrainerServiceDeps, TrainModelInput,
        },
    },
};
use quant_pivot_models::{
    domain::{
        BacktestPathSetView, CompleteTrainingDatasetBuild, CpcvBacktestPort, DecisionClock,
        ModelVersionInfo, NewModelSpec, NewRuntimeConfigVersion, NewTrainingDatasetPlan,
        NoopProgressSink, RunCpcvBacktestRequest,
    },
    entities::{
        market::{Column as MarketColumn, Entity as MarketEntity},
        quant_model_run,
    },
    enums::{
        common::MarketCategory,
        factor::FactorFamily,
        model::ModelFamily,
        quant::{
            DataQualityStatus, DatasetPurpose, FactorDirection, ModelRunKind, ModelRunStatus,
            PublicationStatus, TrainingDatasetStatus,
        },
        runtime_config::RuntimeConfigVersionSource,
    },
    hashing::CanonicalDigest,
    runtime_config::{
        DataQualityConfig, DomainConfig, FactorWeights, FactorsConfig, FeatureFamily,
        FeaturesConfig, MomentumFeaturesConfig, PortfolioBudget, PortfolioConfig,
        PortfolioConstraints, RankLossKind, ResearchTrainingConfig, RuntimeConfig,
        StructuralFeaturesConfig, TrainingOptimizerKind, wire::DecimalString,
    },
    types::{
        BacktestPathSetId, ContentHash, DATASET_ARTIFACT_FORMAT_VERSION, DatasetCoverage,
        DatasetManifest, EventId, FactorDefinitionId, MarketId, ModelSpecId, ModelVersionId,
        Probability, RuntimeConfigVersionId, SchemaVersion, TokenId, TrainingDatasetId,
        TrainingExampleId, TrainingHorizonsSecs, TrainingSampleSource, TrainingSampleSources, Usd,
        default_sample_sources,
    },
};
use quant_pivot_repository::{
    postgres::{
        PgBacktestPathSetRepository, PgBacktestReportRepository, PgCalibrationArtifactRepository,
        PgEventRepository, PgMarketRepository, PgModelComparisonReportRepository,
        PgModelRegistryRepository, PgModelRunRepository, PgRuntimeConfigVersionRepository,
        PgTrainingDatasetRepository,
    },
    traits::{
        BacktestPathSetRepository, CalibrationArtifactRepository, EventRepository,
        MarketRepository, ModelRegistryRepository, RuntimeConfigVersionRepository,
        TrainingDatasetRepository,
    },
};
use quant_pivot_research::{
    artifact::{ArtifactKey, ArtifactNamespace, ArtifactStore, LocalArtifactStore},
    factors::{
        FactorEngine, FactorExplanation, FactorValue, NormalizedFactor, names::LIQUIDITY_DEPTH,
    },
    features::{
        FeatureCell, FeatureName, FeatureSchema, FeatureStaleness, FeatureValue, FeatureVector,
        names,
    },
    hashing::ResearchHasher,
    model::{
        CalibrationArtifactLoader, DefaultModelRuntimeFactoryBuilder, LabelSelector, ModelArtifact,
        ModelRuntimeFactoryBuilder, TrainingObjectiveSpec,
    },
    training::{
        DatasetHashContract, DatasetParquetCodec, LabelName, TrainingDatasetArtifact,
        TrainingExample, TrainingLabel, dataset_manifest_hash, dataset_source_fingerprint,
    },
    validation::PurgeConfig,
};
use quant_pivot_test_support::{
    catalog_fixtures::{make_event, make_market},
    pg::setup_pg,
};
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use sea_orm::{ColumnTrait, DatabaseConnection, EntityTrait, QueryFilter, sea_query::Expr};
use uuid::Uuid;

// Type alias to keep the BTreeMap key readable.
type FeatureName2 = FeatureName;

const EVENT_ID: &str = "evt-train-backtest-e2e";
const TICKS: i64 = 4;
const MARKETS_PER_TICK: usize = 20;
const BASE_TS: i64 = 1_700_000_000;
const TICK_INTERVAL_SECS: i64 = 3600;
const KNOWLEDGE_LAG_SECS: i64 = 10;

fn content_hash(seed: char) -> ContentHash {
    ContentHash::parse(format!("blake3:{}", seed.to_string().repeat(64))).expect("hash")
}

fn settlement() -> LabelName {
    LabelName::new("settlement_outcome")
}

fn market_id(tick: i64, i: usize) -> MarketId {
    MarketId::new(format!("0x{tick}_{i}"))
}

fn token_id(tick: i64, i: usize) -> TokenId {
    TokenId::new(format!("tok_{tick}_{i}"))
}

fn as_of_for(tick: i64) -> chrono::DateTime<Utc> {
    Utc.timestamp_opt(BASE_TS + tick * TICK_INTERVAL_SECS, 0)
        .single()
        .expect("fixture timestamp")
}

/// Cross-sectional liquidity (USD) for the `i`-th market in a tick — strictly
/// increasing in `i` so the frozen `liquidity_depth` rank spreads scores
/// across calibration buckets.
fn liquidity_usd(i: usize) -> Decimal {
    Decimal::from(1_000 * (i as u64 + 1))
}

fn feature_values(i: usize) -> BTreeMap<FeatureName2, FeatureCell> {
    let values = [
        (
            names::book::MID,
            FeatureValue::Probability(Probability::new(dec!(0.5))),
        ),
        (
            names::book::BEST_ASK,
            FeatureValue::Probability(Probability::new(dec!(0.51))),
        ),
        (
            names::book::VISIBLE_LIQUIDITY_USD,
            FeatureValue::Usd(Usd::new(liquidity_usd(i))),
        ),
        // Non-crypto category: this suite exercises frozen liquidity inputs,
        // not the crypto domain-weight publish invariant.
        (
            names::market::CATEGORY,
            FeatureValue::Category(MarketCategory::Politics),
        ),
    ];
    values
        .into_iter()
        .map(|(name, value)| {
            (
                name,
                FeatureCell::observed(value, None, FeatureStaleness::Unknown),
            )
        })
        .collect()
}

/// Frozen examples spanning deterministic cross-sections.
fn examples() -> Vec<TrainingExample> {
    let mut out = Vec::new();
    for tick in 0..TICKS {
        let as_of = as_of_for(tick);
        for i in 0..MARKETS_PER_TICK {
            let strength = Decimal::from(i as u64 % 9 + 1) / dec!(10); // 0.1 ..= 0.9
            let settled_yes = strength > dec!(0.5);
            let market = market_id(tick, i);
            let token = token_id(tick, i);
            let feature_vector = FeatureVector {
                market_id: market.clone(),
                token_id: Some(token.clone()),
                decision_at: as_of,
                generic_schema_version: SchemaVersion::FIRST,
                generic: feature_values(i),
                domain: None,
                data_quality: DataQualityStatus::Fresh,
            };
            let liquidity = FactorValue {
                definition_id: FactorDefinitionId::from_v7(),
                name: LIQUIDITY_DEPTH,
                family: FactorFamily::Liquidity,
                raw_value: Some(liquidity_usd(i)),
                normalization: NormalizedFactor::cross_section(Probability::new(strength)),
                direction: FactorDirection::Positive,
                confidence: Probability::new(dec!(1)),
                explanation: FactorExplanation {
                    headline: "liquidity".to_owned(),
                    drivers: Vec::new(),
                },
                input_feature_refs: Vec::new(),
            };
            out.push(TrainingExample {
                example_id: TrainingExampleId::from_v7(),
                market_id: market.clone(),
                token_id: token.clone(),
                selected_market: quant_pivot_research::selection::SelectedMarket {
                    market_id: market,
                    event_id: EventId::new(EVENT_ID),
                    category: MarketCategory::Politics,
                    primary_token_id: token,
                    secondary_token_id: None,
                    liquidity_usd: Some(Usd::new(liquidity_usd(i))),
                    volume_24h_usd: None,
                    source_refs: Vec::new(),
                },
                decision_boundary: DecisionClock::new(
                    u64::try_from(KNOWLEDGE_LAG_SECS).expect("knowledge lag"),
                )
                .boundary(as_of)
                .expect("boundary"),
                sample_source: TrainingSampleSource::HistoricalPit,
                feature_vector,
                factor_values: vec![liquidity],
                labels: vec![TrainingLabel {
                    label_name: settlement(),
                    horizon_secs: 0,
                    value: if settled_yes {
                        Decimal::ONE
                    } else {
                        Decimal::ZERO
                    },
                    is_resolved: true,
                    matured_at: as_of + ChronoDuration::seconds(1),
                }],
                source_refs: Vec::new(),
                decision_capture: None,
                lot_context: None,
                position_state: None,
                book_fidelity: None,
            });
        }
    }
    out
}

// ── Postgres catalog + ledger seeding ───────────────────────────────────────────────────────────────────────────────

/// Governed training knobs exercised through `TrainingObjectiveSpec::from_runtime_config`.
fn e2e_research_training() -> ResearchTrainingConfig {
    ResearchTrainingConfig {
        rank_loss: RankLossKind::RankIcWeightedRanknet,
        optimizer: TrainingOptimizerKind::CoordinateSearch,
        lambda_tail: DecimalString::new("0.5"),
        tail_fraction: DecimalString::new("0.10"),
        lambda_turnover: DecimalString::new("0.2"),
        lambda_l2: DecimalString::new("0.01"),
        ndcg_k: 5,
        pseudo_top_n: 3,
    }
}

/// Shared frozen training slice used by trainer and CPCV configuration.
struct E2eReplaySlice {
    features: FeaturesConfig,
    factors: FactorsConfig,
    domain: DomainConfig,
    data_quality: DataQualityConfig,
    max_book_staleness_ms: u64,
}

fn e2e_replay_slice() -> E2eReplaySlice {
    let mut weights = BTreeMap::new();
    weights.insert(LIQUIDITY_DEPTH.as_str().to_owned(), DecimalString::new("1"));
    // PriceBook + MarketMetadata only. Cap every lookback-driving window so
    // `features.max_lookback_secs()` (→ CPCV `min_embargo_secs`) stays well
    // below the 1h tick spacing — default 3600s micro / 86400s tape windows
    // embargo entire train partitions on this 4-tick timeline.
    E2eReplaySlice {
        features: FeaturesConfig {
            enabled_feature_families: vec![FeatureFamily::PriceBook, FeatureFamily::MarketMetadata],
            bar_windows_secs: vec![60],
            momentum: MomentumFeaturesConfig {
                roc_windows_secs: vec![120],
                roc_lag_secs: 60,
                ema_fast_secs: 30,
                ema_slow_secs: 60,
                slope_windows_secs: vec![60],
            },
            volatility_windows_secs: vec![60],
            structural: StructuralFeaturesConfig {
                shock_window_secs: 60,
                book_churn_window_secs: 60,
                trade_tape_window_secs: 60,
                ..StructuralFeaturesConfig::default()
            },
            ..FeaturesConfig::default()
        },
        factors: FactorsConfig {
            enabled_factor_families: vec![FactorFamily::Liquidity],
            factor_weights: FactorWeights { weights },
            ..FactorsConfig::default()
        },
        domain: DomainConfig::default(),
        // Frozen data-quality contract used to derive schema bindings.
        data_quality: DataQualityConfig {
            max_book_age_ms: 60_000,
            max_feature_bucket_age_secs: 120,
            ..DataQualityConfig::default()
        },
        max_book_staleness_ms: 60_000,
    }
}

fn e2e_runtime_config() -> RuntimeConfig {
    let mut config = RuntimeConfig::default();
    let replay = e2e_replay_slice();
    config.features = replay.features;
    config.factors = replay.factors;
    config.domain = replay.domain;
    config.data_quality = replay.data_quality;
    config.training.max_book_staleness_ms = replay.max_book_staleness_ms;
    config.research.training = e2e_research_training();
    // Dataset has 4 as_of groups — size CPCV/PBO so the port path can run
    // without failing the T < block_count preflight.
    config.research.validation.cpcv.n_groups = 4;
    config.research.validation.cpcv.k_test = 2;
    config.research.validation.pbo.block_count = 4;
    config.research.validation.trials.lambda_multipliers =
        vec![DecimalString::new("1"), DecimalString::new("0.5")];
    config.research.validation.trials.rank_loss_kinds = vec![RankLossKind::RankIcWeightedRanknet];
    config.research.validation.trials.max_trials = 4;
    config.research.validation.purge.embargo_pct = DecimalString::new("0.02");
    config
}

async fn seed_runtime_config(db: &DatabaseConnection) -> RuntimeConfigVersionId {
    let config = e2e_runtime_config();
    let id = RuntimeConfigVersionId::from_v7();
    PgRuntimeConfigVersionRepository::new(db.clone())
        .create_version(NewRuntimeConfigVersion {
            runtime_config_version_id: id.clone(),
            config_hash: content_hash('c'),
            schema_version: config.schema_version,
            config_json: config.to_json(),
            source: RuntimeConfigVersionSource::Bootstrap,
            created_by: "train-backtest-e2e".to_owned(),
            reason: "integration test".to_owned(),
        })
        .await
        .expect("runtime config");
    id
}

async fn seed_model_spec(db: &DatabaseConnection) -> ModelSpecId {
    let id = ModelSpecId::from_v7();
    PgModelRegistryRepository::new(db.clone())
        .create_model_spec(NewModelSpec {
            model_spec_id: id.clone(),
            name: "train-backtest-e2e".to_owned(),
            model_family: ModelFamily::WeightedFactor,
            prediction_horizon_secs: 86_400,
            feature_schema_version: SchemaVersion::FIRST,
            label_schema_version: SchemaVersion::FIRST,
            spec_json: serde_json::json!({}),
            input_contract: quant_pivot_models::types::ModelInputContract::single_required(
                "book.mid",
            ),
            training_contract: quant_pivot_models::types::ModelTrainingContract::settlement_default(
            ),
            status: PublicationStatus::Published,
        })
        .await
        .expect("model spec");
    id
}

/// Seed the event + every schedule market into the Postgres catalog so the
/// replay window loader can resolve each tick's market metadata.
async fn seed_catalog(db: &DatabaseConnection) {
    PgEventRepository::new(db.clone())
        .upsert(make_event(
            EVENT_ID,
            "Train/Backtest E2E",
            "train-backtest-e2e",
            MarketCategory::Politics,
        ))
        .await
        .expect("seed event");

    let market_repo = PgMarketRepository::new(db.clone());
    let created_at = as_of_for(0) - ChronoDuration::days(1);
    let end_date = as_of_for(TICKS - 1) + ChronoDuration::days(7);
    for tick in 0..TICKS {
        for i in 0..MARKETS_PER_TICK {
            let mid = market_id(tick, i);
            let mut market = make_market(
                mid.as_str(),
                EVENT_ID,
                "Train/Backtest E2E?",
                &format!("tb-{tick}-{i}"),
                MarketCategory::Politics,
                Some(end_date),
            );
            market.yes_token_id = token_id(tick, i);
            market.no_token_id = TokenId::new(format!("tok_no_{tick}_{i}"));
            market_repo.upsert(market).await.expect("seed market");
            MarketEntity::update_many()
                .col_expr(MarketColumn::CreatedAt, Expr::value(created_at))
                .filter(MarketColumn::MarketId.eq(mid.as_str()))
                .exec(db)
                .await
                .expect("backdate created_at");
        }
    }
}

async fn seed_dataset(
    db: &DatabaseConnection,
    store: &Arc<dyn ArtifactStore>,
    model_spec_id: &ModelSpecId,
    rc_id: &RuntimeConfigVersionId,
) -> (TrainingDatasetId, ContentHash) {
    let dataset_id = TrainingDatasetId::from_v7();
    let examples = examples();
    let window_start = as_of_for(0);
    let window_end = window_start + ChronoDuration::hours(4);
    let replay = replay_config();
    let feature_schema = FeatureSchema::build(&replay.features).expect("feature schema");
    let feature_schema_hash =
        ResearchHasher::feature_schema(&feature_schema).expect("feature hash");
    let factor_schema_hash =
        FactorEngine::new(&replay.factors, &replay.features, &replay.domain, None)
            .factor_schema_hash()
            .expect("factor hash");
    let label_schema_hash =
        ResearchHasher::label_schema(&[settlement()]).expect("label schema hash");
    let dataset_hash = TrainingDatasetArtifact::compute_dataset_hash(
        DatasetHashContract {
            model_spec_id,
            window_start,
            window_end,
            purpose: DatasetPurpose::Training,
            feature_schema_hash: &feature_schema_hash,
            factor_schema_hash: &factor_schema_hash,
            label_schema_hash: &label_schema_hash,
        },
        &examples,
    )
    .expect("semantic dataset hash");
    let manifest = DatasetManifest {
        format_version: DATASET_ARTIFACT_FORMAT_VERSION,
        training_dataset_id: dataset_id.clone(),
        model_spec_id: model_spec_id.clone(),
        runtime_config_version_id: rc_id.clone(),
        window_start,
        window_end,
        purpose: DatasetPurpose::Training,
        knowledge_lag_secs: u64::try_from(KNOWLEDGE_LAG_SECS).expect("knowledge lag"),
        sample_interval_secs: u64::try_from(TICK_INTERVAL_SECS).expect("sample interval"),
        horizons_secs: vec![0],
        feature_schema_hash: feature_schema_hash.clone(),
        factor_schema_hash: factor_schema_hash.clone(),
        label_schema_hash: label_schema_hash.clone(),
        semantic_dataset_hash: dataset_hash.clone(),
        source_fingerprint: dataset_source_fingerprint(&examples).expect("source fingerprint"),
        sample_count: u64::try_from(examples.len()).expect("sample count"),
    };
    let bytes = DatasetParquetCodec::encode(&examples, &manifest).expect("encode parquet");
    let manifest_hash = dataset_manifest_hash(&manifest).expect("manifest hash");
    let artifact_bytes_hash =
        ContentHash::parse(CanonicalDigest::prefixed_bytes(&bytes)).expect("bytes hash");
    let hex = dataset_id.as_uuid().simple().to_string();
    let key = ArtifactKey::new(ArtifactNamespace::Dataset, hex, "parquet").expect("key");
    let uri = store.put(key, &bytes).await.expect("store parquet");

    let dataset_repo = PgTrainingDatasetRepository::new(db.clone());
    dataset_repo
        .create_plan(NewTrainingDatasetPlan {
            training_dataset_id: dataset_id.clone(),
            model_spec_id: model_spec_id.clone(),
            window_start,
            window_end,
            purpose: DatasetPurpose::Training,
            knowledge_lag_secs: KNOWLEDGE_LAG_SECS,
            sample_interval_secs: TICK_INTERVAL_SECS,
            horizons_secs: TrainingHorizonsSecs(vec![0]),
            feature_schema_version: Some(SchemaVersion::FIRST),
            sample_sources: Some(TrainingSampleSources(default_sample_sources())),
            runtime_config_version_id: rc_id.clone(),
        })
        .await
        .expect("dataset plan");
    dataset_repo
        .start_build(&dataset_id)
        .await
        .expect("start dataset");
    dataset_repo
        .complete_build(
            &dataset_id,
            CompleteTrainingDatasetBuild {
                status: TrainingDatasetStatus::Ready,
                feature_schema_hash,
                factor_schema_hash,
                label_schema_hash,
                dataset_hash: dataset_hash.clone(),
                manifest_hash,
                manifest_json: manifest,
                artifact_bytes_hash,
                parquet_uri: uri,
                sample_count: TICKS * i64::try_from(MARKETS_PER_TICK).expect("count"),
                coverage_json: DatasetCoverage::default(),
                failure_detail: None,
            },
        )
        .await
        .expect("dataset ledger");
    (dataset_id, dataset_hash)
}

fn trainer_config() -> ModelTrainerConfig {
    let runtime = e2e_runtime_config();
    let replay = e2e_replay_slice();
    ModelTrainerConfig {
        factors: replay.factors,
        // Same path as the production port: parse frozen `research.training`.
        objective: TrainingObjectiveSpec::from_runtime_config(&runtime.research.training)
            .expect("parse research.training"),
        validation_purge: PurgeConfig {
            embargo_pct: dec!(0.02),
            min_embargo_secs: runtime.features.max_lookback_secs(),
        },
    }
}

fn portfolio() -> PortfolioConfig {
    PortfolioConfig {
        budget: PortfolioBudget {
            total_budget_usd: DecimalString::new("1000"),
            min_recommendation_usd: DecimalString::new("10"),
            max_single_recommendation_usd: DecimalString::new("200"),
        },
        constraints: PortfolioConstraints {
            liquidity_usage_cap_pct: DecimalString::new("0.5"),
            ..PortfolioConstraints::default()
        },
        ..PortfolioConfig::default()
    }
}

/// Replay config governing the PIT recompute — identical slice to the frozen
/// runtime JSON [`e2e_runtime_config`] embeds for the CPCV port.
fn replay_config() -> ReplayConfig {
    let replay = e2e_replay_slice();
    ReplayConfig {
        features: replay.features,
        factors: replay.factors,
        domain: replay.domain,
        data_quality: replay.data_quality,
        bias_table: None,
    }
}

/// Assert the training `quant_model_run` row was finalized with version FK + artifact hash.
async fn assert_training_run_ledger(
    db: &DatabaseConnection,
    version: &ModelVersionInfo,
    dataset_hash: ContentHash,
) {
    let training_run = quant_model_run::Entity::find()
        .filter(quant_model_run::Column::RunKind.eq(ModelRunKind::Training))
        .filter(quant_model_run::Column::Status.eq(ModelRunStatus::Succeeded))
        .one(db)
        .await
        .expect("query training run")
        .expect("training run row");
    assert_eq!(
        training_run.model_version_id.as_ref(),
        Some(&version.model_version_id),
        "succeed backfills model_version_id after version registration"
    );
    assert_eq!(
        training_run.output_hash.as_ref(),
        Some(&version.artifact_hash),
        "output_hash links run to registered artifact"
    );
    assert_eq!(training_run.input_hash, dataset_hash, "dataset provenance");
}

fn weighted_train_input(
    model_spec_id: ModelSpecId,
    dataset_id: TrainingDatasetId,
    rc_id: RuntimeConfigVersionId,
) -> TrainModelInput {
    TrainModelInput {
        model_version_id: ModelVersionId::from_v7(),
        model_spec_id,
        training_dataset_id: dataset_id,
        runtime_config_version_id: rc_id,
        model_family: ModelFamily::WeightedFactor,
        input_contract: quant_pivot_models::types::ModelInputContract::single_required("book.mid"),
        label: LabelSelector {
            name: settlement(),
            horizon_secs: 0,
        },
        prediction_horizon_secs: 86_400,
        validation_folds: 3,
        selection_enabled_categories: vec![],
        category_scope: None,
    }
}

fn assert_weighted_train_metrics(version: &ModelVersionInfo) {
    assert_eq!(version.publication_status, PublicationStatus::Candidate);
    assert_eq!(
        version.training_objective_json["rank_loss"],
        serde_json::json!("rank_ic_weighted_ranknet")
    );
    assert_eq!(
        version.training_objective_json["optimizer"],
        serde_json::json!("coordinate_search")
    );
    assert_eq!(
        version.training_objective_json["ndcg_k"],
        serde_json::json!(5)
    );
    assert_eq!(
        version.training_objective_json["pseudo_top_n"],
        serde_json::json!(3)
    );
    assert_eq!(
        version.metrics_json["validation"]["held_out_metric"],
        serde_json::json!("neg_total_ltr_loss")
    );
    assert!(
        version.metrics_json["validation"]["dropped_singleton_groups"].is_number(),
        "dropped_singleton_groups must be present: {}",
        version.metrics_json
    );
    assert!(
        version.metrics_json["in_sample"]["diagnostics"]["mean_rank_ic"].is_string()
            || version.metrics_json["in_sample"]["diagnostics"]["mean_rank_ic"].is_number(),
        "in-sample Rank IC diagnostic must be present: {}",
        version.metrics_json
    );
    assert_eq!(
        version.metrics_json["in_sample"]["diagnostics"]["ndcg_k"],
        serde_json::json!(5),
        "diagnostics must honor runtime-config ndcg_k"
    );
    assert!(
        version.metrics_json["validation"]["held_out_diagnostics"]["mean_ndcg_at_k"].is_string()
            || version.metrics_json["validation"]["held_out_diagnostics"]["mean_ndcg_at_k"]
                .is_number(),
        "held-out NDCG diagnostic must be present: {}",
        version.metrics_json
    );
    assert!(
        version.metrics_json["validation"]["held_out_objective"].is_string()
            || version.metrics_json["validation"]["held_out_objective"].is_number(),
        "held_out_objective must be present: {}",
        version.metrics_json
    );
}

async fn assert_artifact_politics_scope(
    store: &Arc<dyn ArtifactStore>,
    version: &ModelVersionInfo,
) {
    let bytes = store
        .get_by_key(&ModelArtifact::artifact_key(&version.artifact_hash).expect("key"))
        .await
        .expect("artifact bytes");
    let artifact = ModelArtifact::from_bytes(&bytes).expect("decode");
    assert_eq!(
        artifact.content_hash().expect("hash"),
        version.artifact_hash,
        "artifact weights are frozen + content-addressed"
    );
    assert_eq!(
        artifact.category_scope(),
        Some(MarketCategory::Politics),
        "unanimous Politics examples freeze Politics scope (not Crypto domain weights)"
    );
}

fn make_trainer(
    db: &DatabaseConnection,
    store: Arc<dyn ArtifactStore>,
    registry: Arc<dyn ModelRegistryRepository>,
) -> ModelTrainerService {
    ModelTrainerService::new(
        ModelTrainerServiceDeps {
            dataset_repo: Arc::new(PgTrainingDatasetRepository::new(db.clone())),
            artifact_store: store,
            model_registry_repo: registry,
            model_run_repo: Arc::new(PgModelRunRepository::new(db.clone())),
        },
        trainer_config(),
        replay_config(),
    )
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires Docker"]
async fn train_then_backtest_then_calibrate_e2e() {
    let (pool, _container) = setup_pg().await;
    let db = pool.connection().clone();
    let store: Arc<dyn ArtifactStore> = Arc::new(LocalArtifactStore::new(
        std::env::temp_dir().join(format!("qp_tb_e2e_{}", Uuid::new_v4().simple())),
    ));

    let rc_id = seed_runtime_config(&db).await;
    let model_spec_id = seed_model_spec(&db).await;
    seed_catalog(&db).await;
    let (dataset_id, dataset_hash) = seed_dataset(&db, &store, &model_spec_id, &rc_id).await;

    let registry: Arc<dyn ModelRegistryRepository> =
        Arc::new(PgModelRegistryRepository::new(db.clone()));
    let trainer = make_trainer(&db, Arc::clone(&store), Arc::clone(&registry));

    let outcome = trainer
        .train(
            weighted_train_input(model_spec_id.clone(), dataset_id.clone(), rc_id.clone()),
            &NoopProgressSink,
            &CancellationToken::new(),
        )
        .await
        .expect("train");
    let version = outcome.version;
    assert_eq!(version.training_dataset_id.as_ref(), Some(&dataset_id));
    assert_weighted_train_metrics(&version);
    assert_artifact_politics_scope(&store, &version).await;
    assert_training_run_ledger(&db, &version, dataset_hash).await;

    let calibration_loader: Arc<dyn CalibrationArtifactLoader> = Arc::new(
        CoreCalibrationArtifactLoader::new(Arc::new(PgCalibrationArtifactRepository::new(
            db.clone(),
        )) as Arc<dyn CalibrationArtifactRepository>),
    );
    let factory_builder: Arc<dyn ModelRuntimeFactoryBuilder> = Arc::new(
        DefaultModelRuntimeFactoryBuilder::new(Arc::clone(&store), calibration_loader),
    );
    let backtester = BacktestService::new(
        BacktestServiceDeps {
            dataset_repo: Arc::new(PgTrainingDatasetRepository::new(db.clone())),
            artifact_store: Arc::clone(&store),
            model_registry_repo: Arc::clone(&registry),
            model_run_repo: Arc::new(PgModelRunRepository::new(db.clone())),
            backtest_report_repo: Arc::new(PgBacktestReportRepository::new(db.clone())),
            comparison_report_repo: Arc::new(PgModelComparisonReportRepository::new(db.clone())),
            factory_builder,
        },
        &portfolio(),
        None,
    )
    .expect("backtest service");

    let report = backtester
        .run(
            BacktestInput {
                model_version_id: version.model_version_id.clone(),
                training_dataset_id: dataset_id.clone(),
                runtime_config_version_id: rc_id.clone(),
                calibrate: true,
                backtest_report_id: None,
            },
            Arc::new(NoopProgressSink),
            CancellationToken::new(),
        )
        .await
        .expect("backtest");
    assert!(report.sample_count > 0, "resolved samples");
    assert_eq!(report.model_version_id, version.model_version_id);
    assert!(
        report.report_hash.as_str().starts_with("blake3:"),
        "report hash persisted"
    );

    let next = registry
        .next_version_for_spec(&model_spec_id)
        .await
        .expect("count");
    assert!(
        next >= 3,
        "calibration should have registered a calibrated child version (next={next})"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires Docker"]
async fn train_then_cpcv_persists_path_set_with_dsr_n_decomposition() {
    let (pool, _container) = setup_pg().await;
    let db = pool.connection().clone();
    let store: Arc<dyn ArtifactStore> = Arc::new(LocalArtifactStore::new(
        std::env::temp_dir().join(format!("qp_cpcv_e2e_{}", Uuid::new_v4().simple())),
    ));

    let rc_id = seed_runtime_config(&db).await;
    let model_spec_id = seed_model_spec(&db).await;
    seed_catalog(&db).await;
    let (dataset_id, _dataset_hash) = seed_dataset(&db, &store, &model_spec_id, &rc_id).await;

    let registry: Arc<dyn ModelRegistryRepository> =
        Arc::new(PgModelRegistryRepository::new(db.clone()));
    let trainer = make_trainer(&db, Arc::clone(&store), Arc::clone(&registry));

    let outcome = trainer
        .train(
            weighted_train_input(model_spec_id.clone(), dataset_id.clone(), rc_id.clone()),
            &NoopProgressSink,
            &CancellationToken::new(),
        )
        .await
        .expect("train");
    let version = outcome.version;
    let coord_n = version
        .metrics_json
        .pointer("/validation/coord_search_effective_n")
        .and_then(serde_json::Value::as_u64)
        .unwrap_or(0);
    assert!(
        coord_n >= 1,
        "trainer must persist coord_search_effective_n ≥ 1, got {coord_n}"
    );

    let path_set_id = BacktestPathSetId::from_v7();
    let port = CoreCpcvBacktestPort::new(CoreCpcvBacktestPortDeps {
        dataset_repo: Arc::new(PgTrainingDatasetRepository::new(db.clone())),
        artifact_store: Arc::clone(&store),
        path_set_repo: Arc::new(PgBacktestPathSetRepository::new(db.clone())),
        model_registry_repo: Arc::clone(&registry),
        model_run_repo: Arc::new(PgModelRunRepository::new(db.clone())),
        runtime_config: Arc::new(PgRuntimeConfigVersionRepository::new(db.clone())),
        bias_table_repo: Arc::new(PgCalibrationArtifactRepository::new(db.clone())),
    });
    let view = port
        .run(
            version.model_version_id.clone(),
            RunCpcvBacktestRequest {
                training_dataset_id: dataset_id,
                runtime_config_version_id: rc_id,
                reason: "train-then-cpcv e2e".to_owned(),
                path_set_id: Some(path_set_id.clone()),
            },
            Arc::new(NoopProgressSink),
            CancellationToken::new(),
        )
        .await
        .expect("cpcv port");

    assert_cpcv_view_and_bind(&db, &registry, &version, &view, &path_set_id, coord_n).await;
}

async fn assert_cpcv_view_and_bind(
    db: &DatabaseConnection,
    registry: &Arc<dyn ModelRegistryRepository>,
    version: &ModelVersionInfo,
    view: &BacktestPathSetView,
    path_set_id: &BacktestPathSetId,
    coord_n: u64,
) {
    assert_eq!(view.path_set_id, *path_set_id);
    assert_eq!(
        view.trial_count, view.trial_grid_count,
        "DSR N must equal the governed trial-grid count (same population as V)"
    );
    assert_eq!(
        view.coord_search_effective_n,
        i64::try_from(coord_n).unwrap_or(0),
        "coord_search_effective_n is audit-only and must still be persisted"
    );
    assert_eq!(view.path_count, 3);
    assert_eq!(view.combination_count, 6);
    assert!(
        !view.path_set_hash.as_str().is_empty(),
        "port must persist a content hash"
    );

    let cpcv_run = quant_model_run::Entity::find()
        .filter(quant_model_run::Column::RunKind.eq(ModelRunKind::Cpcv))
        .filter(quant_model_run::Column::Status.eq(ModelRunStatus::Succeeded))
        .one(db)
        .await
        .expect("query cpcv run")
        .expect("port must create ModelRunKind::Cpcv");
    assert_eq!(cpcv_run.model_run_id, view.model_run_id);

    let bound = registry
        .find_model_version_by_id(&version.model_version_id)
        .await
        .expect("reload version")
        .expect("version");
    assert!(
        bound.publish_path_set_id.is_none(),
        "CPCV must not auto-bind publish_path_set_id; explicit governance bind required"
    );

    registry
        .set_publish_path_set_id(&version.model_version_id, Some(path_set_id.clone()))
        .await
        .expect("explicit bind for publish gate");

    let bound = registry
        .find_model_version_by_id(&version.model_version_id)
        .await
        .expect("reload after bind")
        .expect("version");
    assert_eq!(
        bound.publish_path_set_id.as_ref(),
        Some(path_set_id),
        "explicit bind must pin publish_path_set_id"
    );

    let listed = PgBacktestPathSetRepository::new(db.clone())
        .list_by_model_version(&version.model_version_id)
        .await
        .expect("list");
    assert_eq!(listed.len(), 1);
}
