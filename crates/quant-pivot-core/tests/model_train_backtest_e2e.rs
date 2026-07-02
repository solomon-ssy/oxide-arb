//! End-to-end trainer + backtest (Phase 3.6): train a weighted model from a
//! frozen dataset, register a Candidate, replay a **point-in-time** backtest, and
//! fit a calibrated child version — all without ever touching a live `BookStore`.
//!
//! Frozen Parquet supplies only the replay **schedule** (`(as_of, market, token)`)
//! and forward **label truth**; both training and backtest **recompute** features
//! and factors point-in-time through the shared [`materialize_cross_section`]
//! kernel from prefetched facts. This test seeds an in-memory
//! [`QuantFactReadRepository`] (never a `BookStore`) plus a Postgres market catalog
//! so train and backtest share the same PIT-resolved `liquidity_depth` cross-section.

use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
    time::Duration,
};

use async_trait::async_trait;
use chrono::{Duration as ChronoDuration, TimeZone, Utc};
use quant_pivot_core::service::{
    backtest::{BacktestInput, BacktestService, BacktestServiceDeps},
    historical_replay::ReplayConfig,
    model_training::{
        ModelTrainerConfig, ModelTrainerService, ModelTrainerServiceDeps, TrainModelInput,
    },
};
use quant_pivot_error::storage::StorageError;
use quant_pivot_models::{
    clickhouse::{
        BookMicrostructureRow, BookSnapshotRow, ChPrice, ChSchemaVersion, ChUsd,
        MarketResolutionRow, MidPriceBucketRow, TickEventRow,
    },
    domain::{ModelVersionInfo, NewModelSpec, NewRuntimeConfigVersion, NewTrainingDataset},
    entities::{
        market::{Column as MarketColumn, Entity as MarketEntity},
        quant_model_run,
    },
    enums::{
        clickhouse::{ChFactSource, ChSnapshotReason},
        common::MarketCategory,
        factor::FactorFamily,
        model::ModelFamily,
        quant::{
            DataQualityStatus, FactorDirection, ModelRunKind, ModelRunStatus, PublicationStatus,
            TrainingDatasetStatus,
        },
        runtime_config::RuntimeConfigVersionSource,
    },
    runtime_config::{
        DataQualityConfig, FactorWeights, FactorsConfig, FeatureFamily, FeaturesConfig,
        ModelConfig, PortfolioBudget, PortfolioConfig, PortfolioConstraints, wire::DecimalString,
    },
    types::{
        ContentHash, FactorDefinitionId, MarketId, ModelSpecId, Price, Probability,
        RuntimeConfigVersionId, SchemaVersion, TokenId, TrainingDatasetId, TrainingExampleId,
        TrainingSampleSource, Usd,
    },
};
use quant_pivot_repository::{
    postgres::{
        PgBacktestReportRepository, PgEventRepository, PgMarketRepository,
        PgModelComparisonReportRepository, PgModelRegistryRepository, PgModelRunRepository,
        PgRuntimeConfigVersionRepository, PgTrainingDatasetRepository,
    },
    traits::{
        EventRepository, MarketRepository, ModelRegistryRepository, QuantFactReadRepository,
        RuntimeConfigVersionRepository, TrainingDatasetRepository,
    },
};
use quant_pivot_research::{
    artifact::{ArtifactKey, ArtifactNamespace, ArtifactStore, LocalArtifactStore},
    factors::{FactorExplanation, FactorValue, names::LIQUIDITY_DEPTH},
    features::{FeatureName, FeatureValue, FeatureVector, names},
    model::{
        DefaultModelRuntimeFactoryBuilder, LabelSelector, ModelArtifact, ModelRuntimeFactoryBuilder,
    },
    training::{DatasetParquetCodec, LabelName, TrainingExample, TrainingLabel},
};
use quant_pivot_test_support::{
    catalog_fixtures::{make_event, make_market},
    pg::setup_pg,
};
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use sea_orm::{ColumnTrait, DatabaseConnection, EntityTrait, QueryFilter, sea_query::Expr};
use std::collections::BTreeMap;
use uuid::Uuid;

// Type alias to keep the BTreeMap key readable.
type FeatureName2 = FeatureName;

const EVENT_ID: &str = "evt-train-backtest-e2e";
const TICKS: i64 = 3;
const MARKETS_PER_TICK: usize = 20;
const BASE_TS: i64 = 1_700_000_000;
const TICK_INTERVAL_SECS: i64 = 3600;
const SOURCE_DELAY_SECS: i64 = 10;

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
        .unwrap()
}

/// Cross-sectional liquidity (USD) for the `i`-th market in a tick — strictly
/// increasing in `i` so the recomputed `liquidity_depth` rank spreads scores
/// across calibration buckets.
fn liquidity_usd(i: usize) -> Decimal {
    Decimal::from(1_000 * (i as u64 + 1))
}

/// `60` examples across `3` ticks. Parquet carries only schedule + settlement
/// labels; stored factor/feature columns are intentionally stale decoys — train
/// and backtest both rematerialize from the shared in-memory fact source.
fn examples() -> Vec<TrainingExample> {
    let mut out = Vec::new();
    for tick in 0..TICKS {
        let as_of = as_of_for(tick);
        for i in 0..MARKETS_PER_TICK {
            let strength = Decimal::from(i as u64 % 9 + 1) / dec!(10); // 0.1 ..= 0.9
            let settled_yes = strength > dec!(0.5);
            let market = market_id(tick, i);
            let token = token_id(tick, i);
            let mut values: BTreeMap<FeatureName2, FeatureValue> = BTreeMap::new();
            values.insert(names::book::MID.clone(), FeatureValue::Decimal(dec!(0.5)));
            values.insert(
                names::book::VISIBLE_LIQUIDITY_USD.clone(),
                FeatureValue::Usd(Usd::new(liquidity_usd(i))),
            );
            values.insert(
                names::market::CATEGORY.clone(),
                FeatureValue::Category(MarketCategory::Crypto),
            );
            let feature_vector = FeatureVector {
                market_id: market.clone(),
                token_id: Some(token.clone()),
                as_of,
                schema_version: SchemaVersion::FIRST,
                values,
                substitutions: Vec::new(),
                data_quality: DataQualityStatus::Fresh,
                staleness_ms: 0,
                source_refs: Vec::new(),
            };
            let liquidity = FactorValue {
                definition_id: FactorDefinitionId::from_v7(),
                name: LIQUIDITY_DEPTH,
                family: FactorFamily::Liquidity,
                raw_value: Some(liquidity_usd(i)),
                normalized_score: Probability::new(strength),
                direction: FactorDirection::Positive,
                confidence: Probability::new(dec!(1)),
                explanation: FactorExplanation {
                    headline: "liquidity".to_owned(),
                    drivers: Vec::new(),
                    clamp: None,
                },
                input_feature_refs: Vec::new(),
            };
            out.push(TrainingExample {
                example_id: TrainingExampleId::from_v7(),
                market_id: market,
                token_id: token,
                as_of,
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
                }],
                source_refs: Vec::new(),
                lot_context: None,
                position_state: None,
                book_fidelity: None,
            });
        }
    }
    out
}

// ── In-memory fact source (never a live BookStore) ───────────────────────────

#[derive(Default)]
struct FactScenario {
    books: HashMap<TokenId, Vec<BookSnapshotRow>>,
    micro: HashMap<TokenId, Vec<BookMicrostructureRow>>,
    resolutions: HashMap<MarketId, Vec<MarketResolutionRow>>,
}

struct ControllableFactRead {
    scenario: Arc<Mutex<FactScenario>>,
}

#[async_trait]
impl QuantFactReadRepository for ControllableFactRead {
    async fn microstructure_window(
        &self,
        token_ids: Vec<TokenId>,
        from_ms: i64,
        to_ms: i64,
    ) -> Result<Vec<BookMicrostructureRow>, StorageError> {
        let scenario = self.scenario.lock().expect("lock");
        let mut rows = Vec::new();
        for token_id in token_ids {
            if let Some(series) = scenario.micro.get(&token_id) {
                for row in series {
                    if row.bucket_time >= from_ms && row.bucket_time < to_ms {
                        rows.push(row.clone());
                    }
                }
            }
        }
        Ok(rows)
    }

    async fn microstructure_series(
        &self,
        _token_ids: Vec<TokenId>,
        _from_ms: i64,
        _to_ms: i64,
        _minute: bool,
    ) -> Result<Vec<BookMicrostructureRow>, StorageError> {
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

    async fn book_snapshot_at(
        &self,
        token_id: &TokenId,
        as_of_ms: i64,
    ) -> Result<Option<BookSnapshotRow>, StorageError> {
        let scenario = self.scenario.lock().expect("lock");
        Ok(scenario.books.get(token_id).and_then(|rows| {
            rows.iter()
                .filter(|row| row.event_time <= as_of_ms)
                .max_by_key(|row| (row.event_time, row.ingestion_time, row.sequence))
                .cloned()
        }))
    }

    async fn book_snapshots_between(
        &self,
        token_ids: Vec<TokenId>,
        from_ms: i64,
        to_ms: i64,
    ) -> Result<Vec<BookSnapshotRow>, StorageError> {
        let scenario = self.scenario.lock().expect("lock");
        let mut rows = Vec::new();
        for token_id in token_ids {
            if let Some(series) = scenario.books.get(&token_id) {
                for row in series {
                    if row.event_time >= from_ms && row.event_time <= to_ms {
                        rows.push(row.clone());
                    }
                }
            }
        }
        Ok(rows)
    }

    async fn resolution_at(
        &self,
        market_id: &MarketId,
        as_of_ms: i64,
    ) -> Result<Option<MarketResolutionRow>, StorageError> {
        let scenario = self.scenario.lock().expect("lock");
        Ok(scenario.resolutions.get(market_id).and_then(|rows| {
            rows.iter()
                .filter(|row| row.resolved_at <= as_of_ms)
                .max_by_key(|row| (row.resolved_at, row.observed_at, row.sequence))
                .cloned()
        }))
    }

    async fn resolutions_between(
        &self,
        market_ids: Vec<MarketId>,
        from_ms: i64,
        to_ms: i64,
    ) -> Result<Vec<MarketResolutionRow>, StorageError> {
        let scenario = self.scenario.lock().expect("lock");
        let mut rows = Vec::new();
        for market_id in market_ids {
            if let Some(series) = scenario.resolutions.get(&market_id) {
                for row in series {
                    if row.resolved_at >= from_ms && row.resolved_at <= to_ms {
                        rows.push(row.clone());
                    }
                }
            }
        }
        Ok(rows)
    }

    async fn mid_price_series(
        &self,
        _token_ids: Vec<TokenId>,
        _from_ms: i64,
        _to_ms: i64,
        _bucket_secs: u32,
    ) -> Result<Vec<MidPriceBucketRow>, StorageError> {
        Ok(Vec::new())
    }
}

/// A top-of-book snapshot whose visible depth scales with `liquidity` so the
/// recomputed `liquidity_depth` factor varies across the cross-section.
fn book_row(
    token: &TokenId,
    market: &MarketId,
    event_time_ms: i64,
    liquidity: Decimal,
) -> BookSnapshotRow {
    // size such that 0.48*size + 0.52*size == liquidity (price-weighted USD depth).
    let size = liquidity; // 1.0 * size across the two sides ≈ liquidity
    BookSnapshotRow {
        token_id: token.clone(),
        market_id: Some(market.clone()),
        snapshot_reason: ChSnapshotReason::Startup,
        top_n: 5,
        bids_json: format!(r#"[["0.48","{size}"]]"#),
        asks_json: format!(r#"[["0.52","{size}"]]"#),
        bid_depth_usd: None,
        ask_depth_usd: None,
        mid_price: Some(ChPrice::from(Price::new(dec!(0.5)))),
        spread_bps: None,
        book_version: 1,
        levels_count: 1,
        event_time: event_time_ms,
        ingestion_time: event_time_ms,
        sequence: 1,
        source: ChFactSource::WsSnapshot,
        schema_version: ChSchemaVersion(2),
    }
}

fn micro_row(token: &TokenId, market: &MarketId, bucket_time_ms: i64) -> BookMicrostructureRow {
    let price = Price::new(dec!(0.5));
    BookMicrostructureRow {
        token_id: token.clone(),
        market_id: Some(market.clone()),
        bucket_time: bucket_time_ms,
        best_bid_open: None,
        best_bid_high: Some(ChPrice::from(price)),
        best_bid_low: Some(ChPrice::from(price)),
        best_bid_close: Some(ChPrice::from(price)),
        best_ask_open: None,
        best_ask_high: None,
        best_ask_low: None,
        best_ask_close: None,
        spread_bps_min: None,
        spread_bps_avg: None,
        spread_bps_max: None,
        mid_price_open: Some(ChPrice::from(price)),
        mid_price_close: Some(ChPrice::from(price)),
        top1_depth_usd_avg: Some(ChUsd::from(Decimal::from(500))),
        top5_depth_usd_avg: None,
        top20_depth_usd_avg: None,
        imbalance_avg: None,
        update_count: 1,
        snapshot_count: 1,
        delta_count: 0,
        delete_count: 0,
        crossed_count: 0,
        invalid_level_count: 0,
        gap_count: 0,
        last_trade_count: 0,
        max_book_age_ms: 0,
        schema_version: ChSchemaVersion(2),
    }
}

/// Build the in-memory facts backing every `(as_of, market, token)` in the
/// schedule: one book + one micro bucket per token, observed `15s` before the
/// tick (older than the `10s` source delay, younger than the relaxed book-age
/// bound), so the PIT replay resolves a Fresh cross-section without leakage.
fn fact_scenario() -> FactScenario {
    let mut scenario = FactScenario::default();
    for tick in 0..TICKS {
        let as_of_ms = as_of_for(tick).timestamp_millis();
        let evidence_ms = as_of_ms - 15_000;
        for i in 0..MARKETS_PER_TICK {
            let token = token_id(tick, i);
            let market = market_id(tick, i);
            scenario.books.insert(
                token.clone(),
                vec![book_row(&token, &market, evidence_ms, liquidity_usd(i))],
            );
            scenario
                .micro
                .insert(token.clone(), vec![micro_row(&token, &market, evidence_ms)]);
        }
    }
    scenario
}

// ── Postgres catalog + ledger seeding ────────────────────────────────────────

async fn seed_runtime_config(db: &DatabaseConnection) -> RuntimeConfigVersionId {
    let id = RuntimeConfigVersionId::from_v7();
    PgRuntimeConfigVersionRepository::new(db.clone())
        .create_version(NewRuntimeConfigVersion {
            runtime_config_version_id: id.clone(),
            config_hash: content_hash('c'),
            schema_version: SchemaVersion::FIRST,
            config_json: serde_json::json!({}),
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
            MarketCategory::Crypto,
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
                MarketCategory::Crypto,
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
) -> TrainingDatasetId {
    let bytes = DatasetParquetCodec::encode(&examples()).expect("encode parquet");
    let dataset_id = TrainingDatasetId::from_v7();
    let hex = dataset_id.as_uuid().simple().to_string();
    let key = ArtifactKey::new(ArtifactNamespace::Dataset, hex, "parquet").expect("key");
    let uri = store.put(key, &bytes).await.expect("store parquet");

    let window_start = as_of_for(0);
    let dataset = NewTrainingDataset {
        training_dataset_id: dataset_id.clone(),
        model_spec_id: model_spec_id.clone(),
        window_start,
        window_end: window_start + ChronoDuration::hours(4),
        status: TrainingDatasetStatus::Built,
        feature_schema_hash: content_hash('a'),
        factor_schema_hash: content_hash('b'),
        label_schema_hash: content_hash('d'),
        dataset_hash: content_hash('e'),
        parquet_uri: uri,
        sample_count: TICKS * i64::try_from(MARKETS_PER_TICK).expect("count"),
        source_delay_secs: SOURCE_DELAY_SECS,
        sample_interval_secs: TICK_INTERVAL_SECS,
        horizons_secs: serde_json::json!([0]),
        coverage_json: serde_json::json!({}),
        runtime_config_version_id: rc_id.clone(),
    };
    PgTrainingDatasetRepository::new(db.clone())
        .create(dataset)
        .await
        .expect("dataset ledger");
    dataset_id
}

fn trainer_config() -> ModelTrainerConfig {
    let mut weights = BTreeMap::new();
    weights.insert(LIQUIDITY_DEPTH.as_str().to_owned(), DecimalString::new("1"));
    ModelTrainerConfig {
        factors: FactorsConfig {
            factor_weights: FactorWeights { weights },
            ..FactorsConfig::default()
        },
        model: ModelConfig::default(),
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

/// Replay config governing the PIT recompute: top-of-book + metadata features and
/// the liquidity factor family (the trained model's factor), with a relaxed
/// book-age bound that fits the `10s` source delay.
fn replay_config() -> ReplayConfig {
    let mut weights = BTreeMap::new();
    weights.insert(LIQUIDITY_DEPTH.as_str().to_owned(), DecimalString::new("1"));
    ReplayConfig {
        features: FeaturesConfig {
            enabled_feature_families: vec![FeatureFamily::PriceBook, FeatureFamily::MarketMetadata],
            ..FeaturesConfig::default()
        },
        factors: FactorsConfig {
            enabled_factor_families: vec![FactorFamily::Liquidity],
            factor_weights: FactorWeights { weights },
            ..FactorsConfig::default()
        },
        data_quality: DataQualityConfig {
            max_book_age_ms: 60_000,
            max_feature_bucket_age_secs: 120,
            ..DataQualityConfig::default()
        },
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
    let dataset_id = seed_dataset(&db, &store, &model_spec_id, &rc_id).await;

    let registry: Arc<dyn ModelRegistryRepository> =
        Arc::new(PgModelRegistryRepository::new(db.clone()));
    let fact_read: Arc<dyn QuantFactReadRepository> = Arc::new(ControllableFactRead {
        scenario: Arc::new(Mutex::new(fact_scenario())),
    });
    let market_repo: Arc<dyn MarketRepository> = Arc::new(PgMarketRepository::new(db.clone()));
    let trainer = ModelTrainerService::new(
        ModelTrainerServiceDeps {
            dataset_repo: Arc::new(PgTrainingDatasetRepository::new(db.clone())),
            artifact_store: Arc::clone(&store),
            model_registry_repo: Arc::clone(&registry),
            model_run_repo: Arc::new(PgModelRunRepository::new(db.clone())),
            fact_read: Arc::clone(&fact_read),
            market_repo: Arc::clone(&market_repo),
        },
        trainer_config(),
        replay_config(),
        Duration::from_mins(1),
    );

    // ── Train ────────────────────────────────────────────────────────────
    let version = trainer
        .train(TrainModelInput {
            model_spec_id: model_spec_id.clone(),
            training_dataset_id: dataset_id.clone(),
            runtime_config_version_id: rc_id.clone(),
            model_family: ModelFamily::WeightedFactor,
            label: LabelSelector {
                name: settlement(),
                horizon_secs: 0,
            },
            validation_folds: 3,
        })
        .await
        .expect("train");
    assert_eq!(version.publication_status, PublicationStatus::Candidate);
    assert_eq!(version.training_dataset_id.as_ref(), Some(&dataset_id));

    // Publish-boundary invariant: the artifact's weights are frozen + content
    // addressed — re-deriving the hash from the stored bytes matches the
    // registry record (config cannot retroactively change a published hash).
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

    // Training run ledger: version FK backfilled on succeed, output_hash links artifact.
    assert_training_run_ledger(&db, &version, content_hash('e')).await;

    // ── Backtest (same PIT rematerialization path as training) ─────────────
    let factory_builder: Arc<dyn ModelRuntimeFactoryBuilder> =
        Arc::new(DefaultModelRuntimeFactoryBuilder::new(Arc::clone(&store)));
    let backtester = BacktestService::new(
        BacktestServiceDeps {
            dataset_repo: Arc::new(PgTrainingDatasetRepository::new(db.clone())),
            artifact_store: Arc::clone(&store),
            model_registry_repo: Arc::clone(&registry),
            model_run_repo: Arc::new(PgModelRunRepository::new(db.clone())),
            backtest_report_repo: Arc::new(PgBacktestReportRepository::new(db.clone())),
            comparison_report_repo: Arc::new(PgModelComparisonReportRepository::new(db.clone())),
            factory_builder,
            fact_read,
            market_repo,
        },
        &portfolio(),
        replay_config(),
        Duration::from_mins(1),
    );

    let report = backtester
        .run(BacktestInput {
            model_version_id: version.model_version_id.clone(),
            training_dataset_id: dataset_id.clone(),
            runtime_config_version_id: rc_id.clone(),
            calibrate: true,
        })
        .await
        .expect("backtest");
    assert!(report.sample_count > 0, "resolved samples");
    assert_eq!(report.model_version_id, version.model_version_id);
    assert!(
        report.report_hash.as_str().starts_with("blake3:"),
        "report hash persisted"
    );

    // ── Calibration registered a child Candidate version (≥ 2 versions) ────
    let next = registry
        .next_version_for_spec(&model_spec_id)
        .await
        .expect("count");
    assert!(
        next >= 3,
        "calibration should have registered a calibrated child version (next={next})"
    );
}
