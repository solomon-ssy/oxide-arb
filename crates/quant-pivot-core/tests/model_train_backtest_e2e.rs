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
    collections::{BTreeSet, HashMap},
    sync::{Arc, Mutex},
    time::Duration,
};

use async_trait::async_trait;
use chrono::{Duration as ChronoDuration, TimeZone, Utc};
use tokio_util::sync::CancellationToken;

use quant_pivot_core::governance::CoreCalibrationArtifactLoader;
use quant_pivot_core::service::{
    backtest::{BacktestInput, BacktestService, BacktestServiceDeps},
    cpcv_backtest::{
        CpcvBacktestConfig, CpcvBacktestInput, CpcvBacktestService, CpcvBacktestServiceDeps,
    },
    historical_replay::ReplayConfig,
    model_training::{
        ModelTrainerConfig, ModelTrainerService, ModelTrainerServiceDeps, TrainModelInput,
    },
};
use quant_pivot_error::storage::StorageError;
use quant_pivot_models::{
    clickhouse::{
        BookMicrostructureRow, BookSnapshotRow, ChPrice, ChSchemaVersion, ChUsd,
        DomainObservationRow, MarketResolutionRow, MidPriceBucketRow, TickEventRow, TradeTapeRow,
    },
    domain::{
        ModelVersionInfo, NewBacktestPathSet, NewModelRun, NewModelSpec, NewRuntimeConfigVersion,
        NewTrainingDataset, NoopProgressSink,
    },
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
            DataQualityStatus, DatasetPurpose, FactorDirection, ModelRunKind, ModelRunStatus,
            PublicationStatus, TrainingDatasetStatus,
        },
        runtime_config::RuntimeConfigVersionSource,
    },
    runtime_config::{
        DataQualityConfig, DomainConfig, FactorWeights, FactorsConfig, FeatureFamily,
        FeaturesConfig, PortfolioBudget, PortfolioConfig, PortfolioConstraints, RankLossKind,
        ResearchTrainingConfig, RuntimeConfig, TrainingOptimizerKind, wire::DecimalString,
    },
    types::{
        BacktestPathSetId, ContentHash, DatasetCoverage, DomainInstrumentKey, FactorDefinitionId,
        MarketId, ModelRunId, ModelSpecId, ModelVersionId, Price, Probability,
        RuntimeConfigVersionId, SchemaVersion, TokenId, TrainingDatasetId, TrainingExampleId,
        TrainingHorizonsSecs, TrainingSampleSource, Usd,
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
        MarketRepository, ModelRegistryRepository, ModelRunRepository, QuantFactReadRepository,
        RuntimeConfigVersionRepository, TrainingDatasetRepository,
    },
};
use quant_pivot_research::{
    artifact::{ArtifactKey, ArtifactNamespace, ArtifactStore, LocalArtifactStore},
    factors::{FactorExplanation, FactorValue, NormalizedFactor, names::LIQUIDITY_DEPTH},
    features::{FeatureName, FeatureValue, FeatureVector, names},
    model::{
        CalibrationArtifactLoader, DefaultModelRuntimeFactoryBuilder, LabelSelector, ModelArtifact,
        ModelRuntimeFactoryBuilder, TrainingObjectiveSpec,
    },
    training::{DatasetParquetCodec, LabelName, TrainingExample, TrainingLabel},
    validation::{CpcvConfig, PboInput, PurgeConfig, TrialGridSpec, WeightedFactorTrialGrid},
};
use quant_pivot_test_support::{
    catalog_fixtures::{make_event, make_market},
    pg::setup_pg,
    report_pipeline_harness::EmptyLinkageRepo,
};
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use sea_orm::{ColumnTrait, DatabaseConnection, EntityTrait, QueryFilter, sea_query::Expr};
use std::collections::BTreeMap;
use uuid::Uuid;

// Type alias to keep the BTreeMap key readable.
type FeatureName2 = FeatureName;

const EVENT_ID: &str = "evt-train-backtest-e2e";
const TICKS: i64 = 4;
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
            // Non-crypto category: this suite exercises liquidity rematerialization,
            // not the Phase 11.2.2 crypto domain-weight publish invariant. Unanimous
            // Crypto examples would infer `category_scope=Crypto` and reject a
            // liquidity-only seed weight set.
            values.insert(
                names::market::CATEGORY.clone(),
                FeatureValue::Category(MarketCategory::Politics),
            );
            let feature_vector = FeatureVector {
                market_id: market.clone(),
                token_id: Some(token.clone()),
                as_of,
                generic_schema_version: SchemaVersion::FIRST,
                generic: values,
                domain: None,
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
                    matured_at: as_of + ChronoDuration::seconds(1),
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

    async fn trade_tape_window_by_market(
        &self,
        _market_ids: Vec<MarketId>,
        _from_ms: i64,
        _to_ms: i64,
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

    async fn observed_markets_between(
        &self,
        from_ms: i64,
        to_ms: i64,
    ) -> Result<Vec<MarketId>, StorageError> {
        let markets: BTreeSet<MarketId> = {
            let scenario = self.scenario.lock().expect("lock");
            scenario
                .books
                .values()
                .flatten()
                .filter(|row| row.event_time >= from_ms && row.event_time <= to_ms)
                .filter_map(|row| row.market_id.clone())
                .collect()
        };
        Ok(markets.into_iter().collect())
    }

    async fn domain_observations_between(
        &self,
        _instrument_keys: Vec<DomainInstrumentKey>,
        _from_ms: i64,
        _to_ms: i64,
    ) -> Result<Vec<DomainObservationRow>, StorageError> {
        Ok(Vec::new())
    }

    async fn domain_observation_at(
        &self,
        _instrument_key: &DomainInstrumentKey,
        _metric: &str,
        _as_of_ms: i64,
    ) -> Result<Option<DomainObservationRow>, StorageError> {
        Ok(None)
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
        schema_version: ChSchemaVersion::FIRST,
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
        schema_version: ChSchemaVersion::FIRST,
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

fn e2e_runtime_config() -> RuntimeConfig {
    let mut config = RuntimeConfig::default();
    config.research.training = e2e_research_training();
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
            feature_requirements: serde_json::json!({}),
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
        purpose: DatasetPurpose::Training,
        feature_schema_hash: content_hash('a'),
        factor_schema_hash: content_hash('b'),
        label_schema_hash: content_hash('d'),
        dataset_hash: content_hash('e'),
        parquet_uri: uri,
        sample_count: TICKS * i64::try_from(MARKETS_PER_TICK).expect("count"),
        source_delay_secs: SOURCE_DELAY_SECS,
        sample_interval_secs: TICK_INTERVAL_SECS,
        horizons_secs: TrainingHorizonsSecs(vec![0]),
        coverage_json: DatasetCoverage::default(),
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
    let runtime = e2e_runtime_config();
    ModelTrainerConfig {
        factors: FactorsConfig {
            factor_weights: FactorWeights { weights },
            ..FactorsConfig::default()
        },
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
        domain: DomainConfig::default(),
        data_quality: DataQualityConfig {
            max_book_age_ms: 60_000,
            max_feature_bucket_age_secs: 120,
            ..DataQualityConfig::default()
        },
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

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires Docker"]
#[allow(clippy::too_many_lines)]
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
            event_repo: Arc::new(PgEventRepository::new(db.clone())),
            linkage_repo: Arc::new(EmptyLinkageRepo),
        },
        trainer_config(),
        replay_config(),
        Duration::from_mins(1),
    );

    // ── Train ────────────────────────────────────────────────────────────
    let outcome = trainer
        .train(
            TrainModelInput {
                model_version_id: ModelVersionId::from_v7(),
                model_spec_id: model_spec_id.clone(),
                training_dataset_id: dataset_id.clone(),
                runtime_config_version_id: rc_id.clone(),
                model_family: ModelFamily::WeightedFactor,
                label: LabelSelector {
                    name: settlement(),
                    horizon_secs: 0,
                },
                prediction_horizon_secs: 86_400,
                validation_folds: 3,
                selection_enabled_categories: vec![],
                category_scope: None,
            },
            &NoopProgressSink,
            &CancellationToken::new(),
        )
        .await
        .expect("train");
    let version = outcome.version;
    assert_eq!(version.publication_status, PublicationStatus::Candidate);
    assert_eq!(version.training_dataset_id.as_ref(), Some(&dataset_id));
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
    assert_eq!(
        artifact.category_scope(),
        Some(MarketCategory::Politics),
        "unanimous Politics examples freeze Politics scope (not Crypto domain weights)"
    );

    // Training run ledger: version FK backfilled on succeed, output_hash links artifact.
    assert_training_run_ledger(&db, &version, content_hash('e')).await;

    // ── Backtest (same PIT rematerialization path as training) ─────────────
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
            fact_read,
            market_repo,
            event_repo: Arc::new(PgEventRepository::new(db.clone())),
            linkage_repo: Arc::new(EmptyLinkageRepo),
        },
        &portfolio(),
        replay_config(),
        Duration::from_mins(1),
    );

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

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires Docker"]
#[allow(clippy::too_many_lines)]
async fn train_then_cpcv_persists_path_set_with_dsr_n_decomposition() {
    let (pool, _container) = setup_pg().await;
    let db = pool.connection().clone();
    let store: Arc<dyn ArtifactStore> = Arc::new(LocalArtifactStore::new(
        std::env::temp_dir().join(format!("qp_cpcv_e2e_{}", Uuid::new_v4().simple())),
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
            event_repo: Arc::new(PgEventRepository::new(db.clone())),
            linkage_repo: Arc::new(EmptyLinkageRepo),
        },
        trainer_config(),
        replay_config(),
        Duration::from_mins(1),
    );

    let outcome = trainer
        .train(
            TrainModelInput {
                model_version_id: ModelVersionId::from_v7(),
                model_spec_id: model_spec_id.clone(),
                training_dataset_id: dataset_id.clone(),
                runtime_config_version_id: rc_id.clone(),
                model_family: ModelFamily::WeightedFactor,
                label: LabelSelector {
                    name: settlement(),
                    horizon_secs: 0,
                },
                prediction_horizon_secs: 86_400,
                validation_folds: 3,
                selection_enabled_categories: vec![],
                category_scope: None,
            },
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

    // Dataset has 4 as_of groups → CPCV n_groups=4; PBO needs ≥4 periods.
    let runtime = e2e_runtime_config();
    let cpcv = CpcvBacktestService::new(
        CpcvBacktestServiceDeps {
            dataset_repo: Arc::new(PgTrainingDatasetRepository::new(db.clone())),
            artifact_store: Arc::clone(&store),
            fact_read: Arc::clone(&fact_read),
            market_repo: Arc::clone(&market_repo),
            event_repo: Arc::new(PgEventRepository::new(db.clone())),
            linkage_repo: Arc::new(EmptyLinkageRepo),
        },
        CpcvBacktestConfig {
            factors: trainer_config().factors,
            objective: TrainingObjectiveSpec::from_runtime_config(&runtime.research.training)
                .expect("objective"),
            cpcv: CpcvConfig {
                n_groups: 4,
                k_test: 2,
            },
            purge: PurgeConfig {
                embargo_pct: dec!(0.02),
                min_embargo_secs: 0,
            },
            trials: TrialGridSpec::WeightedFactor(WeightedFactorTrialGrid {
                lambda_multipliers: vec![Decimal::ONE, dec!(0.5)],
                rank_loss_kinds: vec![RankLossKind::RankIcWeightedRanknet],
                max_trials: 4,
            }),
            pbo: PboInput { block_count: 4 },
            dsr_significance: dec!(0.05),
        },
        &portfolio(),
        replay_config(),
        Duration::from_mins(1),
    );

    let path_set_id = BacktestPathSetId::from_v7();
    let cpcv_outcome = cpcv
        .run(
            CpcvBacktestInput {
                training_dataset_id: dataset_id.clone(),
                runtime_config_version_id: rc_id.clone(),
                label: LabelSelector {
                    name: settlement(),
                    horizon_secs: 0,
                },
                model_family: ModelFamily::WeightedFactor,
                prediction_horizon_secs: 86_400,
                category_scope: None,
                path_set_id: Some(path_set_id.clone()),
                coord_search_effective_n: u32::try_from(coord_n).unwrap_or(0),
            },
            &NoopProgressSink,
            &CancellationToken::new(),
        )
        .await
        .expect("cpcv");

    assert_eq!(
        cpcv_outcome.trial_count,
        cpcv_outcome
            .trial_grid_count
            .saturating_add(cpcv_outcome.coord_search_effective_n),
        "DSR N must equal grid + coord-search effective N"
    );
    assert_eq!(
        cpcv_outcome.coord_search_effective_n,
        u32::try_from(coord_n).unwrap_or(0)
    );
    assert!(!cpcv_outcome.path_set.paths.is_empty(), "φ paths present");
    // N=4,k=2 → φ = C(3,1) = 3 paths; C(4,2) = 6 combinations.
    assert_eq!(cpcv_outcome.path_set.paths.len(), 3);
    assert_eq!(cpcv_outcome.path_set.combination_count, 6);

    // Persist ledger row the same way the admin port does.
    let model_run_id = ModelRunId::from_v7();
    PgModelRunRepository::new(db.clone())
        .create(NewModelRun {
            model_run_id: model_run_id.clone(),
            run_kind: ModelRunKind::Backtest,
            model_version_id: Some(version.model_version_id.clone()),
            runtime_config_version_id: rc_id.clone(),
            market_selection_id: None,
            window_start: cpcv_outcome.window_start,
            window_end: cpcv_outcome.window_end,
            status: ModelRunStatus::Succeeded,
            input_hash: content_hash('7'),
            output_hash: Some(content_hash('8')),
            metrics_json: serde_json::json!({}),
            error_code: None,
            error_message: None,
            started_at: Utc::now(),
            finished_at: Some(Utc::now()),
        })
        .await
        .expect("model run");

    let persisted = PgBacktestPathSetRepository::new(db.clone())
        .create(NewBacktestPathSet {
            path_set_id: path_set_id.clone(),
            model_version_id: version.model_version_id.clone(),
            model_run_id,
            training_dataset_id: dataset_id,
            runtime_config_version_id: rc_id,
            window_start: cpcv_outcome.window_start,
            window_end: cpcv_outcome.window_end,
            path_count: i64::try_from(cpcv_outcome.path_set.paths.len()).unwrap_or(0),
            combination_count: i64::try_from(cpcv_outcome.path_set.combination_count).unwrap_or(0),
            median_rank_ic: cpcv_outcome.path_set.median_rank_ic,
            sharpe_distribution: serde_json::to_value(cpcv_outcome.path_set.sharpe_distribution)
                .expect("sharpe dist"),
            paths: serde_json::to_value(cpcv_outcome.path_set.paths).expect("paths"),
            deflated_sharpe: cpcv_outcome.dsr.deflated_sharpe,
            dsr_benchmark_sharpe: cpcv_outcome.dsr.benchmark_sharpe,
            pbo: cpcv_outcome.pbo,
            min_track_record_length_secs: cpcv_outcome
                .min_track_record_length
                .map(|d| d.num_seconds()),
            trial_count: i64::from(cpcv_outcome.trial_count),
            trial_grid_count: i64::from(cpcv_outcome.trial_grid_count),
            coord_search_effective_n: i64::from(cpcv_outcome.coord_search_effective_n),
        })
        .await
        .expect("persist path set");

    assert_eq!(persisted.path_set_id, path_set_id);
    assert_eq!(
        persisted.trial_count,
        persisted.trial_grid_count + persisted.coord_search_effective_n
    );

    let listed = PgBacktestPathSetRepository::new(db)
        .list_by_model_version(&version.model_version_id)
        .await
        .expect("list");
    assert_eq!(listed.len(), 1);
}
