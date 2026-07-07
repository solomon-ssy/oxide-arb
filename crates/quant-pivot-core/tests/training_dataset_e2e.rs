//! End-to-end training-dataset build: PIT correctness, leakage gate, settlement
//! maturity, and typed `training_dataset_id` FK wiring.

use std::{
    collections::{BTreeSet, HashMap},
    sync::{Arc, Mutex},
};

use async_trait::async_trait;
use chrono::{Duration as ChronoDuration, Utc};
use quant_pivot_api::fees::FeeCalculator;
use quant_pivot_core::service::training_dataset::{
    TrainingDatasetBuildConfig, TrainingDatasetService, TrainingDatasetServiceDeps,
    default_labelers,
};
use quant_pivot_error::storage::StorageError;
use quant_pivot_error::{QuantError, QuantResult, research::ResearchError};
use quant_pivot_models::{
    clickhouse::{
        BookMicrostructureRow, BookSnapshotRow, ChPrice, ChSchemaVersion, ChUsd,
        DomainObservationRow, MarketResolutionRow, MidPriceBucketRow, TickEventRow, TradeTapeRow,
    },
    domain::{
        JobProgressSink, NewModelSpec, NewModelVersion, NewRuntimeConfigVersion,
        NewTrainingDataset, NoopProgressSink, market::book::BookLevel,
    },
    entities::market::{Column as MarketColumn, Entity as MarketEntity},
    enums::{
        clickhouse::{ChFactSource, ChSnapshotReason},
        common::MarketCategory,
        factor::FactorFamily,
        market::MarketStatus,
        model::ModelFamily,
        quant::{PublicationStatus, TrainingDatasetStatus},
        runtime_config::RuntimeConfigVersionSource,
    },
    runtime_config::{
        DataQualityConfig, DecimalString, DomainConfig, FactorsConfig, FeatureFamily,
        FeaturesConfig, SelectionConfig, TrainingConfig,
    },
    types::{
        ArtifactUri, ContentHash, DatasetCoverage, DomainInstrumentKey, MarketId, ModelSpecId,
        ModelVersionId, Price, RuntimeConfigVersionId, SchemaVersion, Shares, TokenId,
        TrainingDatasetId, TrainingHorizonsSecs, TrainingSampleSource, default_sample_sources,
    },
};
use quant_pivot_repository::{
    postgres::{
        PgAttributionRepository, PgEventRepository, PgFeatureRepository, PgMarketRepository,
        PgModelRegistryRepository, PgPositionRepository, PgRecommendationRepository,
        PgRuntimeConfigVersionRepository, PgTrainingDatasetRepository,
    },
    traits::{
        EventRepository, MarketRepository, ModelRegistryRepository, QuantFactReadRepository,
        RuntimeConfigVersionRepository, TrainingDatasetRepository,
    },
};
use quant_pivot_research::{
    artifact::{ArtifactStore, LocalArtifactStore},
    pit::{BookSnapshotAt, MarketContextAt, PitQueryEngine},
    training::{
        DatasetPlan, DatasetPlanRequest, LabelName, TrainingDatasetArtifact,
        TrainingDatasetBuilder, TrainingDatasetPlanner,
    },
};
use quant_pivot_test_support::{
    catalog_fixtures::{make_event, make_market},
    pg::setup_pg,
    report_pipeline_harness::EmptyLinkageRepo,
};
use rust_decimal::Decimal;
use sea_orm::{ColumnTrait, DatabaseConnection, EntityTrait, QueryFilter, sea_query::Expr};
use tokio_util::sync::CancellationToken;

const EVENT_ID: &str = "evt-dataset-e2e";
const MARKET_ID: &str = "0xdatasete2e";
const YES_TOKEN: &str = "dataset-yes";
const NO_TOKEN: &str = "dataset-no";

const SETTLEMENT_LABEL: LabelName = LabelName::from_static("settlement_outcome");

fn runtime_config_id() -> RuntimeConfigVersionId {
    RuntimeConfigVersionId::from_v7()
}

async fn seed_runtime_config(db: &DatabaseConnection) -> RuntimeConfigVersionId {
    let id = runtime_config_id();
    let hash = ContentHash::parse(format!("blake3:{}", "c".repeat(64))).expect("hash");
    PgRuntimeConfigVersionRepository::new(db.clone())
        .create_version(NewRuntimeConfigVersion {
            runtime_config_version_id: id.clone(),
            config_hash: hash,
            schema_version: SchemaVersion::FIRST,
            config_json: serde_json::json!({}),
            source: RuntimeConfigVersionSource::Bootstrap,
            created_by: "dataset-e2e".to_owned(),
            reason: "dataset e2e".to_owned(),
        })
        .await
        .expect("runtime config version");
    id
}

fn dataset_window() -> (chrono::DateTime<Utc>, chrono::DateTime<Utc>) {
    let start = Utc::now() - ChronoDuration::hours(2);
    // One sample at `start` when `sample_interval_secs == 60`.
    let end = start + ChronoDuration::seconds(60);
    (start, end)
}

const fn sample_as_of(window_start: chrono::DateTime<Utc>) -> chrono::DateTime<Utc> {
    window_start
}

#[derive(Default)]
struct FactScenario {
    books: HashMap<TokenId, Vec<BookSnapshotRow>>,
    micro: HashMap<TokenId, Vec<BookMicrostructureRow>>,
    resolutions: HashMap<MarketId, Vec<MarketResolutionRow>>,
}

struct ControllableFactRead {
    scenario: Arc<Mutex<FactScenario>>,
}

impl ControllableFactRead {
    const fn new(scenario: Arc<Mutex<FactScenario>>) -> Self {
        Self { scenario }
    }
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

/// PIT engine that deliberately returns a book observed after `as_of`.
struct LeakyPitEngine {
    token_id: TokenId,
    market_id: MarketId,
    leak_ms: i64,
}

#[async_trait]
impl PitQueryEngine for LeakyPitEngine {
    async fn book_at(
        &self,
        token_id: &TokenId,
        as_of: chrono::DateTime<Utc>,
    ) -> QuantResult<Option<BookSnapshotAt>> {
        if token_id != &self.token_id {
            return Ok(None);
        }
        let observed_ms = as_of.timestamp_millis().saturating_add(self.leak_ms);
        let bid = BookLevel::from_decimal_unchecked(
            Price::new(Decimal::new(48, 2)),
            Shares::new(Decimal::from(100)),
        );
        let ask = BookLevel::from_decimal_unchecked(
            Price::new(Decimal::new(52, 2)),
            Shares::new(Decimal::from(100)),
        );
        Ok(Some(BookSnapshotAt {
            token_id: token_id.clone(),
            as_of,
            bids: Arc::from([bid]),
            asks: Arc::from([ask]),
            timestamp_ms: u64::try_from(observed_ms).unwrap_or(0),
            version: 1,
        }))
    }

    async fn market_at(
        &self,
        market_id: &MarketId,
        as_of: chrono::DateTime<Utc>,
    ) -> QuantResult<Option<MarketContextAt>> {
        if market_id != &self.market_id {
            return Ok(None);
        }
        Ok(Some(MarketContextAt {
            market_id: market_id.clone(),
            as_of,
            observed_at: as_of - ChronoDuration::days(1),
            status: MarketStatus::Active,
            neg_risk: false,
            end_date: Some(as_of + ChronoDuration::days(7)),
            created_at: as_of - ChronoDuration::days(2),
            outcome_count: 2,
        }))
    }
}

fn book_row(token: &str, event_time_ms: i64) -> BookSnapshotRow {
    BookSnapshotRow {
        token_id: TokenId::new(token),
        market_id: Some(MarketId::new(MARKET_ID)),
        snapshot_reason: ChSnapshotReason::Startup,
        top_n: 5,
        bids_json: r#"[["0.48","100"]]"#.to_owned(),
        asks_json: r#"[["0.52","100"]]"#.to_owned(),
        bid_depth_usd: None,
        ask_depth_usd: None,
        mid_price: Some(ChPrice::from(Price::new(Decimal::new(50, 2)))),
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

fn micro_row(token: &str, bucket_time_ms: i64, mid: Decimal) -> BookMicrostructureRow {
    let price = Price::new(mid);
    BookMicrostructureRow {
        token_id: TokenId::new(token),
        market_id: Some(MarketId::new(MARKET_ID)),
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

fn pit_scenario(as_of_ms: i64) -> FactScenario {
    let token = TokenId::new(YES_TOKEN);
    // Book and micro evidence must precede `as_of - source_delay` (10s in tests).
    let evidence_ms = as_of_ms - 15_000;
    let mut books = HashMap::new();
    books.insert(token.clone(), vec![book_row(YES_TOKEN, evidence_ms)]);

    let mut micro = HashMap::new();
    micro.insert(
        token,
        vec![
            micro_row(YES_TOKEN, evidence_ms - 15_000, Decimal::new(49, 2)),
            micro_row(YES_TOKEN, evidence_ms, Decimal::new(50, 2)),
            micro_row(YES_TOKEN, as_of_ms + 30_000, Decimal::new(55, 2)),
            micro_row(YES_TOKEN, as_of_ms + 60_000, Decimal::new(56, 2)),
        ],
    );

    FactScenario {
        books,
        micro,
        resolutions: HashMap::new(),
    }
}

fn features_config() -> FeaturesConfig {
    FeaturesConfig {
        enabled_feature_families: vec![FeatureFamily::PriceBook, FeatureFamily::MarketMetadata],
        ..FeaturesConfig::default()
    }
}

fn factors_config() -> FactorsConfig {
    FactorsConfig {
        enabled_factor_families: vec![FactorFamily::DataQuality],
        ..FactorsConfig::default()
    }
}

async fn seed_catalog(db: &DatabaseConnection, window_start: chrono::DateTime<Utc>) {
    PgEventRepository::new(db.clone())
        .upsert(make_event(
            EVENT_ID,
            "Dataset E2E",
            "dataset-e2e",
            MarketCategory::Sports,
        ))
        .await
        .expect("seed event");
    let mut market = make_market(
        MARKET_ID,
        EVENT_ID,
        "Dataset E2E?",
        "dataset-e2e",
        MarketCategory::Sports,
        Some(window_start + ChronoDuration::days(7)),
    );
    market.yes_token_id = TokenId::new(YES_TOKEN);
    market.no_token_id = TokenId::new(NO_TOKEN);
    PgMarketRepository::new(db.clone())
        .upsert(market)
        .await
        .expect("seed market");

    let created_at = window_start - ChronoDuration::days(1);
    MarketEntity::update_many()
        .col_expr(MarketColumn::CreatedAt, Expr::value(created_at))
        .filter(MarketColumn::MarketId.eq(MARKET_ID))
        .exec(db)
        .await
        .expect("backdate market created_at");
}

async fn seed_model_spec(db: &DatabaseConnection) -> ModelSpecId {
    let model_spec_id = ModelSpecId::from_v7();
    PgModelRegistryRepository::new(db.clone())
        .create_model_spec(NewModelSpec {
            model_spec_id: model_spec_id.clone(),
            name: "dataset-e2e".to_owned(),
            model_family: ModelFamily::WeightedFactor,
            prediction_horizon_secs: 86_400,
            feature_schema_version: SchemaVersion::FIRST,
            label_schema_version: SchemaVersion::FIRST,
            spec_json: serde_json::json!({}),
            status: PublicationStatus::Published,
        })
        .await
        .expect("create spec");
    model_spec_id
}

fn service(
    db: &DatabaseConnection,
    store: Arc<dyn ArtifactStore>,
    fact_read: Arc<dyn QuantFactReadRepository>,
) -> TrainingDatasetService {
    service_with_selection(
        db,
        store,
        fact_read,
        SelectionConfig {
            enabled_categories: vec![MarketCategory::Sports],
            ..SelectionConfig::default()
        },
        DecimalString::new("0"),
    )
}

fn service_with_selection(
    db: &DatabaseConnection,
    store: Arc<dyn ArtifactStore>,
    fact_read: Arc<dyn QuantFactReadRepository>,
    selection: SelectionConfig,
    min_selection_depth_usd: DecimalString,
) -> TrainingDatasetService {
    TrainingDatasetService::new(
        TrainingDatasetServiceDeps {
            fact_read,
            market_repo: Arc::new(PgMarketRepository::new(db.clone())),
            event_repo: Arc::new(PgEventRepository::new(db.clone())),
            artifact_store: store,
            dataset_repo: Arc::new(PgTrainingDatasetRepository::new(db.clone())),
            attribution_repo: Arc::new(PgAttributionRepository::new(db.clone())),
            recommendation_repo: Arc::new(PgRecommendationRepository::new(db.clone())),
            feature_repo: Arc::new(PgFeatureRepository::new(db.clone())),
            position_repo: Arc::new(PgPositionRepository::new(db.clone())),
            fee_calculator: Arc::new(FeeCalculator::new()),
            linkage_repo: Arc::new(EmptyLinkageRepo),
        },
        TrainingDatasetBuildConfig {
            features: features_config(),
            factors: factors_config(),
            domain: DomainConfig::default(),
            data_quality: DataQualityConfig {
                // Default `max_book_age_ms` (5s) conflicts with `source_delay_secs`
                // (10s): PIT evidence must be older than the delay but younger than
                // the book-age bound.
                max_book_age_ms: 60_000,
                max_feature_bucket_age_secs: 120,
                ..DataQualityConfig::default()
            },
            training: TrainingConfig {
                // Offline PIT selection uses book depth as the liquidity proxy.
                min_selection_depth_usd,
                ..TrainingConfig::default()
            },
            // The point-in-time selection funnel replayed during the build uses
            // this frozen selection policy.
            selection,
            labelers: default_labelers(),
            bias_table: None,
        },
        2_000_000,
    )
    .expect("training dataset service")
}

fn temp_artifact_store() -> Arc<dyn ArtifactStore> {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| d.as_nanos());
    let dir = std::env::temp_dir().join(format!("quant-pivot-dataset-e2e-{nanos}"));
    std::fs::create_dir_all(&dir).expect("artifact dir");
    Arc::new(LocalArtifactStore::new(dir))
}

async fn plan_request(
    service: &TrainingDatasetService,
    model_spec_id: ModelSpecId,
    runtime_config_version_id: RuntimeConfigVersionId,
    window_start: chrono::DateTime<Utc>,
    window_end: chrono::DateTime<Utc>,
) -> DatasetPlan {
    service
        .plan(DatasetPlanRequest {
            model_spec_id,
            runtime_config_version_id,
            window_start,
            window_end,
            sample_interval_secs: 60,
            horizons_secs: vec![60],
            source_delay_secs: 10,
            feature_schema_version: SchemaVersion::FIRST,
            sample_sources: default_sample_sources(),
            training_dataset_id: None,
        })
        .await
        .expect("plan")
}

fn assert_no_feature_leakage(artifact: &TrainingDatasetArtifact, source_delay_secs: u64) {
    let delay = ChronoDuration::seconds(i64::try_from(source_delay_secs).unwrap_or(i64::MAX));
    for example in &artifact.examples {
        let cutoff = example.as_of - delay;
        for source in &example.source_refs {
            assert!(
                source.observed_at <= cutoff,
                "future feature evidence: observed_at {} > cutoff {} (as_of {})",
                source.observed_at,
                cutoff,
                example.as_of,
            );
        }
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn historical_pit_no_look_ahead_via_dataset_build() {
    let (pool, _container) = setup_pg().await;
    let db = pool.connection().clone();
    let rc_id = seed_runtime_config(&db).await;
    let (window_start, window_end) = dataset_window();
    seed_catalog(&db, window_start).await;
    let model_spec_id = seed_model_spec(&db).await;

    let as_of = sample_as_of(window_start);
    let as_of_ms = as_of.timestamp_millis();
    let scenario = Arc::new(Mutex::new(pit_scenario(as_of_ms)));
    let store = temp_artifact_store();
    let svc = service(
        &db,
        Arc::clone(&store),
        Arc::new(ControllableFactRead::new(Arc::clone(&scenario))),
    );

    let plan = plan_request(&svc, model_spec_id, rc_id, window_start, window_end).await;
    let artifact = svc.build(plan).await.expect("build");
    assert!(!artifact.examples.is_empty(), "expected built examples");
    assert_no_feature_leakage(&artifact, 10);
    // The point-in-time selection funnel ran and kept the eligible fixture market.
    assert!(
        artifact.coverage.pit_selection_candidates > 0,
        "expected the PIT selection funnel to evaluate candidates",
    );
    assert!(
        artifact.coverage.pit_selection_included > 0,
        "expected the eligible fixture market to survive PIT selection",
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn build_cancelled_before_spine_yields_cancelled_and_no_row() {
    let (pool, _container) = setup_pg().await;
    let db = pool.connection().clone();
    let rc_id = seed_runtime_config(&db).await;
    let (window_start, window_end) = dataset_window();
    seed_catalog(&db, window_start).await;
    let model_spec_id = seed_model_spec(&db).await;

    let as_of_ms = sample_as_of(window_start).timestamp_millis();
    let scenario = Arc::new(Mutex::new(pit_scenario(as_of_ms)));
    let store = temp_artifact_store();
    let svc = service(
        &db,
        Arc::clone(&store),
        Arc::new(ControllableFactRead::new(scenario)),
    );

    let plan = plan_request(&svc, model_spec_id, rc_id, window_start, window_end).await;
    let dataset_id = plan.training_dataset_id.clone();

    // A cancel observed at the first cross-section boundary unwinds the build
    // cooperatively (~one section): it fails closed with `Cancelled` and never
    // persists a partial artifact or ledger row.
    let cancel = CancellationToken::new();
    cancel.cancel();
    let sink: Arc<dyn JobProgressSink> = Arc::new(NoopProgressSink);
    let err = svc
        .build_with_progress(plan, sink, cancel)
        .await
        .expect_err("cancelled build must fail closed");
    assert!(
        matches!(err, QuantError::Research(ResearchError::Cancelled { .. })),
        "expected Cancelled, got {err:?}"
    );
    assert!(
        PgTrainingDatasetRepository::new(db)
            .find_by_id(&dataset_id)
            .await
            .expect("lookup")
            .is_none(),
        "a cancelled build must not persist a ledger row"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn pit_selection_excludes_disabled_category_market() {
    let (pool, _container) = setup_pg().await;
    let db = pool.connection().clone();
    let rc_id = seed_runtime_config(&db).await;
    let (window_start, window_end) = dataset_window();
    seed_catalog(&db, window_start).await;
    let model_spec_id = seed_model_spec(&db).await;

    let as_of = sample_as_of(window_start);
    let as_of_ms = as_of.timestamp_millis();
    let scenario = Arc::new(Mutex::new(pit_scenario(as_of_ms)));
    let store = temp_artifact_store();
    // The market passes the cheap plan prefilter (Sports + lifetime) so it enters
    // the spine as a candidate, but the point-in-time FilterChain then rejects it:
    // its ~100 USD book depth is below this liquidity floor.
    let svc = service_with_selection(
        &db,
        Arc::clone(&store),
        Arc::new(ControllableFactRead::new(Arc::clone(&scenario))),
        SelectionConfig {
            enabled_categories: vec![MarketCategory::Sports],
            ..SelectionConfig::default()
        },
        DecimalString::new("1000000"),
    );

    let plan = plan_request(&svc, model_spec_id, rc_id, window_start, window_end).await;
    let artifact = svc.build(plan).await.expect("build");
    assert!(
        artifact.coverage.pit_selection_candidates > 0,
        "the market should be evaluated by the PIT funnel",
    );
    assert_eq!(
        artifact.coverage.pit_selection_included, 0,
        "a market below the book-depth floor must be excluded by the PIT funnel",
    );
    assert!(
        artifact
            .coverage
            .pit_selection_excluded
            .insufficient_liquidity_count
            > 0,
        "the exclusion must be attributed to insufficient liquidity",
    );
    assert!(
        artifact.examples.is_empty(),
        "no examples should be materialized when selection excludes every market",
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn plan_estimates_pit_keep_rate() {
    let (pool, _container) = setup_pg().await;
    let db = pool.connection().clone();
    let rc_id = seed_runtime_config(&db).await;
    let (window_start, window_end) = dataset_window();
    seed_catalog(&db, window_start).await;
    let model_spec_id = seed_model_spec(&db).await;

    let as_of_ms = sample_as_of(window_start).timestamp_millis();
    let scenario = Arc::new(Mutex::new(pit_scenario(as_of_ms)));
    let store = temp_artifact_store();
    let svc = service(
        &db,
        store,
        Arc::new(ControllableFactRead::new(Arc::clone(&scenario))),
    );

    let request = DatasetPlanRequest {
        model_spec_id,
        runtime_config_version_id: rc_id,
        window_start,
        window_end,
        sample_interval_secs: 60,
        horizons_secs: vec![60],
        source_delay_secs: 10,
        feature_schema_version: SchemaVersion::FIRST,
        sample_sources: vec![TrainingSampleSource::HistoricalPit],
        training_dataset_id: None,
    };
    // 3 as_of slices × the single eligible fixture market.
    let counts = svc.count_plan(&request, 3, 50).await.expect("count plan");
    assert!(counts.spine_upper_bound > 0, "expected a non-empty spine");
    let keep_rate = counts.keep_rate.expect("keep-rate should be estimated");
    assert!(
        (keep_rate - 1.0).abs() < f64::EPSILON,
        "eligible fixture market must pass at every slice, got {keep_rate}",
    );
    assert_eq!(counts.keep_rate_sample_size, 3, "3 slices × 1 market");
    assert_eq!(
        counts.estimated_eligible_samples, counts.spine_upper_bound,
        "keep-rate 1.0 ⇒ estimate equals the upper bound",
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn dataset_builder_rejects_future_features() {
    let (pool, _container) = setup_pg().await;
    let db = pool.connection().clone();
    let rc_id = seed_runtime_config(&db).await;
    let (window_start, window_end) = dataset_window();
    seed_catalog(&db, window_start).await;
    let model_spec_id = seed_model_spec(&db).await;

    let as_of = sample_as_of(window_start);
    let as_of_ms = as_of.timestamp_millis();
    let scenario = Arc::new(Mutex::new(pit_scenario(as_of_ms)));
    let store = temp_artifact_store();
    let svc = service(
        &db,
        Arc::clone(&store),
        Arc::new(ControllableFactRead::new(Arc::clone(&scenario))),
    );
    let plan = plan_request(&svc, model_spec_id, rc_id, window_start, window_end).await;
    let dataset_id = plan.training_dataset_id.clone();

    let leaky = LeakyPitEngine {
        token_id: TokenId::new(YES_TOKEN),
        market_id: MarketId::new(MARKET_ID),
        leak_ms: 5_000,
    };
    let err = svc
        .build_with_pit_source(plan, &leaky)
        .await
        .expect_err("leaky pit must fail leakage gate");

    assert!(
        matches!(
            err,
            QuantError::Research(ResearchError::LeakageDetected { .. })
        ),
        "expected LeakageDetected, got {err:?}"
    );

    let repo = PgTrainingDatasetRepository::new(db.clone());
    assert!(
        repo.find_by_id(&dataset_id)
            .await
            .expect("lookup")
            .is_none(),
        "failed build must not persist ledger row"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn settlement_label_not_mature_before_resolution() {
    let (pool, _container) = setup_pg().await;
    let db = pool.connection().clone();
    let rc_id = seed_runtime_config(&db).await;
    let (window_start, window_end) = dataset_window();
    seed_catalog(&db, window_start).await;
    let model_spec_id = seed_model_spec(&db).await;

    let as_of_ms = sample_as_of(window_start).timestamp_millis();
    let scenario = Arc::new(Mutex::new(pit_scenario(as_of_ms)));
    let store = temp_artifact_store();
    let svc = service(
        &db,
        Arc::clone(&store),
        Arc::new(ControllableFactRead::new(Arc::clone(&scenario))),
    );

    let plan = plan_request(&svc, model_spec_id, rc_id, window_start, window_end).await;
    let artifact = svc.build(plan).await.expect("build");
    assert!(artifact.coverage.labels_not_mature > 0);
    for example in &artifact.examples {
        assert!(
            !example
                .labels
                .iter()
                .any(|label| label.label_name == SETTLEMENT_LABEL),
            "settlement label must not appear before resolution"
        );
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn settlement_label_available_after_resolution() {
    let (pool, _container) = setup_pg().await;
    let db = pool.connection().clone();
    let rc_id = seed_runtime_config(&db).await;
    let (window_start, window_end) = dataset_window();
    seed_catalog(&db, window_start).await;
    let model_spec_id = seed_model_spec(&db).await;

    let as_of = sample_as_of(window_start);
    let as_of_ms = as_of.timestamp_millis();
    let mut fact = pit_scenario(as_of_ms);
    fact.resolutions.insert(
        MarketId::new(MARKET_ID),
        vec![MarketResolutionRow {
            market_id: MarketId::new(MARKET_ID),
            winning_token_id: TokenId::new(YES_TOKEN),
            winning_outcome: "Yes".to_owned(),
            asset_token_ids: vec![TokenId::new(YES_TOKEN), TokenId::new(NO_TOKEN)],
            resolved_at: as_of_ms + 30_000,
            observed_at: as_of_ms + 30_000,
            sequence: 1,
            source: ChFactSource::WsMarketResolved,
            schema_version: ChSchemaVersion::FIRST,
        }],
    );
    let scenario = Arc::new(Mutex::new(fact));
    let store = temp_artifact_store();
    let svc = service(
        &db,
        Arc::clone(&store),
        Arc::new(ControllableFactRead::new(scenario)),
    );

    let plan = plan_request(&svc, model_spec_id, rc_id, window_start, window_end).await;
    let artifact = svc.build(plan).await.expect("build");
    assert!(
        artifact.examples.iter().any(|example| {
            example
                .labels
                .iter()
                .any(|label| label.label_name == SETTLEMENT_LABEL)
        }),
        "expected settlement label after resolution is visible in forward window"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn plan_build_reuses_training_dataset_id() {
    let (pool, _container) = setup_pg().await;
    let db = pool.connection().clone();
    let rc_id = seed_runtime_config(&db).await;
    let (window_start, window_end) = dataset_window();
    seed_catalog(&db, window_start).await;
    let model_spec_id = seed_model_spec(&db).await;

    let as_of_ms = sample_as_of(window_start).timestamp_millis();
    let scenario = Arc::new(Mutex::new(pit_scenario(as_of_ms)));
    let store = temp_artifact_store();
    let svc = service(
        &db,
        Arc::clone(&store),
        Arc::new(ControllableFactRead::new(Arc::clone(&scenario))),
    );

    let plan_a = plan_request(
        &svc,
        model_spec_id.clone(),
        rc_id.clone(),
        window_start,
        window_end,
    )
    .await;
    let plan_b = plan_request(&svc, model_spec_id, rc_id, window_start, window_end).await;
    assert_ne!(
        plan_a.training_dataset_id, plan_b.training_dataset_id,
        "each plan call mints a fresh id"
    );

    let mut build_request = plan_a.request.clone();
    build_request.training_dataset_id = Some(plan_a.training_dataset_id.clone());
    let artifact = svc
        .build(DatasetPlan {
            request: build_request,
            training_dataset_id: plan_a.training_dataset_id.clone(),
            samples: plan_a.samples.clone(),
            lot_samples: plan_a.lot_samples.clone(),
            exit_training_lots: plan_a.exit_training_lots.clone(),
            label_names: plan_a.label_names.clone(),
        })
        .await
        .expect("build");
    assert_eq!(
        artifact.training_dataset_id, plan_a.training_dataset_id,
        "build must reuse the plan-assigned id"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn build_status_insufficient_labels_when_no_labels_mature() {
    let (pool, _container) = setup_pg().await;
    let db = pool.connection().clone();
    let rc_id = seed_runtime_config(&db).await;
    let (window_start, window_end) = dataset_window();
    seed_catalog(&db, window_start).await;
    let model_spec_id = seed_model_spec(&db).await;

    let as_of_ms = sample_as_of(window_start).timestamp_millis();
    let scenario = Arc::new(Mutex::new(pit_scenario(as_of_ms)));
    let store = temp_artifact_store();
    let svc = service(
        &db,
        Arc::clone(&store),
        Arc::new(ControllableFactRead::new(Arc::clone(&scenario))),
    );

    let mut plan = plan_request(&svc, model_spec_id, rc_id, window_start, window_end).await;
    plan.request.horizons_secs = vec![86_400];
    let artifact = svc.build(plan).await.expect("build");
    assert!(artifact.coverage.built_examples > 0);
    assert_eq!(artifact.coverage.labels_available, 0);

    let row = PgTrainingDatasetRepository::new(db)
        .find_by_id(&artifact.training_dataset_id)
        .await
        .expect("lookup")
        .expect("ledger row");
    assert_eq!(row.status, TrainingDatasetStatus::InsufficientLabels);
}

#[tokio::test(flavor = "multi_thread")]
async fn build_status_failed_when_zero_examples() {
    let (pool, _container) = setup_pg().await;
    let db = pool.connection().clone();
    let rc_id = seed_runtime_config(&db).await;
    let (window_start, window_end) = dataset_window();
    seed_catalog(&db, window_start).await;
    let model_spec_id = seed_model_spec(&db).await;

    let as_of_ms = sample_as_of(window_start).timestamp_millis();
    let scenario = Arc::new(Mutex::new(pit_scenario(as_of_ms)));
    let store = temp_artifact_store();
    let svc = service(
        &db,
        Arc::clone(&store),
        Arc::new(ControllableFactRead::new(Arc::clone(&scenario))),
    );

    let mut plan = plan_request(&svc, model_spec_id, rc_id, window_start, window_end).await;
    plan.samples.clear();
    let artifact = svc.build(plan).await.expect("build");
    assert_eq!(artifact.coverage.built_examples, 0);

    let row = PgTrainingDatasetRepository::new(db)
        .find_by_id(&artifact.training_dataset_id)
        .await
        .expect("lookup")
        .expect("ledger row");
    assert_eq!(row.status, TrainingDatasetStatus::Failed);
}

#[tokio::test(flavor = "multi_thread")]
async fn build_records_book_decode_failures() {
    let (pool, _container) = setup_pg().await;
    let db = pool.connection().clone();
    let rc_id = seed_runtime_config(&db).await;
    let (window_start, window_end) = dataset_window();
    seed_catalog(&db, window_start).await;
    let model_spec_id = seed_model_spec(&db).await;

    let as_of_ms = sample_as_of(window_start).timestamp_millis();
    let mut fact = pit_scenario(as_of_ms);
    let evidence_ms = as_of_ms - 15_000;
    let mut bad = book_row(YES_TOKEN, evidence_ms);
    bad.bids_json = "not-json".to_owned();
    fact.books.insert(
        TokenId::new(YES_TOKEN),
        vec![bad, book_row(YES_TOKEN, evidence_ms + 1)],
    );

    let scenario = Arc::new(Mutex::new(fact));
    let store = temp_artifact_store();
    let svc = service(
        &db,
        Arc::clone(&store),
        Arc::new(ControllableFactRead::new(scenario)),
    );

    let plan = plan_request(&svc, model_spec_id, rc_id, window_start, window_end).await;
    let artifact = svc.build(plan).await.expect("build");
    assert!(
        artifact.coverage.book_decode_failures > 0,
        "malformed book JSON must increment decode failures"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn settlement_label_visible_without_micro_past_resolution() {
    let (pool, _container) = setup_pg().await;
    let db = pool.connection().clone();
    let rc_id = seed_runtime_config(&db).await;
    let (window_start, window_end) = dataset_window();
    seed_catalog(&db, window_start).await;
    let model_spec_id = seed_model_spec(&db).await;

    let as_of = sample_as_of(window_start);
    let as_of_ms = as_of.timestamp_millis();
    let evidence_ms = as_of_ms - 15_000;
    let fact = FactScenario {
        books: HashMap::from([(
            TokenId::new(YES_TOKEN),
            vec![book_row(YES_TOKEN, evidence_ms)],
        )]),
        micro: HashMap::from([(
            TokenId::new(YES_TOKEN),
            vec![micro_row(YES_TOKEN, evidence_ms, Decimal::new(50, 2))],
        )]),
        resolutions: HashMap::from([(
            MarketId::new(MARKET_ID),
            vec![MarketResolutionRow {
                market_id: MarketId::new(MARKET_ID),
                winning_token_id: TokenId::new(YES_TOKEN),
                winning_outcome: "Yes".to_owned(),
                asset_token_ids: vec![TokenId::new(YES_TOKEN), TokenId::new(NO_TOKEN)],
                resolved_at: as_of_ms + 30_000,
                observed_at: as_of_ms + 30_000,
                sequence: 1,
                source: ChFactSource::WsMarketResolved,
                schema_version: ChSchemaVersion::FIRST,
            }],
        )]),
    };
    let scenario = Arc::new(Mutex::new(fact));
    let store = temp_artifact_store();
    let svc = service(
        &db,
        Arc::clone(&store),
        Arc::new(ControllableFactRead::new(scenario)),
    );

    let plan = plan_request(&svc, model_spec_id, rc_id, window_start, window_end).await;
    let artifact = svc.build(plan).await.expect("build");
    assert!(
        artifact.examples.iter().any(|example| {
            example
                .labels
                .iter()
                .any(|label| label.label_name == SETTLEMENT_LABEL)
        }),
        "settlement must not depend on microstructure extending past resolution"
    );
}

#[tokio::test]
async fn model_version_training_dataset_id_is_typed() {
    let (pool, _container) = setup_pg().await;
    let db = pool.connection().clone();
    let rc_id = seed_runtime_config(&db).await;
    let model_spec_id = seed_model_spec(&db).await;
    let dataset_id = TrainingDatasetId::from_v7();
    let (window_start, window_end) = dataset_window();
    let hash = ContentHash::parse(format!("blake3:{}", "b".repeat(64))).expect("hash");

    PgTrainingDatasetRepository::new(db.clone())
        .create(NewTrainingDataset {
            training_dataset_id: dataset_id.clone(),
            model_spec_id: model_spec_id.clone(),
            window_start,
            window_end,
            status: TrainingDatasetStatus::Built,
            feature_schema_hash: hash.clone(),
            factor_schema_hash: hash.clone(),
            label_schema_hash: hash.clone(),
            dataset_hash: hash.clone(),
            parquet_uri: ArtifactUri::parse("file:///tmp/dataset.parquet").expect("uri"),
            sample_count: 10,
            source_delay_secs: 10,
            sample_interval_secs: 3600,
            horizons_secs: TrainingHorizonsSecs(vec![3600]),
            coverage_json: DatasetCoverage::default(),
            runtime_config_version_id: rc_id,
        })
        .await
        .expect("create dataset");

    let version_id = ModelVersionId::from_v7();
    PgModelRegistryRepository::new(db.clone())
        .create_model_version(NewModelVersion {
            model_version_id: version_id.clone(),
            model_spec_id,
            version: 2,
            artifact_hash: hash,
            training_dataset_id: Some(dataset_id.clone()),
            metrics_json: serde_json::json!({}),
            quality_gate_report: serde_json::json!({}),
            publication_status: PublicationStatus::Candidate,
            published_at: None,
            retired_at: None,
        })
        .await
        .expect("typed FK insert");

    let loaded = PgModelRegistryRepository::new(db)
        .find_model_version_by_id(&version_id)
        .await
        .expect("load")
        .expect("version");
    assert_eq!(loaded.training_dataset_id, Some(dataset_id));
}

#[tokio::test]
async fn plan_count_respects_sample_sources() {
    let (pool, _container) = setup_pg().await;
    let db = pool.connection().clone();
    let rc_id = seed_runtime_config(&db).await;
    let (window_start, window_end) = dataset_window();
    seed_catalog(&db, window_start).await;
    let model_spec_id = seed_model_spec(&db).await;

    let as_of = sample_as_of(window_start);
    let scenario = Arc::new(Mutex::new(pit_scenario(as_of.timestamp_millis())));
    let store = temp_artifact_store();
    let svc = service(
        &db,
        Arc::clone(&store),
        Arc::new(ControllableFactRead::new(scenario)),
    );

    let default_plan = plan_request(
        &svc,
        model_spec_id.clone(),
        rc_id.clone(),
        window_start,
        window_end,
    )
    .await;
    let historical_only_plan = svc
        .plan(DatasetPlanRequest {
            model_spec_id,
            runtime_config_version_id: rc_id,
            window_start,
            window_end,
            sample_interval_secs: 60,
            horizons_secs: vec![60],
            source_delay_secs: 10,
            feature_schema_version: SchemaVersion::FIRST,
            sample_sources: vec![TrainingSampleSource::HistoricalPit],
            training_dataset_id: None,
        })
        .await
        .expect("historical-only plan");

    let default_count = svc
        .count_planned_samples(&default_plan)
        .await
        .expect("default count");
    let historical_count = svc
        .count_planned_samples(&historical_only_plan)
        .await
        .expect("historical count");

    assert_eq!(
        historical_count,
        historical_only_plan.samples.len() as u64,
        "historical-only plan count must match the sample grid"
    );
    assert!(historical_count >= 1);
    assert_eq!(
        default_count, historical_count,
        "without live attribution rows both sources collapse to historical count"
    );
}
