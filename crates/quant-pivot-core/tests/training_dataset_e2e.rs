//! End-to-end training-dataset build: PIT correctness, leakage gate, settlement
//! maturity, and typed `training_dataset_id` FK wiring.

use std::{
    collections::{BTreeSet, HashMap},
    sync::{Arc, Mutex},
};

use async_trait::async_trait;
use chrono::{DateTime, Duration as ChronoDuration, Utc};
use quant_pivot_core::pit::platform::ch_historical::DurablePitSource;
use quant_pivot_core::service::training_dataset::{
    TrainingDatasetBuildConfig, TrainingDatasetService, TrainingDatasetServiceDeps,
    default_labelers,
};
use quant_pivot_error::storage::StorageError;
use quant_pivot_error::{QuantError, QuantResult, research::ResearchError};
use quant_pivot_models::{
    clickhouse::{
        BookL2CheckpointRow, BookL2EventRow, BookMicrostructureRow, BookStreamSessionRow,
        ChDecimal64, ChPrice, ChSchemaVersion, ChShares, ChUsd, DomainObservationRow,
        MarketResolutionRow, MidPriceBucketRow, TradeTapeRow,
    },
    config::GammaConfig,
    domain::{
        CatalogCommit, CompleteTrainingDatasetBuild, CryptoSubject, DecisionBoundary,
        DecisionSource, EventRegistryInfo, GroundingProof, JobProgressSink, LinkageOutcome,
        MarketLinkageDerivation, MarketRegistryInfo, MarketSubject, NewCatalogSyncBatch,
        NewEventCatalogVersion, NewMarketCatalogVersion, NewMarketLinkage, NewModelSpec,
        NewModelVersion, NewRuntimeConfigVersion, NewTrainingDatasetPlan, NoopProgressSink,
        PriceComparator, ResolutionOracle, ResolvedBinding, ResolvedSourceBinding, UpsertEvent,
        UpsertMarket,
        market::{book::BookLevel, registry::TokenInfo},
    },
    entities::{
        market::{Column as MarketColumn, Entity as MarketEntity},
        quant_market_linkage::{Column as LinkageColumn, Entity as LinkageEntity},
    },
    enums::{
        clickhouse::{
            ChCanonicalBookEventType, ChFactSource, ChStreamSessionEndReason, ChStreamSessionState,
        },
        common::{CategorySet, MarketCategory, TickSize},
        domain::{DomainFamily, DomainMetric, KlineInterval, LinkageSourceRole, ResolverTier},
        factor::FactorFamily,
        market::{EventStatus, MarketStatus},
        model::ModelFamily,
        quant::{DatasetPurpose, PublicationStatus, TrainingDatasetStatus},
        runtime_config::RuntimeConfigVersionSource,
    },
    runtime_config::{
        DataQualityConfig, DecimalString, DomainConfig, FactorsConfig, FeatureFamily,
        FeaturesConfig, SelectionConfig, TrainingConfig,
    },
    types::{
        ArtifactUri, BinanceSymbol, CatalogSyncBatchId, ContentHash, CryptoAsset, CryptoQuote,
        DATASET_ARTIFACT_FORMAT_VERSION, DatasetCoverage, DatasetManifest, DomainInstrumentKey,
        DomainSourceId, EventCatalogVersionId, EventId, MarketCatalogVersionId, MarketId,
        ModelInputContract, ModelSpecId, ModelTrainingContract, ModelVersionId, Price, Probability,
        ResolverVersion, RuntimeConfigVersionId, SchemaVersion, Shares, TokenId, TrainingDatasetId,
        TrainingHorizonsSecs, TrainingSampleSource, TrainingSampleSources, Usd,
        default_sample_sources,
    },
};
use quant_pivot_repository::{
    postgres::{
        PgAttributionRepository, PgCalibrationArtifactRepository, PgCatalogVersionRepository,
        PgClobMarketInfoRepository, PgFeatureRepository, PgMarketLinkageRepository,
        PgMarketRepository, PgMarketSelectionRepository, PgModelRegistryRepository,
        PgPositionRepository, PgRecommendationRepository, PgRuntimeConfigVersionRepository,
        PgTradePolicyRepository, PgTrainingDatasetRepository,
    },
    traits::{
        CatalogVersionRepository, MarketLinkageRepository, ModelRegistryRepository,
        QuantFactReadRepository, RuntimeConfigVersionRepository, TrainingDatasetRepository,
    },
};
use quant_pivot_research::{
    artifact::{ArtifactStore, LocalArtifactStore},
    features::{EvidenceSourceKind, FeatureName},
    pit::{
        BookSnapshotAt, CanonicalBookEventRef, PointInTimeSnapshotSource, ResolvedMarketSnapshot,
    },
    training::{
        DatasetPlan, DatasetPlanRequest, LabelName, TrainingDatasetArtifact,
        TrainingDatasetBuilder, TrainingDatasetPlanner, dataset_manifest_hash,
    },
};
use quant_pivot_test_support::{
    catalog_fixtures::{make_event, make_market},
    execution_pg_seed::{content_hash as fixture_hash, fixture_profile_ref},
    pg::setup_pg,
    report_pipeline_harness::EmptyLinkageRepo,
};
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use sea_orm::{ColumnTrait, DatabaseConnection, EntityTrait, QueryFilter, sea_query::Expr};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

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
    // Catalog coverage begins at the durable commit visibility barrier. Use a
    // logical replay clock after that barrier; these fake facts do not depend
    // on wall-clock maturity, and backdating coverage would invalidate the PIT
    // contract this suite is meant to exercise.
    let start = Utc::now() + ChronoDuration::hours(1);
    // One sample at `start` when `sample_interval_secs == 60`.
    let end = start + ChronoDuration::seconds(60);
    (start, end)
}

const fn sample_as_of(window_start: chrono::DateTime<Utc>) -> chrono::DateTime<Utc> {
    window_start
}

fn ledger_manifest(
    training_dataset_id: &TrainingDatasetId,
    model_spec_id: &ModelSpecId,
    runtime_config_version_id: &RuntimeConfigVersionId,
    window_start: DateTime<Utc>,
    window_end: DateTime<Utc>,
    hash: &ContentHash,
    sample_count: u64,
) -> DatasetManifest {
    DatasetManifest {
        format_version: DATASET_ARTIFACT_FORMAT_VERSION,
        training_dataset_id: training_dataset_id.clone(),
        profile_ref: fixture_profile_ref(),
        research_program_hash: fixture_hash('4'),
        source_slice: quant_pivot_test_support::execution_pg_seed::source_slice_ref('5'),
        model_spec_id: model_spec_id.clone(),
        trade_policy_artifact_id: None,
        trade_policy_hash: None,
        runtime_config_version_id: runtime_config_version_id.clone(),
        window_start,
        window_end,
        purpose: DatasetPurpose::Training,
        knowledge_lag_secs: 10,
        sample_interval_secs: 3_600,
        horizons_secs: vec![3_600],
        feature_schema_hash: hash.clone(),
        factor_schema_hash: hash.clone(),
        label_schema_hash: hash.clone(),
        semantic_dataset_hash: hash.clone(),
        source_fingerprint: hash.clone(),
        sample_count,
    }
}

#[derive(Default)]
struct FactScenario {
    books: HashMap<TokenId, Vec<BookL2CheckpointRow>>,
    micro: HashMap<TokenId, Vec<BookMicrostructureRow>>,
    resolutions: HashMap<MarketId, Vec<MarketResolutionRow>>,
    domain_observations: HashMap<DomainInstrumentKey, Vec<DomainObservationRow>>,
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
        decision_at_ms: i64,
    ) -> Result<Vec<BookMicrostructureRow>, StorageError> {
        let scenario = self.scenario.lock().expect("lock");
        let mut rows = Vec::new();
        for token_id in token_ids {
            if let Some(series) = scenario.micro.get(&token_id) {
                for row in series {
                    if row.bucket_time >= from_ms
                        && row.bucket_time < to_ms
                        && row.available_at <= decision_at_ms
                    {
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

    async fn book_checkpoint_at(
        &self,
        token_id: &TokenId,
        source_cutoff_ms: i64,
        decision_at_ms: i64,
    ) -> Result<Option<BookL2CheckpointRow>, StorageError> {
        let scenario = self.scenario.lock().expect("lock");
        Ok(scenario.books.get(token_id).and_then(|rows| {
            rows.iter()
                .filter(|row| {
                    row.event_time <= source_cutoff_ms && row.created_at <= decision_at_ms
                })
                .max_by_key(|row| (row.event_time, row.created_at, row.token_sequence))
                .cloned()
        }))
    }

    async fn book_l2_events_from(
        &self,
        token_id: &TokenId,
        stream_session_id: Uuid,
        from_sequence: u64,
        source_cutoff_ms: i64,
        decision_at_ms: i64,
    ) -> Result<Vec<BookL2EventRow>, StorageError> {
        let scenario = self.scenario.lock().expect("lock");
        Ok(scenario
            .books
            .get(token_id)
            .and_then(|rows| {
                rows.iter()
                    .filter(|row| {
                        row.stream_session_id == stream_session_id
                            && row.token_sequence == from_sequence
                            && row.event_time <= source_cutoff_ms
                            && row.created_at <= decision_at_ms
                    })
                    .max_by_key(|row| (row.event_time, row.created_at))
            })
            .map(checkpoint_anchor_event)
            .into_iter()
            .collect())
    }

    async fn book_stream_session_at(
        &self,
        stream_session_id: Uuid,
        _decision_at_ms: i64,
    ) -> Result<Option<BookStreamSessionRow>, StorageError> {
        Ok(
            (stream_session_id == Uuid::nil()).then(|| BookStreamSessionRow {
                stream_session_id,
                shard_id: 0,
                ledger_sequence: 1,
                state: ChStreamSessionState::Open,
                end_reason: ChStreamSessionEndReason::None,
                subscription_token_hash: catalog_fixture_hash('0'),
                subscription_token_count: 1,
                received_sequence_json: "{}".to_owned(),
                persisted_sequence_json: "{}".to_owned(),
                opened_at: 0,
                recorded_at: 0,
                schema_version: ChSchemaVersion(2),
            }),
        )
    }

    async fn book_checkpoints_between(
        &self,
        token_ids: Vec<TokenId>,
        from_ms: i64,
        to_ms: i64,
        available_by_ms: i64,
    ) -> Result<Vec<BookL2CheckpointRow>, StorageError> {
        let scenario = self.scenario.lock().expect("lock");
        let mut rows = Vec::new();
        for token_id in token_ids {
            if let Some(series) = scenario.books.get(&token_id) {
                for row in series {
                    if row.event_time >= from_ms
                        && row.event_time <= to_ms
                        && row.created_at <= available_by_ms
                    {
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
        source_cutoff_ms: i64,
        decision_at_ms: i64,
    ) -> Result<Option<MarketResolutionRow>, StorageError> {
        let scenario = self.scenario.lock().expect("lock");
        Ok(scenario.resolutions.get(market_id).and_then(|rows| {
            rows.iter()
                .filter(|row| {
                    row.resolved_at <= source_cutoff_ms && row.observed_at <= decision_at_ms
                })
                .max_by_key(|row| (row.resolved_at, row.observed_at, row.sequence))
                .cloned()
        }))
    }

    async fn resolutions_between(
        &self,
        market_ids: Vec<MarketId>,
        from_ms: i64,
        to_ms: i64,
        decision_at_ms: i64,
    ) -> Result<Vec<MarketResolutionRow>, StorageError> {
        let scenario = self.scenario.lock().expect("lock");
        let mut rows = Vec::new();
        for market_id in market_ids {
            if let Some(series) = scenario.resolutions.get(&market_id) {
                for row in series {
                    if row.resolved_at >= from_ms
                        && row.resolved_at <= to_ms
                        && row.observed_at <= decision_at_ms
                    {
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
        decision_at_ms: i64,
    ) -> Result<Vec<MarketId>, StorageError> {
        let markets: BTreeSet<MarketId> = {
            let scenario = self.scenario.lock().expect("lock");
            scenario
                .books
                .values()
                .flatten()
                .filter(|row| {
                    row.event_time >= from_ms
                        && row.event_time <= to_ms
                        && row.created_at <= decision_at_ms
                })
                .filter_map(|row| row.market_id.clone())
                .collect()
        };
        Ok(markets.into_iter().collect())
    }

    async fn domain_observations_between(
        &self,
        instrument_keys: Vec<DomainInstrumentKey>,
        from_ms: i64,
        to_ms: i64,
        publish_cutoff_ms: i64,
        decision_at_ms: i64,
    ) -> Result<Vec<DomainObservationRow>, StorageError> {
        let scenario = self.scenario.lock().expect("lock");
        let mut rows = Vec::new();
        for instrument_key in instrument_keys {
            if let Some(series) = scenario.domain_observations.get(&instrument_key) {
                for row in series {
                    if row.event_time >= from_ms
                        && row.event_time < to_ms
                        && row.publish_time <= publish_cutoff_ms
                        && row.ingestion_time <= decision_at_ms
                    {
                        rows.push(row.clone());
                    }
                }
            }
        }
        Ok(rows)
    }

    async fn domain_observation_at(
        &self,
        instrument_key: &DomainInstrumentKey,
        metric: &str,
        source_cutoff_ms: i64,
        decision_at_ms: i64,
    ) -> Result<Option<DomainObservationRow>, StorageError> {
        let scenario = self.scenario.lock().expect("lock");
        Ok(scenario
            .domain_observations
            .get(instrument_key)
            .and_then(|rows| {
                rows.iter()
                    .filter(|row| {
                        row.metric == metric
                            && row.event_time <= source_cutoff_ms
                            && row.publish_time <= source_cutoff_ms
                            && row.ingestion_time <= decision_at_ms
                    })
                    .max_by_key(|row| (row.event_time, row.ingestion_time))
                    .cloned()
            }))
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
}

fn checkpoint_anchor_event(checkpoint: &BookL2CheckpointRow) -> BookL2EventRow {
    BookL2EventRow {
        stream_session_id: checkpoint.stream_session_id,
        shard_id: 0,
        token_id: checkpoint.token_id.clone(),
        market_id: checkpoint.market_id.clone(),
        token_sequence: checkpoint.token_sequence,
        event_type: ChCanonicalBookEventType::Snapshot,
        bid_prices: vec![ChPrice::from(Price::new(Decimal::new(48, 2)))],
        bid_sizes: vec![ChShares::from(Decimal::from(100))],
        ask_prices: vec![ChPrice::from(Price::new(Decimal::new(52, 2)))],
        ask_sizes: vec![ChShares::from(Decimal::from(100))],
        book_version: checkpoint.book_version,
        old_tick_size: None,
        new_tick_size: None,
        venue_event_time: checkpoint.event_time,
        ingress_time: checkpoint.created_at,
        persisted_time: checkpoint.created_at,
        payload_hash: checkpoint.source_event_hash.clone(),
        schema_version: checkpoint.schema_version,
    }
}

/// PIT engine that deliberately returns a book observed after the source cutoff.
struct LeakyPitEngine {
    token_id: TokenId,
    leak_ms: i64,
    catalog: Arc<dyn PointInTimeSnapshotSource>,
}

#[async_trait]
impl PointInTimeSnapshotSource for LeakyPitEngine {
    async fn book_at_boundary(
        &self,
        token_id: &TokenId,
        boundary: &DecisionBoundary,
    ) -> QuantResult<Option<BookSnapshotAt>> {
        if token_id != &self.token_id {
            return Ok(None);
        }
        let source_cutoff = boundary.cutoff_for(DecisionSource::Book);
        let observed_ms = source_cutoff
            .timestamp_millis()
            .checked_add(self.leak_ms)
            .expect("test leak timestamp must be representable");
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
            source_cutoff,
            decision_at: boundary.decision_at(),
            bids: Arc::from([bid]),
            asks: Arc::from([ask]),
            timestamp_ms: u64::try_from(observed_ms).expect("positive test timestamp"),
            version: 1,
            sequence: 1,
            source_event: Some(CanonicalBookEventRef {
                stream_session_id: Uuid::from_u128(1),
                token_sequence: 1,
                source_event_hash: ContentHash::parse(format!("blake3:{}", "d".repeat(64)))
                    .expect("canonical event hash"),
            }),
            available_at: boundary.decision_at(),
        }))
    }

    async fn market_snapshot_at(
        &self,
        market_id: &MarketId,
        boundary: &DecisionBoundary,
    ) -> QuantResult<Option<ResolvedMarketSnapshot>> {
        self.catalog.market_snapshot_at(market_id, boundary).await
    }

    async fn market_snapshots_at_boundary(
        &self,
        boundary: &DecisionBoundary,
    ) -> QuantResult<Vec<ResolvedMarketSnapshot>> {
        self.catalog.market_snapshots_at_boundary(boundary).await
    }
}

fn book_row(token: &str, event_time_ms: i64) -> BookL2CheckpointRow {
    BookL2CheckpointRow {
        token_id: TokenId::new(token),
        market_id: Some(MarketId::new(MARKET_ID)),
        stream_session_id: Uuid::nil(),
        token_sequence: 1,
        bids_json: r#"[["0.48","100"]]"#.to_owned(),
        asks_json: r#"[["0.52","100"]]"#.to_owned(),
        book_version: 1,
        source_event_hash: catalog_fixture_hash('1'),
        checkpoint_hash: catalog_fixture_hash('2'),
        event_time: event_time_ms,
        created_at: event_time_ms,
        schema_version: ChSchemaVersion(2),
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
        available_at: bucket_time_ms,
    }
}

fn pit_scenario(as_of_ms: i64) -> FactScenario {
    let token = TokenId::new(YES_TOKEN);
    // Book and micro evidence must precede the frozen source cutoff (10s lag here).
    let evidence_ms = as_of_ms - 15_000;
    let mut books = HashMap::new();
    books.insert(
        token.clone(),
        vec![
            book_row(YES_TOKEN, evidence_ms),
            // Keep-rate midpoint slices must each see a genuinely fresh PIT
            // book; one stale fixture row would measure fixture age, not the
            // selection funnel's keep rate.
            book_row(YES_TOKEN, as_of_ms + 5_000),
            book_row(YES_TOKEN, as_of_ms + 25_000),
        ],
    );

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
        domain_observations: HashMap::new(),
    }
}

fn features_config() -> FeaturesConfig {
    FeaturesConfig {
        enabled_feature_families: vec![FeatureFamily::PriceBook, FeatureFamily::MarketMetadata],
        ..FeaturesConfig::default()
    }
}

/// As [`features_config`], plus `Domain` — required for any test whose model
/// spec requires a `domain.crypto.*` feature, so the governed
/// [`quant_pivot_research::features::FeatureSchema`] actually registers the
/// spec (an unregistered name is unavailable regardless of
/// `domain_availability` — see `FeatureAvailabilityOracle::is_available`).
fn crypto_features_config() -> FeaturesConfig {
    FeaturesConfig {
        enabled_feature_families: vec![
            FeatureFamily::PriceBook,
            FeatureFamily::MarketMetadata,
            FeatureFamily::Domain,
        ],
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
    seed_catalog_with_category(db, window_start, MarketCategory::Sports).await;
}

struct CatalogSeedContext {
    source_effective_at: DateTime<Utc>,
    end_date: DateTime<Utc>,
    batch_id: CatalogSyncBatchId,
    event_version_id: EventCatalogVersionId,
    event_id: EventId,
    market_id: MarketId,
}

impl CatalogSeedContext {
    fn new(window_start: DateTime<Utc>) -> Self {
        Self {
            source_effective_at: window_start - ChronoDuration::days(1),
            end_date: window_start + ChronoDuration::days(7),
            batch_id: CatalogSyncBatchId::from_v7(),
            event_version_id: EventCatalogVersionId::from_v7(),
            event_id: EventId::new(EVENT_ID),
            market_id: MarketId::new(MARKET_ID),
        }
    }
}

fn catalog_fixture_hash(seed: char) -> ContentHash {
    ContentHash::parse(format!("blake3:{}", seed.to_string().repeat(64)))
        .expect("catalog fixture hash")
}

fn current_event_projection(context: &CatalogSeedContext, category: MarketCategory) -> UpsertEvent {
    let mut event = make_event(EVENT_ID, "Dataset E2E", "dataset-e2e", category);
    event.catalog_market_ids = vec![context.market_id.clone()].into();
    event
}

fn current_market_projection(
    context: &CatalogSeedContext,
    category: MarketCategory,
) -> UpsertMarket {
    let mut market = make_market(
        MARKET_ID,
        EVENT_ID,
        "Dataset E2E?",
        "dataset-e2e",
        category,
        Some(context.end_date),
    );
    market.yes_token_id = TokenId::new(YES_TOKEN);
    market.no_token_id = TokenId::new(NO_TOKEN);
    market
}

fn event_registry_payload(
    context: &CatalogSeedContext,
    category: MarketCategory,
) -> EventRegistryInfo {
    EventRegistryInfo {
        event_id: context.event_id.clone(),
        title: "Dataset E2E".to_owned(),
        slug: "dataset-e2e".to_owned(),
        series_slug: None,
        status: EventStatus::Active,
        market_ids: vec![context.market_id.clone()],
        categories: CategorySet::from(category),
        tags: vec![category.as_str().to_owned()],
        neg_risk: false,
        end_date: Some(context.end_date),
        created_at: context.source_effective_at,
        updated_at: context.source_effective_at,
    }
}

fn market_registry_payload(
    context: &CatalogSeedContext,
    category: MarketCategory,
) -> MarketRegistryInfo {
    MarketRegistryInfo {
        market_id: context.market_id.clone(),
        event_id: context.event_id.clone(),
        token_yes: TokenId::new(YES_TOKEN),
        token_no: TokenId::new(NO_TOKEN),
        question: "Dataset E2E?".to_owned(),
        slug: "dataset-e2e".to_owned(),
        description: None,
        categories: CategorySet::from(category),
        status: MarketStatus::Active,
        outcome: None,
        neg_risk: false,
        tick_size: TickSize::Hundredth,
        tokens: vec![
            TokenInfo {
                token_id: TokenId::new(YES_TOKEN),
                outcome: "Yes".to_owned(),
                neg_risk: false,
            },
            TokenInfo {
                token_id: TokenId::new(NO_TOKEN),
                outcome: "No".to_owned(),
                neg_risk: false,
            },
        ],
        best_bid: None,
        best_ask: None,
        depth_usd: None,
        min_order_size: Decimal::ONE,
        liquidity_usd: Some(Usd::new(dec!(1000))),
        volume_24h: Some(Usd::new(dec!(1000))),
        start_date: Some(context.source_effective_at),
        end_date: Some(context.end_date),
        resolved_at: None,
        created_at: Some(context.source_effective_at),
        updated_at: context.source_effective_at,
    }
}

fn durable_catalog_commit(context: &CatalogSeedContext, category: MarketCategory) -> CatalogCommit {
    let event_payload = event_registry_payload(context, category);
    let market_payload = market_registry_payload(context, category);

    CatalogCommit {
        batch: NewCatalogSyncBatch {
            catalog_sync_batch_id: context.batch_id.clone(),
            sync_kind: "full".to_owned(),
            source_cursor: None,
            started_at: context.source_effective_at,
            fetched_at: context.source_effective_at,
            event_count: 1,
            market_count: 1,
            rejected_count: 0,
            batch_hash: catalog_fixture_hash('1'),
        },
        current_events: vec![current_event_projection(context, category)],
        event_versions: vec![NewEventCatalogVersion {
            event_catalog_version_id: context.event_version_id.clone(),
            catalog_sync_batch_id: context.batch_id.clone(),
            event_id: context.event_id.clone(),
            source_effective_at: context.source_effective_at,
            source_timestamp_quality: "source".to_owned(),
            available_at: context.source_effective_at,
            origin: "gamma_sync".to_owned(),
            content_hash: catalog_fixture_hash('2'),
            payload: serde_json::to_value(&event_payload).expect("event payload"),
        }],
        current_markets: vec![current_market_projection(context, category)],
        market_versions: vec![NewMarketCatalogVersion {
            market_catalog_version_id: MarketCatalogVersionId::from_v7(),
            catalog_sync_batch_id: context.batch_id.clone(),
            event_catalog_version_id: context.event_version_id.clone(),
            market_id: context.market_id.clone(),
            event_id: context.event_id.clone(),
            source_effective_at: context.source_effective_at,
            source_timestamp_quality: "source".to_owned(),
            source_created_at: Some(context.source_effective_at),
            available_at: context.source_effective_at,
            origin: "gamma_sync".to_owned(),
            content_hash: catalog_fixture_hash('3'),
            payload: serde_json::to_value(&market_payload).expect("market payload"),
        }],
    }
}

async fn seed_catalog_with_category(
    db: &DatabaseConnection,
    window_start: chrono::DateTime<Utc>,
    category: MarketCategory,
) {
    let context = CatalogSeedContext::new(window_start);
    PgCatalogVersionRepository::new(
        db.clone(),
        GammaConfig::default().catalog_visibility_guard_secs,
    )
    .commit(durable_catalog_commit(&context, category))
    .await
    .expect("seed durable catalog");

    MarketEntity::update_many()
        .col_expr(
            MarketColumn::CreatedAt,
            Expr::value(context.source_effective_at),
        )
        .filter(MarketColumn::MarketId.eq(MARKET_ID))
        .exec(db)
        .await
        .expect("backdate market created_at");
}

async fn seed_model_spec(db: &DatabaseConnection) -> ModelSpecId {
    seed_model_spec_with_contract(db, ModelInputContract::single_required("book.mid")).await
}

async fn seed_model_spec_with_contract(
    db: &DatabaseConnection,
    input_contract: ModelInputContract,
) -> ModelSpecId {
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
            input_contract,
            training_contract: ModelTrainingContract::settlement_default(),
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
    )
}

fn service_with_selection(
    db: &DatabaseConnection,
    store: Arc<dyn ArtifactStore>,
    fact_read: Arc<dyn QuantFactReadRepository>,
    selection: SelectionConfig,
) -> TrainingDatasetService {
    service_with_selection_and_linkage(
        db,
        store,
        fact_read,
        Arc::new(EmptyLinkageRepo),
        features_config(),
        selection,
    )
}

/// As [`service_with_selection`], but with an injectable linkage-ledger
/// repository (lets a test seed real `quant_market_linkage` rows, e.g. a
/// `Resolved` crypto binding, via [`PgMarketLinkageRepository`] instead of
/// the always-empty read-only fake) and an injectable [`FeaturesConfig`]
/// (`features_config()` intentionally excludes `Domain` for the plain
/// Sports-market fixtures; a crypto-domain test needs it enabled so the
/// governed schema actually registers `domain.crypto.*` feature specs).
fn service_with_selection_and_linkage(
    db: &DatabaseConnection,
    store: Arc<dyn ArtifactStore>,
    fact_read: Arc<dyn QuantFactReadRepository>,
    linkage_repo: Arc<dyn MarketLinkageRepository>,
    features: FeaturesConfig,
    selection: SelectionConfig,
) -> TrainingDatasetService {
    TrainingDatasetService::new(
        TrainingDatasetServiceDeps {
            fact_read,
            catalog_repo: Arc::new(PgCatalogVersionRepository::new(
                db.clone(),
                GammaConfig::default().catalog_visibility_guard_secs,
            )),
            market_repo: Arc::new(PgMarketRepository::new(db.clone())),
            artifact_store: store,
            dataset_repo: Arc::new(PgTrainingDatasetRepository::new(db.clone())),
            attribution_repo: Arc::new(PgAttributionRepository::new(db.clone())),
            recommendation_repo: Arc::new(PgRecommendationRepository::new(db.clone())),
            feature_repo: Arc::new(PgFeatureRepository::new(db.clone())),
            selection_repo: Arc::new(PgMarketSelectionRepository::new(db.clone())),
            position_repo: Arc::new(PgPositionRepository::new(db.clone())),
            clob_market_info_repo: Arc::new(PgClobMarketInfoRepository::new(db.clone())),
            linkage_repo,
            model_registry: Arc::new(PgModelRegistryRepository::new(db.clone())),
            trade_policy_repo: Arc::new(PgTradePolicyRepository::new(db.clone())),
            calibration_repo: Arc::new(PgCalibrationArtifactRepository::new(db.clone())),
        },
        TrainingDatasetBuildConfig {
            features,
            factors: factors_config(),
            domain: DomainConfig::default(),
            data_quality: DataQualityConfig {
                // Default `max_book_age_ms` (5s) conflicts with `knowledge_lag_secs`
                // (10s): PIT evidence must be older than the delay but younger than
                // the book-age bound.
                max_book_age_ms: 60_000,
                max_feature_bucket_age_secs: 120,
                ..DataQualityConfig::default()
            },
            training: TrainingConfig::default(),
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
            profile_ref: fixture_profile_ref(),
            research_program_hash: fixture_hash('4'),
            source_slice: quant_pivot_test_support::execution_pg_seed::source_slice_ref('5'),
            runtime_config_version_id,
            window_start,
            window_end,
            pit_cutoff: window_end + chrono::Duration::seconds(60),
            sample_interval_secs: 60,
            horizons_secs: vec![60],
            knowledge_lag_secs: 10,
            feature_schema_version: SchemaVersion::FIRST,
            sample_sources: default_sample_sources(),
            training_dataset_id: None,
            purpose: DatasetPurpose::Training,
        })
        .await
        .expect("plan")
}

fn assert_no_feature_leakage(artifact: &TrainingDatasetArtifact, knowledge_lag_secs: u64) {
    for example in &artifact.examples {
        assert_eq!(
            example.decision_boundary.knowledge_lag_secs(),
            knowledge_lag_secs
        );
        for source in &example.source_refs {
            let cutoff = match source.source_kind {
                EvidenceSourceKind::Book => {
                    example.decision_boundary.cutoff_for(DecisionSource::Book)
                }
                EvidenceSourceKind::GammaMetadata => example
                    .decision_boundary
                    .cutoff_for(DecisionSource::Catalog),
                EvidenceSourceKind::ClickHouseFact => example
                    .decision_boundary
                    .cutoff_for(DecisionSource::Microstructure),
                EvidenceSourceKind::TradeTape => example
                    .decision_boundary
                    .cutoff_for(DecisionSource::TradeTape),
                EvidenceSourceKind::DomainExternal => example
                    .decision_boundary
                    .cutoff_for(DecisionSource::DomainCrypto),
                EvidenceSourceKind::Linkage => example
                    .decision_boundary
                    .cutoff_for(DecisionSource::Linkage),
                EvidenceSourceKind::Derived => example.decision_at(),
            };
            assert!(
                source.effective_at <= cutoff,
                "future feature evidence: observed_at {} > cutoff {} (decision_at {})",
                source.effective_at,
                cutoff,
                example.decision_at(),
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
async fn calibration_dataset_build_fails_closed_on_purge_overlap() {
    // Phase 11.3 P1-8: a `purpose = Calibration` dataset whose window
    // overlaps a `Ready` training dataset must fail closed at *build* time
    // (not only later, at calibrator-fit time) — the purge primitive shared
    // with `ModelCalibrationFitService`/`BiasTableFitService`. The
    // existing training dataset is seeded directly (its own materialization
    // pipeline is irrelevant here — only its ledger row's window/status
    // matter to the purge check).
    let (pool, _container) = setup_pg().await;
    let db = pool.connection().clone();
    let rc_id = seed_runtime_config(&db).await;
    let (window_start, window_end) = dataset_window();
    seed_catalog(&db, window_start).await;
    let model_spec_id = seed_model_spec(&db).await;

    let hash = ContentHash::parse(format!("blake3:{}", "a".repeat(64))).expect("hash");
    let dataset_id = TrainingDatasetId::from_v7();
    let manifest = ledger_manifest(
        &dataset_id,
        &model_spec_id,
        &rc_id,
        window_start,
        window_end,
        &hash,
        10,
    );
    let manifest_hash = dataset_manifest_hash(&manifest).expect("manifest hash");
    let dataset_repo = PgTrainingDatasetRepository::new(db.clone());
    dataset_repo
        .create_plan(NewTrainingDatasetPlan {
            training_dataset_id: dataset_id.clone(),
            model_spec_id: model_spec_id.clone(),
            window_start,
            window_end,
            purpose: DatasetPurpose::Training,
            knowledge_lag_secs: 10,
            sample_interval_secs: 3600,
            horizons_secs: TrainingHorizonsSecs(vec![3600]),
            feature_schema_version: Some(SchemaVersion::FIRST),
            sample_sources: Some(TrainingSampleSources(default_sample_sources())),
            runtime_config_version_id: rc_id.clone(),
        })
        .await
        .expect("seed existing training dataset plan");
    dataset_repo
        .start_build(&dataset_id)
        .await
        .expect("start existing training dataset");
    dataset_repo
        .complete_build(
            &dataset_id,
            CompleteTrainingDatasetBuild {
                status: TrainingDatasetStatus::Ready,
                feature_schema_hash: hash.clone(),
                factor_schema_hash: hash.clone(),
                label_schema_hash: hash.clone(),
                dataset_hash: hash.clone(),
                manifest_hash,
                manifest_json: manifest,
                artifact_bytes_hash: hash,
                parquet_uri: ArtifactUri::parse("file:///tmp/existing-training.parquet")
                    .expect("uri"),
                sample_count: 10,
                coverage_json: DatasetCoverage::default(),
                failure_detail: None,
            },
        )
        .await
        .expect("seed existing Ready training dataset");

    let as_of_ms = sample_as_of(window_start).timestamp_millis();
    let scenario = Arc::new(Mutex::new(pit_scenario(as_of_ms)));
    let store = temp_artifact_store();
    let svc = service(
        &db,
        Arc::clone(&store),
        Arc::new(ControllableFactRead::new(Arc::clone(&scenario))),
    );

    let calibration_plan = svc
        .plan(DatasetPlanRequest {
            model_spec_id,
            profile_ref: fixture_profile_ref(),
            research_program_hash: fixture_hash('4'),
            source_slice: quant_pivot_test_support::execution_pg_seed::source_slice_ref('5'),
            runtime_config_version_id: rc_id,
            window_start,
            window_end,
            pit_cutoff: window_end + chrono::Duration::seconds(60),
            sample_interval_secs: 60,
            horizons_secs: vec![60],
            knowledge_lag_secs: 10,
            feature_schema_version: SchemaVersion::FIRST,
            sample_sources: default_sample_sources(),
            training_dataset_id: None,
            purpose: DatasetPurpose::Calibration,
        })
        .await
        .expect("plan calibration dataset");
    let err = svc
        .build(calibration_plan)
        .await
        .expect_err("a calibration dataset overlapping a Ready training dataset must fail closed");
    assert!(
        matches!(
            err,
            QuantError::Research(ResearchError::DatasetBuild { .. })
        ),
        "expected a DatasetBuild purge error, got {err:?}"
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
    // cooperatively (~one section): it fails closed with `Cancelled`, persists
    // no partial artifact, and retains a terminal Failed audit row.
    let cancel = CancellationToken::new();
    cancel.cancel();
    let sink: Arc<dyn JobProgressSink> = Arc::new(NoopProgressSink);
    let err = Box::pin(svc.build_with_progress(plan, sink, cancel))
        .await
        .expect_err("cancelled build must fail closed");
    assert!(
        matches!(err, QuantError::Research(ResearchError::Cancelled { .. })),
        "expected Cancelled, got {err:?}"
    );
    let row = PgTrainingDatasetRepository::new(db)
        .find_by_id(&dataset_id)
        .await
        .expect("lookup")
        .expect("cancelled build audit row");
    assert_eq!(row.status, TrainingDatasetStatus::Failed);
    assert!(
        row.failure_detail
            .as_deref()
            .is_some_and(|detail| detail.contains("cancelled")),
        "cancelled build must retain its terminal audit reason"
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
    // its historical Gamma liquidity is below this governed online/offline floor.
    let svc = service_with_selection(
        &db,
        Arc::clone(&store),
        Arc::new(ControllableFactRead::new(Arc::clone(&scenario))),
        SelectionConfig {
            enabled_categories: vec![MarketCategory::Sports],
            min_liquidity_usd: DecimalString::new("1000000"),
            ..SelectionConfig::default()
        },
    );

    let plan = plan_request(&svc, model_spec_id, rc_id, window_start, window_end).await;
    let artifact = svc.build(plan).await.expect("build");
    assert!(
        artifact.coverage.pit_selection_candidates > 0,
        "the market should be evaluated by the PIT funnel",
    );
    assert_eq!(
        artifact.coverage.pit_selection_included, 0,
        "a market below the catalog-liquidity floor must be excluded by the PIT funnel",
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
async fn pit_selection_excludes_crypto_market_when_model_requires_unavailable_domain_feature() {
    let (pool, _container) = setup_pg().await;
    let db = pool.connection().clone();
    let rc_id = seed_runtime_config(&db).await;
    let (window_start, window_end) = dataset_window();
    seed_catalog_with_category(&db, window_start, MarketCategory::Crypto).await;
    let model_spec_id = seed_model_spec_with_contract(
        &db,
        ModelInputContract::single_required("domain.crypto.distance_to_strike"),
    )
    .await;

    let as_of = sample_as_of(window_start);
    let as_of_ms = as_of.timestamp_millis();
    let scenario = Arc::new(Mutex::new(pit_scenario(as_of_ms)));
    let store = temp_artifact_store();
    // `EmptyLinkageRepo` never resolves a binding ⇒ `DomainAvailability::Unresolved`
    // ⇒ the required domain feature is genuinely unavailable (not a schema gap:
    // `crypto_features_config` enables `Domain` so the spec IS registered).
    let svc = service_with_selection_and_linkage(
        &db,
        Arc::clone(&store),
        Arc::new(ControllableFactRead::new(Arc::clone(&scenario))),
        Arc::new(EmptyLinkageRepo),
        crypto_features_config(),
        SelectionConfig {
            enabled_categories: vec![MarketCategory::Crypto],
            ..SelectionConfig::default()
        },
    );

    let plan = plan_request(&svc, model_spec_id, rc_id, window_start, window_end).await;
    let artifact = svc.build(plan).await.expect("build");
    assert!(
        artifact.coverage.pit_selection_candidates > 0,
        "the crypto market should enter the PIT funnel as a candidate",
    );
    assert_eq!(
        artifact.coverage.pit_selection_included, 0,
        "unresolved linkage ⇒ domain feature unavailable ⇒ market excluded",
    );
    assert!(
        artifact.coverage.pit_selection_excluded.other_count > 0,
        "exclusion must be attributed to ModelFeatureUnavailable (counted in other_count)",
    );
    assert!(
        artifact.examples.is_empty(),
        "no examples when selection excludes every market",
    );
}

fn crypto_instrument() -> DomainInstrumentKey {
    DomainInstrumentKey::binance_kline(
        &BinanceSymbol::parse("BTCUSDT").expect("symbol"),
        KlineInterval::OneMinute,
    )
}

/// A `Resolved` crypto linkage binding `MARKET_ID` to the Binance BTCUSDT
/// kline instrument, settled directly against the feature source (no
/// Chainlink cross-check needed).
fn resolved_crypto_linkage(
    derived_at: DateTime<Utc>,
    reference_at: DateTime<Utc>,
) -> NewMarketLinkage {
    let market_id = MarketId::new(MARKET_ID);
    let outcome = LinkageOutcome::Resolved(Box::new(ResolvedBinding {
        subject: MarketSubject::Crypto(CryptoSubject {
            asset: CryptoAsset::parse("BTC").expect("asset"),
            quote: CryptoQuote::parse("USD").expect("quote"),
            comparator: PriceComparator::UpVsReference,
            strike: None,
            reference_at: Some(reference_at),
            observation_at: reference_at + ChronoDuration::days(1),
            resolution_oracle: ResolutionOracle::BinanceKline {
                symbol: BinanceSymbol::parse("BTCUSDT").expect("symbol"),
                interval: KlineInterval::OneMinute,
            },
        }),
        source_bindings: vec![ResolvedSourceBinding {
            role: LinkageSourceRole::Feature,
            source_id: DomainSourceId::binance(),
            instrument_key: crypto_instrument(),
            available_at: derived_at,
            binding_hash: ContentHash::parse(format!("blake3:{}", "8".repeat(64)))
                .expect("binding hash"),
        }],
        grounding: GroundingProof { spans: Vec::new() },
        override_context: None,
    }));
    let metadata_hash = ContentHash::parse(format!("blake3:{}", "7".repeat(64))).expect("hash");
    NewMarketLinkage::from_derivation(MarketLinkageDerivation {
        market_id,
        outcome,
        confidence: Probability::ONE,
        resolver_tier: ResolverTier::Tier0Slug,
        resolver_version: ResolverVersion::FIRST,
        metadata_hash,
        effective_at: derived_at,
    })
    .expect("new linkage")
}

fn crypto_close_observation(event_time_ms: i64) -> DomainObservationRow {
    DomainObservationRow {
        family: DomainFamily::Crypto.as_str().to_owned(),
        source_id: DomainSourceId::binance(),
        instrument_key: crypto_instrument(),
        metric: DomainMetric::Close.as_str().to_owned(),
        value: ChDecimal64::from(dec!(100_000)),
        event_time: event_time_ms,
        publish_time: event_time_ms,
        ingestion_time: event_time_ms,
        schema_version: ChSchemaVersion::FIRST,
    }
}

async fn build_crypto_pit_dataset_with_resolved_linkage() -> TrainingDatasetArtifact {
    let (pool, _container) = setup_pg().await;
    let db = pool.connection().clone();
    let rc_id = seed_runtime_config(&db).await;
    let (window_start, window_end) = dataset_window();
    seed_catalog_with_category(&db, window_start, MarketCategory::Crypto).await;
    let model_spec_id = seed_model_spec_with_contract(
        &db,
        ModelInputContract::single_required("domain.crypto.distance_to_strike"),
    )
    .await;

    let as_of = sample_as_of(window_start);
    let as_of_ms = as_of.timestamp_millis();
    let domain_config = DomainConfig::default();
    let mut fact = pit_scenario(as_of_ms);
    let observed_ms = as_of_ms - 15_000;
    fact.domain_observations.insert(
        crypto_instrument(),
        vec![crypto_close_observation(observed_ms)],
    );
    let scenario = Arc::new(Mutex::new(fact));
    let store = temp_artifact_store();

    let linkage_repo = Arc::new(PgMarketLinkageRepository::new(db.clone()));
    let seeded_linkage = linkage_repo
        .append(resolved_crypto_linkage(
            as_of - ChronoDuration::hours(1),
            as_of - ChronoDuration::seconds(10),
        ))
        .await
        .expect("seed resolved crypto linkage");
    // `created_at` is the database-owned availability clock. This historical
    // fixture represents a linkage that had already been persisted before the
    // sampled decision, so backdate that clock explicitly instead of letting a
    // row inserted by this test at wall-clock "now" leak into a past replay.
    LinkageEntity::update_many()
        .col_expr(
            LinkageColumn::CreatedAt,
            Expr::value(as_of - ChronoDuration::seconds(30)),
        )
        .filter(LinkageColumn::LinkageId.eq(seeded_linkage.linkage_id))
        .exec(&db)
        .await
        .expect("backdate linkage availability");

    let svc = service_with_selection_and_linkage(
        &db,
        Arc::clone(&store),
        Arc::new(ControllableFactRead::new(Arc::clone(&scenario))),
        linkage_repo,
        crypto_features_config(),
        SelectionConfig {
            enabled_categories: vec![MarketCategory::Crypto],
            ..SelectionConfig::default()
        },
    );

    let plan = svc
        .plan(DatasetPlanRequest {
            model_spec_id,
            profile_ref: fixture_profile_ref(),
            research_program_hash: fixture_hash('4'),
            source_slice: quant_pivot_test_support::execution_pg_seed::source_slice_ref('5'),
            runtime_config_version_id: rc_id,
            window_start,
            window_end,
            pit_cutoff: window_end + chrono::Duration::seconds(60),
            sample_interval_secs: 60,
            horizons_secs: vec![60],
            knowledge_lag_secs: domain_config.crypto.availability_lag_secs,
            feature_schema_version: SchemaVersion::FIRST,
            sample_sources: default_sample_sources(),
            training_dataset_id: None,
            purpose: DatasetPurpose::Training,
        })
        .await
        .expect("plan");
    svc.build(plan).await.expect("build")
}

fn assert_crypto_pit_selection_includes_domain_feature(artifact: &TrainingDatasetArtifact) {
    assert!(
        artifact.coverage.pit_selection_candidates > 0,
        "the crypto market should enter the PIT funnel as a candidate",
    );
    assert!(
        artifact.coverage.pit_selection_included > 0,
        "a resolved linkage with a visible Binance observation must be Available, \
         never a hardcoded Unresolved that fails ModelFeatureUnavailable: excluded = {:?}",
        artifact.coverage.pit_selection_excluded,
    );
    assert_eq!(
        artifact.coverage.pit_selection_excluded.other_count, 0,
        "no market should be excluded via ModelFeatureUnavailable once the domain \
         evidence is genuinely available",
    );
    assert!(
        !artifact.examples.is_empty(),
        "the surviving crypto market must materialize training examples",
    );

    let distance_to_strike = FeatureName::from_static("domain.crypto.distance_to_strike");
    let computed = artifact.examples.iter().any(|example| {
        example
            .feature_vector
            .domain
            .as_ref()
            .and_then(|slice| slice.values.get(&distance_to_strike))
            .is_some_and(|cell| cell.value().is_some())
    });
    assert!(
        computed,
        "once selection genuinely includes the market, the domain feature-value \
         pipeline (build_domain_slice_inputs, already correct) must actually compute \
         domain.crypto.distance_to_strike from the same prefetched linkage/observations"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn pit_selection_includes_crypto_market_when_domain_feature_is_resolved_and_available() {
    let artifact = build_crypto_pit_dataset_with_resolved_linkage().await;
    assert_crypto_pit_selection_includes_domain_feature(&artifact);
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
        profile_ref: fixture_profile_ref(),
        research_program_hash: fixture_hash('4'),
        source_slice: quant_pivot_test_support::execution_pg_seed::source_slice_ref('5'),
        runtime_config_version_id: rc_id,
        window_start,
        window_end,
        pit_cutoff: window_end + chrono::Duration::seconds(60),
        sample_interval_secs: 60,
        horizons_secs: vec![60],
        knowledge_lag_secs: 10,
        feature_schema_version: SchemaVersion::FIRST,
        sample_sources: vec![TrainingSampleSource::HistoricalPit],
        training_dataset_id: None,
        purpose: DatasetPurpose::Training,
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
    let fact_read = Arc::new(ControllableFactRead::new(Arc::clone(&scenario)));
    let svc = service(
        &db,
        Arc::clone(&store),
        Arc::clone(&fact_read) as Arc<dyn QuantFactReadRepository>,
    );
    let plan = plan_request(&svc, model_spec_id, rc_id, window_start, window_end).await;
    let dataset_id = plan.training_dataset_id.clone();

    let leaky = LeakyPitEngine {
        token_id: TokenId::new(YES_TOKEN),
        leak_ms: 5_000,
        catalog: Arc::new(DurablePitSource::new(
            fact_read as Arc<dyn QuantFactReadRepository>,
            Arc::new(PgCatalogVersionRepository::new(
                db.clone(),
                GammaConfig::default().catalog_visibility_guard_secs,
            )),
            Arc::new(PgClobMarketInfoRepository::new(db.clone())),
        )),
    };
    let err = Box::pin(svc.build_with_pit_source(plan, &leaky))
        .await
        .expect_err("a future book must fail at the PIT resolver boundary");

    assert!(
        matches!(
            err,
            QuantError::Research(ResearchError::PitResolution { .. })
        ),
        "expected PitResolution, got {err:?}"
    );

    let row = PgTrainingDatasetRepository::new(db.clone())
        .find_by_id(&dataset_id)
        .await
        .expect("lookup")
        .expect("failed build audit row");
    assert_eq!(row.status, TrainingDatasetStatus::Failed);
    assert!(
        row.failure_detail
            .as_deref()
            .is_some_and(|detail| detail.contains("after source cutoff")),
        "failed build must retain the precise PIT boundary violation"
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
            trade_policy_artifact_id: plan_a.trade_policy_artifact_id.clone(),
            trade_policy_hash: plan_a.trade_policy_hash.clone(),
            trade_policy: plan_a.trade_policy.clone(),
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
        domain_observations: HashMap::new(),
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
    let manifest = ledger_manifest(
        &dataset_id,
        &model_spec_id,
        &rc_id,
        window_start,
        window_end,
        &hash,
        10,
    );
    let manifest_hash = dataset_manifest_hash(&manifest).expect("manifest hash");

    let dataset_repo = PgTrainingDatasetRepository::new(db.clone());
    dataset_repo
        .create_plan(NewTrainingDatasetPlan {
            training_dataset_id: dataset_id.clone(),
            model_spec_id: model_spec_id.clone(),
            window_start,
            window_end,
            purpose: DatasetPurpose::Training,
            knowledge_lag_secs: 10,
            sample_interval_secs: 3600,
            horizons_secs: TrainingHorizonsSecs(vec![3600]),
            feature_schema_version: Some(SchemaVersion::FIRST),
            sample_sources: Some(TrainingSampleSources(default_sample_sources())),
            runtime_config_version_id: rc_id,
        })
        .await
        .expect("create dataset plan");
    dataset_repo
        .start_build(&dataset_id)
        .await
        .expect("start dataset build");
    dataset_repo
        .complete_build(
            &dataset_id,
            CompleteTrainingDatasetBuild {
                status: TrainingDatasetStatus::Ready,
                feature_schema_hash: hash.clone(),
                factor_schema_hash: hash.clone(),
                label_schema_hash: hash.clone(),
                dataset_hash: hash.clone(),
                manifest_hash,
                manifest_json: manifest,
                artifact_bytes_hash: hash.clone(),
                parquet_uri: ArtifactUri::parse("file:///tmp/dataset.parquet").expect("uri"),
                sample_count: 10,
                coverage_json: DatasetCoverage::default(),
                failure_detail: None,
            },
        )
        .await
        .expect("complete dataset");

    let version_id = ModelVersionId::from_v7();
    PgModelRegistryRepository::new(db.clone())
        .create_model_version(NewModelVersion {
            model_version_id: version_id.clone(),
            model_spec_id,
            version: 2,
            artifact_hash: hash,
            profile_ref: fixture_profile_ref(),
            training_dataset_id: Some(dataset_id.clone()),
            trade_policy_artifact_id: None,
            trade_policy_hash: None,
            publish_path_set_id: None,
            metrics_json: serde_json::json!({}),
            training_objective_json: serde_json::json!({"kind": "not_trained"}),
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
            profile_ref: fixture_profile_ref(),
            research_program_hash: fixture_hash('4'),
            source_slice: quant_pivot_test_support::execution_pg_seed::source_slice_ref('5'),
            runtime_config_version_id: rc_id,
            window_start,
            window_end,
            pit_cutoff: window_end + chrono::Duration::seconds(60),
            sample_interval_secs: 60,
            horizons_secs: vec![60],
            knowledge_lag_secs: 10,
            feature_schema_version: SchemaVersion::FIRST,
            sample_sources: vec![TrainingSampleSource::HistoricalPit],
            training_dataset_id: None,
            purpose: DatasetPurpose::Training,
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
