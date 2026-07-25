//! Training-dataset system contracts: PIT correctness, leakage gate, settlement
//! maturity, and typed `training_dataset_id` FK wiring.

use std::{
    collections::{BTreeSet, HashMap},
    env, fs,
    sync::{Arc, Mutex},
    time::{SystemTime, UNIX_EPOCH},
};

use async_trait::async_trait;
use chrono::{DateTime, Duration as ChronoDuration, Utc};
use quant_pivot_compute::ComputeExecutor;
use quant_pivot_core::{
    pit::platform::ch_historical::DurablePitSource,
    service::training_dataset::{
        TrainingDatasetBuildConfig, TrainingDatasetService, TrainingDatasetServiceDeps,
        default_labelers,
    },
};
use quant_pivot_error::{QuantError, QuantResult, research::ResearchError, storage::StorageError};
use quant_pivot_models::{
    clickhouse::{
        BookL2LedgerRow, BookMicrostructureRow, BookStreamSessionRow, ChDecimal64, ChDigest,
        ChPrice, ChSchemaVersion, ChShares, ChUsd, DomainObservationRow, MarketResolutionFactInput,
        MarketResolutionRow, MidPriceBucketRow, TradeTapeRow,
    },
    domain::{
        data_plane::{DecisionBoundary, DecisionSource},
        market::{
            CATALOG_OBJECT_SCHEMA_VERSION, CatalogBatchCommit, CatalogEventCandidate,
            CatalogMarketCandidate, EventRegistryInfo, MarketRegistryInfo, NewCatalogEventChange,
            NewCatalogEventObject, NewCatalogMarketObject, NewCatalogSyncBatch, UpsertEvent,
            UpsertMarket, book::BookLevel, registry::TokenInfo,
        },
        quant::{
            CryptoSubject, GroundingProof, JobProgressSink, LinkageOutcome,
            MarketLinkageDerivation, MarketSubject, NewMarketLinkage, NewModelVersion,
            NoopProgressSink, PriceComparator, ResolutionOracle, ResolvedBinding,
            ResolvedSourceBinding,
        },
    },
    entities::{
        market::{Column as MarketColumn, Entity as MarketEntity},
        quant_market_linkage::{Column as LinkageColumn, Entity as LinkageEntity},
    },
    enums::{
        catalog::{
            CatalogChangeType, CatalogFilterReasonSet, CatalogSyncKind, CatalogTimestampQuality,
        },
        clickhouse::{ChCanonicalBookEventType, ChStreamSessionEndReason, ChStreamSessionState},
        common::{CategorySet, MarketCategory, TickSize},
        domain::{
            BinanceMarketSegment, DomainFamily, DomainMetric, KlineInterval, LinkageSourceRole,
            ResolverTier,
        },
        factor::FactorFamily,
        feature::EvidenceSourceKind,
        market::{EventStatus, MarketStatus},
        model::ModelFamily,
        quant::{DatasetPurpose, PublicationStatus, TrainingDatasetStatus},
    },
    hashing::CanonicalDigest,
    runtime_config::{
        DataQualityConfig, DecimalValue, DomainConfig, FactorsConfig, FeatureFamily,
        FeaturesConfig, SelectionConfig, TrainingConfig,
    },
    types::{
        ArtifactUri, BinanceSymbol, CatalogEventChangeId, CatalogEventObjectId,
        CatalogMarketChangeId, CatalogMarketObjectId, CatalogSyncBatchId, ContentHash, CryptoAsset,
        CryptoQuote, DatasetCoverage, DecisionPolicySnapshotId, DomainInstrumentKey,
        DomainSourceId, EventId, EvmBlockHash, EvmTransactionHash, MarketId, ModelInputContract,
        ModelSpecId, ModelTrainingContract, ModelVersionId, PayoutRatio, Price, Probability,
        ResolverVersion, SchemaVersion, Shares, TokenId, TrainingDatasetId, TrainingSampleSource,
        TrainingSampleSources, Usd, default_sample_sources, model_metrics::ModelVersionMetrics,
        model_training::ModelTrainingObjective, stable_name::FeatureName,
    },
};
use quant_pivot_repository::{
    postgres::{
        PgCalibrationArtifactRepository, PgCatalogLedgerRepository, PgClobMarketInfoRepository,
        PgMarketLinkageRepository, PgMarketRepository, PgModelRegistryRepository,
        PgPositionRepository, PgTradePolicyRepository, PgTrainingDatasetRepository,
    },
    traits::{
        CatalogLedgerRepository, MarketLinkageRepository, ModelRegistryRepository,
        QuantFactReadRepository, TrainingDatasetRepository,
    },
};
use quant_pivot_research::{
    artifact::{ArtifactStore, LocalArtifactStore},
    pit::{
        BookSnapshotAt, CanonicalBookEventRef, PointInTimeSnapshotSource, ResolvedMarketSnapshot,
    },
    training::{
        DatasetPlan, DatasetPlanRequest, LabelName, TrainingDatasetArtifact,
        TrainingDatasetBuilder, TrainingDatasetPlanner,
    },
};
use quant_pivot_system_tests::{
    postgres::setup_pg,
    support::{
        catalog_fixtures::{make_event, make_market},
        execution_pg_seed::fixture_profile_ref,
        model_spec_fixtures,
        policy_fixtures::bootstrap_default_policy_bundle,
        report_pipeline_harness::EmptyLinkageRepo,
        research_fixtures::{
            DatasetLedgerFixture, DatasetLedgerSeed, DatasetSourceSeed, seed_dataset_source,
        },
    },
};
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use sea_orm::{ColumnTrait, DatabaseConnection, EntityTrait, QueryFilter, sea_query::Expr};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

const EVENT_ID: &str = "evt-dataset-e2e";
const MARKET_ID: &str = "0xdatasete2e";
const YES_TOKEN: &str = "101";
const NO_TOKEN: &str = "202";

const TOKEN_PAYOUT_LABEL: LabelName = LabelName::from_static("token_payout_ratio");

fn resolution_row(
    payout_ratios: [PayoutRatio; 2],
    resolved_at: i64,
    observed_at: i64,
    source_byte: u8,
) -> MarketResolutionRow {
    MarketResolutionRow::seal(MarketResolutionFactInput {
        market_id: MarketId::new(MARKET_ID),
        token_ids: [TokenId::new(YES_TOKEN), TokenId::new(NO_TOKEN)],
        payout_ratios,
        resolved_at,
        observed_at,
        source_block_number: u64::from(source_byte),
        source_block_hash: EvmBlockHash::parse(format!(
            "0x{}",
            format!("{source_byte:02x}").repeat(32)
        ))
        .expect("resolution block hash"),
        source_transaction_hash: EvmTransactionHash::parse(format!(
            "0x{}",
            format!("{:02x}", source_byte.saturating_add(64)).repeat(32)
        ))
        .expect("resolution transaction hash"),
        source_log_index: u64::from(source_byte),
        source_checkpoint_hash: ContentHash::from_bytes([source_byte; 32]),
    })
    .expect("sealed resolution row")
}

async fn seed_runtime_config(db: &DatabaseConnection) -> DecisionPolicySnapshotId {
    bootstrap_default_policy_bundle(db, "dataset-e2e", "dataset e2e").await
}

fn dataset_window() -> (DateTime<Utc>, DateTime<Utc>) {
    // Catalog coverage begins at the durable commit visibility barrier. Use a
    // logical replay clock after that barrier; these fake facts do not depend
    // on wall-clock maturity, and backdating coverage would invalidate the PIT
    // contract this suite is meant to exercise.
    let start = Utc::now() + ChronoDuration::hours(1);
    // One sample at `start` when `sample_interval_secs == 60`.
    let end = start + ChronoDuration::seconds(60);
    (start, end)
}

const fn sample_as_of(window_start: DateTime<Utc>) -> DateTime<Utc> {
    window_start
}

struct DatasetRequestSeed {
    model_spec_id: ModelSpecId,
    decision_policy_snapshot_id: DecisionPolicySnapshotId,
    window_start: DateTime<Utc>,
    window_end: DateTime<Utc>,
    knowledge_lag_secs: u64,
    sample_sources: Vec<TrainingSampleSource>,
    purpose: DatasetPurpose,
}

impl DatasetRequestSeed {
    fn training(
        model_spec_id: ModelSpecId,
        decision_policy_snapshot_id: DecisionPolicySnapshotId,
        window_start: DateTime<Utc>,
        window_end: DateTime<Utc>,
    ) -> Self {
        Self {
            model_spec_id,
            decision_policy_snapshot_id,
            window_start,
            window_end,
            knowledge_lag_secs: 10,
            sample_sources: default_sample_sources(),
            purpose: DatasetPurpose::Training,
        }
    }

    #[must_use]
    const fn with_knowledge_lag(mut self, knowledge_lag_secs: u64) -> Self {
        self.knowledge_lag_secs = knowledge_lag_secs;
        self
    }

    #[must_use]
    fn with_sources(mut self, sample_sources: Vec<TrainingSampleSource>) -> Self {
        self.sample_sources = sample_sources;
        self
    }

    #[must_use]
    const fn with_purpose(mut self, purpose: DatasetPurpose) -> Self {
        self.purpose = purpose;
        self
    }

    async fn build(self, db: &DatabaseConnection) -> DatasetPlanRequest {
        let scope = format!(
            "training-dataset:{}:{}:{}:{}",
            self.model_spec_id,
            self.decision_policy_snapshot_id,
            self.window_start.timestamp_micros(),
            self.window_end.timestamp_micros()
        );
        let source_lineage = seed_dataset_source(
            db,
            DatasetSourceSeed {
                scope,
                profile_ref: fixture_profile_ref(),
                decision_policy_snapshot_id: self.decision_policy_snapshot_id,
                window_start: self.window_start,
                window_end: self.window_end,
                pit_cutoff: self.window_end + ChronoDuration::seconds(60),
            },
        )
        .await
        .expect("dataset source lineage");
        DatasetPlanRequest {
            model_spec_id: self.model_spec_id,
            source_lineage,
            cohort_manifest: None,
            window_start: self.window_start,
            window_end: self.window_end,
            sample_interval_secs: 60,
            horizons_secs: vec![60],
            knowledge_lag_secs: self.knowledge_lag_secs,
            feature_schema_version: SchemaVersion::FIRST,
            sample_sources: self.sample_sources,
            training_dataset_id: None,
            purpose: self.purpose,
        }
    }
}

struct ReadyDatasetSeed {
    model_spec_id: ModelSpecId,
    decision_policy_snapshot_id: DecisionPolicySnapshotId,
    window_start: DateTime<Utc>,
    window_end: DateTime<Utc>,
    hash: ContentHash,
    sample_count: u64,
}

impl ReadyDatasetSeed {
    async fn persist(self, db: &DatabaseConnection) -> TrainingDatasetId {
        let Self {
            model_spec_id,
            decision_policy_snapshot_id,
            window_start,
            window_end,
            hash,
            sample_count,
        } = self;
        let training_dataset_id = TrainingDatasetId::from_v7();
        let source_lineage = seed_dataset_source(
            db,
            DatasetSourceSeed {
                scope: format!("ready-training-dataset-{training_dataset_id}"),
                profile_ref: fixture_profile_ref(),
                decision_policy_snapshot_id,
                window_start,
                window_end,
                pit_cutoff: window_end + ChronoDuration::seconds(60),
            },
        )
        .await
        .expect("ready dataset source lineage");
        let fixture = DatasetLedgerFixture::try_new(DatasetLedgerSeed {
            training_dataset_id,
            model_spec_id,
            model_spec_definition_hash: model_spec_definition_hash(db, &model_spec_id).await,
            source_lineage,
            cohort_manifest: None,
            window_start,
            window_end,
            purpose: DatasetPurpose::Training,
            knowledge_lag_secs: 10,
            sample_interval_secs: 3_600,
            horizons_secs: vec![3_600],
            feature_schema_version: Some(SchemaVersion::FIRST),
            sample_sources: Some(TrainingSampleSources(default_sample_sources())),
            feature_schema_hash: hash,
            factor_schema_hash: hash,
            label_schema_hash: hash,
            semantic_dataset_hash: hash,
            source_fingerprint: hash,
            sample_count,
        })
        .expect("ready dataset ledger");
        let coverage = DatasetCoverage {
            planned_samples: sample_count,
            built_examples: sample_count,
            markets: 1,
            labels_available: sample_count,
            ..DatasetCoverage::default()
        };
        let repository = PgTrainingDatasetRepository::new(db.clone());
        repository
            .create_plan(fixture.plan.clone())
            .await
            .expect("ready dataset plan");
        repository
            .start_build(&training_dataset_id)
            .await
            .expect("start ready dataset");
        repository
            .complete_build(
                &training_dataset_id,
                fixture
                    .completion(
                        TrainingDatasetStatus::Ready,
                        hash,
                        ArtifactUri::parse(format!(
                            "s3://fixture/training-datasets/{training_dataset_id}.parquet"
                        ))
                        .expect("ready dataset artifact URI"),
                        coverage,
                        None,
                    )
                    .expect("ready dataset completion"),
            )
            .await
            .expect("complete ready dataset");
        training_dataset_id
    }
}

#[derive(Default)]
struct FactScenario {
    books: HashMap<TokenId, Vec<BookL2LedgerRow>>,
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

    async fn book_ledger_snapshot_at(
        &self,
        token_id: &TokenId,
        source_cutoff_ms: i64,
        decision_at_ms: i64,
    ) -> Result<Option<BookL2LedgerRow>, StorageError> {
        let scenario = self.scenario.lock().expect("lock");
        Ok(scenario.books.get(token_id).and_then(|rows| {
            rows.iter()
                .filter(|row| {
                    row.event_type == ChCanonicalBookEventType::Snapshot
                        && row.venue_event_time <= source_cutoff_ms
                        && row.persisted_time <= decision_at_ms
                })
                .max_by_key(|row| (row.venue_event_time, row.persisted_time, row.token_sequence))
                .cloned()
        }))
    }

    async fn book_l2_ledger_from(
        &self,
        token_id: &TokenId,
        stream_session_id: Uuid,
        from_sequence: u64,
        source_cutoff_ms: i64,
        decision_at_ms: i64,
    ) -> Result<Vec<BookL2LedgerRow>, StorageError> {
        let scenario = self.scenario.lock().expect("lock");
        Ok(scenario
            .books
            .get(token_id)
            .and_then(|rows| {
                rows.iter()
                    .filter(|row| {
                        row.stream_session_id == stream_session_id
                            && row.token_sequence == from_sequence
                            && row.venue_event_time <= source_cutoff_ms
                            && row.persisted_time <= decision_at_ms
                    })
                    .max_by_key(|row| (row.venue_event_time, row.persisted_time))
                    .cloned()
            })
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

    async fn book_ledger_snapshots_between(
        &self,
        token_ids: Vec<TokenId>,
        from_ms: i64,
        to_ms: i64,
        available_by_ms: i64,
    ) -> Result<Vec<BookL2LedgerRow>, StorageError> {
        let scenario = self.scenario.lock().expect("lock");
        let mut rows = Vec::new();
        for token_id in token_ids {
            if let Some(series) = scenario.books.get(&token_id) {
                for row in series {
                    if row.event_type == ChCanonicalBookEventType::Snapshot
                        && row.venue_event_time >= from_ms
                        && row.venue_event_time <= to_ms
                        && row.persisted_time <= available_by_ms
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
                .max_by_key(|row| {
                    (
                        row.resolved_at,
                        row.observed_at,
                        row.source_block_number,
                        row.source_log_index,
                        row.resolution_fact_hash,
                    )
                })
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
                    row.event_type == ChCanonicalBookEventType::Snapshot
                        && row.venue_event_time >= from_ms
                        && row.venue_event_time <= to_ms
                        && row.persisted_time <= decision_at_ms
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
                source_event_hash: ContentHash::parse(&format!("blake3:{}", "d".repeat(64)))
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

fn book_row(token: &str, event_time_ms: i64) -> BookL2LedgerRow {
    BookL2LedgerRow {
        stream_session_id: Uuid::nil(),
        shard_id: 0,
        token_id: TokenId::new(token),
        market_id: Some(MarketId::new(MARKET_ID)),
        token_sequence: 1,
        event_type: ChCanonicalBookEventType::Snapshot,
        bid_prices: vec![ChPrice::from(Price::new(Decimal::new(48, 2)))],
        bid_sizes: vec![ChShares::from(Shares::new(Decimal::from(100)))],
        ask_prices: vec![ChPrice::from(Price::new(Decimal::new(52, 2)))],
        ask_sizes: vec![ChShares::from(Shares::new(Decimal::from(100)))],
        old_tick_size: None,
        new_tick_size: None,
        trade_price: None,
        trade_side: None,
        trade_size: None,
        fee_rate_bps: None,
        venue_event_time: event_time_ms,
        ingress_time: event_time_ms,
        persisted_time: event_time_ms,
        event_hash: ChDigest::new([0; 32]),
        schema_version: BookL2LedgerRow::SCHEMA_VERSION,
    }
    .seal()
    .expect("seal ledger fixture")
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

async fn seed_catalog(db: &DatabaseConnection, window_start: DateTime<Utc>) {
    seed_catalog_with_category(db, window_start, MarketCategory::Sports).await;
}

struct CatalogSeedContext {
    source_effective_at: DateTime<Utc>,
    end_date: DateTime<Utc>,
    batch_id: CatalogSyncBatchId,
    event_version_id: CatalogEventChangeId,
    event_id: EventId,
    market_id: MarketId,
}

impl CatalogSeedContext {
    fn new(window_start: DateTime<Utc>) -> Self {
        Self {
            source_effective_at: window_start - ChronoDuration::days(1),
            end_date: window_start + ChronoDuration::days(7),
            batch_id: CatalogSyncBatchId::from_v7(),
            event_version_id: CatalogEventChangeId::from_v7(),
            event_id: EventId::new(EVENT_ID),
            market_id: MarketId::new(MARKET_ID),
        }
    }
}

fn catalog_fixture_hash(seed: char) -> ContentHash {
    ContentHash::parse(&format!("blake3:{}", seed.to_string().repeat(64)))
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
        tags: vec![category.to_string()],
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
        filter_reasons: CatalogFilterReasonSet::default(),
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

fn durable_catalog_commit(
    context: &CatalogSeedContext,
    category: MarketCategory,
) -> CatalogBatchCommit {
    let event_payload = event_registry_payload(context, category);
    let market_payload = market_registry_payload(context, category);
    let event_payload = serde_json::to_value(&event_payload).expect("event payload");
    let market_payload = serde_json::to_value(&market_payload).expect("market payload");
    let event_hash =
        CanonicalDigest::content_hash_typed("quant-pivot/catalog-event-object", 1, &event_payload)
            .expect("event content identity");
    let market_hash = CanonicalDigest::content_hash_typed(
        "quant-pivot/catalog-market-object",
        1,
        &market_payload,
    )
    .expect("market content identity");
    let event_object_id = CatalogEventObjectId::from_content_hash(&event_hash);
    let mut event_projection = current_event_projection(context, category);
    event_projection.content_hash = event_hash;
    let mut market_projection = current_market_projection(context, category);
    market_projection.content_hash = market_hash;

    CatalogBatchCommit {
        batch: NewCatalogSyncBatch {
            catalog_sync_batch_id: context.batch_id,
            sync_kind: CatalogSyncKind::Baseline,
            started_at: context.source_effective_at,
            fetched_at: context.source_effective_at,
            event_count: 1,
            market_count: 1,
            rejected_count: 0,
            batch_hash: catalog_fixture_hash('1'),
        },
        events: vec![CatalogEventCandidate {
            projection: event_projection,
            object: NewCatalogEventObject {
                event_object_id,
                content_hash: event_hash,
                schema_version: CATALOG_OBJECT_SCHEMA_VERSION,
                payload: event_payload.into(),
            },
            change: NewCatalogEventChange {
                event_change_id: context.event_version_id,
                catalog_sync_batch_id: context.batch_id,
                event_object_id,
                event_id: context.event_id.clone(),
                source_effective_at: context.source_effective_at,
                source_timestamp_quality: CatalogTimestampQuality::Source,
                change_type: CatalogChangeType::GammaScanUpsert,
            },
        }],
        markets: vec![CatalogMarketCandidate {
            projection: market_projection,
            object: NewCatalogMarketObject {
                market_object_id: CatalogMarketObjectId::from_content_hash(&market_hash),
                content_hash: market_hash,
                schema_version: CATALOG_OBJECT_SCHEMA_VERSION,
                payload: market_payload.into(),
            },
            market_change_id: CatalogMarketChangeId::from_v7(),
            catalog_sync_batch_id: context.batch_id,
            event_object_id,
            source_effective_at: context.source_effective_at,
            source_timestamp_quality: CatalogTimestampQuality::Source,
            source_created_at: Some(context.source_effective_at),
            change_type: CatalogChangeType::GammaScanUpsert,
        }],
    }
}

async fn seed_catalog_with_category(
    db: &DatabaseConnection,
    window_start: DateTime<Utc>,
    category: MarketCategory,
) {
    let context = CatalogSeedContext::new(window_start);
    PgCatalogLedgerRepository::new(db.clone())
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
    seed_model_spec_contract(db, ModelInputContract::single_required("book.mid")).await
}

async fn seed_model_spec_contract(
    db: &DatabaseConnection,
    input_contract: ModelInputContract,
) -> ModelSpecId {
    let model_spec_id = ModelSpecId::from_v7();
    PgModelRegistryRepository::new(db.clone())
        .create_model_spec(model_spec_fixtures::new_model_spec_fixture(
            model_spec_id,
            "dataset-e2e",
            ModelFamily::WeightedFactor,
            86_400,
            input_contract,
            ModelTrainingContract::settlement_default(),
        ))
        .await
        .expect("create spec");
    model_spec_id
}

async fn model_spec_definition_hash(
    db: &DatabaseConnection,
    model_spec_id: &ModelSpecId,
) -> ContentHash {
    PgModelRegistryRepository::new(db.clone())
        .find_model_spec(model_spec_id)
        .await
        .expect("load model spec")
        .expect("model spec exists")
        .definition_hash
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
    service_selection_linkage(
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
/// (`features_config` intentionally excludes `Domain` for the plain
/// Sports-market fixtures; a crypto-domain test needs it enabled so the
/// governed schema actually registers `domain.crypto.*` feature specs).
fn service_selection_linkage(
    db: &DatabaseConnection,
    store: Arc<dyn ArtifactStore>,
    fact_read: Arc<dyn QuantFactReadRepository>,
    linkage_repo: Arc<dyn MarketLinkageRepository>,
    features: FeaturesConfig,
    selection: SelectionConfig,
) -> TrainingDatasetService {
    TrainingDatasetService::new(
        TrainingDatasetServiceDeps {
            compute: Arc::new(ComputeExecutor::new().expect("test compute executor")),
            fact_read,
            catalog_repo: Arc::new(PgCatalogLedgerRepository::new(db.clone())),
            market_repo: Arc::new(PgMarketRepository::new(db.clone())),
            artifact_store: store,
            dataset_repo: Arc::new(PgTrainingDatasetRepository::new(db.clone())),
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
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| d.as_nanos());
    let dir = env::temp_dir().join(format!("quant-pivot-dataset-e2e-{nanos}"));
    fs::create_dir_all(&dir).expect("artifact dir");
    Arc::new(LocalArtifactStore::new(dir))
}

async fn plan_request(
    db: &DatabaseConnection,
    service: &TrainingDatasetService,
    model_spec_id: ModelSpecId,
    decision_policy_snapshot_id: DecisionPolicySnapshotId,
    window_start: DateTime<Utc>,
    window_end: DateTime<Utc>,
) -> DatasetPlan {
    let request = DatasetRequestSeed::training(
        model_spec_id,
        decision_policy_snapshot_id,
        window_start,
        window_end,
    )
    .build(db)
    .await;
    service.plan(request).await.expect("plan")
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

pub async fn historical_pit_no_build() {
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

    let plan = plan_request(&db, &svc, model_spec_id, rc_id, window_start, window_end).await;
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

pub async fn calibration_dataset_rejects_overlap() {
    // P1-8: a `purpose = Calibration` dataset whose window
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

    let hash = ContentHash::parse(&format!("blake3:{}", "a".repeat(64))).expect("hash");
    let _training_dataset_id = ReadyDatasetSeed {
        model_spec_id,
        decision_policy_snapshot_id: rc_id,
        window_start,
        window_end,
        hash,
        sample_count: 10,
    }
    .persist(&db)
    .await;

    let as_of_ms = sample_as_of(window_start).timestamp_millis();
    let scenario = Arc::new(Mutex::new(pit_scenario(as_of_ms)));
    let store = temp_artifact_store();
    let svc = service(
        &db,
        Arc::clone(&store),
        Arc::new(ControllableFactRead::new(Arc::clone(&scenario))),
    );

    let calibration_request =
        DatasetRequestSeed::training(model_spec_id, rc_id, window_start, window_end)
            .with_purpose(DatasetPurpose::Calibration)
            .build(&db)
            .await;
    let calibration_plan = svc
        .plan(calibration_request)
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

pub async fn build_before_no_row() {
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

    let plan = plan_request(&db, &svc, model_spec_id, rc_id, window_start, window_end).await;
    let dataset_id = plan.training_dataset_id;

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

pub async fn pit_selection_excludes_market() {
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
            min_liquidity_usd: DecimalValue::new(rust_decimal_macros::dec!(1000000)),
            ..SelectionConfig::default()
        },
    );

    let plan = plan_request(&db, &svc, model_spec_id, rc_id, window_start, window_end).await;
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

pub async fn pit_excludes_requires_feature() {
    let (pool, _container) = setup_pg().await;
    let db = pool.connection().clone();
    let rc_id = seed_runtime_config(&db).await;
    let (window_start, window_end) = dataset_window();
    seed_catalog_with_category(&db, window_start, MarketCategory::Crypto).await;
    let model_spec_id = seed_model_spec_contract(
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
    let svc = service_selection_linkage(
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

    let plan = plan_request(&db, &svc, model_spec_id, rc_id, window_start, window_end).await;
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
                market: BinanceMarketSegment::Spot,
                symbol: BinanceSymbol::parse("BTCUSDT").expect("symbol"),
                interval: KlineInterval::OneMinute,
            },
        }),
        source_bindings: vec![ResolvedSourceBinding {
            role: LinkageSourceRole::Feature,
            source_id: DomainSourceId::binance(),
            instrument_key: crypto_instrument(),
            available_at: derived_at,
            binding_hash: ContentHash::parse(&format!("blake3:{}", "8".repeat(64)))
                .expect("binding hash"),
        }],
        grounding: GroundingProof { spans: Vec::new() },
        override_context: None,
    }));
    let metadata_hash = ContentHash::parse(&format!("blake3:{}", "7".repeat(64))).expect("hash");
    let capability_registry_hash =
        ContentHash::parse(&format!("blake3:{}", "f".repeat(64))).expect("hash");
    NewMarketLinkage::from_derivation(MarketLinkageDerivation {
        market_id,
        domain_family: DomainFamily::Crypto,
        outcome,
        confidence: Probability::ONE,
        resolver_tier: ResolverTier::Tier0Slug,
        resolver_version: ResolverVersion::FIRST,
        metadata_hash,
        capability_registry_hash,
        effective_at: derived_at,
    })
    .expect("new linkage")
}

fn crypto_close_observation(event_time_ms: i64) -> DomainObservationRow {
    DomainObservationRow {
        family: DomainFamily::Crypto.to_string(),
        source_id: DomainSourceId::binance(),
        instrument_key: crypto_instrument(),
        metric: DomainMetric::Close.to_string(),
        value: ChDecimal64::from(dec!(100_000)),
        event_time: event_time_ms,
        publish_time: event_time_ms,
        ingestion_time: event_time_ms,
        schema_version: ChSchemaVersion::FIRST,
    }
}

async fn build_crypto_pit_linkage() -> TrainingDatasetArtifact {
    let (pool, _container) = setup_pg().await;
    let db = pool.connection().clone();
    let rc_id = seed_runtime_config(&db).await;
    let (window_start, window_end) = dataset_window();
    seed_catalog_with_category(&db, window_start, MarketCategory::Crypto).await;
    let model_spec_id = seed_model_spec_contract(
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

    let svc = service_selection_linkage(
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

    let request = DatasetRequestSeed::training(model_spec_id, rc_id, window_start, window_end)
        .with_knowledge_lag(domain_config.crypto.availability_lag_secs)
        .build(&db)
        .await;
    let plan = svc.plan(request).await.expect("plan");
    svc.build(plan).await.expect("build")
}

fn assert_crypto_pit_feature(artifact: &TrainingDatasetArtifact) {
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

pub async fn pit_selection_includes_available() {
    let artifact = build_crypto_pit_linkage().await;
    assert_crypto_pit_feature(&artifact);
}

pub async fn plan_estimates_keep_rate() {
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

    let request = DatasetRequestSeed::training(model_spec_id, rc_id, window_start, window_end)
        .with_sources(vec![TrainingSampleSource::HistoricalPit])
        .build(&db)
        .await;
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

pub async fn dataset_builder_rejects_features() {
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
    let plan = plan_request(&db, &svc, model_spec_id, rc_id, window_start, window_end).await;
    let dataset_id = plan.training_dataset_id;

    let leaky = LeakyPitEngine {
        token_id: TokenId::new(YES_TOKEN),
        leak_ms: 5_000,
        catalog: Arc::new(DurablePitSource::new(
            fact_read as Arc<dyn QuantFactReadRepository>,
            Arc::new(PgCatalogLedgerRepository::new(db.clone())),
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

pub async fn settlement_not_before_resolution() {
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

    let plan = plan_request(&db, &svc, model_spec_id, rc_id, window_start, window_end).await;
    let artifact = svc.build(plan).await.expect("build");
    assert!(artifact.coverage.labels_not_mature > 0);
    for example in &artifact.examples {
        assert!(
            !example
                .labels
                .iter()
                .any(|label| label.label_name == TOKEN_PAYOUT_LABEL),
            "settlement label must not appear before resolution"
        );
    }
}

pub async fn settlement_label_after_resolution() {
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
        vec![resolution_row(
            [
                PayoutRatio::try_new(Decimal::new(5, 1)).expect("half payout"),
                PayoutRatio::try_new(Decimal::new(5, 1)).expect("half payout"),
            ],
            as_of_ms + 30_000,
            as_of_ms + 30_000,
            1,
        )],
    );
    let scenario = Arc::new(Mutex::new(fact));
    let store = temp_artifact_store();
    let svc = service(
        &db,
        Arc::clone(&store),
        Arc::new(ControllableFactRead::new(scenario)),
    );

    let plan = plan_request(&db, &svc, model_spec_id, rc_id, window_start, window_end).await;
    let artifact = svc.build(plan).await.expect("build");
    assert!(
        artifact.examples.iter().any(|example| {
            example.labels.iter().any(|label| {
                label.label_name == TOKEN_PAYOUT_LABEL && label.value == Decimal::new(5, 1)
            })
        }),
        "expected exact split-payout label after resolution is visible in forward window"
    );
}

pub async fn plan_build_reuses_id() {
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

    let plan_a = plan_request(&db, &svc, model_spec_id, rc_id, window_start, window_end).await;
    let plan_b = plan_request(&db, &svc, model_spec_id, rc_id, window_start, window_end).await;
    assert_ne!(
        plan_a.training_dataset_id, plan_b.training_dataset_id,
        "each plan call mints a fresh id"
    );

    let mut build_request = plan_a.request.clone();
    build_request.training_dataset_id = Some(plan_a.training_dataset_id);
    let artifact = svc
        .build(DatasetPlan {
            request: build_request,
            training_dataset_id: plan_a.training_dataset_id,
            model_spec_definition_hash: plan_a.model_spec_definition_hash,
            samples: plan_a.samples.clone(),
            lot_samples: plan_a.lot_samples.clone(),
            exit_training_lots: plan_a.exit_training_lots.clone(),
            label_names: plan_a.label_names.clone(),
            trade_policy_artifact_id: plan_a.trade_policy_artifact_id,
            trade_policy_hash: plan_a.trade_policy_hash,
            trade_policy: plan_a.trade_policy.clone(),
        })
        .await
        .expect("build");
    assert_eq!(
        artifact.training_dataset_id, plan_a.training_dataset_id,
        "build must reuse the plan-assigned id"
    );
}

pub async fn build_status_no_mature() {
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

    let mut plan = plan_request(&db, &svc, model_spec_id, rc_id, window_start, window_end).await;
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

pub async fn build_failed_zero_examples() {
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

    let mut plan = plan_request(&db, &svc, model_spec_id, rc_id, window_start, window_end).await;
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

pub async fn build_book_decode_failures() {
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
    bad.bid_sizes.clear();
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

    let plan = plan_request(&db, &svc, model_spec_id, rc_id, window_start, window_end).await;
    let artifact = svc.build(plan).await.expect("build");
    assert!(
        artifact.coverage.book_decode_failures > 0,
        "mismatched typed book arrays must increment decode failures"
    );
}

pub async fn settlement_label_without_resolution() {
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
            vec![resolution_row(
                [PayoutRatio::ONE, PayoutRatio::ZERO],
                as_of_ms + 30_000,
                as_of_ms + 30_000,
                2,
            )],
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

    let plan = plan_request(&db, &svc, model_spec_id, rc_id, window_start, window_end).await;
    let artifact = svc.build(plan).await.expect("build");
    assert!(
        artifact.examples.iter().any(|example| {
            example
                .labels
                .iter()
                .any(|label| label.label_name == TOKEN_PAYOUT_LABEL)
        }),
        "settlement must not depend on microstructure extending past resolution"
    );
}

pub async fn model_version_training_typed() {
    let (pool, _container) = setup_pg().await;
    let db = pool.connection().clone();
    let rc_id = seed_runtime_config(&db).await;
    let model_spec_id = seed_model_spec(&db).await;
    let (window_start, window_end) = dataset_window();
    let hash = ContentHash::parse(&format!("blake3:{}", "b".repeat(64))).expect("hash");
    let dataset_id = ReadyDatasetSeed {
        model_spec_id,
        decision_policy_snapshot_id: rc_id,
        window_start,
        window_end,
        hash,
        sample_count: 10,
    }
    .persist(&db)
    .await;

    let version_id = ModelVersionId::from_v7();
    PgModelRegistryRepository::new(db.clone())
        .create_model_version(NewModelVersion {
            model_version_id: version_id,
            model_spec_id,
            version: 2,
            artifact_hash: hash,
            category_scope: None,
            profile_ref: fixture_profile_ref(),
            training_dataset_id: Some(dataset_id),
            trade_policy_artifact_id: None,
            trade_policy_hash: None,
            publish_path_set_id: None,
            derivation: NewModelVersion::training_derivation(),
            metrics: ModelVersionMetrics::not_measured("test fixture"),
            training_objective: ModelTrainingObjective::hand_authored("test fixture"),
            quality_gate_report: None,
            publication_status: PublicationStatus::Candidate,
            published_at: None,
            retired_at: None,
        })
        .await
        .expect("typed FK insert");

    let loaded = PgModelRegistryRepository::new(db)
        .find_model_version(&version_id)
        .await
        .expect("load")
        .expect("version");
    assert_eq!(loaded.training_dataset_id, Some(dataset_id));
}

pub async fn plan_count_respects_sources() {
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

    let default_plan =
        plan_request(&db, &svc, model_spec_id, rc_id, window_start, window_end).await;
    let historical_request =
        DatasetRequestSeed::training(model_spec_id, rc_id, window_start, window_end)
            .with_sources(vec![TrainingSampleSource::HistoricalPit])
            .build(&db)
            .await;
    let historical_only_plan = svc
        .plan(historical_request)
        .await
        .expect("historical-only plan");

    let default_count = svc
        .count_planned_samples(&default_plan)
        .expect("default count");
    let historical_count = svc
        .count_planned_samples(&historical_only_plan)
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
