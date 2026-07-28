//! Trainer and backtest system contracts: train a weighted model from a
//! frozen dataset, register a Candidate, replay its exact frozen input, and
//! persist immutable Evaluation evidence — all without ever touching a live
//! `BookStore` or deriving a child model from the holdout.
//!
//! Frozen Parquet is the sole source of the selected market, `FeatureCell`,
//! factor, and label bytes consumed by both training and backtest.

use std::{
    collections::BTreeMap,
    env, fs,
    io::ErrorKind,
    path::{Path, PathBuf},
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
};

use async_trait::async_trait;
use chrono::{DateTime, Duration as ChronoDuration, TimeZone, Utc};
use quant_pivot_compute::ComputeExecutor;
use quant_pivot_core::{
    app::ports::{
        backtest::{CoreBacktestPort, CoreBacktestPortDeps},
        cpcv_backtest::{CoreCpcvBacktestPort, CoreCpcvBacktestPortDeps},
    },
    service::{
        backtest::{BacktestInput, BacktestService, BacktestServiceDeps},
        model_calibration_fit::ModelCalibrationFitService,
        model_serving_preimage::{ModelServingPreimageDeps, ModelServingPreimageService},
        model_training::{
            ModelTrainerConfig, ModelTrainerService, ModelTrainerServiceDeps, TrainModelInput,
        },
        research_readiness::ResearchReadinessEvidenceService,
        trade_policy_evidence::{TradePolicyEvidenceVerifier, TradePolicyEvidenceVerifierDeps},
        trade_policy_preimage::{TradePolicyPreimageVerifier, TradePolicyPreimageVerifierDeps},
    },
};
use quant_pivot_error::{QuantError, research::ResearchError, storage::StorageError};
use quant_pivot_models::{
    domain::{
        api::{
            BacktestPathSetView, CpcvBacktestJobParams, FactorDefinitionListQuery,
            FitModelCalibratorRequest, RunBacktestRequest, RunCpcvBacktestRequest,
        },
        data_plane::DecisionClock,
        pagination::Paginated,
        ports::{
            BacktestPort, CpcvBacktestPort, FeedbackComparisonCandidateRef,
            FeedbackComparisonContract, FeedbackComparisonJobInput, FeedbackComparisonJobParams,
            FeedbackEvaluationUseRef, FeedbackLearningStageArtifactRef,
            ModelCalibrationFitJobParams, ModelCalibrationFitOutcome, ModelCalibrationFitPort,
        },
        quant::{
            CalibrationArtifactPayload, FactorDefinitionInfo, FactorRegistrationOutcome,
            FactorValueInfo, JobProgressSink, LatestFactorSnapshotBundleInfo,
            LatestFactorSnapshotInfo, ModelSpecInfo, ModelVersionInfo, NewFactorDefinition,
            NewFactorValue, NoopProgressSink, ResearchJobArtifactRef,
        },
    },
    entities::{
        market::{Column as MarketColumn, Entity as MarketEntity},
        quant_backtest_report::Entity as BacktestReportEntity,
        quant_calibration_artifact::Entity as CalibrationArtifactEntity,
        quant_factor_definition::Entity as FactorDefinitionEntity,
        quant_model_comparison_report::Entity as ComparisonReportEntity,
        quant_model_run::{Column, Entity},
        quant_model_version::Entity as ModelVersionEntity,
    },
    enums::{
        common::MarketCategory,
        factor::FactorFamily,
        model::ModelFamily,
        quant::{
            CalibrationKind, CalibrationMethod, DataQualityStatus, DatasetPurpose, FactorDirection,
            FeedbackStage, ModelRunErrorCode, ModelRunKind, ModelRunStatus, OutcomeSide,
            PublicationStatus, TrainingDatasetStatus,
        },
        runtime_config::ConfigResourceKind,
    },
    hashing::CanonicalDigest,
    runtime_config::{
        DataQualityConfig, DecisionPolicySnapshot, DomainConfig, FactorsConfig, FeatureFamily,
        FeaturesConfig, MomentumFeaturesConfig, RankLossKind, ResearchTrainingConfig,
        StructuralFeaturesConfig, TrainingOptimizerKind, wire::DecimalValue,
    },
    types::{
        ArtifactUri, BacktestPathSetId, BacktestReportId, ContentHash, DatasetSourceLineage,
        DecisionPolicySnapshotId, EventId, FactorDefinitionId, FeatureCell, FeatureStaleness,
        FeatureValue, FeatureVectorId, FeedbackComparisonArtifactId, FeedbackCycleId,
        FeedbackEvaluationUseId, FeedbackLearningStageArtifactId, MarketId, ModelInputContract,
        ModelRunId, ModelSpecId, ModelTrainingContract, ModelVersionId, OrderIntentId,
        POOLED_1H_CONTROL_PROFILE_ID, POOLED_1H_HORIZON_SECS, PositionId, Price, Probability,
        ResearchEvaluationTrack, ResearchJobId, ResearchJobProgress, SchemaVersion, Shares,
        SourceSliceManifest, TokenId, TradePolicyEvidenceBundleManifest, TrainingDatasetId,
        TrainingExampleId, TrainingSampleSource, TrainingSampleSources, Usd,
        builtin_research_profiles,
        factor::{FactorDefinitionRef, FactorExplanation, FactorServingPlane},
        model_metrics::{HeldOutMetricKind, ModelVersionMetricsDefinition},
        model_serving::ModelServingTradePolicyBinding,
        model_training::ModelTrainingObjectiveDefinition,
        stable_name::FeatureName,
    },
};
use quant_pivot_repository::{
    postgres::{
        PgBacktestPathSetRepository, PgBacktestReportRepository, PgCalibrationArtifactRepository,
        PgEventRepository, PgFactorRepository, PgMarketRepository,
        PgModelComparisonReportRepository, PgModelRegistryRepository, PgModelRunRepository,
        PgPolicyRepository, PgResearchReadinessEvidenceRepository, PgSourceSliceRepository,
        PgTradePolicyRepository, PgTrainingDatasetRepository,
    },
    traits::{
        BacktestPathSetRepository, BacktestReportRepository, CalibrationArtifactRepository,
        EventRepository, FactorRepository, MarketRepository, ModelComparisonReportRepository,
        ModelRegistryRepository, ModelRunRepository, PolicyRepository, TradePolicyRepository,
        TrainingDatasetRepository,
    },
};
use quant_pivot_research::{
    artifact::{ArtifactKey, ArtifactNamespace, ArtifactStore, LocalArtifactStore},
    execution_semantics::BookFidelity,
    factors::{FactorEngine, FactorValue, NormalizedFactor},
    features::{
        FeatureSchema, FeatureVector,
        names::{
            book::{BEST_ASK, MID, SPREAD_BPS, VISIBLE_LIQUIDITY_USD},
            market::CATEGORY,
        },
    },
    hashing::ResearchHasher,
    model::{ModelArtifact, PositionStateFeatures},
    selection::SelectedMarket,
    training::{
        DatasetHashContract, DatasetParquetCodec, HOLD_VS_EXIT_ALPHA_BPS, LabelName,
        LotTrainingContext, POLICY_NET_RETURN_BPS, RETURN_TO_HORIZON, TrainingDatasetArtifact,
        TrainingExample, TrainingLabel, dataset_source_fingerprint, label_names_for_sources,
    },
};
use quant_pivot_system_tests::{
    postgres::{ScenarioDatabase, setup_pg},
    support::{
        artifact_store::{
            ReadCountingArtifactStoreFixture, ReadTamperArtifactStoreFixture,
            VersionedArtifactStoreFixture,
        },
        catalog_fixtures::{make_event, make_market},
        model_serving_fixtures::{ModelDatasetLedgerFixture, ModelDatasetLedgerSeed},
        model_spec_fixtures,
        policy_fixtures::{activate_policy_bundle, bootstrap_policy_bundle},
        research_fixtures::{
            DatasetLedgerFixture, DatasetLedgerSeed, ReplayableSourceSliceFixture,
            bind_fixture_decision_capture, model_learning_cohort, persist_replayable_source_slice,
            seed_source_manifest,
        },
        trade_policy_fixtures::PublishedTradePolicyFixture,
    },
};
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use sea_orm::{
    ColumnTrait, DatabaseConnection, EntityTrait, PaginatorTrait, QueryFilter, sea_query::Expr,
};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

// Type alias to keep the BTreeMap key readable.
type FeatureName2 = FeatureName;

const EVENT_ID: &str = "evt-train-backtest-e2e";
const TICKS: i64 = 12;
const MARKETS_PER_TICK: usize = 20;
const BASE_TS: i64 = 1_700_000_000;
const TICK_INTERVAL_SECS: i64 = 3600;
const KNOWLEDGE_LAG_SECS: i64 = 10;

struct CancelAtPhase {
    cancel: CancellationToken,
    phase: &'static str,
}

impl JobProgressSink for CancelAtPhase {
    fn report(&self, progress: ResearchJobProgress) {
        if progress.phase == self.phase {
            self.cancel.cancel();
        }
    }
}

/// Observes the trainer's factor-registry boundary while delegating every
/// persistence operation to the real `PostgreSQL` repository.
struct RecordingFactorRepository {
    inner: PgFactorRepository,
    register_calls: AtomicUsize,
    inserted: AtomicUsize,
    already_present: AtomicUsize,
}

impl RecordingFactorRepository {
    const fn new(db: DatabaseConnection) -> Self {
        Self {
            inner: PgFactorRepository::new(db),
            register_calls: AtomicUsize::new(0),
            inserted: AtomicUsize::new(0),
            already_present: AtomicUsize::new(0),
        }
    }

    fn register_calls(&self) -> usize {
        self.register_calls.load(Ordering::SeqCst)
    }

    fn inserted(&self) -> usize {
        self.inserted.load(Ordering::SeqCst)
    }

    fn already_present(&self) -> usize {
        self.already_present.load(Ordering::SeqCst)
    }
}

#[async_trait]
impl FactorRepository for RecordingFactorRepository {
    async fn register_definitions(
        &self,
        definitions: Vec<NewFactorDefinition>,
    ) -> Result<Vec<FactorRegistrationOutcome>, StorageError> {
        self.register_calls.fetch_add(1, Ordering::SeqCst);
        let outcomes = self.inner.register_definitions(definitions).await?;
        for outcome in &outcomes {
            match outcome {
                FactorRegistrationOutcome::Inserted(_) => {
                    self.inserted.fetch_add(1, Ordering::SeqCst);
                }
                FactorRegistrationOutcome::AlreadyPresent(_) => {
                    self.already_present.fetch_add(1, Ordering::SeqCst);
                }
            }
        }
        Ok(outcomes)
    }

    async fn create_values(
        &self,
        values: Vec<NewFactorValue>,
    ) -> Result<Vec<FactorValueInfo>, StorageError> {
        self.inner.create_values(values).await
    }

    async fn find_definition(
        &self,
        factor_definition_id: &FactorDefinitionId,
    ) -> Result<Option<FactorDefinitionInfo>, StorageError> {
        self.inner.find_definition(factor_definition_id).await
    }

    async fn find_definitions_by_ids(
        &self,
        factor_definition_ids: &[FactorDefinitionId],
    ) -> Result<Vec<FactorDefinitionInfo>, StorageError> {
        self.inner
            .find_definitions_by_ids(factor_definition_ids)
            .await
    }

    async fn page_definitions(
        &self,
        query: FactorDefinitionListQuery,
    ) -> Result<Paginated<FactorDefinitionInfo>, StorageError> {
        self.inner.page_definitions(query).await
    }

    async fn list_values_for_run(
        &self,
        model_run_id: &ModelRunId,
    ) -> Result<Vec<FactorValueInfo>, StorageError> {
        self.inner.list_values_for_run(model_run_id).await
    }

    async fn find_values_by_vectors(
        &self,
        feature_vector_ids: &[FeatureVectorId],
    ) -> Result<Vec<FactorValueInfo>, StorageError> {
        self.inner.find_values_by_vectors(feature_vector_ids).await
    }

    async fn recent_values(
        &self,
        factor_definition_ids: &[FactorDefinitionId],
        from: DateTime<Utc>,
        until: DateTime<Utc>,
    ) -> Result<Vec<FactorValueInfo>, StorageError> {
        self.inner
            .recent_values(factor_definition_ids, from, until)
            .await
    }

    async fn latest_snapshot(
        &self,
        factor_definition_id: &FactorDefinitionId,
        market_id: &MarketId,
        model_version_id: &ModelVersionId,
        available_by: DateTime<Utc>,
    ) -> Result<Option<LatestFactorSnapshotInfo>, StorageError> {
        self.inner
            .latest_snapshot(
                factor_definition_id,
                market_id,
                model_version_id,
                available_by,
            )
            .await
    }

    async fn latest_snapshot_bundle(
        &self,
        factor_definition_ids: &[FactorDefinitionId],
        market_id: &MarketId,
        model_version_id: &ModelVersionId,
        available_by: DateTime<Utc>,
    ) -> Result<Option<LatestFactorSnapshotBundleInfo>, StorageError> {
        self.inner
            .latest_snapshot_bundle(
                factor_definition_ids,
                market_id,
                model_version_id,
                available_by,
            )
            .await
    }
}

fn settlement() -> LabelName {
    LabelName::new("token_payout_ratio")
}

fn market_id(tick: i64, i: usize) -> MarketId {
    MarketId::new(format!("0x{tick}_{i}"))
}

fn token_id(tick: i64, i: usize) -> TokenId {
    let index = i64::try_from(i).expect("fixture market index");
    TokenId::new((1_000_000_i64 + tick * 1_000 + index).to_string())
}

fn no_token_id(tick: i64, i: usize) -> TokenId {
    let index = i64::try_from(i).expect("fixture market index");
    TokenId::new((9_000_000_i64 + tick * 1_000 + index).to_string())
}

fn as_of_for(tick: i64) -> DateTime<Utc> {
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
        (MID, FeatureValue::Probability(Probability::new(dec!(0.5)))),
        (
            BEST_ASK,
            FeatureValue::Probability(Probability::new(dec!(0.51))),
        ),
        (
            VISIBLE_LIQUIDITY_USD,
            FeatureValue::Usd(Usd::new(liquidity_usd(i))),
        ),
        (SPREAD_BPS, FeatureValue::Bps(dec!(400))),
        // Non-crypto category: this suite exercises frozen liquidity inputs,
        // not the crypto domain-weight publish invariant.
        (CATEGORY, FeatureValue::Category(MarketCategory::Politics)),
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
    let factor_serving_plane = FactorPlaneFixture::weighted();
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
            let mut example = TrainingExample {
                example_id: TrainingExampleId::from_v7(),
                market_id: market.clone(),
                token_id: token.clone(),
                selected_market: SelectedMarket {
                    market_id: market,
                    event_id: EventId::new(EVENT_ID),
                    category: MarketCategory::Politics,
                    primary_token_id: token,
                    secondary_token_id: Some(no_token_id(tick, i)),
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
                factor_values: FactorPlaneFixture::values(&factor_serving_plane, strength),
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
            };
            bind_fixture_decision_capture(&mut example);
            out.push(example);
        }
    }
    out
}

struct PolicyLabelFixture {
    label_name: LabelName,
    horizon_secs: u64,
}

impl Default for PolicyLabelFixture {
    fn default() -> Self {
        Self {
            label_name: POLICY_NET_RETURN_BPS,
            horizon_secs: 0,
        }
    }
}

impl PolicyLabelFixture {
    fn relabel(&self, examples: Vec<TrainingExample>) -> Vec<TrainingExample> {
        examples
            .into_iter()
            .map(|mut example| {
                let positive = example
                    .labels
                    .first()
                    .is_some_and(|label| label.value > Decimal::ZERO);
                example.labels = vec![TrainingLabel {
                    label_name: self.label_name.clone(),
                    horizon_secs: self.horizon_secs,
                    value: if positive { dec!(100) } else { dec!(-100) },
                    is_resolved: true,
                    matured_at: example.decision_at() + ChronoDuration::seconds(1),
                }];
                example
            })
            .collect()
    }
}

/// Independent later-window examples used only by reusable evaluation.
fn evaluation_examples() -> Vec<TrainingExample> {
    shift_examples(examples(), ChronoDuration::days(30))
}

fn calibration_examples() -> Vec<TrainingExample> {
    shift_examples(examples(), ChronoDuration::days(60))
        .into_iter()
        .enumerate()
        .map(|(index, mut example)| {
            if index / MARKETS_PER_TICK % 2 == 1 {
                for label in &mut example.labels {
                    label.value = Decimal::ONE - label.value;
                }
            }
            example
        })
        .collect()
}

fn shift_examples(examples: Vec<TrainingExample>, shift: ChronoDuration) -> Vec<TrainingExample> {
    examples
        .into_iter()
        .map(|mut example| {
            let decision_at = example.decision_at() + shift;
            example.example_id = TrainingExampleId::from_v7();
            example.decision_boundary =
                DecisionClock::new(u64::try_from(KNOWLEDGE_LAG_SECS).expect("knowledge lag"))
                    .boundary(decision_at)
                    .expect("evaluation boundary");
            example.feature_vector.decision_at = decision_at;
            for label in &mut example.labels {
                label.matured_at = decision_at + ChronoDuration::seconds(1);
            }
            bind_fixture_decision_capture(&mut example);
            example
        })
        .collect()
}

struct ClassicalDatasetFixture {
    rows: Vec<TrainingExample>,
    label_name: LabelName,
    label_horizon_secs: u64,
}

impl ClassicalDatasetFixture {
    fn for_family(family: ModelFamily) -> Self {
        let logistic = family == ModelFamily::ClassicalLogisticRegression;
        let label_name = if logistic {
            settlement()
        } else {
            RETURN_TO_HORIZON
        };
        let label_horizon_secs = if logistic { 0 } else { POOLED_1H_HORIZON_SECS };
        let rows = examples()
            .into_iter()
            .map(|mut example| {
                let positive = example
                    .labels
                    .first()
                    .is_some_and(|label| label.value > Decimal::ZERO);
                example.factor_values.clear();
                example.labels = vec![TrainingLabel {
                    label_name: label_name.clone(),
                    horizon_secs: label_horizon_secs,
                    value: if logistic {
                        if positive {
                            Decimal::ONE
                        } else {
                            Decimal::ZERO
                        }
                    } else if positive {
                        dec!(100)
                    } else {
                        dec!(-100)
                    },
                    is_resolved: true,
                    matured_at: example.decision_at()
                        + ChronoDuration::seconds(
                            i64::try_from(label_horizon_secs.max(1)).expect("classical horizon"),
                        ),
                }];
                example
            })
            .collect();
        Self {
            rows,
            label_name,
            label_horizon_secs,
        }
    }
}

fn exit_examples() -> Vec<TrainingExample> {
    examples()
        .into_iter()
        .enumerate()
        .map(|(index, mut example)| {
            let positive = index % MARKETS_PER_TICK >= MARKETS_PER_TICK / 2;
            let decision_at = example.decision_at();
            example.sample_source = TrainingSampleSource::ExitDecision;
            example.labels = vec![TrainingLabel {
                label_name: HOLD_VS_EXIT_ALPHA_BPS,
                horizon_secs: 0,
                value: if positive { dec!(50) } else { dec!(-50) },
                is_resolved: true,
                matured_at: decision_at + ChronoDuration::seconds(1),
            }];
            example.lot_context = Some(LotTrainingContext {
                order_intent_id: OrderIntentId::from_v7(),
                position_id: PositionId::from_v7(),
                outcome_side: OutcomeSide::Yes,
                remaining_shares: Shares::new(dec!(100)),
                avg_price: Price::new(dec!(0.5)),
                peak_mark: Some(Price::new(dec!(0.6))),
                opened_at: decision_at - ChronoDuration::hours(1),
                max_hold_secs: 86_400,
            });
            example.position_state = Some(PositionStateFeatures {
                unrealized_pnl_pct: Some(if positive { dec!(0.1) } else { dec!(-0.1) }),
                time_in_trade_ratio: if positive { dec!(0.75) } else { dec!(0.25) },
                peak_mark_drawdown: Some(if positive { dec!(0.05) } else { dec!(0.25) }),
            });
            example.book_fidelity = Some(BookFidelity::FullL2);
            example
        })
        .collect()
}

// ── Postgres catalog + ledger seeding ───────────────────────────────────────────────────────────────────────────────

/// Governed training knobs exercised through `TrainingObjectiveSpec::from_runtime_config`.
const fn e2e_research_training() -> ResearchTrainingConfig {
    ResearchTrainingConfig {
        rank_loss: RankLossKind::RankIcWeightedRanknet,
        optimizer: TrainingOptimizerKind::CoordinateSearch,
        lambda_tail: DecimalValue::new(rust_decimal_macros::dec!(0.5)),
        tail_fraction: DecimalValue::new(rust_decimal_macros::dec!(0.10)),
        lambda_turnover: DecimalValue::new(rust_decimal_macros::dec!(0.2)),
        lambda_l2: DecimalValue::new(rust_decimal_macros::dec!(0.01)),
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

impl E2eReplaySlice {
    fn fixture() -> Self {
        // PriceBook + MarketMetadata only. Cap every lookback-driving window so
        // `features.max_lookback_secs` (→ CPCV `min_embargo_secs`) stays well
        // below the 1h tick spacing — default 3600s micro / 86400s tape windows
        // embargo entire train partitions on this 4-tick timeline.
        Self {
            features: FeaturesConfig {
                enabled_feature_families: vec![
                    FeatureFamily::PriceBook,
                    FeatureFamily::MarketMetadata,
                ],
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
                enabled_factor_families: vec![FactorFamily::Liquidity, FactorFamily::Momentum],
                ..FactorsConfig::default()
            },
            // The serving head consumes OutcomeAlpha momentum factors while
            // liquidity remains available as Context. Domain factors stay
            // disabled so every frozen Politics row has an exact applicable
            // serving plane.
            domain: DomainConfig::disabled(),
            // Frozen data-quality contract used to derive schema bindings.
            data_quality: DataQualityConfig {
                max_book_age_ms: 60_000,
                max_feature_bucket_age_secs: 120,
                ..DataQualityConfig::default()
            },
            max_book_staleness_ms: 60_000,
        }
    }
}

fn e2e_runtime_config() -> DecisionPolicySnapshot {
    let mut config = DecisionPolicySnapshot::default();
    let replay = E2eReplaySlice::fixture();
    config.profile_artifacts.features.definition = replay.features;
    config.profile_artifacts.scoring.definition = replay.factors;
    config.profile_artifacts.domain.definition = replay.domain;
    config.recommendation.data_quality = replay.data_quality;
    config
        .profile_artifacts
        .research_method
        .training
        .max_book_staleness_ms = replay.max_book_staleness_ms;
    let portfolio = &mut config.execution_risk.portfolio;
    portfolio.budget.total_budget_usd = DecimalValue::new(dec!(5_000));
    portfolio.budget.min_recommendation_usd = DecimalValue::new(dec!(10));
    portfolio.budget.max_single_recommendation_usd = DecimalValue::new(dec!(100));
    portfolio.constraints.max_market_exposure_usd = DecimalValue::new(dec!(100));
    portfolio.constraints.max_event_exposure_usd = DecimalValue::new(dec!(2_000));
    portfolio.constraints.max_category_exposure_usd = DecimalValue::new(dec!(5_000));
    portfolio.constraints.max_correlated_exposure_usd = DecimalValue::new(dec!(5_000));
    config.profile_artifacts.research_method.research.training = e2e_research_training();
    // Twelve decision groups provide enough independent replay samples for
    // Platt calibration while still partitioning exactly into four CPCV/PBO
    // blocks for the governed validation contract below.
    config
        .profile_artifacts
        .research_method
        .research
        .validation
        .cpcv
        .n_groups = 4;
    config
        .profile_artifacts
        .research_method
        .research
        .validation
        .cpcv
        .k_test = 2;
    config
        .profile_artifacts
        .research_method
        .research
        .validation
        .pbo
        .block_count = 4;
    config
        .profile_artifacts
        .research_method
        .research
        .validation
        .trials
        .lambda_multipliers = vec![
        DecimalValue::new(rust_decimal_macros::dec!(1)),
        DecimalValue::new(rust_decimal_macros::dec!(0.5)),
    ];
    config
        .profile_artifacts
        .research_method
        .research
        .validation
        .trials
        .rank_loss_kinds = vec![RankLossKind::RankIcWeightedRanknet];
    config
        .profile_artifacts
        .research_method
        .research
        .validation
        .trials
        .max_trials = 4;
    config
        .profile_artifacts
        .research_method
        .research
        .validation
        .purge
        .embargo_pct = DecimalValue::new(rust_decimal_macros::dec!(0.02));
    config
}

async fn seed_runtime_config(db: &DatabaseConnection) -> DecisionPolicySnapshotId {
    let config = e2e_runtime_config();
    bootstrap_policy_bundle(
        &PgPolicyRepository::new(db.clone()),
        &config,
        "train-backtest-e2e",
        "integration test",
    )
    .await
}

struct ModelSpecFixture;

impl ModelSpecFixture {
    async fn persist(
        db: &DatabaseConnection,
        name: &str,
        family: ModelFamily,
        prediction_horizon_secs: i64,
        input_contract: ModelInputContract,
        training_contract: ModelTrainingContract,
    ) -> ModelSpecInfo {
        let model_spec_id = ModelSpecId::from_v7();
        let repository = PgModelRegistryRepository::new(db.clone());
        repository
            .create_model_spec(model_spec_fixtures::new_model_spec_fixture(
                model_spec_id,
                name,
                family,
                prediction_horizon_secs,
                input_contract,
                training_contract,
            ))
            .await
            .expect("persist model spec");
        repository
            .find_model_spec(&model_spec_id)
            .await
            .expect("load model spec")
            .expect("persisted model spec")
    }

    async fn weighted(db: &DatabaseConnection) -> ModelSpecInfo {
        Self::persist(
            db,
            "train-backtest-e2e",
            ModelFamily::WeightedFactor,
            model_spec_fixtures::pooled_horizon_secs(),
            ModelInputContract::single_required("book.mid"),
            ModelTrainingContract::settlement_default(),
        )
        .await
    }
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
            market.no_token_id = no_token_id(tick, i);
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

struct SeededDataset {
    id: TrainingDatasetId,
    hash: ContentHash,
    factor_serving_plane: FactorServingPlane,
}

struct FactorPlaneFixture;

impl FactorPlaneFixture {
    fn weighted() -> FactorServingPlane {
        let policy = e2e_runtime_config();
        FactorEngine::new(
            &policy.profile_artifacts.scoring.definition,
            &policy.profile_artifacts.features.definition,
            &policy.profile_artifacts.domain.definition,
            None,
        )
        .serving_plane()
        .expect("weighted fixture factor plane")
        .clone()
    }

    fn values(plane: &FactorServingPlane, strength: Decimal) -> Vec<FactorValue> {
        plane
            .definitions()
            .iter()
            .enumerate()
            .map(|(index, revision)| {
                let definition = revision.definition();
                let score = if index % 2 == 0 {
                    strength
                } else {
                    Decimal::ONE - strength
                };
                let direction = definition
                    .contribution_direction(score)
                    .expect("fixture raw factor projects a contribution direction");
                FactorValue {
                    definition_id: revision.factor_definition_id(),
                    name: definition.name.clone(),
                    family: definition.family,
                    raw_value: Some(score),
                    normalization: NormalizedFactor::cross_section(Probability::new(score)),
                    direction,
                    confidence: Probability::ONE,
                    explanation: FactorExplanation {
                        headline: format!("{} governed fixture score", definition.name),
                        drivers: Vec::new(),
                    },
                    input_feature_refs: definition.input_features.clone(),
                }
            })
            .collect()
    }

    fn rebind(values: &[FactorValue], plane: &FactorServingPlane) -> Vec<FactorValue> {
        let by_name = values
            .iter()
            .map(|value| (value.name.clone(), value))
            .collect::<BTreeMap<_, _>>();
        assert_eq!(
            by_name.len(),
            values.len(),
            "fixture factor values must have unique names"
        );
        assert_eq!(
            by_name.len(),
            plane.definitions().len(),
            "fixture factor values must cover the whole serving plane"
        );
        plane
            .definitions()
            .iter()
            .map(|revision| {
                let definition = revision.definition();
                let mut rebound = (*by_name
                    .get(&definition.name)
                    .expect("fixture value exists for every governed factor"))
                .clone();
                rebound.definition_id = revision.factor_definition_id();
                rebound.family = definition.family;
                rebound.direction = rebound
                    .raw_value
                    .and_then(|raw| definition.contribution_direction(raw))
                    .unwrap_or(FactorDirection::Neutral);
                rebound
                    .input_feature_refs
                    .clone_from(&definition.input_features);
                rebound
                    .validate_against(revision)
                    .expect("rebound fixture value must project the exact factor revision");
                rebound
            })
            .collect()
    }

    fn drifted(plane: &FactorServingPlane) -> FactorServingPlane {
        let mut definitions = plane.definitions().to_vec();
        let original = definitions.first().expect("non-empty factor plane").clone();
        let mut definition = original.definition().clone();
        definition
            .computation
            .semantic_key
            .push_str("+quant-pivot/system-test-drift@1");
        definitions[0] = FactorDefinitionRef::try_seal(
            definition,
            original.feature_contract_hash(),
            original.input_schema_version(),
            original.output_schema_version(),
        )
        .expect("seal drifted factor revision");
        FactorServingPlane::try_seal(definitions).expect("seal drifted factor plane")
    }
}

struct TrainingDatasetSeed<'a> {
    model_spec: &'a ModelSpecInfo,
    policy_snapshot_id: DecisionPolicySnapshotId,
    label_name: LabelName,
    examples: Vec<TrainingExample>,
    purpose: DatasetPurpose,
    scope: &'a str,
    factor_serving_plane: Option<FactorServingPlane>,
}

struct DatasetBuildContext<'a> {
    model_spec: &'a ModelSpecInfo,
    policy_snapshot_id: DecisionPolicySnapshotId,
    examples: Vec<TrainingExample>,
    purpose: DatasetPurpose,
    scope: &'a str,
    dataset_id: TrainingDatasetId,
    window_start: DateTime<Utc>,
    window_end: DateTime<Utc>,
    runtime_config_hash: ContentHash,
    feature_schema_hash: ContentHash,
    factor_serving_plane: FactorServingPlane,
    sample_sources: TrainingSampleSources,
    label_schema_hash: ContentHash,
    dataset_hash: ContentHash,
    trade_policy: Option<ModelServingTradePolicyBinding>,
}

impl<'a> DatasetBuildContext<'a> {
    async fn prepare(db: &DatabaseConnection, seed: TrainingDatasetSeed<'a>) -> Self {
        let TrainingDatasetSeed {
            model_spec,
            policy_snapshot_id,
            label_name,
            mut examples,
            purpose,
            scope,
            factor_serving_plane,
        } = seed;
        let model_spec_id = model_spec.model_spec_id;
        let model_family = model_spec.model_family;
        let dataset_id = TrainingDatasetId::from_v7();
        let window_start = examples
            .iter()
            .map(TrainingExample::decision_at)
            .min()
            .expect("dataset examples");
        let window_end = examples
            .iter()
            .map(TrainingExample::decision_at)
            .max()
            .expect("dataset examples")
            + ChronoDuration::hours(1);
        let policy = PgPolicyRepository::new(db.clone())
            .load_snapshot(&policy_snapshot_id)
            .await
            .expect("load dataset policy snapshot")
            .expect("dataset policy snapshot");
        let runtime = &policy.snapshot;
        let feature_schema = FeatureSchema::build(&runtime.profile_artifacts.features.definition)
            .expect("feature schema");
        let feature_schema_hash =
            ResearchHasher::feature_schema(&feature_schema).expect("feature hash");
        let canonical_plane = if model_family.is_classical() {
            FactorServingPlane::try_empty().expect("canonical factor-free plane")
        } else {
            FactorEngine::new(
                &runtime.profile_artifacts.scoring.definition,
                &runtime.profile_artifacts.features.definition,
                &runtime.profile_artifacts.domain.definition,
                None,
            )
            .serving_plane()
            .expect("factor serving plane")
            .clone()
        };
        let factor_serving_plane = factor_serving_plane.unwrap_or(canonical_plane);
        if model_family.is_classical() {
            assert!(
                examples
                    .iter()
                    .all(|example| example.factor_values.is_empty()),
                "classical fixture rows must be factor-free before encoding"
            );
        } else {
            for example in &mut examples {
                example.factor_values =
                    FactorPlaneFixture::rebind(&example.factor_values, &factor_serving_plane);
            }
        }
        let mut sample_sources = Vec::new();
        for source in examples.iter().map(|example| example.sample_source) {
            if !sample_sources.contains(&source) {
                sample_sources.push(source);
            }
        }
        let sample_sources = TrainingSampleSources::try_from(sample_sources)
            .expect("training fixture sample sources are canonical");
        let label_names = label_names_for_sources(
            sample_sources.as_slice(),
            model_spec
                .training_contract
                .trade_policy_artifact_id
                .is_some(),
        );
        assert!(
            label_names.contains(&label_name),
            "training fixture target label must belong to its canonical Dataset label schema"
        );
        let label_schema_hash =
            ResearchHasher::label_schema(&label_names).expect("label schema hash");
        let dataset_hash = TrainingDatasetArtifact::compute_dataset_hash(
            DatasetHashContract {
                model_spec_id: &model_spec_id,
                model_family,
                window_start,
                window_end,
                purpose,
                feature_schema_hash: &feature_schema_hash,
                factor_serving_plane: &factor_serving_plane,
                label_schema_hash: &label_schema_hash,
            },
            &examples,
        )
        .expect("semantic dataset hash");
        let trade_policy = match model_spec.training_contract.trade_policy_artifact_id {
            Some(artifact_id) => {
                let policy = PgTradePolicyRepository::new(db.clone())
                    .find(&artifact_id)
                    .await
                    .expect("load fixture trade policy")
                    .expect("fixture trade policy");
                Some(ModelServingTradePolicyBinding {
                    artifact_id,
                    content_hash: policy.content_hash,
                })
            }
            None => None,
        };

        Self {
            model_spec,
            policy_snapshot_id,
            examples,
            purpose,
            scope,
            dataset_id,
            window_start,
            window_end,
            runtime_config_hash: policy.snapshot_hash,
            feature_schema_hash,
            factor_serving_plane,
            sample_sources,
            label_schema_hash,
            dataset_hash,
            trade_policy,
        }
    }

    async fn persist_source(
        &self,
        db: &DatabaseConnection,
        store: &Arc<dyn ArtifactStore>,
    ) -> DatasetSourceLineage {
        let profile = builtin_research_profiles()
            .expect("built-in research profiles")
            .into_iter()
            .find(|profile| profile.profile_ref.id.as_str() == POOLED_1H_CONTROL_PROFILE_ID)
            .expect("pooled research profile");
        let source_window_end = self
            .window_end
            .checked_add_signed(ChronoDuration::seconds(
                i64::try_from(profile.spec.target_horizon_secs)
                    .expect("research profile target horizon"),
            ))
            .expect("Source Slice terminal bound");
        let profile_ref = profile.profile_ref;
        let research_program_hash = CanonicalDigest::content_hash_json(&(
            "trainer-dataset-program-v2",
            self.model_spec.definition_hash,
            self.policy_snapshot_id,
            self.factor_serving_plane.factor_schema_hash(),
        ))
        .expect("dataset research-program hash");
        let stored_source = persist_replayable_source_slice(
            store,
            &self.examples,
            ReplayableSourceSliceFixture {
                profile_ref: profile_ref.clone(),
                evaluation_track: ResearchEvaluationTrack::ResearchOnly,
                research_program_hash,
                decision_policy_snapshot_id: self.policy_snapshot_id,
                runtime_config_hash: self.runtime_config_hash,
                window_start: self.window_start,
                window_end: source_window_end,
            },
        )
        .await
        .expect("persist replayable Source Slice");
        seed_source_manifest(db, &stored_source)
            .await
            .expect("seed source-slice ledger")
    }

    fn ledger_fixture(&self, source_lineage: DatasetSourceLineage) -> DatasetLedgerFixture {
        let sample_count = u64::try_from(self.examples.len()).expect("sample count");
        let cohort_manifest = if self.purpose == DatasetPurpose::Evaluation {
            Some(
                model_learning_cohort(
                    self.scope,
                    &source_lineage,
                    self.window_start,
                    self.window_end,
                    sample_count,
                )
                .expect("evaluation cohort"),
            )
        } else {
            None
        };
        let mut horizons_secs = self
            .examples
            .iter()
            .flat_map(|example| example.labels.iter().map(|label| label.horizon_secs))
            .collect::<Vec<_>>();
        horizons_secs.push(
            u64::try_from(self.model_spec.prediction_horizon_secs)
                .expect("model prediction horizon is non-negative"),
        );
        horizons_secs.sort_unstable();
        horizons_secs.dedup();
        let mut fixture = DatasetLedgerFixture::try_new(DatasetLedgerSeed {
            training_dataset_id: self.dataset_id,
            model_spec_id: self.model_spec.model_spec_id,
            model_family: self.model_spec.model_family,
            model_spec_definition_hash: self.model_spec.definition_hash,
            factor_serving_plane: self.factor_serving_plane.clone(),
            source_lineage,
            cohort_manifest,
            window_start: self.window_start,
            window_end: self.window_end,
            purpose: self.purpose,
            knowledge_lag_secs: u64::try_from(KNOWLEDGE_LAG_SECS).expect("knowledge lag"),
            sample_interval_secs: u64::try_from(TICK_INTERVAL_SECS).expect("sample interval"),
            horizons_secs,
            feature_schema_version: SchemaVersion::FIRST,
            sample_sources: Some(self.sample_sources.clone()),
            feature_schema_hash: self.feature_schema_hash,
            label_schema_hash: self.label_schema_hash,
            semantic_dataset_hash: self.dataset_hash,
            source_fingerprint: dataset_source_fingerprint(&self.examples)
                .expect("source fingerprint"),
            sample_count,
        })
        .expect("dataset fixture");
        if let Some(binding) = &self.trade_policy {
            fixture.manifest.trade_policy_artifact_id = Some(binding.artifact_id);
            fixture.manifest.trade_policy_hash = Some(binding.content_hash);
            fixture
                .manifest
                .validate()
                .expect("policy-bound dataset manifest");
        }
        fixture
    }
}

impl SeededDataset {
    async fn persist(
        db: &DatabaseConnection,
        store: &Arc<dyn ArtifactStore>,
        seed: TrainingDatasetSeed<'_>,
    ) -> Self {
        let context = DatasetBuildContext::prepare(db, seed).await;
        let source_lineage = context.persist_source(db, store).await;
        let fixture = context.ledger_fixture(source_lineage);
        let bytes = DatasetParquetCodec::encode(&context.examples, &fixture.manifest)
            .expect("encode parquet");
        let artifact_bytes_hash = CanonicalDigest::content_hash_bytes(&bytes);
        let hex = context.dataset_id.as_uuid().simple().to_string();
        let key = ArtifactKey::new(ArtifactNamespace::Dataset, hex, "parquet").expect("key");
        let uri = store.put(key, &bytes).await.expect("store parquet");

        let dataset_repo = PgTrainingDatasetRepository::new(db.clone());
        dataset_repo
            .create_plan(fixture.plan.clone())
            .await
            .expect("dataset plan");
        dataset_repo
            .start_build(&context.dataset_id)
            .await
            .expect("start dataset");
        dataset_repo
            .complete_build(
                &context.dataset_id,
                fixture
                    .completion(
                        TrainingDatasetStatus::Ready,
                        artifact_bytes_hash,
                        uri,
                        fixture.coverage(),
                        None,
                    )
                    .expect("dataset completion"),
            )
            .await
            .expect("dataset ledger");
        Self {
            id: context.dataset_id,
            hash: context.dataset_hash,
            factor_serving_plane: context.factor_serving_plane,
        }
    }
}

/// Assert the training `quant_model_run` row was finalized with version FK + artifact hash.
async fn assert_training_run_ledger(
    db: &DatabaseConnection,
    version: &ModelVersionInfo,
    dataset_hash: ContentHash,
) {
    let training_run = Entity::find()
        .filter(Column::RunKind.eq(ModelRunKind::Training))
        .filter(Column::Status.eq(ModelRunStatus::Succeeded))
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

struct TrainInputFixture;

impl TrainInputFixture {
    fn for_dataset(
        model_spec: &ModelSpecInfo,
        training_dataset_id: TrainingDatasetId,
    ) -> TrainModelInput {
        TrainModelInput {
            model_version_id: ModelVersionId::from_v7(),
            model_run_id: ModelRunId::from_v7(),
            model_spec: model_spec.clone(),
            training_dataset_id,
        }
    }
}

async fn factor_definition_count(db: &DatabaseConnection) -> u64 {
    FactorDefinitionEntity::find()
        .count(db)
        .await
        .expect("count factor definitions")
}

async fn assert_persisted_plane(db: &DatabaseConnection, expected: &FactorServingPlane) {
    let ids = expected
        .definitions()
        .iter()
        .map(FactorDefinitionRef::factor_definition_id)
        .collect::<Vec<_>>();
    let persisted = PgFactorRepository::new(db.clone())
        .find_definitions_by_ids(&ids)
        .await
        .expect("load persisted factor revisions");
    let reconstructed = persisted
        .iter()
        .map(|definition| {
            FactorDefinitionRef::try_from(definition)
                .expect("persisted definition reconstructs its sealed revision")
        })
        .collect();
    let actual = FactorServingPlane::try_seal(reconstructed).expect("seal persisted serving plane");
    assert_eq!(
        &actual, expected,
        "persisted revisions must reconstruct the exact frozen plane"
    );
}

async fn assert_no_training_rows(db: &DatabaseConnection) {
    assert_eq!(
        Entity::find().count(db).await.expect("count model runs"),
        0,
        "contract drift must fail before creating a model run"
    );
    assert_eq!(
        ModelVersionEntity::find()
            .count(db)
            .await
            .expect("count model versions"),
        0,
        "contract drift must fail before creating a model version"
    );
}

fn assert_no_model_artifacts(root: &Path) {
    assert!(
        !root.join(ArtifactNamespace::Model.as_str()).exists(),
        "contract drift must fail before writing a model artifact"
    );
}

fn model_artifact_count(root: &Path) -> usize {
    let path = root.join(ArtifactNamespace::Model.as_str());
    match fs::read_dir(path) {
        Ok(entries) => entries.count(),
        Err(error) if error.kind() == ErrorKind::NotFound => 0,
        Err(error) => panic!("read model artifact directory: {error}"),
    }
}

async fn assert_classical_dataset(
    db: &DatabaseConnection,
    store: &Arc<dyn ArtifactStore>,
    seeded: &SeededDataset,
    family: ModelFamily,
) {
    let info = PgTrainingDatasetRepository::new(db.clone())
        .find_by_id(&seeded.id)
        .await
        .expect("load classical dataset")
        .expect("classical dataset row");
    assert_eq!(info.model_family, family);
    assert!(
        info.factor_serving_plane.definitions().is_empty(),
        "classical plan projection must have an empty factor plane"
    );
    let materialization = info
        .materialization()
        .expect("classical Dataset v3 materialization");
    assert!(
        materialization
            .manifest
            .factor_serving_plane
            .definitions()
            .is_empty(),
        "classical manifest must have an empty factor plane"
    );
    let bytes = store
        .get(materialization.parquet_uri)
        .await
        .expect("load classical parquet");
    let decoded =
        DatasetParquetCodec::decode_with_manifest(&bytes).expect("decode classical parquet");
    assert_eq!(decoded.manifest.model_family, family);
    assert!(
        decoded
            .manifest
            .factor_serving_plane
            .definitions()
            .is_empty()
    );
    assert!(
        decoded
            .examples
            .iter()
            .all(|example| example.factor_values.is_empty()),
        "every classical Dataset v3 row must be factor-free"
    );
}

struct WeightedVersionContract<'a> {
    version: &'a ModelVersionInfo,
}

impl WeightedVersionContract<'_> {
    fn assert_metrics(&self) {
        let version = self.version;
        assert_eq!(version.publication_status, PublicationStatus::Candidate);
        let ModelTrainingObjectiveDefinition::LearningToRank { spec } =
            &version.training_objective.definition
        else {
            panic!("weighted training must persist a learning-to-rank objective");
        };
        assert_eq!(spec.rank_loss, RankLossKind::RankIcWeightedRanknet);
        assert_eq!(spec.optimizer, TrainingOptimizerKind::CoordinateSearch);
        assert_eq!(spec.ndcg_k, 5);
        assert_eq!(spec.pseudo_top_n, 3);
        let ModelVersionMetricsDefinition::LearningToRank {
            in_sample,
            validation,
            ..
        } = &version.metrics.definition
        else {
            panic!("weighted training must persist learning-to-rank metrics");
        };
        assert_eq!(
            validation.held_out_metric,
            HeldOutMetricKind::NegativeTotalLearningToRankLoss
        );
        assert!(validation.dropped_singleton_groups <= validation.sample_count);
        let in_sample_diagnostics = in_sample
            .diagnostics
            .as_ref()
            .expect("weighted training must persist in-sample ranking diagnostics");
        assert_eq!(in_sample_diagnostics.ndcg_k, 5);
        let held_out_diagnostics = validation
            .held_out_diagnostics
            .as_ref()
            .expect("weighted training must persist held-out ranking diagnostics");
        assert_eq!(held_out_diagnostics.ndcg_k, 5);
    }
}

async fn assert_artifact_pooled_scope(store: &Arc<dyn ArtifactStore>, version: &ModelVersionInfo) {
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
    let bindings = artifact.header().serving_contract().bindings();
    assert_eq!(
        bindings.model.category_scope, None,
        "category scope is governed by the pooled ResearchProfile, never inferred from samples"
    );
    assert_eq!(
        bindings.model.profile_ref.id.as_str(),
        POOLED_1H_CONTROL_PROFILE_ID,
        "the artifact must retain the exact pooled ResearchProfile identity"
    );
    assert!(
        bindings.factors.plane.definitions().iter().all(|revision| {
            !matches!(
                revision.definition().family,
                FactorFamily::DomainCrypto | FactorFamily::DomainWeather
            )
        }),
        "the pooled model must not carry a category-specific domain factor plane"
    );
}

struct TrainerFixture;

struct TrainerPreimageFixture;

impl TrainerPreimageFixture {
    fn build(
        db: &DatabaseConnection,
        store: &Arc<dyn ArtifactStore>,
        registry: &Arc<dyn ModelRegistryRepository>,
        dataset_repo: &Arc<dyn TrainingDatasetRepository>,
        calibration_repo: &Arc<dyn CalibrationArtifactRepository>,
    ) -> (
        Arc<ModelServingPreimageService>,
        Arc<TradePolicyPreimageVerifier>,
    ) {
        let evidence_scope =
            PublishedTradePolicyFixture::evidence_scope().expect("trainer evidence scope");
        let readiness = Arc::new(
            ResearchReadinessEvidenceService::new(
                Arc::new(PgResearchReadinessEvidenceRepository::new(db.clone())),
                Arc::clone(store),
                Some(
                    PublishedTradePolicyFixture::evidence_attestor()
                        .expect("trainer evidence attestor"),
                ),
                &evidence_scope,
            )
            .expect("trainer readiness verifier"),
        );
        let trade_policy_repo: Arc<dyn TradePolicyRepository> =
            Arc::new(PgTradePolicyRepository::new(db.clone()));
        let evidence = Arc::new(TradePolicyEvidenceVerifier::new(
            TradePolicyEvidenceVerifierDeps {
                artifacts: Arc::clone(store),
                policies: Arc::clone(&trade_policy_repo),
                readiness,
            },
        ));
        let trade_policy = Arc::new(TradePolicyPreimageVerifier::new(
            TradePolicyPreimageVerifierDeps {
                trade_policy_repo,
                dataset_repo: Arc::clone(dataset_repo),
                model_registry_repo: Arc::clone(registry),
                evidence,
            },
        ));
        let serving = Arc::new(ModelServingPreimageService::new(ModelServingPreimageDeps {
            model_registry_repo: Arc::clone(registry),
            dataset_repo: Arc::clone(dataset_repo),
            source_slice_repo: Arc::new(PgSourceSliceRepository::new(db.clone())),
            policy_repo: Arc::new(PgPolicyRepository::new(db.clone())),
            calibration_repo: Arc::clone(calibration_repo),
            trade_policy_preimages: Arc::clone(&trade_policy),
            artifact_store: Arc::clone(store),
        }));
        (serving, trade_policy)
    }
}

impl TrainerFixture {
    async fn build(
        db: &DatabaseConnection,
        store: Arc<dyn ArtifactStore>,
        registry: Arc<dyn ModelRegistryRepository>,
        factor_repo: Arc<dyn FactorRepository>,
        policy_snapshot_id: DecisionPolicySnapshotId,
    ) -> ModelTrainerService {
        let policy_snapshot = PgPolicyRepository::new(db.clone())
            .load_snapshot(&policy_snapshot_id)
            .await
            .expect("load trainer policy snapshot")
            .expect("trainer policy snapshot");
        let dataset_repo: Arc<dyn TrainingDatasetRepository> =
            Arc::new(PgTrainingDatasetRepository::new(db.clone()));
        let calibration_repo: Arc<dyn CalibrationArtifactRepository> =
            Arc::new(PgCalibrationArtifactRepository::new(db.clone()));
        let (serving_preimages, trade_policy_preimages) =
            TrainerPreimageFixture::build(db, &store, &registry, &dataset_repo, &calibration_repo);
        ModelTrainerService::new(
            ModelTrainerServiceDeps {
                compute: Arc::new(ComputeExecutor::new().expect("test compute executor")),
                dataset_repo,
                factor_repo,
                artifact_store: store,
                model_registry_repo: registry,
                model_run_repo: Arc::new(PgModelRunRepository::new(db.clone())),
                calibration_repo,
                trade_policy_preimages,
                serving_preimages,
            },
            ModelTrainerConfig { policy_snapshot },
        )
    }
}

struct PolicyTrainingFixture {
    policy_snapshot_id: DecisionPolicySnapshotId,
    policy: PublishedTradePolicyFixture,
    model_spec: ModelSpecInfo,
    dataset_id: TrainingDatasetId,
    tamper_uri: ArtifactUri,
}

struct TrainerContractMatrix {
    db: DatabaseConnection,
    artifact_root: PathBuf,
    store: Arc<dyn ArtifactStore>,
    policy_snapshot_id: DecisionPolicySnapshotId,
    weighted_spec: ModelSpecInfo,
    weighted_dataset: SeededDataset,
    registry: Arc<dyn ModelRegistryRepository>,
    factor_repo: Arc<RecordingFactorRepository>,
    trainer: ModelTrainerService,
    baseline_factor_count: u64,
    _database: ScenarioDatabase,
}

impl TrainerContractMatrix {
    async fn build() -> Self {
        let (pool, database) = setup_pg().await;
        let db = pool.connection().clone();
        let artifact_root =
            env::temp_dir().join(format!("qp_trainer_contracts_{}", Uuid::new_v4().simple()));
        let inner: Arc<dyn ArtifactStore> = Arc::new(LocalArtifactStore::new(&artifact_root));
        let store: Arc<dyn ArtifactStore> = Arc::new(VersionedArtifactStoreFixture::new(inner));
        let policy_snapshot_id = seed_runtime_config(&db).await;
        let weighted_spec = ModelSpecFixture::persist(
            &db,
            "trainer-factor-contract-weighted",
            ModelFamily::WeightedFactor,
            model_spec_fixtures::pooled_horizon_secs(),
            ModelInputContract::single_required("book.mid"),
            ModelTrainingContract::settlement_default(),
        )
        .await;
        let weighted_dataset = SeededDataset::persist(
            &db,
            &store,
            TrainingDatasetSeed {
                model_spec: &weighted_spec,
                policy_snapshot_id,
                label_name: settlement(),
                examples: examples(),
                purpose: DatasetPurpose::Training,
                scope: "trainer-factor-contract-weighted",
                factor_serving_plane: None,
            },
        )
        .await;
        let registry: Arc<dyn ModelRegistryRepository> =
            Arc::new(PgModelRegistryRepository::new(db.clone()));
        let factor_repo = Arc::new(RecordingFactorRepository::new(db.clone()));
        let factor_repo_port = Arc::clone(&factor_repo);
        let factor_repo_port: Arc<dyn FactorRepository> = factor_repo_port;
        let trainer = TrainerFixture::build(
            &db,
            Arc::clone(&store),
            Arc::clone(&registry),
            factor_repo_port,
            policy_snapshot_id,
        )
        .await;
        let baseline_factor_count = factor_definition_count(&db).await;
        Self {
            db,
            artifact_root,
            store,
            policy_snapshot_id,
            weighted_spec,
            weighted_dataset,
            registry,
            factor_repo,
            trainer,
            baseline_factor_count,
            _database: database,
        }
    }

    async fn verify(&self) {
        Box::pin(self.reject_model_spec_drift()).await;
        Box::pin(self.reject_policy_drift()).await;
        Box::pin(self.reject_family_drift()).await;
        Box::pin(self.reject_factor_plane_drift()).await;
        let plane_size = Box::pin(self.verify_weighted_retry()).await;
        Box::pin(self.verify_sell_contract(plane_size)).await;
        Box::pin(self.verify_cancel_contract(plane_size)).await;
        Box::pin(self.verify_classical_contracts()).await;
        let policy = Box::pin(self.policy_training_fixture()).await;
        Box::pin(self.verify_policy_training(&policy)).await;
        Box::pin(self.reject_policy_tamper(&policy)).await;
        Box::pin(self.reject_policy_profile_drift(&policy)).await;
    }

    async fn assert_rejected_state(&self) {
        assert_eq!(self.factor_repo.register_calls(), 0);
        assert_eq!(
            factor_definition_count(&self.db).await,
            self.baseline_factor_count
        );
        assert_no_training_rows(&self.db).await;
        assert_no_model_artifacts(&self.artifact_root);
    }

    async fn reject_model_spec_drift(&self) {
        let mut input =
            TrainInputFixture::for_dataset(&self.weighted_spec, self.weighted_dataset.id);
        input.model_spec.model_spec_id = ModelSpecId::from_v7();
        let cancellation = CancellationToken::new();
        let Err(error) =
            Box::pin(self.trainer.train(input, &NoopProgressSink, &cancellation)).await
        else {
            panic!("model-spec drift must fail closed");
        };
        assert!(
            matches!(
                &error,
                QuantError::Research(ResearchError::DatasetBuild { detail })
                    if detail.contains("model spec mismatch")
            ),
            "model-spec drift must report a typed DatasetBuild mismatch, got {error}"
        );
        self.assert_rejected_state().await;
    }

    async fn reject_policy_drift(&self) {
        let policy_drift_id = activate_policy_bundle(
            &PgPolicyRepository::new(self.db.clone()),
            ConfigResourceKind::RecommendationPolicy,
            "trainer-policy-drift",
            "integration test policy mismatch",
            |snapshot| {
                snapshot.recommendation.data_quality.max_book_age_ms = snapshot
                    .recommendation
                    .data_quality
                    .max_book_age_ms
                    .checked_add(1)
                    .expect("policy drift fixture max book age");
            },
        )
        .await;
        let factor_repo = Arc::clone(&self.factor_repo);
        let factor_repo: Arc<dyn FactorRepository> = factor_repo;
        let trainer = TrainerFixture::build(
            &self.db,
            Arc::clone(&self.store),
            Arc::clone(&self.registry),
            factor_repo,
            policy_drift_id,
        )
        .await;
        let cancellation = CancellationToken::new();
        let Err(error) = Box::pin(trainer.train(
            TrainInputFixture::for_dataset(&self.weighted_spec, self.weighted_dataset.id),
            &NoopProgressSink,
            &cancellation,
        ))
        .await
        else {
            panic!("policy-snapshot drift must fail closed");
        };
        assert!(
            matches!(
                &error,
                QuantError::Research(ResearchError::DatasetBuild { detail })
                    if detail.contains("policy snapshot mismatch")
            ),
            "policy drift must report a typed DatasetBuild mismatch, got {error}"
        );
        self.assert_rejected_state().await;
    }

    async fn reject_family_drift(&self) {
        let mut input =
            TrainInputFixture::for_dataset(&self.weighted_spec, self.weighted_dataset.id);
        input.model_spec.model_family = ModelFamily::ClassicalRandomForest;
        let cancellation = CancellationToken::new();
        let Err(error) =
            Box::pin(self.trainer.train(input, &NoopProgressSink, &cancellation)).await
        else {
            panic!("family drift must fail closed");
        };
        assert!(
            matches!(
                &error,
                QuantError::Research(ResearchError::InvalidModelArtifact { detail })
                    if detail.contains("model-spec definition mismatch")
            ),
            "family tamper must fail at the sealed model-spec definition, got {error}"
        );
        self.assert_rejected_state().await;
    }

    async fn reject_factor_plane_drift(&self) {
        let dataset = SeededDataset::persist(
            &self.db,
            &self.store,
            TrainingDatasetSeed {
                model_spec: &self.weighted_spec,
                policy_snapshot_id: self.policy_snapshot_id,
                label_name: settlement(),
                examples: examples(),
                purpose: DatasetPurpose::Training,
                scope: "trainer-factor-contract-drifted-plane",
                factor_serving_plane: Some(FactorPlaneFixture::drifted(
                    &self.weighted_dataset.factor_serving_plane,
                )),
            },
        )
        .await;
        let cancellation = CancellationToken::new();
        let Err(error) = Box::pin(self.trainer.train(
            TrainInputFixture::for_dataset(&self.weighted_spec, dataset.id),
            &NoopProgressSink,
            &cancellation,
        ))
        .await
        else {
            panic!("factor-plane drift must fail closed");
        };
        assert!(
            matches!(
                &error,
                QuantError::Research(ResearchError::DatasetBuild { detail })
                    if detail.contains("factor plane mismatch")
            ),
            "factor-plane drift must report a typed DatasetBuild mismatch, got {error}"
        );
        self.assert_rejected_state().await;
    }

    async fn policy_training_fixture(&self) -> PolicyTrainingFixture {
        let weather_policy_snapshot_id = activate_policy_bundle(
            &PgPolicyRepository::new(self.db.clone()),
            ConfigResourceKind::RecommendationPolicy,
            "trainer-weather-policy",
            "enable the Weather domain for a complete TradePolicy preimage",
            |snapshot| {
                snapshot.profile_artifacts.domain.definition = DomainConfig::default();
                snapshot.recommendation.data_quality.max_book_age_ms = snapshot
                    .recommendation
                    .data_quality
                    .max_book_age_ms
                    .checked_add(1)
                    .expect("Weather policy fixture max book age");
            },
        )
        .await;
        let window_end_raw = Utc::now() - ChronoDuration::days(2);
        let window_end = Utc
            .timestamp_millis_opt(window_end_raw.timestamp_millis())
            .single()
            .expect("millisecond-aligned policy training window");
        let window_start = window_end - ChronoDuration::days(180);
        let policy = PublishedTradePolicyFixture::persist(
            &self.db,
            &self.store,
            weather_policy_snapshot_id,
            "trainer-policy-bound",
            window_start,
        )
        .await
        .expect("persist complete trainer TradePolicy preimage");
        let profile = builtin_research_profiles()
            .expect("built-in ResearchProfiles")
            .into_iter()
            .find(|profile| profile.spec.category == Some(MarketCategory::Weather))
            .expect("Weather ResearchProfile");
        let policy_snapshot = PgPolicyRepository::new(self.db.clone())
            .load_snapshot(&weather_policy_snapshot_id)
            .await
            .expect("load Weather policy snapshot")
            .expect("Weather policy snapshot");
        let feature_schema = FeatureSchema::build(
            &policy_snapshot
                .snapshot
                .profile_artifacts
                .features
                .definition,
        )
        .expect("Weather feature schema");
        let factor_plane = FactorEngine::for_model_scope(
            &policy_snapshot
                .snapshot
                .profile_artifacts
                .scoring
                .definition,
            &policy_snapshot
                .snapshot
                .profile_artifacts
                .features
                .definition,
            &policy_snapshot.snapshot.profile_artifacts.domain.definition,
            profile.spec.category,
            None,
        )
        .serving_plane()
        .expect("Weather factor plane")
        .clone();
        let model_spec = ModelSpecFixture::persist(
            &self.db,
            "trainer-policy-bound",
            ModelFamily::WeightedFactor,
            i64::try_from(profile.spec.target_horizon_secs)
                .expect("Weather profile horizon fits i64"),
            ModelInputContract::single_required("book.mid"),
            policy.target_training_contract(),
        )
        .await;
        let provenance = policy.provenance();
        let dataset = ModelDatasetLedgerFixture::persist(
            &self.db,
            &self.store,
            ModelDatasetLedgerSeed {
                scope: "trainer-policy-bound".to_owned(),
                model_spec_id: model_spec.model_spec_id,
                model_family: model_spec.model_family,
                model_spec_definition_hash: model_spec.definition_hash,
                factor_serving_plane: factor_plane,
                feature_schema_version: feature_schema.version(),
                feature_schema_hash: ResearchHasher::feature_schema(&feature_schema)
                    .expect("Weather feature schema hash"),
                decision_policy_snapshot_id: weather_policy_snapshot_id,
                profile_ref: profile.profile_ref,
                prediction_horizon_secs: profile.spec.target_horizon_secs,
                purpose: DatasetPurpose::Training,
                window_start,
                window_end,
                research_program_hash: ResearchHasher::canonical(
                    &"trainer-policy-bound-program-v1",
                )
                .expect("policy-bound training program hash"),
                sample_count: 500,
                decision_interval_secs: 604_800,
                trade_policy: Some(ModelServingTradePolicyBinding {
                    artifact_id: provenance.artifact_id,
                    content_hash: provenance.artifact_hash,
                }),
            },
        )
        .await
        .expect("persist policy-bound Training Dataset");
        let policy_row = PgTradePolicyRepository::new(self.db.clone())
            .find(&provenance.artifact_id)
            .await
            .expect("load policy-bound TradePolicy")
            .expect("policy-bound TradePolicy");
        let evidence = policy_row
            .payload_json
            .evidence_bundle
            .as_ref()
            .expect("published policy evidence bundle");
        let manifest_bytes = self
            .store
            .get(&evidence.manifest_uri)
            .await
            .expect("read policy evidence manifest");
        let manifest: TradePolicyEvidenceBundleManifest =
            serde_json::from_slice(&manifest_bytes).expect("decode policy evidence manifest");
        let tamper_uri = manifest
            .objects
            .first()
            .expect("non-empty policy evidence objects")
            .uri
            .clone();
        PolicyTrainingFixture {
            policy_snapshot_id: weather_policy_snapshot_id,
            policy,
            model_spec,
            dataset_id: dataset.training_dataset_id,
            tamper_uri,
        }
    }

    async fn verify_policy_training(&self, fixture: &PolicyTrainingFixture) {
        let factor_repo = Arc::clone(&self.factor_repo);
        let factor_repo: Arc<dyn FactorRepository> = factor_repo;
        let trainer = TrainerFixture::build(
            &self.db,
            Arc::clone(&self.store),
            Arc::clone(&self.registry),
            factor_repo,
            fixture.policy_snapshot_id,
        )
        .await;
        let outcome = Box::pin(trainer.train(
            TrainInputFixture::for_dataset(&fixture.model_spec, fixture.dataset_id),
            &NoopProgressSink,
            &CancellationToken::new(),
        ))
        .await
        .expect("train policy-bound Weather model");
        let contract = outcome
            .version
            .verified_serving_contract()
            .expect("policy-bound serving contract");
        let binding = contract
            .bindings()
            .trade_policy
            .as_ref()
            .expect("policy-bound serving binding");
        assert_eq!(binding.artifact_id, fixture.policy.provenance().artifact_id);
        assert_eq!(
            binding.content_hash,
            fixture.policy.provenance().artifact_hash
        );
        assert_eq!(
            contract.bindings().model.category_scope,
            Some(MarketCategory::Weather)
        );
        let persisted = self
            .registry
            .find_model_version(&outcome.version.model_version_id)
            .await
            .expect("reload policy-bound model version")
            .expect("persisted policy-bound model version");
        let dataset_repo: Arc<dyn TrainingDatasetRepository> =
            Arc::new(PgTrainingDatasetRepository::new(self.db.clone()));
        let calibration_repo: Arc<dyn CalibrationArtifactRepository> =
            Arc::new(PgCalibrationArtifactRepository::new(self.db.clone()));
        let (preimages, _) = TrainerPreimageFixture::build(
            &self.db,
            &self.store,
            &self.registry,
            &dataset_repo,
            &calibration_repo,
        );
        let verified = preimages
            .load(&persisted)
            .await
            .expect("verify complete policy-bound serving graph");
        assert_eq!(
            verified.training_dataset().training_dataset_id,
            fixture.dataset_id
        );
    }

    async fn reject_policy_tamper(&self, fixture: &PolicyTrainingFixture) {
        let run_count_before = Entity::find()
            .count(&self.db)
            .await
            .expect("count model runs before policy evidence tamper");
        let version_count_before = ModelVersionEntity::find()
            .count(&self.db)
            .await
            .expect("count model versions before policy evidence tamper");
        let factor_count_before = factor_definition_count(&self.db).await;
        let factor_calls_before = self.factor_repo.register_calls();
        let artifact_count_before = model_artifact_count(&self.artifact_root);
        let tampered_store: Arc<dyn ArtifactStore> = Arc::new(ReadTamperArtifactStoreFixture::new(
            Arc::clone(&self.store),
            fixture.tamper_uri.clone(),
            b"tampered-policy-evidence".to_vec(),
        ));
        let factor_repo = Arc::clone(&self.factor_repo);
        let factor_repo: Arc<dyn FactorRepository> = factor_repo;
        let trainer = TrainerFixture::build(
            &self.db,
            tampered_store,
            Arc::clone(&self.registry),
            factor_repo,
            fixture.policy_snapshot_id,
        )
        .await;
        let Err(error) = Box::pin(trainer.train(
            TrainInputFixture::for_dataset(&fixture.model_spec, fixture.dataset_id),
            &NoopProgressSink,
            &CancellationToken::new(),
        ))
        .await
        else {
            panic!("tampered TradePolicy evidence must fail closed");
        };
        assert!(
            matches!(
                &error,
                QuantError::Research(ResearchError::ValidationMethodology { detail })
                    if detail.contains("byte hash mismatch")
            ),
            "TradePolicy evidence tamper must report a typed hash mismatch, got {error}"
        );
        assert_eq!(self.factor_repo.register_calls(), factor_calls_before);
        assert_eq!(factor_definition_count(&self.db).await, factor_count_before);
        assert_eq!(
            Entity::find()
                .count(&self.db)
                .await
                .expect("count model runs after policy evidence tamper"),
            run_count_before
        );
        assert_eq!(
            ModelVersionEntity::find()
                .count(&self.db)
                .await
                .expect("count model versions after policy evidence tamper"),
            version_count_before
        );
        assert_eq!(
            model_artifact_count(&self.artifact_root),
            artifact_count_before
        );
    }

    async fn reject_policy_profile_drift(&self, fixture: &PolicyTrainingFixture) {
        let label = PolicyLabelFixture::default();
        let spec = ModelSpecFixture::persist(
            &self.db,
            "trainer-trade-policy-profile-drift",
            ModelFamily::WeightedFactor,
            model_spec_fixtures::pooled_horizon_secs(),
            ModelInputContract::single_required("book.mid"),
            fixture.policy.target_training_contract(),
        )
        .await;
        let dataset = SeededDataset::persist(
            &self.db,
            &self.store,
            TrainingDatasetSeed {
                model_spec: &spec,
                policy_snapshot_id: self.policy_snapshot_id,
                label_name: label.label_name.clone(),
                examples: label.relabel(examples()),
                purpose: DatasetPurpose::Training,
                scope: "trainer-trade-policy-profile-drift",
                factor_serving_plane: None,
            },
        )
        .await;
        let factor_calls_before = self.factor_repo.register_calls();
        let factor_count_before = factor_definition_count(&self.db).await;
        let run_count_before = Entity::find()
            .count(&self.db)
            .await
            .expect("count model runs before policy-profile rejection");
        let version_count_before = ModelVersionEntity::find()
            .count(&self.db)
            .await
            .expect("count model versions before policy-profile rejection");
        let cancellation = CancellationToken::new();
        let Err(error) = Box::pin(self.trainer.train(
            TrainInputFixture::for_dataset(&spec, dataset.id),
            &NoopProgressSink,
            &cancellation,
        ))
        .await
        else {
            panic!("trade-policy ResearchProfile drift must fail closed");
        };
        assert!(
            matches!(
                &error,
                QuantError::Research(ResearchError::DatasetBuild { detail })
                    if detail.contains("trade-policy ResearchProfile mismatch")
            ),
            "trade-policy profile drift must report a typed DatasetBuild mismatch, got {error}"
        );
        assert_eq!(self.factor_repo.register_calls(), factor_calls_before);
        assert_eq!(
            factor_definition_count(&self.db).await,
            factor_count_before,
            "policy-profile rejection must not register factor revisions"
        );
        assert_eq!(
            Entity::find()
                .count(&self.db)
                .await
                .expect("count model runs after policy-profile rejection"),
            run_count_before,
            "policy-profile rejection must precede ModelRun creation"
        );
        assert_eq!(
            ModelVersionEntity::find()
                .count(&self.db)
                .await
                .expect("count model versions after policy-profile rejection"),
            version_count_before,
            "policy-profile rejection must not persist a model version"
        );
    }

    async fn verify_weighted_retry(&self) -> usize {
        let model_version_id = ModelVersionId::from_v7();
        let model_run_id = ModelRunId::from_v7();
        let exact_input = || TrainModelInput {
            model_version_id,
            model_run_id,
            model_spec: self.weighted_spec.clone(),
            training_dataset_id: self.weighted_dataset.id,
        };
        let first_cancellation = CancellationToken::new();
        let first = Box::pin(self.trainer.train(
            exact_input(),
            &NoopProgressSink,
            &first_cancellation,
        ))
        .await
        .expect("first weighted training");
        assert_eq!(first.version.model_version_id, model_version_id);
        assert_eq!(first.model_run_id, model_run_id);
        let plane_size = self
            .weighted_dataset
            .factor_serving_plane
            .definitions()
            .len();
        assert!(plane_size > 0, "weighted plane must not be empty");
        assert_eq!(self.factor_repo.register_calls(), 1);
        assert_eq!(self.factor_repo.inserted(), plane_size);
        assert_eq!(self.factor_repo.already_present(), 0);
        assert_eq!(
            factor_definition_count(&self.db).await,
            self.baseline_factor_count + u64::try_from(plane_size).expect("plane size")
        );
        assert_persisted_plane(&self.db, &self.weighted_dataset.factor_serving_plane).await;

        let retry_cancellation = CancellationToken::new();
        let retry = Box::pin(self.trainer.train(
            exact_input(),
            &NoopProgressSink,
            &retry_cancellation,
        ))
        .await
        .expect("idempotent weighted training retry");
        assert_eq!(retry.version.model_version_id, model_version_id);
        assert_eq!(retry.model_run_id, model_run_id);
        assert_eq!(retry.version.artifact_hash, first.version.artifact_hash);
        assert_eq!(
            retry.version.serving_contract_hash,
            first.version.serving_contract_hash
        );
        assert_eq!(self.factor_repo.register_calls(), 2);
        assert_eq!(self.factor_repo.inserted(), plane_size);
        assert_eq!(self.factor_repo.already_present(), plane_size);
        assert_eq!(
            factor_definition_count(&self.db).await,
            self.baseline_factor_count + u64::try_from(plane_size).expect("plane size")
        );
        assert_persisted_plane(&self.db, &self.weighted_dataset.factor_serving_plane).await;
        plane_size
    }

    async fn verify_sell_contract(&self, plane_size: usize) {
        let training_contract = ModelTrainingContract {
            target_label_name: HOLD_VS_EXIT_ALPHA_BPS.to_string(),
            target_label_horizon_secs: 0,
            validation_folds: 3,
            trade_policy_artifact_id: None,
        };
        let spec = ModelSpecFixture::persist(
            &self.db,
            "trainer-factor-contract-hold",
            ModelFamily::HoldVsExitWeighted,
            model_spec_fixtures::pooled_horizon_secs(),
            ModelInputContract::single_required("book.mid"),
            training_contract,
        )
        .await;
        let dataset = SeededDataset::persist(
            &self.db,
            &self.store,
            TrainingDatasetSeed {
                model_spec: &spec,
                policy_snapshot_id: self.policy_snapshot_id,
                label_name: HOLD_VS_EXIT_ALPHA_BPS,
                examples: exit_examples(),
                purpose: DatasetPurpose::Training,
                scope: "trainer-factor-contract-hold",
                factor_serving_plane: None,
            },
        )
        .await;
        assert_eq!(
            dataset.factor_serving_plane, self.weighted_dataset.factor_serving_plane,
            "Buy and Sell trainers must register the same frozen market-factor plane"
        );
        let cancellation = CancellationToken::new();
        Box::pin(self.trainer.train(
            TrainInputFixture::for_dataset(&spec, dataset.id),
            &NoopProgressSink,
            &cancellation,
        ))
        .await
        .expect("hold-vs-exit training");
        assert_eq!(self.factor_repo.register_calls(), 3);
        assert_eq!(self.factor_repo.inserted(), plane_size);
        assert_eq!(self.factor_repo.already_present(), plane_size * 2);
        assert_eq!(
            factor_definition_count(&self.db).await,
            self.baseline_factor_count + u64::try_from(plane_size).expect("plane size")
        );
        assert_persisted_plane(&self.db, &dataset.factor_serving_plane).await;
    }

    async fn verify_cancel_contract(&self, plane_size: usize) {
        let versions_before = ModelVersionEntity::find()
            .count(&self.db)
            .await
            .expect("count versions before cancellation");
        let register_calls_before = self.factor_repo.register_calls();
        let cancellation = CancellationToken::new();
        let progress = CancelAtPhase {
            cancel: cancellation.clone(),
            phase: "register",
        };
        let cancelled = Box::pin(self.trainer.train(
            TrainInputFixture::for_dataset(&self.weighted_spec, self.weighted_dataset.id),
            &progress,
            &cancellation,
        ))
        .await;
        assert!(
            matches!(
                cancelled,
                Err(QuantError::Research(ResearchError::Cancelled { .. }))
            ),
            "cancellation observed after registration must remain a typed cancellation"
        );
        assert_eq!(
            self.factor_repo.register_calls(),
            register_calls_before + 1,
            "the cancellation boundary must occur after the idempotent batch registration"
        );
        assert_eq!(
            factor_definition_count(&self.db).await,
            self.baseline_factor_count + u64::try_from(plane_size).expect("plane size"),
            "cancelled retries must not add factor revisions"
        );
        assert_eq!(
            ModelVersionEntity::find()
                .count(&self.db)
                .await
                .expect("count versions after cancellation"),
            versions_before,
            "cancellation before fit must not register a model version"
        );
        let cancelled_runs = Entity::find()
            .filter(Column::Status.eq(ModelRunStatus::Cancelled))
            .all(&self.db)
            .await
            .expect("load cancelled model runs");
        let [cancelled_run] = cancelled_runs.as_slice() else {
            panic!("exactly one cancelled training run must be durable");
        };
        assert_eq!(
            cancelled_run.error_code,
            Some(ModelRunErrorCode::CancelledByOperator)
        );
        assert!(cancelled_run.finished_at.is_some());
        assert_eq!(
            Entity::find()
                .filter(Column::Status.eq(ModelRunStatus::Running))
                .count(&self.db)
                .await
                .expect("count orphaned model runs"),
            0,
            "no cancellation boundary may leave a Running model run"
        );
    }

    async fn verify_classical_contracts(&self) {
        // Classical validation is feature-only and never constructs or registers a
        // factor plane from the otherwise factor-enabled frozen policy snapshot.
        for family in [
            ModelFamily::ClassicalRandomForest,
            ModelFamily::ClassicalExtraTrees,
            ModelFamily::ClassicalLogisticRegression,
            ModelFamily::ClassicalRidge,
            ModelFamily::ClassicalLasso,
            ModelFamily::ClassicalElasticNet,
        ] {
            let ClassicalDatasetFixture {
                rows,
                label_name,
                label_horizon_secs,
            } = ClassicalDatasetFixture::for_family(family);
            let input_contract = ModelInputContract::single_required("book.visible_liquidity_usd");
            let training_contract = ModelTrainingContract {
                target_label_name: label_name.to_string(),
                target_label_horizon_secs: label_horizon_secs,
                validation_folds: 3,
                trade_policy_artifact_id: None,
            };
            let spec = ModelSpecFixture::persist(
                &self.db,
                &format!("trainer-factor-contract-{}", family.as_str()),
                family,
                model_spec_fixtures::pooled_horizon_secs(),
                input_contract,
                training_contract,
            )
            .await;
            let dataset = SeededDataset::persist(
                &self.db,
                &self.store,
                TrainingDatasetSeed {
                    model_spec: &spec,
                    policy_snapshot_id: self.policy_snapshot_id,
                    label_name,
                    examples: rows,
                    purpose: DatasetPurpose::Training,
                    scope: family.as_str(),
                    factor_serving_plane: None,
                },
            )
            .await;
            assert!(dataset.factor_serving_plane.definitions().is_empty());
            assert_classical_dataset(&self.db, &self.store, &dataset, family).await;

            let calls_before = self.factor_repo.register_calls();
            let factor_count_before = factor_definition_count(&self.db).await;
            let cancellation = CancellationToken::new();
            let outcome = Box::pin(self.trainer.train(
                TrainInputFixture::for_dataset(&spec, dataset.id),
                &NoopProgressSink,
                &cancellation,
            ))
            .await;
            match outcome {
                Ok(_) => {}
                Err(QuantError::Research(ResearchError::RuntimeUnavailable {
                    family: unavailable,
                    ..
                })) => {
                    assert_eq!(
                        unavailable,
                        family.classical_kind().expect("kind").to_string()
                    );
                }
                Err(error) => {
                    panic!("classical trainer failed unexpectedly for {family}: {error}");
                }
            }
            assert_eq!(
                self.factor_repo.register_calls(),
                calls_before,
                "classical trainer must never call the factor registry"
            );
            assert_eq!(
                factor_definition_count(&self.db).await,
                factor_count_before,
                "classical trainer must not insert factor revisions"
            );
        }
    }
}

pub async fn trainer_freezes_factor_contracts() {
    let matrix = Box::pin(TrainerContractMatrix::build()).await;
    Box::pin(matrix.verify()).await;
    drop(matrix);
}

async fn assert_policy_rejected(
    db: &DatabaseConnection,
    backtester: &BacktestService,
    version: &ModelVersionInfo,
    evaluation_dataset_id: TrainingDatasetId,
) {
    let drift_id = activate_policy_bundle(
        &PgPolicyRepository::new(db.clone()),
        ConfigResourceKind::RecommendationPolicy,
        "backtest-policy-drift",
        "backtest exact-preimage rejection",
        |snapshot| {
            snapshot.recommendation.data_quality.max_book_age_ms = snapshot
                .recommendation
                .data_quality
                .max_book_age_ms
                .checked_add(1)
                .expect("policy drift fixture max book age");
        },
    )
    .await;
    let runs_before = Entity::find()
        .count(db)
        .await
        .expect("count runs before backtest policy drift");
    let result = backtester
        .run(
            BacktestInput {
                model_version_id: version.model_version_id,
                evaluation_dataset_id,
                decision_policy_snapshot_id: drift_id,
                backtest_report_id: None,
            },
            Arc::new(NoopProgressSink),
            CancellationToken::new(),
        )
        .await;
    assert!(
        matches!(
            &result,
            Err(QuantError::Research(ResearchError::InvalidModelArtifact { detail }))
                if detail.contains("policy snapshot")
        ),
        "backtest must reject a non-contract policy snapshot before replay, got {result:?}"
    );
    assert_eq!(
        Entity::find()
            .count(db)
            .await
            .expect("count runs after backtest policy drift"),
        runs_before,
        "policy preimage drift must not create a model run"
    );
}

async fn assert_source_rejected(
    db: &DatabaseConnection,
    backtester: &BacktestService,
    version: &ModelVersionInfo,
    evaluation_dataset_id: TrainingDatasetId,
    policy_snapshot_id: DecisionPolicySnapshotId,
) {
    let dataset = PgTrainingDatasetRepository::new(db.clone())
        .find_by_id(&evaluation_dataset_id)
        .await
        .expect("load source-tamper dataset")
        .expect("source-tamper dataset");
    let uri = dataset.source_lineage.source_slice.manifest_uri;
    let path = PathBuf::from(
        uri.as_str()
            .strip_prefix("file://")
            .expect("local Source Slice URI"),
    );
    let original = tokio::fs::read(&path)
        .await
        .expect("read Source Slice manifest");
    tokio::fs::write(&path, b"tampered Source Slice manifest")
        .await
        .expect("tamper Source Slice manifest");
    let runs_before = Entity::find()
        .count(db)
        .await
        .expect("count runs before Source Slice tamper");
    let result = backtester
        .run(
            BacktestInput {
                model_version_id: version.model_version_id,
                evaluation_dataset_id,
                decision_policy_snapshot_id: policy_snapshot_id,
                backtest_report_id: None,
            },
            Arc::new(NoopProgressSink),
            CancellationToken::new(),
        )
        .await;
    tokio::fs::write(&path, original)
        .await
        .expect("restore Source Slice manifest");
    assert!(
        matches!(
            &result,
            Err(QuantError::Research(ResearchError::DatasetBuild { detail }))
                if detail.contains("Source Slice")
        ),
        "backtest must reject tampered Source Slice bytes, got {result:?}"
    );
    assert_eq!(
        Entity::find()
            .count(db)
            .await
            .expect("count runs after Source Slice tamper"),
        runs_before,
        "Source Slice tamper must not create a model run"
    );
}

async fn assert_dataset_rejected(
    db: &DatabaseConnection,
    backtester: &BacktestService,
    version: &ModelVersionInfo,
    evaluation_dataset_id: TrainingDatasetId,
    policy_snapshot_id: DecisionPolicySnapshotId,
) {
    let dataset = PgTrainingDatasetRepository::new(db.clone())
        .find_by_id(&evaluation_dataset_id)
        .await
        .expect("load dataset-byte tamper target")
        .expect("dataset-byte tamper target");
    let uri = dataset.parquet_uri.expect("Ready Dataset Parquet URI");
    let path = PathBuf::from(
        uri.as_str()
            .strip_prefix("file://")
            .expect("local Dataset URI"),
    );
    let original = tokio::fs::read(&path).await.expect("read Dataset bytes");
    tokio::fs::write(&path, b"tampered Dataset bytes")
        .await
        .expect("tamper Dataset bytes");
    let runs_before = Entity::find()
        .count(db)
        .await
        .expect("count runs before Dataset tamper");
    let result = backtester
        .run(
            BacktestInput {
                model_version_id: version.model_version_id,
                evaluation_dataset_id,
                decision_policy_snapshot_id: policy_snapshot_id,
                backtest_report_id: None,
            },
            Arc::new(NoopProgressSink),
            CancellationToken::new(),
        )
        .await;
    tokio::fs::write(&path, original)
        .await
        .expect("restore Dataset bytes");
    assert!(
        matches!(
            &result,
            Err(QuantError::Research(ResearchError::DatasetBuild { detail }))
                if detail.contains("dataset byte hash mismatch")
        ),
        "backtest must reject tampered Dataset bytes, got {result:?}"
    );
    assert_eq!(
        Entity::find()
            .count(db)
            .await
            .expect("count runs after Dataset tamper"),
        runs_before,
        "Dataset tamper must not create a model run"
    );
}

#[derive(Debug, PartialEq, Eq)]
struct BacktestLedgerCounts {
    runs: u64,
    reports: u64,
    comparisons: u64,
}

impl BacktestLedgerCounts {
    async fn load(db: &DatabaseConnection) -> Self {
        Self {
            runs: Entity::find().count(db).await.expect("count model runs"),
            reports: BacktestReportEntity::find()
                .count(db)
                .await
                .expect("count backtest reports"),
            comparisons: ComparisonReportEntity::find()
                .count(db)
                .await
                .expect("count comparison reports"),
        }
    }
}

async fn assert_cache_rejected(
    db: &DatabaseConnection,
    port: &CoreBacktestPort,
    version: &ModelVersionInfo,
    evaluation_dataset_id: TrainingDatasetId,
    policy_snapshot_id: DecisionPolicySnapshotId,
    backtest_report_id: BacktestReportId,
) {
    let dataset = PgTrainingDatasetRepository::new(db.clone())
        .find_by_id(&evaluation_dataset_id)
        .await
        .expect("load cache-tamper Dataset")
        .expect("cache-tamper Dataset");
    let uri = dataset.parquet_uri.expect("Ready Dataset Parquet URI");
    let path = PathBuf::from(
        uri.as_str()
            .strip_prefix("file://")
            .expect("local Dataset URI"),
    );
    let original = tokio::fs::read(&path)
        .await
        .expect("read cached Dataset bytes");
    tokio::fs::write(&path, b"tampered cached Dataset bytes")
        .await
        .expect("tamper cached Dataset bytes");
    let ledger_before = BacktestLedgerCounts::load(db).await;
    let result = port
        .run(
            version.model_version_id,
            RunBacktestRequest {
                evaluation_dataset_id,
                decision_policy_snapshot_id: policy_snapshot_id,
                comparison_model_version_id: None,
                reason: "verify cached report preimages".to_owned(),
                backtest_report_id: Some(backtest_report_id),
            },
            Arc::new(NoopProgressSink),
            CancellationToken::new(),
        )
        .await;
    tokio::fs::write(&path, original)
        .await
        .expect("restore cached Dataset bytes");
    assert!(
        matches!(
            &result,
            Err(QuantError::Research(ResearchError::DatasetBuild { detail }))
                if detail.contains("dataset byte hash mismatch")
        ),
        "cache lookup must reject tampered Dataset bytes, got {result:?}"
    );
    assert_eq!(
        BacktestLedgerCounts::load(db).await,
        ledger_before,
        "cache preimage rejection must not mutate run/report/comparison ledgers"
    );
    let cached = port
        .run(
            version.model_version_id,
            RunBacktestRequest {
                evaluation_dataset_id,
                decision_policy_snapshot_id: policy_snapshot_id,
                comparison_model_version_id: None,
                reason: "verify restored cached report".to_owned(),
                backtest_report_id: Some(backtest_report_id),
            },
            Arc::new(NoopProgressSink),
            CancellationToken::new(),
        )
        .await
        .expect("restored cache preimages");
    assert_eq!(cached.backtest_report_id, backtest_report_id);
    assert_eq!(
        BacktestLedgerCounts::load(db).await,
        ledger_before,
        "verified cache hit must not duplicate run/report/comparison evidence"
    );
    let wrong_model = ModelVersionId::from_v7();
    let wrong_subject = port
        .run(
            wrong_model,
            RunBacktestRequest {
                evaluation_dataset_id,
                decision_policy_snapshot_id: policy_snapshot_id,
                comparison_model_version_id: None,
                reason: "reject cached report subject drift".to_owned(),
                backtest_report_id: Some(backtest_report_id),
            },
            Arc::new(NoopProgressSink),
            CancellationToken::new(),
        )
        .await;
    assert!(
        matches!(
            &wrong_subject,
            Err(QuantError::Research(ResearchError::InvalidModelArtifact { detail }))
                if detail.contains("different request subject")
        ),
        "cache lookup must reject a different request subject, got {wrong_subject:?}"
    );
    assert_eq!(
        BacktestLedgerCounts::load(db).await,
        ledger_before,
        "cache subject rejection must not mutate run/report/comparison ledgers"
    );
}

async fn assert_calibration_rejected(
    db: &DatabaseConnection,
    fitter: &ModelCalibrationFitService,
    version: &ModelVersionInfo,
    calibration_dataset_id: TrainingDatasetId,
    policy_snapshot_id: DecisionPolicySnapshotId,
) {
    let dataset = PgTrainingDatasetRepository::new(db.clone())
        .find_by_id(&calibration_dataset_id)
        .await
        .expect("load calibration tamper target")
        .expect("calibration tamper target");
    let uri = dataset.parquet_uri.expect("Ready Calibration Dataset URI");
    let path = PathBuf::from(
        uri.as_str()
            .strip_prefix("file://")
            .expect("local Calibration Dataset URI"),
    );
    let original = tokio::fs::read(&path)
        .await
        .expect("read Calibration Dataset bytes");
    tokio::fs::write(&path, b"tampered Calibration Dataset bytes")
        .await
        .expect("tamper Calibration Dataset bytes");
    let runs_before = Entity::find()
        .count(db)
        .await
        .expect("count runs before Calibration Dataset tamper");
    let artifacts_before = CalibrationArtifactEntity::find()
        .count(db)
        .await
        .expect("count artifacts before Calibration Dataset tamper");
    let result = Box::pin(fitter.fit(
        ModelCalibrationFitJobParams {
            model_run_id: ModelRunId::from_v7(),
            request: FitModelCalibratorRequest {
                model_version_id: version.model_version_id,
                calibration_dataset_id,
                method: CalibrationMethod::Platt,
                reason: "reject a tampered Calibration Dataset".to_owned(),
            },
            decision_policy_snapshot_id: policy_snapshot_id,
        },
        Arc::new(NoopProgressSink),
        CancellationToken::new(),
    ))
    .await;
    tokio::fs::write(&path, original)
        .await
        .expect("restore Calibration Dataset bytes");
    assert!(
        matches!(
            &result,
            Err(QuantError::Research(ResearchError::DatasetBuild { detail }))
                if detail.contains("dataset byte hash mismatch")
        ),
        "calibration replay must reject tampered Dataset bytes, got error {:?}",
        result.as_ref().err()
    );
    assert_eq!(
        Entity::find()
            .count(db)
            .await
            .expect("count runs after Calibration Dataset tamper"),
        runs_before,
        "calibration preimage rejection must not create a model run"
    );
    assert_eq!(
        CalibrationArtifactEntity::find()
            .count(db)
            .await
            .expect("count artifacts after Calibration Dataset tamper"),
        artifacts_before,
        "calibration preimage rejection must not persist a calibration artifact"
    );
}

async fn assert_sample_floor_terminal(
    db: &DatabaseConnection,
    fitter: &ModelCalibrationFitService,
    version: &ModelVersionInfo,
    calibration_dataset_id: TrainingDatasetId,
    calibration_dataset_hash: ContentHash,
    policy_snapshot_id: DecisionPolicySnapshotId,
) {
    let dataset = PgTrainingDatasetRepository::new(db.clone())
        .find_by_id(&calibration_dataset_id)
        .await
        .expect("load sample-floor Calibration Dataset")
        .expect("sample-floor Calibration Dataset");
    let artifacts_before = CalibrationArtifactEntity::find()
        .count(db)
        .await
        .expect("count artifacts before sample-floor failure");
    let runs_before = Entity::find()
        .filter(Column::RunKind.eq(ModelRunKind::Calibration))
        .count(db)
        .await
        .expect("count Calibration runs before sample-floor failure");
    let model_run_id = ModelRunId::from_v7();
    let params = ModelCalibrationFitJobParams {
        model_run_id,
        request: FitModelCalibratorRequest {
            model_version_id: version.model_version_id,
            calibration_dataset_id,
            method: CalibrationMethod::Isotonic,
            reason: "record an underpowered isotonic fit".to_owned(),
        },
        decision_policy_snapshot_id: policy_snapshot_id,
    };
    let outcome = Box::pin(fitter.fit(
        params.clone(),
        Arc::new(NoopProgressSink),
        CancellationToken::new(),
    ))
    .await
    .expect("underpowered calibration must return typed evidence");
    let ModelCalibrationFitOutcome::Insufficient {
        sample_count,
        total_sample_count,
        minimum_sample_count,
        outcome_hash,
    } = outcome
    else {
        panic!("underpowered isotonic fit must not persist a calibrator");
    };
    assert!(sample_count < minimum_sample_count);
    assert!(sample_count <= total_sample_count);
    let retry = Box::pin(fitter.fit(params, Arc::new(NoopProgressSink), CancellationToken::new()))
        .await
        .expect("exact underpowered calibration retry");
    assert_eq!(
        retry,
        ModelCalibrationFitOutcome::Insufficient {
            sample_count,
            total_sample_count,
            minimum_sample_count,
            outcome_hash,
        }
    );
    assert_eq!(
        CalibrationArtifactEntity::find()
            .count(db)
            .await
            .expect("count artifacts after sample-floor failure"),
        artifacts_before,
        "sample-floor terminal must not persist a calibration artifact"
    );
    assert_eq!(
        Entity::find()
            .filter(Column::RunKind.eq(ModelRunKind::Calibration))
            .count(db)
            .await
            .expect("count Calibration runs after sample-floor failure"),
        runs_before + 1,
        "verified replay must have exactly one terminal Calibration run"
    );
    let terminal = Entity::find_by_id(model_run_id)
        .one(db)
        .await
        .expect("load insufficient Calibration run")
        .expect("insufficient Calibration run");
    assert_eq!(
        terminal.model_version_id,
        Some(version.model_version_id),
        "terminal run source model"
    );
    assert_eq!(
        terminal.decision_policy_snapshot_id, policy_snapshot_id,
        "terminal run policy snapshot"
    );
    assert_eq!(terminal.window_start, dataset.window_start);
    assert_eq!(terminal.window_end, dataset.window_end);
    assert_eq!(terminal.input_hash, calibration_dataset_hash);
    assert_eq!(terminal.status, ModelRunStatus::Succeeded);
    assert_eq!(terminal.output_hash, Some(outcome_hash));
    assert!(terminal.error_code.is_none());
    assert!(terminal.error_message.is_none());
    assert!(
        terminal.finished_at.is_some(),
        "insufficient run must be terminal"
    );
}

async fn assert_model_rejected(
    db: &DatabaseConnection,
    artifact_root: &Path,
    backtester: &BacktestService,
    version: &ModelVersionInfo,
    evaluation_dataset_id: TrainingDatasetId,
    policy_snapshot_id: DecisionPolicySnapshotId,
) {
    let key = ModelArtifact::artifact_key(&version.artifact_hash).expect("model artifact key");
    let path = artifact_root.join(key.relative_path());
    let original = tokio::fs::read(&path).await.expect("read model artifact");
    tokio::fs::write(&path, b"tampered model artifact")
        .await
        .expect("tamper model artifact");
    let runs_before = Entity::find()
        .count(db)
        .await
        .expect("count runs before model tamper");
    let result = backtester
        .run(
            BacktestInput {
                model_version_id: version.model_version_id,
                evaluation_dataset_id,
                decision_policy_snapshot_id: policy_snapshot_id,
                backtest_report_id: None,
            },
            Arc::new(NoopProgressSink),
            CancellationToken::new(),
        )
        .await;
    tokio::fs::write(&path, original)
        .await
        .expect("restore model artifact");
    assert!(
        result.is_err(),
        "backtest must reject tampered model payload bytes"
    );
    assert_eq!(
        Entity::find()
            .count(db)
            .await
            .expect("count runs after model tamper"),
        runs_before,
        "model payload tamper must not create a model run"
    );
}

struct TrainingBacktestScenario {
    policy_snapshot_id: DecisionPolicySnapshotId,
    model_spec: ModelSpecInfo,
    training_dataset_id: TrainingDatasetId,
    training_dataset_hash: ContentHash,
    evaluation_dataset_id: TrainingDatasetId,
    calibration_dataset_id: TrainingDatasetId,
    calibration_dataset_hash: ContentHash,
    registry: Arc<dyn ModelRegistryRepository>,
    version: ModelVersionInfo,
}

async fn prepare_training_scenario(
    db: &DatabaseConnection,
    store: &Arc<dyn ArtifactStore>,
) -> TrainingBacktestScenario {
    let policy_snapshot_id = seed_runtime_config(db).await;
    let model_spec = ModelSpecFixture::weighted(db).await;
    seed_catalog(db).await;
    let training_dataset = SeededDataset::persist(
        db,
        store,
        TrainingDatasetSeed {
            model_spec: &model_spec,
            policy_snapshot_id,
            label_name: settlement(),
            examples: examples(),
            purpose: DatasetPurpose::Training,
            scope: "train-backtest-training",
            factor_serving_plane: None,
        },
    )
    .await;
    let evaluation_dataset = SeededDataset::persist(
        db,
        store,
        TrainingDatasetSeed {
            model_spec: &model_spec,
            policy_snapshot_id,
            label_name: settlement(),
            examples: evaluation_examples(),
            purpose: DatasetPurpose::Evaluation,
            scope: "train-backtest-evaluation",
            factor_serving_plane: None,
        },
    )
    .await;
    let calibration_dataset = SeededDataset::persist(
        db,
        store,
        TrainingDatasetSeed {
            model_spec: &model_spec,
            policy_snapshot_id,
            label_name: settlement(),
            examples: calibration_examples(),
            purpose: DatasetPurpose::Calibration,
            scope: "train-backtest-calibration",
            factor_serving_plane: None,
        },
    )
    .await;
    let registry: Arc<dyn ModelRegistryRepository> =
        Arc::new(PgModelRegistryRepository::new(db.clone()));
    let trainer = TrainerFixture::build(
        db,
        Arc::clone(store),
        Arc::clone(&registry),
        Arc::new(PgFactorRepository::new(db.clone())),
        policy_snapshot_id,
    )
    .await;
    let outcome = Box::pin(trainer.train(
        TrainInputFixture::for_dataset(&model_spec, training_dataset.id),
        &NoopProgressSink,
        &CancellationToken::new(),
    ))
    .await
    .expect("train");
    let version = outcome.version;
    assert_eq!(
        version.training_dataset_id.as_ref(),
        Some(&training_dataset.id)
    );
    WeightedVersionContract { version: &version }.assert_metrics();
    assert_artifact_pooled_scope(store, &version).await;
    assert_training_run_ledger(db, &version, training_dataset.hash).await;

    TrainingBacktestScenario {
        policy_snapshot_id,
        model_spec,
        training_dataset_id: training_dataset.id,
        training_dataset_hash: training_dataset.hash,
        evaluation_dataset_id: evaluation_dataset.id,
        calibration_dataset_id: calibration_dataset.id,
        calibration_dataset_hash: calibration_dataset.hash,
        registry,
        version,
    }
}

struct BacktestScenarioServices {
    backtester: BacktestService,
    backtest_port: Arc<CoreBacktestPort>,
    calibration_fitter: ModelCalibrationFitService,
    calibration_repo: Arc<dyn CalibrationArtifactRepository>,
    policy_hash: ContentHash,
}

async fn build_backtest_services(
    db: &DatabaseConnection,
    store: &Arc<dyn ArtifactStore>,
    scenario: &TrainingBacktestScenario,
) -> BacktestScenarioServices {
    let calibration_repo: Arc<dyn CalibrationArtifactRepository> =
        Arc::new(PgCalibrationArtifactRepository::new(db.clone()));
    let dataset_repo: Arc<dyn TrainingDatasetRepository> =
        Arc::new(PgTrainingDatasetRepository::new(db.clone()));
    let (serving_preimages, _trade_policy_preimages) = TrainerPreimageFixture::build(
        db,
        store,
        &scenario.registry,
        &dataset_repo,
        &calibration_repo,
    );
    let policy_repo: Arc<dyn PolicyRepository> = Arc::new(PgPolicyRepository::new(db.clone()));
    let compute = Arc::new(ComputeExecutor::new().expect("test compute executor"));
    let model_run_repo: Arc<dyn ModelRunRepository> =
        Arc::new(PgModelRunRepository::new(db.clone()));
    let backtest_report_repo: Arc<dyn BacktestReportRepository> =
        Arc::new(PgBacktestReportRepository::new(db.clone()));
    let comparison_report_repo: Arc<dyn ModelComparisonReportRepository> =
        Arc::new(PgModelComparisonReportRepository::new(db.clone()));
    let policy = policy_repo
        .load_snapshot(&scenario.policy_snapshot_id)
        .await
        .expect("load backtest policy")
        .expect("backtest policy");
    let backtester = BacktestService::new(
        BacktestServiceDeps {
            compute: Arc::clone(&compute),
            dataset_repo: Arc::clone(&dataset_repo),
            artifact_store: Arc::clone(store),
            model_registry_repo: Arc::clone(&scenario.registry),
            model_run_repo: Arc::clone(&model_run_repo),
            backtest_report_repo: Arc::clone(&backtest_report_repo),
            comparison_report_repo: Arc::clone(&comparison_report_repo),
            serving_preimages: Arc::clone(&serving_preimages),
        },
        &policy,
    )
    .expect("backtest service");
    let backtest_port = Arc::new(CoreBacktestPort::new(CoreBacktestPortDeps {
        compute,
        dataset_repo: Arc::clone(&dataset_repo),
        artifact_store: Arc::clone(store),
        model_registry_repo: Arc::clone(&scenario.registry),
        model_run_repo: Arc::clone(&model_run_repo),
        backtest_report_repo,
        comparison_report_repo,
        runtime_config: Arc::clone(&policy_repo),
        serving_preimages,
    }));
    let calibration_fitter = ModelCalibrationFitService::new(
        Arc::clone(&backtest_port),
        Arc::clone(&scenario.registry),
        Arc::clone(&dataset_repo),
        Arc::clone(&calibration_repo),
        model_run_repo,
        policy_repo,
    );
    BacktestScenarioServices {
        backtester,
        backtest_port,
        calibration_fitter,
        calibration_repo,
        policy_hash: policy.snapshot_hash,
    }
}

fn fixture_hash(label: &str) -> ContentHash {
    CanonicalDigest::content_hash_typed("quant-pivot:test:feedback-comparison", 1, &label)
        .expect("feedback-comparison fixture hash")
}

async fn train_comparison_candidate(
    db: &DatabaseConnection,
    store: &Arc<dyn ArtifactStore>,
    scenario: &TrainingBacktestScenario,
) -> ModelVersionInfo {
    let trainer = TrainerFixture::build(
        db,
        Arc::clone(store),
        Arc::clone(&scenario.registry),
        Arc::new(PgFactorRepository::new(db.clone())),
        scenario.policy_snapshot_id,
    )
    .await;
    Box::pin(trainer.train(
        TrainInputFixture::for_dataset(&scenario.model_spec, scenario.training_dataset_id),
        &NoopProgressSink,
        &CancellationToken::new(),
    ))
    .await
    .expect("train distinct comparison candidate")
    .version
}

async fn comparison_replay_fixture(
    db: &DatabaseConnection,
    store: &Arc<dyn ArtifactStore>,
    scenario: &TrainingBacktestScenario,
    candidate: &ModelVersionInfo,
) -> (FeedbackComparisonJobParams, Vec<ArtifactUri>) {
    let dataset = PgTrainingDatasetRepository::new(db.clone())
        .find_by_id(&scenario.evaluation_dataset_id)
        .await
        .expect("load comparison Evaluation Dataset")
        .expect("comparison Evaluation Dataset");
    let materialization = dataset
        .materialization()
        .expect("comparison Evaluation materialization");
    let manifest_uri = dataset.source_lineage.source_slice.manifest_uri.clone();
    let manifest_bytes = store
        .get(&manifest_uri)
        .await
        .expect("read comparison Source Slice manifest fixture");
    let source_manifest = serde_json::from_slice::<SourceSliceManifest>(&manifest_bytes)
        .expect("decode comparison Source Slice manifest fixture");
    let mut read_targets = Vec::with_capacity(source_manifest.objects.len() + 2);
    read_targets.push(materialization.parquet_uri.clone());
    read_targets.push(manifest_uri);
    read_targets.extend(
        source_manifest
            .objects
            .iter()
            .map(|object| object.uri.clone()),
    );

    let cycle_idempotency_hash = fixture_hash("cycle");
    let feedback_cycle_id = FeedbackCycleId::from_idempotency_hash(&cycle_idempotency_hash);
    let candidate_family_hash = fixture_hash("candidate-family");
    let comparison_contract = FeedbackComparisonContract::try_from_policy(
        &dataset
            .research_profile_artifact_id
            .profile_ref()
            .resolve_builtin_research_profile()
            .expect("resolve comparison profile")
            .spec
            .feedback_policy,
    )
    .expect("freeze comparison contract");
    let cpcv_artifact_uri =
        ArtifactUri::parse("s3://feedback-comparison-fixture/cpcv.json").expect("CPCV fixture URI");
    let cpcv_artifact_hash = fixture_hash("cpcv-artifact");
    let previous = FeedbackLearningStageArtifactRef {
        feedback_cycle_id,
        stage: FeedbackStage::Cpcv,
        job_id: ResearchJobId::from_v7(),
        artifact_id: FeedbackLearningStageArtifactId::from_cycle_stage(
            feedback_cycle_id,
            FeedbackStage::Cpcv,
        )
        .expect("CPCV artifact identity"),
        input_hash: fixture_hash("cpcv-input"),
        artifact: ResearchJobArtifactRef {
            uri: cpcv_artifact_uri.clone(),
            content_hash: cpcv_artifact_hash,
        },
    };
    let semantic_use_hash = fixture_hash("evaluation-semantic-use");
    let evaluation_use = FeedbackEvaluationUseRef {
        feedback_evaluation_use_id: FeedbackEvaluationUseId::from_semantic_hash(&semantic_use_hash),
        feedback_cycle_id,
        profile_ref: dataset.research_profile_artifact_id.profile_ref(),
        evaluation_dataset_id: dataset.training_dataset_id,
        evaluation_dataset_hash: *materialization.dataset_hash,
        evaluation_artifact_bytes_hash: *materialization.artifact_bytes_hash,
        cohort_manifest_hash: CanonicalDigest::content_hash_json(
            dataset
                .cohort_manifest
                .as_ref()
                .expect("comparison Evaluation cohort"),
        )
        .expect("comparison cohort hash"),
        evaluation_window_start: dataset.window_start,
        evaluation_window_end: dataset.window_end,
        label_cutoff: dataset.pit_cutoff,
        champion_model_version_id: scenario.version.model_version_id,
        champion_serving_contract_hash: scenario.version.serving_contract_hash,
        candidate_family_hash,
        comparison_contract_hash: comparison_contract.comparison_contract_hash(),
        semantic_use_hash,
        cpcv_artifact_uri,
        cpcv_artifact_hash,
        evaluation_use_hash: fixture_hash("evaluation-use"),
    };
    let artifact_id = FeedbackComparisonArtifactId::from_cycle_id(feedback_cycle_id);
    let candidate_ref = FeedbackComparisonCandidateRef {
        candidate_recipe_hash: fixture_hash("candidate-recipe"),
        model_version_id: candidate.model_version_id,
        serving_contract_hash: candidate.serving_contract_hash,
        path_set_id: BacktestPathSetId::from_v7(),
        path_set_hash: fixture_hash("candidate-path-set"),
        model_run_id: ModelRunId::from_feedback_comparison(artifact_id, candidate.model_version_id),
        backtest_report_id: BacktestReportId::from_feedback_comparison(
            artifact_id,
            candidate.model_version_id,
        ),
    };
    let params = FeedbackComparisonJobParams::try_new(FeedbackComparisonJobInput {
        feedback_cycle_id,
        cycle_idempotency_hash,
        candidate_family_hash,
        previous,
        evaluation_use,
        comparison_contract,
        decision_policy_snapshot_id: scenario.policy_snapshot_id,
        champion_model_version_id: scenario.version.model_version_id,
        champion_serving_contract_hash: scenario.version.serving_contract_hash,
        candidates: vec![candidate_ref],
    })
    .expect("freeze comparison replay params");
    (params, read_targets)
}

async fn assert_calibration_persisted(
    db: &DatabaseConnection,
    scenario: &TrainingBacktestScenario,
    services: &BacktestScenarioServices,
) {
    let calibration_info = PgTrainingDatasetRepository::new(db.clone())
        .find_by_id(&scenario.calibration_dataset_id)
        .await
        .expect("load verified Calibration Dataset")
        .expect("verified Calibration Dataset");
    let fit_params = ModelCalibrationFitJobParams {
        model_run_id: ModelRunId::from_v7(),
        request: FitModelCalibratorRequest {
            model_version_id: scenario.version.model_version_id,
            calibration_dataset_id: scenario.calibration_dataset_id,
            method: CalibrationMethod::Platt,
            reason: "persist an exact-preimage model calibrator".to_owned(),
        },
        decision_policy_snapshot_id: scenario.policy_snapshot_id,
    };
    let model_run_id = fit_params.model_run_id;
    let fit_outcome = Box::pin(services.calibration_fitter.fit(
        fit_params.clone(),
        Arc::new(NoopProgressSink),
        CancellationToken::new(),
    ))
    .await
    .expect("fit model calibrator");
    let ModelCalibrationFitOutcome::Calibrated {
        artifact_id: calibration_artifact_id,
        sample_count,
    } = fit_outcome
    else {
        panic!("successful Platt fit must persist an artifact");
    };
    let retry_outcome = Box::pin(services.calibration_fitter.fit(
        fit_params,
        Arc::new(NoopProgressSink),
        CancellationToken::new(),
    ))
    .await
    .expect("exact model-calibration retry");
    assert_eq!(retry_outcome, fit_outcome);
    let calibration_artifact = services
        .calibration_repo
        .find_by_id(&calibration_artifact_id)
        .await
        .expect("load persisted model calibrator")
        .expect("persisted model calibrator");
    assert_eq!(calibration_artifact.kind, CalibrationKind::ModelScore);
    assert_eq!(
        calibration_artifact.fit_window_start,
        calibration_info.window_start
    );
    assert_eq!(
        calibration_artifact.fit_window_end,
        calibration_info.window_end
    );
    assert_eq!(
        u64::try_from(calibration_artifact.sample_count)
            .expect("non-negative calibration sample count"),
        sample_count
    );
    let successful_run = Entity::find_by_id(model_run_id)
        .one(db)
        .await
        .expect("load successful Calibration run")
        .expect("successful Calibration run");
    assert_eq!(
        successful_run.decision_policy_snapshot_id,
        scenario.policy_snapshot_id
    );
    assert_eq!(successful_run.window_start, calibration_info.window_start);
    assert_eq!(successful_run.window_end, calibration_info.window_end);
    assert_eq!(successful_run.input_hash, scenario.calibration_dataset_hash);
    assert_eq!(
        successful_run.output_hash,
        Some(calibration_artifact.content_hash),
        "successful Calibration run output must be the retrievable artifact commitment"
    );
    assert!(successful_run.error_code.is_none());
    assert!(successful_run.error_message.is_none());
    assert!(
        successful_run.finished_at.is_some(),
        "successful Calibration run must be terminal"
    );
    let CalibrationArtifactPayload::ModelScore(payload) = &calibration_artifact.payload else {
        panic!("model-score fitter persisted a non-model-score payload");
    };
    let payload = payload.as_ref();
    payload
        .validate_contract()
        .expect("persisted calibration payload contract");
    let fit = &payload.fit_contract;
    assert_eq!(
        fit.model.model_version_id,
        scenario.version.model_version_id
    );
    assert_eq!(fit.model.artifact_hash, scenario.version.artifact_hash);
    assert_eq!(
        fit.model.serving_contract_hash,
        scenario.version.serving_contract_hash
    );
    assert_eq!(fit.model.model_spec_id, scenario.model_spec.model_spec_id);
    assert_eq!(
        fit.model.model_spec_definition_hash,
        scenario.model_spec.definition_hash
    );
    assert_eq!(fit.model.training_dataset_id, scenario.training_dataset_id);
    assert_eq!(
        fit.model.training_dataset_hash,
        scenario.training_dataset_hash
    );
    assert_eq!(
        fit.calibration_dataset.calibration_dataset_id,
        scenario.calibration_dataset_id
    );
    assert_eq!(
        fit.calibration_dataset.dataset_hash,
        scenario.calibration_dataset_hash
    );
    assert_eq!(
        Some(fit.calibration_dataset.manifest_hash),
        calibration_info.manifest_hash
    );
    assert_eq!(
        Some(fit.calibration_dataset.artifact_bytes_hash),
        calibration_info.artifact_bytes_hash
    );
    assert_eq!(
        fit.calibration_dataset.source_slice_manifest_hash,
        calibration_info.source_lineage.source_slice.manifest_hash
    );
    assert_eq!(
        fit.calibration_dataset.feature_schema_hash,
        calibration_info.feature_schema_hash
    );
    assert_eq!(
        fit.calibration_dataset.factor_schema_hash,
        calibration_info.factor_schema_hash
    );
    assert_eq!(
        Some(fit.calibration_dataset.label_schema_hash),
        calibration_info.label_schema_hash
    );
    assert_eq!(
        fit.policy_snapshot.decision_policy_snapshot_id,
        scenario.policy_snapshot_id
    );
    assert_eq!(fit.policy_snapshot.snapshot_hash, services.policy_hash);
    fit.validate().expect("calibration fit contract");
}

pub async fn train_backtest_evaluation_e2e() {
    let (pool, _container) = setup_pg().await;
    let db = pool.connection().clone();
    let artifact_root = env::temp_dir().join(format!("qp_tb_e2e_{}", Uuid::new_v4().simple()));
    let inner: Arc<dyn ArtifactStore> = Arc::new(LocalArtifactStore::new(&artifact_root));
    let store: Arc<dyn ArtifactStore> = Arc::new(VersionedArtifactStoreFixture::new(inner));

    let scenario = Box::pin(prepare_training_scenario(&db, &store)).await;
    let services = build_backtest_services(&db, &store, &scenario).await;

    assert_policy_rejected(
        &db,
        &services.backtester,
        &scenario.version,
        scenario.evaluation_dataset_id,
    )
    .await;

    let report = services
        .backtester
        .run(
            BacktestInput {
                model_version_id: scenario.version.model_version_id,
                evaluation_dataset_id: scenario.evaluation_dataset_id,
                decision_policy_snapshot_id: scenario.policy_snapshot_id,
                backtest_report_id: None,
            },
            Arc::new(NoopProgressSink),
            CancellationToken::new(),
        )
        .await
        .expect("backtest");
    assert!(
        report.sample_count > 0,
        "replay produced no resolved sample: {report:#?}"
    );
    assert_eq!(report.model_version_id, scenario.version.model_version_id);
    assert!(
        report
            .report_hash
            .canonical_text()
            .as_bytes()
            .starts_with(b"blake3:"),
        "report hash persisted"
    );

    let next = scenario
        .registry
        .next_version_for_spec(&scenario.model_spec.model_spec_id)
        .await
        .expect("count");
    assert_eq!(
        next, 2,
        "Evaluation backtests must not register a derived model version"
    );
    assert_cache_rejected(
        &db,
        &services.backtest_port,
        &scenario.version,
        scenario.evaluation_dataset_id,
        scenario.policy_snapshot_id,
        report.backtest_report_id,
    )
    .await;
    assert_dataset_rejected(
        &db,
        &services.backtester,
        &scenario.version,
        scenario.evaluation_dataset_id,
        scenario.policy_snapshot_id,
    )
    .await;
    assert_source_rejected(
        &db,
        &services.backtester,
        &scenario.version,
        scenario.evaluation_dataset_id,
        scenario.policy_snapshot_id,
    )
    .await;
    assert_model_rejected(
        &db,
        &artifact_root,
        &services.backtester,
        &scenario.version,
        scenario.evaluation_dataset_id,
        scenario.policy_snapshot_id,
    )
    .await;
    assert_calibration_rejected(
        &db,
        &services.calibration_fitter,
        &scenario.version,
        scenario.calibration_dataset_id,
        scenario.policy_snapshot_id,
    )
    .await;
    assert_sample_floor_terminal(
        &db,
        &services.calibration_fitter,
        &scenario.version,
        scenario.calibration_dataset_id,
        scenario.calibration_dataset_hash,
        scenario.policy_snapshot_id,
    )
    .await;
    assert_calibration_persisted(&db, &scenario, &services).await;
}

pub async fn comparison_reuses_inputs() {
    let (pool, _container) = setup_pg().await;
    let db = pool.connection().clone();
    let artifact_root =
        env::temp_dir().join(format!("qp_feedback_compare_{}", Uuid::new_v4().simple()));
    let inner: Arc<dyn ArtifactStore> = Arc::new(LocalArtifactStore::new(&artifact_root));
    let base_store: Arc<dyn ArtifactStore> = Arc::new(VersionedArtifactStoreFixture::new(inner));
    let scenario = Box::pin(prepare_training_scenario(&db, &base_store)).await;
    let candidate = train_comparison_candidate(&db, &base_store, &scenario).await;
    assert_ne!(
        candidate.model_version_id, scenario.version.model_version_id,
        "comparison requires distinct champion and challenger identities"
    );
    assert_ne!(
        candidate.serving_contract_hash, scenario.version.serving_contract_hash,
        "distinct model versions require distinct serving contracts"
    );
    let (params, read_targets) =
        comparison_replay_fixture(&db, &base_store, &scenario, &candidate).await;
    let counted = Arc::new(ReadCountingArtifactStoreFixture::new(
        base_store,
        read_targets.clone(),
    ));
    let counted_store: Arc<dyn ArtifactStore> =
        Arc::<ReadCountingArtifactStoreFixture>::clone(&counted);
    let dataset_repo: Arc<dyn TrainingDatasetRepository> =
        Arc::new(PgTrainingDatasetRepository::new(db.clone()));
    let calibration_repo: Arc<dyn CalibrationArtifactRepository> =
        Arc::new(PgCalibrationArtifactRepository::new(db.clone()));
    let (serving_preimages, _trade_policy_preimages) = TrainerPreimageFixture::build(
        &db,
        &counted_store,
        &scenario.registry,
        &dataset_repo,
        &calibration_repo,
    );
    serving_preimages
        .load(&scenario.version)
        .await
        .expect("load champion preimage baseline");
    serving_preimages
        .load(&candidate)
        .await
        .expect("load candidate preimage baseline");
    let preimage_reads = read_targets
        .iter()
        .map(|uri| counted.reads(uri))
        .collect::<Vec<_>>();
    counted.reset();
    let services = build_backtest_services(&db, &counted_store, &scenario).await;

    let replay = Box::pin(services.backtester.replay_feedback_family(
        &params,
        Arc::new(NoopProgressSink),
        CancellationToken::new(),
    ))
    .await
    .expect("replay one shared Evaluation input across the candidate family");
    assert_eq!(
        replay.champion.model_version_id,
        scenario.version.model_version_id
    );
    assert_eq!(replay.candidates.len(), 1);
    assert_eq!(
        replay.candidates[0].model_version_id,
        candidate.model_version_id
    );
    assert!(
        !replay.champion.portfolio_returns.is_empty(),
        "shared replay must retain decision-tick observations"
    );
    assert_eq!(
        replay.champion.portfolio_returns.len(),
        replay.candidates[0].portfolio_returns.len(),
        "champion and challenger must share the same observation universe"
    );
    for (uri, preimage_read_count) in read_targets.into_iter().zip(preimage_reads) {
        assert_eq!(
            counted.reads(&uri),
            preimage_read_count + 1,
            "family replay must add exactly one shared Evaluation read after the two full serving-preimage reads: {uri}"
        );
    }
}

struct CpcvTrainingScenario {
    policy_snapshot_id: DecisionPolicySnapshotId,
    training_dataset_id: TrainingDatasetId,
    registry: Arc<dyn ModelRegistryRepository>,
    version: ModelVersionInfo,
    coord_search_effective_n: u32,
}

async fn prepare_cpcv_scenario(
    db: &DatabaseConnection,
    store: &Arc<dyn ArtifactStore>,
) -> CpcvTrainingScenario {
    let policy_snapshot_id = seed_runtime_config(db).await;
    let model_spec = ModelSpecFixture::weighted(db).await;
    seed_catalog(db).await;
    let training_dataset = SeededDataset::persist(
        db,
        store,
        TrainingDatasetSeed {
            model_spec: &model_spec,
            policy_snapshot_id,
            label_name: settlement(),
            examples: examples(),
            purpose: DatasetPurpose::Training,
            scope: "train-cpcv-training",
            factor_serving_plane: None,
        },
    )
    .await;
    let registry: Arc<dyn ModelRegistryRepository> =
        Arc::new(PgModelRegistryRepository::new(db.clone()));
    let trainer = TrainerFixture::build(
        db,
        Arc::clone(store),
        Arc::clone(&registry),
        Arc::new(PgFactorRepository::new(db.clone())),
        policy_snapshot_id,
    )
    .await;
    let outcome = Box::pin(trainer.train(
        TrainInputFixture::for_dataset(&model_spec, training_dataset.id),
        &NoopProgressSink,
        &CancellationToken::new(),
    ))
    .await
    .expect("train");
    let version = outcome.version;
    let ModelVersionMetricsDefinition::LearningToRank { validation, .. } =
        &version.metrics.definition
    else {
        panic!("weighted training must persist learning-to-rank metrics");
    };
    let coord_search_effective_n = validation.coordinate_search_effective_trials;
    assert!(
        coord_search_effective_n >= 1,
        "trainer must persist coord_search_effective_n ≥ 1, got {coord_search_effective_n}"
    );
    CpcvTrainingScenario {
        policy_snapshot_id,
        training_dataset_id: training_dataset.id,
        registry,
        version,
        coord_search_effective_n,
    }
}

pub async fn train_cpcv_persists_decomposition() {
    let (pool, _container) = setup_pg().await;
    let db = pool.connection().clone();
    let inner: Arc<dyn ArtifactStore> = Arc::new(LocalArtifactStore::new(
        env::temp_dir().join(format!("qp_cpcv_e2e_{}", Uuid::new_v4().simple())),
    ));
    let store: Arc<dyn ArtifactStore> = Arc::new(VersionedArtifactStoreFixture::new(inner));

    let CpcvTrainingScenario {
        policy_snapshot_id: rc_id,
        training_dataset_id: dataset_id,
        registry,
        version,
        coord_search_effective_n: coord_n,
    } = prepare_cpcv_scenario(&db, &store).await;

    let path_set_id = BacktestPathSetId::from_v7();
    let dataset_repo: Arc<dyn TrainingDatasetRepository> =
        Arc::new(PgTrainingDatasetRepository::new(db.clone()));
    let calibration_repo: Arc<dyn CalibrationArtifactRepository> =
        Arc::new(PgCalibrationArtifactRepository::new(db.clone()));
    let path_set_repo: Arc<dyn BacktestPathSetRepository> =
        Arc::new(PgBacktestPathSetRepository::new(db.clone()));
    let model_run_repo: Arc<dyn ModelRunRepository> =
        Arc::new(PgModelRunRepository::new(db.clone()));
    let (serving_preimages, _trade_policy_preimages) =
        TrainerPreimageFixture::build(&db, &store, &registry, &dataset_repo, &calibration_repo);
    let port = CoreCpcvBacktestPort::new(CoreCpcvBacktestPortDeps {
        compute: Arc::new(ComputeExecutor::new().expect("test compute executor")),
        artifact_store: Arc::clone(&store),
        path_set_repo,
        model_registry_repo: Arc::clone(&registry),
        model_run_repo,
        bias_table_repo: calibration_repo,
        serving_preimages,
    });
    let model_run_id = ModelRunId::from_v7();
    let params = CpcvBacktestJobParams {
        model_version_id: version.model_version_id,
        model_run_id,
        request: RunCpcvBacktestRequest {
            training_dataset_id: dataset_id,
            decision_policy_snapshot_id: rc_id,
            reason: "persist exact CPCV decomposition".to_owned(),
            path_set_id: Some(path_set_id),
        },
    };
    let view = port
        .run(
            params.clone(),
            Arc::new(NoopProgressSink),
            CancellationToken::new(),
        )
        .await
        .expect("run canonical CPCV port");
    let retry = port
        .run(params, Arc::new(NoopProgressSink), CancellationToken::new())
        .await
        .expect("exact CPCV port retry");
    assert_eq!(retry.path_set_id, view.path_set_id);
    assert_eq!(retry.model_run_id, view.model_run_id);
    assert_eq!(retry.path_set_hash, view.path_set_hash);
    assert_eq!(retry.subject, view.subject);
    assert_eq!(retry.methodology, view.methodology);

    assert_cpcv_view_bind(&db, &registry, &version, &view, &path_set_id, coord_n).await;
}

async fn assert_cpcv_view_bind(
    db: &DatabaseConnection,
    registry: &Arc<dyn ModelRegistryRepository>,
    version: &ModelVersionInfo,
    view: &BacktestPathSetView,
    path_set_id: &BacktestPathSetId,
    coord_n: u32,
) {
    assert_eq!(view.path_set_id, *path_set_id);
    assert_eq!(
        view.trial_count, view.trial_grid_count,
        "DSR N must equal the governed trial-grid count (same population as V)"
    );
    assert_eq!(
        view.coord_search_effective_n,
        i64::from(coord_n),
        "coord_search_effective_n is audit-only and must still be persisted"
    );
    assert_eq!(view.path_count, 3);
    assert_eq!(view.combination_count, 6);
    assert!(
        view.path_set_hash.as_bytes().iter().any(|byte| *byte != 0),
        "canonical CPCV persistence must seal a content hash"
    );

    let cpcv_run = Entity::find()
        .filter(Column::RunKind.eq(ModelRunKind::Cpcv))
        .filter(Column::Status.eq(ModelRunStatus::Succeeded))
        .one(db)
        .await
        .expect("query cpcv run")
        .expect("canonical CPCV persistence must create ModelRunKind::Cpcv");
    assert_eq!(cpcv_run.model_run_id, view.model_run_id);
    assert_eq!(
        cpcv_run.output_hash,
        Some(view.path_set_hash),
        "successful CPCV run must bind the exact sealed path-set hash"
    );
    assert_eq!(
        cpcv_run.model_version_id,
        Some(version.model_version_id),
        "successful CPCV run must bind the exact serving subject"
    );
    assert_eq!(cpcv_run.window_start, view.window_start);
    assert_eq!(cpcv_run.window_end, view.window_end);

    let bound = registry
        .find_model_version(&version.model_version_id)
        .await
        .expect("reload version")
        .expect("version");
    assert!(
        bound.publish_path_set_id.is_none(),
        "CPCV must not auto-bind publish_path_set_id; explicit governance bind required"
    );

    registry
        .set_publish_path(&version.model_version_id, Some(*path_set_id))
        .await
        .expect("explicit bind for publish gate");

    let bound = registry
        .find_model_version(&version.model_version_id)
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
