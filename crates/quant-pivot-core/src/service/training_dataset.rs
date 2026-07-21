//! Offline training-dataset orchestration.
//!
//! Plans a deterministic sample grid, batch-prefetches every historical fact the
//! build needs (book snapshots, microstructure, market metadata, settlements),
//! serves point-in-time lookups from an in-memory
//! `MaterializedPitEngine` so the build loop issues zero DB queries, then runs
//! the **same** feature builder and factor engine per `decision_at` cross-section
//! the online path uses, attaches forward-looking labels, asserts no future
//! leakage, materializes a content-hashed Parquet artifact, and records the
//! ledger row. Features are bounded by the source cutoffs frozen in each
//! [`DecisionBoundary`]; labels look strictly forward from `decision_at`; the
//! dataset hash makes the whole thing reproducible.

use std::{
    collections::{BTreeMap, HashMap, HashSet},
    sync::Arc,
    time::Duration,
};

use async_trait::async_trait;
use chrono::{DateTime, Duration as ChronoDuration, Utc};
use quant_pivot_error::{QuantError, QuantResult, research::ResearchError, storage::StorageError};
use quant_pivot_models::{
    clickhouse::{BookMicrostructureRow, ChPrice},
    domain::{
        data_plane::{DecisionBoundary, DecisionClock, DecisionSource},
        market::{MarketInfo, MarketRegistryInfo, fee::MarketFeeSchedule},
        quant::{
            CompleteTrainingDatasetBuild, ExitTrainingLotRow, FeatureVectorInfo, JobProgressSink,
            MarketSelectionMemberInfo, NewTrainingDatasetPlan, NoopProgressSink,
            RecommendationAttributionInfo, RecommendationInfo, TrainingDatasetInfo,
            TrainingDatasetMaterialization,
        },
        query::TimeWindow,
    },
    enums::{
        common::MarketCategory,
        quant::{
            DatasetPurpose, RecommendationAttributionOutcome, TradePolicyStatus,
            TrainingDatasetStatus,
        },
    },
    hashing::CanonicalDigest,
    runtime_config::{
        DataQualityConfig, DomainConfig, FactorsConfig, FeaturesConfig, SelectionConfig,
        TrainingConfig,
    },
    types::{
        ArtifactUri, ClobMarketInfoVersion, ContentHash, DATASET_ARTIFACT_FORMAT_VERSION,
        DatasetCoverage, DatasetFeatureStateCounts, DatasetManifest, DecisionCaptureEvidence,
        FeatureCellState, FeatureVectorId, MarketId, MarketSelectionId, ModelInputContract,
        ModelSpecId, Price, RecommendationId, ResearchJobProgress, Shares, TokenId,
        TradePolicyArtifactId, TradePolicyArtifactPayload, TrainingDatasetId, TrainingExampleId,
        TrainingHorizonsSecs, TrainingSampleSource, TrainingSampleSources, Usd,
        factor::FactorExplanation, stable_name::FactorName,
    },
};
use quant_pivot_repository::traits::{
    AttributionRepository, CalibrationArtifactRepository, CatalogLedgerRepository,
    ClobMarketInfoRepository, FeatureRepository, MarketLinkageRepository, MarketRepository,
    MarketSelectionRepository, ModelRegistryRepository, PositionRepository,
    QuantFactReadRepository, RecommendationRepository, TradePolicyRepository,
    TrainingDatasetRepository,
};
use quant_pivot_research::{
    artifact::{ArtifactKey, ArtifactNamespace, ArtifactStore},
    execution_semantics::BookFidelity,
    factors::{
        FactorEligibility, FactorEngine, FactorValue, MarketFactorOutcome, NormalizedFactor,
    },
    features::{ConfiguredFeatureBuilder, FeatureVector},
    hashing::ResearchHasher,
    model::{
        FavoriteLongshotBiasTable,
        sell_scorer::{LotStateInput, PositionStateFeatures, position_state_features},
    },
    pit::PointInTimeSnapshotSource,
    selection::{ModelFeatureRequirements, SelectedMarket},
    training::{
        DatasetHashContract, DatasetParquetCodec, DatasetPlan, DatasetPlanRequest, DecisionBook,
        ExitDecisionLabelContext, ForwardWindow, HoldVsExitProceedsLabeler, LabelBuildInput,
        LabelBuildOutput, LabelName, Labeler, LiquidityExitLabeler, LotSamplePlan,
        LotTerminalSnapshot, LotTrainingContext, MaxAdverseExcursionLabeler,
        MaxFavorableExcursionLabeler, PlanMarket, ReturnToHorizonLabeler, SamplePlan,
        SettlementOutcomeLabeler, TrainingDatasetArtifact, TrainingDatasetBuilder,
        TrainingDatasetPlanner, TrainingExample, TrainingLabel, assert_no_future_leakage,
        count_samples, dataset_manifest_hash, dataset_source_fingerprint, label_names_for_sources,
        plan_lot_timeline_samples, plan_samples, probe_matrix_coverage, remaining_shares_at,
    },
};
use rust_decimal::{Decimal, prelude::ToPrimitive};
use tokio::{runtime::Handle, task};
use tokio_util::sync::CancellationToken;

use crate::{
    pit::platform::ch_historical::DurablePitSource,
    prefetch::{
        domain_availability::{LiveDomainAvailabilitySource, PrefetchedDomainAvailabilitySource},
        historical_window::{
            HistoricalWindow, HistoricalWindowLoader, Prefetched, ReplaySample, WindowSpec,
            forward_window, historical_window_from_prefetched, replay_boundary,
        },
        source_slice::FrozenSourceSlice,
    },
    service::{
        calibration_shared::assert_disjoint_from_all_training_datasets,
        historical_replay::{
            CrossSectionRequest, ReplayConfig, ReplayCrossSection, materialize_cross_section,
        },
        pit_selection::OfflinePitSelector,
    },
};

const LIVE_ATTRIBUTION_SAMPLE_LIMIT: u64 = 10_000;
const DATASET_FAILURE_DETAIL_MAX_CHARS: usize = 2_048;

fn bounded_failure_detail(detail: &str) -> String {
    detail
        .chars()
        .take(DATASET_FAILURE_DETAIL_MAX_CHARS)
        .collect()
}

/// Non-materializing dry-run counts for the `plan` endpoint.
#[derive(Debug, Clone, Copy)]
pub struct PlanCounts {
    /// Deterministic historical spine size (selection × alive instants) — an
    /// upper bound before point-in-time eligibility is applied.
    pub spine_upper_bound: u64,
    /// Total samples across all requested sources (spine + attribution + exit),
    /// before point-in-time selection eligibility.
    pub total: u64,
    /// Whether the plan exceeds the configured `max_spine_samples` hard cap.
    pub hard_cap_exceeded: bool,
    /// Estimated total samples after PIT selection: `total` scaled by the sampled
    /// keep-rate (falls back to `total` when the estimate is disabled/unavailable).
    pub estimated_eligible_samples: u64,
    /// Sampled fraction of candidate markets that pass the PIT selection funnel,
    /// in `[0, 1]`. `None` when the estimate is disabled or has no candidates.
    pub keep_rate: Option<f64>,
    /// Number of `(market, slice)` eligibility trials backing [`Self::keep_rate`].
    pub keep_rate_sample_size: u64,
}

#[derive(Debug, Clone, Copy)]
struct KeepRateEstimate {
    included: u64,
    trials: u64,
}

impl KeepRateEstimate {
    fn rate(self) -> QuantResult<f64> {
        if self.trials == 0 || self.included > self.trials {
            return Err(ResearchError::DatasetPlan {
                detail: format!(
                    "invalid keep-rate counts: included={}, trials={}",
                    self.included, self.trials
                ),
            }
            .into());
        }
        let included = self
            .included
            .to_f64()
            .ok_or_else(|| ResearchError::DatasetPlan {
                detail: format!(
                    "keep-rate included count {} is not a finite f64",
                    self.included
                ),
            })?;
        let trials = self
            .trials
            .to_f64()
            .ok_or_else(|| ResearchError::DatasetPlan {
                detail: format!("keep-rate trial count {} is not a finite f64", self.trials),
            })?;
        let rate = included / trials;
        if !rate.is_finite() || !(0.0..=1.0).contains(&rate) {
            return Err(ResearchError::DatasetPlan {
                detail: format!("computed keep rate {rate} is outside [0, 1]"),
            }
            .into());
        }
        Ok(rate)
    }

    fn scale(self, count: u64) -> QuantResult<u64> {
        if self.trials == 0 || self.included > self.trials {
            return Err(ResearchError::DatasetPlan {
                detail: format!(
                    "invalid keep-rate counts: included={}, trials={}",
                    self.included, self.trials
                ),
            }
            .into());
        }
        let numerator = u128::from(count)
            .checked_mul(u128::from(self.included))
            .ok_or_else(|| ResearchError::DatasetPlan {
                detail: "keep-rate scaled-count numerator overflowed u128".to_owned(),
            })?;
        let half = u128::from(self.trials) / 2;
        let rounded = numerator
            .checked_add(half)
            .ok_or_else(|| ResearchError::DatasetPlan {
                detail: "keep-rate rounded numerator overflowed u128".to_owned(),
            })?
            / u128::from(self.trials);
        u64::try_from(rounded).map_err(|error| {
            ResearchError::DatasetPlan {
                detail: format!("keep-rate scaled count does not fit u64: {error}"),
            }
            .into()
        })
    }
}

#[derive(Debug, Clone, Copy)]
struct KeepRateGrid {
    window_start: DateTime<Utc>,
    span_secs: u128,
    slices: u32,
    midpoint_denominator: u128,
    knowledge_lag_secs: u64,
    domain_crypto_lag_secs: u64,
    domain_weather_lag_secs: u64,
}

impl KeepRateGrid {
    fn new(
        request: &DatasetPlanRequest,
        slices: u32,
        domain_crypto_lag_secs: u64,
        domain_weather_lag_secs: u64,
    ) -> QuantResult<Self> {
        let span_secs = (request.window_end - request.window_start).num_seconds();
        if span_secs <= 0 {
            return Err(ResearchError::DatasetPlan {
                detail: "keep-rate estimate requires a positive dataset window".to_owned(),
            }
            .into());
        }
        let span_secs = u128::try_from(span_secs).map_err(|error| ResearchError::DatasetPlan {
            detail: format!("dataset window seconds do not fit u128: {error}"),
        })?;
        let midpoint_denominator = u128::from(slices)
            .checked_mul(2)
            .filter(|value| *value > 0)
            .ok_or_else(|| ResearchError::DatasetPlan {
                detail: "keep-rate slice denominator is zero or overflowed".to_owned(),
            })?;
        Ok(Self {
            window_start: request.window_start,
            span_secs,
            slices,
            midpoint_denominator,
            knowledge_lag_secs: request.knowledge_lag_secs,
            domain_crypto_lag_secs,
            domain_weather_lag_secs,
        })
    }

    fn boundary(self, index: u32) -> QuantResult<DecisionBoundary> {
        if index >= self.slices {
            return Err(ResearchError::DatasetPlan {
                detail: format!(
                    "keep-rate slice index {index} is outside configured grid of {} slices",
                    self.slices
                ),
            }
            .into());
        }
        let midpoint = u128::from(index)
            .checked_mul(2)
            .and_then(|value| value.checked_add(1))
            .ok_or_else(|| ResearchError::DatasetPlan {
                detail: "keep-rate slice midpoint overflowed u128".to_owned(),
            })?;
        let offset =
            self.span_secs
                .checked_mul(midpoint)
                .ok_or_else(|| ResearchError::DatasetPlan {
                    detail: "keep-rate slice offset numerator overflowed u128".to_owned(),
                })?
                / self.midpoint_denominator;
        let offset = i64::try_from(offset).map_err(|error| ResearchError::DatasetPlan {
            detail: format!("keep-rate slice offset does not fit i64: {error}"),
        })?;
        let decision_at = self
            .window_start
            .checked_add_signed(ChronoDuration::seconds(offset))
            .ok_or_else(|| ResearchError::DatasetPlan {
                detail: "keep-rate decision time is outside chrono range".to_owned(),
            })?;
        replay_boundary(
            decision_at,
            self.knowledge_lag_secs,
            self.domain_crypto_lag_secs,
            self.domain_weather_lag_secs,
        )
    }
}

/// The default labeler set materialized by a dataset build.
#[must_use]
pub fn default_labelers() -> Vec<Box<dyn Labeler>> {
    vec![
        Box::new(ReturnToHorizonLabeler),
        Box::new(MaxFavorableExcursionLabeler),
        Box::new(MaxAdverseExcursionLabeler),
        Box::new(LiquidityExitLabeler),
        Box::new(SettlementOutcomeLabeler),
    ]
}

/// Labelers materialized only for [`TrainingSampleSource::ExitDecision`] rows.
#[must_use]
pub fn exit_decision_labelers() -> Vec<Box<dyn Labeler>> {
    vec![
        Box::new(HoldVsExitProceedsLabeler),
        Box::new(LiquidityExitLabeler),
    ]
}

/// Verify exact bytes, the embedded v4 manifest and semantic rows against the
/// immutable dataset ledger.
///
/// This gate runs before any trainer, calibrator or backtest consumes the
/// artifact; callers must never replace its rows with a rematerialized replay.
///
/// The returned examples are the only authorized training input.
pub fn verify_frozen_dataset_artifact(
    dataset: &TrainingDatasetInfo,
    bytes: &[u8],
) -> QuantResult<Vec<TrainingExample>> {
    let materialization = dataset
        .materialization()
        .ok_or_else(|| ResearchError::DatasetBuild {
            detail: format!(
                "dataset {} does not have a complete materialization binding",
                dataset.training_dataset_id
            ),
        })?;
    let expected_bytes_hash = materialization.artifact_bytes_hash;
    let actual_bytes_hash = ContentHash::parse(CanonicalDigest::prefixed_bytes(bytes))?;
    if &actual_bytes_hash != expected_bytes_hash {
        return Err(ResearchError::DatasetBuild {
            detail: format!(
                "frozen dataset byte hash mismatch: recorded {expected_bytes_hash}, loaded {actual_bytes_hash}"
            ),
        }
        .into());
    }

    let decoded = DatasetParquetCodec::decode_with_manifest(bytes)?;
    let expected_manifest_hash = materialization.manifest_hash;
    let actual_manifest_hash = dataset_manifest_hash(&decoded.manifest)?;
    if &actual_manifest_hash != expected_manifest_hash {
        return Err(ResearchError::DatasetBuild {
            detail: format!(
                "frozen dataset manifest hash mismatch: recorded {expected_manifest_hash}, loaded {actual_manifest_hash}"
            ),
        }
        .into());
    }

    let manifest = &decoded.manifest;
    if manifest != materialization.manifest {
        return Err(ResearchError::DatasetBuild {
            detail: format!(
                "frozen dataset manifest payload differs from the ledger for {}",
                dataset.training_dataset_id
            ),
        }
        .into());
    }
    let knowledge_lag_secs =
        u64::try_from(dataset.knowledge_lag_secs).map_err(|error| ResearchError::DatasetBuild {
            detail: format!("negative or invalid dataset knowledge lag: {error}"),
        })?;
    let sample_interval_secs = u64::try_from(dataset.sample_interval_secs).map_err(|error| {
        ResearchError::DatasetBuild {
            detail: format!("negative or invalid dataset sample interval: {error}"),
        }
    })?;
    let sample_count = u64::try_from(materialization.sample_count).map_err(|error| {
        ResearchError::DatasetBuild {
            detail: format!("negative or invalid dataset sample count: {error}"),
        }
    })?;
    let bindings_match = manifest.training_dataset_id == dataset.training_dataset_id
        && manifest.model_spec_id == dataset.model_spec_id
        && manifest.model_spec_definition_hash == dataset.model_spec_definition_hash
        && manifest.decision_policy_snapshot_id == dataset.decision_policy_snapshot_id
        && manifest.window_start == dataset.window_start
        && manifest.window_end == dataset.window_end
        && manifest.purpose == dataset.purpose
        && manifest.knowledge_lag_secs == knowledge_lag_secs
        && manifest.sample_interval_secs == sample_interval_secs
        && manifest.horizons_secs == dataset.horizons_secs.0
        && &manifest.feature_schema_hash == materialization.feature_schema_hash
        && &manifest.factor_schema_hash == materialization.factor_schema_hash
        && &manifest.label_schema_hash == materialization.label_schema_hash
        && &manifest.semantic_dataset_hash == materialization.dataset_hash
        && manifest.sample_count == sample_count;
    if !bindings_match {
        return Err(ResearchError::DatasetBuild {
            detail: format!(
                "dataset {} manifest does not match its immutable ledger bindings",
                dataset.training_dataset_id
            ),
        }
        .into());
    }
    Ok(decoded.examples)
}

/// Require a complete artifact binding without manufacturing values for a
/// planned, building, failed-before-materialization, or legacy row.
pub fn require_dataset_materialization(
    dataset: &TrainingDatasetInfo,
) -> QuantResult<TrainingDatasetMaterialization<'_>> {
    dataset.materialization().ok_or_else(|| {
        ResearchError::DatasetBuild {
            detail: format!(
                "dataset {} has no complete integrity-gated materialization",
                dataset.training_dataset_id
            ),
        }
        .into()
    })
}

/// Dependencies injected into [`TrainingDatasetService`].
pub struct TrainingDatasetServiceDeps {
    /// `ClickHouse` fact reader for batch prefetch.
    pub fact_read: Arc<dyn QuantFactReadRepository>,
    /// Append-only catalog ledger for all historical metadata resolution.
    pub catalog_repo: Arc<dyn CatalogLedgerRepository>,
    /// Postgres market catalog.
    pub market_repo: Arc<dyn MarketRepository>,
    /// Content-addressed artifact store for Parquet output.
    pub artifact_store: Arc<dyn ArtifactStore>,
    /// Training-dataset ledger repository.
    pub dataset_repo: Arc<dyn TrainingDatasetRepository>,
    /// Final attribution rows used as live supervised samples.
    pub attribution_repo: Arc<dyn AttributionRepository>,
    /// Recommendation ledger for frozen evidence refs and factor breakdown.
    pub recommendation_repo: Arc<dyn RecommendationRepository>,
    /// Frozen feature-vector ledger referenced by recommendations.
    pub feature_repo: Arc<dyn FeatureRepository>,
    /// Immutable selection members referenced by live recommendations.
    pub selection_repo: Arc<dyn MarketSelectionRepository>,
    /// Position ledger for closed-lot `ExitDecision` sampling.
    pub position_repo: Arc<dyn PositionRepository>,
    /// Append-only point-in-time CLOB market parameters and fee schedules.
    pub clob_market_info_repo: Arc<dyn ClobMarketInfoRepository>,
    /// Frozen market → external-subject linkage ledger.
    pub linkage_repo: Arc<dyn MarketLinkageRepository>,
    /// Model registry — resolves the target `ModelSpec`'s governed feature
    /// requirements so offline selection genuinely mirrors the online
    /// funnel's `ModelFeatureUnavailable` gate.
    pub model_registry: Arc<dyn ModelRegistryRepository>,
    /// Governed policy catalog used for executable policy-derived labels.
    pub trade_policy_repo: Arc<dyn TradePolicyRepository>,
    /// Published frozen calibration artifacts consumed by PIT feature windows.
    pub calibration_repo: Arc<dyn CalibrationArtifactRepository>,
}

/// Deploy + frozen-config bundle for wiring [`TrainingDatasetService`].
pub struct TrainingDatasetServiceWire {
    /// Frozen runtime-config snapshot bound to one dataset build.
    pub config: TrainingDatasetBuildConfig,
    /// Deploy-level resource guard (not part of the reproducible `dataset_hash`).
    pub max_spine_samples: u64,
}

/// Fail closed on empty/inverted half-open dataset windows (shared by plan/build).
fn require_half_open_dataset_window(
    window_start: DateTime<Utc>,
    window_end: DateTime<Utc>,
) -> QuantResult<()> {
    TimeWindow::try_half_open(window_start, window_end)
        .map_err(|_| {
            ResearchError::DatasetPlan {
                detail: format!("window_start {window_start} must precede window_end {window_end}"),
            }
            .into()
        })
        .map(|_| ())
}

/// Frozen runtime-config snapshot bound to one dataset build.
pub struct TrainingDatasetBuildConfig {
    /// Feature builder configuration.
    pub features: FeaturesConfig,
    /// Factor engine configuration.
    pub factors: FactorsConfig,
    /// External-vertical domain plane configuration.
    pub domain: DomainConfig,
    /// Data-quality gates applied during feature build.
    pub data_quality: DataQualityConfig,
    /// Offline training-dataset build parameters (from runtime `training` section).
    pub training: TrainingConfig,
    /// Selection config — the enabled-category selection gate is applied to the
    /// candidate market set so offline training mirrors the online funnel's
    /// category scope (train/serve consistency), not the entire active catalog.
    pub selection: SelectionConfig,
    /// Labelers materialized per example.
    pub labelers: Vec<Box<dyn Labeler>>,
    /// Favorite-longshot bias table pinned by the frozen factor config (content-
    /// hash verified). `None` keeps `struct.favorite_longshot` inert. Resolved by
    /// the port so offline scoring binds the same table bytes as online serving.
    pub bias_table: Option<Arc<FavoriteLongshotBiasTable>>,
}

/// Orchestrates the offline training-dataset build for one frozen config.
pub struct TrainingDatasetService {
    fact_read: Arc<dyn QuantFactReadRepository>,
    catalog_repo: Arc<dyn CatalogLedgerRepository>,
    market_repo: Arc<dyn MarketRepository>,
    artifact_store: Arc<dyn ArtifactStore>,
    dataset_repo: Arc<dyn TrainingDatasetRepository>,
    attribution_repo: Arc<dyn AttributionRepository>,
    recommendation_repo: Arc<dyn RecommendationRepository>,
    feature_repo: Arc<dyn FeatureRepository>,
    selection_repo: Arc<dyn MarketSelectionRepository>,
    position_repo: Arc<dyn PositionRepository>,
    clob_market_info_repo: Arc<dyn ClobMarketInfoRepository>,
    linkage_repo: Arc<dyn MarketLinkageRepository>,
    model_registry: Arc<dyn ModelRegistryRepository>,
    trade_policy_repo: Arc<dyn TradePolicyRepository>,
    calibration_repo: Arc<dyn CalibrationArtifactRepository>,
    features: FeaturesConfig,
    factors: FactorsConfig,
    domain: DomainConfig,
    data_quality: DataQualityConfig,
    max_book_staleness: Duration,
    min_exit_depth_usd: Usd,
    /// Frozen selection policy (drives the offline point-in-time selection funnel).
    selection: SelectionConfig,
    /// Enabled-category set (derived from [`Self::selection`]) for the cheap
    /// upper-bound candidate prefilter.
    enabled_categories: HashSet<MarketCategory>,
    /// Deploy guard: hard cap on the deterministic historical spine.
    max_spine_samples: u64,
    /// Shared so the historical spine can be built inside a `spawn_blocking`
    /// closure (labelers are `Send + Sync` but not `Clone`).
    labelers: Arc<Vec<Box<dyn Labeler>>>,
    /// Frozen favorite-longshot bias table bound to the offline factor engine.
    bias_table: Option<Arc<FavoriteLongshotBiasTable>>,
}

impl TrainingDatasetService {
    /// Plan exclusively from a verified Source Slice. Dynamic repository reads
    /// for candidate discovery are structurally absent from this path.
    pub async fn plan_with_frozen_source(
        &self,
        request: DatasetPlanRequest,
        source: &FrozenSourceSlice,
    ) -> QuantResult<DatasetPlan> {
        require_half_open_dataset_window(request.window_start, request.window_end)?;
        if request
            .sample_sources
            .iter()
            .any(|source| !matches!(source, TrainingSampleSource::HistoricalPit))
        {
            return Err(ResearchError::DatasetPlan {
                detail: "Source Slice V1 Dataset builds currently accept only historical_pit; live_attribution and exit_decision require their complete immutable evidence graphs"
                    .to_owned(),
            }
            .into());
        }
        let (model_spec_definition_hash, trade_policy_artifact_id, trade_policy_hash, trade_policy) =
            self.resolve_trade_policy_binding(&request).await?;
        let plan_markets = self.frozen_plan_markets(&request, &source.prefetched)?;
        let samples = plan_samples(&request, &plan_markets)?;
        let training_dataset_id = request
            .training_dataset_id
            .clone()
            .unwrap_or_else(TrainingDatasetId::from_v7);
        let label_names =
            label_names_for_sources(&request.sample_sources, trade_policy_artifact_id.is_some());
        Ok(DatasetPlan {
            request,
            training_dataset_id,
            model_spec_definition_hash,
            samples,
            lot_samples: Vec::new(),
            exit_training_lots: Vec::new(),
            label_names,
            trade_policy_artifact_id,
            trade_policy_hash,
            trade_policy,
        })
    }

    fn frozen_plan_markets(
        &self,
        request: &DatasetPlanRequest,
        prefetched: &Prefetched,
    ) -> QuantResult<Vec<PlanMarket>> {
        let mut versions = BTreeMap::<MarketId, Vec<_>>::new();
        for version in &prefetched.catalog.market_changes {
            versions
                .entry(version.market_id.clone())
                .or_default()
                .push(version);
        }
        let mut markets = Vec::new();
        for (market_id, mut history) in versions {
            history.sort_by(|left, right| {
                (
                    left.source_effective_at,
                    left.available_at,
                    &left.market_change_id,
                )
                    .cmp(&(
                        right.source_effective_at,
                        right.available_at,
                        &right.market_change_id,
                    ))
            });
            let Some(latest) = history.last() else {
                continue;
            };
            let info =
                serde_json::from_value::<MarketRegistryInfo>(latest.payload.clone().into_inner())
                    .map_err(|error| ResearchError::DatasetPlan {
                    detail: format!("Source Slice catalog market {market_id} is invalid: {error}"),
                })?;
            let observed = [&info.token_yes, &info.token_no].iter().any(|token| {
                prefetched.books.get(*token).is_some_and(|rows| {
                    rows.iter().any(|row| {
                        DateTime::from_timestamp_millis(row.event_time).is_some_and(|at| {
                            at < request.window_end
                                && at
                                    + ChronoDuration::from_std(self.max_book_staleness)
                                        .unwrap_or(ChronoDuration::MAX)
                                    >= request.window_start
                        })
                    })
                })
            });
            if !observed
                || (!self.enabled_categories.is_empty()
                    && !info
                        .categories
                        .iter()
                        .any(|category| self.enabled_categories.contains(&category)))
            {
                continue;
            }
            let created_at = info
                .created_at
                .unwrap_or_else(|| history[0].source_effective_at);
            if created_at >= request.window_end
                || info.end_date.is_some_and(|end| end <= request.window_start)
            {
                continue;
            }
            markets.push(PlanMarket {
                market_id,
                token_id: info.token_yes,
                created_at,
                end_date: info.end_date,
            });
        }
        markets.sort_by(|left, right| left.market_id.cmp(&right.market_id));
        Ok(markets)
    }

    /// Wire the service from boot-time dependencies and a frozen config snapshot.
    ///
    /// `max_spine_samples` is a deploy-level resource guard (not part of the
    /// reproducible `dataset_hash`): a `plan` beyond it flags `hard_cap_exceeded`
    /// and a `build` beyond it fails closed.
    pub fn new(
        deps: TrainingDatasetServiceDeps,
        config: TrainingDatasetBuildConfig,
        max_spine_samples: u64,
    ) -> QuantResult<Self> {
        let min_exit_depth_usd = config.training.min_exit_depth_usd_typed();
        let max_book_staleness = Duration::from_millis(config.training.max_book_staleness_ms);
        Ok(Self {
            fact_read: deps.fact_read,
            catalog_repo: deps.catalog_repo,
            market_repo: deps.market_repo,
            artifact_store: deps.artifact_store,
            dataset_repo: deps.dataset_repo,
            attribution_repo: deps.attribution_repo,
            recommendation_repo: deps.recommendation_repo,
            feature_repo: deps.feature_repo,
            selection_repo: deps.selection_repo,
            position_repo: deps.position_repo,
            clob_market_info_repo: deps.clob_market_info_repo,
            linkage_repo: deps.linkage_repo,
            model_registry: deps.model_registry,
            trade_policy_repo: deps.trade_policy_repo,
            calibration_repo: deps.calibration_repo,
            features: config.features,
            factors: config.factors,
            domain: config.domain,
            data_quality: config.data_quality,
            max_book_staleness,
            min_exit_depth_usd,
            enabled_categories: config
                .selection
                .enabled_categories
                .iter()
                .copied()
                .collect(),
            selection: config.selection,
            max_spine_samples,
            labelers: Arc::new(config.labelers),
            bias_table: config.bias_table,
        })
    }

    /// Resolve the target `ModelSpec`'s governed required raw inputs.
    ///
    /// `DatasetPlanRequest.model_spec_id` names the spec this dataset is
    /// built for, so the requirement set is genuinely known at plan/build
    /// time — not a hypothetical future model. Optional inputs are retained by
    /// the fitted transform and therefore do not gate selection.
    async fn resolve_model_requirements(
        &self,
        model_spec_id: &ModelSpecId,
    ) -> QuantResult<ModelFeatureRequirements> {
        let spec = self
            .model_registry
            .find_model_spec_by_id(model_spec_id)
            .await
            .map_err(QuantError::from)?
            .ok_or_else(|| StorageError::NotFound {
                entity: "quant_model_spec",
                id: model_spec_id.to_string(),
            })?;
        Ok(ModelFeatureRequirements::from_input_contract(
            &spec.input_contract,
        ))
    }

    async fn resolve_trade_policy_binding(
        &self,
        request: &DatasetPlanRequest,
    ) -> QuantResult<(
        ContentHash,
        Option<TradePolicyArtifactId>,
        Option<ContentHash>,
        Option<TradePolicyArtifactPayload>,
    )> {
        let spec = self
            .model_registry
            .find_model_spec_by_id(&request.model_spec_id)
            .await
            .map_err(QuantError::from)?
            .ok_or_else(|| StorageError::NotFound {
                entity: "quant_model_spec",
                id: request.model_spec_id.to_string(),
            })?;
        if request.purpose == DatasetPurpose::PolicyFit {
            return Ok((spec.definition_hash, None, None, None));
        }
        spec.training_contract
            .validate()
            .map_err(|detail| ResearchError::DatasetPlan { detail })?;
        let Some(artifact_id) = spec.training_contract.trade_policy_artifact_id else {
            return Ok((spec.definition_hash, None, None, None));
        };
        let artifact = self
            .trade_policy_repo
            .find(&artifact_id)
            .await
            .map_err(QuantError::from)?
            .ok_or_else(|| StorageError::NotFound {
                entity: "quant_trade_policy_artifact",
                id: artifact_id.to_string(),
            })?;
        if artifact.status != TradePolicyStatus::Published
            || !artifact.payload_json.is_publishable()
        {
            return Err(ResearchError::DatasetPlan {
                detail: format!(
                    "trade policy {} must be Published with passing validation",
                    artifact.artifact_id
                ),
            }
            .into());
        }
        let computed_hash = ResearchHasher::canonical(&artifact.payload_json)?;
        if computed_hash != artifact.content_hash
            || TradePolicyArtifactId::from_content_hash(&computed_hash) != artifact.artifact_id
        {
            return Err(ResearchError::DatasetPlan {
                detail: format!(
                    "trade policy {} content hash or content-addressed id does not verify",
                    artifact.artifact_id
                ),
            }
            .into());
        }
        let embargo = i64::try_from(artifact.payload_json.embargo_secs).map_err(|error| {
            ResearchError::DatasetPlan {
                detail: format!("trade-policy embargo does not fit chrono seconds: {error}"),
            }
        })?;
        let training_not_before = artifact
            .payload_json
            .fit_contract
            .pit_cutoff
            .checked_add_signed(ChronoDuration::seconds(embargo))
            .ok_or_else(|| ResearchError::DatasetPlan {
                detail: "trade-policy PIT cutoff plus embargo overflows chrono".to_owned(),
            })?;
        if request.window_start < training_not_before {
            return Err(ResearchError::DatasetPlan {
                detail: format!(
                    "dataset window starts at {} before policy-fit embargo ends at {training_not_before}",
                    request.window_start
                ),
            }
            .into());
        }
        Ok((
            spec.definition_hash,
            Some(artifact.artifact_id),
            Some(artifact.content_hash),
            Some(artifact.payload_json),
        ))
    }

    /// The historical candidate set for a window, sourced point-in-time
    /// honestly from `ClickHouse` facts (not the currently-active catalog).
    ///
    /// A market is a candidate iff it had at least one observable book snapshot
    /// during `[window_start - max_book_staleness, window_end]` — the same
    /// staleness slack the PIT `book_at` lookup honors. This candidate set
    /// therefore **includes since-`Settled`/`Delisted` markets** (which carry mature
    /// settlement labels), eliminating the survivorship bias of a
    /// `status = 'active'` catalog scan. Metadata (category, tokens, lifetime) is
    /// hydrated from Postgres via a status-agnostic `find_by_ids`.
    async fn historical_candidate_markets(
        &self,
        window_start: DateTime<Utc>,
        window_end: DateTime<Utc>,
    ) -> QuantResult<Vec<MarketInfo>> {
        let staleness_ms = i64::try_from(self.max_book_staleness.as_millis()).map_err(|error| {
            ResearchError::DatasetPlan {
                detail: format!("max book staleness does not fit chrono milliseconds: {error}"),
            }
        })?;
        let from = window_start
            .checked_sub_signed(ChronoDuration::milliseconds(staleness_ms))
            .ok_or_else(|| ResearchError::DatasetPlan {
                detail: format!(
                    "max book staleness {staleness_ms}ms overflows before dataset window {window_start}"
                ),
            })?;
        let from_ms = from.timestamp_millis();
        let to_ms = window_end.timestamp_millis();
        let ids = self
            .fact_read
            .observed_markets_between(from_ms, to_ms, to_ms)
            .await
            .map_err(QuantError::from)?;
        let markets = self
            .market_repo
            .find_by_ids(&ids)
            .await
            .map_err(QuantError::from)?;
        Ok(markets.into_iter().map(|info| (*info).clone()).collect())
    }

    /// Whether a market belongs to this build's point-in-time selection.
    ///
    /// Survivorship-aware: a market qualifies when its lifetime **overlaps** the
    /// window (`created_at < window_end` and it had not resolved before
    /// `window_start`), so markets that were tradable during the window but have
    /// since resolved are included — not just the currently-active catalog. The
    /// enabled-category gate mirrors the online [`CategoryFilter`] by accepting
    /// any governed category membership, not a lossy single-category projection.
    fn in_selection(
        &self,
        info: &MarketInfo,
        window_start: DateTime<Utc>,
        window_end: DateTime<Utc>,
    ) -> bool {
        if info.created_at >= window_end {
            return false;
        }
        if info.end_date.is_some_and(|end| end <= window_start) {
            return false;
        }
        self.enabled_categories.is_empty()
            || info
                .categories
                .iter()
                .any(|category| self.enabled_categories.contains(category))
    }

    /// The deterministic candidate selection for a plan window.
    fn candidate_plan_markets(
        &self,
        markets: &[MarketInfo],
        window_start: DateTime<Utc>,
        window_end: DateTime<Utc>,
    ) -> Vec<PlanMarket> {
        markets
            .iter()
            .filter(|info| self.in_selection(info, window_start, window_end))
            .map(|info| PlanMarket {
                market_id: info.market_id.clone(),
                token_id: info.yes_token_id.clone(),
                created_at: info.created_at,
                end_date: info.end_date,
            })
            .collect()
    }

    /// Cheap, non-materializing dry-run counts for the `plan` endpoint.
    ///
    /// Computes the historical spine size arithmetically (no grid allocation) and
    /// adds bounded live-attribution + exit-decision counts, so a plan over the
    /// full catalog returns in milliseconds instead of allocating millions of rows.
    pub async fn count_plan(
        &self,
        request: &DatasetPlanRequest,
        sample_slices: u32,
        sample_markets: u32,
    ) -> QuantResult<PlanCounts> {
        require_half_open_dataset_window(request.window_start, request.window_end)?;
        let mut total: u64 = 0;
        let wants_historical = wants_sample_source(request, TrainingSampleSource::HistoricalPit);
        // Candidate `MarketInfo` set (category + lifetime), reused for both the
        // arithmetic spine upper bound and the sampled keep-rate estimate. Sourced
        // from ClickHouse-observed markets so since-resolved markets are included.
        let candidate_markets = if wants_historical {
            self.historical_candidate_markets(request.window_start, request.window_end)
                .await?
                .into_iter()
                .filter(|info| self.in_selection(info, request.window_start, request.window_end))
                .collect()
        } else {
            Vec::new()
        };
        let candidate_infos = candidate_markets.iter().collect::<Vec<_>>();
        let spine_upper_bound =
            self.spine_upper_bound(request, wants_historical, &candidate_markets)?;
        total = total
            .checked_add(spine_upper_bound)
            .ok_or_else(|| ResearchError::DatasetPlan {
                detail: "historical spine count overflowed u64".to_owned(),
            })?;
        total = total
            .checked_add(self.additional_plan_sample_count(request).await?)
            .ok_or_else(|| ResearchError::DatasetPlan {
                detail: "total plan sample count overflowed u64".to_owned(),
            })?;

        // Bounded point-in-time keep-rate estimate: sample K `as_of` slices × M
        // candidate markets, replay the selection funnel, and scale the spine.
        let keep_rate_estimate = if wants_historical
            && spine_upper_bound > 0
            && sample_slices > 0
            && sample_markets > 0
            && !candidate_infos.is_empty()
        {
            self.estimate_keep_rate(request, &candidate_infos, sample_slices, sample_markets)
                .await?
        } else {
            None
        };
        let (keep_rate, keep_rate_sample_size, estimated_eligible_samples) =
            summarize_keep_rate(total, spine_upper_bound, keep_rate_estimate)?;

        Ok(PlanCounts {
            spine_upper_bound,
            total,
            hard_cap_exceeded: total > self.max_spine_samples,
            estimated_eligible_samples,
            keep_rate,
            keep_rate_sample_size,
        })
    }

    fn spine_upper_bound(
        &self,
        request: &DatasetPlanRequest,
        wants_historical: bool,
        candidates: &[MarketInfo],
    ) -> QuantResult<u64> {
        if !wants_historical {
            return Ok(0);
        }
        let plan_markets = candidates
            .iter()
            .map(|info| PlanMarket {
                market_id: info.market_id.clone(),
                token_id: info.yes_token_id.clone(),
                created_at: info.created_at,
                end_date: info.end_date,
            })
            .collect::<Vec<_>>();
        count_samples(request, &plan_markets, self.max_spine_samples)
    }

    async fn additional_plan_sample_count(&self, request: &DatasetPlanRequest) -> QuantResult<u64> {
        let mut total = 0_u64;
        if wants_sample_source(request, TrainingSampleSource::LiveAttribution) {
            let attributions = self
                .attribution_repo
                .find_label_available_between(
                    request.window_start,
                    request.window_end,
                    LIVE_ATTRIBUTION_SAMPLE_LIMIT,
                )
                .await
                .map_err(QuantError::from)?;
            total = checked_sample_count_add(total, attributions.len(), "attribution")?;
        }
        if wants_sample_source(request, TrainingSampleSource::ExitDecision) {
            let lots = self
                .position_repo
                .find_exit_training_lots(
                    request.window_start,
                    request.window_end,
                    LIVE_ATTRIBUTION_SAMPLE_LIMIT,
                )
                .await
                .map_err(QuantError::from)?;
            let lot_samples = plan_lot_timeline_samples(
                request.sample_interval_secs,
                request.window_start,
                &lots,
            )?;
            total = checked_sample_count_add(total, lot_samples.len(), "exit-decision")?;
        }
        Ok(total)
    }

    /// Estimate the point-in-time selection keep-rate by replaying the funnel
    /// over a bounded `slices × markets` sample.
    ///
    /// Returns `(keep_rate, trials)`: the fraction of `(market, slice)` pairs that
    /// pass `FilterChain::standard`, and the number of trials. The market sample
    /// is stride-selected across the id-sorted candidate set for representativeness;
    /// slices are the midpoints of `slices` equal sub-intervals of the window.
    async fn estimate_keep_rate(
        &self,
        request: &DatasetPlanRequest,
        candidates: &[&MarketInfo],
        slices: u32,
        markets: u32,
    ) -> QuantResult<Option<KeepRateEstimate>> {
        let market_limit =
            usize::try_from(markets).map_err(|error| ResearchError::DatasetPlan {
                detail: format!("keep-rate market limit does not fit usize: {error}"),
            })?;
        let sampled = stride_sample(candidates, market_limit);
        if sampled.is_empty() {
            return Ok(None);
        }
        let pit = DurablePitSource::new(
            Arc::clone(&self.fact_read),
            Arc::clone(&self.catalog_repo),
            Arc::clone(&self.clob_market_info_repo),
        );
        let model_requirements = self
            .resolve_model_requirements(&request.model_spec_id)
            .await?;
        let selector = self.offline_pit_selector(request, model_requirements);
        // Bounded live linkage/domain-fact reads (one per slice, batched over
        // `sampled`), the same projector the live pipeline uses — never a
        // hardcoded conservative placeholder — so the dry-run estimate isn't
        // biased against domain-gated models.
        let domain_source = LiveDomainAvailabilitySource::new(
            Arc::clone(&self.linkage_repo),
            Arc::clone(&self.fact_read),
            self.domain.clone(),
        );
        let grid = KeepRateGrid::new(
            request,
            slices,
            self.domain.crypto.availability_lag_secs,
            self.domain.weather.availability_lag_secs,
        )?;
        let market_ids = sampled
            .iter()
            .map(|market| market.market_id.clone())
            .collect::<Vec<_>>();
        let trials_per_slice = checked_len_u64(sampled.len(), "keep-rate trial")?;
        let mut included: u64 = 0;
        let mut trials: u64 = 0;
        for index in 0..slices {
            let boundary = grid.boundary(index)?;
            let result = selector
                .select_at(&boundary, &market_ids, &pit, &domain_source)
                .await?;
            included = checked_u64_add(
                included,
                checked_len_u64(result.included.len(), "included keep-rate")?,
                "keep-rate included count",
            )?;
            trials = checked_u64_add(trials, trials_per_slice, "keep-rate trial count")?;
        }
        if trials == 0 {
            return Ok(None);
        }
        Ok(Some(KeepRateEstimate { included, trials }))
    }
}

fn checked_len_u64(len: usize, label: &str) -> QuantResult<u64> {
    u64::try_from(len).map_err(|error| {
        ResearchError::DatasetPlan {
            detail: format!("{label} count does not fit u64: {error}"),
        }
        .into()
    })
}

fn checked_u64_add(left: u64, right: u64, label: &str) -> QuantResult<u64> {
    left.checked_add(right).ok_or_else(|| {
        ResearchError::DatasetPlan {
            detail: format!("{label} overflowed u64"),
        }
        .into()
    })
}

fn checked_sample_count_add(total: u64, len: usize, label: &str) -> QuantResult<u64> {
    checked_u64_add(
        total,
        checked_len_u64(len, &format!("{label} sample"))?,
        "additional plan sample count",
    )
}

fn summarize_keep_rate(
    total: u64,
    spine_upper_bound: u64,
    estimate: Option<KeepRateEstimate>,
) -> QuantResult<(Option<f64>, u64, u64)> {
    let Some(estimate) = estimate else {
        return Ok((None, 0, total));
    };
    let scaled = estimate.scale(spine_upper_bound)?;
    let non_historical =
        total
            .checked_sub(spine_upper_bound)
            .ok_or_else(|| ResearchError::DatasetPlan {
                detail: "total sample count is below historical spine count".to_owned(),
            })?;
    let estimated = checked_u64_add(scaled, non_historical, "estimated eligible sample count")?;
    Ok((Some(estimate.rate()?), estimate.trials, estimated))
}

/// Deterministically stride-sample up to `limit` markets across the id-sorted set.
fn stride_sample<'a>(candidates: &[&'a MarketInfo], limit: usize) -> Vec<&'a MarketInfo> {
    if candidates.is_empty() || limit == 0 {
        return Vec::new();
    }
    let mut ordered: Vec<&MarketInfo> = candidates.to_vec();
    ordered.sort_by(|a, b| a.market_id.as_str().cmp(b.market_id.as_str()));
    if ordered.len() <= limit {
        return ordered;
    }
    let step = ordered.len() / limit;
    ordered
        .into_iter()
        .step_by(step.max(1))
        .take(limit)
        .collect()
}

#[async_trait]
impl TrainingDatasetPlanner for TrainingDatasetService {
    async fn plan(&self, request: DatasetPlanRequest) -> QuantResult<DatasetPlan> {
        require_half_open_dataset_window(request.window_start, request.window_end)?;
        let (model_spec_definition_hash, trade_policy_artifact_id, trade_policy_hash, trade_policy) =
            self.resolve_trade_policy_binding(&request).await?;
        // Point-in-time candidate selection: markets observed (had a book) in the
        // window whose fee-dominant category is in the enabled set (mirrors the
        // online [`CategoryFilter`]; per-`as_of` liquidity/data-quality eligibility
        // is enforced during materialization). Sourced from ClickHouse facts so
        // since-resolved markets are not survivorship-filtered out.
        let markets = self
            .historical_candidate_markets(request.window_start, request.window_end)
            .await?;
        let plan_markets =
            self.candidate_plan_markets(&markets, request.window_start, request.window_end);
        let samples = plan_samples(&request, &plan_markets)?;
        let mut lot_samples = Vec::new();
        let mut exit_training_lots = Vec::new();
        if wants_sample_source(&request, TrainingSampleSource::ExitDecision) {
            exit_training_lots = self
                .position_repo
                .find_exit_training_lots(
                    request.window_start,
                    request.window_end,
                    LIVE_ATTRIBUTION_SAMPLE_LIMIT,
                )
                .await
                .map_err(QuantError::from)?;
            lot_samples = plan_lot_timeline_samples(
                request.sample_interval_secs,
                request.window_start,
                &exit_training_lots,
            )?;
        }
        let training_dataset_id = request
            .training_dataset_id
            .clone()
            .unwrap_or_else(TrainingDatasetId::from_v7);
        let label_names =
            label_names_for_sources(&request.sample_sources, trade_policy_artifact_id.is_some());
        Ok(DatasetPlan {
            request,
            training_dataset_id,
            model_spec_definition_hash,
            samples,
            lot_samples,
            exit_training_lots,
            label_names,
            trade_policy_artifact_id,
            trade_policy_hash,
            trade_policy,
        })
    }
}

impl TrainingDatasetService {
    /// Dry-run sample count aligned with [`TrainingDatasetBuilder::build`] coverage.
    pub async fn count_planned_samples(&self, plan: &DatasetPlan) -> QuantResult<u64> {
        let mut total = planned_historical_samples(plan);
        if wants_sample_source(&plan.request, TrainingSampleSource::LiveAttribution) {
            let attributions = self
                .attribution_repo
                .find_label_available_between(
                    plan.request.window_start,
                    plan.request.window_end,
                    LIVE_ATTRIBUTION_SAMPLE_LIMIT,
                )
                .await
                .map_err(QuantError::from)?;
            total += attributions.len() as u64;
        }
        if wants_sample_source(&plan.request, TrainingSampleSource::ExitDecision) {
            total += plan.lot_samples.len() as u64;
        }
        Ok(total)
    }
}

#[async_trait]
impl TrainingDatasetBuilder for TrainingDatasetService {
    async fn build(&self, plan: DatasetPlan) -> QuantResult<TrainingDatasetArtifact> {
        Box::pin(self.build_inner(plan, Arc::new(NoopProgressSink), CancellationToken::new())).await
    }
}

impl TrainingDatasetService {
    /// Build a dataset, streaming per-cross-section progress to `sink` and
    /// polling `cancel` at each cross-section boundary.
    pub async fn build_with_progress(
        &self,
        plan: DatasetPlan,
        sink: Arc<dyn JobProgressSink>,
        cancel: CancellationToken,
    ) -> QuantResult<TrainingDatasetArtifact> {
        Box::pin(self.build_inner(plan, sink, cancel)).await
    }

    /// Build from verified object bytes with no dynamic historical reads.
    pub async fn build_with_frozen_source(
        &self,
        plan: DatasetPlan,
        source: FrozenSourceSlice,
        sink: Arc<dyn JobProgressSink>,
        cancel: CancellationToken,
    ) -> QuantResult<TrainingDatasetArtifact> {
        self.prepare_build_ledger(&plan).await?;
        let training_dataset_id = plan.training_dataset_id.clone();
        let result = historical_window_from_prefetched(source.prefetched, self.max_book_staleness)
            .and_then(|window| {
                if source.invalid_sessions.is_empty() {
                    Ok(window)
                } else {
                    Err(ResearchError::DatasetBuild {
                        detail: format!(
                            "Source Slice contains {} invalid L2 sessions",
                            source.invalid_sessions.len()
                        ),
                    }
                    .into())
                }
            });
        let result = match result {
            Ok(window) => {
                self.materialize_window(plan, sink, cancel, window, source.clob_market_info)
                    .await
            }
            Err(error) => Err(error),
        };
        self.persist_build_failure(&training_dataset_id, result)
            .await
    }

    async fn build_inner(
        &self,
        plan: DatasetPlan,
        sink: Arc<dyn JobProgressSink>,
        cancel: CancellationToken,
    ) -> QuantResult<TrainingDatasetArtifact> {
        self.prepare_build_ledger(&plan).await?;
        let training_dataset_id = plan.training_dataset_id.clone();
        let result = Box::pin(self.materialize_inner(plan, sink, cancel)).await;
        self.persist_build_failure(&training_dataset_id, result)
            .await
    }

    async fn materialize_inner(
        &self,
        plan: DatasetPlan,
        sink: Arc<dyn JobProgressSink>,
        cancel: CancellationToken,
    ) -> QuantResult<TrainingDatasetArtifact> {
        self.ensure_factors_enabled()?;
        // Fail closed on an oversized spine: a build this large would exhaust
        // memory/time; the operator must narrow the window / interval / selection
        // (the dry-run plan flags this as `hard_cap_exceeded` first).
        let max_spine_samples = self.max_spine_samples;
        if plan.samples.len() as u64 > max_spine_samples {
            return Err(ResearchError::DatasetPlan {
                detail: format!(
                    "planned spine {} exceeds hard cap {max_spine_samples}: narrow the window, \
                     widen the sample interval, or scope the selection",
                    plan.samples.len()
                ),
            }
            .into());
        }
        sink.report(ResearchJobProgress::indeterminate("prefetch", 0));
        let context = ReplayContext::new(&plan, &self.features)?;
        let loader = self.window_loader();
        // Prefetch is real ClickHouse I/O — stays on the async runtime.
        let window = loader
            .load(&context.window_spec(&plan, &self.domain))
            .await?;
        let clob_market_info = self.load_clob_market_info(&plan).await?;
        self.materialize_window(plan, sink, cancel, window, clob_market_info)
            .await
    }

    async fn materialize_window(
        &self,
        plan: DatasetPlan,
        sink: Arc<dyn JobProgressSink>,
        cancel: CancellationToken,
        window: HistoricalWindow,
        clob_market_info: Vec<ClobMarketInfoVersion>,
    ) -> QuantResult<TrainingDatasetArtifact> {
        let context = ReplayContext::new(&plan, &self.features)?;
        let coverage = DatasetCoverage {
            planned_samples: planned_historical_samples(&plan),
            book_decode_failures: window.book_decode_failures,
            ..DatasetCoverage::default()
        };
        let pit: Arc<dyn PointInTimeSnapshotSource> = Arc::new(window.pit);
        let prefetched = Arc::new(window.prefetched);

        // Offload the unbounded historical PIT loop to a blocking thread so it
        // never occupies an async runtime worker (CPU-bound in-memory scoring
        // that would otherwise starve other jobs' heartbeats / lease renewals),
        // polling `cancel` at each cross-section boundary for a ~one-section
        // cooperative cancel latency.
        let mut spine = HistoricalSpine::default();
        let mut coverage = coverage;
        if wants_sample_source(&plan.request, TrainingSampleSource::HistoricalPit) {
            let inputs = self
                .historical_inputs(
                    &plan,
                    Arc::clone(&pit),
                    Arc::clone(&prefetched),
                    Arc::clone(&sink),
                    cancel.clone(),
                    coverage,
                )
                .await?;
            let output = task::spawn_blocking(move || run_historical_spine_blocking(inputs))
                .await
                .map_err(|error| {
                    QuantError::from(ResearchError::DatasetBuild {
                        detail: format!("historical spine task join failed: {error}"),
                    })
                })??;
            if output.cancelled {
                return Err(ResearchError::Cancelled {
                    detail: "dataset build cancelled during the historical spine".to_owned(),
                }
                .into());
            }
            coverage = output.coverage;
            spine.examples = output.examples;
            spine.market_set = output.market_set;
        }

        self.assemble_and_finalize(
            plan,
            BuildTail {
                pit: &*pit,
                prefetched: &prefetched,
                clob_market_info: &clob_market_info,
                context: &context,
                sink: &*sink,
            },
            coverage,
            spine,
        )
        .await
    }

    async fn load_clob_market_info(
        &self,
        plan: &DatasetPlan,
    ) -> QuantResult<Vec<ClobMarketInfoVersion>> {
        let mut market_ids = plan
            .samples
            .iter()
            .map(|sample| sample.market_id.clone())
            .chain(
                plan.lot_samples
                    .iter()
                    .map(|sample| sample.market_id.clone()),
            )
            .collect::<HashSet<_>>()
            .into_iter()
            .collect::<Vec<_>>();
        market_ids.sort();
        self.clob_market_info_repo
            .window(
                &market_ids,
                plan.request.window_start,
                plan.request.window_end,
                plan.request.pit_cutoff,
            )
            .await
            .map_err(Into::into)
    }

    async fn prepare_build_ledger(&self, plan: &DatasetPlan) -> QuantResult<()> {
        let knowledge_lag_secs =
            i64::try_from(plan.request.knowledge_lag_secs).map_err(|error| {
                ResearchError::DatasetPlan {
                    detail: format!("dataset knowledge lag exceeds Postgres bigint: {error}"),
                }
            })?;
        let sample_interval_secs =
            i64::try_from(plan.request.sample_interval_secs).map_err(|error| {
                ResearchError::DatasetPlan {
                    detail: format!("dataset sample interval exceeds Postgres bigint: {error}"),
                }
            })?;
        let existing = self
            .dataset_repo
            .find_by_id(&plan.training_dataset_id)
            .await?;
        let row = if let Some(existing) = existing {
            existing
        } else {
            let create_result = self
                .dataset_repo
                .create_plan(NewTrainingDatasetPlan {
                    training_dataset_id: plan.training_dataset_id.clone(),
                    model_spec_id: plan.request.model_spec_id.clone(),
                    model_spec_definition_hash: plan.model_spec_definition_hash.clone(),
                    window_start: plan.request.window_start,
                    window_end: plan.request.window_end,
                    purpose: plan.request.purpose,
                    knowledge_lag_secs,
                    sample_interval_secs,
                    horizons_secs: TrainingHorizonsSecs(plan.request.horizons_secs.clone()),
                    feature_schema_version: Some(plan.request.feature_schema_version),
                    sample_sources: Some(TrainingSampleSources(
                        plan.request.sample_sources.clone(),
                    )),
                    decision_policy_snapshot_id: plan.request.decision_policy_snapshot_id.clone(),
                })
                .await;
            match create_result {
                Ok(created) => created,
                Err(StorageError::Duplicate { .. }) => self
                    .dataset_repo
                    .find_by_id(&plan.training_dataset_id)
                    .await?
                    .ok_or_else(|| {
                        StorageError::state_conflict(
                            "quant_training_dataset",
                            Some(&plan.training_dataset_id),
                            "concurrent plan insert reported duplicate but the row is not visible",
                        )
                    })?,
                Err(error) => return Err(error.into()),
            }
        };
        let binding_matches = row.model_spec_id == plan.request.model_spec_id
            && row.model_spec_definition_hash == plan.model_spec_definition_hash
            && row.decision_policy_snapshot_id == plan.request.decision_policy_snapshot_id
            && row.window_start == plan.request.window_start
            && row.window_end == plan.request.window_end
            && row.purpose == plan.request.purpose
            && row.knowledge_lag_secs == knowledge_lag_secs
            && row.sample_interval_secs == sample_interval_secs
            && row.horizons_secs.0 == plan.request.horizons_secs
            && row.feature_schema_version == Some(plan.request.feature_schema_version)
            && row.sample_sources.as_ref().map(|sources| &sources.0)
                == Some(&plan.request.sample_sources);
        if !binding_matches {
            return Err(StorageError::state_conflict(
                "quant_training_dataset",
                Some(&plan.training_dataset_id),
                "pre-assigned dataset id is already bound to a different immutable plan",
            )
            .into());
        }
        self.dataset_repo
            .start_build(&plan.training_dataset_id)
            .await?;
        Ok(())
    }

    async fn persist_build_failure<T>(
        &self,
        training_dataset_id: &TrainingDatasetId,
        result: QuantResult<T>,
    ) -> QuantResult<T> {
        match result {
            Ok(value) => Ok(value),
            Err(build_error) => {
                let detail = bounded_failure_detail(&build_error.to_string());
                if let Err(persistence_error) = self
                    .dataset_repo
                    .fail_build(training_dataset_id, detail)
                    .await
                {
                    return Err(ResearchError::DatasetBuild {
                        detail: format!(
                            "dataset build failed ({build_error}); persisting terminal failure also failed ({persistence_error})"
                        ),
                    }
                    .into());
                }
                Err(build_error)
            }
        }
    }

    /// Build a dataset using a caller-supplied PIT engine (integration tests only).
    ///
    /// Prefetch still runs against the configured fact reader; only point-in-time
    /// book/market resolution is overridden. Runs the historical spine inline on
    /// the async runtime (the borrowed PIT source is not `'static`, so it cannot
    /// be moved into `spawn_blocking`) — acceptable for the leakage-probe tests.
    #[doc(hidden)]
    pub async fn build_with_pit_source(
        &self,
        plan: DatasetPlan,
        pit: &dyn PointInTimeSnapshotSource,
    ) -> QuantResult<TrainingDatasetArtifact> {
        self.prepare_build_ledger(&plan).await?;
        let training_dataset_id = plan.training_dataset_id.clone();
        let result = self.materialize_with_pit_source(plan, pit).await;
        self.persist_build_failure(&training_dataset_id, result)
            .await
    }

    async fn materialize_with_pit_source(
        &self,
        plan: DatasetPlan,
        pit: &dyn PointInTimeSnapshotSource,
    ) -> QuantResult<TrainingDatasetArtifact> {
        self.ensure_factors_enabled()?;
        let context = ReplayContext::new(&plan, &self.features)?;
        let loader = self.window_loader();
        let prefetched = loader
            .prefetch(&context.window_spec(&plan, &self.domain))
            .await?;
        let clob_market_info = self.load_clob_market_info(&plan).await?;
        let mut coverage = DatasetCoverage {
            planned_samples: planned_historical_samples(&plan),
            ..DatasetCoverage::default()
        };
        let cancel = CancellationToken::new();
        let mut spine = HistoricalSpine::default();
        if wants_sample_source(&plan.request, TrainingSampleSource::HistoricalPit) {
            let params = self
                .historical_params(&plan, pit, &prefetched, &NoopProgressSink, &cancel)
                .await?;
            let output = run_historical_spine(params, coverage).await?;
            coverage = output.coverage;
            spine.examples = output.examples;
            spine.market_set = output.market_set;
        }
        self.assemble_and_finalize(
            plan,
            BuildTail {
                pit,
                prefetched: &prefetched,
                clob_market_info: &clob_market_info,
                context: &context,
                sink: &NoopProgressSink,
            },
            coverage,
            spine,
        )
        .await
    }

    /// Owned inputs for the blocking historical spine (moved into `spawn_blocking`).
    async fn historical_inputs(
        &self,
        plan: &DatasetPlan,
        pit: Arc<dyn PointInTimeSnapshotSource>,
        prefetched: Arc<Prefetched>,
        sink: Arc<dyn JobProgressSink>,
        cancel: CancellationToken,
        coverage: DatasetCoverage,
    ) -> QuantResult<HistoricalSpineInputs> {
        let model_requirements = self
            .resolve_model_requirements(&plan.request.model_spec_id)
            .await?;
        Ok(HistoricalSpineInputs {
            pit,
            prefetched,
            sink,
            cancel,
            samples: plan.samples.clone(),
            request: plan.request.clone(),
            features: self.features.clone(),
            factors: self.factors.clone(),
            data_quality: self.data_quality.clone(),
            domain: self.domain.clone(),
            selection: self.selection.clone(),
            model_requirements,
            labelers: Arc::clone(&self.labelers),
            min_exit_depth_usd: self.min_exit_depth_usd,
            bias_table: self.bias_table.as_ref().map(Arc::clone),
            context: ReplayContext::new(plan, &self.features)?,
            coverage,
        })
    }

    /// Borrowed params for the historical spine (async/inline callers).
    async fn historical_params<'a>(
        &'a self,
        plan: &'a DatasetPlan,
        pit: &'a dyn PointInTimeSnapshotSource,
        prefetched: &'a Prefetched,
        sink: &'a dyn JobProgressSink,
        cancel: &'a CancellationToken,
    ) -> QuantResult<HistoricalSpineParams<'a>> {
        let model_requirements = self
            .resolve_model_requirements(&plan.request.model_spec_id)
            .await?;
        Ok(HistoricalSpineParams {
            pit,
            prefetched,
            sink,
            cancel,
            samples: &plan.samples,
            request: &plan.request,
            features: &self.features,
            factors: &self.factors,
            data_quality: &self.data_quality,
            domain: &self.domain,
            selection: &self.selection,
            model_requirements,
            labelers: &self.labelers,
            min_exit_depth_usd: self.min_exit_depth_usd,
            bias_table: &self.bias_table,
            context: ReplayContext::new(plan, &self.features)?,
        })
    }

    /// Append the live-attribution + exit-decision sources (bounded DB reads,
    /// kept on the async runtime), then assert leakage-freedom, materialize the
    /// Parquet artifact, and persist the ledger row.
    async fn assemble_and_finalize(
        &self,
        plan: DatasetPlan,
        tail: BuildTail<'_>,
        mut coverage: DatasetCoverage,
        mut spine: HistoricalSpine,
    ) -> QuantResult<TrainingDatasetArtifact> {
        let BuildTail {
            pit,
            prefetched,
            clob_market_info,
            context,
            sink,
        } = tail;
        let builder = ConfiguredFeatureBuilder::new(&self.features, &self.domain)?;
        let engine = FactorEngine::new(
            &self.factors,
            &self.features,
            &self.domain,
            self.bias_table.as_ref().map(Arc::clone),
        );

        if wants_sample_source(&plan.request, TrainingSampleSource::LiveAttribution) {
            self.append_live_attribution_examples(
                &plan,
                &mut coverage,
                &mut spine.examples,
                &mut spine.market_set,
            )
            .await?;
        }

        if wants_sample_source(&plan.request, TrainingSampleSource::ExitDecision) {
            coverage.exit_decision_candidates = plan.lot_samples.len() as u64;
            coverage.planned_samples += plan.lot_samples.len() as u64;
            self.append_exit_decision_examples(
                ExitDecisionAppendInput {
                    plan: &plan,
                    pit,
                    prefetched,
                    clob_market_info,
                    context,
                },
                &mut ExampleBuildSink {
                    coverage: &mut coverage,
                    examples: &mut spine.examples,
                    market_set: &mut spine.market_set,
                },
            )
            .await?;
        }

        coverage.built_examples = spine.examples.len() as u64;
        coverage.markets = spine.market_set.len() as u64;

        sink.report(ResearchJobProgress::indeterminate(
            "finalize",
            spine.examples.len() as u64,
        ));
        self.finalize(&builder, &engine, plan, spine.examples, coverage)
            .await
    }

    /// Reject an empty factor set (no enabled families).
    fn ensure_factors_enabled(&self) -> QuantResult<()> {
        if FactorEngine::new(&self.factors, &self.features, &self.domain, None)
            .registry()
            .is_empty()
        {
            return Err(QuantError::config(
                "no factors enabled: factors.enabled_factor_families selects an empty factor set",
            ));
        }
        Ok(())
    }

    /// Assemble the historical-window loader from the frozen staleness bound.
    fn window_loader(&self) -> HistoricalWindowLoader {
        HistoricalWindowLoader::new(
            Arc::clone(&self.fact_read),
            Arc::clone(&self.catalog_repo),
            Arc::clone(&self.linkage_repo),
            Arc::clone(&self.calibration_repo),
            self.max_book_staleness,
        )
    }

    /// The offline point-in-time selection funnel for a build/plan, wired from
    /// the frozen selection/data-quality/feature config, PIT catalog liquidity,
    /// and the target `ModelSpec`'s resolved feature requirements.
    fn offline_pit_selector(
        &self,
        request: &DatasetPlanRequest,
        model_requirements: ModelFeatureRequirements,
    ) -> OfflinePitSelector {
        OfflinePitSelector::new(
            &self.selection,
            &self.data_quality,
            &self.features,
            request.decision_policy_snapshot_id.clone(),
            request.knowledge_lag_secs,
            model_requirements,
        )
    }

    /// The frozen replay config (feature/factor/domain/data-quality) for this build.
    fn replay_config(&self) -> ReplayConfig {
        ReplayConfig {
            features: self.features.clone(),
            factors: self.factors.clone(),
            domain: self.domain.clone(),
            data_quality: self.data_quality.clone(),
            bias_table: self.bias_table.as_ref().map(Arc::clone),
        }
    }

    async fn append_live_attribution_examples(
        &self,
        plan: &DatasetPlan,
        coverage: &mut DatasetCoverage,
        examples: &mut Vec<TrainingExample>,
        market_set: &mut HashSet<MarketId>,
    ) -> QuantResult<()> {
        let attributions = self
            .attribution_repo
            .find_label_available_between(
                plan.request.window_start,
                plan.request.window_end,
                LIVE_ATTRIBUTION_SAMPLE_LIMIT,
            )
            .await?;
        coverage.live_attribution_candidates += attributions.len() as u64;

        if attributions.is_empty() {
            return Ok(());
        }

        let recommendation_ids: Vec<_> = attributions
            .iter()
            .map(|a| a.recommendation_id.clone())
            .collect();
        let recommendations = self
            .recommendation_repo
            .find_by_ids(&recommendation_ids)
            .await?;
        let recommendation_by_id: HashMap<_, _> = recommendations
            .into_iter()
            .map(|r| (r.recommendation_id.clone(), r))
            .collect();

        let feature_vector_ids: Vec<_> = recommendation_by_id
            .values()
            .map(|r| r.evidence_refs.feature_vector_id.clone())
            .collect();
        let feature_infos = self.feature_repo.find_by_ids(&feature_vector_ids).await?;
        let feature_by_id: HashMap<_, _> = feature_infos
            .into_iter()
            .map(|f| (f.feature_vector_id.clone(), f))
            .collect();

        let selection_ids = recommendation_by_id
            .values()
            .map(|recommendation| recommendation.evidence_refs.market_selection_id.clone())
            .collect::<HashSet<_>>()
            .into_iter()
            .collect::<Vec<_>>();
        let selection_members = self
            .selection_repo
            .list_members_by_snapshot_ids(&selection_ids)
            .await?;
        let selection_by_key = selection_members
            .into_iter()
            .map(|member| {
                (
                    (member.market_selection_id.clone(), member.market_id.clone()),
                    member,
                )
            })
            .collect::<HashMap<_, _>>();

        for attribution in attributions {
            record_live_attribution_materialization(
                materialize_live_attribution_example(
                    &attribution,
                    &recommendation_by_id,
                    &feature_by_id,
                    &selection_by_key,
                ),
                coverage,
                examples,
                market_set,
            );
        }
        let accounted = coverage
            .live_attribution_materialized
            .checked_add(coverage.live_attribution_dropped_missing_evidence)
            .and_then(|count| {
                count.checked_add(coverage.live_attribution_censored_superseded_unfilled)
            })
            .ok_or_else(|| ResearchError::DatasetBuild {
                detail: "live attribution coverage accounting overflowed".to_owned(),
            })?;
        if accounted != coverage.live_attribution_candidates {
            return Err(ResearchError::DatasetBuild {
                detail: format!(
                    "live attribution coverage is unbalanced: candidates={}, materialized={}, missing_evidence={}, superseded_censors={}",
                    coverage.live_attribution_candidates,
                    coverage.live_attribution_materialized,
                    coverage.live_attribution_dropped_missing_evidence,
                    coverage.live_attribution_censored_superseded_unfilled,
                ),
            }
            .into());
        }
        Ok(())
    }

    async fn append_exit_decision_examples(
        &self,
        input: ExitDecisionAppendInput<'_>,
        sink: &mut ExampleBuildSink<'_>,
    ) -> QuantResult<()> {
        let lot_by_intent: HashMap<_, _> = input
            .plan
            .exit_training_lots
            .iter()
            .map(|lot| (lot.order_intent_id.clone(), lot))
            .collect();
        let exit_labelers = exit_decision_labelers();
        let builder = ConfiguredFeatureBuilder::new(&self.features, &self.domain)?;
        let engine = FactorEngine::new(
            &self.factors,
            &self.features,
            &self.domain,
            self.bias_table.as_ref().map(Arc::clone),
        );
        let replay_config = self.replay_config();

        for (as_of, group) in group_lot_samples(&input.plan.lot_samples) {
            let Some(cross_section) = materialize_lot_cross_section(LotCrossSectionMaterialize {
                builder: &builder,
                engine: &engine,
                replay_config: &replay_config,
                pit: input.pit,
                prefetched: input.prefetched,
                as_of,
                group: &group,
                context: input.context,
            })
            .await?
            else {
                sink.coverage.samples_dropped_insufficient += group.len() as u64;
                continue;
            };
            sink.coverage.samples_dropped_insufficient += cross_section.dropped_insufficient;

            for sample in group {
                let Some(lot) = lot_by_intent.get(&sample.order_intent_id) else {
                    continue;
                };
                let Some(index) = cross_section_index_for_lot_sample(&cross_section, sample) else {
                    sink.coverage.samples_dropped_insufficient += 1;
                    continue;
                };
                self.append_exit_decision_sample(
                    ExitDecisionSampleBuild {
                        sample,
                        lot,
                        cross_section: &cross_section,
                        market_index: index,
                        request: &input.plan.request,
                        prefetched: input.prefetched,
                        clob_market_info: input.clob_market_info,
                        pit: input.pit,
                        max_horizon_secs: input.context.max_horizon_secs,
                        labelers: &exit_labelers,
                    },
                    sink,
                )
                .await?;
            }
        }
        Ok(())
    }

    async fn append_exit_decision_sample(
        &self,
        input: ExitDecisionSampleBuild<'_>,
        sink: &mut ExampleBuildSink<'_>,
    ) -> QuantResult<()> {
        let market = &input.cross_section.markets[input.market_index];
        let vector = &input.cross_section.vectors[input.market_index];
        let entry_mid = input.cross_section.entry_mids[input.market_index];
        let outcome = &input.cross_section.outcomes[input.market_index];
        let factor_values = eligible_factor_values(outcome);
        let remaining = remaining_shares_at(
            &LotTerminalSnapshot::from(input.lot),
            input.sample.decision_at,
        );
        if !remaining.is_positive() {
            return Ok(());
        }
        let evidence = self
            .resolve_exit_decision_evidence(&input, market, entry_mid, remaining)
            .await?;
        let forward = forward_window(
            input.sample.decision_at,
            input.max_horizon_secs,
            input
                .prefetched
                .micro
                .get(&input.sample.token_id)
                .map_or(&[][..], Vec::as_slice),
            input
                .prefetched
                .resolutions
                .get(&input.sample.market_id)
                .map_or(&[][..], Vec::as_slice),
        )?;
        let labels = build_labels_for(
            &LabelBuildParams {
                labelers: input.labelers,
                market,
                as_of: input.sample.decision_at,
                entry_price: entry_mid,
                request: input.request,
                forward: &forward,
                exit_decision: Some(&evidence.label_context),
            },
            self.min_exit_depth_usd,
            sink.coverage,
        )?;
        record_exit_fill_fidelity(sink.coverage, evidence.book_fidelity);
        let decision_capture = input
            .cross_section
            .captures
            .get(&input.sample.market_id)
            .ok_or_else(|| ResearchError::DatasetBuild {
                detail: format!(
                    "exit-decision replay omitted capture for market {}",
                    input.sample.market_id
                ),
            })?
            .evidence();
        sink.market_set.insert(input.sample.market_id.clone());
        sink.examples.push(TrainingExample {
            example_id: TrainingExampleId::from_v7(),
            market_id: input.sample.market_id.clone(),
            token_id: input.sample.token_id.clone(),
            selected_market: market.clone(),
            decision_boundary: input.cross_section.boundary.clone(),
            sample_source: TrainingSampleSource::ExitDecision,
            feature_vector: vector.clone(),
            factor_values,
            labels,
            source_refs: vector.evidence_refs(),
            decision_capture: Some(decision_capture),
            lot_context: Some(LotTrainingContext {
                order_intent_id: input.sample.order_intent_id.clone(),
                position_id: input.sample.position_id.clone(),
                remaining_shares: remaining,
                avg_price: input.lot.avg_price,
                peak_mark: evidence.peak_mark,
                opened_at: input.lot.opened_at,
                max_hold_secs: input.lot.max_hold_secs,
            }),
            position_state: Some(evidence.position_state),
            book_fidelity: evidence.book_fidelity,
        });
        sink.coverage.exit_decision_built += 1;
        Ok(())
    }

    async fn resolve_exit_decision_evidence(
        &self,
        input: &ExitDecisionSampleBuild<'_>,
        market: &SelectedMarket,
        entry_mid: Option<Price>,
        remaining: Shares,
    ) -> QuantResult<ResolvedExitDecisionEvidence> {
        let micro = input
            .prefetched
            .micro
            .get(&input.sample.token_id)
            .map_or(&[][..], Vec::as_slice);
        let boundary = DecisionClock::new(input.request.knowledge_lag_secs)
            .boundary(input.sample.decision_at)?;
        // Peak-to-cutoff is strictly historical. The lot's terminal lifetime
        // peak would leak future price into an early-tick drawdown feature.
        let peak_mark = peak_mark_to(micro, input.lot.opened_at, &boundary);
        let position_state = position_state_features(LotStateInput {
            avg_price: input.lot.avg_price.inner(),
            mark: entry_mid.map(Price::inner),
            opened_at: input.lot.opened_at,
            now: input.sample.decision_at,
            max_hold_secs: input.lot.max_hold_secs,
            peak_mark: peak_mark.map(Price::inner),
        })?;
        let (decision_book, book_fidelity) =
            decision_book_at(input.pit, &input.sample.token_id, &boundary, micro).await?;
        // Quote fees from the actual executable bid used by the label. The lot
        // cost basis and non-executable mid are never substitutes.
        let fee_schedule = decision_book
            .as_ref()
            .and_then(decision_book_quote_price)
            .map(|price| {
                pit_exit_fee_schedule(
                    remaining,
                    price,
                    market,
                    &input.sample.token_id,
                    input.sample.decision_at,
                    boundary.knowledge_cutoff(),
                    input.clob_market_info,
                )
            })
            .transpose()?;
        Ok(ResolvedExitDecisionEvidence {
            peak_mark,
            position_state,
            label_context: ExitDecisionLabelContext {
                remaining_shares: remaining,
                avg_price: input.lot.avg_price,
                fee_schedule,
                terminal: LotTerminalSnapshot::from(input.lot),
                decision_book,
            },
            book_fidelity,
        })
    }

    /// Assert leakage-freedom, hash the schemas + content, write the Parquet
    /// artifact, and record the ledger row.
    async fn finalize(
        &self,
        builder: &ConfiguredFeatureBuilder,
        engine: &FactorEngine,
        plan: DatasetPlan,
        examples: Vec<TrainingExample>,
        coverage: DatasetCoverage,
    ) -> QuantResult<TrainingDatasetArtifact> {
        self.validate_finalization_input(&plan, &examples).await?;

        let feature_schema_hash = ResearchHasher::feature_schema(builder.schema())?;
        let factor_schema_hash = ResearchHasher::factor_schema(&engine.factor_set())?;
        let label_schema_hash = ResearchHasher::label_schema(&plan.label_names)?;
        let coverage = self
            .complete_integrity_coverage(&plan.request.model_spec_id, &examples, builder, coverage)
            .await?;
        let integrity = dataset_integrity_outcome(&coverage)?;
        let dataset_hash = TrainingDatasetArtifact::compute_dataset_hash(
            DatasetHashContract {
                model_spec_id: &plan.request.model_spec_id,
                window_start: plan.request.window_start,
                window_end: plan.request.window_end,
                purpose: plan.request.purpose,
                feature_schema_hash: &feature_schema_hash,
                factor_schema_hash: &factor_schema_hash,
                label_schema_hash: &label_schema_hash,
            },
            &examples,
        )?;

        let persisted = self
            .persist_dataset_artifact(
                &plan,
                &examples,
                DatasetSchemaHashes {
                    feature: feature_schema_hash.clone(),
                    factor: factor_schema_hash.clone(),
                    label: label_schema_hash.clone(),
                },
                dataset_hash.clone(),
            )
            .await?;

        let sample_count_i64 =
            i64::try_from(examples.len()).map_err(|error| ResearchError::DatasetBuild {
                detail: format!("dataset sample count exceeds Postgres bigint: {error}"),
            })?;
        self.dataset_repo
            .complete_build(
                &plan.training_dataset_id,
                CompleteTrainingDatasetBuild {
                    status: integrity.status,
                    feature_schema_hash: feature_schema_hash.clone(),
                    factor_schema_hash: factor_schema_hash.clone(),
                    label_schema_hash: label_schema_hash.clone(),
                    dataset_hash: dataset_hash.clone(),
                    manifest_hash: persisted.manifest_hash,
                    manifest_json: persisted.manifest.clone(),
                    artifact_bytes_hash: persisted.artifact_bytes_hash.clone(),
                    parquet_uri: persisted.parquet_uri.clone(),
                    sample_count: sample_count_i64,
                    coverage_json: coverage.clone(),
                    failure_detail: integrity.failure_detail,
                },
            )
            .await
            .map_err(QuantError::from)?;

        Ok(TrainingDatasetArtifact {
            format_version: DATASET_ARTIFACT_FORMAT_VERSION,
            training_dataset_id: plan.training_dataset_id,
            model_spec_id: plan.request.model_spec_id,
            window_start: plan.request.window_start,
            window_end: plan.request.window_end,
            examples,
            feature_schema_hash,
            factor_schema_hash,
            label_schema_hash,
            dataset_hash,
            manifest: persisted.manifest,
            artifact_bytes_hash: persisted.artifact_bytes_hash,
            parquet_uri: persisted.parquet_uri,
            coverage,
        })
    }

    async fn validate_finalization_input(
        &self,
        plan: &DatasetPlan,
        examples: &[TrainingExample],
    ) -> QuantResult<()> {
        assert_no_future_leakage(examples)?;
        // Reject overlapping calibration windows before artifact I/O. Fit-time
        // services repeat this check; model-specific embargo remains a publish
        // concern because this dataset is not yet bound to one model version.
        if plan.request.purpose == DatasetPurpose::Calibration {
            let window = TimeWindow::new(plan.request.window_start, plan.request.window_end);
            assert_disjoint_from_all_training_datasets(
                self.dataset_repo.as_ref(),
                &window,
                "calibration dataset build",
            )
            .await?;
        }
        Ok(())
    }

    async fn complete_integrity_coverage(
        &self,
        model_spec_id: &ModelSpecId,
        examples: &[TrainingExample],
        builder: &ConfiguredFeatureBuilder,
        coverage: DatasetCoverage,
    ) -> QuantResult<DatasetCoverage> {
        let model_spec = self
            .model_registry
            .find_model_spec_by_id(model_spec_id)
            .await
            .map_err(QuantError::from)?
            .ok_or_else(|| StorageError::NotFound {
                entity: "quant_model_spec",
                id: model_spec_id.to_string(),
            })?;
        model_spec
            .training_contract
            .validate()
            .map_err(|detail| ResearchError::DatasetBuild {
                detail: format!(
                    "model spec {} has invalid training contract: {detail}",
                    model_spec.model_spec_id
                ),
            })?;
        let target_label = LabelName::new(model_spec.training_contract.target_label_name.clone());
        self.complete_coverage(
            examples,
            builder,
            &model_spec.input_contract,
            &target_label,
            model_spec.training_contract.target_label_horizon_secs,
            coverage,
        )
    }

    fn complete_coverage(
        &self,
        examples: &[TrainingExample],
        builder: &ConfiguredFeatureBuilder,
        input_contract: &ModelInputContract,
        target_label: &LabelName,
        target_horizon_secs: u64,
        mut coverage: DatasetCoverage,
    ) -> QuantResult<DatasetCoverage> {
        coverage.bias_table_hash = self
            .bias_table
            .as_ref()
            .map(|table| table.content_hash.clone());
        coverage.feature_state_counts = dataset_feature_state_counts(examples);
        coverage.matrix_probe = Some(probe_matrix_coverage(
            examples,
            builder.schema(),
            input_contract,
            target_label,
            target_horizon_secs,
        )?);
        Ok(coverage)
    }

    async fn persist_dataset_artifact(
        &self,
        plan: &DatasetPlan,
        examples: &[TrainingExample],
        schema_hashes: DatasetSchemaHashes,
        dataset_hash: ContentHash,
    ) -> QuantResult<PersistedDatasetArtifact> {
        let sample_count =
            u64::try_from(examples.len()).map_err(|error| ResearchError::DatasetBuild {
                detail: format!("dataset sample count conversion failed: {error}"),
            })?;
        let manifest = DatasetManifest {
            format_version: DATASET_ARTIFACT_FORMAT_VERSION,
            training_dataset_id: plan.training_dataset_id.clone(),
            profile_ref: plan.request.profile_ref.clone(),
            research_program_hash: plan.request.research_program_hash.clone(),
            source_slice: plan.request.source_slice.clone(),
            model_spec_id: plan.request.model_spec_id.clone(),
            model_spec_definition_hash: plan.model_spec_definition_hash.clone(),
            trade_policy_artifact_id: plan.trade_policy_artifact_id.clone(),
            trade_policy_hash: plan.trade_policy_hash.clone(),
            decision_policy_snapshot_id: plan.request.decision_policy_snapshot_id.clone(),
            window_start: plan.request.window_start,
            window_end: plan.request.window_end,
            purpose: plan.request.purpose,
            knowledge_lag_secs: plan.request.knowledge_lag_secs,
            sample_interval_secs: plan.request.sample_interval_secs,
            horizons_secs: plan.request.horizons_secs.clone(),
            feature_schema_hash: schema_hashes.feature,
            factor_schema_hash: schema_hashes.factor,
            label_schema_hash: schema_hashes.label,
            semantic_dataset_hash: dataset_hash,
            source_fingerprint: dataset_source_fingerprint(examples)?,
            sample_count,
        };
        let manifest_hash = dataset_manifest_hash(&manifest)?;
        let parquet_bytes = DatasetParquetCodec::encode(examples, &manifest)?;
        let artifact_bytes_hash =
            ContentHash::parse(CanonicalDigest::prefixed_bytes(&parquet_bytes))?;
        let key = ArtifactKey::new(
            ArtifactNamespace::Dataset,
            plan.training_dataset_id.as_uuid().to_string(),
            "parquet",
        )?;
        let parquet_uri = self.artifact_store.put(key, &parquet_bytes).await?;
        let persisted_bytes = self.artifact_store.get(&parquet_uri).await?;
        let persisted_hash = ContentHash::parse(CanonicalDigest::prefixed_bytes(&persisted_bytes))?;
        if persisted_hash != artifact_bytes_hash {
            return Err(ResearchError::DatasetBuild {
                detail: format!(
                    "dataset artifact byte hash changed after persistence: encoded {artifact_bytes_hash}, persisted {persisted_hash}"
                ),
            }
            .into());
        }
        let decoded = DatasetParquetCodec::decode_with_manifest(&persisted_bytes)?;
        if decoded.manifest != manifest
            || dataset_manifest_hash(&decoded.manifest)? != manifest_hash
        {
            return Err(ResearchError::DatasetBuild {
                detail: "dataset manifest changed during Parquet persistence".to_owned(),
            }
            .into());
        }
        Ok(PersistedDatasetArtifact {
            manifest,
            manifest_hash,
            artifact_bytes_hash,
            parquet_uri,
        })
    }
}

struct DatasetIntegrityOutcome {
    status: TrainingDatasetStatus,
    failure_detail: Option<String>,
}

fn dataset_integrity_outcome(coverage: &DatasetCoverage) -> QuantResult<DatasetIntegrityOutcome> {
    let probe = coverage
        .matrix_probe
        .as_ref()
        .ok_or_else(|| ResearchError::DatasetBuild {
            detail: "deterministic integrity gate did not produce a matrix probe".to_owned(),
        })?;
    if coverage.built_examples == 0 {
        return Ok(DatasetIntegrityOutcome {
            status: TrainingDatasetStatus::Failed,
            failure_detail: Some(
                "deterministic integrity gate rejected dataset: no materialized examples"
                    .to_owned(),
            ),
        });
    }
    if probe.label_rows == 0 {
        return Ok(DatasetIntegrityOutcome {
            status: TrainingDatasetStatus::InsufficientLabels,
            failure_detail: None,
        });
    }
    if probe.accepted_rows == 0 {
        return Ok(DatasetIntegrityOutcome {
            status: TrainingDatasetStatus::Failed,
            failure_detail: Some(format!(
                "deterministic integrity gate rejected every target-labelled row: accepted={}, rejected={}, target={}/{}",
                probe.accepted_rows,
                probe.rejected_rows,
                probe.label_name,
                probe.label_horizon_secs,
            )),
        });
    }
    Ok(DatasetIntegrityOutcome {
        status: TrainingDatasetStatus::Ready,
        failure_detail: None,
    })
}

fn dataset_feature_state_counts(examples: &[TrainingExample]) -> DatasetFeatureStateCounts {
    let mut counts = DatasetFeatureStateCounts::default();
    for cell in examples
        .iter()
        .flat_map(|example| example.feature_vector.iter_cells().map(|(_, cell)| cell))
    {
        match cell.state {
            FeatureCellState::Observed => counts.observed += 1,
            FeatureCellState::Substituted => counts.substituted += 1,
            FeatureCellState::Missing => counts.missing += 1,
            FeatureCellState::NotApplicable => counts.not_applicable += 1,
        }
    }
    counts
}

struct DatasetSchemaHashes {
    feature: ContentHash,
    factor: ContentHash,
    label: ContentHash,
}

struct PersistedDatasetArtifact {
    manifest: DatasetManifest,
    manifest_hash: ContentHash,
    artifact_bytes_hash: ContentHash,
    parquet_uri: ArtifactUri,
}

/// Build every label (labeler × horizon) for one example, accounting coverage.
/// Free function so the historical spine can call it from a `spawn_blocking`
/// closure (no `&self` borrow).
fn build_labels_for(
    params: &LabelBuildParams<'_>,
    min_exit_depth_usd: Usd,
    coverage: &mut DatasetCoverage,
) -> QuantResult<Vec<TrainingLabel>> {
    let mut labels = Vec::new();
    for labeler in params.labelers {
        let horizons: Vec<u64> = if labeler.is_horizon_dependent() {
            params.request.horizons_secs.clone()
        } else {
            vec![0]
        };
        for horizon_secs in horizons {
            let input = LabelBuildInput {
                market_id: &params.market.market_id,
                token_id: &params.market.primary_token_id,
                yes_token_id: &params.market.primary_token_id,
                decision_at: params.as_of,
                entry_price: params.entry_price,
                horizon_secs,
                min_exit_depth_usd,
                forward: params.forward,
                exit_decision: params.exit_decision,
            };
            match labeler.build_label(&input) {
                LabelBuildOutput::Available(label) => {
                    coverage.labels_available += 1;
                    labels.push(label);
                }
                LabelBuildOutput::NotMature { .. } => coverage.labels_not_mature += 1,
                LabelBuildOutput::Unavailable { .. } => coverage.labels_unavailable += 1,
                LabelBuildOutput::Invalid { detail } => {
                    return Err(ResearchError::LabelResolution {
                        detail: format!(
                            "{} for market {} at {}: {detail}",
                            labeler.label_name(),
                            params.market.market_id,
                            params.as_of
                        ),
                    }
                    .into());
                }
            }
        }
    }
    Ok(labels)
}

/// Accumulated examples + distinct markets from the historical spine, merged
/// with the live-attribution / exit-decision sources before finalization.
#[derive(Default)]
struct HistoricalSpine {
    examples: Vec<TrainingExample>,
    market_set: HashSet<MarketId>,
}

/// Result of the historical PIT loop: examples/markets, the updated coverage,
/// and whether the loop unwound early on cooperative cancellation.
struct HistoricalSpineOutput {
    examples: Vec<TrainingExample>,
    market_set: HashSet<MarketId>,
    coverage: DatasetCoverage,
    cancelled: bool,
}

/// Borrowed inputs for the historical PIT loop (async/inline callers).
struct HistoricalSpineParams<'a> {
    pit: &'a dyn PointInTimeSnapshotSource,
    prefetched: &'a Prefetched,
    sink: &'a dyn JobProgressSink,
    cancel: &'a CancellationToken,
    samples: &'a [SamplePlan],
    request: &'a DatasetPlanRequest,
    features: &'a FeaturesConfig,
    factors: &'a FactorsConfig,
    data_quality: &'a DataQualityConfig,
    domain: &'a DomainConfig,
    selection: &'a SelectionConfig,
    /// Target `ModelSpec`'s resolved feature requirements.
    model_requirements: ModelFeatureRequirements,
    labelers: &'a [Box<dyn Labeler>],
    min_exit_depth_usd: Usd,
    /// Frozen favorite-longshot bias table bound to the spine factor engine.
    bias_table: &'a Option<Arc<FavoriteLongshotBiasTable>>,
    context: ReplayContext,
}

/// Owned inputs for the historical PIT loop, moved into a `spawn_blocking`
/// closure (every field is `Send + 'static`; `Arc` shares the PIT engine,
/// prefetched facts, progress sink, and labelers with the async parent).
struct HistoricalSpineInputs {
    pit: Arc<dyn PointInTimeSnapshotSource>,
    prefetched: Arc<Prefetched>,
    sink: Arc<dyn JobProgressSink>,
    cancel: CancellationToken,
    samples: Vec<SamplePlan>,
    request: DatasetPlanRequest,
    features: FeaturesConfig,
    factors: FactorsConfig,
    data_quality: DataQualityConfig,
    domain: DomainConfig,
    selection: SelectionConfig,
    /// Target `ModelSpec`'s resolved feature requirements.
    model_requirements: ModelFeatureRequirements,
    labelers: Arc<Vec<Box<dyn Labeler>>>,
    min_exit_depth_usd: Usd,
    /// Frozen favorite-longshot bias table bound to the spine factor engine.
    bias_table: Option<Arc<FavoriteLongshotBiasTable>>,
    context: ReplayContext,
    coverage: DatasetCoverage,
}

/// Run the historical PIT spine on a blocking thread.
///
/// The per-section materialization is in-memory (the `MaterializedPitEngine`
/// resolves books/markets without I/O), so its `async` entrypoints resolve
/// immediately — we drive them with `block_on` on this blocking thread rather
/// than occupying an async runtime worker. `cancel` is polled per section.
fn run_historical_spine_blocking(
    inputs: HistoricalSpineInputs,
) -> QuantResult<HistoricalSpineOutput> {
    Handle::current().block_on(run_historical_spine(
        HistoricalSpineParams {
            pit: &*inputs.pit,
            prefetched: &inputs.prefetched,
            sink: &*inputs.sink,
            cancel: &inputs.cancel,
            samples: &inputs.samples,
            request: &inputs.request,
            features: &inputs.features,
            factors: &inputs.factors,
            data_quality: &inputs.data_quality,
            domain: &inputs.domain,
            selection: &inputs.selection,
            model_requirements: inputs.model_requirements,
            labelers: &inputs.labelers,
            min_exit_depth_usd: inputs.min_exit_depth_usd,
            bias_table: &inputs.bias_table,
            context: inputs.context,
        },
        inputs.coverage,
    ))
}

/// Replay the online selection funnel per `as_of` and materialize the surviving
/// cross-sections into training examples (train/serve selection consistency).
///
/// Polls `cancel` at each cross-section boundary: a cancelled build returns
/// early with `cancelled = true` and the partial (discarded) accumulation.
async fn run_historical_spine(
    params: HistoricalSpineParams<'_>,
    mut coverage: DatasetCoverage,
) -> QuantResult<HistoricalSpineOutput> {
    let builder = ConfiguredFeatureBuilder::new(params.features, params.domain)?;
    let engine = FactorEngine::new(
        params.factors,
        params.features,
        params.domain,
        params.bias_table.as_ref().map(Arc::clone),
    );
    let replay_config = ReplayConfig {
        features: params.features.clone(),
        factors: params.factors.clone(),
        domain: params.domain.clone(),
        data_quality: params.data_quality.clone(),
        bias_table: params.bias_table.as_ref().map(Arc::clone),
    };
    let pit_selector = OfflinePitSelector::new(
        params.selection,
        params.data_quality,
        params.features,
        params.request.decision_policy_snapshot_id.clone(),
        params.request.knowledge_lag_secs,
        params.model_requirements.clone(),
    );
    let mut examples: Vec<TrainingExample> = Vec::new();
    let mut market_set: HashSet<MarketId> = HashSet::new();
    let sections = group_samples(params.samples);
    let total_sections = sections.len() as u64;
    let mut processed_sections: u64 = 0;
    for (as_of, group) in sections {
        // Cooperative cancel at the section boundary → ~one-section latency.
        if params.cancel.is_cancelled() {
            return Ok(HistoricalSpineOutput {
                examples,
                market_set,
                coverage,
                cancelled: true,
            });
        }
        processed_sections += 1;
        params.sink.report(ResearchJobProgress::with_total(
            "materialize",
            processed_sections,
            total_sections,
        ));
        let replay_group = pit_selected_replay_group(PitSelectionReplayParams {
            pit_selector: &pit_selector,
            as_of,
            group: &group,
            pit: params.pit,
            prefetched: params.prefetched,
            domain: params.domain,
            knowledge_lag_secs: params.request.knowledge_lag_secs,
            coverage: &mut coverage,
        })
        .await?;
        if replay_group.is_empty() {
            continue;
        }
        let Some(cross_section) = materialize_cross_section(
            &builder,
            &engine,
            &replay_config,
            &CrossSectionRequest {
                pit: params.pit,
                prefetched: params.prefetched,
                decision_at: as_of,
                group: &replay_group,
                knowledge_lag: params.context.knowledge_lag,
                lookback: params.context.lookback,
            },
        )
        .await?
        else {
            continue;
        };
        coverage.samples_dropped_insufficient += cross_section.dropped_insufficient;
        append_historical_examples(
            &CrossSectionAppendInput {
                cross_section: &cross_section,
                prefetched: params.prefetched,
                request: params.request,
                max_horizon_secs: params.context.max_horizon_secs,
            },
            params.labelers,
            params.min_exit_depth_usd,
            &mut ExampleBuildSink {
                coverage: &mut coverage,
                examples: &mut examples,
                market_set: &mut market_set,
            },
        )?;
    }
    Ok(HistoricalSpineOutput {
        examples,
        market_set,
        coverage,
        cancelled: false,
    })
}

/// Replay the point-in-time selection funnel over one `as_of` cross-section,
/// folding exclusions into `coverage` and returning the surviving samples.
struct PitSelectionReplayParams<'a> {
    pit_selector: &'a OfflinePitSelector,
    as_of: DateTime<Utc>,
    group: &'a [&'a SamplePlan],
    pit: &'a dyn PointInTimeSnapshotSource,
    prefetched: &'a Prefetched,
    domain: &'a DomainConfig,
    knowledge_lag_secs: u64,
    coverage: &'a mut DatasetCoverage,
}

async fn pit_selected_replay_group(
    params: PitSelectionReplayParams<'_>,
) -> QuantResult<Vec<ReplaySample>> {
    let market_ids: Vec<MarketId> = params
        .group
        .iter()
        .map(|sample| sample.market_id.clone())
        .collect();
    // Zero-I/O: replayed from the same batch-prefetched linkage +
    // domain-observation window `build_domain_slice_inputs` already uses for
    // this build's actual feature computation for parity verification.
    let domain_source = PrefetchedDomainAvailabilitySource::new(params.prefetched, params.domain);
    let boundary = replay_boundary(
        params.as_of,
        params.knowledge_lag_secs,
        params.domain.crypto.availability_lag_secs,
        params.domain.weather.availability_lag_secs,
    )?;
    let selection = params
        .pit_selector
        .select_at(&boundary, &market_ids, params.pit, &domain_source)
        .await?;
    params.coverage.pit_selection_candidates += market_ids.len() as u64;
    params.coverage.pit_selection_included += selection.included.len() as u64;
    params.coverage.pit_selection_excluded += selection.exclusion_summary;
    let kept: HashSet<MarketId> = selection
        .included
        .iter()
        .map(|market| market.market_id.clone())
        .collect();
    Ok(params
        .group
        .iter()
        .filter(|sample| kept.contains(&sample.market_id))
        .map(|sample| ReplaySample {
            market_id: sample.market_id.clone(),
            token_id: sample.token_id.clone(),
        })
        .collect())
}

/// Append training examples (factors + forward labels) for one PIT-resolved
/// cross-section. Free function so the historical spine can run inside a
/// `spawn_blocking` closure without borrowing the service.
fn append_historical_examples(
    input: &CrossSectionAppendInput<'_>,
    labelers: &[Box<dyn Labeler>],
    min_exit_depth_usd: Usd,
    sink: &mut ExampleBuildSink<'_>,
) -> QuantResult<()> {
    for (index, vector) in input.cross_section.vectors.iter().enumerate() {
        let market = &input.cross_section.markets[index];
        let entry_price = input
            .cross_section
            .captures
            .get(&market.market_id)
            .and_then(|capture| capture.market_context.best_ask);
        let outcome = &input.cross_section.outcomes[index];
        let factor_values = match &outcome.eligibility {
            FactorEligibility::Eligible => outcome
                .factors
                .iter()
                .map(|scored| scored.value.clone())
                .collect(),
            FactorEligibility::RejectCandidate { .. } | FactorEligibility::NotApplicable { .. } => {
                Vec::new()
            }
        };
        let forward = forward_window(
            input.cross_section.boundary.decision_at(),
            input.max_horizon_secs,
            input
                .prefetched
                .micro
                .get(&market.primary_token_id)
                .map_or(&[][..], Vec::as_slice),
            input
                .prefetched
                .resolutions
                .get(&market.market_id)
                .map_or(&[][..], Vec::as_slice),
        )?;
        let labels = build_labels_for(
            &LabelBuildParams {
                labelers,
                market,
                as_of: input.cross_section.boundary.decision_at(),
                entry_price,
                request: input.request,
                forward: &forward,
                exit_decision: None,
            },
            min_exit_depth_usd,
            sink.coverage,
        )?;
        let decision_capture = input
            .cross_section
            .captures
            .get(&market.market_id)
            .ok_or_else(|| ResearchError::DatasetBuild {
                detail: format!(
                    "historical replay omitted capture for market {}",
                    market.market_id
                ),
            })?
            .evidence();
        sink.market_set.insert(market.market_id.clone());
        sink.examples.push(TrainingExample {
            example_id: TrainingExampleId::from_v7(),
            market_id: market.market_id.clone(),
            token_id: market.primary_token_id.clone(),
            selected_market: market.clone(),
            decision_boundary: input.cross_section.boundary.clone(),
            sample_source: TrainingSampleSource::HistoricalPit,
            feature_vector: vector.clone(),
            factor_values,
            labels,
            source_refs: vector.evidence_refs(),
            decision_capture: Some(decision_capture),
            lot_context: None,
            position_state: None,
            book_fidelity: None,
        });
    }
    Ok(())
}

enum LiveAttributionMaterialization {
    Example(Box<TrainingExample>),
    CensoredSupersededUnfilled,
    MissingEvidence,
}

fn record_live_attribution_materialization(
    materialization: LiveAttributionMaterialization,
    coverage: &mut DatasetCoverage,
    examples: &mut Vec<TrainingExample>,
    market_set: &mut HashSet<MarketId>,
) {
    match materialization {
        LiveAttributionMaterialization::Example(example) => {
            coverage.planned_samples += 1;
            coverage.live_attribution_materialized += 1;
            market_set.insert(example.market_id.clone());
            coverage.labels_available += example.labels.len() as u64;
            examples.push(*example);
        }
        LiveAttributionMaterialization::CensoredSupersededUnfilled => {
            coverage.live_attribution_censored_superseded_unfilled += 1;
        }
        LiveAttributionMaterialization::MissingEvidence => {
            coverage.planned_samples += 1;
            coverage.live_attribution_dropped_missing_evidence += 1;
        }
    }
}

fn materialize_live_attribution_example(
    attribution: &RecommendationAttributionInfo,
    recommendations: &HashMap<RecommendationId, RecommendationInfo>,
    features: &HashMap<FeatureVectorId, FeatureVectorInfo>,
    selections: &HashMap<(MarketSelectionId, MarketId), MarketSelectionMemberInfo>,
) -> LiveAttributionMaterialization {
    if attribution.outcome == RecommendationAttributionOutcome::SupersededUnfilled {
        return LiveAttributionMaterialization::CensoredSupersededUnfilled;
    }
    materialize_uncensored_live_attribution_example(
        attribution,
        recommendations,
        features,
        selections,
    )
    .map_or(LiveAttributionMaterialization::MissingEvidence, |example| {
        LiveAttributionMaterialization::Example(Box::new(example))
    })
}

fn materialize_uncensored_live_attribution_example(
    attribution: &RecommendationAttributionInfo,
    recommendations: &HashMap<RecommendationId, RecommendationInfo>,
    features: &HashMap<FeatureVectorId, FeatureVectorInfo>,
    selections: &HashMap<(MarketSelectionId, MarketId), MarketSelectionMemberInfo>,
) -> Option<TrainingExample> {
    let bindings = live_attribution_bindings(attribution, recommendations, features, selections)?;
    let evidence = frozen_live_feature_evidence(bindings.recommendation, bindings.feature_info)?;
    let recommendation = bindings.recommendation;
    let Some(factor_values) = frozen_factor_values(recommendation) else {
        tracing::warn!(
            recommendation_id = %attribution.recommendation_id,
            "live attribution sample dropped: frozen factor definitions are incomplete",
        );
        return None;
    };

    let labels = attribution_labels(attribution, recommendation);
    Some(TrainingExample {
        example_id: TrainingExampleId::from_v7(),
        market_id: recommendation.market_id.clone(),
        token_id: recommendation.token_id.clone(),
        selected_market: selected_market_from_member(bindings.selection_member),
        decision_boundary: evidence.decision_boundary,
        sample_source: TrainingSampleSource::LiveAttribution,
        source_refs: evidence.feature_vector.evidence_refs(),
        decision_capture: Some(evidence.decision_capture),
        feature_vector: evidence.feature_vector,
        factor_values,
        labels,
        lot_context: None,
        position_state: None,
        book_fidelity: None,
    })
}

struct LiveAttributionBindings<'a> {
    recommendation: &'a RecommendationInfo,
    feature_info: &'a FeatureVectorInfo,
    selection_member: &'a MarketSelectionMemberInfo,
}

fn live_attribution_bindings<'a>(
    attribution: &RecommendationAttributionInfo,
    recommendations: &'a HashMap<RecommendationId, RecommendationInfo>,
    features: &'a HashMap<FeatureVectorId, FeatureVectorInfo>,
    selections: &'a HashMap<(MarketSelectionId, MarketId), MarketSelectionMemberInfo>,
) -> Option<LiveAttributionBindings<'a>> {
    let recommendation = recommendations
        .get(&attribution.recommendation_id)
        .or_else(|| {
            tracing::warn!(
                recommendation_id = %attribution.recommendation_id,
                "live attribution sample dropped: recommendation not found",
            );
            None
        })?;
    if !recommendation.status.eligible_for_attribution() {
        tracing::warn!(
            recommendation_id = %attribution.recommendation_id,
            "live attribution sample dropped: recommendation is not attribution-eligible",
        );
        return None;
    }
    let feature_info = features
        .get(&recommendation.evidence_refs.feature_vector_id)
        .or_else(|| {
            tracing::warn!(
                recommendation_id = %attribution.recommendation_id,
                feature_vector_id = %recommendation.evidence_refs.feature_vector_id,
                "live attribution sample dropped: frozen feature vector not found",
            );
            None
        })?;
    let selection_key = (
        recommendation.evidence_refs.market_selection_id.clone(),
        recommendation.market_id.clone(),
    );
    let selection_member = selections.get(&selection_key).or_else(|| {
        tracing::warn!(
            recommendation_id = %attribution.recommendation_id,
            market_selection_id = %recommendation.evidence_refs.market_selection_id,
            market_id = %recommendation.market_id,
            "live attribution sample dropped: frozen selection member not found",
        );
        None
    })?;
    if selection_member.primary_token_id != recommendation.token_id {
        tracing::warn!(
            recommendation_id = %attribution.recommendation_id,
            selection_token_id = %selection_member.primary_token_id,
            recommendation_token_id = %recommendation.token_id,
            "live attribution sample dropped: selection token binding mismatch",
        );
        return None;
    }
    Some(LiveAttributionBindings {
        recommendation,
        feature_info,
        selection_member,
    })
}

struct FrozenLiveFeatureEvidence {
    feature_vector: FeatureVector,
    decision_boundary: DecisionBoundary,
    decision_capture: DecisionCaptureEvidence,
}

fn frozen_live_feature_evidence(
    recommendation: &RecommendationInfo,
    feature_info: &FeatureVectorInfo,
) -> Option<FrozenLiveFeatureEvidence> {
    let feature_vector = frozen_feature_vector(feature_info).or_else(|| {
        tracing::warn!(
            recommendation_id = %recommendation.recommendation_id,
            feature_vector_id = %recommendation.evidence_refs.feature_vector_id,
            "live attribution sample dropped: frozen feature vector payload is invalid",
        );
        None
    })?;
    let decision_boundary = feature_info.decision_boundary.clone();
    let decision_capture = feature_info.decision_capture.clone();
    if !valid_live_feature_evidence(feature_info, &decision_boundary, &decision_capture) {
        tracing::warn!(
            recommendation_id = %recommendation.recommendation_id,
            feature_vector_id = %recommendation.evidence_refs.feature_vector_id,
            "live attribution sample dropped: decision boundary or capture commitment is invalid",
        );
        return None;
    }
    Some(FrozenLiveFeatureEvidence {
        feature_vector,
        decision_boundary,
        decision_capture,
    })
}

fn valid_live_feature_evidence(
    feature_info: &FeatureVectorInfo,
    boundary: &DecisionBoundary,
    capture: &DecisionCaptureEvidence,
) -> bool {
    let capture_hash_matches = ResearchHasher::canonical(capture)
        .ok()
        .is_some_and(|actual| actual == feature_info.decision_capture_hash);
    capture_hash_matches
        && boundary.validate().is_ok()
        && boundary.decision_at() == feature_info.decision_at
}

fn selected_market_from_member(member: &MarketSelectionMemberInfo) -> SelectedMarket {
    SelectedMarket {
        market_id: member.market_id.clone(),
        event_id: member.event_id.clone(),
        category: member.category,
        primary_token_id: member.primary_token_id.clone(),
        secondary_token_id: member.secondary_token_id.clone(),
        liquidity_usd: member.liquidity_usd,
        volume_24h_usd: member.volume_24h_usd,
        source_refs: Vec::new(),
    }
}

fn frozen_feature_vector(info: &FeatureVectorInfo) -> Option<FeatureVector> {
    info.payload.validate().ok()?;
    Some(FeatureVector {
        market_id: info.market_id.clone(),
        token_id: info.token_id.clone(),
        decision_at: info.decision_at,
        generic_schema_version: info.feature_schema_version,
        generic: info.payload.generic.clone(),
        domain: info.payload.domain.clone(),
        data_quality: info.data_quality,
    })
}

fn frozen_factor_values(recommendation: &RecommendationInfo) -> Option<Vec<FactorValue>> {
    let mut values = Vec::with_capacity(recommendation.factor_breakdown.0.len());
    for (index, entry) in recommendation.factor_breakdown.0.iter().enumerate() {
        let definition_id = recommendation
            .evidence_refs
            .factor_definition_versions
            .get(index)?
            .clone();
        let normalization = match (
            entry.normalized_score,
            entry.normalization_source,
            entry.indeterminate_reason,
        ) {
            (Some(score), Some(source), _) => NormalizedFactor::Scored {
                score,
                source,
                clamp: None,
            },
            (_, _, Some(reason)) => NormalizedFactor::Indeterminate { reason },
            _ => NormalizedFactor::MissingInput,
        };
        values.push(FactorValue {
            definition_id,
            name: FactorName::new(entry.factor_name.clone()),
            family: entry.family,
            raw_value: entry.raw_value,
            normalization,
            direction: entry.direction,
            confidence: entry.confidence,
            explanation: FactorExplanation {
                headline: entry.explanation.clone(),
                drivers: Vec::new(),
            },
            input_feature_refs: Vec::new(),
        });
    }
    Some(values)
}

fn attribution_labels(
    attribution: &RecommendationAttributionInfo,
    recommendation: &RecommendationInfo,
) -> Vec<TrainingLabel> {
    // `label_available_at` is the authoritative `matured_at` for
    // live-attribution labels; a live attribution row is only ever built once
    // its outcome is known, so `created_at` (the row's own persistence time,
    // necessarily at/after every value it carries became known) is the safe
    // upper-bound fallback when the dedicated timestamp is absent — never an
    // arbitrary/earlier default that would under-purge.
    let matured_at = attribution
        .label_available_at
        .unwrap_or(attribution.created_at);
    let mut labels = Vec::new();
    if let Some(realized_pnl) = attribution.realized_pnl_usd {
        push_label(
            &mut labels,
            "realized_pnl_usd",
            realized_pnl.inner(),
            matured_at,
        );
        if let Some(sizing) = recommendation.trade_plan.sizing()
            && !sizing.suggested_usd.is_zero()
        {
            push_label(
                &mut labels,
                "realized_return_bps",
                realized_pnl.inner() / sizing.suggested_usd.inner() * Decimal::from(10_000),
                matured_at,
            );
        }
    }
    push_label(
        &mut labels,
        "entry_filled",
        if attribution.entry_outcome_json.entry_filled {
            Decimal::ONE
        } else {
            Decimal::ZERO
        },
        matured_at,
    );
    if let Some(slippage) = attribution.entry_outcome_json.entry_slippage_bps {
        push_label(
            &mut labels,
            "entry_slippage_bps",
            slippage.inner(),
            matured_at,
        );
    }
    if let Some(mfe) = attribution.max_favorable_excursion_bps {
        push_label(&mut labels, "max_favorable_excursion_bps", mfe, matured_at);
    }
    if let Some(mae) = attribution.max_adverse_excursion_bps {
        push_label(&mut labels, "max_adverse_excursion_bps", mae, matured_at);
    }
    push_label(
        &mut labels,
        "missed_return_bps",
        if attribution.entry_outcome_json.entry_filled {
            Decimal::ZERO
        } else {
            recommendation.expected_return_bps.inner()
        },
        matured_at,
    );
    if let Some(code) = recommendation_outcome_code(attribution.outcome) {
        push_label(&mut labels, "recommendation_outcome", code, matured_at);
    }
    labels
}

fn push_label(
    labels: &mut Vec<TrainingLabel>,
    name: &'static str,
    value: Decimal,
    matured_at: DateTime<Utc>,
) {
    labels.push(TrainingLabel {
        label_name: LabelName::from_static(name),
        horizon_secs: 0,
        value,
        is_resolved: true,
        matured_at,
    });
}

fn recommendation_outcome_code(outcome: RecommendationAttributionOutcome) -> Option<Decimal> {
    match outcome {
        RecommendationAttributionOutcome::FilledExited => Some(Decimal::ONE),
        RecommendationAttributionOutcome::FilledSettled => Some(Decimal::from(2)),
        RecommendationAttributionOutcome::ExpiredUnfilled => Some(Decimal::NEGATIVE_ONE),
        RecommendationAttributionOutcome::CancelledUnfilled => Some(Decimal::from(-2)),
        RecommendationAttributionOutcome::FailedUnfilled => Some(Decimal::from(-3)),
        RecommendationAttributionOutcome::SupersededUnfilled => None,
    }
}

fn wants_sample_source(request: &DatasetPlanRequest, source: TrainingSampleSource) -> bool {
    request.sample_sources.contains(&source)
}

fn planned_historical_samples(plan: &DatasetPlan) -> u64 {
    if wants_sample_source(&plan.request, TrainingSampleSource::HistoricalPit) {
        plan.samples.len() as u64
    } else {
        0
    }
}

/// Group sample instants by `as_of` (ascending) so each cross-section is scored
/// together (cross-sectional factor normalization needs the full same-`as_of`
/// set).
fn group_samples(samples: &[SamplePlan]) -> BTreeMap<DateTime<Utc>, Vec<&SamplePlan>> {
    let mut groups: BTreeMap<DateTime<Utc>, Vec<&SamplePlan>> = BTreeMap::new();
    for sample in samples {
        groups.entry(sample.decision_at).or_default().push(sample);
    }
    groups
}

fn group_lot_samples(samples: &[LotSamplePlan]) -> BTreeMap<DateTime<Utc>, Vec<&LotSamplePlan>> {
    let mut groups: BTreeMap<DateTime<Utc>, Vec<&LotSamplePlan>> = BTreeMap::new();
    for sample in samples {
        groups.entry(sample.decision_at).or_default().push(sample);
    }
    groups
}

fn decision_book_quote_price(book: &DecisionBook) -> Option<Price> {
    match book {
        DecisionBook::L2 { bids } => bids.first().map(|level| level.price_decimal()),
    }
}

fn pit_exit_fee_schedule(
    shares: Shares,
    price: Price,
    market: &SelectedMarket,
    token_id: &TokenId,
    effective_at: DateTime<Utc>,
    available_at_cutoff: DateTime<Utc>,
    versions: &[ClobMarketInfoVersion],
) -> QuantResult<MarketFeeSchedule> {
    let notional = shares.inner() * price.inner();
    if notional <= Decimal::ZERO {
        return Err(ResearchError::LabelResolution {
            detail: format!(
                "exit fee resolution requires positive notional, got shares={shares} price={price}"
            ),
        }
        .into());
    }
    let version = versions
        .iter()
        .filter(|version| {
            version.market_id == market.market_id
                && version
                    .tokens
                    .iter()
                    .any(|token| token.token_id == *token_id)
                && version.effective_at <= effective_at
                && version.available_at <= available_at_cutoff
        })
        .max_by(|left, right| {
            (
                left.effective_at,
                left.available_at,
                &left.payload_hash,
            )
                .cmp(&(
                    right.effective_at,
                    right.available_at,
                    &right.payload_hash,
                ))
        })
        .ok_or_else(|| ResearchError::LabelResolution {
            detail: format!(
                "Source Slice has no PIT-visible CLOB fee schedule for market {} token {} at effective_at={effective_at} available_by={available_at_cutoff}",
                market.market_id, token_id
            ),
        })?;
    version
        .validate()
        .map_err(|detail| ResearchError::LabelResolution {
            detail: format!(
                "Source Slice CLOB fee schedule {} is invalid: {detail}",
                version.version_id
            ),
        })?;
    Ok(version.fee_schedule())
}

async fn decision_book_at(
    pit: &dyn PointInTimeSnapshotSource,
    token_id: &TokenId,
    boundary: &DecisionBoundary,
    _micro: &[BookMicrostructureRow],
) -> QuantResult<(Option<DecisionBook>, Option<BookFidelity>)> {
    if let Some(snapshot) = pit.book_at_boundary(token_id, boundary).await?
        && !snapshot.bids.is_empty()
    {
        return Ok((
            Some(DecisionBook::L2 {
                bids: Arc::clone(&snapshot.bids),
            }),
            Some(BookFidelity::FullL2),
        ));
    }
    Ok((None, None))
}

/// Peak mark observed from the microstructure series between lot open and the
/// source cutoff, using only buckets available by decision time. `None` remains
/// explicit missing position-state evidence.
fn peak_mark_to(
    micro: &[BookMicrostructureRow],
    opened_at: DateTime<Utc>,
    boundary: &DecisionBoundary,
) -> Option<Price> {
    let start = opened_at.timestamp_millis();
    let end = boundary
        .cutoff_for(DecisionSource::Microstructure)
        .timestamp_millis();
    let decision_ms = boundary.decision_at().timestamp_millis();
    micro
        .iter()
        .filter(|row| {
            row.bucket_time >= start && row.bucket_time <= end && row.available_at <= decision_ms
        })
        .filter_map(|row| {
            row.best_bid_high
                .or(row.best_bid_close)
                .or(row.mid_price_close)
        })
        .map(ChPrice::to_price)
        .max()
}

struct CrossSectionAppendInput<'a> {
    cross_section: &'a ReplayCrossSection,
    prefetched: &'a Prefetched,
    request: &'a DatasetPlanRequest,
    max_horizon_secs: u64,
}

/// The post-spine tail shared by both build paths: point-in-time source,
/// prefetched facts, replay context, and progress sink.
struct BuildTail<'a> {
    pit: &'a dyn PointInTimeSnapshotSource,
    prefetched: &'a Prefetched,
    clob_market_info: &'a [ClobMarketInfoVersion],
    context: &'a ReplayContext,
    sink: &'a dyn JobProgressSink,
}

struct LabelBuildParams<'a> {
    labelers: &'a [Box<dyn Labeler>],
    market: &'a SelectedMarket,
    as_of: DateTime<Utc>,
    entry_price: Option<Price>,
    request: &'a DatasetPlanRequest,
    forward: &'a ForwardWindow,
    exit_decision: Option<&'a ExitDecisionLabelContext>,
}

struct ExitDecisionAppendInput<'a> {
    plan: &'a DatasetPlan,
    pit: &'a dyn PointInTimeSnapshotSource,
    prefetched: &'a Prefetched,
    clob_market_info: &'a [ClobMarketInfoVersion],
    context: &'a ReplayContext,
}

struct LotCrossSectionMaterialize<'a> {
    builder: &'a ConfiguredFeatureBuilder,
    engine: &'a FactorEngine,
    replay_config: &'a ReplayConfig,
    pit: &'a dyn PointInTimeSnapshotSource,
    prefetched: &'a Prefetched,
    as_of: DateTime<Utc>,
    group: &'a [&'a LotSamplePlan],
    context: &'a ReplayContext,
}

struct ExitDecisionSampleBuild<'a> {
    sample: &'a LotSamplePlan,
    lot: &'a ExitTrainingLotRow,
    cross_section: &'a ReplayCrossSection,
    market_index: usize,
    request: &'a DatasetPlanRequest,
    prefetched: &'a Prefetched,
    clob_market_info: &'a [ClobMarketInfoVersion],
    pit: &'a dyn PointInTimeSnapshotSource,
    max_horizon_secs: u64,
    labelers: &'a [Box<dyn Labeler>],
}

struct ResolvedExitDecisionEvidence {
    peak_mark: Option<Price>,
    position_state: PositionStateFeatures,
    label_context: ExitDecisionLabelContext,
    book_fidelity: Option<BookFidelity>,
}

struct ExampleBuildSink<'a> {
    coverage: &'a mut DatasetCoverage,
    examples: &'a mut Vec<TrainingExample>,
    market_set: &'a mut HashSet<MarketId>,
}

/// Derived replay parameters for one dataset build (shared across cross-sections).
#[derive(Clone, Copy)]
struct ReplayContext {
    knowledge_lag: Duration,
    lookback: Duration,
    max_horizon_secs: u64,
}

impl ReplayContext {
    /// Derive the knowledge lag, feature lookback, and max forward horizon.
    fn new(plan: &DatasetPlan, features: &FeaturesConfig) -> QuantResult<Self> {
        let max_horizon_secs = plan
            .request
            .horizons_secs
            .iter()
            .copied()
            .max()
            .ok_or_else(|| ResearchError::DatasetPlan {
                detail: "at least one label horizon is required".to_owned(),
            })?;
        Ok(Self {
            knowledge_lag: Duration::from_secs(plan.request.knowledge_lag_secs),
            lookback: Duration::from_secs(features.max_lookback_secs()),
            max_horizon_secs,
        })
    }

    /// The prefetch window spec for this build's sample set.
    fn window_spec(&self, plan: &DatasetPlan, domain: &DomainConfig) -> WindowSpec {
        WindowSpec {
            window_start: plan.request.window_start,
            window_end: plan.request.window_end,
            available_by: plan.request.pit_cutoff,
            samples: plan
                .samples
                .iter()
                .map(|sample| ReplaySample {
                    market_id: sample.market_id.clone(),
                    token_id: sample.token_id.clone(),
                })
                .chain(plan.lot_samples.iter().map(|sample| ReplaySample {
                    market_id: sample.market_id.clone(),
                    token_id: sample.token_id.clone(),
                }))
                .collect(),
            lookback: self.lookback,
            knowledge_lag: self.knowledge_lag,
            max_horizon_secs: self.max_horizon_secs,
            domain: domain.clone(),
        }
    }
}

fn cross_section_index_for_lot_sample(
    cross_section: &ReplayCrossSection,
    sample: &LotSamplePlan,
) -> Option<usize> {
    cross_section
        .markets
        .iter()
        .enumerate()
        .find_map(|(index, market)| {
            if market.market_id != sample.market_id {
                return None;
            }
            if market.primary_token_id != sample.token_id {
                return None;
            }
            Some(index)
        })
}

async fn materialize_lot_cross_section(
    input: LotCrossSectionMaterialize<'_>,
) -> QuantResult<Option<ReplayCrossSection>> {
    let replay_group: Vec<ReplaySample> = input
        .group
        .iter()
        .map(|sample| ReplaySample {
            market_id: sample.market_id.clone(),
            token_id: sample.token_id.clone(),
        })
        .collect();
    materialize_cross_section(
        input.builder,
        input.engine,
        input.replay_config,
        &CrossSectionRequest {
            pit: input.pit,
            prefetched: input.prefetched,
            decision_at: input.as_of,
            group: &replay_group,
            knowledge_lag: input.context.knowledge_lag,
            lookback: input.context.lookback,
        },
    )
    .await
}

fn eligible_factor_values(outcome: &MarketFactorOutcome) -> Vec<FactorValue> {
    match &outcome.eligibility {
        FactorEligibility::Eligible => outcome
            .factors
            .iter()
            .map(|scored| scored.value.clone())
            .collect(),
        FactorEligibility::RejectCandidate { .. } | FactorEligibility::NotApplicable { .. } => {
            Vec::new()
        }
    }
}

fn record_exit_fill_fidelity(coverage: &mut DatasetCoverage, book_fidelity: Option<BookFidelity>) {
    if book_fidelity == Some(BookFidelity::FullL2) {
        coverage.exit_fill_l2_rows += 1;
    } else if book_fidelity.is_some() {
        coverage.exit_fill_fallback_rows += 1;
    }
}

#[cfg(test)]
mod keep_rate_tests {
    use chrono::{Duration, TimeZone, Utc};
    use quant_pivot_models::{
        domain::data_plane::DecisionSource,
        enums::quant::DatasetPurpose,
        types::{DecisionPolicySnapshotId, ModelSpecId, SchemaVersion, default_sample_sources},
    };

    use super::{DatasetPlanRequest, KeepRateEstimate, KeepRateGrid};
    use crate::test_fixtures::execution_pg_seed::{
        content_hash, fixture_profile_ref, source_slice_ref,
    };

    fn request() -> DatasetPlanRequest {
        let window_start = Utc.timestamp_opt(1_000, 0).single().expect("start");
        DatasetPlanRequest {
            model_spec_id: ModelSpecId::from_v7(),
            profile_ref: fixture_profile_ref(),
            research_program_hash: content_hash('4'),
            source_slice: source_slice_ref('5'),
            decision_policy_snapshot_id: DecisionPolicySnapshotId::from_v7(),
            window_start,
            window_end: window_start + Duration::seconds(100),
            pit_cutoff: window_start + Duration::seconds(160),
            sample_interval_secs: 10,
            horizons_secs: vec![60],
            knowledge_lag_secs: 10,
            feature_schema_version: SchemaVersion::FIRST,
            sample_sources: default_sample_sources(),
            training_dataset_id: None,
            purpose: DatasetPurpose::Training,
        }
    }

    #[test]
    fn keep_rate_uses_checked_integer_rounding() {
        assert_eq!(
            KeepRateEstimate {
                included: 1,
                trials: 2,
            }
            .scale(1)
            .expect("scale"),
            1,
            "halfway estimates round to the nearest eligible row"
        );
        assert_eq!(
            KeepRateEstimate {
                included: 2,
                trials: 3,
            }
            .scale(10)
            .expect("scale"),
            7
        );
        let rate = KeepRateEstimate {
            included: 1,
            trials: 2,
        }
        .rate()
        .expect("rate");
        assert!((rate - 0.5).abs() < f64::EPSILON);
    }

    #[test]
    fn keep_rate_rejects_impossible_counts() {
        assert!(
            KeepRateEstimate {
                included: 0,
                trials: 0,
            }
            .scale(10)
            .is_err()
        );
        assert!(
            KeepRateEstimate {
                included: 3,
                trials: 2,
            }
            .rate()
            .is_err()
        );
    }

    #[test]
    fn keep_rate_grid_uses_integer_midpoints_and_one_boundary_derivation() {
        let grid = KeepRateGrid::new(&request(), 4, 30, 60).expect("grid");
        let expected_offsets = [12, 37, 62, 87];
        for (index, expected_offset) in expected_offsets.into_iter().enumerate() {
            let index = u32::try_from(index).expect("index");
            let boundary = grid.boundary(index).expect("boundary");
            assert_eq!(
                boundary.decision_at(),
                grid.window_start + Duration::seconds(expected_offset)
            );
            assert_eq!(
                boundary.knowledge_cutoff(),
                boundary.decision_at() - Duration::seconds(10)
            );
            assert_eq!(
                boundary.cutoff_for(DecisionSource::DomainCrypto),
                boundary.decision_at() - Duration::seconds(30)
            );
        }
        assert!(grid.boundary(4).is_err());
        assert!(KeepRateGrid::new(&request(), 0, 30, 60).is_err());
    }
}

#[cfg(test)]
mod decision_book_tests {
    use std::sync::Arc;

    use quant_pivot_models::{
        domain::market::book::BookLevel,
        types::{Price, Shares},
    };
    use rust_decimal::Decimal;

    use super::{DecisionBook, decision_book_quote_price};

    #[test]
    fn fee_quote_price_comes_from_executable_decision_bid() {
        let book = DecisionBook::L2 {
            bids: Arc::from([
                BookLevel::from_decimal_unchecked(
                    Price::new(Decimal::new(55, 2)),
                    Shares::new(Decimal::from(100)),
                ),
                BookLevel::from_decimal_unchecked(
                    Price::new(Decimal::new(50, 2)),
                    Shares::new(Decimal::from(100)),
                ),
            ]),
        };
        assert_eq!(
            decision_book_quote_price(&book),
            Some(Price::new(Decimal::new(55, 2)))
        );
    }
}

#[cfg(test)]
mod attribution_censor_tests {
    use std::collections::{HashMap, HashSet};

    use chrono::Utc;
    use quant_pivot_models::{
        domain::quant::RecommendationAttributionInfo,
        enums::quant::RecommendationAttributionOutcome,
        types::{AttributionDetail, DatasetCoverage, EntryOutcome, ExitOutcome, RecommendationId},
    };

    use super::{
        LiveAttributionMaterialization, materialize_live_attribution_example,
        recommendation_outcome_code, record_live_attribution_materialization,
    };

    fn superseded_attribution() -> RecommendationAttributionInfo {
        RecommendationAttributionInfo {
            recommendation_id: RecommendationId::from_v7(),
            outcome: RecommendationAttributionOutcome::SupersededUnfilled,
            entry_outcome_json: EntryOutcome::default(),
            exit_outcome_json: ExitOutcome::default(),
            realized_pnl_usd: None,
            max_adverse_excursion_bps: None,
            max_favorable_excursion_bps: None,
            label_available_at: Some(Utc::now()),
            attribution_json: AttributionDetail::default(),
            created_at: Utc::now(),
        }
    }

    #[test]
    fn superseded_unfilled_is_censored_from_training_dataset() {
        let attribution = superseded_attribution();

        assert!(matches!(
            materialize_live_attribution_example(
                &attribution,
                &HashMap::new(),
                &HashMap::new(),
                &HashMap::new(),
            ),
            LiveAttributionMaterialization::CensoredSupersededUnfilled
        ));
        assert_eq!(
            recommendation_outcome_code(RecommendationAttributionOutcome::SupersededUnfilled),
            None
        );
    }

    #[test]
    fn superseded_unfilled_has_dedicated_censor_accounting() {
        let mut coverage = DatasetCoverage {
            live_attribution_candidates: 1,
            ..DatasetCoverage::default()
        };
        let mut examples = Vec::new();
        let mut markets = HashSet::new();
        record_live_attribution_materialization(
            materialize_live_attribution_example(
                &superseded_attribution(),
                &HashMap::new(),
                &HashMap::new(),
                &HashMap::new(),
            ),
            &mut coverage,
            &mut examples,
            &mut markets,
        );

        assert_eq!(coverage.live_attribution_censored_superseded_unfilled, 1);
        assert_eq!(coverage.live_attribution_materialized, 0);
        assert_eq!(coverage.live_attribution_dropped_missing_evidence, 0);
        assert_eq!(coverage.planned_samples, 0);
        assert!(examples.is_empty());
        assert!(markets.is_empty());
    }
}

#[cfg(test)]
mod pit_fee_tests {
    use chrono::{DateTime, Duration, TimeZone, Utc};
    use quant_pivot_models::{
        enums::common::{MarketCategory, TickSize},
        hashing::CanonicalDigest,
        types::{
            ClobFeeDetails, ClobMarketInfoVersion, ClobMarketInfoVersionId, ClobTokenDescriptor,
            EventId, MarketId, Price, Shares, TokenId,
        },
    };
    use quant_pivot_research::selection::SelectedMarket;
    use rust_decimal::Decimal;
    use rust_decimal_macros::dec;

    use super::pit_exit_fee_schedule;

    fn version(
        market_id: &MarketId,
        token_id: &TokenId,
        rate: Decimal,
        effective_at: DateTime<Utc>,
        available_at: DateTime<Utc>,
    ) -> ClobMarketInfoVersion {
        let raw_payload = serde_json::json!({
            "market": market_id,
            "rate": rate,
            "effective_at": effective_at,
            "available_at": available_at,
        });
        ClobMarketInfoVersion {
            version_id: ClobMarketInfoVersionId::from_v7(),
            market_id: market_id.clone(),
            tokens: vec![
                ClobTokenDescriptor {
                    token_id: token_id.clone(),
                    outcome: "Yes".to_owned(),
                },
                ClobTokenDescriptor {
                    token_id: TokenId::new("fee-token-no"),
                    outcome: "No".to_owned(),
                },
            ],
            tick_size: TickSize::Hundredth,
            minimum_order_size: dec!(1),
            neg_risk: false,
            taker_order_delay_enabled: false,
            minimum_order_age_secs: None,
            blockaid_check_enabled: false,
            fee_details: ClobFeeDetails {
                rate,
                exponent: 1,
                taker_only: true,
            },
            builder_maker_fee_rate_bps: 0,
            builder_taker_fee_rate_bps: 0,
            effective_at,
            available_at,
            payload_hash: CanonicalDigest::content_hash_json(&raw_payload).expect("payload hash"),
            raw_payload,
        }
    }

    #[test]
    fn exit_fee_uses_only_the_pit_visible_market_info_version() {
        let decision_at = Utc.timestamp_opt(1_700_000_000, 0).single().expect("time");
        let cutoff = decision_at - Duration::seconds(10);
        let market_id = MarketId::new("fee-market");
        let token_id = TokenId::new("fee-token");
        let market = SelectedMarket {
            market_id: market_id.clone(),
            event_id: EventId::new("fee-event"),
            category: MarketCategory::Crypto,
            primary_token_id: token_id.clone(),
            secondary_token_id: None,
            liquidity_usd: None,
            volume_24h_usd: None,
            source_refs: Vec::new(),
        };
        let visible = version(
            &market_id,
            &token_id,
            dec!(0.02),
            decision_at - Duration::hours(1),
            cutoff,
        );
        let future_knowledge = version(
            &market_id,
            &token_id,
            dec!(0.09),
            decision_at - Duration::minutes(1),
            cutoff + Duration::seconds(1),
        );

        let schedule = pit_exit_fee_schedule(
            Shares::new(dec!(10)),
            Price::new(dec!(0.5)),
            &market,
            &token_id,
            decision_at,
            cutoff,
            &[visible, future_knowledge],
        )
        .expect("PIT fee schedule");

        assert_eq!(schedule.platform_rate, dec!(0.02));
        assert_eq!(schedule.available_at, cutoff);
    }
}
