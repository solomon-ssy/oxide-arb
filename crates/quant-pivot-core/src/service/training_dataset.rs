//! Offline training-dataset orchestration.
//!
//! Plans a deterministic sample grid, batch-prefetches every historical fact the
//! build needs (book snapshots, microstructure, market metadata, settlements),
//! serves point-in-time lookups from an in-memory
//! `MaterializedPitEngine` so the build loop issues zero DB queries, then runs
//! the **same** feature builder per `decision_at` cross-section the online path
//! uses. Factor-native families additionally execute their exact frozen factor
//! plane; classical families remain structurally feature-only. The service then
//! attaches forward-looking labels, asserts no future leakage, materializes a
//! content-hashed Parquet artifact, and records the ledger row. Features are
//! bounded by the source cutoffs frozen in each [`DecisionBoundary`]; labels
//! look strictly forward from `decision_at`; the dataset hash makes the whole
//! thing reproducible.

use std::{
    collections::{BTreeMap, BTreeSet, HashMap, HashSet},
    sync::Arc,
    time::Duration,
};

use async_trait::async_trait;
use chrono::{DateTime, Duration as ChronoDuration, Utc};
use quant_pivot_compute::{ComputeExecutor, OfflineMemory};
use quant_pivot_error::{QuantError, QuantResult, research::ResearchError, storage::StorageError};
use quant_pivot_models::{
    clickhouse::BookMicrostructureRow,
    domain::{
        data_plane::{DecisionBoundary, DecisionClock, DecisionSource},
        market::{MarketInfo, MarketRegistryInfo, fee::MarketFeeSchedule},
        quant::{
            CompleteTrainingDatasetBuild, ExitTrainingLotRow, JobProgressSink,
            NewTrainingDatasetPlan, NoopProgressSink, TrainingDatasetInfo,
            TrainingDatasetMaterialization,
        },
        query::TimeWindow,
    },
    enums::{
        common::MarketCategory,
        model::ModelFamily,
        quant::{DatasetPurpose, FeedbackCohort, TradePolicyStatus, TrainingDatasetStatus},
    },
    hashing::CanonicalDigest,
    runtime_config::{
        DataQualityConfig, DomainConfig, FactorsConfig, FeaturesConfig, SelectionConfig,
        TrainingConfig,
    },
    types::{
        ArtifactUri, ClobMarketInfoVersion, ContentHash, DATASET_ARTIFACT_FORMAT_VERSION,
        DatasetCohortManifest, DatasetCoverage, DatasetFeatureStateCounts, DatasetManifest,
        FeatureCellState, MarketId, ModelSpecId, ModelTrainingTarget, Price, ResearchJobProgress,
        ResearchProfileArtifact, SchemaVersion, Shares, TokenId, TradePolicyArtifactId,
        TradePolicyArtifactPayload, TrainingDatasetId, TrainingExampleId, TrainingHorizonsSecs,
        TrainingSampleSource, Usd, factor::FactorServingPlane, stable_name::FeatureName,
    },
};
use quant_pivot_repository::traits::{
    CalibrationArtifactRepository, CatalogLedgerRepository, ClobMarketInfoRepository,
    MarketLinkageRepository, MarketRepository, ModelRegistryRepository, PositionRepository,
    QuantFactReadRepository, TradePolicyRepository, TrainingDatasetRepository,
};
use quant_pivot_research::{
    artifact::{ArtifactKey, ArtifactNamespace, ArtifactStore},
    execution_semantics::BookFidelity,
    factors::{FactorEngine, FactorValue},
    features::ConfiguredFeatureBuilder,
    hashing::ResearchHasher,
    model::{
        FavoriteLongshotBiasTable,
        sell_scorer::{LotStateInput, PositionStateFeatures},
    },
    pit::PointInTimeSnapshotSource,
    selection::{ModelFeatureRequirements, SelectedMarket},
    training::{
        DatasetHashContract, DatasetParquetCodec, DatasetPlan, DatasetPlanRequest, DecisionBook,
        ExitDecisionLabelContext, ForwardWindow, HoldVsExitProceedsLabeler, LabelBuildInput,
        LabelBuildOutput, LabelName, Labeler, LiquidityExitLabeler, LotSamplePlan,
        LotTerminalSnapshot, LotTrainingContext, MaxAdverseExcursionLabeler,
        MaxFavorableExcursionLabeler, PlanMarket, ReturnToHorizonLabeler, SamplePlan,
        TokenPayoutRatioLabeler, TrainingDatasetArtifact, TrainingDatasetBuilder,
        TrainingDatasetPlanner, TrainingExample, TrainingLabel, assert_no_future_leakage,
        count_samples, dataset_source_fingerprint, label_names_for_sources,
        plan_lot_timeline_samples, plan_samples, probe_matrix_coverage, remaining_shares_at,
    },
};
use rust_decimal::{Decimal, prelude::ToPrimitive};
use tokio::runtime::Handle;
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
        calibration_shared::assert_dataset_disjoint,
        historical_replay::{
            CrossSectionRequest, ReplayCaptureKey, ReplayConfig, ReplayCrossSection,
            ReplayFactorMode, ReplayFactorOutput, ReplayTradeTapeSource, materialize_cross_section,
        },
        pit_selection::OfflinePitSelector,
    },
};

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
    /// Total samples across all requested sources (historical spine + exit),
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
        Box::new(TokenPayoutRatioLabeler),
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

/// Verify exact bytes, the embedded v3 manifest and semantic rows against the
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
    let actual_bytes_hash = CanonicalDigest::content_hash_bytes(bytes);
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
    let actual_manifest_hash = decoded
        .manifest
        .content_hash()
        .map_err(|detail| ResearchError::DatasetBuild { detail })?;
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
        && manifest.model_family == dataset.model_family
        && manifest.model_spec_definition_hash == dataset.model_spec_definition_hash
        && manifest.factor_serving_plane == dataset.factor_serving_plane
        && manifest.source_lineage == dataset.source_lineage
        && manifest.cohort_manifest == dataset.cohort_manifest
        && manifest.source_lineage.research_profile_artifact_id
            == dataset.research_profile_artifact_id
        && manifest.source_lineage.source_slice_id == dataset.source_slice_id
        && manifest.source_lineage.pit_cutoff == dataset.pit_cutoff
        && manifest.source_lineage.decision_policy_snapshot_id
            == dataset.decision_policy_snapshot_id
        && manifest.window_start == dataset.window_start
        && manifest.window_end == dataset.window_end
        && manifest.purpose == dataset.purpose
        && manifest.knowledge_lag_secs == knowledge_lag_secs
        && manifest.sample_interval_secs == sample_interval_secs
        && manifest.horizons_secs == dataset.horizons_secs.0
        && manifest.feature_schema_version == dataset.feature_schema_version
        && &manifest.feature_schema_hash == materialization.feature_schema_hash
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
    /// Process-wide offline CPU and memory governor.
    pub compute: Arc<ComputeExecutor>,
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
fn require_half_open_window(
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
    /// Governed online-serving denominator for decision-capture liquidity.
    pub liquidity_cap_usd: Usd,
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
    compute: Arc<ComputeExecutor>,
    fact_read: Arc<dyn QuantFactReadRepository>,
    catalog_repo: Arc<dyn CatalogLedgerRepository>,
    market_repo: Arc<dyn MarketRepository>,
    artifact_store: Arc<dyn ArtifactStore>,
    dataset_repo: Arc<dyn TrainingDatasetRepository>,
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
    liquidity_cap_usd: Usd,
    max_book_staleness: Duration,
    min_exit_depth_usd: Usd,
    /// Frozen selection policy (drives the offline point-in-time selection funnel).
    selection: SelectionConfig,
    /// Enabled-category set (derived from [`Self::selection`]) for the cheap
    /// upper-bound candidate prefilter.
    enabled_categories: HashSet<MarketCategory>,
    /// Deploy guard: hard cap on the deterministic historical spine.
    max_spine_samples: u64,
    /// Shared so the historical spine can be built inside the governed offline
    /// executor (labelers are `Send + Sync` but not `Clone`).
    labelers: Arc<Vec<Box<dyn Labeler>>>,
    /// Frozen favorite-longshot bias table bound to the offline factor engine.
    bias_table: Option<Arc<FavoriteLongshotBiasTable>>,
}

enum DatasetFactorEngine {
    FactorNative {
        engine: FactorEngine,
        category_scope: Option<MarketCategory>,
    },
    FeatureOnly {
        category_scope: Option<MarketCategory>,
    },
}

struct DatasetFactorContract {
    feature_schema_hash: ContentHash,
    factor_serving_plane: FactorServingPlane,
}

impl DatasetFactorEngine {
    fn try_new(
        model_family: ModelFamily,
        expected_plane: &FactorServingPlane,
        category_scope: Option<MarketCategory>,
        factors: &FactorsConfig,
        features: &FeaturesConfig,
        domain: &DomainConfig,
        bias_table: Option<Arc<FavoriteLongshotBiasTable>>,
    ) -> QuantResult<Self> {
        if model_family.is_classical() {
            let empty =
                FactorServingPlane::try_empty().map_err(|error| ResearchError::DatasetBuild {
                    detail: format!("seal classical factor-free plane: {error}"),
                })?;
            if expected_plane != &empty {
                return Err(ResearchError::DatasetBuild {
                    detail:
                        "classical dataset plan is not bound to the canonical empty factor plane"
                            .to_owned(),
                }
                .into());
            }
            return Ok(Self::FeatureOnly { category_scope });
        }
        let engine =
            FactorEngine::for_model_scope(factors, features, domain, category_scope, bias_table);
        if engine.registry().is_empty() {
            return Err(QuantError::config(
                "no factors enabled for a factor-native model family",
            ));
        }
        if engine.serving_plane()? != expected_plane {
            return Err(ResearchError::DatasetBuild {
                detail: "active factor engine does not reproduce the frozen dataset factor plane"
                    .to_owned(),
            }
            .into());
        }
        Ok(Self::FactorNative {
            engine,
            category_scope,
        })
    }

    const fn replay_mode(&self) -> ReplayFactorMode<'_> {
        match self {
            Self::FactorNative { engine, .. } => ReplayFactorMode::FactorNative { engine },
            Self::FeatureOnly { .. } => ReplayFactorMode::FeatureOnly,
        }
    }

    const fn category_scope(&self) -> Option<MarketCategory> {
        match self {
            Self::FactorNative { category_scope, .. } | Self::FeatureOnly { category_scope } => {
                *category_scope
            }
        }
    }
}

impl TrainingDatasetService {
    fn validate_feedback_request(
        request: &DatasetPlanRequest,
    ) -> QuantResult<&DatasetCohortManifest> {
        let cohort =
            request
                .cohort_manifest
                .as_ref()
                .ok_or_else(|| ResearchError::DatasetPlan {
                    detail: "feedback dataset requires an immutable cohort manifest".to_owned(),
                })?;
        cohort
            .validate()
            .map_err(|error| ResearchError::DatasetPlan {
                detail: error.to_string(),
            })?;
        if cohort.cohort != FeedbackCohort::ModelScoreLearning
            || request.sample_sources.as_slice() != [TrainingSampleSource::ModelScoreFeedback]
            || request.sample_interval_secs != 0
            || request.horizons_secs != [0]
            || request.training_dataset_id.is_none()
            || request.window_start != cohort.window.window_start()
            || request.window_end != cohort.window.cutoff()
            || request.source_lineage.pit_cutoff < cohort.window.cutoff()
        {
            return Err(ResearchError::DatasetPlan {
                detail: "feedback plan must bind one complete model-score cohort, a preassigned dataset id, event-driven cadence, and a label-availability cutoff no earlier than the cohort cutoff"
                    .to_owned(),
            }
            .into());
        }
        Ok(cohort)
    }

    /// Freeze a cohort-owned event-driven plan with no synthetic sample grid.
    pub async fn plan_feedback(
        &self,
        request: DatasetPlanRequest,
        factor_serving_plane: FactorServingPlane,
    ) -> QuantResult<DatasetPlan> {
        require_half_open_window(request.window_start, request.window_end)?;
        let cohort = Self::validate_feedback_request(&request)?;
        if cohort.counts.included_count() > self.max_spine_samples {
            return Err(ResearchError::DatasetPlan {
                detail: format!(
                    "feedback cohort has {} samples, exceeding the configured hard cap {}",
                    cohort.counts.included_count(),
                    self.max_spine_samples
                ),
            }
            .into());
        }
        request
            .source_lineage
            .validate()
            .map_err(|error| ResearchError::DatasetPlan {
                detail: error.to_string(),
            })?;
        let profile = Self::resolve_research_profile(&request)?;
        let (
            model_spec_definition_hash,
            model_family,
            training_target,
            trade_policy_artifact_id,
            trade_policy_hash,
            trade_policy,
        ) = self
            .resolve_trade_policy_binding(&request, &profile)
            .await?;
        if training_target != ModelTrainingTarget::OutcomePayout {
            return Err(ResearchError::DatasetPlan {
                detail: format!(
                    "model-score feedback requires outcome_payout target; model spec declares {training_target:?}"
                ),
            }
            .into());
        }
        let expected_contract = self.factor_contract_for(
            model_family,
            request.feature_schema_version,
            profile.spec.category,
        )?;
        if factor_serving_plane != expected_contract.factor_serving_plane {
            return Err(ResearchError::DatasetPlan {
                detail: "feedback factor plane differs from the exact ResearchProfile-scoped plane"
                    .to_owned(),
            }
            .into());
        }
        let training_dataset_id =
            request
                .training_dataset_id
                .ok_or_else(|| ResearchError::DatasetPlan {
                    detail: "feedback dataset id disappeared after validation".to_owned(),
                })?;
        Ok(DatasetPlan {
            request,
            training_dataset_id,
            model_spec_definition_hash,
            model_family,
            feature_schema_hash: expected_contract.feature_schema_hash,
            factor_serving_plane,
            samples: Vec::new(),
            lot_samples: Vec::new(),
            exit_training_lots: Vec::new(),
            label_names: vec![LabelName::new(training_target.label_name())],
            trade_policy_artifact_id,
            trade_policy_hash,
            trade_policy,
        })
    }

    /// Plan exclusively from a verified Source Slice. Dynamic repository reads
    /// for candidate discovery are structurally absent from this path.
    pub async fn plan_with_frozen_source(
        &self,
        request: DatasetPlanRequest,
        source: &FrozenSourceSlice,
    ) -> QuantResult<DatasetPlan> {
        require_half_open_window(request.window_start, request.window_end)?;
        request
            .source_lineage
            .verify_manifest(&source.manifest)
            .map_err(|error| ResearchError::DatasetPlan {
                detail: error.to_string(),
            })?;
        if request
            .sample_sources
            .as_slice()
            .iter()
            .any(|source| !matches!(source, TrainingSampleSource::HistoricalPit))
        {
            return Err(ResearchError::DatasetPlan {
                detail: "Source Slice V1 Dataset builds currently accept only historical_pit; exit_decision requires its complete immutable lot evidence graph".to_owned(),
            }
            .into());
        }
        let profile = Self::resolve_research_profile(&request)?;
        let (
            model_spec_definition_hash,
            model_family,
            _training_target,
            trade_policy_artifact_id,
            trade_policy_hash,
            trade_policy,
        ) = self
            .resolve_trade_policy_binding(&request, &profile)
            .await?;
        let factor_contract = self.factor_contract_for(
            model_family,
            request.feature_schema_version,
            profile.spec.category,
        )?;
        let plan_markets =
            self.frozen_plan_markets(&request, &source.prefetched, profile.spec.category)?;
        let samples = plan_samples(&request, &plan_markets)?;
        let training_dataset_id = request
            .training_dataset_id
            .unwrap_or_else(TrainingDatasetId::from_v7);
        let label_names = label_names_for_sources(
            request.sample_sources.as_slice(),
            trade_policy_artifact_id.is_some(),
        );
        Ok(DatasetPlan {
            request,
            training_dataset_id,
            model_spec_definition_hash,
            model_family,
            feature_schema_hash: factor_contract.feature_schema_hash,
            factor_serving_plane: factor_contract.factor_serving_plane,
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
        category_scope: Option<MarketCategory>,
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
                        DateTime::from_timestamp_millis(row.venue_event_time).is_some_and(|at| {
                            at < request.window_end
                                && at
                                    + ChronoDuration::from_std(self.max_book_staleness)
                                        .unwrap_or(ChronoDuration::MAX)
                                    >= request.window_start
                        })
                    })
                })
            });
            if !observed || !self.categories_selected(info.categories.iter(), category_scope) {
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
        let min_exit_depth_usd = config.training.min_exit_depth();
        let max_book_staleness = Duration::from_millis(config.training.max_book_staleness_ms);
        Ok(Self {
            compute: deps.compute,
            fact_read: deps.fact_read,
            catalog_repo: deps.catalog_repo,
            market_repo: deps.market_repo,
            artifact_store: deps.artifact_store,
            dataset_repo: deps.dataset_repo,
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
            liquidity_cap_usd: config.liquidity_cap_usd,
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
            .find_model_spec(model_spec_id)
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

    fn resolve_research_profile(
        request: &DatasetPlanRequest,
    ) -> QuantResult<ResearchProfileArtifact> {
        request
            .source_lineage
            .research_profile_artifact_id
            .profile_ref()
            .resolve_builtin_research_profile()
            .map_err(|detail| ResearchError::DatasetPlan {
                detail: format!("research profile preimage verification failed: {detail}"),
            })
            .map_err(Into::into)
    }

    async fn resolve_trade_policy_binding(
        &self,
        request: &DatasetPlanRequest,
        profile: &ResearchProfileArtifact,
    ) -> QuantResult<(
        ContentHash,
        ModelFamily,
        ModelTrainingTarget,
        Option<TradePolicyArtifactId>,
        Option<ContentHash>,
        Option<TradePolicyArtifactPayload>,
    )> {
        let spec = self
            .model_registry
            .find_model_spec(&request.model_spec_id)
            .await
            .map_err(QuantError::from)?
            .ok_or_else(|| StorageError::NotFound {
                entity: "quant_model_spec",
                id: request.model_spec_id.to_string(),
            })?;
        let prediction_horizon_secs =
            u64::try_from(spec.prediction_horizon_secs).map_err(|error| {
                ResearchError::DatasetPlan {
                    detail: format!(
                        "model spec {} prediction horizon is invalid: {error}",
                        spec.model_spec_id
                    ),
                }
            })?;
        if prediction_horizon_secs != profile.spec.target_horizon_secs {
            return Err(ResearchError::DatasetPlan {
                detail: format!(
                    "model spec {} prediction horizon {}s differs from ResearchProfile {}@{} target {}s",
                    spec.model_spec_id,
                    prediction_horizon_secs,
                    profile.profile_ref.id,
                    profile.profile_ref.version,
                    profile.spec.target_horizon_secs,
                ),
            }
            .into());
        }
        if request.purpose == DatasetPurpose::PolicyFit {
            return Ok((
                spec.definition_hash,
                spec.model_family,
                spec.training_contract.target,
                None,
                None,
                None,
            ));
        }
        spec.training_contract
            .validate_for(spec.model_family)
            .map_err(|detail| ResearchError::DatasetPlan { detail })?;
        let Some(artifact_id) = spec.training_contract.evaluation_trade_policy_artifact_id else {
            return Ok((
                spec.definition_hash,
                spec.model_family,
                spec.training_contract.target,
                None,
                None,
                None,
            ));
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
            spec.model_family,
            spec.training_contract.target,
            Some(artifact.artifact_id),
            Some(artifact.content_hash),
            Some(artifact.payload_json),
        ))
    }

    fn factor_contract_for(
        &self,
        model_family: ModelFamily,
        feature_schema_version: SchemaVersion,
        category_scope: Option<MarketCategory>,
    ) -> QuantResult<DatasetFactorContract> {
        let builder = ConfiguredFeatureBuilder::new(&self.features, &self.domain)?;
        if builder.schema().version() != feature_schema_version {
            return Err(ResearchError::DatasetPlan {
                detail: format!(
                    "requested feature schema version {feature_schema_version} does not match active profile schema {}",
                    builder.schema().version()
                ),
            }
            .into());
        }
        let feature_schema_hash = ResearchHasher::feature_schema(builder.schema())?;
        if model_family.is_classical() {
            let factor_serving_plane =
                FactorServingPlane::try_empty().map_err(|error| ResearchError::DatasetPlan {
                    detail: format!("seal classical factor-free plane: {error}"),
                })?;
            return Ok(DatasetFactorContract {
                feature_schema_hash,
                factor_serving_plane,
            });
        }
        let engine = FactorEngine::for_model_scope(
            &self.factors,
            &self.features,
            &self.domain,
            category_scope,
            self.bias_table.as_ref().map(Arc::clone),
        );
        if engine.registry().is_empty() {
            return Err(QuantError::config(
                "no factors enabled for a factor-native model family",
            ));
        }
        Ok(DatasetFactorContract {
            feature_schema_hash,
            factor_serving_plane: engine.serving_plane()?.clone(),
        })
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
        category_scope: Option<MarketCategory>,
    ) -> bool {
        if info.created_at >= window_end {
            return false;
        }
        if info.end_date.is_some_and(|end| end <= window_start) {
            return false;
        }
        self.categories_selected(info.categories.iter().copied(), category_scope)
    }

    fn categories_selected<I>(&self, categories: I, category_scope: Option<MarketCategory>) -> bool
    where
        I: IntoIterator<Item = MarketCategory>,
    {
        match category_scope {
            Some(category) => categories
                .into_iter()
                .any(|candidate| candidate == category),
            None if self.enabled_categories.is_empty() => true,
            None => categories
                .into_iter()
                .any(|category| self.enabled_categories.contains(&category)),
        }
    }

    /// The deterministic candidate selection for a plan window.
    fn candidate_plan_markets(
        &self,
        markets: &[MarketInfo],
        window_start: DateTime<Utc>,
        window_end: DateTime<Utc>,
        category_scope: Option<MarketCategory>,
    ) -> Vec<PlanMarket> {
        markets
            .iter()
            .filter(|info| self.in_selection(info, window_start, window_end, category_scope))
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
    /// adds bounded exit-decision counts, so a plan over the
    /// full catalog returns in milliseconds instead of allocating millions of rows.
    pub async fn count_plan(
        &self,
        request: &DatasetPlanRequest,
        sample_slices: u32,
        sample_markets: u32,
    ) -> QuantResult<PlanCounts> {
        require_half_open_window(request.window_start, request.window_end)?;
        let profile = Self::resolve_research_profile(request)?;
        let mut total: u64 = 0;
        let wants_historical = request
            .sample_sources
            .as_slice()
            .contains(&TrainingSampleSource::HistoricalPit);
        // Candidate `MarketInfo` set (category + lifetime), reused for both the
        // arithmetic spine upper bound and the sampled keep-rate estimate. Sourced
        // from ClickHouse-observed markets so since-resolved markets are included.
        let candidate_markets = if wants_historical {
            self.historical_candidate_markets(request.window_start, request.window_end)
                .await?
                .into_iter()
                .filter(|info| {
                    self.in_selection(
                        info,
                        request.window_start,
                        request.window_end,
                        profile.spec.category,
                    )
                })
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
        if request
            .sample_sources
            .as_slice()
            .contains(&TrainingSampleSource::ExitDecision)
        {
            let mut lots = self
                .position_repo
                .find_exit_training_lots(
                    request.window_start,
                    request.window_end,
                    self.max_spine_samples,
                )
                .await
                .map_err(QuantError::from)?;
            let profile = Self::resolve_research_profile(request)?;
            let market_ids = lots
                .iter()
                .map(|lot| lot.market_id.clone())
                .collect::<HashSet<_>>();
            let eligible_markets = self
                .market_repo
                .find_by_ids(&market_ids.into_iter().collect::<Vec<_>>())
                .await
                .map_err(QuantError::from)?
                .into_iter()
                .filter(|market| {
                    self.categories_selected(
                        market.categories.iter().copied(),
                        profile.spec.category,
                    )
                })
                .map(|market| market.market_id.clone())
                .collect::<HashSet<_>>();
            lots.retain(|lot| eligible_markets.contains(&lot.market_id));
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
        let selector = self.offline_pit_selector(request, model_requirements)?;
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
        require_half_open_window(request.window_start, request.window_end)?;
        let profile = Self::resolve_research_profile(&request)?;
        let (
            model_spec_definition_hash,
            model_family,
            _training_target,
            trade_policy_artifact_id,
            trade_policy_hash,
            trade_policy,
        ) = self
            .resolve_trade_policy_binding(&request, &profile)
            .await?;
        let factor_contract = self.factor_contract_for(
            model_family,
            request.feature_schema_version,
            profile.spec.category,
        )?;
        // Point-in-time candidate selection: markets observed (had a book) in the
        // window whose fee-dominant category is in the enabled set (mirrors the
        // online [`CategoryFilter`]; per-`as_of` liquidity/data-quality eligibility
        // is enforced during materialization). Sourced from ClickHouse facts so
        // since-resolved markets are not survivorship-filtered out.
        let markets = self
            .historical_candidate_markets(request.window_start, request.window_end)
            .await?;
        let plan_markets = self.candidate_plan_markets(
            &markets,
            request.window_start,
            request.window_end,
            profile.spec.category,
        );
        let samples = plan_samples(&request, &plan_markets)?;
        let mut lot_samples = Vec::new();
        let mut exit_training_lots = Vec::new();
        if request
            .sample_sources
            .as_slice()
            .contains(&TrainingSampleSource::ExitDecision)
        {
            exit_training_lots = self
                .position_repo
                .find_exit_training_lots(
                    request.window_start,
                    request.window_end,
                    self.max_spine_samples,
                )
                .await
                .map_err(QuantError::from)?;
            let eligible_markets = plan_markets
                .iter()
                .map(|market| &market.market_id)
                .collect::<HashSet<_>>();
            exit_training_lots.retain(|lot| eligible_markets.contains(&lot.market_id));
            lot_samples = plan_lot_timeline_samples(
                request.sample_interval_secs,
                request.window_start,
                &exit_training_lots,
            )?;
        }
        let training_dataset_id = request
            .training_dataset_id
            .unwrap_or_else(TrainingDatasetId::from_v7);
        let label_names = label_names_for_sources(
            request.sample_sources.as_slice(),
            trade_policy_artifact_id.is_some(),
        );
        Ok(DatasetPlan {
            request,
            training_dataset_id,
            model_spec_definition_hash,
            model_family,
            feature_schema_hash: factor_contract.feature_schema_hash,
            factor_serving_plane: factor_contract.factor_serving_plane,
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
    pub fn count_planned_samples(&self, plan: &DatasetPlan) -> QuantResult<u64> {
        let mut total = planned_historical_samples(plan);
        if plan
            .request
            .sample_sources
            .as_slice()
            .contains(&TrainingSampleSource::ExitDecision)
        {
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
    /// Materialize only the exact rows admitted by a frozen feedback cohort.
    pub async fn build_feedback(
        &self,
        plan: DatasetPlan,
        examples: Vec<TrainingExample>,
        coverage: DatasetCoverage,
    ) -> QuantResult<TrainingDatasetArtifact> {
        if plan
            .request
            .cohort_manifest
            .as_ref()
            .map(|manifest| manifest.cohort)
            != Some(FeedbackCohort::ModelScoreLearning)
            || plan.request.sample_sources.as_slice() != [TrainingSampleSource::ModelScoreFeedback]
            || plan.request.sample_interval_secs != 0
            || !plan.samples.is_empty()
            || !plan.lot_samples.is_empty()
        {
            return Err(ResearchError::DatasetBuild {
                detail: "feedback build received a non-cohort plan or a synthetic sample grid"
                    .to_owned(),
            }
            .into());
        }
        if let Some(existing) = self.load_completed_feedback(&plan).await? {
            return Ok(existing);
        }
        self.prepare_build_ledger(&plan).await?;
        let training_dataset_id = plan.training_dataset_id;
        let result = async {
            let builder = ConfiguredFeatureBuilder::new(&self.features, &self.domain)?;
            if builder.schema().version() != plan.request.feature_schema_version {
                return Err(ResearchError::DatasetBuild {
                    detail: format!(
                        "feedback feature schema version {} does not match active profile schema {}",
                        plan.request.feature_schema_version,
                        builder.schema().version()
                    ),
                }
                .into());
            }
            self.finalize_dataset(&builder, plan, examples, coverage)
                .await
        }
        .await;
        self.persist_build_failure(&training_dataset_id, result)
            .await
    }

    async fn load_completed_feedback(
        &self,
        plan: &DatasetPlan,
    ) -> QuantResult<Option<TrainingDatasetArtifact>> {
        let Some(dataset) = self
            .dataset_repo
            .find_by_id(&plan.training_dataset_id)
            .await?
        else {
            return Ok(None);
        };
        if !matches!(
            dataset.status,
            TrainingDatasetStatus::Ready | TrainingDatasetStatus::InsufficientLabels
        ) {
            return Ok(None);
        }
        let plan_matches = dataset.model_spec_id == plan.request.model_spec_id
            && dataset.model_family == plan.model_family
            && dataset.model_spec_definition_hash == plan.model_spec_definition_hash
            && dataset.factor_serving_plane == plan.factor_serving_plane
            && dataset.feature_schema_hash == plan.feature_schema_hash
            && dataset.factor_schema_hash == plan.factor_serving_plane.factor_schema_hash()
            && dataset.source_lineage == plan.request.source_lineage
            && dataset.cohort_manifest == plan.request.cohort_manifest
            && dataset.window_start == plan.request.window_start
            && dataset.window_end == plan.request.window_end
            && dataset.purpose == plan.request.purpose
            && dataset.knowledge_lag_secs
                == i64::try_from(plan.request.knowledge_lag_secs).map_err(|error| {
                    ResearchError::DatasetBuild {
                        detail: format!(
                            "feedback knowledge lag exceeds PostgreSQL bigint: {error}"
                        ),
                    }
                })?
            && dataset.sample_interval_secs == 0
            && dataset.horizons_secs.0 == plan.request.horizons_secs
            && dataset.feature_schema_version == plan.request.feature_schema_version
            && dataset.sample_sources.as_ref() == Some(&plan.request.sample_sources);
        if !plan_matches {
            return Err(StorageError::state_conflict(
                "quant_training_dataset",
                Some(&plan.training_dataset_id),
                "completed feedback dataset id is bound to a different frozen plan",
            )
            .into());
        }
        let materialization = require_dataset_materialization(&dataset)?;
        let bytes = self.artifact_store.get(materialization.parquet_uri).await?;
        let examples = verify_frozen_dataset_artifact(&dataset, &bytes)?;
        Ok(Some(TrainingDatasetArtifact {
            format_version: DATASET_ARTIFACT_FORMAT_VERSION,
            training_dataset_id: dataset.training_dataset_id,
            model_spec_id: dataset.model_spec_id,
            window_start: dataset.window_start,
            window_end: dataset.window_end,
            examples,
            feature_schema_hash: *materialization.feature_schema_hash,
            label_schema_hash: *materialization.label_schema_hash,
            dataset_hash: *materialization.dataset_hash,
            manifest: materialization.manifest.clone(),
            artifact_bytes_hash: *materialization.artifact_bytes_hash,
            parquet_uri: materialization.parquet_uri.clone(),
            coverage: materialization.coverage.clone(),
        }))
    }

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
        let training_dataset_id = plan.training_dataset_id;
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
            Ok(window) => Box::pin(self.materialize_window(plan, sink, cancel, window)).await,
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
        let training_dataset_id = plan.training_dataset_id;
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
        Box::pin(self.materialize_window(plan, sink, cancel, window)).await
    }

    async fn materialize_window(
        &self,
        plan: DatasetPlan,
        sink: Arc<dyn JobProgressSink>,
        cancel: CancellationToken,
        window: HistoricalWindow,
    ) -> QuantResult<TrainingDatasetArtifact> {
        let context = ReplayContext::new(&plan, &self.features)?;
        let coverage = DatasetCoverage {
            planned_samples: planned_historical_samples(&plan),
            book_decode_failures: window.book_decode_failures,
            ..DatasetCoverage::default()
        };
        let pit: Arc<dyn PointInTimeSnapshotSource> = Arc::new(window.pit);
        let prefetched = Arc::new(window.prefetched);

        // Offload the unbounded historical PIT loop to the offline pool so it
        // never occupies an async runtime worker (CPU-bound in-memory scoring
        // that would otherwise starve other jobs' heartbeats / lease renewals),
        // polling `cancel` at each cross-section boundary for a ~one-section
        // cooperative cancel latency.
        let mut spine = HistoricalSpine::default();
        let mut coverage = coverage;
        if plan
            .request
            .sample_sources
            .as_slice()
            .contains(&TrainingSampleSource::HistoricalPit)
        {
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
            let cancellation = inputs.cancel.clone();
            let runtime = Handle::current();
            let output = self
                .compute
                .run_offline_cancellable(OfflineMemory::try_gib(10)?, &cancellation, move || {
                    run_historical_spine_blocking(&runtime, inputs)
                })
                .await?;
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
                clob_market_info: &prefetched.clob_market_info,
                context: &context,
                sink: &*sink,
            },
            coverage,
            spine,
        )
        .await
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
                    training_dataset_id: plan.training_dataset_id,
                    model_spec_id: plan.request.model_spec_id,
                    model_family: plan.model_family,
                    model_spec_definition_hash: plan.model_spec_definition_hash,
                    factor_serving_plane: plan.factor_serving_plane.clone(),
                    feature_schema_hash: plan.feature_schema_hash,
                    factor_schema_hash: plan.factor_serving_plane.factor_schema_hash(),
                    research_profile_artifact_id: plan
                        .request
                        .source_lineage
                        .research_profile_artifact_id
                        .clone(),
                    source_slice_id: plan.request.source_lineage.source_slice_id,
                    pit_cutoff: plan.request.source_lineage.pit_cutoff,
                    source_lineage: plan.request.source_lineage.clone(),
                    feedback_cohort: plan
                        .request
                        .cohort_manifest
                        .as_ref()
                        .map(|manifest| manifest.cohort),
                    cohort_manifest: plan.request.cohort_manifest.clone(),
                    window_start: plan.request.window_start,
                    window_end: plan.request.window_end,
                    purpose: plan.request.purpose,
                    knowledge_lag_secs,
                    sample_interval_secs,
                    horizons_secs: TrainingHorizonsSecs(plan.request.horizons_secs.clone()),
                    feature_schema_version: plan.request.feature_schema_version,
                    sample_sources: Some(plan.request.sample_sources.clone()),
                    decision_policy_snapshot_id: plan
                        .request
                        .source_lineage
                        .decision_policy_snapshot_id,
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
            && row.model_family == plan.model_family
            && row.model_spec_definition_hash == plan.model_spec_definition_hash
            && row.factor_serving_plane == plan.factor_serving_plane
            && row.feature_schema_hash == plan.feature_schema_hash
            && row.factor_schema_hash == plan.factor_serving_plane.factor_schema_hash()
            && row.research_profile_artifact_id
                == plan.request.source_lineage.research_profile_artifact_id
            && row.source_slice_id == plan.request.source_lineage.source_slice_id
            && row.pit_cutoff == plan.request.source_lineage.pit_cutoff
            && row.source_lineage == plan.request.source_lineage
            && row.feedback_cohort
                == plan
                    .request
                    .cohort_manifest
                    .as_ref()
                    .map(|manifest| manifest.cohort)
            && row.cohort_manifest == plan.request.cohort_manifest
            && row.decision_policy_snapshot_id
                == plan.request.source_lineage.decision_policy_snapshot_id
            && row.window_start == plan.request.window_start
            && row.window_end == plan.request.window_end
            && row.purpose == plan.request.purpose
            && row.knowledge_lag_secs == knowledge_lag_secs
            && row.sample_interval_secs == sample_interval_secs
            && row.horizons_secs.0 == plan.request.horizons_secs
            && row.feature_schema_version == plan.request.feature_schema_version
            && row.sample_sources.as_ref() == Some(&plan.request.sample_sources);
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
    /// be moved into the offline executor) — acceptable for the leakage-probe tests.
    #[doc(hidden)]
    pub async fn build_with_pit_source(
        &self,
        plan: DatasetPlan,
        pit: &dyn PointInTimeSnapshotSource,
    ) -> QuantResult<TrainingDatasetArtifact> {
        self.prepare_build_ledger(&plan).await?;
        let training_dataset_id = plan.training_dataset_id;
        let result = Box::pin(self.materialize_with_pit_source(plan, pit)).await;
        self.persist_build_failure(&training_dataset_id, result)
            .await
    }

    async fn materialize_with_pit_source(
        &self,
        plan: DatasetPlan,
        pit: &dyn PointInTimeSnapshotSource,
    ) -> QuantResult<TrainingDatasetArtifact> {
        let context = ReplayContext::new(&plan, &self.features)?;
        let loader = self.window_loader();
        let prefetched = loader
            .prefetch(&context.window_spec(&plan, &self.domain))
            .await?;
        let mut coverage = DatasetCoverage {
            planned_samples: planned_historical_samples(&plan),
            ..DatasetCoverage::default()
        };
        let cancel = CancellationToken::new();
        let mut spine = HistoricalSpine::default();
        if plan
            .request
            .sample_sources
            .as_slice()
            .contains(&TrainingSampleSource::HistoricalPit)
        {
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
                clob_market_info: &prefetched.clob_market_info,
                context: &context,
                sink: &NoopProgressSink,
            },
            coverage,
            spine,
        )
        .await
    }

    /// Owned inputs moved into the governed offline historical spine.
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
            model_family: plan.model_family,
            factor_serving_plane: plan.factor_serving_plane.clone(),
            features: self.features.clone(),
            factors: self.factors.clone(),
            data_quality: self.data_quality.clone(),
            liquidity_cap_usd: self.liquidity_cap_usd,
            domain: self.domain.clone(),
            selection: self.selection.clone(),
            model_requirements,
            labelers: Arc::clone(&self.labelers),
            min_exit_depth_usd: self.min_exit_depth_usd,
            bias_table: if plan.model_family.is_classical() {
                None
            } else {
                self.bias_table.as_ref().map(Arc::clone)
            },
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
            model_family: plan.model_family,
            factor_serving_plane: &plan.factor_serving_plane,
            features: &self.features,
            factors: &self.factors,
            data_quality: &self.data_quality,
            liquidity_cap_usd: self.liquidity_cap_usd,
            domain: &self.domain,
            selection: &self.selection,
            model_requirements,
            labelers: &self.labelers,
            min_exit_depth_usd: self.min_exit_depth_usd,
            bias_table: if plan.model_family.is_classical() {
                None
            } else {
                self.bias_table.as_ref().map(Arc::clone)
            },
            context: ReplayContext::new(plan, &self.features)?,
        })
    }

    /// Append exit-decision samples, then assert leakage-freedom, materialize
    /// the Parquet artifact, and persist the ledger row.
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
        let category_scope = Self::resolve_research_profile(&plan.request)?.spec.category;
        let factor_engine = DatasetFactorEngine::try_new(
            plan.model_family,
            &plan.factor_serving_plane,
            category_scope,
            &self.factors,
            &self.features,
            &self.domain,
            if plan.model_family.is_classical() {
                None
            } else {
                self.bias_table.as_ref().map(Arc::clone)
            },
        );
        let factor_engine = factor_engine?;

        if plan
            .request
            .sample_sources
            .as_slice()
            .contains(&TrainingSampleSource::ExitDecision)
        {
            let required_features = self
                .resolve_model_requirements(&plan.request.model_spec_id)
                .await?
                .union_all();
            coverage.exit_decision_candidates = plan.lot_samples.len() as u64;
            coverage.planned_samples += plan.lot_samples.len() as u64;
            self.append_exit_decision_examples(
                ExitDecisionAppendInput {
                    plan: &plan,
                    pit,
                    prefetched,
                    clob_market_info,
                    context,
                    required_features: &required_features,
                },
                &factor_engine,
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
        self.finalize_dataset(&builder, plan, spine.examples, coverage)
            .await
    }

    /// Assemble the historical-window loader from the frozen staleness bound.
    fn window_loader(&self) -> HistoricalWindowLoader {
        HistoricalWindowLoader::new(
            Arc::clone(&self.fact_read),
            Arc::clone(&self.catalog_repo),
            Arc::clone(&self.clob_market_info_repo),
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
    ) -> QuantResult<OfflinePitSelector> {
        let category_scope = Self::resolve_research_profile(request)?.spec.category;
        Ok(OfflinePitSelector::new(
            &self.selection,
            &self.data_quality,
            &self.features,
            request.source_lineage.decision_policy_snapshot_id,
            request.knowledge_lag_secs,
            model_requirements,
            category_scope,
        ))
    }

    /// The frozen replay config (feature/factor/domain/data-quality) for this build.
    fn replay_config(&self, model_family: ModelFamily) -> ReplayConfig {
        ReplayConfig {
            features: self.features.clone(),
            factors: self.factors.clone(),
            domain: self.domain.clone(),
            data_quality: self.data_quality.clone(),
            liquidity_cap_usd: self.liquidity_cap_usd,
            bias_table: if model_family.is_classical() {
                None
            } else {
                self.bias_table.as_ref().map(Arc::clone)
            },
        }
    }

    async fn append_exit_decision_examples(
        &self,
        input: ExitDecisionAppendInput<'_>,
        factor_engine: &DatasetFactorEngine,
        sink: &mut ExampleBuildSink<'_>,
    ) -> QuantResult<()> {
        let lot_by_intent: HashMap<_, _> = input
            .plan
            .exit_training_lots
            .iter()
            .map(|lot| (lot.order_intent_id, lot))
            .collect();
        let exit_labelers = exit_decision_labelers();
        let builder = ConfiguredFeatureBuilder::new(&self.features, &self.domain)?;
        let replay_config = self.replay_config(input.plan.model_family);

        for (as_of, group) in group_lot_samples(&input.plan.lot_samples) {
            let Some(cross_section) = materialize_lot_cross_section(LotCrossSectionMaterialize {
                builder: &builder,
                factor_mode: factor_engine.replay_mode(),
                category_scope: factor_engine.category_scope(),
                replay_config: &replay_config,
                pit: input.pit,
                prefetched: input.prefetched,
                trade_tape_available_by: input.plan.request.source_lineage.pit_cutoff,
                as_of,
                group: &group,
                context: input.context,
                required_features: input.required_features,
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
                let Some(index) = lot_cross_section_index(&cross_section, sample)? else {
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
        let factor_values =
            factor_values_at(&input.cross_section.factor_output, input.market_index)?;
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
        if evidence.book_fidelity != Some(BookFidelity::FullL2) {
            record_exit_fill_fidelity(sink.coverage, evidence.book_fidelity);
            sink.coverage.samples_dropped_insufficient += 1;
            return Ok(());
        }
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
            .get(&ReplayCaptureKey::new(
                &input.sample.market_id,
                &input.sample.token_id,
            ))
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
                order_intent_id: input.sample.order_intent_id,
                position_id: input.sample.position_id,
                outcome_side: input.sample.outcome_side,
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
        let position_state = LotStateInput {
            avg_price: input.lot.avg_price.inner(),
            mark: entry_mid.map(Price::inner),
            opened_at: input.lot.opened_at,
            now: input.sample.decision_at,
            max_hold_secs: input.lot.max_hold_secs,
            peak_mark: peak_mark.map(Price::inner),
        }
        .position_state_features()?;
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
    async fn finalize_dataset(
        &self,
        builder: &ConfiguredFeatureBuilder,
        plan: DatasetPlan,
        examples: Vec<TrainingExample>,
        coverage: DatasetCoverage,
    ) -> QuantResult<TrainingDatasetArtifact> {
        self.validate_finalization_input(&plan, &examples).await?;

        let feature_schema_hash = ResearchHasher::feature_schema(builder.schema())?;
        if feature_schema_hash != plan.feature_schema_hash
            || plan
                .factor_serving_plane
                .definitions()
                .iter()
                .any(|definition| {
                    definition.feature_contract_hash() != feature_schema_hash
                        || definition.input_schema_version() != plan.request.feature_schema_version
                })
        {
            return Err(ResearchError::DatasetBuild {
                detail: "frozen factor plane does not bind the materialized feature contract"
                    .to_owned(),
            }
            .into());
        }
        let label_schema_hash = ResearchHasher::label_schema(&plan.label_names)?;
        let coverage = self
            .complete_integrity_coverage(
                &plan.request.model_spec_id,
                plan.model_family,
                &examples,
                builder,
                coverage,
            )
            .await?;
        let integrity = dataset_integrity_outcome(&coverage)?;
        let dataset_hash = TrainingDatasetArtifact::compute_dataset_hash(
            DatasetHashContract {
                model_spec_id: &plan.request.model_spec_id,
                model_family: plan.model_family,
                window_start: plan.request.window_start,
                window_end: plan.request.window_end,
                purpose: plan.request.purpose,
                feature_schema_hash: &feature_schema_hash,
                factor_serving_plane: &plan.factor_serving_plane,
                label_schema_hash: &label_schema_hash,
            },
            &examples,
        )?;

        let persisted = self
            .persist_dataset_artifact(
                &plan,
                &examples,
                DatasetSchemaHashes {
                    feature: feature_schema_hash,
                    label: label_schema_hash,
                },
                dataset_hash,
            )
            .await?;

        let completion = CompleteTrainingDatasetBuild::try_new(
            integrity.status,
            persisted.manifest.clone(),
            persisted.artifact_bytes_hash,
            persisted.parquet_uri.clone(),
            coverage.clone(),
            integrity.failure_detail,
        )
        .map_err(|error| ResearchError::DatasetBuild {
            detail: error.to_string(),
        })?;
        self.dataset_repo
            .complete_build(&plan.training_dataset_id, completion)
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
            assert_dataset_disjoint(
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
        model_family: ModelFamily,
        examples: &[TrainingExample],
        builder: &ConfiguredFeatureBuilder,
        coverage: DatasetCoverage,
    ) -> QuantResult<DatasetCoverage> {
        let model_spec = self
            .model_registry
            .find_model_spec(model_spec_id)
            .await
            .map_err(QuantError::from)?
            .ok_or_else(|| StorageError::NotFound {
                entity: "quant_model_spec",
                id: model_spec_id.to_string(),
            })?;
        if model_spec.model_family != model_family {
            return Err(ResearchError::DatasetBuild {
                detail: format!(
                    "frozen dataset family {model_family} no longer matches model spec family {}",
                    model_spec.model_family
                ),
            }
            .into());
        }
        model_spec
            .training_contract
            .validate_for(model_family)
            .map_err(|detail| ResearchError::DatasetBuild {
                detail: format!(
                    "model spec {} has invalid training contract: {detail}",
                    model_spec.model_spec_id
                ),
            })?;
        let target_label =
            LabelName::new(model_spec.training_contract.target.label_name().to_owned());
        let mut coverage = coverage;
        coverage.bias_table_hash = if model_family.is_classical() {
            None
        } else {
            self.bias_table.as_ref().map(|table| table.content_hash)
        };
        coverage.feature_state_counts = dataset_feature_state_counts(examples);
        coverage.matrix_probe = Some(probe_matrix_coverage(
            examples,
            builder.schema(),
            &model_spec.input_contract,
            &target_label,
            model_spec.training_contract.target.label_horizon_secs(),
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
            training_dataset_id: plan.training_dataset_id,
            source_lineage: plan.request.source_lineage.clone(),
            cohort_manifest: plan.request.cohort_manifest.clone(),
            model_spec_id: plan.request.model_spec_id,
            model_family: plan.model_family,
            model_spec_definition_hash: plan.model_spec_definition_hash,
            trade_policy_artifact_id: plan.trade_policy_artifact_id,
            trade_policy_hash: plan.trade_policy_hash,
            window_start: plan.request.window_start,
            window_end: plan.request.window_end,
            purpose: plan.request.purpose,
            knowledge_lag_secs: plan.request.knowledge_lag_secs,
            sample_interval_secs: plan.request.sample_interval_secs,
            horizons_secs: plan.request.horizons_secs.clone(),
            feature_schema_version: plan.request.feature_schema_version,
            feature_schema_hash: schema_hashes.feature,
            factor_serving_plane: plan.factor_serving_plane.clone(),
            label_schema_hash: schema_hashes.label,
            semantic_dataset_hash: dataset_hash,
            source_fingerprint: dataset_source_fingerprint(examples)?,
            sample_count,
        };
        let manifest_hash = manifest
            .content_hash()
            .map_err(|detail| ResearchError::DatasetBuild { detail })?;
        let parquet_bytes = DatasetParquetCodec::encode(examples, &manifest)?;
        let artifact_bytes_hash = CanonicalDigest::content_hash_bytes(&parquet_bytes);
        let key = ArtifactKey::new(
            ArtifactNamespace::Dataset,
            plan.training_dataset_id.as_uuid().to_string(),
            "parquet",
        )?;
        let parquet_uri = self.artifact_store.put(key, &parquet_bytes).await?;
        let persisted_bytes = self.artifact_store.get(&parquet_uri).await?;
        let persisted_hash = CanonicalDigest::content_hash_bytes(&persisted_bytes);
        if persisted_hash != artifact_bytes_hash {
            return Err(ResearchError::DatasetBuild {
                detail: format!(
                    "dataset artifact byte hash changed after persistence: encoded {artifact_bytes_hash}, persisted {persisted_hash}"
                ),
            }
            .into());
        }
        let decoded = DatasetParquetCodec::decode_with_manifest(&persisted_bytes)?;
        let decoded_manifest_hash = decoded
            .manifest
            .content_hash()
            .map_err(|detail| ResearchError::DatasetBuild { detail })?;
        if decoded.manifest != manifest || decoded_manifest_hash != manifest_hash {
            return Err(ResearchError::DatasetBuild {
                detail: "dataset manifest changed during Parquet persistence".to_owned(),
            }
            .into());
        }
        Ok(PersistedDatasetArtifact {
            manifest,
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
            failure_detail: Some(format!(
                "deterministic integrity gate found no mature target labels: target={}/{}, built_examples={}, labels_not_mature={}, labels_unavailable={}",
                probe.label_name,
                probe.label_horizon_secs,
                coverage.built_examples,
                coverage.labels_not_mature,
                coverage.labels_unavailable,
            )),
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
    label: ContentHash,
}

struct PersistedDatasetArtifact {
    manifest: DatasetManifest,
    artifact_bytes_hash: ContentHash,
    parquet_uri: ArtifactUri,
}

/// Build every label (labeler × horizon) for one example, accounting coverage.
/// Free function so the historical spine can call it from the governed offline
/// executor (no `&self` borrow).
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
    labels.sort_unstable_by(|left, right| {
        left.label_name
            .cmp(&right.label_name)
            .then_with(|| left.horizon_secs.cmp(&right.horizon_secs))
    });
    Ok(labels)
}

/// Accumulated examples + distinct markets from the historical spine, merged
/// with exit-decision samples before finalization.
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
    model_family: ModelFamily,
    factor_serving_plane: &'a FactorServingPlane,
    features: &'a FeaturesConfig,
    factors: &'a FactorsConfig,
    data_quality: &'a DataQualityConfig,
    liquidity_cap_usd: Usd,
    domain: &'a DomainConfig,
    selection: &'a SelectionConfig,
    /// Target `ModelSpec`'s resolved feature requirements.
    model_requirements: ModelFeatureRequirements,
    labelers: &'a [Box<dyn Labeler>],
    min_exit_depth_usd: Usd,
    /// Frozen favorite-longshot bias table bound to the spine factor engine.
    bias_table: Option<Arc<FavoriteLongshotBiasTable>>,
    context: ReplayContext,
}

/// Owned inputs for the historical PIT loop, moved into the offline executor
/// (every field is `Send + 'static`; `Arc` shares the PIT engine,
/// prefetched facts, progress sink, and labelers with the async parent).
struct HistoricalSpineInputs {
    pit: Arc<dyn PointInTimeSnapshotSource>,
    prefetched: Arc<Prefetched>,
    sink: Arc<dyn JobProgressSink>,
    cancel: CancellationToken,
    samples: Vec<SamplePlan>,
    request: DatasetPlanRequest,
    model_family: ModelFamily,
    factor_serving_plane: FactorServingPlane,
    features: FeaturesConfig,
    factors: FactorsConfig,
    data_quality: DataQualityConfig,
    liquidity_cap_usd: Usd,
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

/// Run the historical PIT spine on an offline Rayon worker.
///
/// The per-section materialization is in-memory (the `MaterializedPitEngine`
/// resolves books/markets without I/O), so its `async` entrypoints resolve
/// immediately — we drive them with `block_on` on this offline worker rather
/// than occupying an async runtime worker. `cancel` is polled per section.
fn run_historical_spine_blocking(
    runtime: &Handle,
    inputs: HistoricalSpineInputs,
) -> QuantResult<HistoricalSpineOutput> {
    runtime.block_on(run_historical_spine(
        HistoricalSpineParams {
            pit: &*inputs.pit,
            prefetched: &inputs.prefetched,
            sink: &*inputs.sink,
            cancel: &inputs.cancel,
            samples: &inputs.samples,
            request: &inputs.request,
            model_family: inputs.model_family,
            factor_serving_plane: &inputs.factor_serving_plane,
            features: &inputs.features,
            factors: &inputs.factors,
            data_quality: &inputs.data_quality,
            liquidity_cap_usd: inputs.liquidity_cap_usd,
            domain: &inputs.domain,
            selection: &inputs.selection,
            model_requirements: inputs.model_requirements,
            labelers: &inputs.labelers,
            min_exit_depth_usd: inputs.min_exit_depth_usd,
            bias_table: inputs.bias_table.as_ref().map(Arc::clone),
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
    let category_scope = TrainingDatasetService::resolve_research_profile(params.request)?
        .spec
        .category;
    let factor_engine = DatasetFactorEngine::try_new(
        params.model_family,
        params.factor_serving_plane,
        category_scope,
        params.factors,
        params.features,
        params.domain,
        if params.model_family.is_classical() {
            None
        } else {
            params.bias_table.as_ref().map(Arc::clone)
        },
    );
    let factor_engine = factor_engine?;
    let replay_config = ReplayConfig {
        features: params.features.clone(),
        factors: params.factors.clone(),
        domain: params.domain.clone(),
        data_quality: params.data_quality.clone(),
        liquidity_cap_usd: params.liquidity_cap_usd,
        bias_table: if params.model_family.is_classical() {
            None
        } else {
            params.bias_table.as_ref().map(Arc::clone)
        },
    };
    let required_features = params.model_requirements.union_all();
    let pit_selector = OfflinePitSelector::new(
        params.selection,
        params.data_quality,
        params.features,
        params.request.source_lineage.decision_policy_snapshot_id,
        params.request.knowledge_lag_secs,
        params.model_requirements.clone(),
        category_scope,
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
            factor_engine.replay_mode(),
            &replay_config,
            &CrossSectionRequest {
                pit: params.pit,
                prefetched: params.prefetched,
                trade_tape_source: ReplayTradeTapeSource::Materialized {
                    available_by: params.request.source_lineage.pit_cutoff,
                },
                decision_at: as_of,
                group: &replay_group,
                required_features: &required_features,
                category_scope: factor_engine.category_scope(),
                knowledge_lag: params.context.knowledge_lag,
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
/// governed offline executor without borrowing the service.
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
            .get(&ReplayCaptureKey::new(
                &market.market_id,
                &market.primary_token_id,
            ))
            .and_then(|capture| capture.market_context.best_ask);
        let factor_values = factor_values_at(&input.cross_section.factor_output, index)?;
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
            .get(&ReplayCaptureKey::new(
                &market.market_id,
                &market.primary_token_id,
            ))
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

fn planned_historical_samples(plan: &DatasetPlan) -> u64 {
    if plan
        .request
        .sample_sources
        .as_slice()
        .contains(&TrainingSampleSource::HistoricalPit)
    {
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
        .map(Price::from)
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
    required_features: &'a [FeatureName],
}

struct LotCrossSectionMaterialize<'a> {
    builder: &'a ConfiguredFeatureBuilder,
    factor_mode: ReplayFactorMode<'a>,
    category_scope: Option<MarketCategory>,
    replay_config: &'a ReplayConfig,
    pit: &'a dyn PointInTimeSnapshotSource,
    prefetched: &'a Prefetched,
    trade_tape_available_by: DateTime<Utc>,
    as_of: DateTime<Utc>,
    group: &'a [&'a LotSamplePlan],
    context: &'a ReplayContext,
    required_features: &'a [FeatureName],
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
            available_by: plan.request.source_lineage.pit_cutoff,
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

fn lot_cross_section_index(
    cross_section: &ReplayCrossSection,
    sample: &LotSamplePlan,
) -> QuantResult<Option<usize>> {
    for (index, market) in cross_section.markets.iter().enumerate() {
        if (&market.market_id, &market.primary_token_id) != (&sample.market_id, &sample.token_id) {
            continue;
        }
        let binding = cross_section.outcome_bindings.get(index).ok_or_else(|| {
            ResearchError::DatasetBuild {
                detail: format!(
                    "replay row {index} for market/token {}/{} has no outcome binding",
                    sample.market_id, sample.token_id
                ),
            }
        })?;
        if binding.feature_side() != sample.outcome_side {
            return Err(ResearchError::DatasetBuild {
                detail: format!(
                    "lot {} declares side {:?}, but catalog token {} resolves to {:?}",
                    sample.position_id,
                    sample.outcome_side,
                    sample.token_id,
                    binding.feature_side()
                ),
            }
            .into());
        }
        return Ok(Some(index));
    }
    Ok(None)
}

async fn materialize_lot_cross_section(
    input: LotCrossSectionMaterialize<'_>,
) -> QuantResult<Option<ReplayCrossSection>> {
    let replay_group: Vec<ReplaySample> = input
        .group
        .iter()
        .map(|sample| (sample.market_id.clone(), sample.token_id.clone()))
        .collect::<BTreeSet<_>>()
        .into_iter()
        .map(|(market_id, token_id)| ReplaySample {
            market_id,
            token_id,
        })
        .collect();
    materialize_cross_section(
        input.builder,
        input.factor_mode,
        input.replay_config,
        &CrossSectionRequest {
            pit: input.pit,
            prefetched: input.prefetched,
            trade_tape_source: ReplayTradeTapeSource::Materialized {
                available_by: input.trade_tape_available_by,
            },
            decision_at: input.as_of,
            group: &replay_group,
            required_features: input.required_features,
            category_scope: input.category_scope,
            knowledge_lag: input.context.knowledge_lag,
        },
    )
    .await
}

fn factor_values_at(output: &ReplayFactorOutput, index: usize) -> QuantResult<Vec<FactorValue>> {
    match output {
        ReplayFactorOutput::FeatureOnly => Ok(Vec::new()),
        ReplayFactorOutput::FactorNative { outcomes } => outcomes
            .get(index)
            .map(|outcome| {
                outcome
                    .factors
                    .iter()
                    .map(|scored| scored.value.clone())
                    .collect()
            })
            .ok_or_else(|| {
                ResearchError::DatasetBuild {
                    detail: format!(
                        "factor-native replay omitted outcome at aligned row index {index}"
                    ),
                }
                .into()
            }),
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
mod feedback_plan_tests {
    use chrono::{Duration, TimeZone, Utc};
    use quant_pivot_models::{
        domain::quant::FeedbackCohortWindow,
        enums::quant::{DatasetPurpose, FeedbackCohort},
        types::{
            ArtifactUri, CapabilityRegistryHashes, DATASET_COHORT_MANIFEST_FORMAT_VERSION,
            DATASET_SOURCE_LINEAGE_FORMAT_VERSION, DatasetCohortArtifactRef, DatasetCohortCounts,
            DatasetCohortManifest, DatasetSourceLineage, DecisionPolicySnapshotId, ModelSpecId,
            ReaderContractVersion, ResearchProfileArtifactId, SchemaContractVersion, SchemaVersion,
            SourceSliceId, TrainingDatasetId, TrainingSampleSource, TrainingSampleSources,
        },
    };

    use super::{DatasetPlanRequest, TrainingDatasetService};
    use crate::test_fixtures::execution_pg_seed::{
        content_hash, fixture_profile_ref, source_slice_ref,
    };

    fn feedback_request() -> DatasetPlanRequest {
        let window_start = Utc.timestamp_opt(1_000_000, 0).single().expect("start");
        let window_end = window_start + Duration::days(30);
        let label_cutoff = window_end + Duration::days(1);
        let profile_ref = fixture_profile_ref();
        let capabilities =
            CapabilityRegistryHashes::try_new(vec![content_hash('8')]).expect("capabilities");
        let source_lineage = DatasetSourceLineage {
            format_version: DATASET_SOURCE_LINEAGE_FORMAT_VERSION,
            source_slice_id: SourceSliceId::from_v7(),
            source_slice_identity_hash: content_hash('3'),
            research_profile_artifact_id: ResearchProfileArtifactId::from_profile_ref(&profile_ref),
            research_program_hash: content_hash('4'),
            source_slice: source_slice_ref('5'),
            source_window_start: window_start - Duration::days(1),
            source_window_end: label_cutoff,
            pit_cutoff: label_cutoff,
            decision_policy_snapshot_id: DecisionPolicySnapshotId::from_v7(),
            runtime_config_hash: content_hash('6'),
            reader_contract_version: ReaderContractVersion::v1(),
            schema_contract_version: SchemaContractVersion::v1(),
            source_schema_hash: content_hash('7'),
            capability_registry_hashes: capabilities.clone(),
        };
        let counts =
            DatasetCohortCounts::try_new(1, 1, 1, Vec::new(), Vec::new()).expect("cohort counts");
        let cohort_manifest = DatasetCohortManifest {
            format_version: DATASET_COHORT_MANIFEST_FORMAT_VERSION,
            cohort: FeedbackCohort::ModelScoreLearning,
            window: FeedbackCohortWindow::try_new(profile_ref, window_start, window_end)
                .expect("cohort window"),
            artifact: DatasetCohortArtifactRef {
                uri: ArtifactUri::parse("s3://fixture/feedback/cohort.json").expect("cohort URI"),
                bytes_hash: content_hash('a'),
                schema_hash: content_hash('b'),
                source_hash: content_hash('c'),
                row_count: 1,
            },
            counts,
            capability_registry_hashes: capabilities,
        };
        DatasetPlanRequest {
            model_spec_id: ModelSpecId::from_v7(),
            source_lineage,
            cohort_manifest: Some(cohort_manifest),
            window_start,
            window_end,
            sample_interval_secs: 0,
            horizons_secs: vec![0],
            knowledge_lag_secs: 0,
            feature_schema_version: SchemaVersion::FIRST,
            sample_sources: TrainingSampleSources::from(TrainingSampleSource::ModelScoreFeedback),
            training_dataset_id: Some(TrainingDatasetId::from_v7()),
            purpose: DatasetPurpose::Training,
        }
    }

    #[test]
    fn feedback_accepts_label_cutoff() {
        let request = feedback_request();
        let cohort_cutoff = request
            .cohort_manifest
            .as_ref()
            .expect("cohort")
            .window
            .cutoff();

        assert!(request.source_lineage.pit_cutoff > cohort_cutoff);
        TrainingDatasetService::validate_feedback_request(&request)
            .expect("later label-availability cutoff must remain frozen and valid");
    }

    #[test]
    fn feedback_rejects_early_cutoff() {
        let mut request = feedback_request();
        request.source_lineage.pit_cutoff = request.window_end - Duration::seconds(1);

        assert!(TrainingDatasetService::validate_feedback_request(&request).is_err());
    }
}

#[cfg(test)]
mod keep_rate_tests {
    use chrono::{DateTime, Duration, TimeZone, Utc};
    use quant_pivot_models::{
        domain::data_plane::DecisionSource,
        enums::quant::DatasetPurpose,
        types::{
            CapabilityRegistryHashes, DATASET_SOURCE_LINEAGE_FORMAT_VERSION, DatasetSourceLineage,
            DecisionPolicySnapshotId, ModelSpecId, ReaderContractVersion,
            ResearchProfileArtifactId, SchemaContractVersion, SchemaVersion, SourceSliceId,
            TrainingSampleSources,
        },
    };

    use super::{DatasetPlanRequest, KeepRateEstimate, KeepRateGrid};
    use crate::test_fixtures::execution_pg_seed::{
        content_hash, fixture_profile_ref, source_slice_ref,
    };

    fn source_lineage(
        window_start: DateTime<Utc>,
        window_end: DateTime<Utc>,
    ) -> DatasetSourceLineage {
        let profile_ref = fixture_profile_ref();
        let pit_cutoff = window_end + Duration::seconds(60);
        DatasetSourceLineage {
            format_version: DATASET_SOURCE_LINEAGE_FORMAT_VERSION,
            source_slice_id: SourceSliceId::from_v7(),
            source_slice_identity_hash: content_hash('3'),
            research_profile_artifact_id: ResearchProfileArtifactId::from_profile_ref(&profile_ref),
            research_program_hash: content_hash('4'),
            source_slice: source_slice_ref('5'),
            source_window_start: window_start,
            source_window_end: pit_cutoff,
            pit_cutoff,
            decision_policy_snapshot_id: DecisionPolicySnapshotId::from_v7(),
            runtime_config_hash: content_hash('6'),
            reader_contract_version: ReaderContractVersion::v1(),
            schema_contract_version: SchemaContractVersion::v1(),
            source_schema_hash: content_hash('7'),
            capability_registry_hashes: CapabilityRegistryHashes::try_new(vec![content_hash('8')])
                .expect("canonical capabilities"),
        }
    }

    fn request() -> DatasetPlanRequest {
        let window_start = Utc.timestamp_opt(1_000, 0).single().expect("start");
        let window_end = window_start + Duration::seconds(100);
        DatasetPlanRequest {
            model_spec_id: ModelSpecId::from_v7(),
            source_lineage: source_lineage(window_start, window_end),
            cohort_manifest: None,
            window_start,
            window_end,
            sample_interval_secs: 10,
            horizons_secs: vec![60],
            knowledge_lag_secs: 10,
            feature_schema_version: SchemaVersion::FIRST,
            sample_sources: TrainingSampleSources::default(),
            training_dataset_id: None,
            purpose: DatasetPurpose::Training,
        }
    }

    #[test]
    fn keep_rate_uses_rounding() {
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
    fn keep_rate_rejects_counts() {
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
    fn keep_rate_uses_derivation() {
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
    fn fee_quote_price_bid() {
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
    fn exit_fee_uses_version() {
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
