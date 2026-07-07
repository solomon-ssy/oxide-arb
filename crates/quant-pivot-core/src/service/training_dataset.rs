//! Offline training-dataset orchestration (Phase 3.5).
//!
//! Plans a deterministic sample grid, batch-prefetches every historical fact the
//! build needs (book snapshots, microstructure, market metadata, settlements),
//! serves point-in-time lookups from an in-memory
//! [`MaterializedPitEngine`] so the build loop issues zero DB queries, then —
//! per `as_of` cross-section — runs the **same** feature builder + factor engine
//! the online path uses, attaches forward-looking labels, asserts no future
//! leakage, materializes a content-hashed Parquet artifact, and records the
//! ledger row. Features are bounded by `as_of - source_delay`; labels look
//! strictly forward; the dataset hash makes the whole thing reproducible.

use crate::{
    pipeline::{
        historical_pit::ChHistoricalPitSource,
        historical_window::{
            HistoricalWindowLoader, Prefetched, ReplaySample, WindowSpec, forward_window,
        },
    },
    service::{
        historical_replay::{
            CrossSectionRequest, ReplayConfig, ReplayCrossSection, materialize_cross_section,
        },
        pit_selection::OfflinePitSelector,
    },
};
use async_trait::async_trait;
use chrono::{DateTime, Duration as ChronoDuration, Utc};
use quant_pivot_api::fees::FeeCalculator;
use quant_pivot_error::{QuantError, QuantResult, research::ResearchError};
use quant_pivot_models::{
    clickhouse::{BookMicrostructureRow, ChPrice, ChUsd},
    domain::{
        ExitTrainingLotRow, FeatureVectorInfo, JobProgressSink, MarketInfo, NewTrainingDataset,
        NoopProgressSink, RecommendationAttributionInfo, RecommendationInfo, ResearchJobProgress,
        query::TimeWindow,
    },
    enums::{
        common::MarketCategory,
        quant::{RecommendationAttributionOutcome, TrainingDatasetStatus},
    },
    runtime_config::{
        DataQualityConfig, DecimalString, DomainConfig, FactorsConfig, FeaturesConfig,
        SelectionConfig, TrainingConfig,
    },
    types::{
        Bps, MarketId, Price, Shares, TokenId, TrainingDatasetId, TrainingExampleId,
        TrainingHorizonsSecs, TrainingSampleSource, Usd,
    },
};
use quant_pivot_repository::traits::{
    AttributionRepository, EventRepository, FeatureRepository, MarketLinkageRepository,
    MarketRepository, PositionRepository, QuantFactReadRepository, RecommendationRepository,
    TrainingDatasetRepository,
};
use quant_pivot_research::{
    artifact::{ArtifactKey, ArtifactNamespace, ArtifactStore},
    execution_sim::BookFidelity,
    factors::{
        FactorEligibility, FactorEngine, FactorExplanation, FactorName, FactorValue,
        MarketFactorOutcome, NormalizedFactor,
    },
    features::{
        ConfiguredFeatureBuilder, DomainFeatureSlice, EvidenceSourceRef, FeatureName, FeatureValue,
        FeatureVector, SubstitutionAudit,
    },
    hashing::ResearchHasher,
    model::{
        FavoriteLongshotBiasTable,
        sell_scorer::{LotStateInput, position_state_factor_values, position_state_features},
    },
    pit::PitQueryEngine,
    selection::SelectedMarket,
    training::{
        DatasetCoverage, DatasetParquetCodec, DatasetPlan, DatasetPlanRequest, DecisionBook,
        ExitDecisionLabelContext, ForwardWindow, HoldVsExitProceedsLabeler, LabelBuildInput,
        LabelBuildOutput, LabelName, Labeler, LiquidityExitLabeler, LotSamplePlan,
        LotTerminalSnapshot, LotTrainingContext, MaxAdverseExcursionLabeler,
        MaxFavorableExcursionLabeler, PlanMarket, ReturnToHorizonLabeler, SamplePlan,
        SettlementOutcomeLabeler, TrainingDatasetArtifact, TrainingDatasetBuilder,
        TrainingDatasetPlanner, TrainingExample, TrainingLabel, assert_no_future_leakage,
        count_samples, label_names_for_sources, plan_lot_timeline_samples, plan_samples,
        probe_matrix_coverage, remaining_shares_at,
    },
};
use rust_decimal::Decimal;
use std::{
    collections::{BTreeMap, HashMap, HashSet},
    sync::Arc,
    time::Duration,
};
use tokio::{runtime::Handle, task};
use tokio_util::sync::CancellationToken;

const LIVE_ATTRIBUTION_SAMPLE_LIMIT: u64 = 10_000;

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

/// Dependencies injected into [`TrainingDatasetService`].
pub struct TrainingDatasetServiceDeps {
    /// `ClickHouse` fact reader for batch prefetch.
    pub fact_read: Arc<dyn QuantFactReadRepository>,
    /// Postgres market catalog.
    pub market_repo: Arc<dyn MarketRepository>,
    /// Postgres event catalog snapshot for neg-risk leg enumeration.
    pub event_repo: Arc<dyn EventRepository>,
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
    /// Position ledger for closed-lot `ExitDecision` sampling.
    pub position_repo: Arc<dyn PositionRepository>,
    /// Venue fee calculator for governed exit-fee-aware Sell labels (06.1).
    pub fee_calculator: Arc<FeeCalculator>,
    /// Frozen market → external-subject linkage ledger (11.2.2).
    pub linkage_repo: Arc<dyn MarketLinkageRepository>,
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
    /// External-vertical domain plane configuration (Phase 11.2.2).
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
    market_repo: Arc<dyn MarketRepository>,
    event_repo: Arc<dyn EventRepository>,
    artifact_store: Arc<dyn ArtifactStore>,
    dataset_repo: Arc<dyn TrainingDatasetRepository>,
    attribution_repo: Arc<dyn AttributionRepository>,
    recommendation_repo: Arc<dyn RecommendationRepository>,
    feature_repo: Arc<dyn FeatureRepository>,
    position_repo: Arc<dyn PositionRepository>,
    fee_calculator: Arc<FeeCalculator>,
    linkage_repo: Arc<dyn MarketLinkageRepository>,
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
    /// Frozen book-depth floor for offline point-in-time selection.
    min_selection_depth: DecimalString,
    /// Deploy guard: hard cap on the deterministic historical spine.
    max_spine_samples: u64,
    /// Shared so the historical spine can be built inside a `spawn_blocking`
    /// closure (labelers are `Send + Sync` but not `Clone`).
    labelers: Arc<Vec<Box<dyn Labeler>>>,
    /// Frozen favorite-longshot bias table bound to the offline factor engine.
    bias_table: Option<Arc<FavoriteLongshotBiasTable>>,
}

impl TrainingDatasetService {
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
        let min_exit_depth_usd = config
            .training
            .min_exit_depth_usd_typed()
            .map_err(QuantError::config)?;
        let max_book_staleness = Duration::from_millis(config.training.max_book_staleness_ms);
        Ok(Self {
            fact_read: deps.fact_read,
            market_repo: deps.market_repo,
            event_repo: deps.event_repo,
            artifact_store: deps.artifact_store,
            dataset_repo: deps.dataset_repo,
            attribution_repo: deps.attribution_repo,
            recommendation_repo: deps.recommendation_repo,
            feature_repo: deps.feature_repo,
            position_repo: deps.position_repo,
            fee_calculator: deps.fee_calculator,
            linkage_repo: deps.linkage_repo,
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
            min_selection_depth: config.training.min_selection_depth_usd.clone(),
            selection: config.selection,
            max_spine_samples,
            labelers: Arc::new(config.labelers),
            bias_table: config.bias_table,
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
        let staleness = ChronoDuration::milliseconds(
            i64::try_from(self.max_book_staleness.as_millis()).unwrap_or(i64::MAX),
        );
        let from_ms = (window_start - staleness).timestamp_millis();
        let to_ms = window_end.timestamp_millis();
        let ids = self
            .fact_read
            .observed_markets_between(from_ms, to_ms)
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
    /// enabled-category gate mirrors the online [`CategoryFilter`] (fee-dominant
    /// category), not the raw Postgres `categories[]` membership list.
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
        self.enabled_categories.is_empty() || self.enabled_categories.contains(&info.fee_category())
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
        let markets = if wants_historical {
            self.historical_candidate_markets(request.window_start, request.window_end)
                .await?
        } else {
            Vec::new()
        };
        let candidate_infos: Vec<&MarketInfo> = markets
            .iter()
            .filter(|info| self.in_selection(info, request.window_start, request.window_end))
            .collect();
        let spine_upper_bound = if wants_historical {
            let plan_markets: Vec<PlanMarket> = candidate_infos
                .iter()
                .map(|info| PlanMarket {
                    market_id: info.market_id.clone(),
                    token_id: info.yes_token_id.clone(),
                    created_at: info.created_at,
                    end_date: info.end_date,
                })
                .collect();
            count_samples(request, &plan_markets, self.max_spine_samples)
        } else {
            0
        };
        total += spine_upper_bound;

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
            total += attributions.len() as u64;
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
            );
            total += lot_samples.len() as u64;
        }

        // Bounded point-in-time keep-rate estimate: sample K `as_of` slices × M
        // candidate markets, replay the selection funnel, and scale the spine.
        let (keep_rate, keep_rate_sample_size) = if wants_historical
            && spine_upper_bound > 0
            && sample_slices > 0
            && sample_markets > 0
            && !candidate_infos.is_empty()
        {
            self.estimate_keep_rate(request, &candidate_infos, sample_slices, sample_markets)
                .await?
        } else {
            (None, 0)
        };
        let estimated_eligible_samples = keep_rate.map_or(total, |rate| {
            #[allow(
                clippy::cast_precision_loss,
                clippy::cast_sign_loss,
                clippy::cast_possible_truncation
            )]
            let scaled = (spine_upper_bound as f64 * rate).round() as u64;
            // Non-historical sources (attribution / exit) are not selection-gated.
            scaled + (total - spine_upper_bound)
        });

        Ok(PlanCounts {
            spine_upper_bound,
            total,
            hard_cap_exceeded: total > self.max_spine_samples,
            estimated_eligible_samples,
            keep_rate,
            keep_rate_sample_size,
        })
    }

    /// Estimate the point-in-time selection keep-rate by replaying the funnel
    /// over a bounded `slices × markets` sample.
    ///
    /// Returns `(keep_rate, trials)`: the fraction of `(market, slice)` pairs that
    /// pass `FilterChain::standard()`, and the number of trials. The market sample
    /// is stride-selected across the id-sorted candidate set for representativeness;
    /// slices are the midpoints of `slices` equal sub-intervals of the window.
    async fn estimate_keep_rate(
        &self,
        request: &DatasetPlanRequest,
        candidates: &[&MarketInfo],
        slices: u32,
        markets: u32,
    ) -> QuantResult<(Option<f64>, u64)> {
        let sampled = stride_sample(candidates, markets as usize);
        if sampled.is_empty() {
            return Ok((None, 0));
        }
        let pit = ChHistoricalPitSource::new(
            Arc::clone(&self.fact_read),
            Arc::clone(&self.market_repo),
            self.max_book_staleness,
        );
        let selector = self.offline_pit_selector(request);
        let span_secs = (request.window_end - request.window_start)
            .num_seconds()
            .max(1);
        let mut included: u64 = 0;
        let mut trials: u64 = 0;
        for index in 0..slices {
            #[allow(clippy::cast_precision_loss, clippy::cast_possible_truncation)]
            let offset = ((f64::from(index) + 0.5) / f64::from(slices) * span_secs as f64) as i64;
            let as_of = request.window_start + ChronoDuration::seconds(offset);
            let result = selector.select_at(as_of, &sampled, &pit).await?;
            included += result.included.len() as u64;
            trials += sampled.len() as u64;
        }
        if trials == 0 {
            return Ok((None, 0));
        }
        #[allow(clippy::cast_precision_loss)]
        let keep_rate = included as f64 / trials as f64;
        Ok((Some(keep_rate), trials))
    }
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
        let samples = plan_samples(&request, &plan_markets);
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
            );
        }
        let training_dataset_id = request
            .training_dataset_id
            .clone()
            .unwrap_or_else(TrainingDatasetId::from_v7);
        let label_names = label_names_for_sources(&request.sample_sources);
        Ok(DatasetPlan {
            request,
            training_dataset_id,
            samples,
            lot_samples,
            exit_training_lots,
            label_names,
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
        self.build_inner(plan, Arc::new(NoopProgressSink), CancellationToken::new())
            .await
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
        self.build_inner(plan, sink, cancel).await
    }

    async fn build_inner(
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
        let context = ReplayContext::new(&plan, &self.features);
        let loader = self.window_loader();
        // Prefetch is real ClickHouse I/O — stays on the async runtime.
        let window = loader
            .load(&context.window_spec(&plan, &self.domain))
            .await?;
        let coverage = DatasetCoverage {
            planned_samples: planned_historical_samples(&plan),
            book_decode_failures: window.book_decode_failures,
            ..DatasetCoverage::default()
        };
        let pit: Arc<dyn PitQueryEngine> = Arc::new(window.pit);
        let prefetched = Arc::new(window.prefetched);

        // Offload the unbounded historical PIT loop to a blocking thread so it
        // never occupies an async runtime worker (CPU-bound in-memory scoring
        // that would otherwise starve other jobs' heartbeats / lease renewals),
        // polling `cancel` at each cross-section boundary for a ~one-section
        // cooperative cancel latency.
        let mut spine = HistoricalSpine::default();
        let mut coverage = coverage;
        if wants_sample_source(&plan.request, TrainingSampleSource::HistoricalPit) {
            let inputs = self.historical_inputs(
                &plan,
                Arc::clone(&pit),
                Arc::clone(&prefetched),
                Arc::clone(&sink),
                cancel.clone(),
                coverage,
            );
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
                context: &context,
                sink: &*sink,
            },
            coverage,
            spine,
        )
        .await
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
        pit: &dyn PitQueryEngine,
    ) -> QuantResult<TrainingDatasetArtifact> {
        self.ensure_factors_enabled()?;
        let context = ReplayContext::new(&plan, &self.features);
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
        if wants_sample_source(&plan.request, TrainingSampleSource::HistoricalPit) {
            let output = run_historical_spine(
                self.historical_params(&plan, pit, &prefetched, &NoopProgressSink, &cancel),
                coverage,
            )
            .await?;
            coverage = output.coverage;
            spine.examples = output.examples;
            spine.market_set = output.market_set;
        }
        self.assemble_and_finalize(
            plan,
            BuildTail {
                pit,
                prefetched: &prefetched,
                context: &context,
                sink: &NoopProgressSink,
            },
            coverage,
            spine,
        )
        .await
    }

    /// Owned inputs for the blocking historical spine (moved into `spawn_blocking`).
    fn historical_inputs(
        &self,
        plan: &DatasetPlan,
        pit: Arc<dyn PitQueryEngine>,
        prefetched: Arc<Prefetched>,
        sink: Arc<dyn JobProgressSink>,
        cancel: CancellationToken,
        coverage: DatasetCoverage,
    ) -> HistoricalSpineInputs {
        HistoricalSpineInputs {
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
            min_selection_depth: self.min_selection_depth.clone(),
            labelers: Arc::clone(&self.labelers),
            min_exit_depth_usd: self.min_exit_depth_usd,
            bias_table: self.bias_table.as_ref().map(Arc::clone),
            context: ReplayContext::new(plan, &self.features),
            coverage,
        }
    }

    /// Borrowed params for the historical spine (async/inline callers).
    fn historical_params<'a>(
        &'a self,
        plan: &'a DatasetPlan,
        pit: &'a dyn PitQueryEngine,
        prefetched: &'a Prefetched,
        sink: &'a dyn JobProgressSink,
        cancel: &'a CancellationToken,
    ) -> HistoricalSpineParams<'a> {
        HistoricalSpineParams {
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
            min_selection_depth: &self.min_selection_depth,
            labelers: &self.labelers,
            min_exit_depth_usd: self.min_exit_depth_usd,
            bias_table: &self.bias_table,
            context: ReplayContext::new(plan, &self.features),
        }
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
            context,
            sink,
        } = tail;
        let builder = ConfiguredFeatureBuilder::new(&self.features, &self.domain);
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
            Arc::clone(&self.market_repo),
            Arc::clone(&self.event_repo),
            Arc::clone(&self.linkage_repo),
            self.max_book_staleness,
        )
    }

    /// The offline point-in-time selection funnel for a build/plan, wired from
    /// the frozen selection/data-quality/feature config + the book-depth floor.
    fn offline_pit_selector(&self, request: &DatasetPlanRequest) -> OfflinePitSelector {
        OfflinePitSelector::new(
            &self.selection,
            &self.data_quality,
            &self.features,
            &self.min_selection_depth,
            request.runtime_config_version_id.clone(),
            request.source_delay_secs,
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
        coverage.planned_samples += attributions.len() as u64;

        for attribution in attributions {
            match self
                .materialize_live_attribution_example(&attribution)
                .await?
            {
                Some(example) => {
                    market_set.insert(example.market_id.clone());
                    coverage.labels_available += example.labels.len() as u64;
                    examples.push(example);
                }
                None => coverage.live_attribution_dropped_missing_evidence += 1,
            }
        }
        Ok(())
    }

    async fn materialize_live_attribution_example(
        &self,
        attribution: &RecommendationAttributionInfo,
    ) -> QuantResult<Option<TrainingExample>> {
        let Some(recommendation) = self
            .recommendation_repo
            .find_by_id(&attribution.recommendation_id)
            .await?
        else {
            tracing::warn!(
                recommendation_id = %attribution.recommendation_id,
                "live attribution sample dropped: recommendation not found",
            );
            return Ok(None);
        };
        if recommendation.status.excluded_from_attribution() {
            tracing::warn!(
                recommendation_id = %attribution.recommendation_id,
                "live attribution sample dropped: recommendation revoked",
            );
            return Ok(None);
        }
        let Some(feature_info) = self
            .feature_repo
            .find_by_id(&recommendation.evidence_refs.feature_vector_id)
            .await?
        else {
            tracing::warn!(
                recommendation_id = %attribution.recommendation_id,
                feature_vector_id = %recommendation.evidence_refs.feature_vector_id,
                "live attribution sample dropped: frozen feature vector not found",
            );
            return Ok(None);
        };
        let Some(feature_vector) = frozen_feature_vector(&feature_info) else {
            tracing::warn!(
                recommendation_id = %attribution.recommendation_id,
                feature_vector_id = %recommendation.evidence_refs.feature_vector_id,
                "live attribution sample dropped: frozen feature vector payload is invalid",
            );
            return Ok(None);
        };
        let Some(factor_values) = frozen_factor_values(&recommendation) else {
            tracing::warn!(
                recommendation_id = %attribution.recommendation_id,
                "live attribution sample dropped: frozen factor definitions are incomplete",
            );
            return Ok(None);
        };

        let labels = attribution_labels(attribution, &recommendation);
        Ok(Some(TrainingExample {
            example_id: TrainingExampleId::from_v7(),
            market_id: recommendation.market_id.clone(),
            token_id: recommendation.token_id.clone(),
            as_of: feature_vector.as_of,
            sample_source: TrainingSampleSource::LiveAttribution,
            source_refs: feature_vector.source_refs.clone(),
            feature_vector,
            factor_values,
            labels,
            lot_context: None,
            position_state: None,
            book_fidelity: None,
        }))
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
        let builder = ConfiguredFeatureBuilder::new(&self.features, &self.domain);
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
        let remaining =
            remaining_shares_at(&LotTerminalSnapshot::from(input.lot), input.sample.as_of);
        if !remaining.is_positive() {
            return Ok(());
        }
        let micro = input
            .prefetched
            .micro
            .get(&input.sample.token_id)
            .map_or(&[][..], Vec::as_slice);
        // Peak-to-`as_of` (strictly past): the lot's lifetime peak persisted at
        // close would leak future price into an early-tick drawdown feature.
        let peak_mark = peak_mark_to(micro, input.lot.opened_at, input.sample.as_of);
        let position_state = position_state_features(LotStateInput {
            avg_price: input.lot.avg_price.inner(),
            mark: entry_mid.map(Price::inner),
            opened_at: input.lot.opened_at,
            now: input.sample.as_of,
            max_hold_secs: input.lot.max_hold_secs,
            peak_mark: peak_mark.map(Price::inner),
        });
        let (decision_book, book_fidelity) =
            decision_book_at(input.pit, &input.sample.token_id, input.sample.as_of, micro).await?;
        // Govern the exit fee with the same calculator the live exit dispatcher
        // uses (via reconciliation), converted to an effective bps of the exit
        // notional so the label matches production net proceeds.
        let exit_price = entry_mid.unwrap_or(input.lot.avg_price);
        let fee_bps = self.exit_fee_bps(remaining, exit_price, market, &input.sample.token_id);
        let label_ctx = ExitDecisionLabelContext {
            remaining_shares: remaining,
            avg_price: input.lot.avg_price,
            fee_bps,
            terminal: LotTerminalSnapshot::from(input.lot),
            decision_book,
        };
        let forward = forward_window(
            input.sample.as_of,
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
        );
        let labels = build_labels_for(
            &LabelBuildParams {
                labelers: input.labelers,
                market,
                as_of: input.sample.as_of,
                entry_mid,
                request: input.request,
                forward: &forward,
                exit_decision: Some(&label_ctx),
            },
            self.min_exit_depth_usd,
            sink.coverage,
        );
        record_exit_fill_fidelity(sink.coverage, book_fidelity);
        let mut merged_factors = factor_values;
        merged_factors.extend(position_state_factor_values(&position_state));
        sink.market_set.insert(input.sample.market_id.clone());
        sink.examples.push(TrainingExample {
            example_id: TrainingExampleId::from_v7(),
            market_id: input.sample.market_id.clone(),
            token_id: input.sample.token_id.clone(),
            as_of: input.sample.as_of,
            sample_source: TrainingSampleSource::ExitDecision,
            feature_vector: vector.clone(),
            factor_values: merged_factors,
            labels,
            source_refs: vector.source_refs.clone(),
            lot_context: Some(LotTrainingContext {
                order_intent_id: input.sample.order_intent_id.clone(),
                position_id: input.sample.position_id.clone(),
                remaining_shares: remaining,
                avg_price: input.lot.avg_price,
                peak_mark,
                opened_at: input.lot.opened_at,
                max_hold_secs: input.lot.max_hold_secs,
            }),
            position_state: Some(position_state),
            book_fidelity,
        });
        sink.coverage.exit_decision_built += 1;
        Ok(())
    }

    /// Effective exit fee in basis points of the exit notional, quoted from the
    /// governed venue fee calculator (same schedule the live exit dispatcher
    /// uses). Zero notional or no schedule → no fee.
    fn exit_fee_bps(
        &self,
        shares: Shares,
        price: Price,
        market: &SelectedMarket,
        token_id: &TokenId,
    ) -> Bps {
        let notional = shares.inner() * price.inner();
        if notional <= Decimal::ZERO {
            return Bps::ZERO;
        }
        let fee_usd = self.fee_calculator.calculate(
            shares,
            price,
            market.category,
            &market.market_id,
            token_id,
        );
        let bps = (fee_usd.inner() / notional * Decimal::from(10_000)).max(Decimal::ZERO);
        Bps::new(bps)
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
        // Hard, money-critical gate: no feature may observe state past its cutoff.
        assert_no_future_leakage(&examples, plan.request.source_delay_secs)?;

        let feature_schema_hash = ResearchHasher::feature_schema(builder.schema())?;
        let factor_schema_hash = ResearchHasher::factor_schema(&engine.factor_set())?;
        let label_schema_hash = ResearchHasher::label_schema(&plan.label_names)?;
        let mut coverage = coverage;
        coverage.bias_table_hash = self
            .bias_table
            .as_ref()
            .map(|table| table.content_hash.clone());
        if !examples.is_empty() {
            let horizon_secs = plan.request.horizons_secs.first().copied().unwrap_or(0);
            coverage.matrix_probe = Some(probe_matrix_coverage(
                &examples,
                builder.schema(),
                &ReturnToHorizonLabeler.label_name(),
                horizon_secs,
            )?);
        }
        let dataset_hash = TrainingDatasetArtifact::compute_dataset_hash(
            &plan.request.model_spec_id,
            plan.request.window_start,
            plan.request.window_end,
            &feature_schema_hash,
            &factor_schema_hash,
            &label_schema_hash,
            &examples,
        )?;

        let parquet_bytes = DatasetParquetCodec::encode(&examples)?;
        let key = ArtifactKey::new(
            ArtifactNamespace::Dataset,
            plan.training_dataset_id.as_uuid().to_string(),
            "parquet",
        )?;
        let parquet_uri = self.artifact_store.put(key, &parquet_bytes).await?;

        let status = if coverage.built_examples == 0 {
            TrainingDatasetStatus::Failed
        } else if coverage.labels_available == 0 {
            TrainingDatasetStatus::InsufficientLabels
        } else {
            TrainingDatasetStatus::Built
        };
        self.dataset_repo
            .create(NewTrainingDataset {
                training_dataset_id: plan.training_dataset_id.clone(),
                model_spec_id: plan.request.model_spec_id.clone(),
                window_start: plan.request.window_start,
                window_end: plan.request.window_end,
                status,
                feature_schema_hash: feature_schema_hash.clone(),
                factor_schema_hash: factor_schema_hash.clone(),
                label_schema_hash: label_schema_hash.clone(),
                dataset_hash: dataset_hash.clone(),
                parquet_uri: parquet_uri.clone(),
                sample_count: i64::try_from(examples.len()).unwrap_or(i64::MAX),
                source_delay_secs: i64::try_from(plan.request.source_delay_secs)
                    .unwrap_or(i64::MAX),
                sample_interval_secs: i64::try_from(plan.request.sample_interval_secs)
                    .unwrap_or(i64::MAX),
                horizons_secs: TrainingHorizonsSecs(plan.request.horizons_secs.clone()),
                coverage_json: coverage.clone(),
                runtime_config_version_id: plan.request.runtime_config_version_id.clone(),
            })
            .await
            .map_err(QuantError::from)?;

        Ok(TrainingDatasetArtifact {
            training_dataset_id: plan.training_dataset_id,
            model_spec_id: plan.request.model_spec_id,
            window_start: plan.request.window_start,
            window_end: plan.request.window_end,
            examples,
            feature_schema_hash,
            factor_schema_hash,
            label_schema_hash,
            dataset_hash,
            parquet_uri,
            coverage,
        })
    }
}

/// Build every label (labeler × horizon) for one example, accounting coverage.
/// Free function so the historical spine can call it from a `spawn_blocking`
/// closure (no `&self` borrow).
fn build_labels_for(
    params: &LabelBuildParams<'_>,
    min_exit_depth_usd: Usd,
    coverage: &mut DatasetCoverage,
) -> Vec<TrainingLabel> {
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
                as_of: params.as_of,
                entry_mid: params.entry_mid,
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
            }
        }
    }
    labels
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
    pit: &'a dyn PitQueryEngine,
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
    min_selection_depth: &'a DecimalString,
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
    pit: Arc<dyn PitQueryEngine>,
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
    min_selection_depth: DecimalString,
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
            min_selection_depth: &inputs.min_selection_depth,
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
    let builder = ConfiguredFeatureBuilder::new(params.features, params.domain);
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
        params.min_selection_depth,
        params.request.runtime_config_version_id.clone(),
        params.request.source_delay_secs,
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
        let replay_group = pit_selected_replay_group(
            &pit_selector,
            as_of,
            &group,
            params.pit,
            params.prefetched,
            &mut coverage,
        )
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
                as_of,
                group: &replay_group,
                source_delay: params.context.source_delay,
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
        );
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
async fn pit_selected_replay_group(
    pit_selector: &OfflinePitSelector,
    as_of: DateTime<Utc>,
    group: &[&SamplePlan],
    pit: &dyn PitQueryEngine,
    prefetched: &Prefetched,
    coverage: &mut DatasetCoverage,
) -> QuantResult<Vec<ReplaySample>> {
    let market_infos: Vec<&MarketInfo> = group
        .iter()
        .filter_map(|sample| {
            prefetched
                .markets_by_id
                .get(&sample.market_id)
                .map(AsRef::as_ref)
        })
        .collect();
    let selection = pit_selector.select_at(as_of, &market_infos, pit).await?;
    coverage.pit_selection_candidates += market_infos.len() as u64;
    coverage.pit_selection_included += selection.included.len() as u64;
    coverage.pit_selection_excluded += selection.exclusion_summary;
    let kept: HashSet<MarketId> = selection
        .included
        .iter()
        .map(|market| market.market_id.clone())
        .collect();
    Ok(group
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
) {
    for (index, vector) in input.cross_section.vectors.iter().enumerate() {
        let market = &input.cross_section.markets[index];
        let entry_mid = input.cross_section.entry_mids[index];
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
            input.cross_section.as_of,
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
        );
        let labels = build_labels_for(
            &LabelBuildParams {
                labelers,
                market,
                as_of: input.cross_section.as_of,
                entry_mid,
                request: input.request,
                forward: &forward,
                exit_decision: None,
            },
            min_exit_depth_usd,
            sink.coverage,
        );
        sink.market_set.insert(market.market_id.clone());
        sink.examples.push(TrainingExample {
            example_id: TrainingExampleId::from_v7(),
            market_id: market.market_id.clone(),
            token_id: market.primary_token_id.clone(),
            as_of: input.cross_section.as_of,
            sample_source: TrainingSampleSource::HistoricalPit,
            feature_vector: vector.clone(),
            factor_values,
            labels,
            source_refs: vector.source_refs.clone(),
            lot_context: None,
            position_state: None,
            book_fidelity: None,
        });
    }
}

fn frozen_feature_vector(info: &FeatureVectorInfo) -> Option<FeatureVector> {
    let generic = info.payload.get("generic")?.clone();
    let domain = info
        .payload
        .get("domain")
        .cloned()
        .unwrap_or(serde_json::Value::Null);
    let substitutions = info
        .payload
        .get("substitutions")
        .cloned()
        .unwrap_or_else(|| serde_json::json!([]));
    Some(FeatureVector {
        market_id: info.market_id.clone(),
        token_id: info.token_id.clone(),
        as_of: info.as_of,
        generic_schema_version: info.feature_schema_version,
        generic: serde_json::from_value::<BTreeMap<FeatureName, FeatureValue>>(generic).ok()?,
        domain: serde_json::from_value::<Option<DomainFeatureSlice>>(domain).ok()?,
        substitutions: serde_json::from_value::<Vec<SubstitutionAudit>>(substitutions).ok()?,
        data_quality: info.data_quality,
        staleness_ms: u64::try_from(info.staleness_ms.max(0)).ok()?,
        source_refs: serde_json::from_value::<Vec<EvidenceSourceRef>>(info.source_refs.clone())
            .ok()?,
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
    let mut labels = Vec::new();
    if let Some(realized_pnl) = attribution.realized_pnl_usd {
        push_label(&mut labels, "realized_pnl_usd", realized_pnl.inner());
        if !recommendation.sizing_plan.suggested_usd.is_zero() {
            push_label(
                &mut labels,
                "realized_return_bps",
                realized_pnl.inner() / recommendation.sizing_plan.suggested_usd.inner()
                    * Decimal::from(10_000),
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
    );
    if let Some(slippage) = attribution.entry_outcome_json.entry_slippage_bps {
        push_label(&mut labels, "entry_slippage_bps", slippage.inner());
    }
    if let Some(mfe) = attribution.max_favorable_excursion_bps {
        push_label(&mut labels, "max_favorable_excursion_bps", mfe);
    }
    if let Some(mae) = attribution.max_adverse_excursion_bps {
        push_label(&mut labels, "max_adverse_excursion_bps", mae);
    }
    push_label(
        &mut labels,
        "missed_return_bps",
        if attribution.entry_outcome_json.entry_filled {
            Decimal::ZERO
        } else {
            recommendation.expected_return_bps.inner()
        },
    );
    push_label(
        &mut labels,
        "recommendation_outcome",
        recommendation_outcome_code(attribution.outcome),
    );
    labels
}

fn push_label(labels: &mut Vec<TrainingLabel>, name: &'static str, value: Decimal) {
    labels.push(TrainingLabel {
        label_name: LabelName::from_static(name),
        horizon_secs: 0,
        value,
        is_resolved: true,
    });
}

fn recommendation_outcome_code(outcome: RecommendationAttributionOutcome) -> Decimal {
    match outcome {
        RecommendationAttributionOutcome::FilledExited => Decimal::ONE,
        RecommendationAttributionOutcome::FilledSettled => Decimal::from(2),
        RecommendationAttributionOutcome::ExpiredUnfilled => Decimal::NEGATIVE_ONE,
        RecommendationAttributionOutcome::CancelledUnfilled => Decimal::from(-2),
        RecommendationAttributionOutcome::FailedUnfilled => Decimal::from(-3),
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
        groups.entry(sample.as_of).or_default().push(sample);
    }
    groups
}

fn group_lot_samples(samples: &[LotSamplePlan]) -> BTreeMap<DateTime<Utc>, Vec<&LotSamplePlan>> {
    let mut groups: BTreeMap<DateTime<Utc>, Vec<&LotSamplePlan>> = BTreeMap::new();
    for sample in samples {
        groups.entry(sample.as_of).or_default().push(sample);
    }
    groups
}

async fn decision_book_at(
    pit: &dyn PitQueryEngine,
    token_id: &TokenId,
    as_of: DateTime<Utc>,
    micro: &[BookMicrostructureRow],
) -> QuantResult<(Option<DecisionBook>, Option<BookFidelity>)> {
    if let Some(snapshot) = pit.book_at(token_id, as_of).await?
        && !snapshot.bids.is_empty()
    {
        return Ok((
            Some(DecisionBook::L2 {
                bids: Arc::clone(&snapshot.bids),
            }),
            Some(BookFidelity::L2),
        ));
    }
    // No L2 depth at the decision instant: fall back to the latest microstructure
    // bucket at or before `as_of` (best bid + aggregate depth), tagged degraded so
    // the sell quality gate can bound the fallback ratio. Under-estimates slippage
    // (single-price walk) — acceptable for a coverage-recovering degraded row.
    if let Some((best_bid, depth)) = microstructure_fallback(micro, as_of) {
        return Ok((
            Some(DecisionBook::Microstructure { best_bid, depth }),
            Some(BookFidelity::MicrostructureFallback),
        ));
    }
    Ok((None, None))
}

/// Peak mark observed over `[opened_at, as_of]` from the microstructure series
/// (per-bucket best-bid high). PIT-safe: never reads a bucket past `as_of`, so
/// the derived drawdown feature cannot leak a future peak. `None` when no bucket
/// covers the range (drawdown then degrades to zero, not a leaked value).
fn peak_mark_to(
    micro: &[BookMicrostructureRow],
    opened_at: DateTime<Utc>,
    as_of: DateTime<Utc>,
) -> Option<Price> {
    let start = opened_at.timestamp_millis();
    let end = as_of.timestamp_millis();
    micro
        .iter()
        .filter(|row| row.bucket_time >= start && row.bucket_time <= end)
        .filter_map(|row| {
            row.best_bid_high
                .or(row.best_bid_close)
                .or(row.mid_price_close)
        })
        .map(ChPrice::to_price)
        .max()
}

/// Best bid + aggregate share depth from the latest microstructure bucket at or
/// before `as_of`. PIT-safe: never reads a future bucket. Share depth is the
/// top-of-book USD depth divided by the best bid.
fn microstructure_fallback(
    micro: &[BookMicrostructureRow],
    as_of: DateTime<Utc>,
) -> Option<(Price, Shares)> {
    let as_of_ms = as_of.timestamp_millis();
    let row = micro
        .iter()
        .filter(|row| row.bucket_time <= as_of_ms)
        .max_by_key(|row| row.bucket_time)?;
    let best_bid = row
        .best_bid_close
        .or(row.best_bid_open)
        .or(row.mid_price_close)
        .map(ChPrice::to_price)?;
    if best_bid.inner() <= Decimal::ZERO {
        return None;
    }
    let depth_usd = row
        .top5_depth_usd_avg
        .or(row.top1_depth_usd_avg)
        .map(ChUsd::to_usd)?;
    let depth_shares = depth_usd.inner() / best_bid.inner();
    if depth_shares <= Decimal::ZERO {
        return None;
    }
    Some((best_bid, Shares::new(depth_shares)))
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
    pit: &'a dyn PitQueryEngine,
    prefetched: &'a Prefetched,
    context: &'a ReplayContext,
    sink: &'a dyn JobProgressSink,
}

struct LabelBuildParams<'a> {
    labelers: &'a [Box<dyn Labeler>],
    market: &'a SelectedMarket,
    as_of: DateTime<Utc>,
    entry_mid: Option<Price>,
    request: &'a DatasetPlanRequest,
    forward: &'a ForwardWindow,
    exit_decision: Option<&'a ExitDecisionLabelContext>,
}

struct ExitDecisionAppendInput<'a> {
    plan: &'a DatasetPlan,
    pit: &'a dyn PitQueryEngine,
    prefetched: &'a Prefetched,
    context: &'a ReplayContext,
}

struct LotCrossSectionMaterialize<'a> {
    builder: &'a ConfiguredFeatureBuilder,
    engine: &'a FactorEngine,
    replay_config: &'a ReplayConfig,
    pit: &'a dyn PitQueryEngine,
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
    pit: &'a dyn PitQueryEngine,
    max_horizon_secs: u64,
    labelers: &'a [Box<dyn Labeler>],
}

struct ExampleBuildSink<'a> {
    coverage: &'a mut DatasetCoverage,
    examples: &'a mut Vec<TrainingExample>,
    market_set: &'a mut HashSet<MarketId>,
}

/// Derived replay parameters for one dataset build (shared across cross-sections).
#[derive(Clone, Copy)]
struct ReplayContext {
    source_delay: Duration,
    lookback: Duration,
    max_horizon_secs: u64,
}

impl ReplayContext {
    /// Derive the source delay, feature lookback, and max forward horizon.
    fn new(plan: &DatasetPlan, features: &FeaturesConfig) -> Self {
        Self {
            source_delay: Duration::from_secs(plan.request.source_delay_secs),
            lookback: Duration::from_secs(features.max_lookback_secs()),
            max_horizon_secs: plan
                .request
                .horizons_secs
                .iter()
                .copied()
                .max()
                .unwrap_or(0),
        }
    }

    /// The prefetch window spec for this build's sample set.
    fn window_spec(&self, plan: &DatasetPlan, domain: &DomainConfig) -> WindowSpec {
        WindowSpec {
            window_start: plan.request.window_start,
            window_end: plan.request.window_end,
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
            source_delay: self.source_delay,
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
            as_of: input.as_of,
            group: &replay_group,
            source_delay: input.context.source_delay,
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
    if book_fidelity == Some(BookFidelity::L2) {
        coverage.exit_fill_l2_rows += 1;
    } else if book_fidelity.is_some() {
        coverage.exit_fill_fallback_rows += 1;
    }
}
