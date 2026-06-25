//! Backtest plane (Phase 3.6): deterministic point-in-time replay of a model
//! version into a [`BacktestReport`].
//!
//! The replay is split cleanly across the crate boundary (D7): `quant-pivot-core`
//! materializes each tick's model input from a **historical PIT source**
//! (`MaterializedPitEngine` over prefetched `ClickHouse` facts — never the live
//! `BookStore`) plus the realized settlement outcomes, and this pure engine runs
//! model inference → greedy allocation → outcome resolution → metrics. Because
//! the engine has no access to a `BookStore` or any live source, "no live
//! `BookStore` in a backtest" is structurally guaranteed.
//!
//! Money / price / probability stay in project newtypes; `f64` never appears.

mod allocator;
mod comparison;
mod metrics;
mod runner;
mod simulator;

pub use allocator::{
    Allocation, AllocationInput, AllocationOutput, CandidateMeta, GreedyPortfolioAllocator,
    PortfolioAllocator,
};
pub use comparison::{CategoryRankIcDelta, ModelComparisonReport, compare_reports};
pub use runner::GreedyBacktester;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use quant_pivot_error::QuantResult;
use quant_pivot_models::{
    enums::{
        common::MarketCategory,
        quant::{DataQualityStatus, SignalSide},
    },
    runtime_config::PortfolioConfig,
    types::{
        BacktestReportId, ContentHash, EventId, MarketId, ModelVersionId, Probability,
        RuntimeConfigVersionId, TokenId, Usd,
    },
};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

use crate::{
    features::SubstitutionAudit,
    model::{QuantModelRuntime, runtime::ModelRuntimeInput},
};

/// Identity + window of a backtest run (the report's stable header).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BacktestRequest {
    /// Pre-minted report id.
    pub backtest_report_id: BacktestReportId,
    /// Model version under test.
    pub model_version_id: ModelVersionId,
    /// Frozen runtime-config version governing the replay.
    pub runtime_config_version_id: RuntimeConfigVersionId,
    /// Inclusive window start.
    pub window_start: DateTime<Utc>,
    /// Exclusive window end.
    pub window_end: DateTime<Utc>,
}

/// Per-market metadata for one tick (allocation caps + category breakdown).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BacktestMarketMeta {
    /// Market id.
    pub market_id: MarketId,
    /// Market category (domain breakdown + category exposure caps).
    pub category: MarketCategory,
    /// Owning event (per-event exposure caps), when known.
    pub event_id: Option<EventId>,
    /// Visible liquidity (liquidity-usage cap), when known.
    pub liquidity_usd: Option<Usd>,
}

/// The realized settlement outcome of a market, resolved from the 3.5
/// `market_resolution_event` truth at/after the label horizon.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MarketOutcome {
    /// Market id.
    pub market_id: MarketId,
    /// Whether the YES token won (`winning_token_id == yes_token_id`).
    pub settled_yes: bool,
    /// Whether the market had matured (resolved) by the resolution cutoff. An
    /// unmatured market contributes no realized sample (never zero-filled).
    pub matured: bool,
}

/// One replay tick: the model input + realized outcomes + market metadata, all
/// resolved point-in-time by the core orchestrator.
///
/// The model input is whichever shape the model under test consumes — a
/// `FactorTable` for the weighted scorer or a `FeatureMatrix` for a classical
/// model — assembled from the same PIT-resolved cross-section, so a classical
/// candidate is backtested through the identical computation graph.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BacktestTick {
    /// Decision time.
    pub as_of: DateTime<Utc>,
    /// The model's batch input for this tick (PIT-resolved features/factors).
    pub model_input: ModelRuntimeInput,
    /// Realized settlement outcomes for the tick's markets.
    pub outcomes: Vec<MarketOutcome>,
    /// Per-market metadata for allocation + breakdown.
    pub market_meta: Vec<BacktestMarketMeta>,
}

/// Portfolio budget / exposure / liquidity caps (projected from `PortfolioConfig`).
///
/// `total_budget_usd`, `max_single_recommendation_usd`, and `min_recommendation_usd`
/// are literal (a zero budget allocates nothing); the per-market / per-event /
/// per-category caps are treated as unlimited when non-positive (unconfigured).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PortfolioCaps {
    /// Total capital available across the report.
    pub total_budget_usd: Decimal,
    /// Maximum USD allocated to a single recommendation.
    pub max_single_recommendation_usd: Decimal,
    /// Minimum useful recommendation size (smaller intents are dropped).
    pub min_recommendation_usd: Decimal,
    /// Maximum USD exposure per market (unlimited when `<= 0`).
    pub max_market_exposure_usd: Decimal,
    /// Maximum USD exposure per event (unlimited when `<= 0`).
    pub max_event_exposure_usd: Decimal,
    /// Maximum USD exposure per category (unlimited when `<= 0`).
    pub max_category_exposure_usd: Decimal,
    /// Fraction of visible liquidity an allocation may consume (`[0, 1]`).
    pub liquidity_usage_cap_pct: Decimal,
}

impl From<&PortfolioConfig> for PortfolioCaps {
    /// Project the runtime-config portfolio section into allocator caps.
    ///
    /// Each `DecimalString` is parsed leniently (an unparseable value collapses
    /// to zero, i.e. "no budget / no cap"); runtime-config validation rejects
    /// malformed decimals upstream, so a fail-open here is unreachable in
    /// practice and never silently widens a cap. `max_correlated_exposure_usd`
    /// and the confidence/drawdown curves are report-builder concerns and are
    /// intentionally not part of the allocator's caps.
    fn from(config: &PortfolioConfig) -> Self {
        let decimal = |value: &str| value.parse::<Decimal>().unwrap_or(Decimal::ZERO);
        let budget = &config.budget;
        let constraints = &config.constraints;
        Self {
            total_budget_usd: decimal(&budget.total_budget_usd.value),
            max_single_recommendation_usd: decimal(&budget.max_single_recommendation_usd.value),
            min_recommendation_usd: decimal(&budget.min_recommendation_usd.value),
            max_market_exposure_usd: decimal(&constraints.max_market_exposure_usd.value),
            max_event_exposure_usd: decimal(&constraints.max_event_exposure_usd.value),
            max_category_exposure_usd: decimal(&constraints.max_category_exposure_usd.value),
            liquidity_usage_cap_pct: decimal(&constraints.liquidity_usage_cap_pct.value),
        }
    }
}

/// Inputs to a backtest run. The model runtime + ticks are borrowed/owned
/// in-memory; nothing here can reach a live source.
pub struct BacktestInputs<'a> {
    /// Report identity + window.
    pub request: BacktestRequest,
    /// The model runtime under test (already hash/schema-validated by the factory).
    pub model: &'a dyn QuantModelRuntime,
    /// Time-ordered replay ticks.
    pub ticks: Vec<BacktestTick>,
    /// Portfolio caps for the greedy allocator.
    pub caps: PortfolioCaps,
}

/// The expected-vs-realized agreement summary.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExpectedVsRealized {
    /// Mean predicted expected return (bps).
    pub mean_expected_bps: Decimal,
    /// Mean realized return (bps).
    pub mean_realized_bps: Decimal,
    /// Pearson correlation between predicted and realized return.
    pub correlation: Decimal,
    /// Mean prediction bias (`expected - realized`, bps).
    pub bias_bps: Decimal,
}

/// Per-category performance breakdown (domain slice diagnostics).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CategoryMetric {
    /// Market category.
    pub category: MarketCategory,
    /// Resolved samples in this category.
    pub sample_count: u64,
    /// Rank IC within the category.
    pub rank_ic: Decimal,
    /// Hit rate within the category.
    pub hit_rate: Probability,
    /// Mean realized return (bps) within the category.
    pub mean_realized_bps: Decimal,
}

/// One point of the equity curve (cumulative realized `PnL` after a tick).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EquityPoint {
    /// Tick decision time.
    pub as_of: DateTime<Utc>,
    /// Cumulative realized `PnL` (USD) through this tick.
    pub equity_usd: Decimal,
}

/// Portfolio-level `PnL` simulation summary.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PnlSimulation {
    /// Total capital allocated across all ticks.
    pub total_allocated_usd: Decimal,
    /// Total realized `PnL` (USD).
    pub realized_pnl_usd: Decimal,
    /// Realized `PnL` as a fraction of total allocated capital.
    pub gross_return: Decimal,
    /// Cumulative realized-PnL equity curve.
    pub equity_curve: Vec<EquityPoint>,
}

/// A point-in-time backtest report (the persisted, content-addressed summary).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BacktestReport {
    /// Report id.
    pub backtest_report_id: BacktestReportId,
    /// Model version under test.
    pub model_version_id: ModelVersionId,
    /// Runtime-config version governing the replay.
    pub runtime_config_version_id: RuntimeConfigVersionId,
    /// Inclusive window start.
    pub window_start: DateTime<Utc>,
    /// Exclusive window end.
    pub window_end: DateTime<Utc>,
    /// Fraction of emitted candidates that matured into resolved samples.
    pub coverage: Decimal,
    /// Resolved (matured) sample count.
    pub sample_count: u64,
    /// Count of missing-factor warnings across the replay.
    pub missing_feature_count: u64,
    /// Spearman rank IC between composite score and realized return.
    pub rank_ic: Decimal,
    /// Fraction of resolved samples with a positive realized return.
    pub hit_rate: Probability,
    /// Expected-vs-realized agreement.
    pub expected_vs_realized: ExpectedVsRealized,
    /// Maximum equity drawdown as a fraction of the total budget.
    pub max_drawdown: Decimal,
    /// Mean per-tick portfolio turnover.
    pub turnover: Decimal,
    /// Fraction of allocations that respected the liquidity-usage cap.
    pub liquidity_feasibility: Probability,
    /// Per-category breakdown.
    pub category_breakdown: Vec<CategoryMetric>,
    /// Conditional mean of the worst-decile realized returns (tail loss, bps).
    pub tail_loss: Decimal,
    /// Portfolio `PnL` simulation.
    pub report_pnl_simulation: PnlSimulation,
    /// Canonical hash over every field above.
    pub report_hash: ContentHash,
}

/// One resolved candidate outcome, retained for calibration + optional Parquet.
///
/// Not part of the persisted summary (so it does not enter `report_hash`); the
/// core calibration step maps these to `CalibrationSample`s.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SampleOutcome {
    /// Decision time.
    pub as_of: DateTime<Utc>,
    /// Market id.
    pub market_id: MarketId,
    /// Outcome token id.
    pub token_id: TokenId,
    /// Market category.
    pub category: MarketCategory,
    /// Directional action.
    pub side: SignalSide,
    /// Composite ranking score.
    pub composite_score: Probability,
    /// Model confidence.
    pub confidence: Probability,
    /// Predicted expected return (bps).
    pub expected_return_bps: Decimal,
    /// Realized return (bps).
    pub realized_return_bps: Decimal,
    /// Capital allocated to this candidate (USD).
    pub allocated_usd: Usd,
    /// Whether the allocation respected the liquidity-usage cap.
    pub liquidity_feasible: bool,
    /// Data-quality stratum the candidate was scored under (calibration input —
    /// never assumed `Fresh`; carried from the PIT-resolved scoring context).
    pub data_quality: DataQualityStatus,
    /// Visible liquidity (USD) at decision time, when known (liquidity stratum).
    pub liquidity_usd: Option<Usd>,
    /// Seconds until market resolution at decision time, when known (horizon stratum).
    pub time_to_resolution_secs: Option<u64>,
    /// The model's frozen prediction horizon (seconds); the horizon-ratio denominator.
    pub prediction_horizon_secs: u64,
    /// Audited feature substitutions on the scored vector (substitution stratum).
    pub substitutions: Vec<SubstitutionAudit>,
}

/// The result of a backtest run: the persisted report + per-sample outcomes.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BacktestRunResult {
    /// The content-addressed summary report.
    pub report: BacktestReport,
    /// Per-sample resolved outcomes (calibration input; not in `report_hash`).
    pub sample_outcomes: Vec<SampleOutcome>,
}

/// Runs a point-in-time backtest of a model version.
#[async_trait]
pub trait Backtester: Send + Sync {
    /// Execute the backtest and produce a report + per-sample outcomes.
    async fn run(&self, inputs: BacktestInputs<'_>) -> QuantResult<BacktestRunResult>;
}
