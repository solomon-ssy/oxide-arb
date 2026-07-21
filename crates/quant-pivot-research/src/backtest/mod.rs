//! Backtest plane: deterministic point-in-time replay of a model
//! version into a [`BacktestReport`].
//!
//! The replay is split cleanly across the crate boundary (D7): `quant-pivot-core`
//! materializes each tick's model input from a **historical PIT source**
//! (`MaterializedPitEngine` over prefetched `ClickHouse` facts — never the live
//! `BookStore`) plus the realized settlement outcomes, and this pure engine runs
//! model inference → LP/MILP allocation → outcome resolution → metrics. Because
//! the engine has no access to a `BookStore` or any live source, "no live
//! `BookStore` in a backtest" is structurally guaranteed.
//!
//! Money / price / probability stay in project newtypes; `f64` never appears.

mod calendarize;
mod comparison;
mod lot_replay;
pub(crate) mod metrics;
mod runner;
mod simulator;

use async_trait::async_trait;
pub use calendarize::{
    CalendarReturn, active_observation_count, calendarize_lot_returns, mean_calendar_return,
};
use chrono::{DateTime, Utc};
pub use comparison::{ModelComparisonReport, compare_reports};
pub use lot_replay::{
    LotBacktestInputs, LotBacktestRunResult, LotBacktester, LotDecisionSequence, LotOutcome,
    LotReplayBacktester, SellNullBaseline, replay_lot_null_baseline,
};
pub use metrics::sharpe_ratio;
use quant_pivot_error::{QuantError, QuantResult};
use quant_pivot_models::{
    domain::market::book::BookLevel,
    enums::{
        common::MarketCategory,
        quant::{DataQualityStatus, OutcomeSide},
    },
    runtime_config::PortfolioConfig,
    types::{
        BacktestReportId, ContentHash, DecisionPolicySnapshotId, EventId, MarketId, ModelVersionId,
        Price, Probability, Shares, TokenId, Usd,
        backtest::{CategoryMetric, ExpectedVsRealized, PnlSimulation},
    },
};
pub use runner::PortfolioReplayBacktester;
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

use crate::{
    execution_semantics::PitFeeSchedule,
    features::NullReason,
    model::{QuantModelRuntime, runtime::ModelRuntimeInput},
};

/// Exact decision-time execution inputs for one outcome token.
///
/// Core reconstructs these from the Dataset-bound Source Slice. The pure
/// backtester can therefore walk the same full L2 and PIT fee schedule as
/// report composition, policy replay, and admission without any repository or
/// live-book access.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BacktestExecutionSnapshot {
    pub market_id: MarketId,
    pub token_id: TokenId,
    pub asks: Vec<BookLevel>,
    pub fee_schedule: PitFeeSchedule,
    pub fill_at: DateTime<Utc>,
    pub limit_price: Price,
    pub book_hash: ContentHash,
}

/// Identity + window of a backtest run (the report's stable header).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BacktestRequest {
    /// Pre-minted report id.
    pub backtest_report_id: BacktestReportId,
    /// Model version under test.
    pub model_version_id: ModelVersionId,
    /// Frozen runtime-config version governing the replay.
    pub decision_policy_snapshot_id: DecisionPolicySnapshotId,
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

/// The realized settlement outcome of a market, resolved from
/// `market_resolution_event` truth at or after the label horizon.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MarketOutcome {
    /// Market id.
    pub market_id: MarketId,
    /// Whether the YES token won (`winning_token_id == yes_token_id`).
    pub settled_yes: bool,
    /// Whether the market had matured (resolved) by the resolution cutoff. An
    /// unmatured market contributes no realized sample (never zero-filled).
    pub matured: bool,
    /// `max_adverse_excursion_bps` label value, when materialized: the
    /// calibration downside source and empirical worst intra-horizon
    /// unfavorable move, distinct from the binary settlement outcome).
    pub max_adverse_excursion_bps: Option<Decimal>,
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
    pub decision_at: DateTime<Utc>,
    /// The model's batch input for this tick (PIT-resolved features/factors).
    pub model_input: ModelRuntimeInput,
    /// Realized settlement outcomes for the tick's markets.
    pub outcomes: Vec<MarketOutcome>,
    /// Per-market metadata for allocation + breakdown.
    pub market_meta: Vec<BacktestMarketMeta>,
    /// Full-L2/PIT-fee execution inputs for every outcome token a model may buy.
    pub execution: Vec<BacktestExecutionSnapshot>,
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
    /// Total simultaneous exposure cap as a fraction of bankroll (`[0, 1]`).
    pub max_aggregate_exposure_pct: Decimal,
}

impl TryFrom<&PortfolioConfig> for PortfolioCaps {
    type Error = QuantError;

    /// Project the runtime-config portfolio section into allocator caps.
    ///
    /// `max_correlated_exposure_usd` and the confidence/drawdown curves are
    /// report-builder concerns and are intentionally not part of the
    /// allocator's caps.
    fn try_from(config: &PortfolioConfig) -> Result<Self, Self::Error> {
        let budget = &config.budget;
        let constraints = &config.constraints;
        Ok(Self {
            total_budget_usd: budget.total_budget_usd.value,
            max_single_recommendation_usd: budget.max_single_recommendation_usd.value,
            min_recommendation_usd: budget.min_recommendation_usd.value,
            max_market_exposure_usd: constraints.max_market_exposure_usd.value,
            max_event_exposure_usd: constraints.max_event_exposure_usd.value,
            max_category_exposure_usd: constraints.max_category_exposure_usd.value,
            liquidity_usage_cap_pct: constraints.liquidity_usage_cap_pct.value,
            max_aggregate_exposure_pct: config.kelly_safety.max_aggregate_exposure_pct.value,
        })
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

/// A point-in-time backtest report (the persisted, content-addressed summary).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BacktestReport {
    /// Report id.
    pub backtest_report_id: BacktestReportId,
    /// Model version under test.
    pub model_version_id: ModelVersionId,
    /// Frozen decision-policy snapshot governing the replay.
    pub decision_policy_snapshot_id: DecisionPolicySnapshotId,
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
    /// Unannualized Sharpe ratio of the per-tick portfolio return series
    /// (`0` for fewer than two ticks or a zero-variance
    /// series). The single-path replay's own Sharpe — the debug-view sibling
    /// of the CPCV path-set's `SharpeDistribution`,
    /// never itself the alpha-significance gate's data source.
    pub sharpe: Decimal,
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
    pub decision_at: DateTime<Utc>,
    /// Market id.
    pub market_id: MarketId,
    /// Outcome token id.
    pub token_id: TokenId,
    /// Market category.
    pub category: MarketCategory,
    /// Outcome side opened (always buy-to-open; `Yes`/`No`).
    pub outcome_side: OutcomeSide,
    /// Composite ranking score.
    pub composite_score: Probability,
    /// Model confidence.
    pub confidence: Probability,
    /// Predicted expected return (bps).
    pub expected_return_bps: Decimal,
    /// Realized return (bps).
    pub realized_return_bps: Decimal,
    /// Whether the YES token won (side-independent ground truth;
    /// `ProbabilityCalibrator` fits on this, not on realized-return sign).
    pub settled_yes: bool,
    /// `max_adverse_excursion_bps` label value, when materialized; this is the
    /// `DownsideSource::MfeMae` input.
    pub max_adverse_excursion_bps: Option<Decimal>,
    /// Capital allocated to this candidate (USD).
    pub allocated_usd: Usd,
    /// Exact entry fee charged by the PIT schedule at walk precision.
    pub entry_fee_usd: Usd,
    /// Shares actually acquired by the cash-budget walk.
    pub filled_shares: Shares,
    /// Content identity of the PIT fee schedule used by the walk.
    pub fee_schedule_hash: ContentHash,
    /// Content identity of the full L2 snapshot used by the walk.
    pub book_hash: ContentHash,
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
    /// Reasons of substituted feature cells on the scored vector.
    pub substitution_reasons: Vec<NullReason>,
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

#[cfg(test)]
mod tests {
    use quant_pivot_models::runtime_config::DecimalValue;

    #[test]
    fn malformed_portfolio_cap_is_rejected_at_the_wire_boundary() {
        serde_json::from_value::<DecimalValue>(serde_json::json!("not-a-decimal"))
            .expect_err("malformed cap must fail DTO deserialization");
    }
}
