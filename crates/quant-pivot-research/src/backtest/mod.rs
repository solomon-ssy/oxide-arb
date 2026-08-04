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

use std::collections::BTreeMap;

use async_trait::async_trait;
pub use calendarize::{
    CalendarReturn, active_observation_count, calendarize_lot_returns, mean_calendar_return,
};
use chrono::{DateTime, Duration as ChronoDuration, Utc};
pub use comparison::ModelComparisonReport;
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
        BacktestReportId, Bps, ContentHash, DecisionPolicySnapshotId, EventId, MarketId,
        ModelVersionId, PayoutRatio, Price, Probability, Shares, TokenId, TrainingDatasetId, Usd,
        backtest::{CategoryMetric, ExpectedVsRealized, PnlSimulation},
    },
};
pub use runner::PortfolioReplayBacktester;
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

use crate::{
    execution_semantics::PitFeeSchedule,
    features::NullReason,
    model::{ModelRankTarget, QuantModelRuntime, runtime::ModelRuntimeInput},
    precision::RESEARCH_DECIMAL_SCALE,
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

/// One immutable post-decision price observation used to derive empirical
/// downside for the exact outcome token emitted by a replayed model.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BacktestDownsidePoint {
    pub at: DateTime<Utc>,
    pub best_bid_low: Option<Price>,
}

/// Frozen executable downside path for one `(market, token, decision)`.
///
/// The entry basis is the decision-time best ask (the price a buy-to-open
/// would actually cross), while forward observations use best-bid lows (the
/// executable side of a later exit). The model's own suggested horizon is
/// applied only after inference, so YES/NO side changes always consume the
/// matching token path instead of a market-level surrogate.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BacktestDownsideTrajectory {
    pub market_id: MarketId,
    pub token_id: TokenId,
    pub anchor: DateTime<Utc>,
    pub entry_ask: Price,
    pub data_available_until: DateTime<Utc>,
    pub points: Vec<BacktestDownsidePoint>,
}

impl BacktestDownsideTrajectory {
    /// Resolve non-positive MAE bps for the requested horizon.
    ///
    /// `None` is explicit unevaluable evidence: the frozen source did not
    /// mature through the horizon or contained no executable forward bid.
    pub fn max_adverse_excursion_bps(&self, horizon_secs: u64) -> QuantResult<Option<Decimal>> {
        let horizon_secs = i64::try_from(horizon_secs).map_err(|error| {
            QuantError::config(format!("backtest downside horizon exceeds i64: {error}"))
        })?;
        if horizon_secs <= 0 {
            return Err(QuantError::config(
                "backtest downside horizon must be positive",
            ));
        }
        let horizon_end = self
            .anchor
            .checked_add_signed(ChronoDuration::seconds(horizon_secs))
            .ok_or_else(|| QuantError::config("backtest downside horizon is outside chrono"))?;
        if self.data_available_until < horizon_end {
            return Ok(None);
        }
        if self.points.windows(2).any(|pair| pair[0].at >= pair[1].at)
            || self.points.iter().any(|point| point.at <= self.anchor)
        {
            return Err(QuantError::config(
                "backtest downside trajectory points are not strictly ordered after the anchor",
            ));
        }
        let lowest_bid = self
            .points
            .iter()
            .take_while(|point| point.at <= horizon_end)
            .filter_map(|point| point.best_bid_low)
            .map(Price::inner)
            .min();
        let Some(lowest_bid) = lowest_bid else {
            return Ok(None);
        };
        let entry = self.entry_ask.inner();
        if entry <= Decimal::ZERO {
            return Err(QuantError::config(
                "backtest downside trajectory entry ask must be positive",
            ));
        }
        let excursion = ((lowest_bid - entry) / entry * Decimal::from(10_000))
            .min(Decimal::ZERO)
            .round_dp(RESEARCH_DECIMAL_SCALE);
        Ok(Some(excursion))
    }
}

/// Identity + window of a backtest run (the report's stable header).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BacktestRequest {
    /// Pre-minted report id.
    pub backtest_report_id: BacktestReportId,
    /// Model version under test.
    pub model_version_id: ModelVersionId,
    /// Exact frozen dataset consumed by this replay.
    pub dataset_id: TrainingDatasetId,
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
    /// Exact YES-token payout when mature. `None` means no resolution was
    /// available by the frozen cutoff; it never means a zero payout.
    pub yes_payout_ratio: Option<PayoutRatio>,
}

/// One immutable supervised target from the replay dataset.
///
/// Rank-quality validation joins this value only to a [`ModelRankScore`](crate::model::ModelRankScore)
/// carrying the exact same target binding and canonical token identity.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BacktestRankTarget {
    pub market_id: MarketId,
    pub token_id: TokenId,
    pub target: ModelRankTarget,
    pub realized: Decimal,
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
    /// Canonical supervised targets for allocation-independent rank validation.
    pub rank_targets: Vec<BacktestRankTarget>,
    /// Per-market metadata for allocation + breakdown.
    pub market_meta: Vec<BacktestMarketMeta>,
    /// Full-L2/PIT-fee execution inputs for every outcome token a model may buy.
    pub execution: Vec<BacktestExecutionSnapshot>,
    /// Token-specific post-decision downside paths from the same Source Slice.
    pub downside_trajectories: Vec<BacktestDownsideTrajectory>,
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
    /// Exact frozen dataset consumed by this replay.
    pub dataset_id: TrainingDatasetId,
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
    /// Conditional mean return of the worst realized-return decile, in bps.
    pub tail_loss: Decimal,
    /// Portfolio `PnL` simulation.
    pub report_pnl_simulation: PnlSimulation,
    /// Canonical hash over every field above.
    pub report_hash: ContentHash,
}

/// One allocated, executable, and resolved candidate outcome retained for
/// economic backtest and comparison metrics.
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
    /// Exact payout of the bought outcome token. Binary-only calibrators
    /// explicitly exclude fractional values instead of coercing them.
    pub token_payout_ratio: PayoutRatio,
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

/// Allocation-independent resolved model-score observation.
///
/// Probability calibration fits the model output over the complete
/// purpose-bound Calibration Dataset. Portfolio caps, optimizer selection, L2
/// fill capacity, and execution fees are downstream economic concerns and must
/// not censor the score/outcome population.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ModelCalibrationOutcome {
    pub decision_at: DateTime<Utc>,
    pub market_id: MarketId,
    pub token_id: TokenId,
    pub composite_score: Probability,
    pub token_payout_ratio: PayoutRatio,
    pub max_adverse_excursion_bps: Option<Decimal>,
}

/// Allocation-independent canonical ranking observation.
///
/// This is intentionally distinct from [`ModelCalibrationOutcome`]: the rank
/// score and realized value share the model's supervised target units, while
/// calibration remains a selected-side probability/outcome contract.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ModelRankOutcome {
    pub decision_at: DateTime<Utc>,
    pub market_id: MarketId,
    pub token_id: TokenId,
    pub score: Decimal,
    pub target: ModelRankTarget,
    pub realized: Decimal,
}

/// Net portfolio result for one governed decision tick.
///
/// Comparison keeps every decision tick, including genuine no-allocation
/// ticks whose realized `PnL` and return are exactly zero. The positive governed
/// capital base is shared across champion and challengers and is never replaced
/// by allocated capital.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PortfolioReturnObservation {
    pub decision_at: DateTime<Utc>,
    pub realized_pnl_usd: Usd,
    pub capital_base_usd: Usd,
    pub net_return_bps: Bps,
}

/// The result of a backtest run: the persisted report plus purpose-specific
/// score, execution, and portfolio observations.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BacktestRunResult {
    /// The content-addressed summary report.
    pub report: BacktestReport,
    /// Resolved model-score outcomes before portfolio allocation. This is the
    /// calibration input and is intentionally not part of `report_hash`.
    pub calibration_outcomes: Vec<ModelCalibrationOutcome>,
    /// Canonical score/label evidence for rank-quality validation. This field
    /// is intentionally not part of `report_hash`.
    pub rank_outcomes: Vec<ModelRankOutcome>,
    /// Resolved outcomes that were allocated and executed by the economic
    /// replay. These drive report and comparison metrics.
    pub sample_outcomes: Vec<SampleOutcome>,
    /// Complete same-window decision-tick portfolio-return series.
    pub portfolio_returns: Vec<PortfolioReturnObservation>,
    /// Canonical per-market allocation weights for every replay tick, in the
    /// same ascending decision-time order as `portfolio_returns`. CPCV uses
    /// these exact weights to reconstruct path-level turnover after stitching
    /// out-of-sample fold partitions; averaging fold-level turnover would lose
    /// the transitions at partition boundaries.
    pub tick_weights: Vec<BTreeMap<String, Decimal>>,
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
    fn malformed_portfolio_rejected_boundary() {
        serde_json::from_value::<DecimalValue>(serde_json::json!("not-a-decimal"))
            .expect_err("malformed cap must fail DTO deserialization");
    }
}
