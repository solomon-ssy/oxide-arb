//! Backtest plane: deterministic point-in-time replay of a model
//! version into a [`BacktestReport`].
//!
//! The replay is split cleanly across the crate boundary (D7): `quant-pivot-core`
//! materializes each tick's model input from a **historical PIT source**
//! (`MaterializedPitEngine` over prefetched `ClickHouse` facts — never the live
//! `BookStore`) plus the realized settlement outcomes, and this pure engine runs
//! model inference → the same global `HiGHS` MILP used by reporting → outcome
//! resolution → metrics. Because
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

use std::sync::Arc;

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
    config::PortfolioSolverDeployConfig,
    domain::{
        market::book::BookLevel,
        quant::{PortfolioScenarioModelArtifact, PortfolioScenarioVisibility, RepresentedRouteSet},
    },
    enums::{
        common::MarketCategory,
        quant::{AccountSource, DataQualityStatus, OutcomeSide},
    },
    hashing::CanonicalDigest,
    runtime_config::{BuyModelRoute, PortfolioConfig, PortfolioScenarioModelArtifactBinding},
    types::{
        BacktestReportId, Bps, ContentHash, DecisionPolicySnapshotId, EventId, MarketId,
        ModelVersionId, PayoutRatio, Price, Probability, ReportRouteRunId, Shares, TokenId,
        TrainingDatasetId, Usd,
        backtest::{BacktestPortfolioFunnel, CategoryMetric, ExpectedVsRealized, PnlSimulation},
    },
};
pub use runner::{ModelCalibrationReplay, PortfolioReplayBacktester};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

use crate::{
    execution_semantics::PitFeeSchedule,
    features::NullReason,
    model::{
        ModelRankTarget, QuantModelRuntime,
        runtime::{ModelRuntimeInput, ModelRuntimeOutput},
    },
    portfolio::{AccountSnapshot, VerifiedPortfolioScenarioModel, scenario_economic_function_hash},
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
    pub bids: Vec<BookLevel>,
    pub asks: Vec<BookLevel>,
    pub fee_schedule: PitFeeSchedule,
    pub fill_at: DateTime<Utc>,
    pub limit_price: Price,
    pub book_hash: ContentHash,
}

/// Exact decision-time executable liquidation state for one outcome token.
///
/// This contract is deliberately distinct from [`BacktestExecutionSnapshot`]:
/// entry eligibility is defined by the current model cross-section and asks,
/// while a self-financing replay must revalue every still-open position from
/// contemporaneous bids even after that token leaves the model universe.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BacktestLiquidationSnapshot {
    pub market_id: MarketId,
    pub token_id: TokenId,
    pub bids: Vec<BookLevel>,
    pub fee_schedule: PitFeeSchedule,
    pub marked_at: DateTime<Utc>,
    pub book_hash: ContentHash,
}

impl From<&BacktestExecutionSnapshot> for BacktestLiquidationSnapshot {
    /// Project one exact entry-boundary book into its sell-side mark view.
    ///
    /// This conversion does not populate later retention boundaries; the core
    /// orchestrator must still construct that independent time-series plane.
    fn from(snapshot: &BacktestExecutionSnapshot) -> Self {
        Self {
            market_id: snapshot.market_id.clone(),
            token_id: snapshot.token_id.clone(),
            bids: snapshot.bids.clone(),
            fee_schedule: snapshot.fee_schedule.clone(),
            marked_at: snapshot.fill_at,
            book_hash: snapshot.book_hash,
        }
    }
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
    /// Owning event (per-event exposure caps). Missing event identity invalidates the tick.
    pub event_id: EventId,
    /// Visible liquidity (liquidity-usage cap), when known.
    pub liquidity_usd: Option<Usd>,
}

/// The realized settlement outcome of a market, resolved from
/// `market_resolution_event` truth at or after the label horizon.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MarketOutcome {
    /// Market id.
    pub market_id: MarketId,
    /// Economic settlement time from the frozen canonical resolution fact.
    /// `None` is permitted only together with an unresolved payout.
    pub resolved_at: Option<DateTime<Utc>>,
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
    /// Exact executable marks for every token that could remain open from an
    /// earlier decision tick, independent of current model membership.
    pub liquidation: Vec<BacktestLiquidationSnapshot>,
    /// Token-specific post-decision downside paths from the same Source Slice.
    pub downside_trajectories: Vec<BacktestDownsideTrajectory>,
    /// Fully frozen economic/scenario contract for the unique global optimizer path.
    pub portfolio_contract: BacktestPortfolioContract,
}

/// Allocation-independent model input and truth for probability calibration.
///
/// A challenger must be calibrated before it can become a promoted Route
/// champion. Consequently this replay contract intentionally contains no
/// account, scenario-model, portfolio-policy, or active-serving binding. It
/// still consumes the exact frozen model input, settlement truth, and downside
/// paths used by the full replay engine.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CalibrationReplayTick {
    pub decision_at: DateTime<Utc>,
    pub model_input: ModelRuntimeInput,
    pub outcomes: Vec<MarketOutcome>,
    pub downside_trajectories: Vec<BacktestDownsideTrajectory>,
}

/// Frozen, complete global-portfolio contract for one replay decision tick.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BacktestPortfolioContract {
    pub report_route_run_id: ReportRouteRunId,
    pub route: BuyModelRoute,
    pub account: AccountSnapshot,
    pub represented_routes: RepresentedRouteSet,
    pub policy: PortfolioConfig,
    pub solver: PortfolioSolverDeployConfig,
    pub top_n: u32,
}

/// Immutable promoted scenario/methodology context shared by every tick in one replay.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BacktestPortfolioContext {
    pub report_route_run_id: ReportRouteRunId,
    pub route: BuyModelRoute,
    pub represented_routes: RepresentedRouteSet,
    pub policy: PortfolioConfig,
    pub solver: PortfolioSolverDeployConfig,
    pub top_n: u32,
}

/// Scenario estimator attached at replay execution rather than frozen into
/// reusable market/economic ticks.
///
/// CPCV can therefore attach one independent
/// fold-local estimator without ever materializing a future-informed promoted
/// model inside its immutable tick cache.
#[derive(Debug, PartialEq, Eq)]
struct VerifiedBacktestScenarioContract {
    binding: PortfolioScenarioModelArtifactBinding,
    model: PortfolioScenarioModelArtifact,
    represented_routes: RepresentedRouteSet,
    economic_function_hash: ContentHash,
}

/// Fully verified immutable scenario contract shared by every replay tick.
///
/// Construction performs the complete model/binding/Route-set integrity
/// audit once. Cloning this value only clones an [`Arc`]; it never deep-copies
/// the scenario state catalog or repeats cryptographic verification inside the
/// portfolio loop.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BacktestScenarioContext {
    contract: Arc<VerifiedBacktestScenarioContract>,
}

impl BacktestScenarioContext {
    /// Verify and freeze a scenario model for deterministic replay.
    pub fn try_new(
        binding: PortfolioScenarioModelArtifactBinding,
        model: PortfolioScenarioModelArtifact,
        represented_routes: RepresentedRouteSet,
    ) -> QuantResult<Self> {
        VerifiedPortfolioScenarioModel::verify(&binding, &model, &represented_routes)?;
        let economic_function_hash = scenario_economic_function_hash(&model)?;
        Ok(Self {
            contract: Arc::new(VerifiedBacktestScenarioContract {
                binding,
                model,
                represented_routes,
                economic_function_hash,
            }),
        })
    }

    #[must_use]
    pub fn binding(&self) -> &PortfolioScenarioModelArtifactBinding {
        &self.contract.binding
    }

    #[must_use]
    pub fn model(&self) -> &PortfolioScenarioModelArtifact {
        &self.contract.model
    }

    #[must_use]
    pub fn represented_routes(&self) -> &RepresentedRouteSet {
        &self.contract.represented_routes
    }

    /// Complete economics-only identity shared by lineage-distinct scenario artifacts.
    #[must_use]
    pub fn economic_function_hash(&self) -> ContentHash {
        self.contract.economic_function_hash
    }

    pub(crate) fn verified(&self) -> VerifiedPortfolioScenarioModel<'_> {
        VerifiedPortfolioScenarioModel::from_verified(
            &self.contract.binding,
            &self.contract.model,
            &self.contract.represented_routes,
        )
    }
}

impl BacktestPortfolioContext {
    /// Freeze one tick-specific account and exact zero-position preimage.
    pub fn contract(&self, decision_at: DateTime<Utc>) -> QuantResult<BacktestPortfolioContract> {
        let total_budget = Usd::new(self.policy.budget.total_budget_usd.value);
        Ok(BacktestPortfolioContract {
            report_route_run_id: self.report_route_run_id,
            route: self.route,
            account: AccountSnapshot::new(
                decision_at,
                AccountSource::HistoricalReplay,
                total_budget,
                total_budget,
                total_budget,
                Usd::ZERO,
                Vec::new(),
            ),
            represented_routes: self.represented_routes.clone(),
            policy: self.policy.clone(),
            solver: self.solver,
            top_n: self.top_n,
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
    /// Scenario estimator selected for this complete run. Standard replay uses
    /// a promoted PIT model; CPCV supplies one fold-local estimator.
    pub scenario: &'a BacktestScenarioContext,
    /// Explicit PIT versus purged-CV visibility semantics.
    pub scenario_visibility: PortfolioScenarioVisibility,
    /// Time-ordered replay ticks.
    pub ticks: Vec<BacktestTick>,
}

/// One fold-local OOS inference result retained until the complete CPCV path is assembled.
///
/// Scenario context is tick-specific because adjacent path partitions can
/// originate from different purged estimators.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PrecomputedBacktestTick {
    pub model_version_id: ModelVersionId,
    pub tick: BacktestTick,
    pub output: ModelRuntimeOutput,
    pub scenario: BacktestScenarioContext,
    pub scenario_visibility: PortfolioScenarioVisibility,
}

impl PrecomputedBacktestTick {
    /// Hash exactly the immutable inputs that can change stateful portfolio cash flows.
    ///
    /// Model inputs, calibration/rank diagnostics, runtime metrics, and artifact
    /// lineage are intentionally absent after OOS inference. Candidate economics,
    /// executable market state, policy, account state, scenario economics, and
    /// validation visibility remain fully committed. Equal digests therefore
    /// authorize deterministic replay reuse without collapsing governed trials.
    pub fn economic_replay_digest(&self) -> QuantResult<ContentHash> {
        let candidates = self
            .output
            .candidates
            .iter()
            .map(|candidate| {
                (
                    candidate.signal_candidate_id,
                    candidate.model_run_id,
                    &candidate.market_id,
                    &candidate.token_id,
                    candidate.outcome_side,
                    candidate.payout_distribution,
                    candidate.suggested_horizon_secs,
                )
            })
            .collect::<Vec<_>>();
        CanonicalDigest::content_hash_typed(
            "quant-pivot/precomputed-backtest-economic-input",
            1,
            &(
                self.tick.decision_at,
                &self.tick.outcomes,
                &self.tick.rank_targets,
                &self.tick.market_meta,
                &self.tick.execution,
                &self.tick.liquidation,
                &self.tick.downside_trajectories,
                &self.tick.portfolio_contract,
                candidates,
                self.scenario.economic_function_hash(),
                self.scenario_visibility,
            ),
        )
        .map_err(QuantError::from)
    }
}

/// Complete timeline input to the stateful self-financing replay boundary.
pub struct PrecomputedBacktestInputs {
    pub request: BacktestRequest,
    pub ticks: Vec<PrecomputedBacktestTick>,
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
    /// Fraction of emitted candidates with mature canonical settlement truth;
    /// independent of whether the global portfolio funded them.
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
    /// Mean executed entry cash divided by frozen capital across every fixed-
    /// cadence decision tick. Settlement/redemption is not a second trade.
    pub turnover: Decimal,
    /// Fraction of allocations that respected the liquidity-usage cap.
    pub liquidity_feasibility: Probability,
    /// Per-category breakdown.
    pub category_breakdown: Vec<CategoryMetric>,
    /// Conditional mean return of the worst realized-return decile, in bps.
    pub tail_loss: Decimal,
    /// Portfolio `PnL` simulation.
    pub report_pnl_simulation: PnlSimulation,
    /// Count-conserving candidate → tier → admission → selection → execution funnel.
    pub portfolio_funnel: BacktestPortfolioFunnel,
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
    /// Executed entry cash outlay divided by the frozen capital base for every
    /// replay tick, in the same ascending order as `portfolio_returns`.
    pub tick_cash_turnover: Vec<Decimal>,
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
