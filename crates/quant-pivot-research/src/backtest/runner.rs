//! [`PortfolioReplayBacktester`]: the deterministic PIT replay loop.
//!
//! Per tick: model inference (over the PIT-resolved factor table) → LP/MILP
//! allocation → outcome resolution against settled truth → metric accumulation.
//! The engine never touches a live `BookStore`; its only inputs are the
//! in-memory ticks and the model runtime. It pins the deterministic continuous
//! relaxation mode on the pure-Rust microlp backend
//! ([`backtest_optimizer`](crate::portfolio::backtest_optimizer)) so the report
//! hash is reproducible and build-independent.

use std::{collections::BTreeMap, sync::Arc};

use async_trait::async_trait;
use quant_pivot_error::{QuantError, QuantResult, research::ResearchError};
use quant_pivot_models::{
    enums::quant::{DataQualityStatus, FillRequirement, OutcomeSide},
    types::{
        Bps, ContentHash, ExposureBreakdown, MarketId, PayoutRatio, TokenId, Usd,
        backtest::{BacktestReportHashInput, PnlCurvePoint, PnlSimulation},
    },
};
use rust_decimal::Decimal;

use crate::{
    backtest::{
        BacktestDownsideTrajectory, BacktestExecutionSnapshot, BacktestInputs, BacktestMarketMeta,
        BacktestRankTarget, BacktestReport, BacktestRequest, BacktestRunResult, BacktestTick,
        Backtester, MarketOutcome, ModelCalibrationOutcome, ModelRankOutcome, PortfolioCaps,
        PortfolioReturnObservation, SampleOutcome, metrics, simulator,
    },
    execution_semantics::{BookWalkOutcome, LiquidityRole, walk_buy_cash_budget},
    model::{
        SignalCandidate,
        runtime::{MarketInferenceContext, ModelInputAuditState, ModelRuntimeOutput},
    },
    portfolio::{
        allocator::{
            Allocation, AllocationInput, AllocationOutput, CandidateMeta, PortfolioAllocator,
        },
        optimizer::backtest_optimizer,
    },
    precision::RESEARCH_DECIMAL_SCALE,
};

/// The lower tail fraction used for the tail-loss (`CVaR`) metric.
fn tail_quantile() -> Decimal {
    Decimal::new(10, 2) // 0.10
}

/// Deterministic PIT-replay backtester over the LP/MILP portfolio allocator.
#[derive(Clone)]
pub struct PortfolioReplayBacktester {
    allocator: Arc<dyn PortfolioAllocator>,
}

impl PortfolioReplayBacktester {
    /// Construct the backtester with the pinned deterministic relaxation allocator.
    #[must_use]
    pub fn new() -> Self {
        Self {
            allocator: backtest_optimizer(),
        }
    }
}

impl Default for PortfolioReplayBacktester {
    fn default() -> Self {
        Self::new()
    }
}

/// Mutable replay accumulators threaded across ticks.
#[derive(Default)]
struct RunAccumulator {
    calibration_outcomes: Vec<ModelCalibrationOutcome>,
    rank_outcomes: Vec<ModelRankOutcome>,
    samples: Vec<SampleOutcome>,
    pnl_curve: Vec<PnlCurvePoint>,
    tick_weights: Vec<BTreeMap<String, Decimal>>,
    /// Per-tick return (`tick_pnl / tick_allocated`), `0` for a tick with no
    /// allocation — the Sharpe-ratio input series.
    tick_returns: Vec<Decimal>,
    portfolio_returns: Vec<PortfolioReturnObservation>,
    missing_feature_count: u64,
    total_emitted: u64,
    total_allocated: Decimal,
    realized_pnl: Decimal,
}

#[async_trait]
impl Backtester for PortfolioReplayBacktester {
    async fn run(&self, inputs: BacktestInputs<'_>) -> QuantResult<BacktestRunResult> {
        let mut ticks = inputs.ticks;
        ticks.sort_by_key(|tick| tick.decision_at);

        let mut acc = RunAccumulator::default();
        for tick in &ticks {
            let output = inputs.model.infer_batch(tick.model_input.clone()).await?;
            process_tick(
                tick,
                &output,
                self.allocator.as_ref(),
                &inputs.caps,
                &mut acc,
            )?;
        }

        let metrics = BuildMetrics {
            samples: &acc.samples,
            pnl_curve: &acc.pnl_curve,
            tick_weights: &acc.tick_weights,
            tick_returns: &acc.tick_returns,
            missing_feature_count: acc.missing_feature_count,
            total_emitted: acc.total_emitted,
            total_allocated: acc.total_allocated,
            realized_pnl: acc.realized_pnl,
            budget: inputs.caps.total_budget_usd,
        };
        let report = build_report(&inputs.request, &metrics)?;
        Ok(BacktestRunResult {
            report,
            calibration_outcomes: acc.calibration_outcomes,
            rank_outcomes: acc.rank_outcomes,
            sample_outcomes: acc.samples,
            portfolio_returns: acc.portfolio_returns,
            tick_weights: acc.tick_weights,
        })
    }
}

/// Process one replay tick: allocate over the inferred candidates, resolve their
/// realized outcomes against settled truth, and fold the results into `acc`.
fn process_tick(
    tick: &BacktestTick,
    output: &ModelRuntimeOutput,
    allocator: &dyn PortfolioAllocator,
    caps: &PortfolioCaps,
    acc: &mut RunAccumulator,
) -> QuantResult<()> {
    acc.total_emitted += output.candidates.len() as u64;
    record_missing_input_count(acc, output)?;

    let meta: BTreeMap<&str, &BacktestMarketMeta> = tick
        .market_meta
        .iter()
        .map(|m| (m.market_id.as_str(), m))
        .collect();
    let outcomes: BTreeMap<&str, &MarketOutcome> = tick
        .outcomes
        .iter()
        .map(|o| (o.market_id.as_str(), o))
        .collect();
    // The PIT-resolved scoring context per market, so each realized sample
    // records the *actual* data-quality / liquidity / horizon / substitution
    // stratum it was scored under (never a hardcoded `Fresh`).
    let context: BTreeMap<&str, &MarketInferenceContext> = tick
        .model_input
        .market_contexts()
        .into_iter()
        .map(|(market_id, context)| (market_id.as_str(), context))
        .collect();
    let execution: BTreeMap<&str, &BacktestExecutionSnapshot> = tick
        .execution
        .iter()
        .map(|snapshot| (snapshot.token_id.as_str(), snapshot))
        .collect();
    let downside: BTreeMap<&str, &BacktestDownsideTrajectory> = tick
        .downside_trajectories
        .iter()
        .map(|trajectory| (trajectory.token_id.as_str(), trajectory))
        .collect();

    record_calibration_outcomes(tick, output, &outcomes, &downside, acc)?;
    record_rank_outcomes(tick, output, acc)?;

    let allocation = allocate_tick(output, allocator, caps, &meta)?;
    let alloc_by_id: BTreeMap<String, &Allocation> = allocation
        .allocations
        .iter()
        .map(|a| (a.signal_candidate_id.to_string(), a))
        .collect();
    let mut tick_pnl = Decimal::ZERO;
    let mut tick_allocated = Decimal::ZERO;
    let mut executed_allocations = Vec::new();
    for candidate in &output.candidates {
        let (Some(market), Some(outcome)) = (
            meta.get(candidate.market_id.as_str()),
            outcomes.get(candidate.market_id.as_str()),
        ) else {
            continue;
        };
        let Some(token_payout_ratio) = token_payout(outcome, candidate.outcome_side) else {
            continue;
        };
        let Some(allocation) = alloc_by_id.get(&candidate.signal_candidate_id.to_string()) else {
            continue;
        };
        if !allocation.allocated_usd.is_positive() {
            continue;
        }
        let snapshot = execution.get(candidate.token_id.as_str()).ok_or_else(|| {
            ResearchError::ValidationMethodology {
                detail: format!(
                    "backtest tick {} has no executable PIT snapshot for token {}",
                    tick.decision_at, candidate.token_id
                ),
            }
        })?;
        if snapshot.market_id != candidate.market_id {
            return Err(ResearchError::ValidationMethodology {
                detail: format!(
                    "backtest execution snapshot token {} is bound to market {}, expected {}",
                    candidate.token_id, snapshot.market_id, candidate.market_id
                ),
            }
            .into());
        }
        let fill = walk_buy_cash_budget(
            &snapshot.asks,
            allocation.allocated_usd,
            snapshot.limit_price,
            FillRequirement::AllOrNothing,
            &snapshot.fee_schedule,
            LiquidityRole::Taker,
            snapshot.fill_at,
        )
        .map_err(|error| ResearchError::ValidationMethodology {
            detail: format!(
                "backtest executable entry walk failed for token {}: {error:?}",
                candidate.token_id
            ),
        })?;
        if fill.outcome == BookWalkOutcome::Unfilled {
            continue;
        }
        let settlement =
            simulator::settle_executed_buy(&fill, token_payout_ratio).map_err(|error| {
                ResearchError::ValidationMethodology {
                    detail: format!(
                        "backtest executable settlement failed for token {}: {error:?}",
                        candidate.token_id
                    ),
                }
            })?;
        let allocated_usd = settlement.economics.cash_outlay;
        let realized = settlement.realized_return_bps.inner();
        let liquidity_feasible = allocation.liquidity_feasible;
        tick_pnl += settlement.realized_pnl_usd.inner();
        acc.total_allocated += allocated_usd.inner();
        tick_allocated += allocated_usd.inner();
        executed_allocations.push((candidate.market_id.as_str(), allocated_usd.inner()));
        let market_context = context.get(candidate.market_id.as_str());
        acc.samples.push(SampleOutcome {
            decision_at: tick.decision_at,
            market_id: candidate.market_id.clone(),
            token_id: candidate.token_id.clone(),
            category: market.category,
            outcome_side: candidate.outcome_side,
            composite_score: candidate.composite_score,
            confidence: candidate.confidence,
            expected_return_bps: candidate.expected_return_bps,
            realized_return_bps: realized,
            token_payout_ratio,
            max_adverse_excursion_bps: candidate_downside(candidate, &downside)?,
            allocated_usd,
            entry_fee_usd: settlement.economics.entry_fee,
            filled_shares: settlement.economics.filled_shares,
            fee_schedule_hash: snapshot.fee_schedule.schedule_hash,
            book_hash: snapshot.book_hash,
            liquidity_feasible,
            data_quality: market_context
                .map_or(DataQualityStatus::Insufficient, |c| c.data_quality),
            liquidity_usd: market_context.and_then(|c| c.liquidity_usd),
            time_to_resolution_secs: market_context.and_then(|c| c.time_to_resolution_secs),
            prediction_horizon_secs: candidate.suggested_horizon_secs,
            substitution_reasons: market_context
                .map_or_else(Vec::new, |c| c.substitution_reasons.clone()),
        });
    }
    acc.tick_weights
        .push(weights_for_executed_tick(&executed_allocations));
    acc.realized_pnl += tick_pnl;
    acc.pnl_curve.push(PnlCurvePoint {
        decision_at: tick.decision_at,
        cumulative_realized_pnl_usd: acc.realized_pnl.round_dp(RESEARCH_DECIMAL_SCALE),
    });
    record_portfolio_return(tick, tick_pnl, caps, acc)?;
    // A tick with no allocation contributes no return observation (never a
    // silent zero-return sample, which would bias the Sharpe ratio's
    // variance downward).
    if tick_allocated > Decimal::ZERO {
        acc.tick_returns.push(tick_pnl / tick_allocated);
    }
    Ok(())
}

fn record_calibration_outcomes(
    tick: &BacktestTick,
    output: &ModelRuntimeOutput,
    outcomes: &BTreeMap<&str, &MarketOutcome>,
    downside: &BTreeMap<&str, &BacktestDownsideTrajectory>,
    acc: &mut RunAccumulator,
) -> QuantResult<()> {
    for score in &output.calibration_scores {
        let Some(outcome) = outcomes.get(score.market_id.as_str()) else {
            continue;
        };
        let Some(token_payout_ratio) = token_payout(outcome, score.outcome_side) else {
            continue;
        };
        acc.calibration_outcomes.push(ModelCalibrationOutcome {
            decision_at: tick.decision_at,
            market_id: score.market_id.clone(),
            token_id: score.token_id.clone(),
            composite_score: score.composite_score,
            token_payout_ratio,
            max_adverse_excursion_bps: score_downside(
                &score.market_id,
                &score.token_id,
                score.prediction_horizon_secs,
                downside,
            )?,
        });
    }
    Ok(())
}

fn record_rank_outcomes(
    tick: &BacktestTick,
    output: &ModelRuntimeOutput,
    acc: &mut RunAccumulator,
) -> QuantResult<()> {
    let mut targets = BTreeMap::<&str, &BacktestRankTarget>::new();
    for target in &tick.rank_targets {
        if targets.insert(target.market_id.as_str(), target).is_some() {
            return Err(ResearchError::ValidationMethodology {
                detail: format!(
                    "backtest tick {} duplicates rank target for market {}",
                    tick.decision_at, target.market_id
                ),
            }
            .into());
        }
    }
    for score in &output.rank_scores {
        let Some(target) = targets.get(score.market_id.as_str()) else {
            continue;
        };
        if target.token_id != score.token_id || target.target != score.target {
            return Err(ResearchError::ValidationMethodology {
                detail: format!(
                    "rank score/target binding mismatch for market {} at {}: \
                     score token {} target {:?}, dataset token {} target {:?}",
                    score.market_id,
                    tick.decision_at,
                    score.token_id,
                    score.target,
                    target.token_id,
                    target.target
                ),
            }
            .into());
        }
        acc.rank_outcomes.push(ModelRankOutcome {
            decision_at: tick.decision_at,
            market_id: score.market_id.clone(),
            token_id: score.token_id.clone(),
            score: score.score,
            target: score.target.clone(),
            realized: target.realized,
        });
    }
    Ok(())
}

fn candidate_downside(
    candidate: &SignalCandidate,
    trajectories: &BTreeMap<&str, &BacktestDownsideTrajectory>,
) -> QuantResult<Option<Decimal>> {
    score_downside(
        &candidate.market_id,
        &candidate.token_id,
        candidate.suggested_horizon_secs,
        trajectories,
    )
}

fn score_downside(
    market_id: &MarketId,
    token_id: &TokenId,
    prediction_horizon_secs: u64,
    trajectories: &BTreeMap<&str, &BacktestDownsideTrajectory>,
) -> QuantResult<Option<Decimal>> {
    let Some(trajectory) = trajectories.get(token_id.as_str()) else {
        return Ok(None);
    };
    if trajectory.market_id != *market_id {
        return Err(ResearchError::ValidationMethodology {
            detail: format!(
                "backtest downside trajectory token {} belongs to market {}, expected {}",
                token_id, trajectory.market_id, market_id
            ),
        }
        .into());
    }
    trajectory.max_adverse_excursion_bps(prediction_horizon_secs)
}

fn token_payout(outcome: &MarketOutcome, side: OutcomeSide) -> Option<PayoutRatio> {
    let yes_payout_ratio = outcome.yes_payout_ratio?;
    Some(match side {
        OutcomeSide::Yes => yes_payout_ratio,
        OutcomeSide::No => yes_payout_ratio.complement(),
    })
}

fn record_portfolio_return(
    tick: &BacktestTick,
    tick_pnl: Decimal,
    caps: &PortfolioCaps,
    acc: &mut RunAccumulator,
) -> QuantResult<()> {
    if caps.total_budget_usd <= Decimal::ZERO {
        return Ok(());
    }
    let net_return_bps = Bps::relative(tick_pnl, caps.total_budget_usd).ok_or_else(|| {
        ResearchError::ValidationMethodology {
            detail: "positive governed backtest capital unexpectedly produced no return ratio"
                .to_owned(),
        }
    })?;
    acc.portfolio_returns.push(PortfolioReturnObservation {
        decision_at: tick.decision_at,
        realized_pnl_usd: Usd::new(tick_pnl.round_dp(RESEARCH_DECIMAL_SCALE)),
        capital_base_usd: Usd::new(caps.total_budget_usd),
        net_return_bps: Bps::new(net_return_bps.inner().round_dp(RESEARCH_DECIMAL_SCALE)),
    });
    Ok(())
}

fn allocate_tick(
    output: &ModelRuntimeOutput,
    allocator: &dyn PortfolioAllocator,
    caps: &PortfolioCaps,
    meta: &BTreeMap<&str, &BacktestMarketMeta>,
) -> QuantResult<AllocationOutput> {
    let candidate_metas = backtest_candidate_metas(output, meta, caps);
    // Per-tick independent allocation (no inventory carry): `initial_exposures`
    // is always empty, correlation clustering is off, and every candidate is
    // eligible. Cross-tick netting is the production planner's job.
    let top_n = candidate_metas.len();
    allocator.allocate(&AllocationInput {
        candidates: candidate_metas,
        caps,
        initial_exposures: &ExposureBreakdown::default(),
        available_usd: caps.total_budget_usd,
        // Historical replay has no venue account. The governed test budget is
        // both available capital and capital base.
        capital_base_usd: caps.total_budget_usd,
        correlation: None,
        top_n,
    })
}

fn record_missing_input_count(
    acc: &mut RunAccumulator,
    output: &ModelRuntimeOutput,
) -> QuantResult<()> {
    let missing_input_count = output
        .input_audit
        .iter()
        .filter(|row| {
            matches!(
                row.raw_state,
                ModelInputAuditState::Missing | ModelInputAuditState::MissingInput
            )
        })
        .count();
    acc.missing_feature_count = acc
        .missing_feature_count
        .checked_add(u64::try_from(missing_input_count).map_err(|error| {
            ResearchError::ValidationMethodology {
                detail: format!("missing model-input count does not fit u64: {error}"),
            }
        })?)
        .ok_or_else(|| ResearchError::ValidationMethodology {
            detail: "missing model-input count overflowed u64".to_owned(),
        })?;
    Ok(())
}

/// Build the allocator metas for one tick: every candidate desires the
/// single-recommendation cap (flat sizing, preserving the original behavior).
fn backtest_candidate_metas<'a>(
    output: &'a ModelRuntimeOutput,
    meta: &BTreeMap<&str, &'a BacktestMarketMeta>,
    caps: &PortfolioCaps,
) -> Vec<CandidateMeta<'a>> {
    let desired = Usd::new(caps.max_single_recommendation_usd);
    output
        .candidates
        .iter()
        .filter_map(|candidate| {
            let market = meta.get(candidate.market_id.as_str())?;
            Some(CandidateMeta {
                candidate,
                desired_usd: desired,
                category: market.category,
                event_id: market.event_id.clone(),
                liquidity_usd: market.liquidity_usd,
            })
        })
        .collect()
}

/// Per-market allocation weights for one tick (normalized to the tick total).
fn weights_for_executed_tick(allocations: &[(&str, Decimal)]) -> BTreeMap<String, Decimal> {
    let total: Decimal = allocations.iter().map(|(_, amount)| *amount).sum();
    let mut weights = BTreeMap::new();
    if total > Decimal::ZERO {
        for (market_id, amount) in allocations {
            if *amount > Decimal::ZERO {
                *weights
                    .entry((*market_id).to_owned())
                    .or_insert(Decimal::ZERO) += *amount / total;
            }
        }
    }
    weights
}

/// Aggregated inputs for report assembly.
struct BuildMetrics<'a> {
    samples: &'a [SampleOutcome],
    pnl_curve: &'a [PnlCurvePoint],
    tick_weights: &'a [BTreeMap<String, Decimal>],
    tick_returns: &'a [Decimal],
    missing_feature_count: u64,
    total_emitted: u64,
    total_allocated: Decimal,
    realized_pnl: Decimal,
    budget: Decimal,
}

/// Assemble the metrics + canonical report hash.
fn build_report(request: &BacktestRequest, m: &BuildMetrics<'_>) -> QuantResult<BacktestReport> {
    let sample_count = m.samples.len() as u64;
    let coverage = if m.total_emitted > 0 {
        (Decimal::from(sample_count) / Decimal::from(m.total_emitted))
            .round_dp(RESEARCH_DECIMAL_SCALE)
    } else {
        Decimal::ZERO
    };
    let rank_ic = metrics::rank_ic(m.samples);
    let sharpe = metrics::sharpe_ratio(m.tick_returns, Decimal::ONE);
    let hit_rate = metrics::hit_rate(m.samples);
    let expected_vs_realized = metrics::expected_vs_realized(m.samples);
    let max_drawdown = metrics::max_drawdown(m.pnl_curve, m.budget);
    let turnover = metrics::turnover(m.tick_weights);
    let liquidity_feasibility = metrics::liquidity_feasibility(m.samples);
    let category_breakdown = metrics::category_breakdown(m.samples);
    let tail_loss = metrics::tail_loss(m.samples, tail_quantile())?;

    let gross_return = if m.total_allocated > Decimal::ZERO {
        (m.realized_pnl / m.total_allocated).round_dp(RESEARCH_DECIMAL_SCALE)
    } else {
        Decimal::ZERO
    };
    let report_pnl_simulation = PnlSimulation {
        total_allocated_usd: m.total_allocated.round_dp(RESEARCH_DECIMAL_SCALE),
        realized_pnl_usd: m.realized_pnl.round_dp(RESEARCH_DECIMAL_SCALE),
        gross_return,
        pnl_curve: m.pnl_curve.to_vec(),
    };

    let report_hash = BacktestReportHashInput {
        backtest_report_id: &request.backtest_report_id,
        model_version_id: &request.model_version_id,
        dataset_id: &request.dataset_id,
        decision_policy_snapshot_id: &request.decision_policy_snapshot_id,
        window_start: request.window_start,
        window_end: request.window_end,
        coverage,
        sample_count,
        missing_feature_count: m.missing_feature_count,
        rank_ic,
        sharpe,
        hit_rate,
        expected_vs_realized: &expected_vs_realized,
        max_drawdown,
        turnover,
        liquidity_feasibility,
        category_breakdown: &category_breakdown,
        tail_loss,
        report_pnl_simulation: &report_pnl_simulation,
    }
    .content_hash()?;

    Ok(BacktestReport {
        backtest_report_id: request.backtest_report_id,
        model_version_id: request.model_version_id,
        dataset_id: request.dataset_id,
        decision_policy_snapshot_id: request.decision_policy_snapshot_id,
        window_start: request.window_start,
        window_end: request.window_end,
        coverage,
        sample_count,
        missing_feature_count: m.missing_feature_count,
        rank_ic,
        sharpe,
        hit_rate,
        expected_vs_realized,
        max_drawdown,
        turnover,
        liquidity_feasibility,
        category_breakdown,
        tail_loss,
        report_pnl_simulation,
        report_hash,
    })
}

impl BacktestReport {
    /// Recompute the canonical report hash from every immutable report field.
    pub fn recomputed_hash(&self) -> QuantResult<ContentHash> {
        BacktestReportHashInput {
            backtest_report_id: &self.backtest_report_id,
            model_version_id: &self.model_version_id,
            dataset_id: &self.dataset_id,
            decision_policy_snapshot_id: &self.decision_policy_snapshot_id,
            window_start: self.window_start,
            window_end: self.window_end,
            coverage: self.coverage,
            sample_count: self.sample_count,
            missing_feature_count: self.missing_feature_count,
            rank_ic: self.rank_ic,
            sharpe: self.sharpe,
            hit_rate: self.hit_rate,
            expected_vs_realized: &self.expected_vs_realized,
            max_drawdown: self.max_drawdown,
            turnover: self.turnover,
            liquidity_feasibility: self.liquidity_feasibility,
            category_breakdown: &self.category_breakdown,
            tail_loss: self.tail_loss,
            report_pnl_simulation: &self.report_pnl_simulation,
        }
        .content_hash()
        .map_err(QuantError::from)
    }

    /// Require the persisted report hash to match its exact canonical preimage.
    pub fn verify_hash(&self) -> QuantResult<()> {
        let recomputed = self.recomputed_hash()?;
        if recomputed != self.report_hash {
            return Err(ResearchError::InvalidModelArtifact {
                detail: format!(
                    "backtest report {} hash mismatch: stored {}, recomputed {recomputed}",
                    self.backtest_report_id, self.report_hash
                ),
            }
            .into());
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use chrono::{DateTime, Duration, TimeZone, Utc};
    use quant_pivot_models::{
        domain::market::{book::BookLevel, fee::BuilderFeeAttribution},
        enums::{
            common::MarketCategory,
            quant::{DataQualityStatus, FactorDirection},
        },
        types::{
            BacktestReportId, Bps, DecisionPolicySnapshotId, MarketId, ModelRunId, ModelVersionId,
            PayoutRatio, Price, Probability, Shares, TokenId, TrainingDatasetId, Usd,
            factor::FactorExplanation,
        },
    };
    use rust_decimal::Decimal;
    use rust_decimal_macros::dec;

    use super::PortfolioReplayBacktester;
    use crate::{
        backtest::{
            BacktestDownsidePoint, BacktestDownsideTrajectory, BacktestExecutionSnapshot,
            BacktestInputs, BacktestMarketMeta, BacktestRankTarget, BacktestRequest, BacktestTick,
            Backtester, MarketOutcome, PortfolioCaps,
        },
        execution_semantics::PitFeeSchedule,
        factors::{FactorValue, NormalizedFactor, names::MOMENTUM_ROC},
        model::{
            artifact::ModelArtifact,
            runtime::{
                FactorInferenceRow, FactorInferenceTable, MarketInferenceContext, ModelRankTarget,
                ModelRuntimeInput,
            },
            weighted::WeightedFactorRuntime,
        },
        test_support::{content_hash as hash, weighted_factor_plane},
        training::TOKEN_PAYOUT_RATIO,
    };

    impl WeightedFactorRuntime {
        fn backtest_fixture() -> Self {
            Self::new(ModelArtifact::weighted_fixture(), None).expect("runtime")
        }
    }

    fn row(market: &str, bullish: bool) -> FactorInferenceRow {
        let direction = if bullish {
            FactorDirection::Positive
        } else {
            FactorDirection::Negative
        };
        let plane = weighted_factor_plane();
        let factors = plane
            .definitions()
            .iter()
            .map(|revision| {
                let factor_direction = if revision.definition().is_outcome_alpha() {
                    direction
                } else {
                    FactorDirection::Neutral
                };
                let raw_value = match factor_direction {
                    FactorDirection::Positive | FactorDirection::Neutral => dec!(1),
                    FactorDirection::Negative => dec!(-1),
                };
                FactorValue {
                    definition_id: revision.factor_definition_id(),
                    name: revision.factor_name().clone(),
                    family: revision.definition().family,
                    raw_value: Some(raw_value),
                    normalization: NormalizedFactor::cross_section(Probability::new(dec!(0.9))),
                    direction: factor_direction,
                    confidence: Probability::new(dec!(1)),
                    explanation: FactorExplanation {
                        headline: "t".to_owned(),
                        drivers: Vec::new(),
                    },
                    input_feature_refs: Vec::new(),
                }
            })
            .collect();
        FactorInferenceRow {
            market_id: MarketId::new(market),
            token_id: TokenId::new("yes"),
            factors,
            context: MarketInferenceContext {
                secondary_token_id: Some(TokenId::new("no")),
                yes_price: Price::new(dec!(0.5)),
                no_price: Some(Price::new(dec!(0.52))),
                liquidity_usd: Some(Usd::new(dec!(50000))),
                data_quality: DataQualityStatus::Fresh,
                time_to_resolution_secs: Some(86_400),
                substitution_reasons: Vec::new(),
            },
        }
    }

    fn tick(idx: i64, model_run_id: &ModelRunId) -> BacktestTick {
        let as_of = Utc.timestamp_opt(1_700_000_000 + idx * 3600, 0).unwrap();
        // Bullish market settles YES (correct), bearish settles NO (correct).
        let meta = vec![
            BacktestMarketMeta {
                market_id: MarketId::new("0xbull"),
                category: MarketCategory::Crypto,
                event_id: None,
                liquidity_usd: Some(Usd::new(dec!(50000))),
            },
            BacktestMarketMeta {
                market_id: MarketId::new("0xbear"),
                category: MarketCategory::Sports,
                event_id: None,
                liquidity_usd: Some(Usd::new(dec!(50000))),
            },
        ];
        BacktestTick {
            decision_at: as_of,
            model_input: ModelRuntimeInput::FactorTable(FactorInferenceTable {
                model_run_id: *model_run_id,
                decision_at: as_of,
                rows: vec![row("0xbull", true), row("0xbear", false)],
            }),
            outcomes: vec![
                MarketOutcome {
                    market_id: MarketId::new("0xbull"),
                    yes_payout_ratio: Some(PayoutRatio::ONE),
                },
                MarketOutcome {
                    market_id: MarketId::new("0xbear"),
                    yes_payout_ratio: Some(PayoutRatio::ZERO),
                },
            ],
            rank_targets: vec![
                BacktestRankTarget {
                    market_id: MarketId::new("0xbull"),
                    token_id: TokenId::new("yes"),
                    target: ModelRankTarget {
                        label_name: TOKEN_PAYOUT_RATIO,
                        label_horizon_secs: 0,
                    },
                    realized: Decimal::ONE,
                },
                BacktestRankTarget {
                    market_id: MarketId::new("0xbear"),
                    token_id: TokenId::new("yes"),
                    target: ModelRankTarget {
                        label_name: TOKEN_PAYOUT_RATIO,
                        label_horizon_secs: 0,
                    },
                    realized: Decimal::ZERO,
                },
            ],
            market_meta: meta,
            execution: vec![
                execution_snapshot("0xbull", "yes", as_of, dec!(0.5)),
                execution_snapshot("0xbear", "no", as_of, dec!(0.52)),
            ],
            downside_trajectories: Vec::new(),
        }
    }

    fn empty_tick(idx: i64, model_run_id: ModelRunId) -> BacktestTick {
        let decision_at = Utc
            .timestamp_opt(1_700_000_000 + idx * 3600, 0)
            .single()
            .expect("valid empty-tick timestamp");
        BacktestTick {
            decision_at,
            model_input: ModelRuntimeInput::FactorTable(FactorInferenceTable {
                model_run_id,
                decision_at,
                rows: Vec::new(),
            }),
            outcomes: Vec::new(),
            rank_targets: Vec::new(),
            market_meta: Vec::new(),
            execution: Vec::new(),
            downside_trajectories: Vec::new(),
        }
    }

    #[test]
    fn downside_uses_executable_sides() {
        let anchor = Utc.timestamp_opt(1_700_000_000, 0).unwrap();
        let trajectory = BacktestDownsideTrajectory {
            market_id: MarketId::new("market"),
            token_id: TokenId::new("yes"),
            anchor,
            entry_ask: Price::new(dec!(0.50)),
            data_available_until: anchor + Duration::hours(2),
            points: vec![
                BacktestDownsidePoint {
                    at: anchor + Duration::hours(1),
                    best_bid_low: Some(Price::new(dec!(0.45))),
                },
                BacktestDownsidePoint {
                    at: anchor + Duration::hours(2),
                    best_bid_low: Some(Price::new(dec!(0.40))),
                },
            ],
        };
        assert_eq!(
            trajectory
                .max_adverse_excursion_bps(3_600)
                .expect("mature downside"),
            Some(dec!(-1000))
        );
        assert_eq!(
            trajectory
                .max_adverse_excursion_bps(10_800)
                .expect("immature downside"),
            None
        );
    }

    fn execution_snapshot(
        market_id: &str,
        token_id: &str,
        at: DateTime<Utc>,
        price: Decimal,
    ) -> BacktestExecutionSnapshot {
        BacktestExecutionSnapshot {
            market_id: MarketId::new(market_id),
            token_id: TokenId::new(token_id),
            asks: vec![BookLevel::from_decimal_unchecked(
                Price::new(price),
                Shares::new(dec!(100000)),
            )],
            fee_schedule: PitFeeSchedule {
                schedule_hash: hash("fee"),
                effective_at: at,
                available_at: at,
                platform_rate: dec!(0.05),
                exponent: Decimal::ONE,
                taker_only: true,
                builder_maker_fee_bps: Bps::ZERO,
                builder_taker_fee_bps: Bps::ZERO,
                builder_attribution: BuilderFeeAttribution::NoBuilderCode,
            },
            fill_at: at,
            limit_price: Price::new(price),
            book_hash: hash(if token_id == "yes" { "1" } else { "2" }),
        }
    }

    impl PortfolioCaps {
        fn backtest_fixture() -> Self {
            Self {
                total_budget_usd: dec!(1000),
                max_single_recommendation_usd: dec!(200),
                min_recommendation_usd: dec!(10),
                max_market_exposure_usd: dec!(0),
                max_event_exposure_usd: dec!(0),
                max_category_exposure_usd: dec!(0),
                liquidity_usage_cap_pct: dec!(0.1),
                max_aggregate_exposure_pct: dec!(0),
            }
        }
    }

    impl BacktestRequest {
        fn test_fixture() -> Self {
            Self {
                backtest_report_id: BacktestReportId::from_v7(),
                model_version_id: ModelVersionId::from_v7(),
                dataset_id: TrainingDatasetId::from_v7(),
                decision_policy_snapshot_id: DecisionPolicySnapshotId::from_v7(),
                window_start: Utc.timestamp_opt(1_700_000_000, 0).unwrap(),
                window_end: Utc.timestamp_opt(1_700_100_000, 0).unwrap(),
            }
        }
    }

    #[tokio::test]
    async fn backtest_report_metrics_complete() {
        let model = WeightedFactorRuntime::backtest_fixture();
        let run_id = ModelRunId::from_v7();
        let ticks = vec![tick(0, &run_id), tick(1, &run_id)];
        let result = PortfolioReplayBacktester::new()
            .run(BacktestInputs {
                request: BacktestRequest::test_fixture(),
                model: &model,
                ticks,
                caps: PortfolioCaps::backtest_fixture(),
            })
            .await
            .expect("backtest");

        let report = &result.report;
        // Every market matured ⇒ full coverage, all candidates resolved.
        assert!(report.sample_count > 0, "resolved samples produced");
        assert_eq!(report.coverage, dec!(1), "all emitted candidates matured");
        // Correct directional calls ⇒ positive hit rate + rank IC.
        assert!(report.hit_rate.inner() > dec!(0.5), "mostly correct calls");
        assert!(
            !report.category_breakdown.is_empty(),
            "category breakdown filled"
        );
        assert!(
            report.report_pnl_simulation.total_allocated_usd > dec!(0),
            "capital allocated"
        );
        assert!(
            !report.report_pnl_simulation.pnl_curve.is_empty(),
            "PnL curve filled"
        );
        assert!(
            report.report_hash.to_string().starts_with("blake3:"),
            "canonical report hash"
        );
        report.verify_hash().expect("report hash preimage");
        let mut tampered = report.clone();
        tampered.rank_ic += dec!(0.01);
        assert!(
            tampered.verify_hash().is_err(),
            "a cached report field mutation must invalidate its canonical hash"
        );
    }

    /// The same inputs must produce a byte-identical report hash (the report id /
    /// version are fixed here), proving deterministic replay.
    #[tokio::test]
    async fn backtest_report_hash_deterministic() {
        let model = WeightedFactorRuntime::backtest_fixture();
        let run_id = ModelRunId::from_v7();
        let req = BacktestRequest::test_fixture();
        let ticks_a = vec![tick(0, &run_id), tick(1, &run_id)];
        let ticks_b = vec![tick(0, &run_id), tick(1, &run_id)];
        let a = PortfolioReplayBacktester::new()
            .run(BacktestInputs {
                request: req.clone(),
                model: &model,
                ticks: ticks_a,
                caps: PortfolioCaps::backtest_fixture(),
            })
            .await
            .expect("a");
        let b = PortfolioReplayBacktester::new()
            .run(BacktestInputs {
                request: req,
                model: &model,
                ticks: ticks_b,
                caps: PortfolioCaps::backtest_fixture(),
            })
            .await
            .expect("b");
        assert_eq!(a.report.report_hash, b.report.report_hash);
    }

    #[tokio::test]
    async fn score_planes_ignore_allocation() {
        let model = WeightedFactorRuntime::backtest_fixture();
        let run_id = ModelRunId::from_v7();
        let mut caps = PortfolioCaps::backtest_fixture();
        caps.total_budget_usd = Decimal::ZERO;
        let result = PortfolioReplayBacktester::new()
            .run(BacktestInputs {
                request: BacktestRequest::test_fixture(),
                model: &model,
                ticks: vec![tick(0, &run_id)],
                caps,
            })
            .await
            .expect("zero-budget replay");

        assert!(result.sample_outcomes.is_empty());
        assert_eq!(result.report.sample_count, 0);
        assert_eq!(result.calibration_outcomes.len(), 2);
        assert!(
            result
                .calibration_outcomes
                .iter()
                .all(|sample| sample.token_payout_ratio == PayoutRatio::ONE),
            "model-score calibration observes resolved predictions before allocation"
        );
        assert_eq!(result.rank_outcomes.len(), 2);
        let bullish = result
            .rank_outcomes
            .iter()
            .find(|sample| sample.market_id.as_str() == "0xbull")
            .expect("bullish canonical rank observation");
        let bearish = result
            .rank_outcomes
            .iter()
            .find(|sample| sample.market_id.as_str() == "0xbear")
            .expect("bearish canonical rank observation");
        assert!(bullish.score.is_sign_positive());
        assert_eq!(bullish.realized, Decimal::ONE);
        assert!(bearish.score.is_sign_negative());
        assert_eq!(bearish.realized, Decimal::ZERO);
        assert!(bullish.score > bearish.score);
    }

    #[tokio::test]
    async fn rank_target_mismatch_fails() {
        let model = WeightedFactorRuntime::backtest_fixture();
        let run_id = ModelRunId::from_v7();
        let mut mismatched = tick(0, &run_id);
        mismatched.rank_targets[0].target.label_horizon_secs = 60;

        let error = PortfolioReplayBacktester::new()
            .run(BacktestInputs {
                request: BacktestRequest::test_fixture(),
                model: &model,
                ticks: vec![mismatched],
                caps: PortfolioCaps::backtest_fixture(),
            })
            .await
            .expect_err("rank target drift must fail closed");

        assert!(
            error
                .to_string()
                .contains("rank score/target binding mismatch")
        );
    }

    #[tokio::test]
    async fn deadband_keeps_calibration_scores() {
        let model = WeightedFactorRuntime::backtest_fixture();
        let run_id = ModelRunId::from_v7();
        let mut score_only_tick = tick(0, &run_id);
        let ModelRuntimeInput::FactorTable(table) = &mut score_only_tick.model_input else {
            panic!("weighted fixture must use factor-table input");
        };
        for row in &mut table.rows {
            for factor in &mut row.factors {
                if factor.name.as_str() == MOMENTUM_ROC.as_str() {
                    factor.raw_value = Some(Decimal::ZERO);
                    factor.normalization = NormalizedFactor::cross_section(Probability::ZERO);
                    factor.direction = FactorDirection::Neutral;
                    factor.confidence = Probability::ZERO;
                }
            }
        }
        let result = PortfolioReplayBacktester::new()
            .run(BacktestInputs {
                request: BacktestRequest::test_fixture(),
                model: &model,
                ticks: vec![score_only_tick],
                caps: PortfolioCaps::backtest_fixture(),
            })
            .await
            .expect("score-only replay");

        assert!(result.sample_outcomes.is_empty());
        assert_eq!(result.report.sample_count, 0);
        assert_eq!(result.calibration_outcomes.len(), 2);
        assert!(
            result
                .calibration_outcomes
                .iter()
                .all(|sample| sample.composite_score == Probability::ZERO),
            "deadband scores must remain in calibration without becoming decisions"
        );
    }

    #[tokio::test]
    async fn no_allocation_retains_observation() {
        let model = WeightedFactorRuntime::backtest_fixture();
        let run_id = ModelRunId::from_v7();
        let request = BacktestRequest::test_fixture();
        let active = PortfolioReplayBacktester::new()
            .run(BacktestInputs {
                request: request.clone(),
                model: &model,
                ticks: vec![tick(0, &run_id), tick(1, &run_id)],
                caps: PortfolioCaps::backtest_fixture(),
            })
            .await
            .expect("active-only replay");
        let with_empty = PortfolioReplayBacktester::new()
            .run(BacktestInputs {
                request,
                model: &model,
                ticks: vec![tick(0, &run_id), tick(1, &run_id), empty_tick(2, run_id)],
                caps: PortfolioCaps::backtest_fixture(),
            })
            .await
            .expect("replay with genuine no-allocation tick");

        assert_eq!(with_empty.portfolio_returns.len(), 3);
        let empty = with_empty
            .portfolio_returns
            .last()
            .expect("no-allocation observation");
        assert_eq!(empty.realized_pnl_usd, Usd::ZERO);
        assert_eq!(empty.capital_base_usd, Usd::new(dec!(1000)));
        assert_eq!(empty.net_return_bps, Bps::ZERO);
        assert_eq!(
            with_empty.report.sharpe, active.report.sharpe,
            "report Sharpe remains an active-allocation statistic"
        );
    }
}
