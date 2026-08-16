//! [`PortfolioReplayBacktester`]: the deterministic PIT replay loop.
//!
//! Per tick: model inference (over the PIT-resolved factor table) → the same
//! global `HiGHS` MILP used by reporting → outcome resolution against settled
//! truth → metric accumulation.
//! The engine never touches a live `BookStore`; its only inputs are the
//! in-memory ticks, a model runtime, and a promoted scenario contract. There is
//! no relaxation, alternate solver, or empty-plan recovery path.

use std::collections::{BTreeMap, HashMap};

use async_trait::async_trait;
use chrono::{DateTime, Duration, Utc};
use quant_pivot_error::{QuantError, QuantResult, research::ResearchError};
use quant_pivot_models::{
    domain::{
        market::{book::BookLevel, fee::ImmediateExecutionCost},
        quant::{
            AggressiveEntryEconomics, EntryExecutionEconomics, ExecutableEconomicTier,
            PassiveEntryEconomics, PortfolioScenarioVisibility,
        },
    },
    enums::{
        common::MarketCategory,
        quant::{AccountSource, DataQualityStatus, FillRequirement, OutcomeSide},
    },
    hashing::CanonicalDigest,
    runtime_config::BuyModelRoute,
    types::{
        Bps, ContentHash, EventId, MarketId, PayoutRatio, PortfolioPlanId,
        PortfolioRejectionReason, PositionSnapshot, Price, Shares, SignalCandidateId, TokenId, Usd,
        backtest::{
            BacktestPortfolioFunnel, BacktestReportHashInput, BacktestTierExclusionCount,
            PnlCurvePoint, PnlSimulation,
        },
        calibration::CalibratedPayoutDistribution,
    },
};
use rust_decimal::Decimal;

use crate::{
    backtest::{
        BacktestDownsideTrajectory, BacktestExecutionSnapshot, BacktestInputs,
        BacktestLiquidationSnapshot, BacktestMarketMeta, BacktestPortfolioContract,
        BacktestRankTarget, BacktestReport, BacktestRequest, BacktestRunResult, BacktestTick,
        Backtester, CalibrationReplayTick, MarketOutcome, ModelCalibrationOutcome,
        ModelRankOutcome, PortfolioReturnObservation, PrecomputedBacktestInputs,
        PrecomputedBacktestTick, SampleOutcome, metrics, simulator,
    },
    execution_semantics::{
        BookWalkFill, BookWalkOutcome, LiquidityRole, PassiveQueueState, PassiveTrade,
        PitFeeSchedule, ResolutionBuySettlement, walk_buy_exact_shares, walk_sell_exact_shares,
    },
    model::{
        QuantModelRuntime, SignalCandidate,
        runtime::{MarketInferenceContext, ModelInputAuditState, ModelRuntimeOutput},
    },
    portfolio::{
        AccountSnapshot, EconomicTierFactory, ExecutableTierLadderSeedFactory,
        ExecutableTierLadderSeedInput, ExecutableTierSeed, ExistingPortfolioFactory,
        GlobalPortfolioInput, GlobalPortfolioPlanner, PortfolioScenarioGenerationInput,
        PortfolioScenarioGenerator, PortfolioScenarioLegInput, SealedPortfolioScenarioArtifact,
        TierAdmissionRejection, VerifiedPortfolioScenarioModel,
    },
    precision::{RESEARCH_DECIMAL_SCALE, quantize_venue_amount},
};

/// The lower tail fraction used for the tail-loss (`CVaR`) metric.
fn tail_quantile() -> Decimal {
    Decimal::new(10, 2) // 0.10
}

/// Deterministic PIT-replay backtester over the production MILP portfolio allocator.
#[derive(Debug, Clone, Copy, Default)]
pub struct PortfolioReplayBacktester;

impl PortfolioReplayBacktester {
    /// Construct the backtester with the pinned deterministic global allocator.
    #[must_use]
    pub const fn new() -> Self {
        Self
    }
}

/// Exact allocation-independent inference replay used to fit a challenger
/// calibrator before the challenger has any active Route authority.
#[derive(Debug, Clone, Copy, Default)]
pub struct ModelCalibrationReplay;

impl ModelCalibrationReplay {
    #[must_use]
    pub const fn new() -> Self {
        Self
    }

    /// Infer every frozen cross-section and retain the complete model-score
    /// population. Portfolio admission and optimizer selection cannot censor
    /// calibration observations.
    pub async fn run(
        &self,
        model: &dyn QuantModelRuntime,
        mut ticks: Vec<CalibrationReplayTick>,
    ) -> QuantResult<Vec<ModelCalibrationOutcome>> {
        ticks.sort_by_key(|tick| tick.decision_at);
        let mut calibration_outcomes = Vec::new();
        for tick in ticks {
            let output = model.infer_batch(tick.model_input).await?;
            let mut outcomes = BTreeMap::new();
            for outcome in &tick.outcomes {
                if outcomes
                    .insert(outcome.market_id.as_str(), outcome)
                    .is_some()
                {
                    return Err(ResearchError::ValidationMethodology {
                        detail: format!(
                            "calibration tick {} duplicates settlement truth for market {}",
                            tick.decision_at, outcome.market_id
                        ),
                    }
                    .into());
                }
            }
            let mut downside = BTreeMap::new();
            for trajectory in &tick.downside_trajectories {
                if downside
                    .insert(trajectory.token_id.as_str(), trajectory)
                    .is_some()
                {
                    return Err(ResearchError::ValidationMethodology {
                        detail: format!(
                            "calibration tick {} duplicates downside path for token {}",
                            tick.decision_at, trajectory.token_id
                        ),
                    }
                    .into());
                }
            }
            append_calibration_outcomes(
                tick.decision_at,
                &output,
                &outcomes,
                &downside,
                &mut calibration_outcomes,
            )?;
        }
        Ok(calibration_outcomes)
    }
}

struct BacktestAllocation {
    tier: ExecutableEconomicTier,
    liquidity_feasible: bool,
    observed_exit_capacity_shares: Shares,
}

#[derive(Default)]
struct TickPortfolioFunnel {
    emitted_candidate_count: u64,
    candidate_without_executable_tier_count: u64,
    executable_tier_count: u64,
    admission_rejected_tier_count: u64,
    admitted_tier_count: u64,
    selected_tier_count: u64,
    tier_exclusion_reasons: BTreeMap<PortfolioRejectionReason, u64>,
}

struct TickPortfolioDecision {
    allocations: Vec<BacktestAllocation>,
    funnel: TickPortfolioFunnel,
}

struct TickLookups<'a> {
    meta: BTreeMap<&'a str, &'a BacktestMarketMeta>,
    outcomes: BTreeMap<&'a str, &'a MarketOutcome>,
    context: BTreeMap<&'a str, &'a MarketInferenceContext>,
    execution: BTreeMap<&'a str, &'a BacktestExecutionSnapshot>,
    liquidation: BTreeMap<&'a str, &'a BacktestLiquidationSnapshot>,
    downside: BTreeMap<&'a str, &'a BacktestDownsideTrajectory>,
}

#[derive(Clone, Copy)]
struct ExecutableMarkRef<'a> {
    market_id: &'a MarketId,
    token_id: &'a TokenId,
    bids: &'a [BookLevel],
    fee_schedule: &'a PitFeeSchedule,
    at: DateTime<Utc>,
    book_hash: ContentHash,
}

impl<'a> From<&'a BacktestExecutionSnapshot> for ExecutableMarkRef<'a> {
    fn from(snapshot: &'a BacktestExecutionSnapshot) -> Self {
        Self {
            market_id: &snapshot.market_id,
            token_id: &snapshot.token_id,
            bids: &snapshot.bids,
            fee_schedule: &snapshot.fee_schedule,
            at: snapshot.fill_at,
            book_hash: snapshot.book_hash,
        }
    }
}

impl<'a> From<&'a BacktestLiquidationSnapshot> for ExecutableMarkRef<'a> {
    fn from(snapshot: &'a BacktestLiquidationSnapshot) -> Self {
        Self {
            market_id: &snapshot.market_id,
            token_id: &snapshot.token_id,
            bids: &snapshot.bids,
            fee_schedule: &snapshot.fee_schedule,
            at: snapshot.marked_at,
            book_hash: snapshot.book_hash,
        }
    }
}

impl<'a> TryFrom<&'a BacktestTick> for TickLookups<'a> {
    type Error = QuantError;

    fn try_from(tick: &'a BacktestTick) -> Result<Self, Self::Error> {
        let mut meta = BTreeMap::new();
        for market in &tick.market_meta {
            if meta.insert(market.market_id.as_str(), market).is_some() {
                return Err(ResearchError::ValidationMethodology {
                    detail: format!(
                        "backtest tick {} duplicates frozen metadata for market {}",
                        tick.decision_at, market.market_id
                    ),
                }
                .into());
            }
        }
        let mut outcomes = BTreeMap::new();
        for outcome in &tick.outcomes {
            if outcomes
                .insert(outcome.market_id.as_str(), outcome)
                .is_some()
            {
                return Err(ResearchError::ValidationMethodology {
                    detail: format!(
                        "backtest tick {} duplicates settlement truth for market {}",
                        tick.decision_at, outcome.market_id
                    ),
                }
                .into());
            }
        }
        let mut context = BTreeMap::new();
        for (market_id, market_context) in tick.model_input.market_contexts() {
            if context.insert(market_id.as_str(), market_context).is_some() {
                return Err(ResearchError::ValidationMethodology {
                    detail: format!(
                        "backtest tick {} duplicates inference context for market {}",
                        tick.decision_at, market_id
                    ),
                }
                .into());
            }
        }
        let mut execution = BTreeMap::new();
        for snapshot in &tick.execution {
            if execution
                .insert(snapshot.token_id.as_str(), snapshot)
                .is_some()
            {
                return Err(ResearchError::ValidationMethodology {
                    detail: format!(
                        "backtest tick {} duplicates execution snapshot for token {}",
                        tick.decision_at, snapshot.token_id
                    ),
                }
                .into());
            }
        }
        let mut liquidation = BTreeMap::new();
        for snapshot in &tick.liquidation {
            if liquidation
                .insert(snapshot.token_id.as_str(), snapshot)
                .is_some()
            {
                return Err(ResearchError::ValidationMethodology {
                    detail: format!(
                        "backtest tick {} duplicates liquidation snapshot for token {}",
                        tick.decision_at, snapshot.token_id
                    ),
                }
                .into());
            }
        }
        let mut downside = BTreeMap::new();
        for trajectory in &tick.downside_trajectories {
            if downside
                .insert(trajectory.token_id.as_str(), trajectory)
                .is_some()
            {
                return Err(ResearchError::ValidationMethodology {
                    detail: format!(
                        "backtest tick {} duplicates downside trajectory for token {}",
                        tick.decision_at, trajectory.token_id
                    ),
                }
                .into());
            }
        }
        Ok(Self {
            meta,
            outcomes,
            context,
            execution,
            liquidation,
            downside,
        })
    }
}

struct OpenReplayPosition {
    route: BuyModelRoute,
    market_id: MarketId,
    event_id: EventId,
    category: MarketCategory,
    token_id: TokenId,
    outcome_side: OutcomeSide,
    shares: Shares,
    entry_vwap: Price,
    cash_outlay: Usd,
    current_mark: Price,
    current_value: Usd,
    resolved_at: DateTime<Utc>,
    payout_usd: Usd,
    calibrated_payout_distribution: CalibratedPayoutDistribution,
    observed_exit_capacity_shares: Shares,
    base_capital_release_secs: u64,
    entry_lineage_hash: ContentHash,
    current_lineage_hash: ContentHash,
    entry_tick_index: usize,
    sample: SampleOutcome,
}

impl OpenReplayPosition {
    fn mark(
        &mut self,
        snapshot: &BacktestLiquidationSnapshot,
        at: DateTime<Utc>,
    ) -> QuantResult<()> {
        self.apply_mark(ExecutableMarkRef::from(snapshot), at)
    }

    fn mark_entry(
        &mut self,
        snapshot: &BacktestExecutionSnapshot,
        at: DateTime<Utc>,
    ) -> QuantResult<()> {
        self.apply_mark(ExecutableMarkRef::from(snapshot), at)
    }

    fn apply_mark(
        &mut self,
        snapshot: ExecutableMarkRef<'_>,
        at: DateTime<Utc>,
    ) -> QuantResult<()> {
        if snapshot.market_id != &self.market_id
            || snapshot.token_id != &self.token_id
            || snapshot.at != at
            || snapshot.bids.windows(2).any(|levels| {
                levels[0].price_decimal() < levels[1].price_decimal()
                    || !levels[0].size_decimal().is_positive()
            })
            || snapshot
                .bids
                .last()
                .is_some_and(|level| !level.size_decimal().is_positive())
        {
            return Err(ResearchError::ValidationMethodology {
                detail: format!(
                    "replay mark snapshot for token {} is not the exact ordered PIT book",
                    self.token_id
                ),
            }
            .into());
        }
        let observed_exit_capacity =
            snapshot
                .bids
                .iter()
                .try_fold(Decimal::ZERO, |total, level| {
                    total
                        .checked_add(level.size_decimal().inner())
                        .ok_or_else(|| ResearchError::ValidationMethodology {
                            detail: "replay mark bid capacity overflowed shares".to_owned(),
                        })
                })?;
        let (current_value, current_mark, filled_shares, expected_fee) = if let Some(limit_price) =
            snapshot.bids.last().map(|level| level.price_decimal())
        {
            let fill = walk_sell_exact_shares(
                snapshot.bids,
                self.shares,
                limit_price,
                FillRequirement::AllowPartial,
                snapshot.fee_schedule,
                LiquidityRole::Taker,
                at,
            )
            .map_err(|error| ResearchError::ValidationMethodology {
                detail: format!(
                    "replay executable mark failed for token {}: {error:?}",
                    self.token_id
                ),
            })?;
            if fill.account_cash_delta_usd.is_sign_negative()
                || fill.filled_shares > self.shares
                || (fill.filled_shares.is_positive() && fill.outcome == BookWalkOutcome::Unfilled)
            {
                return Err(ResearchError::ValidationMethodology {
                        detail: format!(
                            "replay executable mark returned invalid liquidation economics for token {}",
                            self.token_id
                        ),
                    }
                    .into());
            }
            let value = Usd::new(quantize_venue_amount(fill.account_cash_delta_usd));
            let mark = if value.is_positive() {
                Price::new(value.inner() / self.shares.inner())
            } else {
                Price::ZERO
            };
            (
                value,
                mark,
                fill.filled_shares,
                fill.immediate_cost.total_fee_usd(),
            )
        } else {
            (Usd::ZERO, Price::ZERO, Shares::ZERO, Usd::ZERO)
        };
        let observed_exit_capacity_shares = Shares::new(observed_exit_capacity);
        let current_lineage_hash = CanonicalDigest::content_hash_typed(
            "quant-pivot/replay-executable-exit-mark",
            1,
            &(
                self.entry_lineage_hash,
                snapshot.book_hash,
                snapshot.fee_schedule.schedule_hash,
                at,
                self.shares,
                observed_exit_capacity_shares,
                filled_shares,
                expected_fee,
                current_mark,
                current_value,
            ),
        )?;
        self.current_mark = current_mark;
        self.current_value = current_value;
        self.observed_exit_capacity_shares = observed_exit_capacity_shares;
        self.current_lineage_hash = current_lineage_hash;
        Ok(())
    }
}

struct ReplaySettlement {
    realized_pnl: Decimal,
    entry_tick_index: usize,
    sample: SampleOutcome,
}

struct ReplaySettlementBatch {
    resolved_at: DateTime<Utc>,
    settlements: Vec<ReplaySettlement>,
    net_liquidation: Usd,
}

struct ReplayLedger {
    capital_base: Usd,
    available_cash: Usd,
    peak_net_liquidation: Usd,
    current_drawdown: Usd,
    positions: Vec<OpenReplayPosition>,
}

impl ReplayLedger {
    fn new(contract: &BacktestPortfolioContract) -> QuantResult<Self> {
        let capital_base = contract.account.capital_base_usd;
        if !capital_base.is_positive()
            || contract.account.available_usd != capital_base
            || !contract.account.positions.is_empty()
        {
            return Err(ResearchError::ValidationMethodology {
                detail: "historical replay must start from one positive all-cash account"
                    .to_owned(),
            }
            .into());
        }
        Ok(Self {
            capital_base,
            available_cash: capital_base,
            peak_net_liquidation: capital_base,
            current_drawdown: Usd::ZERO,
            positions: Vec::new(),
        })
    }

    fn settle_through(&mut self, at: DateTime<Utc>) -> QuantResult<Vec<ReplaySettlementBatch>> {
        let mut batches = Vec::new();
        loop {
            let next_resolution = self
                .positions
                .iter()
                .filter(|position| position.resolved_at <= at)
                .map(|position| position.resolved_at)
                .min();
            let Some(resolved_at) = next_resolution else {
                break;
            };
            let mut pending = Vec::with_capacity(self.positions.len());
            let mut settlements = Vec::new();
            for position in self.positions.drain(..) {
                if position.resolved_at == resolved_at {
                    self.available_cash = Usd::new(
                        self.available_cash
                            .inner()
                            .checked_add(position.payout_usd.inner())
                            .ok_or_else(|| ResearchError::ValidationMethodology {
                                detail: "replay settlement cash overflowed USD".to_owned(),
                            })?,
                    );
                    settlements.push(ReplaySettlement {
                        realized_pnl: position.payout_usd.inner() - position.cash_outlay.inner(),
                        entry_tick_index: position.entry_tick_index,
                        sample: position.sample,
                    });
                } else {
                    pending.push(position);
                }
            }
            self.positions = pending;
            self.update_drawdown()?;
            batches.push(ReplaySettlementBatch {
                resolved_at,
                settlements,
                net_liquidation: self.net_liquidation()?,
            });
        }
        Ok(batches)
    }

    fn settle_all(&mut self) -> QuantResult<Vec<ReplaySettlementBatch>> {
        let terminal_at = self
            .positions
            .iter()
            .map(|position| position.resolved_at)
            .max();
        terminal_at.map_or_else(|| Ok(Vec::new()), |at| self.settle_through(at))
    }

    fn open(&mut self, positions: Vec<OpenReplayPosition>) -> QuantResult<Usd> {
        let cash_outlay = positions
            .iter()
            .try_fold(Decimal::ZERO, |total, position| {
                total
                    .checked_add(position.cash_outlay.inner())
                    .ok_or_else(|| ResearchError::ValidationMethodology {
                        detail: "replay entry cash outlay overflowed USD".to_owned(),
                    })
            })?;
        let remaining_cash = self
            .available_cash
            .inner()
            .checked_sub(cash_outlay)
            .filter(|value| *value >= Decimal::ZERO)
            .ok_or_else(|| ResearchError::ValidationMethodology {
                detail: format!(
                    "stateful replay attempted to spend {cash_outlay} with only {} available",
                    self.available_cash
                ),
            })?;
        self.available_cash = Usd::new(remaining_cash);
        self.positions.extend(positions);
        self.update_drawdown()?;
        Ok(Usd::new(cash_outlay))
    }

    fn mark_positions(
        &mut self,
        liquidation: &BTreeMap<&str, &BacktestLiquidationSnapshot>,
        at: DateTime<Utc>,
    ) -> QuantResult<()> {
        for position in &mut self.positions {
            let snapshot = liquidation
                .get(position.token_id.as_str())
                .copied()
                .ok_or_else(|| ResearchError::ValidationMethodology {
                    detail: format!(
                        "open replay token {} has no exact PIT liquidation snapshot at {at}",
                        position.token_id
                    ),
                })?;
            position.mark(snapshot, at)?;
        }
        self.update_drawdown()
    }

    fn snapshot(&self, at: DateTime<Utc>) -> QuantResult<AccountSnapshot> {
        let positions = self
            .positions
            .iter()
            .map(|position| PositionSnapshot {
                token_id: position.token_id.clone(),
                market_id: position.market_id.clone(),
                event_id: Some(position.event_id.clone()),
                category: position.category,
                outcome: position.outcome_side.as_str().to_owned(),
                size: position.shares,
                avg_price: position.entry_vwap,
                cur_price: position.current_mark,
                current_value: position.current_value,
                redeemable: false,
            })
            .collect::<Vec<_>>();
        let marked_positions = positions
            .iter()
            .try_fold(Decimal::ZERO, |total, position| {
                total
                    .checked_add(position.current_value.inner())
                    .ok_or_else(|| ResearchError::ValidationMethodology {
                        detail: "replay marked position value overflowed USD".to_owned(),
                    })
            })?;
        let venue_net_liquidation_usd = self
            .available_cash
            .inner()
            .checked_add(marked_positions)
            .ok_or_else(|| ResearchError::ValidationMethodology {
            detail: "replay net liquidation value overflowed USD".to_owned(),
        })?;
        Ok(AccountSnapshot::new(
            at,
            AccountSource::HistoricalReplay,
            Usd::new(venue_net_liquidation_usd),
            self.capital_base,
            self.available_cash,
            Usd::ZERO,
            positions,
        ))
    }

    fn update_drawdown(&mut self) -> QuantResult<()> {
        let net_liquidation = self.net_liquidation()?.inner();
        self.peak_net_liquidation = self.peak_net_liquidation.max(Usd::new(net_liquidation));
        self.current_drawdown = Usd::new(
            self.peak_net_liquidation
                .inner()
                .checked_sub(net_liquidation)
                .ok_or_else(|| ResearchError::ValidationMethodology {
                    detail: "replay drawdown underflowed USD".to_owned(),
                })?,
        );
        Ok(())
    }

    fn net_liquidation(&self) -> QuantResult<Usd> {
        let current_positions =
            self.positions
                .iter()
                .try_fold(Decimal::ZERO, |total, position| {
                    total
                        .checked_add(position.current_value.inner())
                        .ok_or_else(|| ResearchError::ValidationMethodology {
                            detail: "replay position value overflowed USD".to_owned(),
                        })
                })?;
        self.available_cash
            .inner()
            .checked_add(current_positions)
            .map(Usd::new)
            .ok_or_else(|| {
                ResearchError::ValidationMethodology {
                    detail: "replay net liquidation value overflowed USD".to_owned(),
                }
                .into()
            })
    }
}

#[derive(Default)]
struct PortfolioFunnelAccumulator {
    decision_tick_count: u64,
    emitted_candidate_count: u64,
    candidate_without_executable_tier_count: u64,
    executable_tier_count: u64,
    admission_rejected_tier_count: u64,
    admitted_tier_count: u64,
    selected_tier_count: u64,
    executed_entry_count: u64,
    resolved_allocation_count: u64,
    no_candidate_tick_count: u64,
    no_executable_tier_tick_count: u64,
    no_selection_tick_count: u64,
    selected_tick_count: u64,
    tier_exclusion_reasons: BTreeMap<PortfolioRejectionReason, u64>,
}

impl PortfolioFunnelAccumulator {
    fn record(&mut self, tick: TickPortfolioFunnel, executed_entries: u64) -> QuantResult<()> {
        if executed_entries != tick.selected_tier_count {
            return Err(ResearchError::ValidationMethodology {
                detail: format!(
                    "backtest executed entry count {executed_entries} differs from selected tier count {}",
                    tick.selected_tier_count
                ),
            }
            .into());
        }
        Self::increase(&mut self.decision_tick_count, 1, "decision_tick_count")?;
        Self::increase(
            &mut self.emitted_candidate_count,
            tick.emitted_candidate_count,
            "emitted_candidate_count",
        )?;
        Self::increase(
            &mut self.candidate_without_executable_tier_count,
            tick.candidate_without_executable_tier_count,
            "candidate_without_executable_tier_count",
        )?;
        Self::increase(
            &mut self.executable_tier_count,
            tick.executable_tier_count,
            "executable_tier_count",
        )?;
        Self::increase(
            &mut self.admission_rejected_tier_count,
            tick.admission_rejected_tier_count,
            "admission_rejected_tier_count",
        )?;
        Self::increase(
            &mut self.admitted_tier_count,
            tick.admitted_tier_count,
            "admitted_tier_count",
        )?;
        Self::increase(
            &mut self.selected_tier_count,
            tick.selected_tier_count,
            "selected_tier_count",
        )?;
        Self::increase(
            &mut self.executed_entry_count,
            executed_entries,
            "executed_entry_count",
        )?;
        if tick.emitted_candidate_count == 0 {
            Self::increase(
                &mut self.no_candidate_tick_count,
                1,
                "no_candidate_tick_count",
            )?;
        } else if tick.executable_tier_count == 0 {
            Self::increase(
                &mut self.no_executable_tier_tick_count,
                1,
                "no_executable_tier_tick_count",
            )?;
        } else if tick.selected_tier_count == 0 {
            Self::increase(
                &mut self.no_selection_tick_count,
                1,
                "no_selection_tick_count",
            )?;
        } else {
            Self::increase(&mut self.selected_tick_count, 1, "selected_tick_count")?;
        }
        for (reason, count) in tick.tier_exclusion_reasons {
            let total = self.tier_exclusion_reasons.entry(reason).or_default();
            Self::increase(total, count, "tier_exclusion_reasons")?;
        }
        Ok(())
    }

    fn resolve(&mut self, resolved_allocations: u64) -> QuantResult<()> {
        Self::increase(
            &mut self.resolved_allocation_count,
            resolved_allocations,
            "resolved_allocation_count",
        )?;
        if self.resolved_allocation_count > self.executed_entry_count {
            return Err(ResearchError::ValidationMethodology {
                detail: "backtest resolved allocations exceed executed entries".to_owned(),
            }
            .into());
        }
        Ok(())
    }

    fn finish(self) -> QuantResult<BacktestPortfolioFunnel> {
        let funnel = BacktestPortfolioFunnel {
            schema_version: 1,
            decision_tick_count: self.decision_tick_count,
            emitted_candidate_count: self.emitted_candidate_count,
            candidate_without_executable_tier_count: self.candidate_without_executable_tier_count,
            executable_tier_count: self.executable_tier_count,
            admission_rejected_tier_count: self.admission_rejected_tier_count,
            admitted_tier_count: self.admitted_tier_count,
            selected_tier_count: self.selected_tier_count,
            executed_entry_count: self.executed_entry_count,
            resolved_allocation_count: self.resolved_allocation_count,
            no_candidate_tick_count: self.no_candidate_tick_count,
            no_executable_tier_tick_count: self.no_executable_tier_tick_count,
            no_selection_tick_count: self.no_selection_tick_count,
            selected_tick_count: self.selected_tick_count,
            tier_exclusion_reasons: self
                .tier_exclusion_reasons
                .into_iter()
                .map(|(reason, count)| BacktestTierExclusionCount { reason, count })
                .collect(),
        };
        funnel
            .validate()
            .map_err(|detail| ResearchError::ValidationMethodology { detail })?;
        Ok(funnel)
    }

    fn increase(target: &mut u64, increment: u64, field: &'static str) -> QuantResult<()> {
        *target =
            target
                .checked_add(increment)
                .ok_or_else(|| ResearchError::ValidationMethodology {
                    detail: format!("backtest portfolio funnel {field} overflowed u64"),
                })?;
        Ok(())
    }
}

/// Mutable replay accumulators threaded across ticks.
#[derive(Default)]
struct RunAccumulator {
    calibration_outcomes: Vec<ModelCalibrationOutcome>,
    rank_outcomes: Vec<ModelRankOutcome>,
    samples: Vec<SampleOutcome>,
    pnl_curve: Vec<PnlCurvePoint>,
    tick_cash_turnover: Vec<Decimal>,
    /// Decision-cohort realized return against the fixed capital base. Trade
    /// `PnL` is attributed to its OOS decision group while the ledger still
    /// locks capital until the actual resolution event.
    tick_returns: Vec<Decimal>,
    decision_times: Vec<DateTime<Utc>>,
    cohort_pnl: Vec<Decimal>,
    portfolio_returns: Vec<PortfolioReturnObservation>,
    portfolio_funnel: PortfolioFunnelAccumulator,
    missing_feature_count: u64,
    total_emitted: u64,
    resolved_emitted: u64,
    total_allocated: Decimal,
    realized_pnl: Decimal,
}

impl RunAccumulator {
    fn record_calibration(
        &mut self,
        tick: &BacktestTick,
        output: &ModelRuntimeOutput,
        outcomes: &BTreeMap<&str, &MarketOutcome>,
        downside: &BTreeMap<&str, &BacktestDownsideTrajectory>,
    ) -> QuantResult<()> {
        append_calibration_outcomes(
            tick.decision_at,
            output,
            outcomes,
            downside,
            &mut self.calibration_outcomes,
        )
    }
}

#[async_trait]
impl Backtester for PortfolioReplayBacktester {
    async fn run(&self, inputs: BacktestInputs<'_>) -> QuantResult<BacktestRunResult> {
        let mut ticks = inputs.ticks;
        ticks.sort_by_key(|tick| tick.decision_at);
        let mut precomputed = Vec::with_capacity(ticks.len());
        for tick in ticks {
            let output = inputs.model.infer_batch(tick.model_input.clone()).await?;
            precomputed.push(PrecomputedBacktestTick {
                model_version_id: inputs.model.model_version_id(),
                tick,
                output,
                scenario: inputs.scenario.clone(),
                scenario_visibility: inputs.scenario_visibility,
            });
        }
        self.run_precomputed(PrecomputedBacktestInputs {
            request: inputs.request,
            ticks: precomputed,
        })
    }
}

impl PortfolioReplayBacktester {
    /// Execute a complete, time-ordered, self-financing replay from already
    /// inferred OOS ticks. This is the sole CPCV economic boundary: fold-local
    /// estimators cannot allocate or recycle capital independently.
    pub fn run_precomputed(
        &self,
        mut inputs: PrecomputedBacktestInputs,
    ) -> QuantResult<BacktestRunResult> {
        inputs.ticks.sort_by_key(|input| input.tick.decision_at);
        if inputs
            .ticks
            .windows(2)
            .any(|window| window[0].tick.decision_at >= window[1].tick.decision_at)
        {
            return Err(ResearchError::ValidationMethodology {
                detail: "stateful replay decision times must be strictly increasing".to_owned(),
            }
            .into());
        }
        let first = inputs
            .ticks
            .first()
            .ok_or_else(|| ResearchError::ValidationMethodology {
                detail: "portfolio replay requires at least one decision tick".to_owned(),
            })?;
        if inputs
            .ticks
            .iter()
            .any(|input| input.model_version_id != inputs.request.model_version_id)
        {
            return Err(ResearchError::ValidationMethodology {
                detail: "precomputed replay mixes model versions or differs from its request"
                    .to_owned(),
            }
            .into());
        }
        let budget = governed_budget(&inputs.ticks)?;
        let mut ledger = ReplayLedger::new(&first.tick.portfolio_contract)?;
        let mut acc = RunAccumulator::default();
        for input in &inputs.ticks {
            process_tick(input, &mut ledger, &mut acc)?;
        }
        let terminal_batches = ledger.settle_all()?;
        record_batches(&mut acc, terminal_batches, ledger.capital_base)?;
        if !ledger.positions.is_empty() {
            return Err(ResearchError::ValidationMethodology {
                detail: "stateful replay terminal flush left unresolved open positions".to_owned(),
            }
            .into());
        }
        let terminal_pnl = ledger
            .net_liquidation()?
            .inner()
            .checked_sub(ledger.capital_base.inner())
            .ok_or_else(|| ResearchError::ValidationMethodology {
                detail: "terminal replay equity overflowed Decimal".to_owned(),
            })?;
        if terminal_pnl != acc.realized_pnl {
            return Err(ResearchError::ValidationMethodology {
                detail: format!(
                    "terminal replay equity {terminal_pnl} differs from settled PnL {}",
                    acc.realized_pnl
                ),
            }
            .into());
        }
        finalize_cohorts(&mut acc, ledger.capital_base)?;

        let portfolio_funnel = acc.portfolio_funnel.finish()?;
        let metrics = BuildMetrics {
            samples: &acc.samples,
            pnl_curve: &acc.pnl_curve,
            tick_cash_turnover: &acc.tick_cash_turnover,
            tick_returns: &acc.tick_returns,
            missing_feature_count: acc.missing_feature_count,
            total_emitted: acc.total_emitted,
            resolved_emitted: acc.resolved_emitted,
            total_allocated: acc.total_allocated,
            realized_pnl: acc.realized_pnl,
            budget,
            portfolio_funnel: &portfolio_funnel,
        };
        let report = build_report(&inputs.request, &metrics)?;
        Ok(BacktestRunResult {
            report,
            calibration_outcomes: acc.calibration_outcomes,
            rank_outcomes: acc.rank_outcomes,
            sample_outcomes: acc.samples,
            portfolio_returns: acc.portfolio_returns,
            tick_cash_turnover: acc.tick_cash_turnover,
        })
    }
}

/// Process one replay tick: allocate over the inferred candidates, resolve their
/// realized outcomes against settled truth, and fold the results into `acc`.
fn process_tick(
    input: &PrecomputedBacktestTick,
    ledger: &mut ReplayLedger,
    acc: &mut RunAccumulator,
) -> QuantResult<()> {
    let settlement_batches = ledger.settle_through(input.tick.decision_at)?;
    record_batches(acc, settlement_batches, ledger.capital_base)?;
    let lookups = TickLookups::try_from(&input.tick)?;
    ledger.mark_positions(&lookups.liquidation, input.tick.decision_at)?;
    let mut tick = input.tick.clone();
    tick.portfolio_contract.account = ledger.snapshot(tick.decision_at)?;
    if input.scenario.represented_routes() != &tick.portfolio_contract.represented_routes {
        return Err(ResearchError::ValidationMethodology {
            detail: "backtest tick Route set differs from its verified scenario context".to_owned(),
        }
        .into());
    }
    let scenario_model = input.scenario.verified();
    let output = &input.output;
    let emitted = u64::try_from(output.candidates.len()).map_err(|error| {
        ResearchError::ValidationMethodology {
            detail: format!("backtest emitted candidate count does not fit u64: {error}"),
        }
    })?;
    acc.total_emitted = acc.total_emitted.checked_add(emitted).ok_or_else(|| {
        ResearchError::ValidationMethodology {
            detail: "backtest emitted candidate count overflowed u64".to_owned(),
        }
    })?;
    record_missing_input_count(acc, output)?;

    record_candidate_coverage(&tick, output, &lookups.outcomes, acc)?;
    acc.record_calibration(&tick, output, &lookups.outcomes, &lookups.downside)?;
    record_rank_outcomes(&tick, output, acc)?;

    let entry_tick_index = acc.decision_times.len();
    acc.decision_times.push(tick.decision_at);
    acc.cohort_pnl.push(Decimal::ZERO);

    let decision = TickPortfolioContext {
        tick: &tick,
        output,
        scenario_model: &scenario_model,
        scenario_visibility: input.scenario_visibility,
        open_positions: &ledger.positions,
        current_drawdown: ledger.current_drawdown,
    }
    .allocate()?;
    let replay = replay_selected(&tick, output, &decision, &lookups, entry_tick_index)?;
    let cash_outlay = ledger.open(replay.opened_positions)?;
    acc.total_allocated = acc
        .total_allocated
        .checked_add(cash_outlay.inner())
        .ok_or_else(|| ResearchError::ValidationMethodology {
            detail: "backtest total allocated cash overflowed Decimal".to_owned(),
        })?;
    let turnover = cash_outlay.inner() / ledger.capital_base.inner();
    acc.tick_cash_turnover
        .push(turnover.round_dp(RESEARCH_DECIMAL_SCALE));
    record_equity(
        acc,
        tick.decision_at,
        ledger.net_liquidation()?,
        ledger.capital_base,
    )?;
    acc.portfolio_funnel
        .record(decision.funnel, replay.executed_entry_count)?;
    Ok(())
}

struct TickReplaySummary {
    executed_entry_count: u64,
    opened_positions: Vec<OpenReplayPosition>,
}

fn replay_selected(
    tick: &BacktestTick,
    output: &ModelRuntimeOutput,
    decision: &TickPortfolioDecision,
    lookups: &TickLookups<'_>,
    entry_tick_index: usize,
) -> QuantResult<TickReplaySummary> {
    let alloc_by_id = decision
        .allocations
        .iter()
        .map(|allocation| (allocation.tier.candidate_id, allocation))
        .collect::<HashMap<_, _>>();
    let mut executed_entry_count = 0_u64;
    let mut opened_positions = Vec::new();
    for candidate in &output.candidates {
        let Some(allocation) = alloc_by_id.get(&candidate.signal_candidate_id) else {
            continue;
        };
        let opened = CandidateReplayContext {
            tick,
            candidate,
            allocation,
            lookups,
            entry_tick_index,
        }
        .replay()?;
        if let Some(opened) = opened {
            executed_entry_count = executed_entry_count.checked_add(1).ok_or_else(|| {
                ResearchError::ValidationMethodology {
                    detail: "backtest executed entry count overflowed u64".to_owned(),
                }
            })?;
            opened_positions.push(opened);
        }
    }
    Ok(TickReplaySummary {
        executed_entry_count,
        opened_positions,
    })
}

#[derive(Clone, Copy)]
struct CandidateReplayContext<'a> {
    tick: &'a BacktestTick,
    candidate: &'a SignalCandidate,
    allocation: &'a BacktestAllocation,
    lookups: &'a TickLookups<'a>,
    entry_tick_index: usize,
}

impl<'a> CandidateReplayContext<'a> {
    fn replay(self) -> QuantResult<Option<OpenReplayPosition>> {
        self.validate_binding()?;
        let market = self.market()?;
        let snapshot = self.snapshot()?;
        let Some(fill) = self.entry_fill(snapshot)? else {
            return Ok(None);
        };
        let entry_vwap = fill
            .vwap
            .ok_or_else(|| ResearchError::ValidationMethodology {
                detail: format!(
                    "filled backtest entry for token {} has no executable VWAP",
                    self.candidate.token_id
                ),
            })?;
        let (resolved_at, token_payout_ratio) = self.resolution()?;
        let settlement =
            simulator::settle_executed_buy(&fill, token_payout_ratio).map_err(|error| {
                ResearchError::ValidationMethodology {
                    detail: format!(
                        "backtest executable settlement failed for token {}: {error:?}",
                        self.candidate.token_id
                    ),
                }
            })?;
        self.validate_economics(&settlement)?;
        self.open_position(
            market,
            snapshot,
            &settlement,
            entry_vwap,
            resolved_at,
            token_payout_ratio,
        )
        .map(Some)
    }

    fn validate_binding(self) -> QuantResult<()> {
        let tier = &self.allocation.tier;
        if tier.market_id != self.candidate.market_id
            || tier.token_id != self.candidate.token_id
            || tier.outcome_side != self.candidate.outcome_side
        {
            return Err(ResearchError::ValidationMethodology {
                detail: format!(
                    "selected economic tier {} differs from candidate {} binding",
                    tier.economic_tier_id, self.candidate.signal_candidate_id
                ),
            }
            .into());
        }
        Ok(())
    }

    fn market(self) -> QuantResult<&'a BacktestMarketMeta> {
        self.lookups
            .meta
            .get(self.candidate.market_id.as_str())
            .copied()
            .ok_or_else(|| {
                ResearchError::ValidationMethodology {
                    detail: format!(
                        "selected candidate {} has no frozen market metadata",
                        self.candidate.signal_candidate_id
                    ),
                }
                .into()
            })
    }

    fn snapshot(self) -> QuantResult<&'a BacktestExecutionSnapshot> {
        let snapshot = self
            .lookups
            .execution
            .get(self.candidate.token_id.as_str())
            .copied()
            .ok_or_else(|| ResearchError::ValidationMethodology {
                detail: format!(
                    "backtest tick {} has no executable PIT snapshot for token {}",
                    self.tick.decision_at, self.candidate.token_id
                ),
            })?;
        if snapshot.market_id != self.candidate.market_id {
            return Err(ResearchError::ValidationMethodology {
                detail: format!(
                    "backtest execution snapshot token {} is bound to market {}, expected {}",
                    self.candidate.token_id, snapshot.market_id, self.candidate.market_id
                ),
            }
            .into());
        }
        Ok(snapshot)
    }

    fn entry_fill(self, snapshot: &BacktestExecutionSnapshot) -> QuantResult<Option<BookWalkFill>> {
        match &self.allocation.tier.entry_execution {
            EntryExecutionEconomics::Aggressive(entry) => {
                self.aggressive_fill(snapshot, entry).map(Some)
            }
            EntryExecutionEconomics::Passive(entry) => self.passive_fill(snapshot, entry),
        }
    }

    fn aggressive_fill(
        self,
        snapshot: &BacktestExecutionSnapshot,
        entry: &AggressiveEntryEconomics,
    ) -> QuantResult<BookWalkFill> {
        let tier = &self.allocation.tier;
        let fill = walk_buy_exact_shares(
            &snapshot.asks,
            entry.filled_shares,
            snapshot.limit_price,
            FillRequirement::AllOrNothing,
            &snapshot.fee_schedule,
            LiquidityRole::Taker,
            snapshot.fill_at,
        )
        .map_err(|error| ResearchError::ValidationMethodology {
            detail: format!(
                "backtest executable entry walk failed for token {}: {error:?}",
                self.candidate.token_id
            ),
        })?;
        if fill.outcome != BookWalkOutcome::Filled
            || fill.filled_shares != entry.filled_shares
            || fill.vwap != Some(entry.entry_vwap)
        {
            return Err(ResearchError::ValidationMethodology {
                detail: format!(
                    "selected economic tier {} did not replay to its exact shares/VWAP",
                    tier.economic_tier_id
                ),
            }
            .into());
        }
        Ok(fill)
    }

    fn passive_fill(
        self,
        snapshot: &BacktestExecutionSnapshot,
        entry: &PassiveEntryEconomics,
    ) -> QuantResult<Option<BookWalkFill>> {
        let tape = &snapshot.passive_tape;
        let expires_at = entry
            .decision_at
            .checked_add_signed(Duration::seconds(
                i64::try_from(entry.good_til_secs).map_err(|error| {
                    ResearchError::ValidationMethodology {
                        detail: format!("passive GTD does not fit chrono: {error}"),
                    }
                })?,
            ))
            .ok_or_else(|| ResearchError::ValidationMethodology {
                detail: "passive GTD overflows chrono".to_owned(),
            })?;
        if tape.coverage_through < expires_at {
            return Err(ResearchError::ValidationMethodology {
                detail: format!(
                    "passive tape for token {} covers only through {}, before GTD {}",
                    self.candidate.token_id, tape.coverage_through, expires_at
                ),
            }
            .into());
        }
        let queue_ahead = snapshot
            .bids
            .iter()
            .find(|level| level.price_decimal() == entry.limit_price)
            .map_or(Shares::ZERO, |level| level.size_decimal());
        let mut queue = PassiveQueueState::new(
            tape.stream_session_id,
            entry.limit_price,
            queue_ahead,
            entry.requested_shares,
        );
        let mut prior_sequence = tape.anchor_token_sequence;
        let mut principal = Usd::ZERO;
        let mut fee = Usd::ZERO;
        for trade in tape
            .trades
            .iter()
            .filter(|trade| trade.event_at >= entry.decision_at && trade.event_at <= expires_at)
        {
            if trade.stream_session_id != tape.stream_session_id
                || trade.token_sequence <= prior_sequence
            {
                return Err(ResearchError::ValidationMethodology {
                    detail: format!(
                        "passive tape for token {} resets, duplicates, or reorders its session",
                        self.candidate.token_id
                    ),
                }
                .into());
            }
            prior_sequence = trade.token_sequence;
            let filled = queue.apply_trade(PassiveTrade {
                stream_session_id: trade.stream_session_id,
                side: trade.side,
                price: trade.price,
                shares: trade.shares,
            });
            if filled.is_positive() {
                principal += filled * entry.limit_price;
                fee += snapshot
                    .fee_schedule
                    .fee(
                        LiquidityRole::Maker,
                        entry.limit_price,
                        filled,
                        trade.event_at,
                    )
                    .map_err(|error| ResearchError::ValidationMethodology {
                        detail: format!(
                            "passive fill fee failed for token {}: {error:?}",
                            self.candidate.token_id
                        ),
                    })?;
            }
            if queue.remaining_shares.is_zero() {
                break;
            }
        }
        if queue.filled_shares.is_zero() {
            return Ok(None);
        }
        let immediate_cost =
            ImmediateExecutionCost::new(principal, fee, Usd::ZERO).map_err(|detail| {
                ResearchError::ValidationMethodology {
                    detail: format!("passive immediate cost is invalid: {detail}"),
                }
            })?;
        Ok(Some(BookWalkFill {
            outcome: if queue.remaining_shares.is_zero() {
                BookWalkOutcome::Filled
            } else {
                BookWalkOutcome::Partial
            },
            vwap: Some(entry.limit_price),
            worst_price: Some(entry.limit_price),
            filled_shares: queue.filled_shares,
            immediate_cost,
            account_cash_delta_usd: -immediate_cost.cash_outlay_usd.inner(),
            unfilled_cash_budget: Usd::ZERO,
            unfilled_shares: queue.remaining_shares,
        }))
    }

    fn resolution(self) -> QuantResult<(DateTime<Utc>, PayoutRatio)> {
        let outcome = self
            .lookups
            .outcomes
            .get(self.candidate.market_id.as_str())
            .copied()
            .ok_or_else(|| ResearchError::ValidationMethodology {
                detail: format!(
                    "selected candidate {} has no frozen settlement outcome",
                    self.candidate.signal_candidate_id
                ),
            })?;
        match (
            outcome.resolved_at,
            token_payout(outcome, self.candidate.outcome_side),
        ) {
            (Some(resolved_at), Some(token_payout_ratio))
                if resolved_at > self.tick.decision_at =>
            {
                Ok((resolved_at, token_payout_ratio))
            }
            (None, None) => Err(ResearchError::ValidationMethodology {
                detail: format!(
                    "selected candidate {} is unresolved inside a complete economic replay",
                    self.candidate.signal_candidate_id
                ),
            }
            .into()),
            _ => Err(ResearchError::ValidationMethodology {
                detail: format!(
                    "selected candidate {} has inconsistent or non-causal resolution truth",
                    self.candidate.signal_candidate_id
                ),
            }
            .into()),
        }
    }

    fn validate_economics(self, settlement: &ResolutionBuySettlement) -> QuantResult<()> {
        let tier = &self.allocation.tier;
        let valid = match &tier.entry_execution {
            EntryExecutionEconomics::Aggressive(entry) => {
                settlement.economics.cash_outlay == entry.immediate_cost.cash_outlay_usd
                    && settlement.economics.entry_fee == entry.immediate_cost.total_fee_usd()
                    && settlement.economics.filled_shares == entry.filled_shares
            }
            EntryExecutionEconomics::Passive(entry) => {
                settlement.economics.cash_outlay <= entry.hard_reserved_cash_usd
                    && settlement.economics.filled_shares <= entry.requested_shares
            }
        };
        if !valid {
            return Err(ResearchError::ValidationMethodology {
                detail: format!(
                    "selected economic tier {} entry economics drifted during replay: \
                     actual notional={} fee={} shares={}",
                    tier.economic_tier_id,
                    settlement.economics.cash_outlay,
                    settlement.economics.entry_fee,
                    settlement.economics.filled_shares,
                ),
            }
            .into());
        }
        Ok(())
    }

    fn open_position(
        self,
        market: &BacktestMarketMeta,
        snapshot: &BacktestExecutionSnapshot,
        settlement: &ResolutionBuySettlement,
        entry_vwap: Price,
        resolved_at: DateTime<Utc>,
        token_payout_ratio: PayoutRatio,
    ) -> QuantResult<OpenReplayPosition> {
        let tier = &self.allocation.tier;
        let market_context = self.lookups.context.get(self.candidate.market_id.as_str());
        let allocated_usd = settlement.economics.cash_outlay;
        let calibrated_payout_distribution =
            self.candidate.payout_distribution.ok_or_else(|| {
                ResearchError::ValidationMethodology {
                    detail: format!(
                        "selected candidate {} has no calibrated payout distribution",
                        self.candidate.signal_candidate_id
                    ),
                }
            })?;
        let mut position = OpenReplayPosition {
            route: tier.route,
            market_id: self.candidate.market_id.clone(),
            event_id: tier.event_id.clone(),
            category: market.category,
            token_id: self.candidate.token_id.clone(),
            outcome_side: self.candidate.outcome_side,
            shares: settlement.economics.filled_shares,
            entry_vwap,
            cash_outlay: allocated_usd,
            current_mark: Price::ZERO,
            current_value: Usd::ZERO,
            resolved_at,
            payout_usd: settlement.payout_usd,
            calibrated_payout_distribution,
            observed_exit_capacity_shares: self.allocation.observed_exit_capacity_shares,
            base_capital_release_secs: self.candidate.suggested_horizon_secs,
            entry_lineage_hash: tier.lineage_hash,
            current_lineage_hash: tier.lineage_hash,
            entry_tick_index: self.entry_tick_index,
            sample: SampleOutcome {
                decision_at: self.tick.decision_at,
                market_id: self.candidate.market_id.clone(),
                token_id: self.candidate.token_id.clone(),
                category: market.category,
                outcome_side: self.candidate.outcome_side,
                composite_score: self.candidate.composite_score,
                confidence: self.candidate.confidence,
                expected_return_bps: self.candidate.expected_return_bps,
                realized_return_bps: settlement.realized_return_bps.inner(),
                token_payout_ratio,
                max_adverse_excursion_bps: candidate_downside(
                    self.candidate,
                    &self.lookups.downside,
                )?,
                allocated_usd,
                entry_fee_usd: settlement.economics.entry_fee,
                filled_shares: settlement.economics.filled_shares,
                fee_schedule_hash: snapshot.fee_schedule.schedule_hash,
                book_hash: snapshot.book_hash,
                liquidity_feasible: self.allocation.liquidity_feasible,
                data_quality: market_context
                    .map_or(DataQualityStatus::Insufficient, |value| value.data_quality),
                liquidity_usd: market_context.and_then(|value| value.liquidity_usd),
                time_to_resolution_secs: market_context
                    .and_then(|value| value.time_to_resolution_secs),
                prediction_horizon_secs: self.candidate.suggested_horizon_secs,
                substitution_reasons: market_context
                    .map_or_else(Vec::new, |value| value.substitution_reasons.clone()),
            },
        };
        position.mark_entry(snapshot, self.tick.decision_at)?;
        Ok(position)
    }
}

fn append_calibration_outcomes(
    decision_at: DateTime<Utc>,
    output: &ModelRuntimeOutput,
    outcomes: &BTreeMap<&str, &MarketOutcome>,
    downside: &BTreeMap<&str, &BacktestDownsideTrajectory>,
    calibration_outcomes: &mut Vec<ModelCalibrationOutcome>,
) -> QuantResult<()> {
    for score in &output.calibration_scores {
        let Some(outcome) = outcomes.get(score.market_id.as_str()) else {
            continue;
        };
        let Some(token_payout_ratio) = token_payout(outcome, score.outcome_side) else {
            continue;
        };
        calibration_outcomes.push(ModelCalibrationOutcome {
            decision_at,
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

fn record_batches(
    acc: &mut RunAccumulator,
    batches: Vec<ReplaySettlementBatch>,
    capital_base: Usd,
) -> QuantResult<()> {
    for batch in batches {
        record_settlements(acc, batch.settlements)?;
        record_equity(acc, batch.resolved_at, batch.net_liquidation, capital_base)?;
    }
    Ok(())
}

fn record_settlements(
    acc: &mut RunAccumulator,
    settlements: Vec<ReplaySettlement>,
) -> QuantResult<()> {
    let settlement_count =
        u64::try_from(settlements.len()).map_err(|error| ResearchError::ValidationMethodology {
            detail: format!("replay settlement count does not fit u64: {error}"),
        })?;
    for settlement in settlements {
        let cohort = acc
            .cohort_pnl
            .get_mut(settlement.entry_tick_index)
            .ok_or_else(|| ResearchError::ValidationMethodology {
                detail: format!(
                    "replay settlement references missing entry tick {}",
                    settlement.entry_tick_index
                ),
            })?;
        *cohort = cohort.checked_add(settlement.realized_pnl).ok_or_else(|| {
            ResearchError::ValidationMethodology {
                detail: "decision-cohort PnL overflowed Decimal".to_owned(),
            }
        })?;
        acc.realized_pnl = acc
            .realized_pnl
            .checked_add(settlement.realized_pnl)
            .ok_or_else(|| ResearchError::ValidationMethodology {
                detail: "backtest cumulative realized PnL overflowed Decimal".to_owned(),
            })?;
        acc.samples.push(settlement.sample);
    }
    acc.portfolio_funnel.resolve(settlement_count)
}

fn record_equity(
    acc: &mut RunAccumulator,
    at: DateTime<Utc>,
    net_liquidation: Usd,
    capital_base: Usd,
) -> QuantResult<()> {
    let cumulative_pnl = net_liquidation
        .inner()
        .checked_sub(capital_base.inner())
        .ok_or_else(|| ResearchError::ValidationMethodology {
            detail: "replay equity minus capital base overflowed Decimal".to_owned(),
        })?
        .round_dp(RESEARCH_DECIMAL_SCALE);
    if let Some(last) = acc.pnl_curve.last_mut() {
        if last.decision_at > at {
            return Err(ResearchError::ValidationMethodology {
                detail: "replay equity events are not time ordered".to_owned(),
            }
            .into());
        }
        if last.decision_at == at {
            last.cumulative_realized_pnl_usd = cumulative_pnl;
            return Ok(());
        }
    }
    acc.pnl_curve.push(PnlCurvePoint {
        decision_at: at,
        cumulative_realized_pnl_usd: cumulative_pnl,
    });
    Ok(())
}

fn finalize_cohorts(acc: &mut RunAccumulator, capital_base: Usd) -> QuantResult<()> {
    if !capital_base.is_positive() {
        return Err(ResearchError::ValidationMethodology {
            detail: "backtest account capital base must be positive".to_owned(),
        }
        .into());
    }
    if acc.decision_times.len() != acc.cohort_pnl.len()
        || acc.decision_times.len() != acc.tick_cash_turnover.len()
        || !acc.tick_returns.is_empty()
        || !acc.portfolio_returns.is_empty()
    {
        return Err(ResearchError::ValidationMethodology {
            detail: "replay decision-cohort accumulator lengths are inconsistent".to_owned(),
        }
        .into());
    }
    let mut cohort_total = Decimal::ZERO;
    for cohort_pnl in &acc.cohort_pnl {
        cohort_total = cohort_total.checked_add(*cohort_pnl).ok_or_else(|| {
            ResearchError::ValidationMethodology {
                detail: "decision-cohort total PnL overflowed Decimal".to_owned(),
            }
        })?;
    }
    if cohort_total != acc.realized_pnl {
        return Err(ResearchError::ValidationMethodology {
            detail: format!(
                "decision-cohort PnL {cohort_total} differs from settled PnL {}",
                acc.realized_pnl
            ),
        }
        .into());
    }
    for (&decision_at, &cohort_pnl) in acc.decision_times.iter().zip(&acc.cohort_pnl) {
        let net_return_bps = Bps::relative(cohort_pnl, capital_base.inner()).ok_or_else(|| {
            ResearchError::ValidationMethodology {
                detail: "positive governed capital produced no decision-cohort return ratio"
                    .to_owned(),
            }
        })?;
        acc.tick_returns.push(cohort_pnl / capital_base.inner());
        acc.portfolio_returns.push(PortfolioReturnObservation {
            decision_at,
            realized_pnl_usd: Usd::new(cohort_pnl.round_dp(RESEARCH_DECIMAL_SCALE)),
            capital_base_usd: capital_base,
            net_return_bps: Bps::new(net_return_bps.inner().round_dp(RESEARCH_DECIMAL_SCALE)),
        });
    }
    Ok(())
}

#[derive(Clone, Copy)]
struct TickPortfolioContext<'a> {
    tick: &'a BacktestTick,
    output: &'a ModelRuntimeOutput,
    scenario_model: &'a VerifiedPortfolioScenarioModel<'a>,
    scenario_visibility: PortfolioScenarioVisibility,
    open_positions: &'a [OpenReplayPosition],
    current_drawdown: Usd,
}

struct PreparedTickPortfolio {
    scenario_artifact: SealedPortfolioScenarioArtifact,
    observed_capacity_by_candidate: HashMap<SignalCandidateId, Shares>,
    tiers: Vec<ExecutableEconomicTier>,
}

impl TickPortfolioContext<'_> {
    fn allocate(self) -> QuantResult<TickPortfolioDecision> {
        let emitted_candidate_count =
            u64::try_from(self.output.candidates.len()).map_err(|error| {
                ResearchError::ValidationMethodology {
                    detail: format!("backtest emitted candidate count does not fit u64: {error}"),
                }
            })?;
        if self.output.candidates.is_empty() {
            return Ok(TickPortfolioDecision {
                allocations: Vec::new(),
                funnel: TickPortfolioFunnel::default(),
            });
        }
        let contract = &self.tick.portfolio_contract;
        if &contract.represented_routes != self.scenario_model.represented_routes() {
            return Err(ResearchError::ValidationMethodology {
                detail: "backtest tick Route set differs from the verified scenario contract"
                    .to_owned(),
            }
            .into());
        }
        let seeded = seed_tiers(self.tick, self.output, self.scenario_model)?;
        let missing_tier_count = seeded.candidate_without_executable_tier_count;
        if seeded.tiers.is_empty() {
            return Ok(TickPortfolioDecision {
                allocations: Vec::new(),
                funnel: TickPortfolioFunnel {
                    emitted_candidate_count,
                    candidate_without_executable_tier_count: missing_tier_count,
                    ..TickPortfolioFunnel::default()
                },
            });
        }
        let prepared = self.prepare(seeded.tiers)?;
        self.solve(&prepared, emitted_candidate_count, missing_tier_count)
    }

    fn prepare(self, seeded_tiers: Vec<ExecutableTierSeed>) -> QuantResult<PreparedTickPortfolio> {
        let contract = &self.tick.portfolio_contract;
        let legs = scenario_legs(
            contract,
            self.output,
            &seeded_tiers,
            self.open_positions,
            self.tick.decision_at,
        )?;
        let input_universe_hash = CanonicalDigest::content_hash_typed(
            "quant-pivot/backtest-scenario-input-universe",
            1,
            &(
                self.tick.decision_at,
                self.scenario_model.model().content_hash,
                contract.represented_routes.digest,
                &legs,
            ),
        )?;
        let scenario_artifact =
            PortfolioScenarioGenerator::generate(PortfolioScenarioGenerationInput {
                model_contract: self.scenario_model,
                decision_at: self.tick.decision_at,
                visibility: self.scenario_visibility,
                input_universe_hash,
                legs: &legs,
            })?;
        let mut observed_capacity_by_candidate = HashMap::new();
        for seed in &seeded_tiers {
            if observed_capacity_by_candidate
                .insert(seed.candidate_id, seed.observed_exit_capacity_shares)
                .is_some_and(|capacity| capacity != seed.observed_exit_capacity_shares)
            {
                return Err(ResearchError::ValidationMethodology {
                    detail: format!(
                        "candidate {} has inconsistent exit capacity across executable tiers",
                        seed.candidate_id
                    ),
                }
                .into());
            }
        }
        let tiers = seeded_tiers
            .into_iter()
            .map(|seed| EconomicTierFactory::build(seed, &scenario_artifact))
            .collect::<QuantResult<Vec<_>>>()?;
        Ok(PreparedTickPortfolio {
            scenario_artifact,
            observed_capacity_by_candidate,
            tiers,
        })
    }

    fn solve(
        self,
        prepared: &PreparedTickPortfolio,
        emitted_candidate_count: u64,
        missing_tier_count: u64,
    ) -> QuantResult<TickPortfolioDecision> {
        let contract = &self.tick.portfolio_contract;
        let portfolio_plan_hash = CanonicalDigest::content_hash_typed(
            "quant-pivot/backtest-portfolio-plan",
            1,
            &(
                self.tick.decision_at,
                contract.report_route_run_id,
                self.scenario_model.model().content_hash,
                &contract.policy,
                &contract.solver,
            ),
        )?;
        let existing = ExistingPortfolioFactory::build(
            &contract.account,
            self.current_drawdown,
            &prepared.scenario_artifact,
        )?;
        let result = GlobalPortfolioPlanner::solve_replay_and_verify(GlobalPortfolioInput {
            portfolio_plan_id: PortfolioPlanId::from_content_hash(&portfolio_plan_hash),
            account: &contract.account,
            existing: &existing,
            represented_routes: &contract.represented_routes,
            scenario_model_binding: self.scenario_model.binding(),
            scenario_artifact: &prepared.scenario_artifact,
            policy: &contract.policy,
            solver: &contract.solver,
            tiers: &prepared.tiers,
            top_n: contract.top_n,
        })?;
        TickPortfolioDecision::from_selection(
            emitted_candidate_count,
            missing_tier_count,
            prepared,
            result.selected,
            &result.rejected,
        )
    }
}

impl TickPortfolioDecision {
    fn from_selection(
        emitted_candidate_count: u64,
        candidate_without_executable_tier_count: u64,
        prepared: &PreparedTickPortfolio,
        selected: Vec<ExecutableEconomicTier>,
        rejected: &[TierAdmissionRejection],
    ) -> QuantResult<Self> {
        let executable_tier_count = u64::try_from(prepared.tiers.len()).map_err(|error| {
            ResearchError::ValidationMethodology {
                detail: format!("backtest executable tier count does not fit u64: {error}"),
            }
        })?;
        let admission_rejected_tier_count = u64::try_from(rejected.len()).map_err(|error| {
            ResearchError::ValidationMethodology {
                detail: format!("backtest rejected tier count does not fit u64: {error}"),
            }
        })?;
        let admitted_tier_count = executable_tier_count
            .checked_sub(admission_rejected_tier_count)
            .ok_or_else(|| ResearchError::ValidationMethodology {
                detail: "backtest planner rejected more tiers than it received".to_owned(),
            })?;
        let selected_tier_count = u64::try_from(selected.len()).map_err(|error| {
            ResearchError::ValidationMethodology {
                detail: format!("backtest selected tier count does not fit u64: {error}"),
            }
        })?;
        let not_selected_count = admitted_tier_count
            .checked_sub(selected_tier_count)
            .ok_or_else(|| ResearchError::ValidationMethodology {
                detail: "backtest planner selected more tiers than it admitted".to_owned(),
            })?;
        let tier_exclusion_reasons = Self::exclusion_reasons(rejected, not_selected_count)?;
        let allocations = selected
            .into_iter()
            .map(|tier| {
                let observed_exit_capacity_shares = prepared
                    .observed_capacity_by_candidate
                    .get(&tier.candidate_id)
                    .copied()
                    .ok_or_else(|| ResearchError::ValidationMethodology {
                        detail: format!(
                            "selected candidate {} lost its frozen exit capacity",
                            tier.candidate_id
                        ),
                    })?;
                Ok(BacktestAllocation {
                    tier,
                    liquidity_feasible: true,
                    observed_exit_capacity_shares,
                })
            })
            .collect::<QuantResult<Vec<_>>>()?;
        Ok(Self {
            allocations,
            funnel: TickPortfolioFunnel {
                emitted_candidate_count,
                candidate_without_executable_tier_count,
                executable_tier_count,
                admission_rejected_tier_count,
                admitted_tier_count,
                selected_tier_count,
                tier_exclusion_reasons,
            },
        })
    }

    fn exclusion_reasons(
        rejected: &[TierAdmissionRejection],
        not_selected_count: u64,
    ) -> QuantResult<BTreeMap<PortfolioRejectionReason, u64>> {
        let mut reasons = BTreeMap::new();
        for rejection in rejected {
            let count = reasons
                .entry(PortfolioRejectionReason::from(rejection.code))
                .or_insert(0_u64);
            *count = count
                .checked_add(1)
                .ok_or_else(|| ResearchError::ValidationMethodology {
                    detail: "backtest tier rejection reason count overflowed u64".to_owned(),
                })?;
        }
        if not_selected_count > 0 {
            reasons.insert(
                PortfolioRejectionReason::NotSelectedByGlobalOptimum,
                not_selected_count,
            );
        }
        Ok(reasons)
    }
}

struct SeededTierBatch {
    tiers: Vec<ExecutableTierSeed>,
    candidate_without_executable_tier_count: u64,
}

fn seed_tiers(
    tick: &BacktestTick,
    output: &ModelRuntimeOutput,
    scenario_model: &VerifiedPortfolioScenarioModel<'_>,
) -> QuantResult<SeededTierBatch> {
    let contract = &tick.portfolio_contract;
    let meta = tick
        .market_meta
        .iter()
        .map(|market| (market.market_id.as_str(), market))
        .collect::<BTreeMap<_, _>>();
    let execution = tick
        .execution
        .iter()
        .map(|snapshot| (snapshot.token_id.as_str(), snapshot))
        .collect::<BTreeMap<_, _>>();
    let mut seeded_tiers = Vec::new();
    let mut candidate_without_executable_tier_count = 0_u64;
    for candidate in &output.candidates {
        let market = meta.get(candidate.market_id.as_str()).ok_or_else(|| {
            ResearchError::ValidationMethodology {
                detail: format!(
                    "candidate {} has no frozen market metadata",
                    candidate.signal_candidate_id
                ),
            }
        })?;
        if BuyModelRoute::from(market.category) != contract.route {
            return Err(ResearchError::ValidationMethodology {
                detail: format!(
                    "candidate {} category {:?} is outside replay Route {:?}",
                    candidate.signal_candidate_id, market.category, contract.route
                ),
            }
            .into());
        }
        let snapshot = execution.get(candidate.token_id.as_str()).ok_or_else(|| {
            ResearchError::ValidationMethodology {
                detail: format!(
                    "candidate {} has no frozen executable L2 snapshot",
                    candidate.signal_candidate_id
                ),
            }
        })?;
        if snapshot.market_id != candidate.market_id {
            return Err(ResearchError::ValidationMethodology {
                detail: format!(
                    "candidate {} L2 snapshot market differs from the candidate",
                    candidate.signal_candidate_id
                ),
            }
            .into());
        }
        if snapshot.fill_at != tick.decision_at {
            return Err(ResearchError::ValidationMethodology {
                detail: format!(
                    "candidate {} L2 snapshot clock differs from the decision tick",
                    candidate.signal_candidate_id
                ),
            }
            .into());
        }
        let lineage_hash = CanonicalDigest::content_hash_typed(
            "quant-pivot/backtest-tier-source",
            1,
            &(
                candidate.model_run_id,
                candidate.signal_candidate_id,
                snapshot.book_hash,
                snapshot.fee_schedule.schedule_hash,
                scenario_model.model().content_hash,
            ),
        )?;
        let mut candidate_tiers =
            ExecutableTierLadderSeedFactory::build(&ExecutableTierLadderSeedInput {
                report_route_run_id: contract.report_route_run_id,
                candidate_id: candidate.signal_candidate_id,
                route: contract.route,
                market_id: candidate.market_id.clone(),
                event_id: market.event_id.clone(),
                category: market.category,
                token_id: candidate.token_id.clone(),
                outcome_side: candidate.outcome_side,
                bids: &snapshot.bids,
                asks: &snapshot.asks,
                fee_schedule: &snapshot.fee_schedule,
                fill_at: snapshot.fill_at,
                limit_price: snapshot.limit_price,
                max_notional_usd: Usd::new(
                    contract
                        .policy
                        .exposure_limits
                        .max_single_recommendation_usd
                        .value,
                ),
                source_lineage_hash: lineage_hash,
            })?;
        if candidate_tiers.is_empty() {
            candidate_without_executable_tier_count = candidate_without_executable_tier_count
                .checked_add(1)
                .ok_or_else(|| ResearchError::ValidationMethodology {
                    detail: "backtest non-executable candidate count overflowed u64".to_owned(),
                })?;
            continue;
        }
        seeded_tiers.append(&mut candidate_tiers);
    }
    Ok(SeededTierBatch {
        tiers: seeded_tiers,
        candidate_without_executable_tier_count,
    })
}

fn scenario_legs(
    contract: &BacktestPortfolioContract,
    output: &ModelRuntimeOutput,
    seeded_tiers: &[ExecutableTierSeed],
    open_positions: &[OpenReplayPosition],
    decision_at: DateTime<Utc>,
) -> QuantResult<Vec<PortfolioScenarioLegInput>> {
    let mut legs = Vec::with_capacity(output.candidates.len() + open_positions.len());
    for candidate in &output.candidates {
        let mut observed_capacities = seeded_tiers
            .iter()
            .filter(|seed| seed.candidate_id == candidate.signal_candidate_id)
            .map(|seed| seed.observed_exit_capacity_shares);
        let Some(observed_exit_capacity_shares) = observed_capacities.next() else {
            continue;
        };
        if observed_capacities.any(|capacity| capacity != observed_exit_capacity_shares) {
            return Err(ResearchError::ValidationMethodology {
                detail: format!(
                    "candidate {} has inconsistent frozen exit capacities across tiers",
                    candidate.signal_candidate_id
                ),
            }
            .into());
        }
        let calibrated_payout_distribution =
            candidate
                .payout_distribution
                .ok_or_else(|| ResearchError::ValidationMethodology {
                    detail: format!(
                        "candidate {} has no calibrated payout distribution",
                        candidate.signal_candidate_id
                    ),
                })?;
        let lineage_hash = CanonicalDigest::content_hash_typed(
            "quant-pivot/backtest-scenario-leg",
            1,
            &(
                candidate.model_run_id,
                candidate.signal_candidate_id,
                candidate.market_id.as_str(),
                candidate.token_id.as_str(),
                candidate.outcome_side,
                calibrated_payout_distribution,
                observed_exit_capacity_shares,
                candidate.suggested_horizon_secs,
            ),
        )?;
        legs.push(PortfolioScenarioLegInput {
            route: contract.route,
            market_id: candidate.market_id.clone(),
            token_id: candidate.token_id.clone(),
            outcome_side: candidate.outcome_side,
            calibrated_payout_distribution,
            observed_exit_capacity_shares,
            base_capital_release_secs: candidate.suggested_horizon_secs,
            lineage_hash,
        });
    }
    for position in open_positions {
        if position.resolved_at <= decision_at {
            return Err(ResearchError::ValidationMethodology {
                detail: format!(
                    "resolved position {} remained open at decision {}",
                    position.token_id, decision_at
                ),
            }
            .into());
        }
        if legs.iter().any(|leg| {
            leg.route == position.route
                && leg.market_id == position.market_id
                && leg.token_id == position.token_id
                && leg.outcome_side == position.outcome_side
        }) {
            continue;
        }
        legs.push(PortfolioScenarioLegInput {
            route: position.route,
            market_id: position.market_id.clone(),
            token_id: position.token_id.clone(),
            outcome_side: position.outcome_side,
            calibrated_payout_distribution: position.calibrated_payout_distribution,
            observed_exit_capacity_shares: position.observed_exit_capacity_shares,
            // The future realized resolution time is outcome truth, not PIT
            // decision information. Keep the governed entry-time holding
            // horizon as the conservative scenario clock until settlement.
            base_capital_release_secs: position.base_capital_release_secs,
            lineage_hash: position.current_lineage_hash,
        });
    }
    legs.sort_by(|left, right| {
        (
            left.route,
            left.market_id.as_str(),
            left.token_id.as_str(),
            left.outcome_side.as_str(),
        )
            .cmp(&(
                right.route,
                right.market_id.as_str(),
                right.token_id.as_str(),
                right.outcome_side.as_str(),
            ))
    });
    Ok(legs)
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

fn record_candidate_coverage(
    tick: &BacktestTick,
    output: &ModelRuntimeOutput,
    outcomes: &BTreeMap<&str, &MarketOutcome>,
    acc: &mut RunAccumulator,
) -> QuantResult<()> {
    for candidate in &output.candidates {
        let Some(outcome) = outcomes.get(candidate.market_id.as_str()) else {
            continue;
        };
        match (
            outcome.resolved_at,
            token_payout(outcome, candidate.outcome_side),
        ) {
            (Some(resolved_at), Some(_)) if resolved_at > tick.decision_at => {
                acc.resolved_emitted = acc.resolved_emitted.checked_add(1).ok_or_else(|| {
                    ResearchError::ValidationMethodology {
                        detail: "resolved emitted-candidate count overflowed u64".to_owned(),
                    }
                })?;
            }
            (None, None) => {}
            _ => {
                return Err(ResearchError::ValidationMethodology {
                    detail: format!(
                        "candidate {} has inconsistent or non-causal resolution coverage truth",
                        candidate.signal_candidate_id
                    ),
                }
                .into());
            }
        }
    }
    Ok(())
}

/// Aggregated inputs for report assembly.
struct BuildMetrics<'a> {
    samples: &'a [SampleOutcome],
    pnl_curve: &'a [PnlCurvePoint],
    tick_cash_turnover: &'a [Decimal],
    tick_returns: &'a [Decimal],
    missing_feature_count: u64,
    total_emitted: u64,
    resolved_emitted: u64,
    total_allocated: Decimal,
    realized_pnl: Decimal,
    budget: Decimal,
    portfolio_funnel: &'a BacktestPortfolioFunnel,
}

fn governed_budget(ticks: &[PrecomputedBacktestTick]) -> QuantResult<Decimal> {
    let Some(first) = ticks.first() else {
        return Ok(Decimal::ZERO);
    };
    let expected = first
        .tick
        .portfolio_contract
        .policy
        .budget
        .total_budget_usd
        .value;
    if ticks.iter().any(|input| {
        input
            .tick
            .portfolio_contract
            .policy
            .budget
            .total_budget_usd
            .value
            != expected
    }) {
        return Err(ResearchError::ValidationMethodology {
            detail: "backtest ticks do not share one frozen total-budget contract".to_owned(),
        }
        .into());
    }
    Ok(expected)
}

/// Assemble the metrics + canonical report hash.
fn build_report(request: &BacktestRequest, m: &BuildMetrics<'_>) -> QuantResult<BacktestReport> {
    let sample_count =
        u64::try_from(m.samples.len()).map_err(|error| ResearchError::ValidationMethodology {
            detail: format!("backtest sample count does not fit u64: {error}"),
        })?;
    if m.portfolio_funnel.emitted_candidate_count != m.total_emitted
        || m.portfolio_funnel.resolved_allocation_count != sample_count
    {
        return Err(ResearchError::ValidationMethodology {
            detail: format!(
                "backtest report/funnel conservation failed: emitted={}/{}, resolved={}/{}",
                m.total_emitted,
                m.portfolio_funnel.emitted_candidate_count,
                sample_count,
                m.portfolio_funnel.resolved_allocation_count
            ),
        }
        .into());
    }
    if m.resolved_emitted > m.total_emitted {
        return Err(ResearchError::ValidationMethodology {
            detail: "resolved emitted candidates exceed the emitted population".to_owned(),
        }
        .into());
    }
    let coverage = if m.total_emitted > 0 {
        (Decimal::from(m.resolved_emitted) / Decimal::from(m.total_emitted))
            .round_dp(RESEARCH_DECIMAL_SCALE)
    } else {
        Decimal::ZERO
    };
    let realized_return_rank_correlation = metrics::realized_return_rank_correlation(m.samples);
    let sharpe = metrics::sharpe_ratio(m.tick_returns, Decimal::ONE);
    let hit_rate = metrics::hit_rate(m.samples);
    let expected_vs_realized = metrics::expected_vs_realized(m.samples);
    let max_drawdown = metrics::max_drawdown(m.pnl_curve, m.budget);
    let turnover = metrics::executed_cash_turnover(m.tick_cash_turnover)?;
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
        realized_return_rank_correlation,
        sharpe,
        hit_rate,
        expected_vs_realized: &expected_vs_realized,
        max_drawdown,
        turnover,
        liquidity_feasibility,
        category_breakdown: &category_breakdown,
        tail_loss,
        report_pnl_simulation: &report_pnl_simulation,
        portfolio_funnel: m.portfolio_funnel,
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
        realized_return_rank_correlation,
        sharpe,
        hit_rate,
        expected_vs_realized,
        max_drawdown,
        turnover,
        liquidity_feasibility,
        category_breakdown,
        tail_loss,
        report_pnl_simulation,
        portfolio_funnel: m.portfolio_funnel.clone(),
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
            realized_return_rank_correlation: self.realized_return_rank_correlation,
            sharpe: self.sharpe,
            hit_rate: self.hit_rate,
            expected_vs_realized: &self.expected_vs_realized,
            max_drawdown: self.max_drawdown,
            turnover: self.turnover,
            liquidity_feasibility: self.liquidity_feasibility,
            category_breakdown: &self.category_breakdown,
            tail_loss: self.tail_loss,
            report_pnl_simulation: &self.report_pnl_simulation,
            portfolio_funnel: &self.portfolio_funnel,
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
    use std::{collections::HashSet, sync::Arc};

    use chrono::{DateTime, Duration, TimeZone, Utc};
    use quant_pivot_error::{QuantError, research::ResearchError};
    use quant_pivot_models::{
        config::PortfolioSolverDeployConfig,
        domain::market::{book::BookLevel, fee::BuilderFeeAttribution},
        domain::quant::{
            DiscountCurvePoint, PortfolioScenarioEvidenceRegime, PortfolioScenarioFitEvidence,
            PortfolioScenarioKind, PortfolioScenarioModelArtifact, PortfolioScenarioModelState,
            PortfolioScenarioResamplingMethod, PortfolioScenarioRouteFactor,
            PortfolioScenarioRouteFitLineage, PortfolioScenarioRouteModelLineage,
            PortfolioScenarioVisibility, RepresentedRouteSet, ScenarioDistribution, ScenarioWeight,
        },
        enums::{
            common::MarketCategory,
            quant::{AccountSource, DataQualityStatus, FactorDirection},
        },
        hashing::CanonicalDigest,
        runtime_config::{BuyModelRoute, PortfolioConfig, PortfolioScenarioModelArtifactBinding},
        types::{
            BacktestPathSetId, BacktestReportId, Bps, CalibrationArtifactId,
            DecisionPolicySnapshotId, EventId, MarketId, ModelRunId, ModelVersionId, PayoutRatio,
            PortfolioPlanId, PortfolioRejectionReason, PortfolioScenarioModelArtifactId, Price,
            Probability, ReportRouteRunId, SchemaVersion, Shares, TokenId, TrainingDatasetId, Usd,
            calibration::{
                IsotonicKnot, MonotoneMapping, ReliabilityBin, ReliabilityReport,
                SplitPayoutRateEvidence,
            },
            factor::FactorExplanation,
        },
    };
    use rust_decimal::Decimal;
    use rust_decimal_macros::dec;
    use uuid::Uuid;

    use super::{PortfolioReplayBacktester, TickLookups, scenario_legs, seed_tiers};
    use crate::{
        backtest::{
            BacktestDownsidePoint, BacktestDownsideTrajectory, BacktestExecutionSnapshot,
            BacktestInputs, BacktestLiquidationSnapshot, BacktestMarketMeta, BacktestPassiveTape,
            BacktestPortfolioContract, BacktestRankTarget, BacktestRequest,
            BacktestScenarioContext, BacktestTick, Backtester, MarketOutcome,
        },
        execution_semantics::PitFeeSchedule,
        factors::{FactorValue, NormalizedFactor, names::MOMENTUM_ROC},
        model::{
            CrossFittedRuntime, QuantModelRuntime, ResolvedCalibration,
            artifact::ModelArtifact,
            runtime::{
                FactorInferenceRow, FactorInferenceTable, MarketInferenceContext, ModelRankTarget,
                ModelRuntimeInput,
            },
            weighted::WeightedFactorRuntime,
        },
        portfolio::{
            AccountSnapshot, CapitalTimeBucketContract, EconomicTierFactory,
            ExistingPortfolioFactory, GlobalPortfolioInput, GlobalPortfolioPlanner,
            PortfolioScenarioGenerationInput, PortfolioScenarioGenerator,
        },
        precision::RESEARCH_DECIMAL_SCALE,
        test_support::{content_hash as hash, weighted_factor_plane},
        training::TOKEN_PAYOUT_RATIO,
    };

    impl WeightedFactorRuntime {
        fn backtest_fixture() -> Self {
            Self::new(ModelArtifact::weighted_fixture(), None).expect("runtime")
        }
    }

    impl CrossFittedRuntime {
        fn backtest_fixture() -> Self {
            Self::new(
                Box::new(WeightedFactorRuntime::backtest_fixture()),
                ResolvedCalibration {
                    artifact_id: CalibrationArtifactId::from_v7(),
                    mapping: MonotoneMapping::Isotonic {
                        knots: vec![
                            IsotonicKnot {
                                score: dec!(0),
                                probability: dec!(0.3),
                            },
                            IsotonicKnot {
                                score: dec!(1),
                                probability: dec!(0.7),
                            },
                        ],
                    },
                    reliability: ReliabilityReport {
                        n_samples: 100,
                        bins: vec![ReliabilityBin {
                            predicted_lo: dec!(0),
                            predicted_hi: dec!(1),
                            sample_count: 100,
                            mean_predicted: Probability::new(dec!(0.5)),
                            empirical_frequency: Probability::new(dec!(0.5)),
                            wilson_ci: (Probability::new(dec!(0.4)), Probability::new(dec!(0.6))),
                            mean_adverse_excursion_bps: Some(dec!(-500)),
                        }],
                        ece: dec!(0),
                        brier_score: dec!(0.25),
                        log_loss: dec!(0.693147),
                    },
                    split_payout_rate: SplitPayoutRateEvidence {
                        total_sample_count: 100,
                        split_sample_count: 0,
                        empirical_probability: Probability::ZERO,
                        wilson_ci: (Probability::ZERO, Probability::new(dec!(0.036994))),
                        split_payout_ratio: PayoutRatio::try_new(dec!(0.5))
                            .expect("split payout ratio"),
                    },
                },
            )
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

    fn portfolio_contract(decision_at: DateTime<Utc>) -> BacktestPortfolioContract {
        let routes =
            RepresentedRouteSet::from_routes([BuyModelRoute::Pooled]).expect("represented routes");
        let policy = portfolio_policy();
        BacktestPortfolioContract {
            report_route_run_id: ReportRouteRunId::new(Uuid::from_u128(2)),
            route: BuyModelRoute::Pooled,
            account: AccountSnapshot::new(
                decision_at,
                AccountSource::HistoricalReplay,
                Usd::new(dec!(1000)),
                Usd::new(dec!(1000)),
                Usd::new(dec!(1000)),
                Usd::ZERO,
                Vec::new(),
            ),
            represented_routes: routes,
            policy,
            solver: PortfolioSolverDeployConfig {
                deadline_secs: 10,
                threads: 1,
                max_tiers: 10,
                max_scenarios: 10,
                max_top_n: 10,
            },
            top_n: 10,
        }
    }

    impl BacktestScenarioContext {
        fn fixture() -> Self {
            let decision_at = Utc
                .timestamp_opt(1_700_000_000, 0)
                .single()
                .expect("valid scenario timestamp");
            let routes = RepresentedRouteSet::from_routes([BuyModelRoute::Pooled])
                .expect("represented routes");
            let policy = portfolio_policy();
            let (scenario_model, scenario_model_binding) =
                scenario_contract(decision_at, &routes, &policy);
            Self::try_new(scenario_model_binding, scenario_model, routes)
                .expect("verified backtest scenario context")
        }
    }

    #[test]
    fn scenario_context_shares_contract() {
        let scenario = BacktestScenarioContext::fixture();
        let cloned = scenario.clone();

        assert!(Arc::ptr_eq(&scenario.contract, &cloned.contract));
        assert_eq!(scenario, cloned);
    }

    #[test]
    fn scenario_context_rejects_tamper() {
        let decision_at = Utc
            .timestamp_opt(1_700_000_000, 0)
            .single()
            .expect("valid scenario timestamp");
        let routes =
            RepresentedRouteSet::from_routes([BuyModelRoute::Pooled]).expect("represented routes");
        let policy = portfolio_policy();
        let (mut scenario_model, scenario_model_binding) =
            scenario_contract(decision_at, &routes, &policy);
        scenario_model.as_of += Duration::seconds(1);

        assert!(
            BacktestScenarioContext::try_new(scenario_model_binding, scenario_model, routes)
                .is_err()
        );
    }

    fn portfolio_policy() -> PortfolioConfig {
        let mut policy = PortfolioConfig::default();
        policy.budget.total_budget_usd.value = dec!(1000);
        policy.budget.cash_reserve_usd.value = Decimal::ZERO;
        policy.budget.max_open_capital_usd.value = dec!(1000);
        policy.exposure_limits.max_single_recommendation_usd.value = dec!(200);
        policy.exposure_limits.max_market_exposure_usd.value = dec!(200);
        policy.exposure_limits.max_event_exposure_usd.value = dec!(400);
        policy.exposure_limits.max_category_exposure_usd.value = dec!(600);
        policy.exposure_limits.max_route_exposure_usd.value = dec!(600);
        policy.exposure_limits.max_open_recommendations = 10;
        policy.tail_risk.max_cvar_usd.value = dec!(1000);
        policy.tail_risk.max_scenario_loss_usd.value = dec!(1000);
        policy.tail_risk.max_drawdown_usd.value = dec!(1000);
        policy.admission.min_nominal_expected_net_usd.value = dec!(1);
        policy.admission.min_robust_expected_net_usd.value = dec!(1);
        policy.admission.min_profit_probability_bps = 4_000;
        policy.admission.max_probability_interval_width_bps = 3_000;
        policy
    }

    fn scenario_contract(
        decision_at: DateTime<Utc>,
        routes: &RepresentedRouteSet,
        policy: &PortfolioConfig,
    ) -> (
        PortfolioScenarioModelArtifact,
        PortfolioScenarioModelArtifactBinding,
    ) {
        let bucket_digest =
            CapitalTimeBucketContract::try_from(policy.tail_risk.capital_time_buckets.as_slice())
                .expect("capital-time grid")
                .content_hash()
                .expect("capital-time contract hash");
        let serving_digest = hash("backtest-serving");
        let calibration_digest = hash("backtest-calibration");
        let trade_policy_digest = hash("backtest-trade-policy");
        let mut states = vec![
            scenario_model_state(0, PortfolioScenarioKind::PitBootstrap, "pit_up", 1_000),
            scenario_model_state(
                1,
                PortfolioScenarioKind::CalibrationUncertainty,
                "calibration_down",
                9_000,
            ),
            scenario_model_state(
                2,
                PortfolioScenarioKind::StructuralStress,
                "joint_stress",
                9_500,
            ),
        ];
        for state in &mut states {
            state.scenario_state_hash = state.recomputed_state_hash().expect("scenario state hash");
        }
        let distributions = vec![
            ScenarioDistribution {
                distribution_id: "nominal".to_owned(),
                nominal: true,
                weights: vec![
                    ScenarioWeight {
                        scenario_index: 0,
                        probability_bps: 6_000,
                    },
                    ScenarioWeight {
                        scenario_index: 1,
                        probability_bps: 3_000,
                    },
                    ScenarioWeight {
                        scenario_index: 2,
                        probability_bps: 1_000,
                    },
                ],
            },
            ScenarioDistribution {
                distribution_id: "robust".to_owned(),
                nominal: false,
                weights: vec![
                    ScenarioWeight {
                        scenario_index: 0,
                        probability_bps: 6_000,
                    },
                    ScenarioWeight {
                        scenario_index: 1,
                        probability_bps: 1_500,
                    },
                    ScenarioWeight {
                        scenario_index: 2,
                        probability_bps: 2_500,
                    },
                ],
            },
        ];
        let mut scenario_model = PortfolioScenarioModelArtifact {
            portfolio_scenario_model_artifact_id: PortfolioScenarioModelArtifactId::new(
                Uuid::from_u128(1),
            ),
            schema_version: SchemaVersion::FIRST,
            as_of: decision_at,
            fit_window_start: decision_at - Duration::days(30),
            time_bucket_secs: 3_600,
            ordered_routes: routes.routes.clone(),
            route_set_digest: routes.digest,
            serving_contract_digest: serving_digest,
            calibration_contract_digest: calibration_digest,
            recommendation_contract_digest: trade_policy_digest,
            evidence_regime: PortfolioScenarioEvidenceRegime::FullL2ExecutionEconomics,
            capital_time_bucket_contract_digest: bucket_digest,
            scenario_random_stream_hash: hash("scenario-random-stream"),
            pit_residual_panel_hash: hash("pit-lineage"),
            calibration_uncertainty_model_hash: hash("calibration-uncertainty"),
            stress_catalog_hash: hash("stress-catalog"),
            resampling_method: PortfolioScenarioResamplingMethod::StationaryBootstrap {
                expected_block_length: 8,
                scenario_horizon_buckets: 24,
            },
            route_fit_lineage: vec![PortfolioScenarioRouteFitLineage {
                route: BuyModelRoute::Pooled,
                model_lineage: PortfolioScenarioRouteModelLineage {
                    evaluated_model_version_id: ModelVersionId::from_v7(),
                    evaluated_model_artifact_hash: hash("evaluated-model-artifact"),
                    evaluated_serving_contract_hash: serving_digest,
                    calibration_source_model_version_id: ModelVersionId::from_v7(),
                    calibration_source_model_artifact_hash: hash(
                        "calibration-source-model-artifact",
                    ),
                    calibration_source_serving_contract_hash: hash(
                        "calibration-source-serving-contract",
                    ),
                },
                fit_evidence: PortfolioScenarioFitEvidence::CpcvPath {
                    backtest_path_set_id: BacktestPathSetId::from_v7(),
                    backtest_path_set_hash: hash("backtest-path-set"),
                    representative_path_index: 0,
                },
                calibration_artifact_id: CalibrationArtifactId::from_v7(),
                calibration_artifact_hash: calibration_digest,
                recommendation_contract_hash: trade_policy_digest,
                fit_window_start: decision_at - Duration::days(30),
                fit_window_end: decision_at,
            }],
            states,
            distributions,
            discount_curve: policy
                .tail_risk
                .capital_time_buckets
                .iter()
                .map(|bucket| DiscountCurvePoint {
                    end_secs: bucket.end_secs,
                    annualized_cost_bps: 500,
                })
                .collect(),
            content_hash: hash("pending-scenario-model"),
        };
        scenario_model.content_hash = scenario_model
            .recomputed_hash()
            .expect("scenario model hash");
        scenario_model.portfolio_scenario_model_artifact_id =
            PortfolioScenarioModelArtifactId::from_content_hash(&scenario_model.content_hash);
        let scenario_model_binding = PortfolioScenarioModelArtifactBinding {
            portfolio_scenario_model_artifact_id: scenario_model
                .portfolio_scenario_model_artifact_id,
            ordered_routes: routes.routes.clone(),
            route_set_digest: routes.digest,
            serving_contract_digest: serving_digest,
            calibration_contract_digest: calibration_digest,
            recommendation_contract_digest: trade_policy_digest,
            scenario_model_schema_version: SchemaVersion::FIRST,
            capital_time_bucket_contract_digest: bucket_digest,
            model_content_hash: scenario_model.content_hash,
            bound_at: decision_at,
        };
        (scenario_model, scenario_model_binding)
    }

    fn scenario_model_state(
        scenario_index: u32,
        kind: PortfolioScenarioKind,
        label: &str,
        systematic_quantile_bps: u32,
    ) -> PortfolioScenarioModelState {
        PortfolioScenarioModelState {
            scenario_index,
            kind,
            label: label.to_owned(),
            scenario_state_hash: hash("pending-scenario-state"),
            route_factors: vec![PortfolioScenarioRouteFactor {
                route: BuyModelRoute::Pooled,
                systematic_quantile_bps,
                systematic_weight_bps: 10_000,
                calibrated_probability_shift_bps: 0,
                split_probability_quantile_bps: match kind {
                    PortfolioScenarioKind::PitBootstrap => 5_000,
                    PortfolioScenarioKind::CalibrationUncertainty => 0,
                    PortfolioScenarioKind::StructuralStress => 10_000,
                },
                win_cash_recovery_bps: 10_000,
                split_cash_recovery_bps: 5_000,
                loss_cash_recovery_bps: 0,
                executable_share_bps: 10_000,
                capital_release_multiplier_bps: 10_000,
                factor_lineage_hash: hash(&format!("scenario-factor-{scenario_index}")),
            }],
        }
    }

    fn tick(idx: i64, model_run_id: &ModelRunId) -> BacktestTick {
        let as_of = Utc.timestamp_opt(1_700_000_000 + idx * 3600, 0).unwrap();
        let portfolio_contract = portfolio_contract(as_of);
        // Bullish market settles YES (correct), bearish settles NO (correct).
        let meta = vec![
            BacktestMarketMeta {
                market_id: MarketId::new("0xbull"),
                category: MarketCategory::Sports,
                event_id: EventId::new("bull-event"),
                liquidity_usd: Some(Usd::new(dec!(50000))),
            },
            BacktestMarketMeta {
                market_id: MarketId::new("0xbear"),
                category: MarketCategory::Sports,
                event_id: EventId::new("bear-event"),
                liquidity_usd: Some(Usd::new(dec!(50000))),
            },
        ];
        let execution = vec![
            execution_snapshot("0xbull", "yes", as_of, dec!(0.5)),
            execution_snapshot("0xbear", "no", as_of, dec!(0.52)),
        ];
        let liquidation = execution
            .iter()
            .map(BacktestLiquidationSnapshot::from)
            .collect::<Vec<_>>();
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
                    resolved_at: Some(as_of + Duration::hours(2)),
                    yes_payout_ratio: Some(PayoutRatio::ONE),
                },
                MarketOutcome {
                    market_id: MarketId::new("0xbear"),
                    resolved_at: Some(as_of + Duration::hours(2)),
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
            execution,
            liquidation,
            downside_trajectories: Vec::new(),
            portfolio_contract,
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
            liquidation: Vec::new(),
            downside_trajectories: Vec::new(),
            portfolio_contract: portfolio_contract(decision_at),
        }
    }

    impl BacktestTick {
        fn assert_duplicate_rejected(&self) {
            match TickLookups::try_from(self) {
                Err(QuantError::Research(ResearchError::ValidationMethodology { .. })) => {}
                Err(error) => panic!("unexpected duplicate-input error: {error}"),
                Ok(_) => panic!("duplicate PIT replay input must fail closed"),
            }
        }
    }

    #[test]
    fn duplicate_tick_inputs_rejected() {
        let model_run_id = ModelRunId::from_v7();

        let mut duplicate_meta = tick(0, &model_run_id);
        duplicate_meta
            .market_meta
            .push(duplicate_meta.market_meta[0].clone());
        duplicate_meta.assert_duplicate_rejected();

        let mut duplicate_outcome = tick(0, &model_run_id);
        duplicate_outcome
            .outcomes
            .push(duplicate_outcome.outcomes[0].clone());
        duplicate_outcome.assert_duplicate_rejected();

        let mut duplicate_context = tick(0, &model_run_id);
        match &mut duplicate_context.model_input {
            ModelRuntimeInput::FactorTable(table) => table.rows.push(table.rows[0].clone()),
            ModelRuntimeInput::FeatureMatrix(_) => {
                panic!("weighted backtest fixture must use a factor table")
            }
        }
        duplicate_context.assert_duplicate_rejected();

        let mut duplicate_execution = tick(0, &model_run_id);
        duplicate_execution
            .execution
            .push(duplicate_execution.execution[0].clone());
        duplicate_execution.assert_duplicate_rejected();

        let mut duplicate_liquidation = tick(0, &model_run_id);
        duplicate_liquidation
            .liquidation
            .push(duplicate_liquidation.liquidation[0].clone());
        duplicate_liquidation.assert_duplicate_rejected();

        let mut duplicate_downside = tick(0, &model_run_id);
        let downside = BacktestDownsideTrajectory {
            market_id: MarketId::new("0xbull"),
            token_id: TokenId::new("yes"),
            anchor: duplicate_downside.decision_at,
            entry_ask: Price::new(dec!(0.5)),
            data_available_until: duplicate_downside.decision_at,
            points: Vec::new(),
        };
        duplicate_downside.downside_trajectories = vec![downside.clone(), downside];
        duplicate_downside.assert_duplicate_rejected();
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
            bids: vec![BookLevel::from_decimal_unchecked(
                Price::new(price - dec!(0.01)),
                Shares::new(dec!(100000)),
            )],
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
            passive_tape: BacktestPassiveTape {
                stream_session_id: Uuid::from_u128(1),
                anchor_token_sequence: 1,
                coverage_through: at + Duration::days(1),
                trades: Vec::new(),
            },
        }
    }

    impl BacktestRequest {
        fn test_fixture(model_version_id: ModelVersionId) -> Self {
            Self {
                backtest_report_id: BacktestReportId::from_v7(),
                model_version_id,
                dataset_id: TrainingDatasetId::from_v7(),
                decision_policy_snapshot_id: DecisionPolicySnapshotId::from_v7(),
                window_start: Utc.timestamp_opt(1_700_000_000, 0).unwrap(),
                window_end: Utc.timestamp_opt(1_700_100_000, 0).unwrap(),
            }
        }
    }

    #[tokio::test]
    async fn backtest_report_metrics_complete() {
        let model = CrossFittedRuntime::backtest_fixture();
        let scenario = BacktestScenarioContext::fixture();
        let run_id = ModelRunId::from_v7();
        let ticks = vec![tick(0, &run_id), tick(1, &run_id)];
        let result = PortfolioReplayBacktester::new()
            .run(BacktestInputs {
                request: BacktestRequest::test_fixture(model.model_version_id()),
                model: &model,
                scenario: &scenario,
                scenario_visibility: PortfolioScenarioVisibility::PointInTime,
                ticks,
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
        report
            .portfolio_funnel
            .validate()
            .expect("count-conserving portfolio funnel");
        assert_eq!(
            report.portfolio_funnel.resolved_allocation_count,
            report.sample_count
        );
        assert_eq!(
            report.portfolio_funnel.executed_entry_count,
            report.portfolio_funnel.selected_tier_count
        );
        report.verify_hash().expect("report hash preimage");
        let mut tampered = report.clone();
        tampered.realized_return_rank_correlation += dec!(0.01);
        assert!(
            tampered.verify_hash().is_err(),
            "a cached report field mutation must invalidate its canonical hash"
        );
    }

    #[tokio::test]
    async fn capital_lock_until_resolution() {
        let model = CrossFittedRuntime::backtest_fixture();
        let scenario = BacktestScenarioContext::fixture();
        let run_id = ModelRunId::from_v7();
        let result = PortfolioReplayBacktester::new()
            .run(BacktestInputs {
                request: BacktestRequest::test_fixture(model.model_version_id()),
                model: &model,
                scenario: &scenario,
                scenario_visibility: PortfolioScenarioVisibility::PointInTime,
                ticks: vec![tick(0, &run_id), tick(1, &run_id)],
            })
            .await
            .expect("stateful capital replay");

        assert_eq!(result.tick_cash_turnover.len(), 2);
        assert!(result.tick_cash_turnover[0] > Decimal::ZERO);
        assert_eq!(
            result.tick_cash_turnover[1],
            Decimal::ZERO,
            "cash committed at the first tick cannot be recycled before resolution"
        );
        assert_eq!(
            result.report.turnover,
            (result.tick_cash_turnover[0] / Decimal::from(2)).round_dp(RESEARCH_DECIMAL_SCALE),
            "turnover is mean executed entry cash over every fixed-cadence tick"
        );
        assert_eq!(result.report.portfolio_funnel.selected_tick_count, 1);
        assert_eq!(result.report.portfolio_funnel.no_selection_tick_count, 1);
        assert!(
            result
                .sample_outcomes
                .iter()
                .all(|sample| sample.decision_at == result.portfolio_returns[0].decision_at),
            "only the funded first decision cohort may execute"
        );
    }

    #[tokio::test]
    async fn drawdown_uses_exit_mark() {
        let model = CrossFittedRuntime::backtest_fixture();
        let scenario = BacktestScenarioContext::fixture();
        let run_id = ModelRunId::from_v7();
        let first = tick(0, &run_id);
        let mut stressed = tick(1, &run_id);
        for (ordinal, snapshot) in stressed.liquidation.iter_mut().enumerate() {
            snapshot.bids = vec![BookLevel::from_decimal_unchecked(
                Price::new(dec!(0.10)),
                Shares::new(dec!(100000)),
            )];
            snapshot.book_hash = hash(&format!("stressed-exit-book-{ordinal}"));
        }
        let result = PortfolioReplayBacktester::new()
            .run(BacktestInputs {
                request: BacktestRequest::test_fixture(model.model_version_id()),
                model: &model,
                scenario: &scenario,
                scenario_visibility: PortfolioScenarioVisibility::PointInTime,
                ticks: vec![first, stressed],
            })
            .await
            .expect("executable-mark replay");

        let curve = &result.report.report_pnl_simulation.pnl_curve;
        assert!(
            curve.len() >= 3,
            "entry, re-mark, and settlement are recorded"
        );
        assert!(
            curve[1].cumulative_realized_pnl_usd < curve[0].cumulative_realized_pnl_usd,
            "a lower executable bid must lower interim net liquidation"
        );
        assert!(
            result.report.max_drawdown > dec!(0.10),
            "full-depth bid liquidation, including spread and sell fee, must drive drawdown"
        );
    }

    #[tokio::test]
    async fn open_position_requires_mark() {
        let model = CrossFittedRuntime::backtest_fixture();
        let scenario = BacktestScenarioContext::fixture();
        let run_id = ModelRunId::from_v7();
        let first = tick(0, &run_id);
        let mut missing = tick(1, &run_id);
        missing
            .liquidation
            .retain(|snapshot| snapshot.token_id.as_str() != "yes");
        let error = PortfolioReplayBacktester::new()
            .run(BacktestInputs {
                request: BacktestRequest::test_fixture(model.model_version_id()),
                model: &model,
                scenario: &scenario,
                scenario_visibility: PortfolioScenarioVisibility::PointInTime,
                ticks: vec![first, missing],
            })
            .await
            .expect_err("an unmarked open position must fail closed");

        assert!(
            error
                .to_string()
                .contains("has no exact PIT liquidation snapshot"),
            "unexpected mark-evidence error: {error}"
        );
    }

    #[tokio::test]
    async fn rotating_universe_retains_marks() {
        let model = CrossFittedRuntime::backtest_fixture();
        let scenario = BacktestScenarioContext::fixture();
        let run_id = ModelRunId::from_v7();
        let first = tick(0, &run_id);
        let marks = tick(1, &run_id).liquidation;
        let mut rotated = empty_tick(1, run_id);
        rotated.liquidation = marks;

        let result = PortfolioReplayBacktester::new()
            .run(BacktestInputs {
                request: BacktestRequest::test_fixture(model.model_version_id()),
                model: &model,
                scenario: &scenario,
                scenario_visibility: PortfolioScenarioVisibility::PointInTime,
                ticks: vec![first, rotated],
            })
            .await
            .expect("rotated universe with independent liquidation plane");

        assert_eq!(result.tick_cash_turnover.len(), 2);
        assert_eq!(result.tick_cash_turnover[1], Decimal::ZERO);
        assert_eq!(result.report.portfolio_funnel.selected_tick_count, 1);
    }

    #[tokio::test]
    async fn replay_matches_report_solve() {
        let model = CrossFittedRuntime::backtest_fixture();
        let scenario = BacktestScenarioContext::fixture();
        let run_id = ModelRunId::from_v7();
        let replay_tick = tick(0, &run_id);
        let output = model
            .infer_batch(replay_tick.model_input.clone())
            .await
            .expect("model inference");
        let contract = &replay_tick.portfolio_contract;
        assert_eq!(scenario.represented_routes(), &contract.represented_routes);
        let scenario_model = scenario.verified();
        let seeded = seed_tiers(&replay_tick, &output, &scenario_model).expect("executable tiers");
        let legs = scenario_legs(
            contract,
            &output,
            &seeded.tiers,
            &[],
            replay_tick.decision_at,
        )
        .expect("scenario legs");
        let input_universe_hash = CanonicalDigest::content_hash_typed(
            "quant-pivot/backtest-scenario-input-universe",
            1,
            &(
                replay_tick.decision_at,
                scenario.model().content_hash,
                contract.represented_routes.digest,
                &legs,
            ),
        )
        .expect("scenario input hash");
        let scenario_artifact =
            PortfolioScenarioGenerator::generate(PortfolioScenarioGenerationInput {
                model_contract: &scenario_model,
                decision_at: replay_tick.decision_at,
                visibility: PortfolioScenarioVisibility::PointInTime,
                input_universe_hash,
                legs: &legs,
            })
            .expect("scenario artifact");
        let portfolio_plan_hash = CanonicalDigest::content_hash_typed(
            "quant-pivot/backtest-portfolio-plan",
            1,
            &(
                replay_tick.decision_at,
                contract.report_route_run_id,
                scenario.model().content_hash,
                &contract.policy,
                &contract.solver,
            ),
        )
        .expect("portfolio plan hash");
        let existing =
            ExistingPortfolioFactory::build(&contract.account, Usd::ZERO, &scenario_artifact)
                .expect("existing portfolio");
        let tiers = seeded
            .tiers
            .into_iter()
            .map(|seed| EconomicTierFactory::build(seed, &scenario_artifact))
            .collect::<Result<Vec<_>, _>>()
            .expect("economic tiers");
        let input = GlobalPortfolioInput {
            portfolio_plan_id: PortfolioPlanId::from_content_hash(&portfolio_plan_hash),
            account: &contract.account,
            existing: &existing,
            represented_routes: &contract.represented_routes,
            scenario_model_binding: scenario.binding(),
            scenario_artifact: &scenario_artifact,
            policy: &contract.policy,
            solver: &contract.solver,
            tiers: &tiers,
            top_n: contract.top_n,
        };

        let replay = GlobalPortfolioPlanner::solve_replay_and_verify(input)
            .expect("verified replay selection");
        let report = GlobalPortfolioPlanner::solve_and_verify(input).expect("publishable solve");
        let replay_ids = replay
            .selected
            .iter()
            .map(|tier| tier.economic_tier_id)
            .collect::<HashSet<_>>();
        let report_ids = report
            .selected
            .iter()
            .map(|tier| tier.tier.economic_tier_id)
            .collect::<HashSet<_>>();

        assert_eq!(replay_ids, report_ids);
        assert_eq!(replay.rejected, report.rejected);
        assert!(
            report
                .plan
                .expect("publishable plan")
                .exact_verification
                .passed
        );
    }

    /// The same inputs must produce a byte-identical report hash (the report id /
    /// version are fixed here), proving deterministic replay.
    #[tokio::test]
    async fn backtest_report_hash_deterministic() {
        let model = CrossFittedRuntime::backtest_fixture();
        let scenario = BacktestScenarioContext::fixture();
        let run_id = ModelRunId::from_v7();
        let req = BacktestRequest::test_fixture(model.model_version_id());
        let ticks_a = vec![tick(0, &run_id), tick(1, &run_id)];
        let ticks_b = vec![tick(0, &run_id), tick(1, &run_id)];
        let a = PortfolioReplayBacktester::new()
            .run(BacktestInputs {
                request: req.clone(),
                model: &model,
                scenario: &scenario,
                scenario_visibility: PortfolioScenarioVisibility::PointInTime,
                ticks: ticks_a,
            })
            .await
            .expect("a");
        let b = PortfolioReplayBacktester::new()
            .run(BacktestInputs {
                request: req,
                model: &model,
                scenario: &scenario,
                scenario_visibility: PortfolioScenarioVisibility::PointInTime,
                ticks: ticks_b,
            })
            .await
            .expect("b");
        assert_eq!(a.report.report_hash, b.report.report_hash);
    }

    #[tokio::test]
    async fn score_planes_ignore_allocation() {
        let model = CrossFittedRuntime::backtest_fixture();
        let scenario = BacktestScenarioContext::fixture();
        let run_id = ModelRunId::from_v7();
        let mut replay_tick = tick(0, &run_id);
        replay_tick
            .portfolio_contract
            .policy
            .admission
            .min_robust_expected_net_usd
            .value = dec!(1000);
        let result = PortfolioReplayBacktester::new()
            .run(BacktestInputs {
                request: BacktestRequest::test_fixture(model.model_version_id()),
                model: &model,
                scenario: &scenario,
                scenario_visibility: PortfolioScenarioVisibility::PointInTime,
                ticks: vec![replay_tick],
            })
            .await
            .expect("zero-budget replay");

        assert!(result.sample_outcomes.is_empty());
        assert_eq!(result.report.sample_count, 0);
        assert_eq!(result.report.portfolio_funnel.decision_tick_count, 1);
        assert_eq!(result.report.portfolio_funnel.no_selection_tick_count, 1);
        assert_eq!(result.report.portfolio_funnel.emitted_candidate_count, 2);
        assert!(
            result
                .report
                .portfolio_funnel
                .tier_exclusion_reasons
                .iter()
                .any(|reason| {
                    reason.reason == PortfolioRejectionReason::RobustExpectedNetFloor
                        && reason.count > 0
                }),
            "economic admission rejection evidence must survive the replay"
        );
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
        let model = CrossFittedRuntime::backtest_fixture();
        let scenario = BacktestScenarioContext::fixture();
        let run_id = ModelRunId::from_v7();
        let mut mismatched = tick(0, &run_id);
        mismatched.rank_targets[0].target.label_horizon_secs = 60;

        let error = PortfolioReplayBacktester::new()
            .run(BacktestInputs {
                request: BacktestRequest::test_fixture(model.model_version_id()),
                model: &model,
                scenario: &scenario,
                scenario_visibility: PortfolioScenarioVisibility::PointInTime,
                ticks: vec![mismatched],
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
        let model = CrossFittedRuntime::backtest_fixture();
        let scenario = BacktestScenarioContext::fixture();
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
                request: BacktestRequest::test_fixture(model.model_version_id()),
                model: &model,
                scenario: &scenario,
                scenario_visibility: PortfolioScenarioVisibility::PointInTime,
                ticks: vec![score_only_tick],
            })
            .await
            .expect("score-only replay");

        assert!(result.sample_outcomes.is_empty());
        assert_eq!(result.report.sample_count, 0);
        assert_eq!(result.report.portfolio_funnel.decision_tick_count, 1);
        assert_eq!(result.report.portfolio_funnel.no_candidate_tick_count, 1);
        assert_eq!(result.report.portfolio_funnel.emitted_candidate_count, 0);
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
        let model = CrossFittedRuntime::backtest_fixture();
        let scenario = BacktestScenarioContext::fixture();
        let run_id = ModelRunId::from_v7();
        let request = BacktestRequest::test_fixture(model.model_version_id());
        let active = PortfolioReplayBacktester::new()
            .run(BacktestInputs {
                request: request.clone(),
                model: &model,
                scenario: &scenario,
                scenario_visibility: PortfolioScenarioVisibility::PointInTime,
                ticks: vec![tick(0, &run_id), tick(1, &run_id)],
            })
            .await
            .expect("active-only replay");
        let with_empty = PortfolioReplayBacktester::new()
            .run(BacktestInputs {
                request,
                model: &model,
                scenario: &scenario,
                scenario_visibility: PortfolioScenarioVisibility::PointInTime,
                ticks: vec![tick(0, &run_id), tick(1, &run_id), empty_tick(2, run_id)],
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
            with_empty.report.portfolio_funnel.no_candidate_tick_count,
            active.report.portfolio_funnel.no_candidate_tick_count + 1
        );
        assert!(
            with_empty.report.sharpe < active.report.sharpe,
            "fixed-cadence Sharpe must retain genuine no-trade periods"
        );
    }
}
