//! Deterministic candidate entry/exit replay shared by policy Fit and Validate.
//!
//! The kernel is deliberately I/O-free. Callers must resolve every observation
//! from one verified Source Slice, evaluate the frozen entry-condition AST, and
//! provide the point-in-time fee schedule before invoking it. Missing evidence
//! is returned as a typed coverage gap; raw trajectory labels are never used as
//! a fill or barrier substitute.

use std::collections::BTreeSet;

use chrono::{DateTime, Duration, Utc};
use quant_pivot_error::{QuantError, QuantResult, research::ResearchError};
use quant_pivot_models::{
    domain::market::book::BookLevel,
    enums::{
        common::{Side, TickSize},
        execution::ExitReason,
        quant::{ExitSettlementMode, FillRequirement, OutcomeSide},
    },
    types::{
        Bps, ConditionTruth, ContentHash, EntryConditionTemplate, EntryOrderTemplate,
        PassivePlacement, PayoutRatio, Price, Shares, TokenId, TradePolicyCandidateSpec,
        TradePolicyReplayGap, Usd, trade_policy_evidence::TradePolicyEvidenceCoverage,
    },
};
use rust_decimal::{Decimal, RoundingStrategy, prelude::ToPrimitive};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::execution_semantics::{
    BookWalkFill, BookWalkOutcome, LiquidityRole, PassiveQueueState, PassiveTrade, PitFeeSchedule,
    PitMakerRebateEvidence, walk_buy_cash_budget, walk_sell_exact_shares,
};

/// Versioned identity of the pure replay semantics sealed into evidence.
pub const POLICY_REPLAY_KERNEL_VERSION: &str = "weather_candidate_replay_v2";

/// One full-depth, sequence-addressed book visible at an observation boundary.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PolicyReplayBook {
    pub bids: Vec<BookLevel>,
    pub asks: Vec<BookLevel>,
    pub observed_at: DateTime<Utc>,
    pub available_at: DateTime<Utc>,
    pub stream_session_id: Uuid,
    pub token_sequence: u64,
    pub source_event_hash: ContentHash,
}

/// Model state independently re-inferred from the frozen `PolicyFit` Dataset.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PolicyReplaySignal {
    pub token_id: TokenId,
    pub outcome_side: OutcomeSide,
    pub composite_score: Decimal,
    pub expected_return_bps: Decimal,
    pub route_gate_eligible: bool,
    pub opportunistic_confidence: Option<Decimal>,
    pub opportunistic_expected_alpha_bps: Option<Decimal>,
    pub opportunistic_p_exit_better: Option<Decimal>,
}

/// One authenticated market-stream print available to a passive queue simulation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PolicyReplayTrade {
    pub event_at: DateTime<Utc>,
    pub available_at: DateTime<Utc>,
    pub stream_session_id: Uuid,
    pub token_sequence: u64,
    pub side: Side,
    pub price: Price,
    pub shares: Shares,
    pub source_event_id: String,
}

/// Resolution visible at this observation, if the market has settled.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PolicyReplayResolution {
    pub token_payout_ratio: PayoutRatio,
    pub resolved_at: DateTime<Utc>,
    pub observed_at: DateTime<Utc>,
}

/// One monotonically ordered replay heartbeat.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PolicyReplayObservation {
    pub at: DateTime<Utc>,
    /// Whether entry/exit predicates are evaluated at this instant. Synthetic
    /// latency-action snapshots carry `false`: they may fill an already
    /// triggered action but cannot manufacture a second trigger.
    pub decision_tick: bool,
    pub condition_truth: ConditionTruth,
    pub book: Option<PolicyReplayBook>,
    pub fee_schedule: Option<PitFeeSchedule>,
    pub maker_rebate_evidence: PitMakerRebateEvidence,
    pub signal: Option<PolicyReplaySignal>,
    /// Completeness of Market-WS trade reconciliation since the preceding
    /// observation. Passive queue simulation fails closed on any unknown span.
    pub passive_trade_coverage: bool,
    pub passive_trades: Vec<PolicyReplayTrade>,
    pub resolution: Option<PolicyReplayResolution>,
}

/// Analysis-path latency applied to every trigger-to-prepared action.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct PolicyReplayLatency {
    pub base_delay_ms: u64,
    pub stress_multiplier: Decimal,
}

impl PolicyReplayLatency {
    fn action_delay(self) -> QuantResult<Duration> {
        if self.stress_multiplier < Decimal::ONE {
            return Err(methodology(
                "policy replay latency multiplier must be at least one".to_owned(),
            ));
        }
        let millis = (Decimal::from(self.base_delay_ms) * self.stress_multiplier)
            .round_dp_with_strategy(0, RoundingStrategy::ToPositiveInfinity)
            .to_i64()
            .ok_or_else(|| methodology("policy replay latency does not fit i64".to_owned()))?;
        Ok(Duration::milliseconds(millis))
    }
}

/// One exact simulated venue fill or resolution payout.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PolicyReplayFill {
    pub leg_ordinal: u32,
    pub side: Side,
    pub exit_reason: Option<ExitReason>,
    pub triggered_at: DateTime<Utc>,
    pub filled_at: DateTime<Utc>,
    pub liquidity_role: LiquidityRole,
    pub outcome: BookWalkOutcome,
    pub requested_shares: Option<Shares>,
    pub filled_shares: Shares,
    pub vwap: Option<Price>,
    pub gross_amount: Usd,
    pub execution_fee_usd: Usd,
    pub expected_maker_rebate_accrual_usd: Usd,
    pub risk_cash_delta: Decimal,
    pub fee_schedule_hash: Option<ContentHash>,
    pub maker_rebate_evidence: Option<PitMakerRebateEvidence>,
    pub stream_session_id: Option<Uuid>,
    pub token_sequence: Option<u64>,
    pub source_event_hash: Option<ContentHash>,
}

/// Terminal replay result for one observation × candidate × latency scenario.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PolicyReplayOutcome {
    pub candidate_id: String,
    pub outcome_side: OutcomeSide,
    pub cash_budget: Usd,
    pub latency: PolicyReplayLatency,
    pub entry_triggered_at: Option<DateTime<Utc>>,
    pub entered_at: Option<DateTime<Utc>>,
    pub terminal_at: Option<DateTime<Utc>>,
    pub terminal_reason: Option<ExitReason>,
    pub entry_fill_ratio: Decimal,
    pub entry_fill_latency_ms: Option<u64>,
    pub post_fill_markout_bps: Option<Bps>,
    pub exit_fill_ratio: Decimal,
    pub entry_filled_shares: Shares,
    pub exited_shares: Shares,
    pub execution_fee_usd: Usd,
    pub expected_maker_rebate_accrual_usd: Usd,
    pub expected_net_return_bps: Option<Decimal>,
    pub risk_net_return_bps: Option<Decimal>,
    pub ambiguous_touch: bool,
    pub full_l2_coverage: TradePolicyEvidenceCoverage,
    pub fee_covered: bool,
    pub passive_rebate_evidence_coverage: TradePolicyEvidenceCoverage,
    pub passive_reconciled_trade_covered: Option<bool>,
    pub gap: Option<TradePolicyReplayGap>,
    pub fills: Vec<PolicyReplayFill>,
}

impl PolicyReplayOutcome {
    fn gap(
        candidate: &TradePolicyCandidateSpec,
        outcome_side: OutcomeSide,
        cash_budget: Usd,
        latency: PolicyReplayLatency,
        gap: TradePolicyReplayGap,
    ) -> Self {
        let valid_no_trade = matches!(
            gap,
            TradePolicyReplayGap::EntryNotTriggered | TradePolicyReplayGap::EntryDepthInsufficient
        );
        let passive_rebate_evidence_coverage = match candidate.entry_execution {
            EntryOrderTemplate::PassivePostOnly { .. }
                if matches!(
                    gap,
                    TradePolicyReplayGap::PitMakerRebateUnavailable
                        | TradePolicyReplayGap::PassiveTermsDrift
                        | TradePolicyReplayGap::PassiveCancelFillRace
                ) =>
            {
                TradePolicyEvidenceCoverage::Missing
            }
            EntryOrderTemplate::PassivePostOnly { .. }
                if matches!(
                    gap,
                    TradePolicyReplayGap::EntryDepthInsufficient
                        | TradePolicyReplayGap::PassiveTradeCoverageUnavailable
                ) =>
            {
                TradePolicyEvidenceCoverage::Covered
            }
            EntryOrderTemplate::Aggressive { .. } | EntryOrderTemplate::PassivePostOnly { .. } => {
                TradePolicyEvidenceCoverage::NotRequired
            }
        };
        Self {
            candidate_id: candidate.candidate_id.clone(),
            outcome_side,
            cash_budget,
            latency,
            entry_triggered_at: None,
            entered_at: None,
            terminal_at: None,
            terminal_reason: None,
            entry_fill_ratio: Decimal::ZERO,
            entry_fill_latency_ms: None,
            post_fill_markout_bps: None,
            exit_fill_ratio: Decimal::ZERO,
            entry_filled_shares: Shares::ZERO,
            exited_shares: Shares::ZERO,
            execution_fee_usd: Usd::ZERO,
            expected_maker_rebate_accrual_usd: Usd::ZERO,
            expected_net_return_bps: valid_no_trade.then_some(Decimal::ZERO),
            risk_net_return_bps: valid_no_trade.then_some(Decimal::ZERO),
            ambiguous_touch: false,
            full_l2_coverage: TradePolicyEvidenceCoverage::from(valid_no_trade),
            fee_covered: valid_no_trade,
            passive_rebate_evidence_coverage,
            passive_reconciled_trade_covered: None,
            gap: Some(gap),
            fills: Vec::new(),
        }
    }

    fn passive_no_fill(
        candidate: &TradePolicyCandidateSpec,
        outcome_side: OutcomeSide,
        cash_budget: Usd,
        latency: PolicyReplayLatency,
        triggered_at: DateTime<Utc>,
        expired_at: DateTime<Utc>,
    ) -> Self {
        Self {
            candidate_id: candidate.candidate_id.clone(),
            outcome_side,
            cash_budget,
            latency,
            entry_triggered_at: Some(triggered_at),
            entered_at: None,
            terminal_at: Some(expired_at),
            terminal_reason: None,
            entry_fill_ratio: Decimal::ZERO,
            entry_fill_latency_ms: None,
            post_fill_markout_bps: None,
            exit_fill_ratio: Decimal::ZERO,
            entry_filled_shares: Shares::ZERO,
            exited_shares: Shares::ZERO,
            execution_fee_usd: Usd::ZERO,
            expected_maker_rebate_accrual_usd: Usd::ZERO,
            expected_net_return_bps: Some(Decimal::ZERO),
            risk_net_return_bps: Some(Decimal::ZERO),
            ambiguous_touch: false,
            full_l2_coverage: TradePolicyEvidenceCoverage::Covered,
            fee_covered: true,
            passive_rebate_evidence_coverage: TradePolicyEvidenceCoverage::Covered,
            passive_reconciled_trade_covered: Some(true),
            gap: None,
            fills: Vec::new(),
        }
    }
}

enum EntryAttempt {
    Filled(EntryResult),
    PassiveNoFill {
        triggered_at: DateTime<Utc>,
        expired_at: DateTime<Utc>,
    },
}

struct EntryResult {
    triggered_at: DateTime<Utc>,
    entered_at: DateTime<Utc>,
    fill_ratio: Decimal,
    fills: Vec<PolicyReplayFill>,
    passive_coverage: Option<bool>,
    entry_gap: Option<TradePolicyReplayGap>,
}

struct ExitExecution {
    fills: Vec<PolicyReplayFill>,
    gap: Option<TradePolicyReplayGap>,
}

#[derive(Clone, Copy)]
struct PassiveEntryRequest<'a> {
    triggered_at: DateTime<Utc>,
    placement: PassivePlacement,
    good_til_secs: u64,
    max_book_age_ms: u64,
    cash_budget: Usd,
    tick_size: TickSize,
    delay: Duration,
    observations: &'a [PolicyReplayObservation],
}

struct PassiveFillRequest<'a> {
    triggered_at: DateTime<Utc>,
    requested: Shares,
    price: Price,
    schedule: &'a PitFeeSchedule,
    maker_rebate_evidence: &'a PitMakerRebateEvidence,
    trades: Vec<&'a PolicyReplayTrade>,
}

struct PassiveTradeWindow<'a> {
    observations: &'a [PolicyReplayObservation],
    placement_at: DateTime<Utc>,
    coverage_through: DateTime<Utc>,
    expires_at: DateTime<Utc>,
    cancel_at: Option<DateTime<Utc>>,
    stream_session_id: Uuid,
}

impl<'a> PassiveTradeWindow<'a> {
    fn covered_trades(&self) -> Result<Vec<&'a PolicyReplayTrade>, TradePolicyReplayGap> {
        if self.observations.iter().any(|observation| {
            observation.at >= self.placement_at
                && observation.at <= self.coverage_through
                && !observation.passive_trade_coverage
        }) {
            return Err(TradePolicyReplayGap::PassiveTradeCoverageUnavailable);
        }
        let mut trades = self
            .observations
            .iter()
            .filter(|observation| observation.at >= self.placement_at)
            .flat_map(|observation| {
                observation
                    .passive_trades
                    .iter()
                    .filter(|trade| trade.available_at <= observation.at)
            })
            .filter(|trade| {
                let occurs_after_placement = trade.event_at >= self.placement_at;
                let causally_available = trade.event_at <= trade.available_at;
                let available_before_expiry = trade.available_at <= self.expires_at;
                let before_cancel = self.cancel_at.is_none_or(|cancel_at| {
                    trade.event_at < cancel_at && trade.available_at < cancel_at
                });
                occurs_after_placement
                    && causally_available
                    && available_before_expiry
                    && before_cancel
            })
            .collect::<Vec<_>>();
        trades.sort_by(|left, right| {
            (
                left.token_sequence,
                left.event_at,
                left.available_at,
                &left.source_event_id,
            )
                .cmp(&(
                    right.token_sequence,
                    right.event_at,
                    right.available_at,
                    &right.source_event_id,
                ))
        });
        let mut source_event_ids = BTreeSet::new();
        if trades.iter().any(|trade| {
            trade.stream_session_id != self.stream_session_id
                || !source_event_ids.insert(&trade.source_event_id)
        }) {
            return Err(TradePolicyReplayGap::PassiveTradeCoverageUnavailable);
        }
        Ok(trades)
    }
}

#[derive(Clone, Copy)]
struct ReplayFillContext<'a> {
    leg_ordinal: u32,
    side: Side,
    exit_reason: Option<ExitReason>,
    triggered_at: DateTime<Utc>,
    filled_at: DateTime<Utc>,
    role: LiquidityRole,
    requested_shares: Option<Shares>,
    schedule: &'a PitFeeSchedule,
    book: &'a PolicyReplayBook,
}

struct OpenPositionState {
    entry_triggered_at: DateTime<Utc>,
    entered_at: DateTime<Utc>,
    entry_fill_ratio: Decimal,
    passive_coverage: Option<bool>,
    entry_shares: Shares,
    entry_cash: Decimal,
    entry_price: Price,
    initial_signal: Option<PolicyReplaySignal>,
    fills: Vec<PolicyReplayFill>,
    remaining: Shares,
    exited: Shares,
    exit_cash: Decimal,
    peak: Price,
    trailing_active: bool,
    next_scale_out: usize,
    terminal_reason: Option<ExitReason>,
    terminal_at: Option<DateTime<Utc>>,
    gap: Option<TradePolicyReplayGap>,
}

/// Replay one complete candidate without repository, clock, or network access.
pub fn replay_policy_candidate(
    candidate: &TradePolicyCandidateSpec,
    token_side: OutcomeSide,
    cash_budget: Usd,
    tick_size: TickSize,
    latency: PolicyReplayLatency,
    observations: &[PolicyReplayObservation],
) -> QuantResult<PolicyReplayOutcome> {
    replay_candidate(
        candidate,
        token_side,
        cash_budget,
        tick_size,
        latency,
        observations,
        None,
    )
}

/// Replay one recommendation and force any residual position through the
/// canonical full-depth exit path at the frozen economic horizon.
pub fn replay_policy_horizon(
    candidate: &TradePolicyCandidateSpec,
    token_side: OutcomeSide,
    cash_budget: Usd,
    tick_size: TickSize,
    latency: PolicyReplayLatency,
    observations: &[PolicyReplayObservation],
    horizon_at: DateTime<Utc>,
) -> QuantResult<PolicyReplayOutcome> {
    replay_candidate(
        candidate,
        token_side,
        cash_budget,
        tick_size,
        latency,
        observations,
        Some(horizon_at),
    )
}

fn replay_candidate(
    candidate: &TradePolicyCandidateSpec,
    token_side: OutcomeSide,
    cash_budget: Usd,
    tick_size: TickSize,
    latency: PolicyReplayLatency,
    observations: &[PolicyReplayObservation],
    horizon_at: Option<DateTime<Utc>>,
) -> QuantResult<PolicyReplayOutcome> {
    let observations = horizon_at.map_or(observations, |horizon| {
        &observations[..observations.partition_point(|observation| observation.at <= horizon)]
    });
    validate_observations(observations)?;
    if observations.is_empty() {
        return Ok(PolicyReplayOutcome::gap(
            candidate,
            token_side,
            cash_budget,
            latency,
            TradePolicyReplayGap::EmptyTimeline,
        ));
    }
    let delay = latency.action_delay()?;
    let entry = match find_entry(candidate, cash_budget, tick_size, delay, observations)? {
        Ok(entry) => entry,
        Err(gap) => {
            return Ok(PolicyReplayOutcome::gap(
                candidate,
                token_side,
                cash_budget,
                latency,
                gap,
            ));
        }
    };
    match entry {
        EntryAttempt::Filled(entry) => OpenPositionReplayInput {
            candidate,
            token_side,
            cash_budget,
            latency,
            delay,
            observations,
            entry,
            horizon_at,
        }
        .replay(),
        EntryAttempt::PassiveNoFill {
            triggered_at,
            expired_at,
        } => Ok(PolicyReplayOutcome::passive_no_fill(
            candidate,
            token_side,
            cash_budget,
            latency,
            triggered_at,
            expired_at,
        )),
    }
}

fn validate_observations(observations: &[PolicyReplayObservation]) -> QuantResult<()> {
    if observations.windows(2).any(|pair| pair[0].at >= pair[1].at) {
        return Err(methodology(
            "policy replay observations must be strictly increasing".to_owned(),
        ));
    }
    if observations.iter().any(|observation| {
        observation.book.as_ref().is_some_and(|book| {
            book.available_at > observation.at || book.observed_at > observation.at
        }) || observation.fee_schedule.as_ref().is_some_and(|schedule| {
            schedule.available_at > observation.at || schedule.effective_at > observation.at
        }) || match &observation.maker_rebate_evidence {
            PitMakerRebateEvidence::NoProgram { available_at, .. } => {
                *available_at > observation.at
            }
            PitMakerRebateEvidence::Available { schedule } => {
                schedule.available_at > observation.at
            }
            PitMakerRebateEvidence::Unavailable { .. } => false,
        } || observation
            .passive_trades
            .iter()
            .any(|trade| trade.available_at > observation.at || trade.event_at > observation.at)
            || observation.resolution.as_ref().is_some_and(|resolution| {
                resolution.resolved_at > observation.at || resolution.observed_at > observation.at
            })
    }) {
        return Err(methodology(
            "policy replay observation contains future evidence".to_owned(),
        ));
    }
    Ok(())
}

fn find_entry(
    candidate: &TradePolicyCandidateSpec,
    cash_budget: Usd,
    tick_size: TickSize,
    delay: Duration,
    observations: &[PolicyReplayObservation],
) -> QuantResult<Result<EntryAttempt, TradePolicyReplayGap>> {
    let mut satisfied_since = None;
    let mut prior_at = None;
    let mut saw_unavailable = false;
    for observation in observations {
        if !observation.decision_tick {
            continue;
        }
        let triggered = match &candidate.entry_condition {
            EntryConditionTemplate::Immediate => true,
            EntryConditionTemplate::Conditional {
                confirmation_ms,
                max_observation_gap_ms,
                ..
            } => match &observation.condition_truth {
                ConditionTruth::Satisfied => {
                    if prior_at.is_some_and(|prior| {
                        observation.at - prior
                            > Duration::milliseconds(
                                i64::try_from(*max_observation_gap_ms).unwrap_or(i64::MAX),
                            )
                    }) {
                        satisfied_since = None;
                    }
                    let since = *satisfied_since.get_or_insert(observation.at);
                    observation.at - since
                        >= Duration::milliseconds(i64::try_from(*confirmation_ms).map_err(
                            |error| {
                                methodology(format!(
                                    "entry confirmation does not fit chrono: {error}"
                                ))
                            },
                        )?)
                }
                ConditionTruth::Unsatisfied => {
                    satisfied_since = None;
                    false
                }
                ConditionTruth::Unavailable(_) => {
                    saw_unavailable = true;
                    satisfied_since = None;
                    false
                }
            },
        };
        prior_at = Some(observation.at);
        if !triggered {
            continue;
        }
        return match &candidate.entry_execution {
            EntryOrderTemplate::Aggressive {
                fill_requirement,
                max_slippage_bps,
                max_book_age_ms,
            } => aggressive_entry(
                observation.at,
                *fill_requirement,
                *max_slippage_bps,
                *max_book_age_ms,
                cash_budget,
                delay,
                observations,
            )
            .map(|result| result.map(EntryAttempt::Filled)),
            EntryOrderTemplate::PassivePostOnly {
                placement,
                good_til_secs,
                max_book_age_ms,
            } => passive_entry(PassiveEntryRequest {
                triggered_at: observation.at,
                placement: *placement,
                good_til_secs: *good_til_secs,
                max_book_age_ms: *max_book_age_ms,
                cash_budget,
                tick_size,
                delay,
                observations,
            }),
        };
    }
    Ok(Err(if saw_unavailable {
        TradePolicyReplayGap::EntryConditionUnavailable
    } else {
        TradePolicyReplayGap::EntryNotTriggered
    }))
}

fn aggressive_entry(
    triggered_at: DateTime<Utc>,
    requirement: FillRequirement,
    slippage: Bps,
    max_book_age_ms: u64,
    cash_budget: Usd,
    delay: Duration,
    observations: &[PolicyReplayObservation],
) -> QuantResult<Result<EntryResult, TradePolicyReplayGap>> {
    let action_at = triggered_at
        .checked_add_signed(delay)
        .ok_or_else(|| methodology("entry action time overflow".to_owned()))?;
    let Some(observation) = observation_at_or_after(observations, action_at) else {
        return Ok(Err(TradePolicyReplayGap::EntryBookUnavailable));
    };
    let Some(book) = &observation.book else {
        return Ok(Err(TradePolicyReplayGap::EntryBookUnavailable));
    };
    if book_is_stale(book, observation.at, max_book_age_ms)? {
        return Ok(Err(TradePolicyReplayGap::EntryBookStale));
    }
    let Some(schedule) = &observation.fee_schedule else {
        return Ok(Err(TradePolicyReplayGap::PitFeeScheduleUnavailable));
    };
    let Some(best_ask) = book.asks.first().map(|level| level.price_decimal()) else {
        return Ok(Err(TradePolicyReplayGap::EntryDepthInsufficient));
    };
    let limit =
        Price::new((best_ask.inner() * (Decimal::ONE + slippage.to_fraction())).min(Decimal::ONE));
    let walk = walk_buy_cash_budget(
        &book.asks,
        cash_budget,
        limit,
        requirement,
        schedule,
        LiquidityRole::Taker,
        observation.at,
    )
    .map_err(|error| methodology(format!("aggressive entry walk failed: {error:?}")))?;
    if walk.filled_shares == Shares::ZERO {
        return Ok(Err(TradePolicyReplayGap::EntryDepthInsufficient));
    }
    let requested_shares = walk.filled_shares + walk.unfilled_shares;
    let fill = replay_fill_from_walk(
        ReplayFillContext {
            leg_ordinal: 0,
            side: Side::Buy,
            exit_reason: None,
            triggered_at,
            filled_at: observation.at,
            role: LiquidityRole::Taker,
            requested_shares: Some(requested_shares),
            schedule,
            book,
        },
        &walk,
    );
    Ok(Ok(EntryResult {
        triggered_at,
        entered_at: observation.at,
        fill_ratio: ((fill.gross_amount.inner() + fill.execution_fee_usd.inner())
            / cash_budget.inner())
        .min(Decimal::ONE),
        fills: vec![fill],
        passive_coverage: None,
        entry_gap: None,
    }))
}

fn passive_entry(
    request: PassiveEntryRequest<'_>,
) -> QuantResult<Result<EntryAttempt, TradePolicyReplayGap>> {
    let PassiveEntryRequest {
        triggered_at,
        placement,
        good_til_secs,
        max_book_age_ms,
        cash_budget,
        tick_size,
        delay,
        observations,
    } = request;
    let placed_at = triggered_at
        .checked_add_signed(delay)
        .ok_or_else(|| methodology("passive placement time overflow".to_owned()))?;
    let Some(placement_observation) = observation_at_or_after(observations, placed_at) else {
        return Ok(Err(TradePolicyReplayGap::EntryBookUnavailable));
    };
    let Some(book) = &placement_observation.book else {
        return Ok(Err(TradePolicyReplayGap::EntryBookUnavailable));
    };
    if book_is_stale(book, placement_observation.at, max_book_age_ms)? {
        return Ok(Err(TradePolicyReplayGap::EntryBookStale));
    }
    let Some(schedule) = &placement_observation.fee_schedule else {
        return Ok(Err(TradePolicyReplayGap::PitFeeScheduleUnavailable));
    };
    let maker_rebate_evidence = placement_observation.maker_rebate_evidence.clone();
    if !maker_rebate_evidence.is_decidable() {
        return Ok(Err(TradePolicyReplayGap::PitMakerRebateUnavailable));
    }
    let (price, queue_ahead) = passive_price(book, placement, tick_size)?;
    let synthetic_size = Shares::new(cash_budget.inner() / price.inner() * Decimal::TWO);
    let level = BookLevel::try_from_decimal(price, synthetic_size)
        .ok_or_else(|| methodology("passive synthetic sizing level is invalid".to_owned()))?;
    let sizing = walk_buy_cash_budget(
        &[level],
        cash_budget,
        price,
        FillRequirement::AllOrNothing,
        schedule,
        LiquidityRole::Maker,
        placement_observation.at,
    )
    .map_err(|error| methodology(format!("passive sizing failed: {error:?}")))?;
    if sizing.filled_shares == Shares::ZERO {
        return Ok(Err(TradePolicyReplayGap::EntryDepthInsufficient));
    }
    let requested = sizing.filled_shares;
    let mut queue = PassiveQueueState::new(book.stream_session_id, price, queue_ahead, requested);
    let expires_at = placement_observation
        .at
        .checked_add_signed(Duration::seconds(i64::try_from(good_til_secs).map_err(
            |error| methodology(format!("passive GTD does not fit chrono: {error}")),
        )?))
        .ok_or_else(|| methodology("passive GTD overflows chrono".to_owned()))?;
    let Some(expiration_observation) = observation_at_or_after(observations, expires_at) else {
        return Ok(Err(TradePolicyReplayGap::PassiveTradeCoverageUnavailable));
    };
    let cancel_at = observations
        .iter()
        .filter(|observation| {
            observation.at > placement_observation.at && observation.at <= expiration_observation.at
        })
        .find(|observation| {
            observation
                .fee_schedule
                .as_ref()
                .is_none_or(|current| current.schedule_hash != schedule.schedule_hash)
                || observation.maker_rebate_evidence != maker_rebate_evidence
        })
        .map(|observation| observation.at);
    if let Some(cancel_at) = cancel_at
        && observations.iter().any(|observation| {
            observation.passive_trades.iter().any(|trade| {
                trade.stream_session_id == book.stream_session_id
                    && trade.event_at <= cancel_at
                    && trade.available_at >= cancel_at
            })
        })
    {
        return Ok(Err(TradePolicyReplayGap::PassiveCancelFillRace));
    }
    let trades = match (PassiveTradeWindow {
        observations,
        placement_at: placement_observation.at,
        coverage_through: cancel_at.unwrap_or(expiration_observation.at),
        expires_at,
        cancel_at,
        stream_session_id: book.stream_session_id,
    })
    .covered_trades()
    {
        Ok(trades) => trades,
        Err(gap) => return Ok(Err(gap)),
    };
    let fill_slices = passive_fill_slices(
        PassiveFillRequest {
            triggered_at,
            requested,
            price,
            schedule,
            maker_rebate_evidence: &maker_rebate_evidence,
            trades,
        },
        &mut queue,
    )?;
    let Some(entered_at) = fill_slices.first().map(|fill| fill.filled_at) else {
        if cancel_at.is_some() {
            return Ok(Err(TradePolicyReplayGap::PassiveTermsDrift));
        }
        return Ok(Ok(EntryAttempt::PassiveNoFill {
            triggered_at,
            expired_at: expires_at,
        }));
    };
    Ok(Ok(EntryAttempt::Filled(EntryResult {
        triggered_at,
        entered_at,
        fill_ratio: queue.filled_shares.inner() / requested.inner(),
        fills: fill_slices,
        passive_coverage: Some(true),
        entry_gap: (cancel_at.is_some() && queue.remaining_shares > Shares::ZERO)
            .then_some(TradePolicyReplayGap::PassiveTermsDrift),
    })))
}

fn passive_fill_slices(
    request: PassiveFillRequest<'_>,
    queue: &mut PassiveQueueState,
) -> QuantResult<Vec<PolicyReplayFill>> {
    let mut fills = Vec::new();
    for trade in request.trades {
        let filled = queue.apply_trade(PassiveTrade {
            stream_session_id: trade.stream_session_id,
            side: trade.side,
            price: trade.price,
            shares: trade.shares,
        });
        if filled == Shares::ZERO {
            continue;
        }
        let fee = request
            .schedule
            .fee(LiquidityRole::Maker, request.price, filled, trade.event_at)
            .map_err(|error| methodology(format!("passive fee failed: {error:?}")))?;
        let expected_maker_rebate_accrual_usd = request
            .maker_rebate_evidence
            .schedule()
            .map(|rebate| {
                rebate
                    .expected_incentive(
                        request.schedule,
                        LiquidityRole::Maker,
                        request.price,
                        filled,
                        trade.event_at,
                    )
                    .map(|incentive| incentive.map_or(Usd::ZERO, |value| value.expected_rebate_usd))
            })
            .transpose()
            .map_err(|error| methodology(format!("passive maker rebate failed: {error:?}")))?
            .unwrap_or(Usd::ZERO);
        let gross = filled * request.price;
        let leg_ordinal = u32::try_from(fills.len()).map_err(|error| {
            methodology(format!("passive fill ordinal does not fit u32: {error}"))
        })?;
        let source_event_hash = ContentHash::parse(&trade.source_event_id).map_err(|error| {
            methodology(format!("passive trade event hash is invalid: {error}"))
        })?;
        fills.push(PolicyReplayFill {
            leg_ordinal,
            side: Side::Buy,
            exit_reason: None,
            triggered_at: request.triggered_at,
            filled_at: trade.event_at,
            liquidity_role: LiquidityRole::Maker,
            outcome: if queue.remaining_shares == Shares::ZERO {
                BookWalkOutcome::Filled
            } else {
                BookWalkOutcome::Partial
            },
            requested_shares: Some(request.requested),
            filled_shares: filled,
            vwap: Some(request.price),
            gross_amount: gross,
            execution_fee_usd: fee,
            expected_maker_rebate_accrual_usd,
            risk_cash_delta: -(gross.inner() + fee.inner()),
            fee_schedule_hash: Some(request.schedule.schedule_hash),
            maker_rebate_evidence: Some(request.maker_rebate_evidence.clone()),
            stream_session_id: Some(trade.stream_session_id),
            token_sequence: Some(trade.token_sequence),
            source_event_hash: Some(source_event_hash),
        });
        if queue.remaining_shares == Shares::ZERO {
            break;
        }
    }
    Ok(fills)
}

fn passive_price(
    book: &PolicyReplayBook,
    placement: PassivePlacement,
    tick_size: TickSize,
) -> QuantResult<(Price, Shares)> {
    let best_bid = book
        .bids
        .first()
        .map(|level| level.price_decimal())
        .ok_or_else(|| methodology("passive entry has no best bid".to_owned()))?;
    let best_ask = book
        .asks
        .first()
        .map(|level| level.price_decimal())
        .ok_or_else(|| methodology("passive entry has no best ask".to_owned()))?;
    let price = match placement {
        PassivePlacement::JoinBestBid => best_bid,
        PassivePlacement::ImproveBestBidByTicks { ticks } => Price::new(
            (best_bid.inner() + tick_size.as_decimal() * Decimal::from(ticks))
                .min(best_ask.inner() - tick_size.as_decimal()),
        ),
    };
    if price <= Price::ZERO || price >= best_ask {
        return Err(methodology(
            "passive post-only price would cross or leave the valid range".to_owned(),
        ));
    }
    let queue_ahead = book
        .bids
        .iter()
        .find(|level| level.price_decimal() == price)
        .map_or(Shares::ZERO, |level| level.size_decimal());
    Ok((price, queue_ahead))
}

struct OpenPositionReplayInput<'a> {
    candidate: &'a TradePolicyCandidateSpec,
    token_side: OutcomeSide,
    cash_budget: Usd,
    latency: PolicyReplayLatency,
    delay: Duration,
    observations: &'a [PolicyReplayObservation],
    entry: EntryResult,
    horizon_at: Option<DateTime<Utc>>,
}

impl OpenPositionReplayInput<'_> {
    fn replay(self) -> QuantResult<PolicyReplayOutcome> {
        let Self {
            candidate,
            token_side,
            cash_budget,
            latency,
            delay,
            observations,
            entry,
            horizon_at,
        } = self;
        let initial_signal = observations
            .iter()
            .find(|observation| observation.at >= entry.entered_at)
            .and_then(|observation| observation.signal.as_ref())
            .cloned();
        let mut state = open_position_state(entry, initial_signal)?;
        let entered_at = state.entered_at;
        for observation in observations
            .iter()
            .filter(|observation| observation.at >= entered_at)
        {
            if advance_open_position(
                candidate,
                token_side,
                delay,
                observations,
                observation,
                &mut state,
            )? {
                break;
            }
        }
        if let Some(horizon_at) = horizon_at
            && state.remaining > Shares::ZERO
            && state.gap.is_none()
        {
            let visible_end =
                observations.partition_point(|observation| observation.at <= horizon_at);
            let visible = &observations[..visible_end];
            let execution = execute_exit(
                candidate,
                ExitReason::TimeExit,
                state.remaining,
                horizon_at,
                Duration::zero(),
                visible,
                next_ordinal(&state.fills)?,
            )?;
            apply_exit_execution(&mut state, ExitReason::TimeExit, execution);
        }
        Ok(finalize_open_position(
            candidate,
            token_side,
            cash_budget,
            latency,
            observations,
            state,
        ))
    }
}

fn open_position_state(
    entry: EntryResult,
    initial_signal: Option<PolicyReplaySignal>,
) -> QuantResult<OpenPositionState> {
    let entry_shares: Shares = entry.fills.iter().map(|fill| fill.filled_shares).sum();
    let entry_cash = -entry
        .fills
        .iter()
        .map(|fill| fill.risk_cash_delta)
        .sum::<Decimal>();
    let entry_principal = entry
        .fills
        .iter()
        .map(|fill| fill.gross_amount.inner())
        .sum::<Decimal>();
    if !entry_shares.is_positive() || entry_principal <= Decimal::ZERO {
        return Err(methodology(
            "entry fill slices contain no executed shares".to_owned(),
        ));
    }
    let entry_price = Price::new(entry_principal / entry_shares.inner());
    Ok(OpenPositionState {
        entry_triggered_at: entry.triggered_at,
        entered_at: entry.entered_at,
        entry_fill_ratio: entry.fill_ratio,
        passive_coverage: entry.passive_coverage,
        entry_shares,
        entry_cash,
        entry_price,
        initial_signal,
        fills: entry.fills,
        remaining: entry_shares,
        exited: Shares::ZERO,
        exit_cash: Decimal::ZERO,
        peak: entry_price,
        trailing_active: false,
        next_scale_out: 0,
        terminal_reason: None,
        terminal_at: None,
        gap: entry.entry_gap,
    })
}

fn advance_open_position(
    candidate: &TradePolicyCandidateSpec,
    token_side: OutcomeSide,
    delay: Duration,
    observations: &[PolicyReplayObservation],
    observation: &PolicyReplayObservation,
    state: &mut OpenPositionState,
) -> QuantResult<bool> {
    if let Some(resolution) = &observation.resolution {
        settle_resolution(state, resolution, observation.at)?;
        return Ok(true);
    }
    if !observation.decision_tick {
        return Ok(false);
    }
    let Some(current_signal) = observation.signal.as_ref() else {
        state.gap = Some(TradePolicyReplayGap::SignalReinferenceUnavailable);
        return Ok(true);
    };
    let Some(mark) = observation
        .book
        .as_ref()
        .and_then(|book| book.bids.first())
        .map(|level| level.price_decimal())
    else {
        return Ok(false);
    };
    let Some(reason) = exit_reason_at(
        candidate,
        token_side,
        current_signal,
        observation.at,
        mark,
        state,
    )?
    else {
        return Ok(false);
    };
    let target = exit_target(candidate, reason, state)?;
    if target == Shares::ZERO {
        state.next_scale_out = state.next_scale_out.saturating_add(1);
        return Ok(false);
    }
    let execution = execute_exit(
        candidate,
        reason,
        target.min(state.remaining),
        observation.at,
        delay,
        observations,
        next_ordinal(&state.fills)?,
    )?;
    apply_exit_execution(state, reason, execution);
    Ok(state.remaining == Shares::ZERO && state.terminal_reason.is_some())
}

fn exit_reason_at(
    candidate: &TradePolicyCandidateSpec,
    token_side: OutcomeSide,
    current_signal: &PolicyReplaySignal,
    observation_at: DateTime<Utc>,
    mark: Price,
    state: &mut OpenPositionState,
) -> QuantResult<Option<ExitReason>> {
    state.peak = state.peak.max(mark);
    let return_bps = Bps::spread(mark, state.entry_price).map_or(Decimal::ZERO, Bps::inner);
    if candidate
        .exit
        .trailing_stop
        .as_ref()
        .is_some_and(|trailing| return_bps >= trailing.activation_return_bps.inner())
    {
        state.trailing_active = true;
    }
    let trailing_hit = candidate
        .exit
        .trailing_stop
        .as_ref()
        .is_some_and(|trailing| {
            state.trailing_active
                && mark.inner()
                    <= state.peak.inner() * (Decimal::ONE - trailing.trail_bps.to_fraction())
        });
    let signal_reason = signal_exit_reason(
        candidate,
        token_side,
        state.initial_signal.as_ref(),
        current_signal,
    );
    let scale_out_due = candidate
        .exit
        .scale_out_targets
        .get(state.next_scale_out)
        .is_some_and(|target| return_bps >= target.trigger_return_bps.inner());
    let time_exit_at = state
        .entered_at
        .checked_add_signed(Duration::seconds(
            i64::try_from(candidate.exit.vertical_barrier_secs).map_err(|error| {
                methodology(format!("vertical barrier does not fit chrono: {error}"))
            })?,
        ))
        .ok_or_else(|| methodology("vertical barrier time overflow".to_owned()))?;
    Ok(
        if return_bps <= -candidate.exit.lower_barrier_bps.inner() || trailing_hit {
            Some(ExitReason::StopLoss)
        } else if signal_reason.is_some() {
            signal_reason
        } else if return_bps >= candidate.exit.upper_barrier_bps.inner() {
            Some(ExitReason::TakeProfit)
        } else if scale_out_due {
            Some(ExitReason::PartialExit)
        } else if observation_at >= time_exit_at {
            Some(ExitReason::TimeExit)
        } else {
            None
        },
    )
}

fn exit_target(
    candidate: &TradePolicyCandidateSpec,
    reason: ExitReason,
    state: &OpenPositionState,
) -> QuantResult<Shares> {
    if reason != ExitReason::PartialExit {
        return Ok(state.remaining);
    }
    let target_pct = candidate
        .exit
        .scale_out_targets
        .get(state.next_scale_out)
        .ok_or_else(|| {
            methodology("partial exit has no corresponding scale-out target".to_owned())
        })?
        .target_cumulative_exit_pct;
    Ok(Shares::new(
        (state.entry_shares.inner() * target_pct - state.exited.inner()).max(Decimal::ZERO),
    ))
}

fn apply_exit_execution(
    state: &mut OpenPositionState,
    reason: ExitReason,
    execution: ExitExecution,
) {
    for fill in execution.fills {
        state.exit_cash += fill.risk_cash_delta;
        state.exited += fill.filled_shares;
        state.remaining -= fill.filled_shares;
        state.fills.push(fill);
    }
    if let Some(execution_gap) = execution.gap {
        state.gap = Some(execution_gap);
    }
    if reason == ExitReason::PartialExit {
        state.next_scale_out = state.next_scale_out.saturating_add(1);
    }
    if state.remaining == Shares::ZERO {
        state.terminal_reason = Some(reason);
        state.terminal_at = state.fills.last().map(|fill| fill.filled_at);
    }
}

fn settle_resolution(
    state: &mut OpenPositionState,
    resolution: &PolicyReplayResolution,
    applied_at: DateTime<Utc>,
) -> QuantResult<()> {
    if state.remaining == Shares::ZERO {
        return Ok(());
    }
    if applied_at <= state.entered_at
        || resolution.resolved_at > applied_at
        || resolution.observed_at > applied_at
    {
        return Err(methodology(
            "resolution application must follow entry and contain only visible evidence".to_owned(),
        ));
    }
    let payout = state.remaining.inner() * resolution.token_payout_ratio.inner();
    let reason = ExitReason::ResolutionRedeem;
    state.fills.push(PolicyReplayFill {
        leg_ordinal: next_ordinal(&state.fills)?,
        side: Side::Sell,
        exit_reason: Some(reason),
        // Source event timestamps remain on the canonical resolution fact.
        // Cash becomes available only when this replay actually consumes it.
        triggered_at: applied_at,
        filled_at: applied_at,
        liquidity_role: LiquidityRole::Maker,
        outcome: BookWalkOutcome::Filled,
        requested_shares: Some(state.remaining),
        filled_shares: state.remaining,
        vwap: Some(Price::new(resolution.token_payout_ratio.inner())),
        gross_amount: Usd::new(payout),
        execution_fee_usd: Usd::ZERO,
        expected_maker_rebate_accrual_usd: Usd::ZERO,
        risk_cash_delta: payout,
        fee_schedule_hash: None,
        maker_rebate_evidence: None,
        stream_session_id: None,
        token_sequence: None,
        source_event_hash: None,
    });
    state.exit_cash += payout;
    state.exited += state.remaining;
    state.remaining = Shares::ZERO;
    state.terminal_reason = Some(reason);
    state.terminal_at = Some(applied_at);
    Ok(())
}

fn finalize_open_position(
    candidate: &TradePolicyCandidateSpec,
    token_side: OutcomeSide,
    cash_budget: Usd,
    latency: PolicyReplayLatency,
    observations: &[PolicyReplayObservation],
    mut state: OpenPositionState,
) -> PolicyReplayOutcome {
    if state.remaining > Shares::ZERO && state.gap.is_none() {
        state.gap = Some(
            if observations
                .iter()
                .any(|observation| observation.resolution.is_some())
            {
                TradePolicyReplayGap::ExitDepthInsufficient
            } else if candidate.exit.settlement_mode == ExitSettlementMode::HoldToResolution {
                TradePolicyReplayGap::ResolutionEvidenceUnavailable
            } else {
                TradePolicyReplayGap::ResidualPositionUnresolved
            },
        );
    }
    let execution_fee_usd = state.fills.iter().map(|fill| fill.execution_fee_usd).sum();
    let expected_maker_rebate_accrual_usd = state
        .fills
        .iter()
        .map(|fill| fill.expected_maker_rebate_accrual_usd)
        .sum::<Usd>();
    let terminal = state.remaining == Shares::ZERO && state.entry_cash > Decimal::ZERO;
    let risk_net_return_bps = terminal.then(|| {
        ((state.exit_cash - state.entry_cash) / state.entry_cash * Decimal::from(10_000))
            .round_dp(8)
    });
    // Trade-policy selection measures the route's nominal maker economics.
    // Account/day payout eligibility is applied later by the report MILP; risk
    // remains a separate zero-rebate series.
    let expected_net_return_bps = terminal.then(|| {
        ((state.exit_cash + expected_maker_rebate_accrual_usd.inner() - state.entry_cash)
            / state.entry_cash
            * Decimal::from(10_000))
        .round_dp(8)
    });
    let exit_fill_ratio = if state.entry_shares > Shares::ZERO {
        (state.exited.inner() / state.entry_shares.inner()).min(Decimal::ONE)
    } else {
        Decimal::ZERO
    };
    let full_l2 = state
        .fills
        .iter()
        .filter(|fill| fill.exit_reason != Some(ExitReason::ResolutionRedeem))
        .all(|fill| {
            fill.stream_session_id.is_some()
                && fill.token_sequence.is_some()
                && fill.source_event_hash.is_some()
        });
    let fee_covered = state
        .fills
        .iter()
        .filter(|fill| fill.vwap.is_some() && fill.gross_amount > Usd::ZERO)
        .all(|fill| {
            fill.fee_schedule_hash.is_some()
                || fill.exit_reason == Some(ExitReason::ResolutionRedeem)
        });
    let maker_entry_fills = state
        .fills
        .iter()
        .filter(|fill| {
            fill.side == Side::Buy
                && fill.exit_reason.is_none()
                && fill.liquidity_role == LiquidityRole::Maker
        })
        .collect::<Vec<_>>();
    let passive_rebate_evidence_coverage = if maker_entry_fills.is_empty() {
        TradePolicyEvidenceCoverage::NotRequired
    } else if matches!(
        state.gap,
        Some(TradePolicyReplayGap::PassiveTermsDrift | TradePolicyReplayGap::PassiveCancelFillRace)
    ) || maker_entry_fills.iter().any(|fill| {
        fill.maker_rebate_evidence
            .as_ref()
            .is_none_or(|evidence| !evidence.is_decidable())
    }) {
        TradePolicyEvidenceCoverage::Missing
    } else {
        TradePolicyEvidenceCoverage::Covered
    };
    let entry_fill_latency_ms = state
        .entered_at
        .signed_duration_since(state.entry_triggered_at)
        .num_milliseconds()
        .try_into()
        .ok();
    let post_fill_markout_bps = observations
        .iter()
        .filter(|observation| observation.at > state.entered_at)
        .filter_map(|observation| observation.book.as_ref())
        .find_map(|book| {
            let bid = book.bids.first()?.price_decimal();
            let ask = book.asks.first()?.price_decimal();
            let mid = (bid.inner() + ask.inner()) / Decimal::TWO;
            Some(Bps::new(
                ((mid - state.entry_price.inner()) / state.entry_price.inner()
                    * Decimal::from(10_000))
                .round_dp(8),
            ))
        });
    PolicyReplayOutcome {
        candidate_id: candidate.candidate_id.clone(),
        outcome_side: token_side,
        cash_budget,
        latency,
        entry_triggered_at: Some(state.entry_triggered_at),
        entered_at: Some(state.entered_at),
        terminal_at: state.terminal_at,
        terminal_reason: state.terminal_reason,
        entry_fill_ratio: state.entry_fill_ratio,
        entry_fill_latency_ms,
        post_fill_markout_bps,
        exit_fill_ratio,
        entry_filled_shares: state.entry_shares,
        exited_shares: state.exited,
        execution_fee_usd,
        expected_maker_rebate_accrual_usd,
        expected_net_return_bps,
        risk_net_return_bps,
        ambiguous_touch: false,
        full_l2_coverage: full_l2.into(),
        fee_covered,
        passive_rebate_evidence_coverage,
        passive_reconciled_trade_covered: state.passive_coverage,
        gap: state.gap,
        fills: state.fills,
    }
}

fn signal_exit_reason(
    candidate: &TradePolicyCandidateSpec,
    token_side: OutcomeSide,
    initial: Option<&PolicyReplaySignal>,
    current: &PolicyReplaySignal,
) -> Option<ExitReason> {
    if current.outcome_side != token_side
        || candidate.exit.require_route_gate_eligibility && !current.route_gate_eligible
        || current.expected_return_bps < candidate.exit.min_expected_return_bps.inner()
        || initial.is_some_and(|initial| {
            current.composite_score < initial.composite_score * candidate.exit.min_score_retention
        })
    {
        return Some(ExitReason::SignalInvalidated);
    }
    let policy = &candidate.exit.opportunistic_exit;
    if current
        .opportunistic_confidence
        .is_some_and(|value| value >= policy.min_confidence.inner())
        && current
            .opportunistic_expected_alpha_bps
            .is_some_and(|value| value >= policy.min_expected_alpha_bps.inner())
        && current
            .opportunistic_p_exit_better
            .is_some_and(|value| value >= policy.min_p_exit_better.inner())
    {
        Some(ExitReason::Opportunistic)
    } else {
        None
    }
}

fn execute_exit(
    candidate: &TradePolicyCandidateSpec,
    reason: ExitReason,
    target: Shares,
    triggered_at: DateTime<Utc>,
    delay: Duration,
    observations: &[PolicyReplayObservation],
    first_ordinal: u32,
) -> QuantResult<ExitExecution> {
    let rule = candidate
        .exit
        .reason_execution
        .iter()
        .find(|rule| rule.reason == reason)
        .ok_or_else(|| methodology(format!("candidate has no exit rule for {reason}")))?;
    let mut remaining = target;
    let mut fills = Vec::new();
    let mut saw_book = false;
    let mut saw_schedule = false;
    let mut saw_fresh_book = false;
    for attempt in 0..rule.max_attempts {
        let retry_ms = u64::from(attempt)
            .checked_mul(rule.retry_cadence_ms)
            .ok_or_else(|| methodology("exit retry delay overflow".to_owned()))?;
        let attempt_at = triggered_at
            .checked_add_signed(
                delay
                    + Duration::milliseconds(i64::try_from(retry_ms).map_err(|error| {
                        methodology(format!("exit retry delay does not fit chrono: {error}"))
                    })?),
            )
            .ok_or_else(|| methodology("exit attempt time overflow".to_owned()))?;
        let Some(observation) = observation_at_or_after(observations, attempt_at) else {
            break;
        };
        let Some(book) = &observation.book else {
            continue;
        };
        saw_book = true;
        let max_book_age_ms = candidate_max_book_age(&candidate.entry_execution);
        if book_is_stale(book, observation.at, max_book_age_ms)? {
            continue;
        }
        saw_fresh_book = true;
        let Some(schedule) = &observation.fee_schedule else {
            continue;
        };
        saw_schedule = true;
        let Some(best_bid) = book.bids.first().map(|level| level.price_decimal()) else {
            continue;
        };
        let limit = Price::new(
            (best_bid.inner() * (Decimal::ONE - rule.max_slippage_bps.to_fraction()))
                .max(Decimal::new(1, 6)),
        );
        let walk = walk_sell_exact_shares(
            &book.bids,
            remaining,
            limit,
            rule.fill_requirement,
            schedule,
            LiquidityRole::Taker,
            observation.at,
        )
        .map_err(|error| methodology(format!("exit walk failed: {error:?}")))?;
        if walk.filled_shares == Shares::ZERO {
            continue;
        }
        let ordinal = first_ordinal
            .checked_add(u32::try_from(fills.len()).map_err(|error| {
                methodology(format!("exit fill ordinal does not fit u32: {error}"))
            })?)
            .ok_or_else(|| methodology("exit fill ordinal overflow".to_owned()))?;
        remaining -= walk.filled_shares;
        fills.push(replay_fill_from_walk(
            ReplayFillContext {
                leg_ordinal: ordinal,
                side: Side::Sell,
                exit_reason: Some(reason),
                triggered_at,
                filled_at: observation.at,
                role: LiquidityRole::Taker,
                requested_shares: Some(target),
                schedule,
                book,
            },
            &walk,
        ));
        if remaining == Shares::ZERO {
            break;
        }
    }
    let gap = (remaining > Shares::ZERO).then_some(if !saw_book {
        TradePolicyReplayGap::ExitBookUnavailable
    } else if !saw_fresh_book {
        TradePolicyReplayGap::ExitBookStale
    } else if !saw_schedule {
        TradePolicyReplayGap::PitFeeScheduleUnavailable
    } else {
        TradePolicyReplayGap::ExitDepthInsufficient
    });
    Ok(ExitExecution { fills, gap })
}

fn replay_fill_from_walk(context: ReplayFillContext<'_>, walk: &BookWalkFill) -> PolicyReplayFill {
    PolicyReplayFill {
        leg_ordinal: context.leg_ordinal,
        side: context.side,
        exit_reason: context.exit_reason,
        triggered_at: context.triggered_at,
        filled_at: context.filled_at,
        liquidity_role: context.role,
        outcome: walk.outcome,
        requested_shares: context.requested_shares,
        filled_shares: walk.filled_shares,
        vwap: walk.vwap,
        gross_amount: walk.immediate_cost.principal_usd,
        execution_fee_usd: walk.immediate_cost.total_fee_usd(),
        expected_maker_rebate_accrual_usd: Usd::ZERO,
        risk_cash_delta: walk.account_cash_delta_usd,
        fee_schedule_hash: Some(context.schedule.schedule_hash),
        maker_rebate_evidence: None,
        stream_session_id: Some(context.book.stream_session_id),
        token_sequence: Some(context.book.token_sequence),
        source_event_hash: Some(context.book.source_event_hash),
    }
}

fn observation_at_or_after(
    observations: &[PolicyReplayObservation],
    at: DateTime<Utc>,
) -> Option<&PolicyReplayObservation> {
    let index = observations.partition_point(|observation| observation.at < at);
    observations.get(index)
}

fn book_is_stale(book: &PolicyReplayBook, at: DateTime<Utc>, max_age_ms: u64) -> QuantResult<bool> {
    let age = at
        .signed_duration_since(book.observed_at)
        .num_milliseconds();
    if age < 0 {
        return Err(methodology(
            "book observation is from the future".to_owned(),
        ));
    }
    Ok(u64::try_from(age).map_or(true, |age| age > max_age_ms))
}

const fn candidate_max_book_age(entry: &EntryOrderTemplate) -> u64 {
    match entry {
        EntryOrderTemplate::PassivePostOnly {
            max_book_age_ms, ..
        }
        | EntryOrderTemplate::Aggressive {
            max_book_age_ms, ..
        } => *max_book_age_ms,
    }
}

fn next_ordinal(fills: &[PolicyReplayFill]) -> QuantResult<u32> {
    u32::try_from(fills.len())
        .map_err(|error| methodology(format!("fill ordinal does not fit u32: {error}")))
}

fn methodology(detail: String) -> QuantError {
    ResearchError::ValidationMethodology { detail }.into()
}

#[cfg(test)]
mod tests {
    use chrono::{DateTime, Duration, TimeZone, Utc};
    use quant_pivot_models::{
        domain::{
            data_plane::{DecisionClock, DecisionSource},
            market::{book::BookLevel, fee::BuilderFeeAttribution},
        },
        enums::{
            common::{Side, TickSize},
            execution::ExitReason,
            quant::{ExitSettlementMode, FillRequirement, OutcomeSide, RedeemPolicy},
        },
        types::{
            Bps, ConditionTruth, ContentHash, EntryConditionTemplate, EntryOrderTemplate,
            ExitExecutionTemplate, OpportunisticExitPolicy, PassivePlacement, PayoutRatio, Price,
            Probability, ResidualSharePolicy, ScaleOutTemplate, Shares, TokenId,
            TradePolicyCandidateSpec, TradePolicyExitTemplate, TradePolicyReplayGap,
            TrailingStopTemplate, Usd, trade_policy_evidence::TradePolicyEvidenceCoverage,
        },
    };
    use rust_decimal::Decimal;
    use rust_decimal_macros::dec;
    use uuid::Uuid;

    use super::{
        POLICY_REPLAY_KERNEL_VERSION, PolicyReplayBook, PolicyReplayLatency,
        PolicyReplayObservation, PolicyReplayResolution, PolicyReplaySignal, PolicyReplayTrade,
        replay_policy_candidate, replay_policy_horizon,
    };
    use crate::execution_semantics::{
        BookWalkOutcome, PitFeeSchedule, PitMakerRebateEvidence, PitMakerRebateSchedule,
        PitMakerRebateUnavailableReason,
    };

    fn hash(seed: char) -> ContentHash {
        ContentHash::parse(&format!("blake3:{}", seed.to_string().repeat(64))).expect("hash")
    }

    fn at(seconds: i64) -> DateTime<Utc> {
        Utc.timestamp_opt(1_700_000_000 + seconds, 0)
            .single()
            .expect("time")
    }

    fn level(price: Decimal, shares: Decimal) -> BookLevel {
        BookLevel::try_from_decimal(Price::new(price), Shares::new(shares)).expect("level")
    }

    fn schedule(time: DateTime<Utc>) -> PitFeeSchedule {
        PitFeeSchedule {
            schedule_hash: hash('a'),
            effective_at: time,
            available_at: time,
            platform_rate: dec!(0.02),
            exponent: Decimal::ONE,
            taker_only: true,
            builder_maker_fee_bps: Bps::ZERO,
            builder_taker_fee_bps: Bps::ZERO,
            builder_attribution: BuilderFeeAttribution::NoBuilderCode,
        }
    }

    fn maker_rebate_schedule(time: DateTime<Utc>) -> PitMakerRebateSchedule {
        PitMakerRebateSchedule {
            terms_hash: hash('c'),
            available_at: time,
            platform_rate: dec!(0.02),
            exponent: Decimal::ONE,
            taker_only: true,
            rebate_rate: dec!(0.2),
        }
    }

    fn observation(seconds: i64, bid: Decimal, ask: Decimal) -> PolicyReplayObservation {
        let time = at(seconds);
        PolicyReplayObservation {
            at: time,
            decision_tick: true,
            condition_truth: ConditionTruth::Satisfied,
            book: Some(PolicyReplayBook {
                bids: vec![level(bid, dec!(1000))],
                asks: vec![level(ask, dec!(1000))],
                observed_at: time,
                available_at: time,
                stream_session_id: Uuid::nil(),
                token_sequence: u64::try_from(seconds + 1).expect("sequence"),
                source_event_hash: hash('b'),
            }),
            fee_schedule: Some(schedule(time)),
            maker_rebate_evidence: PitMakerRebateEvidence::NoProgram {
                terms_hash: hash('d'),
                available_at: at(0),
            },
            signal: Some(PolicyReplaySignal {
                token_id: TokenId::new("yes"),
                outcome_side: OutcomeSide::Yes,
                composite_score: dec!(0.8),
                expected_return_bps: dec!(200),
                route_gate_eligible: true,
                opportunistic_confidence: None,
                opportunistic_expected_alpha_bps: None,
                opportunistic_p_exit_better: None,
            }),
            passive_trade_coverage: true,
            passive_trades: Vec::new(),
            resolution: None,
        }
    }

    fn candidate(requirement: FillRequirement) -> TradePolicyCandidateSpec {
        TradePolicyCandidateSpec {
            candidate_id: "weather-immediate-fak".to_owned(),
            entry_condition: EntryConditionTemplate::Immediate,
            entry_execution: EntryOrderTemplate::Aggressive {
                fill_requirement: requirement,
                max_slippage_bps: Bps::new(dec!(100)),
                max_book_age_ms: 5_000,
            },
            exit: TradePolicyExitTemplate {
                upper_barrier_bps: Bps::new(dec!(1000)),
                lower_barrier_bps: Bps::new(dec!(500)),
                vertical_barrier_secs: 120,
                scale_out_targets: vec![ScaleOutTemplate {
                    target_id: "first".to_owned(),
                    trigger_return_bps: Bps::new(dec!(500)),
                    target_cumulative_exit_pct: dec!(0.5),
                }],
                trailing_stop: None,
                min_score_retention: dec!(0.5),
                min_expected_return_bps: Bps::ZERO,
                require_route_gate_eligibility: true,
                opportunistic_exit: OpportunisticExitPolicy {
                    min_confidence: Probability::ONE,
                    min_expected_alpha_bps: Bps::new(dec!(1000)),
                    min_p_exit_better: Probability::ONE,
                    max_cumulative_exit_pct: dec!(0.5),
                    min_incremental_exit_pct: dec!(0.25),
                },
                settlement_mode: ExitSettlementMode::ExitBeforeResolution,
                redeem_policy: RedeemPolicy::Manual,
                reason_execution: ExitReason::ALL
                    .iter()
                    .copied()
                    .map(|reason| ExitExecutionTemplate {
                        reason,
                        fill_requirement: FillRequirement::AllowPartial,
                        max_attempts: 2,
                        retry_cadence_ms: 1_000,
                        max_slippage_bps: Bps::new(dec!(100)),
                        residual_share_policy: ResidualSharePolicy::RetryUntilVertical,
                    })
                    .collect(),
            },
        }
    }

    #[test]
    fn aggressive_entry_scale_conserving() {
        let observations = vec![
            observation(0, dec!(0.49), dec!(0.50)),
            observation(60, dec!(0.53), dec!(0.54)),
            observation(120, dec!(0.56), dec!(0.57)),
            observation(180, dec!(0.57), dec!(0.58)),
        ];
        let outcome = replay_policy_candidate(
            &candidate(FillRequirement::AllowPartial),
            OutcomeSide::Yes,
            Usd::new(dec!(25)),
            TickSize::Hundredth,
            PolicyReplayLatency {
                base_delay_ms: 0,
                stress_multiplier: Decimal::ONE,
            },
            &observations,
        )
        .expect("replay");

        assert_eq!(POLICY_REPLAY_KERNEL_VERSION, "weather_candidate_replay_v2");
        assert_eq!(outcome.gap, None);
        assert_eq!(outcome.terminal_reason, Some(ExitReason::TakeProfit));
        assert_eq!(outcome.entry_fill_ratio, Decimal::ONE);
        assert_eq!(outcome.exit_fill_ratio, Decimal::ONE);
        assert!(
            outcome
                .expected_net_return_bps
                .is_some_and(|value| value > Decimal::ZERO)
        );
        assert!(outcome.fills.len() >= 3);
    }

    #[test]
    fn horizon_uses_bid_ladder() {
        let observations = vec![
            observation(0, dec!(0.49), dec!(0.50)),
            observation(60, dec!(0.51), dec!(0.52)),
        ];
        let outcome = replay_policy_horizon(
            &candidate(FillRequirement::AllowPartial),
            OutcomeSide::Yes,
            Usd::new(dec!(25)),
            TickSize::Hundredth,
            PolicyReplayLatency {
                base_delay_ms: 0,
                stress_multiplier: Decimal::ONE,
            },
            &observations,
            at(60),
        )
        .expect("horizon replay");

        assert_eq!(outcome.gap, None);
        assert_eq!(outcome.terminal_at, Some(at(60)));
        assert_eq!(outcome.terminal_reason, Some(ExitReason::TimeExit));
        assert_eq!(
            outcome.full_l2_coverage,
            TradePolicyEvidenceCoverage::Covered,
        );
    }

    #[test]
    fn two_x_uses_returns() {
        let observations = vec![
            observation(0, dec!(0.49), dec!(0.50)),
            observation(1, dec!(0.50), dec!(0.51)),
            observation(2, dec!(0.52), dec!(0.53)),
            observation(120, dec!(0.60), dec!(0.61)),
            observation(121, dec!(0.60), dec!(0.61)),
            observation(122, dec!(0.60), dec!(0.61)),
        ];
        let one = replay_policy_candidate(
            &candidate(FillRequirement::AllowPartial),
            OutcomeSide::Yes,
            Usd::new(dec!(25)),
            TickSize::Hundredth,
            PolicyReplayLatency {
                base_delay_ms: 1_000,
                stress_multiplier: Decimal::ONE,
            },
            &observations,
        )
        .expect("one x");
        let two = replay_policy_candidate(
            &candidate(FillRequirement::AllowPartial),
            OutcomeSide::Yes,
            Usd::new(dec!(25)),
            TickSize::Hundredth,
            PolicyReplayLatency {
                base_delay_ms: 1_000,
                stress_multiplier: dec!(2),
            },
            &observations,
        )
        .expect("two x");

        assert_eq!(one.fills[0].filled_at, at(1));
        assert_eq!(two.fills[0].filled_at, at(2));
        assert_ne!(one.fills[0].vwap, two.fills[0].vwap);
    }

    #[test]
    fn fok_never_turns_position() {
        let mut first = observation(0, dec!(0.49), dec!(0.50));
        first.book.as_mut().expect("book").asks = vec![level(dec!(0.50), dec!(1))];
        let outcome = replay_policy_candidate(
            &candidate(FillRequirement::AllOrNothing),
            OutcomeSide::Yes,
            Usd::new(dec!(25)),
            TickSize::Hundredth,
            PolicyReplayLatency {
                base_delay_ms: 0,
                stress_multiplier: Decimal::ONE,
            },
            &[first, observation(60, dec!(0.49), dec!(0.50))],
        )
        .expect("replay");

        assert_eq!(outcome.entry_filled_shares, Shares::ZERO);
        assert_eq!(
            outcome.gap,
            Some(TradePolicyReplayGap::EntryDepthInsufficient)
        );
    }

    #[test]
    fn future_evidence_is_rejected() {
        let mut invalid = observation(0, dec!(0.49), dec!(0.50));
        invalid.book.as_mut().expect("book").available_at = at(1);
        assert!(
            replay_policy_candidate(
                &candidate(FillRequirement::AllowPartial),
                OutcomeSide::Yes,
                Usd::new(dec!(25)),
                TickSize::Hundredth,
                PolicyReplayLatency {
                    base_delay_ms: 0,
                    stress_multiplier: Decimal::ONE,
                },
                &[invalid],
            )
            .is_err()
        );
    }

    #[test]
    fn pit_visibility_requires_freshness() {
        let decision_at = at(0);
        let action_at = decision_at + Duration::milliseconds(110);
        let decision_boundary = DecisionClock::new(2)
            .boundary(decision_at)
            .expect("decision boundary");
        let action_boundary = DecisionClock::new(2)
            .boundary(action_at)
            .expect("action boundary");
        let initial_book_at = decision_boundary.cutoff_for(DecisionSource::Book);
        let fresh_book_at = action_boundary.cutoff_for(DecisionSource::Book);
        let mut policy = candidate(FillRequirement::AllowPartial);
        policy.entry_execution = EntryOrderTemplate::Aggressive {
            fill_requirement: FillRequirement::AllowPartial,
            max_slippage_bps: Bps::new(dec!(100)),
            max_book_age_ms: 2_000,
        };
        for (case, event_at, expected_gap) in [
            (
                "decision book after latency",
                initial_book_at,
                Some(TradePolicyReplayGap::EntryBookStale),
            ),
            ("exact action freshness", fresh_book_at, None),
            (
                "one millisecond stale",
                fresh_book_at - Duration::milliseconds(1),
                Some(TradePolicyReplayGap::EntryBookStale),
            ),
        ] {
            let mut trigger = observation(0, dec!(0.49), dec!(0.50));
            let trigger_book = trigger.book.as_mut().expect("trigger book");
            trigger_book.observed_at = initial_book_at;
            trigger_book.available_at = initial_book_at;
            trigger.fee_schedule = Some(schedule(initial_book_at));
            let mut action = observation(0, dec!(0.49), dec!(0.50));
            action.at = action_at;
            action.decision_tick = false;
            let action_book = action.book.as_mut().expect("action book");
            action_book.observed_at = event_at;
            action_book.available_at = event_at;
            action_book.token_sequence = 2;
            action.fee_schedule = Some(schedule(initial_book_at));
            // Every action book is PIT-valid under the actual two-second lag.
            // Execution freshness is a separate age check at D + 110 ms.
            assert!(
                event_at <= action_boundary.cutoff_for(DecisionSource::Book),
                "{case}"
            );
            assert!(event_at <= action_boundary.knowledge_cutoff(), "{case}");
            let mut terminal = observation(60, dec!(0.49), dec!(0.50));
            let terminal_book = terminal.book.as_mut().expect("terminal book");
            terminal_book.observed_at = at(58);
            terminal_book.available_at = at(58);
            terminal.fee_schedule = Some(schedule(at(58)));
            let outcome = replay_policy_horizon(
                &policy,
                OutcomeSide::Yes,
                Usd::new(dec!(25)),
                TickSize::Hundredth,
                PolicyReplayLatency {
                    base_delay_ms: 110,
                    stress_multiplier: Decimal::ONE,
                },
                &[trigger, action, terminal],
                at(60),
            )
            .expect("complete fee/depth replay");
            assert_eq!(outcome.gap, expected_gap, "{case}");
            if expected_gap.is_none() {
                assert_eq!(outcome.entered_at, Some(action_at), "{case}");
                assert_eq!(outcome.entry_fill_latency_ms, Some(110), "{case}");
                assert_eq!(outcome.entry_fill_ratio, Decimal::ONE, "{case}");
                assert_eq!(outcome.exit_fill_ratio, Decimal::ONE, "{case}");
                assert_eq!(
                    outcome.terminal_reason,
                    Some(ExitReason::TimeExit),
                    "{case}"
                );
                assert_eq!(
                    outcome.full_l2_coverage,
                    TradePolicyEvidenceCoverage::Covered,
                    "{case}"
                );
                assert!(outcome.fee_covered, "{case}");
                assert!(outcome.execution_fee_usd.is_positive(), "{case}");
            } else {
                assert_eq!(outcome.entered_at, None, "{case}");
                assert!(outcome.fills.is_empty(), "{case}");
            }
        }
    }

    #[test]
    fn signal_invalidation_precedes_profit() {
        let mut invalidated = observation(60, dec!(0.56), dec!(0.57));
        invalidated
            .signal
            .as_mut()
            .expect("signal")
            .route_gate_eligible = false;
        let outcome = replay_policy_candidate(
            &candidate(FillRequirement::AllowPartial),
            OutcomeSide::Yes,
            Usd::new(dec!(25)),
            TickSize::Hundredth,
            PolicyReplayLatency {
                base_delay_ms: 0,
                stress_multiplier: Decimal::ONE,
            },
            &[observation(0, dec!(0.49), dec!(0.50)), invalidated],
        )
        .expect("replay");

        assert_eq!(outcome.terminal_reason, Some(ExitReason::SignalInvalidated));
    }

    #[test]
    fn passive_requires_after_ahead() {
        let mut passive = candidate(FillRequirement::AllowPartial);
        passive.entry_execution = EntryOrderTemplate::PassivePostOnly {
            placement: PassivePlacement::JoinBestBid,
            good_til_secs: 60,
            max_book_age_ms: 5_000,
        };
        let first = observation(0, dec!(0.49), dec!(0.50));
        let mut matched = observation(1, dec!(0.49), dec!(0.50));
        matched.passive_trades.push(PolicyReplayTrade {
            event_at: at(1),
            available_at: at(1),
            stream_session_id: Uuid::nil(),
            token_sequence: 2,
            side: Side::Sell,
            price: Price::new(dec!(0.49)),
            shares: Shares::new(dec!(1_100)),
            source_event_id: format!("blake3:{}", "1".repeat(64)),
        });
        let outcome = replay_policy_candidate(
            &passive,
            OutcomeSide::Yes,
            Usd::new(dec!(25)),
            TickSize::Hundredth,
            PolicyReplayLatency {
                base_delay_ms: 0,
                stress_multiplier: Decimal::ONE,
            },
            &[first, matched, observation(121, dec!(0.49), dec!(0.50))],
        )
        .expect("passive replay");

        assert_eq!(outcome.entered_at, Some(at(1)));
        assert_eq!(outcome.expected_maker_rebate_accrual_usd, Usd::ZERO);
        assert_eq!(outcome.expected_net_return_bps, outcome.risk_net_return_bps);
        assert_eq!(outcome.passive_reconciled_trade_covered, Some(true));
        assert_eq!(outcome.terminal_reason, Some(ExitReason::TimeExit));
    }

    #[test]
    fn passive_unavailable_rejected() {
        let mut passive = candidate(FillRequirement::AllowPartial);
        passive.entry_execution = EntryOrderTemplate::PassivePostOnly {
            placement: PassivePlacement::JoinBestBid,
            good_til_secs: 60,
            max_book_age_ms: 5_000,
        };
        let mut first = observation(0, dec!(0.49), dec!(0.50));
        first.maker_rebate_evidence = PitMakerRebateEvidence::Unavailable {
            reason: PitMakerRebateUnavailableReason::NotPointInTime,
            terms_hash: hash('f'),
            available_at: at(1),
        };

        let outcome = replay_policy_candidate(
            &passive,
            OutcomeSide::Yes,
            Usd::new(dec!(25)),
            TickSize::Hundredth,
            PolicyReplayLatency {
                base_delay_ms: 0,
                stress_multiplier: Decimal::ONE,
            },
            &[first, observation(61, dec!(0.49), dec!(0.50))],
        )
        .expect("passive replay");

        assert_eq!(
            outcome.gap,
            Some(TradePolicyReplayGap::PitMakerRebateUnavailable)
        );
        assert_eq!(
            outcome.passive_rebate_evidence_coverage,
            TradePolicyEvidenceCoverage::Missing
        );
    }

    #[test]
    fn passive_drift_cancels() {
        let mut passive = candidate(FillRequirement::AllowPartial);
        passive.entry_execution = EntryOrderTemplate::PassivePostOnly {
            placement: PassivePlacement::JoinBestBid,
            good_til_secs: 60,
            max_book_age_ms: 5_000,
        };
        let first = observation(0, dec!(0.49), dec!(0.50));
        let mut changed = observation(1, dec!(0.49), dec!(0.50));
        changed.maker_rebate_evidence = PitMakerRebateEvidence::NoProgram {
            terms_hash: hash('e'),
            available_at: at(1),
        };

        let outcome = replay_policy_candidate(
            &passive,
            OutcomeSide::Yes,
            Usd::new(dec!(25)),
            TickSize::Hundredth,
            PolicyReplayLatency {
                base_delay_ms: 0,
                stress_multiplier: Decimal::ONE,
            },
            &[first, changed, observation(61, dec!(0.49), dec!(0.50))],
        )
        .expect("passive replay");

        assert_eq!(outcome.gap, Some(TradePolicyReplayGap::PassiveTermsDrift));
        assert!(outcome.entered_at.is_none());
    }

    #[test]
    fn passive_drift_race_fails() {
        let mut passive = candidate(FillRequirement::AllowPartial);
        passive.entry_execution = EntryOrderTemplate::PassivePostOnly {
            placement: PassivePlacement::JoinBestBid,
            good_til_secs: 60,
            max_book_age_ms: 5_000,
        };
        let first = observation(0, dec!(0.49), dec!(0.50));
        let mut changed = observation(1, dec!(0.49), dec!(0.50));
        changed.maker_rebate_evidence = PitMakerRebateEvidence::NoProgram {
            terms_hash: hash('e'),
            available_at: at(1),
        };
        changed.passive_trades.push(PolicyReplayTrade {
            event_at: at(1),
            available_at: at(1),
            stream_session_id: Uuid::nil(),
            token_sequence: 2,
            side: Side::Sell,
            price: Price::new(dec!(0.49)),
            shares: Shares::new(dec!(1_100)),
            source_event_id: format!("blake3:{}", "4".repeat(64)),
        });

        let outcome = replay_policy_candidate(
            &passive,
            OutcomeSide::Yes,
            Usd::new(dec!(25)),
            TickSize::Hundredth,
            PolicyReplayLatency {
                base_delay_ms: 0,
                stress_multiplier: Decimal::ONE,
            },
            &[first, changed, observation(61, dec!(0.49), dec!(0.50))],
        )
        .expect("passive replay");

        assert_eq!(
            outcome.gap,
            Some(TradePolicyReplayGap::PassiveCancelFillRace)
        );
        assert_eq!(
            outcome.passive_rebate_evidence_coverage,
            TradePolicyEvidenceCoverage::Missing
        );
    }

    #[test]
    fn passive_rebate_stays_nominal() {
        let mut passive = candidate(FillRequirement::AllowPartial);
        passive.entry_execution = EntryOrderTemplate::PassivePostOnly {
            placement: PassivePlacement::JoinBestBid,
            good_til_secs: 60,
            max_book_age_ms: 5_000,
        };
        let mut first = observation(0, dec!(0.49), dec!(0.50));
        let schedule = maker_rebate_schedule(at(0));
        let evidence = PitMakerRebateEvidence::Available { schedule };
        first.maker_rebate_evidence = evidence.clone();
        let mut matched = observation(1, dec!(0.49), dec!(0.50));
        matched.maker_rebate_evidence = evidence.clone();
        matched.passive_trades.push(PolicyReplayTrade {
            event_at: at(1),
            available_at: at(1),
            stream_session_id: Uuid::nil(),
            token_sequence: 2,
            side: Side::Sell,
            price: Price::new(dec!(0.49)),
            shares: Shares::new(dec!(1_100)),
            source_event_id: format!("blake3:{}", "3".repeat(64)),
        });
        let mut expired = observation(121, dec!(0.49), dec!(0.50));
        expired.maker_rebate_evidence = evidence;
        let outcome = replay_policy_candidate(
            &passive,
            OutcomeSide::Yes,
            Usd::new(dec!(25)),
            TickSize::Hundredth,
            PolicyReplayLatency {
                base_delay_ms: 0,
                stress_multiplier: Decimal::ONE,
            },
            &[first, matched, expired],
        )
        .expect("passive rebate replay");

        assert!(outcome.expected_maker_rebate_accrual_usd.is_positive());
        assert!(outcome.expected_net_return_bps > outcome.risk_net_return_bps);
        assert_eq!(
            outcome.passive_rebate_evidence_coverage,
            TradePolicyEvidenceCoverage::Covered
        );
        assert!(outcome.fills.iter().any(|fill| {
            fill.expected_maker_rebate_accrual_usd.is_positive()
                && matches!(
                    fill.maker_rebate_evidence,
                    Some(PitMakerRebateEvidence::Available { .. })
                )
        }));
    }

    #[test]
    fn no_fill_is_covered() {
        let mut passive = candidate(FillRequirement::AllowPartial);
        passive.entry_execution = EntryOrderTemplate::PassivePostOnly {
            placement: PassivePlacement::JoinBestBid,
            good_til_secs: 60,
            max_book_age_ms: 5_000,
        };

        let outcome = replay_policy_candidate(
            &passive,
            OutcomeSide::Yes,
            Usd::new(dec!(25)),
            TickSize::Hundredth,
            PolicyReplayLatency {
                base_delay_ms: 0,
                stress_multiplier: Decimal::ONE,
            },
            &[
                observation(0, dec!(0.49), dec!(0.50)),
                observation(61, dec!(0.49), dec!(0.50)),
            ],
        )
        .expect("passive replay");

        assert_eq!(outcome.entry_triggered_at, Some(at(0)));
        assert_eq!(outcome.terminal_at, Some(at(60)));
        assert_eq!(outcome.entry_fill_ratio, Decimal::ZERO);
        assert_eq!(outcome.expected_net_return_bps, Some(Decimal::ZERO));
        assert_eq!(outcome.risk_net_return_bps, Some(Decimal::ZERO));
        assert_eq!(outcome.passive_reconciled_trade_covered, Some(true));
        assert!(outcome.gap.is_none());
    }

    #[test]
    fn passive_accumulates_partial_slices() {
        let mut passive = candidate(FillRequirement::AllowPartial);
        passive.entry_execution = EntryOrderTemplate::PassivePostOnly {
            placement: PassivePlacement::JoinBestBid,
            good_til_secs: 60,
            max_book_age_ms: 5_000,
        };
        let first = observation(0, dec!(0.49), dec!(0.50));
        let mut partial = observation(1, dec!(0.49), dec!(0.50));
        partial.passive_trades.push(PolicyReplayTrade {
            event_at: at(1),
            available_at: at(1),
            stream_session_id: Uuid::nil(),
            token_sequence: 2,
            side: Side::Sell,
            price: Price::new(dec!(0.49)),
            shares: Shares::new(dec!(1_010)),
            source_event_id: format!("blake3:{}", "1".repeat(64)),
        });
        let mut completed = observation(2, dec!(0.49), dec!(0.50));
        completed.passive_trades.push(PolicyReplayTrade {
            event_at: at(2),
            available_at: at(2),
            stream_session_id: Uuid::nil(),
            token_sequence: 3,
            side: Side::Sell,
            price: Price::new(dec!(0.48)),
            shares: Shares::new(dec!(100)),
            source_event_id: format!("blake3:{}", "2".repeat(64)),
        });

        let outcome = replay_policy_candidate(
            &passive,
            OutcomeSide::Yes,
            Usd::new(dec!(25)),
            TickSize::Hundredth,
            PolicyReplayLatency {
                base_delay_ms: 0,
                stress_multiplier: Decimal::ONE,
            },
            &[
                first,
                partial,
                completed,
                observation(122, dec!(0.49), dec!(0.50)),
            ],
        )
        .expect("passive replay");
        let entry_fills = outcome
            .fills
            .iter()
            .filter(|fill| fill.exit_reason.is_none())
            .collect::<Vec<_>>();

        assert_eq!(entry_fills.len(), 2);
        assert_eq!(entry_fills[0].filled_shares, Shares::new(dec!(10)));
        assert_eq!(entry_fills[1].outcome, BookWalkOutcome::Filled);
        assert_eq!(outcome.entry_fill_ratio, Decimal::ONE);
    }

    #[test]
    fn passive_rejects_session_reset() {
        let mut passive = candidate(FillRequirement::AllowPartial);
        passive.entry_execution = EntryOrderTemplate::PassivePostOnly {
            placement: PassivePlacement::JoinBestBid,
            good_til_secs: 60,
            max_book_age_ms: 5_000,
        };
        let first = observation(0, dec!(0.49), dec!(0.50));
        let mut reset = observation(1, dec!(0.49), dec!(0.50));
        reset.passive_trades.push(PolicyReplayTrade {
            event_at: at(1),
            available_at: at(1),
            stream_session_id: Uuid::now_v7(),
            token_sequence: 2,
            side: Side::Sell,
            price: Price::new(dec!(0.49)),
            shares: Shares::new(dec!(1_100)),
            source_event_id: format!("blake3:{}", "3".repeat(64)),
        });

        let outcome = replay_policy_candidate(
            &passive,
            OutcomeSide::Yes,
            Usd::new(dec!(25)),
            TickSize::Hundredth,
            PolicyReplayLatency {
                base_delay_ms: 0,
                stress_multiplier: Decimal::ONE,
            },
            &[first, reset, observation(61, dec!(0.49), dec!(0.50))],
        )
        .expect("passive replay");

        assert_eq!(
            outcome.gap,
            Some(TradePolicyReplayGap::PassiveTradeCoverageUnavailable)
        );
    }

    #[test]
    fn trailing_stop_activates_residual() {
        let mut trailing = candidate(FillRequirement::AllowPartial);
        trailing.exit.upper_barrier_bps = Bps::new(dec!(20_000));
        trailing.exit.trailing_stop = Some(TrailingStopTemplate {
            trail_bps: Bps::new(dec!(300)),
            activation_return_bps: Bps::new(dec!(500)),
        });
        let outcome = replay_policy_candidate(
            &trailing,
            OutcomeSide::Yes,
            Usd::new(dec!(25)),
            TickSize::Hundredth,
            PolicyReplayLatency {
                base_delay_ms: 0,
                stress_multiplier: Decimal::ONE,
            },
            &[
                observation(0, dec!(0.49), dec!(0.50)),
                observation(30, dec!(0.55), dec!(0.56)),
                observation(60, dec!(0.53), dec!(0.54)),
            ],
        )
        .expect("trailing replay");

        assert_eq!(outcome.terminal_reason, Some(ExitReason::StopLoss));
        assert_eq!(outcome.exit_fill_ratio, Decimal::ONE);
    }

    #[test]
    fn hold_uses_without_schedule() {
        let mut held = candidate(FillRequirement::AllowPartial);
        held.exit.settlement_mode = ExitSettlementMode::HoldToResolution;
        held.exit.upper_barrier_bps = Bps::new(dec!(20_000));
        held.exit.lower_barrier_bps = Bps::new(dec!(20_000));
        held.exit.vertical_barrier_secs = 10_000;
        held.exit.scale_out_targets.clear();
        let mut resolution = observation(60, dec!(0.49), dec!(0.50));
        resolution.decision_tick = false;
        resolution.book = None;
        resolution.fee_schedule = None;
        resolution.signal = None;
        resolution.resolution = Some(PolicyReplayResolution {
            token_payout_ratio: PayoutRatio::try_new(dec!(0.5)).expect("half payout"),
            resolved_at: at(59),
            observed_at: at(60),
        });
        let outcome = replay_policy_candidate(
            &held,
            OutcomeSide::Yes,
            Usd::new(dec!(25)),
            TickSize::Hundredth,
            PolicyReplayLatency {
                base_delay_ms: 0,
                stress_multiplier: Decimal::ONE,
            },
            &[observation(0, dec!(0.49), dec!(0.50)), resolution],
        )
        .expect("resolution replay");

        assert_eq!(outcome.terminal_reason, Some(ExitReason::ResolutionRedeem));
        assert_eq!(outcome.exit_fill_ratio, Decimal::ONE);
        assert_eq!(
            outcome.fills.last().and_then(|fill| fill.vwap),
            Some(Price::new(dec!(0.5)))
        );
    }

    #[test]
    fn resolution_uses_application_time() {
        let mut held = candidate(FillRequirement::AllowPartial);
        held.exit.settlement_mode = ExitSettlementMode::HoldToResolution;
        held.exit.scale_out_targets.clear();
        let mut economics = Vec::new();
        for (observed_at, delay_ms, entered_at) in [(3, 5_000, 5), (12, 5_000, 5), (12, 0, 0)] {
            let mut action = observation(5, dec!(0.49), dec!(0.50));
            action.decision_tick = false;
            let mut terminal = observation(12, dec!(0.49), dec!(0.50));
            terminal.decision_tick = false;
            terminal.book = None;
            terminal.fee_schedule = None;
            terminal.signal = None;
            terminal.resolution = Some(PolicyReplayResolution {
                token_payout_ratio: PayoutRatio::try_new(dec!(0.5)).expect("half payout"),
                resolved_at: at(2),
                observed_at: at(observed_at),
            });
            let observations = [observation(0, dec!(0.49), dec!(0.50)), action, terminal];
            let outcome = replay_policy_candidate(
                &held,
                OutcomeSide::Yes,
                Usd::new(dec!(25)),
                TickSize::Hundredth,
                PolicyReplayLatency {
                    base_delay_ms: delay_ms,
                    stress_multiplier: Decimal::ONE,
                },
                &observations,
            )
            .expect("PIT resolution application replay");
            assert_eq!(outcome.gap, None);
            assert_eq!(outcome.entered_at, Some(at(entered_at)));
            assert_eq!(outcome.terminal_at, Some(at(12)));
            assert_eq!(outcome.terminal_reason, Some(ExitReason::ResolutionRedeem));
            assert_eq!(outcome.entry_filled_shares, outcome.exited_shares);
            let payout = outcome.fills.last().expect("resolution payout fill");
            assert_eq!(payout.triggered_at, at(12));
            assert_eq!(payout.filled_at, at(12));
            assert!(payout.filled_at > outcome.entered_at.expect("executed entry"));
            assert_eq!(
                payout.gross_amount,
                Usd::new(outcome.entry_filled_shares.inner() * dec!(0.5))
            );
            assert_eq!(payout.execution_fee_usd, Usd::ZERO);
            assert_eq!(payout.expected_maker_rebate_accrual_usd, Usd::ZERO);
            assert_eq!(
                outcome.execution_fee_usd,
                outcome.fills[0].execution_fee_usd
            );
            assert_eq!(
                observations[2]
                    .resolution
                    .as_ref()
                    .expect("source resolution")
                    .observed_at,
                at(observed_at)
            );
            economics.push((
                outcome.entry_filled_shares,
                payout.gross_amount,
                outcome.execution_fee_usd,
                outcome.expected_net_return_bps,
            ));
        }
        assert!(economics.windows(2).all(|pair| pair[0] == pair[1]));
    }

    #[test]
    fn resolution_rejects_future_source() {
        for (resolved_at, observed_at) in [(13, 13), (2, 13)] {
            let mut terminal = observation(12, dec!(0.49), dec!(0.50));
            terminal.resolution = Some(PolicyReplayResolution {
                token_payout_ratio: PayoutRatio::ONE,
                resolved_at: at(resolved_at),
                observed_at: at(observed_at),
            });
            let error = replay_policy_candidate(
                &candidate(FillRequirement::AllowPartial),
                OutcomeSide::Yes,
                Usd::new(dec!(25)),
                TickSize::Hundredth,
                PolicyReplayLatency {
                    base_delay_ms: 0,
                    stress_multiplier: Decimal::ONE,
                },
                &[observation(0, dec!(0.49), dec!(0.50)), terminal],
            )
            .expect_err("future resolution source must remain forbidden");
            assert!(error.to_string().contains("future evidence"));
        }
    }

    #[test]
    fn time_rejects_duplicate_heartbeats() {
        let duplicated = observation(0, dec!(0.49), dec!(0.50));
        assert!(
            replay_policy_candidate(
                &candidate(FillRequirement::AllowPartial),
                OutcomeSide::Yes,
                Usd::new(dec!(25)),
                TickSize::Hundredth,
                PolicyReplayLatency {
                    base_delay_ms: 0,
                    stress_multiplier: Decimal::ONE,
                },
                &[duplicated.clone(), duplicated],
            )
            .is_err()
        );
    }

    #[test]
    fn latency_delay_ceil_quantized() {
        let observations = vec![
            observation(0, dec!(0.49), dec!(0.50)),
            observation(1, dec!(0.50), dec!(0.51)),
            observation(2, dec!(0.51), dec!(0.52)),
            observation(120, dec!(0.60), dec!(0.61)),
        ];
        let outcome = replay_policy_candidate(
            &candidate(FillRequirement::AllowPartial),
            OutcomeSide::Yes,
            Usd::new(dec!(25)),
            TickSize::Hundredth,
            PolicyReplayLatency {
                base_delay_ms: 501,
                stress_multiplier: dec!(2),
            },
            &observations,
        )
        .expect("replay");
        assert_eq!(outcome.entered_at, Some(at(2)));
        assert!(at(2) - at(0) >= Duration::milliseconds(1_002));
    }
}
