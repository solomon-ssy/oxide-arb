//! Deterministic candidate entry/exit replay shared by policy Fit and Validate.
//!
//! The kernel is deliberately I/O-free. Callers must resolve every observation
//! from one verified Source Slice, evaluate the frozen entry-condition AST, and
//! provide the point-in-time fee schedule before invoking it. Missing evidence
//! is returned as a typed coverage gap; raw trajectory labels are never used as
//! a fill or barrier substitute.

use chrono::{DateTime, Duration, Utc};
use quant_pivot_error::{QuantError, QuantResult, research::ResearchError};
use quant_pivot_models::{
    domain::market::book::BookLevel,
    enums::{
        clickhouse::ChTradeReconciliationStatus,
        common::{Side, TickSize},
        execution::ExitReason,
        quant::{ExitSettlementMode, FillRequirement, OutcomeSide},
    },
    hashing::CanonicalDigest,
    types::{
        Bps, ConditionTruth, ContentHash, EntryConditionTemplate, EntryOrderTemplate,
        PassivePlacement, PayoutRatio, Price, Shares, TokenId, TradePolicyCandidateSpec,
        TradePolicyReplayGap, Usd,
    },
};
use rust_decimal::{Decimal, RoundingStrategy, prelude::ToPrimitive};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::execution_semantics::{
    BookWalkFill, BookWalkOutcome, LiquidityRole, PassiveQueueAvailability, PassiveQueueState,
    PassiveTrade, PitFeeSchedule, walk_buy_cash_budget, walk_sell_exact_shares,
};

/// Versioned identity of the pure replay semantics sealed into evidence.
pub const POLICY_REPLAY_KERNEL_VERSION: &str = "weather_candidate_replay_v1";

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

/// One reconciled tape print available to a passive queue simulation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PolicyReplayTrade {
    pub event_at: DateTime<Utc>,
    pub available_at: DateTime<Utc>,
    pub stream_session_id: Uuid,
    pub side: Side,
    pub price: Price,
    pub shares: Shares,
    pub reconciliation_status: ChTradeReconciliationStatus,
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
    pub signal: Option<PolicyReplaySignal>,
    /// Completeness of Market-WS trade reconciliation since the preceding
    /// observation. Passive queue simulation fails closed on any unknown span.
    pub passive_trade_coverage: bool,
    pub passive_trades: Vec<PolicyReplayTrade>,
    pub resolution: Option<PolicyReplayResolution>,
}

/// `ReportOnly` latency applied to every trigger-to-prepared action.
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
    pub fee: Usd,
    pub cash_delta: Decimal,
    pub fee_schedule_hash: Option<ContentHash>,
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
    pub exit_fill_ratio: Decimal,
    pub entry_filled_shares: Shares,
    pub exited_shares: Shares,
    pub total_fees: Usd,
    pub net_return_bps: Option<Decimal>,
    pub ambiguous_touch: bool,
    pub full_l2: bool,
    pub fee_covered: bool,
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
            exit_fill_ratio: Decimal::ZERO,
            entry_filled_shares: Shares::ZERO,
            exited_shares: Shares::ZERO,
            total_fees: Usd::ZERO,
            net_return_bps: valid_no_trade.then_some(Decimal::ZERO),
            ambiguous_touch: false,
            full_l2: valid_no_trade,
            fee_covered: valid_no_trade,
            passive_reconciled_trade_covered: None,
            gap: Some(gap),
            fills: Vec::new(),
        }
    }
}

struct EntryResult {
    triggered_at: DateTime<Utc>,
    entered_at: DateTime<Utc>,
    fill_ratio: Decimal,
    fill: PolicyReplayFill,
    passive_coverage: Option<bool>,
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
    replay_open_position(
        candidate,
        token_side,
        cash_budget,
        latency,
        delay,
        observations,
        entry,
    )
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
        }) || observation
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
) -> QuantResult<Result<EntryResult, TradePolicyReplayGap>> {
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
            ),
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
        fill_ratio: ((fill.gross_amount.inner() + fill.fee.inner()) / cash_budget.inner())
            .min(Decimal::ONE),
        fill,
        passive_coverage: None,
    }))
}

fn passive_entry(
    request: PassiveEntryRequest<'_>,
) -> QuantResult<Result<EntryResult, TradePolicyReplayGap>> {
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
    if observations.iter().any(|observation| {
        observation.at >= placement_observation.at
            && observation.at <= expires_at
            && !observation.passive_trade_coverage
    }) {
        return Ok(Err(TradePolicyReplayGap::PassiveTradeCoverageUnavailable));
    }
    let mut trades = observations
        .iter()
        .filter(|observation| observation.at >= placement_observation.at)
        .flat_map(|observation| &observation.passive_trades)
        .filter(|trade| trade.event_at >= placement_observation.at && trade.event_at <= expires_at)
        .collect::<Vec<_>>();
    trades.sort_by(|left, right| {
        (left.event_at, left.available_at, &left.source_event_id).cmp(&(
            right.event_at,
            right.available_at,
            &right.source_event_id,
        ))
    });
    trades.dedup_by(|left, right| left.source_event_id == right.source_event_id);
    let mut fill_at = None;
    let mut source_event_hash = None;
    for trade in trades {
        queue.reset_session(trade.stream_session_id);
        let filled = queue.apply_trade(PassiveTrade {
            stream_session_id: trade.stream_session_id,
            side: trade.side,
            price: trade.price,
            shares: trade.shares,
            reconciliation_status: trade.reconciliation_status,
        });
        if filled > Shares::ZERO {
            fill_at = Some(trade.event_at);
            source_event_hash = Some(
                CanonicalDigest::content_hash_json(&trade.source_event_id)
                    .map_err(|error| methodology(format!("passive trade hash failed: {error}")))?,
            );
            break;
        }
    }
    if queue.availability != PassiveQueueAvailability::Available {
        return Ok(Err(TradePolicyReplayGap::PassiveTradeCoverageUnavailable));
    }
    let Some(filled_at) = fill_at else {
        return Ok(Err(TradePolicyReplayGap::EntryDepthInsufficient));
    };
    let fee = schedule
        .fee(LiquidityRole::Maker, price, queue.filled_shares, filled_at)
        .map_err(|error| methodology(format!("passive fee failed: {error:?}")))?;
    let gross = queue.filled_shares * price;
    let fill = PolicyReplayFill {
        leg_ordinal: 0,
        side: Side::Buy,
        exit_reason: None,
        triggered_at,
        filled_at,
        liquidity_role: LiquidityRole::Maker,
        outcome: if queue.filled_shares == requested {
            BookWalkOutcome::Filled
        } else {
            BookWalkOutcome::Partial
        },
        requested_shares: Some(requested),
        filled_shares: queue.filled_shares,
        vwap: Some(price),
        gross_amount: gross,
        fee,
        cash_delta: -(gross.inner() + fee.inner()),
        fee_schedule_hash: Some(schedule.schedule_hash),
        stream_session_id: Some(book.stream_session_id),
        token_sequence: Some(book.token_sequence),
        source_event_hash,
    };
    Ok(Ok(EntryResult {
        triggered_at,
        entered_at: filled_at,
        fill_ratio: queue.filled_shares.inner() / requested.inner(),
        fill,
        passive_coverage: Some(true),
    }))
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

fn replay_open_position(
    candidate: &TradePolicyCandidateSpec,
    token_side: OutcomeSide,
    cash_budget: Usd,
    latency: PolicyReplayLatency,
    delay: Duration,
    observations: &[PolicyReplayObservation],
    entry: EntryResult,
) -> QuantResult<PolicyReplayOutcome> {
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
    Ok(finalize_open_position(
        candidate,
        token_side,
        cash_budget,
        latency,
        observations,
        state,
    ))
}

fn open_position_state(
    entry: EntryResult,
    initial_signal: Option<PolicyReplaySignal>,
) -> QuantResult<OpenPositionState> {
    let entry_shares = entry.fill.filled_shares;
    let entry_cash = -entry.fill.cash_delta;
    let entry_price = entry
        .fill
        .vwap
        .ok_or_else(|| methodology("filled entry has no VWAP".to_owned()))?;
    Ok(OpenPositionState {
        entry_triggered_at: entry.triggered_at,
        entered_at: entry.entered_at,
        entry_fill_ratio: entry.fill_ratio,
        passive_coverage: entry.passive_coverage,
        entry_shares,
        entry_cash,
        entry_price,
        initial_signal,
        fills: vec![entry.fill],
        remaining: entry_shares,
        exited: Shares::ZERO,
        exit_cash: Decimal::ZERO,
        peak: entry_price,
        trailing_active: false,
        next_scale_out: 0,
        terminal_reason: None,
        terminal_at: None,
        gap: None,
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
        settle_resolution(state, resolution)?;
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
        state.exit_cash += fill.cash_delta;
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
) -> QuantResult<()> {
    if state.remaining == Shares::ZERO {
        return Ok(());
    }
    let payout = state.remaining.inner() * resolution.token_payout_ratio.inner();
    let reason = ExitReason::ResolutionRedeem;
    state.fills.push(PolicyReplayFill {
        leg_ordinal: next_ordinal(&state.fills)?,
        side: Side::Sell,
        exit_reason: Some(reason),
        triggered_at: resolution.resolved_at,
        filled_at: resolution.observed_at,
        liquidity_role: LiquidityRole::Maker,
        outcome: BookWalkOutcome::Filled,
        requested_shares: Some(state.remaining),
        filled_shares: state.remaining,
        vwap: Some(Price::new(resolution.token_payout_ratio.inner())),
        gross_amount: Usd::new(payout),
        fee: Usd::ZERO,
        cash_delta: payout,
        fee_schedule_hash: None,
        stream_session_id: None,
        token_sequence: None,
        source_event_hash: None,
    });
    state.exit_cash += payout;
    state.exited += state.remaining;
    state.remaining = Shares::ZERO;
    state.terminal_reason = Some(reason);
    state.terminal_at = Some(resolution.observed_at);
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
    let total_fees = state.fills.iter().map(|fill| fill.fee).sum();
    let net_return_bps = (state.remaining == Shares::ZERO && state.entry_cash > Decimal::ZERO)
        .then(|| {
            ((state.exit_cash - state.entry_cash) / state.entry_cash * Decimal::from(10_000))
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
        exit_fill_ratio,
        entry_filled_shares: state.entry_shares,
        exited_shares: state.exited,
        total_fees,
        net_return_bps,
        ambiguous_touch: false,
        full_l2,
        fee_covered,
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

const fn replay_fill_from_walk(
    context: ReplayFillContext<'_>,
    walk: &BookWalkFill,
) -> PolicyReplayFill {
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
        gross_amount: walk.gross_order_amount,
        fee: walk.expected_fee,
        cash_delta: walk.total_cash_delta,
        fee_schedule_hash: Some(context.schedule.schedule_hash),
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
        domain::market::{book::BookLevel, fee::BuilderFeeAttribution},
        enums::{
            clickhouse::ChTradeReconciliationStatus,
            common::{Side, TickSize},
            execution::ExitReason,
            quant::{ExitSettlementMode, FillRequirement, OutcomeSide, RedeemPolicy},
        },
        types::{
            Bps, ConditionTruth, ContentHash, EntryConditionTemplate, EntryOrderTemplate,
            ExitExecutionTemplate, OpportunisticExitPolicy, PassivePlacement, PayoutRatio, Price,
            Probability, ResidualSharePolicy, ScaleOutTemplate, Shares, TokenId,
            TradePolicyCandidateSpec, TradePolicyExitTemplate, TradePolicyReplayGap,
            TrailingStopTemplate, Usd,
        },
    };
    use rust_decimal::Decimal;
    use rust_decimal_macros::dec;
    use uuid::Uuid;

    use super::{
        POLICY_REPLAY_KERNEL_VERSION, PolicyReplayBook, PolicyReplayLatency,
        PolicyReplayObservation, PolicyReplayResolution, PolicyReplaySignal, PolicyReplayTrade,
        replay_policy_candidate,
    };
    use crate::execution_semantics::PitFeeSchedule;

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

        assert_eq!(POLICY_REPLAY_KERNEL_VERSION, "weather_candidate_replay_v1");
        assert_eq!(outcome.gap, None);
        assert_eq!(outcome.terminal_reason, Some(ExitReason::TakeProfit));
        assert_eq!(outcome.entry_fill_ratio, Decimal::ONE);
        assert_eq!(outcome.exit_fill_ratio, Decimal::ONE);
        assert!(
            outcome
                .net_return_bps
                .is_some_and(|value| value > Decimal::ZERO)
        );
        assert!(outcome.fills.len() >= 3);
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
            side: Side::Sell,
            price: Price::new(dec!(0.49)),
            shares: Shares::new(dec!(1_100)),
            reconciliation_status: ChTradeReconciliationStatus::Matched,
            source_event_id: "matched-passive-fill".to_owned(),
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
        assert_eq!(outcome.passive_reconciled_trade_covered, Some(true));
        assert_eq!(outcome.terminal_reason, Some(ExitReason::TimeExit));
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
