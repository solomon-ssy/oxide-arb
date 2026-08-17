//! Strong-typed `quant_order_intent` JSONB column content types.
//!
//! Content contract for `entry_order_json` / `exit_policy_json`. Defined here
//! (below the entity layer) so the entity uses them directly as JSONB columns —
//! never a bare `serde_json::Value`. They are the **executable projection** of a
//! recommendation's `EntryPlan` / `ExitPlan`, materialized as
//! `EntryOrderSpec` / `ExitPolicy`.
//!
//! The order-intent write path owns mutation; this module defines
//! the strong-typed contract so the dormant table never carries a bare `Value`.

use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use schemars::JsonSchema;
use sea_orm::FromJsonQueryResult;
use serde::{Deserialize, Serialize};

use crate::{
    domain::market::fee::{BuilderFeeAttribution, FrozenMakerRebateSchedule},
    enums::{
        common::{OrderType, Side},
        quant::{ExitSettlementMode, RedeemPolicy},
    },
    types::{
        Bps, ContentHash, ModelVersionId, OpportunisticExitPolicy, Price, Probability,
        ResearchProfileRef, ScaleOutTarget, Shares, ThesisInvalidationPolicy, TokenId,
        TrailingStopPolicy, Usd,
    },
};

/// Governed intent amount. Aggressive BUY orders carry a total cash budget;
/// resting orders and SELL orders carry an exact share quantity.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case", tag = "unit", content = "value")]
pub enum OrderAmount {
    CashBudget(Usd),
    Shares(Shares),
}

/// Exact amount encoded in a venue order after admission.
///
/// This is intentionally distinct from [`OrderAmount`]: CLOB market BUY orders
/// encode fee-exclusive principal, not the governed cash budget.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "unit", content = "value")]
pub enum VenueOrderAmount {
    GrossUsd(Usd),
    Shares(Shares),
}

/// Route-aware maker-rebate contract frozen at recommendation and intent time.
///
/// This sum type makes route applicability and a confirmed no-program state
/// explicit; neither is represented by an ambiguous `None` or numeric zero.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case", tag = "state", deny_unknown_fields)]
pub enum EntryMakerRebateTerms {
    AggressiveNotApplicable,
    PassiveNoProgram {
        terms_hash: ContentHash,
        available_at: DateTime<Utc>,
    },
    PassiveProgram {
        schedule: FrozenMakerRebateSchedule,
    },
}

impl EntryMakerRebateTerms {
    /// Active maker-rebate schedule, only for a passive program route.
    #[must_use]
    pub const fn schedule(&self) -> Option<FrozenMakerRebateSchedule> {
        match self {
            Self::PassiveProgram { schedule } => Some(*schedule),
            Self::AggressiveNotApplicable | Self::PassiveNoProgram { .. } => None,
        }
    }

    /// Stable Gamma terms identity for passive routes.
    #[must_use]
    pub const fn passive_terms_hash(&self) -> Option<ContentHash> {
        match self {
            Self::AggressiveNotApplicable => None,
            Self::PassiveNoProgram { terms_hash, .. } => Some(*terms_hash),
            Self::PassiveProgram { schedule } => Some(schedule.terms_hash),
        }
    }
}

/// Exact fee schedule frozen at admission. Reconciliation must use this
/// decision-time evidence, never a process-current fee cache.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, FromJsonQueryResult)]
pub struct PreparedFeeSchedule {
    pub schedule_hash: ContentHash,
    pub effective_at: DateTime<Utc>,
    pub available_at: DateTime<Utc>,
    pub platform_rate: Decimal,
    pub exponent: Decimal,
    pub taker_only: bool,
    pub builder_maker_fee_bps: Bps,
    pub builder_taker_fee_bps: Bps,
    pub builder_attribution: BuilderFeeAttribution,
}

/// Atomic, hash-linked venue order prepared by final admission.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, FromJsonQueryResult)]
#[serde(deny_unknown_fields)]
pub struct PreparedVenueOrder {
    pub profile_ref: ResearchProfileRef,
    pub token_id: TokenId,
    pub side: Side,
    pub order_type: OrderType,
    pub post_only: bool,
    pub worst_price: Price,
    pub cash_budget: Option<Usd>,
    pub venue_amount: VenueOrderAmount,
    pub expected_fee: Usd,
    pub total_cash_delta: Decimal,
    pub expected_filled_shares: Shares,
    pub book_hash: ContentHash,
    pub clob_market_info_hash: ContentHash,
    pub fee_schedule: PreparedFeeSchedule,
    /// Decision-time route applicability and Gamma terms.
    pub maker_rebate_terms: EntryMakerRebateTerms,
    pub prepared_at: DateTime<Utc>,
    pub valid_until: DateTime<Utc>,
}

/// Persisted classification of the latest governed thesis re-inference.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum ExitReinferenceVerdictKind {
    Holds,
    ThesisInvalidated,
    Indeterminate,
}

/// Latest re-inference evidence persisted on the intent at the governed cadence.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, FromJsonQueryResult, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ExitReinferenceObservation {
    pub observed_at: DateTime<Utc>,
    pub model_version_id: ModelVersionId,
    pub model_artifact_hash: ContentHash,
    pub factor_snapshot_hash: ContentHash,
    pub mark: Price,
    pub score: Probability,
    #[serde(with = "rust_decimal::serde::str")]
    #[schemars(with = "String")]
    pub score_retention: Decimal,
    pub expected_return_bps: Bps,
    pub route_gate_eligible: bool,
    pub verdict: ExitReinferenceVerdictKind,
    pub detail: String,
    pub shadow: bool,
}

impl OrderAmount {
    #[must_use]
    pub const fn cash_budget(self) -> Option<Usd> {
        match self {
            Self::CashBudget(value) => Some(value),
            Self::Shares(_) => None,
        }
    }

    #[must_use]
    pub const fn as_shares(self) -> Option<Shares> {
        match self {
            Self::Shares(value) => Some(value),
            Self::CashBudget(_) => None,
        }
    }
}

impl VenueOrderAmount {
    #[must_use]
    pub const fn gross_usd(self) -> Option<Usd> {
        match self {
            Self::GrossUsd(value) => Some(value),
            Self::Shares(_) => None,
        }
    }

    #[must_use]
    pub const fn shares(self) -> Option<Shares> {
        match self {
            Self::Shares(value) => Some(value),
            Self::GrossUsd(_) => None,
        }
    }
}

/// Exact next cumulative scale-out projection shared by monitor and read APIs.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct NextScaleOutProjection {
    pub target_id: String,
    pub trigger_price: Price,
    #[serde(with = "rust_decimal::serde::str")]
    #[schemars(with = "String")]
    pub target_cumulative_exit_pct: Decimal,
    pub delta_shares: Shares,
}

/// The concrete entry order an approved intent will submit to the venue.
///
/// `side` is always [`Side::Buy`] for an opening recommendation (the outcome is
/// chosen by `token_id`); the type stays general so a future closing intent can
/// reuse it.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, FromJsonQueryResult, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct EntryOrderSpec {
    /// Outcome token to trade.
    pub token_id: TokenId,
    /// Order direction (opening = `Buy`).
    pub side: Side,
    /// Time-in-force / order type.
    pub order_type: OrderType,
    /// Whether venue admission must reject immediately marketable placement.
    pub post_only: bool,
    /// Hard limit price for the order.
    pub limit_price: Price,
    /// Venue amount with side/order-type semantics frozen at intent creation.
    pub amount: OrderAmount,
    /// Required route applicability and independently sourced Gamma terms.
    pub maker_rebate_terms: EntryMakerRebateTerms,
    /// Maximum tolerated slippage from the reference price.
    pub max_slippage_bps: Bps,
    /// Latest time the order may be submitted.
    pub valid_until: DateTime<Utc>,
}

impl EntryOrderSpec {
    /// Conservative share projection used by pre-submit depth and ledger checks.
    #[must_use]
    pub fn projected_shares(&self) -> Shares {
        match self.amount {
            OrderAmount::Shares(shares) => shares,
            OrderAmount::CashBudget(usd) if self.limit_price.is_positive() => {
                Shares::new(usd.inner() / self.limit_price.inner())
            }
            OrderAmount::CashBudget(_) => Shares::ZERO,
        }
    }

    /// Maximum capital this frozen order can consume.
    #[must_use]
    pub fn notional(&self) -> Usd {
        match self.amount {
            OrderAmount::CashBudget(usd) => usd,
            OrderAmount::Shares(shares) => shares * self.limit_price,
        }
    }
}

/// The exit policy an approved intent freezes after the entry fills.
///
/// A **faithful, complete** projection of the recommendation's `ExitPlan` — the
/// exit monitor evaluates every trigger deterministically from this frozen
/// contract and never re-reads the (possibly expired/revoked) recommendation
/// for the price/time/trailing/partial ladder. `entry_reference_price` and
/// `entry_composite_score` are the frozen entry-thesis baselines used for
/// percentage-based stops/targets and signal-degradation re-inference.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, FromJsonQueryResult, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ExitPolicySpec {
    /// Take-profit price target.
    pub take_profit_price: Option<Price>,
    /// Take-profit as a percentage move from the entry reference price.
    #[serde(with = "crate::types::decimal_string_option")]
    #[schemars(with = "Option<String>")]
    pub take_profit_pct: Option<Decimal>,
    /// Stop-loss price target.
    pub stop_loss_price: Option<Price>,
    /// Stop-loss as a percentage move from the entry reference price.
    #[serde(with = "crate::types::decimal_string_option")]
    #[schemars(with = "Option<String>")]
    pub stop_loss_pct: Option<Decimal>,
    /// Absolute time-based exit.
    pub time_exit_at: Option<DateTime<Utc>>,
    /// Maximum holding period in seconds (relative to the lot's open time).
    pub max_hold_secs: Option<u64>,
    /// Optional trailing-stop policy (folded into the effective stop-loss).
    pub trailing_stop: Option<TrailingStopPolicy>,
    /// Machine-evaluable thesis invalidation thresholds.
    pub thesis_invalidation: ThesisInvalidationPolicy,
    /// Policy-fitted advisory exit thresholds.
    pub opportunistic_exit: OpportunisticExitPolicy,
    /// Monotone cumulative scale-out targets.
    pub scale_out_targets: Vec<ScaleOutTarget>,
    /// Whether the lot exits before resolution or holds through resolution.
    pub settlement_mode: ExitSettlementMode,
    /// Whether a resolved hold-to-resolution lot is redeemed automatically.
    pub redeem_policy: RedeemPolicy,
    /// Optional manual-review checkpoint time.
    pub manual_review_at: Option<DateTime<Utc>>,
    /// Frozen entry reference price (recommendation `entry_price_ref` / limit),
    /// the basis for percentage-based take-profit / stop-loss / trailing math.
    pub entry_reference_price: Price,
    /// Frozen entry composite score — the baseline the signal-degradation
    /// re-inference compares the fresh score against.
    pub entry_composite_score: Probability,
}

impl ExitPolicySpec {
    /// Tightest absolute, percentage, or armed trailing stop.
    #[must_use]
    pub fn effective_stop(&self, avg_price: Price, peak: Option<Price>) -> Option<Price> {
        let mut stop = self.stop_loss_price;
        if let Some(pct) = self.stop_loss_pct {
            let from_pct = avg_price * (Decimal::ONE - pct);
            stop = Some(stop.map_or(from_pct, |value| value.max(from_pct)));
        }
        if let (Some(trailing), Some(peak)) = (&self.trailing_stop, peak)
            && trailing
                .activation_price
                .is_none_or(|activation| peak >= activation)
        {
            let trail = peak * (Decimal::ONE - trailing.trail_bps.inner() / Decimal::from(10_000));
            stop = Some(stop.map_or(trail, |value| value.max(trail)));
        }
        stop
    }

    /// First active, unsettled scale-out target and its exact incremental shares.
    #[must_use]
    pub fn next_scale_out(
        &self,
        state: &ScaleOutState,
        remaining_shares: Shares,
        mark: Option<Price>,
        now: DateTime<Utc>,
    ) -> Option<NextScaleOutProjection> {
        let mark = mark?;
        self.scale_out_targets.iter().find_map(|target| {
            if state.contains(&target.target_id)
                || target.valid_after.is_some_and(|after| now < after)
                || target.valid_until.is_some_and(|until| now > until)
                || target.min_price.is_some_and(|minimum| mark < minimum)
                || mark < target.trigger_price
            {
                return None;
            }
            let delta_shares = state
                .delta_to_target(target.target_cumulative_exit_pct)
                .min(remaining_shares);
            delta_shares.is_positive().then(|| NextScaleOutProjection {
                target_id: target.target_id.clone(),
                trigger_price: target.trigger_price,
                target_cumulative_exit_pct: target.target_cumulative_exit_pct,
                delta_shares,
            })
        })
    }
}

/// One in-flight cumulative scale-out target.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct PendingScaleOut {
    /// Deterministic target id; opportunistic cumulative targets have no id.
    pub target_id: Option<String>,
    /// Desired cumulative exit fraction of the frozen entry-filled denominator.
    #[serde(with = "rust_decimal::serde::str")]
    #[schemars(with = "String")]
    pub target_cumulative_exit_pct: Decimal,
}

/// Unified scale-out state for deterministic and opportunistic partial exits.
#[derive(
    Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize, FromJsonQueryResult, JsonSchema,
)]
#[serde(deny_unknown_fields)]
pub struct ScaleOutState {
    /// Frozen entry-filled denominator shared by every scale-out source.
    pub denominator_shares: Option<Shares>,
    /// Cumulative settled partial exits relative to the denominator.
    pub cumulative_exited_shares: Shares,
    /// Stable target ids already settled (append-only, deduplicated).
    pub settled_target_ids: Vec<String>,
    /// Cumulative target currently submitted to the venue, if any.
    pub pending_target: Option<PendingScaleOut>,
}

impl ScaleOutState {
    /// Whether `target_id` has already settled.
    #[must_use]
    pub fn contains(&self, target_id: &str) -> bool {
        self.settled_target_ids.iter().any(|id| id == target_id)
    }

    /// Record a settled scale-out delta and close a deterministic target only
    /// after its cumulative fraction has actually been reached.
    pub fn record(&mut self, filled: Shares) {
        self.cumulative_exited_shares =
            Shares::new(self.cumulative_exited_shares.inner() + filled.inner());
        if let (Some(denominator), Some(pending)) =
            (self.denominator_shares, self.pending_target.as_ref())
        {
            let reached = self.cumulative_exited_shares.inner()
                >= denominator.inner() * pending.target_cumulative_exit_pct;
            if reached
                && let Some(target_id) = pending.target_id.as_deref()
                && !self.contains(target_id)
            {
                self.settled_target_ids.push(target_id.to_owned());
            }
        }
        self.pending_target = None;
    }

    /// Shares needed to reach `target_pct`, or zero when already satisfied.
    #[must_use]
    pub fn delta_to_target(&self, target_pct: Decimal) -> Shares {
        let Some(denominator) = self.denominator_shares else {
            return Shares::ZERO;
        };
        let target = denominator.inner() * target_pct;
        Shares::new((target - self.cumulative_exited_shares.inner()).max(Decimal::ZERO))
    }

    #[must_use]
    pub fn cumulative_exit_pct(&self) -> Option<Decimal> {
        let denominator = self.denominator_shares?;
        denominator.is_positive().then(|| {
            (self.cumulative_exited_shares.inner() / denominator.inner())
                .clamp(Decimal::ZERO, Decimal::ONE)
        })
    }
}

#[cfg(test)]
mod tests {
    use rust_decimal_macros::dec;

    use super::{PendingScaleOut, ScaleOutState};
    use crate::types::Shares;

    #[test]
    fn partial_fill_never_settles() {
        let mut state = ScaleOutState {
            denominator_shares: Some(Shares::new(dec!(100))),
            pending_target: Some(PendingScaleOut {
                target_id: Some("tp1".to_owned()),
                target_cumulative_exit_pct: dec!(0.5),
            }),
            ..ScaleOutState::default()
        };

        state.record(Shares::new(dec!(20)));

        assert!(!state.contains("tp1"));
        assert_eq!(state.delta_to_target(dec!(0.5)), Shares::new(dec!(30)));
    }

    #[test]
    fn deterministic_opportunistic_fills_denominator() {
        let mut state = ScaleOutState {
            denominator_shares: Some(Shares::new(dec!(100))),
            pending_target: Some(PendingScaleOut {
                target_id: None,
                target_cumulative_exit_pct: dec!(0.2),
            }),
            ..ScaleOutState::default()
        };
        state.record(Shares::new(dec!(20)));
        state.pending_target = Some(PendingScaleOut {
            target_id: Some("tp1".to_owned()),
            target_cumulative_exit_pct: dec!(0.5),
        });
        state.record(Shares::new(dec!(30)));

        assert!(state.contains("tp1"));
        assert_eq!(state.cumulative_exited_shares, Shares::new(dec!(50)));
        assert_eq!(state.delta_to_target(dec!(0.5)), Shares::ZERO);
    }
}
