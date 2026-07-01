//! Strong-typed `quant_order_intent` JSONB column content types.
//!
//! Content contract for `entry_order_json` / `exit_policy_json`. Defined here
//! (below the entity layer) so the entity uses them directly as JSONB columns —
//! never a bare `serde_json::Value`. They are the **executable projection** of a
//! recommendation's `EntryPlan` / `ExitPlan` (parent `01-domain-model-and-schema.md`
//! §10.4 `EntryOrderSpec` / `ExitPolicy`).
//!
//! The order-intent **write path** lands in a later phase; this module only fixes
//! the strong-typed contract so the dormant table never carries a bare `Value`.

use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use sea_orm::FromJsonQueryResult;
use serde::{Deserialize, Serialize};

use crate::{
    enums::{
        common::{OrderType, Side},
        quant::{ExitSettlementMode, RedeemPolicy},
    },
    jsonb_active,
    types::{
        Bps, PartialExitNode, Price, Probability, Shares, SignalInvalidationRule, TokenId,
        TrailingStop,
    },
};

/// The concrete entry order an approved intent will submit to the venue.
///
/// `side` is always [`Side::Buy`] for an opening recommendation (the outcome is
/// chosen by `token_id`); the type stays general so a future closing intent can
/// reuse it.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, FromJsonQueryResult)]
pub struct EntryOrderSpec {
    /// Outcome token to trade.
    pub token_id: TokenId,
    /// Order direction (opening = `Buy`).
    pub side: Side,
    /// Time-in-force / order type.
    pub order_type: OrderType,
    /// Hard limit price for the order.
    pub limit_price: Price,
    /// Share quantity to submit.
    pub shares: Shares,
    /// Maximum tolerated slippage from the reference price.
    pub max_slippage_bps: Bps,
    /// Latest time the order may be submitted.
    pub valid_until: DateTime<Utc>,
}

/// The exit policy an approved intent freezes after the entry fills.
///
/// A **faithful, complete** projection of the recommendation's `ExitPlan` — the
/// exit monitor evaluates every trigger deterministically from this frozen
/// contract and never re-reads the (possibly expired/revoked) recommendation
/// for the price/time/trailing/partial ladder. `entry_reference_price` and
/// `entry_composite_score` are the frozen entry-thesis baselines used for
/// percentage-based stops/targets and signal-degradation re-inference.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, FromJsonQueryResult)]
pub struct ExitPolicySpec {
    /// Take-profit price target.
    pub take_profit_price: Option<Price>,
    /// Take-profit as a percentage move from the entry reference price.
    pub take_profit_pct: Option<Decimal>,
    /// Stop-loss price target.
    pub stop_loss_price: Option<Price>,
    /// Stop-loss as a percentage move from the entry reference price.
    pub stop_loss_pct: Option<Decimal>,
    /// Absolute time-based exit.
    pub time_exit_at: Option<DateTime<Utc>>,
    /// Maximum holding period in seconds (relative to the lot's open time).
    pub max_hold_secs: Option<u64>,
    /// Optional trailing-stop policy (folded into the effective stop-loss).
    pub trailing_stop: Option<TrailingStop>,
    /// Conditions that invalidate the thesis (audit context for re-inference).
    pub signal_invalidation_rules: Vec<SignalInvalidationRule>,
    /// Scaled partial-exit nodes (empty for a single full exit).
    pub partial_exit_nodes: Vec<PartialExitNode>,
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

/// Partial-exit node ids whose tranches have **settled** on this intent lot.
///
/// Each `PartialExitNode::node_id` fires at most once; pending in-flight exits
/// are tracked separately on the intent row (`pending_partial_exit_node_id`).
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize, FromJsonQueryResult)]
pub struct ExecutedPartialExitNodes {
    /// Stable node ids already reduced on the lot (append-only, deduped).
    pub node_ids: Vec<String>,
}

impl ExecutedPartialExitNodes {
    /// Whether `node_id` has already settled.
    #[must_use]
    pub fn contains(&self, node_id: &str) -> bool {
        self.node_ids.iter().any(|id| id == node_id)
    }

    /// Record a settled node id (no-op when already present).
    pub fn record(&mut self, node_id: &str) {
        if !self.contains(node_id) {
            self.node_ids.push(node_id.to_owned());
        }
    }
}

/// Per-lot opportunistic-Sell scale-out state (Phase 06.1).
///
/// The Sell scorer emits a **target cumulative exit fraction** of the lot's
/// entry-filled shares. `denominator_shares` freezes that base at the first
/// opportunistic evaluation so the target is always measured against a fixed
/// quantity — immune to later reductions from stop-loss / take-profit / partial
/// nodes. `cumulative_sold_shares` is the monotonically accumulated total already
/// sold via opportunistic exits, so the ladder only ever submits the incremental
/// delta and repeated ticks at the same target never churn the position.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize, FromJsonQueryResult)]
pub struct OpportunisticExitState {
    /// Frozen entry-filled-shares denominator (set once, on first evaluation).
    pub denominator_shares: Option<Shares>,
    /// Cumulative shares already opportunistically sold on this lot (monotonic).
    pub cumulative_sold_shares: Shares,
}

impl OpportunisticExitState {
    /// Record an additional `filled` opportunistic exit (accumulates, never
    /// resets). Idempotency is the caller's responsibility (delta computation).
    pub fn record_sold(&mut self, filled: Shares) {
        self.cumulative_sold_shares =
            Shares::new(self.cumulative_sold_shares.inner() + filled.inner());
    }
}

jsonb_active!(
    EntryOrderSpec,
    ExitPolicySpec,
    ExecutedPartialExitNodes,
    OpportunisticExitState
);
