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
use sea_orm::FromJsonQueryResult;
use serde::{Deserialize, Serialize};

use crate::{
    enums::{
        common::{OrderType, Side},
        quant::SettlementPolicy,
    },
    jsonb_active,
    types::{Bps, PartialExitNode, Price, Shares, TokenId},
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

/// The exit policy an approved intent enforces after the entry fills.
///
/// Projected from the recommendation's `ExitPlan`: the take-profit / stop-loss /
/// time-exit scalars are the canonical (full) exit; `partial_exit_nodes` carries
/// only genuine scaled exits (`sell_pct < 1`).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, FromJsonQueryResult)]
pub struct ExitPolicySpec {
    /// Take-profit price target.
    pub take_profit_price: Option<Price>,
    /// Stop-loss price target.
    pub stop_loss_price: Option<Price>,
    /// Absolute time-based exit.
    pub time_exit_at: Option<DateTime<Utc>>,
    /// Scaled partial-exit nodes (empty for a single full exit).
    pub partial_exit_nodes: Vec<PartialExitNode>,
    /// How the position settles at resolution.
    pub settlement_policy: SettlementPolicy,
}

jsonb_active!(EntryOrderSpec, ExitPolicySpec);
