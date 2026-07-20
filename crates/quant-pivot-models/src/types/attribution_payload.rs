//! Strong-typed `quant_recommendation_attribution` JSONB column content types.
//!
//! Content contract for `entry_outcome_json` / `exit_outcome_json` /
//! `attribution_json`. Defined here (below the entity layer) so the entity uses
//! them directly as JSONB columns — never a bare `serde_json::Value`. Fields are
//! grounded in the post-trade / labelling vocabulary of
//! `docs/plans/quant-pivot/03-data-factor-model-pipeline.md` (`entry_filled`,
//! `entry_slippage_bps`, `exit_compliance`, `realized_pnl_usd`, `hit_stop_loss`,
//! `liquidity_exit_possible`, `settlement_outcome`).
//!
//! The attribution **write path** lands in a later phase; this module only fixes
//! the strong-typed contract so the dormant table never carries a bare `Value`.

use chrono::{DateTime, Utc};
use sea_orm::FromJsonQueryResult;
use serde::{Deserialize, Serialize};

use crate::{
    enums::{execution::ExitReason, quant::RecommendationOutcome},
    types::{Bps, Price, Shares},
};

/// How a recommendation's entry actually executed against the venue.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize, FromJsonQueryResult)]
#[serde(deny_unknown_fields)]
pub struct EntryOutcome {
    /// Whether the entry order filled at all.
    pub entry_filled: bool,
    /// Realized average fill price, when filled.
    pub fill_price: Option<Price>,
    /// Realized filled share quantity, when filled.
    pub fill_shares: Option<Shares>,
    /// Realized entry slippage vs the planned reference, when filled.
    pub entry_slippage_bps: Option<Bps>,
    /// When the entry filled.
    pub filled_at: Option<DateTime<Utc>>,
}

/// How a recommendation's exit actually resolved.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize, FromJsonQueryResult)]
#[serde(deny_unknown_fields)]
pub struct ExitOutcome {
    /// Realized average exit price, when exited on the book.
    pub exit_price: Option<Price>,
    /// Realized exited share quantity, when exited.
    pub exit_shares: Option<Shares>,
    /// Which exit trigger fired (take-profit / stop-loss / time / …).
    pub exit_trigger: Option<ExitReason>,
    /// Whether the realized exit complied with the planned exit policy.
    pub exit_compliance: bool,
    /// Terminal settlement outcome for the position.
    pub settlement_outcome: Option<RecommendationOutcome>,
    /// When the position was exited / settled.
    pub exited_at: Option<DateTime<Utc>>,
}

/// Post-hoc attribution detail comparing realized behaviour to the thesis.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize, FromJsonQueryResult)]
#[serde(deny_unknown_fields)]
pub struct AttributionDetail {
    /// Whether the stop-loss bound was reached.
    pub hit_stop_loss: bool,
    /// Whether the take-profit target was reached.
    pub hit_take_profit: bool,
    /// Whether a liquidity-feasible exit was available at the decision points.
    pub liquidity_exit_possible: bool,
    /// Free-form attribution notes.
    pub notes: Vec<String>,
}
