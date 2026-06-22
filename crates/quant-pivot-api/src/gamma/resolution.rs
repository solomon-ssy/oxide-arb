//! Resolution probe result from `GET /markets?condition_ids={cid}`.

use chrono::{DateTime, Utc};
use oxide_arb_models::types::TokenId;
use serde::{Deserialize, Serialize};

/// Settlement status derived from a single Gamma market payload.
///
/// Gamma encodes the conclusion as a `"1"` settlement price on the winning
/// leg (`outcomePrices`) once `umaResolutionStatus` is `resolved` — there is
/// no `outcome` field on the wire. The winning leg is therefore identified by
/// **token id**, which is label-agnostic and safe for binary markets whose
/// outcomes are team names or Over/Under rather than Yes/No.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GammaResolution {
    /// Market is closed and UMA reports `resolved`.
    pub resolved: bool,
    /// CLOB token id of the winning leg; `None` when the settlement prices
    /// are ambiguous (fail-closed, never guessed).
    pub winning_token_id: Option<TokenId>,
    /// Outcome label of the winning leg (informational only — settlement and
    /// calibration must key on [`Self::winning_token_id`]).
    pub winning_outcome: Option<String>,
    /// Settlement close time (`closedTime`).
    pub resolved_at: Option<DateTime<Utc>>,
}
