//! Neg-risk structural monitor HTTP contract (Phase 11.2.1).
//!
//! Read surface for the `struct.negrisk_leg_sum_drift` structural signal at the
//! event level: the sum of best-ask across all YES legs of a neg-risk event
//! should be ≈ 1; a persistent drift is a structural mispricing. The view is
//! computed live from the `MarketRegistry` + `BookStore` (no persisted fact).

use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use serde::Serialize;

use crate::types::{EventId, MarketId, TokenId};

/// One YES leg of a neg-risk event with its live best ask.
#[derive(Debug, Clone, Serialize)]
pub struct NegRiskLegView {
    pub market_id: MarketId,
    pub yes_token_id: TokenId,
    /// The leg's question (for operator context).
    pub question: String,
    /// Live best ask, or `None` when the leg has no published ask.
    pub best_ask: Option<Decimal>,
}

/// Per-event neg-risk leg-sum drift snapshot.
#[derive(Debug, Clone, Serialize)]
pub struct NegRiskEventDriftView {
    pub event_id: EventId,
    pub title: String,
    /// Number of resolved YES legs.
    pub leg_count: u32,
    /// Sum of best-ask across all legs, or `None` when any leg lacks an ask.
    pub ask_sum: Option<Decimal>,
    /// Structural drift `ask_sum − 1`, or `None` when `ask_sum` is unavailable.
    pub drift: Option<Decimal>,
    /// The individual legs and their asks.
    pub legs: Vec<NegRiskLegView>,
    /// When the snapshot was computed.
    pub as_of: DateTime<Utc>,
}
