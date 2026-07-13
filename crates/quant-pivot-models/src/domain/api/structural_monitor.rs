//! Structural Alpha monitor HTTP contract (Phase 11.2.1+).
//!
//! Read surface for structural signals: neg-risk leg-sum drift remains live
//! book-derived, while trade-tape participant concentration is ClickHouse-backed
//! with explicit source health and missing-reason accounting.

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
    pub computed_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize)]
pub struct MissingReasonCountView {
    pub reason: String,
    pub count: u64,
}

#[derive(Debug, Clone, Serialize)]
pub struct TradeTapeSourceHealthView {
    pub source: String,
    pub enabled: bool,
    pub token_cursor_count: u64,
    pub bootstrap_count: u64,
    pub catching_up_count: u64,
    pub live_count: u64,
    pub empty_count: u64,
    pub error_count: u64,
    pub worst_lag_blocks: Option<i64>,
    pub last_updated_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, Serialize)]
pub struct TradeTapeCoverageView {
    pub decision_at: DateTime<Utc>,
    pub knowledge_cutoff: DateTime<Utc>,
    pub window_secs: u64,
    pub knowledge_lag_secs: u64,
    pub active_market_count: u64,
    pub token_cursor_count: u64,
    pub market_cursor_count: u64,
    pub covered_market_ratio: Decimal,
    pub source_health: Vec<TradeTapeSourceHealthView>,
    pub missing_reason_breakdown: Vec<MissingReasonCountView>,
}

#[derive(Debug, Clone, Serialize)]
pub struct ParticipantConcentrationMarketView {
    pub market_id: MarketId,
    pub token_id: TokenId,
    pub question: String,
    pub knowledge_cutoff: DateTime<Utc>,
    pub trade_count: Option<u64>,
    pub participant_count: Option<u64>,
    pub notional_usd: Option<Decimal>,
    pub coverage_ratio: Option<Decimal>,
    pub gini: Option<Decimal>,
    pub hhi: Option<Decimal>,
    pub cr1_share: Option<Decimal>,
    pub composite_raw: Option<Decimal>,
    pub lag_blocks: Option<i64>,
    pub missing_reason: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct ParticipantConcentrationSummaryView {
    pub decision_at: DateTime<Utc>,
    pub knowledge_cutoff: DateTime<Utc>,
    pub window_secs: u64,
    pub knowledge_lag_secs: u64,
    pub min_unique_participants: u64,
    pub min_notional_usd: Decimal,
    pub min_coverage_ratio: Decimal,
    pub markets: Vec<ParticipantConcentrationMarketView>,
    pub missing_reason_breakdown: Vec<MissingReasonCountView>,
}

#[derive(Debug, Clone, Serialize)]
pub struct ParticipantConcentrationParticipantView {
    pub participant_address: String,
    pub participant_role: String,
    pub trade_count: u64,
    pub notional_usd: Decimal,
    pub share: Decimal,
}

#[derive(Debug, Clone, Serialize)]
pub struct ParticipantConcentrationDetailView {
    pub decision_at: DateTime<Utc>,
    pub knowledge_cutoff: DateTime<Utc>,
    pub market: ParticipantConcentrationMarketView,
    pub top_participants: Vec<ParticipantConcentrationParticipantView>,
}
