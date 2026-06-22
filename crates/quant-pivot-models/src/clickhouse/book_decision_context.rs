use serde::{Deserialize, Serialize};

use crate::{
    clickhouse::{ChBps, ChPrice, ChSchemaVersion, ChUsd},
    enums::clickhouse::{ChBookDecisionStage, ChBookEvidenceTier, ChBookQuality, ChFactSource},
    types::{MarketId, RecommendationId, TokenId},
};

/// Immutable book context captured at a money-decision boundary.
#[derive(Debug, Clone, clickhouse::Row, Serialize, Deserialize)]
pub struct BookDecisionContextRow {
    pub context_id: String,
    pub recommendation_id: Option<RecommendationId>,
    pub execution_id: Option<String>,
    pub market_id: MarketId,
    pub yes_token_id: TokenId,
    pub no_token_id: TokenId,
    pub decision_stage: ChBookDecisionStage,
    pub evidence_tier: ChBookEvidenceTier,
    pub decision_time: i64,
    pub yes_book_version: Option<u64>,
    pub no_book_version: Option<u64>,
    pub yes_book_age_ms: Option<u64>,
    pub no_book_age_ms: Option<u64>,
    pub top_n: u16,
    pub yes_bids_json: String,
    pub yes_asks_json: String,
    pub no_bids_json: String,
    pub no_asks_json: String,
    pub yes_depth_usd: Option<ChUsd>,
    pub no_depth_usd: Option<ChUsd>,
    pub spread_bps: Option<ChBps>,
    pub mid_price: Option<ChPrice>,
    pub imbalance: Option<String>,
    pub slippage_curve_json: Option<String>,
    pub book_quality: ChBookQuality,
    pub latency_trace_json: Option<String>,
    pub source: ChFactSource,
    pub ingestion_time: i64,
    pub sequence: u64,
    pub schema_version: ChSchemaVersion,
}
