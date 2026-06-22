//! Quant-pivot `ClickHouse` fact rows.

use crate::{
    clickhouse::{ChDecimal64, ChPrice, ChProbability, ChShares, ChUsd},
    types::{
        MarketId, ModelRunId, OrderIntentId, RecommendationId, RecommendationReportId, TokenId,
    },
};
use serde::{Deserialize, Serialize};

/// Feature value fact emitted by PIT feature builders.
#[derive(Debug, Clone, clickhouse::Row, Serialize, Deserialize)]
pub struct QuantFeatureEventRow {
    pub event_time: i64,
    pub as_of: i64,
    pub market_id: MarketId,
    pub token_id: TokenId,
    pub feature_schema_version: u32,
    pub feature_name: String,
    pub feature_value: ChDecimal64,
    pub value_kind: i8,
    pub source_kind: String,
    pub staleness_ms: u64,
    pub ingestion_time: i64,
}

/// Factor value fact emitted after feature normalization.
#[derive(Debug, Clone, clickhouse::Row, Serialize, Deserialize)]
pub struct QuantFactorEventRow {
    pub event_time: i64,
    pub as_of: i64,
    pub market_id: MarketId,
    pub factor_name: String,
    pub factor_family: String,
    pub raw_value: ChDecimal64,
    pub normalized_score: ChProbability,
    pub confidence: ChProbability,
    pub direction: i8,
    pub model_run_id: ModelRunId,
    pub ingestion_time: i64,
}

/// Candidate signal fact before portfolio pruning.
#[derive(Debug, Clone, clickhouse::Row, Serialize, Deserialize)]
pub struct QuantSignalCandidateEventRow {
    pub event_time: i64,
    pub signal_candidate_id: String,
    pub model_run_id: ModelRunId,
    pub market_id: MarketId,
    pub token_id: TokenId,
    pub side: i8,
    pub score: ChProbability,
    pub confidence: ChProbability,
    pub entry_price: ChPrice,
    pub target_price: ChPrice,
    pub stop_price: ChPrice,
    pub rank_before_portfolio: u32,
    pub rejection_reason: String,
}

/// Published recommendation fact.
#[derive(Debug, Clone, clickhouse::Row, Serialize, Deserialize)]
pub struct QuantRecommendationEventRow {
    pub event_time: i64,
    pub recommendation_report_id: RecommendationReportId,
    pub recommendation_id: RecommendationId,
    pub rank: u32,
    pub market_id: MarketId,
    pub token_id: TokenId,
    pub side: i8,
    pub score: ChProbability,
    pub risk_adjusted_score: ChProbability,
    pub suggested_usd: ChUsd,
    pub valid_until: i64,
    pub status: String,
}

/// Execution lifecycle fact.
#[derive(Debug, Clone, clickhouse::Row, Serialize, Deserialize)]
pub struct QuantExecutionEventRow {
    pub event_time: i64,
    pub order_intent_id: OrderIntentId,
    pub execution_order_id: String,
    pub recommendation_id: RecommendationId,
    pub event_kind: String,
    pub market_id: MarketId,
    pub token_id: TokenId,
    pub side: i8,
    pub price: ChPrice,
    pub shares: ChShares,
    pub cost_usd: ChUsd,
    pub venue_order_id: String,
    pub ingestion_time: i64,
}
