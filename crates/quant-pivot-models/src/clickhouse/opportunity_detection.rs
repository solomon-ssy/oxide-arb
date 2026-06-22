use crate::{
    clickhouse::{
        ChBps, ChDecimal64, ChFactor, ChPrice, ChProbability, ChSchemaVersion, ChShares, ChUsd,
    },
    domain::ScoredOpportunitySnapshot,
    enums::clickhouse::{ChDurationBucket, ChMarketCategory, ChPriceZone, ChSide},
    types::{EventId, FactorPublicationId, MarketId, MicroScore, OpportunityId, TokenId},
};
use serde::{Deserialize, Serialize};

/// `ClickHouse` row for the `opportunity_detection` table — scanner funnel analytics.
#[derive(Debug, Clone, clickhouse::Row, Serialize, Deserialize)]
pub struct OpportunityDetectionRow {
    pub opportunity_id: OpportunityId,
    pub market_id: MarketId,
    pub event_id: EventId,
    pub token_id: TokenId,
    pub token_yes: Option<TokenId>,
    pub token_no: Option<TokenId>,
    pub side: ChSide,
    pub entry_price: ChPrice,
    pub edge_bps: ChBps,
    pub expected_net_profit_usd: ChUsd,
    pub net_profit_if_correct_usd: ChUsd,
    pub shares: ChShares,
    pub total_cost_usd: ChUsd,
    pub total_fees_usd: ChUsd,
    pub resolution_prob: ChProbability,
    pub confidence: ChProbability,
    pub fill_probability: Option<ChProbability>,
    pub score: Option<i64>,
    pub urgency_factor: Option<ChFactor>,
    pub category_weight: Option<ChFactor>,
    pub staleness_discount: Option<ChFactor>,
    pub depth_used_pct: ChFactor,
    pub convergence_secs: u32,
    pub category: ChMarketCategory,
    pub price_zone: ChPriceZone,
    pub duration_bucket: ChDurationBucket,
    pub calibration_sample_size: u32,
    pub calibration_fallback_tier: u8,
    pub calibration_alpha: ChDecimal64,
    pub calibration_beta: ChDecimal64,
    pub calibration_posterior_mean: ChProbability,
    pub calibration_snapshot_hash: Option<String>,
    pub book_age_ms: Option<u64>,
    pub yes_book_version: Option<u64>,
    pub no_book_version: Option<u64>,
    pub control_publication_id: Option<FactorPublicationId>,
    pub score_components_json: String,
    pub calibration_snapshot_json: String,
    pub book_context_json: Option<String>,
    pub applied_factors_json: Option<String>,
    pub applied_factor_ids_json: Option<String>,
    pub latency_trace_json: Option<String>,
    pub missing_fields_json: Option<String>,
    pub detected_at: i64,
    pub ingestion_time: i64,
    pub sequence: u64,
    pub schema_version: ChSchemaVersion,
}

impl From<&ScoredOpportunitySnapshot> for OpportunityDetectionRow {
    fn from(snapshot: &ScoredOpportunitySnapshot) -> Self {
        Self {
            opportunity_id: snapshot.opportunity_id.clone(),
            market_id: snapshot.market_id.clone(),
            event_id: snapshot.event_id.clone(),
            token_id: snapshot.token_id.clone(),
            token_yes: snapshot.token_yes.clone(),
            token_no: snapshot.token_no.clone(),
            side: ChSide::from(snapshot.side),
            entry_price: ChPrice::from(snapshot.entry_price),
            edge_bps: ChBps::from(snapshot.edge_bps),
            expected_net_profit_usd: ChUsd::from(snapshot.expected_net_profit),
            net_profit_if_correct_usd: ChUsd::from(snapshot.net_profit_if_correct),
            shares: ChShares::from(snapshot.shares),
            total_cost_usd: ChUsd::from(snapshot.total_cost),
            total_fees_usd: ChUsd::from(snapshot.total_fees),
            resolution_prob: ChProbability::from(snapshot.resolution_prob_decimal),
            confidence: ChProbability::from(snapshot.confidence_decimal),
            fill_probability: snapshot
                .fill_probability
                .map(|value| ChProbability::from(value.to_decimal())),
            score: snapshot.score.map(MicroScore::micro),
            urgency_factor: snapshot
                .urgency_factor
                .map(|value| ChFactor::from(value.to_decimal())),
            category_weight: snapshot
                .category_weight
                .map(|value| ChFactor::from(value.to_decimal())),
            staleness_discount: snapshot
                .staleness_discount
                .map(|value| ChFactor::from(value.to_decimal())),
            depth_used_pct: ChFactor::from(snapshot.depth_used_pct_decimal),
            convergence_secs: snapshot.convergence_secs,
            category: ChMarketCategory::from(snapshot.category),
            price_zone: ChPriceZone::from(snapshot.price_zone),
            duration_bucket: ChDurationBucket::from(snapshot.duration_bucket),
            calibration_sample_size: snapshot.calibration.sample_size,
            calibration_fallback_tier: snapshot.calibration.fallback_tier,
            calibration_alpha: ChDecimal64::from(snapshot.calibration.alpha_prior),
            calibration_beta: ChDecimal64::from(snapshot.calibration.beta_prior),
            calibration_posterior_mean: ChProbability::from(snapshot.calibration.posterior_mean),
            calibration_snapshot_hash: snapshot.calibration.snapshot_hash.clone(),
            book_age_ms: snapshot.book.as_ref().and_then(|book| book.book_age_ms),
            yes_book_version: snapshot
                .book
                .as_ref()
                .and_then(|book| book.yes_book_version),
            no_book_version: snapshot.book.as_ref().and_then(|book| book.no_book_version),
            control_publication_id: snapshot
                .factors
                .as_ref()
                .and_then(|factors| factors.control_publication_id.as_ref())
                .cloned(),
            score_components_json: serde_json::json!({
                "fill_probability": snapshot.fill_probability,
                "score": snapshot.score,
                "urgency_factor": snapshot.urgency_factor,
                "category_weight": snapshot.category_weight,
                "staleness_discount": snapshot.staleness_discount,
            })
            .to_string(),
            calibration_snapshot_json: serde_json::to_string(&snapshot.calibration)
                .unwrap_or_else(|_| "{}".to_owned()),
            book_context_json: snapshot
                .book
                .as_ref()
                .and_then(|book| serde_json::to_string(book).ok()),
            applied_factors_json: snapshot
                .factors
                .as_ref()
                .and_then(|factors| serde_json::to_string(factors).ok()),
            applied_factor_ids_json: snapshot
                .factors
                .as_ref()
                .and_then(|factors| serde_json::to_string(&factors.factor_ids).ok()),
            latency_trace_json: None,
            missing_fields_json: if snapshot.missing_fields.is_empty() {
                None
            } else {
                serde_json::to_string(&snapshot.missing_fields).ok()
            },
            detected_at: snapshot.detected_at.timestamp_millis(),
            ingestion_time: snapshot.detected_at.timestamp_millis(),
            sequence: 0,
            schema_version: ChSchemaVersion(snapshot.schema_version),
        }
    }
}
