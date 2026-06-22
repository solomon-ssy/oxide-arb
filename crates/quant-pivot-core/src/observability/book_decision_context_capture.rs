//! Capture immutable top-N book context at money-decision boundaries.

use chrono::Utc;
use oxide_arb_algorithm::scorer::ScoredOpportunity;
use oxide_arb_models::{
    clickhouse::{BookDecisionContextRow, ChBps, ChPrice, ChSchemaVersion, ChUsd},
    domain::{
        book::{BookLevel, BookSnapshot, EndgameBookPair},
        latency::LatencyTrace,
        opportunity::Opportunity,
    },
    enums::{
        clickhouse::{ChBookDecisionStage, ChBookEvidenceTier, ChBookQuality, ChFactSource},
        common::Side,
    },
    types::{ExecutionId, Price, TokenId, Usd},
};
use rust_decimal::Decimal;
use serde::Serialize;
use std::sync::Arc;

const DEFAULT_TOP_N: usize = 20;

/// Immutable capture policy for decision-time book evidence.
#[derive(Debug, Clone)]
pub struct BookDecisionContextCapture {
    top_n: usize,
}

/// Captured row plus whether it is strong enough for Live admission.
#[derive(Debug, Clone)]
pub struct CapturedBookDecisionContext {
    pub row: BookDecisionContextRow,
    pub production_eligible: bool,
}

/// Lightweight context reference retained after the full `ClickHouse` row is enqueued.
#[derive(Debug, Clone)]
pub struct BookDecisionContextSummary {
    pub context_id: String,
    pub yes_book_age_ms: Option<u64>,
    pub no_book_age_ms: Option<u64>,
    pub production_eligible: bool,
}

impl From<&CapturedBookDecisionContext> for BookDecisionContextSummary {
    fn from(captured: &CapturedBookDecisionContext) -> Self {
        Self {
            context_id: captured.row.context_id.clone(),
            yes_book_age_ms: captured.row.yes_book_age_ms,
            no_book_age_ms: captured.row.no_book_age_ms,
            production_eligible: captured.production_eligible,
        }
    }
}

#[derive(Serialize)]
struct JsonLevel {
    price: String,
    size: String,
}

#[derive(Serialize)]
struct SlippagePoint {
    price: String,
    size: String,
    cumulative_shares: String,
    cumulative_usd: String,
}

#[derive(Serialize)]
struct LatencySummary {
    ws_to_book: Option<u64>,
    book_to_scan: Option<u64>,
    scan_to_emit: Option<u64>,
}

struct CaptureRequest<'a> {
    stage: ChBookDecisionStage,
    opp: &'a Opportunity,
    yes_token: &'a TokenId,
    no_token: &'a TokenId,
    yes_book_version: Option<u64>,
    no_book_version: Option<u64>,
    latency: Option<&'a LatencyTrace>,
    pair: &'a EndgameBookPair,
    execution_id: Option<&'a ExecutionId>,
    max_book_age_ms: u64,
    source: ChFactSource,
}

impl Default for BookDecisionContextCapture {
    fn default() -> Self {
        Self {
            top_n: DEFAULT_TOP_N,
        }
    }
}

impl BookDecisionContextCapture {
    #[must_use]
    pub const fn new(top_n: usize) -> Self {
        Self { top_n }
    }

    #[must_use]
    pub fn capture_scored(
        &self,
        stage: ChBookDecisionStage,
        scored: &ScoredOpportunity,
        pair: &EndgameBookPair,
        execution_id: Option<&ExecutionId>,
        max_book_age_ms: u64,
        source: ChFactSource,
    ) -> CapturedBookDecisionContext {
        self.capture(&CaptureRequest {
            stage,
            opp: scored.opportunity.as_ref(),
            yes_token: &scored.token_yes,
            no_token: &scored.token_no,
            yes_book_version: Some(scored.book_yes_version),
            no_book_version: Some(scored.book_no_version),
            latency: Some(scored.trace.as_ref()),
            pair,
            execution_id,
            max_book_age_ms,
            source,
        })
    }

    #[must_use]
    pub fn capture_detection(
        &self,
        scored: &ScoredOpportunity,
        pair: &EndgameBookPair,
        max_book_age_ms: u64,
    ) -> BookDecisionContextRow {
        self.capture_scored(
            ChBookDecisionStage::OpportunityDetected,
            scored,
            pair,
            None,
            max_book_age_ms,
            ChFactSource::Scanner,
        )
        .row
    }

    #[must_use]
    fn capture(&self, request: &CaptureRequest<'_>) -> CapturedBookDecisionContext {
        let now_ms = u64::try_from(Utc::now().timestamp_millis()).unwrap_or(0);
        let yes_age = now_ms.saturating_sub(request.pair.yes.timestamp_ms);
        let no_age = now_ms.saturating_sub(request.pair.no.timestamp_ms);
        let quality = book_quality(request.pair, yes_age.max(no_age), request.max_book_age_ms);
        let production_eligible = quality == ChBookQuality::Fresh;
        let tier = if production_eligible {
            ChBookEvidenceTier::DecisionContext
        } else {
            ChBookEvidenceTier::Insufficient
        };
        let traded = traded_book(
            request.opp,
            request.yes_token,
            request.no_token,
            &request.pair.yes,
            &request.pair.no,
        );
        let (spread_bps, mid_price) = spread_and_mid(traded);
        let slippage_curve_json = slippage_curve_json(traded, request.opp.side, self.top_n);
        let top_n = u16::try_from(self.top_n).unwrap_or(u16::MAX);
        let row = BookDecisionContextRow {
            context_id: context_id(
                request.opp,
                request.stage,
                request.yes_book_version,
                request.no_book_version,
            ),
            opportunity_id: Some(request.opp.opportunity_id.clone()),
            execution_id: request.execution_id.map(ToString::to_string),
            market_id: request.opp.market_id.clone(),
            yes_token_id: request.yes_token.clone(),
            no_token_id: request.no_token.clone(),
            decision_stage: request.stage,
            evidence_tier: tier,
            decision_time: i64::try_from(now_ms).unwrap_or(i64::MAX),
            yes_book_version: request.yes_book_version,
            no_book_version: request.no_book_version,
            yes_book_age_ms: Some(yes_age),
            no_book_age_ms: Some(no_age),
            top_n,
            yes_bids_json: levels_json(&request.pair.yes.bids, self.top_n),
            yes_asks_json: levels_json(&request.pair.yes.asks, self.top_n),
            no_bids_json: levels_json(&request.pair.no.bids, self.top_n),
            no_asks_json: levels_json(&request.pair.no.asks, self.top_n),
            yes_depth_usd: Some(ChUsd::from(executable_depth(
                &request.pair.yes,
                request.opp.side,
                self.top_n,
            ))),
            no_depth_usd: Some(ChUsd::from(executable_depth(
                &request.pair.no,
                request.opp.side,
                self.top_n,
            ))),
            spread_bps: spread_bps.map(ChBps::from),
            mid_price: mid_price.map(ChPrice::from),
            imbalance: imbalance(&request.pair.yes, &request.pair.no, self.top_n)
                .map(|v| v.to_string()),
            slippage_curve_json,
            book_quality: quality,
            latency_trace_json: request.latency.and_then(latency_json),
            source: request.source,
            ingestion_time: i64::try_from(now_ms).unwrap_or(i64::MAX),
            sequence: request
                .yes_book_version
                .unwrap_or(0)
                .max(request.no_book_version.unwrap_or(0)),
            schema_version: ChSchemaVersion(1),
        };
        CapturedBookDecisionContext {
            row,
            production_eligible,
        }
    }
}

fn context_id(
    opp: &Opportunity,
    stage: ChBookDecisionStage,
    yes_book_version: Option<u64>,
    no_book_version: Option<u64>,
) -> String {
    format!(
        "{}:{}:{}:{}",
        opp.opportunity_id,
        stage_label(stage),
        yes_book_version.unwrap_or(0),
        no_book_version.unwrap_or(0)
    )
}

const fn stage_label(stage: ChBookDecisionStage) -> &'static str {
    match stage {
        ChBookDecisionStage::OpportunityDetected => "opportunity_detected",
        ChBookDecisionStage::RiskGateEvaluated => "risk_gate_evaluated",
        ChBookDecisionStage::SizeComputed => "size_computed",
        ChBookDecisionStage::OrderPrepared => "order_prepared",
        ChBookDecisionStage::OrderSubmitted => "order_submitted",
        ChBookDecisionStage::OrderFilled => "order_filled",
        ChBookDecisionStage::OrderMissed => "order_missed",
        ChBookDecisionStage::OrderFailed => "order_failed",
        ChBookDecisionStage::SettlementAttributed => "settlement_attributed",
    }
}

fn book_quality(pair: &EndgameBookPair, max_age_ms: u64, threshold_ms: u64) -> ChBookQuality {
    if pair.yes.bids.is_empty()
        || pair.yes.asks.is_empty()
        || pair.no.bids.is_empty()
        || pair.no.asks.is_empty()
    {
        return ChBookQuality::Insufficient;
    }
    if crossed(&pair.yes) || crossed(&pair.no) {
        return ChBookQuality::Crossed;
    }
    if max_age_ms > threshold_ms {
        return ChBookQuality::Stale;
    }
    ChBookQuality::Fresh
}

fn crossed(book: &BookSnapshot) -> bool {
    matches!(
        (book.best_bid(), book.best_ask()),
        (Some(bid), Some(ask)) if bid >= ask
    )
}

fn traded_book<'a>(
    opp: &Opportunity,
    yes_token: &oxide_arb_models::types::TokenId,
    no_token: &oxide_arb_models::types::TokenId,
    yes: &'a Arc<BookSnapshot>,
    no: &'a Arc<BookSnapshot>,
) -> &'a BookSnapshot {
    if &opp.token_id == yes_token || (&opp.token_id != no_token && opp.meta.predicted_yes) {
        yes
    } else {
        no
    }
}

fn levels_json(levels: &[BookLevel], top_n: usize) -> String {
    let rows = levels
        .iter()
        .take(top_n)
        .map(|level| JsonLevel {
            price: level.price_decimal().to_string(),
            size: level.size_decimal().to_string(),
        })
        .collect::<Vec<_>>();
    serde_json::to_string(&rows).unwrap_or_else(|_| "[]".to_owned())
}

fn executable_depth(book: &BookSnapshot, side: Side, top_n: usize) -> Usd {
    let levels = match side {
        Side::Buy => &book.asks,
        Side::Sell => &book.bids,
    };
    depth_usd(levels.iter().take(top_n))
}

fn depth_usd<'a>(levels: impl Iterator<Item = &'a BookLevel>) -> Usd {
    levels.fold(Usd::ZERO, |acc, level| {
        Usd::new(acc.inner() + level.depth_usd().to_decimal())
    })
}

fn spread_and_mid(book: &BookSnapshot) -> (Option<Decimal>, Option<Price>) {
    let bid = book.best_bid();
    let ask = book.best_ask();
    let mid = match (bid, ask) {
        (Some(bid), Some(ask)) => Some(Price::new((bid.inner() + ask.inner()) / Decimal::from(2))),
        _ => None,
    };
    let spread = match (bid, ask, mid) {
        (Some(bid), Some(ask), Some(mid)) if !mid.inner().is_zero() => {
            Some((ask.inner() - bid.inner()) / mid.inner() * Decimal::from(10_000))
        }
        _ => None,
    };
    (spread, mid)
}

fn imbalance(yes: &BookSnapshot, no: &BookSnapshot, top_n: usize) -> Option<Decimal> {
    let bid_depth = depth_usd(yes.bids.iter().take(top_n)) + depth_usd(no.bids.iter().take(top_n));
    let ask_depth = depth_usd(yes.asks.iter().take(top_n)) + depth_usd(no.asks.iter().take(top_n));
    let denom = bid_depth.inner() + ask_depth.inner();
    if denom.is_zero() {
        return None;
    }
    Some((bid_depth.inner() - ask_depth.inner()) / denom)
}

fn slippage_curve_json(book: &BookSnapshot, side: Side, top_n: usize) -> Option<String> {
    let levels = match side {
        Side::Buy => &book.asks,
        Side::Sell => &book.bids,
    };
    if levels.is_empty() {
        return None;
    }
    let mut shares = Decimal::ZERO;
    let mut usd = Decimal::ZERO;
    let points = levels
        .iter()
        .take(top_n)
        .map(|level| {
            let level_shares = level.size_decimal().inner();
            let notional = level.depth_usd().to_decimal();
            shares += level_shares;
            usd += notional;
            SlippagePoint {
                price: level.price_decimal().to_string(),
                size: level.size_decimal().to_string(),
                cumulative_shares: shares.to_string(),
                cumulative_usd: usd.to_string(),
            }
        })
        .collect::<Vec<_>>();
    serde_json::to_string(&points).ok()
}

fn latency_json(trace: &LatencyTrace) -> Option<String> {
    let summary = LatencySummary {
        ws_to_book: elapsed_ms(trace.ws_ingress, trace.book_applied),
        book_to_scan: elapsed_ms(trace.book_applied, trace.scan_started),
        scan_to_emit: elapsed_ms(trace.scan_started, trace.scan_emitted),
    };
    serde_json::to_string(&summary).ok()
}

fn elapsed_ms(start: Option<std::time::Instant>, end: Option<std::time::Instant>) -> Option<u64> {
    let start = start?;
    let end = end?;
    u64::try_from(end.saturating_duration_since(start).as_millis()).ok()
}
