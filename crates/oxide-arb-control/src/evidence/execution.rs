use chrono::{TimeZone, Utc};
use oxide_arb_models::{
    clickhouse::{ChPrice, ChShares, ChUsd, OpportunityAuditRow},
    domain::{
        book::BookLevel,
        control_factor::{QueryFingerprint, SimulationConfig},
        evidence::EvidenceMetric,
    },
    enums::clickhouse::{ChAuditOutcome, ChOpportunityAuditStage, ChSide},
    types::{MarketId, OpportunityId, Price},
};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

use crate::evidence::{
    book::{
        BookReconstructionArtifact, DecisionBookView, DecisionBookViewPurpose,
        DecisionTokenBookView,
    },
    replay::{FokReplayRequest, FokReplayResult, replay_fok, stress_levels},
};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExecutionEvidenceArtifact {
    pub report: ExecutionEvidenceReport,
    pub examples: Vec<ExecutionReplayExample>,
    pub audits: Vec<OpportunityAuditRow>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExecutionEvidenceReport {
    pub strict_fok_fill_rate_bps: u64,
    pub live_fill_rate_bps: u64,
    pub true_fill_count: u64,
    pub true_miss_count: u64,
    pub false_fill_count: u64,
    pub false_miss_count: u64,
    pub simulated_vwap_p50_bps: EvidenceMetric<u64>,
    pub simulated_vwap_p95_bps: EvidenceMetric<u64>,
    pub realized_slippage_p50_bps: EvidenceMetric<u64>,
    pub realized_slippage_p95_bps: EvidenceMetric<u64>,
    pub depth_consumed_pct_p50_bps: EvidenceMetric<u64>,
    pub depth_consumed_pct_p95_bps: EvidenceMetric<u64>,
    pub latency_shifted_miss_rate_bps: EvidenceMetric<u64>,
    pub adverse_selection_loss_p95_bps: EvidenceMetric<u64>,
    pub book_age_fill_correlation_bps: EvidenceMetric<i64>,
    pub missing_attribution_count: u64,
    pub query_fingerprints: Vec<QueryFingerprint>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExecutionReplayExample {
    pub opportunity_id: OpportunityId,
    pub market_id: MarketId,
    pub simulated_strict_fok_fill: bool,
    pub live_filled: bool,
    pub missing_attribution: bool,
}

#[must_use]
pub fn build(
    book: &BookReconstructionArtifact,
    audits: &[OpportunityAuditRow],
    query_fingerprints: Vec<QueryFingerprint>,
    simulation_config: &SimulationConfig,
) -> ExecutionEvidenceArtifact {
    let terminal_audits = terminal_audits(audits);
    let summary = summarize_execution(book, &terminal_audits, simulation_config);
    let report = build_report(&summary, query_fingerprints);
    ExecutionEvidenceArtifact {
        report,
        examples: summary.examples,
        audits: terminal_audits,
    }
}

#[derive(Debug, Default)]
struct ExecutionSummary {
    examples: Vec<ExecutionReplayExample>,
    true_fill_count: u64,
    true_miss_count: u64,
    false_fill_count: u64,
    false_miss_count: u64,
    missing_attribution_count: u64,
    simulated_vwap_bps: Vec<u64>,
    realized_slippage_bps: Vec<u64>,
    depth_consumed_pct_bps: Vec<u64>,
    latency_shifted_misses: u64,
    adverse_selection_loss_bps: Vec<u64>,
}

fn summarize_execution(
    book: &BookReconstructionArtifact,
    terminal_audits: &[OpportunityAuditRow],
    simulation_config: &SimulationConfig,
) -> ExecutionSummary {
    let mut summary = ExecutionSummary::default();
    for audit in terminal_audits {
        let live_filled = matches!(audit.outcome, Some(ChAuditOutcome::Success));
        let missing_attribution =
            audit.scored_snapshot_json.is_none() || audit.missing_fields_json.is_some();
        if missing_attribution {
            summary.missing_attribution_count = summary.missing_attribution_count.saturating_add(1);
        }
        let replay = replay_execution_sample(book, audit, simulation_config);
        let simulated_strict_fok_fill =
            replay.as_ref().is_some_and(|sample| sample.strict_fok_fill);
        if let Some(sample) = replay {
            summary
                .simulated_vwap_bps
                .push(price_to_bps(sample.simulated_vwap));
            summary
                .depth_consumed_pct_bps
                .push(sample.depth_consumed_pct_bps);
            if let Some(slippage) = sample.realized_slippage_bps {
                summary.realized_slippage_bps.push(slippage);
            }
            if !sample.latency_shifted_fill {
                summary.latency_shifted_misses = summary.latency_shifted_misses.saturating_add(1);
            }
            summary
                .adverse_selection_loss_bps
                .push(sample.adverse_selection_loss_bps);
        }
        match (simulated_strict_fok_fill, live_filled) {
            (true, true) => summary.true_fill_count = summary.true_fill_count.saturating_add(1),
            (false, false) => summary.true_miss_count = summary.true_miss_count.saturating_add(1),
            (true, false) => summary.false_fill_count = summary.false_fill_count.saturating_add(1),
            (false, true) => summary.false_miss_count = summary.false_miss_count.saturating_add(1),
        }
        summary.examples.push(ExecutionReplayExample {
            opportunity_id: audit.opportunity_id.clone(),
            market_id: audit.market_id.clone(),
            simulated_strict_fok_fill,
            live_filled,
            missing_attribution,
        });
    }
    summary
}

fn build_report(
    summary: &ExecutionSummary,
    query_fingerprints: Vec<QueryFingerprint>,
) -> ExecutionEvidenceReport {
    let total = u64::try_from(summary.examples.len()).unwrap_or(u64::MAX);
    let simulated_fills = summary
        .true_fill_count
        .saturating_add(summary.false_fill_count);
    let live_fills = summary
        .true_fill_count
        .saturating_add(summary.false_miss_count);
    ExecutionEvidenceReport {
        strict_fok_fill_rate_bps: simulated_fills
            .saturating_mul(10_000)
            .checked_div(total)
            .unwrap_or(0),
        live_fill_rate_bps: live_fills
            .saturating_mul(10_000)
            .checked_div(total)
            .unwrap_or(0),
        true_fill_count: summary.true_fill_count,
        true_miss_count: summary.true_miss_count,
        false_fill_count: summary.false_fill_count,
        false_miss_count: summary.false_miss_count,
        simulated_vwap_p50_bps: percentile_metric(
            &summary.simulated_vwap_bps,
            50,
            "execution.vwap_model_missing",
            "VWAP distribution requires depth-weighted replay samples",
        ),
        simulated_vwap_p95_bps: percentile_metric(
            &summary.simulated_vwap_bps,
            95,
            "execution.vwap_model_missing",
            "VWAP distribution requires depth-weighted replay samples",
        ),
        realized_slippage_p50_bps: percentile_metric(
            &summary.realized_slippage_bps,
            50,
            "execution.slippage_labels_missing",
            "realized slippage requires comparable terminal fill attribution",
        ),
        realized_slippage_p95_bps: percentile_metric(
            &summary.realized_slippage_bps,
            95,
            "execution.slippage_labels_missing",
            "realized slippage requires comparable terminal fill attribution",
        ),
        depth_consumed_pct_p50_bps: percentile_metric(
            &summary.depth_consumed_pct_bps,
            50,
            "execution.depth_distribution_missing",
            "depth consumption distribution requires depth-weighted replay samples",
        ),
        depth_consumed_pct_p95_bps: percentile_metric(
            &summary.depth_consumed_pct_bps,
            95,
            "execution.depth_distribution_missing",
            "depth consumption distribution requires depth-weighted replay samples",
        ),
        latency_shifted_miss_rate_bps: if total == 0 {
            EvidenceMetric::Unavailable {
                code: "execution.latency_trace_missing".to_owned(),
                reason: "latency-shifted FOK requires matched replay samples".to_owned(),
            }
        } else {
            EvidenceMetric::Available {
                value: summary
                    .latency_shifted_misses
                    .saturating_mul(10_000)
                    .checked_div(total)
                    .unwrap_or(0),
            }
        },
        adverse_selection_loss_p95_bps: percentile_metric(
            &summary.adverse_selection_loss_bps,
            95,
            "execution.stress_model_missing",
            "adverse-selection stress requires matched replay samples",
        ),
        book_age_fill_correlation_bps: EvidenceMetric::Unavailable {
            code: "execution.correlation_sample_missing".to_owned(),
            reason: "book age/fill correlation requires sufficient matched samples".to_owned(),
        },
        missing_attribution_count: summary.missing_attribution_count,
        query_fingerprints,
    }
}

#[derive(Debug, Clone, Copy)]
struct ExecutionReplaySample {
    strict_fok_fill: bool,
    simulated_vwap: Decimal,
    realized_slippage_bps: Option<u64>,
    depth_consumed_pct_bps: u64,
    latency_shifted_fill: bool,
    adverse_selection_loss_bps: u64,
}

fn replay_execution_sample(
    book: &BookReconstructionArtifact,
    audit: &OpportunityAuditRow,
    simulation_config: &SimulationConfig,
) -> Option<ExecutionReplaySample> {
    let limit_price = audit.entry_price.map(ChPrice::to_price)?;
    let decision_time = Utc.timestamp_millis_opt(audit.stage_at).single()?;
    let decision_view = terminal_decision_view_for(book, audit, decision_time)?;
    if !decision_view.production_eligible {
        return None;
    }
    let token_book = token_book_for_audit(decision_view, audit)?;
    let request = replay_request(audit, limit_price)?;
    let levels = levels_for_side(token_book, audit.side);
    let replay = replay_fok(levels, request)?;
    let latency_shifted_fill = latency_shifted_fill(book, audit, request, simulation_config);
    let adverse_selection_loss_bps =
        adverse_selection_loss_bps(levels, request, replay, simulation_config);
    let realized_slippage_bps = audit.fill_price.map(ChPrice::to_price).map(|fill_price| {
        decimal_bps(
            (fill_price.inner() - replay.vwap.inner()).abs(),
            replay.vwap.inner(),
        )
    });
    Some(ExecutionReplaySample {
        strict_fok_fill: replay.strict_fill,
        simulated_vwap: replay.vwap.inner(),
        realized_slippage_bps,
        depth_consumed_pct_bps: replay.depth_consumed_pct_bps,
        latency_shifted_fill,
        adverse_selection_loss_bps,
    })
}

fn replay_request(audit: &OpportunityAuditRow, limit_price: Price) -> Option<FokReplayRequest> {
    Some(match audit.side {
        ChSide::Buy => FokReplayRequest {
            side: audit.side,
            limit_price,
            buy_budget: Some(audit.total_cost_usd.map(ChUsd::to_usd)?),
            sell_shares: None,
        },
        ChSide::Sell => FokReplayRequest {
            side: audit.side,
            limit_price,
            buy_budget: None,
            sell_shares: Some(audit.requested_shares.map(ChShares::to_shares)?),
        },
    })
}

fn terminal_decision_view_for<'a>(
    book: &'a BookReconstructionArtifact,
    audit: &OpportunityAuditRow,
    decision_time: chrono::DateTime<Utc>,
) -> Option<&'a DecisionBookView> {
    book.decision_views.iter().find(|view| {
        view.market_id == audit.market_id
            && view.decision_time == decision_time
            && view.purpose == DecisionBookViewPurpose::TerminalExecution
    })
}

fn token_book_for_audit<'a>(
    decision_view: &'a DecisionBookView,
    audit: &OpportunityAuditRow,
) -> Option<&'a DecisionTokenBookView> {
    if decision_view
        .yes_book
        .as_ref()
        .is_some_and(|book| book.book.token_id == audit.token_id)
    {
        decision_view.yes_book.as_ref()
    } else if decision_view
        .no_book
        .as_ref()
        .is_some_and(|book| book.book.token_id == audit.token_id)
    {
        decision_view.no_book.as_ref()
    } else {
        None
    }
}

fn levels_for_side(token_book: &DecisionTokenBookView, side: ChSide) -> &[BookLevel] {
    match side {
        ChSide::Buy => &token_book.book.asks,
        ChSide::Sell => &token_book.book.bids,
    }
}

fn latency_shifted_fill(
    book: &BookReconstructionArtifact,
    audit: &OpportunityAuditRow,
    request: FokReplayRequest,
    simulation_config: &SimulationConfig,
) -> bool {
    if simulation_config.latency_buckets.is_empty() {
        return false;
    }
    simulation_config.latency_buckets.iter().all(|bucket| {
        let shifted_time = Utc
            .timestamp_millis_opt(
                audit
                    .stage_at
                    .saturating_add(i64::try_from(bucket.shift_ms).unwrap_or(i64::MAX)),
            )
            .single();
        let Some(shifted_time) = shifted_time else {
            return false;
        };
        let Some(token_book) = book.token_book_at(&audit.token_id, shifted_time) else {
            return false;
        };
        let levels = match audit.side {
            ChSide::Buy => token_book.asks.as_slice(),
            ChSide::Sell => token_book.bids.as_slice(),
        };
        replay_fok(levels, request).is_some_and(|result| result.strict_fill)
    })
}

fn adverse_selection_loss_bps(
    levels: &[BookLevel],
    request: FokReplayRequest,
    replay: FokReplayResult,
    simulation_config: &SimulationConfig,
) -> u64 {
    simulation_config
        .adverse_selection_bps
        .iter()
        .filter_map(|stress_bps| {
            let stressed_levels = stress_levels(levels, request.side, *stress_bps);
            let stressed = replay_fok(&stressed_levels, request)?;
            let loss = match request.side {
                ChSide::Buy => stressed.vwap.inner() - replay.vwap.inner(),
                ChSide::Sell => replay.vwap.inner() - stressed.vwap.inner(),
            };
            Some(decimal_bps(loss.max(Decimal::ZERO), replay.vwap.inner()))
        })
        .max()
        .unwrap_or(0)
}

fn terminal_audits(audits: &[OpportunityAuditRow]) -> Vec<OpportunityAuditRow> {
    let mut rows = audits
        .iter()
        .filter(|row| {
            matches!(
                row.stage,
                ChOpportunityAuditStage::Filled
                    | ChOpportunityAuditStage::Missed
                    | ChOpportunityAuditStage::Failed
            )
        })
        .cloned()
        .collect::<Vec<_>>();
    // Reduce to one most-terminal row per opportunity. This mirrors the
    // ClickHouse `ORDER BY opportunity_id ASC, stage_order DESC, …` + `LIMIT 1
    // BY opportunity_id` contract of `TimeseriesRepository::terminal_audits`,
    // so the in-Rust reduction and the SQL query agree exactly.
    //
    // `opportunity_id` is a *grouping* key here, not a display order: comparing
    // the UUID bytes (`as_uuid`) reproduces ClickHouse's `ORDER BY` on the
    // lowercase-hyphenated string column (lexicographic order of that form is
    // identical to binary UUID order), keeping `dedup_by` adjacency correct.
    rows.sort_by(|left, right| {
        left.opportunity_id
            .as_uuid()
            .cmp(&right.opportunity_id.as_uuid())
            .then(right.stage_order.cmp(&left.stage_order))
            .then(right.stage_at.cmp(&left.stage_at))
            .then(right.ingestion_time.cmp(&left.ingestion_time))
            .then(right.sequence.cmp(&left.sequence))
    });
    rows.dedup_by(|left, right| left.opportunity_id == right.opportunity_id);
    rows
}

fn percentile_metric(values: &[u64], pct: usize, code: &str, reason: &str) -> EvidenceMetric<u64> {
    percentile_u64(values, pct).map_or_else(
        || EvidenceMetric::Unavailable {
            code: code.to_owned(),
            reason: reason.to_owned(),
        },
        |value| EvidenceMetric::Available { value },
    )
}

fn percentile_u64(values: &[u64], pct: usize) -> Option<u64> {
    if values.is_empty() {
        return None;
    }
    let mut sorted = values.to_vec();
    sorted.sort_unstable();
    let idx = sorted
        .len()
        .saturating_sub(1)
        .saturating_mul(pct)
        .saturating_div(100);
    Some(sorted[idx])
}

fn price_to_bps(price: Decimal) -> u64 {
    decimal_bps(price, Decimal::ONE)
}

fn decimal_bps(numerator: Decimal, denominator: Decimal) -> u64 {
    if denominator <= Decimal::ZERO {
        return 0;
    }
    let value = (numerator / denominator * Decimal::from(10_000_u64)).round();
    value.try_into().unwrap_or(u64::MAX)
}

#[cfg(test)]
mod tests {
    use crate::evidence::{
        book::{
            BookEvidenceTier, BookReconstructionArtifact, BookReconstructionReport,
            DecisionBookView, DecisionBookViewPurpose, DecisionTokenBookView,
            MarketBookReconstruction, ReconstructedTokenBook, ReconstructedTokenBookTimeline,
        },
        execution::build,
    };
    use chrono::{TimeZone, Utc};
    use oxide_arb_models::{
        clickhouse::{ChPrice, ChSchemaVersion, ChShares, ChUsd, OpportunityAuditRow},
        domain::{
            BookLevel,
            control_factor::{EvidenceSourceBundle, SimulationConfig},
        },
        enums::clickhouse::{ChAuditOutcome, ChOpportunityAuditStage, ChSide},
        types::{
            EventId, ExecutionId, MarketId, OpportunityId, Price, Shares, TokenId, TradeId, Usd,
        },
    };
    use rust_decimal_macros::dec;

    #[test]
    fn execution_metrics_are_available_for_terminal_audit() {
        let artifact = build(
            &book(),
            &[audit()],
            Vec::new(),
            &SimulationConfig::production_default(),
        );

        assert_eq!(artifact.report.true_fill_count, 1);
        assert!(artifact.report.simulated_vwap_p50_bps.is_available());
        assert!(artifact.report.realized_slippage_p50_bps.is_available());
        assert!(artifact.report.depth_consumed_pct_p50_bps.is_available());
        assert!(artifact.report.latency_shifted_miss_rate_bps.is_available());
        assert!(
            artifact
                .report
                .adverse_selection_loss_p95_bps
                .is_available()
        );
    }

    fn book() -> BookReconstructionArtifact {
        let market_id = MarketId::new("market");
        let yes = TokenId::new("yes");
        let no = TokenId::new("no");
        let event_time = Utc.timestamp_millis_opt(1_000).single().expect("time");
        let yes_book = ReconstructedTokenBook {
            token_id: yes.clone(),
            bids: Vec::new(),
            asks: vec![level(dec!(0.94), dec!(20))],
            event_time,
            book_version: 1,
            source_event_count: 1,
            invalid_level_count: 0,
            crossed: false,
            max_gap_ms: 0,
            stale_interval_ms: 0,
        };
        let no_book = ReconstructedTokenBook {
            token_id: no.clone(),
            bids: Vec::new(),
            asks: vec![level(dec!(0.06), dec!(20))],
            event_time,
            book_version: 1,
            source_event_count: 1,
            invalid_level_count: 0,
            crossed: false,
            max_gap_ms: 0,
            stale_interval_ms: 0,
        };
        let event_time = Utc.timestamp_millis_opt(1_000).single().expect("time");
        BookReconstructionArtifact {
            report: BookReconstructionReport {
                evidence_tier: BookEvidenceTier::ExactReplay,
                token_count_expected: 2,
                token_count_reconstructed: 2,
                l2_event_count: 0,
                snapshot_bootstrap_count: 2,
                gap_count: 0,
                max_gap_ms: 0,
                median_book_age_ms: 0,
                p95_book_age_ms: 0,
                crossed_book_count: 0,
                invalid_level_count: 0,
                stale_interval_ms: 0,
                insufficient_reasons: Vec::new(),
                query_fingerprints: Vec::new(),
            },
            market_books: vec![MarketBookReconstruction {
                market_id,
                yes_token_id: yes.clone(),
                no_token_id: no.clone(),
                settlement_deadline: Some(event_time),
                yes_book: Some(yes_book.clone()),
                no_book: Some(no_book.clone()),
            }],
            token_timelines: vec![
                ReconstructedTokenBookTimeline {
                    token_id: yes,
                    books: vec![yes_book.clone()],
                },
                ReconstructedTokenBookTimeline {
                    token_id: no,
                    books: vec![no_book.clone()],
                },
            ],
            decision_views: vec![DecisionBookView {
                market_id: MarketId::new("market"),
                decision_time: event_time,
                purpose: DecisionBookViewPurpose::TerminalExecution,
                yes_book: Some(DecisionTokenBookView {
                    book: yes_book,
                    book_age_ms: 0,
                    max_gap_ms: 0,
                    stale: false,
                    crossed: false,
                    invalid_level_count: 0,
                }),
                no_book: Some(DecisionTokenBookView {
                    book: no_book,
                    book_age_ms: 0,
                    max_gap_ms: 0,
                    stale: false,
                    crossed: false,
                    invalid_level_count: 0,
                }),
                production_eligible: true,
                insufficient_reasons: Vec::new(),
                query_fingerprints: Vec::new(),
            }],
            source_bundle: EvidenceSourceBundle::empty(),
        }
    }

    fn audit() -> OpportunityAuditRow {
        OpportunityAuditRow {
            opportunity_id: OpportunityId::new(oxide_arb_test_support::seeded_uuid("opp")),
            execution_id: ExecutionId::from_v7(),
            trade_id: Some(TradeId::from_v7()),
            market_id: MarketId::new("market"),
            event_id: EventId::new("event"),
            token_id: TokenId::new("yes"),
            side: ChSide::Buy,
            entry_price: Some(ChPrice::from(Price::new(dec!(0.95)))),
            fill_price: Some(ChPrice::from(Price::new(dec!(0.94)))),
            requested_shares: Some(ChShares::from(Shares::new(dec!(10)))),
            filled_shares: Some(ChShares::from(Shares::new(dec!(10)))),
            total_cost_usd: Some(ChUsd::from(Usd::new(dec!(9.4)))),
            fees_usd: Some(ChUsd::from(Usd::ZERO)),
            net_profit_usd: None,
            expected_profit_usd: None,
            edge_bps: None,
            resolution_prob: None,
            confidence: None,
            fill_probability: None,
            convergence_secs: None,
            price_zone: None,
            duration_bucket: None,
            depth_used_pct: None,
            staleness: None,
            category: None,
            stage: ChOpportunityAuditStage::Filled,
            stage_order: 70,
            stage_at: 1_000,
            payout_usd: None,
            realized_pnl_usd: None,
            settlement_status: None,
            settlement_trigger: None,
            winning_token_id: None,
            accounting_status: None,
            fee_source: None,
            redeem_route: None,
            redeem_resolution: None,
            outcome: Some(ChAuditOutcome::Success),
            rejection_stage: None,
            rejection_reason: None,
            scored_snapshot_json: Some("{}".to_owned()),
            book_context_json: None,
            applied_factor_ids_json: None,
            missing_fields_json: None,
            detected_at: 1_000,
            ingestion_time: 1_000,
            sequence: 1,
            schema_version: ChSchemaVersion(2),
            updated_at: 1_000,
        }
    }

    fn level(price: rust_decimal::Decimal, size: rust_decimal::Decimal) -> BookLevel {
        BookLevel::try_from_decimal(Price::new(price), Shares::new(size)).expect("valid level")
    }
}
