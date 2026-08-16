//! Repeated p99 gate for the 2K-market report-funnel materialization kernel.

use std::{collections::HashMap, error::Error, time::Instant};

use chrono::{DateTime, Utc};
use quant_pivot_bench::{self as _, peak_rss_bytes};
use quant_pivot_core::{
    report::funnel::{PublishedRecommendationRef, ReportFunnelInput, build_report_market_funnel},
    service::model_runner::ModelMarketDecision,
};
use quant_pivot_models::{
    domain::quant::{NewReportRouteRun, RouteCandidateFunnel, RouteRunOutcome},
    enums::common::MarketCategory,
    runtime_config::BuyModelRoute,
    types::{
        ContentHash, DecisionPolicySnapshotId, EventId, FeatureVectorId, MarketId,
        MarketSelectionId, RecommendationId, RecommendationReportId, ReportRouteRunId, ReportRunId,
        SelectionExclusionSummary, SelectorHashEvidence, SignalCandidateId, TokenId,
    },
};
use quant_pivot_research::selection::{MarketSelectionSnapshot, SelectedMarket};
use uuid::Uuid;

const MARKET_COUNT: usize = 2_000;
const WARMUP_SAMPLES: usize = 5;
const MEASURED_SAMPLES: usize = 100;
const MAX_P99_SECONDS: f64 = 2.0;
const TOP_N: usize = 100;

fn selection(
    decision_at: DateTime<Utc>,
    decision_policy_snapshot_id: DecisionPolicySnapshotId,
) -> MarketSelectionSnapshot {
    let included = (0..MARKET_COUNT)
        .map(|index| SelectedMarket {
            market_id: MarketId::new(format!("0x{index:040x}")),
            event_id: EventId::new(format!("event-{index}")),
            category: MarketCategory::Other,
            primary_token_id: TokenId::new(format!("token-{index}")),
            secondary_token_id: None,
            liquidity_usd: None,
            volume_24h_usd: None,
            source_refs: Vec::new(),
        })
        .collect();
    let selector_hash = ContentHash::from_bytes([1; 32]);
    MarketSelectionSnapshot {
        market_selection_id: MarketSelectionId::from_v7(),
        decision_at,
        decision_policy_snapshot_id,
        selector_hash,
        selector_evidence: SelectorHashEvidence {
            selector_hash,
            contract_hash: ContentHash::from_bytes([2; 32]),
            boundary_hash: ContentHash::from_bytes([3; 32]),
            selection_policy_hash: ContentHash::from_bytes([4; 32]),
            data_quality_policy_hash: ContentHash::from_bytes([5; 32]),
            feature_schema_hash: ContentHash::from_bytes([6; 32]),
            model_requirements_hash: ContentHash::from_bytes([7; 32]),
            candidates_hash: ContentHash::from_bytes([8; 32]),
            candidate_catalog_hash: ContentHash::from_bytes([9; 32]),
            candidate_book_hash: ContentHash::from_bytes([10; 32]),
            candidate_domain_hash: ContentHash::from_bytes([11; 32]),
            candidate_decision_hash: ContentHash::from_bytes([12; 32]),
            included_hash: ContentHash::from_bytes([13; 32]),
            excluded_hash: ContentHash::from_bytes([14; 32]),
            exclusion_summary_hash: ContentHash::from_bytes([15; 32]),
        },
        included,
        excluded: Vec::new(),
        exclusion_summary: SelectionExclusionSummary::default(),
    }
}

fn percentile_99(samples: &mut [f64]) -> f64 {
    samples.sort_unstable_by(f64::total_cmp);
    let rank = samples.len().saturating_mul(99).div_ceil(100);
    samples[rank.saturating_sub(1)]
}

fn full_compute_inputs(
    selection: &MarketSelectionSnapshot,
) -> (
    HashMap<MarketId, FeatureVectorId>,
    Vec<ModelMarketDecision>,
    Vec<PublishedRecommendationRef>,
) {
    let mut feature_vectors = HashMap::with_capacity(MARKET_COUNT);
    let mut model_decisions = Vec::with_capacity(MARKET_COUNT);
    let mut recommendations = Vec::with_capacity(TOP_N);
    for (index, market) in selection.included.iter().enumerate() {
        feature_vectors.insert(market.market_id.clone(), FeatureVectorId::from_v7());
        let signal_candidate_id = SignalCandidateId::from_v7();
        model_decisions.push(ModelMarketDecision {
            signal_candidate_id,
            market_id: market.market_id.clone(),
            token_id: market.primary_token_id.clone(),
            gate_passed: true,
            primary_reason: None,
        });
        if index < TOP_N {
            recommendations.push(PublishedRecommendationRef {
                recommendation_id: RecommendationId::from_v7(),
                market_id: market.market_id.clone(),
                report_route_run_id: ReportRouteRunId::new(Uuid::nil()),
                route: BuyModelRoute::Pooled,
            });
        }
    }
    (feature_vectors, model_decisions, recommendations)
}

fn main() -> Result<(), Box<dyn Error>> {
    let decision_at = DateTime::<Utc>::UNIX_EPOCH;
    let decision_policy_snapshot_id = DecisionPolicySnapshotId::from_v7();
    let selection = selection(decision_at, decision_policy_snapshot_id);
    let report_id = RecommendationReportId::from_v7();
    let report_route_run_id = ReportRouteRunId::new(Uuid::nil());
    let market_count = u32::try_from(MARKET_COUNT)?;
    let selected_count = u32::try_from(TOP_N)?;
    let route_runs = [NewReportRouteRun {
        report_route_run_id,
        report_run_id: ReportRunId::from_v7(),
        route: BuyModelRoute::Pooled,
        outcome: RouteRunOutcome::Ready,
        model_version_id: None,
        model_run_id: None,
        calibration_artifact_id: None,
        trade_policy_artifact_id: None,
        research_profile_artifact_id: None,
        lineage_json: None,
        funnel_json: RouteCandidateFunnel {
            eligible_markets: market_count,
            feature_complete_markets: market_count,
            calibrated_candidates: market_count,
            admitted_economic_tiers: 0,
            selected_recommendations: selected_count,
        },
        diagnostic_code: None,
        finished_at: decision_at,
    }];
    let (feature_vectors, model_decisions, recommendations) = full_compute_inputs(&selection);
    let funnel_input = || ReportFunnelInput {
        report_id: &report_id,
        decision_policy_snapshot_id: &decision_policy_snapshot_id,
        selection: &selection,
        route_runs: &route_runs,
        feature_rejected: &[],
        feature_vector_by_market: &feature_vectors,
        model_decisions: &model_decisions,
        tiers: &[],
        tier_rejections: &[],
        tier_build_rejections: &[],
        recommendations: &recommendations,
        event_time: decision_at,
    };

    for _ in 0..WARMUP_SAMPLES {
        let rows = build_report_market_funnel(funnel_input())?;
        if rows.len() != MARKET_COUNT {
            return Err("report compute warmup row count mismatch".into());
        }
    }
    let mut samples = Vec::with_capacity(MEASURED_SAMPLES);
    for _ in 0..MEASURED_SAMPLES {
        let started = Instant::now();
        let rows = build_report_market_funnel(funnel_input())?;
        let elapsed = started.elapsed().as_secs_f64();
        if rows.len() != MARKET_COUNT
            || rows
                .iter()
                .filter(|row| row.recommendation_id.is_some())
                .count()
                != TOP_N
            || rows
                .iter()
                .any(|row| row.feature_vector_id.is_none() || row.signal_candidate_id.is_none())
            || !rows
                .windows(2)
                .all(|window| window[0].market_id.as_str() < window[1].market_id.as_str())
        {
            return Err("report compute output shape/order mismatch".into());
        }
        samples.push(elapsed);
    }
    let p99 = percentile_99(&mut samples);
    let median = samples[samples.len() / 2];
    let peak_rss = peak_rss_bytes()?;
    let peak_rss_label = peak_rss.map_or_else(|| "unavailable".to_owned(), |rss| rss.to_string());
    println!(
        "report_funnel_gate path=report_funnel_materialization markets={MARKET_COUNT} top_n={TOP_N} samples={MEASURED_SAMPLES} median_seconds={median:.6} p99_seconds={p99:.6} peak_rss_bytes={peak_rss_label}"
    );
    if p99 > MAX_P99_SECONDS {
        return Err(
            format!("report pure-compute p99 exceeded {MAX_P99_SECONDS}s: {p99:.6}s").into(),
        );
    }
    Ok(())
}
