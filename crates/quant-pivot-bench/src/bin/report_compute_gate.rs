//! Repeated p99 gate for the 2K-market full-path report funnel kernel.

use std::{collections::HashMap, error::Error, time::Instant};

use chrono::{DateTime, Utc};
use quant_pivot_bench::{self as _, peak_rss_bytes};
use quant_pivot_core::{
    report::funnel::{PublishedRecommendationRef, ReportFunnelInput, build_report_market_funnel},
    service::model_runner::ModelMarketDecision,
};
use quant_pivot_models::{
    enums::{common::MarketCategory, quant::RejectionReason},
    types::{
        ContentHash, DecisionPolicySnapshotId, EventId, FeatureVectorId, MarketId,
        MarketSelectionId, ModelRunId, ModelVersionId, RecommendationId, RecommendationReportId,
        ResearchProfileId, ResearchProfileRef, SelectionExclusionSummary, SignalCandidateId,
        TokenId,
    },
};
use quant_pivot_research::{
    portfolio::RejectedCandidate,
    selection::{MarketSelectionSnapshot, SelectedMarket},
};

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
    MarketSelectionSnapshot {
        market_selection_id: MarketSelectionId::from_v7(),
        decision_at,
        decision_policy_snapshot_id,
        selector_hash: ContentHash::from_bytes([1; 32]),
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
    Vec<RejectedCandidate>,
    Vec<PublishedRecommendationRef>,
) {
    let mut feature_vectors = HashMap::with_capacity(MARKET_COUNT);
    let mut model_decisions = Vec::with_capacity(MARKET_COUNT);
    let mut planner_rejected = Vec::with_capacity(MARKET_COUNT - TOP_N);
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
            });
        } else {
            planner_rejected.push(RejectedCandidate {
                signal_candidate_id,
                market_id: market.market_id.clone(),
                reason: RejectionReason::BeyondTopN,
                detail: "ranked beyond governed TopN".to_owned(),
            });
        }
    }
    (
        feature_vectors,
        model_decisions,
        planner_rejected,
        recommendations,
    )
}

fn main() -> Result<(), Box<dyn Error>> {
    let decision_at = DateTime::<Utc>::UNIX_EPOCH;
    let decision_policy_snapshot_id = DecisionPolicySnapshotId::from_v7();
    let selection = selection(decision_at, decision_policy_snapshot_id);
    let profile_ref = ResearchProfileRef {
        id: ResearchProfileId::new("report_compute_gate"),
        version: 1,
        content_hash: ContentHash::from_bytes([2; 32]),
    };
    let report_id = RecommendationReportId::from_v7();
    let model_version_id = ModelVersionId::from_v7();
    let model_run_id = ModelRunId::from_v7();
    let (feature_vectors, model_decisions, planner_rejected, recommendations) =
        full_compute_inputs(&selection);
    let input = ReportFunnelInput {
        report_id: &report_id,
        profile_ref: &profile_ref,
        decision_policy_snapshot_id: &decision_policy_snapshot_id,
        model_version_id: &model_version_id,
        model_run_id: Some(&model_run_id),
        selection: &selection,
        feature_rejected: &[],
        feature_vector_by_market: &feature_vectors,
        model_decisions: &model_decisions,
        planner_rejected: &planner_rejected,
        recommendations: &recommendations,
        early_terminal: None,
        event_time: decision_at,
    };

    for _ in 0..WARMUP_SAMPLES {
        let rows = build_report_market_funnel(input)?;
        if rows.len() != MARKET_COUNT {
            return Err("report compute warmup row count mismatch".into());
        }
    }
    let mut samples = Vec::with_capacity(MEASURED_SAMPLES);
    for _ in 0..MEASURED_SAMPLES {
        let started = Instant::now();
        let rows = build_report_market_funnel(input)?;
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
        "report_compute_gate path=feature+model+planner+optimizer+topn markets={MARKET_COUNT} top_n={TOP_N} samples={MEASURED_SAMPLES} median_seconds={median:.6} p99_seconds={p99:.6} peak_rss_bytes={peak_rss_label}"
    );
    if p99 > MAX_P99_SECONDS {
        return Err(
            format!("report pure-compute p99 exceeded {MAX_P99_SECONDS}s: {p99:.6}s").into(),
        );
    }
    Ok(())
}
