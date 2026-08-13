//! Conserved, report-scoped market funnel for global portfolio reports.

use std::{
    collections::{HashMap, HashSet},
    hash::Hash,
};

use chrono::{DateTime, Utc};
use quant_pivot_error::{QuantResult, report::ReportError};
use quant_pivot_models::{
    clickhouse::ReportMarketFunnelRow,
    domain::quant::{ExecutableEconomicTier, NewReportRouteRun},
    runtime_config::BuyModelRoute,
    types::{
        DecisionPolicySnapshotId, EconomicTierId, EventId, FeatureVectorId, MarketId,
        MissingFeatureDiagnostic, ModelRunId, ModelVersionId, RecommendationId,
        RecommendationReportId, ReportFunnelDiagnostics, ReportFunnelReason, ReportFunnelStage,
        ReportRouteRunId, SignalCandidateId, TokenId,
    },
};
use quant_pivot_research::{
    portfolio::TierAdmissionRejectionCode,
    selection::{ExcludedMarket, ExclusionReason, MarketSelectionSnapshot, SelectedMarket},
};

use super::types::ReportTierRejection;
use crate::service::{feature_pipeline::RejectedMarket, model_runner::ModelMarketDecision};

/// Published recommendation identity used to close a market funnel row.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PublishedRecommendationRef {
    pub recommendation_id: RecommendationId,
    pub market_id: MarketId,
    pub report_route_run_id: ReportRouteRunId,
    pub route: BuyModelRoute,
}

/// Frozen inputs for the complete catalog-visible market funnel.
#[derive(Clone, Copy)]
pub struct ReportFunnelInput<'a> {
    pub report_id: &'a RecommendationReportId,
    pub decision_policy_snapshot_id: &'a DecisionPolicySnapshotId,
    pub selection: &'a MarketSelectionSnapshot,
    pub route_runs: &'a [NewReportRouteRun],
    pub feature_rejected: &'a [RejectedMarket],
    pub feature_vector_by_market: &'a HashMap<MarketId, FeatureVectorId>,
    pub model_decisions: &'a [ModelMarketDecision],
    pub tiers: &'a [ExecutableEconomicTier],
    pub tier_rejections: &'a [ReportTierRejection],
    pub recommendations: &'a [PublishedRecommendationRef],
    pub event_time: DateTime<Utc>,
}

/// Build exactly one terminal row for every catalog-visible market in the
/// persisted selection snapshot. Every row is sealed before it leaves Core.
pub fn build_report_market_funnel(
    input: ReportFunnelInput<'_>,
) -> QuantResult<Vec<ReportMarketFunnelRow>> {
    validate_unique_inputs(&input)?;
    let route_runs = input
        .route_runs
        .iter()
        .map(|run| (run.route, run))
        .collect::<HashMap<_, _>>();
    let included = input
        .selection
        .included
        .iter()
        .map(|market| (market.market_id.clone(), market))
        .collect::<HashMap<_, _>>();
    let rejected_features = input
        .feature_rejected
        .iter()
        .map(|market| (market.market_id.clone(), market))
        .collect::<HashMap<_, _>>();
    let decisions = input
        .model_decisions
        .iter()
        .map(|decision| (decision.market_id.clone(), decision))
        .collect::<HashMap<_, _>>();
    let recommendations = input
        .recommendations
        .iter()
        .map(|recommendation| (recommendation.market_id.clone(), recommendation))
        .collect::<HashMap<_, _>>();
    let tiers_by_market = group_tiers(input.tiers);
    let rejected_tiers = input
        .tier_rejections
        .iter()
        .map(|rejection| (rejection.economic_tier_id, rejection.code))
        .collect::<HashMap<_, _>>();

    let mut rows =
        Vec::with_capacity(input.selection.included.len() + input.selection.excluded.len());
    for excluded in &input.selection.excluded {
        rows.push(excluded_row(&input, excluded)?);
    }
    for market in &input.selection.included {
        let route = BuyModelRoute::from(market.category);
        let route_run =
            route_runs
                .get(&route)
                .copied()
                .ok_or_else(|| ReportError::InvariantViolation {
                    stage: "report_funnel",
                    detail: format!("included market {} has no Route run", market.market_id),
                })?;
        let state = IncludedFunnelState {
            input: &input,
            market,
            route_run,
            rejected_feature: rejected_features.get(&market.market_id).copied(),
            feature_vector_id: input
                .feature_vector_by_market
                .get(&market.market_id)
                .copied(),
            model_decision: decisions.get(&market.market_id).copied(),
            tiers: tiers_by_market
                .get(&market.market_id)
                .map_or(&[][..], Vec::as_slice),
            rejected_tiers: &rejected_tiers,
            recommendation: recommendations.get(&market.market_id).copied(),
        };
        rows.push(included_row(state)?);
    }
    rows.sort_by(|left, right| left.market_id.as_str().cmp(right.market_id.as_str()));
    if rows.len() != included.len() + input.selection.excluded.len() {
        return Err(ReportError::InvariantViolation {
            stage: "report_funnel",
            detail: "catalog-visible funnel conservation failed".to_owned(),
        }
        .into());
    }
    Ok(rows)
}

#[derive(Clone, Copy)]
struct IncludedFunnelState<'a> {
    input: &'a ReportFunnelInput<'a>,
    market: &'a SelectedMarket,
    route_run: &'a NewReportRouteRun,
    rejected_feature: Option<&'a RejectedMarket>,
    feature_vector_id: Option<FeatureVectorId>,
    model_decision: Option<&'a ModelMarketDecision>,
    tiers: &'a [&'a ExecutableEconomicTier],
    rejected_tiers: &'a HashMap<EconomicTierId, TierAdmissionRejectionCode>,
    recommendation: Option<&'a PublishedRecommendationRef>,
}

fn included_row(state: IncludedFunnelState<'_>) -> QuantResult<ReportMarketFunnelRow> {
    let lineage = state.route_run.lineage_json.as_ref();
    let route = BuyModelRoute::from(state.market.category);
    if state.route_run.route != route {
        return Err(ReportError::InvariantViolation {
            stage: "report_funnel",
            detail: format!(
                "market {} Route does not match its Route run",
                state.market.market_id
            ),
        }
        .into());
    }

    let mut signal_candidate_id = state
        .model_decision
        .map(|decision| decision.signal_candidate_id);
    let (terminal_stage, primary_reason, diagnostics, recommendation_id) =
        if let Some(published) = state.recommendation {
            if published.route != route
                || published.report_route_run_id != state.route_run.report_route_run_id
            {
                return Err(ReportError::InvariantViolation {
                    stage: "report_funnel",
                    detail: format!(
                        "published market {} disagrees with Route lineage",
                        state.market.market_id
                    ),
                }
                .into());
            }
            (
                ReportFunnelStage::Published,
                ReportFunnelReason::Published,
                ReportFunnelDiagnostics::None {},
                Some(published.recommendation_id),
            )
        } else if let Some(rejected) = state.rejected_feature {
            (
                ReportFunnelStage::FeatureReady,
                ReportFunnelReason::FeatureDataQualityRejected,
                ReportFunnelDiagnostics::FeatureDataQuality {
                    missing: rejected
                        .missing_required
                        .iter()
                        .map(|(feature_name, reason)| MissingFeatureDiagnostic {
                            feature_name: feature_name.clone(),
                            reason: *reason,
                        })
                        .collect(),
                },
                None,
            )
        } else if state.feature_vector_id.is_none() {
            return Err(ReportError::InvariantViolation {
                stage: "report_funnel",
                detail: format!(
                    "market {} has neither a feature vector nor typed rejection evidence",
                    state.market.market_id
                ),
            }
            .into());
        } else if let Some(decision) = state
            .model_decision
            .filter(|decision| !decision.gate_passed)
        {
            (
                ReportFunnelStage::ModelGatePassed,
                model_gate_reason(decision.primary_reason.as_deref()),
                ReportFunnelDiagnostics::None {},
                None,
            )
        } else if state.model_decision.is_none() {
            signal_candidate_id = None;
            (
                ReportFunnelStage::ModelScored,
                ReportFunnelReason::MissingModelOutput,
                ReportFunnelDiagnostics::None {},
                None,
            )
        } else if state.tiers.is_empty() {
            (
                ReportFunnelStage::PolicyReady,
                ReportFunnelReason::ExecutableEntryUnavailable,
                ReportFunnelDiagnostics::None {},
                None,
            )
        } else {
            let admitted = state
                .tiers
                .iter()
                .filter(|tier| !state.rejected_tiers.contains_key(&tier.economic_tier_id))
                .count();
            if admitted > 0 {
                (
                    ReportFunnelStage::PortfolioFunded,
                    ReportFunnelReason::NotSelectedByGlobalOptimum,
                    ReportFunnelDiagnostics::PlannerRejection {
                        detail: "admitted executable tier was not selected by the global optimum"
                            .to_owned(),
                    },
                    None,
                )
            } else {
                let code = state
                    .tiers
                    .iter()
                    .filter_map(|tier| state.rejected_tiers.get(&tier.economic_tier_id).copied())
                    .min_by_key(|code| tier_rejection_rank(*code))
                    .ok_or_else(|| ReportError::InvariantViolation {
                        stage: "report_funnel",
                        detail: format!(
                            "market {} has tiers but neither admission nor rejection evidence",
                            state.market.market_id
                        ),
                    })?;
                let reason = tier_rejection_reason(code);
                (
                    ReportFunnelStage::SizingEligible,
                    reason,
                    rejection_diagnostics(reason, code),
                    None,
                )
            }
        };

    sealed_row(RowInput {
        report: state.input,
        market_id: state.market.market_id.clone(),
        event_id: state.market.event_id.clone(),
        primary_token_id: state.market.primary_token_id.clone(),
        route_run: Some(state.route_run),
        model_version_id: lineage.map(|lineage| lineage.model_version_id),
        model_run_id: lineage.and_then(|lineage| lineage.model_run_id),
        terminal_stage,
        primary_reason,
        diagnostics,
        feature_vector_id: state.feature_vector_id,
        signal_candidate_id,
        recommendation_id,
    })
}

fn excluded_row(
    input: &ReportFunnelInput<'_>,
    market: &ExcludedMarket,
) -> QuantResult<ReportMarketFunnelRow> {
    let (stage, reason, diagnostics) = exclusion_terminal(&market.reason);
    sealed_row(RowInput {
        report: input,
        market_id: market.market_id.clone(),
        event_id: market.event_id.clone(),
        primary_token_id: market.primary_token_id.clone(),
        route_run: None,
        model_version_id: None,
        model_run_id: None,
        terminal_stage: stage,
        primary_reason: reason,
        diagnostics,
        feature_vector_id: None,
        signal_candidate_id: None,
        recommendation_id: None,
    })
}

struct RowInput<'a> {
    report: &'a ReportFunnelInput<'a>,
    market_id: MarketId,
    event_id: EventId,
    primary_token_id: TokenId,
    route_run: Option<&'a NewReportRouteRun>,
    model_version_id: Option<ModelVersionId>,
    model_run_id: Option<ModelRunId>,
    terminal_stage: ReportFunnelStage,
    primary_reason: ReportFunnelReason,
    diagnostics: ReportFunnelDiagnostics,
    feature_vector_id: Option<FeatureVectorId>,
    signal_candidate_id: Option<SignalCandidateId>,
    recommendation_id: Option<RecommendationId>,
}

fn sealed_row(input: RowInput<'_>) -> QuantResult<ReportMarketFunnelRow> {
    let diagnostics = serde_json::to_string(&input.diagnostics).map_err(|error| {
        ReportError::InvariantViolation {
            stage: "report_funnel",
            detail: format!("encode canonical diagnostics: {error}"),
        }
    })?;
    let mut row = ReportMarketFunnelRow {
        event_time: input.report.event_time.timestamp_millis(),
        recommendation_report_id: *input.report.report_id,
        market_selection_id: input.report.selection.market_selection_id,
        decision_policy_snapshot_id: *input.report.decision_policy_snapshot_id,
        report_route_run_id: input.route_run.map(|run| run.report_route_run_id),
        route: input.route_run.map(|run| run.route.as_str().to_owned()),
        model_version_id: input.model_version_id,
        model_run_id: input.model_run_id,
        market_id: input.market_id,
        event_id: input.event_id,
        primary_token_id: input.primary_token_id,
        terminal_stage: input.terminal_stage.as_str().to_owned(),
        primary_reason: input.primary_reason.as_str().to_owned(),
        secondary_diagnostics_json: diagnostics,
        feature_vector_id: input.feature_vector_id,
        signal_candidate_id: input.signal_candidate_id,
        recommendation_id: input.recommendation_id,
        row_hash: String::new(),
        ingestion_time: input.report.event_time.timestamp_millis(),
    };
    row.seal_hash()
        .map_err(|error| ReportError::InvariantViolation {
            stage: "report_funnel",
            detail: error.to_string(),
        })?;
    Ok(row)
}

fn validate_unique_inputs(input: &ReportFunnelInput<'_>) -> QuantResult<()> {
    ensure_unique(
        input
            .selection
            .included
            .iter()
            .map(|market| &market.market_id),
        "included market",
    )?;
    ensure_unique(
        input
            .selection
            .excluded
            .iter()
            .map(|market| &market.market_id),
        "excluded market",
    )?;
    let included = input
        .selection
        .included
        .iter()
        .map(|market| &market.market_id)
        .collect::<HashSet<_>>();
    if input
        .selection
        .excluded
        .iter()
        .any(|market| included.contains(&market.market_id))
    {
        return Err(ReportError::InvariantViolation {
            stage: "report_funnel",
            detail: "selection includes and excludes the same market".to_owned(),
        }
        .into());
    }
    ensure_unique(input.route_runs.iter().map(|run| run.route), "Route run")?;
    ensure_unique(
        input
            .model_decisions
            .iter()
            .map(|decision| &decision.market_id),
        "model decision",
    )?;
    ensure_unique(
        input.recommendations.iter().map(|item| &item.market_id),
        "published recommendation market",
    )?;
    ensure_unique(
        input
            .tier_rejections
            .iter()
            .map(|rejection| rejection.economic_tier_id),
        "tier rejection",
    )
}

fn ensure_unique<T>(items: impl IntoIterator<Item = T>, label: &'static str) -> QuantResult<()>
where
    T: Eq + Hash,
{
    let mut seen = HashSet::new();
    if items.into_iter().any(|item| !seen.insert(item)) {
        return Err(ReportError::InvariantViolation {
            stage: "report_funnel",
            detail: format!("duplicate {label} identity"),
        }
        .into());
    }
    Ok(())
}

fn group_tiers(
    tiers: &[ExecutableEconomicTier],
) -> HashMap<MarketId, Vec<&ExecutableEconomicTier>> {
    let mut grouped = HashMap::<MarketId, Vec<&ExecutableEconomicTier>>::new();
    for tier in tiers {
        grouped
            .entry(tier.market_id.clone())
            .or_default()
            .push(tier);
    }
    grouped
}

fn exclusion_terminal(
    reason: &ExclusionReason,
) -> (
    ReportFunnelStage,
    ReportFunnelReason,
    ReportFunnelDiagnostics,
) {
    match reason {
        ExclusionReason::NotOpen => terminal(
            ReportFunnelStage::BusinessEligible,
            ReportFunnelReason::NotOpen,
        ),
        ExclusionReason::CategoryDisabled => terminal(
            ReportFunnelStage::BusinessEligible,
            ReportFunnelReason::CategoryDisabled,
        ),
        ExclusionReason::InsufficientLiquidity => terminal(
            ReportFunnelStage::ExecutableDataEligible,
            ReportFunnelReason::InsufficientLiquidity,
        ),
        ExclusionReason::SpreadTooWide => terminal(
            ReportFunnelStage::ExecutableDataEligible,
            ReportFunnelReason::SpreadTooWide,
        ),
        ExclusionReason::StaleBook => terminal(
            ReportFunnelStage::ExecutableDataEligible,
            ReportFunnelReason::StaleBook,
        ),
        ExclusionReason::IngestLagExceeded => terminal(
            ReportFunnelStage::ExecutableDataEligible,
            ReportFunnelReason::IngestLagExceeded,
        ),
        ExclusionReason::ResolutionAmbiguous => terminal(
            ReportFunnelStage::BusinessEligible,
            ReportFunnelReason::ResolutionAmbiguous,
        ),
        ExclusionReason::ManuallyBlocked => terminal(
            ReportFunnelStage::BusinessEligible,
            ReportFunnelReason::ManuallyBlocked,
        ),
        ExclusionReason::ModelFeatureUnavailable { missing } => (
            ReportFunnelStage::FeatureReady,
            ReportFunnelReason::ModelFeatureUnavailable,
            ReportFunnelDiagnostics::MissingModelFeatures {
                features: missing.clone(),
            },
        ),
    }
}

const fn terminal(
    stage: ReportFunnelStage,
    reason: ReportFunnelReason,
) -> (
    ReportFunnelStage,
    ReportFunnelReason,
    ReportFunnelDiagnostics,
) {
    (stage, reason, ReportFunnelDiagnostics::None {})
}

fn model_gate_reason(reason: Option<&str>) -> ReportFunnelReason {
    match reason {
        Some("score_below_floor") => ReportFunnelReason::ScoreBelowFloor,
        Some("low_confidence") => ReportFunnelReason::LowConfidence,
        _ => ReportFunnelReason::NoPositiveSignal,
    }
}

const fn tier_rejection_rank(code: TierAdmissionRejectionCode) -> u8 {
    match code {
        TierAdmissionRejectionCode::ScenarioExitCapacity => 0,
        TierAdmissionRejectionCode::NominalExpectedNetFloor => 1,
        TierAdmissionRejectionCode::RobustExpectedNetFloor => 2,
        TierAdmissionRejectionCode::ProfitProbabilityFloor => 3,
        TierAdmissionRejectionCode::ProbabilityIntervalWidth => 4,
        TierAdmissionRejectionCode::LiquidityBuffer => 5,
        TierAdmissionRejectionCode::SingleRecommendationExposure => 6,
        TierAdmissionRejectionCode::ExistingStructuralConflict => 7,
    }
}

const fn tier_rejection_reason(code: TierAdmissionRejectionCode) -> ReportFunnelReason {
    match code {
        TierAdmissionRejectionCode::ScenarioExitCapacity => {
            ReportFunnelReason::ScenarioExitCapacityInsufficient
        }
        TierAdmissionRejectionCode::NominalExpectedNetFloor => {
            ReportFunnelReason::NominalExpectedNetBelowFloor
        }
        TierAdmissionRejectionCode::RobustExpectedNetFloor => {
            ReportFunnelReason::RobustExpectedNetBelowFloor
        }
        TierAdmissionRejectionCode::ProfitProbabilityFloor => {
            ReportFunnelReason::ProfitProbabilityBelowFloor
        }
        TierAdmissionRejectionCode::ProbabilityIntervalWidth => {
            ReportFunnelReason::ProbabilityIntervalTooWide
        }
        TierAdmissionRejectionCode::LiquidityBuffer => {
            ReportFunnelReason::LiquidityBufferInsufficient
        }
        TierAdmissionRejectionCode::SingleRecommendationExposure => {
            ReportFunnelReason::SingleRecommendationExposureExceeded
        }
        TierAdmissionRejectionCode::ExistingStructuralConflict => {
            ReportFunnelReason::ExistingStructuralConflict
        }
    }
}

fn rejection_diagnostics(
    reason: ReportFunnelReason,
    code: TierAdmissionRejectionCode,
) -> ReportFunnelDiagnostics {
    ReportFunnelDiagnostics::PlannerRejection {
        detail: format!(
            "global tier admission rejected with {} ({code:?})",
            reason.as_str()
        ),
    }
}

#[cfg(test)]
mod tests {
    use super::model_gate_reason;
    use quant_pivot_models::types::ReportFunnelReason;

    #[test]
    fn model_gate_mapping_stable() {
        assert_eq!(
            model_gate_reason(Some("score_below_floor")),
            ReportFunnelReason::ScoreBelowFloor
        );
        assert_eq!(
            model_gate_reason(Some("low_confidence")),
            ReportFunnelReason::LowConfidence
        );
    }
}
