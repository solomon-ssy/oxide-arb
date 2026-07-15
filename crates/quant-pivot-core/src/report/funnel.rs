//! Conserved report-market funnel construction.

use std::collections::{BTreeMap, HashMap};

use chrono::{DateTime, Utc};
use quant_pivot_error::{QuantResult, report::ReportError};
use quant_pivot_models::{
    clickhouse::ReportMarketFunnelRow,
    domain::NewRecommendation,
    enums::quant::RejectionReason,
    hashing::CanonicalDigest,
    types::{
        EventId, FeatureVectorId, MarketId, MarketSelectionId, ModelRunId, ModelVersionId,
        RecommendationId, RecommendationReportId, ReportFunnelReason, ReportFunnelStage,
        ResearchProfileRef, RuntimeConfigVersionId, SignalCandidateId, TokenId,
    },
};
use quant_pivot_research::{
    portfolio::RejectedCandidate,
    selection::{ExcludedMarket, ExclusionReason, MarketSelectionSnapshot, SelectedMarket},
};
use serde::Serialize;

use crate::service::{feature_pipeline::RejectedMarket, model_runner::ModelMarketDecision};

#[derive(Clone, Copy)]
pub struct ReportFunnelInput<'a> {
    pub report_id: &'a RecommendationReportId,
    pub profile_ref: &'a ResearchProfileRef,
    pub runtime_config_version_id: &'a RuntimeConfigVersionId,
    pub model_version_id: &'a ModelVersionId,
    pub model_run_id: Option<&'a ModelRunId>,
    pub selection: &'a MarketSelectionSnapshot,
    pub feature_rejected: &'a [RejectedMarket],
    pub feature_vector_by_market: &'a HashMap<MarketId, FeatureVectorId>,
    pub model_decisions: &'a [ModelMarketDecision],
    pub planner_rejected: &'a [RejectedCandidate],
    pub recommendations: &'a [NewRecommendation],
    pub early_terminal: Option<(ReportFunnelStage, ReportFunnelReason)>,
    pub event_time: DateTime<Utc>,
}

struct DraftDecision {
    event_id: EventId,
    token_id: TokenId,
    terminal_stage: Option<ReportFunnelStage>,
    primary_reason: Option<ReportFunnelReason>,
    secondary_diagnostics_json: String,
    feature_vector_id: Option<FeatureVectorId>,
    signal_candidate_id: Option<SignalCandidateId>,
    recommendation_id: Option<RecommendationId>,
}

impl DraftDecision {
    fn included(market: &SelectedMarket) -> Self {
        Self {
            event_id: market.event_id.clone(),
            token_id: market.primary_token_id.clone(),
            terminal_stage: None,
            primary_reason: None,
            secondary_diagnostics_json: "{}".to_owned(),
            feature_vector_id: None,
            signal_candidate_id: None,
            recommendation_id: None,
        }
    }

    fn excluded(market: &ExcludedMarket) -> QuantResult<Self> {
        let (terminal_stage, primary_reason) = selection_terminal(&market.reason);
        let secondary_diagnostics_json =
            serde_json::to_string(&market.reason).map_err(|error| {
                ReportError::InvariantViolation {
                    stage: "report_funnel",
                    detail: format!("selection diagnostic serialization failed: {error}"),
                }
            })?;
        Ok(Self {
            event_id: market.event_id.clone(),
            token_id: market.primary_token_id.clone(),
            terminal_stage: Some(terminal_stage),
            primary_reason: Some(primary_reason),
            secondary_diagnostics_json,
            feature_vector_id: None,
            signal_candidate_id: None,
            recommendation_id: None,
        })
    }

    fn terminate(
        &mut self,
        stage: ReportFunnelStage,
        reason: ReportFunnelReason,
        diagnostics: String,
    ) -> QuantResult<()> {
        if self.terminal_stage.is_some() {
            return Err(ReportError::InvariantViolation {
                stage: "report_funnel",
                detail: "market received more than one terminal decision".to_owned(),
            }
            .into());
        }
        self.terminal_stage = Some(stage);
        self.primary_reason = Some(reason);
        self.secondary_diagnostics_json = diagnostics;
        Ok(())
    }
}

pub fn build_report_market_funnel(
    input: ReportFunnelInput<'_>,
) -> QuantResult<Vec<ReportMarketFunnelRow>> {
    let mut decisions = BTreeMap::new();
    for market in &input.selection.included {
        insert_unique(
            &mut decisions,
            &market.market_id,
            DraftDecision::included(market),
        )?;
    }
    for market in &input.selection.excluded {
        insert_unique(
            &mut decisions,
            &market.market_id,
            DraftDecision::excluded(market)?,
        )?;
    }

    for rejected in input.feature_rejected {
        let decision = require_pending(&mut decisions, &rejected.market_id)?;
        decision.terminate(
            ReportFunnelStage::FeatureReady,
            ReportFunnelReason::FeatureDataQualityRejected,
            serde_json::to_string(
                &rejected
                    .missing_required
                    .iter()
                    .map(|(name, reason)| (name.as_str(), format!("{reason:?}")))
                    .collect::<Vec<_>>(),
            )
            .map_err(|error| ReportError::InvariantViolation {
                stage: "report_funnel",
                detail: format!("feature diagnostic serialization failed: {error}"),
            })?,
        )?;
    }
    for (market_id, feature_vector_id) in input.feature_vector_by_market {
        let decision = require_pending(&mut decisions, market_id)?;
        decision.feature_vector_id = Some(feature_vector_id.clone());
    }
    for model in input.model_decisions {
        let decision = require_pending(&mut decisions, &model.market_id)?;
        if decision.feature_vector_id.is_none() {
            return Err(ReportError::InvariantViolation {
                stage: "report_funnel",
                detail: format!(
                    "model decision for market {} has no feature-vector lineage",
                    model.market_id
                ),
            }
            .into());
        }
        decision.signal_candidate_id = Some(model.signal_candidate_id.clone());
        if !model.gate_passed {
            decision.terminate(
                ReportFunnelStage::ModelGatePassed,
                model_gate_reason(model.primary_reason.as_deref())?,
                "{}".to_owned(),
            )?;
        }
    }
    for rejected in input.planner_rejected {
        let decision = require_pending(&mut decisions, &rejected.market_id)?;
        let (stage, reason) = planner_terminal(rejected.reason);
        decision.terminate(
            stage,
            reason,
            serde_json::to_string(&serde_json::json!({ "detail": rejected.detail })).map_err(
                |error| ReportError::InvariantViolation {
                    stage: "report_funnel",
                    detail: format!("planner diagnostic serialization failed: {error}"),
                },
            )?,
        )?;
    }
    for recommendation in input.recommendations {
        let decision = require_pending(&mut decisions, &recommendation.market_id)?;
        decision.recommendation_id = Some(recommendation.recommendation_id.clone());
        decision.terminate(
            ReportFunnelStage::Published,
            ReportFunnelReason::Published,
            "{}".to_owned(),
        )?;
    }

    for (market_id, decision) in &mut decisions {
        if decision.terminal_stage.is_some() {
            continue;
        }
        if let Some((stage, reason)) = input.early_terminal {
            decision.terminate(stage, reason, "{}".to_owned())?;
        } else if decision.feature_vector_id.is_some()
            && input.model_run_id.is_some()
            && decision.signal_candidate_id.is_none()
        {
            decision.terminate(
                ReportFunnelStage::ModelScored,
                ReportFunnelReason::MissingModelOutput,
                "{}".to_owned(),
            )?;
        } else {
            return Err(ReportError::InvariantViolation {
                stage: "report_funnel",
                detail: format!("market {market_id} has no terminal funnel decision"),
            }
            .into());
        }
    }

    let expected = input
        .selection
        .included
        .len()
        .checked_add(input.selection.excluded.len())
        .ok_or_else(|| ReportError::NumericOverflow {
            field: "report_funnel.catalog_visible_count",
            detail: "catalog-visible count overflow".to_owned(),
        })?;
    if decisions.len() != expected {
        return Err(ReportError::InvariantViolation {
            stage: "report_funnel",
            detail: "catalog-visible markets do not form a unique included/excluded partition"
                .to_owned(),
        }
        .into());
    }
    let published_count = decisions
        .values()
        .filter(|decision| decision.terminal_stage == Some(ReportFunnelStage::Published))
        .count();
    if published_count != input.recommendations.len() {
        return Err(ReportError::InvariantViolation {
            stage: "report_funnel",
            detail: "published funnel count does not equal recommendation count".to_owned(),
        }
        .into());
    }

    decisions
        .into_iter()
        .map(|(market_id, decision)| row_from_decision(&input, &market_id, decision))
        .collect()
}

fn insert_unique(
    decisions: &mut BTreeMap<MarketId, DraftDecision>,
    market_id: &MarketId,
    decision: DraftDecision,
) -> QuantResult<()> {
    if decisions.insert(market_id.clone(), decision).is_some() {
        return Err(ReportError::InvariantViolation {
            stage: "report_funnel",
            detail: format!("duplicate catalog-visible market {market_id}"),
        }
        .into());
    }
    Ok(())
}

fn require_pending<'a>(
    decisions: &'a mut BTreeMap<MarketId, DraftDecision>,
    market_id: &MarketId,
) -> QuantResult<&'a mut DraftDecision> {
    let decision = decisions
        .get_mut(market_id)
        .ok_or_else(|| ReportError::InvariantViolation {
            stage: "report_funnel",
            detail: format!("downstream decision references non-catalog market {market_id}"),
        })?;
    if decision.terminal_stage.is_some() {
        return Err(ReportError::InvariantViolation {
            stage: "report_funnel",
            detail: format!("downstream decision references terminal market {market_id}"),
        }
        .into());
    }
    Ok(decision)
}

fn row_from_decision(
    input: &ReportFunnelInput<'_>,
    market_id: &MarketId,
    decision: DraftDecision,
) -> QuantResult<ReportMarketFunnelRow> {
    let terminal_stage =
        decision
            .terminal_stage
            .ok_or_else(|| ReportError::InvariantViolation {
                stage: "report_funnel",
                detail: format!("market {market_id} has no terminal stage"),
            })?;
    let primary_reason =
        decision
            .primary_reason
            .ok_or_else(|| ReportError::InvariantViolation {
                stage: "report_funnel",
                detail: format!("market {market_id} has no primary reason"),
            })?;
    let hash_input = FunnelRowHashInput {
        report_id: input.report_id,
        market_selection_id: &input.selection.market_selection_id,
        profile_ref: input.profile_ref,
        market_id,
        event_id: &decision.event_id,
        token_id: &decision.token_id,
        terminal_stage,
        primary_reason,
        secondary_diagnostics_json: &decision.secondary_diagnostics_json,
        feature_vector_id: decision.feature_vector_id.as_ref(),
        signal_candidate_id: decision.signal_candidate_id.as_ref(),
        recommendation_id: decision.recommendation_id.as_ref(),
    };
    let row_hash = CanonicalDigest::content_hash_json(&hash_input)?;
    Ok(ReportMarketFunnelRow {
        event_time: input.event_time.timestamp_millis(),
        recommendation_report_id: input.report_id.clone(),
        market_selection_id: input.selection.market_selection_id.clone(),
        profile_id: input.profile_ref.id.clone(),
        profile_version: input.profile_ref.version,
        profile_content_hash: input.profile_ref.content_hash.to_string(),
        runtime_config_version_id: input.runtime_config_version_id.clone(),
        model_version_id: input.model_version_id.clone(),
        model_run_id: input.model_run_id.cloned(),
        market_id: market_id.clone(),
        event_id: decision.event_id,
        token_id: decision.token_id,
        terminal_stage: terminal_stage.as_str().to_owned(),
        primary_reason: primary_reason.as_str().to_owned(),
        secondary_diagnostics_json: decision.secondary_diagnostics_json,
        feature_vector_id: decision.feature_vector_id,
        signal_candidate_id: decision.signal_candidate_id,
        recommendation_id: decision.recommendation_id,
        row_hash: row_hash.to_string(),
        ingestion_time: input.event_time.timestamp_millis(),
    })
}

#[derive(Serialize)]
struct FunnelRowHashInput<'a> {
    report_id: &'a RecommendationReportId,
    market_selection_id: &'a MarketSelectionId,
    profile_ref: &'a ResearchProfileRef,
    market_id: &'a MarketId,
    event_id: &'a EventId,
    token_id: &'a TokenId,
    terminal_stage: ReportFunnelStage,
    primary_reason: ReportFunnelReason,
    secondary_diagnostics_json: &'a str,
    feature_vector_id: Option<&'a FeatureVectorId>,
    signal_candidate_id: Option<&'a SignalCandidateId>,
    recommendation_id: Option<&'a RecommendationId>,
}

const fn selection_terminal(reason: &ExclusionReason) -> (ReportFunnelStage, ReportFunnelReason) {
    match reason {
        ExclusionReason::NotOpen => (
            ReportFunnelStage::BusinessEligible,
            ReportFunnelReason::NotOpen,
        ),
        ExclusionReason::CategoryDisabled => (
            ReportFunnelStage::BusinessEligible,
            ReportFunnelReason::CategoryDisabled,
        ),
        ExclusionReason::ResolutionAmbiguous => (
            ReportFunnelStage::BusinessEligible,
            ReportFunnelReason::ResolutionAmbiguous,
        ),
        ExclusionReason::ManuallyBlocked => (
            ReportFunnelStage::BusinessEligible,
            ReportFunnelReason::ManuallyBlocked,
        ),
        ExclusionReason::InsufficientLiquidity => (
            ReportFunnelStage::ExecutableDataEligible,
            ReportFunnelReason::InsufficientLiquidity,
        ),
        ExclusionReason::SpreadTooWide => (
            ReportFunnelStage::ExecutableDataEligible,
            ReportFunnelReason::SpreadTooWide,
        ),
        ExclusionReason::StaleBook => (
            ReportFunnelStage::ExecutableDataEligible,
            ReportFunnelReason::StaleBook,
        ),
        ExclusionReason::IngestLagExceeded => (
            ReportFunnelStage::ExecutableDataEligible,
            ReportFunnelReason::IngestLagExceeded,
        ),
        ExclusionReason::ModelFeatureUnavailable { .. } => (
            ReportFunnelStage::FeatureReady,
            ReportFunnelReason::ModelFeatureUnavailable,
        ),
    }
}

fn model_gate_reason(reason: Option<&str>) -> QuantResult<ReportFunnelReason> {
    match reason {
        Some("score_below_floor") => Ok(ReportFunnelReason::ScoreBelowFloor),
        Some("low_confidence") => Ok(ReportFunnelReason::LowConfidence),
        _ => Err(ReportError::InvariantViolation {
            stage: "report_funnel",
            detail: format!("unknown model-gate reason {reason:?}"),
        }
        .into()),
    }
}

const fn planner_terminal(reason: RejectionReason) -> (ReportFunnelStage, ReportFunnelReason) {
    match reason {
        RejectionReason::NoPositiveSignal => (
            ReportFunnelStage::SizingEligible,
            ReportFunnelReason::NoPositiveSignal,
        ),
        RejectionReason::InvalidEdgeInputs => (
            ReportFunnelStage::SizingEligible,
            ReportFunnelReason::InvalidEdgeInputs,
        ),
        RejectionReason::ReturnModelUncalibrated => (
            ReportFunnelStage::SizingEligible,
            ReportFunnelReason::ReturnModelUncalibrated,
        ),
        RejectionReason::ExecutableEntryUnavailable => (
            ReportFunnelStage::SizingEligible,
            ReportFunnelReason::ExecutableEntryUnavailable,
        ),
        RejectionReason::BelowMinSize => (
            ReportFunnelStage::SizingEligible,
            ReportFunnelReason::BelowMinSize,
        ),
        RejectionReason::LiquidityInfeasible => (
            ReportFunnelStage::SizingEligible,
            ReportFunnelReason::LiquidityInfeasible,
        ),
        RejectionReason::BudgetExhausted => (
            ReportFunnelStage::PortfolioFunded,
            ReportFunnelReason::BudgetExhausted,
        ),
        RejectionReason::MarketCapExhausted => (
            ReportFunnelStage::PortfolioFunded,
            ReportFunnelReason::MarketCapExhausted,
        ),
        RejectionReason::EventCapExhausted => (
            ReportFunnelStage::PortfolioFunded,
            ReportFunnelReason::EventCapExhausted,
        ),
        RejectionReason::CategoryCapExhausted => (
            ReportFunnelStage::PortfolioFunded,
            ReportFunnelReason::CategoryCapExhausted,
        ),
        RejectionReason::CorrelationCapExhausted => (
            ReportFunnelStage::PortfolioFunded,
            ReportFunnelReason::CorrelationCapExhausted,
        ),
        RejectionReason::AvailableCashExhausted => (
            ReportFunnelStage::PortfolioFunded,
            ReportFunnelReason::AvailableCashExhausted,
        ),
        RejectionReason::AggregateExposureCapExhausted => (
            ReportFunnelStage::PortfolioFunded,
            ReportFunnelReason::AggregateExposureCapExhausted,
        ),
        RejectionReason::BeyondTopN => (
            ReportFunnelStage::PortfolioFunded,
            ReportFunnelReason::BeyondTopN,
        ),
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use chrono::{TimeZone, Utc};
    use quant_pivot_models::{
        enums::common::MarketCategory,
        types::{
            ContentHash, EventId, MarketId, MarketSelectionId, ModelVersionId,
            RecommendationReportId, ReportFunnelReason, ReportFunnelStage, RuntimeConfigVersionId,
            SelectionExclusionSummary, TokenId,
        },
    };
    use quant_pivot_research::selection::{
        ExcludedMarket, ExclusionReason, MarketSelectionSnapshot, SelectedMarket,
    };
    use quant_pivot_test_support::execution_pg_seed::fixture_profile_ref;

    use super::{ReportFunnelInput, build_report_market_funnel};

    fn selection() -> MarketSelectionSnapshot {
        let decision_at = Utc
            .with_ymd_and_hms(2026, 7, 14, 0, 0, 0)
            .single()
            .expect("time");
        MarketSelectionSnapshot {
            market_selection_id: MarketSelectionId::from_v7(),
            decision_at,
            runtime_config_version_id: RuntimeConfigVersionId::from_v7(),
            selector_hash: ContentHash::parse(format!("blake3:{}", "1".repeat(64))).expect("hash"),
            included: vec![SelectedMarket {
                market_id: MarketId::new("included"),
                event_id: EventId::new("event-included"),
                category: MarketCategory::Weather,
                primary_token_id: TokenId::new("token-included"),
                secondary_token_id: None,
                liquidity_usd: None,
                volume_24h_usd: None,
                source_refs: Vec::new(),
            }],
            excluded: vec![ExcludedMarket {
                market_id: MarketId::new("excluded"),
                event_id: EventId::new("event-excluded"),
                primary_token_id: TokenId::new("token-excluded"),
                reason: ExclusionReason::StaleBook,
            }],
            exclusion_summary: SelectionExclusionSummary::default(),
        }
    }

    #[test]
    fn every_catalog_market_gets_exactly_one_terminal_decision() {
        let selection = selection();
        let report_id = RecommendationReportId::from_v7();
        let model_version_id = ModelVersionId::from_v7();
        let profile = fixture_profile_ref();
        let rows = build_report_market_funnel(ReportFunnelInput {
            report_id: &report_id,
            profile_ref: &profile,
            runtime_config_version_id: &selection.runtime_config_version_id,
            model_version_id: &model_version_id,
            model_run_id: None,
            selection: &selection,
            feature_rejected: &[],
            feature_vector_by_market: &HashMap::default(),
            model_decisions: &[],
            planner_rejected: &[],
            recommendations: &[],
            early_terminal: Some((
                ReportFunnelStage::FeatureReady,
                ReportFunnelReason::SystemDegraded,
            )),
            event_time: selection.decision_at,
        })
        .expect("conserved funnel");

        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].market_id.as_str(), "excluded");
        assert_eq!(rows[0].terminal_stage, "executable_data_eligible");
        assert_eq!(rows[1].market_id.as_str(), "included");
        assert_eq!(rows[1].terminal_stage, "feature_ready");
        assert_ne!(rows[0].row_hash, rows[1].row_hash);
    }

    #[test]
    fn unresolved_survivor_fails_closed() {
        let selection = selection();
        let report_id = RecommendationReportId::from_v7();
        let model_version_id = ModelVersionId::from_v7();
        let profile = fixture_profile_ref();
        let result = build_report_market_funnel(ReportFunnelInput {
            report_id: &report_id,
            profile_ref: &profile,
            runtime_config_version_id: &selection.runtime_config_version_id,
            model_version_id: &model_version_id,
            model_run_id: None,
            selection: &selection,
            feature_rejected: &[],
            feature_vector_by_market: &HashMap::default(),
            model_decisions: &[],
            planner_rejected: &[],
            recommendations: &[],
            early_terminal: None,
            event_time: selection.decision_at,
        });

        assert!(result.is_err());
    }
}
