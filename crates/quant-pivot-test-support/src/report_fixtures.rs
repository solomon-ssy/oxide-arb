//! Strong-typed recommendation report / recommendation fixtures.
//!
//! Shared by `quant-pivot-core` and `quant-pivot-web` tests so report-plane
//! fixtures (header, summary, and the full per-recommendation payload blocks)
//! are built one way. Defaults are sensible; knobs cover the dimensions tests
//! actually vary (id, status, kind, market, side, rank, suggested USD).

use std::collections::BTreeMap;

use chrono::{TimeZone, Utc};
use rust_decimal_macros::dec;

use quant_pivot_models::enums::market::MarketStatus;
use quant_pivot_models::{
    domain::{RecommendationInfo, RecommendationReportInfo},
    enums::{
        common::MarketCategory,
        factor::{FactorFamily, FactorValueState, NormalizationSource},
        quant::{
            AccountSource, BindingConstraint, EntryTriggerKind, ExitSettlementMode,
            FactorDirection, IneligibilityReason, OutcomeSide, QuantRuntimeMode,
            RecommendationReportStatus, RecommendationStatus, RedeemPolicy, ReportKind,
            ReportTriggerKind, SizingModelKind,
        },
    },
    types::{
        AccountSnapshotId, BookSnapshotRef, Bps, ConfidenceSummary, ContentHash,
        DataQualitySummary, EligibilitySummary, EntryPlan, EquitySnapshotId, EventId, EvidenceRefs,
        ExecutionEligibility, ExitPlan, FactorBreakdownEntry, FeatureVectorId, MarketContext,
        MarketId, MarketSelectionId, ModelRunId, ModelVersionId, PortfolioPlanId, Price,
        Probability, RecommendationFactorBreakdown, RecommendationId, RecommendationIdentity,
        RecommendationReportId, ReportDataQualitySnapshotId, ReportSummary, RiskEnvelope,
        RuntimeConfigVersionId, Shares, SignalCandidateId, SizingPlan, TokenId, Usd,
    },
};
use std::str::FromStr;

fn content_hash() -> ContentHash {
    ContentHash::parse(format!("blake3:{}", "0".repeat(64))).expect("valid hash")
}

/// A report header fixture with the given id / kind / status.
#[must_use]
pub fn report(
    id: RecommendationReportId,
    kind: ReportKind,
    status: RecommendationReportStatus,
) -> RecommendationReportInfo {
    RecommendationReportInfo {
        recommendation_report_id: id,
        report_kind: kind,
        trigger_kind: ReportTriggerKind::Scheduled,
        trigger_key: "scheduled:daily-topn:2023-11-14T22:13:20Z".to_owned(),
        trigger_time: Utc.timestamp_opt(1_700_000_000, 0).unwrap(),
        source_delay_secs: 120,
        as_of: Utc.timestamp_opt(1_699_999_880, 0).unwrap(),
        horizon_secs: 86_400,
        runtime_mode: QuantRuntimeMode::ReportOnly,
        runtime_config_version_id: RuntimeConfigVersionId::from_v7(),
        model_version_id: ModelVersionId::from_v7(),
        market_selection_id: MarketSelectionId::from_v7(),
        portfolio_plan_id: PortfolioPlanId::from_v7(),
        top_n: 20,
        status,
        account_source: AccountSource::Polymarket,
        capital_base_usd: Usd::new(dec!(10000)),
        account_snapshot_ref: AccountSnapshotId::from_v7(),
        equity_snapshot_ref: EquitySnapshotId::from_v7(),
        data_quality_snapshot_ref: ReportDataQualitySnapshotId::from_v7(),
        summary_json: report_summary(),
        published_at: Some(Utc.timestamp_opt(1_700_000_000, 0).unwrap()),
        valid_until: Some(Utc.timestamp_opt(1_700_003_600, 0).unwrap()),
        revoked_at: None,
        expired_at: None,
        status_reason: None,
        created_at: Utc.timestamp_opt(1_700_000_000, 0).unwrap(),
    }
}

/// A report summary fixture.
#[must_use]
pub fn report_summary() -> ReportSummary {
    let mut category_allocation = BTreeMap::new();
    category_allocation.insert(MarketCategory::Politics, Usd::new(dec!(250)));
    let mut event_allocation = BTreeMap::new();
    event_allocation.insert(EventId::new("evt-1"), Usd::new(dec!(250)));
    ReportSummary {
        market_selection_count: 12,
        candidate_count: 8,
        rejected_count: 3,
        published_recommendation_count: 2,
        total_suggested_usd: Usd::new(dec!(500)),
        max_single_recommendation_usd: Usd::new(dec!(300)),
        aggregate_exposure_cap_usd: Some(Usd::new(dec!(2500))),
        category_allocation,
        event_allocation,
        average_score: Probability::new(dec!(0.7)),
        min_score: Probability::new(dec!(0.55)),
        model_confidence_summary: ConfidenceSummary {
            mean_confidence: Probability::new(dec!(0.72)),
            min_confidence: Probability::new(dec!(0.6)),
            max_confidence: Probability::new(dec!(0.9)),
        },
        data_quality_summary: DataQualitySummary {
            fresh_count: 5,
            acceptable_count: 2,
            degraded_count: 1,
            stale_count: 0,
            insufficient_count: 0,
        },
        top_rejection_reasons: Vec::new(),
        execution_eligibility_summary: EligibilitySummary {
            eligible_report_only: 2,
            eligible_semi_auto: 0,
            eligible_auto_execution: 0,
        },
        empty_reason: None,
        warnings: vec!["thin book on 1 market".to_owned()],
    }
}

/// A single recommendation fixture for `report_id` at `rank`.
#[must_use]
pub fn recommendation(
    report_id: RecommendationReportId,
    id: RecommendationId,
    rank: i32,
    market: &str,
    side: OutcomeSide,
    suggested_usd: Usd,
) -> RecommendationInfo {
    RecommendationInfo {
        recommendation_id: id,
        recommendation_report_id: report_id,
        rank,
        market_id: MarketId::new(market),
        event_id: EventId::new("evt-1"),
        token_id: TokenId::new(format!("token-{market}")),
        outcome_side: side,
        composite_score: Probability::new(dec!(0.71)),
        risk_adjusted_score: Probability::new(dec!(0.66)),
        confidence: Probability::new(dec!(0.72)),
        expected_return_bps: Bps::new(dec!(150)),
        downside_bps: Bps::new(dec!(80)),
        identity: recommendation_identity(),
        market_context: market_context(),
        rank_before_portfolio: rank,
        liquidity_score: Probability::new(dec!(0.8)),
        data_quality_score: Probability::new(dec!(0.9)),
        model_score_percentile: Probability::new(dec!(0.75)),
        entry_plan: entry_plan(),
        sizing_plan: sizing_plan(suggested_usd),
        exit_plan: exit_plan(),
        risk_envelope: risk_envelope(),
        factor_breakdown: factor_breakdown(),
        evidence_refs: evidence_refs(),
        execution_eligibility: execution_eligibility(),
        valid_from: Utc.timestamp_opt(1_700_000_000, 0).unwrap(),
        valid_until: Utc.timestamp_opt(1_700_086_400, 0).unwrap(),
        status: RecommendationStatus::Published,
        created_at: Utc.timestamp_opt(1_700_000_000, 0).unwrap(),
    }
}

fn entry_plan() -> EntryPlan {
    EntryPlan {
        trigger_kind: EntryTriggerKind::LimitPrice,
        trigger_price: Some(Price::new(dec!(0.42))),
        limit_price: Some(Price::new(dec!(0.43))),
        max_slippage_bps: Bps::new(dec!(50)),
        valid_from: Utc.timestamp_opt(1_700_000_000, 0).unwrap(),
        valid_until: Utc.timestamp_opt(1_700_003_600, 0).unwrap(),
        min_depth_usd: Usd::new(dec!(100)),
        max_book_age_ms: 2_000,
        cancel_if_not_triggered: true,
        entry_reason: "limit entry at edge".to_owned(),
    }
}

fn sizing_plan(suggested_usd: Usd) -> SizingPlan {
    SizingPlan {
        suggested_usd,
        suggested_shares: Shares::new(dec!(500)),
        max_usd: Usd::new(dec!(500)),
        min_usd: Usd::new(dec!(10)),
        portfolio_weight_pct: dec!(0.05),
        market_exposure_after_usd: suggested_usd,
        event_exposure_after_usd: suggested_usd,
        category_exposure_after_usd: suggested_usd,
        binding_constraint: BindingConstraint::KellyCap,
        sizing_reason: "half-kelly capped".to_owned(),
        sizing_model: SizingModelKind::Kelly,
        edge_bps: Some(Bps::new(dec!(120))),
        kelly_fraction_applied: Some(dec!(0.5)),
        edge_uncertainty_shrink_applied: None,
        correlation_shrink_applied: None,
        f_star_applied: Some(dec!(1.0)),
        kelly_fraction_config_applied: Some(dec!(0.5)),
        confidence_shrink_applied: Some(dec!(1.0)),
        drawdown_shrink_applied: Some(dec!(1.0)),
        raw_fraction_applied: Some(dec!(0.5)),
        position_cap_fraction_applied: Some(dec!(0.05)),
    }
}

fn exit_plan() -> ExitPlan {
    ExitPlan {
        take_profit_price: Some(Price::new(dec!(0.7))),
        take_profit_pct: Some(dec!(0.6)),
        stop_loss_price: Some(Price::new(dec!(0.3))),
        stop_loss_pct: Some(dec!(0.3)),
        time_exit_at: None,
        max_hold_secs: Some(86_400),
        partial_exit_nodes: vec![],
        trailing_stop: None,
        signal_invalidation_rules: Vec::new(),
        settlement_mode: ExitSettlementMode::HoldToResolution,
        redeem_policy: RedeemPolicy::Manual,
        manual_review_at: None,
        exit_reason: "tp/sl".to_owned(),
    }
}

fn risk_envelope() -> RiskEnvelope {
    RiskEnvelope {
        max_loss_usd: Usd::new(dec!(120)),
        max_slippage_bps: Bps::new(dec!(50)),
        max_position_usd: Usd::new(dec!(500)),
        max_market_exposure_usd: Usd::new(dec!(500)),
        max_event_exposure_usd: Usd::new(dec!(750)),
        max_category_exposure_usd: Usd::new(dec!(1500)),
        requires_approval: true,
        auto_execution_allowed: false,
        risk_notes: vec!["thin book".to_owned()],
        envelope_hash: content_hash(),
    }
}

fn factor_breakdown() -> RecommendationFactorBreakdown {
    RecommendationFactorBreakdown(vec![FactorBreakdownEntry {
        factor_name: "liquidity_depth".to_owned(),
        family: FactorFamily::Liquidity,
        value_state: FactorValueState::Scored,
        raw_value: Some(dec!(1234.5)),
        normalized_score: Some(Probability::new(dec!(0.8))),
        normalization_source: Some(NormalizationSource::CrossSection),
        indeterminate_reason: None,
        weight: dec!(0.4),
        contribution: dec!(0.32),
        confidence: Probability::new(dec!(0.75)),
        direction: FactorDirection::Positive,
        explanation: "deep book".to_owned(),
        source_refs: vec!["feature:liquidity_depth".to_owned()],
    }])
}

fn recommendation_identity() -> RecommendationIdentity {
    RecommendationIdentity {
        category: MarketCategory::Politics,
        question: "Will the event resolve Yes?".to_owned(),
        outcome_name: "Yes".to_owned(),
    }
}

const fn market_context() -> MarketContext {
    MarketContext {
        best_bid: Some(Price::new(dec!(0.41))),
        best_ask: Some(Price::new(dec!(0.43))),
        mid_price: Some(Price::new(dec!(0.42))),
        spread_bps: Some(Bps::new(dec!(50))),
        depth_usd: Usd::new(dec!(5000)),
        volume_24h_usd: Some(Usd::new(dec!(10000))),
        book_age_ms: 500,
        time_to_resolution_secs: Some(86_400),
        market_status: MarketStatus::Active,
        neg_risk: false,
        fee_rate: None,
    }
}

fn book_snapshot_ref() -> BookSnapshotRef {
    BookSnapshotRef::from_str(&format!(
        "book:live:token-abc:1:1700000000@blake3:{}",
        "0".repeat(64)
    ))
    .expect("valid book snapshot ref")
}

fn evidence_refs() -> EvidenceRefs {
    EvidenceRefs {
        signal_candidate_id: SignalCandidateId::from_v7(),
        feature_vector_id: FeatureVectorId::from_v7(),
        model_run_id: ModelRunId::from_v7(),
        market_selection_id: MarketSelectionId::from_v7(),
        book_snapshot_ref: book_snapshot_ref(),
        runtime_config_version_id: RuntimeConfigVersionId::from_v7(),
        model_version_id: ModelVersionId::from_v7(),
        factor_definition_versions: Vec::new(),
        data_quality_snapshot_ref: ReportDataQualitySnapshotId::from_v7(),
    }
}

fn execution_eligibility() -> ExecutionEligibility {
    ExecutionEligibility {
        eligible_modes: vec![QuantRuntimeMode::ReportOnly],
        ineligibility_reasons: vec![IneligibilityReason::ReportOnlyMode],
        approval_required: true,
        auto_policy_id: None,
    }
}
