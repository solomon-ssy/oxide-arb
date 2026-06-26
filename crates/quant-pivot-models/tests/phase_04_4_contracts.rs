//! Phase 04.4 API / WS contract acceptance tests.
//!
//! Pure (no DB): the report/recommendation `*View` projections, the report diff
//! view, the single-channel `quant.report` WebSocket payload discriminant, and
//! the RBAC surface (no opportunity/trade semantics; `QuantReport` ops).

use std::collections::BTreeMap;

use chrono::{TimeZone, Utc};
use rust_decimal_macros::dec;
use sea_orm::Iterable;
use serde_json::json;

use quant_pivot_models::enums::market::MarketStatus;
use quant_pivot_models::{
    domain::{
        CoreEvent, EligibilityShift, QuantEvidenceView, QuantRecommendationView,
        QuantReportDetailView, QuantReportView, RecommendationDelta, RecommendationInfo,
        RecommendationReportInfo, ReportDiff, ReportDiffView, ReportEventKind,
        ReportLifecycleEvent, compute_report_diff, ws::mapping::event_envelope,
    },
    enums::{
        factor::FactorFamily,
        quant::{
            AccountSource, BindingConstraint, EntryTriggerKind, FactorDirection,
            IneligibilityReason, OutcomeSide, QuantRuntimeMode, RecommendationReportStatus,
            RecommendationStatus, ReportKind, ReportTriggerKind, SettlementPolicy, SizingModelKind,
        },
        rbac::{Operation, ResourceType},
    },
    types::{
        AccountSnapshotId, BookSnapshotRef, Bps, ConfidenceSummary, ContentHash,
        DataQualitySummary, EligibilitySummary, EntryPlan, EventId, EvidenceRefs,
        ExecutionEligibility, ExitPlan, FactorBreakdownEntry, FeatureVectorId, MarketContext,
        MarketId, MarketSelectionId, ModelRunId, ModelVersionId, PortfolioPlanId, Price,
        Probability, RecommendationFactorBreakdown, RecommendationId, RecommendationIdentity,
        RecommendationReportId, ReportDataQualitySnapshotId, ReportSummary, RiskEnvelope,
        RuntimeConfigVersionId, Shares, SignalCandidateId, SizingPlan, TokenId, Usd,
    },
};
use std::str::FromStr;

fn at(ts: i64) -> chrono::DateTime<Utc> {
    Utc.timestamp_opt(ts, 0).unwrap()
}

fn report_summary(total_usd: Usd, published: u32) -> ReportSummary {
    ReportSummary {
        market_selection_count: 12,
        candidate_count: 8,
        rejected_count: 3,
        published_recommendation_count: published,
        total_suggested_usd: total_usd,
        max_single_recommendation_usd: Usd::new(dec!(300)),
        category_allocation: BTreeMap::new(),
        event_allocation: BTreeMap::new(),
        average_score: Probability::new(dec!(0.7)),
        min_score: Probability::new(dec!(0.55)),
        model_confidence_summary: ConfidenceSummary::default(),
        data_quality_summary: DataQualitySummary::default(),
        top_rejection_reasons: Vec::new(),
        execution_eligibility_summary: EligibilitySummary {
            eligible_report_only: published,
            eligible_semi_auto: 0,
            eligible_auto_execution: 0,
        },
        empty_reason: None,
        warnings: vec!["thin book".to_owned()],
    }
}

fn report(
    id: &RecommendationReportId,
    status: RecommendationReportStatus,
) -> RecommendationReportInfo {
    RecommendationReportInfo {
        recommendation_report_id: id.clone(),
        report_kind: ReportKind::TopN,
        trigger_kind: ReportTriggerKind::Scheduled,
        trigger_key: "scheduled:daily:2023-11-14T22:13:20Z".to_owned(),
        trigger_time: at(1_700_000_000),
        source_delay_secs: 120,
        as_of: at(1_699_999_880),
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
        data_quality_snapshot_ref: ReportDataQualitySnapshotId::from_v7(),
        summary_json: report_summary(Usd::new(dec!(500)), 2),
        published_at: Some(at(1_700_000_000)),
        revoked_at: None,
        expired_at: None,
        status_reason: None,
        created_at: at(1_700_000_000),
    }
}

fn recommendation(
    report_id: &RecommendationReportId,
    rank: i32,
    market: &str,
    side: OutcomeSide,
    suggested_usd: Usd,
) -> RecommendationInfo {
    RecommendationInfo {
        recommendation_id: RecommendationId::from_v7(),
        recommendation_report_id: report_id.clone(),
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
        valid_from: at(1_700_000_000),
        valid_until: at(1_700_086_400),
        status: RecommendationStatus::Published,
        created_at: at(1_700_000_000),
    }
}

fn entry_plan() -> EntryPlan {
    EntryPlan {
        trigger_kind: EntryTriggerKind::LimitPrice,
        trigger_price: Some(Price::new(dec!(0.42))),
        limit_price: Some(Price::new(dec!(0.43))),
        max_slippage_bps: Bps::new(dec!(50)),
        valid_from: at(1_700_000_000),
        valid_until: at(1_700_003_600),
        min_depth_usd: Usd::new(dec!(100)),
        max_book_age_ms: 2_000,
        confirmation_window_secs: 30,
        cancel_if_not_triggered: true,
        entry_reason: "limit entry".to_owned(),
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
        sizing_reason: "kelly".to_owned(),
        sizing_model: SizingModelKind::Kelly,
        edge_bps: Some(Bps::new(dec!(120))),
        kelly_fraction_applied: Some(dec!(0.5)),
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
        partial_exit_nodes: Vec::new(),
        trailing_stop: None,
        signal_invalidation_rules: Vec::new(),
        settlement_policy: SettlementPolicy::HoldToResolution,
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
        envelope_hash: ContentHash::parse(format!("blake3:{}", "0".repeat(64))).unwrap(),
    }
}

fn factor_breakdown() -> RecommendationFactorBreakdown {
    RecommendationFactorBreakdown(vec![FactorBreakdownEntry {
        factor_name: "liquidity_depth".to_owned(),
        family: FactorFamily::Liquidity,
        raw_value: Some(dec!(1234.5)),
        normalized_score: Probability::new(dec!(0.8)),
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
        category: quant_pivot_models::enums::common::MarketCategory::Politics,
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
        approval_role: Some("trader".to_owned()),
        auto_policy_id: None,
    }
}

#[test]
fn report_detail_includes_summary_and_header() {
    let info = report(
        &RecommendationReportId::from_v7(),
        RecommendationReportStatus::Published,
    );
    let json = serde_json::to_value(QuantReportDetailView::from(info)).unwrap();
    assert_eq!(json["account_source"], json!("polymarket"));
    assert_eq!(json["capital_base_usd"], json!("10000"));
    assert!(json["account_snapshot_ref"].is_string());
    assert_eq!(json["status"], json!("published"));
    assert!(json["summary"].is_object());
    assert_eq!(json["summary"]["published_recommendation_count"], json!(2));
    // Header replay handles are exposed as strings.
    assert!(json["model_version_id"].is_string());
    assert!(json["market_selection_id"].is_string());
}

#[test]
fn report_list_view_rolls_up_summary() {
    let info = report(
        &RecommendationReportId::from_v7(),
        RecommendationReportStatus::Published,
    );
    let json = serde_json::to_value(QuantReportView::from(info)).unwrap();
    assert_eq!(json["published_recommendation_count"], json!(2));
    assert_eq!(json["total_suggested_usd"], json!("500"));
    assert_eq!(json["status"], json!("published"));
    // List rows do not embed the full summary object.
    assert!(json.get("summary").is_none());
}

#[test]
fn recommendation_view_has_entry_sizing_exit_eligibility() {
    let report_id = RecommendationReportId::from_v7();
    let rec = recommendation(
        &report_id,
        1,
        "0xmarketA",
        OutcomeSide::Yes,
        Usd::new(dec!(250)),
    );
    let json = serde_json::to_value(QuantRecommendationView::from(rec)).unwrap();
    assert!(json["entry_plan"].is_object());
    assert!(json["sizing_plan"].is_object());
    assert!(json["exit_plan"].is_object());
    assert!(json["risk_envelope"].is_object());
    assert!(json["execution_eligibility"].is_object());
    assert_eq!(json["sizing_plan"]["suggested_usd"], json!("250"));
    assert_eq!(json["outcome_side"], json!("yes"));
    // Evidence is a separate endpoint — never leaked into the recommendation view.
    assert!(json.get("evidence_refs").is_none());
}

#[test]
fn evidence_view_enables_replay() {
    let report_id = RecommendationReportId::from_v7();
    let rec = recommendation(
        &report_id,
        1,
        "0xmarketA",
        OutcomeSide::Yes,
        Usd::new(dec!(250)),
    );
    let json = serde_json::to_value(QuantEvidenceView::from(rec)).unwrap();
    for key in [
        "signal_candidate_id",
        "feature_vector_id",
        "model_run_id",
        "market_selection_id",
        "runtime_config_version_id",
        "model_version_id",
    ] {
        assert!(
            json[key].is_string(),
            "evidence handle {key} must be a string"
        );
    }
    assert_eq!(
        json["book_snapshot_ref"],
        json!(book_snapshot_ref().canonical_string())
    );
}

#[test]
fn report_diff_view_shape() {
    let base_id = RecommendationReportId::from_v7();
    let compare_id = RecommendationReportId::from_v7();
    let base = report(&base_id, RecommendationReportStatus::Published);
    let compare = report(&compare_id, RecommendationReportStatus::Published);
    // base: A + B; compare: A (re-weighted) + C → retained A, removed B, added C.
    let base_recs = vec![
        recommendation(&base_id, 1, "0xA", OutcomeSide::Yes, Usd::new(dec!(100))),
        recommendation(&base_id, 2, "0xB", OutcomeSide::Yes, Usd::new(dec!(200))),
    ];
    let compare_recs = vec![
        recommendation(&compare_id, 1, "0xA", OutcomeSide::Yes, Usd::new(dec!(150))),
        recommendation(&compare_id, 2, "0xC", OutcomeSide::Yes, Usd::new(dec!(50))),
    ];
    let diff = compute_report_diff(&base, &base_recs, &compare, &compare_recs);
    let view = ReportDiffView::from(diff);
    assert_eq!(view.added.len(), 1, "C added");
    assert_eq!(view.removed.len(), 1, "B removed");
    assert_eq!(view.retained.len(), 1, "A retained");
    assert_eq!(view.added[0].market_id, "0xC");
    assert_eq!(view.removed[0].market_id, "0xB");
    // retained A: 150 - 100 = +50
    assert_eq!(view.retained[0].suggested_usd_delta, Usd::new(dec!(50)));
    // total: (150+50) - (100+200) = -100
    assert_eq!(view.total_suggested_usd_delta, Usd::new(dec!(-100)));

    let json = serde_json::to_value(&view).unwrap();
    assert_eq!(json["total_suggested_usd_delta"], json!("-100"));
    assert!(json["base_eligibility"].is_object());
    assert!(json["compare_eligibility"].is_object());
}

#[test]
fn ws_report_event_uses_single_channel_with_discriminant() {
    let info = report(
        &RecommendationReportId::from_v7(),
        RecommendationReportStatus::Published,
    );
    let event = CoreEvent::Report(ReportLifecycleEvent::committed(&info));
    let (key, envelope) = event_envelope(&event).expect("report event maps");
    assert_eq!(envelope.kind.as_str(), "quant.report");
    assert_eq!(key.channel.as_str(), "quant.report");
    let data = serde_json::to_value(&envelope.data).unwrap();
    assert_eq!(data["event"], json!("published"));
    assert!(data["recommendation_report_id"].is_string());
    assert_eq!(data["runtime_mode"], json!("report_only"));
}

#[test]
fn ws_ephemeral_started_and_failed_carry_no_report_id() {
    let started = ReportLifecycleEvent::started(
        "ad_hoc:req-1".to_owned(),
        ReportKind::TopN,
        QuantRuntimeMode::ReportOnly,
        at(1_700_000_000),
    );
    assert_eq!(started.event, ReportEventKind::Started);
    assert!(started.recommendation_report_id.is_none());

    let failed = ReportLifecycleEvent::failed(
        "ad_hoc:req-1".to_owned(),
        ReportKind::TopN,
        QuantRuntimeMode::ReportOnly,
        at(1_700_000_000),
        "account".to_owned(),
        "venue read failed".to_owned(),
    );
    let json = serde_json::to_value(&failed).unwrap();
    assert_eq!(json["event"], json!("failed"));
    assert_eq!(json["error_code"], json!("account"));
    assert_eq!(json["recommendation_report_id"], json!(null));
    assert_eq!(json["trigger_key"], json!("ad_hoc:req-1"));
}

#[test]
fn empty_report_event_is_discriminated_from_published() {
    let info = report(
        &RecommendationReportId::from_v7(),
        RecommendationReportStatus::PublishedEmpty,
    );
    let event = ReportLifecycleEvent::committed(&info);
    assert_eq!(event.event, ReportEventKind::Empty);
}

#[test]
fn rbac_exposes_no_opportunity_or_trade_semantics() {
    for variant in ResourceType::iter() {
        let name = variant.as_str();
        assert!(
            !matches!(name, "opportunity" | "trade" | "pnl" | "risk" | "blacklist"),
            "endgame resource {name} must be removed"
        );
    }
    // QuantReport carries exactly read + enqueue + revoke.
    let ops = ResourceType::QuantReport.operations();
    assert!(ops.contains(&Operation::Read));
    assert!(ops.contains(&Operation::Enqueue));
    assert!(ops.contains(&Operation::Revoke));
    assert!(!ops.contains(&Operation::Delete));

    let intent_ops = ResourceType::OrderIntent.operations();
    assert!(intent_ops.contains(&Operation::Read));
    assert!(intent_ops.contains(&Operation::Create));
    assert!(intent_ops.contains(&Operation::Approve));
    assert!(intent_ops.contains(&Operation::Reject));
    assert!(intent_ops.contains(&Operation::Cancel));
    assert!(intent_ops.contains(&Operation::Submit));
    assert!(!intent_ops.contains(&Operation::Delete));

    assert_eq!(Operation::from_str("approve").unwrap(), Operation::Approve);
    assert_eq!(Operation::from_str("cancel").unwrap(), Operation::Cancel);
    assert_eq!(Operation::from_str("submit").unwrap(), Operation::Submit);
    assert!(ResourceType::ExecutionOrder.allows(Operation::Read));
    assert!(ResourceType::Position.allows(Operation::Read));
}

#[test]
fn report_diff_eligibility_shift_carries_both_sides() {
    let diff = ReportDiff {
        base_report_id: RecommendationReportId::from_v7(),
        compare_report_id: RecommendationReportId::from_v7(),
        added: vec![RecommendationDelta {
            market_id: MarketId::new("0xC"),
            outcome_side: OutcomeSide::No,
            base_recommendation_id: None,
            compare_recommendation_id: Some(RecommendationId::from_v7()),
            base_rank: None,
            compare_rank: Some(1),
            base_suggested_usd: None,
            compare_suggested_usd: Some(Usd::new(dec!(50))),
            suggested_usd_delta: Usd::new(dec!(50)),
        }],
        removed: Vec::new(),
        retained: Vec::new(),
        base_total_suggested_usd: Usd::ZERO,
        compare_total_suggested_usd: Usd::new(dec!(50)),
        total_suggested_usd_delta: Usd::new(dec!(50)),
        eligibility: EligibilityShift {
            base: EligibilitySummary::default(),
            compare: EligibilitySummary {
                eligible_report_only: 1,
                eligible_semi_auto: 0,
                eligible_auto_execution: 0,
            },
        },
    };
    let json = serde_json::to_value(ReportDiffView::from(diff)).unwrap();
    assert_eq!(json["added"][0]["outcome_side"], json!("no"));
    assert_eq!(json["added"][0]["base_suggested_usd"], json!(null));
    assert_eq!(
        json["compare_eligibility"]["eligible_report_only"],
        json!(1)
    );
}

#[test]
fn recommendation_view_rank_scores_snapshot() {
    use insta::assert_json_snapshot;
    use quant_pivot_test_support::report_snapshots::recommendation_limit_entry;

    assert_json_snapshot!(recommendation_limit_entry());
}
