//! Deterministic report payload fixtures for insta JSON snapshot tests.
//!
//! Parent doc §19: non-empty `TopN`, empty, limit/immediate entry, partial exits,
//! not-auto-eligible, and revoked report header. Uses [`seeded_uuid`] so snapshots
//! stay stable across runs.

use std::collections::BTreeMap;
use std::str::FromStr;

use chrono::{TimeZone, Utc};
use rust_decimal_macros::dec;
use serde::Serialize;

use quant_pivot_models::{
    domain::{
        RecommendationInfo, RecommendationReportInfo,
        api::{QuantRecommendationView, QuantReportDetailView},
    },
    enums::quant::{
        EntryTriggerKind, ExitSettlementMode, ExitTriggerKind, IneligibilityReason, OutcomeSide,
        QuantRuntimeMode, RecommendationReportStatus, RedeemPolicy, ReportKind,
    },
    types::{
        BookSnapshotRef, Bps, EligibilitySummary, EntryPlan, EquitySnapshotId, EvidenceRefs,
        ExecutionEligibility, ExitPlan, PartialExitNode, Price, RecommendationId,
        RecommendationReportId, ReportSummary, TrailingStop, Usd,
    },
};

use crate::{report_fixtures, seeded_uuid};

/// Full `TopN` report payload: header plus published recommendations.
#[derive(Debug, Clone, Serialize)]
pub struct TopNReportSnapshot {
    pub report: QuantReportDetailView,
    pub recommendations: Vec<QuantRecommendationView>,
}

fn at(ts: i64) -> chrono::DateTime<Utc> {
    Utc.timestamp_opt(ts, 0).unwrap()
}

fn report_id(seed: &str) -> RecommendationReportId {
    RecommendationReportId::new(seeded_uuid(seed))
}

fn recommendation_id(seed: &str) -> RecommendationId {
    RecommendationId::new(seeded_uuid(seed))
}

fn ref_id<T>(seed: &str) -> T
where
    T: From<uuid::Uuid>,
{
    T::from(seeded_uuid(seed))
}

fn book_snapshot_ref() -> BookSnapshotRef {
    BookSnapshotRef::from_str(&format!(
        "book:live:token-abc:1:1700000000@blake3:{}",
        "0".repeat(64)
    ))
    .expect("valid book snapshot ref")
}

/// Summary safe for insta JSON snapshots (non-string map keys are not JSON-serializable).
fn snapshot_summary() -> ReportSummary {
    let mut summary = report_fixtures::report_summary();
    summary.category_allocation = BTreeMap::new();
    summary.event_allocation = BTreeMap::new();
    summary
}

fn base_report(
    seed: &str,
    status: RecommendationReportStatus,
    summary: ReportSummary,
) -> RecommendationReportInfo {
    let id = report_id(seed);
    let mut info = report_fixtures::report(id, ReportKind::TopN, status);
    "scheduled:daily-topn:2023-11-14T22:13:20Z".clone_into(&mut info.trigger_key);
    info.trigger_time = at(1_700_000_000);
    info.as_of = at(1_699_999_880);
    info.runtime_config_version_id = ref_id("snapshot-runtime-config");
    info.model_version_id = ref_id("snapshot-model-version");
    info.market_selection_id = ref_id("snapshot-market-selection");
    info.portfolio_plan_id = ref_id("snapshot-portfolio-plan");
    info.account_snapshot_ref = ref_id("snapshot-account");
    info.equity_snapshot_ref = EquitySnapshotId::new(seeded_uuid("snapshot-equity"));
    info.data_quality_snapshot_ref = ref_id("snapshot-data-quality");
    info.summary_json = summary;
    info.published_at = Some(at(1_700_000_000));
    info.created_at = at(1_700_000_000);
    info
}

fn base_recommendation(
    report_seed: &str,
    rec_seed: &str,
    rank: i32,
    market: &str,
    side: OutcomeSide,
    suggested_usd: Usd,
) -> RecommendationInfo {
    let report_id = report_id(report_seed);
    let id = recommendation_id(rec_seed);
    let mut info =
        report_fixtures::recommendation(report_id, id, rank, market, side, suggested_usd);
    info.evidence_refs = evidence_refs();
    info.valid_from = at(1_700_000_000);
    info.valid_until = at(1_700_086_400);
    info.created_at = at(1_700_000_000);
    info
}

fn evidence_refs() -> EvidenceRefs {
    EvidenceRefs {
        signal_candidate_id: ref_id("snapshot-signal-candidate"),
        feature_vector_id: ref_id("snapshot-feature-vector"),
        model_run_id: ref_id("snapshot-model-run"),
        market_selection_id: ref_id("snapshot-market-selection"),
        book_snapshot_ref: book_snapshot_ref(),
        runtime_config_version_id: ref_id("snapshot-runtime-config"),
        model_version_id: ref_id("snapshot-model-version"),
        factor_definition_versions: Vec::new(),
        data_quality_snapshot_ref: ref_id("snapshot-data-quality"),
    }
}

fn limit_entry_plan() -> EntryPlan {
    EntryPlan {
        trigger_kind: EntryTriggerKind::LimitPrice,
        trigger_price: Some(Price::new(dec!(0.42))),
        limit_price: Some(Price::new(dec!(0.42))),
        max_slippage_bps: Bps::new(dec!(50)),
        valid_from: at(1_700_000_000),
        valid_until: at(1_700_003_600),
        min_depth_usd: Usd::new(dec!(100)),
        max_book_age_ms: 2_000,
        confirmation_window_secs: 30,
        cancel_if_not_triggered: true,
        entry_reason: "limit entry at edge".to_owned(),
    }
}

fn immediate_entry_plan() -> EntryPlan {
    EntryPlan {
        trigger_kind: EntryTriggerKind::Immediate,
        trigger_price: None,
        limit_price: Some(Price::new(dec!(0.43))),
        max_slippage_bps: Bps::new(dec!(50)),
        valid_from: at(1_700_000_000),
        valid_until: at(1_700_003_600),
        min_depth_usd: Usd::new(dec!(100)),
        max_book_age_ms: 2_000,
        confirmation_window_secs: 30,
        cancel_if_not_triggered: false,
        entry_reason: "immediate entry with slippage cap".to_owned(),
    }
}

fn partial_exit_plan() -> ExitPlan {
    ExitPlan {
        take_profit_price: Some(Price::new(dec!(0.7))),
        take_profit_pct: Some(dec!(0.6)),
        stop_loss_price: Some(Price::new(dec!(0.3))),
        stop_loss_pct: Some(dec!(0.3)),
        time_exit_at: None,
        max_hold_secs: Some(86_400),
        partial_exit_nodes: vec![PartialExitNode {
            node_id: "tp1".to_owned(),
            trigger_kind: ExitTriggerKind::TakeProfit,
            trigger_value: dec!(0.6),
            sell_pct: dec!(0.5),
            min_price: Some(Price::new(dec!(0.55))),
            valid_after: None,
            valid_until: None,
            reason: "scale out at first target".to_owned(),
        }],
        trailing_stop: Some(TrailingStop {
            trail_bps: Bps::new(dec!(300)),
            activation_price: Some(Price::new(dec!(0.6))),
        }),
        signal_invalidation_rules: Vec::new(),
        settlement_mode: ExitSettlementMode::HoldToResolution,
        redeem_policy: RedeemPolicy::Manual,
        manual_review_at: None,
        exit_reason: "tp/sl + scaled partial exit".to_owned(),
    }
}

fn not_auto_eligible() -> ExecutionEligibility {
    ExecutionEligibility {
        eligible_modes: vec![QuantRuntimeMode::ReportOnly, QuantRuntimeMode::SemiAuto],
        ineligibility_reasons: vec![IneligibilityReason::LowConfidence],
        approval_required: true,
        auto_policy_id: None,
    }
}

/// Non-empty `TopN` report with two published recommendations.
#[must_use]
pub fn non_empty_topn_report() -> TopNReportSnapshot {
    let summary = snapshot_summary();
    let report = QuantReportDetailView::from(base_report(
        "snapshot-report-topn",
        RecommendationReportStatus::Published,
        summary,
    ));
    let recommendations = vec![
        QuantRecommendationView::from(base_recommendation(
            "snapshot-report-topn",
            "snapshot-rec-1",
            1,
            "0xmarketA",
            OutcomeSide::Yes,
            Usd::new(dec!(300)),
        )),
        QuantRecommendationView::from(base_recommendation(
            "snapshot-report-topn",
            "snapshot-rec-2",
            2,
            "0xmarketB",
            OutcomeSide::No,
            Usd::new(dec!(200)),
        )),
    ];
    TopNReportSnapshot {
        report,
        recommendations,
    }
}

/// Published-empty report with an explicit [`ReportSummary::empty_reason`].
#[must_use]
pub fn empty_report() -> QuantReportDetailView {
    let mut summary = report_fixtures::report_summary();
    summary.published_recommendation_count = 0;
    summary.total_suggested_usd = Usd::ZERO;
    summary.max_single_recommendation_usd = Usd::ZERO;
    summary.category_allocation = BTreeMap::new();
    summary.event_allocation = BTreeMap::new();
    summary.execution_eligibility_summary = EligibilitySummary::default();
    summary.empty_reason = Some(quant_pivot_models::enums::quant::EmptyReason::NoPositiveSignal);
    summary.warnings = vec!["no candidates passed score floor".to_owned()];
    QuantReportDetailView::from(base_report(
        "snapshot-report-empty",
        RecommendationReportStatus::PublishedEmpty,
        summary,
    ))
}

/// Revoked report header (immutable body; revocation via lifecycle fields).
#[must_use]
pub fn revoked_report() -> QuantReportDetailView {
    let mut info = base_report(
        "snapshot-report-revoked",
        RecommendationReportStatus::Revoked,
        snapshot_summary(),
    );
    info.revoked_at = Some(at(1_700_010_000));
    info.status_reason = Some("operator revoked stale report".to_owned());
    QuantReportDetailView::from(info)
}

/// Recommendation with production-default limit-price entry.
#[must_use]
pub fn recommendation_limit_entry() -> QuantRecommendationView {
    let mut rec = base_recommendation(
        "snapshot-report-topn",
        "snapshot-rec-limit",
        1,
        "0xmarketA",
        OutcomeSide::Yes,
        Usd::new(dec!(250)),
    );
    rec.entry_plan = limit_entry_plan();
    QuantRecommendationView::from(rec)
}

/// Recommendation with immediate entry (fixture-only variety).
#[must_use]
pub fn recommendation_immediate_entry() -> QuantRecommendationView {
    let mut rec = base_recommendation(
        "snapshot-report-topn",
        "snapshot-rec-immediate",
        1,
        "0xmarketA",
        OutcomeSide::Yes,
        Usd::new(dec!(250)),
    );
    rec.entry_plan = immediate_entry_plan();
    QuantRecommendationView::from(rec)
}

/// Recommendation with scaled partial-exit nodes.
#[must_use]
pub fn recommendation_partial_exits() -> QuantRecommendationView {
    let mut rec = base_recommendation(
        "snapshot-report-topn",
        "snapshot-rec-partial-exits",
        1,
        "0xmarketA",
        OutcomeSide::Yes,
        Usd::new(dec!(250)),
    );
    rec.exit_plan = partial_exit_plan();
    QuantRecommendationView::from(rec)
}

/// Recommendation withheld from auto-execution while remaining semi-auto eligible.
#[must_use]
pub fn recommendation_not_auto_eligible() -> QuantRecommendationView {
    let mut rec = base_recommendation(
        "snapshot-report-topn",
        "snapshot-rec-not-auto",
        1,
        "0xmarketA",
        OutcomeSide::Yes,
        Usd::new(dec!(250)),
    );
    rec.execution_eligibility = not_auto_eligible();
    QuantRecommendationView::from(rec)
}
