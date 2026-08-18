//! Deterministic report payload fixtures for insta JSON snapshot tests.
//!
//! Covers non-empty `TopN`, empty, limit/immediate entry, partial exits,
//! not-auto-eligible, and revoked report header. Uses [`seeded_uuid`] so snapshots
//! stay stable across runs.

use std::{collections::BTreeMap, str::FromStr};

use chrono::{DateTime, TimeZone, Utc};
use quant_pivot_models::{
    domain::{
        api::{QuantRecommendationView, QuantReportDetailView, RecommendationViewContext},
        quant::{
            ExactVerificationEvidence, GlobalPortfolioPlan, PortfolioConstraintEvidence,
            PortfolioDecisionResult, PortfolioObjectiveEvidence, RecommendationInfo,
            RecommendationReportInfo, SolverEvidence,
        },
    },
    enums::quant::{
        EmptyReportReason, ExecutionAuthorityCeiling, ExitSettlementMode, FillRequirement,
        IneligibilityReason, OutcomeSide, RecommendationReportStatus, RedeemPolicy, ReportKind,
    },
    types::{
        BookSnapshotRef, Bps, ContentHash, EconomicTierId, EligibilitySummary,
        EntryConditionArtifactId, EntryConditionPlan, EntryOrderPolicy, EntryPlan,
        EquitySnapshotId, EvidenceRefs, ExecutionEligibility, ExitPlan, OpportunisticExitPolicy,
        PortfolioPlanId, Price, Probability, RecommendationId, RecommendationReportId,
        ReportSummary, ScaleOutTarget, ThesisInvalidationPolicy, TrailingStopPolicy, Usd, UsdHours,
    },
};
use rust_decimal_macros::dec;
use serde::Serialize;
use uuid::Uuid;

use super::{report_fixtures, seeded_uuid};

/// Full `TopN` report payload: header plus published recommendations.
#[derive(Debug, Clone, Serialize)]
pub struct TopNReportSnapshot {
    pub report: QuantReportDetailView,
    pub recommendations: Vec<QuantRecommendationView>,
}

fn at(ts: i64) -> DateTime<Utc> {
    Utc.timestamp_opt(ts, 0).unwrap()
}

/// Project a fixture recommendation into its view for snapshots: parent report
/// published, no blocking intent.
fn view(recommendation: RecommendationInfo) -> QuantRecommendationView {
    RecommendationViewContext {
        recommendation,
        report_status: RecommendationReportStatus::Published,
        active_order_intent_id: None,
    }
    .into()
}

fn report_id(seed: &str) -> RecommendationReportId {
    RecommendationReportId::new(seeded_uuid(seed))
}

fn recommendation_id(seed: &str) -> RecommendationId {
    RecommendationId::new(seeded_uuid(seed))
}

fn ref_id<T>(seed: &str) -> T
where
    T: From<Uuid>,
{
    T::from(seeded_uuid(seed))
}

fn book_snapshot_ref() -> BookSnapshotRef {
    BookSnapshotRef::from_str(&format!(
        "book:l2|token-abc|00000000-0000-0000-0000-000000000001|1|blake3:{}|1700000000|1700000000@blake3:{}",
        "1".repeat(64),
        "0".repeat(64),
    ))
    .expect("valid book snapshot ref")
}

/// Summary safe for insta JSON snapshots (non-string map keys are not JSON-serializable).
fn snapshot_summary() -> ReportSummary {
    let mut summary = report_fixtures::report_summary();
    summary.category_allocation = BTreeMap::new();
    summary.event_allocation = BTreeMap::new();
    summary.route_allocation = BTreeMap::new();
    summary
}

fn base_report(
    seed: &str,
    status: RecommendationReportStatus,
    summary: ReportSummary,
) -> RecommendationReportInfo {
    let id = report_id(seed);
    let mut info = report_fixtures::report(id, ReportKind::TopN, status);
    info.report_run_id = ref_id(&format!("snapshot-report-run:{seed}"));
    info.decision_at = at(1_699_999_880);
    info.decision_policy_snapshot_id = ref_id("snapshot-runtime-config");
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

fn fixture_hash(seed: char) -> ContentHash {
    ContentHash::parse(&format!("blake3:{}", seed.to_string().repeat(64)))
        .expect("snapshot content hash")
}

fn optimized_decision(portfolio_plan_id: PortfolioPlanId) -> PortfolioDecisionResult {
    let selected_tier_ids = vec![
        EconomicTierId::new(seeded_uuid("snapshot-tier-1")),
        EconomicTierId::new(seeded_uuid("snapshot-tier-2")),
    ];
    PortfolioDecisionResult::Optimized {
        plan: Box::new(GlobalPortfolioPlan {
            portfolio_plan_id,
            selected_tier_ids,
            objectives: PortfolioObjectiveEvidence {
                robust_expected_net_usd: Usd::new(dec!(18.25)),
                nominal_expected_net_usd: Usd::new(dec!(24.50)),
                cvar_usd: Usd::new(dec!(82.00)),
                capital_occupancy_usd_hours: UsdHours::new(dec!(9600)),
                stable_tie_break_stages: 2,
            },
            constraints: PortfolioConstraintEvidence {
                available_cash_used_usd: Usd::new(dec!(500)),
                open_capital_usd: Usd::new(dec!(500)),
                selected_recommendation_count: 2,
                maximum_scenario_loss_usd: Usd::new(dec!(110)),
                checked_constraint_count: 14,
                evidence_hash: fixture_hash('d'),
            },
            solver: SolverEvidence {
                backend: "highs".to_owned(),
                lexicographic_model_build_count: 1,
                lexicographic_solve_count: 7,
                tie_break_proof_count: 1,
                lexicographic_warm_start_count: 6,
                marginal_model_build_count: 0,
                marginal_solve_count: 2,
                marginal_model_reuse_count: 2,
                configured_deadline_secs: 30,
                deterministic_threads: 1,
                coefficient_scale: 10_000,
                bound_scale_exponent: 0,
                optimal: true,
            },
            exact_verification: ExactVerificationEvidence {
                passed: true,
                selected_tier_digest: fixture_hash('e'),
                recomputed_economics_hash: fixture_hash('f'),
            },
            content_hash: fixture_hash('a'),
        }),
    }
}

fn detail_view(info: RecommendationReportInfo) -> QuantReportDetailView {
    let decision = if info.summary_json.published_recommendation_count == 0 {
        PortfolioDecisionResult::ZeroCandidates {
            rejected_tier_count: info.summary_json.rejected_tier_count,
            evidence_hash: fixture_hash('b'),
        }
    } else {
        optimized_decision(info.portfolio_plan_id)
    };
    QuantReportDetailView::from_parts(info, None, None, None, decision)
}

fn base_recommendation(
    report_seed: &str,
    rec_seed: &str,
    rank: i32,
    market: &str,
    side: OutcomeSide,
    hard_reserved_cash_usd: Usd,
) -> RecommendationInfo {
    let report_id = report_id(report_seed);
    let id = recommendation_id(rec_seed);
    let mut info =
        report_fixtures::recommendation(report_id, id, rank, market, side, hard_reserved_cash_usd);
    let report_route_run_id = ref_id(&format!("snapshot-route-run:{report_seed}:pooled"));
    let candidate_id = ref_id(&format!("snapshot-candidate:{rec_seed}"));
    let economic_tier_id = ref_id(&format!("snapshot-tier:{rec_seed}:1"));
    info.report_route_run_id = report_route_run_id;
    info.portfolio_plan_id = ref_id("snapshot-portfolio-plan");
    info.economic_tier_id = economic_tier_id;
    info.economic_tier_json.report_route_run_id = report_route_run_id;
    info.economic_tier_json.candidate_id = candidate_id;
    info.economic_tier_json.economic_tier_id = economic_tier_id;
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
        decision_policy_snapshot_id: ref_id("snapshot-runtime-config"),
        model_version_id: ref_id("snapshot-model-version"),
        factor_definition_versions: Vec::new(),
        data_quality_snapshot_ref: ref_id("snapshot-data-quality"),
    }
}

fn limit_entry_plan() -> EntryPlan {
    EntryPlan {
        condition: EntryConditionPlan::Conditional {
            artifact_id: EntryConditionArtifactId::new(seeded_uuid("snapshot-condition")),
            content_hash: ContentHash::parse(&format!("blake3:{}", "c".repeat(64)))
                .expect("condition hash"),
        },
        order_policy: EntryOrderPolicy::Passive {
            limit_price: Price::new(dec!(0.42)),
            post_only: true,
        },
        max_slippage_bps: Bps::new(dec!(50)),
        valid_from: at(1_700_000_000),
        valid_until: at(1_700_003_600),
        min_depth_usd: Usd::new(dec!(100)),
        max_book_age_ms: 2_000,
        cancel_if_not_triggered: true,
        entry_reason: "limit entry at edge".to_owned(),
    }
}

fn immediate_entry_plan() -> EntryPlan {
    EntryPlan {
        condition: EntryConditionPlan::Immediate,
        order_policy: EntryOrderPolicy::Aggressive {
            worst_price: Price::new(dec!(0.43)),
            fill_requirement: FillRequirement::AllowPartial,
        },
        max_slippage_bps: Bps::new(dec!(50)),
        valid_from: at(1_700_000_000),
        valid_until: at(1_700_003_600),
        min_depth_usd: Usd::new(dec!(100)),
        max_book_age_ms: 2_000,
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
        scale_out_targets: vec![ScaleOutTarget {
            target_id: "tp1".to_owned(),
            trigger_price: Price::new(dec!(0.6)),
            target_cumulative_exit_pct: dec!(0.5),
            min_price: Some(Price::new(dec!(0.55))),
            valid_after: None,
            valid_until: None,
            reason: "scale out at first target".to_owned(),
        }],
        trailing_stop: Some(TrailingStopPolicy {
            trail_bps: Bps::new(dec!(300)),
            activation_price: Some(Price::new(dec!(0.6))),
        }),
        thesis_invalidation: ThesisInvalidationPolicy {
            min_score_retention: dec!(0.6),
            min_expected_return_bps: Bps::ZERO,
            require_route_gate_eligibility: true,
        },
        opportunistic_exit: OpportunisticExitPolicy {
            min_confidence: Probability::new(dec!(0.65)),
            min_expected_alpha_bps: Bps::new(dec!(50)),
            min_p_exit_better: Probability::new(dec!(0.5)),
            max_cumulative_exit_pct: dec!(1),
            min_incremental_exit_pct: dec!(0.1),
        },
        settlement_mode: ExitSettlementMode::HoldToResolution,
        redeem_policy: RedeemPolicy::Manual,
        manual_review_at: None,
        exit_reason: "tp/sl + scaled partial exit".to_owned(),
    }
}

fn not_auto_eligible() -> ExecutionEligibility {
    ExecutionEligibility {
        ceiling: ExecutionAuthorityCeiling::OperatorApproval,
        blockers: vec![IneligibilityReason::AutomationCapExceeded],
        policy_binding: None,
    }
}

impl TopNReportSnapshot {
    /// Non-empty `TopN` report with two published recommendations.
    #[must_use]
    pub fn non_empty() -> Self {
        let summary = snapshot_summary();
        let report = detail_view(base_report(
            "snapshot-report-topn",
            RecommendationReportStatus::Published,
            summary,
        ));
        let recommendations = vec![
            view(base_recommendation(
                "snapshot-report-topn",
                "snapshot-rec-1",
                1,
                "0xmarketA",
                OutcomeSide::Yes,
                Usd::new(dec!(300)),
            )),
            view(base_recommendation(
                "snapshot-report-topn",
                "snapshot-rec-2",
                2,
                "0xmarketB",
                OutcomeSide::No,
                Usd::new(dec!(200)),
            )),
        ];
        Self {
            report,
            recommendations,
        }
    }
}

/// Published-empty report with an explicit [`ReportSummary::empty_reason`].
#[must_use]
pub fn empty_report() -> QuantReportDetailView {
    let mut summary = report_fixtures::report_summary();
    summary.published_recommendation_count = 0;
    summary.total_hard_reserved_cash_usd = Usd::ZERO;
    summary.max_single_recommendation_usd = Usd::ZERO;
    summary.category_allocation = BTreeMap::new();
    summary.event_allocation = BTreeMap::new();
    summary.route_allocation = BTreeMap::new();
    summary.execution_eligibility_summary = EligibilitySummary::default();
    summary.empty_reason = Some(EmptyReportReason::NoPositiveSignal);
    summary.warnings = vec!["no candidates passed score floor".to_owned()];
    detail_view(base_report(
        "snapshot-report-empty",
        RecommendationReportStatus::Published,
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
    detail_view(info)
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
    rec.trade_plan.entry = limit_entry_plan();
    view(rec)
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
    rec.trade_plan.entry = immediate_entry_plan();
    view(rec)
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
    *rec.trade_plan.exit = partial_exit_plan().into();
    view(rec)
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
    view(rec)
}
