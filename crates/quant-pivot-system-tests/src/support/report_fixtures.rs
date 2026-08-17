//! Strong-typed recommendation report fixtures owned by system tests.
//!
//! Shared by `quant-pivot-core` and `quant-pivot-web` tests so report-plane
//! fixtures (header, summary, and the full per-recommendation payload blocks)
//! are built one way. Defaults are sensible; knobs cover the dimensions tests
//! actually vary (id, status, kind, market, side, rank, hard reservation).

use std::{collections::BTreeMap, str::FromStr};

use chrono::{Duration, TimeZone, Utc};
use quant_pivot_models::{
    domain::{
        api::{FeatureParityJobParams, RunFullFeatureParityRequest},
        market::fee::ImmediateExecutionCost,
        quant::{
            EntryExecutionEconomics, ExecutableEconomicTier, HardReservationBucket,
            NewFeatureParityRun, NewRecommendationReport, NewReportFactDelivery,
            NewReportFeatureParity, NewResearchJob, PassiveEntryEconomics, RecommendationEconomics,
            RecommendationInfo, RecommendationReportInfo, RepresentedRouteSet,
            ScenarioCapitalOccupancySlice, ScenarioEntryExecution, ScenarioExecutionCashflow,
        },
    },
    enums::{
        common::{MarketCategory, TickSize::Hundredth},
        factor::{FactorFamily, FactorValueState, NormalizationSource},
        market::MarketStatus,
        quant::{
            AccountSource, ExitSettlementMode, FactorDirection, FeatureParityRunKind,
            FeatureParityRunStatus, IneligibilityReason, OutcomeSide, QuantRuntimeMode,
            RecommendationReportStatus, RecommendationStatus, RedeemPolicy,
            ReportFactDeliveryStatus, ReportKind, ResearchJobKind, ResearchJobStatus,
        },
    },
    runtime_config::BuyModelRoute,
    types::{
        AccountSnapshotId, ArtifactUri, BookSnapshotRef, Bps, ContentHash, DataQualitySummary,
        DecisionPolicySnapshotId, EconomicTierId, EligibilitySummary, EntryConditionPlan,
        EntryOrderPolicy, EntryPlan, EquitySnapshotId, EventId, EvidenceRefs, ExecutionEligibility,
        ExitPlan, FactorBreakdownEntry, FeatureParityRunId, FeatureVectorId, MarketContext,
        MarketId, MarketSelectionId, ModelRunId, ModelVersionId, OpportunisticExitPolicy,
        PassiveFillDistribution, PassiveFillState, PassiveFillStateKind, PortfolioPlanId,
        PortfolioScenarioArtifactId, Price, Probability, RecommendationFactorBreakdown,
        RecommendationId, RecommendationIdentity, RecommendationReportId, RecommendationTradePlan,
        ReportDataQualitySnapshotId, ReportRouteRunId, ReportRunId, ReportSummary, ResearchJobId,
        ResearchJobParams, RiskEnvelope, RoleCode, Shares, SignalCandidateId, SizingPlan,
        ThesisInvalidationPolicy, TokenId, TradePolicyArtifactId, TradePolicyCohortDimension,
        TradePolicyCohortKey, TradePolicyCohortProvenance, TradePolicyEntryRoute, Usd, UsdHours,
        builtin_research_profiles,
    },
};
use rust_decimal_macros::dec;

fn content_hash() -> ContentHash {
    ContentHash::parse(&format!("blake3:{}", "0".repeat(64))).expect("valid hash")
}

/// Build a pending fixture outbox row for repository-level report tests.
#[must_use]
pub fn pending_fact_delivery(report_id: &RecommendationReportId) -> NewReportFactDelivery {
    NewReportFactDelivery {
        recommendation_report_id: *report_id,
        status: ReportFactDeliveryStatus::Pending,
        bundle_uri: ArtifactUri::parse(format!("fixture://report-facts/{report_id}.json"))
            .expect("valid fixture URI"),
        bundle_hash: content_hash(),
        bundle_bytes: 2,
        recommendation_row_count: 0,
        recommendation_row_chain_hash: content_hash(),
        funnel_row_count: 0,
        funnel_row_chain_hash: content_hash(),
    }
}

/// Build the mandatory sampled-parity run/job committed atomically with a
/// fixture report.
///
/// Production code uses `FeatureParityRunCoordinator`; this
/// helper mirrors only the repository contract so persistence tests cannot
/// bypass the invariant.
#[must_use]
pub fn sampled_parity(report: &NewRecommendationReport) -> NewReportFeatureParity {
    let run_id = FeatureParityRunId::from_v7();
    let window_end = report.decision_at + Duration::milliseconds(1);
    let reason = format!(
        "fixture sampled replay for report {}",
        report.recommendation_report_id
    );
    let request = RunFullFeatureParityRequest {
        window_start: Some(report.decision_at),
        window_end: Some(window_end),
        reason: reason.clone(),
    };
    let run = NewFeatureParityRun {
        run_id,
        kind: FeatureParityRunKind::Sampled,
        status: FeatureParityRunStatus::Queued,
        window_start: report.decision_at,
        window_end,
        report_id: Some(report.recommendation_report_id),
        model_version_id: None,
        training_dataset_id: None,
        triggered_by: "test:fixture".to_owned(),
        requested_by: None,
        acting_role: RoleCode::new("test"),
        reason,
        total_count: 0,
        compared_count: 0,
        matched_count: 0,
        mismatched_count: 0,
        pending_materialization_count: 0,
        feature_contract_hash: Some(content_hash()),
        transform_hash: None,
        failure_code: None,
        failure_detail: None,
        started_at: None,
        pending_since: None,
        containment_completed_at: None,
        finished_at: None,
    };
    let params = FeatureParityJobParams {
        parity_run_id: run_id,
        materialization_timeout_secs: 600,
        request,
    };
    let job = NewResearchJob {
        job_id: ResearchJobId::from_v7(),
        feedback_cycle_id: None,
        feedback_stage: None,
        kind: ResearchJobKind::FeatureParity,
        status: ResearchJobStatus::Queued,
        model_spec_id: None,
        decision_policy_snapshot_id: Some(report.decision_policy_snapshot_id),
        params_json: ResearchJobParams::FeatureParity(params),
        requested_by: None,
        acting_role: RoleCode::new("test"),
        parent_job_id: None,
        recovery_attempt: 0,
        max_recovery_attempts: 0,
    };
    NewReportFeatureParity { run, job }
}

/// A report header fixture with the given id / kind / status.
#[must_use]
pub fn report(
    id: RecommendationReportId,
    kind: ReportKind,
    status: RecommendationReportStatus,
) -> RecommendationReportInfo {
    let represented_routes =
        RepresentedRouteSet::from_routes([BuyModelRoute::Pooled]).expect("Route set");
    let scenario_hash = content_hash();
    RecommendationReportInfo {
        recommendation_report_id: id,
        report_run_id: ReportRunId::from_v7(),
        report_kind: kind,
        decision_at: Utc.timestamp_opt(1_699_999_880, 0).unwrap(),
        runtime_mode: QuantRuntimeMode::ReportOnly,
        decision_policy_snapshot_id: DecisionPolicySnapshotId::from_v7(),
        market_selection_id: MarketSelectionId::from_v7(),
        portfolio_plan_id: PortfolioPlanId::from_v7(),
        represented_routes_json: represented_routes,
        scenario_artifact_id: Some(PortfolioScenarioArtifactId::from_content_hash(
            &scenario_hash,
        )),
        scenario_artifact_hash: Some(scenario_hash),
        top_n: 20,
        status,
        account_source: AccountSource::Polymarket,
        capital_base_usd: Usd::new(dec!(10000)),
        account_snapshot_ref: AccountSnapshotId::from_v7(),
        equity_snapshot_ref: EquitySnapshotId::from_v7(),
        data_quality_snapshot_ref: ReportDataQualitySnapshotId::from_v7(),
        summary_json: report_summary(),
        published_at: Some(Utc.timestamp_opt(1_700_000_000, 0).unwrap()),
        successor_report_id: None,
        superseded_at: None,
        obsoleted_at: None,
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
        represented_route_count: 1,
        candidate_count: 8,
        rejected_tier_count: 3,
        published_recommendation_count: 2,
        total_hard_reserved_cash_usd: Usd::new(dec!(500)),
        max_single_recommendation_usd: Usd::new(dec!(300)),
        robust_expected_net_usd: Usd::new(dec!(75)),
        nominal_expected_net_usd: Usd::new(dec!(95)),
        cvar_usd: Usd::new(dec!(120)),
        maximum_scenario_loss_usd: Usd::new(dec!(140)),
        capital_occupancy_usd_hours: UsdHours::new(dec!(12000)),
        category_allocation,
        event_allocation,
        route_allocation: BTreeMap::from([(BuyModelRoute::Pooled, Usd::new(dec!(500)))]),
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
    hard_reserved_cash_usd: Usd,
) -> RecommendationInfo {
    let economics = recommendation_economics();
    let tier = economic_tier(market, side, hard_reserved_cash_usd, economics);
    RecommendationInfo {
        recommendation_id: id,
        recommendation_report_id: report_id,
        report_route_run_id: tier.report_route_run_id,
        portfolio_plan_id: PortfolioPlanId::from_v7(),
        economic_tier_id: tier.economic_tier_id,
        rank,
        route: BuyModelRoute::Pooled,
        market_id: MarketId::new(market),
        event_id: EventId::new("evt-1"),
        token_id: TokenId::new(format!("token-{market}")),
        outcome_side: side,
        economics_json: economics,
        economic_tier_json: tier,
        identity: recommendation_identity(),
        market_context: market_context(),
        trade_plan: RecommendationTradePlan {
            policy: Box::new(trade_policy_provenance().into()),
            entry: entry_plan(),
            sizing: Box::new(sizing_plan(hard_reserved_cash_usd)),
            exit: Box::new(exit_plan().into()),
            risk_envelope: Box::new(risk_envelope()),
        },
        factor_breakdown: factor_breakdown(),
        evidence_refs: evidence_refs(),
        execution_eligibility: execution_eligibility(),
        valid_from: Utc.timestamp_opt(1_700_000_000, 0).unwrap(),
        valid_until: Utc.timestamp_opt(1_700_086_400, 0).unwrap(),
        status: RecommendationStatus::Published,
        status_changed_at: Utc.timestamp_opt(1_700_000_000, 0).unwrap(),
        created_at: Utc.timestamp_opt(1_700_000_000, 0).unwrap(),
    }
}

const fn recommendation_economics() -> RecommendationEconomics {
    RecommendationEconomics {
        profit_probability_bps: Bps::new(dec!(6200)),
        nominal_expected_net_usd: Usd::new(dec!(55)),
        robust_expected_net_usd: Usd::new(dec!(42)),
        max_loss_usd: Usd::new(dec!(120)),
        cvar_contribution_usd: Usd::new(dec!(40)),
        capital_occupancy_usd_hours: UsdHours::new(dec!(12000)),
        marginal_portfolio_value_usd: Usd::new(dec!(38)),
    }
}

fn economic_tier(
    market: &str,
    side: OutcomeSide,
    hard_reserved_cash_usd: Usd,
    economics: RecommendationEconomics,
) -> ExecutableEconomicTier {
    let lineage_hash = content_hash();
    let limit_price = Price::new(dec!(0.43));
    let requested_shares = Shares::new(hard_reserved_cash_usd.inner() / limit_price.inner());
    let full_fill_cost = ImmediateExecutionCost::new(hard_reserved_cash_usd, Usd::ZERO, Usd::ZERO)
        .expect("valid passive full-fill cost");
    ExecutableEconomicTier {
        economic_tier_id: EconomicTierId::from_content_hash(&lineage_hash),
        report_route_run_id: ReportRouteRunId::from_v7(),
        candidate_id: SignalCandidateId::from_v7(),
        tier_ordinal: 1,
        route: BuyModelRoute::Pooled,
        market_id: MarketId::new(market),
        event_id: EventId::new("evt-1"),
        category: MarketCategory::Politics,
        token_id: TokenId::new(format!("token-{market}")),
        outcome_side: side,
        entry_execution: EntryExecutionEconomics::Passive(Box::new(PassiveEntryEconomics {
            requested_shares,
            limit_price,
            decision_at: Utc.timestamp_opt(1_700_000_000, 0).unwrap(),
            good_til_secs: 3_600,
            hard_reserved_cash_usd,
            expected_filled_shares: requested_shares,
            full_fill_cost,
            fill_distribution: PassiveFillDistribution {
                sample_count: 100,
                source_evidence_hash: content_hash(),
                states: vec![PassiveFillState {
                    kind: PassiveFillStateKind::FullFill,
                    probability_bps: 10_000,
                    fill_ratio_bps: 10_000,
                    fill_latency_ms: 1_000,
                    post_fill_markout_bps: Bps::ZERO,
                }],
            },
            maker_rebate_schedule: None,
            full_fill_maker_rebate: None,
            expected_maker_rebate_usd: Usd::ZERO,
            visible_liquidity_usd: Usd::new(dec!(5000)),
        })),
        profit_probability_lower_bps: 5_800,
        probability_interval_width_bps: 800,
        scenario_cashflows: vec![
            ScenarioExecutionCashflow {
                scenario_index: 0,
                entry_execution: ScenarioEntryExecution::PassiveFullFill {
                    fill_latency_ms: 1_000,
                    post_fill_markout_bps: Bps::ZERO,
                },
                filled_shares: requested_shares,
                immediate_cash_outlay_usd: hard_reserved_cash_usd,
                discounted_exit_cash_usd: hard_reserved_cash_usd + Usd::new(dec!(90)),
                delayed_maker_rebate_usd: Usd::ZERO,
                discounted_maker_rebate_usd: Usd::ZERO,
                capital_cost_usd: Usd::ZERO,
                capital_occupancy: vec![ScenarioCapitalOccupancySlice {
                    locked_cash_usd: hard_reserved_cash_usd,
                    duration_secs: 86_400,
                }],
                discounted_net_usd: Usd::new(dec!(90)),
                risk_net_usd: Usd::new(dec!(90)),
            },
            ScenarioExecutionCashflow {
                scenario_index: 1,
                entry_execution: ScenarioEntryExecution::PassiveFullFill {
                    fill_latency_ms: 1_000,
                    post_fill_markout_bps: Bps::ZERO,
                },
                filled_shares: requested_shares,
                immediate_cash_outlay_usd: hard_reserved_cash_usd,
                discounted_exit_cash_usd: hard_reserved_cash_usd - Usd::new(dec!(120)),
                delayed_maker_rebate_usd: Usd::ZERO,
                discounted_maker_rebate_usd: Usd::ZERO,
                capital_cost_usd: Usd::ZERO,
                capital_occupancy: vec![ScenarioCapitalOccupancySlice {
                    locked_cash_usd: hard_reserved_cash_usd,
                    duration_secs: 86_400,
                }],
                discounted_net_usd: Usd::new(dec!(-120)),
                risk_net_usd: Usd::new(dec!(-120)),
            },
        ],
        hard_reservation_envelope: vec![HardReservationBucket {
            end_secs: 86_400,
            reserved_cash_usd: hard_reserved_cash_usd,
        }],
        economics,
        lineage_hash,
    }
}

fn entry_plan() -> EntryPlan {
    EntryPlan {
        condition: EntryConditionPlan::Immediate,
        order_policy: EntryOrderPolicy::Passive {
            limit_price: Price::new(dec!(0.43)),
            post_only: true,
        },
        max_slippage_bps: Bps::new(dec!(50)),
        valid_from: Utc.timestamp_opt(1_700_000_000, 0).unwrap(),
        valid_until: Utc.timestamp_opt(1_700_003_600, 0).unwrap(),
        min_depth_usd: Usd::new(dec!(100)),
        max_book_age_ms: 2_000,
        cancel_if_not_triggered: true,
        entry_reason: "limit entry at edge".to_owned(),
    }
}

fn sizing_plan(hard_reserved_cash_usd: Usd) -> SizingPlan {
    let tier_id = EconomicTierId::from_content_hash(&content_hash());
    SizingPlan {
        economic_tier_id: tier_id,
        requested_shares: Shares::new(hard_reserved_cash_usd.inner() / dec!(0.43)),
        expected_filled_shares: Shares::new(hard_reserved_cash_usd.inner() / dec!(0.43)),
        hard_reserved_cash_usd,
        immediate_fee_usd: Usd::ZERO,
        expected_maker_rebate_usd: Usd::ZERO,
        maker_rebate_schedule: None,
        reference_entry_price: Price::new(dec!(0.43)),
        portfolio_weight_pct: dec!(0.05),
        market_exposure_after_usd: hard_reserved_cash_usd,
        event_exposure_after_usd: hard_reserved_cash_usd,
        category_exposure_after_usd: hard_reserved_cash_usd,
        route_exposure_after_usd: hard_reserved_cash_usd,
        capital_occupancy_usd_hours: UsdHours::new(dec!(12000)),
        sizing_reason: "selected executable tier from the exact global MILP".to_owned(),
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
        scale_out_targets: vec![],
        trailing_stop: None,
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
        exit_reason: "tp/sl".to_owned(),
    }
}

fn trade_policy_provenance() -> TradePolicyCohortProvenance {
    let artifact_hash = content_hash();
    let dimension = TradePolicyCohortDimension {
        methodology_id: "fixture-v1".to_owned(),
        methodology_hash: artifact_hash,
        bucket_id: "fixture".to_owned(),
    };
    TradePolicyCohortProvenance {
        artifact_id: TradePolicyArtifactId::from_content_hash(&artifact_hash),
        artifact_hash,
        cohort_index: 0,
        cohort_key: TradePolicyCohortKey {
            profile_ref: builtin_research_profiles()
                .expect("research profiles")
                .into_iter()
                .next()
                .expect("control profile")
                .profile_ref,
            category: MarketCategory::Politics,
            horizon_secs: 86_400,
            entry_route: TradePolicyEntryRoute::Aggressive,
            entry_price_min: Price::new(dec!(0.01)),
            entry_price_max: Price::new(dec!(0.99)),
            cash_budget_tier: Usd::new(dec!(250)),
            liquidity: dimension.clone(),
            volatility: dimension,
        },
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
        max_route_exposure_usd: Usd::new(dec!(1500)),
        cvar_contribution_usd: Usd::new(dec!(40)),
        portfolio_cvar_cap_usd: Usd::new(dec!(500)),
        maximum_scenario_loss_cap_usd: Usd::new(dec!(750)),
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
        tick_size: Hundredth,
        fee_rate: None,
    }
}

fn book_snapshot_ref() -> BookSnapshotRef {
    BookSnapshotRef::from_str(&format!(
        "book:l2|token-abc|00000000-0000-0000-0000-000000000001|1|blake3:{}|1700000000|1700000000@blake3:{}",
        "1".repeat(64),
        "0".repeat(64),
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
        decision_policy_snapshot_id: DecisionPolicySnapshotId::from_v7(),
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
