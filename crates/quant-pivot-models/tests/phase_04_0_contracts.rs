//! Phase 04.0 data-contract acceptance tests.
//!
//! Covers the strong-typed report/account payloads (serde round-trip ↔ JSON
//! columns), the report header account columns, runtime-config v3 (three-section
//! portfolio + schedule cadence), config validation, and exposure aggregation.

use std::collections::BTreeMap;

use chrono::{TimeZone, Utc};
use rust_decimal_macros::dec;
use serde_json::json;

use quant_pivot_models::{
    domain::{NewAccountSnapshot, NewRecommendationReport},
    enums::{
        common::MarketCategory,
        factor::FactorFamily,
        quant::{
            AccountSource, BindingConstraint, EntryTriggerKind, ExitTriggerKind, FactorDirection,
            IneligibilityReason, QuantRuntimeMode, RecommendationReportStatus, ReportKind,
            ReportTriggerKind, SettlementPolicy, SizingModelKind,
        },
    },
    runtime_config::{
        ConfidenceSizeCurve, DecimalString, DrawdownMultiplierPolicy, RuntimeConfig,
        ScheduleCadence, SizingModelConfig, validate_runtime_config,
    },
    types::{
        AccountPositions, AccountSnapshotId, Bps, ConfidenceSummary, ContentHash,
        DataQualitySummary, EligibilitySummary, EntryPlan, EventId, EvidenceRefs,
        ExecutionEligibility, ExitPlan, ExposureBreakdown, FactorBreakdownEntry, FeatureVectorId,
        MarketId, MarketSelectionId, ModelRunId, ModelVersionId, PartialExitNode, PortfolioPlanId,
        PositionSnapshot, Price, Probability, RecommendationFactorBreakdown,
        RecommendationReportId, ReportSummary, RiskEnvelope, RuntimeConfigVersionId, Shares,
        SignalCandidateId, SizingPlan, TokenId, TrailingStop, Usd,
    },
};

fn content_hash() -> ContentHash {
    ContentHash::parse(format!("blake3:{}", "0".repeat(64))).expect("valid hash")
}

fn roundtrip<T>(value: &T) -> T
where
    T: serde::Serialize + serde::de::DeserializeOwned + PartialEq + std::fmt::Debug,
{
    let json = serde_json::to_value(value).expect("serialize");
    let back: T = serde_json::from_value(json).expect("deserialize");
    assert_eq!(value, &back, "serde round-trip must be stable");
    back
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
        confirmation_window_secs: 30,
        cancel_if_not_triggered: true,
        entry_reason: "limit entry at edge".to_owned(),
    }
}

fn sizing_plan() -> SizingPlan {
    SizingPlan {
        suggested_usd: Usd::new(dec!(250)),
        suggested_shares: Shares::new(dec!(581.395348837209302325)),
        max_usd: Usd::new(dec!(500)),
        min_usd: Usd::new(dec!(10)),
        portfolio_weight_pct: dec!(0.05),
        market_exposure_after_usd: Usd::new(dec!(250)),
        event_exposure_after_usd: Usd::new(dec!(250)),
        category_exposure_after_usd: Usd::new(dec!(250)),
        binding_constraint: BindingConstraint::KellyCap,
        sizing_reason: "half-kelly capped".to_owned(),
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
        partial_exit_nodes: vec![PartialExitNode {
            node_id: "tp1".to_owned(),
            trigger_kind: ExitTriggerKind::TakeProfit,
            trigger_value: dec!(0.6),
            sell_pct: dec!(0.5),
            min_price: Some(Price::new(dec!(0.55))),
            valid_after: None,
            valid_until: None,
            reason: "scale out".to_owned(),
        }],
        trailing_stop: Some(TrailingStop {
            trail_bps: Bps::new(dec!(300)),
            activation_price: Some(Price::new(dec!(0.6))),
        }),
        signal_invalidation_rules: Vec::new(),
        settlement_policy: SettlementPolicy::HoldToResolution,
        manual_review_at: None,
        exit_reason: "tp/sl + trailing".to_owned(),
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

fn evidence_refs() -> EvidenceRefs {
    EvidenceRefs {
        signal_candidate_id: SignalCandidateId::from_v7(),
        feature_vector_id: FeatureVectorId::from_v7(),
        model_run_id: ModelRunId::from_v7(),
        market_selection_id: MarketSelectionId::from_v7(),
        book_snapshot_ref: Some("book:abc".to_owned()),
        runtime_config_version_id: RuntimeConfigVersionId::from_v7(),
        model_version_id: ModelVersionId::from_v7(),
        factor_definition_versions: Vec::new(),
        data_quality_report_ref: None,
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

fn position(
    market: &str,
    event: Option<&str>,
    category: MarketCategory,
    value: Usd,
) -> PositionSnapshot {
    PositionSnapshot {
        token_id: TokenId::new(format!("token-{market}")),
        market_id: MarketId::new(market),
        event_id: event.map(EventId::new),
        category,
        outcome: "Yes".to_owned(),
        size: Shares::new(dec!(100)),
        avg_price: Price::new(dec!(0.5)),
        cur_price: Price::new(dec!(0.6)),
        current_value: value,
        redeemable: false,
    }
}

fn report_summary() -> ReportSummary {
    let mut category_allocation = BTreeMap::new();
    category_allocation.insert(MarketCategory::Politics, Usd::new(dec!(250)));
    let mut event_allocation = BTreeMap::new();
    event_allocation.insert(EventId::new("evt-1"), Usd::new(dec!(250)));
    ReportSummary {
        market_selection_count: 12,
        candidate_count: 8,
        rejected_count: 3,
        published_recommendation_count: 5,
        total_suggested_usd: Usd::new(dec!(1250)),
        max_single_recommendation_usd: Usd::new(dec!(500)),
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
            eligible_report_only: 5,
            eligible_semi_auto: 0,
            eligible_auto_execution: 0,
        },
        empty_reason: None,
        warnings: Vec::new(),
    }
}

#[test]
fn report_payload_serde_roundtrip() {
    roundtrip(&entry_plan());
    roundtrip(&sizing_plan());
    roundtrip(&exit_plan());
    roundtrip(&risk_envelope());
    roundtrip(&factor_breakdown());
    roundtrip(&evidence_refs());
    roundtrip(&execution_eligibility());
    roundtrip(&report_summary());
}

#[test]
fn recommendation_report_header_has_account_columns() {
    let report = NewRecommendationReport {
        recommendation_report_id: RecommendationReportId::from_v7(),
        report_kind: ReportKind::TopN,
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
        status: RecommendationReportStatus::Published,
        account_source: AccountSource::Polymarket,
        capital_base_usd: Usd::new(dec!(10000)),
        account_snapshot_ref: AccountSnapshotId::from_v7(),
        summary_json: report_summary(),
        published_at: None,
        revoked_at: None,
        expired_at: None,
        status_reason: None,
    };
    let json = serde_json::to_value(&report).expect("serialize");
    assert_eq!(
        json["trigger_key"],
        json!("scheduled:daily-topn:2023-11-14T22:13:20Z")
    );
    assert_eq!(json["account_source"], json!("polymarket"));
    assert_eq!(json["capital_base_usd"], json!("10000"));
    assert!(json["account_snapshot_ref"].is_string());
}

#[test]
fn exposure_breakdown_aggregates_positions_by_market_event_category() {
    let positions = vec![
        position(
            "0xmarketA",
            Some("evt-1"),
            MarketCategory::Politics,
            Usd::new(dec!(60)),
        ),
        position(
            "0xmarketA",
            Some("evt-1"),
            MarketCategory::Politics,
            Usd::new(dec!(40)),
        ),
        position(
            "0xmarketB",
            Some("evt-1"),
            MarketCategory::Politics,
            Usd::new(dec!(25)),
        ),
        // Untracked-market position: no event, Other category, still counts.
        position("0xmarketC", None, MarketCategory::Other, Usd::new(dec!(15))),
    ];
    let exposures = ExposureBreakdown::from_positions(&positions);

    assert_eq!(
        exposures.per_market[&MarketId::new("0xmarketA")],
        Usd::new(dec!(100))
    );
    assert_eq!(
        exposures.per_market[&MarketId::new("0xmarketB")],
        Usd::new(dec!(25))
    );
    assert_eq!(
        exposures.per_market[&MarketId::new("0xmarketC")],
        Usd::new(dec!(15))
    );
    assert_eq!(
        exposures.per_event[&EventId::new("evt-1")],
        Usd::new(dec!(125))
    );
    assert!(
        !exposures
            .per_event
            .contains_key(&EventId::new("evt-missing"))
    );
    assert_eq!(
        exposures.per_category[&MarketCategory::Politics],
        Usd::new(dec!(125))
    );
    assert_eq!(
        exposures.per_category[&MarketCategory::Other],
        Usd::new(dec!(15))
    );
}

#[test]
fn account_snapshot_persists_positions_json_for_replay() {
    let positions = vec![
        position(
            "0xmarketA",
            Some("evt-1"),
            MarketCategory::Politics,
            Usd::new(dec!(60)),
        ),
        position("0xmarketC", None, MarketCategory::Other, Usd::new(dec!(15))),
    ];
    let snapshot = NewAccountSnapshot {
        account_snapshot_id: AccountSnapshotId::from_v7(),
        as_of: Utc.timestamp_opt(1_700_000_000, 0).unwrap(),
        source: AccountSource::Polymarket,
        equity_usd: Usd::new(dec!(10000)),
        available_usd: Usd::new(dec!(9000)),
        reserved_usd: Usd::new(dec!(0)),
        positions_json: AccountPositions(positions.clone()),
        exposures_json: ExposureBreakdown::from_positions(&positions),
    };
    // Persistence DTOs are not `PartialEq`; assert a stable JSON round-trip plus
    // the replayable fields survive.
    let json = serde_json::to_value(&snapshot).expect("serialize");
    let back: NewAccountSnapshot = serde_json::from_value(json.clone()).expect("deserialize");
    assert_eq!(serde_json::to_value(&back).expect("re-serialize"), json);
    assert_eq!(back.positions_json.0.len(), 2);
    assert_eq!(
        back.exposures_json.per_market[&MarketId::new("0xmarketA")],
        Usd::new(dec!(60))
    );
}

#[test]
fn portfolio_config_v3_three_section_roundtrip() {
    let config = RuntimeConfig::default();
    let back = RuntimeConfig::from_json(&config.to_json()).expect("v3 round-trip");
    assert_eq!(config, back);
    // Three sections present.
    let json = config.to_json();
    assert!(json["portfolio"]["budget"].is_object());
    assert!(json["portfolio"]["constraints"].is_object());
    assert!(json["portfolio"]["sizing"].is_object());
    // Kelly is the single sizing model: a flat parameter struct, not a tagged enum.
    assert!(json["portfolio"]["sizing"]["kelly_fraction"].is_string());
    assert!(json["portfolio"]["sizing"]["target_reward_multiple"].is_string());
}

#[test]
fn portfolio_config_deny_unknown_fields() {
    let mut doc = RuntimeConfig::default().to_json();
    doc["portfolio"]["budget"]["bogus_field"] = json!(1);
    assert!(
        RuntimeConfig::from_json(&doc).is_err(),
        "unknown portfolio.budget field must be rejected"
    );
}

#[test]
fn schedule_cadence_interval_and_cron_roundtrip() {
    let interval = ScheduleCadence::Interval { interval_secs: 300 };
    assert_eq!(interval, roundtrip(&interval));
    let cron = ScheduleCadence::Cron {
        expr: "0 0 * * * *".to_owned(),
        timezone: Some("America/New_York".to_owned()),
    };
    assert_eq!(cron, roundtrip(&cron));
    // Wire tag.
    assert_eq!(
        serde_json::to_value(&interval).unwrap()["kind"],
        json!("interval")
    );
    assert_eq!(serde_json::to_value(&cron).unwrap()["kind"], json!("cron"));
}

#[test]
fn portfolio_config_validation_rejects_invalid() {
    // Negative budget.
    let mut config = RuntimeConfig::default();
    config.portfolio.budget.total_budget_usd = DecimalString::new("-1");
    assert!(validate_runtime_config(&config).has_errors());

    // Invalid Kelly fraction (> 1).
    let mut config = RuntimeConfig::default();
    config.portfolio.sizing = SizingModelConfig {
        kelly_fraction: DecimalString::new("1.5"),
        max_position_pct: DecimalString::new("0.1"),
        target_reward_multiple: DecimalString::new("2.0"),
        confidence_weighting: ConfidenceSizeCurve::Linear,
        drawdown_scaling: DrawdownMultiplierPolicy::Fixed,
    };
    assert!(validate_runtime_config(&config).has_errors());

    // Invalid target reward multiple (<= 0).
    let mut config = RuntimeConfig::default();
    config.portfolio.sizing = SizingModelConfig {
        kelly_fraction: DecimalString::new("0.5"),
        max_position_pct: DecimalString::new("0.1"),
        target_reward_multiple: DecimalString::new("0"),
        confidence_weighting: ConfidenceSizeCurve::Linear,
        drawdown_scaling: DrawdownMultiplierPolicy::Fixed,
    };
    assert!(validate_runtime_config(&config).has_errors());

    // Enabled schedule with interval 0.
    let mut config = RuntimeConfig::default();
    config.reports.schedules[0].cadence = ScheduleCadence::Interval { interval_secs: 0 };
    config.reports.schedules[0].enabled = true;
    assert!(validate_runtime_config(&config).has_errors());

    // Malformed cron expression.
    let mut config = RuntimeConfig::default();
    config.reports.schedules[0].cadence = ScheduleCadence::Cron {
        expr: "not a cron".to_owned(),
        timezone: None,
    };
    config.reports.schedules[0].enabled = true;
    assert!(validate_runtime_config(&config).has_errors());
}
