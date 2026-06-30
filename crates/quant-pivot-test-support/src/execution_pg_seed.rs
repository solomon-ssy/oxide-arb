//! Postgres seed helpers for execution-ledger integration tests.
//!
//! Shared fixture chain extracted from `pg_execution_submission` so attribution,
//! submission, and capital tests can drive the same money-critical ledger paths.

use std::{collections::BTreeMap, str::FromStr};

use chrono::Utc;
use quant_pivot_models::{
    domain::{
        CapitalSettlement, ExitLedgerWrite, NewAccountSnapshot, NewCapitalAllocation,
        NewEquitySnapshot, NewExecutionOrder, NewMarketSelection, NewModelRun, NewModelSpec,
        NewModelVersion, NewOperationLog, NewOrderIntent, NewPortfolioPlan, NewRecommendation,
        NewRecommendationReport, NewReconciliation, NewReportDataQualitySnapshot,
        NewReportTransaction, NewRuntimeConfigVersion, PositionExit, PositionFill,
        SubmissionLedgerWrite,
    },
    enums::{
        common::{MarketCategory, OrderType, Side},
        execution::{
            CapitalAllocationState, ExecutionOrderPhase, ExitReason, ExitState, OrderIntentKind,
            OrderTypeKind, ReconciliationEvidenceKind, ReconciliationResult, VenueOrderStatus,
        },
        factor::FactorFamily,
        market::MarketStatus,
        model::ModelFamily,
        operation_log::{OperationCategory, OperationOutcome},
        quant::{
            AccountSource, ApprovalStatus, BindingConstraint, EntryTriggerKind,
            ExecutionOrderState, ModelRunKind, ModelRunStatus, OrderIntentStatus, OutcomeSide,
            PublicationStatus, QuantRuntimeMode, RecommendationReportStatus, RecommendationStatus,
            ReportKind, ReportTriggerKind, SettlementPolicy, SizingModelKind,
        },
        rbac::ResourceType,
        runtime_config::RuntimeConfigVersionSource,
    },
    types::{
        AccountPositions, AccountSnapshotId, BookSnapshotRef, Bps, CapitalAllocationId,
        ConfidenceSummary, ContentHash, DataQualitySummary, EligibilitySummary, EntryOrderSpec,
        EntryPlan, EquitySnapshotId, EventId, EvidenceRefs, ExecutionEligibility, ExecutionOrderId,
        ExitPlan, ExitPolicySpec, ExposureBreakdown, FactorBreakdownEntry, FeatureVectorId,
        MarketContext, MarketId, MarketSelectionId, ModelRunId, ModelSpecId, ModelVersionId,
        OperationLogId, OrderId, OrderIntentId, PortfolioConstraintsSnapshot,
        PortfolioOptimizerMeta, PortfolioPlanId, PortfolioRejectedSummary, PortfolioRiskBudget,
        PositionSnapshot, Price, Probability, RecommendationFactorBreakdown, RecommendationId,
        RecommendationIdentity, RecommendationReportId, ReconciliationEvidence,
        ReconciliationEvidenceChain, ReconciliationId, ReportDataQualitySnapshotId,
        ReportDataQualityTokens, ReportSummary, RiskEnvelope, RuntimeConfigVersionId,
        SchemaVersion, SelectionExclusionSummary, Shares, SignalCandidateId, SizingPlan, TokenId,
        Usd,
    },
};
use quant_pivot_repository::{
    postgres::{
        PgEventRepository, PgExecutionSubmissionRepository, PgMarketRepository,
        PgMarketSelectionRepository, PgModelRegistryRepository, PgModelRunRepository,
        PgOrderIntentRepository, PgRecommendationReportRepository,
        PgRuntimeConfigVersionRepository,
    },
    traits::{
        EventRepository, ExecutionSubmissionRepository, MarketRepository,
        MarketSelectionRepository, ModelRegistryRepository, ModelRunRepository,
        OrderIntentRepository, RecommendationReportRepository, RuntimeConfigVersionRepository,
    },
};
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use sea_orm::DatabaseConnection;

/// shares (100) * `limit_price` (0.6).
pub const EXECUTION_NOTIONAL: Decimal = dec!(60);

/// Stable ids produced by [`seed_report_fixture`].
pub struct ExecutionTxnIds {
    pub account_snapshot: AccountSnapshotId,
    pub data_quality_snapshot: ReportDataQualitySnapshotId,
    pub portfolio_plan: PortfolioPlanId,
    pub report: RecommendationReportId,
    pub recommendation: RecommendationId,
    pub model_version: ModelVersionId,
    pub model_run: ModelRunId,
    pub market_selection: MarketSelectionId,
    pub runtime_config_version: RuntimeConfigVersionId,
    pub market: String,
    pub event: String,
}

/// Seed runtime config, catalog, model lineage, market selection, and a published report.
pub async fn seed_report_fixture(db: &DatabaseConnection) -> ExecutionTxnIds {
    let rc_id = seed_runtime_config(db).await;
    let (model_version_id, model_run_id) = seed_model_version(db, &rc_id).await;
    let event_id = "evt-1";
    let market_id = "0xmarket";
    seed_market_catalog(db, event_id, market_id).await;
    let market_selection_id = seed_market_selection(db, &rc_id, market_id).await;
    let ids = ExecutionTxnIds {
        account_snapshot: AccountSnapshotId::from_v7(),
        data_quality_snapshot: ReportDataQualitySnapshotId::from_v7(),
        portfolio_plan: PortfolioPlanId::from_v7(),
        report: RecommendationReportId::from_v7(),
        recommendation: RecommendationId::from_v7(),
        model_version: model_version_id,
        model_run: model_run_id,
        market_selection: market_selection_id,
        runtime_config_version: rc_id,
        market: market_id.to_owned(),
        event: event_id.to_owned(),
    };
    PgRecommendationReportRepository::new(db.clone())
        .create_report(build_report_transaction(&ids))
        .await
        .expect("create report");
    ids
}

/// Create an auto-approved intent with capital allocation reserved.
pub async fn seed_approved_intent(db: &DatabaseConnection, ids: &ExecutionTxnIds) -> OrderIntentId {
    let order_intent_id = OrderIntentId::from_v7();
    PgOrderIntentRepository::new(db.clone())
        .create_with_allocation(
            NewOrderIntent {
                order_intent_id: order_intent_id.clone(),
                recommendation_id: ids.recommendation.clone(),
                runtime_mode: QuantRuntimeMode::AutoExecution,
                runtime_config_version_id: ids.runtime_config_version.clone(),
                model_version_id: ids.model_version.clone(),
                intent_kind: OrderIntentKind::Buy,
                status: OrderIntentStatus::ApprovedByPolicy,
                approval_status: ApprovalStatus::NotRequired,
                approved_by: None,
                approval_reason: Some("policy".to_owned()),
                approved_at: Some(Utc::now()),
                policy_id: Some("auto".to_owned()),
                policy_hash: None,
                status_reason: None,
                admission_trace_ref: None,
                entry_order_json: EntryOrderSpec {
                    token_id: TokenId::new("token-1"),
                    side: Side::Buy,
                    order_type: OrderType::Gtc,
                    limit_price: Price::new(dec!(0.6)),
                    shares: Shares::new(dec!(100)),
                    max_slippage_bps: Bps::new(dec!(50)),
                    valid_until: Utc::now() + chrono::Duration::hours(1),
                },
                exit_policy_json: ExitPolicySpec {
                    take_profit_price: Some(Price::new(dec!(0.8))),
                    take_profit_pct: None,
                    stop_loss_price: Some(Price::new(dec!(0.5))),
                    stop_loss_pct: None,
                    time_exit_at: None,
                    max_hold_secs: None,
                    trailing_stop: None,
                    signal_invalidation_rules: Vec::new(),
                    partial_exit_nodes: Vec::new(),
                    settlement_policy: SettlementPolicy::ExitBeforeResolution,
                    manual_review_at: None,
                    entry_reference_price: Price::new(dec!(0.6)),
                    entry_composite_score: Probability::new(dec!(0.8)),
                },
                risk_envelope_hash: content_hash('e'),
                expires_at: Utc::now() + chrono::Duration::hours(1),
            },
            NewCapitalAllocation {
                capital_allocation_id: CapitalAllocationId::from_v7(),
                order_intent_id: order_intent_id.clone(),
                recommendation_id: ids.recommendation.clone(),
                state: CapitalAllocationState::Allocated,
                planned_usd: Usd::new(EXECUTION_NOTIONAL),
                allocated_usd: Usd::new(EXECUTION_NOTIONAL),
                locked_usd: Usd::ZERO,
                spent_usd: Usd::ZERO,
                released_usd: Usd::ZERO,
                reason: "intent created".to_owned(),
            },
        )
        .await
        .expect("create approved intent")
        .order_intent_id
}

/// Drive an approved intent's entry to a confirmed full fill: capital `Spent`,
/// one open lot (100 @ 0.60), intent `Filled`.
pub async fn fill_entry_lot(
    submission: &PgExecutionSubmissionRepository,
    ids: &ExecutionTxnIds,
    intent_id: &OrderIntentId,
) {
    submission
        .claim_for_submission(intent_id, Utc::now())
        .await
        .expect("claim");
    let order = submission
        .create_entry_order_and_lock_capital(new_execution_order(intent_id, ids))
        .await
        .expect("create entry order");
    submission
        .record_submission_result(
            &order.execution_order_id,
            SubmissionLedgerWrite {
                state: ExecutionOrderState::Filled,
                intent_status: OrderIntentStatus::Filled,
                venue_order_id: Some(OrderId::new("venue-entry")),
                venue_status: Some(VenueOrderStatus::Filled),
                submitted_at: Utc::now(),
                filled_at: Some(Utc::now()),
                cancelled_at: None,
                error_message: None,
                capital: CapitalSettlement::SettleFull {
                    spent_usd: Usd::new(EXECUTION_NOTIONAL),
                },
                fill: Some(position_fill(ids, intent_id)),
                reconciliation: Some(reconciliation_row(&order.execution_order_id, intent_id)),
            },
        )
        .await
        .expect("record entry fill");
}

/// Full exit flow: entry fill then exit fill at 0.55 (realized -5), position `Closed`.
/// When `peak_mark_price` is set, seeds it on the exit monitor after entry fill.
pub async fn close_position_full(
    submission: &PgExecutionSubmissionRepository,
    ids: &ExecutionTxnIds,
    intent_id: &OrderIntentId,
    peak_mark_price: Option<Price>,
) {
    fill_entry_lot(submission, ids, intent_id).await;

    if let Some(peak) = peak_mark_price {
        submission
            .touch_exit_monitor(intent_id, Utc::now(), Some(peak), None)
            .await
            .expect("seed peak mark price");
    }

    let exit = submission
        .create_exit_order_and_mark_closing(
            exit_order(intent_id, ids, dec!(100), dec!(0.55)),
            ExitReason::StopLoss,
            None,
        )
        .await
        .expect("exit order");

    submission
        .record_exit_result(
            &exit.execution_order_id,
            ExitLedgerWrite {
                order_state: ExecutionOrderState::Filled,
                venue_order_id: Some(OrderId::new("venue-exit")),
                venue_status: Some(VenueOrderStatus::Filled),
                filled_at: Some(Utc::now()),
                cancelled_at: None,
                error_message: None,
                exit_state: ExitState::Exited,
                exit_reason: ExitReason::StopLoss,
                position_exit: Some(PositionExit {
                    shares: Shares::new(dec!(100)),
                    avg_price: Price::new(dec!(0.55)),
                    proceeds_usd: Usd::new(dec!(55)),
                    realized_pnl_usd: Usd::new(dec!(-5)),
                    exited_at: Utc::now(),
                    reason: ExitReason::StopLoss,
                }),
                fully_exited: true,
                revert_to_open: false,
                reconciliation: None,
            },
        )
        .await
        .expect("record exit");
}

pub fn report_operation_log(ids: &ExecutionTxnIds) -> NewOperationLog {
    NewOperationLog {
        id: OperationLogId::from_v7(),
        request_id: format!("scheduled:test:{}", ids.report),
        actor_user_id: None,
        actor_username: Some("system".to_owned()),
        acting_role: Some("test".to_owned()),
        category: OperationCategory::QuantReport,
        action: "publish".to_owned(),
        resource_type: Some(ResourceType::QuantReport),
        resource_id: Some(ids.report.to_string()),
        http_method: "SYSTEM".to_owned(),
        http_path: "/test/quant/report".to_owned(),
        http_status: 201,
        outcome: OperationOutcome::Success,
        client_ip: None,
        user_agent: None,
        latency_ms: 0,
        detail: serde_json::json!({ "test": true }),
        before_hash: None,
        after_hash: None,
        governance_audit_event_id: None,
        governance_audit_sequence: None,
    }
}

fn new_execution_order(intent_id: &OrderIntentId, ids: &ExecutionTxnIds) -> NewExecutionOrder {
    NewExecutionOrder {
        execution_order_id: ExecutionOrderId::from_v7(),
        order_intent_id: intent_id.clone(),
        order_phase: ExecutionOrderPhase::Entry,
        market_id: MarketId::new(&ids.market),
        token_id: TokenId::new("token-1"),
        side: Side::Buy,
        order_type: OrderTypeKind::Gtc,
        price: Price::new(dec!(0.6)),
        shares: Shares::new(dec!(100)),
        cost_usd: Usd::new(EXECUTION_NOTIONAL),
        venue_order_id: None,
        venue_status: None,
        state: ExecutionOrderState::Submitted,
        submitted_at: None,
        filled_at: None,
        cancelled_at: None,
        gtd_expiration_at: None,
        error_message: None,
    }
}

fn position_fill(ids: &ExecutionTxnIds, intent_id: &OrderIntentId) -> PositionFill {
    PositionFill {
        order_intent_id: intent_id.clone(),
        token_id: TokenId::new("token-1"),
        market_id: MarketId::new(&ids.market),
        event_id: Some(EventId::new(&ids.event)),
        category: MarketCategory::Politics,
        side: OutcomeSide::Yes,
        shares: Shares::new(dec!(100)),
        price: Price::new(dec!(0.6)),
        cost_usd: Usd::new(EXECUTION_NOTIONAL),
        filled_at: Utc::now(),
        source: AccountSource::Polymarket,
    }
}

fn reconciliation_row(
    execution_order_id: &ExecutionOrderId,
    intent_id: &OrderIntentId,
) -> NewReconciliation {
    NewReconciliation {
        reconciliation_id: ReconciliationId::from_v7(),
        execution_order_id: execution_order_id.clone(),
        order_intent_id: intent_id.clone(),
        result: ReconciliationResult::Unresolvable,
        evidence_json: ReconciliationEvidenceChain(vec![ReconciliationEvidence {
            kind: ReconciliationEvidenceKind::ClobOrderStatus,
            observed_at: Utc::now(),
            detail: "submission result".to_owned(),
            venue_ref: None,
            shares: None,
            price: None,
        }]),
        venue_filled_shares: None,
        venue_avg_price: None,
        discrepancy_usd: None,
        resolved_by: None,
        resolved_at: None,
    }
}

fn exit_order(
    intent_id: &OrderIntentId,
    ids: &ExecutionTxnIds,
    shares: Decimal,
    price: Decimal,
) -> NewExecutionOrder {
    NewExecutionOrder {
        execution_order_id: ExecutionOrderId::from_v7(),
        order_intent_id: intent_id.clone(),
        order_phase: ExecutionOrderPhase::Exit,
        market_id: MarketId::new(&ids.market),
        token_id: TokenId::new("token-1"),
        side: Side::Sell,
        order_type: OrderTypeKind::Gtc,
        price: Price::new(price),
        shares: Shares::new(shares),
        cost_usd: Shares::new(shares) * Price::new(price),
        venue_order_id: None,
        venue_status: None,
        state: ExecutionOrderState::Submitted,
        submitted_at: None,
        filled_at: None,
        cancelled_at: None,
        gtd_expiration_at: None,
        error_message: None,
    }
}

async fn seed_market_catalog(db: &DatabaseConnection, event_id: &str, market_id: &str) {
    use crate::catalog_fixtures::{make_event, make_market};

    PgEventRepository::new(db.clone())
        .upsert(make_event(
            event_id,
            "Event",
            "event",
            MarketCategory::Politics,
        ))
        .await
        .expect("seed event");
    PgMarketRepository::new(db.clone())
        .upsert(make_market(
            market_id,
            event_id,
            "Will it?",
            "will-it",
            MarketCategory::Politics,
            None,
        ))
        .await
        .expect("seed market");
}

async fn seed_runtime_config(db: &DatabaseConnection) -> RuntimeConfigVersionId {
    let id = RuntimeConfigVersionId::from_v7();
    PgRuntimeConfigVersionRepository::new(db.clone())
        .create_version(NewRuntimeConfigVersion {
            runtime_config_version_id: id.clone(),
            config_hash: content_hash('c'),
            schema_version: SchemaVersion::FIRST,
            config_json: serde_json::json!({}),
            source: RuntimeConfigVersionSource::Bootstrap,
            created_by: "pg-exec-it".to_owned(),
            reason: "integration test".to_owned(),
        })
        .await
        .expect("runtime config");
    id
}

async fn seed_model_version(
    db: &DatabaseConnection,
    rc_id: &RuntimeConfigVersionId,
) -> (ModelVersionId, ModelRunId) {
    let registry = PgModelRegistryRepository::new(db.clone());
    let model_spec_id = ModelSpecId::from_v7();
    registry
        .create_model_spec(NewModelSpec {
            model_spec_id: model_spec_id.clone(),
            name: "pg-exec-it".to_owned(),
            model_family: ModelFamily::WeightedFactor,
            prediction_horizon_secs: 86_400,
            feature_schema_version: SchemaVersion::FIRST,
            label_schema_version: SchemaVersion::FIRST,
            spec_json: serde_json::json!({}),
            status: PublicationStatus::Published,
        })
        .await
        .expect("model spec");
    let model_version_id = ModelVersionId::from_v7();
    registry
        .create_model_version(NewModelVersion {
            model_version_id: model_version_id.clone(),
            model_spec_id,
            version: 1,
            artifact_hash: content_hash('a'),
            training_dataset_id: None,
            metrics_json: serde_json::json!({}),
            quality_gate_report: serde_json::json!({}),
            publication_status: PublicationStatus::Published,
            published_at: Some(Utc::now()),
            retired_at: None,
        })
        .await
        .expect("model version");
    let model_run_id = ModelRunId::from_v7();
    PgModelRunRepository::new(db.clone())
        .create(NewModelRun {
            model_run_id: model_run_id.clone(),
            run_kind: ModelRunKind::LiveInference,
            model_version_id: Some(model_version_id.clone()),
            runtime_config_version_id: rc_id.clone(),
            market_selection_id: None,
            window_start: Utc::now(),
            window_end: Utc::now(),
            status: ModelRunStatus::Succeeded,
            input_hash: content_hash('d'),
            output_hash: None,
            metrics_json: serde_json::json!({}),
            error_code: None,
            error_message: None,
            started_at: Utc::now(),
            finished_at: Some(Utc::now()),
        })
        .await
        .expect("model run");
    (model_version_id, model_run_id)
}

async fn seed_market_selection(
    db: &DatabaseConnection,
    rc_id: &RuntimeConfigVersionId,
    _market_id: &str,
) -> MarketSelectionId {
    let id = MarketSelectionId::from_v7();
    PgMarketSelectionRepository::new(db.clone())
        .create_snapshot(
            NewMarketSelection {
                market_selection_id: id.clone(),
                as_of: Utc::now(),
                runtime_config_version_id: rc_id.clone(),
                selector_hash: content_hash('b'),
                market_count: 1,
                exclusion_summary: SelectionExclusionSummary::default(),
            },
            Vec::new(),
        )
        .await
        .expect("market selection");
    id
}

fn build_report_transaction(ids: &ExecutionTxnIds) -> NewReportTransaction {
    let equity_snapshot_id = EquitySnapshotId::from_v7();
    NewReportTransaction {
        account_snapshot: NewAccountSnapshot {
            account_snapshot_id: ids.account_snapshot.clone(),
            ..new_account_snapshot()
        },
        equity_snapshot: NewEquitySnapshot {
            equity_snapshot_id: equity_snapshot_id.clone(),
            as_of: Utc::now(),
            source: AccountSource::Polymarket,
            venue_net_liquidation_usd: Usd::new(dec!(10000)),
            capital_base_usd: Usd::new(dec!(10000)),
            available_usd: Usd::new(dec!(9000)),
            reserved_usd: Usd::ZERO,
            realized_pnl_cumulative_usd: Usd::ZERO,
            unrealized_pnl_usd: Usd::ZERO,
            high_water_mark_usd: Usd::new(dec!(10000)),
            drawdown_pct: Decimal::ZERO,
            account_snapshot_ref: Some(ids.account_snapshot.clone()),
        },
        data_quality_snapshot: NewReportDataQualitySnapshot {
            report_data_quality_snapshot_id: ids.data_quality_snapshot.clone(),
            as_of: Utc::now(),
            runtime_config_version_id: ids.runtime_config_version.clone(),
            tokens_json: ReportDataQualityTokens(Vec::new()),
        },
        portfolio_plan: NewPortfolioPlan {
            portfolio_plan_id: ids.portfolio_plan.clone(),
            model_run_id: Some(ids.model_run.clone()),
            market_selection_id: ids.market_selection.clone(),
            as_of: Utc::now(),
            budget_usd: Usd::new(dec!(10000)),
            allocated_usd: Usd::new(EXECUTION_NOTIONAL),
            risk_budget_json: PortfolioRiskBudget::default(),
            constraints_json: PortfolioConstraintsSnapshot::default(),
            rejected_summary: PortfolioRejectedSummary::default(),
            optimizer_meta_json: PortfolioOptimizerMeta::default(),
        },
        report: NewRecommendationReport {
            recommendation_report_id: ids.report.clone(),
            report_kind: ReportKind::TopN,
            trigger_kind: ReportTriggerKind::Scheduled,
            trigger_key: format!("scheduled:test:{}", ids.report),
            trigger_time: Utc::now(),
            source_delay_secs: 10,
            as_of: Utc::now(),
            horizon_secs: 86_400,
            runtime_mode: QuantRuntimeMode::AutoExecution,
            runtime_config_version_id: ids.runtime_config_version.clone(),
            model_version_id: ids.model_version.clone(),
            market_selection_id: ids.market_selection.clone(),
            portfolio_plan_id: ids.portfolio_plan.clone(),
            top_n: 20,
            status: RecommendationReportStatus::Published,
            account_source: AccountSource::Polymarket,
            capital_base_usd: Usd::new(dec!(10000)),
            account_snapshot_ref: ids.account_snapshot.clone(),
            equity_snapshot_ref: equity_snapshot_id,
            data_quality_snapshot_ref: ids.data_quality_snapshot.clone(),
            summary_json: report_summary(),
            published_at: Some(Utc::now()),
            valid_until: Some(Utc::now() + chrono::Duration::hours(1)),
            revoked_at: None,
            expired_at: None,
            status_reason: None,
        },
        recommendations: vec![NewRecommendation {
            recommendation_id: ids.recommendation.clone(),
            recommendation_report_id: ids.report.clone(),
            rank: 1,
            market_id: MarketId::new(&ids.market),
            event_id: EventId::new(&ids.event),
            token_id: TokenId::new("token-1"),
            outcome_side: OutcomeSide::Yes,
            composite_score: Probability::new(dec!(0.7)),
            risk_adjusted_score: Probability::new(dec!(0.65)),
            confidence: Probability::new(dec!(0.72)),
            expected_return_bps: Bps::new(dec!(150)),
            downside_bps: Bps::new(dec!(80)),
            identity: recommendation_identity(),
            market_context: market_context(),
            rank_before_portfolio: 1,
            liquidity_score: Probability::new(dec!(0.8)),
            data_quality_score: Probability::new(dec!(0.9)),
            model_score_percentile: Probability::new(dec!(0.75)),
            entry_plan: entry_plan(),
            sizing_plan: sizing_plan(),
            exit_plan: exit_plan(),
            risk_envelope: risk_envelope(),
            factor_breakdown: factor_breakdown(),
            evidence_refs: evidence_refs(),
            execution_eligibility: execution_eligibility(),
            valid_from: Utc::now(),
            valid_until: Utc::now() + chrono::Duration::hours(1),
            status: RecommendationStatus::Published,
        }],
        operation_log: report_operation_log(ids),
    }
}

fn content_hash(seed: char) -> ContentHash {
    ContentHash::parse(format!("blake3:{}", seed.to_string().repeat(64))).expect("hash")
}

fn new_account_snapshot() -> NewAccountSnapshot {
    let positions = vec![PositionSnapshot {
        token_id: TokenId::new("token-1"),
        market_id: MarketId::new("0xmarket"),
        event_id: Some(EventId::new("evt-1")),
        category: MarketCategory::Politics,
        outcome: "Yes".to_owned(),
        size: Shares::new(dec!(100)),
        avg_price: Price::new(dec!(0.5)),
        cur_price: Price::new(dec!(0.6)),
        current_value: Usd::new(dec!(60)),
        redeemable: false,
    }];
    NewAccountSnapshot {
        account_snapshot_id: AccountSnapshotId::from_v7(),
        as_of: Utc::now(),
        source: AccountSource::Polymarket,
        venue_net_liquidation_usd: Usd::new(dec!(10000)),
        capital_base_usd: Usd::new(dec!(10000)),
        available_usd: Usd::new(dec!(9000)),
        reserved_usd: Usd::new(dec!(0)),
        positions_json: AccountPositions(positions.clone()),
        exposures_json: ExposureBreakdown::from_positions(&positions),
    }
}

fn entry_plan() -> EntryPlan {
    EntryPlan {
        trigger_kind: EntryTriggerKind::Immediate,
        trigger_price: None,
        limit_price: Some(Price::new(dec!(0.6))),
        max_slippage_bps: Bps::new(dec!(50)),
        valid_from: Utc::now(),
        valid_until: Utc::now() + chrono::Duration::hours(1),
        min_depth_usd: Usd::new(dec!(100)),
        max_book_age_ms: 2_000,
        confirmation_window_secs: 30,
        cancel_if_not_triggered: true,
        entry_reason: "immediate".to_owned(),
    }
}

fn sizing_plan() -> SizingPlan {
    SizingPlan {
        suggested_usd: Usd::new(EXECUTION_NOTIONAL),
        suggested_shares: Shares::new(dec!(100)),
        max_usd: Usd::new(dec!(500)),
        min_usd: Usd::new(dec!(10)),
        portfolio_weight_pct: dec!(0.025),
        market_exposure_after_usd: Usd::new(EXECUTION_NOTIONAL),
        event_exposure_after_usd: Usd::new(EXECUTION_NOTIONAL),
        category_exposure_after_usd: Usd::new(EXECUTION_NOTIONAL),
        binding_constraint: BindingConstraint::KellyCap,
        sizing_reason: "kelly".to_owned(),
        sizing_model: SizingModelKind::Kelly,
        edge_bps: Some(Bps::new(dec!(100))),
        kelly_fraction_applied: Some(dec!(0.5)),
    }
}

fn exit_plan() -> ExitPlan {
    ExitPlan {
        take_profit_price: Some(Price::new(dec!(0.8))),
        take_profit_pct: None,
        stop_loss_price: Some(Price::new(dec!(0.4))),
        stop_loss_pct: None,
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
        auto_execution_allowed: true,
        risk_notes: Vec::new(),
        envelope_hash: content_hash('f'),
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
        direction: quant_pivot_models::enums::quant::FactorDirection::Positive,
        explanation: "deep".to_owned(),
        source_refs: Vec::new(),
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

fn evidence_refs() -> EvidenceRefs {
    EvidenceRefs {
        signal_candidate_id: SignalCandidateId::from_v7(),
        feature_vector_id: FeatureVectorId::from_v7(),
        model_run_id: ModelRunId::from_v7(),
        market_selection_id: MarketSelectionId::from_v7(),
        book_snapshot_ref: BookSnapshotRef::from_str(&format!(
            "book:live:token-1:1:1700000000@blake3:{}",
            "0".repeat(64)
        ))
        .expect("book ref"),
        runtime_config_version_id: RuntimeConfigVersionId::from_v7(),
        model_version_id: ModelVersionId::from_v7(),
        factor_definition_versions: Vec::new(),
        data_quality_snapshot_ref: ReportDataQualitySnapshotId::from_v7(),
    }
}

fn execution_eligibility() -> ExecutionEligibility {
    ExecutionEligibility {
        eligible_modes: vec![
            QuantRuntimeMode::ReportOnly,
            QuantRuntimeMode::SemiAuto,
            QuantRuntimeMode::AutoExecution,
        ],
        ineligibility_reasons: Vec::new(),
        approval_required: false,
        auto_policy_id: None,
    }
}

fn report_summary() -> ReportSummary {
    ReportSummary {
        market_selection_count: 1,
        candidate_count: 1,
        rejected_count: 0,
        published_recommendation_count: 1,
        total_suggested_usd: Usd::new(EXECUTION_NOTIONAL),
        max_single_recommendation_usd: Usd::new(EXECUTION_NOTIONAL),
        category_allocation: BTreeMap::new(),
        event_allocation: BTreeMap::new(),
        average_score: Probability::new(dec!(0.7)),
        min_score: Probability::new(dec!(0.7)),
        model_confidence_summary: ConfidenceSummary::default(),
        data_quality_summary: DataQualitySummary::default(),
        top_rejection_reasons: Vec::new(),
        execution_eligibility_summary: EligibilitySummary::default(),
        empty_reason: None,
        warnings: Vec::new(),
    }
}
