//! Account-capital persistence integration tests (Postgres + testcontainers).
//!
//! Requires Docker. Covers the `quant_account_snapshot` repository, the
//! reserved-capital aggregation, and the end-to-end report-creation transaction
//! (`account_snapshot` → `portfolio_plan` → report → recommendations) with its
//! foreign-key ordering plus the strong-typed JSONB payload round-trip.

use std::collections::BTreeMap;

use chrono::{Duration, Utc};
use quant_pivot_models::{
    domain::{
        NewAccountSnapshot, NewMarketSelection, NewModelRun, NewModelSpec, NewModelVersion,
        NewOperationLog, NewOrderIntent, NewPortfolioPlan, NewRecommendation,
        NewRecommendationReport, NewReportTransaction, NewRuntimeConfigVersion, OperationLogQuery,
    },
    entities::quant_market_selection::{SelectionExcludedMarketIds, SelectionIncludedMarketIds},
    enums::{
        common::{MarketCategory, OrderType, Side},
        execution::OrderIntentKind,
        factor::FactorFamily,
        model::ModelFamily,
        operation_log::{OperationCategory, OperationOutcome},
        quant::{
            AccountSource, ApprovalStatus, BindingConstraint, EntryTriggerKind, FactorDirection,
            ModelRunKind, ModelRunStatus, OrderIntentStatus, OutcomeSide, PublicationStatus,
            QuantRuntimeMode, RecommendationReportStatus, RecommendationStatus, ReportKind,
            ReportTriggerKind, SettlementPolicy, SizingModelKind,
        },
        rbac::ResourceType,
        runtime_config::RuntimeConfigVersionSource,
    },
    types::{
        AccountPositions, AccountSnapshotId, Bps, ConfidenceSummary, ContentHash,
        DataQualitySummary, EligibilitySummary, EntryOrderSpec, EntryPlan, EventId, EvidenceRefs,
        ExecutionEligibility, ExitPlan, ExitPolicySpec, ExposureBreakdown, FactorBreakdownEntry,
        FeatureVectorId, MarketId, MarketSelectionId, ModelRunId, ModelSpecId, ModelVersionId,
        OperationLogId, OrderIntentId, PortfolioConstraintsSnapshot, PortfolioPlanId,
        PortfolioRejectedSummary, PortfolioRiskBudget, PositionSnapshot, Price, Probability,
        RecommendationFactorBreakdown, RecommendationId, RecommendationReportId, ReportSummary,
        RiskEnvelope, RuntimeConfigVersionId, SchemaVersion, SelectionExclusionSummary, Shares,
        SignalCandidateId, SizingPlan, TokenId, Usd,
    },
};
use quant_pivot_repository::{
    postgres::{
        PgAccountSnapshotRepository, PgEventRepository, PgMarketRepository,
        PgMarketSelectionRepository, PgModelRegistryRepository, PgModelRunRepository,
        PgOperationLogRepository, PgOrderIntentRepository, PgRecommendationReportRepository,
        PgRecommendationRepository, PgReservedCapitalRepository, PgRuntimeConfigVersionRepository,
    },
    traits::{
        AccountSnapshotRepository, EventRepository, MarketRepository, MarketSelectionRepository,
        ModelRegistryRepository, ModelRunRepository, OperationLogRepository, OrderIntentRepository,
        RecommendationReportRepository, RecommendationRepository, ReservedCapitalRepository,
        RuntimeConfigVersionRepository,
    },
};
use quant_pivot_test_support::{
    catalog_fixtures::{make_event, make_market},
    pg::setup_pg,
};
use rust_decimal_macros::dec;

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
        equity_usd: Usd::new(dec!(10000)),
        available_usd: Usd::new(dec!(9000)),
        reserved_usd: Usd::new(dec!(0)),
        positions_json: AccountPositions(positions.clone()),
        exposures_json: ExposureBreakdown::from_positions(&positions),
    }
}

#[tokio::test]
#[ignore = "requires Docker"]
async fn account_snapshot_repo_create_find() {
    let (pool, _container) = setup_pg().await;
    let db = pool.connection().clone();
    let repo = PgAccountSnapshotRepository::new(db.clone());

    let snapshot = new_account_snapshot();
    let id = snapshot.account_snapshot_id.clone();
    let created = repo.create(snapshot).await.expect("create snapshot");
    assert_eq!(created.account_snapshot_id, id);
    assert_eq!(created.equity_usd, Usd::new(dec!(10000)));
    assert_eq!(created.positions_json.0.len(), 1);

    let found = repo
        .find_by_id(&id)
        .await
        .expect("find")
        .expect("snapshot present");
    assert_eq!(found.account_snapshot_id, id);
    assert_eq!(
        found.exposures_json.per_market[&MarketId::new("0xmarket")],
        Usd::new(dec!(60))
    );

    assert!(
        repo.find_by_id(&AccountSnapshotId::from_v7())
            .await
            .expect("find missing")
            .is_none()
    );
}

#[tokio::test]
#[ignore = "requires Docker"]
async fn reserved_capital_reader_returns_zero_when_empty() {
    let (pool, _container) = setup_pg().await;
    let db = pool.connection().clone();
    let reader = PgReservedCapitalRepository::new(db);
    assert_eq!(reader.sum_locked_usd().await.expect("sum"), Usd::ZERO);
}

#[tokio::test]
#[ignore = "requires Docker"]
async fn report_transaction_persists_chain_and_reserved_capital_sums_pending_intents() {
    let (pool, _container) = setup_pg().await;
    let db = pool.connection().clone();

    let rc_id = seed_runtime_config(&db).await;
    let (model_version_id, model_run_id) = seed_model_version(&db, &rc_id).await;
    let event_id = "evt-1";
    let market_id = "0xmarket";
    seed_market_catalog(&db, event_id, market_id).await;
    let market_selection_id = seed_market_selection(&db, &rc_id, market_id, event_id).await;

    let ids = TxnIds {
        account_snapshot: AccountSnapshotId::from_v7(),
        portfolio_plan: PortfolioPlanId::from_v7(),
        report: RecommendationReportId::from_v7(),
        recommendation: RecommendationId::from_v7(),
        model_version: model_version_id.clone(),
        model_run: model_run_id.clone(),
        market_selection: market_selection_id.clone(),
        runtime_config_version: rc_id.clone(),
        market: market_id.to_owned(),
        event: event_id.to_owned(),
    };
    create_and_assert_report_transaction(&db, &ids).await;
    assert_recommendation_roundtrip(&db, &ids.report).await;
    assert_reserved_capital_tracks_pending_intent(&db, &ids.recommendation).await;
}

#[tokio::test]
#[ignore = "requires Docker"]
async fn find_expirable_returns_published_reports_before_cutoff_only() {
    let (pool, _container) = setup_pg().await;
    let db = pool.connection().clone();

    let rc_id = seed_runtime_config(&db).await;
    let (model_version_id, model_run_id) = seed_model_version(&db, &rc_id).await;
    let event_id = "evt-1";
    let market_id = "0xmarket";
    seed_market_catalog(&db, event_id, market_id).await;
    let market_selection_id = seed_market_selection(&db, &rc_id, market_id, event_id).await;

    let ids = TxnIds {
        account_snapshot: AccountSnapshotId::from_v7(),
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

    let report_repo = PgRecommendationReportRepository::new(db.clone());
    report_repo
        .create_report(build_report_transaction(&ids))
        .await
        .expect("create report");

    // Not yet due: a cutoff earlier than `published_at` excludes the report.
    let not_due = report_repo
        .find_expirable(Utc::now() - Duration::minutes(1), 100)
        .await
        .expect("find before cutoff");
    assert!(
        not_due.is_empty(),
        "a report published ~now must not be expirable at an earlier cutoff"
    );

    // Due: a cutoff after `published_at` includes the published report.
    let due = report_repo
        .find_expirable(Utc::now() + Duration::minutes(1), 100)
        .await
        .expect("find due");
    assert_eq!(due, vec![ids.report.clone()]);

    // Once expired, the status filter removes it from the expirable set.
    report_repo
        .expire(
            &ids.report,
            "ttl_expired",
            Utc::now(),
            report_operation_log(&ids),
        )
        .await
        .expect("expire report");
    let after_expiry = report_repo
        .find_expirable(Utc::now() + Duration::minutes(1), 100)
        .await
        .expect("find after expiry");
    assert!(
        after_expiry.is_empty(),
        "expired reports must not be returned by find_expirable"
    );
}

async fn seed_market_catalog(db: &sea_orm::DatabaseConnection, event_id: &str, market_id: &str) {
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

async fn create_and_assert_report_transaction(db: &sea_orm::DatabaseConnection, ids: &TxnIds) {
    let trigger_key = report_trigger_key(ids);
    let report_repo = PgRecommendationReportRepository::new(db.clone());
    let created = report_repo
        .create_report(build_report_transaction(ids))
        .await
        .expect("create report transaction");
    assert_eq!(created.recommendation_report_id, ids.report);
    assert_eq!(created.capital_base_usd, Usd::new(dec!(10000)));
    assert_eq!(created.account_snapshot_ref, ids.account_snapshot);

    let found_by_trigger = report_repo
        .find_by_trigger_key(&trigger_key)
        .await
        .expect("find by trigger key")
        .expect("trigger key row");
    assert_eq!(found_by_trigger.recommendation_report_id, ids.report);

    let op_logs = PgOperationLogRepository::new(db.clone())
        .page(OperationLogQuery {
            request_id: Some(trigger_key),
            ..OperationLogQuery::default()
        })
        .await
        .expect("operation log page");
    assert_eq!(op_logs.total, 1);
    assert_eq!(
        op_logs.items[0].resource_id.as_deref(),
        Some(ids.report.to_string().as_str())
    );
}

async fn assert_recommendation_roundtrip(
    db: &sea_orm::DatabaseConnection,
    report_id: &RecommendationReportId,
) {
    let recs = PgRecommendationRepository::new(db.clone())
        .find_by_report(report_id)
        .await
        .expect("find recommendations");
    assert_eq!(recs.len(), 1);
    assert_eq!(recs[0].sizing_plan.suggested_usd, Usd::new(dec!(250)));
    assert_eq!(recs[0].sizing_plan.sizing_model, SizingModelKind::Kelly);
}

async fn assert_reserved_capital_tracks_pending_intent(
    db: &sea_orm::DatabaseConnection,
    recommendation_id: &RecommendationId,
) {
    let reserved_repo = PgReservedCapitalRepository::new(db.clone());
    assert_eq!(
        reserved_repo.sum_locked_usd().await.expect("sum"),
        Usd::ZERO
    );

    PgOrderIntentRepository::new(db.clone())
        .create_pending(NewOrderIntent {
            order_intent_id: OrderIntentId::from_v7(),
            recommendation_id: recommendation_id.clone(),
            runtime_mode: QuantRuntimeMode::SemiAuto,
            intent_kind: OrderIntentKind::Buy,
            status: OrderIntentStatus::PendingApproval,
            approval_status: ApprovalStatus::Pending,
            approved_by: None,
            approval_reason: None,
            approved_at: None,
            entry_order_json: EntryOrderSpec {
                token_id: TokenId::new("token-1"),
                side: Side::Buy,
                order_type: OrderType::Gtc,
                limit_price: Price::new(dec!(0.6)),
                shares: Shares::new(dec!(416.66)),
                max_slippage_bps: Bps::new(dec!(50)),
                valid_until: Utc::now(),
            },
            exit_policy_json: ExitPolicySpec {
                take_profit_price: Some(Price::new(dec!(0.8))),
                stop_loss_price: Some(Price::new(dec!(0.5))),
                time_exit_at: Some(Utc::now()),
                partial_exit_nodes: Vec::new(),
                settlement_policy: SettlementPolicy::ExitBeforeResolution,
            },
            risk_envelope_hash: content_hash('e'),
            expires_at: Utc::now(),
        })
        .await
        .expect("create intent");

    assert_eq!(
        reserved_repo.sum_locked_usd().await.expect("sum"),
        Usd::new(dec!(250))
    );
}

// ── Seed helpers ────────────────────────────────────────────────────────────

async fn seed_runtime_config(db: &sea_orm::DatabaseConnection) -> RuntimeConfigVersionId {
    let id = RuntimeConfigVersionId::from_v7();
    PgRuntimeConfigVersionRepository::new(db.clone())
        .create_version(NewRuntimeConfigVersion {
            runtime_config_version_id: id.clone(),
            config_hash: content_hash('c'),
            schema_version: SchemaVersion::FIRST,
            config_json: serde_json::json!({}),
            source: RuntimeConfigVersionSource::Bootstrap,
            created_by: "pg-account-it".to_owned(),
            reason: "integration test".to_owned(),
        })
        .await
        .expect("runtime config");
    id
}

async fn seed_model_version(
    db: &sea_orm::DatabaseConnection,
    rc_id: &RuntimeConfigVersionId,
) -> (ModelVersionId, ModelRunId) {
    let registry = PgModelRegistryRepository::new(db.clone());
    let model_spec_id = ModelSpecId::from_v7();
    registry
        .create_model_spec(NewModelSpec {
            model_spec_id: model_spec_id.clone(),
            name: "pg-account-it".to_owned(),
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
    db: &sea_orm::DatabaseConnection,
    rc_id: &RuntimeConfigVersionId,
    market_id: &str,
    _event_id: &str,
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
                included_market_ids: SelectionIncludedMarketIds(vec![market_id.to_owned()]),
                excluded_market_ids: SelectionExcludedMarketIds(Vec::new()),
                exclusion_summary: SelectionExclusionSummary::default(),
            },
            Vec::new(),
        )
        .await
        .expect("market selection");
    id
}

// ── Report transaction builder ────────────────────────────────────────────────

struct TxnIds {
    account_snapshot: AccountSnapshotId,
    portfolio_plan: PortfolioPlanId,
    report: RecommendationReportId,
    recommendation: RecommendationId,
    model_version: ModelVersionId,
    model_run: ModelRunId,
    market_selection: MarketSelectionId,
    runtime_config_version: RuntimeConfigVersionId,
    market: String,
    event: String,
}

fn build_report_transaction(ids: &TxnIds) -> NewReportTransaction {
    NewReportTransaction {
        account_snapshot: NewAccountSnapshot {
            account_snapshot_id: ids.account_snapshot.clone(),
            ..new_account_snapshot()
        },
        portfolio_plan: NewPortfolioPlan {
            portfolio_plan_id: ids.portfolio_plan.clone(),
            model_run_id: Some(ids.model_run.clone()),
            market_selection_id: ids.market_selection.clone(),
            as_of: Utc::now(),
            budget_usd: Usd::new(dec!(10000)),
            allocated_usd: Usd::new(dec!(250)),
            risk_budget_json: PortfolioRiskBudget::default(),
            constraints_json: PortfolioConstraintsSnapshot::default(),
            rejected_summary: PortfolioRejectedSummary::default(),
        },
        report: NewRecommendationReport {
            recommendation_report_id: ids.report.clone(),
            report_kind: ReportKind::TopN,
            trigger_kind: ReportTriggerKind::Scheduled,
            trigger_key: report_trigger_key(ids),
            trigger_time: Utc::now(),
            source_delay_secs: 10,
            as_of: Utc::now(),
            horizon_secs: 86_400,
            runtime_mode: QuantRuntimeMode::ReportOnly,
            runtime_config_version_id: ids.runtime_config_version.clone(),
            model_version_id: ids.model_version.clone(),
            market_selection_id: ids.market_selection.clone(),
            portfolio_plan_id: ids.portfolio_plan.clone(),
            top_n: 20,
            status: RecommendationReportStatus::Published,
            account_source: AccountSource::Polymarket,
            capital_base_usd: Usd::new(dec!(10000)),
            account_snapshot_ref: ids.account_snapshot.clone(),
            summary_json: report_summary(),
            published_at: Some(Utc::now()),
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
            entry_plan: entry_plan(),
            sizing_plan: sizing_plan(Usd::new(dec!(250))),
            exit_plan: exit_plan(),
            risk_envelope: risk_envelope(),
            factor_breakdown: factor_breakdown(),
            evidence_refs: evidence_refs(),
            execution_eligibility: execution_eligibility(),
            valid_from: Utc::now(),
            valid_until: Utc::now(),
            status: RecommendationStatus::Published,
        }],
        operation_log: report_operation_log(ids),
    }
}

fn report_operation_log(ids: &TxnIds) -> NewOperationLog {
    NewOperationLog {
        id: OperationLogId::from_v7(),
        request_id: report_trigger_key(ids),
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
        governance_audit_event_id: None,
        governance_audit_sequence: None,
    }
}

fn report_trigger_key(ids: &TxnIds) -> String {
    format!("scheduled:test:{}", ids.report)
}

// ── Payload builders ──────────────────────────────────────────────────────────

fn entry_plan() -> EntryPlan {
    EntryPlan {
        trigger_kind: EntryTriggerKind::Immediate,
        trigger_price: None,
        limit_price: Some(Price::new(dec!(0.6))),
        max_slippage_bps: Bps::new(dec!(50)),
        valid_from: Utc::now(),
        valid_until: Utc::now(),
        min_depth_usd: Usd::new(dec!(100)),
        max_book_age_ms: 2_000,
        confirmation_window_secs: 30,
        cancel_if_not_triggered: true,
        entry_reason: "immediate".to_owned(),
    }
}

fn sizing_plan(suggested: Usd) -> SizingPlan {
    SizingPlan {
        suggested_usd: suggested,
        suggested_shares: Shares::new(dec!(416.66)),
        max_usd: Usd::new(dec!(500)),
        min_usd: Usd::new(dec!(10)),
        portfolio_weight_pct: dec!(0.025),
        market_exposure_after_usd: suggested,
        event_exposure_after_usd: suggested,
        category_exposure_after_usd: suggested,
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
        auto_execution_allowed: false,
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
        direction: FactorDirection::Positive,
        explanation: "deep".to_owned(),
        source_refs: Vec::new(),
    }])
}

fn evidence_refs() -> EvidenceRefs {
    EvidenceRefs {
        signal_candidate_id: SignalCandidateId::from_v7(),
        feature_vector_id: FeatureVectorId::from_v7(),
        model_run_id: ModelRunId::from_v7(),
        market_selection_id: MarketSelectionId::from_v7(),
        book_snapshot_ref: None,
        runtime_config_version_id: RuntimeConfigVersionId::from_v7(),
        model_version_id: ModelVersionId::from_v7(),
        factor_definition_versions: Vec::new(),
        data_quality_report_ref: None,
    }
}

fn execution_eligibility() -> ExecutionEligibility {
    ExecutionEligibility {
        eligible_modes: vec![QuantRuntimeMode::ReportOnly],
        ineligibility_reasons: Vec::new(),
        approval_required: false,
        approval_role: None,
        auto_policy_id: None,
    }
}

fn report_summary() -> ReportSummary {
    ReportSummary {
        market_selection_count: 1,
        candidate_count: 1,
        rejected_count: 0,
        published_recommendation_count: 1,
        total_suggested_usd: Usd::new(dec!(250)),
        max_single_recommendation_usd: Usd::new(dec!(250)),
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
