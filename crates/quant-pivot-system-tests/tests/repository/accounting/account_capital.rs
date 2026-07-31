//! Account-capital persistence system contracts.
//!
//! Requires Docker. Covers the `quant_account_snapshot` repository, the
//! reserved-capital aggregation, and the end-to-end report-creation transaction
//! (`account_snapshot` → `portfolio_plan` → report → recommendations) with its
//! foreign-key ordering plus the strong-typed JSONB payload round-trip.

use std::{collections::BTreeMap, str::FromStr};

use chrono::{DateTime, Duration, Utc};
use quant_pivot_models::{
    domain::{
        api::OperationLogQuery,
        governance::{NewOperationLog, RuntimeControlUpdate},
        patch::{NullablePatch, Patch},
        quant::{
            AppendReconciliationEvidence, ExecutionOrderPatch, NewAccountSnapshot,
            NewCapitalAllocation, NewEntryConditionInstance, NewEquitySnapshot, NewExecutionOrder,
            NewFeatureParityState, NewMarketSelection, NewModelRun, NewOrderIntent,
            NewPortfolioPlan, NewRecommendation, NewRecommendationReport, NewReconciliation,
            NewReportDataQualitySnapshot, NewReportTransaction, ReconciliationPatch,
        },
    },
    entities::{
        quant_feature_parity_state::Entity as QuantFeatureParityStateEntity,
        quant_report_fact_delivery::Entity,
    },
    enums::{
        common::{MarketCategory, OrderType, Side, TickSize},
        execution::{
            CapitalAllocationState, ExecutionOrderPhase, KillSwitchState, OrderIntentKind,
            OrderTypeKind, ReconciliationEvidenceKind, ReconciliationResult, VenueOrderStatus,
        },
        factor::{FactorFamily, FactorValueState, NormalizationSource},
        market::MarketStatus,
        model::ModelFamily,
        operation_log::{OperationCategory, OperationHttpMethod, OperationOutcome},
        quant::{
            AccountSource, ApprovalStatus, BindingConstraint, EntryConditionState,
            ExecutionOrderState, ExitSettlementMode, FactorDirection, FeatureParityLatchState,
            FeatureParityStateTransition, ModelRunKind, OrderIntentStatus, OutcomeSide,
            QuantRuntimeMode, RecommendationReportStatus, RecommendationStatus, RedeemPolicy,
            ReportFactDeliveryStatus, ReportKind, SizingModelKind,
        },
        rbac::ResourceType,
    },
    types::{
        AccountPositions, AccountSnapshotId, BookSnapshotRef, Bps, CapitalAllocationId,
        ConditionTruth, ConfidenceSummary, ContentHash, DataQualitySummary,
        DecisionPolicySnapshotId, EligibilitySummary, EntryConditionFoldState,
        EntryConditionInstanceId, EntryConditionPlan, EntryOrderPolicy, EntryOrderSpec, EntryPlan,
        EquitySnapshotId, EventId, EvidenceRefs, ExecutionEligibility, ExecutionOrderId, ExitPlan,
        ExitPolicySpec, ExposureBreakdown, FactorBreakdownEntry, FeatureParityStateId,
        FeatureVectorId, MarketContext, MarketId, MarketSelectionId, ModelInputContract,
        ModelRunId, ModelSpecId, ModelTrainingContract, ModelVersionId, OperationDetailDocument,
        OperationLogId, OpportunisticExitPolicy, OrderAmount, OrderId, OrderIntentId,
        PortfolioConstraintsSnapshot, PortfolioOptimizerMeta, PortfolioPlanId,
        PortfolioRejectedSummary, PortfolioRiskBudget, PositionSnapshot, Price, Probability,
        RecommendationFactorBreakdown, RecommendationId, RecommendationIdentity,
        RecommendationReportId, RecommendationTradePlan, ReconciliationEvidence,
        ReconciliationEvidenceChain, ReconciliationId, ReportDataQualitySnapshotId,
        ReportDataQualityTokens, ReportSummary, ReportTriggerKey, RiskEnvelope, RoleCode,
        SelectionExclusionSummary, Shares, SignalCandidateId, SizingPlan, ThesisInvalidationPolicy,
        TokenId, TradePolicyArtifactId, TradePolicyCohortDimension, TradePolicyCohortKey,
        TradePolicyCohortProvenance, Usd, VenueOrderAmount, WorkerId, builtin_research_profiles,
    },
};
use quant_pivot_repository::{
    postgres::{
        PgAccountSnapshotRepository, PgCapitalAllocationRepository, PgEventRepository,
        PgExecutionOrderRepository, PgMarketRepository, PgMarketSelectionRepository,
        PgModelRegistryRepository, PgModelRunRepository, PgOperationLogRepository,
        PgOrderIntentRepository, PgRecommendationReportRepository, PgRecommendationRepository,
        PgReconciliationRepository, PgReportRunRepository, PgReservedCapitalRepository,
        PgRuntimeControlRepository,
    },
    traits::{
        AccountSnapshotRepository, CapitalAllocationRepository, EventRepository,
        ExecutionOrderRepository, MarketRepository, MarketSelectionRepository,
        ModelRegistryRepository, ModelRunRepository, OperationLogRepository, OrderIntentRepository,
        RecommendationReportRepository, RecommendationRepository, ReconciliationRepository,
        ReportRunRepository, ReservedCapitalRepository, RuntimeControlRepository,
    },
};
use quant_pivot_system_tests::{
    postgres::setup_pg,
    support::{
        catalog_fixtures::{make_event, make_market},
        execution_pg_seed,
        execution_pg_seed::{fixture_profile_ref, prepared_order},
        model_serving_fixtures::{ModelVersionFixture, ModelVersionFixtureSeed},
        model_spec_fixtures,
        policy_fixtures::bootstrap_default_policy_bundle,
        report_fixtures,
        report_lifecycle_seed::{persist_and_publish_report, persist_prepared_report},
    },
};
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use sea_orm::{ActiveModelTrait, ActiveValue, DatabaseConnection, EntityTrait, IntoActiveModel};

fn content_hash(seed: char) -> ContentHash {
    ContentHash::parse(&format!("blake3:{}", seed.to_string().repeat(64))).expect("hash")
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
        execution_account_id: execution_pg_seed::fixture_execution_account().execution_account_id,
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

pub async fn account_snapshot_repo_find() {
    let (pool, _container) = setup_pg().await;
    let db = pool.connection().clone();
    execution_pg_seed::ensure_fixture_execution_account(&db).await;
    let repo = PgAccountSnapshotRepository::new(db.clone());

    let snapshot = new_account_snapshot();
    let id = snapshot.account_snapshot_id;
    let created = repo.create(snapshot).await.expect("create snapshot");
    assert_eq!(created.account_snapshot_id, id);
    assert_eq!(created.capital_base_usd, Usd::new(dec!(10000)));
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

pub async fn reserved_returns_zero_empty() {
    let (pool, _container) = setup_pg().await;
    let db = pool.connection().clone();
    let reader = PgReservedCapitalRepository::new(db);
    assert_eq!(reader.sum_reserved_usd().await.expect("sum"), Usd::ZERO);
}

pub async fn report_transaction_persists_intents() {
    let (pool, _container) = setup_pg().await;
    let db = pool.connection().clone();

    let rc_id = seed_runtime_config(&db).await;
    let (model_version_id, model_run_id) = seed_model_version(&db, &rc_id).await;
    let event_id = "evt-1";
    let market_id = "0xmarket";
    seed_market_catalog(&db, event_id, market_id).await;
    let market_selection_id = seed_market_selection(&db, &rc_id, market_id, event_id).await;

    let ids = TxnIds {
        feature_parity_state_id: clear_feature_parity(&db).await,
        account_snapshot: AccountSnapshotId::from_v7(),
        data_quality_snapshot: ReportDataQualitySnapshotId::from_v7(),
        portfolio_plan: PortfolioPlanId::from_v7(),
        report: RecommendationReportId::from_v7(),
        recommendation: RecommendationId::from_v7(),
        condition_instance: EntryConditionInstanceId::from_v7(),
        model_version: model_version_id,
        model_run: model_run_id,
        market_selection: market_selection_id,
        decision_policy_snapshot: rc_id,
        market: market_id.to_owned(),
        event: event_id.to_owned(),
    };
    create_assert_report_transaction(&db, &ids).await;
    assert_recommendation_roundtrip(&db, &ids.report).await;
    explicitly_enable_entry_test(&db).await;
    assert_reserved_tracks_intent(
        &db,
        &ids.recommendation,
        &ids.condition_instance,
        &ids.decision_policy_snapshot,
        &ids.model_version,
    )
    .await;
}

pub async fn find_returns_before_only() {
    let (pool, _container) = setup_pg().await;
    let db = pool.connection().clone();

    let rc_id = seed_runtime_config(&db).await;
    let (model_version_id, model_run_id) = seed_model_version(&db, &rc_id).await;
    let event_id = "evt-1";
    let market_id = "0xmarket";
    seed_market_catalog(&db, event_id, market_id).await;
    let market_selection_id = seed_market_selection(&db, &rc_id, market_id, event_id).await;

    let ids = TxnIds {
        feature_parity_state_id: clear_feature_parity(&db).await,
        account_snapshot: AccountSnapshotId::from_v7(),
        data_quality_snapshot: ReportDataQualitySnapshotId::from_v7(),
        portfolio_plan: PortfolioPlanId::from_v7(),
        report: RecommendationReportId::from_v7(),
        recommendation: RecommendationId::from_v7(),
        condition_instance: EntryConditionInstanceId::from_v7(),
        model_version: model_version_id,
        model_run: model_run_id,
        market_selection: market_selection_id,
        decision_policy_snapshot: rc_id,
        market: market_id.to_owned(),
        event: event_id.to_owned(),
    };

    let report_repo = PgRecommendationReportRepository::new(db.clone());
    persist_prepared_report(
        &db,
        ids.build_report_transaction(),
        &ids.report_trigger_key(),
        10,
    )
    .await;

    let pending_due = report_repo
        .find_expirable(Utc::now() + Duration::hours(2), 100)
        .await
        .expect("find pending delivery");
    assert!(
        pending_due.is_empty(),
        "a report with unverified facts must not enter actionable lifecycle queries"
    );
    let worker_id = WorkerId::from_v7();
    let claimed = report_repo
        .claim_fact_delivery(worker_id, 60)
        .await
        .expect("claim report facts")
        .expect("pending report facts");
    assert_eq!(claimed.recommendation_report_id, ids.report);
    report_repo
        .verify_and_publish_report(&ids.report, worker_id, Utc::now())
        .await
        .expect("verify and publish report facts")
        .into_applied()
        .expect("report delivery claim must remain held");

    // Not yet due: the report's roll-up `valid_until` is in the future (now + 1h).
    let not_due = report_repo
        .find_expirable(Utc::now(), 100)
        .await
        .expect("find before cutoff");
    assert!(
        not_due.is_empty(),
        "a report whose valid_until is in the future must not be expirable"
    );

    // Due: a cutoff past `valid_until` includes the report.
    let due = report_repo
        .find_expirable(Utc::now() + Duration::hours(2), 100)
        .await
        .expect("find due");
    assert_eq!(due, vec![ids.report]);

    // Roll-up is gated on every recommendation being terminal; the report's
    // recommendation is still `Published`, so the roll-up is a no-op.
    let blocked = report_repo
        .roll_up_to_expired(&ids.report, Utc::now(), ids.report_operation_log())
        .await
        .expect("roll up attempt");
    assert!(
        blocked.is_none(),
        "a report with an actionable recommendation must not roll up"
    );

    // Expire the recommendation, then the report rolls up to Expired.
    let recommendation_repo = PgRecommendationRepository::new(db.clone());
    recommendation_repo
        .expire(&ids.recommendation, Utc::now(), ids.report_operation_log())
        .await
        .expect("expire recommendation");
    let rolled = report_repo
        .roll_up_to_expired(&ids.report, Utc::now(), ids.report_operation_log())
        .await
        .expect("roll up");
    assert!(
        rolled.is_some(),
        "all recommendations terminal -> report rolls up to Expired"
    );
    let after_expiry = report_repo
        .find_expirable(Utc::now() + Duration::hours(2), 100)
        .await
        .expect("find after expiry");
    assert!(
        after_expiry.is_empty(),
        "rolled-up reports must not be returned by find_expirable"
    );

    let op_logs = PgOperationLogRepository::new(db.clone())
        .page(OperationLogQuery {
            resource_type: Some(ResourceType::QuantReport),
            ..OperationLogQuery::default()
        })
        .await
        .expect("operation logs");
    let hashed_transitions = op_logs
        .items
        .iter()
        .filter(|log| log.before_hash.is_some() && log.after_hash.is_some())
        .count();
    assert!(
        hashed_transitions >= 2,
        "recommendation expire and report roll-up must write before/after hashes"
    );
}

pub async fn report_recovers_without_claim() {
    let (pool, _container) = setup_pg().await;
    let db = pool.connection().clone();

    let rc_id = seed_runtime_config(&db).await;
    let (model_version_id, model_run_id) = seed_model_version(&db, &rc_id).await;
    let event_id = "evt-1";
    let market_id = "0xmarket";
    seed_market_catalog(&db, event_id, market_id).await;
    let market_selection_id = seed_market_selection(&db, &rc_id, market_id, event_id).await;
    let ids = TxnIds {
        feature_parity_state_id: clear_feature_parity(&db).await,
        account_snapshot: AccountSnapshotId::from_v7(),
        data_quality_snapshot: ReportDataQualitySnapshotId::from_v7(),
        portfolio_plan: PortfolioPlanId::from_v7(),
        report: RecommendationReportId::from_v7(),
        recommendation: RecommendationId::from_v7(),
        condition_instance: EntryConditionInstanceId::from_v7(),
        model_version: model_version_id,
        model_run: model_run_id,
        market_selection: market_selection_id,
        decision_policy_snapshot: rc_id,
        market: market_id.to_owned(),
        event: event_id.to_owned(),
    };
    let repo = PgRecommendationReportRepository::new(db.clone());
    persist_prepared_report(
        &db,
        ids.build_report_transaction(),
        &ids.report_trigger_key(),
        10,
    )
    .await;

    let worker_one = WorkerId::from_v7();
    repo.claim_fact_delivery(worker_one, 60)
        .await
        .expect("claim initial delivery")
        .expect("pending delivery");
    let retrying = repo
        .fail_fact_delivery(
            &ids.report,
            worker_one,
            ReportFactDeliveryStatus::Retrying,
            "injected ClickHouse chunk crash",
        )
        .await
        .expect("schedule retry")
        .into_applied()
        .expect("retry settlement must retain its claim");
    assert!(
        retrying.next_attempt_at.is_some(),
        "retrying delivery must persist its retry deadline"
    );
    assert!(
        repo.claim_fact_delivery(WorkerId::from_v7(), 60)
            .await
            .expect("poll before retry deadline")
            .is_none(),
        "retry must not hot-loop before next_attempt_at"
    );

    let row = Entity::find_by_id(ids.report)
        .one(&db)
        .await
        .expect("load retrying delivery")
        .expect("retrying delivery");
    let mut due = row.into_active_model();
    due.next_attempt_at = ActiveValue::Set(Some(DateTime::<Utc>::UNIX_EPOCH));
    due.update(&db).await.expect("make retry deadline due");

    let worker_two = WorkerId::from_v7();
    let second = repo
        .claim_fact_delivery(worker_two, 60)
        .await
        .expect("claim due retry")
        .expect("retry became claimable");
    assert_eq!(second.attempt_count, 2);
    assert!(
        repo.claim_fact_delivery(WorkerId::from_v7(), 60)
            .await
            .expect("poll live lease")
            .is_none(),
        "a live delivery lease must be exclusive"
    );

    let row = Entity::find_by_id(ids.report)
        .one(&db)
        .await
        .expect("load delivering lease")
        .expect("delivering lease");
    let mut expired = row.into_active_model();
    expired.lease_expires_at = ActiveValue::Set(Some(DateTime::<Utc>::UNIX_EPOCH));
    expired.update(&db).await.expect("expire delivery lease");

    let worker_three = WorkerId::from_v7();
    let recovered = repo
        .claim_fact_delivery(worker_three, 60)
        .await
        .expect("recover expired lease")
        .expect("expired delivery lease");
    assert_eq!(recovered.attempt_count, 3);
    let failed = repo
        .fail_fact_delivery(
            &ids.report,
            worker_three,
            ReportFactDeliveryStatus::Failed,
            "retry budget exhausted",
        )
        .await
        .expect("terminal failure")
        .into_applied()
        .expect("failure settlement must retain its claim");
    assert!(failed.next_attempt_at.is_none());
    assert!(
        repo.claim_fact_delivery(WorkerId::from_v7(), 60)
            .await
            .expect("poll terminal failure")
            .is_none(),
        "terminal delivery failure must require explicit operator intervention"
    );
}

async fn seed_market_catalog(db: &DatabaseConnection, event_id: &str, market_id: &str) {
    execution_pg_seed::ensure_fixture_execution_account(db).await;
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

async fn create_assert_report_transaction(db: &DatabaseConnection, ids: &TxnIds) {
    let trigger_key = (ids).report_trigger_key();
    let typed_trigger_key =
        ReportTriggerKey::parse(trigger_key.clone()).expect("report trigger key");
    let created =
        persist_and_publish_report(db, (ids).build_report_transaction(), &trigger_key, 10).await;
    assert_eq!(created.recommendation_report_id, ids.report);
    assert_eq!(created.capital_base_usd, Usd::new(dec!(10000)));
    assert_eq!(created.account_snapshot_ref, ids.account_snapshot);

    let found_by_trigger = PgReportRunRepository::new(db.clone())
        .find_by_trigger_key(&typed_trigger_key)
        .await
        .expect("find by trigger key")
        .expect("trigger key row");
    assert_eq!(found_by_trigger.output_report_id, Some(ids.report));

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
    db: &DatabaseConnection,
    report_id: &RecommendationReportId,
) {
    let recs = PgRecommendationRepository::new(db.clone())
        .find_by_report(report_id)
        .await
        .expect("find recommendations");
    assert_eq!(recs.len(), 1);
    let sizing = recs[0].trade_plan.sizing().expect("frozen sizing");
    assert_eq!(sizing.suggested_usd, Usd::new(dec!(250)));
    assert_eq!(sizing.sizing_model, SizingModelKind::Kelly);
}

async fn assert_reserved_tracks_intent(
    db: &DatabaseConnection,
    recommendation_id: &RecommendationId,
    condition_instance_id: &EntryConditionInstanceId,
    decision_policy_snapshot_id: &DecisionPolicySnapshotId,
    model_version_id: &ModelVersionId,
) {
    let reserved_repo = PgReservedCapitalRepository::new(db.clone());
    assert_eq!(
        reserved_repo.sum_reserved_usd().await.expect("sum"),
        Usd::ZERO
    );

    let intent_repo = PgOrderIntentRepository::new(db.clone());
    let order_intent_id = OrderIntentId::from_v7();
    intent_repo
        .create_with_allocation(
            NewOrderIntent {
                order_intent_id,
                recommendation_id: *recommendation_id,
                execution_account_id: execution_pg_seed::fixture_execution_account()
                    .execution_account_id,
                runtime_mode: QuantRuntimeMode::SemiAuto,
                decision_policy_snapshot_id: *decision_policy_snapshot_id,
                model_version_id: *model_version_id,
                research_profile_artifact_id: fixture_profile_ref().artifact_id(),
                intent_kind: OrderIntentKind::Buy,
                status: OrderIntentStatus::PendingApproval,
                approval_status: ApprovalStatus::Pending,
                approved_by: None,
                approval_reason: None,
                approved_at: None,
                policy_id: None,
                policy_hash: None,
                status_reason: None,
                admission_trace_ref: None,
                condition_instance_id: *condition_instance_id,
                entry_order_json: EntryOrderSpec {
                    token_id: TokenId::new("token-1"),
                    side: Side::Buy,
                    order_type: OrderType::Gtc,
                    post_only: false,
                    limit_price: Price::new(dec!(0.6)),
                    amount: OrderAmount::Shares(Shares::new(dec!(416.66))),
                    max_slippage_bps: Bps::new(dec!(50)),
                    valid_until: Utc::now(),
                },
                exit_policy_json: ExitPolicySpec {
                    take_profit_price: Some(Price::new(dec!(0.8))),
                    take_profit_pct: None,
                    stop_loss_price: Some(Price::new(dec!(0.5))),
                    stop_loss_pct: None,
                    time_exit_at: Some(Utc::now()),
                    max_hold_secs: None,
                    trailing_stop: None,
                    thesis_invalidation: ThesisInvalidationPolicy {
                        min_score_retention: dec!(0.6),
                        min_expected_return_bps: Bps::ZERO,
                        require_execution_eligibility: true,
                    },
                    opportunistic_exit: opportunistic_exit_policy(),
                    scale_out_targets: Vec::new(),
                    settlement_mode: ExitSettlementMode::ExitBeforeResolution,
                    redeem_policy: RedeemPolicy::Manual,
                    manual_review_at: None,
                    entry_reference_price: Price::new(dec!(0.6)),
                    entry_composite_score: Probability::new(dec!(0.8)),
                },
                risk_envelope_hash: content_hash('e'),
                expires_at: Utc::now(),
            },
            NewCapitalAllocation {
                capital_allocation_id: CapitalAllocationId::from_v7(),
                order_intent_id,
                recommendation_id: *recommendation_id,
                state: CapitalAllocationState::Allocated,
                planned_usd: Usd::new(dec!(250)),
                allocated_usd: Usd::new(dec!(250)),
                locked_usd: Usd::ZERO,
                spent_usd: Usd::ZERO,
                released_usd: Usd::ZERO,
                reason: "intent created".to_owned(),
            },
        )
        .await
        .expect("create intent with allocation");

    // Allocated capital is reserved …
    assert_eq!(
        reserved_repo.sum_reserved_usd().await.expect("sum"),
        Usd::new(dec!(250))
    );

    // … and a reject releases it in the same transaction as the intent move.
    let rejected = intent_repo
        .reject(
            &order_intent_id,
            "operator veto".to_owned(),
            Utc::now(),
            intent_operation_log(&order_intent_id, "quant.intent.reject.test"),
        )
        .await
        .expect("reject intent");
    assert_eq!(rejected.status, OrderIntentStatus::Rejected);
    assert_eq!(
        reserved_repo.sum_reserved_usd().await.expect("sum"),
        Usd::ZERO,
        "rejected intent must release its reservation"
    );
}

pub async fn execution_order_reconciliation_trip() {
    let (pool, _container) = setup_pg().await;
    let db = pool.connection().clone();
    let ids = seed_report_fixture(&db).await;
    explicitly_enable_entry_test(&db).await;
    let order_intent_id = create_pending_intent(&db, &ids).await;
    let execution_order_id = ExecutionOrderId::from_v7();

    let execution_repo = PgExecutionOrderRepository::new(db.clone());
    create_submit_execution_order(&execution_repo, &ids, &order_intent_id, &execution_order_id)
        .await;

    let reconciliation_repo = PgReconciliationRepository::new(db.clone());
    append_and_resolve_reconciliation(&reconciliation_repo, &execution_order_id, &order_intent_id)
        .await;
}

async fn create_submit_execution_order(
    execution_repo: &PgExecutionOrderRepository,
    ids: &TxnIds,
    order_intent_id: &OrderIntentId,
    execution_order_id: &ExecutionOrderId,
) {
    let order = execution_repo
        .create(NewExecutionOrder {
            execution_order_id: *execution_order_id,
            order_intent_id: *order_intent_id,
            order_phase: ExecutionOrderPhase::Entry,
            market_id: MarketId::new(ids.market.clone()),
            token_id: TokenId::new("token-1"),
            side: Side::Buy,
            order_type: OrderTypeKind::Gtc,
            price: Price::new(dec!(0.6)),
            shares: Shares::new(dec!(100)),
            cost_usd: Usd::new(dec!(60)),
            prepared_order_json: prepared_order(
                TokenId::new("token-1"),
                Side::Buy,
                OrderType::Gtc,
                VenueOrderAmount::Shares(Shares::new(dec!(100))),
                Usd::ZERO,
                Shares::new(dec!(100)),
                Price::new(dec!(0.6)),
            ),
            venue_order_id: None,
            venue_status: None,
            state: ExecutionOrderState::Planned,
            submitted_at: None,
            filled_at: None,
            cancelled_at: None,
            gtd_expiration_at: None,
            error_message: None,
        })
        .await
        .expect("create execution order");
    assert_eq!(order.state, ExecutionOrderState::Planned);

    let submitted = execution_repo
        .transition(
            execution_order_id,
            ExecutionOrderPatch {
                state: Patch::set(ExecutionOrderState::Submitted),
                venue_order_id: NullablePatch::set(OrderId::new("0xvenue")),
                venue_status: NullablePatch::set(VenueOrderStatus::Open),
                submitted_at: NullablePatch::set(Utc::now()),
                ..ExecutionOrderPatch::default()
            },
        )
        .await
        .expect("submit execution order");
    assert_eq!(submitted.state, ExecutionOrderState::Submitted);
    assert_eq!(submitted.venue_status, Some(VenueOrderStatus::Open));
}

async fn append_and_resolve_reconciliation(
    reconciliation_repo: &PgReconciliationRepository,
    execution_order_id: &ExecutionOrderId,
    order_intent_id: &OrderIntentId,
) {
    let reconciliation_id = ReconciliationId::from_v7();
    reconciliation_repo
        .create(NewReconciliation {
            reconciliation_id,
            execution_order_id: *execution_order_id,
            order_intent_id: *order_intent_id,
            result: ReconciliationResult::Unresolvable,
            evidence_json: ReconciliationEvidenceChain::default(),
            venue_filled_shares: None,
            venue_avg_price: None,
            expected_cash_delta_usd: None,
            venue_cash_delta_usd: None,
            realized_pnl_usd: None,
            expected_fee_usd: None,
            observed_fee_usd: None,
            fee_delta_usd: None,
            resolved_by: None,
            resolved_at: None,
        })
        .await
        .expect("create reconciliation");

    assert!(
        reconciliation_repo
            .has_unresolvable()
            .await
            .expect("has unresolvable")
    );
    let appended = reconciliation_repo
        .append_evidence(
            &reconciliation_id,
            AppendReconciliationEvidence {
                evidence: ReconciliationEvidence {
                    kind: ReconciliationEvidenceKind::ClobOrderStatus,
                    observed_at: Utc::now(),
                    detail: "venue order still open".to_owned(),
                    venue_ref: Some("0xvenue".to_owned()),
                    shares: None,
                    price: None,
                    fee_evidence: None,
                },
            },
        )
        .await
        .expect("append evidence");
    assert_eq!(appended.evidence_json.0.len(), 1);

    let resolved = reconciliation_repo
        .patch(
            &reconciliation_id,
            ReconciliationPatch {
                result: Patch::set(ReconciliationResult::PartiallyFilled),
                venue_filled_shares: NullablePatch::set(Shares::new(dec!(10))),
                venue_avg_price: NullablePatch::set(Price::new(dec!(0.6))),
                expected_cash_delta_usd: NullablePatch::set(Usd::ZERO),
                venue_cash_delta_usd: NullablePatch::set(Usd::ZERO),
                realized_pnl_usd: NullablePatch::Keep,
                expected_fee_usd: NullablePatch::set(Usd::ZERO),
                observed_fee_usd: NullablePatch::set(Usd::ZERO),
                fee_delta_usd: NullablePatch::set(Usd::ZERO),
                resolved_by: NullablePatch::set("operator".to_owned()),
                resolved_at: NullablePatch::set(Utc::now()),
            },
        )
        .await
        .expect("resolve reconciliation");
    assert_eq!(resolved.result, ReconciliationResult::PartiallyFilled);
    assert!(
        !reconciliation_repo
            .has_unresolvable()
            .await
            .expect("has unresolvable after resolve")
    );
}

pub async fn capital_kill_switch_trip() {
    let (pool, _container) = setup_pg().await;
    let db = pool.connection().clone();
    let ids = seed_report_fixture(&db).await;
    explicitly_enable_entry_test(&db).await;
    // Creating an intent reserves its capital atomically (planned = allocated).
    let _order_intent_id = create_pending_intent(&db, &ids).await;

    let capital_repo = PgCapitalAllocationRepository::new(db.clone());
    assert_eq!(
        CapitalAllocationRepository::sum_reserved_usd(&capital_repo)
            .await
            .expect("sum reserved"),
        Usd::new(dec!(250))
    );
    assert!(
        !capital_repo
            .has_impaired()
            .await
            .expect("has_impaired query"),
        "a freshly allocated intent must not be impaired"
    );

    let runtime_control = PgRuntimeControlRepository::new(db.clone());
    let current = runtime_control.load().await.expect("load runtime control");
    let control = runtime_control
        .compare_and_set(RuntimeControlUpdate {
            expected_revision: current.revision,
            quant_runtime_mode: None,
            settlement_write_policy: None,
            kill_switch_state: Some(KillSwitchState::ExitOnly),
            kill_switch_requires_ack: Some(false),
            actor: "operator".to_owned(),
            reason: "recovery default".to_owned(),
        })
        .await
        .expect("update runtime control");
    assert_eq!(control.kill_switch_state, KillSwitchState::ExitOnly);
    assert_eq!(
        runtime_control
            .load()
            .await
            .expect("load runtime control")
            .kill_switch_state,
        KillSwitchState::ExitOnly
    );
}

async fn seed_report_fixture(db: &DatabaseConnection) -> TxnIds {
    let rc_id = seed_runtime_config(db).await;
    let (model_version_id, model_run_id) = seed_model_version(db, &rc_id).await;
    let event_id = "evt-1";
    let market_id = "0xmarket";
    seed_market_catalog(db, event_id, market_id).await;
    let market_selection_id = seed_market_selection(db, &rc_id, market_id, event_id).await;
    let ids = TxnIds {
        feature_parity_state_id: clear_feature_parity(db).await,
        account_snapshot: AccountSnapshotId::from_v7(),
        data_quality_snapshot: ReportDataQualitySnapshotId::from_v7(),
        portfolio_plan: PortfolioPlanId::from_v7(),
        report: RecommendationReportId::from_v7(),
        recommendation: RecommendationId::from_v7(),
        condition_instance: EntryConditionInstanceId::from_v7(),
        model_version: model_version_id,
        model_run: model_run_id,
        market_selection: market_selection_id,
        decision_policy_snapshot: rc_id,
        market: market_id.to_owned(),
        event: event_id.to_owned(),
    };
    persist_and_publish_report(
        db,
        ids.build_report_transaction(),
        &ids.report_trigger_key(),
        10,
    )
    .await;
    ids
}

async fn clear_feature_parity(db: &DatabaseConnection) -> FeatureParityStateId {
    let state_id = FeatureParityStateId::from_v7();
    QuantFeatureParityStateEntity::insert(
        NewFeatureParityState {
            state_id,
            state: FeatureParityLatchState::Clear,
            transition: FeatureParityStateTransition::GovernedAcknowledge,
            cause_run_id: None,
            recovery_run_id: None,
            previous_state_id: None,
            actor: Some("pg-account-test".to_owned()),
            acting_role: Some(RoleCode::new("risk_owner")),
            reason: "test fixture clear generation".to_owned(),
        }
        .into_active_model(),
    )
    .exec(db)
    .await
    .expect("seed feature parity clear generation");
    state_id
}

async fn create_pending_intent(db: &DatabaseConnection, ids: &TxnIds) -> OrderIntentId {
    let order_intent_id = OrderIntentId::from_v7();
    PgOrderIntentRepository::new(db.clone())
        .create_with_allocation(
            NewOrderIntent {
                order_intent_id,
                recommendation_id: ids.recommendation,
                execution_account_id: execution_pg_seed::fixture_execution_account()
                    .execution_account_id,
                runtime_mode: QuantRuntimeMode::SemiAuto,
                decision_policy_snapshot_id: ids.decision_policy_snapshot,
                model_version_id: ids.model_version,
                research_profile_artifact_id: fixture_profile_ref().artifact_id(),
                intent_kind: OrderIntentKind::Buy,
                status: OrderIntentStatus::PendingApproval,
                approval_status: ApprovalStatus::Pending,
                approved_by: None,
                approval_reason: None,
                approved_at: None,
                policy_id: None,
                policy_hash: None,
                status_reason: None,
                admission_trace_ref: None,
                condition_instance_id: ids.condition_instance,
                entry_order_json: EntryOrderSpec {
                    token_id: TokenId::new("token-1"),
                    side: Side::Buy,
                    order_type: OrderType::Gtc,
                    post_only: false,
                    limit_price: Price::new(dec!(0.6)),
                    amount: OrderAmount::Shares(Shares::new(dec!(416.66))),
                    max_slippage_bps: Bps::new(dec!(50)),
                    valid_until: Utc::now(),
                },
                exit_policy_json: ExitPolicySpec {
                    take_profit_price: Some(Price::new(dec!(0.8))),
                    take_profit_pct: None,
                    stop_loss_price: Some(Price::new(dec!(0.5))),
                    stop_loss_pct: None,
                    time_exit_at: Some(Utc::now()),
                    max_hold_secs: None,
                    trailing_stop: None,
                    thesis_invalidation: ThesisInvalidationPolicy {
                        min_score_retention: dec!(0.6),
                        min_expected_return_bps: Bps::ZERO,
                        require_execution_eligibility: true,
                    },
                    opportunistic_exit: opportunistic_exit_policy(),
                    scale_out_targets: Vec::new(),
                    settlement_mode: ExitSettlementMode::ExitBeforeResolution,
                    redeem_policy: RedeemPolicy::Manual,
                    manual_review_at: None,
                    entry_reference_price: Price::new(dec!(0.6)),
                    entry_composite_score: Probability::new(dec!(0.8)),
                },
                risk_envelope_hash: content_hash('e'),
                expires_at: Utc::now(),
            },
            NewCapitalAllocation {
                capital_allocation_id: CapitalAllocationId::from_v7(),
                order_intent_id,
                recommendation_id: ids.recommendation,
                state: CapitalAllocationState::Allocated,
                planned_usd: Usd::new(dec!(250)),
                allocated_usd: Usd::new(dec!(250)),
                locked_usd: Usd::ZERO,
                spent_usd: Usd::ZERO,
                released_usd: Usd::ZERO,
                reason: "intent created".to_owned(),
            },
        )
        .await
        .expect("create intent with allocation")
        .order_intent_id
}

async fn explicitly_enable_entry_test(db: &DatabaseConnection) {
    execution_pg_seed::enable_test_admission(db, "pg-account-it-operator").await;
}

// ── Seed helpers ────────────────────────────────────────────────────────────

async fn seed_runtime_config(db: &DatabaseConnection) -> DecisionPolicySnapshotId {
    bootstrap_default_policy_bundle(db, "pg-account-it", "integration test").await
}

async fn seed_model_version(
    db: &DatabaseConnection,
    rc_id: &DecisionPolicySnapshotId,
) -> (ModelVersionId, ModelRunId) {
    let registry = PgModelRegistryRepository::new(db.clone());
    let model_spec_id = ModelSpecId::from_v7();
    registry
        .create_model_spec(model_spec_fixtures::new_model_spec_fixture(
            model_spec_id,
            "pg-account-it",
            ModelFamily::WeightedFactor,
            model_spec_fixtures::pooled_horizon_secs(),
            ModelInputContract::single_required("book.mid"),
            ModelTrainingContract::settlement_default(),
        ))
        .await
        .expect("model spec");

    let model_version_id = ModelVersionId::from_v7();
    let version = ModelVersionFixture::prepare(
        db,
        ModelVersionFixtureSeed::training(
            format!("account-capital:{model_version_id}"),
            model_version_id,
            model_spec_id,
            content_hash('a'),
        ),
    )
    .await
    .expect("prepare exact model version");
    ModelVersionFixture::persist_route_candidate(db, version)
        .await
        .expect("publish model version through exact parity proof");

    let model_run_id = ModelRunId::from_v7();
    let runs = PgModelRunRepository::new(db.clone());
    let window_at = Utc::now();
    runs.create(NewModelRun {
        model_run_id,
        run_kind: ModelRunKind::LiveInference,
        model_version_id: Some(model_version_id),
        decision_policy_snapshot_id: *rc_id,
        market_selection_id: None,
        window_start: window_at,
        window_end: window_at,
        input_hash: content_hash('d'),
    })
    .await
    .expect("create model run");
    runs.succeed(&model_run_id, content_hash('e'), None)
        .await
        .expect("finish model run");

    (model_version_id, model_run_id)
}

async fn seed_market_selection(
    db: &DatabaseConnection,
    rc_id: &DecisionPolicySnapshotId,
    _market_id: &str,
    _event_id: &str,
) -> MarketSelectionId {
    let id = MarketSelectionId::from_v7();
    PgMarketSelectionRepository::new(db.clone())
        .create_snapshot(
            NewMarketSelection {
                market_selection_id: id,
                decision_at: Utc::now(),
                decision_policy_snapshot_id: *rc_id,
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

// ── Report transaction builder ────────────────────────────────────────────────

struct TxnIds {
    feature_parity_state_id: FeatureParityStateId,
    account_snapshot: AccountSnapshotId,
    data_quality_snapshot: ReportDataQualitySnapshotId,
    portfolio_plan: PortfolioPlanId,
    report: RecommendationReportId,
    recommendation: RecommendationId,
    condition_instance: EntryConditionInstanceId,
    model_version: ModelVersionId,
    model_run: ModelRunId,
    market_selection: MarketSelectionId,
    decision_policy_snapshot: DecisionPolicySnapshotId,
    market: String,
    event: String,
}

impl TxnIds {
    fn build_report_transaction(&self) -> NewReportTransaction {
        let equity_snapshot_id = EquitySnapshotId::from_v7();
        let decision_at = Utc::now();
        let report = report_row(self, equity_snapshot_id, decision_at);
        let sampled_feature_parity = report_fixtures::sampled_parity(&report);
        let recommendation = report_recommendation(self, decision_at);
        let entry_condition_instance = NewEntryConditionInstance {
            condition_instance_id: self.condition_instance,
            recommendation_id: self.recommendation,
            artifact_id: None,
            artifact_hash: None,
            state: EntryConditionState::NotRequired,
            truth_json: Some(ConditionTruth::Satisfied),
            revision: 0,
            evaluation_hash: None,
            input_fingerprint: None,
            continuity_hash: None,
            fold_state_json: EntryConditionFoldState::default(),
            confirmation_started_at: None,
            last_evaluated_at: None,
            next_evaluation_at: None,
            expires_at: recommendation.valid_until,
            lease_owner: None,
            lease_expires_at: None,
            lease_epoch: 0,
            claimed_by_intent_id: None,
            claim_admission_state_version: None,
            consumed_at: None,
        };
        NewReportTransaction {
            feature_parity_state_id: Some(self.feature_parity_state_id),
            account_snapshot: NewAccountSnapshot {
                account_snapshot_id: self.account_snapshot,
                ..new_account_snapshot()
            },
            equity_snapshot: report_equity_snapshot(self, &equity_snapshot_id, decision_at),
            data_quality_snapshot: NewReportDataQualitySnapshot {
                report_data_quality_snapshot_id: self.data_quality_snapshot,
                decision_at,
                decision_policy_snapshot_id: self.decision_policy_snapshot,
                tokens_json: ReportDataQualityTokens(Vec::new()),
            },
            portfolio_plan: report_portfolio_plan(self, decision_at),
            report,
            recommendations: vec![recommendation],
            entry_condition_artifacts: Vec::new(),
            entry_condition_instances: vec![entry_condition_instance],
            sampled_feature_parity: Some(sampled_feature_parity),
            fact_delivery: Some(report_fixtures::pending_fact_delivery(&self.report)),
            operation_log: (self).report_operation_log(),
        }
    }
}

const fn report_equity_snapshot(
    ids: &TxnIds,
    equity_snapshot_id: &EquitySnapshotId,
    decision_at: DateTime<Utc>,
) -> NewEquitySnapshot {
    NewEquitySnapshot {
        equity_snapshot_id: *equity_snapshot_id,
        as_of: decision_at,
        source: AccountSource::Polymarket,
        venue_net_liquidation_usd: Usd::new(dec!(10000)),
        capital_base_usd: Usd::new(dec!(10000)),
        available_usd: Usd::new(dec!(9000)),
        reserved_usd: Usd::ZERO,
        realized_pnl_cumulative_usd: Usd::ZERO,
        unrealized_pnl_usd: Usd::ZERO,
        high_water_mark_usd: Usd::new(dec!(10000)),
        drawdown_pct: Decimal::ZERO,
        account_snapshot_ref: Some(ids.account_snapshot),
    }
}

fn report_portfolio_plan(ids: &TxnIds, decision_at: DateTime<Utc>) -> NewPortfolioPlan {
    NewPortfolioPlan {
        portfolio_plan_id: ids.portfolio_plan,
        model_run_id: Some(ids.model_run),
        market_selection_id: ids.market_selection,
        decision_at,
        budget_usd: Usd::new(dec!(10000)),
        allocated_usd: Usd::new(dec!(250)),
        risk_budget_json: PortfolioRiskBudget::default(),
        constraints_json: PortfolioConstraintsSnapshot::default(),
        rejected_summary: PortfolioRejectedSummary::default(),
        optimizer_meta_json: PortfolioOptimizerMeta::default(),
    }
}

fn report_row(
    ids: &TxnIds,
    equity_snapshot_id: EquitySnapshotId,
    decision_at: DateTime<Utc>,
) -> NewRecommendationReport {
    NewRecommendationReport {
        recommendation_report_id: ids.report,
        research_profile_artifact_id: execution_pg_seed::fixture_profile_ref().artifact_id(),
        report_kind: ReportKind::TopN,
        decision_at,
        horizon_secs: 86_400,
        runtime_mode: QuantRuntimeMode::ReportOnly,
        decision_policy_snapshot_id: ids.decision_policy_snapshot,
        model_run_id: None,
        model_version_id: ids.model_version,
        market_selection_id: ids.market_selection,
        portfolio_plan_id: ids.portfolio_plan,
        top_n: 20,
        status: RecommendationReportStatus::Prepared,
        account_source: AccountSource::Polymarket,
        capital_base_usd: Usd::new(dec!(10000)),
        account_snapshot_ref: ids.account_snapshot,
        equity_snapshot_ref: equity_snapshot_id,
        data_quality_snapshot_ref: ids.data_quality_snapshot,
        summary_json: report_summary(),
        published_at: None,
        successor_report_id: None,
        superseded_at: None,
        obsoleted_at: None,
        valid_until: Some(decision_at + Duration::hours(1)),
        revoked_at: None,
        expired_at: None,
        status_reason: None,
    }
}

fn report_recommendation(ids: &TxnIds, decision_at: DateTime<Utc>) -> NewRecommendation {
    NewRecommendation {
        recommendation_id: ids.recommendation,
        research_profile_artifact_id: execution_pg_seed::fixture_profile_ref().artifact_id(),
        recommendation_report_id: ids.report,
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
        trade_plan: trade_plan(Usd::new(dec!(250))),
        factor_breakdown: factor_breakdown(),
        evidence_refs: evidence_refs(),
        execution_eligibility: execution_eligibility(),
        valid_from: decision_at,
        valid_until: decision_at + Duration::hours(1),
        status: RecommendationStatus::Prepared,
    }
}

impl TxnIds {
    fn report_operation_log(&self) -> NewOperationLog {
        NewOperationLog {
            id: OperationLogId::from_v7(),
            request_id: (self).report_trigger_key().into(),
            actor_user_id: None,
            actor_username: Some("system".to_owned()),
            acting_role: Some("test".into()),
            category: OperationCategory::QuantReport,
            action: "publish".into(),
            resource_type: Some(ResourceType::QuantReport),
            resource_id: Some(self.report.to_string()),
            http_method: OperationHttpMethod::System,
            http_path: "/test/quant/report".to_owned(),
            http_status: 201,
            outcome: OperationOutcome::Success,
            client_ip: None,
            user_agent: None,
            latency_ms: 0,
            detail: OperationDetailDocument::try_from(serde_json::json!({ "test": true }))
                .expect("static operation detail"),
            before_hash: None,
            after_hash: None,
            governance_audit_event_id: None,
            governance_audit_sequence: None,
        }
    }
}

fn intent_operation_log(intent_id: &OrderIntentId, action: &str) -> NewOperationLog {
    NewOperationLog {
        id: OperationLogId::from_v7(),
        request_id: format!("account-capital:{action}:{intent_id}").into(),
        actor_user_id: None,
        actor_username: Some("test".to_owned()),
        acting_role: Some("test".into()),
        category: OperationCategory::Governance,
        action: action.into(),
        resource_type: Some(ResourceType::OrderIntent),
        resource_id: Some(intent_id.to_string()),
        http_method: OperationHttpMethod::System,
        http_path: format!("/test/quant/intents/{intent_id}/reject"),
        http_status: 200,
        outcome: OperationOutcome::Success,
        client_ip: None,
        user_agent: None,
        latency_ms: 0,
        detail: OperationDetailDocument::try_from(serde_json::json!({ "test": true }))
            .expect("static operation detail"),
        before_hash: None,
        after_hash: None,
        governance_audit_event_id: None,
        governance_audit_sequence: None,
    }
}

impl TxnIds {
    fn report_trigger_key(&self) -> String {
        format!("scheduled:test:{}", self.report)
    }
}

// ── Payload builders ──────────────────────────────────────────────────────────

fn entry_plan() -> EntryPlan {
    EntryPlan {
        condition: EntryConditionPlan::Immediate,
        order_policy: EntryOrderPolicy::Passive {
            limit_price: Price::new(dec!(0.6)),
            post_only: true,
        },
        max_slippage_bps: Bps::new(dec!(50)),
        valid_from: Utc::now(),
        valid_until: Utc::now(),
        min_depth_usd: Usd::new(dec!(100)),
        max_book_age_ms: 2_000,
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
        edge_uncertainty_shrink_applied: None,
        correlation_shrink_applied: None,
        f_star_applied: None,
        kelly_fraction_config_applied: None,
        confidence_shrink_applied: None,
        drawdown_shrink_applied: None,
        raw_fraction_applied: None,
        position_cap_fraction_applied: None,
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
        scale_out_targets: Vec::new(),
        trailing_stop: None,
        thesis_invalidation: ThesisInvalidationPolicy {
            min_score_retention: dec!(0.6),
            min_expected_return_bps: Bps::ZERO,
            require_execution_eligibility: true,
        },
        opportunistic_exit: opportunistic_exit_policy(),
        settlement_mode: ExitSettlementMode::HoldToResolution,
        redeem_policy: RedeemPolicy::Manual,
        manual_review_at: None,
        exit_reason: "tp/sl".to_owned(),
    }
}

const fn opportunistic_exit_policy() -> OpportunisticExitPolicy {
    OpportunisticExitPolicy {
        min_confidence: Probability::new(dec!(0.65)),
        min_expected_alpha_bps: Bps::new(dec!(50)),
        min_p_exit_better: Probability::new(dec!(0.5)),
        max_cumulative_exit_pct: dec!(1),
        min_incremental_exit_pct: dec!(0.1),
    }
}

fn trade_plan(notional: Usd) -> RecommendationTradePlan {
    let artifact_hash = content_hash('f');
    let dimension = TradePolicyCohortDimension {
        methodology_id: "fixture-v1".to_owned(),
        methodology_hash: artifact_hash,
        bucket_id: "fixture".to_owned(),
    };
    RecommendationTradePlan::Frozen {
        policy: Box::new(TradePolicyCohortProvenance {
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
                entry_price_min: Price::new(dec!(0.01)),
                entry_price_max: Price::new(dec!(0.99)),
                cash_budget_tier: notional,
                liquidity: dimension.clone(),
                volatility: dimension,
            },
        }),
        entry: entry_plan(),
        sizing: Box::new(sizing_plan(notional)),
        exit: Box::new(exit_plan()),
        risk_envelope: Box::new(risk_envelope()),
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
        value_state: FactorValueState::Scored,
        raw_value: Some(dec!(1234.5)),
        normalized_score: Some(Probability::new(dec!(0.8))),
        normalization_source: Some(NormalizationSource::CrossSection),
        indeterminate_reason: None,
        weight: dec!(0.4),
        contribution: dec!(0.32),
        confidence: Probability::new(dec!(0.75)),
        direction: FactorDirection::Positive,
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
        tick_size: TickSize::Hundredth,
        fee_rate: None,
    }
}

fn book_snapshot_ref() -> BookSnapshotRef {
    BookSnapshotRef::from_str(&format!(
        "book:l2|token-1|00000000-0000-0000-0000-000000000001|1|blake3:{}|1700000000|1700000000@blake3:{}",
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
        ineligibility_reasons: Vec::new(),
        approval_required: false,
        auto_policy_id: None,
        uncalibrated_watermark: false,
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
        aggregate_exposure_cap_usd: None,
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
