//! Phase 05.3 — execution admission engine + check integration tests.
//!
//! The fixture passes all 22 checks; each test mutates exactly one input field
//! to drive the matching deny/defer outcome. Checks are pure, so the engine is
//! exercised entirely in-memory with no DB / venue.

use std::{collections::HashSet, sync::Arc};

use async_trait::async_trait;
use chrono::{DateTime, Duration, TimeZone, Utc};
use quant_pivot_core::{
    execution::{
        AdmissionDecision, AdmissionInput, AdmissionInputBuilder, AdmissionInputBuilderDeps,
        AdmissionSeams, DefaultAdmissionEngine, ExecutionAdmissionEngine, ExitMonitorHealthHandle,
        StateVersion, VenueHealth, VenueHealthHandle,
    },
    governance::{KillSwitchHandle, RuntimeModeHandle},
    observability::metrics_hub::MetricsHub,
    pipeline::{book_store::BookStore, market_registry::MarketRegistry},
    runtime_config::RuntimeConfigStore,
    service::account::{AccountProviderFactory, ReservedCapitalReader},
};
use quant_pivot_error::{QuantError, QuantResult, account::AccountError, storage::StorageError};
use quant_pivot_models::{
    domain::{
        AppendReconciliationEvidence, ApproveOrderIntent, ApproveOrderIntentOutcome, BookLevel,
        BookSnapshot, CapitalAllocationInfo, DataQualityPort, DataQualitySnapshot,
        ExecutionOrderInfo, ExecutionOrderPatch, MarketInfo, MarketPageQuery, ModelSpecInfo,
        ModelVersionInfo, NewCapitalAllocation, NewExecutionOrder, NewModelSpec, NewModelVersion,
        NewOperationLog, NewOrderIntent, NewReconciliation, NewReportTransaction,
        NewRuntimeConfigActivation, NewRuntimeConfigVersion, OrderIntentInfo, OrderIntentListQuery,
        Paginated, QuantReportListQuery, RecommendationInfo, RecommendationReportInfo,
        ReconciliationInfo, ReconciliationListQuery, ReconciliationPatch,
        RuntimeConfigActivationInfo, RuntimeConfigVersionInfo, UpsertMarket,
    },
    enums::{
        common::{MarketCategory, OrderType, Side, TickSize},
        execution::{
            AdmissionCheckId, AdmissionOutcome, ApprovalInvalidation, CapitalAllocationState,
            ExitState, KillSwitchState, OrderIntentKind,
        },
        market::MarketStatus,
        quant::{
            AccountSource, ApprovalStatus, EntryTriggerKind, OrderIntentStatus, OutcomeSide,
            QuantRuntimeMode, RecommendationReportStatus, ReportKind,
        },
        runtime_config::RuntimeConfigVersionSource,
    },
    runtime_config::RuntimeConfig,
    types::{
        Bps, CapitalAllocationId, ContentHash, EntryOrderSpec, EventId, ExecutedPartialExitNodes,
        ExecutionOrderId, ExitPolicySpec, MarketId, ModelSpecId, ModelVersionId,
        OpportunisticExitState, OrderIntentId, Price, RecommendationId, RecommendationReportId,
        ReconciliationId, RuntimeConfigVersionId, SchemaVersion, Shares, TokenId, Usd,
    },
};
use quant_pivot_repository::traits::{
    CapitalAllocationRepository, ExecutionOrderRepository, MarketRepository,
    ModelRegistryRepository, OrderIntentRepository, RecommendationReportRepository,
    RecommendationRepository, ReconciliationRepository, RuntimeConfigVersionRepository,
};
use quant_pivot_research::portfolio::AccountSnapshot;
use quant_pivot_test_support::report_fixtures;
use rust_decimal_macros::dec;

const NOW_SECS: i64 = 1_700_001_000;
const NOW_MS: u64 = 1_700_001_000_000;

fn now() -> DateTime<Utc> {
    Utc.timestamp_opt(NOW_SECS, 0).unwrap()
}

fn other_hash() -> ContentHash {
    ContentHash::parse(format!("blake3:{}", "1".repeat(64))).expect("hash")
}

fn recommendation() -> RecommendationInfo {
    report_fixtures::recommendation(
        RecommendationReportId::from_v7(),
        RecommendationId::from_v7(),
        1,
        "0xmkt",
        OutcomeSide::Yes,
        Usd::new(dec!(250)),
    )
}

fn report(rec: &RecommendationInfo) -> RecommendationReportInfo {
    report_fixtures::report(
        rec.recommendation_report_id.clone(),
        ReportKind::TopN,
        RecommendationReportStatus::Published,
    )
}

fn intent(rec: &RecommendationInfo) -> OrderIntentInfo {
    OrderIntentInfo {
        order_intent_id: OrderIntentId::from_v7(),
        recommendation_id: rec.recommendation_id.clone(),
        runtime_mode: QuantRuntimeMode::SemiAuto,
        runtime_config_version_id: RuntimeConfigVersionId::from_v7(),
        model_version_id: ModelVersionId::from_v7(),
        intent_kind: OrderIntentKind::Buy,
        status: OrderIntentStatus::Approved,
        approval_status: ApprovalStatus::Approved,
        approved_by: None,
        approval_reason: None,
        approved_at: Some(now()),
        policy_id: None,
        policy_hash: None,
        status_reason: None,
        admission_trace_ref: None,
        entry_order_json: EntryOrderSpec {
            token_id: rec.token_id.clone(),
            side: Side::Buy,
            order_type: OrderType::Gtc,
            limit_price: Price::new(dec!(0.43)),
            shares: Shares::new(dec!(500)),
            max_slippage_bps: Bps::new(dec!(50)),
            valid_until: rec.entry_plan.valid_until,
        },
        exit_policy_json: ExitPolicySpec {
            take_profit_price: rec.exit_plan.take_profit_price,
            take_profit_pct: rec.exit_plan.take_profit_pct,
            stop_loss_price: rec.exit_plan.stop_loss_price,
            stop_loss_pct: rec.exit_plan.stop_loss_pct,
            time_exit_at: None,
            max_hold_secs: rec.exit_plan.max_hold_secs,
            trailing_stop: rec.exit_plan.trailing_stop.clone(),
            signal_invalidation_rules: rec.exit_plan.signal_invalidation_rules.clone(),
            partial_exit_nodes: Vec::new(),
            settlement_mode: rec.exit_plan.settlement_mode,
            redeem_policy: rec.exit_plan.redeem_policy,
            manual_review_at: rec.exit_plan.manual_review_at,
            entry_reference_price: rec.entry_plan.limit_price.unwrap_or(Price::ZERO),
            entry_composite_score: rec.composite_score,
        },
        risk_envelope_hash: rec.risk_envelope.canonical_hash().expect("hash"),
        expires_at: Utc.timestamp_opt(1_700_003_600, 0).unwrap(),
        exit_state: ExitState::NotStarted,
        exit_reason: None,
        next_check_at: None,
        peak_mark_price: None,
        last_signal_recheck_at: None,
        executed_partial_exit_node_ids: ExecutedPartialExitNodes::default(),
        pending_partial_exit_node_id: None,
        opportunistic_exit_state: OpportunisticExitState::default(),
        created_at: now(),
        updated_at: now(),
    }
}

fn allocation(intent: &OrderIntentInfo, rec: &RecommendationInfo) -> CapitalAllocationInfo {
    CapitalAllocationInfo {
        capital_allocation_id: CapitalAllocationId::from_v7(),
        order_intent_id: intent.order_intent_id.clone(),
        recommendation_id: rec.recommendation_id.clone(),
        state: CapitalAllocationState::Allocated,
        planned_usd: Usd::new(dec!(250)),
        allocated_usd: Usd::new(dec!(250)),
        locked_usd: Usd::ZERO,
        spent_usd: Usd::ZERO,
        released_usd: Usd::ZERO,
        reason: "test".to_owned(),
        created_at: now(),
        updated_at: now(),
    }
}

fn book(asks: Vec<BookLevel>, timestamp_ms: u64) -> Arc<BookSnapshot> {
    Arc::new(BookSnapshot::new(
        Arc::from(Vec::<BookLevel>::new()),
        Arc::from(asks),
        timestamp_ms,
        1,
    ))
}

fn level(price: &str, shares: &str) -> BookLevel {
    BookLevel::from_decimal(
        Price::new(price.parse().expect("price")),
        Shares::new(shares.parse().expect("shares")),
    )
    .expect("level")
}

fn green_data_quality() -> DataQualitySnapshot {
    DataQualitySnapshot {
        as_of: now(),
        total_tokens: 10,
        fresh: 10,
        acceptable: 0,
        degraded: 0,
        stale: 0,
        insufficient: 0,
        max_book_age_ms: 5_000,
        worst_book_age_ms: 0,
        max_ingest_lag_ms: 30_000,
        worst_ingest_lag_ms: 0,
        ingest_lag_exceeded: false,
    }
}

fn passing() -> AdmissionInput {
    let rec = recommendation();
    let report = report(&rec);
    let intent = intent(&rec);
    let allocation = allocation(&intent, &rec);
    let account = AccountSnapshot::new(
        now(),
        AccountSource::Polymarket,
        Usd::new(dec!(10000)),
        Usd::new(dec!(10000)),
        Usd::new(dec!(10000)),
        Usd::new(dec!(250)),
        Vec::new(),
    );
    AdmissionInput {
        intent,
        recommendation: rec,
        report,
        mode: QuantRuntimeMode::SemiAuto,
        kill_switch: KillSwitchState::Closed,
        account,
        allocation: Some(allocation),
        book: Some(book(vec![level("0.42", "600")], NOW_MS - 500)),
        budget_total_usd: Usd::new(dec!(10000)),
        open_intent_count: 0,
        max_open_intents: 0,
        max_reserved_usd: Usd::ZERO,
        model_published: true,
        data_quality: green_data_quality(),
        max_stale_book_ratio_bps: 2_000,
        has_blocking_inflight: false,
        manual_block: false,
        seams: AdmissionSeams {
            venue_health: VenueHealth::Healthy,
            credentials_ready: true,
            exit_monitor_ready: true,
        },
        now: now(),
        now_ms: NOW_MS,
        state_version: StateVersion {
            config_version_id: RuntimeConfigVersionId::from_v7(),
            account_as_of: now(),
            book_version: Some(1),
            book_as_of_ms: Some(NOW_MS - 500),
            kill_switch_state: KillSwitchState::Closed,
        },
    }
}

fn engine() -> DefaultAdmissionEngine {
    DefaultAdmissionEngine::new(Arc::new(MetricsHub::new()))
}

async fn full(input: AdmissionInput) -> AdmissionDecision {
    engine().evaluate_full(input).await.expect("evaluate")
}

fn denied_checks(decision: &AdmissionDecision) -> Vec<AdmissionCheckId> {
    decision
        .trace
        .iter()
        .filter(|trace| trace.outcome == AdmissionOutcome::Deny)
        .map(|trace| trace.check)
        .collect()
}

const CANONICAL_ORDER: [AdmissionCheckId; 22] = [
    AdmissionCheckId::IntentState,
    AdmissionCheckId::RecommendationFreshness,
    AdmissionCheckId::ReportStatus,
    AdmissionCheckId::RuntimeMode,
    AdmissionCheckId::ModelPublication,
    AdmissionCheckId::DataQuality,
    AdmissionCheckId::BookFreshness,
    AdmissionCheckId::EntryTrigger,
    AdmissionCheckId::RiskEnvelopeHash,
    AdmissionCheckId::CapitalBudget,
    AdmissionCheckId::MaxOpenIntents,
    AdmissionCheckId::MaxReservedCapital,
    AdmissionCheckId::MarketExposure,
    AdmissionCheckId::EventExposure,
    AdmissionCheckId::CategoryExposure,
    AdmissionCheckId::LiquidityDepth,
    AdmissionCheckId::Slippage,
    AdmissionCheckId::ManualBlock,
    AdmissionCheckId::KillSwitch,
    AdmissionCheckId::VenueGuard,
    AdmissionCheckId::CredentialReadiness,
    AdmissionCheckId::ExitMonitorReadiness,
];

#[tokio::test]
async fn admission_allows_when_all_checks_pass() {
    let decision = engine().evaluate(passing()).await.expect("evaluate");
    assert_eq!(
        decision.outcome,
        AdmissionOutcome::Allow,
        "trace: {:?}",
        decision.trace
    );
    assert!(decision.denial_reason.is_none());
    assert_eq!(decision.trace.len(), 22);
}

#[tokio::test]
async fn admission_runs_all_checks_in_fixed_order() {
    let decision = full(passing()).await;
    let order: Vec<AdmissionCheckId> = decision.trace.iter().map(|trace| trace.check).collect();
    assert_eq!(order, CANONICAL_ORDER.to_vec());
}

#[tokio::test]
async fn admission_decision_is_deterministic_for_same_input() {
    let engine = engine();
    let first = engine.evaluate(passing()).await.expect("first");
    let second = engine.evaluate(passing()).await.expect("second");
    assert_eq!(first.outcome, second.outcome);
    let project = |decision: &AdmissionDecision| -> Vec<(AdmissionCheckId, AdmissionOutcome)> {
        decision
            .trace
            .iter()
            .map(|trace| (trace.check, trace.outcome))
            .collect()
    };
    assert_eq!(project(&first), project(&second));
    assert_eq!(first.denial_reason, second.denial_reason);
}

#[tokio::test]
async fn admission_denies_when_open_intent_cap_exceeded() {
    let mut input = passing();
    input.max_open_intents = 3;
    input.open_intent_count = 4;
    let decision = full(input).await;
    assert_eq!(decision.outcome, AdmissionOutcome::Deny);
    assert!(denied_checks(&decision).contains(&AdmissionCheckId::MaxOpenIntents));
}

#[tokio::test]
async fn admission_allows_open_intent_count_at_cap() {
    let mut input = passing();
    input.max_open_intents = 3;
    input.open_intent_count = 3;
    let decision = full(input).await;
    assert!(!denied_checks(&decision).contains(&AdmissionCheckId::MaxOpenIntents));
}

#[tokio::test]
async fn admission_open_intent_cap_disabled_when_zero() {
    let mut input = passing();
    input.max_open_intents = 0;
    input.open_intent_count = 10_000;
    let decision = full(input).await;
    assert!(!denied_checks(&decision).contains(&AdmissionCheckId::MaxOpenIntents));
}

#[tokio::test]
async fn admission_denies_when_reserved_capital_cap_exceeded() {
    let mut input = passing();
    input.max_reserved_usd = Usd::new(dec!(100));
    // Fixture reserves 250 USD on the account snapshot.
    let decision = full(input).await;
    assert_eq!(decision.outcome, AdmissionOutcome::Deny);
    assert!(denied_checks(&decision).contains(&AdmissionCheckId::MaxReservedCapital));
}

#[tokio::test]
async fn admission_reserved_capital_cap_disabled_when_zero() {
    let mut input = passing();
    input.max_reserved_usd = Usd::ZERO;
    let decision = full(input).await;
    assert!(!denied_checks(&decision).contains(&AdmissionCheckId::MaxReservedCapital));
}

#[tokio::test]
async fn admission_denies_on_risk_envelope_hash_mismatch() {
    let mut input = passing();
    input.intent.risk_envelope_hash = other_hash();
    let decision = full(input).await;
    assert_eq!(decision.outcome, AdmissionOutcome::Deny);
    assert!(denied_checks(&decision).contains(&AdmissionCheckId::RiskEnvelopeHash));
}

#[tokio::test]
async fn admission_denies_when_recommendation_expired() {
    let mut input = passing();
    input.recommendation.valid_until = input.now - Duration::seconds(1);
    let decision = full(input).await;
    assert_eq!(decision.outcome, AdmissionOutcome::Deny);
    assert!(denied_checks(&decision).contains(&AdmissionCheckId::RecommendationFreshness));
}

#[tokio::test]
async fn admission_denies_when_kill_switch_blocks_entry() {
    let mut input = passing();
    input.kill_switch = KillSwitchState::ExecutionHalted;
    let decision = full(input).await;
    assert_eq!(decision.outcome, AdmissionOutcome::Deny);
    assert!(denied_checks(&decision).contains(&AdmissionCheckId::KillSwitch));
}

#[tokio::test]
async fn admission_denies_when_unresolvable_recon_for_auto() {
    let mut input = passing();
    input.mode = QuantRuntimeMode::AutoExecution;
    input.intent.status = OrderIntentStatus::ApprovedByPolicy;
    input.has_blocking_inflight = true;
    let decision = full(input).await;
    assert_eq!(decision.outcome, AdmissionOutcome::Deny);
    assert!(denied_checks(&decision).contains(&AdmissionCheckId::KillSwitch));
}

#[tokio::test]
async fn admission_defers_when_book_stale() {
    let mut input = passing();
    input.book = Some(book(vec![level("0.42", "600")], NOW_MS - 10_000));
    let decision = full(input).await;
    assert_eq!(decision.outcome, AdmissionOutcome::Defer);
    assert!(decision.denial_reason.is_none());
}

#[tokio::test]
async fn admission_denies_when_book_missing() {
    let mut input = passing();
    input.book = None;
    let decision = full(input).await;
    assert_eq!(decision.outcome, AdmissionOutcome::Deny);
    assert!(denied_checks(&decision).contains(&AdmissionCheckId::BookFreshness));
}

#[tokio::test]
async fn admission_defers_when_limit_trigger_unmet() {
    let mut input = passing();
    // Best ask above the trigger price → not yet triggered.
    input.book = Some(book(vec![level("0.50", "600")], NOW_MS - 500));
    let decision = full(input).await;
    assert_eq!(decision.outcome, AdmissionOutcome::Defer);
}

#[tokio::test]
async fn admission_denies_on_market_exposure_breach() {
    let mut input = passing();
    let market = input.recommendation.market_id.clone();
    input
        .account
        .exposures
        .per_market
        .insert(market, Usd::new(dec!(400)));
    let decision = full(input).await;
    assert_eq!(decision.outcome, AdmissionOutcome::Deny);
    assert!(denied_checks(&decision).contains(&AdmissionCheckId::MarketExposure));
}

#[tokio::test]
async fn admission_denies_on_event_exposure_breach() {
    let mut input = passing();
    let event = input.recommendation.event_id.clone();
    input
        .account
        .exposures
        .per_event
        .insert(event, Usd::new(dec!(600)));
    let decision = full(input).await;
    assert_eq!(decision.outcome, AdmissionOutcome::Deny);
    assert!(denied_checks(&decision).contains(&AdmissionCheckId::EventExposure));
}

#[tokio::test]
async fn admission_denies_on_category_exposure_breach() {
    let mut input = passing();
    let category = input.recommendation.identity.category;
    input
        .account
        .exposures
        .per_category
        .insert(category, Usd::new(dec!(1400)));
    let decision = full(input).await;
    assert_eq!(decision.outcome, AdmissionOutcome::Deny);
    assert!(denied_checks(&decision).contains(&AdmissionCheckId::CategoryExposure));
}

#[tokio::test]
async fn admission_denies_on_slippage_breach() {
    let mut input = passing();
    // Raise the limit so the deep level is fillable, then walk a costly book.
    input.intent.entry_order_json.limit_price = Price::new(dec!(0.99));
    input.book = Some(book(
        vec![level("0.42", "100"), level("0.60", "500")],
        NOW_MS - 500,
    ));
    let decision = full(input).await;
    assert_eq!(decision.outcome, AdmissionOutcome::Deny);
    assert!(denied_checks(&decision).contains(&AdmissionCheckId::Slippage));
}

#[tokio::test]
async fn admission_defers_when_liquidity_insufficient() {
    let mut input = passing();
    // Only 100 shares fillable at/below the 0.43 limit — order needs 500.
    input.book = Some(book(vec![level("0.42", "100")], NOW_MS - 500));
    let decision = full(input).await;
    assert_eq!(decision.outcome, AdmissionOutcome::Defer);
}

#[tokio::test]
async fn admission_denies_on_manual_block() {
    let mut input = passing();
    input.manual_block = true;
    let decision = full(input).await;
    assert_eq!(decision.outcome, AdmissionOutcome::Deny);
    assert!(denied_checks(&decision).contains(&AdmissionCheckId::ManualBlock));
}

#[tokio::test]
async fn admission_denies_when_model_retired() {
    let mut input = passing();
    input.model_published = false;
    let decision = full(input).await;
    assert_eq!(decision.outcome, AdmissionOutcome::Deny);
    assert!(denied_checks(&decision).contains(&AdmissionCheckId::ModelPublication));
}

#[tokio::test]
async fn admission_denies_when_report_not_published() {
    let mut input = passing();
    input.report.status = RecommendationReportStatus::Revoked;
    let decision = full(input).await;
    assert_eq!(decision.outcome, AdmissionOutcome::Deny);
    assert!(denied_checks(&decision).contains(&AdmissionCheckId::ReportStatus));
}

#[tokio::test]
async fn admission_denies_in_report_only_mode() {
    let mut input = passing();
    input.mode = QuantRuntimeMode::ReportOnly;
    let decision = full(input).await;
    assert_eq!(decision.outcome, AdmissionOutcome::Deny);
    assert!(denied_checks(&decision).contains(&AdmissionCheckId::RuntimeMode));
}

#[tokio::test]
async fn admission_denies_when_credentials_not_ready() {
    let mut input = passing();
    input.seams.credentials_ready = false;
    let decision = full(input).await;
    assert_eq!(decision.outcome, AdmissionOutcome::Deny);
    assert!(denied_checks(&decision).contains(&AdmissionCheckId::CredentialReadiness));
}

#[tokio::test]
async fn admission_denies_when_exit_monitor_not_ready() {
    let mut input = passing();
    input.seams.exit_monitor_ready = false;
    let decision = full(input).await;
    assert_eq!(decision.outcome, AdmissionOutcome::Deny);
    assert!(denied_checks(&decision).contains(&AdmissionCheckId::ExitMonitorReadiness));
}

#[tokio::test]
async fn admission_denies_when_data_quality_degraded() {
    let mut input = passing();
    input.data_quality.stale = 9;
    input.data_quality.fresh = 1;
    let decision = full(input).await;
    assert_eq!(decision.outcome, AdmissionOutcome::Deny);
    assert!(denied_checks(&decision).contains(&AdmissionCheckId::DataQuality));
}

#[tokio::test]
async fn admission_respects_configured_stale_book_ratio_cap() {
    let mut input = passing();
    input.data_quality.stale = 1;
    input.data_quality.fresh = 9;
    input.max_stale_book_ratio_bps = 2_000;
    let allowed = full(input.clone()).await;
    assert_eq!(allowed.outcome, AdmissionOutcome::Allow);

    input.max_stale_book_ratio_bps = 500;
    let denied = full(input).await;
    assert_eq!(denied.outcome, AdmissionOutcome::Deny);
    assert!(denied_checks(&denied).contains(&AdmissionCheckId::DataQuality));
}

#[tokio::test]
async fn admission_denies_on_unsupported_entry_trigger_kind() {
    let mut input = passing();
    input.recommendation.entry_plan.trigger_kind = EntryTriggerKind::Breakout;
    let decision = full(input).await;
    assert_eq!(decision.outcome, AdmissionOutcome::Deny);
    assert!(denied_checks(&decision).contains(&AdmissionCheckId::EntryTrigger));
}

#[tokio::test]
async fn admission_short_circuit_matches_full_evaluation_outcome() {
    let mut input = passing();
    input.intent.risk_envelope_hash = other_hash();
    let engine = engine();
    let short = engine.evaluate(input.clone()).await.expect("short");
    let full = engine.evaluate_full(input).await.expect("full");
    assert_eq!(short.outcome, full.outcome);
    assert_eq!(short.denial_reason, full.denial_reason);
    assert!(short.trace.len() <= full.trace.len());
    // Short-circuit stops at the first hard deny (#9).
    assert_eq!(short.trace.len(), 9);
}

#[tokio::test]
async fn quant_admission_denied_total_increments_by_check_id() {
    let metrics = Arc::new(MetricsHub::new());
    let engine = DefaultAdmissionEngine::new(Arc::clone(&metrics));
    let mut input = passing();
    input.intent.risk_envelope_hash = other_hash();
    let _ = engine.evaluate(input).await.expect("evaluate");
    assert_eq!(
        metrics
            .admission_denied
            .with_label_values(&["risk_envelope_hash"])
            .get(),
        1
    );
    assert_eq!(
        metrics
            .admission_denied
            .with_label_values(&["intent_state"])
            .get(),
        0
    );
}

#[tokio::test]
async fn admission_fail_closed_when_account_unavailable() {
    let rec = recommendation();
    let report = report(&rec);
    let intent = intent(&rec);
    // The account factory has no signing client, so the snapshot read fails
    // closed; the builder must surface that as an error, not a partial input.
    let deps = AdmissionInputBuilderDeps {
        recommendations: Arc::new(StubRecommendations(rec.clone())),
        reports: Arc::new(StubReports(report)),
        model_registry: Arc::new(StubModelRegistry),
        reconciliation: Arc::new(StubReconciliation),
        execution_orders: Arc::new(StubExecutionOrders),
        intents: Arc::new(StubIntents),
        capital: Arc::new(StubCapital),
        markets: Arc::new(StubMarkets),
        config_versions: Arc::new(StubConfigVersions),
        account_factory: Arc::new(AccountProviderFactory::new(
            None,
            Arc::new(MarketRegistry::new()),
            Arc::new(StubReserved),
            Some("0xfunder".to_owned()),
        )),
        book_store: Arc::new(BookStore::new(Arc::new(MetricsHub::new()))),
        data_quality: Arc::new(StubDataQuality),
        config: Arc::new(RuntimeConfigStore::new(RuntimeConfig::default())),
        runtime_mode: RuntimeModeHandle::new(QuantRuntimeMode::SemiAuto),
        kill_switch: KillSwitchHandle::new(KillSwitchState::Closed),
        venue_health: VenueHealthHandle::default(),
        exit_monitor_health: {
            let health = ExitMonitorHealthHandle::new();
            health.publish(now(), 3_600);
            health
        },
    };
    let builder = AdmissionInputBuilder::new(deps);
    let result = builder.build(&intent, now()).await;
    assert!(
        matches!(
            result,
            Err(QuantError::Account(AccountError::CredentialsMissing))
        ),
        "expected fail-closed credentials error"
    );
}

// ── Stub repositories for the builder fail-closed test ───────────────────────
// Only the methods the build path reaches before the account read return canned
// values; everything else is unreachable in this test.

struct StubRecommendations(RecommendationInfo);

#[async_trait]
impl RecommendationRepository for StubRecommendations {
    async fn find_by_report(
        &self,
        _report_id: &RecommendationReportId,
    ) -> Result<Vec<RecommendationInfo>, StorageError> {
        unimplemented!()
    }

    async fn find_by_id(
        &self,
        _recommendation_id: &RecommendationId,
    ) -> Result<Option<RecommendationInfo>, StorageError> {
        Ok(Some(self.0.clone()))
    }

    async fn find_expirable(
        &self,
        _now: DateTime<Utc>,
        _limit: u64,
    ) -> Result<Vec<RecommendationId>, StorageError> {
        unimplemented!()
    }

    async fn upcoming_expirations(
        &self,
        _before: DateTime<Utc>,
        _limit: u64,
    ) -> Result<Vec<(RecommendationId, DateTime<Utc>)>, StorageError> {
        unimplemented!()
    }

    async fn expire(
        &self,
        _recommendation_id: &RecommendationId,
        _operation_log: NewOperationLog,
    ) -> Result<RecommendationInfo, StorageError> {
        unimplemented!()
    }

    async fn find_expired_attribution_candidates(
        &self,
        _limit: u64,
    ) -> Result<Vec<RecommendationInfo>, StorageError> {
        unimplemented!()
    }

    async fn recommendation_blocks_final_attribution(
        &self,
        _recommendation_id: &RecommendationId,
    ) -> Result<bool, StorageError> {
        unimplemented!()
    }
}

struct StubReports(RecommendationReportInfo);

#[async_trait]
impl RecommendationReportRepository for StubReports {
    async fn create_report(
        &self,
        _transaction: NewReportTransaction,
    ) -> Result<RecommendationReportInfo, StorageError> {
        unimplemented!()
    }

    async fn find_by_id(
        &self,
        _report_id: &RecommendationReportId,
    ) -> Result<Option<RecommendationReportInfo>, StorageError> {
        Ok(Some(self.0.clone()))
    }

    async fn page(
        &self,
        _query: QuantReportListQuery,
    ) -> Result<Paginated<RecommendationReportInfo>, StorageError> {
        unimplemented!()
    }

    async fn latest_published(
        &self,
        _kind: ReportKind,
    ) -> Result<Option<RecommendationReportInfo>, StorageError> {
        unimplemented!()
    }

    async fn find_by_trigger_key(
        &self,
        _trigger_key: &str,
    ) -> Result<Option<RecommendationReportInfo>, StorageError> {
        unimplemented!()
    }

    async fn find_expirable(
        &self,
        _now: DateTime<Utc>,
        _limit: u64,
    ) -> Result<Vec<RecommendationReportId>, StorageError> {
        unimplemented!()
    }

    async fn roll_up_to_expired(
        &self,
        _report_id: &RecommendationReportId,
        _expired_at: DateTime<Utc>,
        _operation_log: NewOperationLog,
    ) -> Result<Option<RecommendationReportInfo>, StorageError> {
        unimplemented!()
    }

    async fn revoke(
        &self,
        _report_id: &RecommendationReportId,
        _reason: &str,
        _revoked_at: DateTime<Utc>,
        _operation_log: NewOperationLog,
    ) -> Result<RecommendationReportInfo, StorageError> {
        unimplemented!()
    }
}

struct StubMarkets;

#[async_trait]
impl MarketRepository for StubMarkets {
    async fn find_by_id(&self, id: &MarketId) -> Result<Option<Arc<MarketInfo>>, StorageError> {
        Ok(Some(Arc::new(active_market(id.clone()))))
    }

    async fn find_by_ids(&self, ids: &[MarketId]) -> Result<Vec<Arc<MarketInfo>>, StorageError> {
        Ok(ids
            .iter()
            .cloned()
            .map(active_market)
            .map(Arc::new)
            .collect())
    }

    async fn page(&self, _query: MarketPageQuery) -> Result<Paginated<MarketInfo>, StorageError> {
        unimplemented!()
    }

    async fn find_active(&self) -> Result<Arc<[MarketInfo]>, StorageError> {
        unimplemented!()
    }

    async fn find_by_event(&self, _event_id: &str) -> Result<Vec<Arc<MarketInfo>>, StorageError> {
        unimplemented!()
    }

    async fn find_existing_ids(&self, _ids: &[MarketId]) -> Result<HashSet<String>, StorageError> {
        unimplemented!()
    }

    async fn upsert(&self, _market: UpsertMarket) -> Result<Arc<MarketInfo>, StorageError> {
        unimplemented!()
    }

    async fn upsert_batch(&self, _markets: Vec<UpsertMarket>) -> Result<u64, StorageError> {
        unimplemented!()
    }

    async fn update_status(
        &self,
        _id: &MarketId,
        _status: &str,
        _outcome: Option<&str>,
    ) -> Result<(), StorageError> {
        unimplemented!()
    }
}

fn active_market(market_id: MarketId) -> MarketInfo {
    MarketInfo {
        market_id,
        event_id: EventId::new("evt-test"),
        question: "Will the test market resolve yes?".to_owned(),
        slug: "test-market".to_owned(),
        categories: vec![MarketCategory::Politics],
        status: MarketStatus::Active,
        outcome: None,
        yes_token_id: TokenId::new("yes-token"),
        no_token_id: TokenId::new("no-token"),
        tick_size: TickSize::Hundredth,
        neg_risk: false,
        end_date: None,
        resolved_at: None,
        fees_enabled: true,
        fee_rate: None,
        fee_exponent: None,
        fee_taker_only: None,
        fee_rebate_rate: None,
        fee_source: None,
        fee_observed_at: None,
        created_at: now(),
        updated_at: now(),
    }
}

struct StubModelRegistry;

#[async_trait]
impl ModelRegistryRepository for StubModelRegistry {
    async fn create_model_spec(&self, _spec: NewModelSpec) -> Result<ModelSpecInfo, StorageError> {
        unimplemented!()
    }

    async fn find_model_spec_by_id(
        &self,
        _model_spec_id: &ModelSpecId,
    ) -> Result<Option<ModelSpecInfo>, StorageError> {
        Ok(None)
    }

    async fn create_model_version(
        &self,
        _version: NewModelVersion,
    ) -> Result<ModelVersionInfo, StorageError> {
        unimplemented!()
    }

    async fn next_version_for_spec(
        &self,
        _model_spec_id: &ModelSpecId,
    ) -> Result<i32, StorageError> {
        unimplemented!()
    }

    async fn find_model_version_by_id(
        &self,
        _model_version_id: &ModelVersionId,
    ) -> Result<Option<ModelVersionInfo>, StorageError> {
        Ok(None)
    }

    async fn list_published_for_spec(
        &self,
        _model_spec_id: &ModelSpecId,
    ) -> Result<Vec<ModelVersionInfo>, StorageError> {
        unimplemented!()
    }

    async fn publish_model_version(
        &self,
        _model_version_id: &ModelVersionId,
    ) -> Result<ModelVersionInfo, StorageError> {
        unimplemented!()
    }

    async fn retire_model_version(
        &self,
        _model_version_id: &ModelVersionId,
    ) -> Result<ModelVersionInfo, StorageError> {
        unimplemented!()
    }

    async fn promote_model_to_shadow(
        &self,
        _model_version_id: &ModelVersionId,
    ) -> Result<ModelVersionInfo, StorageError> {
        unimplemented!()
    }

    async fn restore_model_version(
        &self,
        _model_version_id: &ModelVersionId,
    ) -> Result<ModelVersionInfo, StorageError> {
        unimplemented!()
    }

    async fn set_quality_gate_report(
        &self,
        _model_version_id: &ModelVersionId,
        _quality_gate_report: serde_json::Value,
    ) -> Result<ModelVersionInfo, StorageError> {
        unimplemented!()
    }
}

struct StubReconciliation;

#[async_trait]
impl ReconciliationRepository for StubReconciliation {
    async fn create(
        &self,
        _reconciliation: NewReconciliation,
    ) -> Result<ReconciliationInfo, StorageError> {
        unimplemented!()
    }

    async fn append_evidence(
        &self,
        _reconciliation_id: &ReconciliationId,
        _evidence: AppendReconciliationEvidence,
    ) -> Result<ReconciliationInfo, StorageError> {
        unimplemented!()
    }

    async fn patch(
        &self,
        _reconciliation_id: &ReconciliationId,
        _patch: ReconciliationPatch,
    ) -> Result<ReconciliationInfo, StorageError> {
        unimplemented!()
    }

    async fn find_by_id(
        &self,
        _reconciliation_id: &ReconciliationId,
    ) -> Result<Option<ReconciliationInfo>, StorageError> {
        Ok(None)
    }

    async fn page(
        &self,
        _query: ReconciliationListQuery,
    ) -> Result<Paginated<ReconciliationInfo>, StorageError> {
        Ok(Paginated::empty(1, 10))
    }

    async fn find_by_execution_order(
        &self,
        _execution_order_id: &ExecutionOrderId,
    ) -> Result<Option<ReconciliationInfo>, StorageError> {
        unimplemented!()
    }

    async fn find_unresolved(&self) -> Result<Vec<ReconciliationInfo>, StorageError> {
        unimplemented!()
    }

    async fn has_unresolvable(&self) -> Result<bool, StorageError> {
        Ok(false)
    }

    async fn count_blocking_unresolvable(&self) -> Result<u64, StorageError> {
        Ok(0)
    }
}

struct StubIntents;

#[async_trait]
impl OrderIntentRepository for StubIntents {
    async fn create_with_allocation(
        &self,
        _: NewOrderIntent,
        _: NewCapitalAllocation,
    ) -> Result<OrderIntentInfo, StorageError> {
        unimplemented!()
    }

    async fn approve(
        &self,
        _: &OrderIntentId,
        _: ApproveOrderIntent,
        _: Option<EntryOrderSpec>,
        _: Option<Usd>,
        _: DateTime<Utc>,
    ) -> Result<ApproveOrderIntentOutcome, StorageError> {
        unimplemented!()
    }

    async fn reject(&self, _: &OrderIntentId, _: String) -> Result<OrderIntentInfo, StorageError> {
        unimplemented!()
    }

    async fn cancel(&self, _: &OrderIntentId, _: String) -> Result<OrderIntentInfo, StorageError> {
        unimplemented!()
    }

    async fn expire(
        &self,
        _: &OrderIntentId,
        _: NewOperationLog,
    ) -> Result<OrderIntentInfo, StorageError> {
        unimplemented!()
    }

    async fn invalidate(
        &self,
        _: &OrderIntentId,
        _: ApprovalInvalidation,
        _: NewOperationLog,
    ) -> Result<OrderIntentInfo, StorageError> {
        unimplemented!()
    }

    async fn find_by_id(&self, _: &OrderIntentId) -> Result<Option<OrderIntentInfo>, StorageError> {
        unimplemented!()
    }

    async fn page(
        &self,
        _: OrderIntentListQuery,
    ) -> Result<Paginated<OrderIntentInfo>, StorageError> {
        unimplemented!()
    }

    async fn find_expired(&self, _: DateTime<Utc>) -> Result<Vec<OrderIntentInfo>, StorageError> {
        unimplemented!()
    }

    async fn upcoming_expirations(
        &self,
        _: DateTime<Utc>,
        _: u64,
    ) -> Result<Vec<(OrderIntentId, DateTime<Utc>)>, StorageError> {
        unimplemented!()
    }

    async fn find_active_by_recommendation(
        &self,
        _: &RecommendationId,
    ) -> Result<Option<OrderIntentInfo>, StorageError> {
        unimplemented!()
    }

    async fn find_active_intents_by_recommendation(
        &self,
        _: &RecommendationId,
    ) -> Result<Vec<OrderIntentInfo>, StorageError> {
        unimplemented!()
    }

    async fn find_active_by_report(
        &self,
        _: &RecommendationReportId,
    ) -> Result<Vec<OrderIntentInfo>, StorageError> {
        unimplemented!()
    }

    async fn find_blocking_by_report(
        &self,
        _: &RecommendationReportId,
    ) -> Result<Vec<OrderIntentInfo>, StorageError> {
        unimplemented!()
    }

    async fn count_open(&self) -> Result<u64, StorageError> {
        Ok(0)
    }

    async fn find_attribution_candidates(
        &self,
        _: Vec<OrderIntentStatus>,
        _: u64,
    ) -> Result<Vec<OrderIntentInfo>, StorageError> {
        unimplemented!()
    }
}

struct StubExecutionOrders;

#[async_trait]
impl ExecutionOrderRepository for StubExecutionOrders {
    async fn create(&self, _order: NewExecutionOrder) -> Result<ExecutionOrderInfo, StorageError> {
        unimplemented!()
    }

    async fn find_by_intent(
        &self,
        _order_intent_id: &OrderIntentId,
    ) -> Result<Vec<ExecutionOrderInfo>, StorageError> {
        unimplemented!()
    }

    async fn find_by_id(
        &self,
        _execution_order_id: &ExecutionOrderId,
    ) -> Result<Option<ExecutionOrderInfo>, StorageError> {
        unimplemented!()
    }

    async fn has_ambiguous_inflight(&self) -> Result<bool, StorageError> {
        Ok(false)
    }

    async fn find_reconcilable(
        &self,
        _limit: u64,
    ) -> Result<Vec<ExecutionOrderInfo>, StorageError> {
        Ok(Vec::new())
    }

    async fn transition(
        &self,
        _execution_order_id: &ExecutionOrderId,
        _patch: ExecutionOrderPatch,
    ) -> Result<ExecutionOrderInfo, StorageError> {
        unimplemented!()
    }
}

struct StubCapital;

#[async_trait]
impl CapitalAllocationRepository for StubCapital {
    async fn find_by_intent(
        &self,
        _order_intent_id: &OrderIntentId,
    ) -> Result<Option<CapitalAllocationInfo>, StorageError> {
        Ok(None)
    }

    async fn sum_reserved_usd(&self) -> Result<Usd, StorageError> {
        unimplemented!()
    }

    async fn has_impaired(&self) -> Result<bool, StorageError> {
        unimplemented!()
    }
}

struct StubConfigVersions;

#[async_trait]
impl RuntimeConfigVersionRepository for StubConfigVersions {
    async fn create_version(
        &self,
        _version: NewRuntimeConfigVersion,
    ) -> Result<RuntimeConfigVersionInfo, StorageError> {
        unimplemented!()
    }

    async fn activate_version(
        &self,
        _activation: NewRuntimeConfigActivation,
    ) -> Result<RuntimeConfigActivationInfo, StorageError> {
        unimplemented!()
    }

    async fn load_current_activation(
        &self,
    ) -> Result<Option<RuntimeConfigActivationInfo>, StorageError> {
        unimplemented!()
    }

    async fn load_version(
        &self,
        _version_id: &RuntimeConfigVersionId,
    ) -> Result<Option<RuntimeConfigVersionInfo>, StorageError> {
        unimplemented!()
    }

    async fn load_by_hash(
        &self,
        _config_hash: &ContentHash,
    ) -> Result<Option<RuntimeConfigVersionInfo>, StorageError> {
        unimplemented!()
    }

    async fn load_current(&self) -> Result<Option<RuntimeConfigVersionInfo>, StorageError> {
        Ok(Some(RuntimeConfigVersionInfo {
            runtime_config_version_id: RuntimeConfigVersionId::from_v7(),
            config_hash: ContentHash::parse(format!("blake3:{}", "c".repeat(64))).expect("hash"),
            schema_version: SchemaVersion::FIRST,
            config_json: serde_json::json!({}),
            source: RuntimeConfigVersionSource::Operator,
            created_by: "stub".to_owned(),
            reason: "stub".to_owned(),
            created_at: now(),
        }))
    }

    async fn load_active_at(
        &self,
        _at: DateTime<Utc>,
    ) -> Result<Option<RuntimeConfigVersionInfo>, StorageError> {
        unimplemented!()
    }

    async fn list_versions(
        &self,
        _limit: u64,
    ) -> Result<Vec<RuntimeConfigVersionInfo>, StorageError> {
        unimplemented!()
    }

    async fn list_activations(
        &self,
        _limit: u64,
    ) -> Result<Vec<RuntimeConfigActivationInfo>, StorageError> {
        unimplemented!()
    }
}

struct StubDataQuality;

impl DataQualityPort for StubDataQuality {
    fn snapshot(&self) -> DataQualitySnapshot {
        unimplemented!()
    }
}

struct StubReserved;

#[async_trait]
impl ReservedCapitalReader for StubReserved {
    async fn sum_locked(&self) -> QuantResult<Usd> {
        unimplemented!()
    }
}
