//! Phase 05.5 — `ReconciliationService` orchestration (in-memory mocks).
//!
//! Exercises the write path wiring: venue read failure semantics, stale resting
//! cancel → re-collect → terminal release, breaker trip, and metrics — without
//! Postgres or a live CLOB. Repository-level capital/idempotency tests live in
//! `quant-pivot-repository/tests/pg_execution_submission.rs`.

use std::sync::{
    Arc, Mutex,
    atomic::{AtomicU32, AtomicUsize, Ordering},
};

use async_trait::async_trait;
use chrono::{Duration, TimeZone, Utc};
use quant_pivot_api::clob::{ClobTrade, OpenOrder};
use quant_pivot_api::fees::FeeCalculator;
use quant_pivot_core::{
    execution::{
        CollectedReconciliation, EvidenceCollector, ExecutionBreaker, PolymarketOrderClient,
        ReconciliationService, ReconciliationServiceDeps, VenueEvidenceCollector,
        VenueReconciliationReader,
    },
    observability::metrics_hub::MetricsHub,
    pipeline::book_store::BookStore,
    runtime_config::RuntimeConfigStore,
};
use quant_pivot_error::{QuantError, QuantResult, api::ApiError, storage::StorageError};
use quant_pivot_models::{
    domain::{
        AppendReconciliationEvidence, ApproveOrderIntent, ApproveOrderIntentOutcome,
        CapitalReconcileSettlement, ExecutionOrderInfo, ExecutionOrderPatch, KillSwitchPort,
        KillSwitchView, NewCapitalAllocation, NewExecutionOrder, NewOperationLog, NewOrderIntent,
        NewReconciliation, OperationLogInfo, OperationLogQuery, OrderIntentInfo,
        OrderIntentListQuery, Paginated, RecommendationInfo, ReconciliationInfo,
        ReconciliationLedgerWrite, ReconciliationPatch, SetKillSwitchCommand,
        SubmissionLedgerWrite,
    },
    enums::{
        common::{OrderType, Side},
        execution::{
            ApprovalInvalidation, ExecutionOrderPhase, KillSwitchState, OrderIntentKind,
            OrderTypeKind, ReconciliationResult, VenueOrderStatus,
        },
        quant::{
            ApprovalStatus, ExecutionOrderState, OrderIntentStatus, OutcomeSide, QuantRuntimeMode,
        },
    },
    runtime_config::{ReconciliationPolicy, RuntimeConfig},
    types::{
        Bps, EntryOrderSpec, ExecutionOrderId, ExitPolicySpec, MarketId, ModelVersionId, OrderId,
        OrderIntentId, Price, RecommendationId, RecommendationReportId, ReconciliationId,
        RuntimeConfigVersionId, Shares, TokenId, Usd,
    },
};
use quant_pivot_repository::traits::{
    ExecutionOrderRepository, ExecutionSubmissionRepository, OperationLogRepository,
    OrderIntentRepository, RecommendationRepository, ReconciliationRepository,
};
use quant_pivot_test_support::report_fixtures;
use rust_decimal_macros::dec;

const VENUE_ORDER_ID: &str = "venue-stale-1";
const STALE_SECS: u64 = 300;
const NOW_SECS: i64 = 1_700_010_000;

fn now() -> chrono::DateTime<Utc> {
    Utc.timestamp_opt(NOW_SECS, 0).unwrap()
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

fn intent(rec: &RecommendationInfo) -> OrderIntentInfo {
    OrderIntentInfo {
        order_intent_id: OrderIntentId::from_v7(),
        recommendation_id: rec.recommendation_id.clone(),
        runtime_mode: QuantRuntimeMode::AutoExecution,
        runtime_config_version_id: RuntimeConfigVersionId::from_v7(),
        model_version_id: ModelVersionId::from_v7(),
        intent_kind: OrderIntentKind::Buy,
        status: OrderIntentStatus::Submitted,
        approval_status: ApprovalStatus::NotRequired,
        approved_by: None,
        approval_reason: None,
        approved_at: None,
        policy_id: Some("auto".to_owned()),
        policy_hash: None,
        status_reason: None,
        admission_trace_ref: None,
        entry_order_json: EntryOrderSpec {
            token_id: rec.token_id.clone(),
            side: Side::Buy,
            order_type: OrderType::Gtc,
            limit_price: Price::new(dec!(0.6)),
            shares: Shares::new(dec!(100)),
            max_slippage_bps: Bps::new(dec!(50)),
            valid_until: rec.entry_plan.valid_until,
        },
        exit_policy_json: ExitPolicySpec {
            take_profit_price: rec.exit_plan.take_profit_price,
            stop_loss_price: rec.exit_plan.stop_loss_price,
            time_exit_at: None,
            partial_exit_nodes: Vec::new(),
            settlement_policy: rec.exit_plan.settlement_policy,
        },
        risk_envelope_hash: rec.risk_envelope.canonical_hash().expect("hash"),
        expires_at: now() + Duration::hours(1),
        created_at: now(),
        updated_at: now(),
    }
}

fn execution_order(
    intent_id: &OrderIntentId,
    token_id: &TokenId,
    submitted_at: chrono::DateTime<Utc>,
) -> ExecutionOrderInfo {
    ExecutionOrderInfo {
        execution_order_id: ExecutionOrderId::from_v7(),
        order_intent_id: intent_id.clone(),
        order_phase: ExecutionOrderPhase::Entry,
        market_id: MarketId::new("0xmkt"),
        token_id: token_id.clone(),
        side: Side::Buy,
        order_type: OrderTypeKind::Gtc,
        price: Price::new(dec!(0.6)),
        shares: Shares::new(dec!(100)),
        cost_usd: Usd::new(dec!(60)),
        venue_order_id: Some(OrderId::new(VENUE_ORDER_ID)),
        venue_status: None,
        state: ExecutionOrderState::Submitted,
        submitted_at: Some(submitted_at),
        filled_at: None,
        cancelled_at: None,
        gtd_expiration_at: None,
        error_message: None,
        created_at: submitted_at,
        updated_at: now(),
    }
}

struct RecordingKillSwitch {
    sets: Mutex<Vec<SetKillSwitchCommand>>,
}

impl Default for RecordingKillSwitch {
    fn default() -> Self {
        Self {
            sets: Mutex::new(Vec::new()),
        }
    }
}

#[async_trait]
impl KillSwitchPort for RecordingKillSwitch {
    fn current(&self) -> KillSwitchState {
        KillSwitchState::Closed
    }

    fn view(&self) -> KillSwitchView {
        KillSwitchView {
            state: self.current(),
            requires_operator_ack: false,
            last_reason: "test".to_owned(),
            changed_by: "test".to_owned(),
            changed_at: now(),
        }
    }

    async fn set(&self, command: SetKillSwitchCommand) -> QuantResult<KillSwitchView> {
        self.sets.lock().unwrap().push(command.clone());
        Ok(KillSwitchView {
            state: command.target,
            requires_operator_ack: command.latch,
            last_reason: command.reason,
            changed_by: command.actor,
            changed_at: now(),
        })
    }
}

struct MemoryExecutionOrders {
    orders: Mutex<Vec<ExecutionOrderInfo>>,
}

#[async_trait]
impl ExecutionOrderRepository for MemoryExecutionOrders {
    async fn create(&self, _order: NewExecutionOrder) -> Result<ExecutionOrderInfo, StorageError> {
        unimplemented!()
    }

    async fn find_by_intent(
        &self,
        _order_intent_id: &OrderIntentId,
    ) -> Result<Vec<ExecutionOrderInfo>, StorageError> {
        Ok(Vec::new())
    }

    async fn find_by_id(
        &self,
        _execution_order_id: &ExecutionOrderId,
    ) -> Result<Option<ExecutionOrderInfo>, StorageError> {
        Ok(None)
    }

    async fn has_ambiguous_inflight(&self) -> Result<bool, StorageError> {
        Ok(false)
    }

    async fn find_reconcilable(&self, limit: u64) -> Result<Vec<ExecutionOrderInfo>, StorageError> {
        let orders = self.orders.lock().unwrap();
        Ok(orders
            .iter()
            .take(usize::try_from(limit).unwrap_or(usize::MAX))
            .cloned()
            .collect())
    }

    async fn transition(
        &self,
        _execution_order_id: &ExecutionOrderId,
        _patch: ExecutionOrderPatch,
    ) -> Result<ExecutionOrderInfo, StorageError> {
        unimplemented!()
    }
}

struct MemoryIntents {
    intent: OrderIntentInfo,
}

#[async_trait]
impl OrderIntentRepository for MemoryIntents {
    async fn create_with_allocation(
        &self,
        _: NewOrderIntent,
        _: NewCapitalAllocation,
    ) -> Result<OrderIntentInfo, StorageError> {
        Err(StorageError::Conflict("stub".to_owned()))
    }

    async fn approve(
        &self,
        _: &OrderIntentId,
        _: ApproveOrderIntent,
        _: Option<EntryOrderSpec>,
        _: Option<Usd>,
        _: chrono::DateTime<Utc>,
    ) -> Result<ApproveOrderIntentOutcome, StorageError> {
        Err(StorageError::Conflict("stub".to_owned()))
    }

    async fn reject(&self, _: &OrderIntentId, _: String) -> Result<OrderIntentInfo, StorageError> {
        Err(StorageError::Conflict("stub".to_owned()))
    }

    async fn cancel(&self, _: &OrderIntentId, _: String) -> Result<OrderIntentInfo, StorageError> {
        Err(StorageError::Conflict("stub".to_owned()))
    }

    async fn expire(
        &self,
        _: &OrderIntentId,
        _: NewOperationLog,
    ) -> Result<OrderIntentInfo, StorageError> {
        Err(StorageError::Conflict("stub".to_owned()))
    }

    async fn invalidate(
        &self,
        _: &OrderIntentId,
        _: ApprovalInvalidation,
        _: NewOperationLog,
    ) -> Result<OrderIntentInfo, StorageError> {
        Err(StorageError::Conflict("stub".to_owned()))
    }

    async fn find_by_id(
        &self,
        id: &OrderIntentId,
    ) -> Result<Option<OrderIntentInfo>, StorageError> {
        Ok((self.intent.order_intent_id == *id).then(|| self.intent.clone()))
    }

    async fn page(
        &self,
        _: OrderIntentListQuery,
    ) -> Result<Paginated<OrderIntentInfo>, StorageError> {
        Ok(Paginated::empty(1, 0))
    }

    async fn find_expired(
        &self,
        _: chrono::DateTime<Utc>,
    ) -> Result<Vec<OrderIntentInfo>, StorageError> {
        Ok(Vec::new())
    }

    async fn upcoming_expirations(
        &self,
        _: chrono::DateTime<Utc>,
        _: u64,
    ) -> Result<Vec<(OrderIntentId, chrono::DateTime<Utc>)>, StorageError> {
        Ok(Vec::new())
    }

    async fn find_active_by_recommendation(
        &self,
        _: &RecommendationId,
    ) -> Result<Option<OrderIntentInfo>, StorageError> {
        Ok(None)
    }

    async fn find_active_intents_by_recommendation(
        &self,
        _: &RecommendationId,
    ) -> Result<Vec<OrderIntentInfo>, StorageError> {
        Ok(Vec::new())
    }

    async fn find_active_by_report(
        &self,
        _: &RecommendationReportId,
    ) -> Result<Vec<OrderIntentInfo>, StorageError> {
        Ok(Vec::new())
    }
}

struct StubRecommendations(RecommendationInfo);

#[async_trait]
impl RecommendationRepository for StubRecommendations {
    async fn find_by_report(
        &self,
        _: &RecommendationReportId,
    ) -> Result<Vec<RecommendationInfo>, StorageError> {
        Ok(Vec::new())
    }

    async fn find_by_id(
        &self,
        id: &RecommendationId,
    ) -> Result<Option<RecommendationInfo>, StorageError> {
        Ok((self.0.recommendation_id == *id).then(|| self.0.clone()))
    }

    async fn find_expirable(
        &self,
        _: chrono::DateTime<Utc>,
        _: u64,
    ) -> Result<Vec<RecommendationId>, StorageError> {
        Ok(Vec::new())
    }

    async fn upcoming_expirations(
        &self,
        _: chrono::DateTime<Utc>,
        _: u64,
    ) -> Result<Vec<(RecommendationId, chrono::DateTime<Utc>)>, StorageError> {
        Ok(Vec::new())
    }

    async fn expire(
        &self,
        _: &RecommendationId,
        _: NewOperationLog,
    ) -> Result<RecommendationInfo, StorageError> {
        Err(StorageError::Conflict("stub".to_owned()))
    }
}

struct MemoryReconciliation;

#[async_trait]
impl ReconciliationRepository for MemoryReconciliation {
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

    async fn resolve(
        &self,
        _reconciliation_id: &ReconciliationId,
        _patch: ReconciliationPatch,
    ) -> Result<ReconciliationInfo, StorageError> {
        unimplemented!()
    }

    async fn find_by_execution_order(
        &self,
        _execution_order_id: &ExecutionOrderId,
    ) -> Result<Option<ReconciliationInfo>, StorageError> {
        Ok(None)
    }

    async fn find_unresolved(&self) -> Result<Vec<ReconciliationInfo>, StorageError> {
        Ok(Vec::new())
    }

    async fn has_unresolvable(&self) -> Result<bool, StorageError> {
        Ok(false)
    }
}

struct RecordingSubmissionRepo {
    applies: Mutex<Vec<ReconciliationLedgerWrite>>,
}

impl Default for RecordingSubmissionRepo {
    fn default() -> Self {
        Self {
            applies: Mutex::new(Vec::new()),
        }
    }
}

impl RecordingSubmissionRepo {
    fn applied_writes(&self) -> Vec<ReconciliationLedgerWrite> {
        self.applies.lock().unwrap().clone()
    }
}

#[async_trait]
impl ExecutionSubmissionRepository for RecordingSubmissionRepo {
    async fn claim_for_submission(
        &self,
        _intent_id: &OrderIntentId,
        _now: chrono::DateTime<Utc>,
    ) -> Result<OrderIntentInfo, StorageError> {
        unimplemented!()
    }

    async fn revert_claim(
        &self,
        _intent_id: &OrderIntentId,
    ) -> Result<OrderIntentInfo, StorageError> {
        unimplemented!()
    }

    async fn reject_admission(
        &self,
        _intent_id: &OrderIntentId,
        _status_reason: String,
        _admission_trace_ref: Option<String>,
    ) -> Result<OrderIntentInfo, StorageError> {
        unimplemented!()
    }

    async fn create_entry_order_and_lock_capital(
        &self,
        _order: NewExecutionOrder,
    ) -> Result<ExecutionOrderInfo, StorageError> {
        unimplemented!()
    }

    async fn record_submission_result(
        &self,
        _execution_order_id: &ExecutionOrderId,
        _write: SubmissionLedgerWrite,
    ) -> Result<ExecutionOrderInfo, StorageError> {
        unimplemented!()
    }

    async fn recover_dangling(&self, _limit: u64) -> Result<Vec<ExecutionOrderInfo>, StorageError> {
        Ok(Vec::new())
    }

    async fn apply_reconciliation(
        &self,
        _execution_order_id: &ExecutionOrderId,
        write: ReconciliationLedgerWrite,
    ) -> Result<ExecutionOrderInfo, StorageError> {
        self.applies.lock().unwrap().push(write.clone());
        Ok(ExecutionOrderInfo {
            execution_order_id: ExecutionOrderId::from_v7(),
            order_intent_id: OrderIntentId::from_v7(),
            order_phase: ExecutionOrderPhase::Entry,
            market_id: MarketId::new("0xmkt"),
            token_id: TokenId::new("token"),
            side: Side::Buy,
            order_type: OrderTypeKind::Gtc,
            price: write.venue_avg_price.unwrap_or(Price::new(dec!(0.6))),
            shares: Shares::new(dec!(100)),
            cost_usd: Usd::new(dec!(60)),
            venue_order_id: write.venue_order_id,
            venue_status: write.venue_status,
            state: write.order_state,
            submitted_at: Some(now()),
            filled_at: write.filled_at,
            cancelled_at: write.cancelled_at,
            gtd_expiration_at: None,
            error_message: write.error_message,
            created_at: now(),
            updated_at: now(),
        })
    }
}

struct FailingCollector;

#[async_trait]
impl EvidenceCollector for FailingCollector {
    async fn collect(
        &self,
        _order: &ExecutionOrderInfo,
        _now: chrono::DateTime<Utc>,
        _stale_after: Duration,
    ) -> QuantResult<CollectedReconciliation> {
        Err(QuantError::Api(ApiError::Timeout {
            operation: "venue unreachable".to_owned(),
            elapsed_ms: 5_000,
        }))
    }
}

struct StaleCancelReader {
    collect_pass: AtomicUsize,
    venue_order_id: OrderId,
    token_id: TokenId,
}

#[async_trait]
impl VenueReconciliationReader for StaleCancelReader {
    async fn open_orders(&self) -> QuantResult<Vec<OpenOrder>> {
        if self.collect_pass.fetch_add(1, Ordering::SeqCst) == 0 {
            Ok(vec![OpenOrder {
                order_id: self.venue_order_id.clone(),
                token_id: self.token_id.clone(),
                side: Side::Buy,
                price: Price::new(dec!(0.6)),
                size: Shares::new(dec!(100)),
                filled: Shares::ZERO,
            }])
        } else {
            Ok(Vec::new())
        }
    }

    async fn trades_for(
        &self,
        _token_id: &TokenId,
        _after: chrono::DateTime<Utc>,
    ) -> QuantResult<Vec<ClobTrade>> {
        Ok(Vec::new())
    }

    async fn token_balance(&self, _token_id: &TokenId) -> QuantResult<Shares> {
        Ok(Shares::ZERO)
    }

    async fn collateral_balance(&self) -> QuantResult<Usd> {
        Ok(Usd::new(dec!(9000)))
    }
}

struct RecordingCancelClient {
    cancel_count: AtomicU32,
}

#[async_trait]
impl PolymarketOrderClient for RecordingCancelClient {
    async fn submit(
        &self,
        _order: quant_pivot_core::execution::VenueOrder,
    ) -> quant_pivot_core::execution::VenueSubmitResult {
        unimplemented!()
    }

    async fn cancel(
        &self,
        venue_order_id: &OrderId,
    ) -> quant_pivot_core::execution::VenueCancelResult {
        self.cancel_count.fetch_add(1, Ordering::SeqCst);
        quant_pivot_core::execution::VenueCancelResult {
            venue_order_id: venue_order_id.clone(),
            cancelled: true,
            detail: Some("cancelled in test".to_owned()),
            responded_at: now(),
        }
    }
}

#[derive(Default)]
struct RecordingOpLog;

#[async_trait]
impl OperationLogRepository for RecordingOpLog {
    async fn append(&self, _log: NewOperationLog) -> Result<(), StorageError> {
        Ok(())
    }

    async fn append_batch(&self, _logs: Vec<NewOperationLog>) -> Result<(), StorageError> {
        Ok(())
    }

    async fn page(
        &self,
        _query: OperationLogQuery,
    ) -> Result<Paginated<OperationLogInfo>, StorageError> {
        Ok(Paginated::empty(1, 0))
    }
}

struct ServiceHarness {
    service: ReconciliationService,
    submission: Arc<RecordingSubmissionRepo>,
    cancel_client: Arc<RecordingCancelClient>,
    kill_switch: Arc<RecordingKillSwitch>,
    metrics: Arc<MetricsHub>,
}

fn service_harness(
    collector: Arc<dyn EvidenceCollector>,
    orders: Vec<ExecutionOrderInfo>,
    rec: &RecommendationInfo,
    row_intent: &OrderIntentInfo,
) -> ServiceHarness {
    let mut config = RuntimeConfig::default();
    config.execution.reconciliation = ReconciliationPolicy {
        enabled: true,
        interval_secs: 60,
        stale_open_secs: STALE_SECS,
    };
    let metrics = Arc::new(MetricsHub::new());
    let kill_switch = Arc::new(RecordingKillSwitch::default());
    let submission = Arc::new(RecordingSubmissionRepo::default());
    let cancel_client = Arc::new(RecordingCancelClient {
        cancel_count: AtomicU32::new(0),
    });
    let op_log: Arc<dyn OperationLogRepository> = Arc::new(RecordingOpLog);
    let breaker = Arc::new(ExecutionBreaker::new(
        config.execution.breaker.clone(),
        Arc::clone(&kill_switch) as Arc<dyn KillSwitchPort>,
        op_log,
        Arc::clone(&metrics),
    ));
    let service = ReconciliationService::new(ReconciliationServiceDeps {
        collector,
        order_client: Arc::clone(&cancel_client) as Arc<dyn PolymarketOrderClient>,
        execution_orders: Arc::new(MemoryExecutionOrders {
            orders: Mutex::new(orders),
        }),
        intents: Arc::new(MemoryIntents {
            intent: row_intent.clone(),
        }),
        recommendations: Arc::new(StubRecommendations(rec.clone())),
        reconciliation: Arc::new(MemoryReconciliation),
        submission: Arc::clone(&submission) as Arc<dyn ExecutionSubmissionRepository>,
        fees: Arc::new(FeeCalculator::new()),
        breaker,
        metrics: Arc::clone(&metrics),
        config: Arc::new(RuntimeConfigStore::new(config)),
    });
    ServiceHarness {
        service,
        submission,
        cancel_client,
        kill_switch,
        metrics,
    }
}

#[tokio::test]
async fn recon_service_venue_unreachable_before_stale_does_not_apply_correction() {
    let rec = recommendation();
    let row_intent = intent(&rec);
    let submitted_at = now() - Duration::seconds(30);
    let order = execution_order(&row_intent.order_intent_id, &rec.token_id, submitted_at);
    let harness = service_harness(Arc::new(FailingCollector), vec![order], &rec, &row_intent);

    harness.service.reconcile_pass(now()).await.expect("pass");

    assert!(
        harness.submission.applied_writes().is_empty(),
        "venue unreachable before the staleness deadline must not release or impair capital",
    );
    assert!(
        harness.kill_switch.sets.lock().unwrap().is_empty(),
        "must not trip the kill-switch while retrying",
    );
}

#[tokio::test]
async fn recon_service_venue_unreachable_past_stale_impairs_without_releasing() {
    let rec = recommendation();
    let row_intent = intent(&rec);
    let submitted_at = now() - Duration::seconds(i64::try_from(STALE_SECS + 60).unwrap());
    let order = execution_order(&row_intent.order_intent_id, &rec.token_id, submitted_at);
    let harness = service_harness(Arc::new(FailingCollector), vec![order], &rec, &row_intent);

    harness.service.reconcile_pass(now()).await.expect("pass");

    let writes = harness.submission.applied_writes();
    assert_eq!(writes.len(), 1, "must persist one unresolvable correction");
    assert_eq!(writes[0].result, ReconciliationResult::Unresolvable);
    assert_eq!(writes[0].capital, CapitalReconcileSettlement::Impair);
    assert_ne!(
        writes[0].capital,
        CapitalReconcileSettlement::Release,
        "fail-closed: never release capital when venue truth is unknown",
    );
    assert_eq!(harness.kill_switch.sets.lock().unwrap().len(), 1);
    assert_eq!(harness.metrics.reconciliation_unresolvable.get(), 1);
}

#[tokio::test]
async fn recon_service_stale_resting_order_cancel_then_releases_capital() {
    let rec = recommendation();
    let row_intent = intent(&rec);
    let submitted_at = now() - Duration::seconds(i64::try_from(STALE_SECS + 60).unwrap());
    let order = execution_order(&row_intent.order_intent_id, &rec.token_id, submitted_at);

    let reader = Arc::new(StaleCancelReader {
        collect_pass: AtomicUsize::new(0),
        venue_order_id: OrderId::new(VENUE_ORDER_ID),
        token_id: rec.token_id.clone(),
    });
    let metrics = Arc::new(MetricsHub::new());
    let collector = Arc::new(VenueEvidenceCollector::new(
        reader as Arc<dyn VenueReconciliationReader>,
        Arc::new(BookStore::new(metrics)),
    ));

    let harness = service_harness(collector, vec![order], &rec, &row_intent);

    harness.service.reconcile_pass(now()).await.expect("pass");

    assert_eq!(
        harness.cancel_client.cancel_count.load(Ordering::SeqCst),
        1,
        "stale resting order must be actively cancelled once",
    );

    let writes = harness.submission.applied_writes();
    assert_eq!(writes.len(), 1);
    assert_eq!(writes[0].result, ReconciliationResult::Cancelled);
    assert_eq!(writes[0].capital, CapitalReconcileSettlement::Release);
    assert_eq!(writes[0].order_state, ExecutionOrderState::Cancelled);
    assert_eq!(writes[0].venue_status, Some(VenueOrderStatus::Cancelled));
}
