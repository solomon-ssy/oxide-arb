//! Phase 05.4 — execution dispatcher orchestration tests.
//!
//! In-memory stubs exercise `CoreExecutionDispatcher::submit_if_admitted` without
//! Postgres or a live venue.

use std::{
    collections::HashSet,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
};

use async_trait::async_trait;
use chrono::{DateTime, Duration, TimeZone, Utc};
use quant_pivot_api::data_api::VenuePosition;
use quant_pivot_core::{
    execution::{
        AdmissionCheckTrace, AdmissionDecision, AdmissionInput, AdmissionInputBuilder,
        AdmissionInputBuilderDeps, AdmissionSeams, DefaultAdmissionEngine,
        ExecutionAdmissionEngine, ExecutionDispatcherDeps, ExitMonitorHealthHandle, StateVersion,
        VenueHealth, VenueHealthHandle,
        breaker::ExecutionBreaker,
        dispatcher::CoreExecutionDispatcher,
        order_client::{
            PolymarketOrderClient, VenueCancelResult, VenueOrder, VenueOutcome, VenueSubmitResult,
        },
    },
    governance::{KillSwitchHandle, RuntimeModeHandle},
    observability::{execution_fact_writer::ExecutionEventWriter, metrics_hub::MetricsHub},
    pipeline::{book_store::BookStore, market_registry::MarketRegistry},
    runtime_config::RuntimeConfigStore,
    service::account::{AccountProviderFactory, PolymarketAccountClient, ReservedCapitalReader},
};
use quant_pivot_error::{
    QuantError, QuantResult,
    execution::ExecutionError,
    storage::{StorageError, entity},
};
use quant_pivot_models::{
    domain::{
        AppendReconciliationEvidence, ApproveOrderIntent, ApproveOrderIntentOutcome, BookLevel,
        BookSnapshot, CapitalAllocationInfo, CapitalSettlement, DataQualityPort,
        DataQualitySnapshot, ExecutionOrderInfo, ExecutionOrderPatch, ExecutionSubmitPort,
        ExitLedgerWrite, KillSwitchPort, KillSwitchView, MarketInfo, MarketPageQuery,
        ModelSpecInfo, ModelVersionInfo, NewCapitalAllocation, NewExecutionOrder, NewModelSpec,
        NewModelVersion, NewOperationLog, NewOrderIntent, NewReconciliation, NewReportTransaction,
        NewRuntimeConfigActivation, NewRuntimeConfigVersion, OperationLogInfo, OperationLogQuery,
        OrderIntentInfo, OrderIntentListQuery, Paginated, QuantReportListQuery, RecommendationInfo,
        RecommendationReportInfo, ReconciliationInfo, ReconciliationLedgerWrite,
        ReconciliationListQuery, ReconciliationPatch, RuntimeConfigActivationInfo,
        RuntimeConfigVersionInfo, SetKillSwitchCommand, SubmissionLedgerWrite, UpsertMarket,
    },
    enums::{
        common::{MarketCategory, OrderType, Side, TickSize},
        execution::{
            AdmissionCheckId, AdmissionOutcome, ApprovalInvalidation, CapitalAllocationState,
            ExitReason, ExitState, KillSwitchState, OrderIntentKind,
        },
        market::MarketStatus,
        quant::{
            AccountSource, ApprovalStatus, ExecutionOrderState, OrderIntentStatus, OutcomeSide,
            PublicationStatus, QuantRuntimeMode, RecommendationReportStatus, ReportKind,
        },
        runtime_config::RuntimeConfigVersionSource,
    },
    runtime_config::{DecimalString, ExecutionBreakerConfig, RuntimeConfig},
    types::{
        Bps, CapitalAllocationId, ContentHash, EntryOrderSpec, EventId, ExecutedPartialExitNodes,
        ExecutionOrderId, ExitPolicySpec, MarketId, ModelSpecId, ModelVersionId,
        OpportunisticExitState, OrderId, OrderIntentId, Price, RecommendationId,
        RecommendationReportId, ReconciliationId, RuntimeConfigVersionId, SchemaVersion, Shares,
        TokenId, Usd,
    },
};
use quant_pivot_repository::traits::{
    CapitalAllocationRepository, ExecutionOrderRepository, ExecutionSubmissionRepository,
    MarketRepository, ModelRegistryRepository, OperationLogRepository, OrderIntentRepository,
    RecommendationReportRepository, RecommendationRepository, ReconciliationRepository,
    RuntimeConfigVersionRepository,
};
use quant_pivot_research::portfolio::AccountSnapshot;
use quant_pivot_storage::write::{AsyncWriter, AsyncWriterConfig, AsyncWriterObservability};
use quant_pivot_test_support::report_fixtures;
use rust_decimal_macros::dec;

const NOW_SECS: i64 = 1_700_001_000;
const NOW_MS: u64 = 1_700_001_000_000;

fn now() -> DateTime<Utc> {
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

fn report(rec: &RecommendationInfo) -> RecommendationReportInfo {
    report_fixtures::report(
        rec.recommendation_report_id.clone(),
        ReportKind::TopN,
        RecommendationReportStatus::Published,
    )
}

fn intent(
    rec: &RecommendationInfo,
    status: OrderIntentStatus,
    order_type: OrderType,
) -> OrderIntentInfo {
    OrderIntentInfo {
        order_intent_id: OrderIntentId::from_v7(),
        recommendation_id: rec.recommendation_id.clone(),
        runtime_mode: QuantRuntimeMode::SemiAuto,
        runtime_config_version_id: RuntimeConfigVersionId::from_v7(),
        model_version_id: ModelVersionId::from_v7(),
        intent_kind: OrderIntentKind::Buy,
        status,
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
            order_type,
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
        expires_at: Utc::now() + Duration::hours(1),
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

fn auto_intent(rec: &RecommendationInfo) -> OrderIntentInfo {
    let mut row = intent(rec, OrderIntentStatus::ApprovedByPolicy, OrderType::Gtc);
    row.runtime_mode = QuantRuntimeMode::AutoExecution;
    row.approval_status = ApprovalStatus::NotRequired;
    row.policy_id = Some("auto".to_owned());
    row
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

fn book(asks: Vec<BookLevel>) -> Arc<BookSnapshot> {
    Arc::new(BookSnapshot::new(
        Arc::from(Vec::<BookLevel>::new()),
        Arc::from(asks),
        NOW_MS - 500,
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

// ── In-memory submission + intent repos ─────────────────────────────────────

struct MemoryIntentRepo {
    intent: Mutex<OrderIntentInfo>,
}

#[async_trait]
impl OrderIntentRepository for MemoryIntentRepo {
    async fn create_with_allocation(
        &self,
        _: NewOrderIntent,
        _: NewCapitalAllocation,
    ) -> Result<OrderIntentInfo, StorageError> {
        Err(StorageError::state_conflict(
            entity::QUANT_ORDER_INTENT,
            None::<&str>,
            "stub",
        ))
    }

    async fn approve(
        &self,
        _: &OrderIntentId,
        _: ApproveOrderIntent,
        _: Option<EntryOrderSpec>,
        _: Option<Usd>,
        _: DateTime<Utc>,
    ) -> Result<ApproveOrderIntentOutcome, StorageError> {
        Err(StorageError::state_conflict(
            entity::QUANT_ORDER_INTENT,
            None::<&str>,
            "stub",
        ))
    }

    async fn reject(&self, _: &OrderIntentId, _: String) -> Result<OrderIntentInfo, StorageError> {
        Err(StorageError::state_conflict(
            entity::QUANT_ORDER_INTENT,
            None::<&str>,
            "stub",
        ))
    }

    async fn cancel(&self, _: &OrderIntentId, _: String) -> Result<OrderIntentInfo, StorageError> {
        Err(StorageError::state_conflict(
            entity::QUANT_ORDER_INTENT,
            None::<&str>,
            "stub",
        ))
    }

    async fn expire(
        &self,
        _: &OrderIntentId,
        _: NewOperationLog,
    ) -> Result<OrderIntentInfo, StorageError> {
        Err(StorageError::state_conflict(
            entity::QUANT_ORDER_INTENT,
            None::<&str>,
            "stub",
        ))
    }

    async fn invalidate(
        &self,
        _: &OrderIntentId,
        _: ApprovalInvalidation,
        _: NewOperationLog,
    ) -> Result<OrderIntentInfo, StorageError> {
        Err(StorageError::state_conflict(
            entity::QUANT_ORDER_INTENT,
            None::<&str>,
            "stub",
        ))
    }

    async fn find_by_id(
        &self,
        id: &OrderIntentId,
    ) -> Result<Option<OrderIntentInfo>, StorageError> {
        let row = self.intent.lock().unwrap();
        Ok((row.order_intent_id == *id).then(|| row.clone()))
    }

    async fn page(
        &self,
        _: OrderIntentListQuery,
    ) -> Result<Paginated<OrderIntentInfo>, StorageError> {
        Ok(Paginated::empty(1, 0))
    }

    async fn find_expired(&self, _: DateTime<Utc>) -> Result<Vec<OrderIntentInfo>, StorageError> {
        Ok(Vec::new())
    }

    async fn upcoming_expirations(
        &self,
        _: DateTime<Utc>,
        _: u64,
    ) -> Result<Vec<(OrderIntentId, DateTime<Utc>)>, StorageError> {
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

    async fn find_blocking_by_report(
        &self,
        _: &RecommendationReportId,
    ) -> Result<Vec<OrderIntentInfo>, StorageError> {
        Ok(Vec::new())
    }

    async fn count_open(&self) -> Result<u64, StorageError> {
        Ok(0)
    }

    async fn find_attribution_candidates(
        &self,
        _: Vec<OrderIntentStatus>,
        _: u64,
    ) -> Result<Vec<OrderIntentInfo>, StorageError> {
        Ok(Vec::new())
    }
}

struct MemorySubmissionRepo {
    intent: Mutex<OrderIntentInfo>,
    in_txn: AtomicBool,
    venue_called_in_txn: AtomicBool,
    reject_called: AtomicBool,
    revert_called: AtomicBool,
    orders: Mutex<Vec<ExecutionOrderInfo>>,
    last_ledger_write: Mutex<Option<SubmissionLedgerWrite>>,
}

impl MemorySubmissionRepo {
    const fn new(intent: OrderIntentInfo) -> Self {
        Self {
            intent: Mutex::new(intent),
            in_txn: AtomicBool::new(false),
            venue_called_in_txn: AtomicBool::new(false),
            reject_called: AtomicBool::new(false),
            revert_called: AtomicBool::new(false),
            orders: Mutex::new(Vec::new()),
            last_ledger_write: Mutex::new(None),
        }
    }

    fn current_status(&self) -> OrderIntentStatus {
        self.intent.lock().unwrap().status
    }

    fn last_ledger_write(&self) -> Option<SubmissionLedgerWrite> {
        self.last_ledger_write.lock().unwrap().clone()
    }
}

#[async_trait]
impl ExecutionSubmissionRepository for MemorySubmissionRepo {
    async fn claim_for_submission(
        &self,
        intent_id: &OrderIntentId,
        now: DateTime<Utc>,
    ) -> Result<OrderIntentInfo, StorageError> {
        let mut row = self.intent.lock().unwrap();
        if row.order_intent_id != *intent_id {
            return Err(StorageError::NotFound {
                entity: "order_intent",
                id: intent_id.to_string(),
            });
        }
        if !matches!(
            row.status,
            OrderIntentStatus::Approved | OrderIntentStatus::ApprovedByPolicy
        ) {
            return Err(StorageError::state_conflict(
                entity::QUANT_ORDER_INTENT,
                Some(intent_id),
                format!("not submittable from {}", row.status.as_str()),
            ));
        }
        if row.expires_at <= now {
            return Err(StorageError::state_conflict(
                entity::QUANT_ORDER_INTENT,
                Some(intent_id),
                "intent has expired and cannot be submitted",
            ));
        }
        row.status = OrderIntentStatus::AdmissionPending;
        Ok(row.clone())
    }

    async fn revert_claim(
        &self,
        intent_id: &OrderIntentId,
    ) -> Result<OrderIntentInfo, StorageError> {
        self.revert_called.store(true, Ordering::SeqCst);
        let mut row = self.intent.lock().unwrap();
        if row.order_intent_id != *intent_id {
            return Err(StorageError::NotFound {
                entity: "order_intent",
                id: intent_id.to_string(),
            });
        }
        if row.status == OrderIntentStatus::AdmissionPending {
            row.status = if row.policy_id.is_some() {
                OrderIntentStatus::ApprovedByPolicy
            } else {
                OrderIntentStatus::Approved
            };
        }
        Ok(row.clone())
    }

    async fn reject_admission(
        &self,
        intent_id: &OrderIntentId,
        reason: String,
        trace: Option<String>,
    ) -> Result<OrderIntentInfo, StorageError> {
        self.reject_called.store(true, Ordering::SeqCst);
        let mut row = self.intent.lock().unwrap();
        row.status = OrderIntentStatus::AdmissionRejected;
        row.status_reason = Some(reason);
        row.admission_trace_ref = trace;
        assert_eq!(row.order_intent_id, *intent_id);
        Ok(row.clone())
    }

    async fn create_entry_order_and_lock_capital(
        &self,
        order: NewExecutionOrder,
    ) -> Result<ExecutionOrderInfo, StorageError> {
        self.in_txn.store(true, Ordering::SeqCst);
        let info = {
            let mut row = self.intent.lock().unwrap();
            row.status = OrderIntentStatus::Submitted;
            drop(row);
            ExecutionOrderInfo {
                execution_order_id: order.execution_order_id.clone(),
                order_intent_id: order.order_intent_id.clone(),
                order_phase: order.order_phase,
                market_id: order.market_id.clone(),
                token_id: order.token_id.clone(),
                side: order.side,
                order_type: order.order_type,
                price: order.price,
                shares: order.shares,
                cost_usd: order.cost_usd,
                venue_order_id: None,
                venue_status: None,
                state: ExecutionOrderState::Submitted,
                submitted_at: None,
                filled_at: None,
                cancelled_at: None,
                gtd_expiration_at: order.gtd_expiration_at,
                error_message: None,
                created_at: now(),
                updated_at: now(),
            }
        };
        self.orders.lock().unwrap().push(info.clone());
        self.in_txn.store(false, Ordering::SeqCst);
        Ok(info)
    }

    async fn record_submission_result(
        &self,
        execution_order_id: &ExecutionOrderId,
        write: SubmissionLedgerWrite,
    ) -> Result<ExecutionOrderInfo, StorageError> {
        self.in_txn.store(true, Ordering::SeqCst);
        let write_snapshot = write.clone();
        let recorded = {
            let mut orders = self.orders.lock().unwrap();
            let order = orders
                .iter_mut()
                .find(|o| o.execution_order_id == *execution_order_id)
                .ok_or_else(|| {
                    StorageError::not_found(entity::QUANT_EXECUTION_ORDER, execution_order_id)
                })?;
            order.state = write.state;
            order.venue_order_id = write.venue_order_id;
            order.venue_status = write.venue_status;
            let recorded = order.clone();
            drop(orders);
            let mut intent = self.intent.lock().unwrap();
            if write.intent_status != intent.status {
                intent.status = write.intent_status;
            }
            drop(intent);
            *self.last_ledger_write.lock().unwrap() = Some(write_snapshot);
            recorded
        };
        self.in_txn.store(false, Ordering::SeqCst);
        Ok(recorded)
    }

    async fn mark_exit_manual(
        &self,
        _intent_id: &OrderIntentId,
        _reason: ExitReason,
    ) -> Result<(), StorageError> {
        Ok(())
    }

    async fn touch_exit_monitor(
        &self,
        _intent_id: &OrderIntentId,
        _next_check_at: DateTime<Utc>,
        _peak_mark_price: Option<Price>,
        _last_signal_recheck_at: Option<DateTime<Utc>>,
    ) -> Result<(), StorageError> {
        Ok(())
    }

    async fn create_exit_order_and_mark_closing(
        &self,
        _order: NewExecutionOrder,
        _exit_reason: ExitReason,
        _partial_exit_node_id: Option<String>,
    ) -> Result<ExecutionOrderInfo, StorageError> {
        unimplemented!("exit submission not exercised by entry-dispatcher tests")
    }

    async fn record_exit_result(
        &self,
        _execution_order_id: &ExecutionOrderId,
        _write: ExitLedgerWrite,
    ) -> Result<ExecutionOrderInfo, StorageError> {
        unimplemented!("exit submission not exercised by entry-dispatcher tests")
    }

    async fn recover_dangling(&self, _: u64) -> Result<Vec<ExecutionOrderInfo>, StorageError> {
        Ok(Vec::new())
    }

    async fn apply_reconciliation(
        &self,
        _execution_order_id: &ExecutionOrderId,
        _write: ReconciliationLedgerWrite,
    ) -> Result<ExecutionOrderInfo, StorageError> {
        unimplemented!("reconciliation is exercised in phase_05_5 tests")
    }
}

struct RecordingOrderClient {
    submission: Arc<MemorySubmissionRepo>,
    result: VenueSubmitResult,
    captured: Mutex<Option<VenueOrder>>,
    submit_count: Mutex<u32>,
}

#[async_trait]
impl PolymarketOrderClient for RecordingOrderClient {
    async fn submit(&self, order: VenueOrder) -> VenueSubmitResult {
        if self.submission.in_txn.load(Ordering::SeqCst) {
            self.submission
                .venue_called_in_txn
                .store(true, Ordering::SeqCst);
        }
        *self.captured.lock().unwrap() = Some(order);
        *self.submit_count.lock().unwrap() += 1;
        self.result.clone()
    }

    async fn cancel(&self, _: &OrderId) -> VenueCancelResult {
        unimplemented!("not needed")
    }
}

struct ConfigurableAdmission {
    outcome: AdmissionOutcome,
    reason: String,
}

#[async_trait]
impl ExecutionAdmissionEngine for ConfigurableAdmission {
    async fn evaluate(
        &self,
        _: quant_pivot_core::execution::AdmissionInput,
    ) -> QuantResult<AdmissionDecision> {
        let trace = AdmissionCheckTrace {
            check: AdmissionCheckId::RuntimeMode,
            outcome: self.outcome,
            threshold: None,
            actual: None,
            elapsed_us: 0,
            detail: self.reason.clone(),
        };
        Ok(AdmissionDecision {
            outcome: self.outcome,
            trace: vec![trace],
            state_version: StateVersion {
                config_version_id: RuntimeConfigVersionId::from_v7(),
                account_as_of: now(),
                book_version: Some(1),
                book_as_of_ms: Some(NOW_MS),
                kill_switch_state: KillSwitchState::Closed,
            },
            elapsed_ms: 0,
            denial_reason: (self.outcome == AdmissionOutcome::Deny).then(|| self.reason.clone()),
        })
    }

    async fn evaluate_full(
        &self,
        input: quant_pivot_core::execution::AdmissionInput,
    ) -> QuantResult<AdmissionDecision> {
        self.evaluate(input).await
    }
}

struct StubKillSwitch;

#[async_trait]
impl KillSwitchPort for StubKillSwitch {
    fn current(&self) -> KillSwitchState {
        KillSwitchState::Closed
    }

    fn view(&self) -> KillSwitchView {
        KillSwitchView {
            state: KillSwitchState::Closed,
            requires_operator_ack: false,
            last_reason: "test".to_owned(),
            changed_by: "test".to_owned(),
            changed_at: now(),
        }
    }

    async fn set(&self, _: SetKillSwitchCommand) -> QuantResult<KillSwitchView> {
        Ok(self.view())
    }
}

struct StubOpLog;

#[async_trait]
impl OperationLogRepository for StubOpLog {
    async fn append(&self, _: NewOperationLog) -> Result<(), StorageError> {
        Ok(())
    }

    async fn append_batch(&self, _: Vec<NewOperationLog>) -> Result<(), StorageError> {
        Ok(())
    }

    async fn page(
        &self,
        _: OperationLogQuery,
    ) -> Result<Paginated<OperationLogInfo>, StorageError> {
        Ok(Paginated::empty(1, 0))
    }
}

struct StubAccountClient;

#[async_trait]
impl PolymarketAccountClient for StubAccountClient {
    async fn available_collateral(&self) -> QuantResult<Usd> {
        Ok(Usd::new(dec!(10_000)))
    }

    async fn positions(&self, _: &str) -> QuantResult<Vec<VenuePosition>> {
        Ok(Vec::new())
    }
}

struct StubReserved;

#[async_trait]
impl ReservedCapitalReader for StubReserved {
    async fn sum_locked(&self) -> QuantResult<Usd> {
        Ok(Usd::ZERO)
    }
}

struct StubRecommendations(RecommendationInfo);

#[async_trait]
impl RecommendationRepository for StubRecommendations {
    async fn find_by_report(
        &self,
        _: &RecommendationReportId,
    ) -> Result<Vec<RecommendationInfo>, StorageError> {
        unimplemented!()
    }

    async fn find_by_id(
        &self,
        _: &RecommendationId,
    ) -> Result<Option<RecommendationInfo>, StorageError> {
        Ok(Some(self.0.clone()))
    }

    async fn find_expirable(
        &self,
        _: DateTime<Utc>,
        _: u64,
    ) -> Result<Vec<RecommendationId>, StorageError> {
        unimplemented!()
    }

    async fn upcoming_expirations(
        &self,
        _: DateTime<Utc>,
        _: u64,
    ) -> Result<Vec<(RecommendationId, DateTime<Utc>)>, StorageError> {
        unimplemented!()
    }

    async fn expire(
        &self,
        _: &RecommendationId,
        _: NewOperationLog,
    ) -> Result<RecommendationInfo, StorageError> {
        unimplemented!()
    }

    async fn find_expired_attribution_candidates(
        &self,
        _: u64,
    ) -> Result<Vec<RecommendationInfo>, StorageError> {
        unimplemented!()
    }

    async fn recommendation_blocks_final_attribution(
        &self,
        _: &RecommendationId,
    ) -> Result<bool, StorageError> {
        unimplemented!()
    }
}

struct StubReports(RecommendationReportInfo);

#[async_trait]
impl RecommendationReportRepository for StubReports {
    async fn create_report(
        &self,
        _: NewReportTransaction,
    ) -> Result<RecommendationReportInfo, StorageError> {
        unimplemented!()
    }

    async fn find_by_id(
        &self,
        _: &RecommendationReportId,
    ) -> Result<Option<RecommendationReportInfo>, StorageError> {
        Ok(Some(self.0.clone()))
    }

    async fn page(
        &self,
        _: QuantReportListQuery,
    ) -> Result<Paginated<RecommendationReportInfo>, StorageError> {
        unimplemented!()
    }

    async fn latest_published(
        &self,
        _: ReportKind,
    ) -> Result<Option<RecommendationReportInfo>, StorageError> {
        unimplemented!()
    }

    async fn find_by_trigger_key(
        &self,
        _: &str,
    ) -> Result<Option<RecommendationReportInfo>, StorageError> {
        unimplemented!()
    }

    async fn find_expirable(
        &self,
        _: DateTime<Utc>,
        _: u64,
    ) -> Result<Vec<RecommendationReportId>, StorageError> {
        unimplemented!()
    }

    async fn roll_up_to_expired(
        &self,
        _: &RecommendationReportId,
        _: DateTime<Utc>,
        _: NewOperationLog,
    ) -> Result<Option<RecommendationReportInfo>, StorageError> {
        unimplemented!()
    }

    async fn revoke(
        &self,
        _: &RecommendationReportId,
        _: &str,
        _: DateTime<Utc>,
        _: NewOperationLog,
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
    async fn create_model_spec(&self, _: NewModelSpec) -> Result<ModelSpecInfo, StorageError> {
        unimplemented!()
    }

    async fn find_model_spec_by_id(
        &self,
        _: &ModelSpecId,
    ) -> Result<Option<ModelSpecInfo>, StorageError> {
        Ok(None)
    }

    async fn create_model_version(
        &self,
        _: NewModelVersion,
    ) -> Result<ModelVersionInfo, StorageError> {
        unimplemented!()
    }

    async fn next_version_for_spec(&self, _: &ModelSpecId) -> Result<i32, StorageError> {
        unimplemented!()
    }

    async fn find_model_version_by_id(
        &self,
        _: &ModelVersionId,
    ) -> Result<Option<ModelVersionInfo>, StorageError> {
        Ok(Some(ModelVersionInfo {
            model_version_id: ModelVersionId::from_v7(),
            model_spec_id: ModelSpecId::from_v7(),
            version: 1,
            artifact_hash: ContentHash::parse(format!("blake3:{}", "a".repeat(64))).expect("hash"),
            training_dataset_id: None,
            metrics_json: serde_json::json!({}),
            quality_gate_report: serde_json::json!({}),
            publication_status: PublicationStatus::Published,
            published_at: Some(now()),
            retired_at: None,
            created_at: now(),
        }))
    }

    async fn list_published_for_spec(
        &self,
        _: &ModelSpecId,
    ) -> Result<Vec<ModelVersionInfo>, StorageError> {
        unimplemented!()
    }

    async fn publish_model_version(
        &self,
        _: &ModelVersionId,
    ) -> Result<ModelVersionInfo, StorageError> {
        unimplemented!()
    }

    async fn retire_model_version(
        &self,
        _: &ModelVersionId,
    ) -> Result<ModelVersionInfo, StorageError> {
        unimplemented!()
    }

    async fn promote_model_to_shadow(
        &self,
        _: &ModelVersionId,
    ) -> Result<ModelVersionInfo, StorageError> {
        unimplemented!()
    }

    async fn restore_model_version(
        &self,
        _: &ModelVersionId,
    ) -> Result<ModelVersionInfo, StorageError> {
        unimplemented!()
    }

    async fn set_quality_gate_report(
        &self,
        _: &ModelVersionId,
        _: serde_json::Value,
    ) -> Result<ModelVersionInfo, StorageError> {
        unimplemented!()
    }
}

struct StubReconciliation;

#[async_trait]
impl ReconciliationRepository for StubReconciliation {
    async fn create(&self, _: NewReconciliation) -> Result<ReconciliationInfo, StorageError> {
        unimplemented!()
    }

    async fn append_evidence(
        &self,
        _: &ReconciliationId,
        _: AppendReconciliationEvidence,
    ) -> Result<ReconciliationInfo, StorageError> {
        unimplemented!()
    }

    async fn patch(
        &self,
        _: &ReconciliationId,
        _: ReconciliationPatch,
    ) -> Result<ReconciliationInfo, StorageError> {
        unimplemented!()
    }

    async fn find_by_id(
        &self,
        _: &ReconciliationId,
    ) -> Result<Option<ReconciliationInfo>, StorageError> {
        Ok(None)
    }

    async fn page(
        &self,
        _: ReconciliationListQuery,
    ) -> Result<Paginated<ReconciliationInfo>, StorageError> {
        Ok(Paginated::empty(1, 10))
    }

    async fn find_by_execution_order(
        &self,
        _: &ExecutionOrderId,
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

struct StubExecutionOrders;

#[async_trait]
impl ExecutionOrderRepository for StubExecutionOrders {
    async fn create(&self, _: NewExecutionOrder) -> Result<ExecutionOrderInfo, StorageError> {
        unimplemented!()
    }

    async fn find_by_intent(
        &self,
        _: &OrderIntentId,
    ) -> Result<Vec<ExecutionOrderInfo>, StorageError> {
        unimplemented!()
    }

    async fn find_by_id(
        &self,
        _: &ExecutionOrderId,
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
        _: &ExecutionOrderId,
        _: ExecutionOrderPatch,
    ) -> Result<ExecutionOrderInfo, StorageError> {
        unimplemented!()
    }
}

struct StubCapital(CapitalAllocationInfo);

#[async_trait]
impl CapitalAllocationRepository for StubCapital {
    async fn find_by_intent(
        &self,
        id: &OrderIntentId,
    ) -> Result<Option<CapitalAllocationInfo>, StorageError> {
        Ok((id == &self.0.order_intent_id).then(|| self.0.clone()))
    }

    async fn sum_reserved_usd(&self) -> Result<Usd, StorageError> {
        Ok(Usd::ZERO)
    }

    async fn has_impaired(&self) -> Result<bool, StorageError> {
        Ok(false)
    }
}

struct StubConfigVersions;

#[async_trait]
impl RuntimeConfigVersionRepository for StubConfigVersions {
    async fn create_version(
        &self,
        _: NewRuntimeConfigVersion,
    ) -> Result<RuntimeConfigVersionInfo, StorageError> {
        unimplemented!()
    }

    async fn activate_version(
        &self,
        _: NewRuntimeConfigActivation,
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
        _: &RuntimeConfigVersionId,
    ) -> Result<Option<RuntimeConfigVersionInfo>, StorageError> {
        unimplemented!()
    }

    async fn load_by_hash(
        &self,
        _: &ContentHash,
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
        _: DateTime<Utc>,
    ) -> Result<Option<RuntimeConfigVersionInfo>, StorageError> {
        unimplemented!()
    }

    async fn list_versions(&self, _: u64) -> Result<Vec<RuntimeConfigVersionInfo>, StorageError> {
        unimplemented!()
    }

    async fn list_activations(
        &self,
        _: u64,
    ) -> Result<Vec<RuntimeConfigActivationInfo>, StorageError> {
        unimplemented!()
    }
}

struct StubDataQuality;

impl DataQualityPort for StubDataQuality {
    fn snapshot(&self) -> DataQualitySnapshot {
        green_data_quality()
    }
}

fn venue_result_with_fee(outcome: VenueOutcome, fee_paid: Usd) -> VenueSubmitResult {
    VenueSubmitResult {
        venue_order_id: Some(OrderId::new("venue-test")),
        outcome,
        filled_shares: Shares::new(dec!(500)),
        avg_fill_price: Some(Price::new(dec!(0.43))),
        fee_paid,
        tx_hash: None,
        submitted_at: now(),
        responded_at: now(),
        detail: None,
    }
}

struct Harness {
    dispatcher: CoreExecutionDispatcher,
    submission: Arc<MemorySubmissionRepo>,
    order_client: Arc<RecordingOrderClient>,
    metrics: Arc<MetricsHub>,
}

fn build_harness(
    intent: OrderIntentInfo,
    admission_outcome: AdmissionOutcome,
    venue_outcome: VenueOutcome,
) -> Harness {
    build_harness_with_result(
        intent,
        admission_outcome,
        venue_result_with_fee(venue_outcome, Usd::ZERO),
    )
}

fn build_harness_with_result(
    intent: OrderIntentInfo,
    admission_outcome: AdmissionOutcome,
    venue: VenueSubmitResult,
) -> Harness {
    let rec = recommendation();
    let rep = report(&rec);
    let alloc = allocation(&intent, &rec);
    let book_store = Arc::new(BookStore::new(Arc::new(MetricsHub::new())));
    book_store.apply_snapshot(
        &intent.entry_order_json.token_id,
        Arc::from(Vec::<BookLevel>::new()),
        Arc::from([level("0.42", "600")]),
        NOW_MS - 500,
        None,
    );
    let metrics = Arc::new(MetricsHub::new());
    let intents = Arc::new(MemoryIntentRepo {
        intent: Mutex::new(intent.clone()),
    });
    let submission = Arc::new(MemorySubmissionRepo::new(intent));
    let order_client = Arc::new(RecordingOrderClient {
        submission: Arc::clone(&submission),
        result: venue,
        captured: Mutex::new(None),
        submit_count: Mutex::new(0),
    });
    let admission_builder = Arc::new(AdmissionInputBuilder::new(AdmissionInputBuilderDeps {
        recommendations: Arc::new(StubRecommendations(rec)),
        reports: Arc::new(StubReports(rep)),
        model_registry: Arc::new(StubModelRegistry),
        reconciliation: Arc::new(StubReconciliation),
        execution_orders: Arc::new(StubExecutionOrders),
        intents: Arc::clone(&intents) as Arc<dyn OrderIntentRepository>,
        capital: Arc::new(StubCapital(alloc)),
        markets: Arc::new(StubMarkets),
        config_versions: Arc::new(StubConfigVersions),
        account_factory: Arc::new(AccountProviderFactory::new(
            Some(Arc::new(StubAccountClient)),
            Arc::new(MarketRegistry::new()),
            Arc::new(StubReserved),
            Some("0xfunder".to_owned()),
        )),
        book_store,
        data_quality: Arc::new(StubDataQuality),
        config: Arc::new(RuntimeConfigStore::new(RuntimeConfig::default())),
        runtime_mode: RuntimeModeHandle::new(submission.intent.lock().unwrap().runtime_mode),
        kill_switch: KillSwitchHandle::new(KillSwitchState::Closed),
        venue_health: VenueHealthHandle::default(),
        exit_monitor_health: ExitMonitorHealthHandle::new(),
    }));
    let admission: Arc<dyn ExecutionAdmissionEngine> = Arc::new(ConfigurableAdmission {
        outcome: admission_outcome,
        reason: match admission_outcome {
            AdmissionOutcome::Deny => "denied".to_owned(),
            AdmissionOutcome::Defer => "deferred".to_owned(),
            AdmissionOutcome::Allow => "ok".to_owned(),
        },
    });
    let breaker = Arc::new(ExecutionBreaker::new(
        ExecutionBreakerConfig {
            venue_consecutive_failures_to_degrade: 99,
            venue_consecutive_failures_to_halt: 99,
            venue_error_rate_bps_to_halt: 10_001,
            venue_min_window_samples: u32::MAX,
            venue_window_secs: 60,
            cooldown_secs: 0,
            daily_realized_loss_cap_usd: DecimalString::new("0"),
        },
        Arc::new(StubKillSwitch),
        Arc::new(StubOpLog),
        Arc::clone(&metrics),
    ));
    let dispatcher = CoreExecutionDispatcher::new(ExecutionDispatcherDeps {
        intents: Arc::clone(&intents) as Arc<dyn OrderIntentRepository>,
        submission: Arc::clone(&submission) as Arc<dyn ExecutionSubmissionRepository>,
        admission_builder,
        admission,
        order_client: Arc::clone(&order_client) as Arc<dyn PolymarketOrderClient>,
        breaker,
        metrics: Arc::clone(&metrics),
        execution_events: noop_execution_writer(),
    });
    Harness {
        dispatcher,
        submission,
        order_client,
        metrics,
    }
}

// ── Tests ────────────────────────────────────────────────────────────────────

fn noop_execution_writer() -> Arc<ExecutionEventWriter> {
    let (writer, _worker) = AsyncWriter::new(
        AsyncWriterConfig::new("test_execution_events"),
        |_| Box::pin(async move { Ok(()) }),
        prometheus::IntCounter::new("test_execution_events_dropped", "test").unwrap(),
        AsyncWriterObservability::default(),
    );
    Arc::new(ExecutionEventWriter::new(Arc::new(writer)))
}

#[tokio::test]
async fn semi_auto_unapproved_cannot_submit() {
    let rec = recommendation();
    let pending = intent(&rec, OrderIntentStatus::PendingApproval, OrderType::Gtc);
    let id = pending.order_intent_id.clone();
    let harness = build_harness(pending, AdmissionOutcome::Allow, VenueOutcome::Filled);
    let err = harness
        .dispatcher
        .submit_if_admitted(&id)
        .await
        .expect_err("pending");
    assert!(matches!(
        err,
        QuantError::Execution(ExecutionError::NotSubmittable { .. })
    ));
    assert_eq!(*harness.order_client.submit_count.lock().unwrap(), 0);
}

#[tokio::test]
async fn submit_denied_when_admission_denies() {
    let rec = recommendation();
    let approved = intent(&rec, OrderIntentStatus::Approved, OrderType::Gtc);
    let id = approved.order_intent_id.clone();
    let harness = build_harness(approved, AdmissionOutcome::Deny, VenueOutcome::Filled);
    let err = harness
        .dispatcher
        .submit_if_admitted(&id)
        .await
        .expect_err("deny");
    assert!(matches!(
        err,
        QuantError::Execution(ExecutionError::AdmissionDenied { .. })
    ));
    assert!(harness.submission.reject_called.load(Ordering::SeqCst));
    assert_eq!(*harness.order_client.submit_count.lock().unwrap(), 0);
}

#[tokio::test]
async fn submit_deferred_reverts_claim_for_auto_intent() {
    let rec = recommendation();
    let approved = auto_intent(&rec);
    let id = approved.order_intent_id.clone();
    let harness = build_harness(approved, AdmissionOutcome::Defer, VenueOutcome::Filled);
    let err = harness
        .dispatcher
        .submit_if_admitted(&id)
        .await
        .expect_err("defer");
    assert!(matches!(
        err,
        QuantError::Execution(ExecutionError::AdmissionDeferred { .. })
    ));
    assert!(harness.submission.revert_called.load(Ordering::SeqCst));
    assert_eq!(
        harness.submission.current_status(),
        OrderIntentStatus::ApprovedByPolicy
    );
}

#[tokio::test]
async fn auto_execution_submit_still_passes_admission_allow_path() {
    let rec = recommendation();
    let approved = auto_intent(&rec);
    let id = approved.order_intent_id.clone();
    let harness = build_harness(approved, AdmissionOutcome::Allow, VenueOutcome::Filled);
    let order = harness
        .dispatcher
        .submit_if_admitted(&id)
        .await
        .expect("submit");
    assert_eq!(order.state, ExecutionOrderState::Filled);
    assert_eq!(*harness.order_client.submit_count.lock().unwrap(), 1);
    assert!(
        !harness
            .submission
            .venue_called_in_txn
            .load(Ordering::SeqCst),
        "venue submit must not run inside a DB transaction"
    );
}

#[tokio::test]
async fn entry_order_type_preserves_fok_and_gtd_from_intent() {
    let rec = recommendation();
    let expiration = u64::try_from((now() + Duration::hours(1)).timestamp()).unwrap_or(u64::MAX);
    let approved = intent(&rec, OrderIntentStatus::Approved, OrderType::Fok);
    let id = approved.order_intent_id.clone();
    let harness = build_harness(approved, AdmissionOutcome::Allow, VenueOutcome::Rejected);
    harness
        .dispatcher
        .submit_if_admitted(&id)
        .await
        .expect("submit");
    let captured = harness
        .order_client
        .captured
        .lock()
        .unwrap()
        .clone()
        .expect("order");
    assert!(matches!(captured.order_type, OrderType::Fok));

    let gtd_intent = intent(
        &rec,
        OrderIntentStatus::Approved,
        OrderType::Gtd { expiration },
    );
    let gtd_id = gtd_intent.order_intent_id.clone();
    let gtd_harness = build_harness(gtd_intent, AdmissionOutcome::Allow, VenueOutcome::Rejected);
    gtd_harness
        .dispatcher
        .submit_if_admitted(&gtd_id)
        .await
        .expect("submit gtd");
    let gtd_order = gtd_harness
        .order_client
        .captured
        .lock()
        .unwrap()
        .clone()
        .expect("gtd order");
    assert!(matches!(gtd_order.order_type, OrderType::Gtd { .. }));
}

#[tokio::test]
async fn quant_execution_orders_submitted_total_and_fills_total_increment() {
    let rec = recommendation();
    let approved = intent(&rec, OrderIntentStatus::Approved, OrderType::Gtc);
    let id = approved.order_intent_id.clone();
    let harness = build_harness(approved, AdmissionOutcome::Allow, VenueOutcome::Filled);
    assert_eq!(harness.metrics.execution_orders_submitted.get(), 0);
    assert_eq!(harness.metrics.execution_fills.get(), 0);
    harness
        .dispatcher
        .submit_if_admitted(&id)
        .await
        .expect("submit");
    assert_eq!(harness.metrics.execution_orders_submitted.get(), 1);
    assert_eq!(harness.metrics.execution_fills.get(), 1);
}

#[tokio::test]
async fn report_only_mode_denied_by_admission_engine() {
    let rec = recommendation();
    let mut input = AdmissionInput {
        intent: intent(&rec, OrderIntentStatus::Approved, OrderType::Gtc),
        recommendation: rec.clone(),
        report: report(&rec),
        mode: QuantRuntimeMode::ReportOnly,
        kill_switch: KillSwitchState::Closed,
        account: AccountSnapshot::new(
            now(),
            AccountSource::Polymarket,
            Usd::new(dec!(10_000)),
            Usd::new(dec!(10_000)),
            Usd::new(dec!(10_000)),
            Usd::ZERO,
            Vec::new(),
        ),
        allocation: Some(allocation(
            &intent(&rec, OrderIntentStatus::Approved, OrderType::Gtc),
            &rec,
        )),
        book: Some(book(vec![level("0.42", "600")])),
        budget_total_usd: Usd::new(dec!(10_000)),
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
            book_as_of_ms: Some(NOW_MS),
            kill_switch_state: KillSwitchState::Closed,
        },
    };
    input.mode = QuantRuntimeMode::ReportOnly;
    let decision = DefaultAdmissionEngine::new(Arc::new(MetricsHub::new()))
        .evaluate(input)
        .await
        .expect("evaluate");
    assert_eq!(decision.outcome, AdmissionOutcome::Deny);
}

#[tokio::test]
async fn fok_anomalous_partial_fails_closed_to_ambiguous() {
    let rec = recommendation();
    let approved = intent(&rec, OrderIntentStatus::Approved, OrderType::Fok);
    let id = approved.order_intent_id.clone();
    let harness = build_harness_with_result(
        approved,
        AdmissionOutcome::Allow,
        venue_result_with_fee(VenueOutcome::PartiallyFilled, Usd::ZERO),
    );
    assert_eq!(harness.metrics.execution_fills.get(), 0);
    let order = harness
        .dispatcher
        .submit_if_admitted(&id)
        .await
        .expect("submit");
    assert_eq!(order.state, ExecutionOrderState::Ambiguous);
    let write = harness
        .submission
        .last_ledger_write()
        .expect("ledger write");
    assert!(matches!(write.capital, CapitalSettlement::Hold));
    assert!(write.reconciliation.is_some());
    assert_eq!(harness.metrics.execution_fills.get(), 0);
}

#[tokio::test]
async fn settle_includes_fee_in_spent_usd() {
    let rec = recommendation();
    let approved = intent(&rec, OrderIntentStatus::Approved, OrderType::Gtc);
    let id = approved.order_intent_id.clone();
    let fee = Usd::new(dec!(1.25));
    let harness = build_harness_with_result(
        approved,
        AdmissionOutcome::Allow,
        venue_result_with_fee(VenueOutcome::Filled, fee),
    );
    harness
        .dispatcher
        .submit_if_admitted(&id)
        .await
        .expect("submit");
    let write = harness
        .submission
        .last_ledger_write()
        .expect("ledger write");
    let fill_cost = Shares::new(dec!(500)) * Price::new(dec!(0.43));
    assert_eq!(
        write.capital,
        CapitalSettlement::SettleFull {
            spent_usd: fill_cost + fee,
        }
    );
}
