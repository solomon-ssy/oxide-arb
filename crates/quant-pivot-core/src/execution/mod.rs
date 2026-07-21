//! Execution plane.
//!
//! This module owns admission, dispatch, reconciliation, exit monitoring, and
//! their production boundaries.

pub mod admission;
pub mod attribution;
pub mod breaker;
pub mod dispatch_wake;
pub mod dispatcher;
pub mod entry_condition;
pub mod entry_condition_worker;
pub mod exit_dispatcher;
pub mod exit_monitor;
pub mod exit_monitor_service;
pub mod intent_lifecycle;
pub mod intent_service;
pub mod mode_gate;
pub mod order_client;
pub mod reconciliation;
pub mod settlement_redeem;
pub mod trade_policy_guard;

pub use admission::{
    AdmissionCheck, AdmissionCheckTrace, AdmissionDecision, AdmissionExposureState, AdmissionInput,
    AdmissionInputBuilder, AdmissionInputBuilderDeps, AdmissionModelState, AdmissionSeams,
    AdmissionVenueMetadata, DefaultAdmissionEngine, ExecutionAdmissionEngine, StateVersion,
    VenueHealth, prepare_entry_order,
};
pub use attribution::{AttributionPassSummary, AttributionService, AttributionServiceDeps};
pub use breaker::{ExecutionBreaker, VenueHealthHandle};
pub use dispatch_wake::DispatchWake;
pub use dispatcher::{CoreExecutionDispatcher, ExecutionDispatcherDeps};
pub use entry_condition::{
    EntryConditionEvaluation, EntryConditionStateDecision, decide_entry_condition_state,
    evaluate_entry_condition,
};
pub use entry_condition_worker::{
    EntryConditionInputProvider, EntryConditionWorker, LiveEntryConditionInputDeps,
    LiveEntryConditionInputProvider,
};
pub use exit_dispatcher::{CoreExitDispatcher, ExitDispatcherDeps, ExitSubmitRequest};
pub use exit_monitor::{
    CompositeExitSignalEvaluator, ExitDecision, ExitMonitorHealth, ExitMonitorHealthHandle,
    ExitMonitorInput, ExitOrderSpec, ExitSignalContext, ExitSignalEvaluation, ExitSignalEvaluator,
    ExitSignalVerdict, decide_exit,
};
pub use exit_monitor_service::{ExitMonitorService, ExitMonitorServiceDeps};
pub use intent_lifecycle::IntentLifecyclePublisher;
pub use intent_service::{CoreOrderIntentService, IntentTerminalEventSink, OrderIntentServiceDeps};
pub use mode_gate::{DefaultRuntimeModeGate, IntentPolicyDecision, RuntimeModeGate};
pub use order_client::{
    ClobOrderClient, PolymarketOrderClient, VenueCancelResult, VenueOrder, VenueOutcome,
    VenueSubmitResult,
};
pub use reconciliation::{
    ClobReconciliationReader, CollectedReconciliation, EvidenceCollector,
    OperatorReconcileResolution, ReconcileFacts, ReconciliationDecision, ReconciliationService,
    ReconciliationServiceDeps, VenueEvidenceCollector, VenuePresence, VenueReconciliationReader,
    decide,
};
pub use settlement_redeem::{
    RelayerConnectParams, RelayerSettlementClient, SettlementCtfBalances, SettlementCtfClient,
    SettlementCtfPayoutVector, SettlementCtfRedeemReceipt, SettlementCtfSubmittedRedeemReceipt,
    SettlementRedeemPassSummary, SettlementRedeemService, SettlementRedeemServiceDeps,
    SettlementRedeemTx,
};
pub use trade_policy_guard::require_frozen_trade_policy;
