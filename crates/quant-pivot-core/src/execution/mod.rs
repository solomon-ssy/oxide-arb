//! Execution plane.
//!
//! This module owns admission, dispatch, reconciliation, exit monitoring, and
//! their production boundaries.

pub mod account_chain_projector;
pub mod account_pause;
pub mod admission;
pub mod breaker;
pub mod dispatch_wake;
pub mod dispatcher;
pub mod entry_condition;
pub mod entry_condition_worker;
pub mod execution_order_lifecycle;
pub mod exit_dispatcher;
pub mod exit_monitor;
pub mod exit_monitor_service;
pub mod intent_lifecycle;
pub mod intent_service;
pub mod order_client;
pub mod outcome_reconciliation;
pub mod reconciliation;
pub mod settlement_confirmation;
pub mod settlement_discovery;
pub mod settlement_discovery_wake;
pub mod settlement_executor;
pub mod settlement_external;
pub mod settlement_governed_action_service;
pub mod settlement_lifecycle;
pub mod settlement_preflight;
pub mod settlement_recovery_admission;
pub mod settlement_service;
mod settlement_timing;
pub mod terms_drift_wake;
pub mod trade_policy_guard;

pub use account_chain_projector::{
    AccountChainExecutionProjector, AccountChainExecutionProjectorDeps,
};
pub use account_pause::AccountPauseCoordinator;
pub use admission::{
    AdmissionCheck, AdmissionCheckTrace, AdmissionDecision, AdmissionExposureState, AdmissionInput,
    AdmissionInputBuilder, AdmissionInputBuilderDeps, AdmissionModelState, AdmissionSeams,
    AdmissionVenueMetadata, DefaultAdmissionEngine, ExecutionAdmissionEngine, StateVersion,
    VenueHealth,
};
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
pub use execution_order_lifecycle::ExecutionOrderLifecyclePublisher;
pub use exit_dispatcher::{CoreExitDispatcher, ExitDispatcherDeps, ExitSubmitRequest};
pub use exit_monitor::{
    CompositeExitSignalEvaluator, ExitDecision, ExitMonitorHealth, ExitMonitorHealthHandle,
    ExitMonitorInput, ExitOrderSpec, ExitSignalContext, ExitSignalEvaluation, ExitSignalEvaluator,
    ExitSignalVerdict,
};
pub use exit_monitor_service::{ExitMonitorService, ExitMonitorServiceDeps};
pub use intent_lifecycle::IntentLifecyclePublisher;
pub use intent_service::{CoreOrderIntentService, IntentTerminalEventSink, OrderIntentServiceDeps};
pub use order_client::{
    ClobOrderClient, PolymarketOrderClient, VenueCancelResult, VenueOrder, VenueOutcome,
    VenueSubmitResult,
};
pub use outcome_reconciliation::{
    OutcomeReconciliationPassConfig, OutcomeReconciliationPassSummary,
    OutcomeReconciliationService, OutcomeReconciliationServiceDeps,
};
pub use reconciliation::{
    ClobReconciliationReader, CollectedReconciliation, EvidenceCollector,
    OperatorReconcileResolution, ReconcileFacts, ReconciliationDecision, ReconciliationService,
    ReconciliationServiceDeps, VenueEvidenceCollector, VenuePresence, VenueReconciliationReader,
};
pub use settlement_lifecycle::SettlementLifecyclePublisher;
pub use trade_policy_guard::require_frozen_trade_policy;
