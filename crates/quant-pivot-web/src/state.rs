//! Shared application state injected into every request.

use std::sync::Arc;

use crate::{
    audit::OperationLogBuffer,
    auth::casbin::{CasbinService, PermChecker},
    jwt::{JwtService, RedisTokenBlacklist},
    ws::SessionRegistry,
};
use quant_pivot_models::{
    config::DeployConfig,
    domain::{
        ports::{
            AccountReadPort, BacktestPort, CalibrationArtifactFitPort, CatalogStatusPort,
            CommittedPolicyApplyPort, CpcvBacktestPort, DataQualityPort, ExecutionReadPort,
            ExecutionRecoveryPort, FeatureIntegrityPort, FeedbackMutationPort, FeedbackReadPort,
            KillSwitchPort, MarketDataPort, MarketLinkageGovernancePort, MetricsScrapePort,
            ModelCalibrationFitPort, ModelGovernancePort, ModelSpecPort, ModelTrainingPort,
            OrderIntentPort, PolicySnapshotPort, QuantReportPort, ReadinessPort,
            ReconciliationPort, ResearchCatalogPort, ResearchJobPort, ResearchReadinessPort,
            RuntimeControlPort, StructuralMonitorPort, SystemCapabilityPort, TradePolicyPort,
            TrainingDatasetPort, settlement_control::SettlementControlPort,
        },
        runtime::{
            CoreEvent, CoreEventPublisher, MaterializationRunEvent, MaterializationRunKind,
            MaterializationRunStatus,
        },
    },
};
use quant_pivot_repository::traits::{
    BasisAlertRepository, DomainSourceCursorRepository, DomainSourceExpectationRepository,
    EntryConditionRepository, FeedbackOutboxRepository, MarketLinkageRepository, MarketRepository,
    MenuRepository, OperationLogRepository, PolicyRepository, QuantFactReadRepository,
    RoleMenuRepository, RolePermissionRepository, RoleRepository, UserRepository,
    UserRoleRepository,
};

/// Dependency bundle shared by all handlers and middleware.
#[derive(Clone)]
pub struct AppState {
    pub deploy: Arc<DeployConfig>,
    pub runtime_config_apply: Arc<dyn PolicySnapshotPort>,
    pub committed_policy_apply: Arc<dyn CommittedPolicyApplyPort>,
    pub jwt: Arc<JwtService>,
    pub jwt_blacklist: Arc<RedisTokenBlacklist>,
    pub users: Arc<dyn UserRepository>,
    pub roles: Arc<dyn RoleRepository>,
    pub menus: Arc<dyn MenuRepository>,
    pub user_roles: Arc<dyn UserRoleRepository>,
    pub role_menus: Arc<dyn RoleMenuRepository>,
    pub role_permissions: Arc<dyn RolePermissionRepository>,
    pub casbin: Arc<CasbinService>,
    pub perm_checker: Arc<PermChecker>,
    pub runtime_config: Arc<dyn PolicyRepository>,
    pub operation_logs: Arc<dyn OperationLogRepository>,
    pub operation_log: OperationLogBuffer,
    pub control: Arc<dyn RuntimeControlPort>,
    pub capabilities: Arc<dyn SystemCapabilityPort>,
    /// Operational kill-switch governed read/write surface.
    pub kill_switch: Arc<dyn KillSwitchPort>,
    pub market_data: Arc<dyn MarketDataPort>,
    pub catalog: Arc<dyn CatalogStatusPort>,
    pub data_quality: Arc<dyn DataQualityPort>,
    pub events: CoreEventPublisher,
    pub markets: Arc<dyn MarketRepository>,
    /// Historical `ClickHouse` fact read port for market-detail charts
    /// (microstructure series + last-trade prints).
    pub quant_facts: Arc<dyn QuantFactReadRepository>,
    pub ws_sessions: SessionRegistry,
    pub metrics: Arc<dyn MetricsScrapePort>,
    pub readiness: Arc<dyn ReadinessPort>,
    /// Offline training-dataset plan/build API.
    pub training_datasets: Arc<dyn TrainingDatasetPort>,
    /// Offline model training API.
    pub model_training: Arc<dyn ModelTrainingPort>,
    /// Offline PIT backtest API.
    pub backtests: Arc<dyn BacktestPort>,
    /// CPCV and governed trial-grid validation API.
    pub cpcv_backtests: Arc<dyn CpcvBacktestPort>,
    /// Model publish and rollback governance API.
    pub model_governance: Arc<dyn ModelGovernancePort>,
    /// Model-spec authoring — the offline research lifecycle root write path.
    pub model_spec: Arc<dyn ModelSpecPort>,
    /// Read-only research catalog paging (datasets / models / backtests /
    /// comparisons / factors) for the operator workbench.
    pub research_catalog: Arc<dyn ResearchCatalogPort>,
    /// Durable async research-job engine (dataset build / model train / backtest
    /// / bias-table fit): enqueue + task-center list/get/cancel/retry.
    pub research_jobs: Arc<dyn ResearchJobPort>,
    /// Verified operational evidence for the research-readiness dashboard gate.
    pub research_readiness: Arc<dyn ResearchReadinessPort>,
    /// Feedback overview, cycle catalog, and immutable evidence detail.
    pub feedback_read: Arc<dyn FeedbackReadPort>,
    /// Durable feedback revision claim/replay owner used by the WebSocket adapter.
    pub feedback_outbox: Arc<dyn FeedbackOutboxRepository>,
    /// Governed manual-cycle and promotion-permit mutations.
    pub feedback_mutation: Arc<dyn FeedbackMutationPort>,
    /// Deterministic feature replay evidence and governed parity latch.
    pub feature_integrity: Arc<dyn FeatureIntegrityPort>,
    /// Favorite-longshot bias-table fit enqueue plus unified calibration-artifact
    /// read/activate operations for every artifact kind.
    pub calibration_artifacts: Arc<dyn CalibrationArtifactFitPort>,
    /// Model-score `ProbabilityCalibrator` fit enqueue.
    pub model_calibration_fit: Arc<dyn ModelCalibrationFitPort>,
    /// Governed entry/exit policy artifact fit, catalog, and publication surface.
    pub trade_policies: Arc<dyn TradePolicyPort>,
    /// Market → external-subject linkage ledger.
    pub market_linkages: Arc<dyn MarketLinkageRepository>,
    /// Domain-source ingest cursor health.
    pub domain_source_cursors: Arc<dyn DomainSourceCursorRepository>,
    /// Capability-declared source bindings, including pre-cursor blockers.
    pub domain_source_expectations: Arc<dyn DomainSourceExpectationRepository>,
    /// Basis-cross-check exceedance alert feed.
    pub basis_alerts: Arc<dyn BasisAlertRepository>,
    /// Offline market-linkage resolver.
    pub linkage_governance: Arc<dyn MarketLinkageGovernancePort>,
    /// Live neg-risk structural-drift monitor.
    pub structural_monitor: Arc<dyn StructuralMonitorPort>,
    /// Recommendation report read and governed mutation API.
    pub quant_reports: Arc<dyn QuantReportPort>,
    /// Venue account live + snapshot read surface.
    pub account_read: Arc<dyn AccountReadPort>,
    /// Order-intent read and governed mutation API.
    pub order_intents: Arc<dyn OrderIntentPort>,
    /// Recommendation-owned condition state and WORM audit timeline.
    pub entry_conditions: Arc<dyn EntryConditionRepository>,
    /// Execution-order and position read API.
    pub execution_read: Arc<dyn ExecutionReadPort>,
    /// Live settlement deployment truth and governed authorization mutations.
    pub settlement_control: Arc<dyn SettlementControlPort>,
    /// Operator reconciliation resolution API.
    pub reconciliation: Arc<dyn ReconciliationPort>,
    /// Execution recovery playbook detail API.
    pub execution_recovery: Arc<dyn ExecutionRecoveryPort>,
}

impl AppState {
    /// Fan out a `materialization.run_update` revision hint so open research catalogs re-fetch.
    pub fn publish_materialization_run(
        &self,
        run_id: impl Into<String>,
        kind: MaterializationRunKind,
        status: MaterializationRunStatus,
    ) {
        self.events.publish(CoreEvent::MaterializationRun(
            MaterializationRunEvent::revision(run_id, kind, status),
        ));
    }
}
