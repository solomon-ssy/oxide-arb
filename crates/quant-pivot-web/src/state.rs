//! Shared application state injected into every request (Phase 0).

use std::sync::Arc;

use quant_pivot_models::{
    config::DeployConfig,
    domain::ExecutionRecoveryPort,
    domain::{
        AccountReadPort, BacktestPort, CatalogStatusPort, CoreEvent, CoreEventPublisher,
        DataQualityPort, ExecutionReadPort, ExecutionSubmitPort, FactorGovernancePort,
        KillSwitchPort, MarketDataPort, MaterializationRunEvent, MaterializationRunKind,
        MaterializationRunStatus, MetricsScrapePort, ModelGovernancePort, ModelTrainingPort,
        OrderIntentPort, QuantReportPort, ReadinessPort, ReconciliationPort, ResearchCatalogPort,
        ResearchJobPort, RuntimeConfigPort, RuntimeControlPort, TrainingDatasetPort,
    },
};
use quant_pivot_repository::traits::{
    MarketRepository, MenuRepository, OperationLogRepository, QuantFactReadRepository,
    RoleMenuRepository, RolePermissionRepository, RoleRepository, RuntimeConfigVersionRepository,
    UserRepository, UserRoleRepository,
};

use crate::{
    audit::OperationLogBuffer,
    auth::casbin::{CasbinService, PermChecker},
    jwt::{JwtService, RedisTokenBlacklist},
    ws::SessionRegistry,
};

/// Dependency bundle shared by all handlers and middleware.
#[derive(Clone)]
pub struct AppState {
    pub deploy: Arc<DeployConfig>,
    pub runtime_config_apply: Arc<dyn RuntimeConfigPort>,
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
    pub runtime_config: Arc<dyn RuntimeConfigVersionRepository>,
    pub operation_logs: Arc<dyn OperationLogRepository>,
    pub operation_log: OperationLogBuffer,
    pub control: Arc<dyn RuntimeControlPort>,
    /// Operational kill-switch governed read/write surface (05.1).
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
    /// Offline training-dataset plan/build (Phase 3.5 Admin API).
    pub training_datasets: Arc<dyn TrainingDatasetPort>,
    /// Offline model training (Phase 3.6 Admin API).
    pub model_training: Arc<dyn ModelTrainingPort>,
    /// Offline PIT backtests (Phase 3.6 Admin API).
    pub backtests: Arc<dyn BacktestPort>,
    /// Model publish / rollback governance (Phase 3.7 Admin API).
    pub model_governance: Arc<dyn ModelGovernancePort>,
    /// Factor-definition publish / retire governance (Phase 05.7 Admin API).
    pub factor_governance: Arc<dyn FactorGovernancePort>,
    /// Read-only research catalog paging (datasets / models / backtests /
    /// comparisons / factors) for the operator workbench (Phase 10.5).
    pub research_catalog: Arc<dyn ResearchCatalogPort>,
    /// Durable async research-job engine (dataset build / model train / backtest):
    /// enqueue + task-center list/get/cancel/retry.
    pub research_jobs: Arc<dyn ResearchJobPort>,
    /// Recommendation report read + governed mutation (Phase 04.4 API).
    pub quant_reports: Arc<dyn QuantReportPort>,
    /// Venue account live + snapshot read surface.
    pub account_read: Arc<dyn AccountReadPort>,
    /// Order-intent read + governed mutation (Phase 05.2 API).
    pub order_intents: Arc<dyn OrderIntentPort>,
    /// Execution order, position, and attribution read surface (Phase 05.7 API).
    pub execution_read: Arc<dyn ExecutionReadPort>,
    /// Entry-execution submission bridge (Phase 05.4 API): claim → admit → submit.
    pub execution_submit: Arc<dyn ExecutionSubmitPort>,
    /// Operator reconciliation resolve (Phase 05.5 closeout).
    pub reconciliation: Arc<dyn ReconciliationPort>,
    /// Execution recovery playbook detail (Phase 05.5 closeout).
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
