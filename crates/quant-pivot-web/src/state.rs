//! Shared application state injected into every request (Phase 0).

use std::sync::Arc;

use quant_pivot_models::{
    config::DeployConfig,
    domain::{
        BacktestPort, CatalogStatusPort, CoreEventPublisher, DataQualityPort, KillSwitchPort,
        MarketDataPort, MetricsScrapePort, ModelGovernancePort, ModelTrainingPort, QuantReportPort,
        ReadinessPort, RuntimeConfigPort, RuntimeControlPort, TrainingDatasetPort,
    },
};
use quant_pivot_repository::traits::{
    MarketRepository, MenuRepository, OperationLogRepository, RoleMenuRepository,
    RolePermissionRepository, RoleRepository, RuntimeConfigVersionRepository, UserRepository,
    UserRoleRepository,
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
    /// Recommendation report read + governed mutation (Phase 04.4 API).
    pub quant_reports: Arc<dyn QuantReportPort>,
}
