//! Postgres repository bundle for web integration tests.

use std::sync::Arc;

use sea_orm::DatabaseConnection;

use oxide_arb_repository::{
    pg_arc_repo,
    postgres::{
        PgControlFactorRepository, PgFactDataRepository, PgMarketRepository, PgMenuRepository,
        PgOperationLogRepository, PgPositionRepository, PgReportRepository, PgRiskAuditRepository,
        PgRoleMenuRepository, PgRolePermissionRepository, PgRoleRepository,
        PgRuntimeConfigVersionRepository, PgTradeRepository, PgUserRepository,
        PgUserRoleRepository,
    },
    traits::{
        ControlFactorRepository, ControlFactorShadowDecisionRepository, MarketRepository,
        MenuRepository, OperationLogRepository, PositionRepository, ReportRepository,
        RiskAuditRepository, RoleMenuRepository, RolePermissionRepository, RoleRepository,
        RuntimeConfigVersionRepository, TradeRepository, UserRepository, UserRoleRepository,
    },
};

/// All Postgres repositories required to assemble [`AppState`] in web integration tests.
pub struct WebHarnessRepos {
    pub control_factors: Arc<dyn ControlFactorRepository>,
    pub runtime_config: Arc<dyn RuntimeConfigVersionRepository>,
    pub shadow_decisions: Arc<dyn ControlFactorShadowDecisionRepository>,
    pub operation_logs: Arc<dyn OperationLogRepository>,
    pub users: Arc<dyn UserRepository>,
    pub roles: Arc<dyn RoleRepository>,
    pub menus: Arc<dyn MenuRepository>,
    pub user_roles: Arc<dyn UserRoleRepository>,
    pub role_menus: Arc<dyn RoleMenuRepository>,
    pub role_permissions: Arc<dyn RolePermissionRepository>,
    pub positions: Arc<dyn PositionRepository>,
    pub trades: Arc<dyn TradeRepository>,
    pub markets: Arc<dyn MarketRepository>,
    pub reports: Arc<dyn ReportRepository>,
    pub risk_audit: Arc<dyn RiskAuditRepository>,
}

impl WebHarnessRepos {
    /// Construct every web-layer repository over clones of `db`.
    pub fn from_connection(db: &DatabaseConnection) -> Self {
        Self {
            control_factors: pg_arc_repo!(db, PgControlFactorRepository),
            runtime_config: pg_arc_repo!(db, PgRuntimeConfigVersionRepository),
            shadow_decisions: pg_arc_repo!(db, PgFactDataRepository),
            operation_logs: pg_arc_repo!(db, PgOperationLogRepository),
            users: pg_arc_repo!(db, PgUserRepository),
            roles: pg_arc_repo!(db, PgRoleRepository),
            menus: pg_arc_repo!(db, PgMenuRepository),
            user_roles: pg_arc_repo!(db, PgUserRoleRepository),
            role_menus: pg_arc_repo!(db, PgRoleMenuRepository),
            role_permissions: pg_arc_repo!(db, PgRolePermissionRepository),
            positions: pg_arc_repo!(db, PgPositionRepository),
            trades: pg_arc_repo!(db, PgTradeRepository),
            markets: pg_arc_repo!(db, PgMarketRepository),
            reports: pg_arc_repo!(db, PgReportRepository),
            risk_audit: pg_arc_repo!(db, PgRiskAuditRepository),
        }
    }
}
