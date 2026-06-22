//! Postgres repository bundle for web integration tests.

use std::sync::Arc;

use sea_orm::DatabaseConnection;

use quant_pivot_repository::{
    pg_arc_repo,
    postgres::{
        PgMarketRepository, PgMenuRepository, PgOperationLogRepository, PgRoleMenuRepository,
        PgRolePermissionRepository, PgRoleRepository, PgRuntimeConfigVersionRepository,
        PgUserRepository, PgUserRoleRepository,
    },
    traits::{
        MarketRepository, MenuRepository, OperationLogRepository, RoleMenuRepository,
        RolePermissionRepository, RoleRepository, RuntimeConfigVersionRepository, UserRepository,
        UserRoleRepository,
    },
};

/// Postgres repositories wired into the Phase 0 web integration harness.
pub struct WebHarnessRepos {
    pub runtime_config: Arc<dyn RuntimeConfigVersionRepository>,
    pub operation_logs: Arc<dyn OperationLogRepository>,
    pub users: Arc<dyn UserRepository>,
    pub roles: Arc<dyn RoleRepository>,
    pub menus: Arc<dyn MenuRepository>,
    pub user_roles: Arc<dyn UserRoleRepository>,
    pub role_menus: Arc<dyn RoleMenuRepository>,
    pub role_permissions: Arc<dyn RolePermissionRepository>,
    pub markets: Arc<dyn MarketRepository>,
}

impl WebHarnessRepos {
    pub fn from_connection(db: &DatabaseConnection) -> Self {
        Self {
            runtime_config: pg_arc_repo!(db, PgRuntimeConfigVersionRepository),
            operation_logs: pg_arc_repo!(db, PgOperationLogRepository),
            users: pg_arc_repo!(db, PgUserRepository),
            roles: pg_arc_repo!(db, PgRoleRepository),
            menus: pg_arc_repo!(db, PgMenuRepository),
            user_roles: pg_arc_repo!(db, PgUserRoleRepository),
            role_menus: pg_arc_repo!(db, PgRoleMenuRepository),
            role_permissions: pg_arc_repo!(db, PgRolePermissionRepository),
            markets: pg_arc_repo!(db, PgMarketRepository),
        }
    }
}
