//! Shared application state injected into every request.
//!
//! `AppState` is cheap to clone (every field is an [`Arc`] or a cloneable
//! handle) and is registered once as actix `web::Data`. It bundles the
//! authentication service, the RBAC repositories, the live Casbin enforcer, the
//! route-level permission registry, the governance control-plane (registry +
//! read repositories), and the operation-log buffer. Later sub-phases extend it
//! further (business repositories, WebSocket broadcaster).

use std::sync::Arc;

use oxide_arb_control::governance::ControlFactorRegistry;
use oxide_arb_repository::traits::{
    ControlFactorRepository, ControlFactorShadowDecisionRepository, MenuRepository,
    OperationLogRepository, RoleMenuRepository, RolePermissionRepository, RoleRepository,
    RuntimeConfigVersionRepository, UserRepository, UserRoleRepository,
};

use crate::{
    audit::OperationLogBuffer,
    auth::casbin::{CasbinService, PermChecker},
    jwt::JwtService,
};

/// Dependency bundle shared by all handlers and middleware.
#[derive(Clone)]
pub struct AppState {
    /// JWT signer/validator with its revocation blacklist.
    pub jwt: Arc<JwtService>,
    /// User account access (login, profile, CRUD).
    pub users: Arc<dyn UserRepository>,
    /// Role catalog access (CRUD, status transitions).
    pub roles: Arc<dyn RoleRepository>,
    /// Menu access (tree, accessibility, CRUD).
    pub menus: Arc<dyn MenuRepository>,
    /// User→role assignment (replace-set, per-request role resolution).
    pub user_roles: Arc<dyn UserRoleRepository>,
    /// Role→menu assignment.
    pub role_menus: Arc<dyn RoleMenuRepository>,
    /// Role→permission assignment (Casbin `p` projection).
    pub role_permissions: Arc<dyn RolePermissionRepository>,
    /// Live Casbin enforcer (read + reload).
    pub casbin: Arc<CasbinService>,
    /// Route-level authorization registry (fail-closed).
    pub perm_checker: Arc<PermChecker>,
    /// Governance control-plane: state-machine mutations that write the audit
    /// hash chain transactionally (publish / rollback / reject / runtime-config).
    pub registry: Arc<ControlFactorRegistry>,
    /// Read access to control-factor state, publications, and the audit chain.
    pub control_factors: Arc<dyn ControlFactorRepository>,
    /// Read access to immutable runtime-config versions and activations.
    pub runtime_config: Arc<dyn RuntimeConfigVersionRepository>,
    /// Read access to shadow-publication decision evidence.
    pub shadow_decisions: Arc<dyn ControlFactorShadowDecisionRepository>,
    /// Append-only operation log (paginated forensic queries).
    pub operation_logs: Arc<dyn OperationLogRepository>,
    /// Non-blocking producer handle for the operation-log writer pipeline.
    pub operation_log: OperationLogBuffer,
}
