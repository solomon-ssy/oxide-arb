//! Shared application state injected into every request.
//!
//! `AppState` is cheap to clone (every field is an [`Arc`]) and is registered
//! once as actix `web::Data`. It bundles the authentication service, the RBAC
//! repositories, the live Casbin enforcer, and the route-level permission
//! registry. Later sub-phases extend it further (control-plane registry,
//! business repositories, WebSocket broadcaster).

use std::sync::Arc;

use oxide_arb_repository::traits::{
    MenuRepository, RoleMenuRepository, RolePermissionRepository, RoleRepository, UserRepository,
    UserRoleRepository,
};

use crate::{
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
}
