//! Shared application state injected into every request.
//!
//! `AppState` is cheap to clone (every field is an [`Arc`]) and is registered
//! once as actix `web::Data`. This sub-phase carries only the dependencies the
//! authentication surface needs; later sub-phases extend it (Casbin enforcer,
//! control-plane registry, business repositories, WebSocket broadcaster).

use std::sync::Arc;

use oxide_arb_repository::traits::{MenuRepository, UserRepository, UserRoleRepository};

use crate::jwt::JwtService;

/// Dependency bundle shared by all handlers and middleware.
#[derive(Clone)]
pub struct AppState {
    /// JWT signer/validator with its revocation blacklist.
    pub jwt: Arc<JwtService>,
    /// User account access (login + profile projection).
    pub users: Arc<dyn UserRepository>,
    /// Per-request role resolution (authn loads roles fresh each request).
    pub user_roles: Arc<dyn UserRoleRepository>,
    /// Menu accessibility for the `/me` projection.
    pub menus: Arc<dyn MenuRepository>,
}

impl AppState {
    /// Assemble the state from its constituent dependencies.
    #[must_use]
    pub fn new(
        jwt: Arc<JwtService>,
        users: Arc<dyn UserRepository>,
        user_roles: Arc<dyn UserRoleRepository>,
        menus: Arc<dyn MenuRepository>,
    ) -> Self {
        Self {
            jwt,
            users,
            user_roles,
            menus,
        }
    }
}
