//! Graph-ordered RBAC bootstrap seeds.
//!
//! These seeds form a dependency DAG: roles and menus first, then the admin
//! user, then the join tables and the Casbin policy set. IDs created upstream
//! are passed downstream through [`SeedContext`]
//! artifacts; idempotency is provided by the seed ledger plus per-loader
//! `ON CONFLICT DO NOTHING`.
//!
//! [`SeedContext`]: crate::seed::SeedContext

pub mod admin_user;
pub mod casbin;
pub mod menus;
pub mod role_menu;
pub mod roles;
pub mod user_role;

use std::collections::HashMap;

use crate::{seed::SeedArtifactKey, types::RoleId};

/// Bootstrap admin username. Changeable post-deploy via the UI/API.
pub const DEFAULT_ADMIN_USERNAME: &str = "admin";
/// Default bootstrap admin display name.
pub const DEFAULT_ADMIN_NICKNAME: &str = "Administrator";

/// Built-in role code: unrestricted access (Casbin matcher bypass; needs no `p`).
pub const ROLE_SUPER_ADMIN: &str = "super_admin";
/// Built-in role code: platform + RBAC administration.
pub const ROLE_ADMIN: &str = "admin";
/// Built-in role code: governance / money-risk approval authority.
pub const ROLE_RISK_OWNER: &str = "risk_owner";
/// Built-in role code: quant analyst — read-only plus ad-hoc report generation.
pub const ROLE_ANALYST: &str = "analyst";
/// Built-in role code: day-to-day operational controls.
pub const ROLE_OPERATOR: &str = "operator";
/// Built-in role code: read-only access.
pub const ROLE_VIEWER: &str = "viewer";
/// Built-in role code: break-glass emergency controls.
pub const ROLE_EMERGENCY_OPERATOR: &str = "emergency_operator";

/// Seed artifact: `role code -> RoleId` map produced by the roles seed.
pub const ROLES_ARTIFACT: SeedArtifactKey = SeedArtifactKey::new("rbac.roles");
/// Seed artifact: list of all seeded menu IDs produced by the menus seed.
pub const MENUS_ARTIFACT: SeedArtifactKey = SeedArtifactKey::new("rbac.menus");
/// Seed artifact: menu nodes for role-menu assignment (id, kind, permission).
pub const MENU_GRANTS_ARTIFACT: SeedArtifactKey = SeedArtifactKey::new("rbac.menu_grants");
/// Seed artifact: bootstrap admin `UserId` produced by the admin-user seed.
pub const ADMIN_USER_ARTIFACT: SeedArtifactKey = SeedArtifactKey::new("rbac.admin_user");
/// Deploy-only input: Argon2id PHC hash for the bootstrap administrator.
pub const BOOTSTRAP_ADMIN_PASSWORD_HASH_INPUT: SeedArtifactKey =
    SeedArtifactKey::new("rbac.bootstrap_admin_password_hash");

/// Map of built-in role code to its freshly-assigned [`RoleId`].
pub type RoleIdMap = HashMap<&'static str, RoleId>;
