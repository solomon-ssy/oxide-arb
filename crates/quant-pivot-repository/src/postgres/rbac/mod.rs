//! Postgres implementations of the RBAC repositories plus the Casbin adapter.
//!
//! Assignment repositories (`user_role`, `role_menu`, `role_permission`) keep
//! their join tables and the backing Casbin `g` / `p` rows atomically
//! consistent inside a single transaction. Reloading the live enforcer is left
//! to the service layer.

pub mod casbin;
pub mod junction;
pub mod menu;
pub mod role;
pub mod role_menu;
pub mod role_permission;
pub mod user;
pub mod user_role;

pub(crate) mod util;

pub use casbin::PgCasbinAdapter;
pub use menu::PgMenuRepository;
pub use role::PgRoleRepository;
pub use role_menu::PgRoleMenuRepository;
pub use role_permission::PgRolePermissionRepository;
pub use user::PgUserRepository;
pub use user_role::PgUserRoleRepository;
