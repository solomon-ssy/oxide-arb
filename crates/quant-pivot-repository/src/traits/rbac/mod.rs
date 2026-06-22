//! RBAC repository traits: users, roles, menus, and the three assignment
//! surfaces (user→role, role→menu, role→permission).
//!
//! Assignment repositories use **replace-set** semantics: the caller submits the
//! complete desired set and the implementation diffs it against the current
//! state inside a single transaction, keeping the join tables and the backing
//! Casbin `g` / `p` rows atomically consistent. Reloading the live enforcer is
//! the responsibility of the service layer.

pub mod menu;
pub mod role;
pub mod role_menu;
pub mod role_permission;
pub mod user;
pub mod user_role;

pub use menu::MenuRepository;
pub use role::RoleRepository;
pub use role_menu::RoleMenuRepository;
pub use role_permission::RolePermissionRepository;
pub use user::UserRepository;
pub use user_role::UserRoleRepository;
