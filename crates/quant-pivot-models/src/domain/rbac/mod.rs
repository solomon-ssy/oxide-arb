//! RBAC domain DTOs (users, roles, menus, assignments).

mod assignment;
mod menu;
mod role;
mod user;

pub use assignment::{AssignMenus, AssignPermissions, AssignRoles, Permission};
pub use menu::{MenuInfo, MenuPatch, MenuTreeNode, NewMenu};
pub use role::{NewRole, RoleInfo, RolePatch};
pub use user::{ChangeUserPassword, NewUser, UserInfo, UserPatch};
