//! Assignment DTOs: user→roles, role→menus, role→permissions.

use crate::{
    enums::rbac::{Operation, ResourceType},
    types::{MenuId, RoleId, UserId},
};
use serde::{Deserialize, Serialize};

/// Replace the set of roles assigned to a user (writes `user_role` + Casbin `g`).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AssignRoles {
    pub user_id: UserId,
    pub role_ids: Vec<RoleId>,
}

/// Replace the set of menus visible to a role (writes `role_menu`).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AssignMenus {
    pub role_id: RoleId,
    pub menu_ids: Vec<MenuId>,
}

/// Replace the permission set of a role (writes Casbin `p`).
///
/// Permissions are validated against `RESOURCE_OPERATIONS` before being applied
/// so no phantom resource×operation combinations can be persisted.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AssignPermissions {
    pub role_code: String,
    pub permissions: Vec<(ResourceType, Operation)>,
}
