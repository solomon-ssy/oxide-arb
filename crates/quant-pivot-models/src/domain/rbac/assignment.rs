//! Assignment DTOs: user→roles, role→menus, role→permissions.

use serde::{Deserialize, Serialize};

use crate::{
    enums::rbac::{Operation, ResourceType},
    types::{MenuId, RoleId, UserId},
};

/// A single `resource × operation` grant — the typed form of a Casbin `p` line's
/// `(obj, act)` pair.
///
/// Replaces the bare `(ResourceType, Operation)` tuple so the permission space
/// is self-describing at every boundary (assignment payloads, repository
/// returns, audit envelopes).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct Permission {
    pub resource: ResourceType,
    pub operation: Operation,
}

impl Permission {
    /// Construct a permission from its resource and operation.
    #[must_use]
    pub const fn new(resource: ResourceType, operation: Operation) -> Self {
        Self {
            resource,
            operation,
        }
    }

    /// Whether this pair exists in the canonical permission catalog
    /// (`RESOURCE_OPERATIONS`) — i.e. is a real, assignable permission.
    #[must_use]
    pub fn is_valid(self) -> bool {
        self.resource.allows(self.operation)
    }
}

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
/// Keyed by `role_id` for symmetry with the other assignment DTOs and the
/// `/roles/{id}/permissions` route; the repository resolves the immutable
/// `role_code` (the Casbin subject) inside its transaction. Permissions are
/// validated against `RESOURCE_OPERATIONS` before being applied so no phantom
/// resource×operation combinations can be persisted.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AssignPermissions {
    pub role_id: RoleId,
    pub permissions: Vec<Permission>,
}
