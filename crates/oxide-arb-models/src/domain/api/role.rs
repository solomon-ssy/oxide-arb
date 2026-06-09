//! Role management API contract.

use crate::{
    domain::{
        Permission, RolePatch,
        patch::{NullablePatch, Patch},
    },
    enums::rbac::RoleStatus,
    types::MenuId,
};
use serde::Deserialize;
use validator::Validate;

use super::serde::double_option;

/// Create-role payload (`kind` is fixed to `Custom`, `status` to `Enabled`).
#[derive(Debug, Deserialize, Validate)]
pub struct CreateRoleRequest {
    #[validate(length(min = 1, max = 64))]
    pub code: String,
    #[validate(length(min = 1, max = 128))]
    pub name: String,
    #[validate(length(max = 512))]
    pub description: Option<String>,
    #[serde(default)]
    pub sort: i32,
}

/// Partial role update (`code` and `kind` are immutable; status flows through
/// the dedicated status endpoint).
#[derive(Debug, Deserialize, Validate)]
pub struct UpdateRoleRequest {
    #[validate(length(min = 1, max = 128))]
    pub name: Option<String>,
    #[serde(default, deserialize_with = "double_option")]
    pub description: Option<Option<String>>,
    pub sort: Option<i32>,
}

impl From<UpdateRoleRequest> for RolePatch {
    fn from(request: UpdateRoleRequest) -> Self {
        Self {
            name: Patch::from_option(request.name),
            description: NullablePatch::from_nested_option(request.description),
            sort: Patch::from_option(request.sort),
        }
    }
}

/// Status-transition payload.
#[derive(Debug, Deserialize, Validate)]
pub struct ChangeRoleStatusRequest {
    pub status: RoleStatus,
}

/// Permission-assignment payload (replace-set).
#[derive(Debug, Deserialize, Validate)]
pub struct AssignPermissionsRequest {
    pub permissions: Vec<Permission>,
}

/// Menu-assignment payload (replace-set).
#[derive(Debug, Deserialize, Validate)]
pub struct AssignMenusRequest {
    pub menu_ids: Vec<MenuId>,
}
