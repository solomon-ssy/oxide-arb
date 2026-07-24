//! Role management API contract.

use serde::Deserialize;
use serde_with::rust::double_option;
use validator::{Validate, ValidationError};

use crate::{
    domain::{
        patch::{NullablePatch, Patch},
        rbac::{Permission, RolePatch},
    },
    enums::rbac::RoleStatus,
    types::MenuId,
};

fn validate_role_code(code: &str) -> Result<(), ValidationError> {
    let mut bytes = code.bytes();
    if bytes.next().is_some_and(|byte| byte.is_ascii_lowercase())
        && bytes.all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'_')
    {
        Ok(())
    } else {
        Err(ValidationError::new("role_code"))
    }
}

/// Create-role payload (`kind` is fixed to `Custom`, `status` to `Enabled`).
#[derive(Debug, Deserialize, Validate)]
pub struct CreateRoleRequest {
    #[validate(length(min = 1, max = 64), custom(function = "validate_role_code"))]
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
    #[serde(default, with = "double_option")]
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

#[cfg(test)]
mod tests {
    use super::*;

    fn create_request(code: &str) -> CreateRoleRequest {
        CreateRoleRequest {
            code: code.to_owned(),
            name: "Risk reviewer".to_owned(),
            description: None,
            sort: 0,
        }
    }

    #[test]
    fn role_code_accepts_case() {
        assert!(create_request("risk_owner_2").validate().is_ok());
        for invalid in ["RiskOwner", "risk-owner", "risk owner", "_risk_owner", ""] {
            assert!(
                create_request(invalid).validate().is_err(),
                "role code `{invalid}` must be rejected"
            );
        }
    }
}
