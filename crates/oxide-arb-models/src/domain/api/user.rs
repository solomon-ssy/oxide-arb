//! User management API contract.

use crate::{
    domain::{
        UserPatch,
        patch::{NullablePatch, Patch},
    },
    enums::rbac::UserStatus,
    types::RoleId,
};
use serde::Deserialize;
use validator::Validate;

use super::serde::double_option;

/// Create-user payload.
#[derive(Debug, Deserialize, Validate)]
pub struct CreateUserRequest {
    #[validate(length(min = 1, max = 128))]
    pub username: String,
    #[validate(length(min = 8, max = 256))]
    pub password: String,
    #[validate(length(min = 1, max = 128))]
    pub nickname: String,
    #[validate(length(max = 512))]
    pub avatar: Option<String>,
    #[validate(email)]
    pub email: Option<String>,
    #[validate(length(max = 64))]
    pub phone: Option<String>,
    /// Initial status; defaults to active when omitted.
    #[serde(default)]
    pub status: Option<UserStatus>,
}

/// Partial update of a user's profile. Absent fields are left unchanged; an
/// explicit `null` clears a nullable field.
#[derive(Debug, Deserialize, Validate)]
pub struct UpdateUserRequest {
    #[validate(length(min = 1, max = 128))]
    pub nickname: Option<String>,
    #[serde(default, deserialize_with = "double_option")]
    pub avatar: Option<Option<String>>,
    #[serde(default, deserialize_with = "double_option")]
    pub email: Option<Option<String>>,
    #[serde(default, deserialize_with = "double_option")]
    pub phone: Option<Option<String>>,
}

impl From<UpdateUserRequest> for UserPatch {
    fn from(request: UpdateUserRequest) -> Self {
        Self {
            nickname: Patch::from_option(request.nickname),
            avatar: NullablePatch::from_nested_option(request.avatar),
            email: NullablePatch::from_nested_option(request.email),
            phone: NullablePatch::from_nested_option(request.phone),
        }
    }
}

/// Status-transition payload.
#[derive(Debug, Deserialize, Validate)]
pub struct ChangeUserStatusRequest {
    pub status: UserStatus,
}

/// Password-reset payload (the new plaintext, hashed server-side).
#[derive(Debug, Deserialize, Validate)]
pub struct ChangePasswordRequest {
    #[validate(length(min = 8, max = 256))]
    pub password: String,
}

/// Role-assignment payload (replace-set: the given roles become the user's
/// complete role set).
#[derive(Debug, Deserialize, Validate)]
pub struct AssignRolesRequest {
    pub role_ids: Vec<RoleId>,
}
