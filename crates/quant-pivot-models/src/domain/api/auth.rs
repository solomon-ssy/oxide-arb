//! Authentication API contract (login, refresh, logout, `/me`).

use crate::{
    domain::{MenuTreeNode, RoleInfo, UserInfo},
    enums::rbac::UserStatus,
    types::{RoleId, UserId},
};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use validator::Validate;

/// Login credentials.
#[derive(Debug, Deserialize, Validate)]
pub struct LoginRequest {
    /// Account username.
    #[validate(length(min = 1, max = 128))]
    pub username: String,
    /// Plaintext password (verified against the stored argon2id hash).
    #[validate(length(min = 1, max = 256))]
    pub password: String,
}

/// Refresh-token rotation request.
#[derive(Debug, Deserialize, Validate)]
pub struct RefreshRequest {
    /// The refresh token to exchange for a fresh access/refresh pair.
    #[validate(length(min = 1))]
    pub refresh_token: String,
}

/// Optional logout payload carrying the refresh token to revoke alongside the
/// access token.
#[derive(Debug, Deserialize)]
pub struct LogoutRequest {
    /// Refresh token to revoke (optional; the access token is always revoked).
    pub refresh_token: Option<String>,
}

/// Token pair issued on login/refresh.
#[derive(Debug, Serialize)]
pub struct TokenResponse {
    /// Short-lived access token.
    pub access_token: String,
    /// Long-lived refresh token.
    pub refresh_token: String,
    /// Always `"Bearer"`.
    pub token_type: &'static str,
    /// Access-token lifetime in seconds.
    pub expires_in: i64,
}

/// Credential-free projection of a user account.
#[derive(Debug, Serialize)]
pub struct UserView {
    /// Stable user id.
    pub id: UserId,
    /// Username.
    pub username: String,
    /// Display name.
    pub nickname: String,
    /// Optional avatar URL.
    pub avatar: Option<String>,
    /// Optional email.
    pub email: Option<String>,
    /// Optional phone.
    pub phone: Option<String>,
    /// Account status.
    pub status: UserStatus,
    /// Creation timestamp.
    pub created_at: DateTime<Utc>,
    /// Last update timestamp.
    pub updated_at: DateTime<Utc>,
}

impl From<UserInfo> for UserView {
    fn from(user: UserInfo) -> Self {
        Self {
            id: user.id,
            username: user.username,
            nickname: user.nickname,
            avatar: user.avatar,
            email: user.email,
            phone: user.phone,
            status: user.status,
            created_at: user.created_at,
            updated_at: user.updated_at,
        }
    }
}

/// Compact role projection for the `/me` response.
#[derive(Debug, Serialize)]
pub struct RoleView {
    /// Role id.
    pub id: RoleId,
    /// Role code (Casbin subject).
    pub code: String,
    /// Display name.
    pub name: String,
    /// Optional description.
    pub description: Option<String>,
}

impl From<&RoleInfo> for RoleView {
    fn from(role: &RoleInfo) -> Self {
        Self {
            id: role.id.clone(),
            code: role.code.clone(),
            name: role.name.clone(),
            description: role.description.clone(),
        }
    }
}

/// `/me` projection: the current user, their roles, and accessible menus.
#[derive(Debug, Serialize)]
pub struct MeResponse {
    /// The authenticated user.
    pub user: UserView,
    /// The user's roles.
    pub roles: Vec<RoleView>,
    /// The menu tree the user's roles grant access to.
    pub menus: Vec<MenuTreeNode>,
}
