//! User account DTOs (read model, insert, partial update, credential changes).

use crate::{
    domain::{
        pagination::PageRequest,
        patch::{NullablePatch, Patch},
    },
    enums::rbac::UserStatus,
    types::UserId,
};
use chrono::{DateTime, Utc};
use sea_orm::{DeriveIntoActiveModel, DerivePartialModel, FromQueryResult};
use serde::{Deserialize, Serialize};

/// DB row projection for the `user` table.
///
/// Carries `password_hash` because the repository login path needs it; the web
/// layer projects this into a response type that omits the credential.
#[derive(Debug, Clone, Serialize, Deserialize, DerivePartialModel, FromQueryResult)]
#[sea_orm(entity = "crate::entities::user::Entity")]
pub struct UserInfo {
    pub id: UserId,
    pub username: String,
    pub password_hash: String,
    pub nickname: String,
    pub avatar: Option<String>,
    pub email: Option<String>,
    pub phone: Option<String>,
    pub status: UserStatus,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

info_from_model!(UserInfo, crate::entities::user::Model, {
    id, username, password_hash, nickname, avatar, email, phone, status,
    created_at, updated_at,
});

/// Insert payload for a new user. The caller assigns `id` and provides the
/// already-hashed `password_hash`.
#[derive(Debug, Clone, DeriveIntoActiveModel)]
#[sea_orm(active_model = "crate::entities::user::ActiveModel")]
pub struct NewUser {
    pub id: UserId,
    pub username: String,
    pub password_hash: String,
    pub nickname: String,
    pub avatar: Option<String>,
    pub email: Option<String>,
    pub phone: Option<String>,
    pub status: UserStatus,
}

/// Partial update for a user's profile attributes.
///
/// Neither the password nor the account `status` is mutable here: credentials
/// flow through [`ChangeUserPassword`] and activation/deactivation through
/// [`UserRepository::change_status`], keeping every sensitive transition on its
/// own audited, single-purpose path.
///
/// [`UserRepository::change_status`]: ../../../oxide_arb_repository/traits/trait.UserRepository.html
#[derive(Debug, Clone, Default, DeriveIntoActiveModel)]
#[sea_orm(active_model = "crate::entities::user::ActiveModel")]
pub struct UserPatch {
    pub nickname: Patch<String>,
    pub avatar: NullablePatch<String>,
    pub email: NullablePatch<String>,
    pub phone: NullablePatch<String>,
}

/// Dedicated credential change. `password_hash` is already argon2id-hashed by
/// the caller; plaintext never reaches the repository.
#[derive(Debug, Clone)]
pub struct ChangeUserPassword {
    pub password_hash: String,
}

/// Pagination + filter parameters for listing users.
///
/// `keyword` is a case-insensitive substring match against `username` and
/// `nickname`. The pagination window is the shared [`PageRequest`], flattened
/// so the query string stays flat (`?keyword=&status=&page=&size=`).
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct UserPageQuery {
    pub keyword: Option<String>,
    pub status: Option<UserStatus>,
    #[serde(flatten)]
    pub page: PageRequest,
}

impl UserPageQuery {
    /// Return a copy with the embedded pagination window normalized.
    #[must_use]
    pub fn normalized(self) -> Self {
        Self {
            page: self.page.normalized(),
            ..self
        }
    }
}
