//! User account DTOs (read model, insert, partial update, credential changes).

use crate::{
    domain::patch::{NullablePatch, Patch},
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

/// Partial update for a user's profile and status (never the password).
#[derive(Debug, Clone, Default, DeriveIntoActiveModel)]
#[sea_orm(active_model = "crate::entities::user::ActiveModel")]
pub struct UserPatch {
    pub nickname: Patch<String>,
    pub avatar: NullablePatch<String>,
    pub email: NullablePatch<String>,
    pub phone: NullablePatch<String>,
    pub status: Patch<UserStatus>,
}

/// Dedicated credential change. `password_hash` is already argon2id-hashed by
/// the caller; plaintext never reaches the repository.
#[derive(Debug, Clone)]
pub struct ChangeUserPassword {
    pub password_hash: String,
}
