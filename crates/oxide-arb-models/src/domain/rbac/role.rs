//! Role DTOs (read model, insert, partial update).

use crate::{
    domain::patch::{NullablePatch, Patch},
    enums::rbac::{RoleKind, RoleStatus},
    types::RoleId,
};
use chrono::{DateTime, Utc};
use sea_orm::{DeriveIntoActiveModel, DerivePartialModel, FromQueryResult};
use serde::{Deserialize, Serialize};

/// DB row projection for the `role` table.
#[derive(Debug, Clone, Serialize, Deserialize, DerivePartialModel, FromQueryResult)]
#[sea_orm(entity = "crate::entities::role::Entity")]
pub struct RoleInfo {
    pub id: RoleId,
    pub code: String,
    pub name: String,
    pub description: Option<String>,
    pub kind: RoleKind,
    pub status: RoleStatus,
    pub sort: i32,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

info_from_model!(RoleInfo, crate::entities::role::Model, {
    id, code, name, description, kind, status, sort, created_at, updated_at,
});

/// Insert payload for a new role. `code` is the immutable Casbin subject.
#[derive(Debug, Clone, DeriveIntoActiveModel)]
#[sea_orm(active_model = "crate::entities::role::ActiveModel")]
pub struct NewRole {
    pub id: RoleId,
    pub code: String,
    pub name: String,
    pub description: Option<String>,
    pub kind: RoleKind,
    pub status: RoleStatus,
    pub sort: i32,
}

/// Partial update for a role (its `code` and `kind` are immutable).
#[derive(Debug, Clone, Default, DeriveIntoActiveModel)]
#[sea_orm(active_model = "crate::entities::role::ActiveModel")]
pub struct RolePatch {
    pub name: Patch<String>,
    pub description: NullablePatch<String>,
    pub status: Patch<RoleStatus>,
    pub sort: Patch<i32>,
}
