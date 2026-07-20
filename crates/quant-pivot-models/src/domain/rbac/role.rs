//! Role DTOs (read model, insert, partial update).

use crate::{
    domain::patch::{NullablePatch, Patch},
    enums::rbac::{RoleKind, RoleStatus},
    types::{RoleCode, RoleId},
};
use chrono::{DateTime, Utc};
use sea_orm::{DeriveIntoActiveModel, DerivePartialModel};
use serde::{Deserialize, Serialize};

/// DB row projection for the `role` table.
#[derive(Debug, Clone, Serialize, Deserialize, DerivePartialModel)]
#[sea_orm(entity = "crate::entities::role::Entity")]
pub struct RoleInfo {
    pub id: RoleId,
    pub code: RoleCode,
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
    pub code: RoleCode,
    pub name: String,
    pub description: Option<String>,
    pub kind: RoleKind,
    pub status: RoleStatus,
    pub sort: i32,
}

/// Partial update for a role's descriptive attributes.
///
/// `code` and `kind` are immutable, and `status` is intentionally **absent**:
/// enabling/disabling a role transitions Casbin grouping (`g`) bindings and
/// therefore must flow exclusively through [`RoleRepository::change_status`],
/// never a generic column update that would silently bypass the policy sync.
///
/// [`RoleRepository::change_status`]: ../../../quant_pivot_repository/traits/trait.RoleRepository.html
#[derive(Debug, Clone, Default, DeriveIntoActiveModel)]
#[sea_orm(active_model = "crate::entities::role::ActiveModel")]
pub struct RolePatch {
    pub name: Patch<String>,
    pub description: NullablePatch<String>,
    pub sort: Patch<i32>,
}
