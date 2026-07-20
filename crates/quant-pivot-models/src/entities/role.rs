//! `role` table entity (RBAC roles).

use crate::{
    enums::rbac::{RoleKind, RoleStatus},
    types::{RoleCode, RoleId},
};
use chrono::{DateTime, Utc};
use sea_orm::entity::prelude::*;
use serde::{Deserialize, Serialize};

#[sea_orm::model]
#[derive(Clone, Debug, PartialEq, Eq, DeriveEntityModel, Serialize, Deserialize)]
#[sea_orm(table_name = "role")]
pub struct Model {
    #[sea_orm(primary_key, auto_increment = false)]
    pub id: RoleId,
    #[sea_orm(column_type = "Text")]
    pub code: RoleCode,
    #[sea_orm(column_type = "Text")]
    pub name: String,
    #[sea_orm(column_type = "Text", nullable)]
    pub description: Option<String>,
    pub kind: RoleKind,
    pub status: RoleStatus,
    pub sort: i32,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

impl ActiveModelBehavior for ActiveModel {}
