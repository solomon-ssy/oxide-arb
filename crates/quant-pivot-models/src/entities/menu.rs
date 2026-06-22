//! `menu` table entity (navigation tree + permission points).

use crate::{
    enums::rbac::{MenuKind, RoleStatus},
    types::MenuId,
};
use chrono::{DateTime, Utc};
use sea_orm::entity::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, Eq, DeriveEntityModel, Serialize, Deserialize)]
#[sea_orm(table_name = "menu")]
pub struct Model {
    #[sea_orm(primary_key, auto_increment = false)]
    pub id: MenuId,
    pub parent_id: Option<MenuId>,
    #[sea_orm(column_type = "Text")]
    pub name: String,
    pub kind: MenuKind,
    #[sea_orm(column_type = "Text", nullable)]
    pub path: Option<String>,
    #[sea_orm(column_type = "Text", nullable)]
    pub component: Option<String>,
    #[sea_orm(column_type = "Text")]
    pub title: String,
    #[sea_orm(column_type = "Text", nullable)]
    pub icon: Option<String>,
    #[sea_orm(column_type = "Text", nullable)]
    pub permission_code: Option<String>,
    pub sort: i32,
    pub keep_alive: bool,
    pub hide_in_menu: bool,
    pub affix_tab: bool,
    pub status: RoleStatus,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Copy, Clone, Debug, EnumIter, DeriveRelation)]
pub enum Relation {}

impl ActiveModelBehavior for ActiveModel {}
