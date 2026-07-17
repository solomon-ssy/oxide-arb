//! `role_menu` table entity (role→menu assignments).

use crate::types::{MenuId, RoleId};
use chrono::{DateTime, Utc};
use sea_orm::entity::prelude::*;
use serde::{Deserialize, Serialize};

#[sea_orm::model]
#[derive(Clone, Debug, PartialEq, Eq, DeriveEntityModel, Serialize, Deserialize)]
#[sea_orm(table_name = "role_menu")]
pub struct Model {
    #[sea_orm(primary_key, auto_increment = false)]
    pub role_id: RoleId,
    #[sea_orm(primary_key, auto_increment = false)]
    pub menu_id: MenuId,
    pub created_at: DateTime<Utc>,
}

impl ActiveModelBehavior for ActiveModel {}
