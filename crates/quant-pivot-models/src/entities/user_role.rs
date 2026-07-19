//! `user_role` table entity (user→role assignments).

use crate::types::{RoleId, UserId};
use chrono::{DateTime, Utc};
use sea_orm::entity::prelude::*;
use serde::{Deserialize, Serialize};

#[sea_orm::model]
#[derive(Clone, Debug, PartialEq, Eq, DeriveEntityModel, Serialize, Deserialize)]
#[sea_orm(table_name = "user_role")]
pub struct Model {
    #[sea_orm(primary_key, auto_increment = false)]
    pub user_id: UserId,
    #[sea_orm(primary_key, auto_increment = false)]
    pub role_id: RoleId,
    pub created_at: DateTime<Utc>,

    #[sea_orm(belongs_to, relation_enum = "User", from = "user_id", to = "id")]
    pub user: BelongsTo<super::user::Entity>,
    #[sea_orm(belongs_to, relation_enum = "Role", from = "role_id", to = "id")]
    pub role: BelongsTo<super::role::Entity>,
}

impl ActiveModelBehavior for ActiveModel {}
