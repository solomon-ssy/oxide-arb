//! `casbin_rule` table entity (Casbin policy storage).

use sea_orm::entity::prelude::*;
use serde::{Deserialize, Serialize};

#[sea_orm::model]
#[derive(Clone, Debug, PartialEq, Eq, DeriveEntityModel, Serialize, Deserialize)]
#[sea_orm(table_name = "casbin_rule")]
pub struct Model {
    #[sea_orm(primary_key)]
    pub id: i64,
    #[sea_orm(column_type = "Text")]
    pub ptype: String,
    #[sea_orm(column_type = "Text")]
    pub v0: String,
    #[sea_orm(column_type = "Text")]
    pub v1: String,
    #[sea_orm(column_type = "Text")]
    pub v2: String,
    #[sea_orm(column_type = "Text")]
    pub v3: String,
    #[sea_orm(column_type = "Text")]
    pub v4: String,
    #[sea_orm(column_type = "Text")]
    pub v5: String,
}

impl ActiveModelBehavior for ActiveModel {}
