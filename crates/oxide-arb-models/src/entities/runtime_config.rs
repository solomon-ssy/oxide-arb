//! `runtime_config` table entity (key-value store for hot-reloadable params).

use crate::enums::runtime_config::RuntimeConfigKey;
use chrono::{DateTime, Utc};
use sea_orm::entity::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, Eq, DeriveEntityModel, Serialize, Deserialize)]
#[sea_orm(table_name = "runtime_config")]
pub struct Model {
    #[sea_orm(primary_key, auto_increment = false)]
    pub key: RuntimeConfigKey,
    #[sea_orm(column_type = "JsonBinary")]
    pub value: serde_json::Value,
    #[sea_orm(column_type = "Text", nullable)]
    pub description: Option<String>,
    #[sea_orm(column_type = "Text")]
    pub updated_by: String,
    pub updated_at: DateTime<Utc>,
}

#[derive(Copy, Clone, Debug, EnumIter, DeriveRelation)]
pub enum Relation {}

impl ActiveModelBehavior for ActiveModel {}
