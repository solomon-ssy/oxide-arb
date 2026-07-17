//! Applied catalog-seed checksum ledger.

use chrono::{DateTime, Utc};
use sea_orm::entity::prelude::*;
use serde::{Deserialize, Serialize};

#[sea_orm::model]
#[derive(Clone, Debug, PartialEq, Eq, DeriveEntityModel, Serialize, Deserialize)]
#[sea_orm(table_name = "seed_application")]
pub struct Model {
    #[sea_orm(primary_key, auto_increment = false, column_type = "Text")]
    pub seed_id: String,
    #[sea_orm(primary_key, auto_increment = false)]
    pub seed_version: i32,
    #[sea_orm(column_type = "Text")]
    pub checksum: String,
    pub applied_at: DateTime<Utc>,
    pub rows_affected: i64,
}

impl ActiveModelBehavior for ActiveModel {}
