//! `control_factor_value` table entity.

use crate::{
    enums::control_factor::{ControlFactorType, FactorStatus},
    types::{ControlFactorId, MaterializationRunId},
};
use chrono::{DateTime, Utc};
use sea_orm::entity::prelude::*;

#[derive(Clone, Debug, PartialEq, Eq, DeriveEntityModel)]
#[sea_orm(table_name = "control_factor_value")]
pub struct Model {
    #[sea_orm(primary_key, auto_increment = false)]
    pub factor_id: ControlFactorId,
    pub run_id: MaterializationRunId,
    pub factor_type: ControlFactorType,
    #[sea_orm(column_type = "JsonBinary")]
    pub dimensions: Json,
    #[sea_orm(column_type = "Text")]
    pub dimensions_hash: String,
    #[sea_orm(column_type = "JsonBinary")]
    pub payload: Json,
    #[sea_orm(column_type = "Text")]
    pub payload_hash: String,
    #[sea_orm(column_type = "JsonBinary")]
    pub evidence: Json,
    pub status: FactorStatus,
    #[sea_orm(column_type = "Text", nullable)]
    pub status_reason: Option<String>,
    pub generated_at: DateTime<Utc>,
    pub expires_at: DateTime<Utc>,
    #[sea_orm(column_type = "Text")]
    pub owner: String,
    pub schema_version: i32,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Copy, Clone, Debug, EnumIter, DeriveRelation)]
pub enum Relation {}

impl ActiveModelBehavior for ActiveModel {}
