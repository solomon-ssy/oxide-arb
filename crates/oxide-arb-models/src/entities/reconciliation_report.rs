//! `reconciliation_reports` table entity.

use crate::{enums::risk::ReconciliationStatus, types::Usd};
use chrono::{DateTime, Utc};
use sea_orm::entity::prelude::*;

#[derive(Clone, Debug, PartialEq, Eq, DeriveEntityModel)]
#[sea_orm(table_name = "reconciliation_report")]
pub struct Model {
    #[sea_orm(primary_key)]
    pub id: i64,
    pub status: ReconciliationStatus,
    #[sea_orm(column_type = "JsonBinary")]
    pub mismatches: serde_json::Value,
    pub internal_balance: Usd,
    pub external_balance: Usd,
    pub internal_exposure: Usd,
    pub external_exposure: Usd,
    pub reserved: Usd,
    pub tolerance: Usd,
    pub checked_at: DateTime<Utc>,
    pub duration_ms: i64,
}

#[derive(Copy, Clone, Debug, EnumIter, DeriveRelation)]
pub enum Relation {}

impl ActiveModelBehavior for ActiveModel {}
