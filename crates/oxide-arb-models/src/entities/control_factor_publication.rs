//! `control_factor_publication` table entity.

use crate::{
    enums::control_factor::{PublicationMode, PublicationStatus},
    types::FactorPublicationId,
};
use chrono::{DateTime, Utc};
use sea_orm::entity::prelude::*;

#[derive(Clone, Debug, PartialEq, Eq, DeriveEntityModel)]
#[sea_orm(table_name = "control_factor_publication")]
pub struct Model {
    #[sea_orm(primary_key, auto_increment = false)]
    pub publication_id: FactorPublicationId,
    pub mode: PublicationMode,
    pub previous_publication_id: Option<FactorPublicationId>,
    pub status: PublicationStatus,
    pub effective_from: DateTime<Utc>,
    pub expires_at: DateTime<Utc>,
    #[sea_orm(column_type = "Text", nullable)]
    pub approved_by: Option<String>,
    #[sea_orm(column_type = "Text")]
    pub approval_reason: String,
    #[sea_orm(column_type = "Text")]
    pub idempotency_key: String,
    #[sea_orm(column_type = "Text")]
    pub publication_hash: String,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Copy, Clone, Debug, EnumIter, DeriveRelation)]
pub enum Relation {}

impl ActiveModelBehavior for ActiveModel {}
