//! Mutable lease ledger for executing one sealed partition drop exactly once.

use chrono::{DateTime, Utc};
use sea_orm::entity::prelude::*;
use uuid::Uuid;

#[derive(Clone, Debug, PartialEq, Eq, DeriveEntityModel)]
#[sea_orm(table_name = "quant_archive_partition_drop_command")]
pub struct Model {
    #[sea_orm(primary_key, auto_increment = false)]
    pub manifest_id: Uuid,
    pub claim_owner: Option<Uuid>,
    pub lease_expires_at: Option<DateTime<Utc>>,
    pub attempts: i32,
    pub last_error: Option<String>,
    pub completed_at: Option<DateTime<Utc>>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Copy, Clone, Debug, EnumIter, DeriveRelation)]
pub enum Relation {
    #[sea_orm(
        belongs_to = "super::quant_archive_partition_manifest::Entity",
        from = "Column::ManifestId",
        to = "super::quant_archive_partition_manifest::Column::ManifestId",
        on_delete = "Restrict",
        on_update = "Restrict"
    )]
    Manifest,
}

impl Related<super::quant_archive_partition_manifest::Entity> for Entity {
    fn to() -> RelationDef {
        Relation::Manifest.def()
    }
}

impl ActiveModelBehavior for ActiveModel {}
