//! `quant_source_slice` server-owned materialization ledger.

use chrono::{DateTime, Utc};
use sea_orm::entity::prelude::*;

use crate::{
    enums::quant::SourceSliceStatus,
    types::{
        ArtifactUri, ContentHash, ResearchProfileRef, RuntimeConfigVersionId, SourceSliceId,
        SourceSliceManifestV2,
    },
};

#[derive(Clone, Debug, PartialEq, Eq, DeriveEntityModel)]
#[sea_orm(table_name = "quant_source_slice")]
pub struct Model {
    #[sea_orm(primary_key, auto_increment = false)]
    pub source_slice_id: SourceSliceId,
    pub identity_hash: ContentHash,
    #[sea_orm(column_type = "JsonBinary")]
    pub profile_ref: ResearchProfileRef,
    pub evaluation_track: String,
    pub research_program_hash: ContentHash,
    pub runtime_config_version_id: RuntimeConfigVersionId,
    pub runtime_config_hash: ContentHash,
    pub window_start: DateTime<Utc>,
    pub window_end: DateTime<Utc>,
    pub pit_cutoff: DateTime<Utc>,
    pub reader_contract_version: String,
    pub schema_contract_version: String,
    pub status: SourceSliceStatus,
    pub manifest_uri: Option<ArtifactUri>,
    pub manifest_hash: Option<ContentHash>,
    #[sea_orm(column_type = "JsonBinary")]
    pub manifest_json: Option<SourceSliceManifestV2>,
    pub failure_detail: Option<String>,
    pub completed_at: Option<DateTime<Utc>>,
    pub created_at: DateTime<Utc>,
}

#[derive(Copy, Clone, Debug, EnumIter, DeriveRelation)]
pub enum Relation {
    #[sea_orm(
        belongs_to = "super::runtime_config_version::Entity",
        from = "Column::RuntimeConfigVersionId",
        to = "super::runtime_config_version::Column::RuntimeConfigVersionId"
    )]
    RuntimeConfigVersion,
}

impl Related<super::runtime_config_version::Entity> for Entity {
    fn to() -> RelationDef {
        Relation::RuntimeConfigVersion.def()
    }
}

impl ActiveModelBehavior for ActiveModel {}
