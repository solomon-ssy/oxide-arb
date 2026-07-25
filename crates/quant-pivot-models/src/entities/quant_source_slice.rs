//! `quant_source_slice` server-owned materialization ledger.

use chrono::{DateTime, Utc};
use sea_orm::entity::prelude::*;

use super::decision_policy_snapshot;
use crate::{
    enums::quant::SourceSliceStatus,
    types::{
        ArtifactUri, ContentHash, DecisionPolicySnapshotId, ReaderContractVersion,
        ResearchEvaluationTrack, ResearchProfileRef, SchemaContractVersion, SourceSliceId,
        SourceSliceManifest,
    },
};

#[sea_orm::model]
#[derive(Clone, Debug, PartialEq, Eq, DeriveEntityModel)]
#[sea_orm(table_name = "quant_source_slice")]
pub struct Model {
    #[sea_orm(primary_key, auto_increment = false)]
    pub source_slice_id: SourceSliceId,
    pub identity_hash: ContentHash,
    #[sea_orm(column_type = "JsonBinary")]
    pub profile_ref: ResearchProfileRef,
    pub evaluation_track: ResearchEvaluationTrack,
    pub research_program_hash: ContentHash,
    pub decision_policy_snapshot_id: DecisionPolicySnapshotId,
    pub runtime_config_hash: ContentHash,
    pub window_start: DateTime<Utc>,
    pub window_end: DateTime<Utc>,
    pub pit_cutoff: DateTime<Utc>,
    pub reader_contract_version: ReaderContractVersion,
    pub schema_contract_version: SchemaContractVersion,
    pub status: SourceSliceStatus,
    pub manifest_uri: Option<ArtifactUri>,
    pub manifest_hash: Option<ContentHash>,
    #[sea_orm(column_type = "JsonBinary")]
    pub manifest: Option<SourceSliceManifest>,
    pub failure_detail: Option<String>,
    pub completed_at: Option<DateTime<Utc>>,
    pub created_at: DateTime<Utc>,

    #[sea_orm(
        belongs_to,
        relation_enum = "DecisionPolicySnapshot",
        from = "decision_policy_snapshot_id",
        to = "decision_policy_snapshot_id"
    )]
    pub decision_policy_snapshot: BelongsTo<decision_policy_snapshot::Entity>,
}

impl ActiveModelBehavior for ActiveModel {}
