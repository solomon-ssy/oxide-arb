//! Immutable `ClickHouse` partition archive manifests and drop proofs.

use chrono::{DateTime, Utc};
use sea_orm::{DeriveIntoActiveModel, DerivePartialModel, FromQueryResult};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::{
    entities::{quant_archive_partition_drop_audit, quant_archive_partition_manifest},
    types::{ArtifactUri, ContentHash},
};

#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, DerivePartialModel, FromQueryResult,
)]
#[sea_orm(entity = "quant_archive_partition_manifest::Entity")]
pub struct ArchivePartitionManifestInfo {
    pub manifest_id: Uuid,
    pub table_name: String,
    pub partition_key: String,
    pub partition_start_at: DateTime<Utc>,
    pub partition_end_at: DateTime<Utc>,
    pub retention_days: i32,
    pub row_count: i64,
    pub parquet_uri: ArtifactUri,
    pub parquet_byte_count: i64,
    pub object_etag: Option<String>,
    pub object_version_id: Option<String>,
    pub byte_hash: ContentHash,
    pub content_hash: ContentHash,
    pub source_schema_hash: ContentHash,
    pub manifest_hash: ContentHash,
    pub sealed_at: DateTime<Utc>,
    pub created_at: DateTime<Utc>,
}

info_from_model!(
    ArchivePartitionManifestInfo,
    quant_archive_partition_manifest::Model,
    {
        manifest_id,
        table_name,
        partition_key,
        partition_start_at,
        partition_end_at,
        retention_days,
        row_count,
        parquet_uri,
        parquet_byte_count,
        object_etag,
        object_version_id,
        byte_hash,
        content_hash,
        source_schema_hash,
        manifest_hash,
        sealed_at,
        created_at,
    }
);

#[derive(Debug, Clone, Serialize, Deserialize, DeriveIntoActiveModel)]
#[sea_orm(active_model = "quant_archive_partition_manifest::ActiveModel")]
pub struct NewArchivePartitionManifest {
    pub manifest_id: Uuid,
    pub table_name: String,
    pub partition_key: String,
    pub partition_start_at: DateTime<Utc>,
    pub partition_end_at: DateTime<Utc>,
    pub retention_days: i32,
    pub row_count: i64,
    pub parquet_uri: ArtifactUri,
    pub parquet_byte_count: i64,
    pub object_etag: Option<String>,
    pub object_version_id: Option<String>,
    pub byte_hash: ContentHash,
    pub content_hash: ContentHash,
    pub source_schema_hash: ContentHash,
    pub manifest_hash: ContentHash,
    pub sealed_at: DateTime<Utc>,
}

#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, DerivePartialModel, FromQueryResult,
)]
#[sea_orm(entity = "quant_archive_partition_drop_audit::Entity")]
pub struct ArchivePartitionDropAuditInfo {
    pub audit_id: Uuid,
    pub manifest_id: Uuid,
    pub dropped_at: DateTime<Utc>,
    pub created_at: DateTime<Utc>,
}

info_from_model!(
    ArchivePartitionDropAuditInfo,
    quant_archive_partition_drop_audit::Model,
    {
        audit_id,
        manifest_id,
        dropped_at,
        created_at,
    }
);
