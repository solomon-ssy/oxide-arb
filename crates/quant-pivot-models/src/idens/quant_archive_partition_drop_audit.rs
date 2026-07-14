use quant_pivot_macros::quant_schema;
use sea_orm::{
    Iden,
    sea_query::{ColumnDef, ForeignKey, ForeignKeyAction, Index, Table, TableCreateStatement},
};

use crate::{
    idens::quant_archive_partition_manifest::QuantArchivePartitionManifest,
    schema::{
        column,
        dependency::TableDependency,
        index::{IndexBuildMode, IndexSpec},
        seed::SeedSpec,
        timestamp_with_write_default,
    },
};

#[quant_schema(lifecycle = "audit")]
pub enum QuantArchivePartitionDropAudit {
    Table,
    AuditId,
    ManifestId,
    DroppedAt,
    CreatedAt,
}

pub fn table() -> TableCreateStatement {
    Table::create()
        .table(QuantArchivePartitionDropAudit::Table)
        .if_not_exists()
        .col(column::uuid_pk(QuantArchivePartitionDropAudit::AuditId))
        .col(
            ColumnDef::new(QuantArchivePartitionDropAudit::ManifestId)
                .uuid()
                .not_null(),
        )
        .col(
            ColumnDef::new(QuantArchivePartitionDropAudit::DroppedAt)
                .timestamp_with_time_zone()
                .not_null(),
        )
        .col(timestamp_with_write_default(
            QuantArchivePartitionDropAudit::CreatedAt,
        ))
        .foreign_key(
            ForeignKey::create()
                .name("fk_quant_archive_partition_drop_manifest")
                .from(
                    QuantArchivePartitionDropAudit::Table,
                    QuantArchivePartitionDropAudit::ManifestId,
                )
                .to(
                    QuantArchivePartitionManifest::Table,
                    QuantArchivePartitionManifest::ManifestId,
                )
                .on_delete(ForeignKeyAction::Restrict)
                .on_update(ForeignKeyAction::Restrict),
        )
        .to_owned()
}

pub fn indexes() -> Vec<IndexSpec> {
    vec![IndexSpec::sea_query(
        "uq_quant_archive_partition_drop_manifest",
        table_name,
        IndexBuildMode::Transactional,
        Index::create()
            .name("uq_quant_archive_partition_drop_manifest")
            .table(QuantArchivePartitionDropAudit::Table)
            .col(QuantArchivePartitionDropAudit::ManifestId)
            .unique()
            .to_owned(),
        "one immutable drop proof per sealed manifest",
    )]
}

pub fn dependencies() -> Vec<TableDependency> {
    vec![TableDependency::foreign_key(
        quant_archive_partition_manifest_table_name,
    )]
}

pub const fn seed_units() -> Vec<SeedSpec> {
    Vec::new()
}

fn table_name() -> String {
    QuantArchivePartitionDropAudit::Table.to_string()
}

fn quant_archive_partition_manifest_table_name() -> String {
    QuantArchivePartitionManifest::Table.to_string()
}
