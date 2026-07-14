use quant_pivot_macros::quant_schema;
use sea_orm::{
    Iden,
    sea_query::{ColumnDef, Index, Table, TableCreateStatement},
};

use crate::schema::{
    column,
    dependency::TableDependency,
    index::{IndexBuildMode, IndexSpec},
    seed::SeedSpec,
    timestamp_with_write_default,
};

#[quant_schema(lifecycle = "audit")]
pub enum QuantArchivePartitionManifest {
    Table,
    ManifestId,
    TableName,
    PartitionKey,
    RetentionDays,
    RowCount,
    ParquetUri,
    ByteHash,
    ContentHash,
    ManifestHash,
    SealedAt,
    CreatedAt,
}

pub fn table() -> TableCreateStatement {
    Table::create()
        .table(QuantArchivePartitionManifest::Table)
        .if_not_exists()
        .col(column::uuid_pk(QuantArchivePartitionManifest::ManifestId))
        .col(
            ColumnDef::new(QuantArchivePartitionManifest::TableName)
                .text()
                .not_null(),
        )
        .col(
            ColumnDef::new(QuantArchivePartitionManifest::PartitionKey)
                .text()
                .not_null(),
        )
        .col(
            ColumnDef::new(QuantArchivePartitionManifest::RetentionDays)
                .integer()
                .not_null(),
        )
        .col(
            ColumnDef::new(QuantArchivePartitionManifest::RowCount)
                .big_integer()
                .not_null(),
        )
        .col(
            ColumnDef::new(QuantArchivePartitionManifest::ParquetUri)
                .text()
                .not_null(),
        )
        .col(
            ColumnDef::new(QuantArchivePartitionManifest::ByteHash)
                .text()
                .not_null(),
        )
        .col(
            ColumnDef::new(QuantArchivePartitionManifest::ContentHash)
                .text()
                .not_null(),
        )
        .col(
            ColumnDef::new(QuantArchivePartitionManifest::ManifestHash)
                .text()
                .not_null(),
        )
        .col(
            ColumnDef::new(QuantArchivePartitionManifest::SealedAt)
                .timestamp_with_time_zone()
                .not_null(),
        )
        .col(timestamp_with_write_default(
            QuantArchivePartitionManifest::CreatedAt,
        ))
        .to_owned()
}

pub fn indexes() -> Vec<IndexSpec> {
    vec![
        IndexSpec::sea_query(
            "uq_quant_archive_partition_manifest_partition",
            table_name,
            IndexBuildMode::Transactional,
            Index::create()
                .name("uq_quant_archive_partition_manifest_partition")
                .table(QuantArchivePartitionManifest::Table)
                .col(QuantArchivePartitionManifest::TableName)
                .col(QuantArchivePartitionManifest::PartitionKey)
                .unique()
                .to_owned(),
            "one immutable sealed manifest per ClickHouse partition",
        ),
        IndexSpec::sea_query(
            "uq_quant_archive_partition_manifest_hash",
            table_name,
            IndexBuildMode::Transactional,
            Index::create()
                .name("uq_quant_archive_partition_manifest_hash")
                .table(QuantArchivePartitionManifest::Table)
                .col(QuantArchivePartitionManifest::ManifestHash)
                .unique()
                .to_owned(),
            "content-addressed archive manifests are globally unique",
        ),
    ]
}

pub const fn dependencies() -> Vec<TableDependency> {
    Vec::new()
}

pub const fn seed_units() -> Vec<SeedSpec> {
    Vec::new()
}

fn table_name() -> String {
    QuantArchivePartitionManifest::Table.to_string()
}
