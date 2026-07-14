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

#[quant_schema(lifecycle = "ledger")]
pub enum QuantArchivePartitionDropCommand {
    Table,
    ManifestId,
    ClaimOwner,
    LeaseExpiresAt,
    Attempts,
    LastError,
    CompletedAt,
    CreatedAt,
    UpdatedAt,
}

pub fn table() -> TableCreateStatement {
    Table::create()
        .table(QuantArchivePartitionDropCommand::Table)
        .if_not_exists()
        .col(column::uuid_pk(
            QuantArchivePartitionDropCommand::ManifestId,
        ))
        .col(column::uuid_null(
            QuantArchivePartitionDropCommand::ClaimOwner,
        ))
        .col(
            ColumnDef::new(QuantArchivePartitionDropCommand::LeaseExpiresAt)
                .timestamp_with_time_zone()
                .null(),
        )
        .col(
            ColumnDef::new(QuantArchivePartitionDropCommand::Attempts)
                .integer()
                .not_null()
                .default(0),
        )
        .col(
            ColumnDef::new(QuantArchivePartitionDropCommand::LastError)
                .text()
                .null(),
        )
        .col(
            ColumnDef::new(QuantArchivePartitionDropCommand::CompletedAt)
                .timestamp_with_time_zone()
                .null(),
        )
        .col(timestamp_with_write_default(
            QuantArchivePartitionDropCommand::CreatedAt,
        ))
        .col(timestamp_with_write_default(
            QuantArchivePartitionDropCommand::UpdatedAt,
        ))
        .foreign_key(
            ForeignKey::create()
                .name("fk_quant_archive_partition_drop_command_manifest")
                .from(
                    QuantArchivePartitionDropCommand::Table,
                    QuantArchivePartitionDropCommand::ManifestId,
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
        "idx_quant_archive_partition_drop_command_claim",
        table_name,
        IndexBuildMode::Transactional,
        Index::create()
            .name("idx_quant_archive_partition_drop_command_claim")
            .table(QuantArchivePartitionDropCommand::Table)
            .col(QuantArchivePartitionDropCommand::CompletedAt)
            .col(QuantArchivePartitionDropCommand::LeaseExpiresAt)
            .col(QuantArchivePartitionDropCommand::CreatedAt)
            .to_owned(),
        "crash-recoverable sealed partition drop queue",
    )]
}

pub fn dependencies() -> Vec<TableDependency> {
    vec![TableDependency::foreign_key(manifest_table_name)]
}

pub const fn seed_units() -> Vec<SeedSpec> {
    Vec::new()
}

fn table_name() -> String {
    QuantArchivePartitionDropCommand::Table.to_string()
}

fn manifest_table_name() -> String {
    QuantArchivePartitionManifest::Table.to_string()
}
