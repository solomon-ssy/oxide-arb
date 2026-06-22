use oxide_arb_macros::oxide_schema;
use sea_orm::{
    Iden,
    sea_query::{ColumnDef, Index, IndexOrder, Table, TableCreateStatement},
};

use crate::schema::{
    column,
    dependency::TableDependency,
    index::{IndexBuildMode, IndexSpec},
    seed::SeedSpec,
    timestamp_with_write_default,
};

#[oxide_schema(lifecycle = "control")]
pub enum RuntimeConfigVersion {
    Table,
    RuntimeConfigVersionId,
    ConfigHash,
    SchemaVersion,
    ConfigJson,
    Source,
    CreatedBy,
    Reason,
    CreatedAt,
}

pub fn table() -> TableCreateStatement {
    Table::create()
        .table(RuntimeConfigVersion::Table)
        .if_not_exists()
        .col(column::uuid_pk(
            RuntimeConfigVersion::RuntimeConfigVersionId,
        ))
        .col(
            ColumnDef::new(RuntimeConfigVersion::ConfigHash)
                .text()
                .not_null()
                .unique_key(),
        )
        .col(
            ColumnDef::new(RuntimeConfigVersion::SchemaVersion)
                .integer()
                .not_null(),
        )
        .col(
            ColumnDef::new(RuntimeConfigVersion::ConfigJson)
                .json_binary()
                .not_null(),
        )
        .col(
            ColumnDef::new(RuntimeConfigVersion::Source)
                .text()
                .not_null(),
        )
        .col(
            ColumnDef::new(RuntimeConfigVersion::CreatedBy)
                .text()
                .not_null(),
        )
        .col(
            ColumnDef::new(RuntimeConfigVersion::Reason)
                .text()
                .not_null(),
        )
        .col(timestamp_with_write_default(
            RuntimeConfigVersion::CreatedAt,
        ))
        .to_owned()
}

pub fn indexes() -> Vec<IndexSpec> {
    vec![IndexSpec::sea_query(
        "idx_runtime_config_version_created_at",
        runtime_config_version_table_name,
        IndexBuildMode::Transactional,
        Index::create()
            .name("idx_runtime_config_version_created_at")
            .table(RuntimeConfigVersion::Table)
            .col((RuntimeConfigVersion::CreatedAt, IndexOrder::Desc))
            .to_owned(),
        "runtime config versions by recency",
    )]
}

pub const fn dependencies() -> Vec<TableDependency> {
    Vec::new()
}

pub const fn seed_units() -> Vec<SeedSpec> {
    Vec::new()
}

fn runtime_config_version_table_name() -> String {
    RuntimeConfigVersion::Table.to_string()
}
