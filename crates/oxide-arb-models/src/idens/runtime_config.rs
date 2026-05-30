use crate::{
    schema::{
        dependency::TableDependency, index::IndexSpec, seed::SeedSpec, timestamp_with_write_default,
    },
    seed::runtime_config,
};
use oxide_arb_macros::oxide_schema;
use sea_orm::sea_query::{ColumnDef, Table, TableCreateStatement};

#[oxide_schema]
pub enum RuntimeConfig {
    Table,
    Key,
    Value,
    Description,
    UpdatedBy,
    UpdatedAt,
}

pub fn table() -> TableCreateStatement {
    Table::create()
        .table(RuntimeConfig::Table)
        .if_not_exists()
        .col(
            ColumnDef::new(RuntimeConfig::Key)
                .text()
                .not_null()
                .primary_key(),
        )
        .col(
            ColumnDef::new(RuntimeConfig::Value)
                .json_binary()
                .not_null(),
        )
        .col(ColumnDef::new(RuntimeConfig::Description).text().null())
        .col(
            ColumnDef::new(RuntimeConfig::UpdatedBy)
                .text()
                .not_null()
                .default("system"),
        )
        .col(timestamp_with_write_default(RuntimeConfig::UpdatedAt))
        .to_owned()
}

pub const fn indexes() -> Vec<IndexSpec> {
    Vec::new()
}

pub const fn dependencies() -> Vec<TableDependency> {
    Vec::new()
}

pub fn seed_units() -> Vec<SeedSpec> {
    vec![runtime_config::RUNTIME_CONFIG_SEED]
}
