use crate::schema::{
    dependency::TableDependency, index::IndexSpec, seed::SeedSpec, timestamp_with_write_default,
};
use oxide_arb_macros::oxide_schema;
use sea_orm::sea_query::{ColumnDef, Table, TableCreateStatement};

#[oxide_schema]
pub enum SeedApplication {
    Table,
    SeedId,
    SeedVersion,
    Checksum,
    AppliedAt,
    RowsAffected,
}

pub fn table() -> TableCreateStatement {
    Table::create()
        .table(SeedApplication::Table)
        .if_not_exists()
        .col(ColumnDef::new(SeedApplication::SeedId).text().not_null())
        .col(
            ColumnDef::new(SeedApplication::SeedVersion)
                .integer()
                .not_null(),
        )
        .col(ColumnDef::new(SeedApplication::Checksum).text().not_null())
        .col(timestamp_with_write_default(SeedApplication::AppliedAt))
        .col(
            ColumnDef::new(SeedApplication::RowsAffected)
                .big_integer()
                .not_null(),
        )
        .primary_key(
            sea_orm::sea_query::Index::create()
                .col(SeedApplication::SeedId)
                .col(SeedApplication::SeedVersion),
        )
        .to_owned()
}

pub const fn indexes() -> Vec<IndexSpec> {
    Vec::new()
}

pub const fn dependencies() -> Vec<TableDependency> {
    Vec::new()
}

pub const fn seed_units() -> Vec<SeedSpec> {
    Vec::new()
}
