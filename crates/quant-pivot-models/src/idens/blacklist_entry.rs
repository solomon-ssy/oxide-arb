use oxide_arb_macros::oxide_schema;
use sea_orm::sea_query::{ColumnDef, Table, TableCreateStatement};

use crate::schema::{
    column, dependency::TableDependency, index::IndexSpec, seed::SeedSpec,
    timestamp_with_write_default,
};

#[oxide_schema(lifecycle = "runtime")]
pub enum BlacklistEntry {
    Table,
    MarketId,
    TokenId,
    Scope,
    Reason,
    ExpiresAt,
    MissCount,
    CreatedAt,
    UpdatedAt,
}

pub fn table() -> TableCreateStatement {
    Table::create()
        .table(BlacklistEntry::Table)
        .if_not_exists()
        .col(column::market_id_pk(BlacklistEntry::MarketId))
        .col(column::token_id_null(BlacklistEntry::TokenId))
        .col(ColumnDef::new(BlacklistEntry::Scope).text().not_null())
        .col(ColumnDef::new(BlacklistEntry::Reason).text().not_null())
        .col(
            ColumnDef::new(BlacklistEntry::ExpiresAt)
                .timestamp_with_time_zone()
                .null(),
        )
        .col(
            ColumnDef::new(BlacklistEntry::MissCount)
                .integer()
                .not_null()
                .default(0),
        )
        .col(timestamp_with_write_default(BlacklistEntry::CreatedAt))
        .col(timestamp_with_write_default(BlacklistEntry::UpdatedAt))
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
