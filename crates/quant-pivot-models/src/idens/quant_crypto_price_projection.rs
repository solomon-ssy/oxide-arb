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

#[quant_schema(lifecycle = "runtime")]
pub enum QuantCryptoPriceProjection {
    Table,
    SourceId,
    InstrumentKey,
    PreviousPrice,
    CurrentPrice,
    SourceSequence,
    EventTime,
    AvailableAt,
    ReportHash,
    GapGeneration,
    SourceHealthy,
    UpdatedAt,
}

pub fn table() -> TableCreateStatement {
    Table::create()
        .table(QuantCryptoPriceProjection::Table)
        .if_not_exists()
        .col(column::text_id(QuantCryptoPriceProjection::SourceId))
        .col(column::text_id(QuantCryptoPriceProjection::InstrumentKey))
        .col(column::usd_null(QuantCryptoPriceProjection::PreviousPrice))
        .col(column::usd(QuantCryptoPriceProjection::CurrentPrice))
        .col(
            ColumnDef::new(QuantCryptoPriceProjection::SourceSequence)
                .big_unsigned()
                .not_null(),
        )
        .col(
            ColumnDef::new(QuantCryptoPriceProjection::EventTime)
                .timestamp_with_time_zone()
                .not_null(),
        )
        .col(
            ColumnDef::new(QuantCryptoPriceProjection::AvailableAt)
                .timestamp_with_time_zone()
                .not_null(),
        )
        .col(
            ColumnDef::new(QuantCryptoPriceProjection::ReportHash)
                .text()
                .not_null(),
        )
        .col(
            ColumnDef::new(QuantCryptoPriceProjection::GapGeneration)
                .big_integer()
                .not_null(),
        )
        .col(
            ColumnDef::new(QuantCryptoPriceProjection::SourceHealthy)
                .boolean()
                .not_null(),
        )
        .col(timestamp_with_write_default(
            QuantCryptoPriceProjection::UpdatedAt,
        ))
        .primary_key(
            Index::create()
                .col(QuantCryptoPriceProjection::SourceId)
                .col(QuantCryptoPriceProjection::InstrumentKey),
        )
        .to_owned()
}

pub fn indexes() -> Vec<IndexSpec> {
    vec![IndexSpec::sea_query(
        "idx_quant_crypto_price_projection_health",
        table_name,
        IndexBuildMode::Transactional,
        Index::create()
            .name("idx_quant_crypto_price_projection_health")
            .table(QuantCryptoPriceProjection::Table)
            .col(QuantCryptoPriceProjection::SourceHealthy)
            .col(QuantCryptoPriceProjection::EventTime)
            .to_owned(),
        "live crypto source health and lag",
    )]
}

pub const fn dependencies() -> Vec<TableDependency> {
    Vec::new()
}
pub const fn seed_units() -> Vec<SeedSpec> {
    Vec::new()
}
fn table_name() -> String {
    QuantCryptoPriceProjection::Table.to_string()
}
