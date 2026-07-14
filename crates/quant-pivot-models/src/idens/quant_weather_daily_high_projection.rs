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
pub enum QuantWeatherDailyHighProjection {
    Table,
    SourceId,
    InstrumentKey,
    Station,
    LocalDate,
    Timezone,
    CurrentHighCelsius,
    PreviousHighCelsius,
    LastObservationTime,
    LastReportHash,
    LastEventId,
    Revision,
    DayClosed,
    GapGeneration,
    SourceHealthy,
    AvailableAt,
    UpdatedAt,
}

pub fn table() -> TableCreateStatement {
    Table::create()
        .table(QuantWeatherDailyHighProjection::Table)
        .if_not_exists()
        .col(column::text_id(QuantWeatherDailyHighProjection::SourceId))
        .col(column::text_id(
            QuantWeatherDailyHighProjection::InstrumentKey,
        ))
        .col(
            ColumnDef::new(QuantWeatherDailyHighProjection::Station)
                .text()
                .not_null(),
        )
        .col(
            ColumnDef::new(QuantWeatherDailyHighProjection::LocalDate)
                .date()
                .not_null(),
        )
        .col(
            ColumnDef::new(QuantWeatherDailyHighProjection::Timezone)
                .text()
                .not_null(),
        )
        .col(
            ColumnDef::new(QuantWeatherDailyHighProjection::CurrentHighCelsius)
                .decimal_len(8, 4)
                .not_null(),
        )
        .col(
            ColumnDef::new(QuantWeatherDailyHighProjection::PreviousHighCelsius)
                .decimal_len(8, 4)
                .null(),
        )
        .col(
            ColumnDef::new(QuantWeatherDailyHighProjection::LastObservationTime)
                .timestamp_with_time_zone()
                .not_null(),
        )
        .col(
            ColumnDef::new(QuantWeatherDailyHighProjection::LastReportHash)
                .text()
                .not_null(),
        )
        .col(column::uuid_null(
            QuantWeatherDailyHighProjection::LastEventId,
        ))
        .col(
            ColumnDef::new(QuantWeatherDailyHighProjection::Revision)
                .big_integer()
                .not_null(),
        )
        .col(
            ColumnDef::new(QuantWeatherDailyHighProjection::DayClosed)
                .boolean()
                .not_null(),
        )
        .col(
            ColumnDef::new(QuantWeatherDailyHighProjection::GapGeneration)
                .big_integer()
                .not_null(),
        )
        .col(
            ColumnDef::new(QuantWeatherDailyHighProjection::SourceHealthy)
                .boolean()
                .not_null(),
        )
        .col(
            ColumnDef::new(QuantWeatherDailyHighProjection::AvailableAt)
                .timestamp_with_time_zone()
                .not_null(),
        )
        .col(timestamp_with_write_default(
            QuantWeatherDailyHighProjection::UpdatedAt,
        ))
        .primary_key(
            Index::create()
                .col(QuantWeatherDailyHighProjection::SourceId)
                .col(QuantWeatherDailyHighProjection::InstrumentKey)
                .col(QuantWeatherDailyHighProjection::LocalDate),
        )
        .to_owned()
}

pub fn indexes() -> Vec<IndexSpec> {
    vec![IndexSpec::sea_query(
        "idx_quant_weather_daily_high_open",
        table_name,
        IndexBuildMode::Transactional,
        Index::create()
            .name("idx_quant_weather_daily_high_open")
            .table(QuantWeatherDailyHighProjection::Table)
            .col(QuantWeatherDailyHighProjection::DayClosed)
            .col(QuantWeatherDailyHighProjection::LocalDate)
            .col(QuantWeatherDailyHighProjection::Station)
            .to_owned(),
        "open weather local days and correction scan",
    )]
}

pub const fn dependencies() -> Vec<TableDependency> {
    Vec::new()
}
pub const fn seed_units() -> Vec<SeedSpec> {
    Vec::new()
}
fn table_name() -> String {
    QuantWeatherDailyHighProjection::Table.to_string()
}
