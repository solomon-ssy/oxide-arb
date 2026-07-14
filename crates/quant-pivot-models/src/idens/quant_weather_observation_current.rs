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
pub enum QuantWeatherObservationCurrent {
    Table,
    Station,
    LocalDate,
    ObservationTime,
    TemperatureCelsius,
    ReportHash,
    Revision,
    PublishedAt,
    AvailableAt,
    UpdatedAt,
}

pub fn table() -> TableCreateStatement {
    Table::create()
        .table(QuantWeatherObservationCurrent::Table)
        .if_not_exists()
        .col(column::text_id(QuantWeatherObservationCurrent::Station))
        .col(
            ColumnDef::new(QuantWeatherObservationCurrent::LocalDate)
                .date()
                .not_null(),
        )
        .col(
            ColumnDef::new(QuantWeatherObservationCurrent::ObservationTime)
                .timestamp_with_time_zone()
                .not_null(),
        )
        .col(
            ColumnDef::new(QuantWeatherObservationCurrent::TemperatureCelsius)
                .decimal_len(8, 4)
                .not_null(),
        )
        .col(
            ColumnDef::new(QuantWeatherObservationCurrent::ReportHash)
                .text()
                .not_null(),
        )
        .col(
            ColumnDef::new(QuantWeatherObservationCurrent::Revision)
                .big_integer()
                .not_null(),
        )
        .col(
            ColumnDef::new(QuantWeatherObservationCurrent::PublishedAt)
                .timestamp_with_time_zone()
                .not_null(),
        )
        .col(
            ColumnDef::new(QuantWeatherObservationCurrent::AvailableAt)
                .timestamp_with_time_zone()
                .not_null(),
        )
        .col(timestamp_with_write_default(
            QuantWeatherObservationCurrent::UpdatedAt,
        ))
        .primary_key(
            Index::create()
                .col(QuantWeatherObservationCurrent::Station)
                .col(QuantWeatherObservationCurrent::LocalDate)
                .col(QuantWeatherObservationCurrent::ObservationTime),
        )
        .to_owned()
}

pub fn indexes() -> Vec<IndexSpec> {
    vec![IndexSpec::sea_query(
        "idx_quant_weather_observation_current_daily_high",
        table_name,
        IndexBuildMode::Transactional,
        Index::create()
            .name("idx_quant_weather_observation_current_daily_high")
            .table(QuantWeatherObservationCurrent::Table)
            .col(QuantWeatherObservationCurrent::Station)
            .col(QuantWeatherObservationCurrent::LocalDate)
            .col(QuantWeatherObservationCurrent::TemperatureCelsius)
            .to_owned(),
        "recompute corrected station-local daily high",
    )]
}

pub const fn dependencies() -> Vec<TableDependency> {
    Vec::new()
}
pub const fn seed_units() -> Vec<SeedSpec> {
    Vec::new()
}
fn table_name() -> String {
    QuantWeatherObservationCurrent::Table.to_string()
}
