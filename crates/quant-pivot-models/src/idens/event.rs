use quant_pivot_macros::quant_schema;
use sea_orm::{
    Iden,
    sea_query::{ColumnDef, ColumnType, Expr, Index, Table, TableCreateStatement},
};

use crate::{
    enums::market::EventStatus,
    schema::{
        column,
        dependency::TableDependency,
        index::{IndexBuildMode, IndexSpec},
        seed::SeedSpec,
        timestamp_with_write_default,
    },
};

#[quant_schema(lifecycle = "core")]
pub enum Event {
    Table,
    EventId,
    Title,
    Slug,
    Status,
    Tags,
    NegRisk,
    /// Gamma event catalog snapshot: ordered `condition_id`s at sync time.
    CatalogMarketIds,
    EndDate,
    RawGamma,
    CreatedAt,
    UpdatedAt,
}

pub fn table() -> TableCreateStatement {
    Table::create()
        .table(Event::Table)
        .if_not_exists()
        .col(column::text_id_pk(Event::EventId))
        .col(ColumnDef::new(Event::Title).text().not_null())
        .col(ColumnDef::new(Event::Slug).text().not_null())
        .col(column::pg_enum_default::<EventStatus>(
            Event::Status,
            &EventStatus::Active,
        ))
        .col(
            ColumnDef::new(Event::Tags)
                .array(ColumnType::Text)
                .not_null()
                .default(Expr::cust("'{}'::text[]")),
        )
        .col(
            ColumnDef::new(Event::NegRisk)
                .boolean()
                .not_null()
                .default(false),
        )
        .col(
            ColumnDef::new(Event::CatalogMarketIds)
                .array(ColumnType::Text)
                .not_null()
                .default(Expr::cust("'{}'::text[]")),
        )
        .col(
            ColumnDef::new(Event::EndDate)
                .timestamp_with_time_zone()
                .null(),
        )
        .col(ColumnDef::new(Event::RawGamma).json_binary().null())
        .col(timestamp_with_write_default(Event::CreatedAt))
        .col(timestamp_with_write_default(Event::UpdatedAt))
        .to_owned()
}

pub fn indexes() -> Vec<IndexSpec> {
    vec![
        IndexSpec::sea_query(
            "idx_events_status",
            event_table_name,
            IndexBuildMode::Transactional,
            Index::create()
                .name("idx_events_status")
                .table(Event::Table)
                .col(Event::Status)
                .to_owned(),
            "event status filters",
        ),
        IndexSpec::raw(
            "idx_events_tags",
            event_table_name,
            IndexBuildMode::Transactional,
            "CREATE INDEX IF NOT EXISTS idx_events_tags \
             ON event USING GIN (tags)",
            "tag membership filters on the text[] column",
        ),
    ]
}

pub const fn dependencies() -> Vec<TableDependency> {
    Vec::new()
}

pub const fn seed_units() -> Vec<SeedSpec> {
    Vec::new()
}

fn event_table_name() -> String {
    Event::Table.to_string()
}
