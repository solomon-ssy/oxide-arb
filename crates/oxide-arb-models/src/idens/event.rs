use oxide_arb_macros::oxide_schema;
use sea_orm::{
    Iden,
    sea_query::{ColumnDef, Index, Table, TableCreateStatement},
};

use crate::{
    enums::market::EventStatus,
    schema::{
        dependency::TableDependency,
        index::{IndexBuildMode, IndexSpec},
        seed::SeedSpec,
    },
};

#[oxide_schema]
pub enum Event {
    Table,
    EventId,
    Title,
    Slug,
    Category,
    Status,
    NegRisk,
    EndDate,
    RawGamma,
    CreatedAt,
    UpdatedAt,
}

pub fn table() -> TableCreateStatement {
    Table::create()
        .table(Event::Table)
        .if_not_exists()
        .col(
            ColumnDef::new(Event::EventId)
                .text()
                .not_null()
                .primary_key(),
        )
        .col(ColumnDef::new(Event::Title).text().not_null())
        .col(ColumnDef::new(Event::Slug).text().not_null())
        .col(ColumnDef::new(Event::Category).text().not_null())
        .col(
            ColumnDef::new(Event::Status)
                .text()
                .not_null()
                .default(EventStatus::Active),
        )
        .col(
            ColumnDef::new(Event::NegRisk)
                .boolean()
                .not_null()
                .default(false),
        )
        .col(
            ColumnDef::new(Event::EndDate)
                .timestamp_with_time_zone()
                .null(),
        )
        .col(ColumnDef::new(Event::RawGamma).json_binary().null())
        .col(crate::schema::timestamp_with_write_default(
            Event::CreatedAt,
        ))
        .col(crate::schema::timestamp_with_write_default(
            Event::UpdatedAt,
        ))
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
        IndexSpec::sea_query(
            "idx_events_category",
            event_table_name,
            IndexBuildMode::Transactional,
            Index::create()
                .name("idx_events_category")
                .table(Event::Table)
                .col(Event::Category)
                .to_owned(),
            "event category filters",
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
