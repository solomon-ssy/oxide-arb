use oxide_arb_macros::oxide_schema;
use sea_orm::{
    Iden,
    sea_query::{ColumnDef, Table, TableCreateStatement},
};

use crate::schema::{
    dependency::TableDependency,
    index::{IndexBuildMode, IndexSpec},
    seed::SeedSpec,
};

#[oxide_schema]
pub enum OutboxEvent {
    Table,
    EventId,
    AggregateType,
    AggregateId,
    EventType,
    Payload,
    PublishAttempts,
    PublishedAt,
    LastError,
    CreatedAt,
    DeadLetterReason,
}

pub fn table() -> TableCreateStatement {
    Table::create()
        .table(OutboxEvent::Table)
        .if_not_exists()
        .col(
            ColumnDef::new(OutboxEvent::EventId)
                .text()
                .not_null()
                .primary_key(),
        )
        .col(ColumnDef::new(OutboxEvent::AggregateType).text().not_null())
        .col(ColumnDef::new(OutboxEvent::AggregateId).text().not_null())
        .col(ColumnDef::new(OutboxEvent::EventType).text().not_null())
        .col(
            ColumnDef::new(OutboxEvent::Payload)
                .json_binary()
                .not_null(),
        )
        .col(
            ColumnDef::new(OutboxEvent::PublishAttempts)
                .integer()
                .not_null()
                .default(0),
        )
        .col(
            ColumnDef::new(OutboxEvent::PublishedAt)
                .timestamp_with_time_zone()
                .null(),
        )
        .col(ColumnDef::new(OutboxEvent::LastError).text().null())
        .col(ColumnDef::new(OutboxEvent::DeadLetterReason).text().null())
        .col(crate::schema::timestamp_with_write_default(
            OutboxEvent::CreatedAt,
        ))
        .to_owned()
}

pub fn indexes() -> Vec<IndexSpec> {
    vec![
        IndexSpec::raw(
            "idx_outbox_pending",
            outbox_event_table_name,
            IndexBuildMode::Transactional,
            "CREATE INDEX IF NOT EXISTS idx_outbox_pending \
             ON outbox_event (created_at) \
             WHERE published_at IS NULL",
            "pending outbox publication scan",
        ),
        IndexSpec::raw(
            "idx_outbox_unpublished_attempts",
            outbox_event_table_name,
            IndexBuildMode::Transactional,
            "CREATE INDEX IF NOT EXISTS idx_outbox_unpublished_attempts \
             ON outbox_event (publish_attempts, created_at) \
             WHERE published_at IS NULL AND dead_letter_reason IS NULL",
            "retryable unpublished outbox scan",
        ),
    ]
}

pub const fn dependencies() -> Vec<TableDependency> {
    Vec::new()
}

pub const fn seed_units() -> Vec<SeedSpec> {
    Vec::new()
}

fn outbox_event_table_name() -> String {
    OutboxEvent::Table.to_string()
}
