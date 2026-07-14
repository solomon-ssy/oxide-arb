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

#[quant_schema(lifecycle = "ledger")]
pub enum QuantDomainEventOutbox {
    Table,
    EventId,
    EnvelopeJson,
    PublishedAt,
    PublishAttempts,
    ClaimOwner,
    LeaseExpiresAt,
    LastError,
    CreatedAt,
    UpdatedAt,
}

pub fn table() -> TableCreateStatement {
    Table::create()
        .table(QuantDomainEventOutbox::Table)
        .if_not_exists()
        .col(column::uuid_pk(QuantDomainEventOutbox::EventId))
        .col(
            ColumnDef::new(QuantDomainEventOutbox::EnvelopeJson)
                .json_binary()
                .not_null(),
        )
        .col(
            ColumnDef::new(QuantDomainEventOutbox::PublishedAt)
                .timestamp_with_time_zone()
                .null(),
        )
        .col(
            ColumnDef::new(QuantDomainEventOutbox::PublishAttempts)
                .integer()
                .not_null()
                .default(0),
        )
        .col(column::uuid_null(QuantDomainEventOutbox::ClaimOwner))
        .col(
            ColumnDef::new(QuantDomainEventOutbox::LeaseExpiresAt)
                .timestamp_with_time_zone()
                .null(),
        )
        .col(
            ColumnDef::new(QuantDomainEventOutbox::LastError)
                .text()
                .null(),
        )
        .col(timestamp_with_write_default(
            QuantDomainEventOutbox::CreatedAt,
        ))
        .col(timestamp_with_write_default(
            QuantDomainEventOutbox::UpdatedAt,
        ))
        .to_owned()
}

pub fn indexes() -> Vec<IndexSpec> {
    vec![IndexSpec::sea_query(
        "idx_quant_domain_event_outbox_pending",
        table_name,
        IndexBuildMode::Transactional,
        Index::create()
            .name("idx_quant_domain_event_outbox_pending")
            .table(QuantDomainEventOutbox::Table)
            .col(QuantDomainEventOutbox::PublishedAt)
            .col(QuantDomainEventOutbox::LeaseExpiresAt)
            .col(QuantDomainEventOutbox::CreatedAt)
            .to_owned(),
        "durable pending domain-event delivery queue",
    )]
}

pub const fn dependencies() -> Vec<TableDependency> {
    Vec::new()
}
pub const fn seed_units() -> Vec<SeedSpec> {
    Vec::new()
}
fn table_name() -> String {
    QuantDomainEventOutbox::Table.to_string()
}
