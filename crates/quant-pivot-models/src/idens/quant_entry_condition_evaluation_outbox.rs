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
pub enum QuantEntryConditionEvaluationOutbox {
    Table,
    OutboxId,
    EvaluationId,
    EventJson,
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
        .table(QuantEntryConditionEvaluationOutbox::Table)
        .if_not_exists()
        .col(column::uuid_pk(
            QuantEntryConditionEvaluationOutbox::OutboxId,
        ))
        .col(
            ColumnDef::new(QuantEntryConditionEvaluationOutbox::EvaluationId)
                .text()
                .not_null(),
        )
        .col(
            ColumnDef::new(QuantEntryConditionEvaluationOutbox::EventJson)
                .json_binary()
                .not_null(),
        )
        .col(
            ColumnDef::new(QuantEntryConditionEvaluationOutbox::PublishedAt)
                .timestamp_with_time_zone()
                .null(),
        )
        .col(
            ColumnDef::new(QuantEntryConditionEvaluationOutbox::PublishAttempts)
                .integer()
                .not_null()
                .default(0),
        )
        .col(column::uuid_null(
            QuantEntryConditionEvaluationOutbox::ClaimOwner,
        ))
        .col(
            ColumnDef::new(QuantEntryConditionEvaluationOutbox::LeaseExpiresAt)
                .timestamp_with_time_zone()
                .null(),
        )
        .col(
            ColumnDef::new(QuantEntryConditionEvaluationOutbox::LastError)
                .text()
                .null(),
        )
        .col(timestamp_with_write_default(
            QuantEntryConditionEvaluationOutbox::CreatedAt,
        ))
        .col(timestamp_with_write_default(
            QuantEntryConditionEvaluationOutbox::UpdatedAt,
        ))
        .to_owned()
}

pub fn indexes() -> Vec<IndexSpec> {
    vec![
        IndexSpec::sea_query(
            "uq_quant_entry_condition_evaluation_outbox_evaluation",
            table_name,
            IndexBuildMode::Transactional,
            Index::create()
                .name("uq_quant_entry_condition_evaluation_outbox_evaluation")
                .table(QuantEntryConditionEvaluationOutbox::Table)
                .col(QuantEntryConditionEvaluationOutbox::EvaluationId)
                .unique()
                .to_owned(),
            "one durable delivery row per deterministic evaluation id",
        ),
        IndexSpec::sea_query(
            "idx_quant_entry_condition_evaluation_outbox_pending",
            table_name,
            IndexBuildMode::Transactional,
            Index::create()
                .name("idx_quant_entry_condition_evaluation_outbox_pending")
                .table(QuantEntryConditionEvaluationOutbox::Table)
                .col(QuantEntryConditionEvaluationOutbox::PublishedAt)
                .col(QuantEntryConditionEvaluationOutbox::LeaseExpiresAt)
                .col(QuantEntryConditionEvaluationOutbox::CreatedAt)
                .to_owned(),
            "durable pending entry-condition evaluation delivery queue",
        ),
    ]
}

pub const fn dependencies() -> Vec<TableDependency> {
    Vec::new()
}

pub const fn seed_units() -> Vec<SeedSpec> {
    Vec::new()
}

fn table_name() -> String {
    QuantEntryConditionEvaluationOutbox::Table.to_string()
}
