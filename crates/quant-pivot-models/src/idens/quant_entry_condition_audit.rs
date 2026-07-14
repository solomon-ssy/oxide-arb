use quant_pivot_macros::quant_schema;
use sea_orm::{
    Iden,
    sea_query::{
        ColumnDef, ForeignKey, ForeignKeyAction, Index, IndexOrder, Table, TableCreateStatement,
    },
};

use crate::{
    enums::quant::{EntryConditionAuditAction, EntryConditionState},
    idens::quant_entry_condition_instance::QuantEntryConditionInstance,
    schema::{
        column,
        dependency::TableDependency,
        index::{IndexBuildMode, IndexSpec},
        seed::SeedSpec,
        timestamp_with_write_default,
    },
};

#[quant_schema(lifecycle = "ledger")]
pub enum QuantEntryConditionAudit {
    Table,
    AuditId,
    ConditionInstanceId,
    Revision,
    Action,
    FromState,
    ToState,
    TruthJson,
    EvaluationHash,
    InputFingerprint,
    ContinuityHash,
    LeaseEpoch,
    Detail,
    OccurredAt,
    CreatedAt,
}

pub fn table() -> TableCreateStatement {
    Table::create()
        .table(QuantEntryConditionAudit::Table)
        .if_not_exists()
        .col(column::uuid_pk(QuantEntryConditionAudit::AuditId))
        .col(column::uuid_fk(
            QuantEntryConditionAudit::ConditionInstanceId,
        ))
        .col(
            ColumnDef::new(QuantEntryConditionAudit::Revision)
                .big_integer()
                .not_null(),
        )
        .col(column::pg_enum::<EntryConditionAuditAction>(
            QuantEntryConditionAudit::Action,
        ))
        .col(column::pg_enum_null::<EntryConditionState>(
            QuantEntryConditionAudit::FromState,
        ))
        .col(column::pg_enum::<EntryConditionState>(
            QuantEntryConditionAudit::ToState,
        ))
        .col(
            ColumnDef::new(QuantEntryConditionAudit::TruthJson)
                .json_binary()
                .null(),
        )
        .col(
            ColumnDef::new(QuantEntryConditionAudit::EvaluationHash)
                .text()
                .null(),
        )
        .col(
            ColumnDef::new(QuantEntryConditionAudit::InputFingerprint)
                .text()
                .null(),
        )
        .col(
            ColumnDef::new(QuantEntryConditionAudit::ContinuityHash)
                .text()
                .null(),
        )
        .col(
            ColumnDef::new(QuantEntryConditionAudit::LeaseEpoch)
                .big_integer()
                .not_null(),
        )
        .col(
            ColumnDef::new(QuantEntryConditionAudit::Detail)
                .text()
                .null(),
        )
        .col(
            ColumnDef::new(QuantEntryConditionAudit::OccurredAt)
                .timestamp_with_time_zone()
                .not_null(),
        )
        .col(timestamp_with_write_default(
            QuantEntryConditionAudit::CreatedAt,
        ))
        .foreign_key(
            ForeignKey::create()
                .name("fk_quant_entry_condition_audit_instance")
                .from(
                    QuantEntryConditionAudit::Table,
                    QuantEntryConditionAudit::ConditionInstanceId,
                )
                .to(
                    QuantEntryConditionInstance::Table,
                    QuantEntryConditionInstance::ConditionInstanceId,
                )
                .on_delete(ForeignKeyAction::Restrict),
        )
        .to_owned()
}

pub fn indexes() -> Vec<IndexSpec> {
    vec![IndexSpec::sea_query(
        "idx_quant_entry_condition_audit_timeline",
        table_name,
        IndexBuildMode::Transactional,
        Index::create()
            .name("idx_quant_entry_condition_audit_timeline")
            .table(QuantEntryConditionAudit::Table)
            .col(QuantEntryConditionAudit::ConditionInstanceId)
            .col((QuantEntryConditionAudit::Revision, IndexOrder::Asc))
            .unique()
            .to_owned(),
        "one immutable audit row per instance revision",
    )]
}

pub fn dependencies() -> Vec<TableDependency> {
    vec![TableDependency::foreign_key(instance_table_name)]
}

pub const fn seed_units() -> Vec<SeedSpec> {
    Vec::new()
}

fn table_name() -> String {
    QuantEntryConditionAudit::Table.to_string()
}

fn instance_table_name() -> String {
    QuantEntryConditionInstance::Table.to_string()
}
