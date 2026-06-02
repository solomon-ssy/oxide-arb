use oxide_arb_macros::oxide_schema;
use sea_orm::{
    Iden,
    sea_query::{
        ColumnDef, ForeignKey, ForeignKeyAction, Index, IndexOrder, Table, TableCreateStatement,
    },
};

use crate::{
    idens::{
        control_factor_publication::ControlFactorPublication,
        control_factor_value::ControlFactorValue,
    },
    schema::{
        dependency::TableDependency,
        index::{IndexBuildMode, IndexSpec},
        seed::SeedSpec,
        timestamp_with_write_default,
    },
};

#[oxide_schema(lifecycle = "audit")]
pub enum ControlFactorAuditEvent {
    Table,
    Id,
    EventType,
    FactorId,
    PublicationId,
    Actor,
    Reason,
    Payload,
    CreatedAt,
}

pub fn table() -> TableCreateStatement {
    Table::create()
        .table(ControlFactorAuditEvent::Table)
        .if_not_exists()
        .col(
            ColumnDef::new(ControlFactorAuditEvent::Id)
                .big_integer()
                .not_null()
                .auto_increment()
                .primary_key(),
        )
        .col(
            ColumnDef::new(ControlFactorAuditEvent::EventType)
                .text()
                .not_null(),
        )
        .col(
            ColumnDef::new(ControlFactorAuditEvent::FactorId)
                .text()
                .null(),
        )
        .col(
            ColumnDef::new(ControlFactorAuditEvent::PublicationId)
                .text()
                .null(),
        )
        .col(
            ColumnDef::new(ControlFactorAuditEvent::Actor)
                .text()
                .not_null(),
        )
        .col(
            ColumnDef::new(ControlFactorAuditEvent::Reason)
                .text()
                .not_null(),
        )
        .col(
            ColumnDef::new(ControlFactorAuditEvent::Payload)
                .json_binary()
                .not_null(),
        )
        .col(timestamp_with_write_default(
            ControlFactorAuditEvent::CreatedAt,
        ))
        .foreign_key(
            ForeignKey::create()
                .name("fk_control_factor_audit_event_factor")
                .from(
                    ControlFactorAuditEvent::Table,
                    ControlFactorAuditEvent::FactorId,
                )
                .to(ControlFactorValue::Table, ControlFactorValue::FactorId)
                .on_delete(ForeignKeyAction::SetNull),
        )
        .foreign_key(
            ForeignKey::create()
                .name("fk_control_factor_audit_event_publication")
                .from(
                    ControlFactorAuditEvent::Table,
                    ControlFactorAuditEvent::PublicationId,
                )
                .to(
                    ControlFactorPublication::Table,
                    ControlFactorPublication::PublicationId,
                )
                .on_delete(ForeignKeyAction::SetNull),
        )
        .to_owned()
}

pub fn indexes() -> Vec<IndexSpec> {
    vec![IndexSpec::sea_query(
        "idx_control_factor_audit_event_created_at",
        audit_event_table_name,
        IndexBuildMode::Transactional,
        Index::create()
            .name("idx_control_factor_audit_event_created_at")
            .table(ControlFactorAuditEvent::Table)
            .col((ControlFactorAuditEvent::CreatedAt, IndexOrder::Desc))
            .to_owned(),
        "control-factor audit events by recency",
    )]
}

pub fn dependencies() -> Vec<TableDependency> {
    vec![
        TableDependency::foreign_key(factor_value_table_name),
        TableDependency::foreign_key(publication_table_name),
    ]
}

pub const fn seed_units() -> Vec<SeedSpec> {
    Vec::new()
}

fn audit_event_table_name() -> String {
    ControlFactorAuditEvent::Table.to_string()
}

fn factor_value_table_name() -> String {
    ControlFactorValue::Table.to_string()
}

fn publication_table_name() -> String {
    ControlFactorPublication::Table.to_string()
}
