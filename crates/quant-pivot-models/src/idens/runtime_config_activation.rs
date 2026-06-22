use quant_pivot_macros::quant_schema;
use sea_orm::{
    Iden,
    sea_query::{
        ColumnDef, ForeignKey, ForeignKeyAction, ForeignKeyCreateStatement, Index, IndexOrder,
        Table, TableCreateStatement,
    },
};

use crate::{
    idens::{
        control_factor_audit_event::ControlFactorAuditEvent,
        runtime_config_version::RuntimeConfigVersion,
    },
    schema::{
        column,
        dependency::TableDependency,
        index::{IndexBuildMode, IndexSpec},
        seed::SeedSpec,
        timestamp_with_write_default,
    },
};

#[quant_schema(lifecycle = "control")]
pub enum RuntimeConfigActivation {
    Table,
    RuntimeConfigActivationId,
    RuntimeConfigVersionId,
    ActivatedAt,
    ActivatedBy,
    Reason,
    ActivationKind,
    PreviousRuntimeConfigVersionId,
    RollbackTargetVersionId,
    AuditEventId,
    CreatedAt,
}

pub fn table() -> TableCreateStatement {
    let mut table = Table::create()
        .table(RuntimeConfigActivation::Table)
        .if_not_exists()
        .col(column::uuid_pk(
            RuntimeConfigActivation::RuntimeConfigActivationId,
        ))
        .col(column::uuid_fk(
            RuntimeConfigActivation::RuntimeConfigVersionId,
        ))
        .col(
            ColumnDef::new(RuntimeConfigActivation::ActivatedAt)
                .timestamp_with_time_zone()
                .not_null(),
        )
        .col(
            ColumnDef::new(RuntimeConfigActivation::ActivatedBy)
                .text()
                .not_null(),
        )
        .col(
            ColumnDef::new(RuntimeConfigActivation::Reason)
                .text()
                .not_null(),
        )
        .col(
            ColumnDef::new(RuntimeConfigActivation::ActivationKind)
                .text()
                .not_null(),
        )
        .col(column::uuid_null(
            RuntimeConfigActivation::PreviousRuntimeConfigVersionId,
        ))
        .col(column::uuid_null(
            RuntimeConfigActivation::RollbackTargetVersionId,
        ))
        .col(column::uuid_null(RuntimeConfigActivation::AuditEventId))
        .col(timestamp_with_write_default(
            RuntimeConfigActivation::CreatedAt,
        ))
        .to_owned();
    let mut version = version_fk();
    let mut previous = previous_version_fk();
    let mut rollback = rollback_target_fk();
    let mut audit = audit_event_fk();
    table
        .foreign_key(&mut version)
        .foreign_key(&mut previous)
        .foreign_key(&mut rollback)
        .foreign_key(&mut audit);
    table
}

fn version_fk() -> ForeignKeyCreateStatement {
    ForeignKey::create()
        .name("fk_runtime_config_activation_version")
        .from(
            RuntimeConfigActivation::Table,
            RuntimeConfigActivation::RuntimeConfigVersionId,
        )
        .to(
            RuntimeConfigVersion::Table,
            RuntimeConfigVersion::RuntimeConfigVersionId,
        )
        .on_delete(ForeignKeyAction::Restrict)
        .to_owned()
}

fn previous_version_fk() -> ForeignKeyCreateStatement {
    ForeignKey::create()
        .name("fk_runtime_config_activation_previous_version")
        .from(
            RuntimeConfigActivation::Table,
            RuntimeConfigActivation::PreviousRuntimeConfigVersionId,
        )
        .to(
            RuntimeConfigVersion::Table,
            RuntimeConfigVersion::RuntimeConfigVersionId,
        )
        .on_delete(ForeignKeyAction::SetNull)
        .to_owned()
}

fn rollback_target_fk() -> ForeignKeyCreateStatement {
    ForeignKey::create()
        .name("fk_runtime_config_activation_rollback_target")
        .from(
            RuntimeConfigActivation::Table,
            RuntimeConfigActivation::RollbackTargetVersionId,
        )
        .to(
            RuntimeConfigVersion::Table,
            RuntimeConfigVersion::RuntimeConfigVersionId,
        )
        .on_delete(ForeignKeyAction::SetNull)
        .to_owned()
}

fn audit_event_fk() -> ForeignKeyCreateStatement {
    ForeignKey::create()
        .name("fk_runtime_config_activation_audit_event")
        .from(
            RuntimeConfigActivation::Table,
            RuntimeConfigActivation::AuditEventId,
        )
        .to(
            ControlFactorAuditEvent::Table,
            ControlFactorAuditEvent::EventId,
        )
        .on_delete(ForeignKeyAction::SetNull)
        .to_owned()
}

pub fn indexes() -> Vec<IndexSpec> {
    vec![
        IndexSpec::sea_query(
            "idx_runtime_config_activation_activated_at",
            runtime_config_activation_table_name,
            IndexBuildMode::Transactional,
            Index::create()
                .name("idx_runtime_config_activation_activated_at")
                .table(RuntimeConfigActivation::Table)
                .col((RuntimeConfigActivation::ActivatedAt, IndexOrder::Desc))
                .to_owned(),
            "runtime config activation PIT lookup",
        ),
        IndexSpec::sea_query(
            "idx_runtime_config_activation_version",
            runtime_config_activation_table_name,
            IndexBuildMode::Transactional,
            Index::create()
                .name("idx_runtime_config_activation_version")
                .table(RuntimeConfigActivation::Table)
                .col(RuntimeConfigActivation::RuntimeConfigVersionId)
                .to_owned(),
            "runtime config activation lookup by version",
        ),
    ]
}

pub fn dependencies() -> Vec<TableDependency> {
    vec![
        TableDependency::foreign_key(runtime_config_version_table_name),
        TableDependency::foreign_key(control_factor_audit_event_table_name),
    ]
}

pub const fn seed_units() -> Vec<SeedSpec> {
    Vec::new()
}

fn runtime_config_activation_table_name() -> String {
    RuntimeConfigActivation::Table.to_string()
}

fn runtime_config_version_table_name() -> String {
    RuntimeConfigVersion::Table.to_string()
}

fn control_factor_audit_event_table_name() -> String {
    ControlFactorAuditEvent::Table.to_string()
}
