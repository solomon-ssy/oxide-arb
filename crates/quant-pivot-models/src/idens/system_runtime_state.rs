//! `system_runtime_state` table — process-level operational control singleton.
//!
//! Holds the active **quant runtime mode** and metadata of the last change.

use crate::{
    enums::quant::QuantRuntimeMode,
    schema::{
        column, dependency::TableDependency, index::IndexSpec, seed::SeedSpec,
        timestamp_with_write_default,
    },
    seed::system_runtime_state,
};
use quant_pivot_macros::quant_schema;
use sea_orm::sea_query::{ColumnDef, Table, TableCreateStatement};

#[quant_schema(lifecycle = "core")]
pub enum SystemRuntimeState {
    Table,
    Id,
    QuantRuntimeMode,
    ChangedBy,
    Reason,
    ChangedAt,
    UpdatedAt,
}

pub fn table() -> TableCreateStatement {
    Table::create()
        .table(SystemRuntimeState::Table)
        .if_not_exists()
        .col(column::singleton_pk(SystemRuntimeState::Id))
        .col(column::pg_enum_default::<QuantRuntimeMode>(
            SystemRuntimeState::QuantRuntimeMode,
            &QuantRuntimeMode::ReportOnly,
        ))
        .col(
            ColumnDef::new(SystemRuntimeState::ChangedBy)
                .text()
                .not_null()
                .default("bootstrap"),
        )
        .col(
            ColumnDef::new(SystemRuntimeState::Reason)
                .text()
                .not_null()
                .default("bootstrap seed"),
        )
        .col(timestamp_with_write_default(SystemRuntimeState::ChangedAt))
        .col(timestamp_with_write_default(SystemRuntimeState::UpdatedAt))
        .to_owned()
}

pub const fn indexes() -> Vec<IndexSpec> {
    Vec::new()
}

pub const fn dependencies() -> Vec<TableDependency> {
    Vec::new()
}

/// Seed the singleton in the safest mode (`ReportOnly`).
pub fn seed_units() -> Vec<SeedSpec> {
    vec![system_runtime_state::SYSTEM_RUNTIME_STATE_SEED]
}
