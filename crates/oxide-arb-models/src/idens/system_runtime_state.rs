//! `system_runtime_state` table — process-level operational control singleton.
//!
//! Holds the **active execution mode** and the metadata of the last change. It
//! is deliberately separate from `risk_engine_state`: that table is the risk
//! engine's high-write snapshot (single writer = the engine), whereas this row
//! is a low-write operational control set only by the web control plane. Keeping
//! them apart preserves the single-writer-per-row invariant and avoids coupling
//! unrelated aggregates.

use crate::{
    enums::common::ExecutionMode,
    schema::{
        column, dependency::TableDependency, index::IndexSpec, seed::SeedSpec,
        timestamp_with_write_default,
    },
    seed::system_runtime_state,
};
use oxide_arb_macros::oxide_schema;
use sea_orm::sea_query::{ColumnDef, Table, TableCreateStatement};

#[oxide_schema(lifecycle = "core")]
pub enum SystemRuntimeState {
    Table,
    Id,
    ExecutionMode,
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
        .col(
            ColumnDef::new(SystemRuntimeState::ExecutionMode)
                .text()
                .not_null()
                .default(ExecutionMode::DryRun),
        )
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

/// Seed the singleton in the safest mode (`DryRun`).
///
/// Uses `ON CONFLICT DO NOTHING`, so a fresh database always has a deterministic
/// row and a re-migration never overwrites an operator's deliberate mode.
/// Escalation beyond `DryRun` happens only via the governed `/system/mode`
/// transition.
pub fn seed_units() -> Vec<SeedSpec> {
    vec![system_runtime_state::SYSTEM_RUNTIME_STATE_SEED]
}
