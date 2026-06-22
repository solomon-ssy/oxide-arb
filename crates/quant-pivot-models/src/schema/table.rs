//! Table metadata for the schema catalog.

use sea_orm::sea_query::TableCreateStatement;

use super::{dependency::TableDependency, index::IndexSpec, seed::SeedSpec, trigger::TriggerSpec};

/// Lifecycle bucket used by migration lanes and docs.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TableLifecycle {
    Core,
    Control,
    Runtime,
    Ledger,
    Audit,
    Report,
    SeedLedger,
}

/// Compile-time metadata for one persisted table.
#[derive(Clone, Copy)]
pub struct TableSpec {
    pub rust_type: &'static str,
    pub table_name: fn() -> String,
    pub table: fn() -> TableCreateStatement,
    pub indexes: fn() -> Vec<IndexSpec>,
    pub dependencies: fn() -> Vec<TableDependency>,
    pub triggers: fn() -> Vec<TriggerSpec>,
    pub seed_units: fn() -> Vec<SeedSpec>,
    pub lifecycle: TableLifecycle,
}
