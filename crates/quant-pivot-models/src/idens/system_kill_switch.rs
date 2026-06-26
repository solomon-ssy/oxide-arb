//! `system_kill_switch` table — execution operational-control singleton.

use quant_pivot_macros::quant_schema;
use sea_orm::sea_query::{ColumnDef, Table, TableCreateStatement};

use crate::{
    enums::execution::KillSwitchState,
    schema::{
        column, dependency::TableDependency, index::IndexSpec, seed::SeedSpec,
        timestamp_with_write_default,
    },
    seed::system_kill_switch,
};

#[quant_schema(lifecycle = "core")]
pub enum SystemKillSwitch {
    Table,
    Id,
    State,
    ChangedBy,
    Reason,
    RequiresOperatorAck,
    ChangedAt,
    UpdatedAt,
}

pub fn table() -> TableCreateStatement {
    Table::create()
        .table(SystemKillSwitch::Table)
        .if_not_exists()
        .col(column::singleton_pk(SystemKillSwitch::Id))
        .col(column::pg_enum_default::<KillSwitchState>(
            SystemKillSwitch::State,
            &KillSwitchState::Closed,
        ))
        .col(
            ColumnDef::new(SystemKillSwitch::ChangedBy)
                .text()
                .not_null()
                .default("bootstrap"),
        )
        .col(
            ColumnDef::new(SystemKillSwitch::Reason)
                .text()
                .not_null()
                .default("bootstrap seed"),
        )
        .col(
            ColumnDef::new(SystemKillSwitch::RequiresOperatorAck)
                .boolean()
                .not_null()
                .default(false),
        )
        .col(timestamp_with_write_default(SystemKillSwitch::ChangedAt))
        .col(timestamp_with_write_default(SystemKillSwitch::UpdatedAt))
        .to_owned()
}

pub const fn indexes() -> Vec<IndexSpec> {
    Vec::new()
}

pub const fn dependencies() -> Vec<TableDependency> {
    Vec::new()
}

pub fn seed_units() -> Vec<SeedSpec> {
    vec![system_kill_switch::SYSTEM_KILL_SWITCH_SEED]
}
