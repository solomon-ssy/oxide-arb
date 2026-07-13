use quant_pivot_macros::quant_schema;
use sea_orm::{
    Iden,
    sea_query::{
        ColumnDef, ForeignKey, ForeignKeyAction, Index, IndexOrder, Table, TableCreateStatement,
    },
};

use crate::{
    enums::quant::{FeatureParityLatchState, FeatureParityStateTransition},
    idens::quant_feature_parity_run::QuantFeatureParityRun,
    schema::{
        column,
        dependency::TableDependency,
        index::{IndexBuildMode, IndexSpec},
        seed::SeedSpec,
        timestamp_with_write_default,
    },
};

// Append-only, governed admission-latch history. No row means uninitialized and
// therefore fail-closed. Every open/clear decision remains reconstructable.
#[quant_schema(lifecycle = "audit")]
pub enum QuantFeatureParityState {
    Table,
    StateId,
    State,
    Transition,
    CauseRunId,
    RecoveryRunId,
    PreviousStateId,
    Actor,
    ActingRole,
    Reason,
    CreatedAt,
}

pub fn table() -> TableCreateStatement {
    Table::create()
        .table(QuantFeatureParityState::Table)
        .if_not_exists()
        .col(column::uuid_pk(QuantFeatureParityState::StateId))
        .col(column::pg_enum::<FeatureParityLatchState>(
            QuantFeatureParityState::State,
        ))
        .col(column::pg_enum::<FeatureParityStateTransition>(
            QuantFeatureParityState::Transition,
        ))
        .col(column::uuid_null(QuantFeatureParityState::CauseRunId))
        .col(column::uuid_null(QuantFeatureParityState::RecoveryRunId))
        .col(column::uuid_null(QuantFeatureParityState::PreviousStateId))
        .col(ColumnDef::new(QuantFeatureParityState::Actor).text().null())
        .col(
            ColumnDef::new(QuantFeatureParityState::ActingRole)
                .text()
                .null(),
        )
        .col(
            ColumnDef::new(QuantFeatureParityState::Reason)
                .text()
                .not_null(),
        )
        .col(timestamp_with_write_default(
            QuantFeatureParityState::CreatedAt,
        ))
        .foreign_key(
            ForeignKey::create()
                .name("fk_quant_feature_parity_state_cause_run")
                .from(
                    QuantFeatureParityState::Table,
                    QuantFeatureParityState::CauseRunId,
                )
                .to(QuantFeatureParityRun::Table, QuantFeatureParityRun::RunId)
                .on_delete(ForeignKeyAction::Restrict),
        )
        .foreign_key(
            ForeignKey::create()
                .name("fk_quant_feature_parity_state_recovery_run")
                .from(
                    QuantFeatureParityState::Table,
                    QuantFeatureParityState::RecoveryRunId,
                )
                .to(QuantFeatureParityRun::Table, QuantFeatureParityRun::RunId)
                .on_delete(ForeignKeyAction::Restrict),
        )
        .foreign_key(
            ForeignKey::create()
                .name("fk_quant_feature_parity_state_previous")
                .from(
                    QuantFeatureParityState::Table,
                    QuantFeatureParityState::PreviousStateId,
                )
                .to(
                    QuantFeatureParityState::Table,
                    QuantFeatureParityState::StateId,
                )
                .on_delete(ForeignKeyAction::Restrict),
        )
        .to_owned()
}

pub fn indexes() -> Vec<IndexSpec> {
    vec![IndexSpec::sea_query(
        "idx_quant_feature_parity_state_created",
        quant_feature_parity_state_table_name,
        IndexBuildMode::Transactional,
        Index::create()
            .name("idx_quant_feature_parity_state_created")
            .table(QuantFeatureParityState::Table)
            .col((QuantFeatureParityState::CreatedAt, IndexOrder::Desc))
            .col((QuantFeatureParityState::StateId, IndexOrder::Desc))
            .to_owned(),
        "current latch state and immutable transition history",
    )]
}

pub fn dependencies() -> Vec<TableDependency> {
    vec![TableDependency::foreign_key(
        quant_feature_parity_run_table_name,
    )]
}

pub const fn seed_units() -> Vec<SeedSpec> {
    Vec::new()
}

fn quant_feature_parity_state_table_name() -> String {
    QuantFeatureParityState::Table.to_string()
}

fn quant_feature_parity_run_table_name() -> String {
    QuantFeatureParityRun::Table.to_string()
}
