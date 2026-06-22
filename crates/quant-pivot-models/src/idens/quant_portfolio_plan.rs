use quant_pivot_macros::quant_schema;
use sea_orm::{
    Iden,
    sea_query::{
        ColumnDef, ForeignKey, ForeignKeyAction, Index, IndexOrder, Table, TableCreateStatement,
    },
};

use crate::{
    idens::{quant_model_run::QuantModelRun, quant_universe_snapshot::QuantUniverseSnapshot},
    schema::{
        column,
        dependency::TableDependency,
        index::{IndexBuildMode, IndexSpec},
        seed::SeedSpec,
        timestamp_with_write_default,
    },
};

#[quant_schema(lifecycle = "report")]
pub enum QuantPortfolioPlan {
    Table,
    PortfolioPlanId,
    ModelRunId,
    UniverseSnapshotId,
    AsOf,
    BudgetUsd,
    AllocatedUsd,
    RiskBudgetJson,
    ConstraintsJson,
    RejectedSummary,
    CreatedAt,
}

pub fn table() -> TableCreateStatement {
    Table::create()
        .table(QuantPortfolioPlan::Table)
        .if_not_exists()
        .col(column::uuid_pk(QuantPortfolioPlan::PortfolioPlanId))
        .col(column::uuid_fk(QuantPortfolioPlan::ModelRunId))
        .col(column::uuid_fk(QuantPortfolioPlan::UniverseSnapshotId))
        .col(
            ColumnDef::new(QuantPortfolioPlan::AsOf)
                .timestamp_with_time_zone()
                .not_null(),
        )
        .col(column::usd(QuantPortfolioPlan::BudgetUsd))
        .col(column::usd(QuantPortfolioPlan::AllocatedUsd))
        .col(
            ColumnDef::new(QuantPortfolioPlan::RiskBudgetJson)
                .json_binary()
                .not_null(),
        )
        .col(
            ColumnDef::new(QuantPortfolioPlan::ConstraintsJson)
                .json_binary()
                .not_null(),
        )
        .col(
            ColumnDef::new(QuantPortfolioPlan::RejectedSummary)
                .json_binary()
                .not_null(),
        )
        .col(timestamp_with_write_default(QuantPortfolioPlan::CreatedAt))
        .foreign_key(
            ForeignKey::create()
                .name("fk_quant_portfolio_plan_model_run")
                .from(QuantPortfolioPlan::Table, QuantPortfolioPlan::ModelRunId)
                .to(QuantModelRun::Table, QuantModelRun::ModelRunId)
                .on_delete(ForeignKeyAction::Restrict),
        )
        .foreign_key(
            ForeignKey::create()
                .name("fk_quant_portfolio_plan_universe")
                .from(
                    QuantPortfolioPlan::Table,
                    QuantPortfolioPlan::UniverseSnapshotId,
                )
                .to(
                    QuantUniverseSnapshot::Table,
                    QuantUniverseSnapshot::UniverseSnapshotId,
                )
                .on_delete(ForeignKeyAction::Restrict),
        )
        .to_owned()
}

pub fn indexes() -> Vec<IndexSpec> {
    vec![IndexSpec::sea_query(
        "idx_quant_portfolio_plan_as_of",
        quant_portfolio_plan_table_name,
        IndexBuildMode::Transactional,
        Index::create()
            .name("idx_quant_portfolio_plan_as_of")
            .table(QuantPortfolioPlan::Table)
            .col((QuantPortfolioPlan::AsOf, IndexOrder::Desc))
            .to_owned(),
        "portfolio plans by PIT timestamp",
    )]
}

pub fn dependencies() -> Vec<TableDependency> {
    vec![
        TableDependency::foreign_key(quant_model_run_table_name),
        TableDependency::foreign_key(quant_universe_snapshot_table_name),
    ]
}

pub const fn seed_units() -> Vec<SeedSpec> {
    Vec::new()
}

fn quant_portfolio_plan_table_name() -> String {
    QuantPortfolioPlan::Table.to_string()
}

fn quant_model_run_table_name() -> String {
    QuantModelRun::Table.to_string()
}

fn quant_universe_snapshot_table_name() -> String {
    QuantUniverseSnapshot::Table.to_string()
}
