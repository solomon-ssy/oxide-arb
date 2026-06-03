use oxide_arb_macros::oxide_schema;
use sea_orm::{
    Iden,
    sea_query::{ColumnDef, Index, IndexOrder, Table, TableCreateStatement},
};

use crate::schema::{
    dependency::TableDependency,
    index::{IndexBuildMode, IndexSpec},
    seed::SeedSpec,
    timestamp_with_write_default,
};

#[oxide_schema(lifecycle = "audit")]
pub enum PositionExitExecution {
    Table,
    ExitExecutionId,
    ExitPlanId,
    OrderId,
    OrderType,
    RequestedShares,
    FilledShares,
    AvgExitPrice,
    FeeUsd,
    RealizedExitPnlUsd,
    Outcome,
    FailureReason,
    SubmittedAt,
    CompletedAt,
    CreatedAt,
}

pub fn table() -> TableCreateStatement {
    Table::create()
        .table(PositionExitExecution::Table)
        .if_not_exists()
        .col(
            ColumnDef::new(PositionExitExecution::ExitExecutionId)
                .text()
                .not_null()
                .primary_key(),
        )
        .col(
            ColumnDef::new(PositionExitExecution::ExitPlanId)
                .text()
                .not_null(),
        )
        .col(ColumnDef::new(PositionExitExecution::OrderId).text().null())
        .col(
            ColumnDef::new(PositionExitExecution::OrderType)
                .text()
                .not_null(),
        )
        .col(
            ColumnDef::new(PositionExitExecution::RequestedShares)
                .text()
                .not_null(),
        )
        .col(
            ColumnDef::new(PositionExitExecution::FilledShares)
                .text()
                .not_null(),
        )
        .col(
            ColumnDef::new(PositionExitExecution::AvgExitPrice)
                .text()
                .null(),
        )
        .col(
            ColumnDef::new(PositionExitExecution::FeeUsd)
                .text()
                .not_null(),
        )
        .col(
            ColumnDef::new(PositionExitExecution::RealizedExitPnlUsd)
                .text()
                .not_null(),
        )
        .col(
            ColumnDef::new(PositionExitExecution::Outcome)
                .text()
                .not_null(),
        )
        .col(
            ColumnDef::new(PositionExitExecution::FailureReason)
                .text()
                .null(),
        )
        .col(
            ColumnDef::new(PositionExitExecution::SubmittedAt)
                .timestamp_with_time_zone()
                .null(),
        )
        .col(
            ColumnDef::new(PositionExitExecution::CompletedAt)
                .timestamp_with_time_zone()
                .null(),
        )
        .col(timestamp_with_write_default(
            PositionExitExecution::CreatedAt,
        ))
        .to_owned()
}

pub fn indexes() -> Vec<IndexSpec> {
    vec![IndexSpec::sea_query(
        "idx_position_exit_execution_plan_created",
        position_exit_execution_table_name,
        IndexBuildMode::Transactional,
        Index::create()
            .name("idx_position_exit_execution_plan_created")
            .table(PositionExitExecution::Table)
            .col(PositionExitExecution::ExitPlanId)
            .col((PositionExitExecution::CreatedAt, IndexOrder::Desc))
            .to_owned(),
        "exit executions by plan",
    )]
}

pub const fn dependencies() -> Vec<TableDependency> {
    Vec::new()
}

pub const fn seed_units() -> Vec<SeedSpec> {
    Vec::new()
}

fn position_exit_execution_table_name() -> String {
    PositionExitExecution::Table.to_string()
}
