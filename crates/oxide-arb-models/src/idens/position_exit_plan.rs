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

#[oxide_schema(lifecycle = "control")]
pub enum PositionExitPlan {
    Table,
    ExitPlanId,
    PositionId,
    MarketId,
    TokenId,
    TriggerType,
    Action,
    TargetShares,
    MinExitPrice,
    Reason,
    PolicyVersion,
    CreatedBy,
    Status,
    CreatedAt,
    UpdatedAt,
}

pub fn table() -> TableCreateStatement {
    Table::create()
        .table(PositionExitPlan::Table)
        .if_not_exists()
        .col(
            ColumnDef::new(PositionExitPlan::ExitPlanId)
                .text()
                .not_null()
                .primary_key(),
        )
        .col(
            ColumnDef::new(PositionExitPlan::PositionId)
                .text()
                .not_null(),
        )
        .col(ColumnDef::new(PositionExitPlan::MarketId).text().not_null())
        .col(ColumnDef::new(PositionExitPlan::TokenId).text().not_null())
        .col(
            ColumnDef::new(PositionExitPlan::TriggerType)
                .text()
                .not_null(),
        )
        .col(ColumnDef::new(PositionExitPlan::Action).text().not_null())
        .col(
            ColumnDef::new(PositionExitPlan::TargetShares)
                .text()
                .not_null(),
        )
        .col(
            ColumnDef::new(PositionExitPlan::MinExitPrice)
                .text()
                .not_null(),
        )
        .col(
            ColumnDef::new(PositionExitPlan::Reason)
                .json_binary()
                .not_null(),
        )
        .col(
            ColumnDef::new(PositionExitPlan::PolicyVersion)
                .text()
                .not_null(),
        )
        .col(
            ColumnDef::new(PositionExitPlan::CreatedBy)
                .text()
                .not_null(),
        )
        .col(ColumnDef::new(PositionExitPlan::Status).text().not_null())
        .col(timestamp_with_write_default(PositionExitPlan::CreatedAt))
        .col(timestamp_with_write_default(PositionExitPlan::UpdatedAt))
        .to_owned()
}

pub fn indexes() -> Vec<IndexSpec> {
    vec![IndexSpec::sea_query(
        "idx_position_exit_plan_position_status",
        position_exit_plan_table_name,
        IndexBuildMode::Transactional,
        Index::create()
            .name("idx_position_exit_plan_position_status")
            .table(PositionExitPlan::Table)
            .col(PositionExitPlan::PositionId)
            .col(PositionExitPlan::Status)
            .col((PositionExitPlan::CreatedAt, IndexOrder::Desc))
            .to_owned(),
        "exit plans by position and status",
    )]
}

pub const fn dependencies() -> Vec<TableDependency> {
    Vec::new()
}

pub const fn seed_units() -> Vec<SeedSpec> {
    Vec::new()
}

fn position_exit_plan_table_name() -> String {
    PositionExitPlan::Table.to_string()
}
