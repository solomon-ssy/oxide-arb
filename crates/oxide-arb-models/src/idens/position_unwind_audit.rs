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
pub enum PositionUnwindAudit {
    Table,
    UnwindAuditId,
    PositionId,
    ExitPlanId,
    ExitExecutionId,
    EventType,
    BeforePosition,
    AfterPosition,
    BookContext,
    TokenBalanceContext,
    Reason,
    Actor,
    CreatedAt,
}

pub fn table() -> TableCreateStatement {
    Table::create()
        .table(PositionUnwindAudit::Table)
        .if_not_exists()
        .col(
            ColumnDef::new(PositionUnwindAudit::UnwindAuditId)
                .text()
                .not_null()
                .primary_key(),
        )
        .col(
            ColumnDef::new(PositionUnwindAudit::PositionId)
                .text()
                .not_null(),
        )
        .col(
            ColumnDef::new(PositionUnwindAudit::ExitPlanId)
                .text()
                .null(),
        )
        .col(
            ColumnDef::new(PositionUnwindAudit::ExitExecutionId)
                .text()
                .null(),
        )
        .col(
            ColumnDef::new(PositionUnwindAudit::EventType)
                .text()
                .not_null(),
        )
        .col(
            ColumnDef::new(PositionUnwindAudit::BeforePosition)
                .json_binary()
                .not_null(),
        )
        .col(
            ColumnDef::new(PositionUnwindAudit::AfterPosition)
                .json_binary()
                .not_null(),
        )
        .col(
            ColumnDef::new(PositionUnwindAudit::BookContext)
                .json_binary()
                .not_null(),
        )
        .col(
            ColumnDef::new(PositionUnwindAudit::TokenBalanceContext)
                .json_binary()
                .not_null(),
        )
        .col(
            ColumnDef::new(PositionUnwindAudit::Reason)
                .text()
                .not_null(),
        )
        .col(ColumnDef::new(PositionUnwindAudit::Actor).text().not_null())
        .col(timestamp_with_write_default(PositionUnwindAudit::CreatedAt))
        .to_owned()
}

pub fn indexes() -> Vec<IndexSpec> {
    vec![IndexSpec::sea_query(
        "idx_position_unwind_audit_position_created",
        position_unwind_audit_table_name,
        IndexBuildMode::Transactional,
        Index::create()
            .name("idx_position_unwind_audit_position_created")
            .table(PositionUnwindAudit::Table)
            .col(PositionUnwindAudit::PositionId)
            .col((PositionUnwindAudit::CreatedAt, IndexOrder::Desc))
            .to_owned(),
        "unwind audit by position",
    )]
}

pub const fn dependencies() -> Vec<TableDependency> {
    Vec::new()
}

pub const fn seed_units() -> Vec<SeedSpec> {
    Vec::new()
}

fn position_unwind_audit_table_name() -> String {
    PositionUnwindAudit::Table.to_string()
}
