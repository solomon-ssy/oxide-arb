use quant_pivot_macros::quant_schema;
use sea_orm::{
    Iden,
    sea_query::{ColumnDef, ForeignKey, ForeignKeyAction, Index, Table, TableCreateStatement},
};

use crate::{
    enums::execution::ReconciliationResult,
    idens::{quant_execution_order::QuantExecutionOrder, quant_order_intent::QuantOrderIntent},
    schema::{
        column,
        dependency::TableDependency,
        index::{IndexBuildMode, IndexSpec},
        seed::SeedSpec,
        timestamp_with_write_default,
    },
};

#[quant_schema(lifecycle = "ledger")]
pub enum QuantReconciliation {
    Table,
    ReconciliationId,
    ExecutionOrderId,
    OrderIntentId,
    Result,
    EvidenceJson,
    VenueFilledShares,
    VenueAvgPrice,
    ExpectedCashDeltaUsd,
    VenueCashDeltaUsd,
    RealizedPnlUsd,
    ExpectedFeeUsd,
    ObservedFeeUsd,
    FeeDeltaUsd,
    ResolvedBy,
    ResolvedAt,
    CreatedAt,
    UpdatedAt,
}

pub fn table() -> TableCreateStatement {
    Table::create()
        .table(QuantReconciliation::Table)
        .if_not_exists()
        .col(column::uuid_pk(QuantReconciliation::ReconciliationId))
        .col(column::uuid_fk(QuantReconciliation::ExecutionOrderId))
        .col(column::uuid_fk(QuantReconciliation::OrderIntentId))
        .col(column::pg_enum::<ReconciliationResult>(
            QuantReconciliation::Result,
        ))
        .col(
            ColumnDef::new(QuantReconciliation::EvidenceJson)
                .json_binary()
                .not_null(),
        )
        .col(column::shares_null(QuantReconciliation::VenueFilledShares))
        .col(column::price_null(QuantReconciliation::VenueAvgPrice))
        .col(column::usd_null(QuantReconciliation::ExpectedCashDeltaUsd))
        .col(column::usd_null(QuantReconciliation::VenueCashDeltaUsd))
        .col(column::usd_null(QuantReconciliation::RealizedPnlUsd))
        .col(column::usd_null(QuantReconciliation::ExpectedFeeUsd))
        .col(column::usd_null(QuantReconciliation::ObservedFeeUsd))
        .col(column::usd_null(QuantReconciliation::FeeDeltaUsd))
        .col(
            ColumnDef::new(QuantReconciliation::ResolvedBy)
                .text()
                .null(),
        )
        .col(
            ColumnDef::new(QuantReconciliation::ResolvedAt)
                .timestamp_with_time_zone()
                .null(),
        )
        .col(timestamp_with_write_default(QuantReconciliation::CreatedAt))
        .col(timestamp_with_write_default(QuantReconciliation::UpdatedAt))
        .foreign_key(
            ForeignKey::create()
                .name("fk_quant_reconciliation_execution_order")
                .from(
                    QuantReconciliation::Table,
                    QuantReconciliation::ExecutionOrderId,
                )
                .to(
                    QuantExecutionOrder::Table,
                    QuantExecutionOrder::ExecutionOrderId,
                )
                .on_delete(ForeignKeyAction::Restrict),
        )
        .foreign_key(
            ForeignKey::create()
                .name("fk_quant_reconciliation_order_intent")
                .from(
                    QuantReconciliation::Table,
                    QuantReconciliation::OrderIntentId,
                )
                .to(QuantOrderIntent::Table, QuantOrderIntent::OrderIntentId)
                .on_delete(ForeignKeyAction::Restrict),
        )
        .to_owned()
}

pub fn indexes() -> Vec<IndexSpec> {
    vec![
        IndexSpec::sea_query(
            "uq_quant_reconciliation_execution_order",
            quant_reconciliation_table_name,
            IndexBuildMode::Transactional,
            Index::create()
                .name("uq_quant_reconciliation_execution_order")
                .table(QuantReconciliation::Table)
                .col(QuantReconciliation::ExecutionOrderId)
                .unique()
                .to_owned(),
            "one reconciliation summary per execution order",
        ),
        IndexSpec::sea_query(
            "idx_quant_reconciliation_result",
            quant_reconciliation_table_name,
            IndexBuildMode::Transactional,
            Index::create()
                .name("idx_quant_reconciliation_result")
                .table(QuantReconciliation::Table)
                .col(QuantReconciliation::Result)
                .to_owned(),
            "reconciliations by result",
        ),
    ]
}

pub fn dependencies() -> Vec<TableDependency> {
    vec![
        TableDependency::foreign_key(quant_execution_order_table_name),
        TableDependency::foreign_key(quant_order_intent_table_name),
    ]
}

pub const fn seed_units() -> Vec<SeedSpec> {
    Vec::new()
}

fn quant_reconciliation_table_name() -> String {
    QuantReconciliation::Table.to_string()
}

fn quant_execution_order_table_name() -> String {
    QuantExecutionOrder::Table.to_string()
}

fn quant_order_intent_table_name() -> String {
    QuantOrderIntent::Table.to_string()
}
