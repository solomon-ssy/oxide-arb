use quant_pivot_macros::quant_schema;
use sea_orm::{
    Iden,
    sea_query::{ColumnDef, ForeignKey, ForeignKeyAction, Index, Table, TableCreateStatement},
};

use crate::{
    enums::execution::CapitalAllocationState,
    idens::{quant_order_intent::QuantOrderIntent, quant_recommendation::QuantRecommendation},
    schema::{
        column,
        dependency::TableDependency,
        index::{IndexBuildMode, IndexSpec},
        seed::SeedSpec,
        timestamp_with_write_default,
    },
};

#[quant_schema(lifecycle = "ledger")]
pub enum QuantCapitalAllocation {
    Table,
    CapitalAllocationId,
    OrderIntentId,
    RecommendationId,
    State,
    PlannedUsd,
    AllocatedUsd,
    LockedUsd,
    SpentUsd,
    ReleasedUsd,
    Reason,
    CreatedAt,
    UpdatedAt,
}

pub fn table() -> TableCreateStatement {
    Table::create()
        .table(QuantCapitalAllocation::Table)
        .if_not_exists()
        .col(column::uuid_pk(QuantCapitalAllocation::CapitalAllocationId))
        .col(column::uuid_fk(QuantCapitalAllocation::OrderIntentId))
        .col(column::uuid_fk(QuantCapitalAllocation::RecommendationId))
        .col(column::pg_enum::<CapitalAllocationState>(
            QuantCapitalAllocation::State,
        ))
        .col(column::usd(QuantCapitalAllocation::PlannedUsd))
        .col(column::usd(QuantCapitalAllocation::AllocatedUsd))
        .col(column::usd(QuantCapitalAllocation::LockedUsd))
        .col(column::usd(QuantCapitalAllocation::SpentUsd))
        .col(column::usd(QuantCapitalAllocation::ReleasedUsd))
        .col(
            ColumnDef::new(QuantCapitalAllocation::Reason)
                .text()
                .not_null(),
        )
        .col(timestamp_with_write_default(
            QuantCapitalAllocation::CreatedAt,
        ))
        .col(timestamp_with_write_default(
            QuantCapitalAllocation::UpdatedAt,
        ))
        .foreign_key(
            ForeignKey::create()
                .name("fk_quant_capital_allocation_intent")
                .from(
                    QuantCapitalAllocation::Table,
                    QuantCapitalAllocation::OrderIntentId,
                )
                .to(QuantOrderIntent::Table, QuantOrderIntent::OrderIntentId)
                .on_delete(ForeignKeyAction::Restrict),
        )
        .foreign_key(
            ForeignKey::create()
                .name("fk_quant_capital_allocation_recommendation")
                .from(
                    QuantCapitalAllocation::Table,
                    QuantCapitalAllocation::RecommendationId,
                )
                .to(
                    QuantRecommendation::Table,
                    QuantRecommendation::RecommendationId,
                )
                .on_delete(ForeignKeyAction::Restrict),
        )
        .to_owned()
}

pub fn indexes() -> Vec<IndexSpec> {
    vec![
        IndexSpec::sea_query(
            "uq_quant_capital_allocation_intent",
            quant_capital_allocation_table_name,
            IndexBuildMode::Transactional,
            Index::create()
                .name("uq_quant_capital_allocation_intent")
                .table(QuantCapitalAllocation::Table)
                .col(QuantCapitalAllocation::OrderIntentId)
                .unique()
                .to_owned(),
            "one capital allocation per order intent",
        ),
        IndexSpec::sea_query(
            "idx_quant_capital_allocation_state",
            quant_capital_allocation_table_name,
            IndexBuildMode::Transactional,
            Index::create()
                .name("idx_quant_capital_allocation_state")
                .table(QuantCapitalAllocation::Table)
                .col(QuantCapitalAllocation::State)
                .to_owned(),
            "capital allocations by state",
        ),
    ]
}

pub fn dependencies() -> Vec<TableDependency> {
    vec![
        TableDependency::foreign_key(quant_order_intent_table_name),
        TableDependency::foreign_key(quant_recommendation_table_name),
    ]
}

pub const fn seed_units() -> Vec<SeedSpec> {
    Vec::new()
}

fn quant_capital_allocation_table_name() -> String {
    QuantCapitalAllocation::Table.to_string()
}

fn quant_order_intent_table_name() -> String {
    QuantOrderIntent::Table.to_string()
}

fn quant_recommendation_table_name() -> String {
    QuantRecommendation::Table.to_string()
}
