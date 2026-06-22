use quant_pivot_macros::quant_schema;
use sea_orm::{
    Iden,
    sea_query::{
        ColumnDef, ForeignKey, ForeignKeyAction, Index, IndexOrder, Table, TableCreateStatement,
    },
};

use crate::{
    idens::{market::Market, quant_order_intent::QuantOrderIntent},
    schema::{
        column,
        dependency::TableDependency,
        index::{IndexBuildMode, IndexSpec},
        seed::SeedSpec,
        timestamp_with_write_default,
    },
};

#[quant_schema(lifecycle = "ledger")]
pub enum QuantExecutionOrder {
    Table,
    ExecutionOrderId,
    OrderIntentId,
    OrderPhase,
    MarketId,
    TokenId,
    Side,
    OrderType,
    Price,
    Shares,
    CostUsd,
    VenueOrderId,
    VenueStatus,
    State,
    SubmittedAt,
    FilledAt,
    CancelledAt,
    ErrorMessage,
    CreatedAt,
    UpdatedAt,
}

pub fn table() -> TableCreateStatement {
    Table::create()
        .table(QuantExecutionOrder::Table)
        .if_not_exists()
        .col(column::uuid_pk(QuantExecutionOrder::ExecutionOrderId))
        .col(column::uuid_fk(QuantExecutionOrder::OrderIntentId))
        .col(
            ColumnDef::new(QuantExecutionOrder::OrderPhase)
                .text()
                .not_null(),
        )
        .col(column::market_id(QuantExecutionOrder::MarketId))
        .col(column::token_id(QuantExecutionOrder::TokenId))
        .col(ColumnDef::new(QuantExecutionOrder::Side).text().not_null())
        .col(
            ColumnDef::new(QuantExecutionOrder::OrderType)
                .text()
                .not_null(),
        )
        .col(column::price(QuantExecutionOrder::Price))
        .col(column::shares(QuantExecutionOrder::Shares))
        .col(column::usd(QuantExecutionOrder::CostUsd))
        .col(column::text_id_null(QuantExecutionOrder::VenueOrderId))
        .col(
            ColumnDef::new(QuantExecutionOrder::VenueStatus)
                .text()
                .null(),
        )
        .col(ColumnDef::new(QuantExecutionOrder::State).text().not_null())
        .col(
            ColumnDef::new(QuantExecutionOrder::SubmittedAt)
                .timestamp_with_time_zone()
                .null(),
        )
        .col(
            ColumnDef::new(QuantExecutionOrder::FilledAt)
                .timestamp_with_time_zone()
                .null(),
        )
        .col(
            ColumnDef::new(QuantExecutionOrder::CancelledAt)
                .timestamp_with_time_zone()
                .null(),
        )
        .col(
            ColumnDef::new(QuantExecutionOrder::ErrorMessage)
                .text()
                .null(),
        )
        .col(timestamp_with_write_default(QuantExecutionOrder::CreatedAt))
        .col(timestamp_with_write_default(QuantExecutionOrder::UpdatedAt))
        .foreign_key(
            ForeignKey::create()
                .name("fk_quant_execution_order_intent")
                .from(
                    QuantExecutionOrder::Table,
                    QuantExecutionOrder::OrderIntentId,
                )
                .to(QuantOrderIntent::Table, QuantOrderIntent::OrderIntentId)
                .on_delete(ForeignKeyAction::Restrict),
        )
        .foreign_key(
            ForeignKey::create()
                .name("fk_quant_execution_order_market")
                .from(QuantExecutionOrder::Table, QuantExecutionOrder::MarketId)
                .to(Market::Table, Market::MarketId)
                .on_delete(ForeignKeyAction::Restrict),
        )
        .to_owned()
}

pub fn indexes() -> Vec<IndexSpec> {
    vec![
        IndexSpec::sea_query(
            "idx_quant_execution_order_intent_created",
            quant_execution_order_table_name,
            IndexBuildMode::Transactional,
            Index::create()
                .name("idx_quant_execution_order_intent_created")
                .table(QuantExecutionOrder::Table)
                .col(QuantExecutionOrder::OrderIntentId)
                .col((QuantExecutionOrder::CreatedAt, IndexOrder::Desc))
                .to_owned(),
            "execution orders by intent and recency",
        ),
        IndexSpec::sea_query(
            "idx_quant_execution_order_state",
            quant_execution_order_table_name,
            IndexBuildMode::Transactional,
            Index::create()
                .name("idx_quant_execution_order_state")
                .table(QuantExecutionOrder::Table)
                .col(QuantExecutionOrder::State)
                .to_owned(),
            "execution orders by lifecycle state",
        ),
    ]
}

pub fn dependencies() -> Vec<TableDependency> {
    vec![
        TableDependency::foreign_key(quant_order_intent_table_name),
        TableDependency::foreign_key(market_table_name),
    ]
}

pub const fn seed_units() -> Vec<SeedSpec> {
    Vec::new()
}

fn quant_execution_order_table_name() -> String {
    QuantExecutionOrder::Table.to_string()
}

fn quant_order_intent_table_name() -> String {
    QuantOrderIntent::Table.to_string()
}

fn market_table_name() -> String {
    Market::Table.to_string()
}
