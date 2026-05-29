use oxide_arb_macros::oxide_schema;
use sea_orm::{
    Iden,
    sea_query::{ColumnDef, ForeignKey, ForeignKeyAction, Table, TableCreateStatement},
};

use crate::{
    idens::trade::Trade,
    schema::{dependency::TableDependency, index::IndexSpec, seed::SeedSpec},
};

/// Durable idempotency marker for committed risk Fill accounting.
///
/// One row exists per `trade_id`. The marker is inserted atomically with the
/// risk-state snapshot so relay replay re-applies a fill at most once.
#[oxide_schema]
pub enum RiskFillApplied {
    Table,
    TradeId,
    AppliedAt,
}

pub fn table() -> TableCreateStatement {
    Table::create()
        .table(RiskFillApplied::Table)
        .if_not_exists()
        .col(
            ColumnDef::new(RiskFillApplied::TradeId)
                .text()
                .not_null()
                .primary_key(),
        )
        .col(
            ColumnDef::new(RiskFillApplied::AppliedAt)
                .timestamp_with_time_zone()
                .not_null(),
        )
        .foreign_key(
            ForeignKey::create()
                .name("fk_risk_fill_applied_trade")
                .from(RiskFillApplied::Table, RiskFillApplied::TradeId)
                .to(Trade::Table, Trade::TradeId)
                .on_delete(ForeignKeyAction::Restrict),
        )
        .to_owned()
}

pub const fn indexes() -> Vec<IndexSpec> {
    Vec::new()
}

pub fn dependencies() -> Vec<TableDependency> {
    vec![TableDependency::foreign_key(trade_table_name)]
}

pub const fn seed_units() -> Vec<SeedSpec> {
    Vec::new()
}

fn trade_table_name() -> String {
    Trade::Table.to_string()
}
