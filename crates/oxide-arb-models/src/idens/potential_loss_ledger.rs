use oxide_arb_macros::oxide_schema;
use sea_orm::{
    Iden,
    sea_query::{ColumnDef, ForeignKey, ForeignKeyAction, Index, Table, TableCreateStatement},
};

use crate::{
    enums::common::LedgerStatus,
    idens::market::Market,
    schema::{
        column,
        dependency::TableDependency,
        index::{IndexBuildMode, IndexSpec},
        seed::SeedSpec,
        timestamp_with_write_default,
    },
};

#[oxide_schema(lifecycle = "ledger")]
pub enum PotentialLossLedger {
    Table,
    LedgerId,
    MarketId,
    TokenId,
    Shares,
    EntryPrice,
    MaxLossUsd,
    Status,
    CreatedAt,
    ResolvedAt,
}

pub fn table() -> TableCreateStatement {
    Table::create()
        .table(PotentialLossLedger::Table)
        .if_not_exists()
        .col(column::uuid_pk(PotentialLossLedger::LedgerId))
        .col(column::market_id(PotentialLossLedger::MarketId))
        .col(column::token_id(PotentialLossLedger::TokenId))
        .col(column::shares(PotentialLossLedger::Shares))
        .col(column::price(PotentialLossLedger::EntryPrice))
        .col(column::usd(PotentialLossLedger::MaxLossUsd))
        .col(
            ColumnDef::new(PotentialLossLedger::Status)
                .text()
                .not_null()
                .default(LedgerStatus::Active),
        )
        .col(timestamp_with_write_default(PotentialLossLedger::CreatedAt))
        .col(
            ColumnDef::new(PotentialLossLedger::ResolvedAt)
                .timestamp_with_time_zone()
                .null(),
        )
        .foreign_key(
            ForeignKey::create()
                .name("fk_pll_market")
                .from(PotentialLossLedger::Table, PotentialLossLedger::MarketId)
                .to(Market::Table, Market::MarketId)
                .on_delete(ForeignKeyAction::Restrict),
        )
        .to_owned()
}

pub fn indexes() -> Vec<IndexSpec> {
    vec![
        IndexSpec::sea_query(
            "idx_pll_market",
            potential_loss_ledger_table_name,
            IndexBuildMode::Transactional,
            Index::create()
                .name("idx_pll_market")
                .table(PotentialLossLedger::Table)
                .col(PotentialLossLedger::MarketId)
                .to_owned(),
            "potential loss entries by market",
        ),
        IndexSpec::raw(
            "idx_pll_active",
            potential_loss_ledger_table_name,
            IndexBuildMode::Transactional,
            "CREATE INDEX IF NOT EXISTS idx_pll_active \
             ON potential_loss_ledger (status) \
             WHERE status = 'active'",
            "active potential loss entries",
        ),
        IndexSpec::raw(
            "idx_pll_active_created",
            potential_loss_ledger_table_name,
            IndexBuildMode::Transactional,
            "CREATE INDEX IF NOT EXISTS idx_pll_active_created \
             ON potential_loss_ledger (created_at DESC) \
             WHERE status = 'active'",
            "active potential loss entries by recency",
        ),
    ]
}

pub fn dependencies() -> Vec<TableDependency> {
    vec![TableDependency::foreign_key(market_table_name)]
}

pub const fn seed_units() -> Vec<SeedSpec> {
    Vec::new()
}

fn potential_loss_ledger_table_name() -> String {
    PotentialLossLedger::Table.to_string()
}

fn market_table_name() -> String {
    Market::Table.to_string()
}
