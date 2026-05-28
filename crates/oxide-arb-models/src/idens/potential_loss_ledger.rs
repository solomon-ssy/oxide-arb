use oxide_arb_macros::oxide_schema;
use sea_orm::{
    Iden,
    sea_query::{ColumnDef, ForeignKey, ForeignKeyAction, Index, Table, TableCreateStatement},
};

use crate::{
    enums::common::LedgerStatus,
    idens::market::Market,
    schema::{
        dependency::TableDependency,
        index::{IndexBuildMode, IndexSpec},
        seed::SeedSpec,
    },
};

#[oxide_schema]
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
        .col(
            ColumnDef::new(PotentialLossLedger::LedgerId)
                .text()
                .not_null()
                .primary_key(),
        )
        .col(
            ColumnDef::new(PotentialLossLedger::MarketId)
                .text()
                .not_null(),
        )
        .col(
            ColumnDef::new(PotentialLossLedger::TokenId)
                .text()
                .not_null(),
        )
        .col(
            ColumnDef::new(PotentialLossLedger::Shares)
                .text()
                .not_null(),
        )
        .col(
            ColumnDef::new(PotentialLossLedger::EntryPrice)
                .text()
                .not_null(),
        )
        .col(
            ColumnDef::new(PotentialLossLedger::MaxLossUsd)
                .text()
                .not_null(),
        )
        .col(
            ColumnDef::new(PotentialLossLedger::Status)
                .text()
                .not_null()
                .default(LedgerStatus::Active),
        )
        .col(crate::schema::timestamp_with_write_default(
            PotentialLossLedger::CreatedAt,
        ))
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
