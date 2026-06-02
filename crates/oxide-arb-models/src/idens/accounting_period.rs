use oxide_arb_macros::oxide_schema;
use sea_orm::{
    Iden,
    sea_query::{ColumnDef, Index, Table, TableCreateStatement},
};

use crate::{
    schema::{
        dependency::TableDependency,
        index::{IndexBuildMode, IndexSpec},
        seed::SeedSpec,
        timestamp_with_write_default,
    },
    types::Usd,
};

#[oxide_schema(lifecycle = "ledger")]
pub enum AccountingPeriod {
    Table,
    PeriodId,
    PeriodType,
    StartDate,
    EndDate,
    RealizedPnl,
    TotalFees,
    TradeCount,
    WinCount,
    LossCount,
    MissCount,
    MaxDrawdown,
    SharpeRatio,
    Finalized,
    CreatedAt,
}

pub fn table() -> TableCreateStatement {
    Table::create()
        .table(AccountingPeriod::Table)
        .if_not_exists()
        .col(
            ColumnDef::new(AccountingPeriod::PeriodId)
                .text()
                .not_null()
                .primary_key(),
        )
        .col(
            ColumnDef::new(AccountingPeriod::PeriodType)
                .text()
                .not_null(),
        )
        .col(
            ColumnDef::new(AccountingPeriod::StartDate)
                .date()
                .not_null(),
        )
        .col(ColumnDef::new(AccountingPeriod::EndDate).date().not_null())
        .col(default_zero_usd(AccountingPeriod::RealizedPnl))
        .col(default_zero_usd(AccountingPeriod::TotalFees))
        .col(
            ColumnDef::new(AccountingPeriod::TradeCount)
                .integer()
                .not_null()
                .default(0),
        )
        .col(
            ColumnDef::new(AccountingPeriod::WinCount)
                .integer()
                .not_null()
                .default(0),
        )
        .col(
            ColumnDef::new(AccountingPeriod::LossCount)
                .integer()
                .not_null()
                .default(0),
        )
        .col(
            ColumnDef::new(AccountingPeriod::MissCount)
                .integer()
                .not_null()
                .default(0),
        )
        .col(default_zero_usd(AccountingPeriod::MaxDrawdown))
        .col(ColumnDef::new(AccountingPeriod::SharpeRatio).text().null())
        .col(
            ColumnDef::new(AccountingPeriod::Finalized)
                .boolean()
                .not_null()
                .default(false),
        )
        .col(timestamp_with_write_default(AccountingPeriod::CreatedAt))
        .to_owned()
}

pub fn indexes() -> Vec<IndexSpec> {
    vec![IndexSpec::sea_query(
        "idx_acct_period_unique",
        accounting_period_table_name,
        IndexBuildMode::Transactional,
        Index::create()
            .name("idx_acct_period_unique")
            .table(AccountingPeriod::Table)
            .col(AccountingPeriod::PeriodType)
            .col(AccountingPeriod::StartDate)
            .unique()
            .to_owned(),
        "unique accounting period per type/start date",
    )]
}

pub const fn dependencies() -> Vec<TableDependency> {
    Vec::new()
}

pub const fn seed_units() -> Vec<SeedSpec> {
    Vec::new()
}

fn default_zero_usd(column: AccountingPeriod) -> ColumnDef {
    let mut col = ColumnDef::new(column);
    col.text().not_null().default(Usd::ZERO);
    col
}

fn accounting_period_table_name() -> String {
    AccountingPeriod::Table.to_string()
}
