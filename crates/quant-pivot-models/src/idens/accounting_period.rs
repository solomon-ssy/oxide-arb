use oxide_arb_macros::oxide_schema;
use sea_orm::{
    Iden,
    sea_query::{ColumnDef, Index, Table, TableCreateStatement},
};

use crate::schema::{
    column,
    dependency::TableDependency,
    index::{IndexBuildMode, IndexSpec},
    seed::SeedSpec,
    timestamp_with_write_default,
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
        .col(column::uuid_pk(AccountingPeriod::PeriodId))
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
        .col(column::usd_default_zero(AccountingPeriod::RealizedPnl))
        .col(column::usd_default_zero(AccountingPeriod::TotalFees))
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
        .col(column::usd_default_zero(AccountingPeriod::MaxDrawdown))
        .col(column::probability_null(AccountingPeriod::SharpeRatio))
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

fn accounting_period_table_name() -> String {
    AccountingPeriod::Table.to_string()
}
