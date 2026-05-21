use super::migrate_up;
use oxide_arb_models::idens::accounting_period::AccountingPeriod;
use sea_orm_migration::prelude::*;

#[derive(DeriveMigrationName)]
pub struct Migration;

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        migrate_up(
            manager,
            create_tables(),
            create_indexes(),
            specials(manager),
            seeding_data(manager),
        )
        .await
    }

    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        super::drop_tables(manager, drop_tables()).await
    }
}

fn create_tables() -> Vec<TableCreateStatement> {
    vec![
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
            .col(
                ColumnDef::new(AccountingPeriod::RealizedPnl)
                    .text()
                    .not_null()
                    .default("0"),
            )
            .col(
                ColumnDef::new(AccountingPeriod::TotalFees)
                    .text()
                    .not_null()
                    .default("0"),
            )
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
            .col(
                ColumnDef::new(AccountingPeriod::MaxDrawdown)
                    .text()
                    .not_null()
                    .default("0"),
            )
            .col(ColumnDef::new(AccountingPeriod::SharpeRatio).text().null())
            .col(
                ColumnDef::new(AccountingPeriod::Finalized)
                    .boolean()
                    .not_null()
                    .default(false),
            )
            .col(
                ColumnDef::new(AccountingPeriod::CreatedAt)
                    .timestamp_with_time_zone()
                    .not_null()
                    .default(Expr::current_timestamp()),
            )
            .to_owned(),
    ]
}

fn create_indexes() -> Vec<IndexCreateStatement> {
    vec![
        Index::create()
            .name("idx_acct_period_unique")
            .table(AccountingPeriod::Table)
            .col(AccountingPeriod::PeriodType)
            .col(AccountingPeriod::StartDate)
            .unique()
            .to_owned(),
    ]
}

async fn specials(manager: &SchemaManager<'_>) -> Result<(), DbErr> {
    super::noop(manager).await
}

async fn seeding_data(manager: &SchemaManager<'_>) -> Result<(), DbErr> {
    super::noop(manager).await
}

fn drop_tables() -> Vec<TableDropStatement> {
    vec![Table::drop().table(AccountingPeriod::Table).to_owned()]
}
