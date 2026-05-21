use super::{execute_sql, migrate_up};
use oxide_arb_models::idens::market::Market;
use oxide_arb_models::idens::potential_loss_ledger::PotentialLossLedger;
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
                    .default("active"),
            )
            .col(
                ColumnDef::new(PotentialLossLedger::CreatedAt)
                    .timestamp_with_time_zone()
                    .not_null()
                    .default(Expr::current_timestamp()),
            )
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
            .to_owned(),
    ]
}

fn create_indexes() -> Vec<IndexCreateStatement> {
    vec![
        Index::create()
            .name("idx_pll_market")
            .table(PotentialLossLedger::Table)
            .col(PotentialLossLedger::MarketId)
            .to_owned(),
    ]
}

async fn specials(manager: &SchemaManager<'_>) -> Result<(), DbErr> {
    execute_sql(
        manager,
        [
            "CREATE INDEX idx_pll_active \
             ON potential_loss_ledger (status) \
             WHERE status = 'active'",
            "CREATE INDEX IF NOT EXISTS idx_pll_active_created \
             ON potential_loss_ledger (created_at DESC) \
             WHERE status = 'active'",
        ],
    )
    .await
}

async fn seeding_data(manager: &SchemaManager<'_>) -> Result<(), DbErr> {
    super::noop(manager).await
}

fn drop_tables() -> Vec<TableDropStatement> {
    vec![Table::drop().table(PotentialLossLedger::Table).to_owned()]
}
