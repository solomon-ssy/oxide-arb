use super::migrate_up;
use oxide_arb_models::{enums::market::EventStatus, idens::event::Event};
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
            .table(Event::Table)
            .if_not_exists()
            .col(
                ColumnDef::new(Event::EventId)
                    .text()
                    .not_null()
                    .primary_key(),
            )
            .col(ColumnDef::new(Event::Title).text().not_null())
            .col(ColumnDef::new(Event::Slug).text().not_null())
            .col(ColumnDef::new(Event::Category).text().not_null())
            .col(
                ColumnDef::new(Event::Status)
                    .text()
                    .not_null()
                    .default(EventStatus::Active),
            )
            .col(
                ColumnDef::new(Event::NegRisk)
                    .boolean()
                    .not_null()
                    .default(false),
            )
            .col(
                ColumnDef::new(Event::EndDate)
                    .timestamp_with_time_zone()
                    .null(),
            )
            .col(ColumnDef::new(Event::RawGamma).json_binary().null())
            .col(
                ColumnDef::new(Event::CreatedAt)
                    .timestamp_with_time_zone()
                    .not_null()
                    .default(Expr::current_timestamp()),
            )
            .col(
                ColumnDef::new(Event::UpdatedAt)
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
            .name("idx_events_status")
            .table(Event::Table)
            .col(Event::Status)
            .to_owned(),
        Index::create()
            .name("idx_events_category")
            .table(Event::Table)
            .col(Event::Category)
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
    vec![Table::drop().table(Event::Table).to_owned()]
}
