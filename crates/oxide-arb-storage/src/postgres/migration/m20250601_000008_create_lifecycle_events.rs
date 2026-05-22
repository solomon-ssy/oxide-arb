use super::migrate_up;
use oxide_arb_models::idens::lifecycle_event::LifecycleEvent;
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
            .table(LifecycleEvent::Table)
            .if_not_exists()
            .col(
                ColumnDef::new(LifecycleEvent::Id)
                    .big_integer()
                    .not_null()
                    .auto_increment()
                    .primary_key(),
            )
            .col(ColumnDef::new(LifecycleEvent::Phase).text().not_null())
            .col(ColumnDef::new(LifecycleEvent::Stage).text().null())
            .col(ColumnDef::new(LifecycleEvent::Message).text().not_null())
            .col(
                ColumnDef::new(LifecycleEvent::Metadata)
                    .json_binary()
                    .null(),
            )
            .col(
                ColumnDef::new(LifecycleEvent::CreatedAt)
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
            .name("idx_lifecycle_created")
            .table(LifecycleEvent::Table)
            .col((LifecycleEvent::CreatedAt, IndexOrder::Desc))
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
    vec![Table::drop().table(LifecycleEvent::Table).to_owned()]
}
