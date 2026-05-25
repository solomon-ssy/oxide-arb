use super::{execute_sql, migrate_up};
use oxide_arb_models::idens::{
    opportunity_lifecycle::OpportunityLifecycleEvent, outbox_event::OutboxEvent,
};
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
    vec![opportunity_lifecycle_event_table(), outbox_event_table()]
}

fn opportunity_lifecycle_event_table() -> TableCreateStatement {
    Table::create()
        .table(OpportunityLifecycleEvent::Table)
        .if_not_exists()
        .col(
            ColumnDef::new(OpportunityLifecycleEvent::EventId)
                .text()
                .not_null()
                .primary_key(),
        )
        .col(
            ColumnDef::new(OpportunityLifecycleEvent::OpportunityId)
                .text()
                .not_null(),
        )
        .col(
            ColumnDef::new(OpportunityLifecycleEvent::ExecutionId)
                .text()
                .null(),
        )
        .col(
            ColumnDef::new(OpportunityLifecycleEvent::Phase)
                .text()
                .not_null(),
        )
        .col(
            ColumnDef::new(OpportunityLifecycleEvent::PhaseData)
                .json_binary()
                .null(),
        )
        .col(
            ColumnDef::new(OpportunityLifecycleEvent::CreatedAt)
                .timestamp_with_time_zone()
                .not_null()
                .default(Expr::current_timestamp()),
        )
        .to_owned()
}

fn outbox_event_table() -> TableCreateStatement {
    Table::create()
        .table(OutboxEvent::Table)
        .if_not_exists()
        .col(
            ColumnDef::new(OutboxEvent::EventId)
                .text()
                .not_null()
                .primary_key(),
        )
        .col(ColumnDef::new(OutboxEvent::AggregateType).text().not_null())
        .col(ColumnDef::new(OutboxEvent::AggregateId).text().not_null())
        .col(ColumnDef::new(OutboxEvent::EventType).text().not_null())
        .col(
            ColumnDef::new(OutboxEvent::Payload)
                .json_binary()
                .not_null(),
        )
        .col(
            ColumnDef::new(OutboxEvent::PublishAttempts)
                .integer()
                .not_null()
                .default(0),
        )
        .col(
            ColumnDef::new(OutboxEvent::PublishedAt)
                .timestamp_with_time_zone()
                .null(),
        )
        .col(ColumnDef::new(OutboxEvent::LastError).text().null())
        .col(ColumnDef::new(OutboxEvent::DeadLetterReason).text().null())
        .col(
            ColumnDef::new(OutboxEvent::CreatedAt)
                .timestamp_with_time_zone()
                .not_null()
                .default(Expr::current_timestamp()),
        )
        .to_owned()
}

fn create_indexes() -> Vec<IndexCreateStatement> {
    vec![
        Index::create()
            .name("idx_opp_lifecycle_opp_id")
            .table(OpportunityLifecycleEvent::Table)
            .col(OpportunityLifecycleEvent::OpportunityId)
            .col((OpportunityLifecycleEvent::CreatedAt, IndexOrder::Asc))
            .to_owned(),
    ]
}

async fn specials(manager: &SchemaManager<'_>) -> Result<(), DbErr> {
    execute_sql(
        manager,
        [
            "CREATE INDEX IF NOT EXISTS idx_outbox_pending \
             ON outbox_event (created_at) \
             WHERE published_at IS NULL",
            "CREATE INDEX IF NOT EXISTS idx_outbox_unpublished_attempts \
             ON outbox_event (publish_attempts, created_at) \
             WHERE published_at IS NULL AND dead_letter_reason IS NULL",
        ],
    )
    .await
}

async fn seeding_data(manager: &SchemaManager<'_>) -> Result<(), DbErr> {
    super::noop(manager).await
}

fn drop_tables() -> Vec<TableDropStatement> {
    vec![
        Table::drop().table(OutboxEvent::Table).to_owned(),
        Table::drop()
            .table(OpportunityLifecycleEvent::Table)
            .to_owned(),
    ]
}
