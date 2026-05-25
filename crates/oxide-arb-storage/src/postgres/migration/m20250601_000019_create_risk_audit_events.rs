//! Migration: create `risk_audit_event` table for risk audit trail.

use oxide_arb_models::idens::risk_audit_event::RiskAuditEvent;
use sea_orm_migration::prelude::*;

#[derive(DeriveMigrationName)]
pub struct Migration;

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        super::migrate_schema(
            manager,
            create_tables(),
            create_indexes(),
            specials(manager),
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
            .table(RiskAuditEvent::Table)
            .if_not_exists()
            .col(
                ColumnDef::new(RiskAuditEvent::Id)
                    .big_integer()
                    .not_null()
                    .auto_increment()
                    .primary_key(),
            )
            .col(ColumnDef::new(RiskAuditEvent::EventType).text().not_null())
            .col(ColumnDef::new(RiskAuditEvent::OpportunityId).text().null())
            .col(ColumnDef::new(RiskAuditEvent::TradeId).text().null())
            .col(
                ColumnDef::new(RiskAuditEvent::Payload)
                    .json_binary()
                    .not_null(),
            )
            .col(
                ColumnDef::new(RiskAuditEvent::CreatedAt)
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
            .name("idx_risk_audit_event_created_at")
            .table(RiskAuditEvent::Table)
            .col((RiskAuditEvent::CreatedAt, IndexOrder::Desc))
            .to_owned(),
    ]
}

async fn specials(manager: &SchemaManager<'_>) -> Result<(), DbErr> {
    super::noop(manager).await
}

fn drop_tables() -> Vec<TableDropStatement> {
    vec![Table::drop().table(RiskAuditEvent::Table).to_owned()]
}
