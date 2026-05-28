//! Initial schema lane: tables, constraints, and triggers from the catalog.

use crate::postgres::migration::SchemaRunner;
use sea_orm_migration::prelude::*;

#[derive(DeriveMigrationName)]
pub struct Migration;

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        SchemaRunner::new(manager).create_schema().await
    }

    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        SchemaRunner::new(manager).drop_schema().await
    }
}
