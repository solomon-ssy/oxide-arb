//! Initial index lane: all catalog-declared indexes.

use crate::postgres::migration::SchemaRunner;
use sea_orm_migration::prelude::*;

#[derive(DeriveMigrationName)]
pub struct Migration;

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        SchemaRunner::new(manager).create_indexes().await
    }

    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        SchemaRunner::new(manager).drop_indexes().await
    }
}
