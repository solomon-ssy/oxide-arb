//! Seed migration: trading bootstrap v1.
//!
//! Depends on the risk engine state and runtime config tables created by
//! earlier schema migrations. The seed plan is idempotent and preserves
//! operator-modified values through `ON CONFLICT DO NOTHING`.

use oxide_arb_models::seed::plans;
use sea_orm_migration::prelude::*;

#[derive(DeriveMigrationName)]
pub struct Migration;

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        super::migrate_seed(crate::postgres::seed::runner::run_plan(
            manager.get_connection(),
            &plans::trading_bootstrap_v1(),
        ))
        .await
    }

    async fn down(&self, _manager: &SchemaManager) -> Result<(), DbErr> {
        // Seed data remains in its original tables (managed by schema
        // migrations 001-013). Rolling back the bootstrap migration is
        // a no-op to prevent accidental deletion of production state.
        Ok(())
    }
}
