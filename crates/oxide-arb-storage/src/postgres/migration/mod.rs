//! `SeaORM` migration registry.
//!
//! Each migration module follows the canonical lifecycle documented in
//! [`helpers`] — `create_tables`, `create_indexes`, `specials`, `seeding_data`,
//! and `drop_tables`.

pub use sea_orm_migration::prelude::*;

mod helpers;
pub use helpers::{create_indexes, create_tables, drop_tables, execute_sql, migrate_up, noop};

mod m20250601_000001_create_events;
mod m20250601_000002_create_markets;
mod m20250601_000003_create_trades;
mod m20250601_000004_create_positions;
mod m20250601_000005_create_risk_engine_state;
mod m20250601_000006_create_calibration;
mod m20250601_000007_create_runtime_config;
mod m20250601_000008_create_lifecycle_events;
mod m20250601_000009_create_accounting_periods;
mod m20250601_000010_create_potential_loss_ledger;
mod m20250601_000012_create_opportunity_lifecycle_outbox;
mod m20250601_000013_create_resolution_event;
mod m20250601_000014_bootstrap_trading_state;

pub struct Migrator;

#[async_trait::async_trait]
impl MigratorTrait for Migrator {
    fn migrations() -> Vec<Box<dyn MigrationTrait>> {
        vec![
            Box::new(m20250601_000001_create_events::Migration),
            Box::new(m20250601_000002_create_markets::Migration),
            Box::new(m20250601_000003_create_trades::Migration),
            Box::new(m20250601_000004_create_positions::Migration),
            Box::new(m20250601_000005_create_risk_engine_state::Migration),
            Box::new(m20250601_000006_create_calibration::Migration),
            Box::new(m20250601_000007_create_runtime_config::Migration),
            Box::new(m20250601_000008_create_lifecycle_events::Migration),
            Box::new(m20250601_000009_create_accounting_periods::Migration),
            Box::new(m20250601_000010_create_potential_loss_ledger::Migration),
            Box::new(m20250601_000012_create_opportunity_lifecycle_outbox::Migration),
            Box::new(m20250601_000013_create_resolution_event::Migration),
            Box::new(m20250601_000014_bootstrap_trading_state::Migration),
        ]
    }
}
