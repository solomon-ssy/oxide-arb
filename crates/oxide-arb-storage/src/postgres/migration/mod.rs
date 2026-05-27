//! `SeaORM` migration registry.
//!
//! Migration files use explicit action prefixes to keep long-term evolution
//! predictable:
//!
//! - `create_*` creates new schema objects and their initial indexes.
//! - `alter_*` changes existing schema.
//! - `backfill_*` performs data-only historical repair or conversion.
//! - `seed_*` inserts idempotent bootstrap/reference data.
//! - `trigger_*` manages database functions, triggers, or extensions.
//!
//! Seed migrations are not required to remain the final migration forever; each
//! seed only depends on schema that appears before it in this registry.

mod helpers;
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
mod m20250601_000014_add_updated_at_triggers;
mod m20250601_000015_seed_trading_bootstrap;
mod m20250601_000016_create_reports;
mod m20250601_000018_create_blacklist_entries;
mod m20250601_000019_create_risk_audit_events;
mod m20250601_000020_create_emergency_snapshots;
mod m20250601_000021_create_reconciliation_reports;

pub use helpers::{
    create_indexes, create_tables, drop_tables, execute_sql, migrate_data, migrate_schema,
    migrate_seed, migrate_up, noop,
};
pub use sea_orm_migration::prelude::*;

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
            Box::new(m20250601_000014_add_updated_at_triggers::Migration),
            Box::new(m20250601_000015_seed_trading_bootstrap::Migration),
            Box::new(m20250601_000016_create_reports::Migration),
            Box::new(m20250601_000018_create_blacklist_entries::Migration),
            Box::new(m20250601_000019_create_risk_audit_events::Migration),
            Box::new(m20250601_000020_create_emergency_snapshots::Migration),
            Box::new(m20250601_000021_create_reconciliation_reports::Migration),
        ]
    }
}
