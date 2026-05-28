//! Catalog-driven `SeaORM` migration registry.

mod helpers;
mod m20250601_000001_initial_schema;
mod m20250601_000002_initial_indexes;
mod m20250601_000003_initial_seed;

pub use helpers::{
    SchemaRunner, create_updated_at_trigger, drop_updated_at_trigger, execute_sql,
    timestamp_with_write_default, write_timestamp,
};
pub use sea_orm_migration::prelude::*;

pub struct Migrator;

#[async_trait::async_trait]
impl MigratorTrait for Migrator {
    fn migrations() -> Vec<Box<dyn MigrationTrait>> {
        vec![
            Box::new(m20250601_000001_initial_schema::Migration),
            Box::new(m20250601_000002_initial_indexes::Migration),
            Box::new(m20250601_000003_initial_seed::Migration),
        ]
    }
}
