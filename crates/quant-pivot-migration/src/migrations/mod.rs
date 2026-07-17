mod m20260716_000001_schema_snapshot;
mod m20260716_000002_relational_invariants;
mod m20260716_000003_query_indexes;
mod m20260716_000004_worm_and_update_triggers;
mod support;

use sea_orm_migration::{MigrationTrait, MigratorTrait};

use crate::MigrationSpec;

pub struct Migrator;

#[async_trait::async_trait]
impl MigratorTrait for Migrator {
    fn migrations() -> Vec<Box<dyn MigrationTrait>> {
        vec![
            Box::new(m20260716_000001_schema_snapshot::Migration),
            Box::new(m20260716_000002_relational_invariants::Migration),
            Box::new(m20260716_000003_query_indexes::Migration),
            Box::new(m20260716_000004_worm_and_update_triggers::Migration),
        ]
    }
}

pub(crate) fn specs() -> Vec<MigrationSpec> {
    vec![
        m20260716_000001_schema_snapshot::spec(),
        m20260716_000002_relational_invariants::spec(),
        m20260716_000003_query_indexes::spec(),
        m20260716_000004_worm_and_update_triggers::spec(),
    ]
}
