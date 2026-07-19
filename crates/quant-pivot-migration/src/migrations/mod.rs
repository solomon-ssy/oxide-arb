mod m00000000_000001_bootstrap;
mod support;

use sea_orm_migration::{MigrationTrait, MigratorTrait};

use crate::MigrationSpec;

pub struct Migrator;

#[async_trait::async_trait]
impl MigratorTrait for Migrator {
    fn migrations() -> Vec<Box<dyn MigrationTrait>> {
        vec![Box::new(m00000000_000001_bootstrap::Migration)]
    }
}

pub(crate) fn specs() -> Vec<MigrationSpec> {
    vec![m00000000_000001_bootstrap::spec()]
}
