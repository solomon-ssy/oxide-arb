use sea_orm::{DbBackend, Schema};
use sea_orm_migration::{prelude::*, sea_query::extension::postgres::Type};

use crate::{
    MigrationSpec, audit, migration_spec,
    snapshots::v1::{
        ARTIFACTS, ENUMS, MODULE_PREFIX, TABLES,
        sea_orm_active_enums::{QpCatalogFilterReason, QpMarketCategory},
    },
};

const NAME: &str = "m20260716_000001_schema_snapshot";
const SOURCE: &str = include_str!("m20260716_000001_schema_snapshot.rs");

pub struct Migration;

impl MigrationName for Migration {
    fn name(&self) -> &str {
        NAME
    }
}

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        let db = manager.get_connection();
        let schema = Schema::new(DbBackend::Postgres);
        for enum_statement in [
            schema.create_enum_from_active_enum::<QpCatalogFilterReason>(),
            schema.create_enum_from_active_enum::<QpMarketCategory>(),
        ] {
            manager
                .create_type(enum_statement.ok_or_else(|| {
                    DbErr::Custom("PostgreSQL native enum definition was not generated".to_owned())
                })?)
                .await?;
        }
        db.get_schema_registry(MODULE_PREFIX).apply(db).await?;
        audit::record(manager, spec()).await
    }

    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        audit::remove(manager, NAME).await?;
        for table in TABLES.iter().rev() {
            manager
                .drop_table(
                    Table::drop()
                        .table((Alias::new("public"), Alias::new(*table)))
                        .to_owned(),
                )
                .await?;
        }
        for enum_name in ENUMS.iter().rev() {
            manager
                .drop_type(
                    Type::drop()
                        .name((Alias::new("public"), Alias::new(*enum_name)))
                        .to_owned(),
                )
                .await?;
        }
        Ok(())
    }
}

pub fn spec() -> MigrationSpec {
    let mut artifacts = Vec::with_capacity(ARTIFACTS.len() + 1);
    artifacts.push(SOURCE.as_bytes());
    artifacts.extend_from_slice(ARTIFACTS);
    migration_spec(NAME, &artifacts)
}
