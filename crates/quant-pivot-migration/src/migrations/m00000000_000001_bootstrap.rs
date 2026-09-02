//! Single clean-install `PostgreSQL` baseline.

use sea_orm::{
    ActiveValue::{NotSet, Set},
    DbBackend, EntityTrait, Schema,
    sea_query::Expr,
};
use sea_orm_migration::prelude::*;

use super::support::{
    column_defaults, column_defaults::SOURCE as COLUMN_DEFAULTS_SQL, query_indexes,
    query_indexes::SOURCE as QUERY_INDEXES_SQL, relational_invariants,
    relational_invariants::SOURCE as RELATIONAL_INVARIANTS_SQL, v1, v1::SOURCE as V1_SCHEMA_SQL,
    worm_triggers, worm_triggers::SOURCE as WORM_TRIGGERS_SQL,
};
use crate::{
    MigrationSpec, audit, migration_spec,
    snapshots::v1::{
        ARTIFACTS, MODULE_PREFIX, SCHEMA_ENUMS, SCHEMA_TABLES,
        policy_activation_guard::{ActiveModel, Entity},
        sea_orm_active_enums::{
            QpCatalogFilterReason, QpEntryAuthorizationPolicy, QpExecutionAuthorityCeiling,
            QpMarketCategory,
        },
    },
};

const NAME: &str = "m00000000_000001_bootstrap";
const SOURCE: &str = include_str!("m00000000_000001_bootstrap.rs");

#[derive(DeriveIden)]
enum SchemaMigrationAudit {
    Table,
    Version,
    ChecksumAlgorithm,
    Checksum,
    ArtifactLength,
    MigrationEngine,
    AppliedAt,
}

pub struct Migration;

impl Migration {
    pub fn spec() -> MigrationSpec {
        let mut artifacts =
            Vec::with_capacity(ARTIFACTS.len() + SCHEMA_TABLES.len() + SCHEMA_ENUMS.len() + 6);
        artifacts.push(SOURCE.as_bytes());
        artifacts.extend_from_slice(ARTIFACTS);
        artifacts.extend(SCHEMA_TABLES.iter().map(|table| table.as_bytes()));
        artifacts.extend(
            SCHEMA_ENUMS
                .iter()
                .map(|native_enum| native_enum.as_bytes()),
        );
        artifacts.extend([
            RELATIONAL_INVARIANTS_SQL,
            QUERY_INDEXES_SQL,
            WORM_TRIGGERS_SQL,
            COLUMN_DEFAULTS_SQL,
            V1_SCHEMA_SQL,
        ]);
        migration_spec(NAME, &artifacts)
    }
}

impl MigrationName for Migration {
    fn name(&self) -> &str {
        NAME
    }
}

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        v1::assert_empty_boot_target(manager).await?;
        let db = manager.get_connection();
        let schema = Schema::new(DbBackend::Postgres);
        for enum_statement in [
            schema.create_enum_from_active_enum::<QpCatalogFilterReason>(),
            schema.create_enum_from_active_enum::<QpMarketCategory>(),
            schema.create_enum_from_active_enum::<QpEntryAuthorizationPolicy>(),
            schema.create_enum_from_active_enum::<QpExecutionAuthorityCeiling>(),
        ] {
            manager
                .create_type(enum_statement.ok_or_else(|| {
                    DbErr::Custom("PostgreSQL native enum definition was not generated".to_owned())
                })?)
                .await?;
        }
        // SeaORM 2.0 does not discover an enum nested only in a PostgreSQL
        // array column. All columns that share those types use an explicit
        // custom column type so Entity First has one deterministic enum owner.
        // The target is proven empty, therefore Entity First `apply` is the
        // deterministic boot primitive; experimental non-destructive `sync`
        // must never hide a schema mismatch.
        db.get_schema_registry(MODULE_PREFIX)
            .apply(db)
            .await
            .map_err(|error| DbErr::Custom(format!("Entity First schema apply failed: {error}")))?;
        column_defaults::apply(manager)
            .await
            .map_err(|error| DbErr::Custom(format!("column defaults failed: {error}")))?;
        seed_boot_rows(manager)
            .await
            .map_err(|error| DbErr::Custom(format!("boot row seed failed: {error}")))?;
        create_migration_audit(manager)
            .await
            .map_err(|error| DbErr::Custom(format!("migration audit schema failed: {error}")))?;
        relational_invariants::apply(manager)
            .await
            .map_err(|error| DbErr::Custom(format!("relational invariants failed: {error}")))?;
        query_indexes::apply(manager)
            .await
            .map_err(|error| DbErr::Custom(format!("query indexes failed: {error}")))?;
        worm_triggers::apply(manager)
            .await
            .map_err(|error| DbErr::Custom(format!("WORM triggers failed: {error}")))?;
        audit::record(manager, Self::spec())
            .await
            .map_err(|error| DbErr::Custom(format!("migration artifact audit failed: {error}")))
    }

    async fn down(&self, _manager: &SchemaManager) -> Result<(), DbErr> {
        Err(DbErr::Custom(
            "the fresh-bootstrap schema has no down path; use the guarded disposable reset command against an explicitly owned empty environment"
                .to_owned(),
        ))
    }
}

async fn seed_boot_rows(manager: &SchemaManager<'_>) -> Result<(), DbErr> {
    Entity::insert(ActiveModel {
        id: Set(1),
        generation: Set(1),
        current_snapshot_id: Set(None),
        current_snapshot_hash: Set(None),
        created_at: NotSet,
        updated_at: NotSet,
    })
    .exec_without_returning(manager.get_connection())
    .await?;
    Ok(())
}

async fn create_migration_audit(manager: &SchemaManager<'_>) -> Result<(), DbErr> {
    manager
        .create_table(
            Table::create()
                .table((Alias::new("public"), SchemaMigrationAudit::Table))
                .col(
                    ColumnDef::new(SchemaMigrationAudit::Version)
                        .string()
                        .not_null()
                        .primary_key(),
                )
                .col(
                    ColumnDef::new(SchemaMigrationAudit::ChecksumAlgorithm)
                        .string()
                        .not_null(),
                )
                .col(
                    ColumnDef::new(SchemaMigrationAudit::Checksum)
                        .string()
                        .not_null(),
                )
                .col(
                    ColumnDef::new(SchemaMigrationAudit::ArtifactLength)
                        .big_integer()
                        .not_null(),
                )
                .col(
                    ColumnDef::new(SchemaMigrationAudit::MigrationEngine)
                        .string()
                        .not_null(),
                )
                .col(
                    ColumnDef::new(SchemaMigrationAudit::AppliedAt)
                        .timestamp_with_time_zone()
                        .not_null()
                        .default(Expr::current_timestamp()),
                )
                .to_owned(),
        )
        .await
}
