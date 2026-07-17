use sea_orm_migration::prelude::*;

use crate::{CHECKSUM_ALGORITHM, MIGRATION_ENGINE, MigrationSpec};

pub async fn record(manager: &SchemaManager<'_>, spec: MigrationSpec) -> Result<(), DbErr> {
    let mut insert = Query::insert();
    insert
        .into_table((Alias::new("public"), Alias::new("schema_migration_audit")))
        .columns([
            Alias::new("version"),
            Alias::new("checksum_algorithm"),
            Alias::new("checksum"),
            Alias::new("artifact_length"),
            Alias::new("migration_engine"),
        ])
        .values([
            spec.version.into(),
            CHECKSUM_ALGORITHM.into(),
            spec.checksum.into(),
            spec.artifact_length.into(),
            MIGRATION_ENGINE.into(),
        ])
        .map_err(|error| DbErr::Custom(format!("build migration audit insert: {error}")))?;
    manager.get_connection().execute(&insert).await?;
    Ok(())
}

pub async fn remove(manager: &SchemaManager<'_>, version: &str) -> Result<(), DbErr> {
    let delete = Query::delete()
        .from_table((Alias::new("public"), Alias::new("schema_migration_audit")))
        .and_where(Expr::col(Alias::new("version")).eq(version))
        .to_owned();
    manager.get_connection().execute(&delete).await?;
    Ok(())
}
