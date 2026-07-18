//! Sealed `PostgreSQL` statement batches for the Phase 11.9 schema generation.

use sea_orm_migration::prelude::*;

pub(in crate::migrations) const SOURCE: &[u8] = include_bytes!("phase_11_9.rs");

pub(in crate::migrations) async fn execute_batch(
    manager: &SchemaManager<'_>,
    sql: &'static str,
) -> Result<(), DbErr> {
    if sql.trim().is_empty() {
        return Err(DbErr::Custom(
            "Phase 11.9 PostgreSQL statement batch is empty".to_owned(),
        ));
    }
    manager
        .get_connection()
        .execute_unprepared(sql)
        .await
        .map(|_| ())
}
