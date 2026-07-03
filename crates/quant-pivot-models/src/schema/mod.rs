//! Compile-time schema catalog for Postgres migrations.

pub mod catalog;
pub mod column;
pub mod dependency;
pub mod factor_names;
pub mod graph;
pub mod index;
pub mod pg_enum;
pub mod seed;
pub mod table;
pub mod trigger;
pub mod ui;

use sea_orm::sea_query::{ColumnDef, Expr, IntoIden, SimpleExpr};

/// Database-side timestamp for managed write-time columns.
///
/// `CURRENT_TIMESTAMP` is fixed at transaction start; `statement_timestamp()`
/// stays stable for one SQL statement without becoming stale in long
/// transactions.
pub fn write_timestamp() -> SimpleExpr {
    Expr::cust("statement_timestamp()")
}

/// Required `timestamptz` column with the canonical write-time default.
pub fn timestamp_with_write_default(column: impl IntoIden) -> ColumnDef {
    let mut column_def = ColumnDef::new(column);
    column_def
        .timestamp_with_time_zone()
        .not_null()
        .default(write_timestamp());
    column_def
}
