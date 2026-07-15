//! Shared serialization lock for report publication and entry submission.

use quant_pivot_error::storage::StorageError;
use quant_pivot_models::enums::quant::ReportKind;
use sea_orm::{ConnectionTrait, DatabaseBackend, Statement};

/// Serialize every authority-changing mutation for one report scope.
///
/// Callers must acquire this transaction advisory lock before any report,
/// recommendation, intent, condition, or capital row lock. A read-only probe
/// may precede it only to discover the scope and must be revalidated afterward.
pub(super) async fn acquire_report_scope_lock(
    db: &impl ConnectionTrait,
    profile_id: &str,
    kind: ReportKind,
) -> Result<(), StorageError> {
    let scope = format!("{profile_id}:{}", kind.as_str());
    db.execute(Statement::from_sql_and_values(
        DatabaseBackend::Postgres,
        "SELECT pg_advisory_xact_lock(hashtextextended($1, 0))",
        [scope.into()],
    ))
    .await
    .map_err(StorageError::from)?;
    Ok(())
}
