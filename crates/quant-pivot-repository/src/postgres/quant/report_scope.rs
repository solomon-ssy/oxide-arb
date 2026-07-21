//! Shared serialization lock for report publication and entry submission.

use quant_pivot_error::storage::StorageError;
use quant_pivot_models::{enums::quant::ReportKind, types::ResearchProfileId};
use sea_orm::ConnectionTrait;

use crate::postgres::primitives;

/// Serialize every authority-changing mutation for one report scope.
///
/// Callers must acquire this transaction advisory lock before any report,
/// recommendation, intent, condition, or capital row lock. A read-only probe
/// may precede it only to discover the scope and must be revalidated afterward.
pub(super) async fn acquire_report_scope_lock(
    db: &impl ConnectionTrait,
    profile_id: &ResearchProfileId,
    kind: ReportKind,
) -> Result<(), StorageError> {
    let scope = format!("{profile_id}:{}", kind.as_str());
    primitives::advisory_text_xact_lock(db, &scope, 0).await
}
