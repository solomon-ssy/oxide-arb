//! Shared serialization lock for report publication and entry submission.

use quant_pivot_error::storage::StorageError;
use quant_pivot_models::{enums::quant::ReportKind, types::ResearchProfileId};
use sea_orm::ConnectionTrait;

use crate::postgres::primitives;

/// Stable identity for the serialization scope shared by report publication
/// and entry submission.
pub(super) struct ReportScope<'a> {
    profile_id: &'a ResearchProfileId,
    kind: ReportKind,
}

impl<'a> ReportScope<'a> {
    pub(super) const fn new(profile_id: &'a ResearchProfileId, kind: ReportKind) -> Self {
        Self { profile_id, kind }
    }

    /// Serialize every authority-changing mutation for this report scope.
    ///
    /// Callers must acquire this transaction advisory lock before any report,
    /// recommendation, intent, condition, or capital row lock. A read-only
    /// probe may precede it only to discover the scope and must be revalidated
    /// afterward.
    pub(super) async fn acquire(self, db: &impl ConnectionTrait) -> Result<(), StorageError> {
        let scope = format!("{}:{}", self.profile_id, self.kind.as_str());
        primitives::advisory_text_xact_lock(db, &scope, 0).await
    }
}
