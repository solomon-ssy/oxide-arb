//! Trigger metadata for catalog-driven migrations.

/// Trigger kinds the migration layer knows how to materialize.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TriggerKind {
    /// Maintains a `updated_at` column on row mutation.
    UpdatedAt,
    /// Enforces write-once-read-many (WORM) semantics: any `UPDATE` or `DELETE`
    /// raises an exception at the database level. Auto-registered for every
    /// `lifecycle = "audit"` table so tamper-resistance does not depend on
    /// application-layer discipline.
    AppendOnly,
}

/// A catalog entry for one table trigger.
#[derive(Clone, Copy)]
pub struct TriggerSpec {
    pub kind: TriggerKind,
    pub table_name: fn() -> String,
}

impl TriggerSpec {
    /// `updated_at` maintenance trigger.
    pub const fn updated_at(table_name: fn() -> String) -> Self {
        Self {
            kind: TriggerKind::UpdatedAt,
            table_name,
        }
    }

    /// Append-only (WORM) guard trigger rejecting `UPDATE` / `DELETE`.
    pub const fn append_only(table_name: fn() -> String) -> Self {
        Self {
            kind: TriggerKind::AppendOnly,
            table_name,
        }
    }
}
