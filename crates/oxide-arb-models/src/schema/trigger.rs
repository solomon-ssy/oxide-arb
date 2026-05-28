//! Trigger metadata for catalog-driven migrations.

/// Trigger kinds the migration layer knows how to materialize.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TriggerKind {
    UpdatedAt,
}

/// A catalog entry for one table trigger.
#[derive(Clone, Copy)]
pub struct TriggerSpec {
    pub kind: TriggerKind,
    pub table_name: fn() -> String,
}

impl TriggerSpec {
    pub const fn updated_at(table_name: fn() -> String) -> Self {
        Self {
            kind: TriggerKind::UpdatedAt,
            table_name,
        }
    }
}
