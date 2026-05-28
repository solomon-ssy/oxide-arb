//! Table and seed dependency metadata.

/// Why one table or seed depends on another schema object.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DependencyKind {
    /// The dependency is enforced by a foreign key.
    ForeignKey,
    /// The dependency only affects seed execution order.
    SeedOnly,
    /// The dependency exists because a trigger references another object.
    TriggerOnly,
    /// Operational dependency without a database constraint.
    Operational,
}

/// A dependency edge from the current schema object to another table.
#[derive(Clone, Copy)]
pub struct TableDependency {
    pub table_name: fn() -> String,
    pub kind: DependencyKind,
}

impl TableDependency {
    pub const fn new(table_name: fn() -> String, kind: DependencyKind) -> Self {
        Self { table_name, kind }
    }

    pub const fn foreign_key(table_name: fn() -> String) -> Self {
        Self::new(table_name, DependencyKind::ForeignKey)
    }
}
