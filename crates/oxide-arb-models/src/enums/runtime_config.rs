//! Runtime configuration versioning enums.

active_string_enum! {
    /// Source that created an immutable runtime config version.
    pub enum RuntimeConfigVersionSource {
        Bootstrap => "bootstrap",
        Operator => "operator",
        Import => "import",
    }
}

active_string_enum! {
    /// Activation reason type for runtime config lineage.
    pub enum RuntimeConfigActivationKind {
        Initial => "initial",
        Promote => "promote",
        Rollback => "rollback",
    }
}
