//! Runtime configuration versioning enums.

crate::pg_enum! {
    type_name = "qp_runtime_config_source",
    pub enum RuntimeConfigVersionSource {
        Bootstrap => "bootstrap",
        Operator => "operator",
        Import => "import",
    }
}

crate::pg_enum! {
    type_name = "qp_runtime_config_activation_kind",
    pub enum RuntimeConfigActivationKind {
        Initial => "initial",
        Promote => "promote",
        Rollback => "rollback",
    }
}
