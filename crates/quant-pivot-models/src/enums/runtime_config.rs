//! Runtime configuration versioning enums.

pg_enum! {
    type_name = "qp_runtime_config_source",
    pub enum RuntimeConfigVersionSource {
        Bootstrap => "bootstrap",
        Operator => "operator",
        Import => "import",
    }
}

pg_enum! {
    type_name = "qp_runtime_config_approval_decision",
    pub enum RuntimeConfigApprovalDecision {
        Approved => "approved",
        Rejected => "rejected",
    }
}

pg_enum! {
    type_name = "qp_runtime_config_activation_kind",
    pub enum RuntimeConfigActivationKind {
        Initial => "initial",
        Promote => "promote",
        Rollback => "rollback",
    }
}
