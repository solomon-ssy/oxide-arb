//! Enums for the append-only general operation log (`operation_log`).

pg_enum! {
    type_name = "qp_operation_category",
    pub enum OperationCategory {
        Auth => "auth",
        Rbac => "rbac",
        Governance => "governance",
        DecisionPolicySnapshot => "config",
        System => "system",
        Risk => "risk",
        QuantReport => "quant_report",
        Market => "market",
        Replay => "replay",
        Other => "other",
    }
}

pg_enum! {
    type_name = "qp_operation_outcome",
    pub enum OperationOutcome {
        Success => "success",
        Failure => "failure",
        Denied => "denied",
    }
}
