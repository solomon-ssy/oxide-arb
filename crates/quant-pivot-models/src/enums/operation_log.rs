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
    type_name = "qp_operation_http_method",
    /// Finite transport origin for an audited action. `System` identifies
    /// internal governed work with no HTTP request.
    pub enum OperationHttpMethod {
        Get => "GET",
        Post => "POST",
        Put => "PUT",
        Patch => "PATCH",
        Delete => "DELETE",
        System => "SYSTEM",
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
