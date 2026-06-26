//! Enums for the append-only general operation log (`operation_log`).
//!
//! This log is the operational activity trail (distinct from the governance
//! hash chain): it captures every mutating/auth HTTP operation for forensics.

active_string_enum! {
    /// Coarse grouping of an audited HTTP operation.
    pub enum OperationCategory {
        Auth => "auth",
        Rbac => "rbac",
        Governance => "governance",
        RuntimeConfig => "runtime_config",
        System => "system",
        Risk => "risk",
        QuantReport => "quant_report",
        Market => "market",
        Replay => "replay",
        Other => "other",
    }
}

active_string_enum! {
    /// Terminal outcome of an audited operation.
    pub enum OperationOutcome {
        Success => "success",
        Failure => "failure",
        Denied => "denied",
    }
}
