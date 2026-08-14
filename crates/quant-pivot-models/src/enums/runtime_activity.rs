//! Cross-domain runtime activity classifications used by the operator UI.

crate::wire_enum! {
    /// Durable fact family represented in the Activity Center.
    @from_str(trim)
    pub enum RuntimeActivityDomain {
        Research => "research",
        Report => "report",
        Execution => "execution",
        Reconciliation => "reconciliation",
        Settlement => "settlement",
    }
}

crate::wire_enum! {
    /// Normalized lifecycle class shared by otherwise unrelated fact ledgers.
    @from_str(trim)
    pub enum RuntimeActivityStatus {
        Pending => "pending",
        Running => "running",
        Succeeded => "succeeded",
        Failed => "failed",
        Cancelled => "cancelled",
        Attention => "attention",
        Skipped => "skipped",
    }
}

crate::wire_enum! {
    /// Existing domain mutation surfaced contextually by an activity item.
    @from_str(trim)
    pub enum RuntimeActivityActionKind {
        CancelResearchJob => "cancel_research_job",
        RetryResearchJob => "retry_research_job",
        RetryReportRun => "retry_report_run",
        ResolveReconciliation => "resolve_reconciliation",
    }
}
