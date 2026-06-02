//! Control-factor lifecycle enums.

active_string_enum! {
    /// Typed control-factor families supported by Phase 5.
    pub enum ControlFactorType {
        BucketRisk => "bucket_risk",
        ExecutionQuality => "execution_quality",
        PortfolioRisk => "portfolio_risk",
        ReconciliationHealth => "reconciliation_health",
        MarketAnomaly => "market_anomaly",
    }
}

active_string_enum! {
    /// Registry status of a single factor value row.
    pub enum FactorStatus {
        Draft => "draft",
        /// Evidence or settlement truth insufficient for promotion; operator-visible only.
        ReportOnly => "report_only",
        Candidate => "candidate",
        Rejected => "rejected",
        Shadow => "shadow",
        Published => "published",
        Superseded => "superseded",
        Expired => "expired",
        RolledBack => "rolled_back",
    }
}

active_string_enum! {
    /// Whether a publication is shadow-only or live-effective.
    pub enum PublicationMode {
        Shadow => "shadow",
        Published => "published",
    }
}

active_string_enum! {
    /// Lifecycle of a publication pointer.
    pub enum PublicationStatus {
        Pending => "pending",
        Active => "active",
        Superseded => "superseded",
        Expired => "expired",
        RolledBack => "rolled_back",
        Rejected => "rejected",
    }
}

active_string_enum! {
    /// Materialization run lifecycle.
    pub enum MaterializationRunStatus {
        Queued => "queued",
        Running => "running",
        Succeeded => "succeeded",
        PartialFailed => "partial_failed",
        Failed => "failed",
        Cancelled => "cancelled",
    }
}

active_string_enum! {
    /// Status of one evidence stage inside a materialization run.
    pub enum EvidenceStageStatus {
        Pending => "pending",
        Running => "running",
        Succeeded => "succeeded",
        InsufficientCoverage => "insufficient_coverage",
        Failed => "failed",
        Skipped => "skipped",
    }
}

active_string_enum! {
    /// Immutable audit event categories for control-factor governance.
    pub enum ControlAuditEventType {
        FactorCreated => "factor_created",
        FactorTransitioned => "factor_transitioned",
        FactorRejected => "factor_rejected",
        PublicationCreated => "publication_created",
        PublicationActivated => "publication_activated",
        PublicationRolledBack => "publication_rolled_back",
        PublicationExpired => "publication_expired",
        SnapshotLoadFailed => "snapshot_load_failed",
    }
}

active_string_enum! {
    /// Operational severity used by anomaly and reconciliation factors.
    pub enum FactorSeverity {
        Info => "info",
        Warning => "warning",
        Critical => "critical",
    }
}
