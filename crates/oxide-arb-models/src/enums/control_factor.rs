//! Control-factor lifecycle enums.

use serde::{Deserialize, Serialize};

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

/// Behavior when a control factor or publication TTL elapses.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FactorExpiryBehavior {
    /// Remove factor effect and continue with baseline policy.
    FailNeutral,
    /// Fail closed when severity is critical; otherwise neutral.
    FailClosedIfCritical,
    /// Drop factor after TTL unless a manual halt/blacklist already applies.
    NeutralAfterTtlUnlessManualHalt,
}

/// Behavior when a typed factor cannot be loaded into `ControlFactorSnapshot`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FactorLoadFailureBehavior {
    FailNeutral,
    EmptyBucketIndex,
    BaselineFillModel,
    BaselineSizer,
    FailClosedInLiveIfConfigured,
    UseBlacklistOrManualHalt,
}

impl ControlFactorType {
    /// Frozen expiry semantics from `phase5.0` §5.
    #[must_use]
    pub const fn expiry_behavior(self) -> FactorExpiryBehavior {
        match self {
            Self::BucketRisk | Self::ExecutionQuality | Self::PortfolioRisk => {
                FactorExpiryBehavior::FailNeutral
            }
            Self::ReconciliationHealth => FactorExpiryBehavior::FailClosedIfCritical,
            Self::MarketAnomaly => FactorExpiryBehavior::NeutralAfterTtlUnlessManualHalt,
        }
    }

    /// Frozen load-failure semantics from `phase5.0` §5.
    #[must_use]
    pub const fn load_failure_behavior(
        self,
        severity: FactorSeverity,
    ) -> FactorLoadFailureBehavior {
        match self {
            Self::BucketRisk => FactorLoadFailureBehavior::EmptyBucketIndex,
            Self::ExecutionQuality => FactorLoadFailureBehavior::BaselineFillModel,
            Self::PortfolioRisk => FactorLoadFailureBehavior::BaselineSizer,
            Self::ReconciliationHealth => match severity {
                FactorSeverity::Critical => FactorLoadFailureBehavior::FailClosedInLiveIfConfigured,
                FactorSeverity::Info | FactorSeverity::Warning => {
                    FactorLoadFailureBehavior::FailNeutral
                }
            },
            Self::MarketAnomaly => FactorLoadFailureBehavior::UseBlacklistOrManualHalt,
        }
    }

    /// Resolves effective expiry behavior for a persisted payload severity.
    #[must_use]
    pub const fn effective_expiry_behavior(self, severity: FactorSeverity) -> FactorExpiryBehavior {
        match (self, self.expiry_behavior()) {
            (Self::ReconciliationHealth, FactorExpiryBehavior::FailClosedIfCritical) => {
                match severity {
                    FactorSeverity::Critical => FactorExpiryBehavior::FailClosedIfCritical,
                    FactorSeverity::Info | FactorSeverity::Warning => {
                        FactorExpiryBehavior::FailNeutral
                    }
                }
            }
            (_, behavior) => behavior,
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::enums::control_factor::{
        ControlFactorType, FactorExpiryBehavior, FactorLoadFailureBehavior, FactorSeverity,
    };

    #[test]
    fn reconciliation_critical_load_fails_closed() {
        assert_eq!(
            ControlFactorType::ReconciliationHealth.load_failure_behavior(FactorSeverity::Critical),
            FactorLoadFailureBehavior::FailClosedInLiveIfConfigured
        );
    }

    #[test]
    fn bucket_risk_expiry_is_neutral() {
        assert_eq!(
            ControlFactorType::BucketRisk.expiry_behavior(),
            FactorExpiryBehavior::FailNeutral
        );
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
