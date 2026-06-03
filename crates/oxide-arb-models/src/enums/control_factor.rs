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
        Completed => "completed",
        CompletedWithRejectedFactors => "completed_with_rejected_factors",
        ReportOnly => "report_only",
        Failed => "failed",
        Cancelled => "cancelled",
    }
}

active_string_enum! {
    /// Why a materialization run exists.
    pub enum MaterializationRunKind {
        Scheduled => "scheduled",
        Backfill => "backfill",
        Incident => "incident",
        ConfigComparison => "config_comparison",
        ForensicReport => "forensic_report",
    }
}

active_string_enum! {
    /// External or internal source that created a materialization run.
    pub enum RunTriggerType {
        Scheduled => "scheduled",
        Backfill => "backfill",
        Incident => "incident",
        ConfigComparison => "config_comparison",
        ForensicReport => "forensic_report",
    }
}

active_string_enum! {
    /// What a materialization run is allowed to persist.
    pub enum MaterializationOutputPolicy {
        EmitDraftCandidates => "emit_draft_candidates",
        EmitDraftOnly => "emit_draft_only",
        ReportOnly => "report_only",
        NoFactorOutput => "no_factor_output",
    }
}

active_string_enum! {
    /// Status of one evidence stage inside a materialization run.
    pub enum EvidenceStageStatus {
        Pending => "pending",
        Running => "running",
        Completed => "completed",
        CompletedWithWarnings => "completed_with_warnings",
        SkippedNotRequired => "skipped_not_required",
        InsufficientCoverage => "insufficient_coverage",
        ReportOnly => "report_only",
        Failed => "failed",
    }
}

active_string_enum! {
    /// Fixed stage names in the materialization graph.
    pub enum MaterializationStageName {
        ResolveInputs => "resolve_inputs",
        BookReconstruction => "book_reconstruction",
        DetectorEvidence => "detector_evidence",
        ExecutionEvidence => "execution_evidence",
        PortfolioRiskEvidence => "portfolio_risk_evidence",
        SettlementReconciliationEvidence => "settlement_reconciliation_evidence",
        FactorBuild => "factor_build",
        QualityGateEvaluation => "quality_gate_evaluation",
        DraftWrite => "draft_write",
    }
}

/// Stable materialization error code used by UI, alerts, and retry policy.
///
/// This enum intentionally implements custom serde so JSON uses the stable
/// dotted code strings rather than Rust variant names.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum MaterializationErrorCode {
    InputMarketMappingMissing,
    InputPitConfigMissing,
    InputCalibrationSnapshotMissing,
    InputFeeScheduleMissing,
    InputBalanceSnapshotMissing,
    InputTokenBalanceSnapshotMissing,
    InputSettlementTruthMissing,
    InputReconciliationStatusMissing,
    InputCurrentStateFallbackForbidden,
    ChCoverageL2Insufficient,
    ChBookSnapshotGap,
    AuditSettlementAttributionMissing,
    RiskSequenceIncomplete,
    GateSampleInsufficient,
    GateNotConservative,
    RunDedupeConflict,
    RunInvalidTransition,
    RunCancelled,
    PublicationLockConflict,
    SnapshotSchemaMismatch,
}

impl MaterializationErrorCode {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::InputMarketMappingMissing => "input.market_mapping_missing",
            Self::InputPitConfigMissing => "input.pit_config_missing",
            Self::InputCalibrationSnapshotMissing => "input.calibration_snapshot_missing",
            Self::InputFeeScheduleMissing => "input.fee_schedule_missing",
            Self::InputBalanceSnapshotMissing => "input.balance_snapshot_missing",
            Self::InputTokenBalanceSnapshotMissing => "input.token_balance_snapshot_missing",
            Self::InputSettlementTruthMissing => "input.settlement_truth_missing",
            Self::InputReconciliationStatusMissing => "input.reconciliation_status_missing",
            Self::InputCurrentStateFallbackForbidden => "input.current_state_fallback_forbidden",
            Self::ChCoverageL2Insufficient => "ch.coverage_l2_insufficient",
            Self::ChBookSnapshotGap => "ch.book_snapshot_gap",
            Self::AuditSettlementAttributionMissing => "audit.settlement_attribution_missing",
            Self::RiskSequenceIncomplete => "risk.sequence_incomplete",
            Self::GateSampleInsufficient => "gate.sample_insufficient",
            Self::GateNotConservative => "gate.not_conservative",
            Self::RunDedupeConflict => "run.dedupe_conflict",
            Self::RunInvalidTransition => "run.invalid_transition",
            Self::RunCancelled => "run.cancelled",
            Self::PublicationLockConflict => "publication.lock_conflict",
            Self::SnapshotSchemaMismatch => "snapshot.schema_mismatch",
        }
    }

    #[must_use]
    pub const fn is_retryable(self) -> bool {
        matches!(
            self,
            Self::ChCoverageL2Insufficient
                | Self::ChBookSnapshotGap
                | Self::AuditSettlementAttributionMissing
                | Self::RiskSequenceIncomplete
                | Self::PublicationLockConflict
        )
    }

    #[must_use]
    pub const fn is_fatal_for_production(self) -> bool {
        matches!(
            self,
            Self::InputMarketMappingMissing
                | Self::InputPitConfigMissing
                | Self::InputCalibrationSnapshotMissing
                | Self::InputFeeScheduleMissing
                | Self::InputBalanceSnapshotMissing
                | Self::InputTokenBalanceSnapshotMissing
                | Self::InputSettlementTruthMissing
                | Self::InputReconciliationStatusMissing
                | Self::InputCurrentStateFallbackForbidden
                | Self::RunInvalidTransition
                | Self::SnapshotSchemaMismatch
        )
    }
}

impl std::fmt::Display for MaterializationErrorCode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

impl std::str::FromStr for MaterializationErrorCode {
    type Err = String;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "input.market_mapping_missing" => Ok(Self::InputMarketMappingMissing),
            "input.pit_config_missing" => Ok(Self::InputPitConfigMissing),
            "input.calibration_snapshot_missing" => Ok(Self::InputCalibrationSnapshotMissing),
            "input.fee_schedule_missing" => Ok(Self::InputFeeScheduleMissing),
            "input.balance_snapshot_missing" => Ok(Self::InputBalanceSnapshotMissing),
            "input.token_balance_snapshot_missing" => Ok(Self::InputTokenBalanceSnapshotMissing),
            "input.settlement_truth_missing" => Ok(Self::InputSettlementTruthMissing),
            "input.reconciliation_status_missing" => Ok(Self::InputReconciliationStatusMissing),
            "input.current_state_fallback_forbidden" => {
                Ok(Self::InputCurrentStateFallbackForbidden)
            }
            "ch.coverage_l2_insufficient" => Ok(Self::ChCoverageL2Insufficient),
            "ch.book_snapshot_gap" => Ok(Self::ChBookSnapshotGap),
            "audit.settlement_attribution_missing" => Ok(Self::AuditSettlementAttributionMissing),
            "risk.sequence_incomplete" => Ok(Self::RiskSequenceIncomplete),
            "gate.sample_insufficient" => Ok(Self::GateSampleInsufficient),
            "gate.not_conservative" => Ok(Self::GateNotConservative),
            "run.dedupe_conflict" => Ok(Self::RunDedupeConflict),
            "run.invalid_transition" => Ok(Self::RunInvalidTransition),
            "run.cancelled" => Ok(Self::RunCancelled),
            "publication.lock_conflict" => Ok(Self::PublicationLockConflict),
            "snapshot.schema_mismatch" => Ok(Self::SnapshotSchemaMismatch),
            _ => Err(format!("unknown materialization error code: {value}")),
        }
    }
}

impl Serialize for MaterializationErrorCode {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_str(self.as_str())
    }
}

impl<'de> serde::Deserialize<'de> for MaterializationErrorCode {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        use std::str::FromStr;

        let value = String::deserialize(deserializer)?;
        Self::from_str(&value).map_err(serde::de::Error::custom)
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
