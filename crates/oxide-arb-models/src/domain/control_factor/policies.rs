//! Expiry and snapshot load-failure semantics frozen in Phase 5.0 §5.

use crate::enums::control_factor::{ControlFactorType, FactorSeverity};
use serde::{Deserialize, Serialize};

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
    use super::{FactorExpiryBehavior, FactorLoadFailureBehavior};
    use crate::enums::control_factor::{ControlFactorType, FactorSeverity};

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
