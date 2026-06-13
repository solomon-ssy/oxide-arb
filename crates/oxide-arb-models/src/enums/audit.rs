//! Audit lifecycle stage enums.

use crate::enums::common::TradeBusinessOutcome;
use serde::{Deserialize, Serialize};
use std::fmt::{self, Display, Formatter};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OpportunityAuditStage {
    Detected,
    ValidationRejected,
    FactorValidationRejected,
    RiskRejected,
    SizingRejected,
    Filled,
    Missed,
    Failed,
    Settled,
}

impl OpportunityAuditStage {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Detected => "detected",
            Self::ValidationRejected => "validation_rejected",
            Self::FactorValidationRejected => "factor_validation_rejected",
            Self::RiskRejected => "risk_rejected",
            Self::SizingRejected => "sizing_rejected",
            Self::Filled => "filled",
            Self::Missed => "missed",
            Self::Failed => "failed",
            Self::Settled => "settled",
        }
    }

    #[must_use]
    pub const fn order(self) -> u8 {
        match self {
            Self::Detected => 10,
            Self::ValidationRejected => 20,
            Self::FactorValidationRejected => 25,
            Self::RiskRejected => 30,
            Self::SizingRejected => 40,
            Self::Filled => 70,
            Self::Missed => 71,
            Self::Failed => 72,
            Self::Settled => 90,
        }
    }

    #[must_use]
    pub fn from_rejection_stage(stage: &str) -> Self {
        match stage {
            "validation" => Self::ValidationRejected,
            "factor_validation" => Self::FactorValidationRejected,
            "risk" => Self::RiskRejected,
            "sizing" => Self::SizingRejected,
            _ => Self::Detected,
        }
    }

    #[must_use]
    pub const fn from_business_outcome(outcome: TradeBusinessOutcome) -> Self {
        match outcome {
            TradeBusinessOutcome::Success => Self::Filled,
            TradeBusinessOutcome::Miss => Self::Missed,
            TradeBusinessOutcome::Failed => Self::Failed,
        }
    }
}

impl Display for OpportunityAuditStage {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Terminal business conclusion recorded on an audit row.
///
/// Combines the rejection short-circuit (`Rejected`) and the settlement
/// terminal (`Settled`) with the execution outcome bucket
/// ([`TradeBusinessOutcome`]). Wire format is `snake_case`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AuditOutcome {
    /// Opportunity rejected before order submission.
    Rejected,
    /// Position settled (market resolved).
    Settled,
    /// Order filled, position opened.
    Success,
    /// FOK order not filled.
    Miss,
    /// Order submission failed or errored.
    Failed,
}

impl AuditOutcome {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Rejected => "rejected",
            Self::Settled => "settled",
            Self::Success => "success",
            Self::Miss => "miss",
            Self::Failed => "failed",
        }
    }
}

impl Display for AuditOutcome {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Pipeline stage at which an opportunity was rejected.
///
/// Mirrors the rejection short-circuit points of the execution pipeline;
/// `Other` captures stages introduced after a row was written. Wire format
/// is `snake_case`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RejectionStage {
    /// Pre-execution validation (book/price re-check).
    Validation,
    /// Risk gate (exposure, breaker, blacklist, …).
    Risk,
    /// Position sizing (Kelly / capital reservation).
    Sizing,
    /// Order submit or trade persistence failure.
    SubmitPersist,
    /// Control-factor execution-quality validation.
    FactorValidation,
    /// Unknown / later-introduced stage.
    Other,
}

impl RejectionStage {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Validation => "validation",
            Self::Risk => "risk",
            Self::Sizing => "sizing",
            Self::SubmitPersist => "submit_persist",
            Self::FactorValidation => "factor_validation",
            Self::Other => "other",
        }
    }
}

impl Display for RejectionStage {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Win/loss conclusion of a settled position, as recorded on the audit row.
///
/// Wire format is `snake_case`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SettlementOutcome {
    /// Position held the winning token.
    Won,
    /// Position held the losing token.
    Lost,
}

impl SettlementOutcome {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Won => "won",
            Self::Lost => "lost",
        }
    }
}

impl Display for SettlementOutcome {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}
