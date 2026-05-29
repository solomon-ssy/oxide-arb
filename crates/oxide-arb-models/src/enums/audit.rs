//! Audit lifecycle stage enums.

use crate::enums::common::TradeBusinessOutcome;
use serde::{Deserialize, Serialize};
use std::fmt::{self, Display, Formatter};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OpportunityAuditStage {
    Detected,
    ValidationRejected,
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
