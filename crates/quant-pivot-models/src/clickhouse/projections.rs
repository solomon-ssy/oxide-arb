//! Domain → `ClickHouse` row boundary conversions for quant fact enums.

use crate::enums::{
    clickhouse::{
        ChCapitalAllocationState, ChExecutionSide, ChFactorDirection, ChFeatureSourceKind,
        ChFeatureValueKind, ChOutcomeSide, ChPositionLedgerState,
        ChRecommendationAttributionOutcome, ChRecommendationStatus,
    },
    common::Side,
    execution::{CapitalAllocationState, PositionLedgerState},
    feature::{EvidenceSourceKind, FeatureValueKind},
    quant::{FactorDirection, OutcomeSide, RecommendationAttributionOutcome, RecommendationStatus},
};

impl From<Side> for ChExecutionSide {
    fn from(value: Side) -> Self {
        match value {
            Side::Buy => Self::Buy,
            Side::Sell => Self::Sell,
        }
    }
}

impl From<OutcomeSide> for ChOutcomeSide {
    fn from(value: OutcomeSide) -> Self {
        match value {
            OutcomeSide::Yes => Self::Yes,
            OutcomeSide::No => Self::No,
        }
    }
}

impl From<CapitalAllocationState> for ChCapitalAllocationState {
    fn from(value: CapitalAllocationState) -> Self {
        match value {
            CapitalAllocationState::Allocated => Self::Allocated,
            CapitalAllocationState::Locked => Self::Locked,
            CapitalAllocationState::Spent => Self::Spent,
            CapitalAllocationState::Released => Self::Released,
            CapitalAllocationState::Impaired => Self::Impaired,
        }
    }
}

impl From<PositionLedgerState> for ChPositionLedgerState {
    fn from(value: PositionLedgerState) -> Self {
        match value {
            PositionLedgerState::Open => Self::Open,
            PositionLedgerState::Closing => Self::Closing,
            PositionLedgerState::Closed => Self::Closed,
            PositionLedgerState::Settled => Self::Settled,
        }
    }
}

impl From<RecommendationStatus> for ChRecommendationStatus {
    fn from(value: RecommendationStatus) -> Self {
        match value {
            RecommendationStatus::Published => Self::Published,
            RecommendationStatus::Revoked => Self::Revoked,
            RecommendationStatus::Expired => Self::Expired,
            RecommendationStatus::IntentCreated => Self::IntentCreated,
            RecommendationStatus::Executed => Self::Executed,
            RecommendationStatus::Attributed => Self::Attributed,
        }
    }
}

impl From<RecommendationAttributionOutcome> for ChRecommendationAttributionOutcome {
    fn from(value: RecommendationAttributionOutcome) -> Self {
        match value {
            RecommendationAttributionOutcome::FilledExited => Self::FilledExited,
            RecommendationAttributionOutcome::FilledSettled => Self::FilledSettled,
            RecommendationAttributionOutcome::ExpiredUnfilled => Self::ExpiredUnfilled,
            RecommendationAttributionOutcome::CancelledUnfilled => Self::CancelledUnfilled,
            RecommendationAttributionOutcome::FailedUnfilled => Self::FailedUnfilled,
        }
    }
}

impl From<FactorDirection> for ChFactorDirection {
    fn from(value: FactorDirection) -> Self {
        match value {
            FactorDirection::Positive => Self::Positive,
            FactorDirection::Neutral => Self::Neutral,
            FactorDirection::Negative => Self::Negative,
        }
    }
}

impl From<FeatureValueKind> for ChFeatureValueKind {
    fn from(value: FeatureValueKind) -> Self {
        Self::from_i8(value.as_i8()).expect("feature value kind codes are stable")
    }
}

impl From<EvidenceSourceKind> for ChFeatureSourceKind {
    fn from(value: EvidenceSourceKind) -> Self {
        match value {
            EvidenceSourceKind::Book => Self::Book,
            EvidenceSourceKind::GammaMetadata => Self::GammaMetadata,
            EvidenceSourceKind::ClickHouseFact => Self::ClickHouseFact,
            EvidenceSourceKind::DomainExternal => Self::DomainExternal,
            EvidenceSourceKind::Derived => Self::Derived,
        }
    }
}

impl ChFeatureValueKind {
    /// Decode a persisted `value_kind` code, rejecting unknown values.
    #[must_use]
    pub const fn from_i8(code: i8) -> Option<Self> {
        match code {
            0 => Some(Self::Decimal),
            1 => Some(Self::Probability),
            2 => Some(Self::Bps),
            3 => Some(Self::Usd),
            4 => Some(Self::Count),
            5 => Some(Self::Bool),
            6 => Some(Self::Category),
            _ => None,
        }
    }
}
