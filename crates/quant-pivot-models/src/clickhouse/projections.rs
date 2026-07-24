//! Domain → `ClickHouse` row boundary conversions for quant fact enums.

use crate::enums::{
    clickhouse::{
        ChCapitalAllocationState, ChExecutionSide, ChFactorDirection, ChFactorValueState,
        ChFeatureSourceKind, ChFeatureValueKind, ChNormalizationSource, ChOutcomeSide,
        ChPositionLedgerState,
    },
    common::Side,
    execution::{CapitalAllocationState, PositionLedgerState},
    factor::{FactorValueState, NormalizationSource},
    feature::{EvidenceSourceKind, FeatureValueKind},
    quant::{FactorDirection, OutcomeSide},
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

impl From<FactorDirection> for ChFactorDirection {
    fn from(value: FactorDirection) -> Self {
        match value {
            FactorDirection::Positive => Self::Positive,
            FactorDirection::Neutral => Self::Neutral,
            FactorDirection::Negative => Self::Negative,
        }
    }
}

impl From<FactorValueState> for ChFactorValueState {
    fn from(value: FactorValueState) -> Self {
        match value {
            FactorValueState::Scored => Self::Scored,
            FactorValueState::MissingInput => Self::MissingInput,
            FactorValueState::NotApplicable => Self::NotApplicable,
            FactorValueState::Indeterminate => Self::Indeterminate,
        }
    }
}

impl From<NormalizationSource> for ChNormalizationSource {
    fn from(value: NormalizationSource) -> Self {
        match value {
            NormalizationSource::CrossSection => Self::CrossSection,
            NormalizationSource::PerMarket => Self::PerMarket,
            NormalizationSource::FrozenReferenceQuantile => Self::FrozenReferenceQuantile,
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
            EvidenceSourceKind::TradeTape => Self::TradeTape,
            EvidenceSourceKind::Derived => Self::Derived,
            EvidenceSourceKind::DomainExternal => Self::DomainExternal,
            EvidenceSourceKind::Linkage => Self::Linkage,
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
