//! Strongly typed `ClickHouse` `Enum8` values for quant-pivot fact rows.

use serde_repr::{Deserialize_repr, Serialize_repr};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize_repr, Deserialize_repr)]
#[repr(i8)]
pub enum ChBookEventType {
    Snapshot = 1,
    Delta = 2,
    Bbo = 3,
    TickSizeChange = 4,
    LastTrade = 5,
    // 6 reserved (formerly `MarketResolved`): settlement now lives in its own
    // typed `market_resolution_event` fact, not the book/tick event stream.
    ShardStatus = 7,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize_repr, Deserialize_repr)]
#[repr(i8)]
pub enum ChFactSource {
    WsSnapshot = 1,
    WsDelta = 2,
    WsBbo = 3,
    WsTickSize = 4,
    WsLastTrade = 5,
    WsMarketResolved = 6,
    QuantPipeline = 7,
    Execution = 8,
    WsShardStatus = 9,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize_repr, Deserialize_repr)]
#[repr(i8)]
pub enum ChSnapshotReason {
    Startup = 1,
    Reconnect = 2,
    Gap = 3,
    Periodic = 4,
    Manual = 5,
    WsSnapshot = 6,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize_repr, Deserialize_repr)]
#[repr(i8)]
pub enum ChBookDecisionStage {
    FeatureGenerated = 1,
    FactorScored = 2,
    ModelScored = 3,
    PortfolioPruned = 4,
    RecommendationPublished = 5,
    IntentCreated = 6,
    ExecutionUpdated = 7,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize_repr, Deserialize_repr)]
#[repr(i8)]
pub enum ChBookQuality {
    Fresh = 1,
    Stale = 2,
    Crossed = 3,
    Gap = 4,
    Invalid = 5,
    Insufficient = 6,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize_repr, Deserialize_repr)]
#[repr(i8)]
pub enum ChBookEvidenceTier {
    ExactReplay = 1,
    DecisionContext = 2,
    AggregateOnly = 3,
    Insufficient = 4,
}

// ── Quant pipeline facts (snake_case SQL labels) ─────────────────────────

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize_repr, Deserialize_repr)]
#[repr(i8)]
pub enum ChExitSignalEvaluatorKind {
    Reinference = 1,
    Opportunistic = 2,
}

impl ChExitSignalEvaluatorKind {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Reinference => "reinference",
            Self::Opportunistic => "opportunistic",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize_repr, Deserialize_repr)]
#[repr(i8)]
pub enum ChExitSignalVerdict {
    ThesisInvalidated = 1,
    OpportunisticSell = 2,
    Holds = 3,
    Indeterminate = 4,
}

impl ChExitSignalVerdict {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::ThesisInvalidated => "thesis_invalidated",
            Self::OpportunisticSell => "opportunistic_sell",
            Self::Holds => "holds",
            Self::Indeterminate => "indeterminate",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize_repr, Deserialize_repr)]
#[repr(i8)]
pub enum ChQuantLedgerEventKind {
    Submitted = 1,
    SubmissionResult = 2,
    ExitSubmitted = 3,
    ExitSubmissionResult = 4,
    Reconciled = 5,
    OperatorResolved = 6,
    Unresolvable = 7,
    SettlementRedeemConfirmed = 8,
    Opened = 9,
}

impl ChQuantLedgerEventKind {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Submitted => "submitted",
            Self::SubmissionResult => "submission_result",
            Self::ExitSubmitted => "exit_submitted",
            Self::ExitSubmissionResult => "exit_submission_result",
            Self::Reconciled => "reconciled",
            Self::OperatorResolved => "operator_resolved",
            Self::Unresolvable => "unresolvable",
            Self::SettlementRedeemConfirmed => "settlement_redeem_confirmed",
            Self::Opened => "opened",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize_repr, Deserialize_repr)]
#[repr(i8)]
pub enum ChExecutionSide {
    Buy = 1,
    Sell = 2,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize_repr, Deserialize_repr)]
#[repr(i8)]
pub enum ChOutcomeSide {
    Yes = 1,
    No = 2,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize_repr, Deserialize_repr)]
#[repr(i8)]
pub enum ChCapitalAllocationState {
    Allocated = 1,
    Locked = 2,
    Spent = 3,
    Released = 4,
    Impaired = 5,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize_repr, Deserialize_repr)]
#[repr(i8)]
pub enum ChPositionLedgerState {
    Open = 1,
    Closing = 2,
    Closed = 3,
    Settled = 4,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize_repr, Deserialize_repr)]
#[repr(i8)]
pub enum ChRecommendationStatus {
    Published = 1,
    Revoked = 2,
    Expired = 3,
    IntentCreated = 4,
    Executed = 5,
    Attributed = 6,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize_repr, Deserialize_repr)]
#[repr(i8)]
pub enum ChRecommendationAttributionOutcome {
    FilledExited = 1,
    FilledSettled = 2,
    ExpiredUnfilled = 3,
    CancelledUnfilled = 4,
    FailedUnfilled = 5,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize_repr, Deserialize_repr)]
#[repr(i8)]
pub enum ChFactorValueState {
    Scored = 1,
    MissingInput = 2,
    NotApplicable = 3,
    Indeterminate = 4,
}

impl ChFactorValueState {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Scored => "scored",
            Self::MissingInput => "missing_input",
            Self::NotApplicable => "not_applicable",
            Self::Indeterminate => "indeterminate",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize_repr, Deserialize_repr)]
#[repr(i8)]
pub enum ChFactorDirection {
    Positive = 1,
    Neutral = 0,
    Negative = -1,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize_repr, Deserialize_repr)]
#[repr(i8)]
pub enum ChNormalizationSource {
    CrossSection = 1,
    PerMarket = 2,
    HistoricalQuantile = 3,
}

impl ChNormalizationSource {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::CrossSection => "cross_section",
            Self::PerMarket => "per_market",
            Self::HistoricalQuantile => "historical_quantile",
        }
    }

    /// Decode a persisted `normalization_source` wire label.
    #[must_use]
    pub fn from_wire(label: &str) -> Option<Self> {
        match label {
            "cross_section" => Some(Self::CrossSection),
            "per_market" => Some(Self::PerMarket),
            "historical_quantile" => Some(Self::HistoricalQuantile),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize_repr, Deserialize_repr)]
#[repr(i8)]
pub enum ChFeatureValueKind {
    Decimal = 0,
    Probability = 1,
    Bps = 2,
    Usd = 3,
    Count = 4,
    Bool = 5,
    Category = 6,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize_repr, Deserialize_repr)]
#[repr(i8)]
pub enum ChFeatureSourceKind {
    Book = 1,
    GammaMetadata = 2,
    ClickHouseFact = 3,
    Derived = 5,
}

impl ChFeatureSourceKind {
    #[must_use]
    pub const fn as_wire(self) -> &'static str {
        match self {
            Self::Book => "book",
            Self::GammaMetadata => "gamma_metadata",
            Self::ClickHouseFact => "clickhouse_fact",
            Self::Derived => "derived",
        }
    }

    /// Decode a persisted `source_kind` wire label.
    #[must_use]
    pub fn from_wire(label: &str) -> Option<Self> {
        match label {
            "book" => Some(Self::Book),
            "gamma_metadata" => Some(Self::GammaMetadata),
            "clickhouse_fact" => Some(Self::ClickHouseFact),
            "derived" => Some(Self::Derived),
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{
        ChExitSignalEvaluatorKind, ChExitSignalVerdict, ChFeatureSourceKind, ChFeatureValueKind,
        ChNormalizationSource, ChQuantLedgerEventKind,
    };

    #[test]
    fn exit_signal_enums_expose_wire_labels() {
        assert_eq!(
            ChExitSignalEvaluatorKind::Reinference.as_str(),
            "reinference"
        );
        assert_eq!(ChExitSignalVerdict::Holds.as_str(), "holds");
    }

    #[test]
    fn feature_kind_codes_are_stable() {
        assert_eq!(
            ChFeatureValueKind::from_i8(1),
            Some(ChFeatureValueKind::Probability)
        );
        assert_eq!(
            ChFeatureSourceKind::from_wire("clickhouse_fact"),
            Some(ChFeatureSourceKind::ClickHouseFact)
        );
    }

    #[test]
    fn normalization_source_wire_labels() {
        assert_eq!(
            ChNormalizationSource::CrossSection.as_str(),
            "cross_section"
        );
        assert_eq!(
            ChNormalizationSource::from_wire("historical_quantile"),
            Some(ChNormalizationSource::HistoricalQuantile)
        );
    }

    #[test]
    fn ledger_event_kind_wire_labels() {
        assert_eq!(
            ChQuantLedgerEventKind::ExitSubmitted.as_str(),
            "exit_submitted"
        );
    }
}
