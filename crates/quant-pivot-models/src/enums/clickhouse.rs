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
pub enum ChCanonicalBookEventType {
    Snapshot = 1,
    Delta = 2,
    TickSizeChange = 3,
    Gap = 4,
    LastTrade = 5,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize_repr, Deserialize_repr)]
#[repr(i8)]
pub enum ChLedgerTradeSide {
    Unknown = 0,
    Buy = 1,
    Sell = 2,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize_repr, Deserialize_repr)]
#[repr(i8)]
pub enum ChStreamSessionState {
    Open = 1,
    Sealed = 2,
    Invalidated = 3,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize_repr, Deserialize_repr)]
#[repr(i8)]
pub enum ChStreamSessionEndReason {
    None = 0,
    Normal = 1,
    Resubscribe = 2,
    Overflow = 3,
    Disconnect = 4,
    Shutdown = 5,
    CrashRecovery = 6,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize_repr, Deserialize_repr)]
#[repr(i8)]
pub enum ChFactSource {
    WsSnapshot = 1,
    WsDelta = 2,
    WsBbo = 3,
    WsTickSize = 4,
    WsLastTrade = 5,
    ResolutionReconciliation = 6,
    QuantPipeline = 7,
    Execution = 8,
    WsShardStatus = 9,
    DataApiTrade = 10,
    ClobTrade = 11,
    WsTrade = 12,
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
pub enum ChTradeParticipantRole {
    Maker = 1,
    Taker = 2,
    Unknown = 3,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize_repr, Deserialize_repr)]
#[repr(i8)]
pub enum ChTradeTapeSource {
    MarketWs = 1,
    OnChainOrderFilled = 2,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize_repr, Deserialize_repr)]
#[repr(i8)]
pub enum ChTradeReconciliationStatus {
    Pending = 1,
    Matched = 2,
    Unavailable = 3,
    Ambiguous = 4,
    OnChainOnly = 5,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize_repr, Deserialize_repr)]
#[repr(i8)]
pub enum ChTradeSide {
    Buy = 1,
    Sell = 2,
    Unknown = 3,
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
    FrozenReferenceQuantile = 3,
}

impl ChNormalizationSource {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::CrossSection => "cross_section",
            Self::PerMarket => "per_market",
            Self::FrozenReferenceQuantile => "frozen_reference_quantile",
        }
    }

    /// Decode a persisted `normalization_source` wire label.
    #[must_use]
    pub fn from_wire(label: &str) -> Option<Self> {
        match label {
            "cross_section" => Some(Self::CrossSection),
            "per_market" => Some(Self::PerMarket),
            "frozen_reference_quantile" => Some(Self::FrozenReferenceQuantile),
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

impl ChFeatureValueKind {
    #[must_use]
    pub const fn as_wire(self) -> &'static str {
        match self {
            Self::Decimal => "decimal",
            Self::Probability => "probability",
            Self::Bps => "bps",
            Self::Usd => "usd",
            Self::Count => "count",
            Self::Bool => "bool",
            Self::Category => "category",
        }
    }
}

/// Semantic state of one persisted feature cell.
///
/// The numeric codes are an append-only `ClickHouse` wire contract. A feature
/// event is emitted for every state; absence of a row is never used to encode
/// missingness.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize_repr, Deserialize_repr)]
#[repr(i8)]
pub enum ChFeatureCellState {
    Observed = 1,
    Substituted = 2,
    Missing = 3,
    NotApplicable = 4,
}

impl ChFeatureCellState {
    #[must_use]
    pub const fn as_wire(self) -> &'static str {
        match self {
            Self::Observed => "observed",
            Self::Substituted => "substituted",
            Self::Missing => "missing",
            Self::NotApplicable => "not_applicable",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize_repr, Deserialize_repr)]
#[repr(i8)]
pub enum ChFeatureSourceKind {
    Book = 1,
    GammaMetadata = 2,
    ClickHouseFact = 3,
    TradeTape = 4,
    Derived = 5,
    DomainExternal = 6,
    Linkage = 7,
}

impl ChFeatureSourceKind {
    #[must_use]
    pub const fn as_wire(self) -> &'static str {
        match self {
            Self::Book => "book",
            Self::GammaMetadata => "gamma_metadata",
            Self::ClickHouseFact => "clickhouse_fact",
            Self::TradeTape => "trade_tape",
            Self::Derived => "derived",
            Self::DomainExternal => "domain_external",
            Self::Linkage => "linkage",
        }
    }

    /// Decode a persisted `source_kind` wire label.
    #[must_use]
    pub fn from_wire(label: &str) -> Option<Self> {
        match label {
            "book" => Some(Self::Book),
            "gamma_metadata" => Some(Self::GammaMetadata),
            "clickhouse_fact" => Some(Self::ClickHouseFact),
            "trade_tape" => Some(Self::TradeTape),
            "derived" => Some(Self::Derived),
            "domain_external" => Some(Self::DomainExternal),
            "linkage" => Some(Self::Linkage),
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{
        ChExitSignalEvaluatorKind, ChExitSignalVerdict, ChFeatureCellState, ChFeatureSourceKind,
        ChFeatureValueKind, ChNormalizationSource, ChQuantLedgerEventKind,
    };

    #[test]
    fn exit_signal_enums_labels() {
        assert_eq!(
            ChExitSignalEvaluatorKind::Reinference.as_str(),
            "reinference"
        );
        assert_eq!(ChExitSignalVerdict::Holds.as_str(), "holds");
    }

    #[test]
    fn feature_kind_codes_stable() {
        assert_eq!(ChFeatureCellState::Missing as i8, 3);
        assert_eq!(
            ChFeatureValueKind::from_i8(1),
            Some(ChFeatureValueKind::Probability)
        );
        assert_eq!(
            ChFeatureSourceKind::from_wire("clickhouse_fact"),
            Some(ChFeatureSourceKind::ClickHouseFact)
        );
        assert_eq!(ChFeatureSourceKind::Linkage as i8, 7);
        assert_eq!(
            ChFeatureSourceKind::from_wire("linkage"),
            Some(ChFeatureSourceKind::Linkage)
        );
    }

    #[test]
    fn normalization_source_wire_labels() {
        assert_eq!(
            ChNormalizationSource::CrossSection.as_str(),
            "cross_section"
        );
        assert_eq!(
            ChNormalizationSource::from_wire("frozen_reference_quantile"),
            Some(ChNormalizationSource::FrozenReferenceQuantile)
        );
    }

    #[test]
    fn ledger_event_kind_labels() {
        assert_eq!(
            ChQuantLedgerEventKind::ExitSubmitted.as_str(),
            "exit_submitted"
        );
    }
}
