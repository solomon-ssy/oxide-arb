//! Data-plane runtime types (WS ingest, book apply, latency tracing).

pub mod data_quality;
pub mod decision_boundary;
pub mod domain_event;
pub mod domain_observation;
pub mod domain_source_expectation;
pub mod latency;
pub mod pipeline;
pub mod trade_tape;

pub use data_quality::{DataQualityInput, DataQualityReport, DataQualitySnapshot};
pub use decision_boundary::{DecisionBoundary, DecisionClock, DecisionSource};
pub use domain_event::{
    CryptoPriceReport, CryptoPriceTransition, DomainEventEnvelope, DomainEventPayload,
    DomainEventType, SettlementRedeemConfirmed, WeatherDailyTemperatureExtremeChange,
    WeatherForecastPoint, WeatherObservationDayClosed, WeatherObservationFact,
    WeatherObservationReport, WeatherObservationReportKind,
};
pub use domain_observation::{
    DomainCursorStatus, DomainCursorStatusEnum, DomainCursorStatusIter,
    DomainCursorStatusParseError, DomainCursorStatusVariant, DomainCursorStatusVariantIter,
    DomainObservation, DomainSourceCheckpoint, DomainSourceCursorInfo, UpsertDomainSourceCursor,
};
pub use domain_source_expectation::{
    AffectedMarketIds, AffectedProfileIds, DomainSourceExpectationDefinition,
    DomainSourceExpectationInfo, DomainSourceExpectationTransition, UpsertDomainSourceExpectation,
};
pub use latency::LatencyTrace;
pub use pipeline::{
    BookSideData, BookSnapshotCmd, IngressTrace, PipelineEvent, PriceDeltaCmd, PriceLevelDelta,
    StreamSessionEndReason,
};
pub use trade_tape::{
    TradeParticipantRole, TradeTapeBlockCursorInfo, TradeTapeBlockCursorStatus,
    TradeTapeBlockCursorStatusEnum, TradeTapeBlockCursorStatusIter,
    TradeTapeBlockCursorStatusParseError, TradeTapeBlockCursorStatusVariant,
    TradeTapeBlockCursorStatusVariantIter, TradeTapePrint, TradeTapeSourceKind,
    TradeTapeSourceKindEnum, TradeTapeSourceKindIter, TradeTapeSourceKindParseError,
    TradeTapeSourceKindVariant, TradeTapeSourceKindVariantIter, UpsertTradeTapeBlockCursor,
    ch_trade_side, ch_trade_side_to_domain, trade_tape_coverage,
};
