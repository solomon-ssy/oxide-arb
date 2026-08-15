//! Data-plane runtime types (WS ingest, book apply, latency tracing).

pub mod data_quality;
pub mod decision_boundary;
pub mod domain_event;
pub mod domain_observation;
pub mod domain_source_expectation;
pub mod exchange_history;
pub mod latency;
pub mod pipeline;

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
    DomainObservation, DomainSourceCheckpoint, DomainSourceCursorCasOutcome,
    DomainSourceCursorInfo, UpsertDomainSourceCursor,
};
pub use domain_source_expectation::{
    AffectedMarketIds, AffectedProfileIds, DomainSourceExpectationDefinition,
    DomainSourceExpectationInfo, DomainSourceExpectationTransition, UpsertDomainSourceExpectation,
};
pub use exchange_history::{
    ColdStartSloStatus, ExchangeHistoryChunkInfo, ExchangeHistoryChunkStatus,
    ExchangeHistoryChunkStatusEnum, ExchangeHistoryChunkStatusIter,
    ExchangeHistoryChunkStatusParseError, ExchangeHistoryChunkStatusVariant,
    ExchangeHistoryChunkStatusVariantIter, ExchangeHistoryContinuityBasis,
    ExchangeHistoryContinuityBasisEnum, ExchangeHistoryContinuityBasisIter,
    ExchangeHistoryContinuityBasisParseError, ExchangeHistoryContinuityBasisVariant,
    ExchangeHistoryContinuityBasisVariantIter, ExchangeHistoryFrontier,
    ExchangeHistoryFrontierEnum, ExchangeHistoryFrontierIter, ExchangeHistoryFrontierParseError,
    ExchangeHistoryFrontierProgress, ExchangeHistoryFrontierVariant,
    ExchangeHistoryFrontierVariantIter, ExchangeHistoryPlanInfo,
    ExchangeHistoryQuarantineDisposition, ExchangeHistoryQuarantineInfo,
    ExchangeHistoryQuarantineReason, ExchangeHistoryQuarantineReasonEnum,
    ExchangeHistoryQuarantineReasonIter, ExchangeHistoryQuarantineReasonParseError,
    ExchangeHistoryQuarantineReasonVariant, ExchangeHistoryQuarantineReasonVariantIter,
    ExchangeHistoryQuarantineResolutionInfo, ExchangeHistoryStage, ExecutionParticipantPrint,
    ExecutionParticipantRole, NewExchangeHistoryChunk, NewExchangeHistoryPlan,
    NewExchangeHistoryQuarantine, NewExchangeHistoryQuarantineResolution,
    ResolveAcceptedHistoryRange,
};
pub use latency::LatencyTrace;
pub use pipeline::{
    BookSideData, BookSnapshotCmd, IngressTrace, PipelineEvent, PriceDeltaCmd, PriceLevelDelta,
    StreamSessionEndReason,
};
