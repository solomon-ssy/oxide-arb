//! `ClickHouse` row types for timeseries insert and query.

mod book_l2_ledger;
mod book_microstructure;
mod book_stream_session;
mod domain_event;
mod domain_observation;
mod exchange_history;
mod market_resolution;
mod projections;
mod quant_facts;
mod serde;
mod types;

pub use book_l2_ledger::{BookL2LedgerRow, BookLedgerReplayAnchor};
pub use book_microstructure::{
    BOOK_MICROSTRUCTURE_1S_BUCKET_MILLIS, BookMicrostructureRow, MidPriceBucketRow,
};
pub use book_stream_session::BookStreamSessionRow;
pub use domain_event::{
    CryptoPriceReportRow, DomainEventRow, EntryConditionEvaluationEventRow, WeatherForecastFactRow,
    WeatherObservationFactRow,
};
pub use domain_observation::DomainObservationRow;
pub use exchange_history::{
    ExchangeEventRow, ExchangeFeeChargeRow, ExchangeHistoryAcceptanceRow, ExchangeLogRawRow,
    ExchangeMatchRow, ExecutionParticipantFactRow, ExecutionParticipantRow, MarketExecutionRow,
};
pub use market_resolution::{MarketResolutionFactInput, MarketResolutionRow};
pub use quant_facts::{
    QuantCapitalAllocationEventRow, QuantExecutionEventRow, QuantExitSignalEvaluationEventRow,
    QuantFactorEventRow, QuantFeatureEventRow, QuantFeatureParityEventRow, QuantModelInputEventRow,
    QuantPositionEventRow, QuantReportRecommendationFactRow, QuantServingEvidenceCompletionRow,
    QuantSignalCandidateEventRow, ReportMarketFunnelCountRow, ReportMarketFunnelRow,
};
pub use types::{
    ChAssetAmount, ChBps, ChDecimal64, ChDigest, ChEpochDay, ChFactor, ChPayoutRatio, ChPrice,
    ChProbability, ChSchemaVersion, ChShares, ChUsd,
};
