//! `ClickHouse` row types for timeseries insert and query.

mod book_l2_replay;
mod book_microstructure;
mod book_snapshot;
mod book_stream_session;
mod domain_event;
mod domain_observation;
mod market_resolution;
mod projections;
mod quant_facts;
mod trade_tape;
mod types;

pub use book_l2_replay::BookL2EventRow;
pub use book_microstructure::{BookMicrostructureRow, MidPriceBucketRow};
pub use book_snapshot::BookL2CheckpointRow;
pub use book_stream_session::BookStreamSessionRow;
pub use domain_event::{
    CryptoPriceReportRow, DomainEventRow, EntryConditionEvaluationEventRow, WeatherForecastFactRow,
    WeatherObservationFactRow,
};
pub use domain_observation::DomainObservationRow;
pub use market_resolution::MarketResolutionRow;
pub use quant_facts::{
    QuantCapitalAllocationEventRow, QuantExecutionEventRow, QuantExitSignalEvaluationEventRow,
    QuantFactorEventRow, QuantFeatureEventRow, QuantFeatureParityEventRow, QuantModelInputEventRow,
    QuantPositionEventRow, QuantRecommendationAttributionEventRow,
    QuantReportRecommendationFactRow, QuantServingEvidenceCompletionRow,
    QuantSignalCandidateEventRow, ReportMarketFunnelCountRow, ReportMarketFunnelRow,
};
pub use trade_tape::TradeTapeRow;
pub use types::{
    ChBps, ChDecimal64, ChEpochDay, ChFactor, ChPrice, ChProbability, ChSchemaVersion, ChShares,
    ChUsd,
};
