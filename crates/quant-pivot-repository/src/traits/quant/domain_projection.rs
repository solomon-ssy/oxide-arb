use chrono::{DateTime, NaiveDate, Utc};
use quant_pivot_error::storage::StorageError;
use quant_pivot_models::{
    domain::{
        CryptoPriceProjectionInfo, CryptoPriceReport, DomainEventEnvelope, DomainSourceCheckpoint,
        WeatherDailyHighProjectionInfo, WeatherObservationReport,
    },
    types::{DomainEventId, DomainInstrumentKey, DomainSourceId, IcaoStation},
};
use uuid::Uuid;

/// Atomic typed projections + source cursor + durable domain-event outbox.
#[async_trait::async_trait]
pub trait DomainProjectionRepository: Send + Sync {
    async fn apply_crypto_report(
        &self,
        report: CryptoPriceReport,
        checkpoint: DomainSourceCheckpoint,
        gap_generation: u64,
        source_healthy: bool,
    ) -> Result<CryptoPriceProjectionInfo, StorageError>;

    async fn apply_weather_report(
        &self,
        report: WeatherObservationReport,
        timezone: String,
        local_date: NaiveDate,
        checkpoint: DomainSourceCheckpoint,
        gap_generation: u64,
        source_healthy: bool,
    ) -> Result<WeatherDailyHighProjectionInfo, StorageError>;

    async fn close_weather_day(
        &self,
        station: &IcaoStation,
        local_date: NaiveDate,
        closed_at: DateTime<Utc>,
    ) -> Result<Option<WeatherDailyHighProjectionInfo>, StorageError>;

    /// Persist an observed crypto-source continuity break. The incremented
    /// generation is returned for the subsequent recovered reports.
    async fn mark_crypto_source_gap(
        &self,
        source_id: &DomainSourceId,
        instrument_key: &DomainInstrumentKey,
        observed_at: DateTime<Utc>,
    ) -> Result<u64, StorageError>;

    /// Persist an `AviationWeather` continuity break for one bound local day.
    async fn mark_weather_source_gap(
        &self,
        station: &IcaoStation,
        local_date: NaiveDate,
        observed_at: DateTime<Utc>,
    ) -> Result<u64, StorageError>;

    async fn claim_pending_events(
        &self,
        worker_id: Uuid,
        now: DateTime<Utc>,
        lease_expires_at: DateTime<Utc>,
        limit: u64,
    ) -> Result<Vec<DomainEventEnvelope>, StorageError>;

    async fn mark_event_published(
        &self,
        event_id: &DomainEventId,
        worker_id: Uuid,
        published_at: DateTime<Utc>,
    ) -> Result<(), StorageError>;

    async fn mark_event_failed(
        &self,
        event_id: &DomainEventId,
        worker_id: Uuid,
        detail: String,
    ) -> Result<(), StorageError>;
}
