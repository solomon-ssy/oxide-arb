//! In-memory domain PIT engine over prefetched observations (offline plane).
//!
//! Mirrors [`crate::pit::MaterializedPitEngine`]: dataset builds and backtests
//! prefetch the full observation range once, then answer every window query
//! from memory with binary search — zero database round-trips inside the
//! sample loop, byte-identical to the `ClickHouse` source for the same bounds.

use std::collections::HashMap;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use quant_pivot_error::QuantResult;
use quant_pivot_models::{domain::DomainObservation, types::DomainInstrumentKey};

use crate::domain::DomainPitQueryEngine;

/// In-memory, instrument-keyed domain observation store.
#[derive(Debug, Default)]
pub struct MaterializedDomainPitEngine {
    /// Ascending `observed_at` per instrument (stable ingestion order within
    /// equal timestamps, as delivered by the prefetch query).
    by_instrument: HashMap<DomainInstrumentKey, Vec<DomainObservation>>,
}

impl MaterializedDomainPitEngine {
    /// Build from prefetched observations, normalizing each series to
    /// ascending `observed_at` order.
    #[must_use]
    pub fn new(mut by_instrument: HashMap<DomainInstrumentKey, Vec<DomainObservation>>) -> Self {
        for series in by_instrument.values_mut() {
            series.sort_by_key(|observation| observation.observed_at);
        }
        Self { by_instrument }
    }

    /// Whether the engine holds any observation for `instrument_key`.
    #[must_use]
    pub fn has_instrument(&self, instrument_key: &DomainInstrumentKey) -> bool {
        self.by_instrument
            .get(instrument_key)
            .is_some_and(|series| !series.is_empty())
    }
}

#[async_trait]
impl DomainPitQueryEngine for MaterializedDomainPitEngine {
    async fn observations_between(
        &self,
        instrument_key: &DomainInstrumentKey,
        from: DateTime<Utc>,
        to_exclusive: DateTime<Utc>,
    ) -> QuantResult<Vec<DomainObservation>> {
        let Some(series) = self.by_instrument.get(instrument_key) else {
            return Ok(Vec::new());
        };
        let start = series.partition_point(|observation| observation.observed_at < from);
        let end = series.partition_point(|observation| observation.observed_at < to_exclusive);
        Ok(series[start..end].to_vec())
    }
}

#[cfg(test)]
mod tests {
    use super::MaterializedDomainPitEngine;
    use crate::domain::DomainPitQueryEngine;
    use chrono::{TimeZone, Timelike, Utc};
    use quant_pivot_models::{
        domain::DomainObservation,
        enums::domain::{DomainFamily, DomainMetric, KlineInterval},
        types::{BinanceSymbol, DomainInstrumentKey, DomainSourceId},
    };
    use rust_decimal::Decimal;
    use std::collections::HashMap;

    fn key() -> DomainInstrumentKey {
        DomainInstrumentKey::binance_kline(
            &BinanceSymbol::parse("BTCUSDT").expect("symbol"),
            KlineInterval::OneMinute,
        )
    }

    fn observation(minute: u32) -> DomainObservation {
        let at = Utc.with_ymd_and_hms(2026, 7, 1, 12, minute, 0).unwrap();
        DomainObservation {
            family: DomainFamily::Crypto,
            source_id: DomainSourceId::binance(),
            instrument_key: key(),
            metric: DomainMetric::Close,
            value: Decimal::from(100_000 + minute),
            observed_at: at,
            publish_time: at,
        }
    }

    #[tokio::test]
    async fn window_is_half_open_and_ascending() {
        let engine = MaterializedDomainPitEngine::new(HashMap::from([(
            key(),
            vec![
                observation(3),
                observation(1),
                observation(2),
                observation(4),
            ],
        )]));
        let from = Utc.with_ymd_and_hms(2026, 7, 1, 12, 2, 0).unwrap();
        let to = Utc.with_ymd_and_hms(2026, 7, 1, 12, 4, 0).unwrap();
        let window = engine
            .observations_between(&key(), from, to)
            .await
            .expect("query");
        let minutes: Vec<i64> = window
            .iter()
            .map(|observation| i64::from(observation.observed_at.time().minute()))
            .collect();
        assert_eq!(minutes, vec![2, 3], "[from, to) half-open, ascending");
    }

    #[tokio::test]
    async fn unknown_instrument_is_empty() {
        let engine = MaterializedDomainPitEngine::new(HashMap::new());
        let window = engine
            .observations_between(&key(), Utc::now(), Utc::now())
            .await
            .expect("query");
        assert!(window.is_empty());
    }
}
