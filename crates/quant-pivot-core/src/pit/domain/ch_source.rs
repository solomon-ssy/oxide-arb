//! ClickHouse-backed domain point-in-time source (Phase 11.2.2).

use std::sync::Arc;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use quant_pivot_error::QuantResult;
use quant_pivot_models::{domain::DomainObservation, types::DomainInstrumentKey};
use quant_pivot_repository::traits::QuantFactReadRepository;
use quant_pivot_research::domain::DomainPitQueryEngine;

/// Resolves domain observations from `quant_domain_observation` with no look-ahead.
pub struct ChDomainPitSource {
    fact_read: Arc<dyn QuantFactReadRepository>,
}

impl ChDomainPitSource {
    /// Build a domain PIT source over the quant fact read port.
    #[must_use]
    pub fn new(fact_read: Arc<dyn QuantFactReadRepository>) -> Self {
        Self { fact_read }
    }
}

#[async_trait]
impl DomainPitQueryEngine for ChDomainPitSource {
    async fn observations_between(
        &self,
        instrument_key: &DomainInstrumentKey,
        from: DateTime<Utc>,
        to_exclusive: DateTime<Utc>,
    ) -> QuantResult<Vec<DomainObservation>> {
        let rows = self
            .fact_read
            .domain_observations_between(
                vec![instrument_key.clone()],
                from.timestamp_millis(),
                to_exclusive.timestamp_millis(),
            )
            .await?;
        Ok(rows
            .iter()
            .filter_map(DomainObservation::from_clickhouse_row)
            .collect())
    }
}
