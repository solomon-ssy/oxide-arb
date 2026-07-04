//! Long-format `ClickHouse` fact projection for factor outcomes.
//!
//! Each present factor value of an **eligible** market becomes one
//! [`QuantFactorEventRow`] (`quant_factor_event`), tagged with the owning
//! `model_run_id`. Rejected markets emit nothing (their absence is the signal),
//! and a missing factor (`raw_value = None`) emits no row — mirroring the
//! present-only feature-event projection.

use quant_pivot_models::{
    clickhouse::{ChDecimal64, ChProbability, QuantFactorEventRow},
    types::ModelRunId,
};

use crate::factors::value::MarketFactorOutcome;

/// Project eligible factor outcomes into long-format factor-event rows.
///
/// `model_run_id` is the owning online round (minted up front and threaded
/// through the factor plane); `ingestion_time` is the caller-supplied write
/// timestamp in epoch milliseconds.
#[must_use]
pub fn factor_events(
    outcomes: &[MarketFactorOutcome],
    model_run_id: &ModelRunId,
    ingestion_time: i64,
) -> Vec<QuantFactorEventRow> {
    let mut rows = Vec::new();
    for outcome in outcomes {
        if !outcome.eligibility.is_eligible() {
            continue;
        }
        let as_of_ms = outcome.as_of.timestamp_millis();
        for scored in &outcome.factors {
            // Present-only, scored-only: a factor that was missing-input or
            // indeterminate carries no normalized score, so it emits no analytics
            // row (its authoritative record — with the reason — is in Postgres).
            let (Some(raw), Some(normalized_score), Some(source)) = (
                scored.value.raw_value,
                scored.value.normalized_score(),
                scored.value.normalization_source(),
            ) else {
                continue;
            };
            rows.push(QuantFactorEventRow {
                event_time: as_of_ms,
                as_of: as_of_ms,
                market_id: outcome.market_id.clone(),
                factor_name: scored.value.name.as_str().to_owned(),
                factor_family: scored.value.family.as_str().to_owned(),
                raw_value: ChDecimal64::from(raw),
                normalized_score: ChProbability::from(normalized_score),
                normalization_source: source.into(),
                confidence: ChProbability::from(scored.value.confidence),
                direction: scored.value.direction.into(),
                model_run_id: model_run_id.clone(),
                ingestion_time,
            });
        }
    }
    rows
}
