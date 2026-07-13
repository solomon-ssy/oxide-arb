//! Long-format `ClickHouse` fact projection for factor outcomes.
//!
//! Each present factor value of an **eligible** market becomes one
//! [`QuantFactorEventRow`] (`quant_factor_event`), tagged with the owning
//! `model_run_id`. Rejected markets emit nothing (their absence is the signal).
//! Scored factors carry present raw/normalized values; structurally
//! **not-applicable** factors also emit a row (tagged `not_applicable`) so
//! neg-risk inapplicability is visible in analytics — missing-input and
//! indeterminate outcomes remain Postgres-authoritative only.

use quant_pivot_models::{
    clickhouse::{ChDecimal64, ChProbability, QuantFactorEventRow},
    enums::factor::FactorValueState,
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
        let decision_at_ms = outcome.decision_at.timestamp_millis();
        for scored in &outcome.factors {
            let value_state = scored.value.value_state();
            match value_state {
                FactorValueState::Scored => {
                    let (Some(raw), Some(normalized_score), Some(source)) = (
                        scored.value.raw_value,
                        scored.value.normalized_score(),
                        scored.value.normalization_source(),
                    ) else {
                        continue;
                    };
                    rows.push(QuantFactorEventRow {
                        event_time: decision_at_ms,
                        decision_at: decision_at_ms,
                        market_id: outcome.market_id.clone(),
                        factor_name: scored.value.name.as_str().to_owned(),
                        factor_family: scored.value.family.as_str().to_owned(),
                        value_state: value_state.into(),
                        raw_value: Some(ChDecimal64::from(raw)),
                        normalized_score: Some(ChProbability::from(normalized_score)),
                        normalization_source: Some(source.into()),
                        confidence: ChProbability::from(scored.value.confidence),
                        direction: scored.value.direction.into(),
                        model_run_id: model_run_id.clone(),
                        ingestion_time,
                    });
                }
                FactorValueState::NotApplicable => {
                    rows.push(QuantFactorEventRow {
                        event_time: decision_at_ms,
                        decision_at: decision_at_ms,
                        market_id: outcome.market_id.clone(),
                        factor_name: scored.value.name.as_str().to_owned(),
                        factor_family: scored.value.family.as_str().to_owned(),
                        value_state: value_state.into(),
                        raw_value: None,
                        normalized_score: None,
                        normalization_source: None,
                        confidence: ChProbability::from(scored.value.confidence),
                        direction: scored.value.direction.into(),
                        model_run_id: model_run_id.clone(),
                        ingestion_time,
                    });
                }
                FactorValueState::MissingInput | FactorValueState::Indeterminate => {}
            }
        }
    }
    rows
}
