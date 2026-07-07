//! Long-format `ClickHouse` fact projection for feature vectors.
//!
//! Each present numeric value becomes one [`QuantFeatureEventRow`]
//! (`quant_feature_event`). Missing values are **never** written — their absence
//! is the signal; the full typed audit (including `Missing` reasons and
//! substitutions) lives in the Postgres `quant_feature_vector` payload instead.
//!
//! The projection is a pure function of its inputs: the vector, the governed
//! schema (the authority for each value's `source_kind`), and the ingestion
//! clock supplied by the caller. It never reads the wall clock or guesses a
//! value's origin from its name.

use quant_pivot_models::clickhouse::{ChDecimal64, QuantFeatureEventRow};

use crate::features::{
    schema::FeatureSchema,
    value::{EvidenceSourceKind, FeatureVector},
};

/// Project a feature vector into long-format feature-event rows.
///
/// `schema` governs each value's `source_kind` (derived from the spec's
/// [`SourceRequirement`](crate::features::SourceRequirement)); `ingestion_time`
/// is the caller-supplied write timestamp in epoch milliseconds.
///
/// Returns no rows when the vector is not token-scoped (the fact is keyed by
/// token) or carries only missing values.
#[must_use]
pub fn feature_events(
    vector: &FeatureVector,
    schema: &FeatureSchema,
    ingestion_time: i64,
) -> Vec<QuantFeatureEventRow> {
    let Some(token_id) = vector.token_id.clone() else {
        return Vec::new();
    };
    let as_of_ms = vector.as_of.timestamp_millis();
    let schema_version = vector.generic_schema_version.get();

    vector
        .iter_values()
        .filter_map(|(name, value)| {
            let kind = value.kind()?;
            let decimal = value.to_fact_decimal()?;
            let source_kind = schema
                .by_name(name)
                .map_or(EvidenceSourceKind::Derived, |spec| {
                    spec.source_requirement.evidence_kind()
                });
            Some(QuantFeatureEventRow {
                event_time: as_of_ms,
                as_of: as_of_ms,
                market_id: vector.market_id.clone(),
                token_id: token_id.clone(),
                feature_schema_version: u32::try_from(schema_version).unwrap_or(0),
                feature_name: name.as_str().to_owned(),
                feature_value: ChDecimal64::from(decimal),
                value_kind: kind.into(),
                source_kind: source_kind.into(),
                staleness_ms: vector.staleness_ms,
                ingestion_time,
            })
        })
        .collect()
}
