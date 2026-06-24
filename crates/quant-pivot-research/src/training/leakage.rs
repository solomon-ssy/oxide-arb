//! Future-leakage detection — the hard, money-critical dataset invariant.
//!
//! A training feature must never observe state newer than `as_of - source_delay`.
//! (Labels look strictly forward of `as_of` by design; their forward reads are
//! validated by labeler maturity, not here — only feature provenance is checked.)

use chrono::Duration;
use quant_pivot_error::{QuantResult, research::ResearchError};

use super::TrainingExample;

/// Assert that no example's feature provenance leaks future state.
///
/// Each [`TrainingExample::source_refs`] entry (feature evidence) must have been
/// observed at or before the PIT cutoff `as_of - source_delay`. The first
/// violation aborts the whole dataset with [`ResearchError::LeakageDetected`];
/// the artifact must never be persisted.
///
/// # Errors
///
/// Returns [`ResearchError::LeakageDetected`] on the first future-dated feature
/// evidence.
pub fn assert_no_future_leakage(
    examples: &[TrainingExample],
    source_delay_secs: u64,
) -> QuantResult<()> {
    let delay = Duration::seconds(i64::try_from(source_delay_secs).unwrap_or(i64::MAX));
    for example in examples {
        let cutoff = example.as_of - delay;
        for source in &example.source_refs {
            if source.observed_at > cutoff {
                return Err(ResearchError::LeakageDetected {
                    detail: format!(
                        "market {} token {} as_of {} source `{}` observed_at {} > cutoff {}",
                        example.market_id,
                        example.token_id,
                        example.as_of,
                        source.reference,
                        source.observed_at,
                        cutoff,
                    ),
                }
                .into());
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::features::{EvidenceSourceKind, EvidenceSourceRef, FeatureVector};
    use chrono::{Duration, TimeZone, Utc};
    use quant_pivot_models::{
        enums::quant::DataQualityStatus,
        types::{MarketId, SchemaVersion, TokenId, TrainingExampleId},
    };
    use std::collections::BTreeMap;

    fn example(source_offset_secs: i64) -> TrainingExample {
        let as_of = Utc.timestamp_opt(1_000_000, 0).single().expect("ts");
        let vector = FeatureVector {
            market_id: MarketId::new("m"),
            token_id: Some(TokenId::new("t")),
            as_of,
            schema_version: SchemaVersion::new(1),
            values: BTreeMap::new(),
            substitutions: Vec::new(),
            data_quality: DataQualityStatus::Fresh,
            staleness_ms: 0,
            source_refs: Vec::new(),
        };
        TrainingExample {
            example_id: TrainingExampleId::from_v7(),
            market_id: MarketId::new("m"),
            token_id: TokenId::new("t"),
            as_of,
            feature_vector: vector,
            factor_values: Vec::new(),
            labels: Vec::new(),
            source_refs: vec![EvidenceSourceRef {
                source_kind: EvidenceSourceKind::Derived,
                reference: "fixture".to_owned(),
                observed_at: as_of + Duration::seconds(source_offset_secs),
            }],
        }
    }

    #[test]
    fn leakage_accepts_past_evidence() {
        // Evidence observed 5s before the cutoff (as_of - 0 delay) is fine.
        let examples = vec![example(-5)];
        assert!(assert_no_future_leakage(&examples, 0).is_ok());
    }

    #[test]
    fn leakage_rejects_future_features() {
        // Evidence observed 5s after as_of with zero source delay → leakage.
        let examples = vec![example(5)];
        assert!(assert_no_future_leakage(&examples, 0).is_err());
    }
}
