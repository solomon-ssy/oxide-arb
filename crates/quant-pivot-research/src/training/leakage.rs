//! Future-leakage detection — the hard, money-critical dataset invariant.
//!
//! A training feature must never observe state newer than the source cutoff
//! frozen in its [`DecisionBoundary`](quant_pivot_models::domain::data_plane::DecisionBoundary).
//! (Labels look strictly forward of `decision_at` by design; their forward reads are
//! validated by labeler maturity, not here — only feature provenance is checked.)
//!
//! Two surfaces over the same scan:
//!
//! - [`scan_future_leakage`] returns **structured** [`LeakageFindings`] (every
//!   violation, with provenance), which the quality gate consumes as a hard
//!   gate input without aborting.
//! - [`assert_no_future_leakage`] is the dataset-build / trainer hard guard: it
//!   runs the same scan and turns any violation into
//!   [`ResearchError::LeakageDetected`], so a leaking artifact is never persisted.

use chrono::{DateTime, Utc};
use quant_pivot_error::{QuantResult, research::ResearchError};
use quant_pivot_models::{
    domain::data_plane::DecisionSource,
    enums::feature::EvidenceSourceKind,
    types::{MarketId, TokenId},
};
use serde::{Deserialize, Serialize};

use super::TrainingExample;

/// One future-leakage violation: a feature evidence reference observed after the
/// frozen point-in-time cutoff for its source.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LeakageViolation {
    /// Market the leaking example describes.
    pub market_id: MarketId,
    /// Outcome token the leaking example describes.
    pub token_id: TokenId,
    /// Decision time the example was computed as of.
    pub decision_at: DateTime<Utc>,
    /// Point-in-time source cutoff the evidence violated.
    pub cutoff: DateTime<Utc>,
    /// The offending evidence reference.
    pub reference: String,
    /// When the offending evidence was observed (strictly after `cutoff`).
    pub observed_at: DateTime<Utc>,
}

/// Structured result of a future-leakage scan over a dataset's examples.
///
/// `Serialize` so the quality gate can fold it into the persisted
/// `QualityGateReport`. A clean dataset has zero violations.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct LeakageFindings {
    /// Number of evidence references inspected.
    pub scanned: u64,
    /// Every violation found (empty ⇒ point-in-time clean).
    pub violations: Vec<LeakageViolation>,
}

impl LeakageFindings {
    /// Whether the scan found no future-dated feature evidence.
    #[must_use]
    pub const fn is_clean(&self) -> bool {
        self.violations.is_empty()
    }

    /// Number of violations found.
    #[must_use]
    pub const fn violation_count(&self) -> usize {
        self.violations.len()
    }
}

/// Scan every example's feature provenance for future leakage, collecting all
/// violations (non-aborting).
///
/// Each [`TrainingExample::source_refs`] entry (feature evidence) must have been
/// observed at or before the PIT cutoff frozen for its source. Unlike
/// [`assert_no_future_leakage`], this records every violation rather than
/// aborting on the first, so the quality gate can report the full picture.
pub fn scan_future_leakage(examples: &[TrainingExample]) -> QuantResult<LeakageFindings> {
    let mut findings = LeakageFindings::default();
    for example in examples {
        let decision_at = example.decision_at();
        for source in &example.source_refs {
            findings.scanned =
                findings
                    .scanned
                    .checked_add(1)
                    .ok_or_else(|| ResearchError::LeakageDetected {
                        detail: "future-leakage evidence count overflow".to_owned(),
                    })?;
            let cutoff = evidence_decision_source(source.source_kind).map_or(decision_at, |kind| {
                example.decision_boundary.cutoff_for(kind)
            });
            if source.effective_at > cutoff {
                findings.violations.push(LeakageViolation {
                    market_id: example.market_id.clone(),
                    token_id: example.token_id.clone(),
                    decision_at,
                    cutoff,
                    reference: source.reference.clone(),
                    observed_at: source.effective_at,
                });
            }
        }
        // Publish-time label-horizon rescan: every label must
        // mature at/after `decision_at`. A matured_at in the past of the decision time
        // means the labeler wrote a non-forward horizon — fail closed.
        for label in &example.labels {
            findings.scanned =
                findings
                    .scanned
                    .checked_add(1)
                    .ok_or_else(|| ResearchError::LeakageDetected {
                        detail: "future-leakage label count overflow".to_owned(),
                    })?;
            if label.matured_at < decision_at {
                findings.violations.push(LeakageViolation {
                    market_id: example.market_id.clone(),
                    token_id: example.token_id.clone(),
                    decision_at,
                    cutoff: decision_at,
                    reference: format!(
                        "label:{}:horizon={}s:matured_at",
                        label.label_name.as_str(),
                        label.horizon_secs
                    ),
                    observed_at: label.matured_at,
                });
            }
        }
    }
    Ok(findings)
}

/// Assert that no example's feature provenance leaks future state.
///
/// Runs [`scan_future_leakage`] and turns any violation into
/// [`ResearchError::LeakageDetected`]; the dataset / trained artifact must never
/// be persisted when this fails.
///
/// # Errors
///
/// Returns [`ResearchError::LeakageDetected`] when any feature evidence is dated
/// after its point-in-time cutoff.
pub fn assert_no_future_leakage(examples: &[TrainingExample]) -> QuantResult<()> {
    let findings = scan_future_leakage(examples)?;
    if let Some(first) = findings.violations.first() {
        return Err(ResearchError::LeakageDetected {
            detail: format!(
                "market {} token {} decision_at {} source `{}` observed_at {} > cutoff {} ({} total violations)",
                first.market_id,
                first.token_id,
                first.decision_at,
                first.reference,
                first.observed_at,
                first.cutoff,
                findings.violations.len(),
            ),
        }
        .into());
    }
    Ok(())
}

const fn evidence_decision_source(source: EvidenceSourceKind) -> Option<DecisionSource> {
    match source {
        EvidenceSourceKind::Book => Some(DecisionSource::Book),
        EvidenceSourceKind::GammaMetadata => Some(DecisionSource::Catalog),
        EvidenceSourceKind::ClickHouseFact => Some(DecisionSource::Microstructure),
        EvidenceSourceKind::TradeTape => Some(DecisionSource::TradeTape),
        EvidenceSourceKind::DomainExternal => Some(DecisionSource::DomainCrypto),
        EvidenceSourceKind::Linkage => Some(DecisionSource::Linkage),
        EvidenceSourceKind::Derived => None,
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use chrono::{Duration, TimeZone, Utc};
    use quant_pivot_models::{
        domain::data_plane::DecisionClock,
        enums::{common::MarketCategory, quant::DataQualityStatus},
        types::{MarketId, SchemaVersion, TokenId, TrainingExampleId, TrainingSampleSource},
    };
    use rust_decimal::Decimal;

    use super::{
        super::{LabelName, TrainingLabel},
        *,
    };
    use crate::{
        features::{EvidenceSourceKind, EvidenceSourceRef, FeatureVector},
        training::fixtures,
    };

    fn example(source_offset_secs: i64) -> TrainingExample {
        let as_of = Utc.timestamp_opt(1_000_000, 0).single().expect("ts");
        let vector = FeatureVector {
            market_id: MarketId::new("m"),
            token_id: Some(TokenId::new("t")),
            decision_at: as_of,
            generic_schema_version: SchemaVersion::FIRST,
            generic: BTreeMap::new(),
            domain: None,
            data_quality: DataQualityStatus::Fresh,
        };
        TrainingExample {
            example_id: TrainingExampleId::from_v7(),
            market_id: MarketId::new("m"),
            token_id: TokenId::new("t"),
            selected_market: fixtures::selected_market(
                &MarketId::new("m"),
                &TokenId::new("t"),
                MarketCategory::Sports,
            ),
            decision_boundary: DecisionClock::new(0).boundary(as_of).expect("boundary"),
            sample_source: TrainingSampleSource::HistoricalPit,
            feature_vector: vector,
            factor_values: Vec::new(),
            labels: Vec::new(),
            source_refs: vec![EvidenceSourceRef {
                source_kind: EvidenceSourceKind::Derived,
                reference: "fixture".to_owned(),
                effective_at: as_of + Duration::seconds(source_offset_secs),
                available_at: Some(as_of),
            }],
            decision_capture: None,
            lot_context: None,
            position_state: None,
            book_fidelity: None,
        }
    }

    #[test]
    fn leakage_accepts_past_evidence() {
        // Evidence observed 5s before the cutoff (as_of - 0 delay) is fine.
        let examples = vec![example(-5)];
        assert!(assert_no_future_leakage(&examples).is_ok());
    }

    #[test]
    fn leakage_rejects_future_features() {
        // Evidence observed 5s after decision_at with zero knowledge lag is leakage.
        let examples = vec![example(5)];
        assert!(assert_no_future_leakage(&examples).is_err());
    }

    #[test]
    fn scan_collects_without_aborting() {
        let findings = scan_future_leakage(&[example(5), example(-5), example(10)]).expect("scan");
        assert_eq!(findings.scanned, 3);
        assert_eq!(findings.violation_count(), 2);
        assert!(!findings.is_clean());
        // Provenance is carried for the gate report.
        assert!(findings.violations[0].observed_at > findings.violations[0].cutoff);
    }

    #[test]
    fn scan_clean_no_violations() {
        let findings = scan_future_leakage(&[example(-5), example(-1)]).expect("scan");
        assert!(findings.is_clean());
        assert_eq!(findings.scanned, 2);
    }

    #[test]
    fn scan_rejects_label_before() {
        let mut ex = example(-5);
        ex.labels.push(TrainingLabel {
            label_name: LabelName::from_static("token_payout_ratio"),
            horizon_secs: 0,
            value: Decimal::ZERO,
            is_resolved: true,
            matured_at: ex.decision_at() - Duration::seconds(1),
        });
        let findings = scan_future_leakage(&[ex]).expect("scan");
        assert!(!findings.is_clean());
        assert!(
            findings
                .violations
                .iter()
                .any(|v| v.reference.contains("matured_at"))
        );
    }
}
