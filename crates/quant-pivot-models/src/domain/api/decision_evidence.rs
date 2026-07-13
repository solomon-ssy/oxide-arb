//! Structured serving-evidence projections shared by report and recommendation diagnostics.

use std::collections::BTreeMap;

use chrono::{DateTime, Utc};
use serde::Serialize;

use crate::domain::DecisionBoundary;

/// Exact decision clock recovered from durable serving evidence.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct DecisionBoundaryEvidenceView {
    pub decision_at: DateTime<Utc>,
    pub knowledge_cutoff: DateTime<Utc>,
    pub per_source_cutoffs: BTreeMap<String, DateTime<Utc>>,
}

impl From<&DecisionBoundary> for DecisionBoundaryEvidenceView {
    fn from(boundary: &DecisionBoundary) -> Self {
        Self {
            decision_at: boundary.decision_at(),
            knowledge_cutoff: boundary.knowledge_cutoff(),
            per_source_cutoffs: boundary
                .per_source_cutoffs()
                .iter()
                .map(|(source, cutoff)| (source.as_str().to_owned(), *cutoff))
                .collect(),
        }
    }
}

/// One full `FeatureCell` audit row used by serving.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct FeatureCellEvidenceView {
    pub feature_name: String,
    pub state: String,
    pub raw_value: Option<String>,
    pub value_kind: String,
    pub source_kind: String,
    pub evidence_source_kind: Option<String>,
    pub evidence_reference: Option<String>,
    pub evidence_effective_at: Option<DateTime<Utc>>,
    pub evidence_available_at: Option<DateTime<Utc>>,
    pub reason: Option<String>,
    pub staleness_ms: Option<u64>,
    pub data_quality: String,
    pub audit_fingerprint: String,
}

/// One raw-to-encoded model-input audit row used by serving.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ModelInputEvidenceView {
    pub raw_input_name: String,
    pub raw_state: String,
    pub raw_value: Option<String>,
    pub encoded_column: String,
    /// IEEE-754 payload represented as text so JavaScript never loses bits.
    pub encoded_value_bits: Option<String>,
    pub input_contract_hash: String,
    pub transform_hash: String,
    pub training_input_hash: String,
    pub audit_fingerprint: String,
}

/// Actual model route and frozen transform identity used by one serving run.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ModelRouteEvidenceView {
    pub model_run_id: String,
    pub model_version_id: String,
    pub model_family: String,
    pub input_contract_hash: String,
    pub transform_hash: String,
    pub training_input_hash: String,
}
