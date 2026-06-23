//! Governed feature schema: version + stable feature-name set.
//!
//! The registry and builder catalog land in 3.2; 3.0 fixes the schema shell so
//! [`crate::hashing::ResearchHasher::feature_schema`] can mint a canonical
//! `feature_schema_hash` without API churn later.

use quant_pivot_models::types::SchemaVersion;
use serde::{Deserialize, Serialize};

use super::FeatureName;

/// A versioned, hashable feature schema (sorted feature names → canonical digest).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FeatureSchema {
    /// Monotonic schema version (`>= 1`).
    pub version: SchemaVersion,
    /// Feature names governed by this schema (order-independent for hashing).
    pub features: Vec<FeatureName>,
}
