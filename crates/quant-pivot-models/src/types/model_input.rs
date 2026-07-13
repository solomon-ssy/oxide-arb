//! Typed, ordered raw-input contract owned by a model specification.

use std::collections::BTreeSet;

use sea_orm::FromJsonQueryResult;
use serde::{Deserialize, Serialize};

use crate::{jsonb_active, types::TradePolicyArtifactId};

/// Whether a raw feature may be imputed by the fitted model-input transform.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ModelInputRequiredness {
    /// Only a genuinely observed cell is accepted; all other states reject the row.
    Required,
    /// Missing states are retained and handled by the fitted transform.
    Optional,
}

/// One ordered raw feature consumed by a model.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ModelInputSpec {
    /// Stable feature name from the governed feature catalog.
    pub feature_name: String,
    /// Model-level availability requirement.
    pub requiredness: ModelInputRequiredness,
}

impl ModelInputSpec {
    #[must_use]
    pub fn required(feature_name: impl Into<String>) -> Self {
        Self {
            feature_name: feature_name.into(),
            requiredness: ModelInputRequiredness::Required,
        }
    }

    #[must_use]
    pub fn optional(feature_name: impl Into<String>) -> Self {
        Self {
            feature_name: feature_name.into(),
            requiredness: ModelInputRequiredness::Optional,
        }
    }
}

/// Frozen ordered raw-input graph for one model specification.
///
/// Encoded/synthetic columns are intentionally absent: they are derived only by
/// the fitted transform and can never enter this source contract.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize, FromJsonQueryResult)]
#[serde(deny_unknown_fields)]
pub struct ModelInputContract {
    pub inputs: Vec<ModelInputSpec>,
}

jsonb_active!(ModelInputContract);

/// Frozen supervised-target and cross-validation policy owned by a model spec.
/// Training requests cannot override these fields.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, FromJsonQueryResult)]
#[serde(deny_unknown_fields)]
pub struct ModelTrainingContract {
    /// Governed label name materialized in the frozen dataset.
    pub target_label_name: String,
    /// Target horizon (`0` for horizon-independent labels).
    pub target_label_horizon_secs: u64,
    /// Rolling validation fold count. Every fold fits its own transform.
    pub validation_folds: u32,
    /// Required for triple-barrier and meta-label targets; absent for unrelated labels.
    pub trade_policy_artifact_id: Option<TradePolicyArtifactId>,
}

jsonb_active!(ModelTrainingContract);

impl ModelTrainingContract {
    /// Common settlement classifier contract used by non-training fixtures.
    #[must_use]
    pub fn settlement_default() -> Self {
        Self {
            target_label_name: "settlement_outcome".to_owned(),
            target_label_horizon_secs: 0,
            validation_folds: 3,
            trade_policy_artifact_id: None,
        }
    }

    pub fn validate(&self) -> Result<(), String> {
        let label = self.target_label_name.trim();
        if label.is_empty() || label != self.target_label_name || label.len() > 128 {
            return Err(
                "target_label_name must be 1..=128 bytes without surrounding whitespace".to_owned(),
            );
        }
        if !(2..=20).contains(&self.validation_folds) {
            return Err("validation_folds must be in 2..=20".to_owned());
        }
        let requires_policy = matches!(
            self.target_label_name.as_str(),
            "triple_barrier_touch" | "triple_barrier_return_bps" | "meta_label"
        );
        if requires_policy != self.trade_policy_artifact_id.is_some() {
            return Err(
                "trade_policy_artifact_id is required exactly for triple-barrier/meta labels"
                    .to_owned(),
            );
        }
        Ok(())
    }
}

impl ModelInputContract {
    /// Build the smallest valid contract for a model with one required input.
    #[must_use]
    pub fn single_required(feature_name: impl Into<String>) -> Self {
        Self {
            inputs: vec![ModelInputSpec::required(feature_name)],
        }
    }

    /// Validate stable-name, raw-column and ordering invariants.
    pub fn validate(&self) -> Result<(), String> {
        let mut names = BTreeSet::new();
        for input in &self.inputs {
            let name = input.feature_name.trim();
            if name.is_empty() {
                return Err("model input feature_name cannot be empty".to_owned());
            }
            if name != input.feature_name {
                return Err(format!(
                    "model input `{}` contains surrounding whitespace",
                    input.feature_name
                ));
            }
            if name.len() > 256 {
                return Err(format!("model input `{name}` exceeds 256 bytes"));
            }
            if name.contains(".__") {
                return Err(format!(
                    "model input `{name}` is an encoded/synthetic column, not a raw feature"
                ));
            }
            if !names.insert(name) {
                return Err(format!("duplicate model input `{name}`"));
            }
        }
        Ok(())
    }

    #[must_use]
    pub fn required_feature_names(&self) -> Vec<&str> {
        self.inputs
            .iter()
            .filter(|input| input.requiredness == ModelInputRequiredness::Required)
            .map(|input| input.feature_name.as_str())
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::{ModelInputContract, ModelInputSpec};

    #[test]
    fn rejects_duplicate_and_encoded_columns() {
        let duplicate = ModelInputContract {
            inputs: vec![
                ModelInputSpec::required("book.mid"),
                ModelInputSpec::optional("book.mid"),
            ],
        };
        assert!(duplicate.validate().is_err());

        let encoded = ModelInputContract {
            inputs: vec![ModelInputSpec::optional("book.mid.__missing")],
        };
        assert!(encoded.validate().is_err());
    }
}
