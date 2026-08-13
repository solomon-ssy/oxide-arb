//! Immutable, content-addressed model-specification contracts.

use std::collections::BTreeSet;

use quant_pivot_error::hashing::CanonicalDigestError;
use schemars::JsonSchema;
use sea_orm::FromJsonQueryResult;
use serde::{Deserialize, Serialize};

use crate::{
    enums::model::ModelFamily,
    hashing::CanonicalDigest,
    types::{ContentHash, ModelInputContract, ModelTrainingContract, SchemaVersion},
};

/// Breaking semantic format of the canonical model-spec definition.
pub const MODEL_SPEC_DEFINITION_FORMAT_VERSION: u32 = 1;

/// Human-authored research thesis that cannot be inferred from executable fields.
///
/// This is intentionally a closed document rather than a free-form metadata map.
/// It is read and written atomically with the immutable model spec, never queried
/// by individual JSON keys, and therefore uses typed JSONB through
/// [`FromJsonQueryResult`]. Executable inputs, targets, horizons, and lifecycle
/// state do not belong here.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, FromJsonQueryResult, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ModelSpecThesis {
    /// Concise catalog summary for operators.
    pub summary: String,
    /// Falsifiable relationship the research line is expected to demonstrate.
    pub hypothesis: String,
    /// Known boundaries that must be considered when evaluating a trained version.
    pub limitations: Vec<String>,
}

impl ModelSpecThesis {
    /// Validate bounded, non-blank, duplicate-free documentation.
    pub fn validate(&self) -> Result<(), String> {
        validate_text("summary", &self.summary, 512)?;
        validate_text("hypothesis", &self.hypothesis, 2_048)?;
        if self.limitations.is_empty() || self.limitations.len() > 16 {
            return Err("limitations must contain 1..=16 entries".to_owned());
        }
        let mut unique = BTreeSet::new();
        for limitation in &self.limitations {
            validate_text("limitation", limitation, 1_024)?;
            if !unique.insert(limitation.as_str()) {
                return Err(format!("duplicate limitation `{limitation}`"));
            }
        }
        Ok(())
    }
}

fn validate_text(field: &str, value: &str, max_len: usize) -> Result<(), String> {
    if value.is_empty() || value.trim() != value || value.len() > max_len {
        return Err(format!(
            "{field} must be 1..={max_len} bytes without surrounding whitespace"
        ));
    }
    Ok(())
}

/// Canonical immutable definition whose digest follows a spec into every
/// dataset manifest and trained model artifact.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ModelSpecDefinition<'a> {
    pub name: &'a str,
    pub model_family: ModelFamily,
    pub prediction_horizon_secs: i64,
    pub feature_schema_version: SchemaVersion,
    pub label_schema_version: SchemaVersion,
    pub thesis: &'a ModelSpecThesis,
    pub input_contract: &'a ModelInputContract,
    pub training_contract: &'a ModelTrainingContract,
}

impl ModelSpecDefinition<'_> {
    /// Validate every semantic member before persistence or hash verification.
    pub fn validate(&self) -> Result<(), String> {
        validate_text("name", self.name, 128)?;
        if self.prediction_horizon_secs <= 0 {
            return Err("prediction_horizon_secs must be positive".to_owned());
        }
        self.thesis.validate()?;
        self.input_contract.validate()?;
        if self.input_contract.inputs.is_empty() {
            return Err("input_contract must contain at least one raw feature".to_owned());
        }
        self.training_contract.validate_for(self.model_family)
    }

    /// Domain-separated content hash of the complete semantic definition.
    pub fn content_hash(&self) -> Result<ContentHash, CanonicalDigestError> {
        CanonicalDigest::content_hash_typed(
            "quant-pivot/model-spec-definition",
            MODEL_SPEC_DEFINITION_FORMAT_VERSION,
            self,
        )
    }
}

#[cfg(test)]
mod tests {
    use super::{ModelSpecDefinition, ModelSpecThesis};
    use crate::{
        enums::model::ModelFamily,
        types::{ModelInputContract, ModelTrainingContract, SchemaVersion},
    };

    impl ModelSpecThesis {
        fn test_fixture() -> Self {
            Self {
                summary: "Buy-side weighted-factor baseline".to_owned(),
                hypothesis: "Governed factors forecast terminal payout probability".to_owned(),
                limitations: vec![
                    "Evaluate only on Polymarket markets covered by the frozen profile".to_owned(),
                ],
            }
        }
    }

    #[test]
    fn thesis_rejects_unknown_invalid() {
        let unknown = serde_json::json!({
            "summary": "summary",
            "hypothesis": "hypothesis",
            "limitations": ["limitation"],
            "notes": "opaque escape hatch"
        });
        assert!(serde_json::from_value::<ModelSpecThesis>(unknown).is_err());

        let mut invalid = ModelSpecThesis::test_fixture();
        invalid.limitations.push(invalid.limitations[0].clone());
        assert!(invalid.validate().is_err());
    }

    #[test]
    fn semantic_changes_change_hash() {
        let thesis = ModelSpecThesis::test_fixture();
        let input = ModelInputContract::single_required("book.mid");
        let training = ModelTrainingContract::outcome_default();
        let definition = ModelSpecDefinition {
            name: "buy-baseline",
            model_family: ModelFamily::WeightedFactor,
            prediction_horizon_secs: 86_400,
            feature_schema_version: SchemaVersion::FIRST,
            label_schema_version: SchemaVersion::FIRST,
            thesis: &thesis,
            input_contract: &input,
            training_contract: &training,
        };
        definition.validate().expect("valid definition");
        let left = definition.content_hash().expect("definition hash");

        let changed = ModelSpecDefinition {
            prediction_horizon_secs: 3_600,
            ..definition
        };
        let right = changed.content_hash().expect("changed hash");
        assert_ne!(left, right);
    }
}
