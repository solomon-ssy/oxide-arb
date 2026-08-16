//! Typed, ordered raw-input contract owned by a model specification.

use std::collections::BTreeSet;

use schemars::JsonSchema;
use sea_orm::FromJsonQueryResult;
use serde::{Deserialize, Serialize};

use crate::{enums::model::ModelFamily, types::TradePolicyArtifactId};

/// Closed supervised-task taxonomy for production model specifications.
///
/// A Buy model forecasts the selected token's terminal redemption fraction.
/// Executable prices, fills, fees, exits, and capital costs belong to the
/// independently frozen Trade Policy evaluation and global portfolio layers;
/// they are never folded into this forecasting target. The sell-side scorer
/// owns the only other supported task.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum ModelTrainingTarget {
    /// Calibrated expected terminal payout in `[0, 1]` for a canonical token.
    OutcomePayout,
    /// Executable advantage, in bps, of exiting a held lot instead of holding.
    HoldVsExitAlpha,
}

impl ModelTrainingTarget {
    /// Stable governed label name materialized in an immutable Dataset.
    #[must_use]
    pub const fn label_name(self) -> &'static str {
        match self {
            Self::OutcomePayout => "token_payout_ratio",
            Self::HoldVsExitAlpha => "hold_vs_exit_alpha_bps",
        }
    }

    /// Exact governed label horizon for this target.
    #[must_use]
    pub const fn label_horizon_secs(self) -> u64 {
        0
    }

    fn validate_family(self, model_family: ModelFamily) -> Result<(), String> {
        match (self, model_family) {
            (
                Self::OutcomePayout,
                ModelFamily::WeightedFactor | ModelFamily::ClassicalLogisticRegression,
            )
            | (Self::HoldVsExitAlpha, ModelFamily::HoldVsExitWeighted) => Ok(()),
            (Self::OutcomePayout, ModelFamily::HoldVsExitWeighted) => {
                Err("hold-vs-exit model families cannot use a Buy target".to_owned())
            }
            (Self::HoldVsExitAlpha, _) => {
                Err("Buy model families cannot use the sell-side hold-vs-exit target".to_owned())
            }
        }
    }
}

/// Whether a raw feature may be imputed by the fitted model-input transform.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum ModelInputRequiredness {
    /// Only a genuinely observed cell is accepted; all other states reject the row.
    Required,
    /// Missing states are retained and handled by the fitted transform.
    Optional,
}

/// One ordered raw feature consumed by a model.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
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
#[derive(
    Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize, FromJsonQueryResult, JsonSchema,
)]
#[serde(deny_unknown_fields)]
pub struct ModelInputContract {
    pub inputs: Vec<ModelInputSpec>,
}

/// Frozen supervised-target and cross-validation policy owned by a model spec.
/// Training requests cannot override these fields.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, FromJsonQueryResult, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ModelTrainingContract {
    /// Closed task whose exact label name and horizon are derived, never typed
    /// as an arbitrary string by an operator.
    pub target: ModelTrainingTarget,
    /// Rolling validation fold count. Every fold fits its own transform.
    pub validation_folds: u32,
    /// Published policy used only for OOS executable evaluation and Route
    /// readiness. It does not generate or redefine the supervised target.
    pub evaluation_trade_policy_artifact_id: Option<TradePolicyArtifactId>,
}

impl ModelTrainingContract {
    /// Common Buy outcome-forecast contract used by fixtures and bootstrap.
    #[must_use]
    pub const fn outcome_default() -> Self {
        Self {
            target: ModelTrainingTarget::OutcomePayout,
            validation_folds: 3,
            evaluation_trade_policy_artifact_id: None,
        }
    }

    /// Common Sell hold-vs-exit contract used by fixtures and bootstrap.
    #[must_use]
    pub const fn hold_vs_exit_default() -> Self {
        Self {
            target: ModelTrainingTarget::HoldVsExitAlpha,
            validation_folds: 3,
            evaluation_trade_policy_artifact_id: None,
        }
    }

    pub fn validate(&self) -> Result<(), String> {
        if !(2..=20).contains(&self.validation_folds) {
            return Err("validation_folds must be in 2..=20".to_owned());
        }
        Ok(())
    }

    /// Validate both generic training bounds and the task/family boundary.
    pub fn validate_for(&self, model_family: ModelFamily) -> Result<(), String> {
        self.validate()?;
        self.target.validate_family(model_family)
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
    use serde_json::json;

    use super::{ModelInputContract, ModelInputSpec, ModelTrainingContract, ModelTrainingTarget};
    use crate::enums::model::ModelFamily;

    #[test]
    fn rejects_duplicate_encoded_columns() {
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

    #[test]
    fn target_family_matrix() {
        let contract = |target| ModelTrainingContract {
            target,
            validation_folds: 3,
            evaluation_trade_policy_artifact_id: None,
        };
        assert!(
            contract(ModelTrainingTarget::OutcomePayout)
                .validate_for(ModelFamily::WeightedFactor)
                .is_ok()
        );
        assert!(
            contract(ModelTrainingTarget::OutcomePayout)
                .validate_for(ModelFamily::ClassicalLogisticRegression)
                .is_ok()
        );
        assert!(
            contract(ModelTrainingTarget::HoldVsExitAlpha)
                .validate_for(ModelFamily::HoldVsExitWeighted)
                .is_ok()
        );
        assert!(
            contract(ModelTrainingTarget::HoldVsExitAlpha)
                .validate_for(ModelFamily::WeightedFactor)
                .is_err()
        );
    }

    #[test]
    fn freeform_target_rejected() {
        let legacy = json!({
            "target_label_name": "policy_net_return_bps",
            "target_label_horizon_secs": 0,
            "validation_folds": 3,
            "trade_policy_artifact_id": null
        });
        assert!(serde_json::from_value::<ModelTrainingContract>(legacy).is_err());

        let unknown = json!({
            "target": { "kind": "policy_net_return" },
            "validation_folds": 3,
            "evaluation_trade_policy_artifact_id": null
        });
        assert!(serde_json::from_value::<ModelTrainingContract>(unknown).is_err());
    }
}
