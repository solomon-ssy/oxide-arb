//! Canonical model-training provenance persisted on model versions.

use rust_decimal::Decimal;
use sea_orm::FromJsonQueryResult;
use serde::{Deserialize, Serialize};

use crate::{
    enums::model::ClassicalKind,
    runtime_config::{RankLossKind, TrainingOptimizerKind},
};

/// System-owned schema version for [`ModelTrainingObjective`].
pub const MODEL_TRAINING_OBJECTIVE_FORMAT_VERSION: u16 = 2;

/// Honest fit state for the governed Hold-vs-Exit estimator.
///
/// Payload preparation may freeze and validate the governed estimator without
/// claiming a same-data refit. Fitting requires leakage-safe typed out-of-fold
/// predictions and therefore remains a separate phase.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum GovernedSellFitStatus {
    OofPredictionsRequired,
}

/// Full governed learning-to-rank objective snapshot.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TrainingObjectiveSpec {
    pub rank_loss: RankLossKind,
    pub optimizer: TrainingOptimizerKind,
    pub lambda_tail: Decimal,
    pub tail_fraction: Decimal,
    pub lambda_turnover: Decimal,
    pub lambda_l2: Decimal,
    pub ndcg_k: u32,
    pub pseudo_top_n: u32,
}

impl Default for TrainingObjectiveSpec {
    fn default() -> Self {
        Self {
            rank_loss: RankLossKind::default(),
            optimizer: TrainingOptimizerKind::default(),
            lambda_tail: Decimal::new(5, 1),
            tail_fraction: Decimal::new(10, 2),
            lambda_turnover: Decimal::new(2, 1),
            lambda_l2: Decimal::new(1, 2),
            ndcg_k: 20,
            pseudo_top_n: 20,
        }
    }
}

/// Closed set of training strategies that can produce a model version.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "kind", deny_unknown_fields)]
pub enum ModelTrainingObjectiveDefinition {
    /// Governed cross-sectional learning-to-rank optimization.
    LearningToRank { spec: TrainingObjectiveSpec },
    /// Pointwise classical estimator, validated by rolling out-of-sample rank IC.
    ClassicalPointwise {
        model_kind: ClassicalKind,
        validation_metric: ClassicalValidationMetric,
    },
    /// Governed Sell payload preparation with no leakage-prone same-data refit.
    GovernedSellEstimator { fit_status: GovernedSellFitStatus },
    /// A governed model artifact authored outside the trainer pipeline.
    HandAuthored { rationale: String },
}

/// Fixed validation metric for the classical pointwise training path.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ClassicalValidationMetric {
    MeanRollingFoldRankIc,
}

/// Versioned, typed JSONB document containing exact training provenance.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, FromJsonQueryResult)]
#[serde(deny_unknown_fields)]
pub struct ModelTrainingObjective {
    pub format_version: u16,
    pub definition: ModelTrainingObjectiveDefinition,
}

impl ModelTrainingObjective {
    #[must_use]
    pub const fn learning_to_rank(spec: TrainingObjectiveSpec) -> Self {
        Self {
            format_version: MODEL_TRAINING_OBJECTIVE_FORMAT_VERSION,
            definition: ModelTrainingObjectiveDefinition::LearningToRank { spec },
        }
    }

    #[must_use]
    pub const fn classical(model_kind: ClassicalKind) -> Self {
        Self {
            format_version: MODEL_TRAINING_OBJECTIVE_FORMAT_VERSION,
            definition: ModelTrainingObjectiveDefinition::ClassicalPointwise {
                model_kind,
                validation_metric: ClassicalValidationMetric::MeanRollingFoldRankIc,
            },
        }
    }

    #[must_use]
    pub const fn governed_sell(fit_status: GovernedSellFitStatus) -> Self {
        Self {
            format_version: MODEL_TRAINING_OBJECTIVE_FORMAT_VERSION,
            definition: ModelTrainingObjectiveDefinition::GovernedSellEstimator { fit_status },
        }
    }

    #[must_use]
    pub fn hand_authored(rationale: impl Into<String>) -> Self {
        Self {
            format_version: MODEL_TRAINING_OBJECTIVE_FORMAT_VERSION,
            definition: ModelTrainingObjectiveDefinition::HandAuthored {
                rationale: rationale.into(),
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::{GovernedSellFitStatus, ModelTrainingObjective, TrainingObjectiveSpec};

    #[test]
    fn objective_tagged_rejects_drift() {
        let objective = ModelTrainingObjective::learning_to_rank(TrainingObjectiveSpec::default());
        let value = serde_json::to_value(objective).expect("serialize objective");
        assert_eq!(value["format_version"], json!(2));
        assert_eq!(value["definition"]["kind"], json!("learning_to_rank"));

        let mut unknown = value.clone();
        unknown["definition"]["spec"]["unknown"] = json!(true);
        assert!(serde_json::from_value::<ModelTrainingObjective>(unknown).is_err());

        let mut invalid_kind = value;
        invalid_kind["definition"]["kind"] = json!("future_algorithm");
        assert!(serde_json::from_value::<ModelTrainingObjective>(invalid_kind).is_err());

        let sell =
            ModelTrainingObjective::governed_sell(GovernedSellFitStatus::OofPredictionsRequired);
        let sell_value = serde_json::to_value(sell).expect("serialize sell objective");
        assert_eq!(sell_value["format_version"], json!(2));
        assert_eq!(
            sell_value["definition"]["kind"],
            json!("governed_sell_estimator")
        );
    }
}
