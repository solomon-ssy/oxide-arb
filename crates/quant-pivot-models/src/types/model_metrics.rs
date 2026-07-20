//! Canonical model-version metrics and artifact-lineage projections.

use rust_decimal::Decimal;
use sea_orm::FromJsonQueryResult;
use serde::{Deserialize, Serialize};

use crate::{
    enums::{model::ClassicalKind, quant::ModelSerializationFormat},
    types::{
        ContentHash,
        stable_name::{FactorName, ModelMetricName},
    },
};

/// System-owned schema version for [`ModelVersionMetrics`].
pub const MODEL_VERSION_METRICS_FORMAT_VERSION: u16 = 1;

/// Loss decomposition for one LTR evaluation partition.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ObjectiveComponentMetrics {
    pub rank_loss: Decimal,
    pub tail_penalty: Decimal,
    pub turnover_penalty: Decimal,
    pub l2_penalty: Decimal,
    pub total_loss: Decimal,
    pub group_count: u64,
    pub rank_loss_group_count: u64,
    pub pair_count: u64,
}

/// Ranking diagnostics that are deliberately outside the optimized loss.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RankingDiagnosticsMetrics {
    pub mean_rank_ic: Decimal,
    pub mean_ndcg_at_k: Decimal,
    pub ndcg_k: u32,
    pub group_count: u64,
}

/// Meaning of a held-out objective value.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum HeldOutMetricKind {
    NegativeTotalLearningToRankLoss,
    MeanRollingFoldRankIc,
}

/// Shared frozen validation result for a trained model version.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ModelValidationMetrics {
    pub held_out_objective: Decimal,
    pub held_out_components: Option<ObjectiveComponentMetrics>,
    pub held_out_diagnostics: Option<RankingDiagnosticsMetrics>,
    pub fold_objectives: Vec<Decimal>,
    pub fold_components: Vec<ObjectiveComponentMetrics>,
    pub sample_count: u64,
    pub dropped_singleton_groups: u64,
    pub dropped_singleton_rows: u64,
    pub coordinate_search_effective_trials: u32,
    pub held_out_metric: HeldOutMetricKind,
}

/// LTR in-sample fit summary.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LearningToRankInSampleMetrics {
    pub objective_value: Decimal,
    pub components: ObjectiveComponentMetrics,
    pub diagnostics: Option<RankingDiagnosticsMetrics>,
    pub summary: String,
}

/// Classical pointwise in-sample fit summary.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ClassicalInSampleMetrics {
    pub validation_objective: Decimal,
    pub train_samples: u64,
    pub feature_count: u32,
}

/// One estimator feature-importance value.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ModelFeatureImportance {
    pub feature: ModelMetricName,
    pub importance: Decimal,
}

/// Minimal, non-duplicative lineage copied from the canonical model artifact.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "kind")]
pub enum ModelArtifactTrainingLineage {
    FactorNative {
        training_dataset_hash: ContentHash,
        training_input_hash: ContentHash,
        input_contract_hash: ContentHash,
        input_transform_hash: ContentHash,
        factor_inputs: Vec<FactorName>,
    },
    FittedFeatureMatrix {
        model_kind: ClassicalKind,
        training_dataset_hash: ContentHash,
        training_input_hash: ContentHash,
        input_contract_hash: ContentHash,
        input_transform_hash: ContentHash,
        serialized_model_hash: ContentHash,
        serialization_format: ModelSerializationFormat,
    },
}

/// Closed set of model-version metric families.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "kind")]
pub enum ModelVersionMetricsDefinition {
    LearningToRank {
        in_sample: LearningToRankInSampleMetrics,
        validation: ModelValidationMetrics,
        artifact_lineage: ModelArtifactTrainingLineage,
    },
    ClassicalPointwise {
        model_kind: ClassicalKind,
        in_sample: ClassicalInSampleMetrics,
        validation: ModelValidationMetrics,
        feature_importances: Vec<ModelFeatureImportance>,
        artifact_lineage: ModelArtifactTrainingLineage,
    },
    /// No quantitative training result exists for a governed hand-authored artifact.
    NotMeasured { rationale: String },
}

/// Versioned, strongly typed JSONB document stored on `quant_model_version`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, FromJsonQueryResult)]
#[serde(deny_unknown_fields)]
pub struct ModelVersionMetrics {
    pub format_version: u16,
    pub definition: ModelVersionMetricsDefinition,
}

impl ModelVersionMetrics {
    #[must_use]
    pub const fn learning_to_rank(
        in_sample: LearningToRankInSampleMetrics,
        validation: ModelValidationMetrics,
        artifact_lineage: ModelArtifactTrainingLineage,
    ) -> Self {
        Self {
            format_version: MODEL_VERSION_METRICS_FORMAT_VERSION,
            definition: ModelVersionMetricsDefinition::LearningToRank {
                in_sample,
                validation,
                artifact_lineage,
            },
        }
    }

    #[must_use]
    pub const fn classical_pointwise(
        model_kind: ClassicalKind,
        in_sample: ClassicalInSampleMetrics,
        validation: ModelValidationMetrics,
        feature_importances: Vec<ModelFeatureImportance>,
        artifact_lineage: ModelArtifactTrainingLineage,
    ) -> Self {
        Self {
            format_version: MODEL_VERSION_METRICS_FORMAT_VERSION,
            definition: ModelVersionMetricsDefinition::ClassicalPointwise {
                model_kind,
                in_sample,
                validation,
                feature_importances,
                artifact_lineage,
            },
        }
    }

    #[must_use]
    pub fn not_measured(rationale: impl Into<String>) -> Self {
        Self {
            format_version: MODEL_VERSION_METRICS_FORMAT_VERSION,
            definition: ModelVersionMetricsDefinition::NotMeasured {
                rationale: rationale.into(),
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::ModelVersionMetrics;

    #[test]
    fn metrics_reject_unknown_family_and_fields() {
        let value = serde_json::to_value(ModelVersionMetrics::not_measured("fixture"))
            .expect("serialize metrics");
        assert_eq!(value["definition"]["kind"], json!("not_measured"));

        let mut unknown = value.clone();
        unknown["extra"] = json!(true);
        assert!(serde_json::from_value::<ModelVersionMetrics>(unknown).is_err());

        let mut invalid = value;
        invalid["definition"]["kind"] = json!("future_metrics");
        assert!(serde_json::from_value::<ModelVersionMetrics>(invalid).is_err());
    }
}
