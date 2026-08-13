//! Immutable, content-addressed candidate serving manifest.

use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use sea_orm::{DeriveIntoActiveModel, DerivePartialModel, FromJsonQueryResult};
use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::{
    entities::quant_model_candidate_manifest,
    enums::{common::MarketCategory, model::ModelFamily},
    hashing::CanonicalDigest,
    runtime_config::{BuyModelRoute, PortfolioScenarioModelArtifactBinding},
    types::{
        BacktestPathSetId, CalibrationArtifactId, ContentHash, FeedbackCycleId,
        ModelCandidateManifestId, ModelSpecId, ModelVersionId, PortfolioScenarioModelArtifactId,
        ResearchProfileRef, TrainingDatasetId,
        model_serving::{ModelServingBindings, ModelServingEstimatorBinding},
    },
};

use super::RepresentedRouteSet;

const MANIFEST_FORMAT_VERSION: u32 = 4;
const MANIFEST_HASH_DOMAIN: &str = "quant-pivot/model-candidate-manifest";
const EXPLANATION_FORMAT_VERSION: u32 = 3;
const EXPLANATION_HASH_DOMAIN: &str = "quant-pivot/candidate-explanation-validation";
const PROMOTION_GATE_FORMAT_VERSION: u32 = 2;
const PROMOTION_GATE_HASH_DOMAIN: &str = "quant-pivot/candidate-readiness-gate";

/// Explainability method validated before a candidate can enter governance.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CandidateExplanationMethod {
    WeightedClosedForm,
    ExactTreeShap,
}

impl CandidateExplanationMethod {
    #[must_use]
    pub const fn wire_name(self) -> &'static str {
        match self {
            Self::WeightedClosedForm => "weighted_closed_form",
            Self::ExactTreeShap => "exact_tree_shap",
        }
    }
}

/// Method-specific quantity and efficiency evidence.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum CandidateExplanationVerification {
    WeightedClosedForm {
        verified_term_count: u64,
    },
    ExactTreeShap {
        verified_case_count: u64,
        max_efficiency_residual: Decimal,
        max_prediction_residual: Decimal,
    },
}

/// Immutable explainability-contract proof bound into the candidate manifest.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CandidateExplanationValidation {
    pub format_version: u32,
    pub method: CandidateExplanationMethod,
    pub input_contract_hash: ContentHash,
    pub verification: CandidateExplanationVerification,
    pub report_hash: ContentHash,
}

#[derive(Serialize)]
struct ExplanationValidationPreimage {
    format_version: u32,
    method: CandidateExplanationMethod,
    input_contract_hash: ContentHash,
    verification: CandidateExplanationVerification,
}

impl CandidateExplanationValidation {
    pub fn weighted(
        input_contract_hash: ContentHash,
        verified_term_count: u64,
    ) -> Result<Self, ModelCandidateManifestError> {
        Self::try_new(
            CandidateExplanationMethod::WeightedClosedForm,
            input_contract_hash,
            CandidateExplanationVerification::WeightedClosedForm {
                verified_term_count,
            },
        )
    }

    pub fn tree_shap(
        input_contract_hash: ContentHash,
        verified_case_count: u64,
        max_efficiency_residual: Decimal,
        max_prediction_residual: Decimal,
    ) -> Result<Self, ModelCandidateManifestError> {
        Self::try_new(
            CandidateExplanationMethod::ExactTreeShap,
            input_contract_hash,
            CandidateExplanationVerification::ExactTreeShap {
                verified_case_count,
                max_efficiency_residual,
                max_prediction_residual,
            },
        )
    }

    fn try_new(
        method: CandidateExplanationMethod,
        input_contract_hash: ContentHash,
        verification: CandidateExplanationVerification,
    ) -> Result<Self, ModelCandidateManifestError> {
        let preimage = ExplanationValidationPreimage {
            format_version: EXPLANATION_FORMAT_VERSION,
            method,
            input_contract_hash,
            verification,
        };
        let report_hash = CanonicalDigest::content_hash_typed(
            EXPLANATION_HASH_DOMAIN,
            EXPLANATION_FORMAT_VERSION,
            &preimage,
        )
        .map_err(|error| ModelCandidateManifestError::Hash(error.to_string()))?;
        let report = Self {
            format_version: preimage.format_version,
            method: preimage.method,
            input_contract_hash: preimage.input_contract_hash,
            verification: preimage.verification,
            report_hash,
        };
        report.validate()?;
        Ok(report)
    }

    pub fn validate(&self) -> Result<(), ModelCandidateManifestError> {
        let residual_limit = Decimal::from_parts(1, 0, 0, false, 12);
        let expected = CanonicalDigest::content_hash_typed(
            EXPLANATION_HASH_DOMAIN,
            EXPLANATION_FORMAT_VERSION,
            &ExplanationValidationPreimage {
                format_version: self.format_version,
                method: self.method,
                input_contract_hash: self.input_contract_hash,
                verification: self.verification.clone(),
            },
        )
        .map_err(|error| ModelCandidateManifestError::Hash(error.to_string()))?;
        let verification_valid = match (&self.method, &self.verification) {
            (
                CandidateExplanationMethod::WeightedClosedForm,
                CandidateExplanationVerification::WeightedClosedForm {
                    verified_term_count,
                },
            ) => *verified_term_count > 0,
            (
                CandidateExplanationMethod::ExactTreeShap,
                CandidateExplanationVerification::ExactTreeShap {
                    verified_case_count,
                    max_efficiency_residual,
                    max_prediction_residual,
                },
            ) => {
                *verified_case_count > 0
                    && !max_efficiency_residual.is_sign_negative()
                    && *max_efficiency_residual <= residual_limit
                    && !max_prediction_residual.is_sign_negative()
                    && *max_prediction_residual <= Decimal::from_parts(1, 0, 0, false, 10)
            }
            _ => false,
        };
        if self.format_version != EXPLANATION_FORMAT_VERSION
            || self.report_hash != expected
            || !verification_valid
        {
            return Err(ModelCandidateManifestError::Invalid(
                "candidate explanation validation is incomplete or violates efficiency".to_owned(),
            ));
        }
        Ok(())
    }
}

impl TryFrom<&ModelServingBindings> for CandidateExplanationValidation {
    type Error = ModelCandidateManifestError;

    fn try_from(bindings: &ModelServingBindings) -> Result<Self, Self::Error> {
        bindings
            .validate()
            .map_err(|error| ModelCandidateManifestError::Invalid(error.to_string()))?;
        match &bindings.model.estimator {
            ModelServingEstimatorBinding::FactorNative { ordered_inputs, .. } => {
                let verified_term_count = u64::try_from(ordered_inputs.len()).map_err(|error| {
                    ModelCandidateManifestError::Invalid(format!(
                        "candidate explanation term count overflow: {error}"
                    ))
                })?;
                Self::weighted(bindings.transform.input_contract_hash, verified_term_count)
            }
            ModelServingEstimatorBinding::Classical {
                tree_shap: Some(tree_shap),
                ..
            } => Self::tree_shap(
                bindings.transform.input_contract_hash,
                tree_shap.verified_case_count,
                tree_shap.max_efficiency_residual,
                tree_shap.max_prediction_residual,
            ),
            ModelServingEstimatorBinding::Classical {
                tree_shap: None, ..
            } => Err(ModelCandidateManifestError::UnsupportedExplanation {
                family: bindings.model.model_family,
            }),
        }
    }
}

/// Immutable aggregate proving one candidate passed every pre-shadow
/// readiness gate.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PromotionGateArtifact {
    pub format_version: u32,
    pub promotion_gate_hash: ContentHash,
    pub feedback_cycle_id: FeedbackCycleId,
    pub candidate_recipe_hash: ContentHash,
    pub candidate_model_version_id: ModelVersionId,
    pub profile_ref: ResearchProfileRef,
    pub category: MarketCategory,
    pub feedback_policy_hash: ContentHash,
    pub decision_policy_snapshot_hash: ContentHash,
    pub truth_freeze_hash: ContentHash,
    pub attribution_manifest_hash: ContentHash,
    pub validation_artifact_hash: ContentHash,
    pub quality_gate_report_hash: ContentHash,
    pub comparison_artifact_hash: ContentHash,
    pub cpcv_path_set_id: BacktestPathSetId,
    pub cpcv_path_set_hash: ContentHash,
    pub explanation_validation_hash: ContentHash,
    pub scenario_model_bindings_hash: ContentHash,
}

#[derive(Serialize)]
struct PromotionGatePreimage<'a> {
    format_version: u32,
    feedback_cycle_id: FeedbackCycleId,
    candidate_recipe_hash: ContentHash,
    candidate_model_version_id: ModelVersionId,
    profile_ref: &'a ResearchProfileRef,
    category: MarketCategory,
    feedback_policy_hash: ContentHash,
    decision_policy_snapshot_hash: ContentHash,
    truth_freeze_hash: ContentHash,
    attribution_manifest_hash: ContentHash,
    validation_artifact_hash: ContentHash,
    quality_gate_report_hash: ContentHash,
    comparison_artifact_hash: ContentHash,
    cpcv_path_set_id: BacktestPathSetId,
    cpcv_path_set_hash: ContentHash,
    explanation_validation_hash: ContentHash,
    scenario_model_bindings_hash: ContentHash,
}

/// Complete server-derived inputs for the pre-shadow readiness gate.
pub struct PromotionGateArtifactInput {
    pub feedback_cycle_id: FeedbackCycleId,
    pub candidate_recipe_hash: ContentHash,
    pub candidate_model_version_id: ModelVersionId,
    pub profile_ref: ResearchProfileRef,
    pub category: MarketCategory,
    pub feedback_policy_hash: ContentHash,
    pub decision_policy_snapshot_hash: ContentHash,
    pub truth_freeze_hash: ContentHash,
    pub attribution_manifest_hash: ContentHash,
    pub validation_artifact_hash: ContentHash,
    pub quality_gate_report_hash: ContentHash,
    pub comparison_artifact_hash: ContentHash,
    pub cpcv_path_set_id: BacktestPathSetId,
    pub cpcv_path_set_hash: ContentHash,
    pub explanation_validation_hash: ContentHash,
    pub scenario_model_bindings_hash: ContentHash,
}

impl PromotionGateArtifact {
    pub fn try_seal(
        input: PromotionGateArtifactInput,
    ) -> Result<Self, ModelCandidateManifestError> {
        let promotion_gate_hash = Self::derive_hash(&PromotionGatePreimage {
            format_version: PROMOTION_GATE_FORMAT_VERSION,
            feedback_cycle_id: input.feedback_cycle_id,
            candidate_recipe_hash: input.candidate_recipe_hash,
            candidate_model_version_id: input.candidate_model_version_id,
            profile_ref: &input.profile_ref,
            category: input.category,
            feedback_policy_hash: input.feedback_policy_hash,
            decision_policy_snapshot_hash: input.decision_policy_snapshot_hash,
            truth_freeze_hash: input.truth_freeze_hash,
            attribution_manifest_hash: input.attribution_manifest_hash,
            validation_artifact_hash: input.validation_artifact_hash,
            quality_gate_report_hash: input.quality_gate_report_hash,
            comparison_artifact_hash: input.comparison_artifact_hash,
            cpcv_path_set_id: input.cpcv_path_set_id,
            cpcv_path_set_hash: input.cpcv_path_set_hash,
            explanation_validation_hash: input.explanation_validation_hash,
            scenario_model_bindings_hash: input.scenario_model_bindings_hash,
        })?;
        let artifact = Self {
            format_version: PROMOTION_GATE_FORMAT_VERSION,
            promotion_gate_hash,
            feedback_cycle_id: input.feedback_cycle_id,
            candidate_recipe_hash: input.candidate_recipe_hash,
            candidate_model_version_id: input.candidate_model_version_id,
            profile_ref: input.profile_ref,
            category: input.category,
            feedback_policy_hash: input.feedback_policy_hash,
            decision_policy_snapshot_hash: input.decision_policy_snapshot_hash,
            truth_freeze_hash: input.truth_freeze_hash,
            attribution_manifest_hash: input.attribution_manifest_hash,
            validation_artifact_hash: input.validation_artifact_hash,
            quality_gate_report_hash: input.quality_gate_report_hash,
            comparison_artifact_hash: input.comparison_artifact_hash,
            cpcv_path_set_id: input.cpcv_path_set_id,
            cpcv_path_set_hash: input.cpcv_path_set_hash,
            explanation_validation_hash: input.explanation_validation_hash,
            scenario_model_bindings_hash: input.scenario_model_bindings_hash,
        };
        artifact.validate()?;
        Ok(artifact)
    }

    pub fn validate(&self) -> Result<(), ModelCandidateManifestError> {
        self.profile_ref
            .validate()
            .map_err(|error| ModelCandidateManifestError::Invalid(error.to_string()))?;
        let profile = self
            .profile_ref
            .resolve_builtin_research_profile()
            .map_err(ModelCandidateManifestError::Invalid)?;
        let expected = Self::derive_hash(&self.preimage())?;
        if self.format_version != PROMOTION_GATE_FORMAT_VERSION
            || profile.spec.category != Some(self.category)
            || !matches!(
                self.category,
                MarketCategory::Crypto | MarketCategory::Weather
            )
            || self.promotion_gate_hash != expected
        {
            return Err(ModelCandidateManifestError::Invalid(
                "promotion gate identity, profile, category, or aggregate hash is invalid"
                    .to_owned(),
            ));
        }
        Ok(())
    }

    const fn preimage(&self) -> PromotionGatePreimage<'_> {
        PromotionGatePreimage {
            format_version: self.format_version,
            feedback_cycle_id: self.feedback_cycle_id,
            candidate_recipe_hash: self.candidate_recipe_hash,
            candidate_model_version_id: self.candidate_model_version_id,
            profile_ref: &self.profile_ref,
            category: self.category,
            feedback_policy_hash: self.feedback_policy_hash,
            decision_policy_snapshot_hash: self.decision_policy_snapshot_hash,
            truth_freeze_hash: self.truth_freeze_hash,
            attribution_manifest_hash: self.attribution_manifest_hash,
            validation_artifact_hash: self.validation_artifact_hash,
            quality_gate_report_hash: self.quality_gate_report_hash,
            comparison_artifact_hash: self.comparison_artifact_hash,
            cpcv_path_set_id: self.cpcv_path_set_id,
            cpcv_path_set_hash: self.cpcv_path_set_hash,
            explanation_validation_hash: self.explanation_validation_hash,
            scenario_model_bindings_hash: self.scenario_model_bindings_hash,
        }
    }

    fn derive_hash(
        preimage: &PromotionGatePreimage<'_>,
    ) -> Result<ContentHash, ModelCandidateManifestError> {
        CanonicalDigest::content_hash_typed(
            PROMOTION_GATE_HASH_DOMAIN,
            PROMOTION_GATE_FORMAT_VERSION,
            preimage,
        )
        .map_err(|error| ModelCandidateManifestError::Hash(error.to_string()))
    }
}

/// Canonical model, data, calibration, CPCV, explanation, and policy binding.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, FromJsonQueryResult)]
#[serde(deny_unknown_fields)]
pub struct ModelCandidateManifestDocument {
    pub format_version: u32,
    pub feedback_cycle_id: FeedbackCycleId,
    pub candidate_recipe_hash: ContentHash,
    pub model_version_id: ModelVersionId,
    pub model_spec_id: ModelSpecId,
    pub model_family: ModelFamily,
    pub model_artifact_hash: ContentHash,
    pub serving_contract_hash: ContentHash,
    pub training_dataset_id: TrainingDatasetId,
    pub training_dataset_hash: ContentHash,
    pub feature_schema_hash: ContentHash,
    pub input_contract_hash: ContentHash,
    pub input_transform_hash: ContentHash,
    pub calibration_artifact_id: Option<CalibrationArtifactId>,
    pub calibration_artifact_hash: Option<ContentHash>,
    pub cpcv_path_set_id: BacktestPathSetId,
    pub cpcv_path_set_hash: ContentHash,
    pub profile_ref: ResearchProfileRef,
    pub category: MarketCategory,
    pub feedback_policy_hash: ContentHash,
    pub decision_policy_snapshot_hash: ContentHash,
    pub explanation_validation: CandidateExplanationValidation,
    pub portfolio_scenario_model_bindings: Vec<PortfolioScenarioModelArtifactBinding>,
    pub scenario_model_bindings_hash: ContentHash,
    pub promotion_gate: PromotionGateArtifact,
}

/// Complete server-derived inputs for a candidate manifest.
pub struct ModelCandidateManifestInput {
    pub feedback_cycle_id: FeedbackCycleId,
    pub candidate_recipe_hash: ContentHash,
    pub model_version_id: ModelVersionId,
    pub model_spec_id: ModelSpecId,
    pub model_family: ModelFamily,
    pub model_artifact_hash: ContentHash,
    pub serving_contract_hash: ContentHash,
    pub training_dataset_id: TrainingDatasetId,
    pub training_dataset_hash: ContentHash,
    pub feature_schema_hash: ContentHash,
    pub input_contract_hash: ContentHash,
    pub input_transform_hash: ContentHash,
    pub calibration_artifact_id: Option<CalibrationArtifactId>,
    pub calibration_artifact_hash: Option<ContentHash>,
    pub cpcv_path_set_id: BacktestPathSetId,
    pub cpcv_path_set_hash: ContentHash,
    pub profile_ref: ResearchProfileRef,
    pub category: MarketCategory,
    pub feedback_policy_hash: ContentHash,
    pub decision_policy_snapshot_hash: ContentHash,
    pub explanation_validation: CandidateExplanationValidation,
    pub portfolio_scenario_model_bindings: Vec<PortfolioScenarioModelArtifactBinding>,
    pub scenario_model_bindings_hash: ContentHash,
    pub promotion_gate: PromotionGateArtifact,
}

impl ModelCandidateManifestDocument {
    pub fn try_new(
        input: ModelCandidateManifestInput,
    ) -> Result<Self, ModelCandidateManifestError> {
        let document = Self {
            format_version: MANIFEST_FORMAT_VERSION,
            feedback_cycle_id: input.feedback_cycle_id,
            candidate_recipe_hash: input.candidate_recipe_hash,
            model_version_id: input.model_version_id,
            model_spec_id: input.model_spec_id,
            model_family: input.model_family,
            model_artifact_hash: input.model_artifact_hash,
            serving_contract_hash: input.serving_contract_hash,
            training_dataset_id: input.training_dataset_id,
            training_dataset_hash: input.training_dataset_hash,
            feature_schema_hash: input.feature_schema_hash,
            input_contract_hash: input.input_contract_hash,
            input_transform_hash: input.input_transform_hash,
            calibration_artifact_id: input.calibration_artifact_id,
            calibration_artifact_hash: input.calibration_artifact_hash,
            cpcv_path_set_id: input.cpcv_path_set_id,
            cpcv_path_set_hash: input.cpcv_path_set_hash,
            profile_ref: input.profile_ref,
            category: input.category,
            feedback_policy_hash: input.feedback_policy_hash,
            decision_policy_snapshot_hash: input.decision_policy_snapshot_hash,
            explanation_validation: input.explanation_validation,
            portfolio_scenario_model_bindings: input.portfolio_scenario_model_bindings,
            scenario_model_bindings_hash: input.scenario_model_bindings_hash,
            promotion_gate: input.promotion_gate,
        };
        document.validate()?;
        Ok(document)
    }

    pub fn validate(&self) -> Result<(), ModelCandidateManifestError> {
        self.profile_ref
            .validate()
            .map_err(|error| ModelCandidateManifestError::Invalid(error.to_string()))?;
        let profile = self
            .profile_ref
            .resolve_builtin_research_profile()
            .map_err(ModelCandidateManifestError::Invalid)?;
        if self.format_version != MANIFEST_FORMAT_VERSION {
            return Err(ModelCandidateManifestError::Invalid(format!(
                "unsupported candidate manifest version {}",
                self.format_version
            )));
        }
        if !matches!(
            self.category,
            MarketCategory::Crypto | MarketCategory::Weather
        ) || profile.spec.category != Some(self.category)
        {
            return Err(ModelCandidateManifestError::Invalid(
                "candidate manifest profile and category differ".to_owned(),
            ));
        }
        if self.calibration_artifact_id.is_some() != self.calibration_artifact_hash.is_some() {
            return Err(ModelCandidateManifestError::Invalid(
                "candidate manifest calibration id/hash binding is partial".to_owned(),
            ));
        }
        self.promotion_gate.validate()?;
        self.explanation_validation.validate()?;
        validate_scenario_model_bindings(self.category, &self.portfolio_scenario_model_bindings)?;
        let expected_scenario_bindings_hash =
            scenario_model_bindings_hash(&self.portfolio_scenario_model_bindings)?;
        if self.feedback_cycle_id != self.promotion_gate.feedback_cycle_id
            || self.candidate_recipe_hash != self.promotion_gate.candidate_recipe_hash
            || self.model_version_id != self.promotion_gate.candidate_model_version_id
            || self.profile_ref != self.promotion_gate.profile_ref
            || self.category != self.promotion_gate.category
            || self.feedback_policy_hash != self.promotion_gate.feedback_policy_hash
            || self.decision_policy_snapshot_hash
                != self.promotion_gate.decision_policy_snapshot_hash
            || self.cpcv_path_set_id != self.promotion_gate.cpcv_path_set_id
            || self.cpcv_path_set_hash != self.promotion_gate.cpcv_path_set_hash
            || self.explanation_validation.input_contract_hash != self.input_contract_hash
            || self.explanation_validation.report_hash
                != self.promotion_gate.explanation_validation_hash
            || self.scenario_model_bindings_hash != expected_scenario_bindings_hash
            || self.scenario_model_bindings_hash != self.promotion_gate.scenario_model_bindings_hash
        {
            return Err(ModelCandidateManifestError::Invalid(
                "candidate manifest differs from its final promotion gate".to_owned(),
            ));
        }
        Ok(())
    }

    pub fn manifest_hash(&self) -> Result<ContentHash, ModelCandidateManifestError> {
        self.validate()?;
        CanonicalDigest::content_hash_typed(MANIFEST_HASH_DOMAIN, MANIFEST_FORMAT_VERSION, self)
            .map_err(|error| ModelCandidateManifestError::Hash(error.to_string()))
    }

    #[must_use]
    pub fn scenario_model_bindings(&self) -> &[PortfolioScenarioModelArtifactBinding] {
        &self.portfolio_scenario_model_bindings
    }
}

pub fn scenario_model_bindings_hash(
    bindings: &[PortfolioScenarioModelArtifactBinding],
) -> Result<ContentHash, ModelCandidateManifestError> {
    CanonicalDigest::content_hash_typed(
        "quant-pivot/candidate-scenario-model-bindings",
        1,
        &bindings,
    )
    .map_err(|error| ModelCandidateManifestError::Hash(error.to_string()))
}

pub(super) fn validate_scenario_model_bindings(
    category: MarketCategory,
    bindings: &[PortfolioScenarioModelArtifactBinding],
) -> Result<(), ModelCandidateManifestError> {
    let route = BuyModelRoute::try_from(Some(category))
        .map_err(|error| ModelCandidateManifestError::Invalid(error.to_string()))?;
    if bindings.is_empty() {
        return Err(ModelCandidateManifestError::Invalid(
            "candidate manifest has no prospective scenario-model binding".to_owned(),
        ));
    }
    let mut previous = None;
    for binding in bindings {
        let represented = RepresentedRouteSet::from_routes(binding.ordered_routes.clone())
            .map_err(|error| ModelCandidateManifestError::Invalid(error.to_string()))?;
        let key = (
            binding.route_set_digest,
            binding.model_content_hash,
            binding.portfolio_scenario_model_artifact_id.as_uuid(),
        );
        if represented.routes != binding.ordered_routes
            || represented.digest != binding.route_set_digest
            || !binding.ordered_routes.contains(&route)
            || binding.portfolio_scenario_model_artifact_id
                != PortfolioScenarioModelArtifactId::from_content_hash(&binding.model_content_hash)
            || binding.scenario_model_schema_version.get() == 0
            || previous.is_some_and(|prior| prior >= key)
        {
            return Err(ModelCandidateManifestError::Invalid(
                "candidate scenario-model bindings are non-canonical, incompatible, or do not cover the promoted Route"
                    .to_owned(),
            ));
        }
        previous = Some(key);
    }
    Ok(())
}

/// Insert payload for the WORM candidate-manifest ledger.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, DeriveIntoActiveModel)]
#[sea_orm(active_model = "crate::entities::quant_model_candidate_manifest::ActiveModel")]
pub struct NewModelCandidateManifest {
    pub manifest_id: ModelCandidateManifestId,
    pub feedback_cycle_id: FeedbackCycleId,
    pub candidate_recipe_hash: ContentHash,
    pub model_version_id: ModelVersionId,
    pub promotion_gate_hash: ContentHash,
    pub manifest_hash: ContentHash,
    pub document: ModelCandidateManifestDocument,
}

impl NewModelCandidateManifest {
    pub fn try_new(
        document: ModelCandidateManifestDocument,
    ) -> Result<Self, ModelCandidateManifestError> {
        let manifest_hash = document.manifest_hash()?;
        Ok(Self {
            manifest_id: ModelCandidateManifestId::from_content_hash(&manifest_hash),
            feedback_cycle_id: document.feedback_cycle_id,
            candidate_recipe_hash: document.candidate_recipe_hash,
            model_version_id: document.model_version_id,
            promotion_gate_hash: document.promotion_gate.promotion_gate_hash,
            manifest_hash,
            document,
        })
    }

    pub fn validate(&self) -> Result<(), ModelCandidateManifestError> {
        self.document.validate()?;
        let expected_hash = self.document.manifest_hash()?;
        if self.manifest_hash != expected_hash
            || self.manifest_id != ModelCandidateManifestId::from_content_hash(&expected_hash)
            || self.feedback_cycle_id != self.document.feedback_cycle_id
            || self.candidate_recipe_hash != self.document.candidate_recipe_hash
            || self.model_version_id != self.document.model_version_id
            || self.promotion_gate_hash != self.document.promotion_gate.promotion_gate_hash
        {
            return Err(ModelCandidateManifestError::IdentityMismatch);
        }
        Ok(())
    }
}

/// Persisted immutable candidate-manifest projection.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, DerivePartialModel)]
#[sea_orm(entity = "crate::entities::quant_model_candidate_manifest::Entity")]
pub struct ModelCandidateManifestInfo {
    pub manifest_id: ModelCandidateManifestId,
    pub feedback_cycle_id: FeedbackCycleId,
    pub candidate_recipe_hash: ContentHash,
    pub model_version_id: ModelVersionId,
    pub promotion_gate_hash: ContentHash,
    pub manifest_hash: ContentHash,
    pub document: ModelCandidateManifestDocument,
    pub created_at: DateTime<Utc>,
}

info_from_model!(
    ModelCandidateManifestInfo,
    quant_model_candidate_manifest::Model,
    {
        manifest_id,
        feedback_cycle_id,
        candidate_recipe_hash,
        model_version_id,
        promotion_gate_hash,
        manifest_hash,
        document,
        created_at,
    }
);

impl ModelCandidateManifestInfo {
    pub fn validate(&self) -> Result<(), ModelCandidateManifestError> {
        NewModelCandidateManifest {
            manifest_id: self.manifest_id,
            feedback_cycle_id: self.feedback_cycle_id,
            candidate_recipe_hash: self.candidate_recipe_hash,
            model_version_id: self.model_version_id,
            promotion_gate_hash: self.promotion_gate_hash,
            manifest_hash: self.manifest_hash,
            document: self.document.clone(),
        }
        .validate()
    }

    #[must_use]
    pub fn matches(&self, candidate: &NewModelCandidateManifest) -> bool {
        self.manifest_id == candidate.manifest_id
            && self.feedback_cycle_id == candidate.feedback_cycle_id
            && self.candidate_recipe_hash == candidate.candidate_recipe_hash
            && self.model_version_id == candidate.model_version_id
            && self.promotion_gate_hash == candidate.promotion_gate_hash
            && self.manifest_hash == candidate.manifest_hash
            && self.document == candidate.document
    }
}

#[derive(Debug, Error)]
pub enum ModelCandidateManifestError {
    #[error("invalid candidate manifest: {0}")]
    Invalid(String),
    #[error("candidate manifest hash failed: {0}")]
    Hash(String),
    #[error("model family {family:?} has no exact supported explanation contract")]
    UnsupportedExplanation { family: ModelFamily },
    #[error("candidate manifest normalized identity differs from its document")]
    IdentityMismatch,
}
