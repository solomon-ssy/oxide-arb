//! Governed recipe-catalog and immutable feedback recipe-plan contracts.

use std::{collections::BTreeSet, sync::Arc};

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use quant_pivot_error::{QuantResult, feedback::FeedbackError};
use rust_decimal::Decimal;
use sea_orm::FromJsonQueryResult;
use serde::{Deserialize, Serialize};
use tokio_util::sync::CancellationToken;

use crate::{
    domain::quant::{JobProgressSink, ResearchJobArtifactRef},
    enums::{
        model::ModelFamily,
        quant::{
            AttributionArtifactKind, AttributionCohort, CalibrationMethod, DownsideSource,
            FeedbackDriftMetric, FeedbackEvaluationMode, FeedbackRecipeTemplateStatus,
        },
    },
    hashing::CanonicalDigest,
    runtime_config::{BuyModelRoute, ResearchValidationConfig},
    types::{
        Bps, CandidateRecipePlanArtifactId, ContentHash, FeedbackCycleId, FeedbackRecipeTemplateId,
        ModelInputContract, ModelSpecId, ModelTrainingContract, ResearchJobId, ResearchProfileRef,
        RoleCode, UserId,
    },
};

use super::feedback_execution::FeedbackCandidateFamily;

const RECIPE_TEMPLATE_DOMAIN: &str = "quant-pivot/feedback-recipe-template";
const RECIPE_TEMPLATE_VERSION: u32 = 3;
const RECIPE_PLAN_DOMAIN: &str = "quant-pivot/candidate-recipe-plan";
const RECIPE_PLAN_VERSION: u32 = 3;
const TRAINING_SPEC_DOMAIN: &str = "quant-pivot/feedback-recipe-training-spec";
const CALIBRATION_SPEC_DOMAIN: &str = "quant-pivot/feedback-recipe-calibration-spec";
const CPCV_SPEC_DOMAIN: &str = "quant-pivot/feedback-recipe-cpcv-spec";
const DOWNSIDE_SPEC_DOMAIN: &str = "quant-pivot/feedback-recipe-downside-spec";
const PLANNER_EVIDENCE_DOMAIN: &str = "quant-pivot/feedback-recipe-planner-evidence";
const SPEC_VERSION: u32 = 1;
const CPCV_SPEC_VERSION: u32 = 2;

/// Explicit CPU, memory, and deadline envelope for one challenger template.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FeedbackRecipeResourceBudget {
    pub max_concurrency: u32,
    pub max_working_set_bytes: u64,
    pub max_resident_model_bytes: u64,
    pub deadline_secs: u64,
}

impl FeedbackRecipeResourceBudget {
    pub fn validate(self) -> Result<(), FeedbackError> {
        if self.max_concurrency == 0
            || self.max_working_set_bytes == 0
            || self.max_resident_model_bytes == 0
            || self.deadline_secs == 0
        {
            return Err(invalid_recipe(
                "recipe CPU, working-set, and deadline budgets must all be positive",
            ));
        }
        Ok(())
    }
}

/// Complete governed training recipe. Estimator parameters remain owned by
/// the immutable model specification; the planner may only select a catalog
/// entry and a bounded historical window.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FeedbackRecipeTrainingSpec {
    pub spec_hash: ContentHash,
    pub model_spec_id: ModelSpecId,
    pub model_spec_definition_hash: ContentHash,
    pub input_contract: ModelInputContract,
    pub training_contract: ModelTrainingContract,
    pub training_window_days: u32,
}

impl FeedbackRecipeTrainingSpec {
    pub fn try_new(
        model_spec_id: ModelSpecId,
        model_spec_definition_hash: ContentHash,
        input_contract: ModelInputContract,
        training_contract: ModelTrainingContract,
        training_window_days: u32,
    ) -> Result<Self, FeedbackError> {
        let spec_hash = Self::derive_hash(
            model_spec_id,
            model_spec_definition_hash,
            &input_contract,
            &training_contract,
            training_window_days,
        )?;
        let spec = Self {
            spec_hash,
            model_spec_id,
            model_spec_definition_hash,
            input_contract,
            training_contract,
            training_window_days,
        };
        spec.validate()?;
        Ok(spec)
    }

    pub fn validate(&self) -> Result<(), FeedbackError> {
        self.input_contract.validate().map_err(invalid_recipe)?;
        self.training_contract.validate().map_err(invalid_recipe)?;
        if self.training_window_days == 0
            || self.spec_hash
                != Self::derive_hash(
                    self.model_spec_id,
                    self.model_spec_definition_hash,
                    &self.input_contract,
                    &self.training_contract,
                    self.training_window_days,
                )?
        {
            return Err(invalid_recipe(
                "training recipe window or immutable spec hash is invalid",
            ));
        }
        Ok(())
    }

    fn derive_hash(
        model_spec_id: ModelSpecId,
        model_spec_definition_hash: ContentHash,
        input_contract: &ModelInputContract,
        training_contract: &ModelTrainingContract,
        training_window_days: u32,
    ) -> Result<ContentHash, FeedbackError> {
        CanonicalDigest::content_hash_typed(
            TRAINING_SPEC_DOMAIN,
            SPEC_VERSION,
            &(
                SPEC_VERSION,
                model_spec_id,
                model_spec_definition_hash,
                input_contract,
                training_contract,
                training_window_days,
            ),
        )
        .map_err(Into::into)
    }
}

/// Complete governed calibration recipe.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FeedbackRecipeCalibrationSpec {
    pub spec_hash: ContentHash,
    pub method: CalibrationMethod,
    pub calibration_window_days: u32,
}

impl FeedbackRecipeCalibrationSpec {
    pub fn try_new(
        method: CalibrationMethod,
        calibration_window_days: u32,
    ) -> Result<Self, FeedbackError> {
        let spec_hash = Self::derive_hash(method, calibration_window_days)?;
        let spec = Self {
            spec_hash,
            method,
            calibration_window_days,
        };
        spec.validate()?;
        Ok(spec)
    }

    pub fn validate(&self) -> Result<(), FeedbackError> {
        if self.calibration_window_days == 0
            || self.spec_hash != Self::derive_hash(self.method, self.calibration_window_days)?
        {
            return Err(invalid_recipe(
                "calibration recipe window or immutable spec hash is invalid",
            ));
        }
        Ok(())
    }

    fn derive_hash(
        method: CalibrationMethod,
        calibration_window_days: u32,
    ) -> Result<ContentHash, FeedbackError> {
        CanonicalDigest::content_hash_typed(
            CALIBRATION_SPEC_DOMAIN,
            SPEC_VERSION,
            &(SPEC_VERSION, method, calibration_window_days),
        )
        .map_err(Into::into)
    }
}

/// Complete CPCV and downside-gate recipe inherited by every candidate run.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FeedbackRecipeCpcvSpec {
    pub spec_hash: ContentHash,
    pub validation: ResearchValidationConfig,
    pub target_horizon_secs: u64,
    pub purge_embargo_secs: u64,
}

impl FeedbackRecipeCpcvSpec {
    pub fn try_new(
        validation: ResearchValidationConfig,
        target_horizon_secs: u64,
        purge_embargo_secs: u64,
    ) -> Result<Self, FeedbackError> {
        let spec_hash = Self::derive_hash(&validation, target_horizon_secs, purge_embargo_secs)?;
        let spec = Self {
            spec_hash,
            validation,
            target_horizon_secs,
            purge_embargo_secs,
        };
        spec.validate()?;
        Ok(spec)
    }

    pub fn validate(&self) -> Result<(), FeedbackError> {
        let expected_paths = self.expected_path_count()?;
        let expected_combinations = self.expected_combination_count()?;
        let gates = &self.validation.gates;
        let trials = &self.validation.trials;
        let weighted_trials = trials
            .lambda_multipliers
            .len()
            .checked_mul(trials.rank_loss_kinds.len())
            .ok_or_else(|| invalid_recipe("weighted CPCV trial-grid size overflow"))?;
        let classical_trials = trials.logistic_alpha_multipliers.len();
        let trial_bound = usize::try_from(trials.max_trials).map_err(|error| {
            invalid_recipe(format!("CPCV max_trials does not fit usize: {error}"))
        })?;
        let multipliers_valid = trials
            .lambda_multipliers
            .iter()
            .chain(&trials.logistic_alpha_multipliers)
            .all(|value| value.value >= Decimal::ZERO);
        let ratios_valid = self.validation.purge.embargo_pct.value >= Decimal::ZERO
            && self.validation.purge.embargo_pct.value < Decimal::ONE
            && gates.target_rank_ic_min.value >= Decimal::ZERO
            && gates.dsr_significance.value >= Decimal::ZERO
            && gates.dsr_significance.value < Decimal::ONE
            && gates.max_pbo.value >= Decimal::ZERO
            && gates.max_pbo.value <= Decimal::ONE
            && gates.max_turnover.value >= Decimal::ZERO;
        if self.target_horizon_secs == 0
            || self.purge_embargo_secs == 0
            || expected_paths < u64::from(gates.min_cpcv_paths)
            || gates.min_cpcv_paths < 21
            || expected_combinations == 0
            || self.validation.pbo.block_count < 4
            || !self.validation.pbo.block_count.is_multiple_of(2)
            || trials.lambda_multipliers.is_empty()
            || trials.rank_loss_kinds.is_empty()
            || trials.logistic_alpha_multipliers.is_empty()
            || weighted_trials.max(classical_trials) > trial_bound
            || !multipliers_valid
            || !ratios_valid
            || self.spec_hash
                != Self::derive_hash(
                    &self.validation,
                    self.target_horizon_secs,
                    self.purge_embargo_secs,
                )?
        {
            return Err(invalid_recipe(
                "CPCV methodology, path floor, horizon, embargo, trial grid, gates, or immutable spec hash is invalid",
            ));
        }
        Ok(())
    }

    pub fn expected_path_count(&self) -> Result<u64, FeedbackError> {
        self.validation
            .cpcv
            .expected_path_count()
            .map_err(invalid_recipe)
    }

    pub fn expected_combination_count(&self) -> Result<u64, FeedbackError> {
        self.validation
            .cpcv
            .expected_combination_count()
            .map_err(invalid_recipe)
    }

    fn derive_hash(
        validation: &ResearchValidationConfig,
        target_horizon_secs: u64,
        purge_embargo_secs: u64,
    ) -> Result<ContentHash, FeedbackError> {
        CanonicalDigest::content_hash_typed(
            CPCV_SPEC_DOMAIN,
            CPCV_SPEC_VERSION,
            &(
                CPCV_SPEC_VERSION,
                validation,
                target_horizon_secs,
                purge_embargo_secs,
            ),
        )
        .map_err(Into::into)
    }
}

/// Complete downside semantics for calibration and validation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FeedbackRecipeDownsideSpec {
    pub spec_hash: ContentHash,
    pub source: DownsideSource,
}

impl FeedbackRecipeDownsideSpec {
    pub fn try_new(source: DownsideSource) -> Result<Self, FeedbackError> {
        let spec_hash = CanonicalDigest::content_hash_typed(
            DOWNSIDE_SPEC_DOMAIN,
            SPEC_VERSION,
            &(SPEC_VERSION, source),
        )?;
        Ok(Self { spec_hash, source })
    }

    pub fn validate(&self) -> Result<(), FeedbackError> {
        if self.spec_hash
            != CanonicalDigest::content_hash_typed(
                DOWNSIDE_SPEC_DOMAIN,
                SPEC_VERSION,
                &(SPEC_VERSION, self.source),
            )?
        {
            return Err(invalid_recipe(
                "downside recipe immutable spec hash is invalid",
            ));
        }
        Ok(())
    }
}

/// Catalog-declared attribution gate. Matching is diagnostic and never a
/// causal claim: only already-approved recipes can be selected.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FeedbackRecipeDiagnosticSpec {
    pub accepted_artifact_kinds: Vec<AttributionArtifactKind>,
    pub responsive_feature_names: Vec<String>,
    pub minimum_evidence_count: u32,
    pub minimum_feature_matches: u32,
}

impl FeedbackRecipeDiagnosticSpec {
    pub fn canonicalize(&mut self) {
        self.accepted_artifact_kinds
            .sort_by_key(|kind| kind.as_str());
        self.accepted_artifact_kinds.dedup();
        self.responsive_feature_names.sort();
        self.responsive_feature_names.dedup();
    }

    pub fn validate(&self) -> Result<(), FeedbackError> {
        let kinds_canonical = !self.accepted_artifact_kinds.is_empty()
            && self
                .accepted_artifact_kinds
                .windows(2)
                .all(|pair| pair[0].as_str() < pair[1].as_str());
        let features_canonical = self.responsive_feature_names.windows(2).all(|pair| {
            pair[0] < pair[1]
                && !pair[0].is_empty()
                && pair[0] == pair[0].trim()
                && !pair[0].chars().any(char::is_control)
        }) && self.responsive_feature_names.last().is_none_or(|name| {
            !name.is_empty() && name == name.trim() && !name.chars().any(char::is_control)
        });
        let feature_bound = usize::try_from(self.minimum_feature_matches)
            .ok()
            .is_some_and(|minimum| minimum <= self.responsive_feature_names.len());
        if !kinds_canonical
            || !features_canonical
            || self.minimum_evidence_count == 0
            || !feature_bound
            || (self.responsive_feature_names.is_empty() != (self.minimum_feature_matches == 0))
        {
            return Err(invalid_recipe(
                "recipe attribution kinds, features, or diagnostic evidence floors are invalid",
            ));
        }
        Ok(())
    }
}

/// Inputs jointly sealed into one immutable recipe-template revision.
pub struct FeedbackRecipeTemplateInput {
    pub recipe_template_id: FeedbackRecipeTemplateId,
    pub revision: u32,
    pub profile_ref: ResearchProfileRef,
    pub route: BuyModelRoute,
    pub model_family: ModelFamily,
    pub training_spec: FeedbackRecipeTrainingSpec,
    pub calibration_spec: FeedbackRecipeCalibrationSpec,
    pub cpcv_spec: FeedbackRecipeCpcvSpec,
    pub downside_spec: FeedbackRecipeDownsideSpec,
    pub diagnostic_spec: FeedbackRecipeDiagnosticSpec,
    pub responsive_triggers: Vec<FeedbackDriftMetric>,
    pub catalog_priority: i32,
    pub resource_budget: FeedbackRecipeResourceBudget,
    pub status: FeedbackRecipeTemplateStatus,
    pub approved_by_user_id: Option<UserId>,
    pub approved_by_role: Option<RoleCode>,
    pub approved_at: Option<DateTime<Utc>>,
    pub governance_reason: String,
}

/// Immutable, versioned recipe-template catalog row.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, FromJsonQueryResult)]
#[serde(deny_unknown_fields)]
pub struct FeedbackRecipeTemplate {
    pub format_version: u32,
    pub recipe_template_id: FeedbackRecipeTemplateId,
    pub revision: u32,
    pub template_hash: ContentHash,
    pub profile_ref: ResearchProfileRef,
    pub route: BuyModelRoute,
    pub model_family: ModelFamily,
    pub training_spec: FeedbackRecipeTrainingSpec,
    pub calibration_spec: FeedbackRecipeCalibrationSpec,
    pub cpcv_spec: FeedbackRecipeCpcvSpec,
    pub downside_spec: FeedbackRecipeDownsideSpec,
    pub diagnostic_spec: FeedbackRecipeDiagnosticSpec,
    pub responsive_triggers: Vec<FeedbackDriftMetric>,
    pub catalog_priority: i32,
    pub resource_budget: FeedbackRecipeResourceBudget,
    pub status: FeedbackRecipeTemplateStatus,
    pub approved_by_user_id: Option<UserId>,
    pub approved_by_role: Option<RoleCode>,
    pub approved_at: Option<DateTime<Utc>>,
    pub governance_reason: String,
}

#[derive(Serialize)]
struct FeedbackRecipeTemplatePreimage<'a> {
    format_version: u32,
    recipe_template_id: FeedbackRecipeTemplateId,
    revision: u32,
    profile_ref: &'a ResearchProfileRef,
    route: BuyModelRoute,
    model_family: ModelFamily,
    training_spec: &'a FeedbackRecipeTrainingSpec,
    calibration_spec: &'a FeedbackRecipeCalibrationSpec,
    cpcv_spec: &'a FeedbackRecipeCpcvSpec,
    downside_spec: &'a FeedbackRecipeDownsideSpec,
    diagnostic_spec: &'a FeedbackRecipeDiagnosticSpec,
    responsive_triggers: &'a [FeedbackDriftMetric],
    catalog_priority: i32,
    resource_budget: FeedbackRecipeResourceBudget,
    status: FeedbackRecipeTemplateStatus,
    approved_by_user_id: Option<UserId>,
    approved_by_role: &'a Option<RoleCode>,
    approved_at: Option<DateTime<Utc>>,
    governance_reason: &'a str,
}

impl FeedbackRecipeTemplate {
    pub fn try_seal(mut input: FeedbackRecipeTemplateInput) -> Result<Self, FeedbackError> {
        input.diagnostic_spec.canonicalize();
        input
            .responsive_triggers
            .sort_by_key(|trigger| trigger.as_str());
        input
            .responsive_triggers
            .dedup_by_key(|trigger| trigger.as_str());
        let template_hash = Self::derive_hash(&input)?;
        let template = Self {
            format_version: RECIPE_TEMPLATE_VERSION,
            recipe_template_id: input.recipe_template_id,
            revision: input.revision,
            template_hash,
            profile_ref: input.profile_ref,
            route: input.route,
            model_family: input.model_family,
            training_spec: input.training_spec,
            calibration_spec: input.calibration_spec,
            cpcv_spec: input.cpcv_spec,
            downside_spec: input.downside_spec,
            diagnostic_spec: input.diagnostic_spec,
            responsive_triggers: input.responsive_triggers,
            catalog_priority: input.catalog_priority,
            resource_budget: input.resource_budget,
            status: input.status,
            approved_by_user_id: input.approved_by_user_id,
            approved_by_role: input.approved_by_role,
            approved_at: input.approved_at,
            governance_reason: input.governance_reason,
        };
        template.validate()?;
        Ok(template)
    }

    pub fn validate(&self) -> Result<(), FeedbackError> {
        self.profile_ref
            .validate()
            .map_err(|error| invalid_recipe(error.to_string()))?;
        self.training_spec.validate()?;
        self.calibration_spec.validate()?;
        self.cpcv_spec.validate()?;
        self.downside_spec.validate()?;
        self.diagnostic_spec.validate()?;
        self.resource_budget.validate()?;
        let approval_absent = self.approved_by_user_id.is_none()
            && self.approved_by_role.is_none()
            && self.approved_at.is_none();
        let approval_complete = self.approved_by_user_id.is_some()
            && self.approved_by_role.is_some()
            && self.approved_at.is_some();
        let approval_valid = match self.status {
            FeedbackRecipeTemplateStatus::Draft => approval_absent,
            FeedbackRecipeTemplateStatus::Approved | FeedbackRecipeTemplateStatus::Retired => {
                approval_complete
            }
        };
        let canonical_triggers = !self.responsive_triggers.is_empty()
            && self
                .responsive_triggers
                .windows(2)
                .all(|pair| pair[0].as_str() < pair[1].as_str());
        if self.format_version != RECIPE_TEMPLATE_VERSION
            || self.revision == 0
            || !approval_valid
            || !canonical_triggers
            || self.governance_reason.is_empty()
            || self.governance_reason != self.governance_reason.trim()
            || self.governance_reason.len() > 2_048
            || self.governance_reason.chars().any(char::is_control)
            || self.template_hash != Self::derive_hash(&self.input())?
        {
            return Err(invalid_recipe(
                "recipe template version, approval, trigger set, reason, or hash is invalid",
            ));
        }
        Ok(())
    }

    #[must_use]
    pub fn responds_to(&self, triggers: &[FeedbackDriftMetric]) -> bool {
        triggers.iter().any(|trigger| {
            self.responsive_triggers
                .iter()
                .any(|allowed| allowed == trigger)
        })
    }

    fn input(&self) -> FeedbackRecipeTemplateInput {
        FeedbackRecipeTemplateInput {
            recipe_template_id: self.recipe_template_id,
            revision: self.revision,
            profile_ref: self.profile_ref.clone(),
            route: self.route,
            model_family: self.model_family,
            training_spec: self.training_spec.clone(),
            calibration_spec: self.calibration_spec.clone(),
            cpcv_spec: self.cpcv_spec.clone(),
            downside_spec: self.downside_spec.clone(),
            diagnostic_spec: self.diagnostic_spec.clone(),
            responsive_triggers: self.responsive_triggers.clone(),
            catalog_priority: self.catalog_priority,
            resource_budget: self.resource_budget,
            status: self.status,
            approved_by_user_id: self.approved_by_user_id,
            approved_by_role: self.approved_by_role.clone(),
            approved_at: self.approved_at,
            governance_reason: self.governance_reason.clone(),
        }
    }

    fn derive_hash(input: &FeedbackRecipeTemplateInput) -> Result<ContentHash, FeedbackError> {
        CanonicalDigest::content_hash_typed(
            RECIPE_TEMPLATE_DOMAIN,
            RECIPE_TEMPLATE_VERSION,
            &FeedbackRecipeTemplatePreimage {
                format_version: RECIPE_TEMPLATE_VERSION,
                recipe_template_id: input.recipe_template_id,
                revision: input.revision,
                profile_ref: &input.profile_ref,
                route: input.route,
                model_family: input.model_family,
                training_spec: &input.training_spec,
                calibration_spec: &input.calibration_spec,
                cpcv_spec: &input.cpcv_spec,
                downside_spec: &input.downside_spec,
                diagnostic_spec: &input.diagnostic_spec,
                responsive_triggers: &input.responsive_triggers,
                catalog_priority: input.catalog_priority,
                resource_budget: input.resource_budget,
                status: input.status,
                approved_by_user_id: input.approved_by_user_id,
                approved_by_role: &input.approved_by_role,
                approved_at: input.approved_at,
                governance_reason: &input.governance_reason,
            },
        )
        .map_err(Into::into)
    }
}

/// Exact historical attribution object set admitted as diagnostic planner
/// evidence. It is intentionally not a causal claim.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FeedbackAttributionManifestRef {
    pub job_id: ResearchJobId,
    pub artifact: ResearchJobArtifactRef,
    pub use_set_hash: ContentHash,
    pub produced_set_hash: ContentHash,
}

/// Exact Drift predecessor used to evaluate catalog trigger compatibility.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FeedbackRecipeDriftManifest {
    pub job_id: ResearchJobId,
    pub artifact: ResearchJobArtifactRef,
    pub exceeded_metrics: Vec<FeedbackDriftMetric>,
}

/// Frozen input for one durable recipe-planning job.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CandidateRecipePlanJobParams {
    pub feedback_cycle_id: FeedbackCycleId,
    pub cycle_idempotency_hash: ContentHash,
    pub artifact_id: CandidateRecipePlanArtifactId,
    pub label_cutoff: DateTime<Utc>,
    pub planned_at: DateTime<Utc>,
    pub evaluation_mode: FeedbackEvaluationMode,
    pub attribution: FeedbackAttributionManifestRef,
    pub drift: FeedbackRecipeDriftManifest,
    pub max_challengers: u32,
}

/// Complete owned predecessor set used to create one recipe-planning job.
pub struct CandidateRecipePlanInput {
    pub feedback_cycle_id: FeedbackCycleId,
    pub cycle_idempotency_hash: ContentHash,
    pub label_cutoff: DateTime<Utc>,
    pub planned_at: DateTime<Utc>,
    pub evaluation_mode: FeedbackEvaluationMode,
    pub attribution: FeedbackAttributionManifestRef,
    pub drift: FeedbackRecipeDriftManifest,
    pub max_challengers: u32,
}

impl CandidateRecipePlanJobParams {
    pub fn try_new(input: CandidateRecipePlanInput) -> Result<Self, FeedbackError> {
        let params = Self {
            feedback_cycle_id: input.feedback_cycle_id,
            cycle_idempotency_hash: input.cycle_idempotency_hash,
            artifact_id: CandidateRecipePlanArtifactId::from_cycle_id(input.feedback_cycle_id),
            label_cutoff: input.label_cutoff,
            planned_at: input.planned_at,
            evaluation_mode: input.evaluation_mode,
            attribution: input.attribution,
            drift: input.drift,
            max_challengers: input.max_challengers,
        };
        params.validate()?;
        Ok(params)
    }

    pub fn validate(&self) -> Result<(), FeedbackError> {
        if FeedbackCycleId::from_idempotency_hash(&self.cycle_idempotency_hash)
            != self.feedback_cycle_id
            || self.artifact_id
                != CandidateRecipePlanArtifactId::from_cycle_id(self.feedback_cycle_id)
            || self.label_cutoff.timestamp_millis() <= 0
            || self.planned_at < self.label_cutoff
            || self.max_challengers == 0
            || (self.evaluation_mode == FeedbackEvaluationMode::Conditional
                && self.drift.exceeded_metrics.is_empty())
        {
            return Err(invalid_recipe(
                "recipe-plan cycle, artifact, trigger, or challenger bound is invalid",
            ));
        }
        Ok(())
    }

    pub fn input_hash(&self) -> Result<ContentHash, FeedbackError> {
        self.validate()?;
        CanonicalDigest::content_hash_typed(RECIPE_PLAN_DOMAIN, RECIPE_PLAN_VERSION, self)
            .map_err(Into::into)
    }
}

/// One exact historical attribution payload that satisfied a catalog
/// diagnostic rule. Feature names are associations observed in the immutable
/// explanation/replay payload, not causal effects.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FeedbackRecipeDiagnosticEvidence {
    pub source_feedback_cycle_id: FeedbackCycleId,
    pub artifact_kind: AttributionArtifactKind,
    pub source_cohort: AttributionCohort,
    pub artifact_hash: ContentHash,
    pub available_at: DateTime<Utc>,
    pub matched_feature_names: Vec<String>,
}

impl FeedbackRecipeDiagnosticEvidence {
    pub fn validate(&self) -> Result<(), FeedbackError> {
        if self.available_at.timestamp_millis() <= 0
            || self.matched_feature_names.windows(2).any(|pair| {
                pair[0] >= pair[1]
                    || pair[0].is_empty()
                    || pair[0] != pair[0].trim()
                    || pair[0].chars().any(char::is_control)
            })
            || self.matched_feature_names.last().is_some_and(|name| {
                name.is_empty() || name != name.trim() || name.chars().any(char::is_control)
            })
        {
            return Err(invalid_recipe(
                "recipe diagnostic evidence timeline or feature set is invalid",
            ));
        }
        Ok(())
    }
}

/// Exact earlier same-template comparison admitted to planner ordering.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FeedbackRecipeOosEvidence {
    pub source_feedback_cycle_id: FeedbackCycleId,
    pub recipe_plan_artifact_hash: ContentHash,
    pub comparison_artifact_hash: ContentHash,
    pub candidate_recipe_hash: ContentHash,
    pub simultaneous_lower_bound_bps: Bps,
    pub available_at: DateTime<Utc>,
}

/// Conservative aggregation method. A new template revision starts a new
/// evidence series instead of silently inheriting incomparable trials.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FeedbackRecipeOosAggregation {
    MinimumExactTemplateRevision,
}

/// Canonical historical OOS summary used by stable recipe ordering.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FeedbackRecipeOosSummary {
    pub aggregation: FeedbackRecipeOosAggregation,
    pub evidence: Vec<FeedbackRecipeOosEvidence>,
    pub lower_bound_bps: Bps,
}

impl FeedbackRecipeOosSummary {
    pub fn try_new(mut evidence: Vec<FeedbackRecipeOosEvidence>) -> Result<Self, FeedbackError> {
        evidence.sort_by(|left, right| {
            left.available_at.cmp(&right.available_at).then_with(|| {
                left.source_feedback_cycle_id
                    .as_uuid()
                    .cmp(&right.source_feedback_cycle_id.as_uuid())
            })
        });
        let lower_bound_bps = evidence
            .iter()
            .map(|item| item.simultaneous_lower_bound_bps)
            .min()
            .ok_or_else(|| invalid_recipe("historical OOS summary requires evidence"))?;
        let summary = Self {
            aggregation: FeedbackRecipeOosAggregation::MinimumExactTemplateRevision,
            evidence,
            lower_bound_bps,
        };
        summary.validate()?;
        Ok(summary)
    }

    pub fn validate(&self) -> Result<(), FeedbackError> {
        let canonical = !self.evidence.is_empty()
            && self.evidence.windows(2).all(|pair| {
                (
                    pair[0].available_at,
                    pair[0].source_feedback_cycle_id.as_uuid(),
                ) < (
                    pair[1].available_at,
                    pair[1].source_feedback_cycle_id.as_uuid(),
                )
            });
        let exact_lower = self
            .evidence
            .iter()
            .map(|item| item.simultaneous_lower_bound_bps)
            .min()
            == Some(self.lower_bound_bps);
        if self.aggregation != FeedbackRecipeOosAggregation::MinimumExactTemplateRevision
            || !canonical
            || !exact_lower
            || self
                .evidence
                .iter()
                .any(|item| item.available_at.timestamp_millis() <= 0)
        {
            return Err(invalid_recipe(
                "historical OOS evidence order, aggregation, or lower bound is invalid",
            ));
        }
        Ok(())
    }
}

/// Stable selection evidence for one catalog challenger.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CandidateRecipeSelection {
    pub template: FeedbackRecipeTemplate,
    pub candidate_recipe_hash: ContentHash,
    pub planner_evidence_hash: ContentHash,
    pub matched_triggers: Vec<FeedbackDriftMetric>,
    pub diagnostic_evidence: Vec<FeedbackRecipeDiagnosticEvidence>,
    pub historical_oos: Option<FeedbackRecipeOosSummary>,
}

impl CandidateRecipeSelection {
    pub fn try_new(
        template: FeedbackRecipeTemplate,
        candidate_recipe_hash: ContentHash,
        attribution_use_set_hash: ContentHash,
        mut matched_triggers: Vec<FeedbackDriftMetric>,
        mut diagnostic_evidence: Vec<FeedbackRecipeDiagnosticEvidence>,
        historical_oos: Option<FeedbackRecipeOosSummary>,
    ) -> Result<Self, FeedbackError> {
        matched_triggers.sort_by_key(|trigger| trigger.as_str());
        matched_triggers.dedup();
        diagnostic_evidence.sort_by_key(|evidence| evidence.artifact_hash);
        let planner_evidence_hash = Self::planner_evidence_hash(
            template.template_hash,
            attribution_use_set_hash,
            &matched_triggers,
            &diagnostic_evidence,
            &historical_oos,
        )?;
        let selection = Self {
            template,
            candidate_recipe_hash,
            planner_evidence_hash,
            matched_triggers,
            diagnostic_evidence,
            historical_oos,
        };
        selection.validate(attribution_use_set_hash)?;
        Ok(selection)
    }

    pub fn validate(&self, attribution_use_set_hash: ContentHash) -> Result<(), FeedbackError> {
        self.template.validate()?;
        let triggers_canonical = self
            .matched_triggers
            .windows(2)
            .all(|pair| pair[0].as_str() < pair[1].as_str());
        let evidence_canonical = !self.diagnostic_evidence.is_empty()
            && self
                .diagnostic_evidence
                .windows(2)
                .all(|pair| pair[0].artifact_hash < pair[1].artifact_hash)
            && self
                .diagnostic_evidence
                .iter()
                .all(|evidence| evidence.validate().is_ok());
        let accepted_kinds = &self.template.diagnostic_spec.accepted_artifact_kinds;
        let responsive_features = &self.template.diagnostic_spec.responsive_feature_names;
        let mut matched_features = BTreeSet::new();
        let diagnostic_contract_exact = self.diagnostic_evidence.iter().all(|evidence| {
            let features_exact = if responsive_features.is_empty() {
                evidence.matched_feature_names.is_empty()
            } else {
                !evidence.matched_feature_names.is_empty()
                    && evidence
                        .matched_feature_names
                        .iter()
                        .all(|name| responsive_features.binary_search(name).is_ok())
            };
            matched_features.extend(evidence.matched_feature_names.iter().cloned());
            accepted_kinds.contains(&evidence.artifact_kind) && features_exact
        });
        let evidence_floor = u32::try_from(self.diagnostic_evidence.len())
            .ok()
            .is_some_and(|count| count >= self.template.diagnostic_spec.minimum_evidence_count);
        let feature_floor = u32::try_from(matched_features.len())
            .ok()
            .is_some_and(|count| count >= self.template.diagnostic_spec.minimum_feature_matches);
        let trigger_contract_exact = self
            .matched_triggers
            .iter()
            .all(|trigger| self.template.responsive_triggers.contains(trigger));
        let oos_valid = self
            .historical_oos
            .as_ref()
            .is_none_or(|summary| summary.validate().is_ok());
        if self.template.status != FeedbackRecipeTemplateStatus::Approved
            || !triggers_canonical
            || !evidence_canonical
            || !diagnostic_contract_exact
            || !evidence_floor
            || !feature_floor
            || !trigger_contract_exact
            || !oos_valid
            || self.planner_evidence_hash
                != Self::planner_evidence_hash(
                    self.template.template_hash,
                    attribution_use_set_hash,
                    &self.matched_triggers,
                    &self.diagnostic_evidence,
                    &self.historical_oos,
                )?
        {
            return Err(invalid_recipe(
                "candidate selection trigger, diagnostic, OOS, or evidence hash is invalid",
            ));
        }
        Ok(())
    }

    pub fn planner_evidence_hash(
        template_hash: ContentHash,
        attribution_use_set_hash: ContentHash,
        matched_triggers: &[FeedbackDriftMetric],
        diagnostic_evidence: &[FeedbackRecipeDiagnosticEvidence],
        historical_oos: &Option<FeedbackRecipeOosSummary>,
    ) -> Result<ContentHash, FeedbackError> {
        CanonicalDigest::content_hash_typed(
            PLANNER_EVIDENCE_DOMAIN,
            SPEC_VERSION,
            &(
                SPEC_VERSION,
                template_hash,
                attribution_use_set_hash,
                matched_triggers,
                diagnostic_evidence,
                historical_oos,
            ),
        )
        .map_err(Into::into)
    }

    #[must_use]
    pub const fn historical_lower_bound(&self) -> Option<Bps> {
        match &self.historical_oos {
            Some(summary) => Some(summary.lower_bound_bps),
            None => None,
        }
    }

    fn precedes(&self, right: &Self) -> bool {
        let left_match = !self.matched_triggers.is_empty();
        let right_match = !right.matched_triggers.is_empty();
        if left_match != right_match {
            return left_match;
        }
        if self.historical_lower_bound() != right.historical_lower_bound() {
            return match (
                self.historical_lower_bound(),
                right.historical_lower_bound(),
            ) {
                (Some(left), Some(right)) => left > right,
                (Some(_), None) => true,
                (None, Some(_) | None) => false,
            };
        }
        if self.template.catalog_priority != right.template.catalog_priority {
            return self.template.catalog_priority < right.template.catalog_priority;
        }
        self.template.recipe_template_id < right.template.recipe_template_id
    }
}

/// Typed readiness reason for a safe no-action recipe-plan result.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CandidateRecipeReadinessBlocker {
    NoApprovedTemplate,
    NoTriggerCompatibleTemplate,
    NoDiagnosticCompatibleTemplate,
    ResourceBudgetUnsupported,
    CatalogRevisionStale,
    ShadowOccupied,
    RouteStateStale,
}

impl CandidateRecipeReadinessBlocker {
    #[must_use]
    pub const fn reason_code(self) -> &'static str {
        match self {
            Self::NoApprovedTemplate => "feedback_recipe_catalog_empty",
            Self::NoTriggerCompatibleTemplate => "feedback_recipe_trigger_unmatched",
            Self::NoDiagnosticCompatibleTemplate => "feedback_recipe_diagnostic_unmatched",
            Self::ResourceBudgetUnsupported => "feedback_recipe_budget_unsupported",
            Self::CatalogRevisionStale => "feedback_recipe_catalog_stale",
            Self::ShadowOccupied => "feedback_shadow_occupied",
            Self::RouteStateStale => "feedback_route_state_stale",
        }
    }
}

/// Immutable planner outcome. Only `Ready` may enter `DatasetSeal`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "outcome", rename_all = "snake_case", deny_unknown_fields)]
pub enum CandidateRecipePlanOutcome {
    Ready {
        candidate_family: Box<FeedbackCandidateFamily>,
        selections: Vec<CandidateRecipeSelection>,
    },
    NoAction {
        blocker: CandidateRecipeReadinessBlocker,
    },
}

/// Immutable recipe-plan artifact consumed by every downstream learning stage.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CandidateRecipePlanArtifact {
    pub format_version: u32,
    pub artifact_id: CandidateRecipePlanArtifactId,
    pub feedback_cycle_id: FeedbackCycleId,
    pub cycle_idempotency_hash: ContentHash,
    pub input_hash: ContentHash,
    pub label_cutoff: DateTime<Utc>,
    pub planned_at: DateTime<Utc>,
    pub evaluation_mode: FeedbackEvaluationMode,
    pub profile_ref: ResearchProfileRef,
    pub route: BuyModelRoute,
    pub model_family: ModelFamily,
    pub attribution: FeedbackAttributionManifestRef,
    pub drift: FeedbackRecipeDriftManifest,
    pub outcome: CandidateRecipePlanOutcome,
}

impl CandidateRecipePlanArtifact {
    pub const FORMAT_VERSION: u32 = RECIPE_PLAN_VERSION;

    pub fn validate(&self) -> Result<(), FeedbackError> {
        self.profile_ref
            .validate()
            .map_err(|error| invalid_recipe(error.to_string()))?;
        let outcome_valid = match &self.outcome {
            CandidateRecipePlanOutcome::Ready {
                candidate_family,
                selections,
            } => {
                candidate_family.validate().is_ok()
                    && !selections.is_empty()
                    && selections.len() == candidate_family.candidates().len()
                    && selections.iter().all(|selection| {
                        selection.template.profile_ref == self.profile_ref
                            && selection.template.route == self.route
                            && selection.template.model_family == self.model_family
                            && selection.template.training_spec.model_spec_id
                                == candidate_family.shared_evaluation().model_spec_id
                            && selection.template.training_spec.model_spec_definition_hash
                                == candidate_family
                                    .shared_evaluation()
                                    .model_spec_definition_hash
                            && (self.evaluation_mode == FeedbackEvaluationMode::ForcedRetraining
                                || !selection.matched_triggers.is_empty())
                            && selection.diagnostic_evidence.iter().all(|evidence| {
                                evidence.source_feedback_cycle_id != self.feedback_cycle_id
                                    && evidence.available_at <= self.label_cutoff
                            })
                            && selection.historical_oos.as_ref().is_none_or(|summary| {
                                summary.evidence.iter().all(|evidence| {
                                    evidence.source_feedback_cycle_id != self.feedback_cycle_id
                                        && evidence.available_at <= self.label_cutoff
                                })
                            })
                            && candidate_family
                                .candidate(selection.candidate_recipe_hash)
                                .is_some_and(|candidate| {
                                    candidate.recipe_template_hash()
                                        == selection.template.template_hash
                                        && candidate.planner_evidence_hash()
                                            == selection.planner_evidence_hash
                                })
                            && selection.validate(self.attribution.use_set_hash).is_ok()
                    })
                    && selections.windows(2).all(|pair| pair[0].precedes(&pair[1]))
            }
            CandidateRecipePlanOutcome::NoAction { .. } => true,
        };
        if self.format_version != Self::FORMAT_VERSION
            || self.artifact_id
                != CandidateRecipePlanArtifactId::from_cycle_id(self.feedback_cycle_id)
            || FeedbackCycleId::from_idempotency_hash(&self.cycle_idempotency_hash)
                != self.feedback_cycle_id
            || self.label_cutoff.timestamp_millis() <= 0
            || self.planned_at < self.label_cutoff
            || !outcome_valid
        {
            return Err(invalid_recipe(
                "recipe-plan identity, candidate family, or stable selection order is invalid",
            ));
        }
        Ok(())
    }

    #[must_use]
    pub fn candidate_family(&self) -> Option<&FeedbackCandidateFamily> {
        match &self.outcome {
            CandidateRecipePlanOutcome::Ready {
                candidate_family, ..
            } => Some(candidate_family.as_ref()),
            CandidateRecipePlanOutcome::NoAction { .. } => None,
        }
    }

    #[must_use]
    pub fn selections(&self) -> Option<&[CandidateRecipeSelection]> {
        match &self.outcome {
            CandidateRecipePlanOutcome::Ready { selections, .. } => Some(selections),
            CandidateRecipePlanOutcome::NoAction { .. } => None,
        }
    }

    #[must_use]
    pub const fn blocker(&self) -> Option<CandidateRecipeReadinessBlocker> {
        match self.outcome {
            CandidateRecipePlanOutcome::Ready { .. } => None,
            CandidateRecipePlanOutcome::NoAction { blocker } => Some(blocker),
        }
    }
}

/// Verified object-store result of one recipe-planning job.
pub struct CandidateRecipePlanExecutionResult {
    pub artifact_id: CandidateRecipePlanArtifactId,
    pub artifact: ResearchJobArtifactRef,
}

/// Offline recipe-planning execution boundary.
#[async_trait]
pub trait CandidateRecipePlanExecutionPort: Send + Sync {
    async fn plan_recipe(
        &self,
        params: CandidateRecipePlanJobParams,
        progress: Arc<dyn JobProgressSink>,
        cancel: CancellationToken,
    ) -> QuantResult<CandidateRecipePlanExecutionResult>;
}

fn invalid_recipe(detail: impl Into<String>) -> FeedbackError {
    FeedbackError::InvalidJobContract {
        detail: detail.into(),
    }
}
