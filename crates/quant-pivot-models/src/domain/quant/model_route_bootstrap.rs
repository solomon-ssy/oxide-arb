//! Dedicated first-champion route bootstrap contracts.
//!
//! Bootstrap is deliberately not a compatibility form of promotion. It may
//! only fill one previously empty Pooled, Crypto, or Weather Buy route, runs under
//! `ReportOnly`, and seals the complete model, validation, parity, policy, and
//! actor preimage into one content-addressed transaction record.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use super::{CandidateExplanationValidation, ModelVersionInfo, PromotionPermitActor};
use crate::{
    enums::{model::ModelFamily, quant::QuantRuntimeMode},
    hashing::CanonicalDigest,
    runtime_config::{
        ActivePolicyBundle, BuyModelRoute, BuyRouteBinding, DecisionPolicySnapshot,
        DecisionPolicySnapshotDocument, ModelBinding, ModelBindingSource,
    },
    types::{
        AuditEventId, BacktestPathSetId, BacktestReportId, CalibrationArtifactId, ContentHash,
        DecisionPolicySnapshotId, FeatureParityRunId, FeatureParityStateId, ModelGovernanceAuditId,
        ModelSpecId, ModelVersionId, PolicyActivationId, PolicyApprovalId, PolicyBundleGeneration,
        PolicyIdempotencyKey, PolicyRevisionId, ResearchProfileRef, RoleCode, TrainingDatasetId,
        UserId,
        model_quality::{GateIntent, GateSubject, QualityGateReport},
    },
};
use quant_pivot_error::feedback::FeedbackError;

const BOOTSTRAP_MANIFEST_VERSION: u32 = 1;
const BOOTSTRAP_MANIFEST_DOMAIN: &str = "quant-pivot/model-route-bootstrap-manifest";
const BOOTSTRAP_PREFLIGHT_VERSION: u32 = 1;
const BOOTSTRAP_PREFLIGHT_DOMAIN: &str = "quant-pivot/model-route-bootstrap-preflight";
const BOOTSTRAP_NON_ROUTE_VERSION: u32 = 1;
const BOOTSTRAP_NON_ROUTE_DOMAIN: &str = "quant-pivot/model-route-bootstrap-non-route";
const BOOTSTRAP_TRANSACTION_VERSION: u32 = 1;
const BOOTSTRAP_TRANSACTION_DOMAIN: &str = "quant-pivot/model-route-bootstrap";
const MAX_ACTOR_BYTES: usize = 256;
const MAX_NOTE_BYTES: usize = 2_048;
const MAX_REASON_CODE_BYTES: usize = 128;
const MAX_ROLE_BYTES: usize = 64;

/// Complete immutable candidate plane required for a first champion.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ModelBootstrapManifest {
    format_version: u32,
    manifest_hash: ContentHash,
    model_version_id: ModelVersionId,
    model_spec_id: ModelSpecId,
    model_family: ModelFamily,
    model_spec_definition_hash: ContentHash,
    model_artifact_hash: ContentHash,
    serving_contract_hash: ContentHash,
    training_dataset_id: TrainingDatasetId,
    training_dataset_hash: ContentHash,
    dataset_manifest_hash: ContentHash,
    dataset_artifact_hash: ContentHash,
    feature_schema_hash: ContentHash,
    input_contract_hash: ContentHash,
    input_transform_hash: ContentHash,
    calibration_artifact_id: Option<CalibrationArtifactId>,
    calibration_artifact_hash: Option<ContentHash>,
    profile_ref: ResearchProfileRef,
    route: BuyModelRoute,
    cpcv_path_set_id: BacktestPathSetId,
    cpcv_path_set_hash: ContentHash,
    backtest_report_id: BacktestReportId,
    backtest_report_hash: ContentHash,
    explanation_validation: CandidateExplanationValidation,
    quality_gate_report: QualityGateReport,
    feature_parity_run_id: FeatureParityRunId,
    feature_parity_state_id: FeatureParityStateId,
    feature_parity_evidence_hash: ContentHash,
}

#[derive(Serialize)]
struct BootstrapManifestPreimage<'a> {
    format_version: u32,
    model_version_id: ModelVersionId,
    model_spec_id: ModelSpecId,
    model_family: ModelFamily,
    model_spec_definition_hash: ContentHash,
    model_artifact_hash: ContentHash,
    serving_contract_hash: ContentHash,
    training_dataset_id: TrainingDatasetId,
    training_dataset_hash: ContentHash,
    dataset_manifest_hash: ContentHash,
    dataset_artifact_hash: ContentHash,
    feature_schema_hash: ContentHash,
    input_contract_hash: ContentHash,
    input_transform_hash: ContentHash,
    calibration_artifact_id: Option<CalibrationArtifactId>,
    calibration_artifact_hash: Option<ContentHash>,
    profile_ref: &'a ResearchProfileRef,
    route: BuyModelRoute,
    cpcv_path_set_id: BacktestPathSetId,
    cpcv_path_set_hash: ContentHash,
    backtest_report_id: BacktestReportId,
    backtest_report_hash: ContentHash,
    explanation_validation: &'a CandidateExplanationValidation,
    quality_gate_report: &'a QualityGateReport,
    feature_parity_run_id: FeatureParityRunId,
    feature_parity_state_id: FeatureParityStateId,
    feature_parity_evidence_hash: ContentHash,
}

/// Server-derived inputs for one bootstrap candidate manifest.
pub struct ModelBootstrapManifestInput {
    pub model_version_id: ModelVersionId,
    pub model_spec_id: ModelSpecId,
    pub model_family: ModelFamily,
    pub model_spec_definition_hash: ContentHash,
    pub model_artifact_hash: ContentHash,
    pub serving_contract_hash: ContentHash,
    pub training_dataset_id: TrainingDatasetId,
    pub training_dataset_hash: ContentHash,
    pub dataset_manifest_hash: ContentHash,
    pub dataset_artifact_hash: ContentHash,
    pub feature_schema_hash: ContentHash,
    pub input_contract_hash: ContentHash,
    pub input_transform_hash: ContentHash,
    pub calibration_artifact_id: Option<CalibrationArtifactId>,
    pub calibration_artifact_hash: Option<ContentHash>,
    pub profile_ref: ResearchProfileRef,
    pub route: BuyModelRoute,
    pub cpcv_path_set_id: BacktestPathSetId,
    pub cpcv_path_set_hash: ContentHash,
    pub backtest_report_id: BacktestReportId,
    pub backtest_report_hash: ContentHash,
    pub explanation_validation: CandidateExplanationValidation,
    pub quality_gate_report: QualityGateReport,
    pub feature_parity_run_id: FeatureParityRunId,
    pub feature_parity_state_id: FeatureParityStateId,
    pub feature_parity_evidence_hash: ContentHash,
}

impl ModelBootstrapManifest {
    pub fn try_seal(input: ModelBootstrapManifestInput) -> Result<Self, FeedbackError> {
        let manifest_hash = Self::derive_hash(&BootstrapManifestPreimage {
            format_version: BOOTSTRAP_MANIFEST_VERSION,
            model_version_id: input.model_version_id,
            model_spec_id: input.model_spec_id,
            model_family: input.model_family,
            model_spec_definition_hash: input.model_spec_definition_hash,
            model_artifact_hash: input.model_artifact_hash,
            serving_contract_hash: input.serving_contract_hash,
            training_dataset_id: input.training_dataset_id,
            training_dataset_hash: input.training_dataset_hash,
            dataset_manifest_hash: input.dataset_manifest_hash,
            dataset_artifact_hash: input.dataset_artifact_hash,
            feature_schema_hash: input.feature_schema_hash,
            input_contract_hash: input.input_contract_hash,
            input_transform_hash: input.input_transform_hash,
            calibration_artifact_id: input.calibration_artifact_id,
            calibration_artifact_hash: input.calibration_artifact_hash,
            profile_ref: &input.profile_ref,
            route: input.route,
            cpcv_path_set_id: input.cpcv_path_set_id,
            cpcv_path_set_hash: input.cpcv_path_set_hash,
            backtest_report_id: input.backtest_report_id,
            backtest_report_hash: input.backtest_report_hash,
            explanation_validation: &input.explanation_validation,
            quality_gate_report: &input.quality_gate_report,
            feature_parity_run_id: input.feature_parity_run_id,
            feature_parity_state_id: input.feature_parity_state_id,
            feature_parity_evidence_hash: input.feature_parity_evidence_hash,
        })?;
        let manifest = Self {
            format_version: BOOTSTRAP_MANIFEST_VERSION,
            manifest_hash,
            model_version_id: input.model_version_id,
            model_spec_id: input.model_spec_id,
            model_family: input.model_family,
            model_spec_definition_hash: input.model_spec_definition_hash,
            model_artifact_hash: input.model_artifact_hash,
            serving_contract_hash: input.serving_contract_hash,
            training_dataset_id: input.training_dataset_id,
            training_dataset_hash: input.training_dataset_hash,
            dataset_manifest_hash: input.dataset_manifest_hash,
            dataset_artifact_hash: input.dataset_artifact_hash,
            feature_schema_hash: input.feature_schema_hash,
            input_contract_hash: input.input_contract_hash,
            input_transform_hash: input.input_transform_hash,
            calibration_artifact_id: input.calibration_artifact_id,
            calibration_artifact_hash: input.calibration_artifact_hash,
            profile_ref: input.profile_ref,
            route: input.route,
            cpcv_path_set_id: input.cpcv_path_set_id,
            cpcv_path_set_hash: input.cpcv_path_set_hash,
            backtest_report_id: input.backtest_report_id,
            backtest_report_hash: input.backtest_report_hash,
            explanation_validation: input.explanation_validation,
            quality_gate_report: input.quality_gate_report,
            feature_parity_run_id: input.feature_parity_run_id,
            feature_parity_state_id: input.feature_parity_state_id,
            feature_parity_evidence_hash: input.feature_parity_evidence_hash,
        };
        manifest.validate()?;
        Ok(manifest)
    }

    pub fn validate(&self) -> Result<(), FeedbackError> {
        self.profile_ref
            .validate()
            .map_err(|error| invalid_bootstrap(error.to_string()))?;
        self.explanation_validation
            .validate()
            .map_err(|error| invalid_bootstrap(error.to_string()))?;
        self.quality_gate_report
            .validate()
            .map_err(|error| invalid_bootstrap(error.to_string()))?;
        let profile = self
            .profile_ref
            .resolve_builtin_research_profile()
            .map_err(invalid_bootstrap)?;
        let valid = self.format_version == BOOTSTRAP_MANIFEST_VERSION
            && profile.spec.category == self.route.category()
            && matches!(
                self.model_family,
                ModelFamily::WeightedFactor | ModelFamily::ClassicalGradientBoostedTrees
            )
            && self.calibration_artifact_id.is_some()
            && self.calibration_artifact_id.is_some() == self.calibration_artifact_hash.is_some()
            && self.explanation_validation.input_contract_hash == self.input_contract_hash
            && self.quality_gate_report.intent == GateIntent::Candidate
            && self.quality_gate_report.subject == GateSubject::ModelVersion(self.model_version_id)
            && self.quality_gate_report.passed
            && self.manifest_hash == Self::derive_hash(&self.preimage())?;
        if !valid {
            return Err(invalid_bootstrap(
                "bootstrap manifest has invalid scope, evidence, calibration, or hash",
            ));
        }
        Ok(())
    }

    pub fn validate_model(&self, model: &ModelVersionInfo) -> Result<(), FeedbackError> {
        self.validate()?;
        let bindings = model
            .verified_serving_contract()
            .map_err(|error| invalid_bootstrap(error.to_string()))?
            .bindings();
        let calibration = bindings
            .model
            .calibration
            .as_ref()
            .map(|binding| (binding.artifact_id, binding.content_hash));
        let model_artifact_matches = model.artifact_hash == self.model_artifact_hash;
        let exact = model.model_version_id == self.model_version_id
            && model.model_spec_id == self.model_spec_id
            && model.model_family == self.model_family
            && model.model_spec_definition_hash == self.model_spec_definition_hash
            && model_artifact_matches
            && model.serving_contract_hash == self.serving_contract_hash
            && model.training_dataset_id == Some(self.training_dataset_id)
            && model.profile_ref == self.profile_ref
            && model.category_scope == self.route.category()
            && bindings.transform.training_dataset_hash == self.training_dataset_hash
            && bindings.dataset.manifest_hash == self.dataset_manifest_hash
            && bindings.dataset.artifact_bytes_hash == self.dataset_artifact_hash
            && bindings.schemas.feature_schema_hash == self.feature_schema_hash
            && bindings.transform.input_contract_hash == self.input_contract_hash
            && bindings.transform.input_transform_hash == self.input_transform_hash
            && calibration
                == self
                    .calibration_artifact_id
                    .zip(self.calibration_artifact_hash);
        if !exact {
            return Err(invalid_bootstrap(
                "bootstrap model differs from its sealed candidate manifest",
            ));
        }
        Ok(())
    }

    const fn preimage(&self) -> BootstrapManifestPreimage<'_> {
        BootstrapManifestPreimage {
            format_version: self.format_version,
            model_version_id: self.model_version_id,
            model_spec_id: self.model_spec_id,
            model_family: self.model_family,
            model_spec_definition_hash: self.model_spec_definition_hash,
            model_artifact_hash: self.model_artifact_hash,
            serving_contract_hash: self.serving_contract_hash,
            training_dataset_id: self.training_dataset_id,
            training_dataset_hash: self.training_dataset_hash,
            dataset_manifest_hash: self.dataset_manifest_hash,
            dataset_artifact_hash: self.dataset_artifact_hash,
            feature_schema_hash: self.feature_schema_hash,
            input_contract_hash: self.input_contract_hash,
            input_transform_hash: self.input_transform_hash,
            calibration_artifact_id: self.calibration_artifact_id,
            calibration_artifact_hash: self.calibration_artifact_hash,
            profile_ref: &self.profile_ref,
            route: self.route,
            cpcv_path_set_id: self.cpcv_path_set_id,
            cpcv_path_set_hash: self.cpcv_path_set_hash,
            backtest_report_id: self.backtest_report_id,
            backtest_report_hash: self.backtest_report_hash,
            explanation_validation: &self.explanation_validation,
            quality_gate_report: &self.quality_gate_report,
            feature_parity_run_id: self.feature_parity_run_id,
            feature_parity_state_id: self.feature_parity_state_id,
            feature_parity_evidence_hash: self.feature_parity_evidence_hash,
        }
    }

    fn derive_hash(preimage: &BootstrapManifestPreimage<'_>) -> Result<ContentHash, FeedbackError> {
        CanonicalDigest::content_hash_typed(
            BOOTSTRAP_MANIFEST_DOMAIN,
            BOOTSTRAP_MANIFEST_VERSION,
            preimage,
        )
        .map_err(Into::into)
    }

    #[must_use]
    pub const fn manifest_hash(&self) -> ContentHash {
        self.manifest_hash
    }

    #[must_use]
    pub const fn model_version_id(&self) -> ModelVersionId {
        self.model_version_id
    }

    #[must_use]
    pub const fn model_spec_id(&self) -> ModelSpecId {
        self.model_spec_id
    }

    #[must_use]
    pub const fn model_spec_hash(&self) -> ContentHash {
        self.model_spec_definition_hash
    }

    #[must_use]
    pub const fn model_artifact_hash(&self) -> ContentHash {
        self.model_artifact_hash
    }

    #[must_use]
    pub const fn serving_contract_hash(&self) -> ContentHash {
        self.serving_contract_hash
    }

    #[must_use]
    pub const fn training_dataset_id(&self) -> TrainingDatasetId {
        self.training_dataset_id
    }

    #[must_use]
    pub const fn profile_ref(&self) -> &ResearchProfileRef {
        &self.profile_ref
    }

    #[must_use]
    pub const fn route(&self) -> BuyModelRoute {
        self.route
    }

    #[must_use]
    pub const fn cpcv_path_set_id(&self) -> BacktestPathSetId {
        self.cpcv_path_set_id
    }

    #[must_use]
    pub const fn cpcv_path_set_hash(&self) -> ContentHash {
        self.cpcv_path_set_hash
    }

    #[must_use]
    pub const fn backtest_report_id(&self) -> BacktestReportId {
        self.backtest_report_id
    }

    #[must_use]
    pub const fn backtest_report_hash(&self) -> ContentHash {
        self.backtest_report_hash
    }

    #[must_use]
    pub const fn quality_gate_report(&self) -> &QualityGateReport {
        &self.quality_gate_report
    }

    #[must_use]
    pub const fn feature_parity_run_id(&self) -> FeatureParityRunId {
        self.feature_parity_run_id
    }

    #[must_use]
    pub const fn feature_parity_state_id(&self) -> FeatureParityStateId {
        self.feature_parity_state_id
    }

    #[must_use]
    pub const fn feature_parity_hash(&self) -> ContentHash {
        self.feature_parity_evidence_hash
    }
}

#[derive(Serialize)]
struct BootstrapNonRouteDocument<'a> {
    format_version: u32,
    route: BuyModelRoute,
    snapshot: &'a DecisionPolicySnapshotDocument,
}

/// Exact empty-route-to-first-champion policy projection.
#[derive(Debug, Clone)]
pub struct ModelBootstrapPolicyProjection {
    route: BuyModelRoute,
    model_version_id: ModelVersionId,
    non_route_policy_hash: ContentHash,
    prospective_snapshot: DecisionPolicySnapshot,
}

impl ModelBootstrapPolicyProjection {
    pub fn try_new(
        bundle: &ActivePolicyBundle,
        route: BuyModelRoute,
        model_version_id: ModelVersionId,
        bound_at: DateTime<Utc>,
    ) -> Result<Self, FeedbackError> {
        let actual_hash = bundle
            .snapshot
            .persistence_hash()
            .map_err(|error| invalid_bootstrap(error.to_string()))?;
        if actual_hash != bundle.snapshot_hash
            || bundle.decision_policy_snapshot_id
                != DecisionPolicySnapshotId::from_content_hash(&actual_hash)
            || bundle.revision_vector != bundle.snapshot.revisions
        {
            return Err(invalid_bootstrap(
                "active policy bundle identity, hash, or revision vector is invalid",
            ));
        }
        let model = &bundle.snapshot.model_routing.model;
        if model.champion(route).is_ok() {
            return Err(invalid_bootstrap(
                "bootstrap target already has a champion route",
            ));
        }
        let already_referenced = model.buy_routes.values().any(|binding| {
            binding.champion.model_version_id == model_version_id
                || binding
                    .shadow
                    .as_ref()
                    .is_some_and(|shadow| shadow.model_version_id == model_version_id)
        }) || model
            .active_exit_model_version_id
            .as_ref()
            .is_some_and(|reference| reference.id == model_version_id);
        if already_referenced {
            return Err(invalid_bootstrap(
                "bootstrap candidate is already referenced by another serving route",
            ));
        }
        let non_route_policy_hash = Self::project_hash(&bundle.snapshot, route)?;
        let mut prospective_snapshot = bundle.snapshot.clone();
        let config_revision = bundle
            .generation
            .checked_next()
            .map_err(|error| invalid_bootstrap(error.to_string()))?;
        prospective_snapshot.model_routing.model.buy_routes.insert(
            route,
            BuyRouteBinding {
                champion: ModelBinding::new(
                    model_version_id,
                    ModelBindingSource::Bootstrap,
                    bound_at,
                    config_revision,
                    1,
                ),
                shadow: None,
            },
        );
        if Self::project_hash(&prospective_snapshot, route)? != non_route_policy_hash {
            return Err(invalid_bootstrap(
                "bootstrap projection changed policy outside the target Buy route",
            ));
        }
        Ok(Self {
            route,
            model_version_id,
            non_route_policy_hash,
            prospective_snapshot,
        })
    }

    fn project_hash(
        snapshot: &DecisionPolicySnapshot,
        route: BuyModelRoute,
    ) -> Result<ContentHash, FeedbackError> {
        let mut document = snapshot
            .persistence_document()
            .map_err(|error| invalid_bootstrap(error.to_string()))?;
        document.model_routing.model.buy_routes.remove(&route);
        document.revisions.model_routing = None;
        CanonicalDigest::content_hash_typed(
            BOOTSTRAP_NON_ROUTE_DOMAIN,
            BOOTSTRAP_NON_ROUTE_VERSION,
            &BootstrapNonRouteDocument {
                format_version: BOOTSTRAP_NON_ROUTE_VERSION,
                route,
                snapshot: &document,
            },
        )
        .map_err(Into::into)
    }

    pub fn validate_candidate(
        &self,
        candidate: &DecisionPolicySnapshot,
    ) -> Result<(), FeedbackError> {
        let binding = candidate
            .model_routing
            .model
            .route_binding(self.route)
            .map_err(|error| invalid_bootstrap(error.to_string()))?;
        if binding.champion.model_version_id != self.model_version_id
            || binding.shadow.is_some()
            || Self::project_hash(candidate, self.route)? != self.non_route_policy_hash
        {
            return Err(invalid_bootstrap(
                "candidate snapshot differs from the exact bootstrap route delta",
            ));
        }
        Ok(())
    }

    #[must_use]
    pub const fn route(&self) -> BuyModelRoute {
        self.route
    }

    #[must_use]
    pub const fn model_version_id(&self) -> ModelVersionId {
        self.model_version_id
    }

    #[must_use]
    pub const fn non_route_policy_hash(&self) -> ContentHash {
        self.non_route_policy_hash
    }

    #[must_use]
    pub const fn prospective_snapshot(&self) -> &DecisionPolicySnapshot {
        &self.prospective_snapshot
    }
}

/// Complete side-effect-free bootstrap proof.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ModelRouteBootstrapPreflight {
    format_version: u32,
    preflight_hash: ContentHash,
    manifest: ModelBootstrapManifest,
    expected_policy_generation: PolicyBundleGeneration,
    expected_snapshot_id: DecisionPolicySnapshotId,
    expected_snapshot_hash: ContentHash,
    expected_model_routing_revision_id: PolicyRevisionId,
    expected_runtime_control_revision: i64,
    current_runtime_mode: QuantRuntimeMode,
    non_route_policy_hash: ContentHash,
    evaluated_at: DateTime<Utc>,
}

#[derive(Serialize)]
struct BootstrapPreflightPreimage<'a> {
    format_version: u32,
    manifest: &'a ModelBootstrapManifest,
    expected_policy_generation: PolicyBundleGeneration,
    expected_snapshot_id: DecisionPolicySnapshotId,
    expected_snapshot_hash: ContentHash,
    expected_model_routing_revision_id: PolicyRevisionId,
    expected_runtime_control_revision: i64,
    current_runtime_mode: QuantRuntimeMode,
    non_route_policy_hash: ContentHash,
    evaluated_at: DateTime<Utc>,
}

/// Server-resolved values used to seal one bootstrap preflight.
pub struct ModelRouteBootstrapPreflightInput {
    pub manifest: ModelBootstrapManifest,
    pub expected_policy_generation: PolicyBundleGeneration,
    pub expected_snapshot_id: DecisionPolicySnapshotId,
    pub expected_snapshot_hash: ContentHash,
    pub expected_model_routing_revision_id: PolicyRevisionId,
    pub expected_runtime_control_revision: i64,
    pub current_runtime_mode: QuantRuntimeMode,
    pub non_route_policy_hash: ContentHash,
    pub evaluated_at: DateTime<Utc>,
}

impl ModelRouteBootstrapPreflight {
    pub fn try_seal(input: ModelRouteBootstrapPreflightInput) -> Result<Self, FeedbackError> {
        let preflight_hash = Self::derive_hash(&BootstrapPreflightPreimage {
            format_version: BOOTSTRAP_PREFLIGHT_VERSION,
            manifest: &input.manifest,
            expected_policy_generation: input.expected_policy_generation,
            expected_snapshot_id: input.expected_snapshot_id,
            expected_snapshot_hash: input.expected_snapshot_hash,
            expected_model_routing_revision_id: input.expected_model_routing_revision_id,
            expected_runtime_control_revision: input.expected_runtime_control_revision,
            current_runtime_mode: input.current_runtime_mode,
            non_route_policy_hash: input.non_route_policy_hash,
            evaluated_at: input.evaluated_at,
        })?;
        let preflight = Self {
            format_version: BOOTSTRAP_PREFLIGHT_VERSION,
            preflight_hash,
            manifest: input.manifest,
            expected_policy_generation: input.expected_policy_generation,
            expected_snapshot_id: input.expected_snapshot_id,
            expected_snapshot_hash: input.expected_snapshot_hash,
            expected_model_routing_revision_id: input.expected_model_routing_revision_id,
            expected_runtime_control_revision: input.expected_runtime_control_revision,
            current_runtime_mode: input.current_runtime_mode,
            non_route_policy_hash: input.non_route_policy_hash,
            evaluated_at: input.evaluated_at,
        };
        preflight.validate()?;
        Ok(preflight)
    }

    pub fn validate(&self) -> Result<(), FeedbackError> {
        self.manifest.validate()?;
        let valid = self.format_version == BOOTSTRAP_PREFLIGHT_VERSION
            && self.expected_snapshot_id
                == DecisionPolicySnapshotId::from_content_hash(&self.expected_snapshot_hash)
            && self.expected_runtime_control_revision >= 0
            && self.current_runtime_mode == QuantRuntimeMode::ReportOnly
            && self.evaluated_at == self.manifest.quality_gate_report().evaluated_at
            && self.preflight_hash == Self::derive_hash(&self.preimage())?;
        if !valid {
            return Err(invalid_bootstrap(
                "bootstrap preflight has invalid policy, runtime, evaluation, or hash",
            ));
        }
        Ok(())
    }

    const fn preimage(&self) -> BootstrapPreflightPreimage<'_> {
        BootstrapPreflightPreimage {
            format_version: self.format_version,
            manifest: &self.manifest,
            expected_policy_generation: self.expected_policy_generation,
            expected_snapshot_id: self.expected_snapshot_id,
            expected_snapshot_hash: self.expected_snapshot_hash,
            expected_model_routing_revision_id: self.expected_model_routing_revision_id,
            expected_runtime_control_revision: self.expected_runtime_control_revision,
            current_runtime_mode: self.current_runtime_mode,
            non_route_policy_hash: self.non_route_policy_hash,
            evaluated_at: self.evaluated_at,
        }
    }

    fn derive_hash(
        preimage: &BootstrapPreflightPreimage<'_>,
    ) -> Result<ContentHash, FeedbackError> {
        CanonicalDigest::content_hash_typed(
            BOOTSTRAP_PREFLIGHT_DOMAIN,
            BOOTSTRAP_PREFLIGHT_VERSION,
            preimage,
        )
        .map_err(Into::into)
    }

    #[must_use]
    pub const fn preflight_hash(&self) -> ContentHash {
        self.preflight_hash
    }

    #[must_use]
    pub const fn manifest(&self) -> &ModelBootstrapManifest {
        &self.manifest
    }

    #[must_use]
    pub const fn expected_policy_generation(&self) -> PolicyBundleGeneration {
        self.expected_policy_generation
    }

    #[must_use]
    pub const fn expected_snapshot_id(&self) -> DecisionPolicySnapshotId {
        self.expected_snapshot_id
    }

    #[must_use]
    pub const fn expected_snapshot_hash(&self) -> ContentHash {
        self.expected_snapshot_hash
    }

    #[must_use]
    pub const fn expected_route_revision(&self) -> PolicyRevisionId {
        self.expected_model_routing_revision_id
    }

    #[must_use]
    pub const fn expected_runtime_revision(&self) -> i64 {
        self.expected_runtime_control_revision
    }

    #[must_use]
    pub const fn current_runtime_mode(&self) -> QuantRuntimeMode {
        self.current_runtime_mode
    }

    #[must_use]
    pub const fn non_route_policy_hash(&self) -> ContentHash {
        self.non_route_policy_hash
    }
}

/// Authenticated first-champion intent. Route and all evidence are derived.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BootstrapModelRoute {
    pub model_version_id: ModelVersionId,
    pub expected_policy_generation: PolicyBundleGeneration,
    pub expected_runtime_control_revision: i64,
    pub idempotency_key: PolicyIdempotencyKey,
    pub actor: PromotionPermitActor,
    pub reason_code: String,
    pub note: String,
}

impl BootstrapModelRoute {
    pub fn validate(&self) -> Result<(), FeedbackError> {
        if self.expected_runtime_control_revision < 0 {
            return Err(invalid_bootstrap(
                "expected runtime-control revision cannot be negative",
            ));
        }
        validate_role_note(&self.actor.acting_role, &self.note)?;
        validate_reason_code(&self.reason_code)
    }
}

/// Repository command binding client intent to server-derived preflight.
#[derive(Debug, Clone)]
pub struct CommitModelRouteBootstrap {
    request: BootstrapModelRoute,
    preflight: ModelRouteBootstrapPreflight,
}

impl CommitModelRouteBootstrap {
    pub fn try_new(
        request: BootstrapModelRoute,
        preflight: ModelRouteBootstrapPreflight,
    ) -> Result<Self, FeedbackError> {
        request.validate()?;
        preflight.validate()?;
        if request.model_version_id != preflight.manifest().model_version_id()
            || request.expected_policy_generation != preflight.expected_policy_generation()
            || request.expected_runtime_control_revision != preflight.expected_runtime_revision()
        {
            return Err(invalid_bootstrap(
                "bootstrap request differs from the server-derived preflight",
            ));
        }
        Ok(Self { request, preflight })
    }

    #[must_use]
    pub const fn request(&self) -> &BootstrapModelRoute {
        &self.request
    }

    #[must_use]
    pub const fn preflight(&self) -> &ModelRouteBootstrapPreflight {
        &self.preflight
    }
}

/// Exact route identity committed by one bootstrap.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ModelRouteBootstrapRoute {
    pub route: BuyModelRoute,
    pub model_version_id: ModelVersionId,
    pub model_artifact_hash: ContentHash,
    pub serving_contract_hash: ContentHash,
}

/// Old/new policy identities of one bootstrap transaction.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ModelRouteBootstrapPolicy {
    pub previous_generation: PolicyBundleGeneration,
    pub transaction_revision: PolicyBundleGeneration,
    pub previous_snapshot_id: DecisionPolicySnapshotId,
    pub previous_snapshot_hash: ContentHash,
    pub committed_snapshot_id: DecisionPolicySnapshotId,
    pub committed_snapshot_hash: ContentHash,
    pub previous_model_routing_revision_id: PolicyRevisionId,
    pub committed_model_routing_revision_id: PolicyRevisionId,
    pub policy_approval_id: PolicyApprovalId,
    pub policy_activation_id: PolicyActivationId,
}

#[derive(Serialize)]
struct BootstrapTransactionPreimage<'a> {
    format_version: u32,
    preflight: &'a ModelRouteBootstrapPreflight,
    actor_user_id: UserId,
    actor_username: &'a str,
    actor_role: &'a RoleCode,
    idempotency_key: &'a PolicyIdempotencyKey,
    reason_code: &'a str,
    note: &'a str,
    route: &'a ModelRouteBootstrapRoute,
    policy: &'a ModelRouteBootstrapPolicy,
}

/// Inputs jointly sealed into the bootstrap audit record.
pub struct ModelRouteBootstrapRecordInput {
    pub preflight: ModelRouteBootstrapPreflight,
    pub actor_user_id: UserId,
    pub actor_username: String,
    pub actor_role: RoleCode,
    pub idempotency_key: PolicyIdempotencyKey,
    pub reason_code: String,
    pub note: String,
    pub route: ModelRouteBootstrapRoute,
    pub policy: ModelRouteBootstrapPolicy,
}

/// Complete content-addressed bootstrap graph.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ModelRouteBootstrapRecord {
    format_version: u32,
    transaction_hash: ContentHash,
    preflight: ModelRouteBootstrapPreflight,
    actor_user_id: UserId,
    actor_username: String,
    actor_role: RoleCode,
    idempotency_key: PolicyIdempotencyKey,
    reason_code: String,
    note: String,
    route: ModelRouteBootstrapRoute,
    policy: ModelRouteBootstrapPolicy,
}

impl ModelRouteBootstrapRecord {
    pub fn try_seal(input: ModelRouteBootstrapRecordInput) -> Result<Self, FeedbackError> {
        let transaction_hash = Self::derive_hash(&BootstrapTransactionPreimage {
            format_version: BOOTSTRAP_TRANSACTION_VERSION,
            preflight: &input.preflight,
            actor_user_id: input.actor_user_id,
            actor_username: &input.actor_username,
            actor_role: &input.actor_role,
            idempotency_key: &input.idempotency_key,
            reason_code: &input.reason_code,
            note: &input.note,
            route: &input.route,
            policy: &input.policy,
        })?;
        let record = Self {
            format_version: BOOTSTRAP_TRANSACTION_VERSION,
            transaction_hash,
            preflight: input.preflight,
            actor_user_id: input.actor_user_id,
            actor_username: input.actor_username,
            actor_role: input.actor_role,
            idempotency_key: input.idempotency_key,
            reason_code: input.reason_code,
            note: input.note,
            route: input.route,
            policy: input.policy,
        };
        record.validate()?;
        Ok(record)
    }

    pub fn validate(&self) -> Result<(), FeedbackError> {
        self.preflight.validate()?;
        validate_actor(&self.actor_username, &self.actor_role, &self.note)?;
        validate_reason_code(&self.reason_code)?;
        let manifest = self.preflight.manifest();
        let next = self
            .policy
            .previous_generation
            .checked_next()
            .map_err(|error| invalid_bootstrap(error.to_string()))?;
        let valid = self.format_version == BOOTSTRAP_TRANSACTION_VERSION
            && self.route.route == manifest.route()
            && self.route.model_version_id == manifest.model_version_id()
            && self.route.model_artifact_hash == manifest.model_artifact_hash()
            && self.route.serving_contract_hash == manifest.serving_contract_hash()
            && self.policy.previous_generation == self.preflight.expected_policy_generation()
            && self.policy.transaction_revision == next
            && self.policy.previous_snapshot_id == self.preflight.expected_snapshot_id()
            && self.policy.previous_snapshot_hash == self.preflight.expected_snapshot_hash()
            && self.policy.previous_model_routing_revision_id
                == self.preflight.expected_route_revision()
            && self.transaction_hash == Self::derive_hash(&self.preimage())?;
        if !valid {
            return Err(invalid_bootstrap(
                "bootstrap transaction record differs from its sealed route or policy preflight",
            ));
        }
        Ok(())
    }

    fn preimage(&self) -> BootstrapTransactionPreimage<'_> {
        BootstrapTransactionPreimage {
            format_version: self.format_version,
            preflight: &self.preflight,
            actor_user_id: self.actor_user_id,
            actor_username: &self.actor_username,
            actor_role: &self.actor_role,
            idempotency_key: &self.idempotency_key,
            reason_code: &self.reason_code,
            note: &self.note,
            route: &self.route,
            policy: &self.policy,
        }
    }

    fn derive_hash(
        preimage: &BootstrapTransactionPreimage<'_>,
    ) -> Result<ContentHash, FeedbackError> {
        CanonicalDigest::content_hash_typed(
            BOOTSTRAP_TRANSACTION_DOMAIN,
            BOOTSTRAP_TRANSACTION_VERSION,
            preimage,
        )
        .map_err(Into::into)
    }

    #[must_use]
    pub const fn transaction_hash(&self) -> ContentHash {
        self.transaction_hash
    }

    #[must_use]
    pub const fn preflight(&self) -> &ModelRouteBootstrapPreflight {
        &self.preflight
    }

    #[must_use]
    pub const fn actor_user_id(&self) -> UserId {
        self.actor_user_id
    }

    #[must_use]
    pub fn actor_username(&self) -> &str {
        &self.actor_username
    }

    #[must_use]
    pub const fn actor_role(&self) -> &RoleCode {
        &self.actor_role
    }

    #[must_use]
    pub const fn idempotency_key(&self) -> &PolicyIdempotencyKey {
        &self.idempotency_key
    }

    #[must_use]
    pub fn reason_code(&self) -> &str {
        &self.reason_code
    }

    #[must_use]
    pub fn note(&self) -> &str {
        &self.note
    }

    #[must_use]
    pub const fn route(&self) -> &ModelRouteBootstrapRoute {
        &self.route
    }

    #[must_use]
    pub const fn policy(&self) -> &ModelRouteBootstrapPolicy {
        &self.policy
    }

    #[must_use]
    pub fn audit_reason(&self) -> String {
        format!("{}: {}", self.reason_code, self.note)
    }

    #[must_use]
    pub fn audit_id(&self) -> ModelGovernanceAuditId {
        ModelGovernanceAuditId::from_content_hash(&self.transaction_hash)
    }

    #[must_use]
    pub fn audit_event_id(&self) -> AuditEventId {
        AuditEventId::from_content_hash(&self.transaction_hash)
    }
}

fn validate_actor(username: &str, role: &RoleCode, note: &str) -> Result<(), FeedbackError> {
    if username.is_empty()
        || username.len() > MAX_ACTOR_BYTES
        || username != username.trim()
        || username.chars().any(char::is_control)
    {
        return Err(invalid_bootstrap(
            "actor username violates the governed text contract",
        ));
    }
    validate_role_note(role, note)
}

fn validate_role_note(role: &RoleCode, note: &str) -> Result<(), FeedbackError> {
    let role = role.as_str();
    if role.is_empty()
        || role.len() > MAX_ROLE_BYTES
        || !role
            .bytes()
            .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'_')
        || note.is_empty()
        || note.len() > MAX_NOTE_BYTES
        || note != note.trim()
        || note.chars().any(char::is_control)
    {
        return Err(invalid_bootstrap(
            "actor role or note violates the governed text contract",
        ));
    }
    Ok(())
}

fn validate_reason_code(reason_code: &str) -> Result<(), FeedbackError> {
    if reason_code.is_empty()
        || reason_code.len() > MAX_REASON_CODE_BYTES
        || !reason_code
            .bytes()
            .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'_')
    {
        return Err(invalid_bootstrap(
            "bootstrap reason code must be lowercase snake_case",
        ));
    }
    Ok(())
}

fn invalid_bootstrap(detail: impl Into<String>) -> FeedbackError {
    FeedbackError::InvalidBootstrapPreflight {
        detail: detail.into(),
    }
}

#[cfg(test)]
mod tests {
    use chrono::Utc;

    use super::ModelBootstrapPolicyProjection;
    use crate::{
        runtime_config::{
            ActivePolicyBundle, BuyModelRoute, DecisionPolicySnapshot, ModelBinding,
            ModelBindingSource,
        },
        types::{DecisionPolicySnapshotId, ModelVersionId, PolicyBundleGeneration},
    };

    impl ActivePolicyBundle {
        fn empty_fixture() -> Self {
            let snapshot = DecisionPolicySnapshot::default();
            let snapshot_hash = snapshot
                .persistence_hash()
                .expect("hash empty policy snapshot");
            Self::from_parts(
                PolicyBundleGeneration::FIRST,
                DecisionPolicySnapshotId::from_content_hash(&snapshot_hash),
                snapshot_hash,
                snapshot,
            )
        }
    }

    #[test]
    fn pooled_projection_isolated() {
        let bundle = ActivePolicyBundle::empty_fixture();
        let model_version_id = ModelVersionId::from_v7();
        let bound_at = Utc::now();
        let projection = ModelBootstrapPolicyProjection::try_new(
            &bundle,
            BuyModelRoute::Pooled,
            model_version_id,
            bound_at,
        )
        .expect("project first pooled champion");
        projection
            .validate_candidate(projection.prospective_snapshot())
            .expect("validate exact pooled route delta");
        let binding = projection
            .prospective_snapshot()
            .model_routing
            .model
            .route_binding(BuyModelRoute::Pooled)
            .expect("pooled route binding");
        assert_eq!(binding.champion.model_version_id, model_version_id);
        assert_eq!(binding.champion.bound_at, bound_at);
        assert_eq!(
            projection
                .prospective_snapshot()
                .model_routing
                .model
                .buy_routes
                .len(),
            1
        );

        let mut drifted = projection.prospective_snapshot().clone();
        drifted
            .model_routing
            .model
            .buy_routes
            .get_mut(&BuyModelRoute::Pooled)
            .expect("pooled route")
            .shadow = Some(ModelBinding::new(
            ModelVersionId::from_v7(),
            ModelBindingSource::Bootstrap,
            bound_at,
            PolicyBundleGeneration::FIRST
                .checked_next()
                .expect("next generation"),
            2,
        ));
        assert!(projection.validate_candidate(&drifted).is_err());
        assert!(
            ModelBootstrapPolicyProjection::try_new(
                &ActivePolicyBundle::from_parts(
                    PolicyBundleGeneration::FIRST,
                    DecisionPolicySnapshotId::from_content_hash(
                        &projection
                            .prospective_snapshot()
                            .persistence_hash()
                            .expect("hash populated pooled snapshot"),
                    ),
                    projection
                        .prospective_snapshot()
                        .persistence_hash()
                        .expect("rehash populated pooled snapshot"),
                    projection.prospective_snapshot().clone(),
                ),
                BuyModelRoute::Pooled,
                model_version_id,
                bound_at,
            )
            .is_err()
        );
    }
}
