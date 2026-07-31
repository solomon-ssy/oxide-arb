//! Governed, hot-reloadable configuration resources.

pub mod schedule_preview;
pub mod sections;
pub mod validation;
pub mod wire;

use std::{
    collections::{BTreeMap, BTreeSet},
    fmt::{Display, Formatter, Result as FmtResult},
};

use quant_pivot_error::{
    QuantError,
    config::ConfigError,
    config_validation::{ConfigValidationError, ConfigValidationReport},
    hashing::CanonicalDigestError,
};
pub use schedule_preview::{
    DueScheduleWindow, MAX_PREVIEW_OCCURRENCES, due_schedule_window, preview_fire_times,
    validate_schedule_cadence,
};
use schemars::{JsonSchema, Schema, SchemaGenerator, generate::SchemaSettings};
use sea_orm::FromJsonQueryResult;
pub use sections::{
    AutoExecutionConfig, CryptoCrossCheckConfig, CryptoDomainConfig, DataQualityConfig,
    DomainConfig, EntryConditionWorkerConfig, ExecutionConfig, FactorCrossSectionConfig,
    FactorHeadConfig, FactorNormalizationConfig, FactorOrthogonalizeConfig, FactorsConfig,
    FavoriteLongshotConfig, FeaturesConfig, KellySafetyConfig, MAX_REPORT_TOP_N,
    ModelCalibrationConfig, ModelConfig, MomentumFeaturesConfig, NegRiskStructuralConfig,
    ParticipantConcentrationConfig, PerFactorNormalization, PolicyValidationConfig,
    PortfolioBudget, PortfolioConfig, PortfolioConstraints, QualityGateConfig,
    ReportScheduleConfig, ReportsConfig, ResearchConfig, ResearchTrainingConfig,
    ResearchValidationConfig, ResearchValidationCpcvConfig, ResearchValidationGatesConfig,
    ResearchValidationPboConfig, ResearchValidationPurgeConfig, ResearchValidationTrialsConfig,
    ReversalAfterShockConfig, SelectionConfig, SellQualityGateConfig, SellScorerConfig,
    SemiAutoConfig, StructuralFactorsConfig, StructuralFeaturesConfig, TrainingConfig,
    WeatherDomainConfig,
};
use serde::{Deserialize, Serialize};
use serde_json::{Error as SerdeJsonError, Value};
use thiserror::Error;
pub use wire::{
    CapitalPolicy, ConfidenceSizeCurve, CorrelationConfig, DecimalValue, DrawdownMultiplierPolicy,
    EmergencyExitKind, EmergencyExitPolicy, EntryOrderPolicy, ExecutionBreakerConfig,
    ExitMonitorPolicy, ExitSignalReinferencePolicy, FeatureFamily, FeatureStalenessPolicy,
    KillSwitchPolicy, MASKED_SECRET, MissingFactorPolicy, ModelVersionRef, NeutralizeDimension,
    NotificationPolicies, OpportunisticSellPolicy, OutcomeReconciliationPolicy,
    PortfolioOptimizerConfig, RankLossKind, ReconciliationPolicy, ReportDeliveryPolicy,
    ScheduleCadence, SizingModelConfig, SmallCrossSectionPolicy, TrainingOptimizerKind,
};

use crate::{
    enums::{
        common::MarketCategory,
        runtime_config::{
            CheckOutcome, ConfigResourceKind, PolicyApplyBoundary, PolicyConsumer,
            PolicyPreflightCheckKind, PolicyPreflightDetailCode, PolicyValidationCode,
            PolicyValidationSeverity, ProfileArtifactKind,
        },
    },
    hashing::CanonicalDigest,
    types::{
        ContentHash, DecisionPolicySnapshotId, PolicyBundleGeneration, PolicyRevisionId,
        ProfileArtifactId, SchemaVersion,
    },
};

/// Boot schema shared by the six independent governed policy resources.
pub const POLICY_RESOURCE_SCHEMA_VERSION: SchemaVersion = SchemaVersion::FIRST;

/// Market-selection, data-quality, and recommendation payload semantics.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(default, deny_unknown_fields)]
pub struct RecommendationPolicy {
    #[schemars(extend("x-format" = "integer", "x-ui-visible" = false))]
    pub schema_version: SchemaVersion,
    pub selection: SelectionConfig,
    pub data_quality: DataQualityConfig,
    pub reports: ReportsConfig,
}

impl Default for RecommendationPolicy {
    fn default() -> Self {
        Self {
            schema_version: POLICY_RESOURCE_SCHEMA_VERSION,
            selection: SelectionConfig::default(),
            data_quality: DataQualityConfig::default(),
            reports: ReportsConfig::default(),
        }
    }
}

/// Capital, sizing, order, exit, reconciliation, and breaker policy.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(default, deny_unknown_fields)]
pub struct ExecutionRiskPolicy {
    #[schemars(extend("x-format" = "integer", "x-ui-visible" = false))]
    pub schema_version: SchemaVersion,
    pub portfolio: PortfolioConfig,
    pub entry_order_policy: EntryOrderPolicy,
    pub exit_monitor: ExitMonitorPolicy,
    pub capital: CapitalPolicy,
    pub reconciliation: ReconciliationPolicy,
    pub breaker: ExecutionBreakerConfig,
}

impl Default for ExecutionRiskPolicy {
    fn default() -> Self {
        Self {
            schema_version: POLICY_RESOURCE_SCHEMA_VERSION,
            portfolio: PortfolioConfig::default(),
            entry_order_policy: EntryOrderPolicy::default(),
            exit_monitor: ExitMonitorPolicy::default(),
            capital: CapitalPolicy::default(),
            reconciliation: ReconciliationPolicy::default(),
            breaker: ExecutionBreakerConfig::default(),
        }
    }
}

/// Active, shadow, and exit artifact routing.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(default, deny_unknown_fields)]
pub struct ModelRouting {
    #[schemars(extend("x-format" = "integer", "x-ui-visible" = false))]
    pub schema_version: SchemaVersion,
    pub model: ModelConfig,
}

impl Default for ModelRouting {
    fn default() -> Self {
        Self {
            schema_version: POLICY_RESOURCE_SCHEMA_VERSION,
            model: ModelConfig::default(),
        }
    }
}

/// Exact Buy-side route represented by one report and one durable model run.
///
/// `Pooled` may contain only non-vertical market categories. `Crypto` and
/// `Weather` are isolated category routes because their `ResearchProfile`,
/// domain-source, factor-plane, and serving-contract preimages are distinct.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BuyModelRoute {
    Pooled,
    Crypto,
    Weather,
}

impl BuyModelRoute {
    #[must_use]
    pub const fn category(self) -> Option<MarketCategory> {
        match self {
            Self::Pooled => None,
            Self::Crypto => Some(MarketCategory::Crypto),
            Self::Weather => Some(MarketCategory::Weather),
        }
    }
}

impl TryFrom<&SelectionConfig> for BuyModelRoute {
    type Error = ConfigError;

    fn try_from(selection: &SelectionConfig) -> Result<Self, Self::Error> {
        let categories = selection
            .enabled_categories
            .iter()
            .copied()
            .collect::<BTreeSet<_>>();
        if categories.len() != selection.enabled_categories.len() {
            return Err(ConfigError::InvalidValue {
                field: "selection.enabled_categories".to_owned(),
                reason: "duplicate categories are not a canonical report scope".to_owned(),
            });
        }
        let verticals = categories
            .iter()
            .copied()
            .filter(|category| matches!(category, MarketCategory::Crypto | MarketCategory::Weather))
            .collect::<Vec<_>>();
        match verticals.as_slice() {
            [] => Ok(Self::Pooled),
            [MarketCategory::Crypto] if categories.len() == 1 => Ok(Self::Crypto),
            [MarketCategory::Weather] if categories.len() == 1 => Ok(Self::Weather),
            _ => Err(ConfigError::InvalidValue {
                field: "selection.enabled_categories".to_owned(),
                reason: "Crypto or Weather must be the sole category in one report; vertical \
                         contracts cannot share a pooled model run"
                    .to_owned(),
            }),
        }
    }
}

impl TryFrom<Option<MarketCategory>> for BuyModelRoute {
    type Error = ConfigError;

    fn try_from(category: Option<MarketCategory>) -> Result<Self, Self::Error> {
        match category {
            None => Ok(Self::Pooled),
            Some(MarketCategory::Crypto) => Ok(Self::Crypto),
            Some(MarketCategory::Weather) => Ok(Self::Weather),
            Some(category) => Err(ConfigError::InvalidValue {
                field: "model.category_scope".to_owned(),
                reason: format!(
                    "{category} cannot own a category-specific Buy route; only Crypto and Weather \
                     are vertical routes"
                ),
            }),
        }
    }
}

impl ModelConfig {
    /// Resolve the exact active pointer for one report route.
    ///
    /// # Errors
    ///
    /// Returns a typed configuration error when the route has no pointer.
    /// Category routes never fall back to the pooled pointer.
    pub fn active_pointer(&self, route: BuyModelRoute) -> Result<&ModelVersionRef, ConfigError> {
        match route {
            BuyModelRoute::Pooled => {
                self.active_model_version_id
                    .as_ref()
                    .ok_or_else(|| ConfigError::MissingField {
                        section: "model".to_owned(),
                        field: "active_model_version_id".to_owned(),
                    })
            }
            BuyModelRoute::Crypto => self
                .category_model_pointers
                .get(&MarketCategory::Crypto)
                .ok_or_else(|| ConfigError::MissingField {
                    section: "model.category_model_pointers".to_owned(),
                    field: MarketCategory::Crypto.to_string(),
                }),
            BuyModelRoute::Weather => self
                .category_model_pointers
                .get(&MarketCategory::Weather)
                .ok_or_else(|| ConfigError::MissingField {
                    section: "model.category_model_pointers".to_owned(),
                    field: MarketCategory::Weather.to_string(),
                }),
        }
    }
}

/// Durable report schedule resource, isolated from report decision semantics.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(default, deny_unknown_fields)]
pub struct ReportSchedule {
    #[schemars(extend("x-format" = "integer", "x-ui-visible" = false))]
    pub schema_version: SchemaVersion,
    pub schedules: Vec<ReportScheduleConfig>,
}

impl Default for ReportSchedule {
    fn default() -> Self {
        Self {
            schema_version: POLICY_RESOURCE_SCHEMA_VERSION,
            schedules: vec![ReportScheduleConfig::default()],
        }
    }
}

/// Immediate operational admission controls and notification routing.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(default, deny_unknown_fields)]
pub struct OperationalControl {
    #[schemars(extend("x-format" = "integer", "x-ui-visible" = false))]
    pub schema_version: SchemaVersion,
    pub entry_condition: EntryConditionWorkerConfig,
    pub kill_switch: KillSwitchPolicy,
    pub outcome_reconciliation: OutcomeReconciliationPolicy,
    pub notifications: NotificationPolicies,
}

impl Default for OperationalControl {
    fn default() -> Self {
        Self {
            schema_version: POLICY_RESOURCE_SCHEMA_VERSION,
            entry_condition: EntryConditionWorkerConfig::default(),
            kill_switch: KillSwitchPolicy::default(),
            outcome_reconciliation: OutcomeReconciliationPolicy::default(),
            notifications: NotificationPolicies::default(),
        }
    }
}

/// Explicit authorization for semi-automatic and automatic execution.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(default, deny_unknown_fields)]
pub struct ExecutionAuthorization {
    #[schemars(extend("x-format" = "integer", "x-ui-visible" = false))]
    pub schema_version: SchemaVersion,
    pub semi_auto: SemiAutoConfig,
    pub auto_execution: AutoExecutionConfig,
}

impl Default for ExecutionAuthorization {
    fn default() -> Self {
        Self {
            schema_version: POLICY_RESOURCE_SCHEMA_VERSION,
            semi_auto: SemiAutoConfig::default(),
            auto_execution: AutoExecutionConfig::default(),
        }
    }
}

/// Closed set of documents accepted by the governed policy repository.
///
/// `PostgreSQL` stores this aggregate in `JSONB` because revisions are immutable
/// documents and none of their leaf fields participate in database queries.
/// `SeaORM` still reads and writes the column as this Rust enum, never as an
/// untyped `serde_json::Value`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema, FromJsonQueryResult)]
#[serde(tag = "resource_kind", content = "document", rename_all = "snake_case")]
pub enum PolicyDocument {
    RecommendationPolicy(RecommendationPolicy),
    ExecutionRiskPolicy(Box<ExecutionRiskPolicy>),
    ModelRouting(ModelRouting),
    ReportSchedule(ReportSchedule),
    OperationalControl(OperationalControl),
    ExecutionAuthorization(ExecutionAuthorization),
}

impl PolicyDocument {
    #[must_use]
    pub const fn kind(&self) -> ConfigResourceKind {
        match self {
            Self::RecommendationPolicy(_) => ConfigResourceKind::RecommendationPolicy,
            Self::ExecutionRiskPolicy(_) => ConfigResourceKind::ExecutionRiskPolicy,
            Self::ModelRouting(_) => ConfigResourceKind::ModelRouting,
            Self::ReportSchedule(_) => ConfigResourceKind::ReportSchedule,
            Self::OperationalControl(_) => ConfigResourceKind::OperationalControl,
            Self::ExecutionAuthorization(_) => ConfigResourceKind::ExecutionAuthorization,
        }
    }

    #[must_use]
    pub const fn schema_version(&self) -> SchemaVersion {
        match self {
            Self::RecommendationPolicy(policy) => policy.schema_version,
            Self::ExecutionRiskPolicy(policy) => policy.schema_version,
            Self::ModelRouting(policy) => policy.schema_version,
            Self::ReportSchedule(policy) => policy.schema_version,
            Self::OperationalControl(policy) => policy.schema_version,
            Self::ExecutionAuthorization(policy) => policy.schema_version,
        }
    }
}

/// One stable, machine-readable validation diagnostic.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct PolicyValidationIssue {
    pub severity: PolicyValidationSeverity,
    pub code: PolicyValidationCode,
    /// Canonical field path. A path is data, not a categorical state, so a
    /// validated string is the correct representation.
    pub path: String,
    pub message: String,
}

/// One dependency or consumer preflight result.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct PolicyPreflightResult {
    pub check: PolicyPreflightCheckKind,
    pub outcome: CheckOutcome,
    pub detail_code: PolicyPreflightDetailCode,
    /// Optional machine-produced diagnostic for a failed dependency. Stable
    /// success and skip explanations are represented by `detail_code`.
    pub failure_detail: Option<String>,
}

/// Typed validation evidence persisted with a validated policy revision.
#[derive(
    Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema, FromJsonQueryResult,
)]
#[serde(default, deny_unknown_fields)]
pub struct PolicyValidationEvidence {
    /// Exact active bundle against which this revision was validated.
    pub subject: Option<PolicyValidationSubject>,
    pub issues: Vec<PolicyValidationIssue>,
    pub preflight: Vec<PolicyPreflightResult>,
}

/// Immutable subject bound to typed validation, preflight and approval.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema, FromJsonQueryResult)]
#[serde(deny_unknown_fields)]
pub struct PolicyValidationSubject {
    pub base_generation: PolicyBundleGeneration,
    pub base_revision_vector: PolicyRevisionBundle,
    #[schemars(with = "String", extend("x-format" = "content-hash"))]
    pub candidate_bundle_hash: ContentHash,
}

impl PolicyValidationEvidence {
    #[must_use]
    pub fn is_valid(&self) -> bool {
        !self
            .issues
            .iter()
            .any(|issue| issue.severity == PolicyValidationSeverity::Error)
            && self
                .preflight
                .iter()
                .all(|check| check.outcome != CheckOutcome::Failed)
    }
}

impl ConfigResourceKind {
    #[must_use]
    pub const fn apply_boundary(self) -> PolicyApplyBoundary {
        match self {
            Self::RecommendationPolicy => PolicyApplyBoundary::ReportRunClaim,
            Self::ExecutionRiskPolicy => PolicyApplyBoundary::OrderIntentCreation,
            Self::ModelRouting => PolicyApplyBoundary::ModelEvaluationClaim,
            Self::ReportSchedule => PolicyApplyBoundary::FutureReportRunReconcile,
            Self::OperationalControl => PolicyApplyBoundary::OperationalAdmission,
            Self::ExecutionAuthorization => PolicyApplyBoundary::ExecutionAuthorizationAdmission,
        }
    }

    #[must_use]
    pub const fn consumers(self) -> &'static [PolicyConsumer] {
        match self {
            Self::RecommendationPolicy => &[
                PolicyConsumer::MarketSelection,
                PolicyConsumer::DataQualityGate,
                PolicyConsumer::RecommendationComposer,
                PolicyConsumer::ReportCoordinator,
            ],
            Self::ExecutionRiskPolicy => &[
                PolicyConsumer::PortfolioOptimizer,
                PolicyConsumer::OrderIntentService,
                PolicyConsumer::ExecutionAdmission,
                PolicyConsumer::ExitMonitor,
            ],
            Self::ModelRouting => &[PolicyConsumer::ModelRunner],
            Self::ReportSchedule => &[PolicyConsumer::ReportScheduler],
            Self::OperationalControl => &[
                PolicyConsumer::WorkerAdmission,
                PolicyConsumer::ExecutionAdmission,
                PolicyConsumer::AlertDispatcher,
            ],
            Self::ExecutionAuthorization => &[
                PolicyConsumer::RuntimeModeGate,
                PolicyConsumer::ExecutionAdmission,
            ],
        }
    }
}

/// Immutable feature-definition artifact. Its content hash is the artifact id.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(default, deny_unknown_fields)]
pub struct FeatureProfileArtifact {
    pub schema_version: SchemaVersion,
    pub definition: FeaturesConfig,
}

impl Default for FeatureProfileArtifact {
    fn default() -> Self {
        Self {
            schema_version: SchemaVersion::FIRST,
            definition: FeaturesConfig::default(),
        }
    }
}

/// Immutable factor/scoring methodology artifact.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(default, deny_unknown_fields)]
pub struct ScoringProfileArtifact {
    pub schema_version: SchemaVersion,
    pub definition: FactorsConfig,
}

impl Default for ScoringProfileArtifact {
    fn default() -> Self {
        Self {
            schema_version: SchemaVersion::FIRST,
            definition: FactorsConfig::default(),
        }
    }
}

/// Immutable domain semantics artifact; provider endpoints are Deploy Config.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(default, deny_unknown_fields)]
pub struct DomainProfileArtifact {
    pub schema_version: SchemaVersion,
    pub definition: DomainConfig,
}

impl Default for DomainProfileArtifact {
    fn default() -> Self {
        Self {
            schema_version: SchemaVersion::FIRST,
            definition: DomainConfig::default(),
        }
    }
}

/// Immutable training, validation and promotion methodology artifact.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(default, deny_unknown_fields)]
pub struct ResearchMethodProfileArtifact {
    pub schema_version: SchemaVersion,
    pub model_promotion: QualityGateConfig,
    pub training: TrainingConfig,
    pub research: ResearchConfig,
}

impl Default for ResearchMethodProfileArtifact {
    fn default() -> Self {
        Self {
            schema_version: SchemaVersion::FIRST,
            model_promotion: QualityGateConfig::default(),
            training: TrainingConfig::default(),
            research: ResearchConfig::default(),
        }
    }
}

/// Non-hot artifacts frozen into every decision snapshot and downstream run.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(default, deny_unknown_fields)]
pub struct ImmutableProfileArtifacts {
    pub features: FeatureProfileArtifact,
    pub scoring: ScoringProfileArtifact,
    pub domain: DomainProfileArtifact,
    pub research_method: ResearchMethodProfileArtifact,
}

impl ImmutableProfileArtifacts {
    /// Content-address each artifact independently for lineage and evidence.
    pub fn content_hashes(&self) -> Result<ImmutableProfileArtifactHashes, PolicySnapshotError> {
        Ok(ImmutableProfileArtifactHashes {
            feature_profile: CanonicalDigest::content_hash_json(&self.features)
                .map_err(PolicySnapshotError::ArtifactHash)?,
            scoring_profile: CanonicalDigest::content_hash_json(&self.scoring)
                .map_err(PolicySnapshotError::ArtifactHash)?,
            domain_profile: CanonicalDigest::content_hash_json(&self.domain)
                .map_err(PolicySnapshotError::ArtifactHash)?,
            research_method_profile: CanonicalDigest::content_hash_json(&self.research_method)
                .map_err(PolicySnapshotError::ArtifactHash)?,
        })
    }

    #[must_use]
    pub fn uses_boot_schemas(&self) -> bool {
        [
            self.features.schema_version,
            self.scoring.schema_version,
            self.domain.schema_version,
            self.research_method.schema_version,
        ]
        .into_iter()
        .all(|version| version == SchemaVersion::FIRST)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct ImmutableProfileArtifactHashes {
    #[schemars(with = "String", extend("x-format" = "content-hash"))]
    pub feature_profile: ContentHash,
    #[schemars(with = "String", extend("x-format" = "content-hash"))]
    pub scoring_profile: ContentHash,
    #[schemars(with = "String", extend("x-format" = "content-hash"))]
    pub domain_profile: ContentHash,
    #[schemars(with = "String", extend("x-format" = "content-hash"))]
    pub research_method_profile: ContentHash,
}

/// Closed tagged payload stored in the WORM policy-profile artifact table.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema, FromJsonQueryResult)]
#[serde(tag = "profile_kind", content = "document", rename_all = "snake_case")]
pub enum PolicyProfileDocument {
    Feature(FeatureProfileArtifact),
    Scoring(Box<ScoringProfileArtifact>),
    Domain(DomainProfileArtifact),
    ResearchMethod(Box<ResearchMethodProfileArtifact>),
}

impl PolicyProfileDocument {
    #[must_use]
    pub const fn kind(&self) -> ProfileArtifactKind {
        match self {
            Self::Feature(_) => ProfileArtifactKind::Feature,
            Self::Scoring(_) => ProfileArtifactKind::Scoring,
            Self::Domain(_) => ProfileArtifactKind::Domain,
            Self::ResearchMethod(_) => ProfileArtifactKind::ResearchMethod,
        }
    }

    #[must_use]
    pub const fn schema_version(&self) -> SchemaVersion {
        match self {
            Self::Feature(document) => document.schema_version,
            Self::Scoring(document) => document.schema_version,
            Self::Domain(document) => document.schema_version,
            Self::ResearchMethod(document) => document.schema_version,
        }
    }

    pub fn content_hash(&self) -> Result<ContentHash, PolicySnapshotError> {
        match self {
            Self::Feature(document) => CanonicalDigest::content_hash_json(document),
            Self::Scoring(document) => CanonicalDigest::content_hash_json(document),
            Self::Domain(document) => CanonicalDigest::content_hash_json(document),
            Self::ResearchMethod(document) => CanonicalDigest::content_hash_json(document),
        }
        .map_err(PolicySnapshotError::ArtifactHash)
    }
}

/// Exact content-addressed reference frozen into a decision-policy snapshot.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct PolicyProfileArtifactReference {
    pub profile_artifact_id: ProfileArtifactId,
    pub kind: ProfileArtifactKind,
    #[schemars(with = "String", extend("x-format" = "content-hash"))]
    pub content_hash: ContentHash,
}

impl PolicyProfileArtifactReference {
    pub fn from_document(document: &PolicyProfileDocument) -> Result<Self, PolicySnapshotError> {
        let kind = document.kind();
        let content_hash = document.content_hash()?;
        Ok(Self {
            profile_artifact_id: ProfileArtifactId::from_content_address(
                kind.as_str(),
                &content_hash,
            ),
            kind,
            content_hash,
        })
    }
}

/// Four immutable policy-profile references persisted in every snapshot.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ImmutableProfileArtifactReferences {
    pub features: PolicyProfileArtifactReference,
    pub scoring: PolicyProfileArtifactReference,
    pub domain: PolicyProfileArtifactReference,
    pub research_method: PolicyProfileArtifactReference,
}

impl ImmutableProfileArtifactReferences {
    #[must_use]
    pub const fn all(&self) -> [&PolicyProfileArtifactReference; 4] {
        [
            &self.features,
            &self.scoring,
            &self.domain,
            &self.research_method,
        ]
    }
}

impl ImmutableProfileArtifacts {
    #[must_use]
    pub fn documents(&self) -> [PolicyProfileDocument; 4] {
        [
            PolicyProfileDocument::Feature(self.features.clone()),
            PolicyProfileDocument::Scoring(Box::new(self.scoring.clone())),
            PolicyProfileDocument::Domain(self.domain.clone()),
            PolicyProfileDocument::ResearchMethod(Box::new(self.research_method.clone())),
        ]
    }

    pub fn references(&self) -> Result<ImmutableProfileArtifactReferences, PolicySnapshotError> {
        let [features, scoring, domain, research_method] = self.documents();
        Ok(ImmutableProfileArtifactReferences {
            features: PolicyProfileArtifactReference::from_document(&features)?,
            scoring: PolicyProfileArtifactReference::from_document(&scoring)?,
            domain: PolicyProfileArtifactReference::from_document(&domain)?,
            research_method: PolicyProfileArtifactReference::from_document(&research_method)?,
        })
    }
}

/// Revision identities frozen at a decision boundary.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(default, deny_unknown_fields)]
pub struct PolicyRevisionBundle {
    #[schemars(with = "Option<String>")]
    pub recommendation_policy: Option<PolicyRevisionId>,
    #[schemars(with = "Option<String>")]
    pub execution_risk_policy: Option<PolicyRevisionId>,
    #[schemars(with = "Option<String>")]
    pub model_routing: Option<PolicyRevisionId>,
    #[schemars(with = "Option<String>")]
    pub report_schedule: Option<PolicyRevisionId>,
    #[schemars(with = "Option<String>")]
    pub operational_control: Option<PolicyRevisionId>,
    #[schemars(with = "Option<String>")]
    pub execution_authorization: Option<PolicyRevisionId>,
}

/// Immutable aggregate read by decision pipelines. Each hot resource is still
/// revised, approved, activated, and audited independently.
#[derive(
    Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema, FromJsonQueryResult,
)]
#[serde(default, deny_unknown_fields)]
pub struct DecisionPolicySnapshot {
    pub revisions: PolicyRevisionBundle,
    pub recommendation: RecommendationPolicy,
    pub execution_risk: ExecutionRiskPolicy,
    pub model_routing: ModelRouting,
    pub report_schedule: ReportSchedule,
    pub operational_control: OperationalControl,
    pub execution_authorization: ExecutionAuthorization,
    pub profile_artifacts: ImmutableProfileArtifacts,
}

/// Canonical database document. Profile definitions are never embedded here;
/// only exact WORM artifact references participate in the snapshot hash.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema, FromJsonQueryResult)]
#[serde(deny_unknown_fields)]
pub struct DecisionPolicySnapshotDocument {
    pub revisions: PolicyRevisionBundle,
    pub recommendation: RecommendationPolicy,
    pub execution_risk: ExecutionRiskPolicy,
    pub model_routing: ModelRouting,
    pub report_schedule: ReportSchedule,
    pub operational_control: OperationalControl,
    pub execution_authorization: ExecutionAuthorization,
    pub profile_artifact_refs: ImmutableProfileArtifactReferences,
}

impl DecisionPolicySnapshotDocument {
    #[must_use]
    pub const fn resource_revision_id(
        &self,
        kind: ConfigResourceKind,
    ) -> Option<&PolicyRevisionId> {
        match kind {
            ConfigResourceKind::RecommendationPolicy => {
                self.revisions.recommendation_policy.as_ref()
            }
            ConfigResourceKind::ExecutionRiskPolicy => {
                self.revisions.execution_risk_policy.as_ref()
            }
            ConfigResourceKind::ModelRouting => self.revisions.model_routing.as_ref(),
            ConfigResourceKind::ReportSchedule => self.revisions.report_schedule.as_ref(),
            ConfigResourceKind::OperationalControl => self.revisions.operational_control.as_ref(),
            ConfigResourceKind::ExecutionAuthorization => {
                self.revisions.execution_authorization.as_ref()
            }
        }
    }

    #[must_use]
    pub fn resource_document(&self, kind: ConfigResourceKind) -> PolicyDocument {
        match kind {
            ConfigResourceKind::RecommendationPolicy => {
                PolicyDocument::RecommendationPolicy(self.recommendation.clone())
            }
            ConfigResourceKind::ExecutionRiskPolicy => {
                PolicyDocument::ExecutionRiskPolicy(Box::new(self.execution_risk.clone()))
            }
            ConfigResourceKind::ModelRouting => {
                PolicyDocument::ModelRouting(self.model_routing.clone())
            }
            ConfigResourceKind::ReportSchedule => {
                PolicyDocument::ReportSchedule(self.report_schedule.clone())
            }
            ConfigResourceKind::OperationalControl => {
                PolicyDocument::OperationalControl(self.operational_control.clone())
            }
            ConfigResourceKind::ExecutionAuthorization => {
                PolicyDocument::ExecutionAuthorization(self.execution_authorization.clone())
            }
        }
    }

    pub fn resolve(
        self,
        documents: Vec<(ProfileArtifactId, PolicyProfileDocument)>,
    ) -> Result<DecisionPolicySnapshot, PolicySnapshotError> {
        let mut by_id = BTreeMap::new();
        for (id, document) in documents {
            if by_id.insert(id, document).is_some() {
                return Err(PolicySnapshotError::DuplicateArtifact { id });
            }
        }
        let resolve = |reference: &PolicyProfileArtifactReference| {
            let document = by_id.get(&reference.profile_artifact_id).ok_or({
                PolicySnapshotError::MissingArtifact {
                    id: reference.profile_artifact_id,
                }
            })?;
            if document.kind() != reference.kind {
                return Err(PolicySnapshotError::ArtifactKindMismatch {
                    id: reference.profile_artifact_id,
                    expected: reference.kind,
                    actual: document.kind(),
                });
            }
            let actual_hash = document.content_hash()?;
            if actual_hash != reference.content_hash
                || ProfileArtifactId::from_content_address(reference.kind.as_str(), &actual_hash)
                    != reference.profile_artifact_id
            {
                return Err(PolicySnapshotError::ArtifactIdentityMismatch {
                    id: reference.profile_artifact_id,
                });
            }
            Ok(document.clone())
        };
        let features = match resolve(&self.profile_artifact_refs.features)? {
            PolicyProfileDocument::Feature(document) => document,
            document => {
                return Err(PolicySnapshotError::ArtifactKindMismatch {
                    id: self.profile_artifact_refs.features.profile_artifact_id,
                    expected: ProfileArtifactKind::Feature,
                    actual: document.kind(),
                });
            }
        };
        let scoring = match resolve(&self.profile_artifact_refs.scoring)? {
            PolicyProfileDocument::Scoring(document) => *document,
            document => {
                return Err(PolicySnapshotError::ArtifactKindMismatch {
                    id: self.profile_artifact_refs.scoring.profile_artifact_id,
                    expected: ProfileArtifactKind::Scoring,
                    actual: document.kind(),
                });
            }
        };
        let domain = match resolve(&self.profile_artifact_refs.domain)? {
            PolicyProfileDocument::Domain(document) => document,
            document => {
                return Err(PolicySnapshotError::ArtifactKindMismatch {
                    id: self.profile_artifact_refs.domain.profile_artifact_id,
                    expected: ProfileArtifactKind::Domain,
                    actual: document.kind(),
                });
            }
        };
        let research_method = match resolve(&self.profile_artifact_refs.research_method)? {
            PolicyProfileDocument::ResearchMethod(document) => *document,
            document => {
                return Err(PolicySnapshotError::ArtifactKindMismatch {
                    id: self
                        .profile_artifact_refs
                        .research_method
                        .profile_artifact_id,
                    expected: ProfileArtifactKind::ResearchMethod,
                    actual: document.kind(),
                });
            }
        };
        Ok(DecisionPolicySnapshot {
            revisions: self.revisions,
            recommendation: self.recommendation,
            execution_risk: self.execution_risk,
            model_routing: self.model_routing,
            report_schedule: self.report_schedule,
            operational_control: self.operational_control,
            execution_authorization: self.execution_authorization,
            profile_artifacts: ImmutableProfileArtifacts {
                features,
                scoring,
                domain,
                research_method,
            },
        })
    }
}

/// Immutable identity of one database-committed policy bundle.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PolicyBundleIdentity {
    pub generation: PolicyBundleGeneration,
    pub decision_policy_snapshot_id: DecisionPolicySnapshotId,
    pub snapshot_hash: ContentHash,
}

/// Typed reason that the process cannot serve the latest committed bundle.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PolicyApplyDegradedCause {
    Applying,
    PrepareFailed,
    PublishFailed,
    GenerationMismatch,
}

impl Display for PolicyApplyDegradedCause {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> FmtResult {
        formatter.write_str(match self {
            Self::Applying => "applying",
            Self::PrepareFailed => "prepare_failed",
            Self::PublishFailed => "publish_failed",
            Self::GenerationMismatch => "generation_mismatch",
        })
    }
}

/// Process-local projection of durable desired and atomically applied policy
/// generations.
///
/// This is observability only. `PostgreSQL` remains the sole desired state
/// authority, and the live stores remain the sole applied state owners.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PolicyApplyReadiness {
    Ready {
        applied: PolicyBundleIdentity,
    },
    Degraded {
        desired: PolicyBundleIdentity,
        applied: PolicyBundleIdentity,
        cause: PolicyApplyDegradedCause,
    },
}

impl PolicyApplyReadiness {
    #[must_use]
    pub const fn is_ready(self) -> bool {
        matches!(self, Self::Ready { .. })
    }

    #[must_use]
    pub const fn desired(self) -> PolicyBundleIdentity {
        match self {
            Self::Ready { applied } => applied,
            Self::Degraded { desired, .. } => desired,
        }
    }

    #[must_use]
    pub const fn applied(self) -> PolicyBundleIdentity {
        match self {
            Self::Ready { applied } | Self::Degraded { applied, .. } => applied,
        }
    }
}

impl Display for PolicyApplyReadiness {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> FmtResult {
        match self {
            Self::Ready { applied } => write!(
                formatter,
                "ready generation={} snapshot_id={} snapshot_hash={}",
                applied.generation, applied.decision_policy_snapshot_id, applied.snapshot_hash
            ),
            Self::Degraded {
                desired,
                applied,
                cause,
            } => write!(
                formatter,
                "degraded cause={cause} desired_generation={} desired_snapshot_id={} \
                 desired_snapshot_hash={} applied_generation={} applied_snapshot_id={} \
                 applied_snapshot_hash={}",
                desired.generation,
                desired.decision_policy_snapshot_id,
                desired.snapshot_hash,
                applied.generation,
                applied.decision_policy_snapshot_id,
                applied.snapshot_hash,
            ),
        }
    }
}

/// Database-authoritative identity and contents of one committed policy bundle.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ActivePolicyBundle {
    pub generation: PolicyBundleGeneration,
    pub decision_policy_snapshot_id: DecisionPolicySnapshotId,
    #[schemars(with = "String", extend("x-format" = "content-hash"))]
    pub snapshot_hash: ContentHash,
    pub revision_vector: PolicyRevisionBundle,
    pub snapshot: DecisionPolicySnapshot,
}

impl ActivePolicyBundle {
    #[must_use]
    pub fn from_parts(
        generation: PolicyBundleGeneration,
        decision_policy_snapshot_id: DecisionPolicySnapshotId,
        snapshot_hash: ContentHash,
        snapshot: DecisionPolicySnapshot,
    ) -> Self {
        let revision_vector = snapshot.revisions.clone();
        Self {
            generation,
            decision_policy_snapshot_id,
            snapshot_hash,
            revision_vector,
            snapshot,
        }
    }
}

impl From<&ActivePolicyBundle> for PolicyBundleIdentity {
    fn from(bundle: &ActivePolicyBundle) -> Self {
        Self {
            generation: bundle.generation,
            decision_policy_snapshot_id: bundle.decision_policy_snapshot_id,
            snapshot_hash: bundle.snapshot_hash,
        }
    }
}

impl DecisionPolicySnapshot {
    pub fn persistence_document(
        &self,
    ) -> Result<DecisionPolicySnapshotDocument, PolicySnapshotError> {
        Ok(DecisionPolicySnapshotDocument {
            revisions: self.revisions.clone(),
            recommendation: self.recommendation.clone(),
            execution_risk: self.execution_risk.clone(),
            model_routing: self.model_routing.clone(),
            report_schedule: self.report_schedule.clone(),
            operational_control: self.operational_control.clone(),
            execution_authorization: self.execution_authorization.clone(),
            profile_artifact_refs: self.profile_artifacts.references()?,
        })
    }

    pub fn persistence_hash(&self) -> Result<ContentHash, PolicySnapshotError> {
        CanonicalDigest::content_hash_json(&self.persistence_document()?)
            .map_err(PolicySnapshotError::ArtifactHash)
    }

    #[must_use]
    pub fn uses_current_resource_schemas(&self) -> bool {
        [
            self.recommendation.schema_version,
            self.execution_risk.schema_version,
            self.model_routing.schema_version,
            self.report_schedule.schema_version,
            self.operational_control.schema_version,
            self.execution_authorization.schema_version,
        ]
        .into_iter()
        .all(|version| version == POLICY_RESOURCE_SCHEMA_VERSION)
            && self.profile_artifacts.uses_boot_schemas()
    }

    #[must_use]
    pub const fn resource_revision_id(
        &self,
        kind: ConfigResourceKind,
    ) -> Option<&PolicyRevisionId> {
        match kind {
            ConfigResourceKind::RecommendationPolicy => {
                self.revisions.recommendation_policy.as_ref()
            }
            ConfigResourceKind::ExecutionRiskPolicy => {
                self.revisions.execution_risk_policy.as_ref()
            }
            ConfigResourceKind::ModelRouting => self.revisions.model_routing.as_ref(),
            ConfigResourceKind::ReportSchedule => self.revisions.report_schedule.as_ref(),
            ConfigResourceKind::OperationalControl => self.revisions.operational_control.as_ref(),
            ConfigResourceKind::ExecutionAuthorization => {
                self.revisions.execution_authorization.as_ref()
            }
        }
    }

    pub const fn set_resource_revision_id(
        &mut self,
        kind: ConfigResourceKind,
        revision_id: PolicyRevisionId,
    ) {
        let target = match kind {
            ConfigResourceKind::RecommendationPolicy => &mut self.revisions.recommendation_policy,
            ConfigResourceKind::ExecutionRiskPolicy => &mut self.revisions.execution_risk_policy,
            ConfigResourceKind::ModelRouting => &mut self.revisions.model_routing,
            ConfigResourceKind::ReportSchedule => &mut self.revisions.report_schedule,
            ConfigResourceKind::OperationalControl => &mut self.revisions.operational_control,
            ConfigResourceKind::ExecutionAuthorization => {
                &mut self.revisions.execution_authorization
            }
        };
        *target = Some(revision_id);
    }

    #[must_use]
    pub fn resource_document(&self, kind: ConfigResourceKind) -> PolicyDocument {
        match kind {
            ConfigResourceKind::RecommendationPolicy => {
                PolicyDocument::RecommendationPolicy(self.recommendation.clone())
            }
            ConfigResourceKind::ExecutionRiskPolicy => {
                PolicyDocument::ExecutionRiskPolicy(Box::new(self.execution_risk.clone()))
            }
            ConfigResourceKind::ModelRouting => {
                PolicyDocument::ModelRouting(self.model_routing.clone())
            }
            ConfigResourceKind::ReportSchedule => {
                PolicyDocument::ReportSchedule(self.report_schedule.clone())
            }
            ConfigResourceKind::OperationalControl => {
                PolicyDocument::OperationalControl(self.operational_control.clone())
            }
            ConfigResourceKind::ExecutionAuthorization => {
                PolicyDocument::ExecutionAuthorization(self.execution_authorization.clone())
            }
        }
    }

    pub fn replace_resource_document(
        &mut self,
        kind: ConfigResourceKind,
        document: PolicyDocument,
    ) -> Result<(), PolicySnapshotError> {
        if document.kind() != kind {
            return Err(PolicySnapshotError::ResourceKindMismatch {
                expected: kind,
                actual: document.kind(),
            });
        }
        match document {
            PolicyDocument::RecommendationPolicy(policy) => self.recommendation = policy,
            PolicyDocument::ExecutionRiskPolicy(policy) => self.execution_risk = *policy,
            PolicyDocument::ModelRouting(policy) => self.model_routing = policy,
            PolicyDocument::ReportSchedule(policy) => self.report_schedule = policy,
            PolicyDocument::OperationalControl(policy) => self.operational_control = policy,
            PolicyDocument::ExecutionAuthorization(policy) => {
                self.execution_authorization = policy;
            }
        }
        if !self.uses_current_resource_schemas() {
            return Err(PolicySnapshotError::UnsupportedSchemaVersion);
        }
        Ok(())
    }

    #[must_use]
    pub fn resource_json_schema(kind: ConfigResourceKind) -> Schema {
        let settings = SchemaSettings::default().with(|settings| settings.inline_subschemas = true);
        let generator = SchemaGenerator::new(settings);
        match kind {
            ConfigResourceKind::RecommendationPolicy => {
                generator.into_root_schema_for::<RecommendationPolicy>()
            }
            ConfigResourceKind::ExecutionRiskPolicy => {
                generator.into_root_schema_for::<ExecutionRiskPolicy>()
            }
            ConfigResourceKind::ModelRouting => generator.into_root_schema_for::<ModelRouting>(),
            ConfigResourceKind::ReportSchedule => {
                generator.into_root_schema_for::<ReportSchedule>()
            }
            ConfigResourceKind::OperationalControl => {
                generator.into_root_schema_for::<OperationalControl>()
            }
            ConfigResourceKind::ExecutionAuthorization => {
                generator.into_root_schema_for::<ExecutionAuthorization>()
            }
        }
    }
}

/// Policy snapshot parse / encode failures.
#[derive(Debug, Error)]
pub enum PolicySnapshotError {
    #[error("policy snapshot parse failed: {0}")]
    Parse(#[from] SerdeJsonError),
    #[error("policy document kind mismatch: expected {expected}, got {actual}")]
    ResourceKindMismatch {
        expected: ConfigResourceKind,
        actual: ConfigResourceKind,
    },
    #[error(
        "one or more policy resources do not use boot schema version {POLICY_RESOURCE_SCHEMA_VERSION}"
    )]
    UnsupportedSchemaVersion,
    #[error("immutable profile artifact hashing failed: {0}")]
    ArtifactHash(CanonicalDigestError),
    #[error("policy profile artifact {id} is missing")]
    MissingArtifact { id: ProfileArtifactId },
    #[error("policy profile artifact {id} was loaded more than once")]
    DuplicateArtifact { id: ProfileArtifactId },
    #[error("policy profile artifact {id} kind mismatch: expected {expected}, found {actual}")]
    ArtifactKindMismatch {
        id: ProfileArtifactId,
        expected: ProfileArtifactKind,
        actual: ProfileArtifactKind,
    },
    #[error("policy profile artifact {id} content address does not match its payload")]
    ArtifactIdentityMismatch { id: ProfileArtifactId },
}

impl From<PolicySnapshotError> for ConfigError {
    fn from(error: PolicySnapshotError) -> Self {
        match error {
            PolicySnapshotError::Parse(err) => ConfigValidationReport::single_error(
                ConfigValidationError::invalid_value("policy_snapshot", err.to_string()),
            )
            .into(),
            PolicySnapshotError::ResourceKindMismatch { expected, actual } => {
                ConfigValidationReport::single_error(ConfigValidationError::invalid_value(
                    "resource_kind",
                    format!("expected {expected}, got {actual}"),
                ))
                .into()
            }
            PolicySnapshotError::UnsupportedSchemaVersion => {
                ConfigValidationReport::single_error(ConfigValidationError::invalid_value(
                    "schema_version",
                    format!("every policy resource must use {POLICY_RESOURCE_SCHEMA_VERSION}"),
                ))
                .into()
            }
            PolicySnapshotError::ArtifactHash(error) => ConfigValidationReport::single_error(
                ConfigValidationError::invalid_value("profile_artifacts", error.to_string()),
            )
            .into(),
            PolicySnapshotError::MissingArtifact { id }
            | PolicySnapshotError::DuplicateArtifact { id }
            | PolicySnapshotError::ArtifactIdentityMismatch { id } => {
                ConfigValidationReport::single_error(ConfigValidationError::invalid_value(
                    "profile_artifacts",
                    format!("profile artifact {id} failed persistence validation"),
                ))
                .into()
            }
            PolicySnapshotError::ArtifactKindMismatch {
                id,
                expected,
                actual,
            } => ConfigValidationReport::single_error(ConfigValidationError::invalid_value(
                "profile_artifacts",
                format!("profile artifact {id} kind mismatch: expected {expected}, found {actual}"),
            ))
            .into(),
        }
    }
}

impl From<PolicySnapshotError> for QuantError {
    fn from(error: PolicySnapshotError) -> Self {
        ConfigError::from(error).into()
    }
}

impl DecisionPolicySnapshot {
    pub fn from_json(config_json: &Value) -> Result<Self, PolicySnapshotError> {
        let config: Self = serde_json::from_value(config_json.clone())?;
        if !config.uses_current_resource_schemas() {
            return Err(PolicySnapshotError::UnsupportedSchemaVersion);
        }
        Ok(config)
    }

    /// Encode to the canonical JSON document stored in `decision_policy_snapshot`.
    #[must_use]
    pub fn to_json(&self) -> Value {
        serde_json::to_value(self).unwrap_or(Value::Null)
    }

    #[must_use]
    pub fn json_schema() -> Schema {
        let settings = SchemaSettings::default().with(|s| s.inline_subschemas = true);
        SchemaGenerator::new(settings).into_root_schema_for::<Self>()
    }

    #[must_use]
    pub fn to_masked_json(&self) -> Value {
        self.to_json()
    }

    /// Canonical PIT knowledge lag from enabled report schedules.
    ///
    /// Returns `None` when enabled schedules disagree on `knowledge_lag_secs`.
    #[must_use]
    pub fn pit_knowledge_lag_secs(&self) -> Option<u64> {
        let delays: Vec<u64> = self
            .report_schedule
            .schedules
            .iter()
            .filter(|schedule| schedule.enabled)
            .map(|schedule| schedule.knowledge_lag_secs)
            .collect();
        if delays.is_empty() {
            return self
                .report_schedule
                .schedules
                .first()
                .map(|schedule| schedule.knowledge_lag_secs);
        }
        let first = delays[0];
        if delays.iter().all(|delay| *delay == first) {
            Some(first)
        } else {
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::{DecisionPolicySnapshot, POLICY_RESOURCE_SCHEMA_VERSION, PolicySnapshotError};

    #[test]
    fn default_document_uses_version() {
        assert_eq!(
            DecisionPolicySnapshot::default()
                .recommendation
                .schema_version,
            POLICY_RESOURCE_SCHEMA_VERSION
        );
    }

    #[test]
    fn rejects_non_schema_documents() {
        let mut document = DecisionPolicySnapshot::default().to_json();
        document["recommendation"]["schema_version"] = json!(2);
        let error =
            DecisionPolicySnapshot::from_json(&document).expect_err("non-current must be rejected");
        assert!(matches!(
            error,
            PolicySnapshotError::UnsupportedSchemaVersion
        ));
    }
}
