//! Training dataset shared wire/domain types.

use std::collections::HashSet;

use chrono::{DateTime, Utc};
use sea_orm::FromJsonQueryResult;
use serde::{Deserialize, Deserializer, Serialize, de::Error};

use crate::{
    domain::quant::{FeedbackCohortWindow, FeedbackResolutionEvidence},
    enums::{
        common::MarketCategory,
        model::ModelFamily,
        quant::{
            CohortCensorReason, CohortExclusionReason, DatasetPurpose, FeedbackCohort, OutcomeSide,
        },
    },
    hashing::CanonicalDigest,
    types::{
        ArtifactUri, BookSnapshotRef, ContentHash, DecisionPolicySnapshotId, EventId,
        FactorDefinitionId, FeatureVectorId, HistoryFitSealId, MarketId, MarketSelectionId,
        ModelRunId, ModelSpecId, ModelVersionId, PayoutRatio, ReaderContractVersion,
        RecommendationId, RecommendationReportId, ReportDataQualitySnapshotId,
        ResearchProfileArtifactId, SchemaContractVersion, SchemaVersion, SourceSliceId,
        SourceSliceManifest, SourceSliceManifestRef, TokenId, TradePolicyArtifactId,
        TrainingDatasetId, TrainingExampleId, factor::FactorServingPlane,
    },
};

/// Breaking dataset artifact and manifest wire version.
pub const DATASET_ARTIFACT_FORMAT_VERSION: u32 = 4;
/// Immutable source-lineage document version.
pub const DATASET_SOURCE_LINEAGE_FORMAT_VERSION: u32 = 2;
/// Immutable feedback-cohort manifest version.
pub const DATASET_COHORT_MANIFEST_FORMAT_VERSION: u32 = 1;
/// Immutable included-row artifact version for a `ModelLearning` cohort.
pub const MODEL_LEARNING_COHORT_FORMAT_VERSION: u32 = 1;
/// Immutable included-row artifact version for a complete scored-serving cohort.
pub const MODEL_SCORE_COHORT_FORMAT_VERSION: u32 = 1;

const DATASET_MANIFEST_HASH_DOMAIN: &str = "quant-pivot.dataset-manifest";
const DATASET_SOURCE_SCHEMA_HASH_DOMAIN: &str = "quant-pivot.dataset-source-schema";
const MODEL_LEARNING_COHORT_HASH_DOMAIN: &str = "quant-pivot.model-learning-cohort";
const MODEL_LEARNING_COHORT_ROW_HASH_DOMAIN: &str = "quant-pivot.model-learning-cohort-row";
const MODEL_LEARNING_COHORT_SCHEMA_HASH_DOMAIN: &str = "quant-pivot.model-learning-cohort-schema";
const MODEL_SCORE_COHORT_HASH_DOMAIN: &str = "quant-pivot.model-score-cohort";
const MODEL_SCORE_COHORT_ROW_HASH_DOMAIN: &str = "quant-pivot.model-score-cohort-row";
const MODEL_SCORE_COHORT_SCHEMA_HASH_DOMAIN: &str = "quant-pivot.model-score-cohort-schema";

/// Stable validation failures for immutable dataset lineage.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum DatasetManifestContractError {
    #[error("unsupported {contract} format {actual}; expected {expected}")]
    UnsupportedFormat {
        contract: &'static str,
        expected: u32,
        actual: u32,
    },
    #[error("capability registry hashes must be unique and canonically sorted")]
    NonCanonicalCapabilities,
    #[error("source lineage has invalid time boundaries")]
    InvalidSourceWindow,
    #[error("cohort reason counts must be non-zero, unique, and canonically sorted")]
    NonCanonicalReasonCounts,
    #[error("cohort counts overflow u64")]
    CohortCountOverflow,
    #[error("candidate count must equal eligible plus excluded plus censored")]
    CohortCountMismatch,
    #[error("included cohort rows cannot exceed eligible rows")]
    IncludedCountMismatch,
    #[error("cohort artifact row count must equal the included count")]
    CohortArtifactCountMismatch,
    #[error("cohort window must be non-empty")]
    InvalidCohortWindow,
    #[error("dataset window must be non-empty")]
    InvalidDatasetWindow,
    #[error("source lineage does not cover the dataset window and cutoff")]
    SourceCoverageMismatch,
    #[error("trade-policy artifact id and hash must either both exist or both be absent")]
    TradePolicyBindingMismatch,
    #[error("sampling cadence, lag, and horizons must be positive and canonical")]
    InvalidSamplingContract,
    #[error("Evaluation datasets require an immutable cohort manifest")]
    MissingEvaluationCohort,
    #[error("PolicyFit datasets cannot consume a feedback cohort")]
    PolicyFitCohortForbidden,
    #[error("cohort profile does not match source lineage")]
    CohortProfileMismatch,
    #[error("cohort window does not match the dataset window")]
    CohortWindowMismatch,
    #[error("cohort capability lineage does not match source lineage")]
    CohortCapabilityMismatch,
    #[error("dataset sample count does not match included cohort rows")]
    CohortSampleCountMismatch,
    #[error("dataset sample count exceeds PostgreSQL bigint")]
    SampleCountOverflow,
    #[error("dataset coverage built-example count does not match manifest sample count")]
    CoverageSampleCountMismatch,
    #[error("dataset completion status and failure detail are inconsistent")]
    InvalidCompletionStatus,
    #[error("dataset source lineage does not match normalized frozen-plan fields")]
    FrozenPlanMismatch,
    #[error("feedback cohort discriminator does not match its manifest")]
    CohortDiscriminatorMismatch,
    #[error("source-slice manifest does not match the frozen dataset lineage")]
    SourceManifestMismatch,
    #[error("model-learning cohort rows are not strictly ordered and unique")]
    NonCanonicalCohortRows,
    #[error("model-learning cohort row identity or payout projection is invalid")]
    InvalidModelLearningRow,
    #[error("model-learning cohort artifact counts do not match its rows")]
    ModelLearningArtifactCountMismatch,
    #[error("model-score cohort rows are not strictly ordered and unique")]
    NonCanonicalModelScoreRows,
    #[error("model-score cohort row identity or serving lineage is invalid")]
    InvalidModelScoreRow,
    #[error("model-score cohort artifact counts do not match its rows")]
    ModelScoreArtifactCountMismatch,
    #[error("dataset factor serving plane is invalid: {detail}")]
    InvalidFactorPlane { detail: String },
    #[error("dataset model family and factor serving plane disagree")]
    FactorPlaneFamilyMismatch,
    #[error("dataset factor serving plane does not bind its feature schema hash")]
    FactorPlaneFeatureMismatch,
    #[error("dataset factor serving plane does not bind its feature schema version")]
    FactorPlaneSchemaMismatch,
}

/// Canonically ordered set of exact capability-registry versions.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, FromJsonQueryResult)]
#[serde(transparent)]
pub struct CapabilityRegistryHashes(Vec<ContentHash>);

impl CapabilityRegistryHashes {
    pub fn try_new(hashes: Vec<ContentHash>) -> Result<Self, DatasetManifestContractError> {
        if hashes.windows(2).any(|pair| pair[0] >= pair[1]) {
            return Err(DatasetManifestContractError::NonCanonicalCapabilities);
        }
        Ok(Self(hashes))
    }

    #[must_use]
    pub fn as_slice(&self) -> &[ContentHash] {
        &self.0
    }
}

impl<'de> Deserialize<'de> for CapabilityRegistryHashes {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        Self::try_new(Vec::<ContentHash>::deserialize(deserializer)?).map_err(D::Error::custom)
    }
}

/// Exact, server-derived Source Slice lineage frozen before materialization.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, FromJsonQueryResult)]
#[serde(deny_unknown_fields, try_from = "DatasetSourceLineageDocument")]
pub struct DatasetSourceLineage {
    pub format_version: u32,
    pub source_slice_id: SourceSliceId,
    pub source_slice_identity_hash: ContentHash,
    pub research_profile_artifact_id: ResearchProfileArtifactId,
    pub research_program_hash: ContentHash,
    pub source_slice: SourceSliceManifestRef,
    pub source_window_start: DateTime<Utc>,
    pub source_window_end: DateTime<Utc>,
    pub pit_cutoff: DateTime<Utc>,
    pub decision_policy_snapshot_id: DecisionPolicySnapshotId,
    pub runtime_config_hash: ContentHash,
    pub fit_seal_id: HistoryFitSealId,
    pub fit_seal_hash: ContentHash,
    pub reader_contract_version: ReaderContractVersion,
    pub schema_contract_version: SchemaContractVersion,
    pub source_schema_hash: ContentHash,
    pub capability_registry_hashes: CapabilityRegistryHashes,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct DatasetSourceLineageDocument {
    format_version: u32,
    source_slice_id: SourceSliceId,
    source_slice_identity_hash: ContentHash,
    research_profile_artifact_id: ResearchProfileArtifactId,
    research_program_hash: ContentHash,
    source_slice: SourceSliceManifestRef,
    source_window_start: DateTime<Utc>,
    source_window_end: DateTime<Utc>,
    pit_cutoff: DateTime<Utc>,
    decision_policy_snapshot_id: DecisionPolicySnapshotId,
    runtime_config_hash: ContentHash,
    fit_seal_id: HistoryFitSealId,
    fit_seal_hash: ContentHash,
    reader_contract_version: ReaderContractVersion,
    schema_contract_version: SchemaContractVersion,
    source_schema_hash: ContentHash,
    capability_registry_hashes: CapabilityRegistryHashes,
}

impl DatasetSourceLineage {
    pub fn validate(&self) -> Result<(), DatasetManifestContractError> {
        if self.format_version != DATASET_SOURCE_LINEAGE_FORMAT_VERSION {
            return Err(DatasetManifestContractError::UnsupportedFormat {
                contract: "dataset source lineage",
                expected: DATASET_SOURCE_LINEAGE_FORMAT_VERSION,
                actual: self.format_version,
            });
        }
        if self.source_window_start >= self.source_window_end
            || self.source_window_end > self.pit_cutoff
        {
            return Err(DatasetManifestContractError::InvalidSourceWindow);
        }
        CapabilityRegistryHashes::try_new(self.capability_registry_hashes.0.clone())?;
        Ok(())
    }

    pub fn derive_schema_hash(
        manifest: &SourceSliceManifest,
    ) -> Result<ContentHash, DatasetManifestContractError> {
        manifest
            .validate()
            .map_err(|_| DatasetManifestContractError::InvalidSourceWindow)?;
        let schemas = manifest
            .objects
            .iter()
            .map(|object| (object.kind, object.schema_hash))
            .collect::<Vec<_>>();
        CanonicalDigest::content_hash_typed(
            DATASET_SOURCE_SCHEMA_HASH_DOMAIN,
            DATASET_SOURCE_LINEAGE_FORMAT_VERSION,
            &(
                &manifest.reader_contract_version,
                &manifest.schema_contract_version,
                manifest.dataset_format_version,
                schemas,
            ),
        )
        .map_err(|_| DatasetManifestContractError::InvalidSourceWindow)
    }

    pub fn verify_manifest(
        &self,
        manifest: &SourceSliceManifest,
    ) -> Result<(), DatasetManifestContractError> {
        self.validate()?;
        let manifest_hash = manifest
            .content_hash()
            .map_err(|_| DatasetManifestContractError::SourceManifestMismatch)?;
        let schema_hash = Self::derive_schema_hash(manifest)?;
        if manifest_hash != self.source_slice.manifest_hash
            || manifest.profile_ref != self.research_profile_artifact_id.profile_ref()
            || manifest.research_program_hash != self.research_program_hash
            || manifest.window_start != self.source_window_start
            || manifest.window_end != self.source_window_end
            || manifest.pit_cutoff != self.pit_cutoff
            || manifest.decision_policy_snapshot_id != self.decision_policy_snapshot_id
            || manifest.runtime_config_hash != self.runtime_config_hash
            || manifest.fit_seal_id != self.fit_seal_id
            || manifest.fit_seal_hash != self.fit_seal_hash
            || manifest.reader_contract_version != self.reader_contract_version
            || manifest.schema_contract_version != self.schema_contract_version
            || manifest.capability_registry_hashes != self.capability_registry_hashes
            || schema_hash != self.source_schema_hash
        {
            return Err(DatasetManifestContractError::SourceManifestMismatch);
        }
        Ok(())
    }
}

impl TryFrom<DatasetSourceLineageDocument> for DatasetSourceLineage {
    type Error = DatasetManifestContractError;

    fn try_from(document: DatasetSourceLineageDocument) -> Result<Self, Self::Error> {
        let lineage = Self {
            format_version: document.format_version,
            source_slice_id: document.source_slice_id,
            source_slice_identity_hash: document.source_slice_identity_hash,
            research_profile_artifact_id: document.research_profile_artifact_id,
            research_program_hash: document.research_program_hash,
            source_slice: document.source_slice,
            source_window_start: document.source_window_start,
            source_window_end: document.source_window_end,
            pit_cutoff: document.pit_cutoff,
            decision_policy_snapshot_id: document.decision_policy_snapshot_id,
            runtime_config_hash: document.runtime_config_hash,
            fit_seal_id: document.fit_seal_id,
            fit_seal_hash: document.fit_seal_hash,
            reader_contract_version: document.reader_contract_version,
            schema_contract_version: document.schema_contract_version,
            source_schema_hash: document.source_schema_hash,
            capability_registry_hashes: document.capability_registry_hashes,
        };
        lineage.validate()?;
        Ok(lineage)
    }
}

/// Stable exclusion bucket and count for one frozen cohort.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CohortExclusionCount {
    pub reason: CohortExclusionReason,
    pub count: u64,
}

/// Stable censor bucket and count for one frozen cohort.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CohortCensorCount {
    pub reason: CohortCensorReason,
    pub count: u64,
}

/// Reconciled candidate-classification counts for one frozen cohort.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(try_from = "DatasetCohortCountsDocument")]
pub struct DatasetCohortCounts {
    candidate_count: u64,
    eligible_count: u64,
    included_count: u64,
    exclusion_counts: Vec<CohortExclusionCount>,
    censor_counts: Vec<CohortCensorCount>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct DatasetCohortCountsDocument {
    candidate_count: u64,
    eligible_count: u64,
    included_count: u64,
    exclusion_counts: Vec<CohortExclusionCount>,
    censor_counts: Vec<CohortCensorCount>,
}

impl DatasetCohortCounts {
    pub fn try_new(
        candidate_count: u64,
        eligible_count: u64,
        included_count: u64,
        exclusion_counts: Vec<CohortExclusionCount>,
        censor_counts: Vec<CohortCensorCount>,
    ) -> Result<Self, DatasetManifestContractError> {
        let counts = Self {
            candidate_count,
            eligible_count,
            included_count,
            exclusion_counts,
            censor_counts,
        };
        counts.validate()?;
        Ok(counts)
    }

    pub fn validate(&self) -> Result<(), DatasetManifestContractError> {
        if self.exclusion_counts.iter().any(|entry| entry.count == 0)
            || self
                .exclusion_counts
                .windows(2)
                .any(|pair| pair[0].reason.as_str() >= pair[1].reason.as_str())
            || self.censor_counts.iter().any(|entry| entry.count == 0)
            || self
                .censor_counts
                .windows(2)
                .any(|pair| pair[0].reason.as_str() >= pair[1].reason.as_str())
        {
            return Err(DatasetManifestContractError::NonCanonicalReasonCounts);
        }
        let excluded = self
            .exclusion_counts
            .iter()
            .try_fold(0_u64, |total, entry| total.checked_add(entry.count))
            .ok_or(DatasetManifestContractError::CohortCountOverflow)?;
        let censored = self
            .censor_counts
            .iter()
            .try_fold(0_u64, |total, entry| total.checked_add(entry.count))
            .ok_or(DatasetManifestContractError::CohortCountOverflow)?;
        let classified = self
            .eligible_count
            .checked_add(excluded)
            .and_then(|total| total.checked_add(censored))
            .ok_or(DatasetManifestContractError::CohortCountOverflow)?;
        if classified != self.candidate_count {
            return Err(DatasetManifestContractError::CohortCountMismatch);
        }
        if self.included_count > self.eligible_count {
            return Err(DatasetManifestContractError::IncludedCountMismatch);
        }
        Ok(())
    }

    #[must_use]
    pub const fn candidate_count(&self) -> u64 {
        self.candidate_count
    }

    #[must_use]
    pub const fn eligible_count(&self) -> u64 {
        self.eligible_count
    }

    #[must_use]
    pub const fn included_count(&self) -> u64 {
        self.included_count
    }

    #[must_use]
    pub fn exclusion_counts(&self) -> &[CohortExclusionCount] {
        &self.exclusion_counts
    }

    #[must_use]
    pub fn censor_counts(&self) -> &[CohortCensorCount] {
        &self.censor_counts
    }
}

impl TryFrom<DatasetCohortCountsDocument> for DatasetCohortCounts {
    type Error = DatasetManifestContractError;

    fn try_from(document: DatasetCohortCountsDocument) -> Result<Self, Self::Error> {
        Self::try_new(
            document.candidate_count,
            document.eligible_count,
            document.included_count,
            document.exclusion_counts,
            document.censor_counts,
        )
    }
}

/// One eligible `ModelLearning` recommendation and its exact serving lineage.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NewModelLearningCohortRow {
    pub recommendation_id: RecommendationId,
    pub recommendation_report_id: RecommendationReportId,
    pub category: MarketCategory,
    pub market_id: MarketId,
    pub event_id: EventId,
    pub recommendation_token_id: TokenId,
    pub model_token_id: TokenId,
    pub outcome_side: OutcomeSide,
    pub decision_at: DateTime<Utc>,
    pub candidate_available_at: DateTime<Utc>,
    pub decision_policy_snapshot_id: DecisionPolicySnapshotId,
    pub market_selection_id: MarketSelectionId,
    pub feature_vector_id: FeatureVectorId,
    pub model_run_id: ModelRunId,
    pub model_version_id: ModelVersionId,
    pub factor_definition_versions: Vec<FactorDefinitionId>,
    pub book_snapshot_ref: BookSnapshotRef,
    pub data_quality_snapshot_id: ReportDataQualitySnapshotId,
    pub resolution: FeedbackResolutionEvidence,
    pub model_token_payout_ratio: PayoutRatio,
}

/// One eligible `ModelLearning` recommendation and its exact serving lineage.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ModelLearningCohortRow {
    pub example_id: TrainingExampleId,
    pub recommendation_id: RecommendationId,
    pub recommendation_report_id: RecommendationReportId,
    pub category: MarketCategory,
    pub market_id: MarketId,
    pub event_id: EventId,
    pub recommendation_token_id: TokenId,
    pub model_token_id: TokenId,
    pub outcome_side: OutcomeSide,
    pub decision_at: DateTime<Utc>,
    pub candidate_available_at: DateTime<Utc>,
    pub decision_policy_snapshot_id: DecisionPolicySnapshotId,
    pub market_selection_id: MarketSelectionId,
    pub feature_vector_id: FeatureVectorId,
    pub model_run_id: ModelRunId,
    pub model_version_id: ModelVersionId,
    pub factor_definition_versions: Vec<FactorDefinitionId>,
    pub book_snapshot_ref: BookSnapshotRef,
    pub data_quality_snapshot_id: ReportDataQualitySnapshotId,
    pub resolution: FeedbackResolutionEvidence,
    /// Label projected onto the feature vector's primary model token.
    pub model_token_payout_ratio: PayoutRatio,
}

impl ModelLearningCohortRow {
    pub fn try_seal(row: NewModelLearningCohortRow) -> Result<Self, DatasetManifestContractError> {
        let example_id = Self::derive_example_id(
            row.recommendation_id,
            row.feature_vector_id,
            row.model_run_id,
            row.model_version_id,
            &row.model_token_id,
            row.resolution.outcome_hash,
        )?;
        let sealed = Self {
            example_id,
            recommendation_id: row.recommendation_id,
            recommendation_report_id: row.recommendation_report_id,
            category: row.category,
            market_id: row.market_id,
            event_id: row.event_id,
            recommendation_token_id: row.recommendation_token_id,
            model_token_id: row.model_token_id,
            outcome_side: row.outcome_side,
            decision_at: row.decision_at,
            candidate_available_at: row.candidate_available_at,
            decision_policy_snapshot_id: row.decision_policy_snapshot_id,
            market_selection_id: row.market_selection_id,
            feature_vector_id: row.feature_vector_id,
            model_run_id: row.model_run_id,
            model_version_id: row.model_version_id,
            factor_definition_versions: row.factor_definition_versions,
            book_snapshot_ref: row.book_snapshot_ref,
            data_quality_snapshot_id: row.data_quality_snapshot_id,
            resolution: row.resolution,
            model_token_payout_ratio: row.model_token_payout_ratio,
        };
        sealed.validate()?;
        Ok(sealed)
    }

    /// Derive the deterministic row identity from immutable recommendation evidence.
    pub fn expected_example_id(&self) -> Result<TrainingExampleId, DatasetManifestContractError> {
        Self::derive_example_id(
            self.recommendation_id,
            self.feature_vector_id,
            self.model_run_id,
            self.model_version_id,
            &self.model_token_id,
            self.resolution.outcome_hash,
        )
    }

    fn derive_example_id(
        recommendation_id: RecommendationId,
        feature_vector_id: FeatureVectorId,
        model_run_id: ModelRunId,
        model_version_id: ModelVersionId,
        model_token_id: &TokenId,
        outcome_hash: ContentHash,
    ) -> Result<TrainingExampleId, DatasetManifestContractError> {
        #[derive(Serialize)]
        struct ExampleIdentity<'a> {
            recommendation_id: RecommendationId,
            feature_vector_id: FeatureVectorId,
            model_run_id: ModelRunId,
            model_version_id: ModelVersionId,
            model_token_id: &'a TokenId,
            outcome_hash: ContentHash,
        }

        let hash = CanonicalDigest::content_hash_typed(
            MODEL_LEARNING_COHORT_ROW_HASH_DOMAIN,
            MODEL_LEARNING_COHORT_FORMAT_VERSION,
            &ExampleIdentity {
                recommendation_id,
                feature_vector_id,
                model_run_id,
                model_version_id,
                model_token_id,
                outcome_hash,
            },
        )
        .map_err(|_| DatasetManifestContractError::InvalidModelLearningRow)?;
        Ok(TrainingExampleId::from_content_hash(&hash))
    }

    pub fn validate(&self) -> Result<(), DatasetManifestContractError> {
        let projected_payout = match self.outcome_side {
            OutcomeSide::Yes if self.model_token_id == self.recommendation_token_id => {
                self.resolution.token_payout_ratio
            }
            OutcomeSide::No if self.model_token_id != self.recommendation_token_id => {
                self.resolution.token_payout_ratio.complement()
            }
            OutcomeSide::Yes | OutcomeSide::No => {
                return Err(DatasetManifestContractError::InvalidModelLearningRow);
            }
        };
        let unique_factors = self
            .factor_definition_versions
            .iter()
            .copied()
            .collect::<HashSet<_>>();
        if self.example_id != self.expected_example_id()?
            || unique_factors.len() != self.factor_definition_versions.len()
            || self.decision_at > self.candidate_available_at
            || self.decision_at >= self.resolution.resolved_at
            || self.resolution.resolved_at > self.resolution.available_at
            || projected_payout != self.model_token_payout_ratio
        {
            return Err(DatasetManifestContractError::InvalidModelLearningRow);
        }
        Ok(())
    }
}

/// Canonical included rows sealed before Dataset materialization.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ModelLearningCohortArtifact {
    pub format_version: u32,
    pub window: FeedbackCohortWindow,
    pub counts: DatasetCohortCounts,
    pub rows: Vec<ModelLearningCohortRow>,
}

impl ModelLearningCohortArtifact {
    pub fn validate(&self) -> Result<(), DatasetManifestContractError> {
        if self.format_version != MODEL_LEARNING_COHORT_FORMAT_VERSION {
            return Err(DatasetManifestContractError::UnsupportedFormat {
                contract: "model-learning cohort artifact",
                expected: MODEL_LEARNING_COHORT_FORMAT_VERSION,
                actual: self.format_version,
            });
        }
        self.counts.validate()?;
        if self.counts.included_count()
            != u64::try_from(self.rows.len())
                .map_err(|_| DatasetManifestContractError::ModelLearningArtifactCountMismatch)?
            || self.counts.eligible_count() != self.counts.included_count()
        {
            return Err(DatasetManifestContractError::ModelLearningArtifactCountMismatch);
        }
        if self.rows.windows(2).any(|pair| {
            (
                pair[0].candidate_available_at,
                pair[0].recommendation_id.as_uuid(),
            ) >= (
                pair[1].candidate_available_at,
                pair[1].recommendation_id.as_uuid(),
            )
        }) {
            return Err(DatasetManifestContractError::NonCanonicalCohortRows);
        }
        self.rows
            .iter()
            .try_for_each(ModelLearningCohortRow::validate)
    }

    pub fn source_hash(&self) -> Result<ContentHash, DatasetManifestContractError> {
        self.validate()?;
        CanonicalDigest::content_hash_typed(
            MODEL_LEARNING_COHORT_HASH_DOMAIN,
            MODEL_LEARNING_COHORT_FORMAT_VERSION,
            self,
        )
        .map_err(|_| DatasetManifestContractError::InvalidModelLearningRow)
    }

    pub fn schema_hash() -> Result<ContentHash, DatasetManifestContractError> {
        CanonicalDigest::content_hash_typed(
            MODEL_LEARNING_COHORT_SCHEMA_HASH_DOMAIN,
            MODEL_LEARNING_COHORT_FORMAT_VERSION,
            &[
                "example_id",
                "recommendation_id",
                "recommendation_report_id",
                "category",
                "market_id",
                "event_id",
                "recommendation_token_id",
                "model_token_id",
                "outcome_side",
                "decision_at",
                "candidate_available_at",
                "decision_policy_snapshot_id",
                "market_selection_id",
                "feature_vector_id",
                "model_run_id",
                "model_version_id",
                "factor_definition_versions",
                "book_snapshot_ref",
                "data_quality_snapshot_id",
                "resolution",
                "model_token_payout_ratio",
            ],
        )
        .map_err(|_| DatasetManifestContractError::InvalidModelLearningRow)
    }
}

/// One resolved sample from the complete population that reached model scoring.
///
/// Unlike [`ModelLearningCohortRow`], this row is deliberately independent of
/// recommendation publication. Its lineage proves that the feature vector was
/// admitted by a completed serving run and conserved by the report funnel, so
/// abstentions remain part of the training population.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NewModelScoreCohortRow {
    pub recommendation_report_id: RecommendationReportId,
    pub category: MarketCategory,
    pub market_id: MarketId,
    pub event_id: EventId,
    pub model_token_id: TokenId,
    pub decision_at: DateTime<Utc>,
    pub serving_evidence_available_at: DateTime<Utc>,
    pub decision_policy_snapshot_id: DecisionPolicySnapshotId,
    pub market_selection_id: MarketSelectionId,
    pub feature_vector_id: FeatureVectorId,
    pub model_run_id: ModelRunId,
    pub model_version_id: ModelVersionId,
    pub model_family: ModelFamily,
    pub factor_definition_versions: Vec<FactorDefinitionId>,
    pub book_snapshot_ref: BookSnapshotRef,
    pub data_quality_snapshot_id: ReportDataQualitySnapshotId,
    pub resolution: FeedbackResolutionEvidence,
    pub model_token_payout_ratio: PayoutRatio,
    pub serving_completion_hash: ContentHash,
    pub model_input_rows_hash: ContentHash,
    pub input_contract_hash: ContentHash,
    pub transform_hash: ContentHash,
    pub training_input_hash: ContentHash,
    pub funnel_row_hash: ContentHash,
}

/// Immutable scored-serving sample and its complete prediction-to-label lineage.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ModelScoreCohortRow {
    pub example_id: TrainingExampleId,
    pub recommendation_report_id: RecommendationReportId,
    pub category: MarketCategory,
    pub market_id: MarketId,
    pub event_id: EventId,
    pub model_token_id: TokenId,
    pub decision_at: DateTime<Utc>,
    pub serving_evidence_available_at: DateTime<Utc>,
    pub decision_policy_snapshot_id: DecisionPolicySnapshotId,
    pub market_selection_id: MarketSelectionId,
    pub feature_vector_id: FeatureVectorId,
    pub model_run_id: ModelRunId,
    pub model_version_id: ModelVersionId,
    pub model_family: ModelFamily,
    pub factor_definition_versions: Vec<FactorDefinitionId>,
    pub book_snapshot_ref: BookSnapshotRef,
    pub data_quality_snapshot_id: ReportDataQualitySnapshotId,
    pub resolution: FeedbackResolutionEvidence,
    pub model_token_payout_ratio: PayoutRatio,
    pub serving_completion_hash: ContentHash,
    pub model_input_rows_hash: ContentHash,
    pub input_contract_hash: ContentHash,
    pub transform_hash: ContentHash,
    pub training_input_hash: ContentHash,
    pub funnel_row_hash: ContentHash,
}

impl ModelScoreCohortRow {
    pub fn try_seal(row: NewModelScoreCohortRow) -> Result<Self, DatasetManifestContractError> {
        let example_id = Self::derive_example_id(&row)?;
        let sealed = Self {
            example_id,
            recommendation_report_id: row.recommendation_report_id,
            category: row.category,
            market_id: row.market_id,
            event_id: row.event_id,
            model_token_id: row.model_token_id,
            decision_at: row.decision_at,
            serving_evidence_available_at: row.serving_evidence_available_at,
            decision_policy_snapshot_id: row.decision_policy_snapshot_id,
            market_selection_id: row.market_selection_id,
            feature_vector_id: row.feature_vector_id,
            model_run_id: row.model_run_id,
            model_version_id: row.model_version_id,
            model_family: row.model_family,
            factor_definition_versions: row.factor_definition_versions,
            book_snapshot_ref: row.book_snapshot_ref,
            data_quality_snapshot_id: row.data_quality_snapshot_id,
            resolution: row.resolution,
            model_token_payout_ratio: row.model_token_payout_ratio,
            serving_completion_hash: row.serving_completion_hash,
            model_input_rows_hash: row.model_input_rows_hash,
            input_contract_hash: row.input_contract_hash,
            transform_hash: row.transform_hash,
            training_input_hash: row.training_input_hash,
            funnel_row_hash: row.funnel_row_hash,
        };
        sealed.validate()?;
        Ok(sealed)
    }

    pub fn expected_example_id(&self) -> Result<TrainingExampleId, DatasetManifestContractError> {
        Self::derive_example_id(&NewModelScoreCohortRow {
            recommendation_report_id: self.recommendation_report_id,
            category: self.category,
            market_id: self.market_id.clone(),
            event_id: self.event_id.clone(),
            model_token_id: self.model_token_id.clone(),
            decision_at: self.decision_at,
            serving_evidence_available_at: self.serving_evidence_available_at,
            decision_policy_snapshot_id: self.decision_policy_snapshot_id,
            market_selection_id: self.market_selection_id,
            feature_vector_id: self.feature_vector_id,
            model_run_id: self.model_run_id,
            model_version_id: self.model_version_id,
            model_family: self.model_family,
            factor_definition_versions: self.factor_definition_versions.clone(),
            book_snapshot_ref: self.book_snapshot_ref.clone(),
            data_quality_snapshot_id: self.data_quality_snapshot_id,
            resolution: self.resolution.clone(),
            model_token_payout_ratio: self.model_token_payout_ratio,
            serving_completion_hash: self.serving_completion_hash,
            model_input_rows_hash: self.model_input_rows_hash,
            input_contract_hash: self.input_contract_hash,
            transform_hash: self.transform_hash,
            training_input_hash: self.training_input_hash,
            funnel_row_hash: self.funnel_row_hash,
        })
    }

    fn derive_example_id(
        row: &NewModelScoreCohortRow,
    ) -> Result<TrainingExampleId, DatasetManifestContractError> {
        #[derive(Serialize)]
        struct ExampleIdentity<'a> {
            recommendation_report_id: RecommendationReportId,
            feature_vector_id: FeatureVectorId,
            model_run_id: ModelRunId,
            model_version_id: ModelVersionId,
            model_token_id: &'a TokenId,
            resolution_hash: ContentHash,
            serving_completion_hash: ContentHash,
            funnel_row_hash: ContentHash,
        }

        let hash = CanonicalDigest::content_hash_typed(
            MODEL_SCORE_COHORT_ROW_HASH_DOMAIN,
            MODEL_SCORE_COHORT_FORMAT_VERSION,
            &ExampleIdentity {
                recommendation_report_id: row.recommendation_report_id,
                feature_vector_id: row.feature_vector_id,
                model_run_id: row.model_run_id,
                model_version_id: row.model_version_id,
                model_token_id: &row.model_token_id,
                resolution_hash: row.resolution.outcome_hash,
                serving_completion_hash: row.serving_completion_hash,
                funnel_row_hash: row.funnel_row_hash,
            },
        )
        .map_err(|_| DatasetManifestContractError::InvalidModelScoreRow)?;
        Ok(TrainingExampleId::from_content_hash(&hash))
    }

    pub fn validate(&self) -> Result<(), DatasetManifestContractError> {
        if self.example_id != self.expected_example_id()?
            || self
                .factor_definition_versions
                .windows(2)
                .any(|pair| pair[0].as_uuid() >= pair[1].as_uuid())
            || self.decision_at > self.serving_evidence_available_at
            || self.serving_evidence_available_at >= self.resolution.resolved_at
            || self.resolution.resolved_at > self.resolution.available_at
            || self.model_token_payout_ratio != self.resolution.token_payout_ratio
        {
            return Err(DatasetManifestContractError::InvalidModelScoreRow);
        }
        Ok(())
    }
}

/// Canonical complete-scoring cohort sealed before Dataset materialization.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ModelScoreCohortArtifact {
    pub format_version: u32,
    pub window: FeedbackCohortWindow,
    pub counts: DatasetCohortCounts,
    pub rows: Vec<ModelScoreCohortRow>,
}

impl ModelScoreCohortArtifact {
    pub fn validate(&self) -> Result<(), DatasetManifestContractError> {
        if self.format_version != MODEL_SCORE_COHORT_FORMAT_VERSION {
            return Err(DatasetManifestContractError::UnsupportedFormat {
                contract: "model-score cohort artifact",
                expected: MODEL_SCORE_COHORT_FORMAT_VERSION,
                actual: self.format_version,
            });
        }
        self.counts.validate()?;
        if self.counts.included_count()
            != u64::try_from(self.rows.len())
                .map_err(|_| DatasetManifestContractError::ModelScoreArtifactCountMismatch)?
            || self.counts.eligible_count() != self.counts.included_count()
        {
            return Err(DatasetManifestContractError::ModelScoreArtifactCountMismatch);
        }
        if self.rows.windows(2).any(|pair| {
            (
                pair[0].serving_evidence_available_at,
                pair[0].recommendation_report_id.as_uuid(),
                pair[0].feature_vector_id.as_uuid(),
            ) >= (
                pair[1].serving_evidence_available_at,
                pair[1].recommendation_report_id.as_uuid(),
                pair[1].feature_vector_id.as_uuid(),
            )
        }) {
            return Err(DatasetManifestContractError::NonCanonicalModelScoreRows);
        }
        self.rows.iter().try_for_each(ModelScoreCohortRow::validate)
    }

    pub fn source_hash(&self) -> Result<ContentHash, DatasetManifestContractError> {
        self.validate()?;
        CanonicalDigest::content_hash_typed(
            MODEL_SCORE_COHORT_HASH_DOMAIN,
            MODEL_SCORE_COHORT_FORMAT_VERSION,
            self,
        )
        .map_err(|_| DatasetManifestContractError::InvalidModelScoreRow)
    }

    pub fn schema_hash() -> Result<ContentHash, DatasetManifestContractError> {
        CanonicalDigest::content_hash_typed(
            MODEL_SCORE_COHORT_SCHEMA_HASH_DOMAIN,
            MODEL_SCORE_COHORT_FORMAT_VERSION,
            &[
                "example_id",
                "recommendation_report_id",
                "category",
                "market_id",
                "event_id",
                "model_token_id",
                "decision_at",
                "serving_evidence_available_at",
                "decision_policy_snapshot_id",
                "market_selection_id",
                "feature_vector_id",
                "model_run_id",
                "model_version_id",
                "model_family",
                "factor_definition_versions",
                "book_snapshot_ref",
                "data_quality_snapshot_id",
                "resolution",
                "model_token_payout_ratio",
                "serving_completion_hash",
                "model_input_rows_hash",
                "input_contract_hash",
                "transform_hash",
                "training_input_hash",
                "funnel_row_hash",
            ],
        )
        .map_err(|_| DatasetManifestContractError::InvalidModelScoreRow)
    }
}

/// Content-addressed sealed rows consumed by one dataset.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DatasetCohortArtifactRef {
    pub uri: ArtifactUri,
    pub bytes_hash: ContentHash,
    pub schema_hash: ContentHash,
    pub source_hash: ContentHash,
    pub row_count: u64,
}

/// Immutable cohort definition and reconciliation evidence.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, FromJsonQueryResult)]
#[serde(deny_unknown_fields, try_from = "DatasetCohortManifestDocument")]
pub struct DatasetCohortManifest {
    pub format_version: u32,
    pub cohort: FeedbackCohort,
    pub window: FeedbackCohortWindow,
    pub artifact: DatasetCohortArtifactRef,
    pub counts: DatasetCohortCounts,
    pub capability_registry_hashes: CapabilityRegistryHashes,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct DatasetCohortManifestDocument {
    format_version: u32,
    cohort: FeedbackCohort,
    window: FeedbackCohortWindow,
    artifact: DatasetCohortArtifactRef,
    counts: DatasetCohortCounts,
    capability_registry_hashes: CapabilityRegistryHashes,
}

impl DatasetCohortManifest {
    pub fn validate(&self) -> Result<(), DatasetManifestContractError> {
        if self.format_version != DATASET_COHORT_MANIFEST_FORMAT_VERSION {
            return Err(DatasetManifestContractError::UnsupportedFormat {
                contract: "dataset cohort manifest",
                expected: DATASET_COHORT_MANIFEST_FORMAT_VERSION,
                actual: self.format_version,
            });
        }
        if self.window.window_start() >= self.window.cutoff() {
            return Err(DatasetManifestContractError::InvalidCohortWindow);
        }
        self.counts.validate()?;
        CapabilityRegistryHashes::try_new(self.capability_registry_hashes.0.clone())?;
        if self.artifact.row_count != self.counts.included_count {
            return Err(DatasetManifestContractError::CohortArtifactCountMismatch);
        }
        Ok(())
    }
}

impl TryFrom<DatasetCohortManifestDocument> for DatasetCohortManifest {
    type Error = DatasetManifestContractError;

    fn try_from(document: DatasetCohortManifestDocument) -> Result<Self, Self::Error> {
        let manifest = Self {
            format_version: document.format_version,
            cohort: document.cohort,
            window: document.window,
            artifact: document.artifact,
            counts: document.counts,
            capability_registry_hashes: document.capability_registry_hashes,
        };
        manifest.validate()?;
        Ok(manifest)
    }
}

/// Immutable manifest embedded in a frozen dataset artifact and ledger.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, FromJsonQueryResult)]
#[serde(deny_unknown_fields, try_from = "DatasetManifestDocument")]
pub struct DatasetManifest {
    pub format_version: u32,
    pub training_dataset_id: TrainingDatasetId,
    pub source_lineage: DatasetSourceLineage,
    pub cohort_manifest: Option<DatasetCohortManifest>,
    pub model_spec_id: ModelSpecId,
    pub model_family: ModelFamily,
    /// Immutable semantic definition bound before dataset materialization.
    pub model_spec_definition_hash: ContentHash,
    pub trade_policy_artifact_id: Option<TradePolicyArtifactId>,
    pub trade_policy_hash: Option<ContentHash>,
    pub window_start: DateTime<Utc>,
    pub window_end: DateTime<Utc>,
    pub purpose: DatasetPurpose,
    pub knowledge_lag_secs: u64,
    pub sample_interval_secs: u64,
    pub horizons_secs: Vec<u64>,
    pub feature_schema_version: SchemaVersion,
    pub feature_schema_hash: ContentHash,
    /// Complete immutable factor revision set. Its scalar schema hash is a
    /// derived persistence/index projection, never an independent owner.
    pub factor_serving_plane: FactorServingPlane,
    pub label_schema_hash: ContentHash,
    pub semantic_dataset_hash: ContentHash,
    pub source_fingerprint: ContentHash,
    pub sample_count: u64,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct DatasetManifestDocument {
    format_version: u32,
    training_dataset_id: TrainingDatasetId,
    source_lineage: DatasetSourceLineage,
    cohort_manifest: Option<DatasetCohortManifest>,
    model_spec_id: ModelSpecId,
    model_family: ModelFamily,
    model_spec_definition_hash: ContentHash,
    trade_policy_artifact_id: Option<TradePolicyArtifactId>,
    trade_policy_hash: Option<ContentHash>,
    window_start: DateTime<Utc>,
    window_end: DateTime<Utc>,
    purpose: DatasetPurpose,
    knowledge_lag_secs: u64,
    sample_interval_secs: u64,
    horizons_secs: Vec<u64>,
    feature_schema_version: SchemaVersion,
    feature_schema_hash: ContentHash,
    factor_serving_plane: FactorServingPlane,
    label_schema_hash: ContentHash,
    semantic_dataset_hash: ContentHash,
    source_fingerprint: ContentHash,
    sample_count: u64,
}

impl DatasetManifest {
    /// Strict scalar projection used by relational indexes and joins.
    #[must_use]
    pub const fn factor_schema_hash(&self) -> ContentHash {
        self.factor_serving_plane.factor_schema_hash()
    }

    pub fn validate(&self) -> Result<(), DatasetManifestContractError> {
        if self.format_version != DATASET_ARTIFACT_FORMAT_VERSION {
            return Err(DatasetManifestContractError::UnsupportedFormat {
                contract: "dataset manifest",
                expected: DATASET_ARTIFACT_FORMAT_VERSION,
                actual: self.format_version,
            });
        }
        self.factor_serving_plane.validate().map_err(|error| {
            DatasetManifestContractError::InvalidFactorPlane {
                detail: error.to_string(),
            }
        })?;
        let factor_free = self.factor_serving_plane.definitions().is_empty();
        if self.model_family.is_classical() != factor_free {
            return Err(DatasetManifestContractError::FactorPlaneFamilyMismatch);
        }
        if self.feature_schema_version.get() < 1 {
            return Err(DatasetManifestContractError::FactorPlaneSchemaMismatch);
        }
        if self
            .factor_serving_plane
            .definitions()
            .iter()
            .any(|definition| definition.feature_contract_hash() != self.feature_schema_hash)
        {
            return Err(DatasetManifestContractError::FactorPlaneFeatureMismatch);
        }
        if self
            .factor_serving_plane
            .definitions()
            .iter()
            .any(|definition| definition.input_schema_version() != self.feature_schema_version)
        {
            return Err(DatasetManifestContractError::FactorPlaneSchemaMismatch);
        }
        self.source_lineage.validate()?;
        if self.window_start >= self.window_end {
            return Err(DatasetManifestContractError::InvalidDatasetWindow);
        }
        if self.source_lineage.source_window_start > self.window_start
            || self.source_lineage.source_window_end < self.window_end
            || self.window_end > self.source_lineage.pit_cutoff
        {
            return Err(DatasetManifestContractError::SourceCoverageMismatch);
        }
        if self.trade_policy_artifact_id.is_some() != self.trade_policy_hash.is_some() {
            return Err(DatasetManifestContractError::TradePolicyBindingMismatch);
        }
        if (self.sample_interval_secs == 0 && self.cohort_manifest.is_none())
            || self.horizons_secs.is_empty()
            || self.horizons_secs.windows(2).any(|pair| pair[0] >= pair[1])
        {
            return Err(DatasetManifestContractError::InvalidSamplingContract);
        }
        match (self.purpose, &self.cohort_manifest) {
            (DatasetPurpose::Evaluation, None) => {
                return Err(DatasetManifestContractError::MissingEvaluationCohort);
            }
            (DatasetPurpose::PolicyFit, Some(_)) => {
                return Err(DatasetManifestContractError::PolicyFitCohortForbidden);
            }
            _ => {}
        }
        if let Some(cohort) = &self.cohort_manifest {
            cohort.validate()?;
            if cohort.window.profile_ref()
                != &self
                    .source_lineage
                    .research_profile_artifact_id
                    .profile_ref()
            {
                return Err(DatasetManifestContractError::CohortProfileMismatch);
            }
            if cohort.window.window_start() != self.window_start
                || cohort.window.cutoff() != self.window_end
            {
                return Err(DatasetManifestContractError::CohortWindowMismatch);
            }
            if cohort.capability_registry_hashes != self.source_lineage.capability_registry_hashes {
                return Err(DatasetManifestContractError::CohortCapabilityMismatch);
            }
            if cohort.counts.included_count() != self.sample_count {
                return Err(DatasetManifestContractError::CohortSampleCountMismatch);
            }
        }
        Ok(())
    }

    pub fn content_hash(&self) -> Result<ContentHash, String> {
        self.validate().map_err(|error| error.to_string())?;
        CanonicalDigest::content_hash_typed(
            DATASET_MANIFEST_HASH_DOMAIN,
            DATASET_ARTIFACT_FORMAT_VERSION,
            self,
        )
        .map_err(|error| format!("dataset manifest hash failed: {error}"))
    }
}

impl TryFrom<DatasetManifestDocument> for DatasetManifest {
    type Error = DatasetManifestContractError;

    fn try_from(document: DatasetManifestDocument) -> Result<Self, Self::Error> {
        let manifest = Self {
            format_version: document.format_version,
            training_dataset_id: document.training_dataset_id,
            source_lineage: document.source_lineage,
            cohort_manifest: document.cohort_manifest,
            model_spec_id: document.model_spec_id,
            model_family: document.model_family,
            model_spec_definition_hash: document.model_spec_definition_hash,
            trade_policy_artifact_id: document.trade_policy_artifact_id,
            trade_policy_hash: document.trade_policy_hash,
            window_start: document.window_start,
            window_end: document.window_end,
            purpose: document.purpose,
            knowledge_lag_secs: document.knowledge_lag_secs,
            sample_interval_secs: document.sample_interval_secs,
            horizons_secs: document.horizons_secs,
            feature_schema_version: document.feature_schema_version,
            feature_schema_hash: document.feature_schema_hash,
            factor_serving_plane: document.factor_serving_plane,
            label_schema_hash: document.label_schema_hash,
            semantic_dataset_hash: document.semantic_dataset_hash,
            source_fingerprint: document.source_fingerprint,
            sample_count: document.sample_count,
        };
        manifest.validate()?;
        Ok(manifest)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TrainingSampleSource {
    HistoricalPit,
    /// Exact immutable feature/factor rows from the complete population that
    /// reached model scoring, joined to later WORM resolution truth. This
    /// includes recommendation abstentions by construction.
    ModelScoreFeedback,
    /// Published recommendation outcomes used for coverage and drift
    /// diagnostics. This selective population is never a training Dataset.
    PublishedDecisionDiagnostic,
    /// Per-tick hold-vs-exit decision points sampled along a closed/settled
    /// lot's life for Sell-scorer training. Anchored on position-lot
    /// timelines rather than a uniform market grid.
    ExitDecision,
}

/// Ordered sample-source contract frozen on a dataset plan.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, FromJsonQueryResult)]
#[serde(transparent)]
pub struct TrainingSampleSources(Vec<TrainingSampleSource>);

/// Stable validation failures for a frozen sample-source contract.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum TrainingSampleSourcesError {
    #[error("training sample sources must not be empty")]
    Empty,
    #[error("training sample sources must be strictly ordered and unique")]
    NonCanonical,
}

impl TrainingSampleSources {
    #[must_use]
    pub fn as_slice(&self) -> &[TrainingSampleSource] {
        &self.0
    }
}

impl Default for TrainingSampleSources {
    fn default() -> Self {
        Self(vec![TrainingSampleSource::HistoricalPit])
    }
}

impl From<TrainingSampleSource> for TrainingSampleSources {
    fn from(source: TrainingSampleSource) -> Self {
        Self(vec![source])
    }
}

impl TryFrom<Vec<TrainingSampleSource>> for TrainingSampleSources {
    type Error = TrainingSampleSourcesError;

    fn try_from(sources: Vec<TrainingSampleSource>) -> Result<Self, Self::Error> {
        if sources.is_empty() {
            return Err(TrainingSampleSourcesError::Empty);
        }
        if sources.windows(2).any(|pair| pair[0] >= pair[1]) {
            return Err(TrainingSampleSourcesError::NonCanonical);
        }
        Ok(Self(sources))
    }
}

impl<'de> Deserialize<'de> for TrainingSampleSources {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        Self::try_from(Vec::<TrainingSampleSource>::deserialize(deserializer)?)
            .map_err(D::Error::custom)
    }
}

#[cfg(test)]
mod sample_source_tests {
    use serde_json::{from_str, to_string};

    use super::{TrainingSampleSource, TrainingSampleSources, TrainingSampleSourcesError};

    #[test]
    fn default_is_historical() {
        assert_eq!(
            TrainingSampleSources::default().as_slice(),
            [TrainingSampleSource::HistoricalPit]
        );
    }

    #[test]
    fn invalid_contracts_fail() {
        assert_eq!(
            TrainingSampleSources::try_from(Vec::new()),
            Err(TrainingSampleSourcesError::Empty)
        );
        assert_eq!(
            TrainingSampleSources::try_from(vec![
                TrainingSampleSource::HistoricalPit,
                TrainingSampleSource::HistoricalPit,
            ]),
            Err(TrainingSampleSourcesError::NonCanonical)
        );
        assert_eq!(
            TrainingSampleSources::try_from(vec![
                TrainingSampleSource::ExitDecision,
                TrainingSampleSource::HistoricalPit,
            ]),
            Err(TrainingSampleSourcesError::NonCanonical)
        );
    }

    #[test]
    fn serde_rejects_drift() {
        let sources = TrainingSampleSources::try_from(vec![
            TrainingSampleSource::HistoricalPit,
            TrainingSampleSource::ExitDecision,
        ])
        .expect("canonical sample sources");
        let encoded = to_string(&sources).expect("serialize sample sources");
        assert_eq!(
            from_str::<TrainingSampleSources>(&encoded).expect("deserialize sample sources"),
            sources
        );
        assert!(
            from_str::<TrainingSampleSources>(r#"["exit_decision","historical_pit"]"#,).is_err()
        );
    }
}
