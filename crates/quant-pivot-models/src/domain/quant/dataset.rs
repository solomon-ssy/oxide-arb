//! Training-dataset ledger persistence DTOs.

use chrono::{DateTime, Utc};
use sea_orm::{DeriveIntoActiveModel, DerivePartialModel};
use serde::{Deserialize, Serialize};

use crate::{
    entities::quant_training_dataset,
    enums::{
        model::ModelFamily,
        quant::{DatasetPurpose, FeedbackCohort, TrainingDatasetStatus},
    },
    types::{
        ArtifactUri, ContentHash, DATASET_ARTIFACT_FORMAT_VERSION, DatasetCohortManifest,
        DatasetCoverage, DatasetManifest, DatasetManifestContractError, DatasetSourceLineage,
        DecisionPolicySnapshotId, ModelSpecId, ResearchProfileArtifactId, SchemaVersion,
        SourceSliceId, TrainingDatasetId, TrainingHorizonsSecs, TrainingSampleSources,
        factor::FactorServingPlane,
    },
};

/// Frozen training-dataset ledger row.
#[derive(Debug, Clone, Serialize, Deserialize, DerivePartialModel)]
#[sea_orm(entity = "crate::entities::quant_training_dataset::Entity")]
pub struct TrainingDatasetInfo {
    pub training_dataset_id: TrainingDatasetId,
    pub model_spec_id: ModelSpecId,
    pub model_family: ModelFamily,
    pub model_spec_definition_hash: ContentHash,
    pub factor_serving_plane: FactorServingPlane,
    pub research_profile_artifact_id: ResearchProfileArtifactId,
    pub source_slice_id: SourceSliceId,
    pub pit_cutoff: DateTime<Utc>,
    pub source_lineage: DatasetSourceLineage,
    pub feedback_cohort: Option<FeedbackCohort>,
    pub cohort_manifest: Option<DatasetCohortManifest>,
    pub window_start: DateTime<Utc>,
    pub window_end: DateTime<Utc>,
    pub status: TrainingDatasetStatus,
    /// What the materialized examples are used for: `Training`
    /// (the default, model-training spine) or `Calibration` (an independent
    /// held-out split a `ProbabilityCalibrator` fits on — must be disjoint +
    /// embargoed from the model's own `Training` dataset).
    pub purpose: DatasetPurpose,
    pub feature_schema_hash: ContentHash,
    pub factor_schema_hash: ContentHash,
    pub label_schema_hash: Option<ContentHash>,
    pub dataset_hash: Option<ContentHash>,
    /// Canonical hash of the manifest embedded in the Parquet envelope.
    pub manifest_hash: Option<ContentHash>,
    /// Exact structured manifest embedded in the Parquet envelope.
    pub manifest: Option<DatasetManifest>,
    /// Exact hash of the persisted Parquet bytes.
    pub artifact_bytes_hash: Option<ContentHash>,
    pub parquet_uri: Option<ArtifactUri>,
    pub sample_count: Option<i64>,
    /// Feature source visibility delay (PIT cutoff) the dataset was built with.
    /// Persisted so a backtest can recompute features byte-identically.
    pub knowledge_lag_secs: i64,
    /// Deterministic sampling cadence (seconds) the build grid used.
    pub sample_interval_secs: i64,
    /// Forward label horizons (seconds) the build materialized.
    pub horizons_secs: TrainingHorizonsSecs,
    pub feature_schema_version: SchemaVersion,
    pub sample_sources: Option<TrainingSampleSources>,
    pub coverage: Option<DatasetCoverage>,
    pub decision_policy_snapshot_id: DecisionPolicySnapshotId,
    pub failure_detail: Option<String>,
    pub completed_at: Option<DateTime<Utc>>,
    pub created_at: DateTime<Utc>,
}

info_from_model!(
    TrainingDatasetInfo,
    quant_training_dataset::Model,
    {
        training_dataset_id,
        model_spec_id,
        model_family,
        model_spec_definition_hash,
        factor_serving_plane,
        research_profile_artifact_id,
        source_slice_id,
        pit_cutoff,
        source_lineage,
        feedback_cohort,
        cohort_manifest,
        window_start,
        window_end,
        status,
        purpose,
        feature_schema_hash,
        factor_schema_hash,
        label_schema_hash,
        dataset_hash,
        manifest_hash,
        manifest,
        artifact_bytes_hash,
        parquet_uri,
        sample_count,
        knowledge_lag_secs,
        sample_interval_secs,
        horizons_secs,
        feature_schema_version,
        sample_sources,
        coverage,
        decision_policy_snapshot_id,
        failure_detail,
        completed_at,
        created_at,
    }
);

/// Immutable plan inserted before materialization starts.
#[derive(Debug, Clone, Serialize, Deserialize, DeriveIntoActiveModel)]
#[sea_orm(active_model = "crate::entities::quant_training_dataset::ActiveModel")]
pub struct NewTrainingDatasetPlan {
    pub training_dataset_id: TrainingDatasetId,
    pub model_spec_id: ModelSpecId,
    pub model_family: ModelFamily,
    pub model_spec_definition_hash: ContentHash,
    pub factor_serving_plane: FactorServingPlane,
    pub feature_schema_hash: ContentHash,
    pub factor_schema_hash: ContentHash,
    pub research_profile_artifact_id: ResearchProfileArtifactId,
    pub source_slice_id: SourceSliceId,
    pub pit_cutoff: DateTime<Utc>,
    pub source_lineage: DatasetSourceLineage,
    pub feedback_cohort: Option<FeedbackCohort>,
    pub cohort_manifest: Option<DatasetCohortManifest>,
    pub window_start: DateTime<Utc>,
    pub window_end: DateTime<Utc>,
    pub purpose: DatasetPurpose,
    pub knowledge_lag_secs: i64,
    pub sample_interval_secs: i64,
    pub horizons_secs: TrainingHorizonsSecs,
    pub feature_schema_version: SchemaVersion,
    pub sample_sources: Option<TrainingSampleSources>,
    pub decision_policy_snapshot_id: DecisionPolicySnapshotId,
}

impl NewTrainingDatasetPlan {
    pub fn validate(&self) -> Result<(), DatasetManifestContractError> {
        self.factor_serving_plane.validate().map_err(|error| {
            DatasetManifestContractError::InvalidFactorPlane {
                detail: error.to_string(),
            }
        })?;
        if self.model_family.is_classical() != self.factor_serving_plane.definitions().is_empty() {
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
        if self.factor_schema_hash != self.factor_serving_plane.factor_schema_hash() {
            return Err(DatasetManifestContractError::InvalidFactorPlane {
                detail: "factor schema hash is not the plane's derived projection".to_owned(),
            });
        }
        self.source_lineage.validate()?;
        if self.research_profile_artifact_id != self.source_lineage.research_profile_artifact_id
            || self.source_slice_id != self.source_lineage.source_slice_id
            || self.pit_cutoff != self.source_lineage.pit_cutoff
            || self.decision_policy_snapshot_id != self.source_lineage.decision_policy_snapshot_id
            || self.window_start >= self.window_end
            || self.source_lineage.source_window_start > self.window_start
            || self.source_lineage.source_window_end < self.window_end
        {
            return Err(DatasetManifestContractError::FrozenPlanMismatch);
        }
        match (
            self.purpose,
            self.feedback_cohort,
            self.cohort_manifest.as_ref(),
        ) {
            (DatasetPurpose::Evaluation, None, _) | (DatasetPurpose::Evaluation, _, None) => {
                return Err(DatasetManifestContractError::MissingEvaluationCohort);
            }
            (DatasetPurpose::PolicyFit, Some(_), _) | (DatasetPurpose::PolicyFit, _, Some(_)) => {
                return Err(DatasetManifestContractError::PolicyFitCohortForbidden);
            }
            (_, Some(discriminator), Some(manifest)) if discriminator != manifest.cohort => {
                return Err(DatasetManifestContractError::CohortDiscriminatorMismatch);
            }
            (_, None, Some(_)) | (_, Some(_), None) => {
                return Err(DatasetManifestContractError::CohortDiscriminatorMismatch);
            }
            _ => {}
        }
        if let Some(manifest) = &self.cohort_manifest {
            manifest.validate()?;
            if manifest.window.profile_ref() != &self.research_profile_artifact_id.profile_ref() {
                return Err(DatasetManifestContractError::CohortProfileMismatch);
            }
            if manifest.window.window_start() != self.window_start
                || manifest.window.cutoff() != self.window_end
            {
                return Err(DatasetManifestContractError::CohortWindowMismatch);
            }
            if manifest.capability_registry_hashes != self.source_lineage.capability_registry_hashes
            {
                return Err(DatasetManifestContractError::CohortCapabilityMismatch);
            }
        }
        Ok(())
    }
}

/// Artifact bindings committed atomically with the build's terminal status.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CompleteTrainingDatasetBuild {
    status: TrainingDatasetStatus,
    manifest: DatasetManifest,
    manifest_hash: ContentHash,
    artifact_bytes_hash: ContentHash,
    parquet_uri: ArtifactUri,
    sample_count: i64,
    coverage: DatasetCoverage,
    failure_detail: Option<String>,
}

impl CompleteTrainingDatasetBuild {
    pub fn try_new(
        status: TrainingDatasetStatus,
        manifest: DatasetManifest,
        artifact_bytes_hash: ContentHash,
        parquet_uri: ArtifactUri,
        coverage: DatasetCoverage,
        failure_detail: Option<String>,
    ) -> Result<Self, DatasetManifestContractError> {
        manifest.validate()?;
        if coverage.built_examples != manifest.sample_count {
            return Err(DatasetManifestContractError::CoverageSampleCountMismatch);
        }
        let valid_terminal = match status {
            TrainingDatasetStatus::Ready => failure_detail.is_none(),
            TrainingDatasetStatus::InsufficientLabels | TrainingDatasetStatus::Failed => {
                failure_detail
                    .as_ref()
                    .is_some_and(|detail| !detail.trim().is_empty())
            }
            TrainingDatasetStatus::Planned
            | TrainingDatasetStatus::Building
            | TrainingDatasetStatus::Expired => false,
        };
        if !valid_terminal {
            return Err(DatasetManifestContractError::InvalidCompletionStatus);
        }
        let sample_count = i64::try_from(manifest.sample_count)
            .map_err(|_| DatasetManifestContractError::SampleCountOverflow)?;
        let manifest_hash = manifest.content_hash().map_err(|_| {
            DatasetManifestContractError::UnsupportedFormat {
                contract: "dataset manifest",
                expected: DATASET_ARTIFACT_FORMAT_VERSION,
                actual: manifest.format_version,
            }
        })?;
        Ok(Self {
            status,
            manifest,
            manifest_hash,
            artifact_bytes_hash,
            parquet_uri,
            sample_count,
            coverage,
            failure_detail,
        })
    }

    #[must_use]
    pub const fn status(&self) -> TrainingDatasetStatus {
        self.status
    }

    #[must_use]
    pub const fn manifest(&self) -> &DatasetManifest {
        &self.manifest
    }

    #[must_use]
    pub const fn feature_schema_hash(&self) -> &ContentHash {
        &self.manifest.feature_schema_hash
    }

    #[must_use]
    pub const fn factor_schema_hash(&self) -> ContentHash {
        self.manifest.factor_schema_hash()
    }

    #[must_use]
    pub const fn label_schema_hash(&self) -> &ContentHash {
        &self.manifest.label_schema_hash
    }

    #[must_use]
    pub const fn dataset_hash(&self) -> &ContentHash {
        &self.manifest.semantic_dataset_hash
    }

    #[must_use]
    pub const fn manifest_hash(&self) -> ContentHash {
        self.manifest_hash
    }

    #[must_use]
    pub const fn artifact_bytes_hash(&self) -> ContentHash {
        self.artifact_bytes_hash
    }

    #[must_use]
    pub const fn parquet_uri(&self) -> &ArtifactUri {
        &self.parquet_uri
    }

    #[must_use]
    pub const fn sample_count(&self) -> i64 {
        self.sample_count
    }

    #[must_use]
    pub const fn coverage(&self) -> &DatasetCoverage {
        &self.coverage
    }

    #[must_use]
    pub fn failure_detail(&self) -> Option<&str> {
        self.failure_detail.as_deref()
    }
}

/// Fully materialized artifact fields borrowed from a lifecycle row.
pub struct TrainingDatasetMaterialization<'a> {
    pub feature_schema_hash: &'a ContentHash,
    pub factor_serving_plane: &'a FactorServingPlane,
    pub label_schema_hash: &'a ContentHash,
    pub dataset_hash: &'a ContentHash,
    pub manifest_hash: &'a ContentHash,
    pub manifest: &'a DatasetManifest,
    pub artifact_bytes_hash: &'a ContentHash,
    pub parquet_uri: &'a ArtifactUri,
    pub sample_count: i64,
    pub coverage: &'a DatasetCoverage,
}

impl TrainingDatasetMaterialization<'_> {
    /// Strict scalar projection of the complete plan-time factor plane.
    #[must_use]
    pub const fn factor_schema_hash(&self) -> ContentHash {
        self.factor_serving_plane.factor_schema_hash()
    }
}

impl TrainingDatasetInfo {
    /// Return the complete artifact binding only when every materialized field
    /// is present. Callers must still enforce the lifecycle status they accept.
    #[must_use]
    pub fn materialization(&self) -> Option<TrainingDatasetMaterialization<'_>> {
        let persisted_factor_hash = self.factor_schema_hash;
        let manifest = self.manifest.as_ref()?;
        if manifest.validate().is_err()
            || persisted_factor_hash != self.factor_serving_plane.factor_schema_hash()
            || self.model_family != manifest.model_family
            || self.factor_serving_plane != manifest.factor_serving_plane
            || self.feature_schema_version != manifest.feature_schema_version
            || self.feature_schema_hash != manifest.feature_schema_hash
        {
            return None;
        }
        Some(TrainingDatasetMaterialization {
            feature_schema_hash: &self.feature_schema_hash,
            factor_serving_plane: &self.factor_serving_plane,
            label_schema_hash: self.label_schema_hash.as_ref()?,
            dataset_hash: self.dataset_hash.as_ref()?,
            manifest_hash: self.manifest_hash.as_ref()?,
            manifest,
            artifact_bytes_hash: self.artifact_bytes_hash.as_ref()?,
            parquet_uri: self.parquet_uri.as_ref()?,
            sample_count: self.sample_count?,
            coverage: self.coverage.as_ref()?,
        })
    }
}

#[cfg(test)]
mod tests {
    use chrono::{DateTime, Duration, TimeZone, Utc};

    use super::CompleteTrainingDatasetBuild;
    use crate::{
        domain::quant::FeedbackCohortWindow,
        enums::{
            model::ModelFamily,
            quant::{
                CohortCensorReason, CohortExclusionReason, DatasetPurpose, FeedbackCohort,
                TrainingDatasetStatus,
            },
        },
        types::{
            ArtifactUri, CapabilityRegistryHashes, CohortCensorCount, CohortExclusionCount,
            ContentHash, DATASET_ARTIFACT_FORMAT_VERSION, DATASET_COHORT_MANIFEST_FORMAT_VERSION,
            DATASET_SOURCE_LINEAGE_FORMAT_VERSION, DatasetCohortArtifactRef, DatasetCohortCounts,
            DatasetCohortManifest, DatasetCoverage, DatasetManifest, DatasetSourceLineage,
            DecisionPolicySnapshotId, ModelSpecId, ReaderContractVersion,
            ResearchProfileArtifactId, ResearchProfileId, ResearchProfileRef,
            SchemaContractVersion, SchemaVersion, SourceSliceId, SourceSliceManifestRef,
            TrainingDatasetId, factor::FactorServingPlane,
        },
    };

    fn hash(digit: char) -> ContentHash {
        ContentHash::parse(&format!("blake3:{}", digit.to_string().repeat(64)))
            .expect("valid fixture hash")
    }

    fn instant(hour: u32) -> DateTime<Utc> {
        Utc.with_ymd_and_hms(2026, 7, 1, hour, 0, 0)
            .single()
            .expect("valid fixture instant")
    }

    impl DatasetManifest {
        fn fixture() -> Self {
            let profile = ResearchProfileRef {
                id: ResearchProfileId::new("crypto_1h"),
                version: 1,
                content_hash: hash('1'),
            };
            let capabilities =
                CapabilityRegistryHashes::try_new(vec![hash('2')]).expect("valid capability set");
            let source_lineage = DatasetSourceLineage {
                format_version: DATASET_SOURCE_LINEAGE_FORMAT_VERSION,
                source_slice_id: SourceSliceId::from_v7(),
                source_slice_identity_hash: hash('3'),
                research_profile_artifact_id: ResearchProfileArtifactId::from_profile_ref(&profile),
                research_program_hash: hash('4'),
                source_slice: SourceSliceManifestRef {
                    manifest_uri: ArtifactUri::parse("s3://worm/source/manifest.json")
                        .expect("valid artifact URI"),
                    manifest_hash: hash('5'),
                },
                source_window_start: instant(0),
                source_window_end: instant(4),
                pit_cutoff: instant(5),
                decision_policy_snapshot_id: DecisionPolicySnapshotId::from_v7(),
                runtime_config_hash: hash('6'),
                reader_contract_version: ReaderContractVersion::v1(),
                schema_contract_version: SchemaContractVersion::v1(),
                source_schema_hash: hash('7'),
                capability_registry_hashes: capabilities.clone(),
            };
            let window = FeedbackCohortWindow::try_new(profile, instant(1), instant(3))
                .expect("valid window");
            let counts = DatasetCohortCounts::try_new(
                8,
                5,
                2,
                vec![CohortExclusionCount {
                    reason: CohortExclusionReason::RecommendationNotPublished,
                    count: 2,
                }],
                vec![CohortCensorCount {
                    reason: CohortCensorReason::ResolutionUnavailableAtCutoff,
                    count: 1,
                }],
            )
            .expect("balanced cohort counts");
            let cohort_manifest = DatasetCohortManifest {
                format_version: DATASET_COHORT_MANIFEST_FORMAT_VERSION,
                cohort: FeedbackCohort::ModelLearning,
                window,
                artifact: DatasetCohortArtifactRef {
                    uri: ArtifactUri::parse("s3://worm/cohorts/model-learning.parquet")
                        .expect("valid artifact URI"),
                    bytes_hash: hash('8'),
                    schema_hash: hash('9'),
                    source_hash: hash('a'),
                    row_count: 2,
                },
                counts,
                capability_registry_hashes: capabilities,
            };
            let factor_serving_plane =
                FactorServingPlane::try_empty().expect("canonical factor-free plane");
            Self {
                format_version: DATASET_ARTIFACT_FORMAT_VERSION,
                training_dataset_id: TrainingDatasetId::from_v7(),
                source_lineage,
                cohort_manifest: Some(cohort_manifest),
                model_spec_id: ModelSpecId::from_v7(),
                model_family: ModelFamily::ClassicalRandomForest,
                model_spec_definition_hash: hash('b'),
                trade_policy_artifact_id: None,
                trade_policy_hash: None,
                window_start: instant(1),
                window_end: instant(3),
                purpose: DatasetPurpose::Evaluation,
                knowledge_lag_secs: 1,
                sample_interval_secs: 60,
                horizons_secs: vec![3_600],
                feature_schema_version: SchemaVersion::FIRST,
                feature_schema_hash: hash('c'),
                factor_serving_plane,
                label_schema_hash: hash('e'),
                semantic_dataset_hash: hash('f'),
                source_fingerprint: hash('0'),
                sample_count: 2,
            }
        }
    }

    mod manifest_contract {
        use super::*;

        #[test]
        fn round_trips() {
            let manifest = DatasetManifest::fixture();
            manifest.validate().expect("valid manifest");
            let encoded = serde_json::to_vec(&manifest).expect("encode manifest");
            let decoded =
                serde_json::from_slice::<DatasetManifest>(&encoded).expect("decode manifest");
            assert_eq!(decoded, manifest);
            assert_eq!(
                decoded.content_hash().expect("manifest hash"),
                manifest.content_hash().expect("manifest hash")
            );
        }

        #[test]
        fn rejects_tamper() {
            let manifest = DatasetManifest::fixture();
            let mut document = serde_json::to_value(&manifest).expect("encode manifest");
            document["sample_count"] = serde_json::json!(3);
            assert!(serde_json::from_value::<DatasetManifest>(document).is_err());

            let mut unknown_manifest = serde_json::to_value(&manifest).expect("encode manifest");
            unknown_manifest["unexpected"] = serde_json::json!(true);
            assert!(serde_json::from_value::<DatasetManifest>(unknown_manifest).is_err());

            let mut unknown_source = serde_json::to_value(&manifest).expect("encode manifest");
            unknown_source["source_lineage"]["unexpected"] = serde_json::json!(true);
            assert!(serde_json::from_value::<DatasetManifest>(unknown_source).is_err());

            let mut unknown_cohort = serde_json::to_value(&manifest).expect("encode manifest");
            unknown_cohort["cohort_manifest"]["unexpected"] = serde_json::json!(true);
            assert!(serde_json::from_value::<DatasetManifest>(unknown_cohort).is_err());

            let mut old_wire = serde_json::to_value(&manifest).expect("encode manifest");
            old_wire["format_version"] = serde_json::json!(2);
            assert!(serde_json::from_value::<DatasetManifest>(old_wire).is_err());

            let mut wrong_family = serde_json::to_value(&manifest).expect("encode manifest");
            wrong_family["model_family"] = serde_json::json!("weighted_factor");
            assert!(serde_json::from_value::<DatasetManifest>(wrong_family).is_err());

            let mut wrong_purpose = manifest;
            wrong_purpose.purpose = DatasetPurpose::PolicyFit;
            assert!(wrong_purpose.validate().is_err());
        }

        #[test]
        fn rejects_count_drift() {
            let mut manifest = DatasetManifest::fixture();
            manifest
                .cohort_manifest
                .as_mut()
                .expect("cohort manifest")
                .artifact
                .row_count = 3;
            assert!(manifest.validate().is_err());
        }
    }

    mod completion_contract {
        use super::*;

        #[test]
        fn derives_bindings() {
            let manifest = DatasetManifest::fixture();
            let expected_hash = manifest.content_hash().expect("manifest hash");
            let completion = CompleteTrainingDatasetBuild::try_new(
                TrainingDatasetStatus::Ready,
                manifest.clone(),
                hash('1'),
                ArtifactUri::parse("s3://worm/datasets/evaluation.parquet")
                    .expect("valid artifact URI"),
                DatasetCoverage {
                    planned_samples: 2,
                    built_examples: 2,
                    labels_available: 2,
                    ..DatasetCoverage::default()
                },
                None,
            )
            .expect("valid completion");

            assert_eq!(completion.manifest_hash(), expected_hash);
            assert_eq!(completion.sample_count(), 2);
            assert_eq!(completion.dataset_hash(), &manifest.semantic_dataset_hash);
        }
    }

    mod capability_contract {
        use super::*;

        #[test]
        fn rejects_noncanonical_set() {
            assert!(CapabilityRegistryHashes::try_new(vec![hash('2'), hash('2')]).is_err());
            assert!(CapabilityRegistryHashes::try_new(vec![hash('3'), hash('2')]).is_err());
        }

        #[test]
        fn source_covers_dataset() {
            let mut manifest = DatasetManifest::fixture();
            manifest.source_lineage.source_window_end = manifest.window_end - Duration::seconds(1);
            assert!(manifest.validate().is_err());
        }
    }
}
