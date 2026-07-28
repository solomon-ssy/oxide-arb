//! Frozen feedback cohort to immutable training Dataset orchestration.

use std::{
    collections::{HashMap, HashSet},
    hash::Hash,
    mem,
    sync::Arc,
};

use chrono::{DateTime, Utc};
use quant_pivot_error::{QuantResult, research::ResearchError, storage::StorageError};
use quant_pivot_models::{
    domain::{
        ports::FeedbackDatasetBuildRequest,
        quant::{
            FEEDBACK_COHORT_PAGE_LIMIT, FactorDefinitionInfo, FactorValueInfo, FeatureVectorInfo,
            FeedbackCohortDecision, FeedbackCohortEvidence, FeedbackCohortPageQuery,
            FeedbackCohortWindow, FeedbackExecutionAttempt, FeedbackRecommendationContext,
            FeedbackResolutionEvidence, JobProgressSink,
        },
    },
    enums::quant::{CohortCensorReason, CohortExclusionReason, FeedbackCohort, OutcomeSide},
    hashing::CanonicalDigest,
    types::{
        CapabilityRegistryHashes, CohortCensorCount, CohortExclusionCount,
        DATASET_COHORT_MANIFEST_FORMAT_VERSION, DatasetCohortArtifactRef, DatasetCohortCounts,
        DatasetCohortManifest, DatasetCoverage, FactorDefinitionId, FeatureSourceRefs,
        FeatureVectorId, MODEL_LEARNING_COHORT_FORMAT_VERSION, ModelLearningCohortArtifact,
        ModelLearningCohortRow, ModelVersionId, NewModelLearningCohortRow, ResearchJobProgress,
        SchemaVersion, TokenId, TrainingSampleSource, TrainingSampleSources,
        factor::{FactorDefinitionRef, FactorServingPlane},
    },
};
use quant_pivot_repository::traits::{
    FactorRepository, FeatureRepository, FeedbackCohortRepository,
};
use quant_pivot_research::{
    artifact::{ArtifactKey, ArtifactNamespace, ArtifactStore},
    factors::FactorValue,
    features::FeatureVector,
    feedback::{FeedbackCoverageCohorts, FeedbackMatureLabel},
    selection::SelectedMarket,
    training::{
        DatasetPlanRequest, ModelLearningCohortCodec, TOKEN_PAYOUT_RATIO, TrainingDatasetArtifact,
        TrainingExample, TrainingLabel,
    },
};
use tokio_util::sync::CancellationToken;

use crate::service::{
    feedback_cohort::evaluate_feedback_cohort, training_dataset::TrainingDatasetService,
};

/// Dependencies for [`FeedbackDatasetService`].
pub struct FeedbackDatasetServiceDeps {
    pub cohort_repository: Arc<dyn FeedbackCohortRepository>,
    pub feature_repository: Arc<dyn FeatureRepository>,
    pub factor_repository: Arc<dyn FactorRepository>,
    pub artifact_store: Arc<dyn ArtifactStore>,
    pub dataset_service: Arc<TrainingDatasetService>,
}

/// Repository dependencies for the canonical feedback-cohort materializer.
pub(crate) struct FeedbackCohortMaterializerDeps {
    pub cohorts: Arc<dyn FeedbackCohortRepository>,
    pub features: Arc<dyn FeatureRepository>,
    pub factors: Arc<dyn FactorRepository>,
}

/// Single owner of feedback keyset scans and exact serving-row reconstruction.
#[derive(Clone)]
pub(crate) struct FeedbackCohortMaterializer {
    cohorts: Arc<dyn FeedbackCohortRepository>,
    features: Arc<dyn FeatureRepository>,
    factors: Arc<dyn FactorRepository>,
}

/// Frozen all-cohort counts and champion-compatible evaluation rows.
pub(crate) struct FeedbackCoverageMaterialization {
    pub cohorts: FeedbackCoverageCohorts,
    pub mature_labels: Vec<FeedbackMatureLabel>,
    pub new_mature_label_count: u64,
    pub champion_rows: Vec<ModelLearningCohortRow>,
    pub champion_examples: Vec<TrainingExample>,
}

impl FeedbackCohortMaterializer {
    #[must_use]
    pub(crate) fn new(deps: FeedbackCohortMaterializerDeps) -> Self {
        Self {
            cohorts: deps.cohorts,
            features: deps.features,
            factors: deps.factors,
        }
    }
}

/// Seals `ModelLearning` truth and materializes only exact serving rows.
pub struct FeedbackDatasetService {
    materializer: FeedbackCohortMaterializer,
    artifact_store: Arc<dyn ArtifactStore>,
    dataset_service: Arc<TrainingDatasetService>,
}

impl FeedbackDatasetService {
    #[must_use]
    pub fn new(deps: FeedbackDatasetServiceDeps) -> Self {
        Self {
            materializer: FeedbackCohortMaterializer::new(FeedbackCohortMaterializerDeps {
                cohorts: deps.cohort_repository,
                features: deps.feature_repository,
                factors: deps.factor_repository,
            }),
            artifact_store: deps.artifact_store,
            dataset_service: deps.dataset_service,
        }
    }

    /// Seal the cohort, verify serving evidence, and persist the Dataset.
    pub async fn build(
        &self,
        request: FeedbackDatasetBuildRequest,
        progress: Arc<dyn JobProgressSink>,
        cancel: CancellationToken,
    ) -> QuantResult<TrainingDatasetArtifact> {
        request.validate()?;
        let expected_model_spec_hash = request.model_spec_definition_hash;
        let mut scan = self
            .materializer
            .scan_cohort(
                FeedbackCohort::ModelLearning,
                &request.window,
                &progress,
                &cancel,
            )
            .await?;
        if scan.eligible.is_empty() {
            return Err(ResearchError::NotEligible {
                code: "feedback_cohort_empty",
                detail: "frozen ModelLearning cohort contains no mature resolution labels"
                    .to_owned(),
            }
            .into());
        }
        let eligible = mem::take(&mut scan.eligible);
        let materialized = self.materializer.materialize(eligible).await?;
        let counts = scan.counts(materialized.rows.len())?;
        let cohort_manifest = self
            .persist_cohort(
                request.window.clone(),
                counts,
                request.source_lineage.capability_registry_hashes.clone(),
                materialized.rows,
            )
            .await?;
        if cancel.is_cancelled() {
            return Err(ResearchError::Cancelled {
                detail: "feedback Dataset cancelled after cohort seal".to_owned(),
            }
            .into());
        }
        let plan = self
            .dataset_service
            .plan_feedback(
                DatasetPlanRequest {
                    model_spec_id: request.model_spec_id,
                    source_lineage: request.source_lineage,
                    cohort_manifest: Some(cohort_manifest),
                    window_start: request.window.window_start(),
                    window_end: request.window.cutoff(),
                    sample_interval_secs: 0,
                    horizons_secs: vec![0],
                    knowledge_lag_secs: materialized.knowledge_lag_secs,
                    feature_schema_version: materialized.feature_schema_version,
                    sample_sources: TrainingSampleSources::from(
                        TrainingSampleSource::RecommendationFeedback,
                    ),
                    training_dataset_id: Some(request.training_dataset_id),
                    purpose: request.purpose,
                },
                materialized.factor_serving_plane,
            )
            .await?;
        if plan.model_spec_definition_hash != expected_model_spec_hash {
            return Err(ResearchError::DatasetPlan {
                detail: "feedback Dataset ModelSpec differs from the frozen candidate recipe"
                    .to_owned(),
            }
            .into());
        }
        self.dataset_service
            .build_feedback(plan, materialized.examples, materialized.coverage)
            .await
    }
}

#[derive(Debug)]
struct EligibleFeedback {
    context: FeedbackRecommendationContext,
    resolution: FeedbackResolutionEvidence,
}

#[derive(Default)]
struct CohortScan {
    candidate_count: u64,
    eligible_count: u64,
    eligible: Vec<EligibleFeedback>,
    exclusions: HashMap<CohortExclusionReason, u64>,
    censors: HashMap<CohortCensorReason, u64>,
}

impl CohortScan {
    fn record(
        &mut self,
        cohort: FeedbackCohort,
        context: FeedbackRecommendationContext,
        decision: FeedbackCohortDecision,
    ) -> QuantResult<()> {
        self.candidate_count =
            self.candidate_count
                .checked_add(1)
                .ok_or_else(|| ResearchError::DatasetBuild {
                    detail: "feedback cohort candidate count overflowed u64".to_owned(),
                })?;
        match (cohort, decision) {
            (
                FeedbackCohort::ModelLearning,
                FeedbackCohortDecision::Eligible(FeedbackCohortEvidence::ModelLearning(resolution)),
            ) => {
                self.increment_eligible()?;
                self.eligible.push(EligibleFeedback {
                    context,
                    resolution,
                });
            }
            (
                FeedbackCohort::ExecutionLearning,
                FeedbackCohortDecision::Eligible(FeedbackCohortEvidence::ExecutionLearning(_)),
            )
            | (
                FeedbackCohort::PolicyEvaluation,
                FeedbackCohortDecision::Eligible(FeedbackCohortEvidence::PolicyEvaluation {
                    ..
                }),
            ) => {
                self.increment_eligible()?;
            }
            (_, FeedbackCohortDecision::Eligible(_)) => {
                return Err(ResearchError::DatasetBuild {
                    detail: format!("{cohort} scan produced evidence from another cohort"),
                }
                .into());
            }
            (_, FeedbackCohortDecision::Excluded(reason)) => {
                Self::increment(&mut self.exclusions, reason)?;
            }
            (_, FeedbackCohortDecision::Censored(reason)) => {
                Self::increment(&mut self.censors, reason)?;
            }
        }
        Ok(())
    }

    fn increment_eligible(&mut self) -> QuantResult<()> {
        self.eligible_count =
            self.eligible_count
                .checked_add(1)
                .ok_or_else(|| ResearchError::DatasetBuild {
                    detail: "feedback cohort eligible count overflowed u64".to_owned(),
                })?;
        Ok(())
    }

    fn increment<K>(counts: &mut HashMap<K, u64>, reason: K) -> QuantResult<()>
    where
        K: Eq + Hash,
    {
        let count = counts.entry(reason).or_default();
        *count = count
            .checked_add(1)
            .ok_or_else(|| ResearchError::DatasetBuild {
                detail: "feedback cohort reason count overflowed u64".to_owned(),
            })?;
        Ok(())
    }

    fn counts(&self, included: usize) -> QuantResult<DatasetCohortCounts> {
        let included = u64::try_from(included).map_err(|error| ResearchError::DatasetBuild {
            detail: format!("feedback included count conversion failed: {error}"),
        })?;
        let mut exclusions = self
            .exclusions
            .iter()
            .map(|(reason, count)| CohortExclusionCount {
                reason: *reason,
                count: *count,
            })
            .collect::<Vec<_>>();
        exclusions.sort_by_key(|entry| entry.reason.as_str());
        let mut censors = self
            .censors
            .iter()
            .map(|(reason, count)| CohortCensorCount {
                reason: *reason,
                count: *count,
            })
            .collect::<Vec<_>>();
        censors.sort_by_key(|entry| entry.reason.as_str());
        DatasetCohortCounts::try_new(
            self.candidate_count,
            self.eligible_count,
            included,
            exclusions,
            censors,
        )
        .map_err(|error| {
            ResearchError::DatasetBuild {
                detail: format!("feedback cohort counts do not reconcile: {error}"),
            }
            .into()
        })
    }

    fn count_all(&self) -> QuantResult<DatasetCohortCounts> {
        let included =
            usize::try_from(self.eligible_count).map_err(|error| ResearchError::DatasetBuild {
                detail: format!("feedback eligible count exceeds usize: {error}"),
            })?;
        self.counts(included)
    }
}

impl FeedbackCohortMaterializer {
    async fn scan_cohort(
        &self,
        cohort: FeedbackCohort,
        window: &FeedbackCohortWindow,
        progress: &Arc<dyn JobProgressSink>,
        cancel: &CancellationToken,
    ) -> QuantResult<CohortScan> {
        let mut scan = CohortScan::default();
        let mut after = None;
        loop {
            if cancel.is_cancelled() {
                return Err(ResearchError::Cancelled {
                    detail: "feedback cohort scan cancelled between keyset pages".to_owned(),
                }
                .into());
            }
            let query = FeedbackCohortPageQuery::try_new(
                cohort,
                window.clone(),
                after,
                FEEDBACK_COHORT_PAGE_LIMIT,
            )
            .map_err(|error| ResearchError::DatasetBuild {
                detail: format!("construct feedback keyset page: {error}"),
            })?;
            let page = self.cohorts.list_page(query).await?;
            for candidate in page.candidates() {
                let decision = evaluate_feedback_cohort(
                    cohort,
                    window,
                    candidate.context(),
                    candidate
                        .execution_attempt()
                        .unwrap_or(FeedbackExecutionAttempt::NotAttempted),
                    candidate.resolution_outcome(),
                    candidate.execution_outcome(),
                )
                .map_err(|error| ResearchError::DatasetBuild {
                    detail: format!("classify {cohort} candidate: {error}"),
                })?;
                scan.record(cohort, candidate.context().clone(), decision)?;
            }
            progress.report(ResearchJobProgress::indeterminate(
                format!("feedback-{cohort}-scan"),
                scan.candidate_count,
            ));
            let Some(cursor) = page.next_cursor() else {
                break;
            };
            after = Some(cursor);
        }
        Ok(scan)
    }

    /// Freeze all three orthogonal cohorts and reconstruct only rows bound to
    /// the exact champion serving version.
    pub(crate) async fn freeze_coverage(
        &self,
        window: &FeedbackCohortWindow,
        champion_model_version_id: ModelVersionId,
        champion_pit_cutoff: DateTime<Utc>,
        progress: &Arc<dyn JobProgressSink>,
        cancel: &CancellationToken,
    ) -> QuantResult<FeedbackCoverageMaterialization> {
        let mut model = self
            .scan_cohort(FeedbackCohort::ModelLearning, window, progress, cancel)
            .await?;
        let execution = self
            .scan_cohort(FeedbackCohort::ExecutionLearning, window, progress, cancel)
            .await?;
        let policy = self
            .scan_cohort(FeedbackCohort::PolicyEvaluation, window, progress, cancel)
            .await?;

        let mut mature_labels = model
            .eligible
            .iter()
            .map(|eligible| FeedbackMatureLabel {
                recommendation_id: eligible.context.recommendation_id(),
                model_version_id: eligible.context.model_version_id(),
                candidate_available_at: eligible.context.available_at(),
                label_available_at: eligible.resolution.available_at,
                outcome_hash: eligible.resolution.outcome_hash,
            })
            .collect::<Vec<_>>();
        mature_labels.sort_by_key(|label| {
            (
                label.recommendation_id.as_uuid(),
                label.candidate_available_at,
                label.label_available_at,
            )
        });
        let new_mature_label_count = u64::try_from(
            mature_labels
                .iter()
                .filter(|label| label.label_available_at > champion_pit_cutoff)
                .count(),
        )
        .map_err(|error| ResearchError::DatasetBuild {
            detail: format!("new mature-label count conversion failed: {error}"),
        })?;
        let champion = mem::take(&mut model.eligible)
            .into_iter()
            .filter(|eligible| eligible.context.model_version_id() == champion_model_version_id)
            .collect::<Vec<_>>();
        let (mut champion_rows, mut champion_examples) = if champion.is_empty() {
            (Vec::new(), Vec::new())
        } else {
            let materialized = self.materialize(champion).await?;
            (materialized.rows, materialized.examples)
        };
        champion_rows.sort_by_key(|row| row.example_id.as_uuid());
        champion_examples.sort_by_key(|example| example.example_id.as_uuid());
        let cohorts = FeedbackCoverageCohorts {
            model_learning: model.counts(champion_rows.len())?,
            execution_learning: execution.count_all()?,
            policy_evaluation: policy.count_all()?,
        };
        Ok(FeedbackCoverageMaterialization {
            cohorts,
            mature_labels,
            new_mature_label_count,
            champion_rows,
            champion_examples,
        })
    }
}

struct MaterializedFeedback {
    rows: Vec<ModelLearningCohortRow>,
    examples: Vec<TrainingExample>,
    factor_serving_plane: FactorServingPlane,
    coverage: DatasetCoverage,
    feature_schema_version: SchemaVersion,
    knowledge_lag_secs: u64,
}

impl FeedbackCohortMaterializer {
    async fn materialize(
        &self,
        eligible: Vec<EligibleFeedback>,
    ) -> QuantResult<MaterializedFeedback> {
        let feature_ids = eligible
            .iter()
            .map(|seed| seed.context.feature_vector_id())
            .collect::<Vec<_>>();
        let unique_features = feature_ids.iter().copied().collect::<HashSet<_>>();
        if unique_features.len() != feature_ids.len() {
            return Err(ResearchError::DatasetBuild {
                detail: "ModelLearning cohort contains duplicate serving feature vectors"
                    .to_owned(),
            }
            .into());
        }
        let features = self.features.find_by_ids(&feature_ids).await?;
        let features = features
            .into_iter()
            .map(|feature| (feature.feature_vector_id, feature))
            .collect::<HashMap<_, _>>();
        if features.len() != feature_ids.len() {
            return Err(StorageError::invariant_violation(
                Some("quant_feature_vector"),
                "ModelLearning cohort references a missing feature vector",
            )
            .into());
        }

        let expected_factor_ids = eligible
            .first()
            .map(|seed| seed.context.factor_definition_versions().to_vec())
            .unwrap_or_default();
        if eligible
            .iter()
            .any(|seed| seed.context.factor_definition_versions() != expected_factor_ids.as_slice())
        {
            return Err(ResearchError::DatasetBuild {
                detail: "one ModelLearning Dataset cannot mix serving factor contracts".to_owned(),
            }
            .into());
        }
        if expected_factor_ids.is_empty() {
            let factor_serving_plane =
                FactorServingPlane::try_empty().map_err(|error| ResearchError::DatasetBuild {
                    detail: format!("seal feedback factor-free plane: {error}"),
                })?;
            return Self::materialize_rows(
                eligible,
                &features,
                HashMap::new(),
                &HashMap::new(),
                factor_serving_plane,
            );
        }
        let definitions = self
            .factors
            .find_definitions_by_ids(&expected_factor_ids)
            .await?;
        let definitions = definitions
            .into_iter()
            .map(|definition| (definition.factor_definition_id, definition))
            .collect::<HashMap<_, _>>();
        if definitions.len() != expected_factor_ids.len() {
            return Err(StorageError::invariant_violation(
                Some("quant_factor_definition"),
                "ModelLearning cohort references a missing factor definition",
            )
            .into());
        }
        let factor_rows = self.factors.find_values_by_vectors(&feature_ids).await?;
        let mut factors_by_vector = HashMap::<FeatureVectorId, Vec<FactorValueInfo>>::new();
        for factor in factor_rows {
            factors_by_vector
                .entry(factor.feature_vector_id)
                .or_default()
                .push(factor);
        }

        let revisions = expected_factor_ids
            .iter()
            .map(|id| -> QuantResult<FactorDefinitionRef> {
                let definition = definitions.get(id).ok_or_else(|| {
                    StorageError::invariant_violation(
                        Some("quant_factor_definition"),
                        format!("factor definition {id} disappeared during assembly"),
                    )
                })?;
                FactorDefinitionRef::try_from(definition).map_err(|error| {
                    ResearchError::DatasetBuild {
                        detail: format!(
                            "reconstruct persisted factor revision {}: {error}",
                            definition.factor_definition_id
                        ),
                    }
                    .into()
                })
            })
            .collect::<QuantResult<Vec<_>>>()?;
        let factor_serving_plane = FactorServingPlane::try_seal(revisions).map_err(|error| {
            ResearchError::DatasetBuild {
                detail: format!("seal feedback factor serving plane: {error}"),
            }
        })?;
        Self::materialize_rows(
            eligible,
            &features,
            factors_by_vector,
            &definitions,
            factor_serving_plane,
        )
    }

    fn materialize_rows(
        eligible: Vec<EligibleFeedback>,
        features: &HashMap<FeatureVectorId, FeatureVectorInfo>,
        mut factors_by_vector: HashMap<FeatureVectorId, Vec<FactorValueInfo>>,
        definitions: &HashMap<FactorDefinitionId, FactorDefinitionInfo>,
        factor_serving_plane: FactorServingPlane,
    ) -> QuantResult<MaterializedFeedback> {
        let mut rows = Vec::with_capacity(eligible.len());
        let mut examples = Vec::with_capacity(eligible.len());
        let mut feature_schema_version = None;
        let mut knowledge_lag_secs = None;
        let mut markets = HashSet::new();

        for seed in eligible {
            let feature_id = seed.context.feature_vector_id();
            let feature = features.get(&feature_id).ok_or_else(|| {
                StorageError::invariant_violation(
                    Some("quant_feature_vector"),
                    format!("feature vector {feature_id} disappeared during assembly"),
                )
            })?;
            Self::validate_feature(&seed.context, feature)?;
            let vector = FeatureVector::try_from(feature)?;
            let factors = Self::materialize_factors(
                &seed.context,
                factors_by_vector.remove(&feature_id).unwrap_or_default(),
                definitions,
            )?;
            Self::validate_breakdown(&seed.context, &factors)?;
            let model_token_id =
                vector
                    .token_id
                    .clone()
                    .ok_or_else(|| ResearchError::DatasetBuild {
                        detail: format!("serving feature vector {feature_id} has no primary token"),
                    })?;
            Self::validate_token_projection(&seed.context, feature, &model_token_id)?;
            let model_token_payout_ratio = match seed.context.outcome_side() {
                OutcomeSide::Yes => seed.resolution.token_payout_ratio,
                OutcomeSide::No => seed.resolution.token_payout_ratio.complement(),
            };
            let row = ModelLearningCohortRow::try_seal(NewModelLearningCohortRow {
                recommendation_id: seed.context.recommendation_id(),
                recommendation_report_id: seed.context.recommendation_report_id(),
                category: seed.context.category(),
                market_id: seed.context.market_id().clone(),
                event_id: seed.context.event_id().clone(),
                recommendation_token_id: seed.context.token_id().clone(),
                model_token_id: model_token_id.clone(),
                outcome_side: seed.context.outcome_side(),
                decision_at: seed.context.decision_at(),
                candidate_available_at: seed.context.available_at(),
                decision_policy_snapshot_id: seed.context.decision_policy_snapshot_id(),
                market_selection_id: seed.context.market_selection_id(),
                feature_vector_id: feature_id,
                model_run_id: seed.context.model_run_id(),
                model_version_id: seed.context.model_version_id(),
                factor_definition_versions: seed.context.factor_definition_versions().to_vec(),
                book_snapshot_ref: seed.context.book_snapshot_ref().clone(),
                data_quality_snapshot_id: seed.context.data_quality_snapshot_id(),
                resolution: seed.resolution,
                model_token_payout_ratio,
            })
            .map_err(|error| ResearchError::DatasetBuild {
                detail: format!("seal ModelLearning cohort row: {error}"),
            })?;
            let example = TrainingExample {
                example_id: row.example_id,
                market_id: row.market_id.clone(),
                token_id: row.model_token_id.clone(),
                selected_market: SelectedMarket::from(&feature.decision_capture.snapshot.selection),
                decision_boundary: feature.decision_boundary.clone(),
                sample_source: TrainingSampleSource::RecommendationFeedback,
                feature_vector: vector,
                factor_values: factors,
                labels: vec![TrainingLabel {
                    label_name: TOKEN_PAYOUT_RATIO,
                    horizon_secs: 0,
                    value: row.model_token_payout_ratio.inner(),
                    is_resolved: true,
                    matured_at: row.resolution.resolved_at,
                }],
                source_refs: feature.source_refs.0.clone(),
                decision_capture: Some(feature.decision_capture.clone()),
                lot_context: None,
                position_state: None,
                book_fidelity: None,
            };
            feature_schema_version =
                Self::one_schema(feature_schema_version, feature.feature_schema_version)?;
            knowledge_lag_secs = Self::one_lag(
                knowledge_lag_secs,
                feature.decision_boundary.knowledge_lag_secs(),
            )?;
            markets.insert(row.market_id.clone());
            rows.push(row);
            examples.push(example);
        }
        examples.sort_by(|left, right| {
            (
                left.market_id.as_str(),
                left.token_id.as_str(),
                left.decision_at(),
                left.example_id.as_uuid(),
            )
                .cmp(&(
                    right.market_id.as_str(),
                    right.token_id.as_str(),
                    right.decision_at(),
                    right.example_id.as_uuid(),
                ))
        });
        let count = u64::try_from(examples.len()).map_err(|error| ResearchError::DatasetBuild {
            detail: format!("feedback example count conversion failed: {error}"),
        })?;
        Ok(MaterializedFeedback {
            rows,
            examples,
            factor_serving_plane,
            coverage: DatasetCoverage {
                planned_samples: count,
                built_examples: count,
                markets: u64::try_from(markets.len()).map_err(|error| {
                    ResearchError::DatasetBuild {
                        detail: format!("feedback market count conversion failed: {error}"),
                    }
                })?,
                labels_available: count,
                ..DatasetCoverage::default()
            },
            feature_schema_version: feature_schema_version.ok_or_else(|| {
                ResearchError::DatasetBuild {
                    detail: "feedback materialization produced no feature schema".to_owned(),
                }
            })?,
            knowledge_lag_secs: knowledge_lag_secs.ok_or_else(|| ResearchError::DatasetBuild {
                detail: "feedback materialization produced no knowledge-lag contract".to_owned(),
            })?,
        })
    }
}

impl FeedbackCohortMaterializer {
    fn validate_feature(
        context: &FeedbackRecommendationContext,
        feature: &FeatureVectorInfo,
    ) -> QuantResult<()> {
        let capture_hash = CanonicalDigest::content_hash_json(&feature.decision_capture)?;
        let snapshot = &feature.decision_capture.snapshot;
        let selection = &snapshot.selection;
        let valid = feature.feature_vector_id == context.feature_vector_id()
            && &feature.market_id == context.market_id()
            && feature.decision_at == context.decision_at()
            && feature.decision_boundary == snapshot.boundary
            && feature.created_at <= context.available_at()
            && feature.decision_capture_hash == capture_hash
            && &snapshot.market_id == context.market_id()
            && snapshot.event_id == *context.event_id()
            && snapshot.book_snapshot_ref == *context.book_snapshot_ref()
            && snapshot.token_id == selection.primary_token_id
            && selection.market_id == *context.market_id()
            && selection.event_id == *context.event_id()
            && selection.category == context.category()
            && feature.decision_capture.identity == *context.identity()
            && feature.decision_capture.market_context == *context.market_context()
            && feature.decision_capture.data_quality == feature.data_quality;
        if !valid {
            return Err(ResearchError::DatasetBuild {
                detail: format!(
                    "feature vector {} does not match recommendation {} serving evidence",
                    feature.feature_vector_id,
                    context.recommendation_id()
                ),
            }
            .into());
        }
        let vector = FeatureVector::try_from(feature)?;
        if FeatureSourceRefs(vector.evidence_refs()) != feature.source_refs {
            return Err(ResearchError::DatasetBuild {
                detail: format!(
                    "feature vector {} source references do not reproduce",
                    feature.feature_vector_id
                ),
            }
            .into());
        }
        Ok(())
    }

    fn validate_token_projection(
        context: &FeedbackRecommendationContext,
        feature: &FeatureVectorInfo,
        model_token_id: &TokenId,
    ) -> QuantResult<()> {
        let selection = &feature.decision_capture.snapshot.selection;
        let valid = selection.primary_token_id == *model_token_id
            && match context.outcome_side() {
                OutcomeSide::Yes => context.token_id() == model_token_id,
                OutcomeSide::No => {
                    selection.secondary_token_id.as_ref() == Some(context.token_id())
                        && context.token_id() != model_token_id
                }
            };
        if !valid {
            return Err(ResearchError::DatasetBuild {
                detail: format!(
                    "recommendation {} token side does not match its frozen selection member",
                    context.recommendation_id()
                ),
            }
            .into());
        }
        Ok(())
    }

    fn materialize_factors(
        context: &FeedbackRecommendationContext,
        rows: Vec<FactorValueInfo>,
        definitions: &HashMap<FactorDefinitionId, FactorDefinitionInfo>,
    ) -> QuantResult<Vec<FactorValue>> {
        let mut by_definition = HashMap::new();
        for row in rows
            .into_iter()
            .filter(|row| row.model_run_id == context.model_run_id())
        {
            if row.market_id != *context.market_id()
                || row.decision_at != context.decision_at()
                || by_definition
                    .insert(row.factor_definition_id, row)
                    .is_some()
            {
                return Err(ResearchError::DatasetBuild {
                    detail: format!(
                        "recommendation {} has contradictory persisted factor rows",
                        context.recommendation_id()
                    ),
                }
                .into());
            }
        }
        if by_definition.len() != context.factor_definition_versions().len()
            || !context
                .factor_definition_versions()
                .iter()
                .all(|id| by_definition.contains_key(id))
        {
            return Err(StorageError::invariant_violation(
                Some("quant_factor_value"),
                format!(
                    "recommendation {} does not have one exact factor row per governed definition",
                    context.recommendation_id()
                ),
            )
            .into());
        }
        context
            .factor_definition_versions()
            .iter()
            .map(|id| {
                let value = by_definition.get(id).ok_or_else(|| {
                    StorageError::invariant_violation(
                        Some("quant_factor_value"),
                        format!("factor value {id} disappeared during assembly"),
                    )
                })?;
                let definition = definitions.get(id).ok_or_else(|| {
                    StorageError::invariant_violation(
                        Some("quant_factor_definition"),
                        format!("factor definition {id} disappeared during assembly"),
                    )
                })?;
                FactorValue::try_from_persistence(value, definition)
            })
            .collect()
    }

    fn validate_breakdown(
        context: &FeedbackRecommendationContext,
        factors: &[FactorValue],
    ) -> QuantResult<()> {
        let mut breakdown = context
            .factor_breakdown()
            .0
            .iter()
            .map(|entry| (entry.factor_name.as_str(), entry))
            .collect::<HashMap<_, _>>();
        if breakdown.len() != factors.len() {
            return Err(ResearchError::DatasetBuild {
                detail: format!(
                    "recommendation {} factor breakdown cardinality does not match its ledger rows",
                    context.recommendation_id()
                ),
            }
            .into());
        }
        for factor in factors {
            let Some(entry) = breakdown.remove(factor.name.as_str()) else {
                return Err(ResearchError::DatasetBuild {
                    detail: format!(
                        "recommendation {} factor breakdown omits {}",
                        context.recommendation_id(),
                        factor.name
                    ),
                }
                .into());
            };
            if entry.family != factor.family
                || entry.value_state != factor.value_state()
                || entry.raw_value != factor.raw_value
                || entry.normalized_score != factor.normalized_score()
                || entry.normalization_source != factor.normalization_source()
                || entry.indeterminate_reason != factor.indeterminate_reason()
                || entry.direction != factor.direction
                || entry.confidence != factor.confidence
                || entry.explanation != factor.explanation.headline
            {
                return Err(ResearchError::DatasetBuild {
                    detail: format!(
                        "recommendation {} factor breakdown disagrees with persisted {}",
                        context.recommendation_id(),
                        factor.name
                    ),
                }
                .into());
            }
        }
        Ok(())
    }

    fn one_schema(
        current: Option<SchemaVersion>,
        next: SchemaVersion,
    ) -> QuantResult<Option<SchemaVersion>> {
        if current.is_some_and(|version| version != next) {
            return Err(ResearchError::DatasetBuild {
                detail: "one feedback Dataset cannot mix feature schema versions".to_owned(),
            }
            .into());
        }
        Ok(Some(next))
    }

    fn one_lag(current: Option<u64>, next: u64) -> QuantResult<Option<u64>> {
        if current.is_some_and(|lag| lag != next) {
            return Err(ResearchError::DatasetBuild {
                detail: "one feedback Dataset cannot mix knowledge-lag contracts".to_owned(),
            }
            .into());
        }
        Ok(Some(next))
    }
}

impl FeedbackDatasetService {
    async fn persist_cohort(
        &self,
        window: FeedbackCohortWindow,
        counts: DatasetCohortCounts,
        capability_registry_hashes: CapabilityRegistryHashes,
        rows: Vec<ModelLearningCohortRow>,
    ) -> QuantResult<DatasetCohortManifest> {
        let artifact = ModelLearningCohortArtifact {
            format_version: MODEL_LEARNING_COHORT_FORMAT_VERSION,
            window: window.clone(),
            counts: counts.clone(),
            rows,
        };
        let source_hash = artifact
            .source_hash()
            .map_err(|error| ResearchError::DatasetBuild {
                detail: format!("hash ModelLearning cohort artifact: {error}"),
            })?;
        let bytes = ModelLearningCohortCodec::encode(&artifact)?;
        let bytes_hash = ModelLearningCohortCodec::bytes_hash(&bytes);
        let schema_hash = ModelLearningCohortCodec::schema_hash()?;
        let key = ArtifactKey::new(ArtifactNamespace::FeedbackCohort, source_hash.hex(), "json")?;
        let uri = self.artifact_store.put(key, &bytes).await?;
        let persisted = self.artifact_store.get(&uri).await?;
        if ModelLearningCohortCodec::bytes_hash(&persisted) != bytes_hash
            || ModelLearningCohortCodec::decode(&persisted)? != artifact
        {
            return Err(ResearchError::ArtifactHashMismatch {
                expected: bytes_hash.to_string(),
                actual: ModelLearningCohortCodec::bytes_hash(&persisted).to_string(),
            }
            .into());
        }
        let manifest = DatasetCohortManifest {
            format_version: DATASET_COHORT_MANIFEST_FORMAT_VERSION,
            cohort: FeedbackCohort::ModelLearning,
            window,
            artifact: DatasetCohortArtifactRef {
                uri,
                bytes_hash,
                schema_hash,
                source_hash,
                row_count: counts.included_count(),
            },
            counts,
            capability_registry_hashes,
        };
        manifest
            .validate()
            .map_err(|error| ResearchError::DatasetBuild {
                detail: format!("validate ModelLearning cohort manifest: {error}"),
            })?;
        Ok(manifest)
    }
}
