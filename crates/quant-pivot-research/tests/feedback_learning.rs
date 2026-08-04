use quant_pivot_models::{
    domain::{
        ports::{FeedbackDatasetRole, FeedbackLearningStageArtifactRef},
        quant::ResearchJobArtifactRef,
    },
    enums::quant::{DatasetPurpose, FeedbackStage},
    hashing::CanonicalDigest,
    types::{
        ArtifactUri, ContentHash, FeedbackCycleId, FeedbackLearningStageArtifactId, ModelRunId,
        ModelVersionId, ResearchJobId, TrainingDatasetId,
    },
};
use quant_pivot_research::feedback_learning::{
    FEEDBACK_LEARNING_ARTIFACT_FORMAT_VERSION, FeedbackDatasetStageResult,
    FeedbackLearningStageArtifact, FeedbackLearningStageCodec, FeedbackLearningStageResults,
    FeedbackTrainingStageResult,
};

const fn content_hash(seed: u8) -> ContentHash {
    ContentHash::from_bytes([seed; 32])
}

fn artifact_uri(name: &str) -> ArtifactUri {
    ArtifactUri::parse(format!("memory://feedback-learning/{name}"))
        .expect("test artifact URI must be valid")
}

fn dataset_result(
    role: FeedbackDatasetRole,
    purpose: DatasetPurpose,
    seed: u8,
) -> FeedbackDatasetStageResult {
    FeedbackDatasetStageResult {
        role,
        training_dataset_id: TrainingDatasetId::from_v7(),
        purpose,
        dataset_hash: content_hash(seed),
        manifest_hash: content_hash(seed.saturating_add(1)),
        artifact_bytes_hash: content_hash(seed.saturating_add(2)),
        parquet_uri: artifact_uri(&format!("dataset-{seed}")),
        cohort_manifest_hash: content_hash(seed.saturating_add(3)),
        sample_count: 100,
    }
}

fn dataset_artifact() -> FeedbackLearningStageArtifact {
    let first_recipe = content_hash(1);
    let second_recipe = content_hash(2);
    FeedbackLearningStageArtifact::try_new(
        FeedbackCycleId::from_idempotency_hash(&content_hash(10)),
        content_hash(10),
        content_hash(11),
        content_hash(12),
        None,
        FeedbackLearningStageResults::DatasetSeal(vec![
            dataset_result(
                FeedbackDatasetRole::CandidateTraining {
                    candidate_recipe_hash: first_recipe,
                },
                DatasetPurpose::Training,
                20,
            ),
            dataset_result(
                FeedbackDatasetRole::CandidateTraining {
                    candidate_recipe_hash: second_recipe,
                },
                DatasetPurpose::Training,
                24,
            ),
            dataset_result(
                FeedbackDatasetRole::CandidateCalibration {
                    candidate_recipe_hash: first_recipe,
                },
                DatasetPurpose::Calibration,
                28,
            ),
            dataset_result(
                FeedbackDatasetRole::CandidateCalibration {
                    candidate_recipe_hash: second_recipe,
                },
                DatasetPurpose::Calibration,
                32,
            ),
            dataset_result(
                FeedbackDatasetRole::SharedEvaluation,
                DatasetPurpose::Evaluation,
                36,
            ),
        ]),
    )
    .expect("canonical DatasetSeal artifact must be valid")
}

trait FeedbackLearningFixtureExt {
    fn dataset_reference(&self) -> FeedbackLearningStageArtifactRef;
}

impl FeedbackLearningFixtureExt for FeedbackLearningStageArtifact {
    fn dataset_reference(&self) -> FeedbackLearningStageArtifactRef {
        let bytes =
            FeedbackLearningStageCodec::encode(self).expect("DatasetSeal artifact must encode");
        self.reference(
            ResearchJobId::from_v7(),
            ResearchJobArtifactRef {
                uri: artifact_uri("dataset-stage.json"),
                content_hash: CanonicalDigest::content_hash_bytes(&bytes),
            },
        )
        .expect("DatasetSeal reference must be valid")
    }
}

#[test]
fn artifact_identity_is_versioned() {
    assert_eq!(FEEDBACK_LEARNING_ARTIFACT_FORMAT_VERSION, 1);
    let cycle_id = FeedbackCycleId::from_idempotency_hash(&content_hash(1));
    let dataset =
        FeedbackLearningStageArtifactId::from_cycle_stage(cycle_id, FeedbackStage::DatasetSeal)
            .expect("DatasetSeal must own a learning artifact");
    let training =
        FeedbackLearningStageArtifactId::from_cycle_stage(cycle_id, FeedbackStage::Training)
            .expect("Training must own a learning artifact");
    assert_ne!(dataset, training);
    assert!(
        FeedbackLearningStageArtifactId::from_cycle_stage(cycle_id, FeedbackStage::Coverage)
            .is_none()
    );
    assert_ne!(
        FeedbackLearningStageCodec::schema_hash().expect("learning-stage schema hash"),
        content_hash(0)
    );
}

#[test]
fn dataset_bytes_round_trip() {
    let artifact = dataset_artifact();
    let bytes = FeedbackLearningStageCodec::encode(&artifact).expect("artifact must encode");
    let decoded = FeedbackLearningStageCodec::decode(&bytes).expect("artifact must decode");

    assert_eq!(decoded, artifact);
    assert_eq!(
        FeedbackLearningStageCodec::encode(&decoded).expect("decoded artifact must re-encode"),
        bytes
    );

    let pretty = serde_json::to_vec_pretty(&artifact).expect("artifact must serialize");
    assert!(FeedbackLearningStageCodec::decode(&pretty).is_err());
}

#[test]
fn dataset_drift_is_rejected() {
    let artifact = dataset_artifact();

    let mut mismatched_purpose = artifact.clone();
    let FeedbackLearningStageResults::DatasetSeal(results) = &mut mismatched_purpose.results else {
        panic!("fixture must be DatasetSeal");
    };
    results[0].purpose = DatasetPurpose::Calibration;
    assert!(mismatched_purpose.validate().is_err());

    let mut missing_evaluation = artifact.clone();
    let FeedbackLearningStageResults::DatasetSeal(results) = &mut missing_evaluation.results else {
        panic!("fixture must be DatasetSeal");
    };
    results.pop();
    assert!(missing_evaluation.validate().is_err());

    let mut duplicate_recipe = artifact.clone();
    let FeedbackLearningStageResults::DatasetSeal(results) = &mut duplicate_recipe.results else {
        panic!("fixture must be DatasetSeal");
    };
    results[1].role = results[0].role;
    assert!(duplicate_recipe.validate().is_err());

    let mut non_canonical = artifact;
    let FeedbackLearningStageResults::DatasetSeal(results) = &mut non_canonical.results else {
        panic!("fixture must be DatasetSeal");
    };
    results.swap(0, 1);
    assert!(non_canonical.validate().is_err());
}

#[test]
fn predecessor_tamper_is_rejected() {
    let dataset = dataset_artifact();
    let cycle_id = dataset.feedback_cycle_id;
    let recipe = content_hash(1);
    let training_results =
        FeedbackLearningStageResults::Training(vec![FeedbackTrainingStageResult {
            candidate_recipe_hash: recipe,
            model_version_id: ModelVersionId::from_v7(),
            model_run_id: ModelRunId::from_v7(),
            training_dataset_id: TrainingDatasetId::from_v7(),
            model_artifact_hash: content_hash(40),
            serving_contract_hash: content_hash(41),
            training_input_hash: content_hash(42),
        }]);
    let training = FeedbackLearningStageArtifact::try_new(
        cycle_id,
        dataset.cycle_idempotency_hash,
        dataset.candidate_family_hash,
        content_hash(43),
        Some(dataset.dataset_reference()),
        training_results,
    )
    .expect("exact predecessor must produce a valid Training artifact");

    let mut wrong_artifact_id = training.clone();
    wrong_artifact_id.artifact_id =
        FeedbackLearningStageArtifactId::from_cycle_stage(cycle_id, FeedbackStage::DatasetSeal)
            .expect("DatasetSeal must own an artifact");
    assert!(wrong_artifact_id.validate().is_err());

    let mut wrong_previous_stage = training.clone();
    let previous = wrong_previous_stage
        .previous
        .as_mut()
        .expect("Training fixture must have a predecessor");
    previous.stage = FeedbackStage::Training;
    assert!(wrong_previous_stage.validate().is_err());

    let mut wrong_previous_cycle = training;
    let previous = wrong_previous_cycle
        .previous
        .as_mut()
        .expect("Training fixture must have a predecessor");
    previous.feedback_cycle_id = FeedbackCycleId::from_idempotency_hash(&content_hash(99));
    assert!(wrong_previous_cycle.validate().is_err());
}

#[test]
fn output_drift_is_rejected() {
    let mut wrong_cycle_hash = dataset_artifact();
    wrong_cycle_hash.cycle_idempotency_hash = content_hash(99);
    assert!(wrong_cycle_hash.validate().is_err());

    let dataset = dataset_artifact();
    let shared_model_version_id = ModelVersionId::from_v7();
    let shared_model_run_id = ModelRunId::from_v7();
    let shared_training_dataset_id = TrainingDatasetId::from_v7();
    let training_results = [content_hash(1), content_hash(2)]
        .into_iter()
        .map(|candidate_recipe_hash| FeedbackTrainingStageResult {
            candidate_recipe_hash,
            model_version_id: shared_model_version_id,
            model_run_id: shared_model_run_id,
            training_dataset_id: shared_training_dataset_id,
            model_artifact_hash: content_hash(40),
            serving_contract_hash: content_hash(41),
            training_input_hash: content_hash(42),
        })
        .collect();
    assert!(
        FeedbackLearningStageArtifact::try_new(
            dataset.feedback_cycle_id,
            dataset.cycle_idempotency_hash,
            dataset.candidate_family_hash,
            content_hash(43),
            Some(dataset.dataset_reference()),
            FeedbackLearningStageResults::Training(training_results),
        )
        .is_err()
    );
}
