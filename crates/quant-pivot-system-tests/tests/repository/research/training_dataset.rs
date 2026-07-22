//! Training-dataset ledger persistence system contracts.

use chrono::{Duration as ChronoDuration, Utc};
use quant_pivot_error::storage::{StorageError, entity};
use quant_pivot_models::{
    domain::quant::{CompleteTrainingDatasetBuild, NewModelVersion, NewTrainingDatasetPlan},
    enums::{
        model::ModelFamily,
        quant::{DatasetPurpose, PublicationStatus, TrainingDatasetStatus},
    },
    hashing::CanonicalDigest,
    types::{
        ArtifactUri, ContentHash, DATASET_ARTIFACT_FORMAT_VERSION, DatasetCoverage,
        DatasetManifest, DecisionPolicySnapshotId, ModelInputContract, ModelSpecId,
        ModelTrainingContract, ModelVersionId, SchemaVersion, TrainingDatasetId,
        TrainingHorizonsSecs, TrainingSampleSources, default_sample_sources,
        model_metrics::ModelVersionMetrics, model_training::ModelTrainingObjective,
    },
};
use quant_pivot_repository::{
    postgres::{PgModelRegistryRepository, PgTrainingDatasetRepository},
    traits::{ModelRegistryRepository, TrainingDatasetRepository},
};
use quant_pivot_system_tests::{
    postgres::setup_pg,
    support::{
        execution_pg_seed, model_spec_fixtures, policy_fixtures::bootstrap_default_policy_bundle,
    },
};
use sea_orm::DatabaseConnection;

fn content_hash(seed: char) -> ContentHash {
    ContentHash::parse(&format!("blake3:{}", seed.to_string().repeat(64))).expect("hash")
}

async fn seed_runtime_config(db: &DatabaseConnection) -> DecisionPolicySnapshotId {
    bootstrap_default_policy_bundle(db, "pg-training-dataset-it", "integration test").await
}

async fn seed_model_spec(db: &DatabaseConnection) -> ModelSpecId {
    let model_spec_id = ModelSpecId::from_v7();
    PgModelRegistryRepository::new(db.clone())
        .create_model_spec(model_spec_fixtures::new_model_spec_fixture(
            model_spec_id,
            "pg-dataset-it",
            ModelFamily::WeightedFactor,
            86_400,
            ModelInputContract::single_required("book.mid"),
            ModelTrainingContract::settlement_default(),
        ))
        .await
        .expect("model spec");
    model_spec_id
}

fn dataset_hash(dataset_id: &TrainingDatasetId) -> ContentHash {
    let mut hex = dataset_id.as_uuid().simple().to_string();
    hex.truncate(64);
    hex.push_str(&"0".repeat(64usize.saturating_sub(hex.len())));
    ContentHash::parse(&format!("blake3:{hex}")).expect("hash")
}

fn new_plan(
    dataset_id: TrainingDatasetId,
    model_spec_id: ModelSpecId,
    decision_policy_snapshot_id: DecisionPolicySnapshotId,
) -> NewTrainingDatasetPlan {
    let window_start = Utc::now() - ChronoDuration::hours(2);
    let model_spec_definition_hash = model_spec_fixtures::new_model_spec_fixture(
        model_spec_id,
        "pg-dataset-it",
        ModelFamily::WeightedFactor,
        86_400,
        ModelInputContract::single_required("book.mid"),
        ModelTrainingContract::settlement_default(),
    )
    .definition_hash;
    NewTrainingDatasetPlan {
        training_dataset_id: dataset_id,
        model_spec_id,
        model_spec_definition_hash,
        window_start,
        window_end: window_start + ChronoDuration::hours(1),
        purpose: DatasetPurpose::Training,
        knowledge_lag_secs: 10,
        sample_interval_secs: 3600,
        horizons_secs: TrainingHorizonsSecs(vec![3600]),
        feature_schema_version: Some(SchemaVersion::FIRST),
        sample_sources: Some(TrainingSampleSources(default_sample_sources())),
        decision_policy_snapshot_id,
    }
}

fn completion(
    plan: &NewTrainingDatasetPlan,
    status: TrainingDatasetStatus,
) -> CompleteTrainingDatasetBuild {
    let hash = dataset_hash(&plan.training_dataset_id);
    let manifest = DatasetManifest {
        format_version: DATASET_ARTIFACT_FORMAT_VERSION,
        training_dataset_id: plan.training_dataset_id,
        profile_ref: execution_pg_seed::fixture_profile_ref(),
        research_program_hash: content_hash('4'),
        source_slice: execution_pg_seed::source_slice_ref('5'),
        model_spec_id: plan.model_spec_id,
        model_spec_definition_hash: plan.model_spec_definition_hash,
        trade_policy_artifact_id: None,
        trade_policy_hash: None,
        decision_policy_snapshot_id: plan.decision_policy_snapshot_id,
        window_start: plan.window_start,
        window_end: plan.window_end,
        purpose: plan.purpose,
        knowledge_lag_secs: u64::try_from(plan.knowledge_lag_secs).expect("knowledge lag"),
        sample_interval_secs: u64::try_from(plan.sample_interval_secs).expect("sample interval"),
        horizons_secs: plan.horizons_secs.0.clone(),
        feature_schema_hash: hash,
        factor_schema_hash: hash,
        label_schema_hash: hash,
        semantic_dataset_hash: hash,
        source_fingerprint: content_hash('f'),
        sample_count: 42,
    };
    let manifest_hash =
        CanonicalDigest::content_hash_json(&manifest).expect("canonical manifest hash");
    CompleteTrainingDatasetBuild {
        status,
        feature_schema_hash: hash,
        factor_schema_hash: hash,
        label_schema_hash: hash,
        dataset_hash: hash,
        manifest_hash,
        manifest_json: manifest,
        artifact_bytes_hash: hash,
        parquet_uri: ArtifactUri::parse("file:///tmp/pg-training-dataset.parquet").expect("uri"),
        sample_count: 42,
        coverage_json: DatasetCoverage {
            planned_samples: 42,
            ..DatasetCoverage::default()
        },
        failure_detail: None,
    }
}

async fn create_ready(
    repo: &PgTrainingDatasetRepository,
    dataset_id: TrainingDatasetId,
    model_spec_id: ModelSpecId,
    decision_policy_snapshot_id: DecisionPolicySnapshotId,
) {
    let plan = new_plan(dataset_id, model_spec_id, decision_policy_snapshot_id);
    repo.create_plan(plan.clone()).await.expect("create plan");
    repo.start_build(&dataset_id).await.expect("start build");
    repo.complete_build(&dataset_id, completion(&plan, TrainingDatasetStatus::Ready))
        .await
        .expect("complete build");
}

pub async fn quant_training_dataset_migration_and_crud() {
    let (pool, _container) = setup_pg().await;
    let db = pool.connection().clone();
    let rc_id = seed_runtime_config(&db).await;
    let model_spec_id = seed_model_spec(&db).await;
    let dataset_id = TrainingDatasetId::from_v7();
    let repo = PgTrainingDatasetRepository::new(db.clone());

    let plan = new_plan(dataset_id, model_spec_id, rc_id);
    repo.create_plan(plan.clone()).await.expect("create plan");
    let building = repo.start_build(&dataset_id).await.expect("start build");
    assert!(building.dataset_hash.is_none());
    let created = repo
        .complete_build(&dataset_id, completion(&plan, TrainingDatasetStatus::Ready))
        .await
        .expect("complete build");
    assert_eq!(created.training_dataset_id, dataset_id);
    assert_eq!(created.status, TrainingDatasetStatus::Ready);
    assert_eq!(created.sample_count, Some(42));

    let found = repo
        .find_by_id(&dataset_id)
        .await
        .expect("find")
        .expect("row");
    assert_eq!(found.dataset_hash, created.dataset_hash);
}

pub async fn training_dataset_plan_rejects_model_spec_definition_drift() {
    let (pool, _container) = setup_pg().await;
    let db = pool.connection().clone();
    let rc_id = seed_runtime_config(&db).await;
    let model_spec_id = seed_model_spec(&db).await;
    let repo = PgTrainingDatasetRepository::new(db);
    let mut plan = new_plan(TrainingDatasetId::from_v7(), model_spec_id, rc_id);
    plan.model_spec_definition_hash = content_hash('f');

    assert!(matches!(
        repo.create_plan(plan).await,
        Err(StorageError::InvariantViolation {
            entity: Some(entity::QUANT_TRAINING_DATASET),
            ..
        })
    ));
}

pub async fn training_dataset_status_transitions_enforce_state_machine() {
    let (pool, _container) = setup_pg().await;
    let db = pool.connection().clone();
    let rc_id = seed_runtime_config(&db).await;
    let model_spec_id = seed_model_spec(&db).await;
    let dataset_id = TrainingDatasetId::from_v7();
    let repo = PgTrainingDatasetRepository::new(db.clone());

    let plan = new_plan(dataset_id, model_spec_id, rc_id);
    repo.create_plan(plan.clone())
        .await
        .expect("create planned");

    repo.start_build(&dataset_id)
        .await
        .expect("planned -> building");
    repo.complete_build(&dataset_id, completion(&plan, TrainingDatasetStatus::Ready))
        .await
        .expect("building -> ready");

    let insufficient_id = TrainingDatasetId::from_v7();
    let insufficient_plan = new_plan(insufficient_id, model_spec_id, rc_id);
    repo.create_plan(insufficient_plan.clone())
        .await
        .expect("create insufficient plan");
    repo.start_build(&insufficient_id)
        .await
        .expect("start insufficient");
    repo.complete_build(
        &insufficient_id,
        completion(
            &insufficient_plan,
            TrainingDatasetStatus::InsufficientLabels,
        ),
    )
    .await
    .expect("complete insufficient");

    let err = repo
        .complete_build(
            &insufficient_id,
            completion(&insufficient_plan, TrainingDatasetStatus::Ready),
        )
        .await
        .expect_err("insufficient -> ready must conflict");
    assert!(
        matches!(
            err,
            StorageError::IllegalTransition {
                entity: entity::QUANT_TRAINING_DATASET,
                ..
            }
        ),
        "expected illegal transition, got {err:?}"
    );
}

pub async fn model_version_training_dataset_foreign_key() {
    let (pool, _container) = setup_pg().await;
    let db = pool.connection().clone();
    let rc_id = seed_runtime_config(&db).await;
    let model_spec_id = seed_model_spec(&db).await;
    let dataset_id = TrainingDatasetId::from_v7();
    let repo = PgTrainingDatasetRepository::new(db.clone());
    let registry = PgModelRegistryRepository::new(db.clone());

    create_ready(&repo, dataset_id, model_spec_id, rc_id).await;

    let hash = content_hash('a');
    registry
        .create_model_version(NewModelVersion {
            model_version_id: ModelVersionId::from_v7(),
            model_spec_id,
            version: 1,
            artifact_hash: hash,
            category_scope: None,
            profile_ref: execution_pg_seed::fixture_profile_ref(),
            training_dataset_id: Some(dataset_id),
            trade_policy_artifact_id: None,
            trade_policy_hash: None,
            publish_path_set_id: None,
            derivation: NewModelVersion::training_derivation(),
            metrics: ModelVersionMetrics::not_measured("test fixture"),
            training_objective: ModelTrainingObjective::hand_authored("test fixture"),
            quality_gate_report: None,
            publication_status: PublicationStatus::Candidate,
            published_at: None,
            retired_at: None,
        })
        .await
        .expect("valid training_dataset_id FK");

    let missing_dataset = TrainingDatasetId::from_v7();
    let fk_err = registry
        .create_model_version(NewModelVersion {
            model_version_id: ModelVersionId::from_v7(),
            model_spec_id,
            version: 2,
            artifact_hash: hash,
            category_scope: None,
            profile_ref: execution_pg_seed::fixture_profile_ref(),
            training_dataset_id: Some(missing_dataset),
            trade_policy_artifact_id: None,
            trade_policy_hash: None,
            publish_path_set_id: None,
            derivation: NewModelVersion::training_derivation(),
            metrics: ModelVersionMetrics::not_measured("test fixture"),
            training_objective: ModelTrainingObjective::hand_authored("test fixture"),
            quality_gate_report: None,
            publication_status: PublicationStatus::Candidate,
            published_at: None,
            retired_at: None,
        })
        .await
        .expect_err("missing training_dataset_id must fail FK");
    assert!(
        matches!(fk_err, StorageError::Database(_)),
        "expected database FK error, got {fk_err:?}"
    );
}
