//! Training-dataset ledger persistence system contracts.

use chrono::{Duration as ChronoDuration, Utc};
use quant_pivot_error::storage::{StorageError, entity};
use quant_pivot_models::{
    domain::quant::{CompleteTrainingDatasetBuild, NewModelVersion},
    enums::{
        model::ModelFamily,
        quant::{DatasetPurpose, PublicationStatus, TrainingDatasetStatus},
    },
    types::{
        ArtifactUri, ContentHash, DecisionPolicySnapshotId, ModelInputContract, ModelSpecId,
        ModelTrainingContract, ModelVersionId, SchemaVersion, TrainingDatasetId,
        TrainingSampleSources, default_sample_sources, model_metrics::ModelVersionMetrics,
        model_training::ModelTrainingObjective,
    },
};
use quant_pivot_repository::{
    postgres::{PgModelRegistryRepository, PgTrainingDatasetRepository},
    traits::{ModelRegistryRepository, TrainingDatasetRepository},
};
use quant_pivot_system_tests::{
    postgres::setup_pg,
    support::{
        execution_pg_seed, model_spec_fixtures,
        policy_fixtures::bootstrap_default_policy_bundle,
        research_fixtures::{
            DatasetLedgerFixture, DatasetLedgerSeed, DatasetSourceSeed, seed_dataset_source,
        },
    },
};
use sea_orm::{ConnectionTrait, DatabaseConnection, DbBackend, Statement};

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

async fn new_fixture(
    db: &DatabaseConnection,
    dataset_id: TrainingDatasetId,
    model_spec_id: ModelSpecId,
    decision_policy_snapshot_id: DecisionPolicySnapshotId,
) -> DatasetLedgerFixture {
    let window_start = Utc::now() - ChronoDuration::hours(2);
    let window_end = window_start + ChronoDuration::hours(1);
    let model_spec_definition_hash = model_spec_fixtures::new_model_spec_fixture(
        model_spec_id,
        "pg-dataset-it",
        ModelFamily::WeightedFactor,
        86_400,
        ModelInputContract::single_required("book.mid"),
        ModelTrainingContract::settlement_default(),
    )
    .definition_hash;
    let source_lineage = seed_dataset_source(
        db,
        DatasetSourceSeed {
            scope: format!("pg-dataset-it:{dataset_id}"),
            profile_ref: execution_pg_seed::fixture_profile_ref(),
            decision_policy_snapshot_id,
            window_start,
            window_end,
            pit_cutoff: window_end + ChronoDuration::hours(1),
        },
    )
    .await
    .expect("source lineage");
    let hash = dataset_hash(&dataset_id);
    DatasetLedgerFixture::try_new(DatasetLedgerSeed {
        training_dataset_id: dataset_id,
        model_spec_id,
        model_spec_definition_hash,
        source_lineage,
        cohort_manifest: None,
        window_start,
        window_end,
        purpose: DatasetPurpose::Training,
        knowledge_lag_secs: 10,
        sample_interval_secs: 3600,
        horizons_secs: vec![3600],
        feature_schema_version: Some(SchemaVersion::FIRST),
        sample_sources: Some(TrainingSampleSources(default_sample_sources())),
        feature_schema_hash: hash,
        factor_schema_hash: hash,
        label_schema_hash: hash,
        semantic_dataset_hash: hash,
        source_fingerprint: content_hash('f'),
        sample_count: 42,
    })
    .expect("dataset fixture")
}

fn completion(
    fixture: &DatasetLedgerFixture,
    status: TrainingDatasetStatus,
) -> CompleteTrainingDatasetBuild {
    let failure_detail = matches!(
        status,
        TrainingDatasetStatus::InsufficientLabels | TrainingDatasetStatus::Failed
    )
    .then(|| "fixture terminal diagnostic".to_owned());
    fixture
        .completion(
            status,
            dataset_hash(&fixture.plan.training_dataset_id),
            ArtifactUri::parse("file:///tmp/pg-training-dataset.parquet").expect("uri"),
            fixture.coverage(),
            failure_detail,
        )
        .expect("valid completion")
}

async fn create_ready(
    db: &DatabaseConnection,
    repo: &PgTrainingDatasetRepository,
    dataset_id: TrainingDatasetId,
    model_spec_id: ModelSpecId,
    decision_policy_snapshot_id: DecisionPolicySnapshotId,
) {
    let fixture = new_fixture(db, dataset_id, model_spec_id, decision_policy_snapshot_id).await;
    repo.create_plan(fixture.plan.clone())
        .await
        .expect("create plan");
    repo.start_build(&dataset_id).await.expect("start build");
    repo.complete_build(
        &dataset_id,
        completion(&fixture, TrainingDatasetStatus::Ready),
    )
    .await
    .expect("complete build");
}

pub async fn quant_training_dataset_crud() {
    let (pool, _container) = setup_pg().await;
    let db = pool.connection().clone();
    let rc_id = seed_runtime_config(&db).await;
    let model_spec_id = seed_model_spec(&db).await;
    let dataset_id = TrainingDatasetId::from_v7();
    let repo = PgTrainingDatasetRepository::new(db.clone());

    let fixture = new_fixture(&db, dataset_id, model_spec_id, rc_id).await;
    repo.create_plan(fixture.plan.clone())
        .await
        .expect("create plan");
    let building = repo.start_build(&dataset_id).await.expect("start build");
    assert!(building.dataset_hash.is_none());
    let created = repo
        .complete_build(
            &dataset_id,
            completion(&fixture, TrainingDatasetStatus::Ready),
        )
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

pub async fn dataset_artifacts_are_immutable() {
    let (pool, _container) = setup_pg().await;
    let db = pool.connection().clone();
    let rc_id = seed_runtime_config(&db).await;
    let model_spec_id = seed_model_spec(&db).await;
    let dataset_id = TrainingDatasetId::from_v7();
    let repo = PgTrainingDatasetRepository::new(db.clone());
    let fixture = new_fixture(&db, dataset_id, model_spec_id, rc_id).await;
    let source_slice_id = fixture.plan.source_slice_id;

    repo.create_plan(fixture.plan.clone())
        .await
        .expect("create immutable plan");
    repo.start_build(&dataset_id)
        .await
        .expect("start immutable build");
    repo.complete_build(
        &dataset_id,
        completion(&fixture, TrainingDatasetStatus::Ready),
    )
    .await
    .expect("complete immutable build");

    let dataset_tamper = db
        .execute_raw(Statement::from_sql_and_values(
            DbBackend::Postgres,
            "UPDATE quant_training_dataset SET manifest_hash = $1 \
             WHERE training_dataset_id = $2",
            [
                content_hash('e').to_string().into(),
                dataset_id.as_uuid().into(),
            ],
        ))
        .await;
    assert!(
        dataset_tamper.is_err(),
        "terminal dataset evidence must reject direct SQL tampering"
    );

    let source_tamper = db
        .execute_raw(Statement::from_sql_and_values(
            DbBackend::Postgres,
            "UPDATE quant_source_slice SET manifest_hash = $1 WHERE source_slice_id = $2",
            [
                content_hash('e').to_string().into(),
                source_slice_id.as_uuid().into(),
            ],
        ))
        .await;
    assert!(
        source_tamper.is_err(),
        "terminal source-slice evidence must reject direct SQL tampering"
    );

    let expired = repo
        .expire(&dataset_id)
        .await
        .expect("ready dataset must retain its controlled expiry transition");
    assert_eq!(expired.status, TrainingDatasetStatus::Expired);
}

pub async fn training_dataset_rejects_drift() {
    let (pool, _container) = setup_pg().await;
    let db = pool.connection().clone();
    let rc_id = seed_runtime_config(&db).await;
    let model_spec_id = seed_model_spec(&db).await;
    let repo = PgTrainingDatasetRepository::new(db.clone());
    let mut plan = new_fixture(&db, TrainingDatasetId::from_v7(), model_spec_id, rc_id)
        .await
        .plan;
    plan.model_spec_definition_hash = content_hash('f');

    assert!(matches!(
        repo.create_plan(plan).await,
        Err(StorageError::InvariantViolation {
            entity: Some(entity::QUANT_TRAINING_DATASET),
            ..
        })
    ));
}

pub async fn training_dataset_status_machine() {
    let (pool, _container) = setup_pg().await;
    let db = pool.connection().clone();
    let rc_id = seed_runtime_config(&db).await;
    let model_spec_id = seed_model_spec(&db).await;
    let dataset_id = TrainingDatasetId::from_v7();
    let repo = PgTrainingDatasetRepository::new(db.clone());

    let fixture = new_fixture(&db, dataset_id, model_spec_id, rc_id).await;
    repo.create_plan(fixture.plan.clone())
        .await
        .expect("create planned");

    repo.start_build(&dataset_id)
        .await
        .expect("planned -> building");
    repo.complete_build(
        &dataset_id,
        completion(&fixture, TrainingDatasetStatus::Ready),
    )
    .await
    .expect("building -> ready");

    let insufficient_id = TrainingDatasetId::from_v7();
    let insufficient_fixture = new_fixture(&db, insufficient_id, model_spec_id, rc_id).await;
    repo.create_plan(insufficient_fixture.plan.clone())
        .await
        .expect("create insufficient plan");
    repo.start_build(&insufficient_id)
        .await
        .expect("start insufficient");
    repo.complete_build(
        &insufficient_id,
        completion(
            &insufficient_fixture,
            TrainingDatasetStatus::InsufficientLabels,
        ),
    )
    .await
    .expect("complete insufficient");

    let err = repo
        .complete_build(
            &insufficient_id,
            completion(&insufficient_fixture, TrainingDatasetStatus::Ready),
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

pub async fn model_version_training_key() {
    let (pool, _container) = setup_pg().await;
    let db = pool.connection().clone();
    let rc_id = seed_runtime_config(&db).await;
    let model_spec_id = seed_model_spec(&db).await;
    let dataset_id = TrainingDatasetId::from_v7();
    let repo = PgTrainingDatasetRepository::new(db.clone());
    let registry = PgModelRegistryRepository::new(db.clone());

    create_ready(&db, &repo, dataset_id, model_spec_id, rc_id).await;

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
