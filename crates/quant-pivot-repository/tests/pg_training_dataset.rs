//! Training-dataset ledger integration tests (Postgres + testcontainers).

use chrono::{Duration as ChronoDuration, Utc};
use quant_pivot_error::storage::StorageError;
use quant_pivot_models::{
    domain::{NewModelSpec, NewModelVersion, NewRuntimeConfigVersion, NewTrainingDataset},
    enums::{
        model::ModelFamily,
        quant::{PublicationStatus, TrainingDatasetStatus},
        runtime_config::RuntimeConfigVersionSource,
    },
    types::{
        ContentHash, ModelSpecId, ModelVersionId, RuntimeConfigVersionId, SchemaVersion,
        TrainingDatasetId,
    },
};
use quant_pivot_repository::{
    postgres::{
        PgModelRegistryRepository, PgRuntimeConfigVersionRepository, PgTrainingDatasetRepository,
    },
    traits::{ModelRegistryRepository, RuntimeConfigVersionRepository, TrainingDatasetRepository},
};
use quant_pivot_test_support::pg::setup_pg;

fn content_hash(seed: char) -> ContentHash {
    ContentHash::parse(format!("blake3:{}", seed.to_string().repeat(64))).expect("hash")
}

async fn seed_runtime_config(db: &sea_orm::DatabaseConnection) -> RuntimeConfigVersionId {
    let id = RuntimeConfigVersionId::from_v7();
    PgRuntimeConfigVersionRepository::new(db.clone())
        .create_version(NewRuntimeConfigVersion {
            runtime_config_version_id: id.clone(),
            config_hash: content_hash('c'),
            schema_version: SchemaVersion::FIRST,
            config_json: serde_json::json!({}),
            source: RuntimeConfigVersionSource::Bootstrap,
            created_by: "pg-training-dataset-it".to_owned(),
            reason: "integration test".to_owned(),
        })
        .await
        .expect("runtime config");
    id
}

async fn seed_model_spec(db: &sea_orm::DatabaseConnection) -> ModelSpecId {
    let model_spec_id = ModelSpecId::from_v7();
    PgModelRegistryRepository::new(db.clone())
        .create_model_spec(NewModelSpec {
            model_spec_id: model_spec_id.clone(),
            name: "pg-dataset-it".to_owned(),
            model_family: ModelFamily::WeightedFactor,
            prediction_horizon_secs: 86_400,
            feature_schema_version: SchemaVersion::FIRST,
            label_schema_version: SchemaVersion::FIRST,
            spec_json: serde_json::json!({}),
            status: PublicationStatus::Published,
        })
        .await
        .expect("model spec");
    model_spec_id
}

fn dataset_hash(dataset_id: &TrainingDatasetId) -> ContentHash {
    let mut hex = dataset_id.as_uuid().simple().to_string();
    hex.truncate(64);
    hex.push_str(&"0".repeat(64usize.saturating_sub(hex.len())));
    ContentHash::parse(format!("blake3:{hex}")).expect("hash")
}

fn new_dataset(
    dataset_id: TrainingDatasetId,
    model_spec_id: ModelSpecId,
    runtime_config_version_id: RuntimeConfigVersionId,
    status: TrainingDatasetStatus,
) -> NewTrainingDataset {
    let hash = dataset_hash(&dataset_id);
    let window_start = Utc::now() - ChronoDuration::hours(2);
    NewTrainingDataset {
        training_dataset_id: dataset_id,
        model_spec_id,
        window_start,
        window_end: window_start + ChronoDuration::hours(1),
        status,
        feature_schema_hash: hash.clone(),
        factor_schema_hash: hash.clone(),
        label_schema_hash: hash.clone(),
        dataset_hash: hash,
        parquet_uri: quant_pivot_models::types::ArtifactUri::parse(
            "file:///tmp/pg-training-dataset.parquet",
        )
        .expect("uri"),
        sample_count: 42,
        source_delay_secs: 10,
        sample_interval_secs: 3600,
        horizons_secs: serde_json::json!([3600]),
        coverage_json: serde_json::json!({ "planned_samples": 42 }),
        runtime_config_version_id,
    }
}

#[tokio::test]
#[ignore = "requires Docker"]
async fn quant_training_dataset_migration_and_crud() {
    let (pool, _container) = setup_pg().await;
    let db = pool.connection().clone();
    let rc_id = seed_runtime_config(&db).await;
    let model_spec_id = seed_model_spec(&db).await;
    let dataset_id = TrainingDatasetId::from_v7();
    let repo = PgTrainingDatasetRepository::new(db.clone());

    let created = repo
        .create(new_dataset(
            dataset_id.clone(),
            model_spec_id,
            rc_id,
            TrainingDatasetStatus::Built,
        ))
        .await
        .expect("create");
    assert_eq!(created.training_dataset_id, dataset_id);
    assert_eq!(created.status, TrainingDatasetStatus::Built);
    assert_eq!(created.sample_count, 42);

    let found = repo
        .find_by_id(&dataset_id)
        .await
        .expect("find")
        .expect("row");
    assert_eq!(found.dataset_hash, created.dataset_hash);
}

#[tokio::test]
#[ignore = "requires Docker"]
async fn training_dataset_status_transitions_enforce_state_machine() {
    let (pool, _container) = setup_pg().await;
    let db = pool.connection().clone();
    let rc_id = seed_runtime_config(&db).await;
    let model_spec_id = seed_model_spec(&db).await;
    let dataset_id = TrainingDatasetId::from_v7();
    let repo = PgTrainingDatasetRepository::new(db.clone());

    repo.create(new_dataset(
        dataset_id.clone(),
        model_spec_id.clone(),
        rc_id.clone(),
        TrainingDatasetStatus::Planned,
    ))
    .await
    .expect("create planned");

    repo.mark_status(&dataset_id, TrainingDatasetStatus::Building)
        .await
        .expect("planned -> building");
    repo.mark_status(&dataset_id, TrainingDatasetStatus::Built)
        .await
        .expect("building -> built");
    repo.mark_status(&dataset_id, TrainingDatasetStatus::Ready)
        .await
        .expect("built -> ready");

    let insufficient_id = TrainingDatasetId::from_v7();
    repo.create(new_dataset(
        insufficient_id.clone(),
        model_spec_id.clone(),
        rc_id.clone(),
        TrainingDatasetStatus::InsufficientLabels,
    ))
    .await
    .expect("create insufficient");

    let err = repo
        .mark_status(&insufficient_id, TrainingDatasetStatus::Ready)
        .await
        .expect_err("insufficient -> ready must conflict");
    assert!(
        matches!(err, StorageError::Conflict(_)),
        "expected conflict, got {err:?}"
    );
}

#[tokio::test]
#[ignore = "requires Docker"]
async fn model_version_training_dataset_foreign_key() {
    let (pool, _container) = setup_pg().await;
    let db = pool.connection().clone();
    let rc_id = seed_runtime_config(&db).await;
    let model_spec_id = seed_model_spec(&db).await;
    let dataset_id = TrainingDatasetId::from_v7();
    let repo = PgTrainingDatasetRepository::new(db.clone());
    let registry = PgModelRegistryRepository::new(db.clone());

    repo.create(new_dataset(
        dataset_id.clone(),
        model_spec_id.clone(),
        rc_id,
        TrainingDatasetStatus::Built,
    ))
    .await
    .expect("create dataset");

    let hash = content_hash('a');
    registry
        .create_model_version(NewModelVersion {
            model_version_id: ModelVersionId::from_v7(),
            model_spec_id: model_spec_id.clone(),
            version: 1,
            artifact_hash: hash.clone(),
            training_dataset_id: Some(dataset_id.clone()),
            metrics_json: serde_json::json!({}),
            quality_gate_report: serde_json::json!({}),
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
            training_dataset_id: Some(missing_dataset),
            metrics_json: serde_json::json!({}),
            quality_gate_report: serde_json::json!({}),
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
