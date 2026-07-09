//! Model registry repository integration tests (Postgres + testcontainers).

use quant_pivot_error::storage::{StorageError, entity};
use quant_pivot_models::{
    domain::{NewModelSpec, NewModelVersion},
    enums::{model::ModelFamily, quant::PublicationStatus},
    types::{ContentHash, ModelSpecId, ModelVersionId, SchemaVersion},
};
use quant_pivot_repository::{
    postgres::PgModelRegistryRepository, traits::ModelRegistryRepository,
};
use quant_pivot_test_support::pg::setup_pg;

fn content_hash(seed: char) -> ContentHash {
    let pair = format!("{:02x}", seed as u32);
    let hex: String = pair.chars().cycle().take(64).collect();
    ContentHash::parse(format!("blake3:{hex}")).expect("hash")
}

fn new_spec(name: &str) -> NewModelSpec {
    NewModelSpec {
        model_spec_id: ModelSpecId::from_v7(),
        name: name.to_owned(),
        model_family: ModelFamily::WeightedFactor,
        prediction_horizon_secs: 86_400,
        feature_schema_version: SchemaVersion::FIRST,
        label_schema_version: SchemaVersion::FIRST,
        spec_json: serde_json::json!({}),
        feature_requirements: serde_json::json!({}),
        status: PublicationStatus::Draft,
    }
}

#[tokio::test]
#[ignore = "requires Docker"]
async fn create_model_spec_duplicate_name_maps_to_storage_duplicate() {
    let (pool, _container) = setup_pg().await;
    let repo = PgModelRegistryRepository::new(pool.connection().clone());

    repo.create_model_spec(new_spec("dup-spec-name"))
        .await
        .expect("first insert");

    let dup = repo.create_model_spec(new_spec("dup-spec-name")).await;
    assert!(matches!(
        dup,
        Err(StorageError::Duplicate {
            entity: entity::QUANT_MODEL_SPEC,
            key,
        }) if key == "dup-spec-name"
    ));
}

#[tokio::test]
#[ignore = "requires Docker"]
async fn create_model_version_duplicate_spec_version_maps_to_storage_duplicate() {
    let (pool, _container) = setup_pg().await;
    let repo = PgModelRegistryRepository::new(pool.connection().clone());
    let model_spec_id = ModelSpecId::from_v7();
    repo.create_model_spec(NewModelSpec {
        model_spec_id: model_spec_id.clone(),
        name: "version-dup-spec".to_owned(),
        model_family: ModelFamily::WeightedFactor,
        prediction_horizon_secs: 86_400,
        feature_schema_version: SchemaVersion::FIRST,
        label_schema_version: SchemaVersion::FIRST,
        spec_json: serde_json::json!({}),
        feature_requirements: serde_json::json!({}),
        status: PublicationStatus::Draft,
    })
    .await
    .expect("model spec");

    let version_row = NewModelVersion {
        model_version_id: ModelVersionId::from_v7(),
        model_spec_id: model_spec_id.clone(),
        version: 1,
        artifact_hash: content_hash('a'),
        training_dataset_id: None,
        metrics_json: serde_json::json!({}),
        training_objective_json: serde_json::json!({"kind": "not_trained"}),
        quality_gate_report: serde_json::json!({}),
        publication_status: PublicationStatus::Candidate,
        published_at: None,
        retired_at: None,
    };
    repo.create_model_version(version_row.clone())
        .await
        .expect("first version");

    let dup = repo
        .create_model_version(NewModelVersion {
            model_version_id: ModelVersionId::from_v7(),
            ..version_row
        })
        .await;
    assert!(matches!(
        dup,
        Err(StorageError::Duplicate {
            entity: entity::QUANT_MODEL_VERSION,
            key,
        }) if key == format!("{model_spec_id}:v1")
    ));
}
