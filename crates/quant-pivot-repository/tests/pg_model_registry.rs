//! Model registry repository integration tests (Postgres + testcontainers).

use quant_pivot_error::storage::{StorageError, entity};
use quant_pivot_models::{
    domain::{ModelVersionListQuery, NewModelSpec, NewModelVersion, PageRequest},
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

fn new_spec(name: &str, family: ModelFamily) -> NewModelSpec {
    NewModelSpec {
        model_spec_id: ModelSpecId::from_v7(),
        name: name.to_owned(),
        model_family: family,
        prediction_horizon_secs: 86_400,
        feature_schema_version: SchemaVersion::FIRST,
        label_schema_version: SchemaVersion::FIRST,
        spec_json: serde_json::json!({}),
        feature_requirements: serde_json::json!({}),
        status: PublicationStatus::Draft,
    }
}

fn new_version(model_spec_id: ModelSpecId, seed: char) -> NewModelVersion {
    NewModelVersion {
        model_version_id: ModelVersionId::from_v7(),
        model_spec_id,
        // Preview only — `create_model_version` re-allocates under lock.
        version: 0,
        artifact_hash: content_hash(seed),
        training_dataset_id: None,
        publish_path_set_id: None,
        metrics_json: serde_json::json!({}),
        training_objective_json: serde_json::json!({"kind": "not_trained"}),
        quality_gate_report: serde_json::json!({}),
        publication_status: PublicationStatus::Candidate,
        published_at: None,
        retired_at: None,
    }
}

#[tokio::test]
#[ignore = "requires Docker"]
async fn create_model_spec_duplicate_name_maps_to_storage_duplicate() {
    let (pool, _container) = setup_pg().await;
    let repo = PgModelRegistryRepository::new(pool.connection().clone());

    repo.create_model_spec(new_spec("dup-spec-name", ModelFamily::WeightedFactor))
        .await
        .expect("first insert");

    let dup = repo
        .create_model_spec(new_spec("dup-spec-name", ModelFamily::WeightedFactor))
        .await;
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
async fn create_model_version_allocates_monotonic_versions_under_lock() {
    let (pool, _container) = setup_pg().await;
    let repo = PgModelRegistryRepository::new(pool.connection().clone());
    let model_spec_id = ModelSpecId::from_v7();
    repo.create_model_spec(NewModelSpec {
        model_spec_id: model_spec_id.clone(),
        name: "version-alloc-spec".to_owned(),
        model_family: ModelFamily::HoldVsExitWeighted,
        prediction_horizon_secs: 86_400,
        feature_schema_version: SchemaVersion::FIRST,
        label_schema_version: SchemaVersion::FIRST,
        spec_json: serde_json::json!({}),
        feature_requirements: serde_json::json!({}),
        status: PublicationStatus::Draft,
    })
    .await
    .expect("model spec");

    let first = repo
        .create_model_version(new_version(model_spec_id.clone(), 'a'))
        .await
        .expect("first version");
    let second = repo
        .create_model_version(new_version(model_spec_id.clone(), 'b'))
        .await
        .expect("second version");

    assert_eq!(first.version, 1);
    assert_eq!(second.version, 2);
    assert_eq!(first.model_family, ModelFamily::HoldVsExitWeighted);
    assert_eq!(second.model_family, ModelFamily::HoldVsExitWeighted);
}

#[tokio::test]
#[ignore = "requires Docker"]
async fn find_and_page_versions_join_model_family_from_spec() {
    let (pool, _container) = setup_pg().await;
    let repo = PgModelRegistryRepository::new(pool.connection().clone());

    let buy_spec = repo
        .create_model_spec(new_spec("buy-join-spec", ModelFamily::WeightedFactor))
        .await
        .expect("buy spec");
    let sell_spec = repo
        .create_model_spec(new_spec("sell-join-spec", ModelFamily::HoldVsExitWeighted))
        .await
        .expect("sell spec");

    let buy = repo
        .create_model_version(new_version(buy_spec.model_spec_id.clone(), 'c'))
        .await
        .expect("buy version");
    let sell = repo
        .create_model_version(new_version(sell_spec.model_spec_id.clone(), 'd'))
        .await
        .expect("sell version");

    let found = repo
        .find_model_version_by_id(&sell.model_version_id)
        .await
        .expect("find")
        .expect("present");
    assert_eq!(found.model_family, ModelFamily::HoldVsExitWeighted);

    let page = repo
        .page_versions(ModelVersionListQuery {
            model_spec_id: None,
            publication_status: None,
            from: None,
            to: None,
            page: PageRequest { page: 1, size: 50 },
        })
        .await
        .expect("page");
    let families: Vec<_> = page
        .items
        .iter()
        .filter(|row| {
            row.model_version_id == buy.model_version_id
                || row.model_version_id == sell.model_version_id
        })
        .map(|row| row.model_family)
        .collect();
    assert!(families.contains(&ModelFamily::WeightedFactor));
    assert!(families.contains(&ModelFamily::HoldVsExitWeighted));
}
