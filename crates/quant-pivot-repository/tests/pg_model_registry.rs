//! Model registry repository integration tests (Postgres + testcontainers).

use quant_pivot_error::storage::{StorageError, entity};
use quant_pivot_models::{
    domain::{ModelPickerSide, ModelVersionListQuery, NewModelSpec, NewModelVersion, PageRequest},
    enums::{common::MarketCategory, model::ModelFamily, quant::PublicationStatus},
    types::{
        ContentHash, ModelInputContract, ModelSpecId, ModelTrainingContract, ModelVersionId,
        SchemaVersion,
    },
};
use quant_pivot_repository::{
    postgres::PgModelRegistryRepository, traits::ModelRegistryRepository,
};
use quant_pivot_test_support::{execution_pg_seed::fixture_profile_ref, pg::setup_pg};

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
        input_contract: ModelInputContract::single_required("book.mid"),
        training_contract: ModelTrainingContract::settlement_default(),
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
        category_scope: None,
        profile_ref: fixture_profile_ref(),
        training_dataset_id: None,
        trade_policy_artifact_id: None,
        trade_policy_hash: None,
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
        input_contract: ModelInputContract::single_required("book.mid"),
        training_contract: ModelTrainingContract::settlement_default(),
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

#[tokio::test]
#[ignore = "requires Docker"]
async fn published_artifacts_coexist_until_model_routing_moves_and_retirement_is_explicit() {
    let (pool, _container) = setup_pg().await;
    let repo = PgModelRegistryRepository::new(pool.connection().clone());
    let spec = repo
        .create_model_spec(new_spec(
            "multiple-published-artifacts-spec",
            ModelFamily::WeightedFactor,
        ))
        .await
        .expect("model spec");

    let mut first = new_version(spec.model_spec_id.clone(), 'e');
    first.publication_status = PublicationStatus::Published;
    repo.create_model_version(first)
        .await
        .expect("first published artifact");
    let mut second = new_version(spec.model_spec_id.clone(), 'f');
    second.publication_status = PublicationStatus::Published;
    repo.create_model_version(second)
        .await
        .expect("second published artifact");

    assert_eq!(
        repo.list_published_for_spec(&spec.model_spec_id)
            .await
            .expect("published artifacts")
            .len(),
        2
    );
}

#[tokio::test]
#[ignore = "requires Docker"]
async fn published_picker_catalog_is_one_typed_join_with_side_and_scope_filters() {
    let (pool, _container) = setup_pg().await;
    let repo = PgModelRegistryRepository::new(pool.connection().clone());
    let buy_spec = repo
        .create_model_spec(new_spec("picker-buy", ModelFamily::WeightedFactor))
        .await
        .expect("buy spec");
    let sell_spec = repo
        .create_model_spec(new_spec("picker-sell", ModelFamily::HoldVsExitWeighted))
        .await
        .expect("sell spec");

    let mut generic_buy = new_version(buy_spec.model_spec_id.clone(), 'g');
    generic_buy.publication_status = PublicationStatus::Published;
    let generic_buy = repo
        .create_model_version(generic_buy)
        .await
        .expect("generic buy");

    let mut crypto_buy = new_version(buy_spec.model_spec_id.clone(), 'h');
    crypto_buy.category_scope = Some(MarketCategory::Crypto);
    crypto_buy.publication_status = PublicationStatus::Published;
    let crypto_buy = repo
        .create_model_version(crypto_buy)
        .await
        .expect("crypto buy");

    let mut weather_buy = new_version(buy_spec.model_spec_id.clone(), 'i');
    weather_buy.category_scope = Some(MarketCategory::Weather);
    weather_buy.publication_status = PublicationStatus::Published;
    repo.create_model_version(weather_buy)
        .await
        .expect("weather buy");

    let mut sell = new_version(sell_spec.model_spec_id.clone(), 'j');
    sell.publication_status = PublicationStatus::Published;
    let sell = repo.create_model_version(sell).await.expect("sell");

    let crypto_catalog = repo
        .list_published_catalog(ModelPickerSide::Buy, Some(MarketCategory::Crypto))
        .await
        .expect("crypto catalog");
    assert_eq!(crypto_catalog.len(), 2);
    assert!(crypto_catalog.iter().any(|row| {
        row.model_version_id == generic_buy.model_version_id
            && row.spec_name == "picker-buy"
            && row.category_scope.is_none()
    }));
    assert!(crypto_catalog.iter().any(|row| {
        row.model_version_id == crypto_buy.model_version_id
            && row.category_scope == Some(MarketCategory::Crypto)
            && row.artifact_hash == crypto_buy.artifact_hash
    }));

    let sell_catalog = repo
        .list_published_catalog(ModelPickerSide::Sell, None)
        .await
        .expect("sell catalog");
    assert_eq!(sell_catalog.len(), 1);
    assert_eq!(sell_catalog[0].model_version_id, sell.model_version_id);
    assert_eq!(
        sell_catalog[0].model_family,
        ModelFamily::HoldVsExitWeighted
    );
}
