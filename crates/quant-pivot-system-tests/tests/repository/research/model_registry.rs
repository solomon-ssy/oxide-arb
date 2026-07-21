//! Model registry persistence system contracts.

use chrono::Utc;
use quant_pivot_error::storage::{StorageError, entity};
use quant_pivot_models::{
    domain::{
        api::{ModelPickerSide, ModelVersionListQuery},
        pagination::PageRequest,
        quant::{NewModelSpec, NewModelVersion},
    },
    entities::{quant_model_spec, quant_model_spec::Entity},
    enums::{common::MarketCategory, model::ModelFamily, quant::PublicationStatus},
    types::{
        ContentHash, ModelInputContract, ModelSpecId, ModelTrainingContract, ModelVersionId,
        model_metrics::ModelVersionMetrics,
        model_quality::{
            GateIntent, GateSubject, QUALITY_GATE_REPORT_FORMAT_VERSION, QualityGateReport,
        },
        model_training::ModelTrainingObjective,
    },
};
use quant_pivot_repository::{
    postgres::PgModelRegistryRepository, traits::ModelRegistryRepository,
};
use quant_pivot_system_tests::{
    postgres::setup_pg,
    support::{execution_pg_seed::fixture_profile_ref, model_spec_fixtures},
};
use sea_orm::{
    ActiveModelTrait, ActiveValue, ConnectionTrait, DatabaseBackend, EntityTrait, IntoActiveModel,
    Statement,
};

fn content_hash(seed: char) -> ContentHash {
    let pair = format!("{:02x}", seed as u32);
    let hex: String = pair.chars().cycle().take(64).collect();
    ContentHash::parse(format!("blake3:{hex}")).expect("hash")
}

fn new_spec(name: &str, family: ModelFamily) -> NewModelSpec {
    model_spec_fixtures::new_model_spec_fixture(
        ModelSpecId::from_v7(),
        name,
        family,
        86_400,
        ModelInputContract::single_required("book.mid"),
        ModelTrainingContract::settlement_default(),
    )
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
        derivation: NewModelVersion::training_derivation(),
        metrics: ModelVersionMetrics::not_measured("test fixture"),
        training_objective: ModelTrainingObjective::hand_authored("test fixture"),
        quality_gate_report: None,
        publication_status: PublicationStatus::Candidate,
        published_at: None,
        retired_at: None,
    }
}

pub async fn create_model_spec_duplicate_name_maps_to_storage_duplicate() {
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

pub async fn model_spec_rejects_forged_hash_and_is_append_only() {
    let (pool, _container) = setup_pg().await;
    let db = pool.connection().clone();
    let repo = PgModelRegistryRepository::new(db.clone());

    let mut forged = new_spec("forged-spec-hash", ModelFamily::WeightedFactor);
    forged.definition_hash = content_hash('z');
    assert!(matches!(
        repo.create_model_spec(forged).await,
        Err(StorageError::InvariantViolation {
            entity: Some(entity::QUANT_MODEL_SPEC),
            ..
        })
    ));

    let created = repo
        .create_model_spec(new_spec("append-only-spec", ModelFamily::WeightedFactor))
        .await
        .expect("create immutable model spec");
    let row = Entity::find_by_id(created.model_spec_id.clone())
        .one(&db)
        .await
        .expect("load model spec")
        .expect("model spec exists");
    let mut active = row.into_active_model();
    active.name = ActiveValue::Set("mutated-spec".to_owned());
    assert!(
        active.update(&db).await.is_err(),
        "model spec update must be rejected by the database"
    );
    assert!(
        quant_model_spec::Entity::delete_by_id(created.model_spec_id)
            .exec(&db)
            .await
            .is_err(),
        "model spec delete must be rejected by the database"
    );
}

pub async fn create_model_version_allocates_monotonic_versions_under_lock() {
    let (pool, _container) = setup_pg().await;
    let repo = PgModelRegistryRepository::new(pool.connection().clone());
    let model_spec_id = ModelSpecId::from_v7();
    repo.create_model_spec(model_spec_fixtures::new_model_spec_fixture(
        model_spec_id.clone(),
        "version-alloc-spec",
        ModelFamily::HoldVsExitWeighted,
        86_400,
        ModelInputContract::single_required("book.mid"),
        ModelTrainingContract::settlement_default(),
    ))
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

pub async fn find_and_page_versions_join_model_family_from_spec() {
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
    assert_eq!(found.model_spec_name, sell_spec.name);
    assert_eq!(found.model_spec_thesis, sell_spec.thesis);
    assert_eq!(found.model_spec_definition_hash, sell_spec.definition_hash);
    assert_eq!(
        found.training_objective,
        ModelTrainingObjective::hand_authored("test fixture")
    );
    assert_eq!(
        found.metrics,
        ModelVersionMetrics::not_measured("test fixture")
    );

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

pub async fn model_version_typed_documents_fail_closed_at_database_boundary() {
    let (pool, _container) = setup_pg().await;
    let db = pool.connection().clone();
    let repo = PgModelRegistryRepository::new(db.clone());
    let spec = repo
        .create_model_spec(new_spec(
            "typed-version-documents",
            ModelFamily::WeightedFactor,
        ))
        .await
        .expect("model spec");
    let version = repo
        .create_model_version(new_version(spec.model_spec_id, 'k'))
        .await
        .expect("model version");

    // Explicit test-only corruption boundary: an unknown discriminator cannot
    // be represented by the Rust type, so bind malformed JSON as a value while
    // keeping the SQL statement and identifier static.
    let corrupt = Statement::from_sql_and_values(
        DatabaseBackend::Postgres,
        "UPDATE quant_model_version SET training_objective = $1::jsonb WHERE model_version_id = $2",
        [
            r#"{"format_version":1,"definition":{"kind":"future_algorithm"}}"#
                .to_owned()
                .into(),
            version.model_version_id.clone().into(),
        ],
    );
    assert!(
        db.execute_raw(corrupt).await.is_err(),
        "the DB constraint must reject unknown training-objective tags"
    );

    let wrong_shape = Statement::from_sql_and_values(
        DatabaseBackend::Postgres,
        "UPDATE quant_model_version SET training_objective = $1::jsonb WHERE model_version_id = $2",
        [
            "[]".to_owned().into(),
            version.model_version_id.clone().into(),
        ],
    );
    assert!(
        db.execute_raw(wrong_shape).await.is_err(),
        "the DB constraint must reject a non-object training objective"
    );

    let wrong_version = Statement::from_sql_and_values(
        DatabaseBackend::Postgres,
        "UPDATE quant_model_version SET training_objective = $1::jsonb WHERE model_version_id = $2",
        [
            r#"{"format_version":2,"definition":{"kind":"hand_authored","rationale":"wrong version"}}"#
                .to_owned()
                .into(),
            version.model_version_id.clone().into(),
        ],
    );
    assert!(
        db.execute_raw(wrong_version).await.is_err(),
        "the DB constraint must reject an unsupported document version"
    );

    let corrupt_metrics = Statement::from_sql_and_values(
        DatabaseBackend::Postgres,
        "UPDATE quant_model_version SET metrics = $1::jsonb WHERE model_version_id = $2",
        [
            r#"{"format_version":1,"definition":{"kind":"future_metrics"}}"#
                .to_owned()
                .into(),
            version.model_version_id.clone().into(),
        ],
    );
    assert!(
        db.execute_raw(corrupt_metrics).await.is_err(),
        "the DB constraint must reject unknown model-metrics tags"
    );

    let mismatched_known_families = Statement::from_sql_and_values(
        DatabaseBackend::Postgres,
        "UPDATE quant_model_version SET metrics = $1::jsonb WHERE model_version_id = $2",
        [
            r#"{"format_version":1,"definition":{"kind":"learning_to_rank","in_sample":{},"validation":{},"artifact_lineage":{"kind":"factor_native"}}}"#
                .to_owned()
                .into(),
            version.model_version_id.clone().into(),
        ],
    );
    assert!(
        db.execute_raw(mismatched_known_families).await.is_err(),
        "the DB constraint must tie metrics and training-objective families together"
    );

    let wrong_subject = QualityGateReport {
        format_version: QUALITY_GATE_REPORT_FORMAT_VERSION,
        subject: GateSubject::ModelVersion(ModelVersionId::from_v7()),
        intent: GateIntent::Publish,
        evaluated_at: Utc::now(),
        gates: Vec::new(),
        hard_failures: Vec::new(),
        soft_warnings: Vec::new(),
        passed: true,
        report_hash: content_hash('q'),
    };
    assert!(
        repo.set_quality_gate_report(&version.model_version_id, wrong_subject)
            .await
            .is_err(),
        "quality-gate subject id must match the owning model version"
    );

    let unknown_field = Statement::from_sql_and_values(
        DatabaseBackend::Postgres,
        "UPDATE quant_model_version SET training_objective = training_objective || $1::jsonb WHERE model_version_id = $2",
        [
            r#"{"future_field":true}"#.to_owned().into(),
            version.model_version_id.clone().into(),
        ],
    );
    db.execute_raw(unknown_field)
        .await
        .expect("test-only corruption may cross the relational shape constraint");
    assert!(
        repo.find_model_version_by_id(&version.model_version_id)
            .await
            .is_err(),
        "typed repository decode must reject unknown fields without fallback"
    );
}

pub async fn published_artifacts_coexist_until_model_routing_moves_and_retirement_is_explicit() {
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

pub async fn published_picker_catalog_is_one_typed_join_with_side_and_scope_filters() {
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
