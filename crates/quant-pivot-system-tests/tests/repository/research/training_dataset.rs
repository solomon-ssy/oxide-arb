//! Training-dataset ledger persistence system contracts.

use chrono::{Duration as ChronoDuration, Utc};
use quant_pivot_error::storage::{StorageError, entity};
use quant_pivot_models::{
    domain::quant::CompleteTrainingDatasetBuild,
    enums::{
        model::ModelFamily,
        quant::{DatasetPurpose, TrainingDatasetStatus},
    },
    types::{
        ArtifactUri, ContentHash, DecisionPolicySnapshotId, ModelInputContract, ModelSpecId,
        ModelTrainingContract, ModelVersionId, SchemaVersion, TrainingDatasetId,
        TrainingSampleSources, builtin_research_profiles, factor::FactorServingPlane,
    },
};
use quant_pivot_repository::{
    postgres::{PgModelRegistryRepository, PgPolicyRepository, PgTrainingDatasetRepository},
    traits::{ModelRegistryRepository, PolicyRepository, TrainingDatasetRepository},
};
use quant_pivot_research::{
    factors::FactorEngine, features::ExecutableFeatureSchema, hashing::ResearchHasher,
};
use quant_pivot_system_tests::{
    postgres::setup_pg,
    support::{
        model_serving_fixtures::{ModelVersionFixture, ModelVersionFixtureSeed},
        model_spec_fixtures,
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
            model_spec_fixtures::pooled_horizon_secs(),
            ModelInputContract::single_required("book.mid"),
            ModelTrainingContract::outcome_default(),
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
    let profile_ref = model_spec_fixtures::pooled_profile_ref();
    let profile = builtin_research_profiles()
        .expect("valid built-in research profiles")
        .into_iter()
        .find(|profile| profile.profile_ref == profile_ref)
        .expect("pooled research profile");
    let model_spec_definition_hash = model_spec_fixtures::new_model_spec_fixture(
        model_spec_id,
        "pg-dataset-it",
        ModelFamily::WeightedFactor,
        model_spec_fixtures::pooled_horizon_secs(),
        ModelInputContract::single_required("book.mid"),
        ModelTrainingContract::outcome_default(),
    )
    .definition_hash;
    let source_lineage = seed_dataset_source(
        db,
        DatasetSourceSeed {
            scope: format!("pg-dataset-it:{dataset_id}"),
            profile_ref: profile.profile_ref.clone(),
            decision_policy_snapshot_id,
            window_start,
            window_end,
            pit_cutoff: window_end + ChronoDuration::hours(1),
        },
    )
    .await
    .expect("source lineage");
    let policy = PgPolicyRepository::new(db.clone())
        .load_snapshot(&decision_policy_snapshot_id)
        .await
        .expect("load dataset policy")
        .expect("dataset policy");
    let features = &policy.snapshot.profile_artifacts.features.definition;
    let scoring = &policy.snapshot.profile_artifacts.scoring.definition;
    let domain = &policy.snapshot.profile_artifacts.domain.definition;
    let feature_schema = ExecutableFeatureSchema::build(features, profile.spec.feature_contract)
        .expect("executable feature schema");
    let feature_schema_hash =
        ResearchHasher::feature_schema(&feature_schema).expect("feature schema hash");
    let factor_serving_plane = FactorEngine::for_model_scope(
        scoring,
        features,
        domain,
        profile.spec.feature_contract,
        profile.spec.category,
        None,
    )
    .serving_plane()
    .expect("factor serving plane")
    .clone();
    let hash = dataset_hash(&dataset_id);
    DatasetLedgerFixture::try_new(DatasetLedgerSeed {
        training_dataset_id: dataset_id,
        model_spec_id,
        model_family: ModelFamily::WeightedFactor,
        model_spec_definition_hash,
        factor_serving_plane,
        source_lineage,
        cohort_manifest: None,
        window_start,
        window_end,
        purpose: DatasetPurpose::Training,
        knowledge_lag_secs: 10,
        sample_interval_secs: 3600,
        horizons_secs: vec![86_400],
        feature_schema_version: SchemaVersion::FIRST,
        sample_sources: Some(TrainingSampleSources::default()),
        feature_schema_hash,
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
    assert_eq!(
        building.feature_schema_hash,
        fixture.plan.feature_schema_hash
    );
    assert_eq!(building.factor_schema_hash, fixture.plan.factor_schema_hash);
    assert_eq!(
        building.factor_serving_plane,
        fixture.plan.factor_serving_plane
    );

    let mut drifted_manifest = fixture.manifest.clone();
    drifted_manifest.model_family = ModelFamily::ClassicalLogisticRegression;
    drifted_manifest.factor_serving_plane =
        FactorServingPlane::try_empty().expect("classical empty plane");
    let drifted_completion = CompleteTrainingDatasetBuild::try_new(
        TrainingDatasetStatus::Ready,
        drifted_manifest,
        dataset_hash(&dataset_id),
        ArtifactUri::parse("file:///tmp/pg-training-dataset-drift.parquet").expect("uri"),
        fixture.coverage(),
        None,
    )
    .expect("self-consistent drifted completion");
    assert!(matches!(
        repo.complete_build(&dataset_id, drifted_completion).await,
        Err(StorageError::InvariantViolation {
            entity: Some(entity::QUANT_TRAINING_DATASET),
            ..
        })
    ));

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

    let mut factor_drift = new_fixture(&db, TrainingDatasetId::from_v7(), model_spec_id, rc_id)
        .await
        .plan;
    factor_drift.factor_schema_hash = content_hash('e');
    assert!(matches!(
        repo.create_plan(factor_drift).await,
        Err(StorageError::InvariantViolation {
            entity: Some(entity::QUANT_TRAINING_DATASET),
            ..
        })
    ));

    let frozen_id = TrainingDatasetId::from_v7();
    let frozen = new_fixture(&db, frozen_id, model_spec_id, rc_id).await;
    repo.create_plan(frozen.plan.clone())
        .await
        .expect("create frozen plan");
    repo.start_build(&frozen_id)
        .await
        .expect("start frozen build");
    let poison_id = TrainingDatasetId::from_v7();
    let malformed_plane = db
        .execute_raw(Statement::from_sql_and_values(
            DbBackend::Postgres,
            "INSERT INTO quant_training_dataset (
                training_dataset_id, model_spec_id, model_family,
                model_spec_definition_hash, factor_serving_plane,
                research_profile_artifact_id, source_slice_id, pit_cutoff,
                source_lineage, feedback_cohort, cohort_manifest, window_start,
                window_end, status, purpose, feature_schema_hash,
                factor_schema_hash, label_schema_hash, dataset_hash, manifest_hash,
                manifest, artifact_bytes_hash, parquet_uri, sample_count,
                knowledge_lag_secs, sample_interval_secs, horizons_secs,
                feature_schema_version, sample_sources, coverage,
                decision_policy_snapshot_id, failure_detail, completed_at, created_at
             )
             SELECT $1, model_spec_id, model_family, model_spec_definition_hash,
                    jsonb_set(factor_serving_plane, '{definitions}', '[null]'::jsonb, false),
                    research_profile_artifact_id, source_slice_id, pit_cutoff,
                    source_lineage, feedback_cohort, cohort_manifest, window_start,
                    window_end, status, purpose, feature_schema_hash,
                    factor_schema_hash, label_schema_hash, dataset_hash, manifest_hash,
                    manifest, artifact_bytes_hash, parquet_uri, sample_count,
                    knowledge_lag_secs, sample_interval_secs, horizons_secs,
                    feature_schema_version, sample_sources, coverage,
                    decision_policy_snapshot_id, failure_detail, completed_at, created_at
             FROM quant_training_dataset
             WHERE training_dataset_id = $2",
            [poison_id.as_uuid().into(), frozen_id.as_uuid().into()],
        ))
        .await;
    assert!(
        malformed_plane.is_err(),
        "relational contract must reject malformed factor-plane members at INSERT"
    );

    let direct_feature_drift = db
        .execute_raw(Statement::from_sql_and_values(
            DbBackend::Postgres,
            "UPDATE quant_training_dataset SET feature_schema_hash = $1 \
             WHERE training_dataset_id = $2",
            [
                content_hash('e').to_string().into(),
                frozen_id.as_uuid().into(),
            ],
        ))
        .await;
    assert!(
        direct_feature_drift.is_err(),
        "plan-time feature schema hash must remain WORM while building"
    );
    let unchanged = repo
        .find_by_id(&frozen_id)
        .await
        .expect("reload frozen plan")
        .expect("frozen plan row");
    assert_eq!(
        unchanged.feature_schema_hash, frozen.plan.feature_schema_hash,
        "rejected direct SQL drift must preserve the frozen feature contract"
    );
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
    let model_version_id = ModelVersionId::from_v7();
    let mut valid_seed = ModelVersionFixtureSeed::training(
        format!("training-dataset-fk:{model_version_id}"),
        model_version_id,
        model_spec_id,
        hash,
    );
    valid_seed.training_dataset_id = Some(dataset_id);
    let version = ModelVersionFixture::prepare(&db, valid_seed)
        .await
        .expect("prepare dataset-bound model version");
    registry
        .create_model_version(version)
        .await
        .expect("valid training_dataset_id FK");

    let missing_dataset = TrainingDatasetId::from_v7();
    let missing_model_version_id = ModelVersionId::from_v7();
    let mut missing_seed = ModelVersionFixtureSeed::training(
        format!("training-dataset-missing:{missing_model_version_id}"),
        missing_model_version_id,
        model_spec_id,
        hash,
    );
    missing_seed.training_dataset_id = Some(dataset_id);
    let mut missing_version = ModelVersionFixture::prepare(&db, missing_seed)
        .await
        .expect("prepare valid preimage for missing-dataset mutation");
    missing_version.training_dataset_id = Some(missing_dataset);
    let fk_err = registry
        .create_model_version(missing_version)
        .await
        .expect_err("missing training_dataset_id must fail sealed projection");
    assert!(
        matches!(
            fk_err,
            StorageError::InvariantViolation {
                entity: Some(entity::QUANT_MODEL_VERSION),
                ..
            }
        ),
        "expected sealed model-version invariant error, got {fk_err:?}"
    );
}
