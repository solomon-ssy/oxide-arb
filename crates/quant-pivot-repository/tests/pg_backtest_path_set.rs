//! CPCV path-set ledger integration tests (Postgres + testcontainers).

use chrono::{Duration as ChronoDuration, Utc};
use quant_pivot_models::{
    domain::{
        CompleteTrainingDatasetBuild, NewBacktestPathSet, NewModelRun, NewModelSpec,
        NewModelVersion, NewTrainingDatasetPlan,
    },
    enums::{
        model::ModelFamily,
        quant::{
            DatasetPurpose, ModelRunKind, ModelRunStatus, PublicationStatus, TrainingDatasetStatus,
        },
    },
    types::{
        ArtifactUri, BacktestPathSetId, ContentHash, DATASET_ARTIFACT_FORMAT_VERSION,
        DatasetCoverage, DatasetManifest, DecisionPolicySnapshotId, ModelInputContract, ModelRunId,
        ModelSpecId, ModelTrainingContract, ModelVersionId, SchemaVersion, TrainingDatasetId,
        TrainingHorizonsSecs, TrainingSampleSources, default_sample_sources,
    },
};
use quant_pivot_repository::{
    postgres::{
        PgBacktestPathSetRepository, PgModelRegistryRepository, PgModelRunRepository,
        PgTrainingDatasetRepository,
    },
    traits::{
        BacktestPathSetRepository, ModelRegistryRepository, ModelRunRepository,
        TrainingDatasetRepository,
    },
};
use quant_pivot_test_support::{pg::setup_pg, policy_fixtures::bootstrap_default_policy_bundle};
use rust_decimal_macros::dec;

fn content_hash(seed: char) -> ContentHash {
    ContentHash::parse(format!("blake3:{}", seed.to_string().repeat(64))).expect("hash")
}

async fn seed_runtime_config(db: &sea_orm::DatabaseConnection) -> DecisionPolicySnapshotId {
    bootstrap_default_policy_bundle(db, "pg-path-set-it", "integration test").await
}

async fn seed_model_and_dataset(
    db: &sea_orm::DatabaseConnection,
    rc_id: &DecisionPolicySnapshotId,
) -> (ModelVersionId, ModelRunId, TrainingDatasetId) {
    let model_spec_id = seed_model_spec(db).await;
    let window_start = Utc::now() - ChronoDuration::hours(2);
    let training_dataset_id = seed_training_dataset(db, rc_id, &model_spec_id, window_start).await;
    let (model_version_id, model_run_id) =
        seed_model_version_and_run(db, rc_id, model_spec_id, &training_dataset_id, window_start)
            .await;

    (model_version_id, model_run_id, training_dataset_id)
}

async fn seed_model_spec(db: &sea_orm::DatabaseConnection) -> ModelSpecId {
    let registry = PgModelRegistryRepository::new(db.clone());
    let model_spec_id = ModelSpecId::from_v7();
    registry
        .create_model_spec(NewModelSpec {
            model_spec_id: model_spec_id.clone(),
            name: "pg-path-set-it".to_owned(),
            model_family: ModelFamily::WeightedFactor,
            prediction_horizon_secs: 86_400,
            feature_schema_version: SchemaVersion::FIRST,
            label_schema_version: SchemaVersion::FIRST,
            spec_json: serde_json::json!({}),
            input_contract: ModelInputContract::single_required("book.mid"),
            training_contract: ModelTrainingContract::settlement_default(),
            status: PublicationStatus::Published,
        })
        .await
        .expect("model spec");
    model_spec_id
}

async fn seed_training_dataset(
    db: &sea_orm::DatabaseConnection,
    rc_id: &DecisionPolicySnapshotId,
    model_spec_id: &ModelSpecId,
    window_start: chrono::DateTime<Utc>,
) -> TrainingDatasetId {
    let training_dataset_id = TrainingDatasetId::from_v7();
    let window_end = window_start + ChronoDuration::hours(1);
    let manifest = DatasetManifest {
        format_version: DATASET_ARTIFACT_FORMAT_VERSION,
        training_dataset_id: training_dataset_id.clone(),
        profile_ref: quant_pivot_test_support::execution_pg_seed::fixture_profile_ref(),
        research_program_hash: content_hash('4'),
        source_slice: quant_pivot_test_support::execution_pg_seed::source_slice_ref('5'),
        model_spec_id: model_spec_id.clone(),
        trade_policy_artifact_id: None,
        trade_policy_hash: None,
        decision_policy_snapshot_id: rc_id.clone(),
        window_start,
        window_end,
        purpose: DatasetPurpose::Training,
        knowledge_lag_secs: 10,
        sample_interval_secs: 3_600,
        horizons_secs: vec![0],
        feature_schema_hash: content_hash('1'),
        factor_schema_hash: content_hash('2'),
        label_schema_hash: content_hash('3'),
        semantic_dataset_hash: content_hash('4'),
        source_fingerprint: content_hash('7'),
        sample_count: 10,
    };
    let dataset_repo = PgTrainingDatasetRepository::new(db.clone());
    dataset_repo
        .create_plan(NewTrainingDatasetPlan {
            training_dataset_id: training_dataset_id.clone(),
            model_spec_id: model_spec_id.clone(),
            window_start,
            window_end,
            purpose: DatasetPurpose::Training,
            knowledge_lag_secs: 10,
            sample_interval_secs: 3_600,
            horizons_secs: TrainingHorizonsSecs(vec![0]),
            feature_schema_version: Some(SchemaVersion::FIRST),
            sample_sources: Some(TrainingSampleSources(default_sample_sources())),
            decision_policy_snapshot_id: rc_id.clone(),
        })
        .await
        .expect("dataset plan");
    dataset_repo
        .start_build(&training_dataset_id)
        .await
        .expect("start dataset");
    dataset_repo
        .complete_build(
            &training_dataset_id,
            CompleteTrainingDatasetBuild {
                status: TrainingDatasetStatus::Ready,
                feature_schema_hash: content_hash('1'),
                factor_schema_hash: content_hash('2'),
                label_schema_hash: content_hash('3'),
                dataset_hash: content_hash('4'),
                manifest_hash: content_hash('5'),
                manifest_json: manifest,
                artifact_bytes_hash: content_hash('6'),
                parquet_uri: ArtifactUri::parse("file:///tmp/pg-path-set-it.parquet").expect("uri"),
                sample_count: 10,
                coverage_json: DatasetCoverage::default(),
                failure_detail: None,
            },
        )
        .await
        .expect("dataset");
    training_dataset_id
}

async fn seed_model_version_and_run(
    db: &sea_orm::DatabaseConnection,
    rc_id: &DecisionPolicySnapshotId,
    model_spec_id: ModelSpecId,
    training_dataset_id: &TrainingDatasetId,
    window_start: chrono::DateTime<Utc>,
) -> (ModelVersionId, ModelRunId) {
    let registry = PgModelRegistryRepository::new(db.clone());
    let model_version_id = ModelVersionId::from_v7();
    registry
        .create_model_version(NewModelVersion {
            model_version_id: model_version_id.clone(),
            model_spec_id,
            version: 1,
            artifact_hash: content_hash('a'),
            category_scope: None,
            profile_ref: quant_pivot_test_support::execution_pg_seed::fixture_profile_ref(),
            training_dataset_id: Some(training_dataset_id.clone()),
            trade_policy_artifact_id: None,
            trade_policy_hash: None,
            publish_path_set_id: None,
            metrics_json: serde_json::json!({}),
            training_objective_json: serde_json::json!({"kind": "not_trained"}),
            quality_gate_report: serde_json::json!({}),
            publication_status: PublicationStatus::Candidate,
            published_at: None,
            retired_at: None,
        })
        .await
        .expect("model version");

    let model_run_id = ModelRunId::from_v7();
    PgModelRunRepository::new(db.clone())
        .create(NewModelRun {
            model_run_id: model_run_id.clone(),
            run_kind: ModelRunKind::Backtest,
            model_version_id: Some(model_version_id.clone()),
            decision_policy_snapshot_id: rc_id.clone(),
            market_selection_id: None,
            window_start,
            window_end: window_start + ChronoDuration::hours(1),
            status: ModelRunStatus::Succeeded,
            input_hash: content_hash('b'),
            output_hash: Some(content_hash('d')),
            metrics_json: serde_json::json!({}),
            error_code: None,
            error_message: None,
            started_at: Utc::now(),
            finished_at: Some(Utc::now()),
        })
        .await
        .expect("model run");

    (model_version_id, model_run_id)
}

#[tokio::test]
#[ignore = "requires Docker"]
async fn quant_backtest_path_set_migration_and_crud() {
    let (pool, _container) = setup_pg().await;
    let db = pool.connection().clone();
    let rc_id = seed_runtime_config(&db).await;
    let (model_version_id, model_run_id, training_dataset_id) =
        seed_model_and_dataset(&db, &rc_id).await;
    let repo = PgBacktestPathSetRepository::new(db.clone());
    let path_set_id = BacktestPathSetId::from_v7();
    let window_start = Utc::now() - ChronoDuration::hours(2);

    let created = repo
        .create(NewBacktestPathSet {
            path_set_id: path_set_id.clone(),
            model_version_id: model_version_id.clone(),
            model_run_id,
            training_dataset_id,
            decision_policy_snapshot_id: rc_id,
            window_start,
            window_end: window_start + ChronoDuration::hours(1),
            path_count: 7,
            combination_count: 28,
            median_rank_ic: dec!(0.12),
            sharpe_distribution: serde_json::json!({
                "min": "0.1",
                "p25": "0.4",
                "median": "0.8",
                "p75": "1.1",
                "max": "1.5"
            }),
            paths: serde_json::json!([]),
            deflated_sharpe: dec!(0.96),
            dsr_benchmark_sharpe: dec!(0.4),
            pbo: dec!(0.25),
            min_track_record_length_secs: Some(86_400),
            trial_count: 12,
            trial_grid_count: 12,
            coord_search_effective_n: 2,
            path_set_hash: ContentHash::parse(
                "blake3:0000000000000000000000000000000000000000000000000000000000000001",
            )
            .expect("hash"),
        })
        .await
        .expect("create");
    assert_eq!(created.path_set_id, path_set_id);
    assert_eq!(created.trial_count, 12);
    assert_eq!(created.trial_grid_count, 12);
    assert_eq!(created.coord_search_effective_n, 2);
    assert_eq!(created.median_rank_ic, dec!(0.12));

    let found = repo
        .find_by_id(&path_set_id)
        .await
        .expect("find")
        .expect("row");
    assert_eq!(found.deflated_sharpe, dec!(0.96));
    assert_eq!(found.pbo, dec!(0.25));
    assert_eq!(found.path_count, 7);
    assert_eq!(found.combination_count, 28);

    let listed = repo
        .list_by_model_version(&model_version_id)
        .await
        .expect("list");
    assert_eq!(listed.len(), 1);
    assert_eq!(listed[0].path_set_id, path_set_id);
}
