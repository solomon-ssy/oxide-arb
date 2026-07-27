//! CPCV path-set ledger persistence system contract.

use chrono::{DateTime, Duration as ChronoDuration, Utc};
use quant_pivot_models::{
    domain::quant::{NewBacktestPathSet, NewBacktestPathSetInput, NewModelRun},
    enums::{model::ModelFamily, quant::ModelRunKind},
    types::{
        BacktestPathSetId, ContentHash, DecisionPolicySnapshotId, ModelInputContract, ModelRunId,
        ModelSpecId, ModelTrainingContract, ModelVersionId, TrainingDatasetId,
        backtest::{
            BacktestPath, CpcvFoldArtifact, CpcvFoldArtifacts, CpcvFoldCalibrationPolicy,
            CpcvFoldRole, CpcvMethodologyBinding, CpcvPathSetSubject, SharpeDistribution,
        },
    },
};
use quant_pivot_repository::{
    postgres::{PgBacktestPathSetRepository, PgModelRegistryRepository, PgModelRunRepository},
    traits::{BacktestPathSetRepository, ModelRegistryRepository, ModelRunRepository},
};
use quant_pivot_system_tests::{
    postgres::setup_pg,
    support::{
        model_serving_fixtures::{ModelVersionFixture, ModelVersionFixtureSeed},
        model_spec_fixtures,
        policy_fixtures::bootstrap_default_policy_bundle,
    },
};
use rust_decimal_macros::dec;
use sea_orm::{ConnectionTrait, DatabaseConnection, DbBackend, Statement};

fn content_hash(seed: char) -> ContentHash {
    ContentHash::parse(&format!("blake3:{}", seed.to_string().repeat(64))).expect("hash")
}

async fn seed_runtime_config(db: &DatabaseConnection) -> DecisionPolicySnapshotId {
    bootstrap_default_policy_bundle(db, "pg-path-set-it", "integration test").await
}

async fn seed_model_and_dataset(
    db: &DatabaseConnection,
    rc_id: &DecisionPolicySnapshotId,
) -> (ModelVersionId, ModelRunId, TrainingDatasetId) {
    let model_spec_id = seed_model_spec(db).await;
    let window_start = Utc::now() - ChronoDuration::hours(2);
    seed_model_version_run(db, rc_id, model_spec_id, window_start).await
}

async fn seed_model_spec(db: &DatabaseConnection) -> ModelSpecId {
    let registry = PgModelRegistryRepository::new(db.clone());
    let model_spec_id = ModelSpecId::from_v7();
    registry
        .create_model_spec(model_spec_fixtures::new_model_spec_fixture(
            model_spec_id,
            "pg-path-set-it",
            ModelFamily::WeightedFactor,
            model_spec_fixtures::pooled_horizon_secs(),
            ModelInputContract::single_required("book.mid"),
            ModelTrainingContract::settlement_default(),
        ))
        .await
        .expect("model spec");
    model_spec_id
}

async fn seed_model_version_run(
    db: &DatabaseConnection,
    rc_id: &DecisionPolicySnapshotId,
    model_spec_id: ModelSpecId,
    window_start: DateTime<Utc>,
) -> (ModelVersionId, ModelRunId, TrainingDatasetId) {
    let registry = PgModelRegistryRepository::new(db.clone());
    let model_version_id = ModelVersionId::from_v7();
    let version = ModelVersionFixture::prepare(
        db,
        ModelVersionFixtureSeed::training(
            format!("backtest-path-set:{model_version_id}"),
            model_version_id,
            model_spec_id,
            content_hash('a'),
        ),
    )
    .await
    .expect("prepare exact model version");
    assert_eq!(
        version
            .serving_contract
            .bindings()
            .policy_snapshot
            .decision_policy_snapshot_id,
        *rc_id
    );
    let training_dataset_id = version
        .training_dataset_id
        .expect("model fixture training dataset");
    registry
        .create_model_version(version)
        .await
        .expect("model version");

    let model_run_id = ModelRunId::from_v7();
    PgModelRunRepository::new(db.clone())
        .create(NewModelRun {
            model_run_id,
            run_kind: ModelRunKind::Cpcv,
            model_version_id: Some(model_version_id),
            decision_policy_snapshot_id: *rc_id,
            market_selection_id: None,
            window_start,
            window_end: window_start + ChronoDuration::hours(1),
            input_hash: content_hash('b'),
        })
        .await
        .expect("model run");

    (model_version_id, model_run_id, training_dataset_id)
}

pub async fn quant_backtest_set_crud() {
    let (pool, _container) = setup_pg().await;
    let db = pool.connection().clone();
    let rc_id = seed_runtime_config(&db).await;
    let (model_version_id, model_run_id, training_dataset_id) =
        seed_model_and_dataset(&db, &rc_id).await;
    let repo = PgBacktestPathSetRepository::new(db.clone());
    let path_set_id = BacktestPathSetId::from_v7();
    let window_start = Utc::now() - ChronoDuration::hours(2);

    let new_path_set = NewBacktestPathSet::try_seal(NewBacktestPathSetInput {
        path_set_id,
        model_version_id,
        model_run_id,
        training_dataset_id,
        decision_policy_snapshot_id: rc_id,
        window_start,
        window_end: window_start + ChronoDuration::hours(1),
        subject: CpcvPathSetSubject::new(
            content_hash('1'),
            content_hash('2'),
            content_hash('3'),
            content_hash('4'),
            content_hash('5'),
            content_hash('6'),
        ),
        methodology: CpcvMethodologyBinding::new(
            content_hash('7'),
            content_hash('8'),
            content_hash('9'),
            CpcvFoldCalibrationPolicy::SubjectHeuristic {
                return_model_hash: content_hash('a'),
            },
        ),
        fold_artifacts: CpcvFoldArtifacts::try_new(vec![
            CpcvFoldArtifact {
                role: CpcvFoldRole::Validation,
                training_groups_hash: content_hash('b'),
                training_group_count: 2,
                model_artifact_hash: content_hash('c'),
                serving_contract_hash: content_hash('d'),
                model_payload_hash: content_hash('e'),
            },
            CpcvFoldArtifact {
                role: CpcvFoldRole::Trial { trial_id: 0 },
                training_groups_hash: content_hash('f'),
                training_group_count: 3,
                model_artifact_hash: content_hash('1'),
                serving_contract_hash: content_hash('2'),
                model_payload_hash: content_hash('3'),
            },
        ])
        .expect("fold artifacts"),
        path_count: 1,
        combination_count: 1,
        median_rank_ic: dec!(0.12),
        sharpe_distribution: SharpeDistribution {
            min: dec!(0.1),
            p25: dec!(0.4),
            median: dec!(0.8),
            p75: dec!(1.1),
            max: dec!(1.5),
            median_max_drawdown: None,
            median_tail_loss: None,
            baseline_uplift: None,
        },
        paths: vec![BacktestPath {
            path_index: 0,
            group_returns: vec![dec!(0.01), dec!(-0.005)],
            sharpe: dec!(0.8),
            rank_ic: dec!(0.12),
            max_drawdown: dec!(0.005),
            tail_loss: dec!(-0.005),
        }]
        .into(),
        deflated_sharpe: dec!(0.96),
        dsr_benchmark_sharpe: dec!(0.4),
        pbo: dec!(0.25),
        min_track_record_length_secs: Some(86_400),
        trial_count: 1,
        trial_grid_count: 1,
        coord_search_effective_n: 2,
    })
    .expect("seal path set");
    let created = repo.create(new_path_set.clone()).await.expect("create");
    assert_eq!(created.path_set_id, path_set_id);
    assert_eq!(created.trial_count, 1);
    assert_eq!(created.trial_grid_count, 1);
    assert_eq!(created.coord_search_effective_n, 2);
    assert_eq!(created.median_rank_ic, dec!(0.12));
    PgModelRunRepository::new(db.clone())
        .succeed(&model_run_id, created.path_set_hash, Some(model_version_id))
        .await
        .expect("bind successful CPCV run to exact path-set hash");

    let found = repo
        .find_by_id(&path_set_id)
        .await
        .expect("find")
        .expect("row");
    assert_eq!(found.deflated_sharpe, dec!(0.96));
    assert_eq!(found.pbo, dec!(0.25));
    assert_eq!(found.path_count, 1);
    assert_eq!(found.combination_count, 1);

    let listed = repo
        .list_by_model_version(&model_version_id)
        .await
        .expect("list");
    assert_eq!(listed.len(), 1);
    assert_eq!(listed[0].path_set_id, path_set_id);

    let mut tampered = serde_json::to_value(new_path_set).expect("serialize sealed path set");
    tampered["median_rank_ic"] = serde_json::json!("0.99");
    let tampered: NewBacktestPathSet =
        serde_json::from_value(tampered).expect("decode structurally valid tamper");
    let error = repo
        .create(tampered)
        .await
        .expect_err("repository must reject a caller-forged path-set hash");
    assert!(error.to_string().contains("hash mismatch"));

    let update = db
        .execute_raw(Statement::from_sql_and_values(
            DbBackend::Postgres,
            "UPDATE quant_backtest_path_set SET pbo = pbo \
             WHERE path_set_id = $1",
            [path_set_id.as_uuid().into()],
        ))
        .await;
    assert!(update.is_err(), "path-set UPDATE must be rejected");
    let delete = db
        .execute_raw(Statement::from_sql_and_values(
            DbBackend::Postgres,
            "DELETE FROM quant_backtest_path_set WHERE path_set_id = $1",
            [path_set_id.as_uuid().into()],
        ))
        .await;
    assert!(delete.is_err(), "path-set DELETE must be rejected");
}
