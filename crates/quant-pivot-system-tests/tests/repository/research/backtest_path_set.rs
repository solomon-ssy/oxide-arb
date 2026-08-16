//! CPCV path-set ledger persistence system contract.

use chrono::{DateTime, Duration as ChronoDuration, Utc};
use quant_pivot_models::{
    domain::quant::{ModelVersionInfo, NewBacktestPathSet, NewBacktestPathSetInput, NewModelRun},
    enums::{
        model::ModelFamily,
        quant::{ModelRunKind, ModelRunStatus},
    },
    types::{
        BacktestPathSetId, ContentHash, DecisionPolicySnapshotId, ModelInputContract, ModelRunId,
        ModelSpecId, ModelTrainingContract, ModelVersionId, TrainingDatasetId,
        backtest::{
            BacktestPath, CpcvEstimatorIdentity, CpcvFoldArtifact, CpcvFoldArtifacts,
            CpcvFoldCalibrationPolicy, CpcvFoldValidationRegime, CpcvMethodologyBinding,
            CpcvPathSetSubject, CpcvTrialPathBinding, SharpeDistribution,
        },
    },
};
use quant_pivot_repository::{
    postgres::{PgBacktestPathSetRepository, PgModelRegistryRepository, PgModelRunRepository},
    traits::{
        BacktestPathSetRepository, CpcvPathSetCommit, ModelRegistryRepository, ModelRunRepository,
    },
};
use quant_pivot_system_tests::{
    postgres::setup_pg,
    support::{
        model_serving_fixtures::{ModelVersionFixture, ModelVersionFixtureSeed},
        model_spec_fixtures,
        policy_fixtures::bootstrap_default_policy_bundle,
        research_fixtures::cscv_selection_fixture,
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
            ModelTrainingContract::outcome_default(),
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
    let runs = PgModelRunRepository::new(db.clone());
    let run = NewModelRun {
        model_run_id,
        run_kind: ModelRunKind::Cpcv,
        model_version_id: Some(model_version_id),
        decision_policy_snapshot_id: *rc_id,
        market_selection_id: None,
        window_start,
        window_end: window_start + ChronoDuration::hours(1),
        input_hash: content_hash('b'),
    };
    let started = runs
        .start_exact(run.clone())
        .await
        .expect("start exact model run");
    let replayed = runs
        .start_exact(run.clone())
        .await
        .expect("replay exact model run");
    assert_eq!(started.model_run_id, replayed.model_run_id);
    assert_eq!(replayed.status, ModelRunStatus::Running);
    let mut drifted = run;
    drifted.input_hash = content_hash('c');
    assert!(
        runs.start_exact(drifted).await.is_err(),
        "same run id with another immutable input must fail closed"
    );

    (model_version_id, model_run_id, training_dataset_id)
}

struct PathSetContract<'a> {
    db: &'a DatabaseConnection,
    repo: &'a PgBacktestPathSetRepository,
    path_set: &'a NewBacktestPathSet,
    path_set_id: BacktestPathSetId,
    model_version_id: ModelVersionId,
    model_run_id: ModelRunId,
}

impl PathSetContract<'_> {
    async fn assert_rollback(&self) {
        let error = self
            .repo
            .commit_cpcv(CpcvPathSetCommit {
                path_set: self.path_set.clone(),
                input_hash: content_hash('c'),
            })
            .await
            .expect_err("wrong run input must roll back the whole CPCV commit");
        assert!(error.to_string().contains("canonical path-set subject"));
        assert!(
            self.repo
                .find_by_id(&self.path_set_id)
                .await
                .expect("find after rollback")
                .is_none(),
            "failed commit must not leave a path-set row"
        );
        let running = PgModelRunRepository::new(self.db.clone())
            .find_by_id(&self.model_run_id)
            .await
            .expect("find run after rollback")
            .expect("run after rollback");
        assert_eq!(running.status, ModelRunStatus::Running);
    }

    async fn assert_exact_commit(&self) {
        let created = self
            .repo
            .commit_cpcv(CpcvPathSetCommit {
                path_set: self.path_set.clone(),
                input_hash: content_hash('b'),
            })
            .await
            .expect("atomic CPCV commit");
        let replayed = self
            .repo
            .commit_cpcv(CpcvPathSetCommit {
                path_set: self.path_set.clone(),
                input_hash: content_hash('b'),
            })
            .await
            .expect("exact CPCV commit replay");
        assert_eq!(replayed.path_set_id, created.path_set_id);
        assert_eq!(replayed.path_set_hash, created.path_set_hash);
        assert_eq!(created.path_set_id, self.path_set_id);
        assert_eq!(created.dsr_conservative_independent_trial_count, 1);
        assert_eq!(created.trial_grid_count, 2);
        assert_eq!(created.coord_search_effective_n, 2);
        assert_eq!(created.median_target_rank_ic, dec!(0.12));
        let terminal = PgModelRunRepository::new(self.db.clone())
            .find_by_id(&self.model_run_id)
            .await
            .expect("find terminal CPCV run")
            .expect("terminal CPCV run");
        assert_eq!(terminal.status, ModelRunStatus::Succeeded);
        assert_eq!(terminal.output_hash, Some(created.path_set_hash));

        let found = self
            .repo
            .find_by_id(&self.path_set_id)
            .await
            .expect("find")
            .expect("row");
        assert_eq!(found.deflated_sharpe, dec!(0.96));
        assert_eq!(found.pbo, dec!(0));
        assert_eq!(found.path_count, 1);
        assert_eq!(found.combination_count, 1);

        let listed = self
            .repo
            .list_by_model_version(&self.model_version_id)
            .await
            .expect("list");
        assert_eq!(listed.len(), 1);
        assert_eq!(listed[0].path_set_id, self.path_set_id);
    }

    async fn assert_worm(&self) {
        let mut tampered = serde_json::to_value(self.path_set).expect("serialize sealed path set");
        tampered["median_target_rank_ic"] = serde_json::json!("0.99");
        let tampered: NewBacktestPathSet =
            serde_json::from_value(tampered).expect("decode structurally valid tamper");
        let error = self
            .repo
            .commit_cpcv(CpcvPathSetCommit {
                path_set: tampered,
                input_hash: content_hash('b'),
            })
            .await
            .expect_err("repository must reject a caller-forged path-set hash");
        assert!(error.to_string().contains("hash mismatch"));

        let update = self
            .db
            .execute_raw(Statement::from_sql_and_values(
                DbBackend::Postgres,
                "UPDATE quant_backtest_path_set SET pbo = pbo \
                 WHERE path_set_id = $1",
                [self.path_set_id.as_uuid().into()],
            ))
            .await;
        assert!(update.is_err(), "path-set UPDATE must be rejected");
        let delete = self
            .db
            .execute_raw(Statement::from_sql_and_values(
                DbBackend::Postgres,
                "DELETE FROM quant_backtest_path_set WHERE path_set_id = $1",
                [self.path_set_id.as_uuid().into()],
            ))
            .await;
        assert!(delete.is_err(), "path-set DELETE must be rejected");
    }
}

struct PathSetFixture<'a> {
    path_set_id: BacktestPathSetId,
    model_version_id: ModelVersionId,
    model_run_id: ModelRunId,
    training_dataset_id: TrainingDatasetId,
    decision_policy_snapshot_id: DecisionPolicySnapshotId,
    window_start: DateTime<Utc>,
    model: &'a ModelVersionInfo,
}

impl PathSetFixture<'_> {
    fn seal(self) -> NewBacktestPathSet {
        let bindings = self
            .model
            .verified_serving_contract()
            .expect("verify CPCV serving contract")
            .bindings();
        let decision_times = [10, 20, 30, 40]
            .into_iter()
            .map(|minutes| self.window_start + ChronoDuration::minutes(minutes))
            .collect::<Vec<_>>();
        let group_returns = vec![dec!(0.01), dec!(-0.005), dec!(0.012), dec!(-0.004)];
        let challenger_returns = group_returns
            .iter()
            .map(|value| *value - dec!(0.001))
            .collect::<Vec<_>>();
        let (trial_grid, cscv_selection_evidence) = cscv_selection_fixture(
            "path-set-repository",
            &decision_times,
            &[group_returns.clone(), challenger_returns],
            4,
        );
        let dsr_conservative_independent_trial_count = i64::from(
            cscv_selection_evidence
                .trial_dependence
                .conservative_independent_trial_count(),
        );
        NewBacktestPathSet::try_seal(NewBacktestPathSetInput {
            path_set_id: self.path_set_id,
            model_version_id: self.model_version_id,
            model_run_id: self.model_run_id,
            training_dataset_id: self.training_dataset_id,
            decision_policy_snapshot_id: self.decision_policy_snapshot_id,
            window_start: self.window_start,
            window_end: self.window_start + ChronoDuration::hours(1),
            subject: CpcvPathSetSubject::new(
                self.model.artifact_hash,
                self.model.serving_contract_hash,
                bindings.transform.training_dataset_hash,
                bindings.dataset.manifest_hash,
                bindings.dataset.artifact_bytes_hash,
                bindings.policy_snapshot.snapshot_hash,
            ),
            methodology: CpcvMethodologyBinding::new(
                content_hash('7'),
                content_hash('8'),
                content_hash('9'),
                CpcvFoldCalibrationPolicy::SubjectHeuristic {
                    return_model_hash: content_hash('a'),
                },
                CpcvTrialPathBinding::try_new(0, vec![0]).expect("trial path"),
                trial_grid,
            ),
            fold_artifacts: fold_artifacts_fixture(),
            path_count: 1,
            combination_count: 1,
            median_target_rank_ic: dec!(0.12),
            sharpe_distribution: SharpeDistribution {
                min: dec!(0.1),
                p25: dec!(0.4),
                median: dec!(0.8),
                p75: dec!(1.1),
                max: dec!(1.5),
                median_max_drawdown: None,
                median_tail_loss: None,
                median_turnover: None,
                baseline_uplift: None,
            },
            paths: vec![BacktestPath {
                path_index: 0,
                decision_times,
                scenario_residuals: group_returns.iter().copied().map(Some).collect(),
                group_returns,
                sharpe: dec!(0.8),
                target_rank_ic: dec!(0.12),
                max_drawdown: dec!(0.005),
                tail_loss: dec!(-0.005),
                turnover: None,
            }]
            .into(),
            deflated_sharpe: dec!(0.96),
            dsr_benchmark_sharpe: dec!(0.4),
            pbo: cscv_selection_evidence.pbo,
            cscv_selection_evidence,
            min_track_record_length_secs: Some(86_400),
            dsr_conservative_independent_trial_count,
            trial_grid_count: 2,
            coord_search_effective_n: 2,
        })
        .expect("seal path set")
    }
}

fn fold_artifacts_fixture() -> CpcvFoldArtifacts {
    CpcvFoldArtifacts::try_new(vec![
        CpcvFoldArtifact {
            validation_regime: CpcvFoldValidationRegime::PortfolioEconomics,
            identity: CpcvEstimatorIdentity::Validation {
                combination_index: 0,
                test_partitions_hash: content_hash('b'),
                test_partition_count: 1,
                test_groups_hash: content_hash('c'),
                test_group_count: 1,
            },
            training_groups_hash: content_hash('b'),
            training_group_count: 2,
            calibration_fit_groups_hash: content_hash('4'),
            calibration_fit_group_count: 1,
            scenario_fit_groups_hash: content_hash('0'),
            scenario_fit_group_count: 1,
            model_artifact_hash: content_hash('c'),
            serving_contract_hash: content_hash('d'),
            model_payload_hash: content_hash('e'),
            calibration_function_hash: content_hash('7'),
            scenario_economic_function_hash: content_hash('8'),
            calibration_artifact_hash: content_hash('5'),
            scenario_model_hash: content_hash('6'),
        },
        CpcvFoldArtifact {
            validation_regime: CpcvFoldValidationRegime::PortfolioEconomics,
            identity: CpcvEstimatorIdentity::TrialPathValidation {
                trial_id: 0,
                path_index: 0,
                combination_index: 0,
                test_partitions_hash: content_hash('b'),
                test_partition_count: 1,
                test_groups_hash: content_hash('c'),
                test_group_count: 1,
            },
            training_groups_hash: content_hash('f'),
            training_group_count: 3,
            calibration_fit_groups_hash: content_hash('4'),
            calibration_fit_group_count: 1,
            scenario_fit_groups_hash: content_hash('0'),
            scenario_fit_group_count: 1,
            model_artifact_hash: content_hash('1'),
            serving_contract_hash: content_hash('2'),
            model_payload_hash: content_hash('3'),
            calibration_function_hash: content_hash('7'),
            scenario_economic_function_hash: content_hash('8'),
            calibration_artifact_hash: content_hash('5'),
            scenario_model_hash: content_hash('6'),
        },
        CpcvFoldArtifact {
            validation_regime: CpcvFoldValidationRegime::PortfolioEconomics,
            identity: CpcvEstimatorIdentity::TrialPathValidation {
                trial_id: 1,
                path_index: 0,
                combination_index: 0,
                test_partitions_hash: content_hash('b'),
                test_partition_count: 1,
                test_groups_hash: content_hash('c'),
                test_group_count: 1,
            },
            training_groups_hash: content_hash('e'),
            training_group_count: 3,
            calibration_fit_groups_hash: content_hash('4'),
            calibration_fit_group_count: 1,
            scenario_fit_groups_hash: content_hash('0'),
            scenario_fit_group_count: 1,
            model_artifact_hash: content_hash('2'),
            serving_contract_hash: content_hash('3'),
            model_payload_hash: content_hash('4'),
            calibration_function_hash: content_hash('9'),
            scenario_economic_function_hash: content_hash('a'),
            calibration_artifact_hash: content_hash('5'),
            scenario_model_hash: content_hash('6'),
        },
    ])
    .expect("fold artifacts")
}

pub async fn quant_backtest_set_crud() {
    let (pool, _container) = setup_pg().await;
    let db = pool.connection().clone();
    let rc_id = seed_runtime_config(&db).await;
    let (model_version_id, model_run_id, training_dataset_id) =
        seed_model_and_dataset(&db, &rc_id).await;
    let repo = PgBacktestPathSetRepository::new(db.clone());
    let path_set_id = BacktestPathSetId::from_v7();
    let run = PgModelRunRepository::new(db.clone())
        .find_by_id(&model_run_id)
        .await
        .expect("find CPCV run")
        .expect("CPCV run");
    let model = PgModelRegistryRepository::new(db.clone())
        .find_model_version(&model_version_id)
        .await
        .expect("find CPCV model")
        .expect("CPCV model");
    let window_start = run.window_start;
    let new_path_set = PathSetFixture {
        path_set_id,
        model_version_id,
        model_run_id,
        training_dataset_id,
        decision_policy_snapshot_id: rc_id,
        window_start,
        model: &model,
    }
    .seal();
    let contract = PathSetContract {
        db: &db,
        repo: &repo,
        path_set: &new_path_set,
        path_set_id,
        model_version_id,
        model_run_id,
    };
    contract.assert_rollback().await;
    contract.assert_exact_commit().await;
    contract.assert_worm().await;
}
