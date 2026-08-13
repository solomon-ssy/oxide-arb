//! Unified calibration-artifact ledger persistence system contracts.

use std::{collections::BTreeMap, future::Future, pin::Pin};

use chrono::{DateTime, Duration, Utc};
use quant_pivot_error::storage::{StorageError, entity};
use quant_pivot_models::{
    domain::quant::{
        CalibrationArtifactPayload, ModelScoreCalibrationCommit,
        ModelScoreCalibrationCommitOutcome, NewCalibrationArtifact, NewModelRun,
    },
    enums::{
        model::ModelFamily,
        quant::{CalibrationKind, DatasetPurpose, ModelRunKind, ModelRunStatus},
    },
    types::{
        CalibrationArtifactId, ContentHash, DecisionPolicySnapshotId, ModelInputContract,
        ModelRunId, ModelSpecId, ModelTrainingContract, ModelVersionId, PayoutRatio, Probability,
        calibration::{
            IsotonicKnot, MODEL_SCORE_CALIBRATION_FORMAT_VERSION, MarketPriceBiasPayload,
            ModelScoreCalibrationDatasetBinding, ModelScoreCalibrationFitContract,
            ModelScoreCalibrationModelBinding, ModelScoreCalibrationPayload,
            ModelScoreCalibrationPolicyBinding, MonotoneMapping, ReliabilityBin, ReliabilityReport,
            SplitPayoutRateEvidence,
        },
    },
};
use quant_pivot_repository::{
    postgres::{
        PgCalibrationArtifactRepository, PgModelRegistryRepository, PgModelRunRepository,
        PgPolicyRepository, PgTrainingDatasetRepository,
    },
    traits::{
        CalibrationArtifactRepository, ModelRegistryRepository, ModelRunRepository,
        PolicyRepository, TrainingDatasetRepository,
    },
};
use quant_pivot_system_tests::{
    postgres::setup_pg,
    support::{
        model_serving_fixtures::{
            ModelDatasetLedgerFixture, ModelDatasetLedgerSeed, ModelVersionFixture,
            ModelVersionFixtureSeed,
        },
        model_spec_fixtures,
        policy_fixtures::bootstrap_default_policy_bundle,
    },
};
use rust_decimal_macros::dec;
use sea_orm::{ConnectionTrait, DatabaseConnection, DbBackend, Statement};

struct CalibrationFixture {
    model_version_id: ModelVersionId,
    decision_policy_snapshot_id: DecisionPolicySnapshotId,
    input_hash: ContentHash,
    fit_window_start: DateTime<Utc>,
    fit_window_end: DateTime<Utc>,
    model_run_id: ModelRunId,
    artifact: NewCalibrationArtifact,
}

impl CalibrationFixture {
    async fn create_run(&self, db: &DatabaseConnection) -> ModelRunId {
        let model_run_id = ModelRunId::from_v7();
        PgModelRunRepository::new(db.clone())
            .create(NewModelRun {
                model_run_id,
                run_kind: ModelRunKind::Calibration,
                model_version_id: Some(self.model_version_id),
                decision_policy_snapshot_id: self.decision_policy_snapshot_id,
                market_selection_id: None,
                window_start: self.fit_window_start,
                window_end: self.fit_window_end,
                input_hash: self.input_hash,
            })
            .await
            .expect("Calibration model run");
        model_run_id
    }

    fn commit(&self) -> ModelScoreCalibrationCommit {
        ModelScoreCalibrationCommit {
            model_run_id: self.model_run_id,
            artifact: self.artifact.clone(),
        }
    }
}

const fn content_hash(seed: u8) -> ContentHash {
    ContentHash::from_bytes([seed; 32])
}

fn fixture_mapping() -> MonotoneMapping {
    MonotoneMapping::Isotonic {
        knots: vec![
            IsotonicKnot {
                score: dec!(0),
                probability: dec!(0.4),
            },
            IsotonicKnot {
                score: dec!(1),
                probability: dec!(0.6),
            },
        ],
    }
}

fn fixture_reliability() -> ReliabilityReport {
    ReliabilityReport {
        bins: vec![ReliabilityBin {
            predicted_lo: dec!(0),
            predicted_hi: dec!(1),
            sample_count: 10,
            mean_predicted: Probability::new(dec!(0.5)),
            empirical_frequency: Probability::new(dec!(0.5)),
            wilson_ci: (Probability::new(dec!(0.2)), Probability::new(dec!(0.8))),
            mean_adverse_excursion_bps: Some(dec!(-15)),
        }],
        brier_score: dec!(0.25),
        log_loss: dec!(0.7),
        ece: dec!(0),
        n_samples: 10,
    }
}

async fn seed_runtime_config(db: &DatabaseConnection) {
    bootstrap_default_policy_bundle(db, "pg-calibration-it", "integration test").await;
}

fn prepare_fixture<'a>(
    db: &'a DatabaseConnection,
    scope: &'a str,
) -> Pin<Box<dyn Future<Output = CalibrationFixture> + Send + 'a>> {
    Box::pin(prepare_fixture_inner(db, scope))
}

async fn prepare_fixture_inner(db: &DatabaseConnection, scope: &str) -> CalibrationFixture {
    seed_runtime_config(db).await;
    let registry = PgModelRegistryRepository::new(db.clone());
    let model_spec_id = ModelSpecId::from_v7();
    registry
        .create_model_spec(model_spec_fixtures::new_model_spec_fixture(
            model_spec_id,
            scope,
            ModelFamily::WeightedFactor,
            model_spec_fixtures::pooled_horizon_secs(),
            ModelInputContract::single_required("book.mid"),
            ModelTrainingContract::outcome_default(),
        ))
        .await
        .expect("model spec");
    let model_version_id = ModelVersionId::from_v7();
    let model_version = registry
        .create_model_version(
            ModelVersionFixture::prepare(
                db,
                ModelVersionFixtureSeed::training(
                    format!("{scope}:{model_version_id}"),
                    model_version_id,
                    model_spec_id,
                    content_hash(1),
                ),
            )
            .await
            .expect("prepare exact model version"),
        )
        .await
        .expect("model version");
    let training_dataset_id = model_version
        .training_dataset_id
        .expect("fixture model has Training Dataset");
    let training_dataset = PgTrainingDatasetRepository::new(db.clone())
        .find_by_id(&training_dataset_id)
        .await
        .expect("load Training Dataset")
        .expect("Training Dataset exists");
    let training = training_dataset
        .materialization()
        .expect("Training Dataset materialization");
    let bindings = model_version
        .verified_serving_contract()
        .expect("verified serving contract")
        .bindings();
    let policy = PgPolicyRepository::new(db.clone())
        .load_snapshot(&bindings.policy_snapshot.decision_policy_snapshot_id)
        .await
        .expect("load policy snapshot")
        .expect("policy snapshot exists");
    let embargo_secs = i64::try_from(policy.snapshot.model_routing.model.calibration.embargo_secs)
        .expect("calibration embargo fits i64");
    let fit_window_start = training_dataset
        .window_end
        .checked_add_signed(Duration::seconds(embargo_secs))
        .expect("calibration window start");
    let fit_window_end = fit_window_start + Duration::hours(1);
    let calibration_dataset = Box::pin(ModelDatasetLedgerFixture::persist(
        db,
        &ModelDatasetLedgerFixture::local_store(),
        ModelDatasetLedgerSeed {
            scope: format!("{scope}:calibration"),
            model_spec_id,
            model_family: model_version.model_family,
            model_spec_definition_hash: model_version.model_spec_definition_hash,
            factor_serving_plane: training.factor_serving_plane.clone(),
            feature_schema_version: training.manifest.feature_schema_version,
            feature_schema_hash: *training.feature_schema_hash,
            decision_policy_snapshot_id: bindings.policy_snapshot.decision_policy_snapshot_id,
            profile_ref: model_version.profile_ref.clone(),
            prediction_horizon_secs: bindings.model.prediction_horizon_secs,
            purpose: DatasetPurpose::Calibration,
            window_start: fit_window_start,
            window_end: fit_window_end,
            research_program_hash: training_dataset.source_lineage.research_program_hash,
            sample_count: 10,
            decision_interval_secs: 1,
            trade_policy: bindings.trade_policy.clone(),
        },
    ))
    .await
    .expect("Calibration Dataset");
    let calibration = calibration_dataset
        .materialization()
        .expect("Calibration Dataset materialization");
    let payload = ModelScoreCalibrationPayload {
        format_version: MODEL_SCORE_CALIBRATION_FORMAT_VERSION,
        fit_contract: ModelScoreCalibrationFitContract {
            model: ModelScoreCalibrationModelBinding {
                model_version_id,
                artifact_hash: model_version.artifact_hash,
                serving_contract_hash: model_version.serving_contract_hash,
                model_spec_id,
                model_spec_definition_hash: model_version.model_spec_definition_hash,
                model_family: model_version.model_family,
                profile_ref: model_version.profile_ref.clone(),
                category_scope: model_version.category_scope,
                prediction_horizon_secs: bindings.model.prediction_horizon_secs,
                training_dataset_id,
                training_dataset_hash: *training.dataset_hash,
            },
            calibration_dataset: ModelScoreCalibrationDatasetBinding {
                calibration_dataset_id: calibration_dataset.training_dataset_id,
                dataset_hash: *calibration.dataset_hash,
                manifest_hash: *calibration.manifest_hash,
                artifact_bytes_hash: *calibration.artifact_bytes_hash,
                source_slice_manifest_hash: calibration
                    .manifest
                    .source_lineage
                    .source_slice
                    .manifest_hash,
                feature_schema_hash: *calibration.feature_schema_hash,
                factor_schema_hash: calibration.factor_schema_hash(),
                label_schema_hash: *calibration.label_schema_hash,
            },
            policy_snapshot: ModelScoreCalibrationPolicyBinding {
                decision_policy_snapshot_id: bindings.policy_snapshot.decision_policy_snapshot_id,
                snapshot_hash: bindings.policy_snapshot.snapshot_hash,
            },
        },
        mapping: fixture_mapping(),
        reliability: fixture_reliability(),
        split_payout_rate: SplitPayoutRateEvidence {
            total_sample_count: 10,
            split_sample_count: 0,
            empirical_probability: Probability::ZERO,
            wilson_ci: (Probability::ZERO, Probability::new(dec!(0.277533))),
            split_payout_ratio: PayoutRatio::try_new(dec!(0.5)).expect("split payout ratio"),
        },
    };
    let artifact = build_calibration_artifact(payload, fit_window_start, fit_window_end);
    let mut fixture = CalibrationFixture {
        model_version_id,
        decision_policy_snapshot_id: bindings.policy_snapshot.decision_policy_snapshot_id,
        input_hash: *calibration.dataset_hash,
        fit_window_start,
        fit_window_end,
        model_run_id: ModelRunId::from_v7(),
        artifact,
    };
    fixture.model_run_id = fixture.create_run(db).await;
    fixture
}

fn build_calibration_artifact(
    payload: ModelScoreCalibrationPayload,
    fit_window_start: DateTime<Utc>,
    fit_window_end: DateTime<Utc>,
) -> NewCalibrationArtifact {
    let calibration_split_hash = content_hash(2);
    let artifact = NewCalibrationArtifact {
        artifact_id: CalibrationArtifactId::from_v7(),
        kind: CalibrationKind::ModelScore,
        content_hash: payload
            .content_hash(fit_window_start, fit_window_end, &calibration_split_hash)
            .expect("canonical ModelScore calibration hash"),
        fit_window_start,
        fit_window_end,
        calibration_split_hash,
        sample_count: 10,
        payload: CalibrationArtifactPayload::ModelScore(Box::new(payload)),
        active: false,
    };
    artifact
        .verify_model_score()
        .expect("complete calibration artifact");
    artifact
}

fn new_bias_artifact(artifact_id: CalibrationArtifactId, seed: u8) -> NewCalibrationArtifact {
    let now = Utc::now();
    NewCalibrationArtifact {
        artifact_id,
        kind: CalibrationKind::MarketPriceBias,
        content_hash: content_hash(seed),
        fit_window_start: now - Duration::days(30),
        fit_window_end: now,
        calibration_split_hash: content_hash(seed.wrapping_add(100)),
        sample_count: 1_000,
        payload: CalibrationArtifactPayload::MarketPriceBias(MarketPriceBiasPayload {
            by_category: BTreeMap::new(),
        }),
        active: false,
    }
}

async fn assert_running(db: &DatabaseConnection, model_run_id: ModelRunId) {
    let run = PgModelRunRepository::new(db.clone())
        .find_by_id(&model_run_id)
        .await
        .expect("load Calibration run")
        .expect("Calibration run exists");
    assert_eq!(run.status, ModelRunStatus::Running);
    assert!(run.output_hash.is_none());
    assert!(run.finished_at.is_none());
}

pub async fn model_score_commit_atomic() {
    let (pool, _container) = setup_pg().await;
    let db = pool.connection().clone();
    let fixture = prepare_fixture(&db, "calibration-atomic").await;
    let repo = PgCalibrationArtifactRepository::new(db.clone());

    let inserted = repo
        .commit_model_score(fixture.commit())
        .await
        .expect("atomic artifact+run commit");
    assert!(matches!(
        &inserted,
        ModelScoreCalibrationCommitOutcome::Inserted { .. }
    ));
    inserted
        .artifact()
        .verify_model_score()
        .expect("persisted ModelScore artifact");
    assert_eq!(
        inserted.artifact().content_hash,
        fixture.artifact.content_hash
    );
    assert_eq!(inserted.model_run().status, ModelRunStatus::Succeeded);
    assert_eq!(
        inserted.model_run().output_hash,
        Some(fixture.artifact.content_hash)
    );
    assert!(inserted.model_run().finished_at.is_some());

    let same_run = repo
        .commit_model_score(fixture.commit())
        .await
        .expect("same-run idempotent replay");
    assert!(matches!(
        &same_run,
        ModelScoreCalibrationCommitOutcome::ExistingExact { .. }
    ));
    assert_eq!(
        same_run.artifact().artifact_id,
        inserted.artifact().artifact_id
    );

    let replay_run_id = fixture.create_run(&db).await;
    let replay = repo
        .commit_model_score(ModelScoreCalibrationCommit {
            model_run_id: replay_run_id,
            artifact: NewCalibrationArtifact {
                artifact_id: CalibrationArtifactId::from_v7(),
                ..fixture.artifact.clone()
            },
        })
        .await
        .expect("new-run exact-content replay");
    assert!(matches!(
        &replay,
        ModelScoreCalibrationCommitOutcome::ExistingExact { .. }
    ));
    assert_eq!(
        replay.artifact().artifact_id,
        inserted.artifact().artifact_id
    );
    assert_eq!(replay.model_run().status, ModelRunStatus::Succeeded);
}

pub async fn model_score_commit_concurrent() {
    let (pool, _container) = setup_pg().await;
    let db = pool.connection().clone();
    let fixture = prepare_fixture(&db, "calibration-concurrent").await;
    let left_repo = PgCalibrationArtifactRepository::new(db.clone());
    let right_repo = PgCalibrationArtifactRepository::new(db.clone());
    let (left, right) = tokio::join!(
        left_repo.commit_model_score(fixture.commit()),
        right_repo.commit_model_score(fixture.commit()),
    );
    let outcomes = [
        left.expect("left concurrent commit"),
        right.expect("right concurrent commit"),
    ];
    assert_eq!(
        outcomes
            .iter()
            .filter(|outcome| matches!(
                outcome,
                ModelScoreCalibrationCommitOutcome::Inserted { .. }
            ))
            .count(),
        1,
        "exactly one concurrent transaction may append the canonical artifact"
    );
    assert_eq!(
        outcomes
            .iter()
            .filter(|outcome| matches!(
                outcome,
                ModelScoreCalibrationCommitOutcome::ExistingExact { .. }
            ))
            .count(),
        1,
        "the lock follower must observe an exact idempotent replay"
    );
    assert_eq!(
        outcomes[0].artifact().artifact_id,
        outcomes[1].artifact().artifact_id
    );
    assert_eq!(outcomes[0].model_run().status, ModelRunStatus::Succeeded);
    assert_eq!(outcomes[1].model_run().status, ModelRunStatus::Succeeded);
}

pub async fn model_score_rejects_tampering() {
    let (pool, _container) = setup_pg().await;
    let db = pool.connection().clone();
    let fixture = prepare_fixture(&db, "calibration-tamper").await;
    let repo = PgCalibrationArtifactRepository::new(db.clone());

    let direct = repo.create(fixture.artifact.clone()).await;
    assert!(
        matches!(
            &direct,
            Err(StorageError::InvariantViolation {
                entity: Some(entity::QUANT_CALIBRATION_ARTIFACT),
                ..
            })
        ),
        "ModelScore create must require the atomic commit path, got {direct:?}"
    );
    assert_running(&db, fixture.model_run_id).await;

    let mut tampered = fixture.artifact.clone();
    let fit_window_start = tampered.fit_window_start;
    let fit_window_end = tampered.fit_window_end;
    let calibration_split_hash = tampered.calibration_split_hash;
    let CalibrationArtifactPayload::ModelScore(payload) = &mut tampered.payload else {
        panic!("fixture must contain ModelScore payload");
    };
    payload.fit_contract.calibration_dataset.manifest_hash = content_hash(99);
    tampered.content_hash = payload
        .content_hash(fit_window_start, fit_window_end, &calibration_split_hash)
        .expect("reseal tampered payload");
    let tampered_hash = tampered.content_hash;
    let rejected = repo
        .commit_model_score(ModelScoreCalibrationCommit {
            model_run_id: fixture.model_run_id,
            artifact: tampered,
        })
        .await;
    assert!(
        matches!(
            &rejected,
            Err(StorageError::InvariantViolation {
                entity: Some(entity::QUANT_CALIBRATION_ARTIFACT),
                ..
            })
        ),
        "lineage tampering must fail closed, got {rejected:?}"
    );
    assert_running(&db, fixture.model_run_id).await;
    assert!(
        repo.find_by_content_hash(&tampered_hash)
            .await
            .expect("query tampered content")
            .is_none(),
        "rejected lineage must not append an artifact"
    );
}

pub async fn model_score_rollback() {
    let (pool, _container) = setup_pg().await;
    let db = pool.connection().clone();
    let fixture = prepare_fixture(&db, "calibration-rollback").await;
    let repo = PgCalibrationArtifactRepository::new(db.clone());
    let collision_id = CalibrationArtifactId::from_v7();
    repo.create(new_bias_artifact(collision_id, 70))
        .await
        .expect("primary-key collision fixture");
    let rejected = repo
        .commit_model_score(ModelScoreCalibrationCommit {
            model_run_id: fixture.model_run_id,
            artifact: NewCalibrationArtifact {
                artifact_id: collision_id,
                ..fixture.artifact.clone()
            },
        })
        .await;
    assert!(
        matches!(&rejected, Err(StorageError::StateConflict { .. })),
        "artifact conflict without exact content must roll back, got {rejected:?}"
    );
    assert_running(&db, fixture.model_run_id).await;
    assert!(
        repo.find_by_content_hash(&fixture.artifact.content_hash)
            .await
            .expect("query rolled-back content")
            .is_none(),
        "failed atomic commit must not leave an orphan artifact"
    );
}

pub async fn calibration_artifact_is_worm() {
    let (pool, _container) = setup_pg().await;
    let db = pool.connection().clone();
    let fixture = prepare_fixture(&db, "calibration-worm").await;
    let artifact = PgCalibrationArtifactRepository::new(db.clone())
        .commit_model_score(fixture.commit())
        .await
        .expect("commit WORM calibration")
        .artifact()
        .clone();

    let immutable_update = db
        .execute_raw(Statement::from_sql_and_values(
            DbBackend::Postgres,
            "UPDATE quant_calibration_artifact SET calibration_split_hash = $2 \
             WHERE artifact_id = $1",
            [
                artifact.artifact_id.as_uuid().into(),
                content_hash(91).into(),
            ],
        ))
        .await;
    assert!(
        immutable_update.is_err(),
        "calibration immutable payload columns must reject raw UPDATE"
    );
    let active_update = db
        .execute_raw(Statement::from_sql_and_values(
            DbBackend::Postgres,
            "UPDATE quant_calibration_artifact SET active = TRUE WHERE artifact_id = $1",
            [artifact.artifact_id.as_uuid().into()],
        ))
        .await
        .expect("active is the sole mutable calibration column");
    assert_eq!(active_update.rows_affected(), 1);
    let reloaded = PgCalibrationArtifactRepository::new(db.clone())
        .find_by_id(&artifact.artifact_id)
        .await
        .expect("reload active artifact")
        .expect("active artifact exists");
    assert!(reloaded.active);

    let delete = db
        .execute_raw(Statement::from_sql_and_values(
            DbBackend::Postgres,
            "DELETE FROM quant_calibration_artifact WHERE artifact_id = $1",
            [artifact.artifact_id.as_uuid().into()],
        ))
        .await;
    assert!(
        delete.is_err(),
        "calibration artifact DELETE must be rejected by the lifecycle guard"
    );
}

pub async fn mark_missing_not_found() {
    let (pool, _container) = setup_pg().await;
    let repo = PgCalibrationArtifactRepository::new(pool.connection().clone());
    let result = repo.mark_active(&CalibrationArtifactId::from_v7()).await;
    assert!(matches!(
        result,
        Err(StorageError::NotFound {
            entity: entity::QUANT_CALIBRATION_ARTIFACT,
            ..
        })
    ));
}

pub async fn activate_market_price_active() {
    let (pool, _container) = setup_pg().await;
    let repo = PgCalibrationArtifactRepository::new(pool.connection().clone());
    let first = repo
        .create(new_bias_artifact(CalibrationArtifactId::from_v7(), 10))
        .await
        .expect("first bias table");
    let second = repo
        .create(new_bias_artifact(CalibrationArtifactId::from_v7(), 20))
        .await
        .expect("second bias table");
    repo.mark_active(&first.artifact_id)
        .await
        .expect("activate first");
    let second = repo
        .mark_active(&second.artifact_id)
        .await
        .expect("activate second");
    assert!(second.active);
    let first_reloaded = repo
        .find_by_id(&first.artifact_id)
        .await
        .expect("find first")
        .expect("first exists");
    assert!(
        !first_reloaded.active,
        "activating the second bias table must deactivate the first"
    );
}

pub async fn model_activation_isolated() {
    let (pool, _container) = setup_pg().await;
    let db = pool.connection().clone();
    let first_fixture = prepare_fixture(&db, "calibration-active-first").await;
    let second_fixture = prepare_fixture(&db, "calibration-active-second").await;
    let repo = PgCalibrationArtifactRepository::new(db.clone());
    let first = repo
        .commit_model_score(first_fixture.commit())
        .await
        .expect("first ModelScore artifact")
        .artifact()
        .clone();
    let second = repo
        .commit_model_score(second_fixture.commit())
        .await
        .expect("second ModelScore artifact")
        .artifact()
        .clone();
    repo.mark_active(&first.artifact_id)
        .await
        .expect("activate first");
    repo.mark_active(&second.artifact_id)
        .await
        .expect("activate second");
    let first = repo
        .find_by_id(&first.artifact_id)
        .await
        .expect("find first")
        .expect("first exists");
    let second = repo
        .find_by_id(&second.artifact_id)
        .await
        .expect("find second")
        .expect("second exists");
    assert!(first.active, "first model-specific calibrator stays active");
    assert!(second.active, "second model-specific calibrator is active");
}

pub async fn bias_activation_isolated() {
    let (pool, _container) = setup_pg().await;
    let db = pool.connection().clone();
    let fixture = prepare_fixture(&db, "calibration-bias-isolation").await;
    let repo = PgCalibrationArtifactRepository::new(db.clone());
    let calibrator = repo
        .commit_model_score(fixture.commit())
        .await
        .expect("ModelScore artifact")
        .artifact()
        .clone();
    let bias_table = repo
        .create(new_bias_artifact(CalibrationArtifactId::from_v7(), 50))
        .await
        .expect("bias table");
    repo.mark_active(&bias_table.artifact_id)
        .await
        .expect("activate bias table");
    repo.mark_active(&calibrator.artifact_id)
        .await
        .expect("activate calibrator");
    let bias_table = repo
        .find_by_id(&bias_table.artifact_id)
        .await
        .expect("find bias table")
        .expect("bias table exists");
    assert!(
        bias_table.active,
        "activating a ModelScore artifact must not affect market-price bias"
    );
}
