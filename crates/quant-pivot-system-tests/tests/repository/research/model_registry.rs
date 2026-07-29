//! Model registry persistence system contracts.

use chrono::Utc;
use quant_pivot_error::storage::{StorageError, entity};
use quant_pivot_models::{
    domain::{
        api::{ModelPickerSide, ModelVersionListQuery},
        pagination::PageRequest,
        quant::{
            ModelSpecInfo, ModelVersionInfo, NewModelRun, NewModelSpec, NewModelVersion,
            TrainingDatasetInfo,
        },
    },
    entities::{
        quant_model_spec::Entity as ModelSpecEntity,
        quant_model_version::{
            ActiveModel as ModelVersionActiveModel, Entity as ModelVersionEntity,
        },
    },
    enums::{
        common::MarketCategory,
        model::ModelFamily,
        quant::{ModelRunKind, ModelRunStatus, PublicationStatus},
    },
    types::{
        CRYPTO_PRICE_15M_HORIZON_SECS, ContentHash, FactorDefinitionId, ModelInputContract,
        ModelRunId, ModelSpecId, ModelTrainingContract, ModelVersionId, POOLED_1H_HORIZON_SECS,
        TrainingDatasetId, WEATHER_FORECAST_24H_HORIZON_SECS,
        model_metrics::ModelVersionMetrics,
        model_quality::{
            GateIntent, GateSubject, QUALITY_GATE_REPORT_FORMAT_VERSION, QualityGateReport,
        },
        model_training::ModelTrainingObjective,
    },
};
use quant_pivot_repository::{
    postgres::{PgModelRegistryRepository, PgModelRunRepository, PgTrainingDatasetRepository},
    traits::{ModelRegistryRepository, ModelRunRepository, TrainingDatasetRepository},
};
use quant_pivot_system_tests::{
    postgres::setup_pg,
    support::{
        model_serving_fixtures::{ModelVersionFixture, ModelVersionFixtureSeed},
        model_spec_fixtures,
    },
};
use sea_orm::{
    ActiveModelTrait, ActiveValue, ConnectionTrait, DatabaseBackend, DatabaseConnection,
    EntityTrait, IntoActiveModel, Statement,
};

fn content_hash(seed: char) -> ContentHash {
    let pair = format!("{:02x}", seed as u32);
    let hex: String = pair.chars().cycle().take(64).collect();
    ContentHash::parse(&format!("blake3:{hex}")).expect("hash")
}

fn new_spec(name: &str, family: ModelFamily) -> NewModelSpec {
    new_scoped_spec(name, family, POOLED_1H_HORIZON_SECS)
}

fn new_scoped_spec(name: &str, family: ModelFamily, prediction_horizon_secs: u64) -> NewModelSpec {
    model_spec_fixtures::new_model_spec_fixture(
        ModelSpecId::from_v7(),
        name,
        family,
        i64::try_from(prediction_horizon_secs).expect("fixture horizon fits i64"),
        ModelInputContract::single_required("book.mid"),
        ModelTrainingContract::settlement_default(),
    )
}

async fn new_version(
    db: &DatabaseConnection,
    model_spec_id: ModelSpecId,
    seed: char,
) -> NewModelVersion {
    let model_version_id = ModelVersionId::from_v7();
    let fixture_seed = ModelVersionFixtureSeed::training(
        format!("model-registry:{model_version_id}:{seed}"),
        model_version_id,
        model_spec_id,
        content_hash(seed),
    );
    ModelVersionFixture::prepare(db, fixture_seed)
        .await
        .expect("prepare exact model version")
}

async fn new_training_version(
    db: &DatabaseConnection,
    model_spec_id: ModelSpecId,
    training_dataset_id: TrainingDatasetId,
    model_version_id: ModelVersionId,
    seed: char,
) -> NewModelVersion {
    let mut fixture_seed = ModelVersionFixtureSeed::training(
        format!("model-training-commit:{model_version_id}:{seed}"),
        model_version_id,
        model_spec_id,
        content_hash(seed),
    );
    fixture_seed.training_dataset_id = Some(training_dataset_id);
    ModelVersionFixture::prepare(db, fixture_seed)
        .await
        .expect("prepare dataset-bound model version")
}

async fn create_training_dataset(
    db: &DatabaseConnection,
    spec: &ModelSpecInfo,
) -> TrainingDatasetInfo {
    let training_dataset_id = {
        let preview = new_version(db, spec.model_spec_id, '0').await;
        preview
            .training_dataset_id
            .expect("model fixture training dataset")
    };
    PgTrainingDatasetRepository::new(db.clone())
        .find_by_id(&training_dataset_id)
        .await
        .expect("load training dataset")
        .expect("training dataset")
}

const fn new_training_run(
    model_run_id: ModelRunId,
    run_kind: ModelRunKind,
    model_version_id: Option<ModelVersionId>,
    dataset: &TrainingDatasetInfo,
) -> NewModelRun {
    NewModelRun {
        model_run_id,
        run_kind,
        model_version_id,
        decision_policy_snapshot_id: dataset.decision_policy_snapshot_id,
        market_selection_id: None,
        window_start: dataset.window_start,
        window_end: dataset.window_end,
        input_hash: dataset.dataset_hash.expect("Ready dataset hash"),
    }
}

pub async fn create_model_duplicate_duplicate() {
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

pub async fn model_spec_rejects_only() {
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
    let row = ModelSpecEntity::find_by_id(created.model_spec_id)
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
        ModelSpecEntity::delete_by_id(created.model_spec_id)
            .exec(&db)
            .await
            .is_err(),
        "model spec delete must be rejected by the database"
    );
}

pub async fn create_model_version_lock() {
    let (pool, _container) = setup_pg().await;
    let db = pool.connection().clone();
    let repo = PgModelRegistryRepository::new(db.clone());
    let model_spec_id = ModelSpecId::from_v7();
    repo.create_model_spec(model_spec_fixtures::new_model_spec_fixture(
        model_spec_id,
        "version-alloc-spec",
        ModelFamily::HoldVsExitWeighted,
        model_spec_fixtures::pooled_horizon_secs(),
        ModelInputContract::single_required("book.mid"),
        ModelTrainingContract::settlement_default(),
    ))
    .await
    .expect("model spec");

    let first = new_version(&db, model_spec_id, 'a').await;
    let first = repo
        .create_model_version(first)
        .await
        .expect("first version");
    let second = new_version(&db, model_spec_id, 'b').await;
    let second = repo
        .create_model_version(second)
        .await
        .expect("second version");

    assert_eq!(first.version, 1);
    assert_eq!(second.version, 2);
    assert_eq!(first.model_family, ModelFamily::HoldVsExitWeighted);
    assert_eq!(second.model_family, ModelFamily::HoldVsExitWeighted);
}

struct TrainingCommitFixture {
    db: DatabaseConnection,
    spec: ModelSpecInfo,
    dataset: TrainingDatasetInfo,
}

struct TrainingCommitWinner {
    run_id: ModelRunId,
    version: ModelVersionInfo,
    seed: char,
}

impl TrainingCommitFixture {
    async fn load(db: DatabaseConnection) -> Self {
        let registry = PgModelRegistryRepository::new(db.clone());
        let spec = registry
            .create_model_spec(new_spec(
                "atomic-training-version",
                ModelFamily::WeightedFactor,
            ))
            .await
            .expect("model spec");
        let dataset = create_training_dataset(&db, &spec).await;
        Self { db, spec, dataset }
    }

    async fn commit_race(&self) -> TrainingCommitWinner {
        let registry = PgModelRegistryRepository::new(self.db.clone());
        let runs = PgModelRunRepository::new(self.db.clone());
        let model_run_id = ModelRunId::from_v7();
        runs.create(new_training_run(
            model_run_id,
            ModelRunKind::Training,
            None,
            &self.dataset,
        ))
        .await
        .expect("running training run");
        let first_id = ModelVersionId::from_v7();
        let first = new_training_version(
            &self.db,
            self.spec.model_spec_id,
            self.dataset.training_dataset_id,
            first_id,
            '1',
        )
        .await;
        let second_id = ModelVersionId::from_v7();
        let second = new_training_version(
            &self.db,
            self.spec.model_spec_id,
            self.dataset.training_dataset_id,
            second_id,
            '2',
        )
        .await;
        let (first_result, second_result) = tokio::join!(
            registry.commit_training_model_version(&model_run_id, first),
            registry.commit_training_model_version(&model_run_id, second),
        );
        let (winner, loser_id, seed, conflict) = match (first_result, second_result) {
            (Ok(winner), Err(conflict)) => (winner, second_id, '1', conflict),
            (Err(conflict), Ok(winner)) => (winner, first_id, '2', conflict),
            (first, second) => {
                panic!("exactly one concurrent training commit must succeed: {first:?}, {second:?}")
            }
        };
        assert!(matches!(
            conflict,
            StorageError::StateConflict {
                entity: entity::QUANT_MODEL_RUN,
                ..
            }
        ));
        assert_eq!(winner.version, 1);
        assert!(
            registry
                .find_model_version(&loser_id)
                .await
                .expect("load losing version")
                .is_none(),
            "losing transaction must not leave a candidate row"
        );
        assert_eq!(
            registry
                .next_version_for_spec(&self.spec.model_spec_id)
                .await
                .expect("next version"),
            2
        );
        let completed = runs
            .find_by_id(&model_run_id)
            .await
            .expect("load completed run")
            .expect("completed run");
        assert_eq!(completed.run_kind, ModelRunKind::Training);
        assert_eq!(completed.status, ModelRunStatus::Succeeded);
        assert_eq!(completed.model_version_id, Some(winner.model_version_id));
        assert_eq!(completed.output_hash, Some(winner.artifact_hash));
        assert!(
            completed
                .finished_at
                .is_some_and(|finished_at| finished_at >= completed.started_at),
            "database-owned terminal time must not precede the database-owned run start"
        );
        assert!(completed.error_code.is_none());
        assert!(completed.error_message.is_none());
        TrainingCommitWinner {
            run_id: model_run_id,
            version: winner,
            seed,
        }
    }

    async fn verify_retry(&self, winner: &TrainingCommitWinner) {
        let registry = PgModelRegistryRepository::new(self.db.clone());
        let retry = new_training_version(
            &self.db,
            self.spec.model_spec_id,
            self.dataset.training_dataset_id,
            winner.version.model_version_id,
            winner.seed,
        )
        .await;
        let retried = registry
            .commit_training_model_version(&winner.run_id, retry)
            .await
            .expect("exact terminal retry");
        assert_eq!(retried.model_version_id, winner.version.model_version_id);
        assert_eq!(retried.version, winner.version.version);

        let drifted_retry = new_training_version(
            &self.db,
            self.spec.model_spec_id,
            self.dataset.training_dataset_id,
            winner.version.model_version_id,
            'f',
        )
        .await;
        assert!(matches!(
            registry
                .commit_training_model_version(&winner.run_id, drifted_retry)
                .await,
            Err(StorageError::StateConflict {
                entity: entity::QUANT_MODEL_RUN,
                ..
            })
        ));
        assert_eq!(
            registry
                .find_model_version(&winner.version.model_version_id)
                .await
                .expect("load exact retry winner")
                .expect("winner persists")
                .artifact_hash,
            winner.version.artifact_hash
        );
    }

    async fn reject_run_state(&self, winner: &ModelVersionInfo) {
        let registry = PgModelRegistryRepository::new(self.db.clone());
        let runs = PgModelRunRepository::new(self.db.clone());
        for (run_kind, bound_version, seed, label) in [
            (ModelRunKind::Backtest, None, '3', "wrong-kind"),
            (
                ModelRunKind::Training,
                Some(winner.model_version_id),
                '4',
                "bound-run",
            ),
        ] {
            let run_id = ModelRunId::from_v7();
            runs.create(new_training_run(
                run_id,
                run_kind,
                bound_version,
                &self.dataset,
            ))
            .await
            .unwrap_or_else(|error| panic!("create {label} training run: {error}"));
            let version_id = ModelVersionId::from_v7();
            let version = new_training_version(
                &self.db,
                self.spec.model_spec_id,
                self.dataset.training_dataset_id,
                version_id,
                seed,
            )
            .await;
            assert!(matches!(
                registry
                    .commit_training_model_version(&run_id, version)
                    .await,
                Err(StorageError::StateConflict {
                    entity: entity::QUANT_MODEL_RUN,
                    ..
                })
            ));
            assert!(
                registry
                    .find_model_version(&version_id)
                    .await
                    .unwrap_or_else(|error| panic!("load rejected {label} version: {error}"))
                    .is_none()
            );
        }
    }

    async fn reject_candidate(&self) -> ModelRunId {
        let registry = PgModelRegistryRepository::new(self.db.clone());
        let runs = PgModelRunRepository::new(self.db.clone());
        let run_id = ModelRunId::from_v7();
        runs.create(new_training_run(
            run_id,
            ModelRunKind::Training,
            None,
            &self.dataset,
        ))
        .await
        .expect("training run for invalid candidate");
        let published_id = ModelVersionId::from_v7();
        let mut published = new_training_version(
            &self.db,
            self.spec.model_spec_id,
            self.dataset.training_dataset_id,
            published_id,
            '5',
        )
        .await;
        published.publication_status = PublicationStatus::Published;
        assert!(matches!(
            registry
                .commit_training_model_version(&run_id, published)
                .await,
            Err(StorageError::InvariantViolation {
                entity: Some(entity::QUANT_MODEL_VERSION),
                ..
            })
        ));
        assert!(
            registry
                .find_model_version(&published_id)
                .await
                .expect("load invalid candidate")
                .is_none()
        );

        let missing_dataset_id = ModelVersionId::from_v7();
        let mut missing_dataset = new_training_version(
            &self.db,
            self.spec.model_spec_id,
            self.dataset.training_dataset_id,
            missing_dataset_id,
            'a',
        )
        .await;
        missing_dataset.training_dataset_id = None;
        assert!(matches!(
            registry
                .commit_training_model_version(&run_id, missing_dataset)
                .await,
            Err(StorageError::InvariantViolation {
                entity: Some(entity::QUANT_MODEL_VERSION),
                ..
            })
        ));
        assert!(
            registry
                .find_model_version(&missing_dataset_id)
                .await
                .expect("load dataset-free version")
                .is_none()
        );
        assert_eq!(
            runs.find_by_id(&run_id)
                .await
                .expect("load unaffected run")
                .expect("unaffected run")
                .status,
            ModelRunStatus::Running
        );
        run_id
    }

    async fn reject_lineage(&self, run_id: &ModelRunId) {
        let registry = PgModelRegistryRepository::new(self.db.clone());
        let runs = PgModelRunRepository::new(self.db.clone());
        let other_spec = registry
            .create_model_spec(new_spec(
                "atomic-training-wrong-spec",
                ModelFamily::WeightedFactor,
            ))
            .await
            .expect("other model spec");
        let other_dataset = create_training_dataset(&self.db, &other_spec).await;
        let wrong_spec_id = ModelVersionId::from_v7();
        let wrong_spec = new_training_version(
            &self.db,
            other_spec.model_spec_id,
            other_dataset.training_dataset_id,
            wrong_spec_id,
            'b',
        )
        .await;
        assert!(matches!(
            registry
                .commit_training_model_version(run_id, wrong_spec)
                .await,
            Err(StorageError::StateConflict {
                entity: entity::QUANT_TRAINING_DATASET,
                ..
            })
        ));
        assert!(
            registry
                .find_model_version(&wrong_spec_id)
                .await
                .expect("load wrong-spec version")
                .is_none()
        );

        let drift_run_id = ModelRunId::from_v7();
        let mut drift_run =
            new_training_run(drift_run_id, ModelRunKind::Training, None, &self.dataset);
        drift_run.input_hash = content_hash('c');
        runs.create(drift_run).await.expect("drifted training run");
        let drift_version_id = ModelVersionId::from_v7();
        let drift_version = new_training_version(
            &self.db,
            self.spec.model_spec_id,
            self.dataset.training_dataset_id,
            drift_version_id,
            'd',
        )
        .await;
        assert!(matches!(
            registry
                .commit_training_model_version(&drift_run_id, drift_version)
                .await,
            Err(StorageError::StateConflict {
                entity: entity::QUANT_TRAINING_DATASET,
                ..
            })
        ));
        assert!(
            registry
                .find_model_version(&drift_version_id)
                .await
                .expect("load dataset-hash drift version")
                .is_none()
        );
    }

    async fn finalize_race(&self) {
        let registry = PgModelRegistryRepository::new(self.db.clone());
        let runs = PgModelRunRepository::new(self.db.clone());
        let run_id = ModelRunId::from_v7();
        runs.create(new_training_run(
            run_id,
            ModelRunKind::Training,
            None,
            &self.dataset,
        ))
        .await
        .expect("training run for finalizer race");
        let version_id = ModelVersionId::from_v7();
        let version = new_training_version(
            &self.db,
            self.spec.model_spec_id,
            self.dataset.training_dataset_id,
            version_id,
            'e',
        )
        .await;
        let (commit, cancel) = tokio::join!(
            registry.commit_training_model_version(&run_id, version),
            runs.cancel(&run_id, "operator cancellation race".to_owned()),
        );
        match (commit, cancel) {
            (Ok(version), Err(StorageError::StateConflict { .. })) => {
                assert_eq!(version.model_version_id, version_id);
                assert_eq!(
                    runs.find_by_id(&run_id)
                        .await
                        .expect("load race winner")
                        .expect("race run")
                        .status,
                    ModelRunStatus::Succeeded
                );
            }
            (Err(StorageError::StateConflict { .. }), Ok(cancelled)) => {
                assert_eq!(cancelled.status, ModelRunStatus::Cancelled);
                assert!(
                    registry
                        .find_model_version(&version_id)
                        .await
                        .expect("load cancelled race version")
                        .is_none(),
                    "cancel winner must leave no candidate row"
                );
            }
            (commit, cancel) => {
                panic!("success/cancel race must have one terminal winner: {commit:?}, {cancel:?}")
            }
        }
    }
}

pub async fn training_version_commit_atomic() {
    let (pool, _container) = setup_pg().await;
    let fixture = TrainingCommitFixture::load(pool.connection().clone()).await;
    let winner = fixture.commit_race().await;
    fixture.verify_retry(&winner).await;
    fixture.reject_run_state(&winner.version).await;
    let invalid_run_id = fixture.reject_candidate().await;
    fixture.reject_lineage(&invalid_run_id).await;
    fixture.finalize_race().await;
}

pub async fn find_page_versions_spec() {
    let (pool, _container) = setup_pg().await;
    let db = pool.connection().clone();
    let repo = PgModelRegistryRepository::new(db.clone());

    let buy_spec = repo
        .create_model_spec(new_spec("buy-join-spec", ModelFamily::WeightedFactor))
        .await
        .expect("buy spec");
    let sell_spec = repo
        .create_model_spec(new_spec("sell-join-spec", ModelFamily::HoldVsExitWeighted))
        .await
        .expect("sell spec");

    let buy_version = new_version(&db, buy_spec.model_spec_id, 'c').await;
    let buy = repo
        .create_model_version(buy_version)
        .await
        .expect("buy version");
    let sell_version = new_version(&db, sell_spec.model_spec_id, 'd').await;
    let sell = repo
        .create_model_version(sell_version)
        .await
        .expect("sell version");

    let found = repo
        .find_model_version(&sell.model_version_id)
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

struct ModelVersionBoundaryFixture {
    db: DatabaseConnection,
    version: ModelVersionInfo,
}

impl ModelVersionBoundaryFixture {
    async fn load(db: DatabaseConnection) -> Self {
        let repo = PgModelRegistryRepository::new(db.clone());
        let spec = repo
            .create_model_spec(new_spec(
                "typed-version-documents",
                ModelFamily::WeightedFactor,
            ))
            .await
            .expect("model spec");
        let candidate = new_version(&db, spec.model_spec_id, 'k').await;
        let expected_contract = candidate.serving_contract.clone();
        let expected_contract_hash = candidate
            .serving_contract_hash()
            .expect("prepared contract is valid");
        let version = repo
            .create_model_version(candidate)
            .await
            .expect("model version");
        assert_eq!(version.serving_contract, expected_contract);
        assert_eq!(version.serving_contract_hash, expected_contract_hash);
        assert_eq!(
            version
                .verified_serving_contract()
                .expect("persisted contract and scalar projections are exact"),
            &expected_contract
        );
        let factor_definition_id = expected_contract
            .bindings()
            .factors
            .plane
            .definitions()
            .first()
            .expect("model fixture has an immutable factor revision")
            .factor_definition_id();
        let usage = repo
            .page_factor_usages(&factor_definition_id, PageRequest::new(0, 1_000))
            .await
            .expect("page exact factor serving usage");
        assert_eq!(
            (usage.total, usage.page, usage.size),
            (1, 1, PageRequest::MAX_SIZE)
        );
        assert_eq!(usage.items[0].model_version_id, version.model_version_id);
        assert_eq!(
            usage.items[0].serving_contract_hash,
            version.serving_contract_hash
        );
        let missing = repo
            .page_factor_usages(&FactorDefinitionId::from_v7(), PageRequest::default())
            .await
            .expect("filter absent factor serving usage");
        assert!(missing.items.is_empty());
        assert_eq!(missing.total, 0);
        Self { db, version }
    }

    async fn reject_insert_drift(&self) {
        let repo = PgModelRegistryRepository::new(self.db.clone());
        let mut published = new_version(&self.db, self.version.model_spec_id, 'l').await;
        let published_id = published.model_version_id;
        published.publication_status = PublicationStatus::Published;
        published.published_at = Some(Utc::now());
        assert!(matches!(
            repo.create_model_version(published).await,
            Err(StorageError::InvariantViolation {
                entity: Some(entity::QUANT_MODEL_VERSION),
                ..
            })
        ));
        assert!(
            repo.find_model_version(&published_id)
                .await
                .expect("load rejected direct-publish version")
                .is_none(),
            "a forged Published insert must leave no model-version row"
        );

        let mut projection_drift = new_version(&self.db, self.version.model_spec_id, 'm').await;
        let projection_drift_id = projection_drift.model_version_id;
        projection_drift.category_scope = Some(MarketCategory::Crypto);
        assert!(matches!(
            repo.create_model_version(projection_drift).await,
            Err(StorageError::InvariantViolation {
                entity: Some(entity::QUANT_MODEL_VERSION),
                ..
            })
        ));
        assert!(
            repo.find_model_version(&projection_drift_id)
                .await
                .expect("load rejected contract projection drift")
                .is_none(),
            "contract/scalar drift must leave no model-version row"
        );

        let mut raw_published = new_version(&self.db, self.version.model_spec_id, 'n').await;
        let raw_published_id = raw_published.model_version_id;
        raw_published.version = 2;
        let mut active: ModelVersionActiveModel = raw_published
            .try_into()
            .expect("prepare valid raw Candidate row");
        active.publication_status = ActiveValue::Set(PublicationStatus::Published);
        active.published_at = ActiveValue::Set(Some(Utc::now()));
        let error = ModelVersionEntity::insert(active)
            .exec(&self.db)
            .await
            .expect_err("database trigger must reject raw Published insertion");
        assert!(
            error
                .to_string()
                .contains("must be inserted as an ungated Candidate")
        );
        assert!(
            repo.find_model_version(&raw_published_id)
                .await
                .expect("load rejected raw Published version")
                .is_none(),
            "a raw forged Published insert must leave no model-version row"
        );

        let other_candidate = new_version(&self.db, self.version.model_spec_id, 'o').await;
        ModelVersionFixture::persist_published(&self.db, other_candidate)
            .await
            .expect("initialize a clear latch from another model's exact proof");
        let raw_update = Statement::from_sql_and_values(
            DatabaseBackend::Postgres,
            "UPDATE quant_model_version
             SET publication_status = 'published'::qp_publication_status,
                 published_at = statement_timestamp()
             WHERE model_version_id = $1",
            [self.version.model_version_id.into()],
        );
        let error = self
            .db
            .execute_raw(raw_update)
            .await
            .expect_err("another model's clear latch/proof must not authorize raw publication");
        assert!(
            error
                .to_string()
                .contains("requires an exact passed full parity proof and clear latch")
        );
        let unchanged = repo
            .find_model_version(&self.version.model_version_id)
            .await
            .expect("reload raw-update target")
            .expect("raw-update target remains present");
        assert_eq!(unchanged.publication_status, PublicationStatus::Candidate);
        assert!(unchanged.published_at.is_none());
    }

    async fn reject_contract_mutation(&self) {
        let hash_hex = "ab".repeat(32);
        let hash = format!("blake3:{hash_hex}");
        let statement = Statement::from_sql_and_values(
            DatabaseBackend::Postgres,
            "UPDATE quant_model_version
             SET serving_contract = jsonb_set(
                     serving_contract,
                     '{contract_hash}',
                     to_jsonb($1::text)
                 ),
                 serving_contract_hash = decode($2::text, 'hex')
             WHERE model_version_id = $3",
            [
                hash.into(),
                hash_hex.into(),
                self.version.model_version_id.into(),
            ],
        );
        assert!(
            self.db.execute_raw(statement).await.is_err(),
            "the WORM trigger must reject a jointly rewritten contract and raw hash"
        );
        let unchanged = PgModelRegistryRepository::new(self.db.clone())
            .find_model_version(&self.version.model_version_id)
            .await
            .expect("reload WORM-protected version")
            .expect("model version remains present");
        assert_eq!(unchanged.serving_contract, self.version.serving_contract);
        assert_eq!(
            unchanged.serving_contract_hash,
            self.version.serving_contract_hash
        );
        unchanged
            .verified_serving_contract()
            .expect("failed mutation cannot corrupt the persisted contract");
    }

    async fn reject_json_contracts(&self) {
        let model_version_id = self.version.model_version_id;
        for (sql, document, detail) in [
            (
                "UPDATE quant_model_version SET training_objective = $1::jsonb WHERE model_version_id = $2",
                r#"{"format_version":2,"definition":{"kind":"future_algorithm"}}"#,
                "unknown training-objective tags",
            ),
            (
                "UPDATE quant_model_version SET training_objective = $1::jsonb WHERE model_version_id = $2",
                "[]",
                "a non-object training objective",
            ),
            (
                "UPDATE quant_model_version SET training_objective = $1::jsonb WHERE model_version_id = $2",
                r#"{"format_version":1,"definition":{"kind":"hand_authored","rationale":"wrong version"}}"#,
                "an unsupported document version",
            ),
            (
                "UPDATE quant_model_version SET metrics = $1::jsonb WHERE model_version_id = $2",
                r#"{"format_version":2,"definition":{"kind":"future_metrics"}}"#,
                "unknown model-metrics tags",
            ),
            (
                "UPDATE quant_model_version SET metrics = $1::jsonb WHERE model_version_id = $2",
                r#"{"format_version":2,"definition":{"kind":"learning_to_rank","in_sample":{},"validation":{},"artifact_lineage":{"kind":"factor_native"}}}"#,
                "mismatched metrics and training-objective families",
            ),
            (
                "UPDATE quant_model_version SET training_objective = training_objective || $1::jsonb WHERE model_version_id = $2",
                r#"{"future_field":true}"#,
                "unknown document fields",
            ),
        ] {
            let statement = Statement::from_sql_and_values(
                DatabaseBackend::Postgres,
                sql,
                [document.to_owned().into(), model_version_id.into()],
            );
            assert!(
                self.db.execute_raw(statement).await.is_err(),
                "the DB constraint must reject {detail}"
            );
        }

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
            PgModelRegistryRepository::new(self.db.clone())
                .set_quality_gate_report(&model_version_id, wrong_subject)
                .await
                .is_err(),
            "quality-gate subject id must match the owning model version"
        );
    }
}

pub async fn model_version_rejects_boundary() {
    let (pool, _container) = setup_pg().await;
    let fixture = ModelVersionBoundaryFixture::load(pool.connection().clone()).await;
    fixture.reject_insert_drift().await;
    fixture.reject_contract_mutation().await;
    fixture.reject_json_contracts().await;
}

pub async fn published_artifacts_coexist_explicit() {
    let (pool, _container) = setup_pg().await;
    let db = pool.connection().clone();
    let repo = PgModelRegistryRepository::new(db.clone());
    let spec = repo
        .create_model_spec(new_spec(
            "multiple-published-artifacts-spec",
            ModelFamily::WeightedFactor,
        ))
        .await
        .expect("model spec");

    let first = new_version(&db, spec.model_spec_id, 'e').await;
    ModelVersionFixture::persist_published(&db, first)
        .await
        .expect("publish first artifact through exact parity proof");
    let second = new_version(&db, spec.model_spec_id, 'f').await;
    ModelVersionFixture::persist_published(&db, second)
        .await
        .expect("publish second artifact through exact parity proof");

    assert_eq!(
        repo.list_published_for_spec(&spec.model_spec_id)
            .await
            .expect("published artifacts")
            .len(),
        2
    );
}

pub async fn published_picker_catalog_filters() {
    let (pool, _container) = setup_pg().await;
    let db = pool.connection().clone();
    let repo = PgModelRegistryRepository::new(db.clone());
    let pooled_buy_spec = repo
        .create_model_spec(new_scoped_spec(
            "picker-buy-pooled",
            ModelFamily::WeightedFactor,
            POOLED_1H_HORIZON_SECS,
        ))
        .await
        .expect("pooled buy spec");
    let crypto_buy_spec = repo
        .create_model_spec(new_scoped_spec(
            "picker-buy-crypto",
            ModelFamily::WeightedFactor,
            CRYPTO_PRICE_15M_HORIZON_SECS,
        ))
        .await
        .expect("crypto buy spec");
    let weather_buy_spec = repo
        .create_model_spec(new_scoped_spec(
            "picker-buy-weather",
            ModelFamily::WeightedFactor,
            WEATHER_FORECAST_24H_HORIZON_SECS,
        ))
        .await
        .expect("weather buy spec");
    let sell_spec = repo
        .create_model_spec(new_spec("picker-sell", ModelFamily::HoldVsExitWeighted))
        .await
        .expect("sell spec");

    let generic_buy = new_version(&db, pooled_buy_spec.model_spec_id, 'g').await;
    let generic_buy = ModelVersionFixture::persist_published(&db, generic_buy)
        .await
        .expect("publish generic buy through exact parity proof");

    let crypto_buy = new_version(&db, crypto_buy_spec.model_spec_id, 'h').await;
    let crypto_buy = ModelVersionFixture::persist_published(&db, crypto_buy)
        .await
        .expect("publish crypto buy through exact parity proof");

    let weather_buy = new_version(&db, weather_buy_spec.model_spec_id, 'i').await;
    ModelVersionFixture::persist_published(&db, weather_buy)
        .await
        .expect("publish weather buy through exact parity proof");

    let sell = new_version(&db, sell_spec.model_spec_id, 'j').await;
    let sell = ModelVersionFixture::persist_published(&db, sell)
        .await
        .expect("publish sell through exact parity proof");

    let crypto_catalog = repo
        .list_published_catalog(ModelPickerSide::Buy, Some(MarketCategory::Crypto))
        .await
        .expect("crypto catalog");
    assert_eq!(crypto_catalog.len(), 1);
    assert_eq!(
        crypto_catalog[0].model_version_id,
        crypto_buy.model_version_id
    );
    assert_eq!(crypto_catalog[0].spec_name, "picker-buy-crypto");
    assert_eq!(
        crypto_catalog[0].category_scope,
        Some(MarketCategory::Crypto)
    );
    assert_eq!(crypto_catalog[0].artifact_hash, crypto_buy.artifact_hash);
    assert!(
        crypto_catalog
            .iter()
            .all(|row| row.model_version_id != generic_buy.model_version_id),
        "a vertical picker must never offer the pooled model as a fallback"
    );

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
