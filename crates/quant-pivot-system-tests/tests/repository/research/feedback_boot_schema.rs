//! Feedback-cycle boot-schema contracts against a real `PostgreSQL` instance.

use chrono::{DateTime, Duration, Utc};
use quant_pivot_models::{
    domain::{
        ports::{
            FeedbackCandidateFamily, FeedbackCandidateFamilyInput, FeedbackCandidateRecipe,
            FeedbackComparisonContract, FeedbackDatasetBuildRequest,
        },
        quant::{
            DriftReportInput, FeedbackCohortWindow, FeedbackCycleKey, FeedbackCycleKeyInput,
            FeedbackEvaluationUseInput, FeedbackStageEventInput, NewDriftReport, NewFeedbackCycle,
            NewFeedbackEvaluationUse, NewFeedbackStageEvent,
        },
    },
    enums::{
        model::ModelFamily,
        quant::{
            CalibrationMethod, DatasetPurpose, DownsideSource, FeedbackDriftAssessment,
            FeedbackDriftKind, FeedbackDriftMetric, FeedbackStage, FeedbackStageEventKind,
            FeedbackTriggerFamily,
        },
    },
    hashing::CanonicalDigest,
    types::{
        ArtifactUri, CapabilityRegistryHashes, ContentHash, FeedbackCycleId, ModelInputContract,
        ModelSpecId, ModelTrainingContract, ModelVersionId, ResearchProfileRef, TrainingDatasetId,
    },
};
use quant_pivot_repository::{
    postgres::{PgModelRegistryRepository, PgTrainingDatasetRepository},
    traits::{ModelRegistryRepository, TrainingDatasetRepository},
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
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use sea_orm::{
    ActiveModelTrait, ConnectionTrait, DatabaseConnection, DbBackend, DbErr, IntoActiveModel,
    Statement, TryGetable,
};

pub struct FeedbackSchemaFixture {
    pub cycle: NewFeedbackCycle,
    pub second_cycle: NewFeedbackCycle,
    pub cycle_id: FeedbackCycleId,
    pub second_cycle_id: FeedbackCycleId,
    pub profile_ref: ResearchProfileRef,
    pub champion_model_version_id: ModelVersionId,
    pub champion_serving_contract_hash: ContentHash,
    pub candidate_family: FeedbackCandidateFamily,
    pub candidate_family_hash: ContentHash,
    pub second_candidate_family_hash: ContentHash,
    pub comparison_contract_hash: ContentHash,
    pub second_comparison_contract_hash: ContentHash,
    pub evaluation_dataset_id: TrainingDatasetId,
    pub evaluation_dataset_hash: ContentHash,
    pub evaluation_artifact_bytes_hash: ContentHash,
    pub cohort_manifest_hash: ContentHash,
    pub evaluation_window_start: DateTime<Utc>,
    pub evaluation_window_end: DateTime<Utc>,
    pub label_cutoff: DateTime<Utc>,
    pub observed_at: DateTime<Utc>,
}

impl FeedbackSchemaFixture {
    pub fn stage_event(
        &self,
        feedback_cycle_id: FeedbackCycleId,
        actor: &str,
    ) -> NewFeedbackStageEvent {
        NewFeedbackStageEvent::try_seal(FeedbackStageEventInput {
            feedback_cycle_id,
            event_sequence: 1,
            stage: FeedbackStage::Trigger,
            event_kind: FeedbackStageEventKind::Triggered,
            research_job_id: None,
            actor: Some(actor.to_owned()),
            reason_code: Some("schema_fixture".to_owned()),
            evidence_uri: None,
            evidence_hash: None,
            occurred_at: self.observed_at,
        })
        .expect("seal feedback stage event")
    }

    pub fn drift_report(
        &self,
        label_cutoff: DateTime<Utc>,
        observed_value: Decimal,
        detail_hash: ContentHash,
    ) -> NewDriftReport {
        NewDriftReport::try_seal(DriftReportInput {
            feedback_cycle_id: self.cycle_id,
            kind: FeedbackDriftKind::Data,
            metric: FeedbackDriftMetric::PopulationStabilityIndex,
            assessment: FeedbackDriftAssessment::ThresholdExceeded,
            baseline_window_start: self.evaluation_window_start - Duration::hours(4),
            baseline_window_end: self.evaluation_window_start - Duration::hours(3),
            evaluation_window_start: self.evaluation_window_start,
            evaluation_window_end: self.evaluation_window_end,
            label_cutoff,
            observed_value: Some(observed_value),
            threshold: dec!(0.10),
            sample_count: 100,
            detail_uri: ArtifactUri::parse("s3://fixture/feedback/drift.json")
                .expect("drift artifact URI"),
            detail_hash,
            observed_at: self.observed_at,
        })
        .expect("seal drift report")
    }

    pub fn evaluation_use(
        &self,
        feedback_cycle_id: FeedbackCycleId,
        candidate_family_hash: ContentHash,
        evaluation_dataset_hash: ContentHash,
        cpcv_artifact_hash: ContentHash,
    ) -> NewFeedbackEvaluationUse {
        let comparison_contract_hash = if candidate_family_hash == self.candidate_family_hash {
            self.comparison_contract_hash
        } else {
            self.second_comparison_contract_hash
        };
        NewFeedbackEvaluationUse::try_seal(FeedbackEvaluationUseInput {
            feedback_cycle_id,
            profile_ref: self.profile_ref.clone(),
            evaluation_dataset_id: self.evaluation_dataset_id,
            evaluation_dataset_hash,
            evaluation_artifact_bytes_hash: self.evaluation_artifact_bytes_hash,
            cohort_manifest_hash: self.cohort_manifest_hash,
            evaluation_window_start: self.evaluation_window_start,
            evaluation_window_end: self.evaluation_window_end,
            label_cutoff: self.label_cutoff,
            champion_model_version_id: self.champion_model_version_id,
            champion_serving_contract_hash: self.champion_serving_contract_hash,
            candidate_family_hash,
            comparison_contract_hash,
            cpcv_artifact_uri: ArtifactUri::parse("s3://fixture/feedback/cpcv.json")
                .expect("CPCV artifact URI"),
            cpcv_artifact_hash,
        })
        .expect("seal feedback evaluation use")
    }

    async fn verify_cycle_contracts(&self, db: &DatabaseConnection) {
        let duplicate_cycle_id = FeedbackCycleId::from_idempotency_hash(&content_hash('f'));
        assert_db_rejection(
            db.execute_raw(Statement::from_sql_and_values(
                DbBackend::Postgres,
                "INSERT INTO quant_feedback_cycle (
                    feedback_cycle_id, idempotency_hash, idempotency_key, trigger_family,
                    profile_ref, research_profile_artifact_id, profile_hash, feedback_policy_hash,
                    label_cutoff, capability_registry_hashes, champion_model_version_id,
                    champion_serving_contract_hash, candidate_family, candidate_family_hash
                 )
                 SELECT $1, idempotency_hash, idempotency_key, trigger_family,
                        profile_ref, research_profile_artifact_id, profile_hash,
                        feedback_policy_hash, label_cutoff, capability_registry_hashes,
                        champion_model_version_id, champion_serving_contract_hash, candidate_family,
                        candidate_family_hash
                 FROM quant_feedback_cycle
                 WHERE feedback_cycle_id = $2",
                [
                    duplicate_cycle_id.as_uuid().into(),
                    self.cycle_id.as_uuid().into(),
                ],
            ))
            .await,
            "uq_quant_feedback_cycle_idempotency",
        );
        assert_db_rejection(
            db.execute_raw(Statement::from_sql_and_values(
                DbBackend::Postgres,
                "UPDATE quant_feedback_cycle SET candidate_family_hash = $1
                 WHERE feedback_cycle_id = $2",
                [
                    content_hash('9').to_string().into(),
                    self.cycle_id.as_uuid().into(),
                ],
            ))
            .await,
            "feedback-cycle frozen identity cannot change",
        );
        assert_db_rejection(
            db.execute_raw(Statement::from_sql_and_values(
                DbBackend::Postgres,
                "DELETE FROM quant_feedback_cycle WHERE feedback_cycle_id = $1",
                [self.cycle_id.as_uuid().into()],
            ))
            .await,
            "feedback-cycle row is immutable",
        );
    }

    async fn verify_stage_contracts(&self, db: &DatabaseConnection) {
        let missing_cycle_id = FeedbackCycleId::from_idempotency_hash(&content_hash('8'));
        assert_db_rejection(
            self.stage_event(missing_cycle_id, "missing-cycle")
                .into_active_model()
                .insert(db)
                .await,
            "fk_quant_feedback_stage_cycle",
        );
        self.stage_event(self.cycle_id, "scheduler")
            .into_active_model()
            .insert(db)
            .await
            .expect("insert canonical feedback stage event");
        assert_db_rejection(
            self.stage_event(self.cycle_id, "operator")
                .into_active_model()
                .insert(db)
                .await,
            "uq_quant_feedback_stage_sequence",
        );
        assert_db_rejection(
            db.execute_raw(Statement::from_sql_and_values(
                DbBackend::Postgres,
                "UPDATE quant_feedback_stage_event SET event_hash = event_hash
                 WHERE feedback_cycle_id = $1",
                [self.cycle_id.as_uuid().into()],
            ))
            .await,
            "append-only",
        );
        assert_db_rejection(
            db.execute_raw(Statement::from_sql_and_values(
                DbBackend::Postgres,
                "DELETE FROM quant_feedback_stage_event WHERE feedback_cycle_id = $1",
                [self.cycle_id.as_uuid().into()],
            ))
            .await,
            "append-only",
        );
    }

    async fn verify_drift_contracts(&self, db: &DatabaseConnection) {
        assert_db_rejection(
            self.drift_report(
                self.label_cutoff + Duration::minutes(1),
                dec!(0.20),
                content_hash('1'),
            )
            .into_active_model()
            .insert(db)
            .await,
            "fk_quant_drift_report_cycle",
        );
        self.drift_report(self.label_cutoff, dec!(0.20), content_hash('2'))
            .into_active_model()
            .insert(db)
            .await
            .expect("insert canonical drift report");
        assert_db_rejection(
            self.drift_report(self.label_cutoff, dec!(0.25), content_hash('3'))
                .into_active_model()
                .insert(db)
                .await,
            "uq_quant_drift_report_cycle_metric",
        );
        assert_db_rejection(
            db.execute_raw(Statement::from_sql_and_values(
                DbBackend::Postgres,
                "UPDATE quant_drift_report SET report_hash = report_hash
                 WHERE feedback_cycle_id = $1",
                [self.cycle_id.as_uuid().into()],
            ))
            .await,
            "append-only",
        );
        assert_db_rejection(
            db.execute_raw(Statement::from_sql_and_values(
                DbBackend::Postgres,
                "DELETE FROM quant_drift_report WHERE feedback_cycle_id = $1",
                [self.cycle_id.as_uuid().into()],
            ))
            .await,
            "append-only",
        );
    }

    async fn verify_evaluation_contracts(&self, db: &DatabaseConnection) {
        assert_db_rejection(
            self.evaluation_use(
                self.cycle_id,
                self.second_candidate_family_hash,
                self.evaluation_dataset_hash,
                content_hash('4'),
            )
            .into_active_model()
            .insert(db)
            .await,
            "fk_quant_feedback_evaluation_cycle",
        );
        assert_db_rejection(
            self.evaluation_use(
                self.cycle_id,
                self.candidate_family_hash,
                content_hash('5'),
                content_hash('6'),
            )
            .into_active_model()
            .insert(db)
            .await,
            "fk_quant_feedback_evaluation_dataset",
        );
        self.evaluation_use(
            self.cycle_id,
            self.candidate_family_hash,
            self.evaluation_dataset_hash,
            content_hash('7'),
        )
        .into_active_model()
        .insert(db)
        .await
        .expect("insert canonical feedback evaluation use");
        assert_db_rejection(
            self.evaluation_use(
                self.second_cycle_id,
                self.second_candidate_family_hash,
                self.evaluation_dataset_hash,
                content_hash('8'),
            )
            .into_active_model()
            .insert(db)
            .await,
            "uq_quant_feedback_evaluation_dataset",
        );
        assert_db_rejection(
            db.execute_raw(Statement::from_sql_and_values(
                DbBackend::Postgres,
                "UPDATE quant_feedback_evaluation_use
                 SET evaluation_use_hash = evaluation_use_hash
                 WHERE feedback_cycle_id = $1",
                [self.cycle_id.as_uuid().into()],
            ))
            .await,
            "append-only",
        );
        assert_db_rejection(
            db.execute_raw(Statement::from_sql_and_values(
                DbBackend::Postgres,
                "DELETE FROM quant_feedback_evaluation_use WHERE feedback_cycle_id = $1",
                [self.cycle_id.as_uuid().into()],
            ))
            .await,
            "append-only",
        );
    }

    async fn verify_row_counts(&self, db: &DatabaseConnection) {
        let counts = db
            .query_one_raw(Statement::from_string(
                DbBackend::Postgres,
                "SELECT
                    (SELECT count(*) FROM quant_feedback_cycle) AS cycles,
                    (SELECT count(*) FROM quant_feedback_stage_event) AS stages,
                    (SELECT count(*) FROM quant_drift_report) AS drift_reports,
                    (SELECT count(*) FROM quant_feedback_evaluation_use) AS evaluation_uses",
            ))
            .await
            .expect("query feedback-schema row counts")
            .expect("feedback-schema row counts");
        assert_eq!(
            i64::try_get(&counts, "", "cycles").expect("decode cycle count"),
            2
        );
        assert_eq!(
            i64::try_get(&counts, "", "stages").expect("decode stage count"),
            1
        );
        assert_eq!(
            i64::try_get(&counts, "", "drift_reports").expect("decode drift count"),
            1
        );
        assert_eq!(
            i64::try_get(&counts, "", "evaluation_uses").expect("decode evaluation-use count"),
            1
        );
    }

    async fn insert_cycles(&self, db: &DatabaseConnection) {
        self.cycle
            .clone()
            .into_active_model()
            .insert(db)
            .await
            .expect("insert canonical feedback cycle");
        self.second_cycle
            .clone()
            .into_active_model()
            .insert(db)
            .await
            .expect("insert second canonical feedback cycle");
    }
}

pub fn content_hash(seed: char) -> ContentHash {
    ContentHash::parse(&format!("blake3:{}", seed.to_string().repeat(64)))
        .expect("valid fixture hash")
}

pub async fn db_clock(db: &DatabaseConnection) -> DateTime<Utc> {
    let row = db
        .query_one_raw(Statement::from_string(
            DbBackend::Postgres,
            "SELECT statement_timestamp() AS observed_at",
        ))
        .await
        .expect("query feedback-schema database clock")
        .expect("feedback-schema database clock row");
    DateTime::<Utc>::try_get(&row, "", "observed_at")
        .expect("decode feedback-schema database clock")
}

fn assert_db_rejection<T>(result: Result<T, DbErr>, expected: &str) {
    let Err(error) = result else {
        panic!("database unexpectedly accepted feedback-schema drift");
    };
    let detail = error.to_string();
    assert!(
        detail.contains(expected),
        "expected database rejection containing {expected:?}, got {detail}"
    );
}

fn feedback_method(profile_ref: &ResearchProfileRef) -> (ContentHash, FeedbackComparisonContract) {
    let profile = profile_ref
        .resolve_builtin_research_profile()
        .expect("resolve feedback-schema ResearchProfile");
    let policy_hash = profile
        .spec
        .feedback_policy
        .content_hash()
        .expect("feedback policy hash");
    let comparison_contract =
        FeedbackComparisonContract::try_from_policy(&profile.spec.feedback_policy)
            .expect("freeze feedback-schema comparison contract");
    (policy_hash, comparison_contract)
}

fn build_candidate_family(
    shared_evaluation: FeedbackDatasetBuildRequest,
    comparison_contract: FeedbackComparisonContract,
) -> FeedbackCandidateFamily {
    let evaluation_start = shared_evaluation.window.window_start();
    let policy_id = shared_evaluation.source_lineage.decision_policy_snapshot_id;
    let request = |purpose, window_start, cutoff| {
        let mut source_lineage = shared_evaluation.source_lineage.clone();
        source_lineage.source_window_start = window_start;
        source_lineage.source_window_end = cutoff;
        source_lineage.pit_cutoff = cutoff;
        FeedbackDatasetBuildRequest {
            training_dataset_id: TrainingDatasetId::from_v7(),
            model_spec_id: shared_evaluation.model_spec_id,
            model_spec_definition_hash: shared_evaluation.model_spec_definition_hash,
            source_lineage,
            window: FeedbackCohortWindow::try_new(
                shared_evaluation.window.profile_ref().clone(),
                window_start,
                cutoff,
            )
            .expect("freeze candidate Dataset window"),
            purpose,
        }
    };
    let recipe = FeedbackCandidateRecipe::try_seal(
        request(
            DatasetPurpose::Training,
            evaluation_start - Duration::hours(8),
            evaluation_start - Duration::hours(6),
        ),
        request(
            DatasetPurpose::Calibration,
            evaluation_start - Duration::hours(5),
            evaluation_start - Duration::hours(3),
        ),
        CalibrationMethod::Platt,
        DownsideSource::MfeMae,
        policy_id,
    )
    .expect("seal feedback-schema candidate recipe");
    FeedbackCandidateFamily::try_seal(FeedbackCandidateFamilyInput {
        shared_evaluation,
        comparison_contract,
        candidates: vec![recipe],
    })
    .expect("seal feedback-schema candidate family")
}

struct FeedbackCyclePair {
    first: NewFeedbackCycle,
    second: NewFeedbackCycle,
    first_family_hash: ContentHash,
    second_family_hash: ContentHash,
    first_comparison_hash: ContentHash,
    second_comparison_hash: ContentHash,
}

struct FeedbackCyclePairSeed {
    profile_ref: ResearchProfileRef,
    feedback_policy_hash: ContentHash,
    label_cutoff: DateTime<Utc>,
    capability_registry_hashes: CapabilityRegistryHashes,
    champion_model_version_id: ModelVersionId,
    champion_serving_contract_hash: ContentHash,
}

impl FeedbackCyclePairSeed {
    fn seal(
        self,
        first_family: FeedbackCandidateFamily,
        second_family: FeedbackCandidateFamily,
    ) -> FeedbackCyclePair {
        let first_family_hash = first_family.candidate_family_hash();
        let second_family_hash = second_family.candidate_family_hash();
        let first_comparison_hash = first_family.comparison_contract_hash();
        let second_comparison_hash = second_family.comparison_contract_hash();
        let seal = |trigger_family, candidate_family| {
            NewFeedbackCycle::try_seal(
                FeedbackCycleKey::try_new(FeedbackCycleKeyInput {
                    trigger_family,
                    profile_ref: self.profile_ref.clone(),
                    feedback_policy_hash: self.feedback_policy_hash,
                    label_cutoff: self.label_cutoff,
                    capability_registry_hashes: self.capability_registry_hashes.clone(),
                    champion_model_version_id: self.champion_model_version_id,
                    champion_serving_contract_hash: self.champion_serving_contract_hash,
                    candidate_family,
                })
                .expect("freeze feedback-cycle identity"),
            )
            .expect("seal feedback cycle")
        };
        FeedbackCyclePair {
            first: seal(FeedbackTriggerFamily::Scheduled, first_family),
            second: seal(FeedbackTriggerFamily::Manual, second_family),
            first_family_hash,
            second_family_hash,
            first_comparison_hash,
            second_comparison_hash,
        }
    }
}

pub async fn prepare_fixture(db: &DatabaseConnection) -> FeedbackSchemaFixture {
    bootstrap_default_policy_bundle(db, "pg-feedback-schema", "feedback boot-schema contract")
        .await;
    let registry = PgModelRegistryRepository::new(db.clone());
    let model_spec_id = ModelSpecId::from_v7();
    registry
        .create_model_spec(model_spec_fixtures::new_model_spec_fixture(
            model_spec_id,
            "pg-feedback-schema",
            ModelFamily::WeightedFactor,
            model_spec_fixtures::pooled_horizon_secs(),
            ModelInputContract::single_required("book.mid"),
            ModelTrainingContract::settlement_default(),
        ))
        .await
        .expect("feedback-schema model spec");
    let model_version_id = ModelVersionId::from_v7();
    let model_version = registry
        .create_model_version(
            ModelVersionFixture::prepare(
                db,
                ModelVersionFixtureSeed::training(
                    "pg-feedback-schema:model",
                    model_version_id,
                    model_spec_id,
                    content_hash('a'),
                ),
            )
            .await
            .expect("prepare feedback-schema model version"),
        )
        .await
        .expect("persist feedback-schema model version");
    let training_dataset_id = model_version
        .training_dataset_id
        .expect("feedback-schema model has Training Dataset");
    let training_dataset = PgTrainingDatasetRepository::new(db.clone())
        .find_by_id(&training_dataset_id)
        .await
        .expect("load feedback-schema Training Dataset")
        .expect("feedback-schema Training Dataset exists");
    let training = training_dataset
        .materialization()
        .expect("feedback-schema Training Dataset materialization");
    let bindings = model_version
        .verified_serving_contract()
        .expect("verify feedback-schema serving contract")
        .bindings();
    let now = db_clock(db).await;
    let evaluation_window_start = now - Duration::hours(4);
    let evaluation_window_end = now - Duration::hours(3);
    let observed_at = now - Duration::hours(1);
    let evaluation_dataset = ModelDatasetLedgerFixture::persist(
        db,
        &ModelDatasetLedgerFixture::local_store(),
        ModelDatasetLedgerSeed {
            scope: "pg-feedback-schema:evaluation".to_owned(),
            model_spec_id,
            model_family: model_version.model_family,
            model_spec_definition_hash: model_version.model_spec_definition_hash,
            factor_serving_plane: training.factor_serving_plane.clone(),
            feature_schema_version: training.manifest.feature_schema_version,
            feature_schema_hash: *training.feature_schema_hash,
            decision_policy_snapshot_id: bindings.policy_snapshot.decision_policy_snapshot_id,
            profile_ref: model_version.profile_ref.clone(),
            prediction_horizon_secs: bindings.model.prediction_horizon_secs,
            purpose: DatasetPurpose::Evaluation,
            window_start: evaluation_window_start,
            window_end: evaluation_window_end,
            research_program_hash: training_dataset.source_lineage.research_program_hash,
            sample_count: 10,
            decision_interval_secs: 1,
            trade_policy: bindings.trade_policy.clone(),
        },
    )
    .await
    .expect("persist feedback-schema Evaluation Dataset");
    let evaluation = evaluation_dataset
        .materialization()
        .expect("feedback-schema Evaluation Dataset materialization");
    let cohort_manifest = evaluation_dataset
        .cohort_manifest
        .as_ref()
        .expect("Evaluation Dataset cohort manifest");
    let cohort_manifest_hash =
        CanonicalDigest::content_hash_json(cohort_manifest).expect("cohort manifest hash");
    let label_cutoff = evaluation_dataset.source_lineage.pit_cutoff;
    let (feedback_policy_hash, comparison_contract) = feedback_method(&model_version.profile_ref);
    let capability_registry_hashes = evaluation_dataset
        .source_lineage
        .capability_registry_hashes
        .clone();
    let shared_evaluation = FeedbackDatasetBuildRequest {
        training_dataset_id: evaluation_dataset.training_dataset_id,
        model_spec_id: evaluation_dataset.model_spec_id,
        model_spec_definition_hash: evaluation_dataset.model_spec_definition_hash,
        source_lineage: evaluation_dataset.source_lineage.clone(),
        window: FeedbackCohortWindow::try_new(
            model_version.profile_ref.clone(),
            evaluation_window_start,
            evaluation_window_end,
        )
        .expect("freeze Evaluation Dataset window"),
        purpose: DatasetPurpose::Evaluation,
    };
    let candidate_family =
        build_candidate_family(shared_evaluation.clone(), comparison_contract.clone());
    let second_candidate_family = build_candidate_family(shared_evaluation, comparison_contract);
    let retained_candidate_family = candidate_family.clone();
    let pair = FeedbackCyclePairSeed {
        profile_ref: model_version.profile_ref.clone(),
        feedback_policy_hash,
        label_cutoff,
        capability_registry_hashes,
        champion_model_version_id: model_version.model_version_id,
        champion_serving_contract_hash: model_version.serving_contract_hash,
    }
    .seal(candidate_family, second_candidate_family);
    let cycle = pair.first;
    let second_cycle = pair.second;
    let cycle_id = cycle.feedback_cycle_id();
    let second_cycle_id = second_cycle.feedback_cycle_id();

    FeedbackSchemaFixture {
        cycle,
        second_cycle,
        cycle_id,
        second_cycle_id,
        profile_ref: model_version.profile_ref,
        champion_model_version_id: model_version.model_version_id,
        champion_serving_contract_hash: model_version.serving_contract_hash,
        candidate_family: retained_candidate_family,
        candidate_family_hash: pair.first_family_hash,
        second_candidate_family_hash: pair.second_family_hash,
        comparison_contract_hash: pair.first_comparison_hash,
        second_comparison_contract_hash: pair.second_comparison_hash,
        evaluation_dataset_id: evaluation_dataset.training_dataset_id,
        evaluation_dataset_hash: *evaluation.dataset_hash,
        evaluation_artifact_bytes_hash: *evaluation.artifact_bytes_hash,
        cohort_manifest_hash,
        evaluation_window_start,
        evaluation_window_end,
        label_cutoff,
        observed_at,
    }
}

pub async fn feedback_schema_rejects_drift() {
    let (pool, _container) = setup_pg().await;
    let db = pool.connection().clone();
    let fixture = Box::pin(prepare_fixture(&db)).await;
    fixture.insert_cycles(&db).await;
    fixture.verify_cycle_contracts(&db).await;
    fixture.verify_stage_contracts(&db).await;
    fixture.verify_drift_contracts(&db).await;
    fixture.verify_evaluation_contracts(&db).await;
    fixture.verify_row_counts(&db).await;
}
