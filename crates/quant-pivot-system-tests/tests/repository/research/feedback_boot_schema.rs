//! Feedback-cycle boot-schema contracts against a real `PostgreSQL` instance.

use std::sync::Arc;

use chrono::{DateTime, Duration, Utc};
use quant_pivot_models::{
    domain::{
        ports::{
            CandidateRecipePlanArtifact, CandidateRecipePlanInput, CandidateRecipePlanJobParams,
            CandidateRecipePlanOutcome, CandidateRecipeSelection, FeedbackAttributionManifestRef,
            FeedbackCandidateFamily, FeedbackCandidateFamilyInput, FeedbackCandidateRecipe,
            FeedbackCandidateRecipeInput, FeedbackComparisonContract, FeedbackDatasetBuildRequest,
            FeedbackRecipeCalibrationSpec, FeedbackRecipeCpcvSpec,
            FeedbackRecipeDiagnosticEvidence, FeedbackRecipeDiagnosticSpec,
            FeedbackRecipeDownsideSpec, FeedbackRecipeDriftManifest, FeedbackRecipeResourceBudget,
            FeedbackRecipeTemplate, FeedbackRecipeTemplateInput, FeedbackRecipeTrainingSpec,
        },
        quant::{
            AttributionSubject, DriftReportInput, FeedbackCohortWindow, FeedbackCycleInfo,
            FeedbackCycleKey, FeedbackCycleKeyInput, FeedbackEvaluationUseInput,
            FeedbackStageEventInput, FeedbackStageJobIdentity, ModelVersionInfo,
            NewAttributionArtifact, NewDriftReport, NewFeedbackCycle, NewFeedbackEvaluationUse,
            NewFeedbackStageEvent, NewResearchJob, ResearchJobArtifactRef, ResearchJobFinalization,
            ResearchJobResultRef, TrainingDatasetInfo,
        },
    },
    entities::quant_attribution_artifact::Entity as AttributionArtifactEntity,
    enums::{
        model::ModelFamily,
        quant::{
            AttributionArtifactKind, AttributionCohort, CalibrationMethod, DatasetPurpose,
            DownsideSource, FeedbackDriftAssessment, FeedbackDriftKind, FeedbackDriftMetric,
            FeedbackEvaluationMode, FeedbackRecipeTemplateStatus, FeedbackStage,
            FeedbackStageEventKind, FeedbackTriggerFamily, ResearchJobKind, ResearchJobResultKind,
            ResearchJobStatus,
        },
    },
    hashing::CanonicalDigest,
    runtime_config::{BuyModelRoute, ResearchValidationConfig},
    types::{
        ArtifactUri, ContentHash, DecisionPolicySnapshotId, FeedbackCycleId,
        FeedbackRecipeTemplateId, ModelInputContract, ModelSpecId, ModelTrainingContract,
        ModelVersionId, PolicyBundleGeneration, ResearchJobId, ResearchJobParams,
        ResearchProfileRef, RoleCode, TrainingDatasetId, UserId, WorkerId,
    },
};
use quant_pivot_repository::{
    postgres::{
        PgAttributionArtifactRepository, PgFeedbackCycleRepository,
        PgFeedbackRecipeTemplateRepository, PgModelRegistryRepository, PgResearchJobRepository,
        PgTrainingDatasetRepository,
    },
    traits::{
        AttributionArtifactRepository, FeedbackCycleLeaseGuard, FeedbackCycleRepository,
        FeedbackRecipeTemplateRepository, FeedbackRecipeTemplateWriteOutcome,
        ModelRegistryRepository, ResearchJobEnqueueOutcome, ResearchJobRepository,
        TrainingDatasetRepository,
    },
};
use quant_pivot_research::{
    artifact::{ArtifactKey, ArtifactNamespace, ArtifactStore},
    feedback_recipe::CandidateRecipePlanCodec,
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
    ActiveModelTrait, ActiveValue::Set, ConnectionTrait, DatabaseConnection, DbBackend, DbErr,
    EntityTrait, IntoActiveModel, Statement, TryGetable,
};
use uuid::Uuid;

pub struct FeedbackSchemaFixture {
    pub cycle: NewFeedbackCycle,
    pub second_cycle: NewFeedbackCycle,
    pub cycle_id: FeedbackCycleId,
    pub second_cycle_id: FeedbackCycleId,
    pub profile_ref: ResearchProfileRef,
    pub champion_model_version_id: ModelVersionId,
    pub champion_serving_contract_hash: ContentHash,
    pub candidate_family: FeedbackCandidateFamily,
    pub second_candidate_family: FeedbackCandidateFamily,
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
    pub second_evaluation_window_start: DateTime<Utc>,
    pub second_evaluation_window_end: DateTime<Utc>,
    pub label_cutoff: DateTime<Utc>,
    pub second_label_cutoff: DateTime<Utc>,
    pub observed_at: DateTime<Utc>,
}

impl FeedbackSchemaFixture {
    pub fn stage_event(
        &self,
        feedback_cycle_id: FeedbackCycleId,
        actor: &str,
    ) -> NewFeedbackStageEvent {
        let trigger_family = if feedback_cycle_id == self.cycle_id {
            FeedbackTriggerFamily::Scheduled
        } else {
            FeedbackTriggerFamily::Manual
        };
        NewFeedbackStageEvent::try_seal(FeedbackStageEventInput {
            feedback_cycle_id,
            event_sequence: 1,
            stage: FeedbackStage::Trigger,
            event_kind: FeedbackStageEventKind::Triggered,
            trigger_family: Some(trigger_family),
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
        let label_cutoff = if feedback_cycle_id == self.cycle_id {
            self.label_cutoff
        } else {
            self.second_label_cutoff
        };
        let (evaluation_window_start, evaluation_window_end) = if feedback_cycle_id == self.cycle_id
        {
            (self.evaluation_window_start, self.evaluation_window_end)
        } else {
            (
                self.second_evaluation_window_start,
                self.second_evaluation_window_end,
            )
        };
        NewFeedbackEvaluationUse::try_seal(FeedbackEvaluationUseInput {
            feedback_cycle_id,
            profile_ref: self.profile_ref.clone(),
            evaluation_dataset_id: self.evaluation_dataset_id,
            evaluation_dataset_hash,
            evaluation_artifact_bytes_hash: self.evaluation_artifact_bytes_hash,
            cohort_manifest_hash: self.cohort_manifest_hash,
            evaluation_window_start,
            evaluation_window_end,
            label_cutoff,
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
                    feedback_cycle_id, idempotency_hash, idempotency_key,
                    profile_ref, research_profile_artifact_id, profile_hash, feedback_policy_hash,
                    label_cutoff, champion_model_version_id, champion_serving_contract_hash,
                    champion_model_spec_id, champion_model_spec_definition_hash,
                    champion_model_family, route,
                    decision_policy_snapshot_id, decision_policy_snapshot_hash,
                    policy_bundle_generation, route_generation, evaluation_mode,
                    parent_cycle_id, forced_idempotency_key
                 )
                 SELECT $1, idempotency_hash, idempotency_key,
                        profile_ref, research_profile_artifact_id, profile_hash,
                        feedback_policy_hash, label_cutoff, champion_model_version_id,
                        champion_serving_contract_hash, champion_model_spec_id,
                        champion_model_spec_definition_hash, champion_model_family, route,
                        decision_policy_snapshot_id, decision_policy_snapshot_hash,
                        policy_bundle_generation, route_generation, evaluation_mode,
                        parent_cycle_id, forced_idempotency_key
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
                "UPDATE quant_feedback_cycle
                 SET champion_model_spec_definition_hash = CASE
                     WHEN champion_model_spec_definition_hash = $1 THEN $2
                     ELSE $1
                 END
                 WHERE feedback_cycle_id = $3",
                [
                    content_hash('9').to_string().into(),
                    content_hash('8').to_string().into(),
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
        let missing_cycle_id = FeedbackCycleId::from_idempotency_hash(&content_hash('3'));
        assert_db_rejection(
            self.evaluation_use(
                missing_cycle_id,
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

struct RecipePlanFixture {
    params: CandidateRecipePlanJobParams,
    artifact: CandidateRecipePlanArtifact,
}

impl RecipePlanFixture {
    async fn prepare(
        cycles: &PgFeedbackCycleRepository,
        cycle: &FeedbackCycleInfo,
        family: &FeedbackCandidateFamily,
    ) -> Self {
        let attribution = FeedbackAttributionManifestRef {
            job_id: ResearchJobId::from_v7(),
            artifact: ResearchJobArtifactRef {
                uri: ArtifactUri::parse("s3://feedback-fixture/attribution.json")
                    .expect("valid attribution fixture URI"),
                content_hash: content_hash('1'),
            },
            use_set_hash: content_hash('2'),
            produced_set_hash: content_hash('3'),
        };
        let drift = FeedbackRecipeDriftManifest {
            job_id: ResearchJobId::from_v7(),
            artifact: ResearchJobArtifactRef {
                uri: ArtifactUri::parse("s3://feedback-fixture/drift.json")
                    .expect("valid drift fixture URI"),
                content_hash: content_hash('4'),
            },
            exceeded_metrics: vec![FeedbackDriftMetric::PopulationStabilityIndex],
        };
        let params = CandidateRecipePlanJobParams::try_new(CandidateRecipePlanInput {
            feedback_cycle_id: cycle.feedback_cycle_id,
            cycle_idempotency_hash: cycle.idempotency_hash,
            label_cutoff: cycle.label_cutoff,
            planned_at: cycles
                .database_time()
                .await
                .expect("read RecipePlan fixture database clock"),
            evaluation_mode: cycle.evaluation_mode,
            attribution,
            drift,
            max_challengers: 1,
        })
        .expect("valid RecipePlan fixture params");
        let recipe = family
            .candidates()
            .first()
            .expect("RecipePlan fixture has one challenger")
            .candidate_recipe_hash();
        let template = fixture_recipe_template(family.shared_evaluation());
        let diagnostic_evidence = fixture_recipe_diagnostics(family.shared_evaluation());
        let selection = CandidateRecipeSelection::try_new(
            template,
            recipe,
            params.attribution.use_set_hash,
            vec![FeedbackDriftMetric::PopulationStabilityIndex],
            diagnostic_evidence,
            None,
        )
        .expect("valid RecipePlan fixture selection");
        let artifact = CandidateRecipePlanArtifact {
            format_version: CandidateRecipePlanArtifact::FORMAT_VERSION,
            artifact_id: params.artifact_id,
            feedback_cycle_id: cycle.feedback_cycle_id,
            cycle_idempotency_hash: cycle.idempotency_hash,
            input_hash: params.input_hash().expect("RecipePlan fixture input hash"),
            label_cutoff: cycle.label_cutoff,
            planned_at: params.planned_at,
            evaluation_mode: cycle.evaluation_mode,
            profile_ref: cycle.profile_ref.clone(),
            route: cycle.route,
            model_family: cycle.champion_model_family,
            attribution: params.attribution.clone(),
            drift: params.drift.clone(),
            outcome: CandidateRecipePlanOutcome::Ready {
                candidate_family: Box::new(family.clone()),
                selections: vec![selection],
            },
        };
        artifact
            .validate()
            .expect("valid RecipePlan fixture artifact");
        Self { params, artifact }
    }

    async fn persist(&self, store: &Arc<dyn ArtifactStore>) -> ResearchJobArtifactRef {
        let bytes = CandidateRecipePlanCodec::encode(&self.artifact)
            .expect("encode RecipePlan fixture artifact");
        let artifact_hash = CanonicalDigest::content_hash_bytes(&bytes);
        let key = ArtifactKey::new(
            ArtifactNamespace::FeedbackRecipePlan,
            artifact_hash.hex(),
            "json",
        )
        .expect("RecipePlan fixture artifact key");
        let uri = store
            .put(key, &bytes)
            .await
            .expect("persist RecipePlan fixture artifact");
        assert_eq!(
            store
                .get(&uri)
                .await
                .expect("read RecipePlan fixture artifact"),
            bytes
        );
        ResearchJobArtifactRef {
            uri,
            content_hash: artifact_hash,
        }
    }

    async fn record(
        self,
        cycles: &PgFeedbackCycleRepository,
        jobs: &PgResearchJobRepository,
        lease: FeedbackCycleLeaseGuard,
        cycle: &FeedbackCycleInfo,
        event_sequence: i64,
        artifact_ref: ResearchJobArtifactRef,
    ) {
        let params = self.params;
        let identity =
            FeedbackStageJobIdentity::try_root(cycle.feedback_cycle_id, FeedbackStage::RecipePlan)
                .expect("RecipePlan fixture root identity");
        let job = NewResearchJob {
            job_id: identity.job_id(),
            feedback_cycle_id: None,
            feedback_stage: None,
            kind: ResearchJobKind::FeedbackRecipePlan,
            status: ResearchJobStatus::Queued,
            model_spec_id: None,
            decision_policy_snapshot_id: None,
            params_json: ResearchJobParams::FeedbackRecipePlan(Box::new(params.clone())),
            requested_by: None,
            acting_role: RoleCode::new("system"),
            parent_job_id: None,
            recovery_attempt: 0,
            max_recovery_attempts: 3,
        }
        .try_bind_feedback(identity)
        .expect("bind RecipePlan fixture job");
        match jobs
            .enqueue(job)
            .await
            .expect("enqueue RecipePlan fixture job")
        {
            ResearchJobEnqueueOutcome::Inserted(_)
            | ResearchJobEnqueueOutcome::AlreadyPresent(_) => {}
        }
        let worker = WorkerId::from_v7();
        let leased = jobs
            .lease_next(
                &[ResearchJobKind::FeedbackRecipePlan],
                &worker,
                Utc::now() + Duration::seconds(90),
            )
            .await
            .expect("lease RecipePlan fixture job")
            .expect("queued RecipePlan fixture job");
        assert_eq!(leased.job_id, identity.job_id());
        let info = jobs
            .finalize(
                &identity.job_id(),
                &worker,
                ResearchJobFinalization::succeeded(
                    Some(ResearchJobResultRef {
                        kind: ResearchJobResultKind::CandidateRecipePlanArtifact,
                        id: params.artifact_id.as_uuid(),
                    }),
                    Some(artifact_ref.clone()),
                    None,
                ),
            )
            .await
            .expect("finalize RecipePlan fixture job");
        cycles
            .append_stage(
                lease,
                NewFeedbackStageEvent::try_seal(FeedbackStageEventInput {
                    feedback_cycle_id: cycle.feedback_cycle_id,
                    event_sequence,
                    stage: FeedbackStage::RecipePlan,
                    event_kind: FeedbackStageEventKind::Succeeded,
                    trigger_family: None,
                    research_job_id: Some(identity.job_id()),
                    actor: None,
                    reason_code: None,
                    evidence_uri: Some(artifact_ref.uri),
                    evidence_hash: Some(artifact_ref.content_hash),
                    occurred_at: info
                        .finished_at
                        .expect("RecipePlan fixture terminal timestamp"),
                })
                .expect("seal RecipePlan fixture success event"),
            )
            .await
            .expect("append RecipePlan fixture success event");
    }
}

pub async fn persist_recipe_plan_fixture(
    cycles: &PgFeedbackCycleRepository,
    jobs: &PgResearchJobRepository,
    store: &Arc<dyn ArtifactStore>,
    lease: FeedbackCycleLeaseGuard,
    cycle: &FeedbackCycleInfo,
    family: &FeedbackCandidateFamily,
    event_sequence: i64,
) {
    let fixture = RecipePlanFixture::prepare(cycles, cycle, family).await;
    let artifact_ref = fixture.persist(store).await;
    fixture
        .record(cycles, jobs, lease, cycle, event_sequence, artifact_ref)
        .await;
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
        panic!(
            "database unexpectedly accepted feedback-schema drift expected to fail with {expected:?}"
        );
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

fn fixture_recipe_template(
    shared_evaluation: &FeedbackDatasetBuildRequest,
) -> FeedbackRecipeTemplate {
    let profile = shared_evaluation
        .window
        .profile_ref()
        .resolve_builtin_research_profile()
        .expect("fixture profile resolves from its exact evaluation window");
    FeedbackRecipeTemplate::try_seal(FeedbackRecipeTemplateInput {
        recipe_template_id: FeedbackRecipeTemplateId::new(Uuid::from_u128(0x501)),
        revision: 1,
        profile_ref: shared_evaluation.window.profile_ref().clone(),
        route: BuyModelRoute::try_from(profile.spec.category)
            .expect("fixture profile owns a canonical Buy route"),
        model_family: ModelFamily::WeightedFactor,
        training_spec: FeedbackRecipeTrainingSpec::try_new(
            shared_evaluation.model_spec_id,
            shared_evaluation.model_spec_definition_hash,
            ModelInputContract::single_required("fixture_feature"),
            ModelTrainingContract::outcome_default(),
            1,
        )
        .expect("fixture training spec"),
        calibration_spec: FeedbackRecipeCalibrationSpec::try_new(CalibrationMethod::Platt, 1)
            .expect("fixture calibration spec"),
        cpcv_spec: FeedbackRecipeCpcvSpec::try_new(
            ResearchValidationConfig::default(),
            profile.spec.target_horizon_secs,
            profile.spec.purge_embargo_secs,
        )
        .expect("fixture CPCV spec"),
        downside_spec: FeedbackRecipeDownsideSpec::try_new(DownsideSource::MfeMae)
            .expect("fixture downside spec"),
        diagnostic_spec: FeedbackRecipeDiagnosticSpec {
            accepted_artifact_kinds: vec![AttributionArtifactKind::PredictionExplanation],
            responsive_feature_names: vec!["fixture_feature".to_owned()],
            minimum_evidence_count: 1,
            minimum_feature_matches: 1,
        },
        responsive_triggers: vec![FeedbackDriftMetric::PopulationStabilityIndex],
        catalog_priority: 0,
        resource_budget: FeedbackRecipeResourceBudget {
            max_concurrency: 1,
            max_working_set_bytes: 10 * 1024 * 1024 * 1024,
            max_resident_model_bytes: 16 * 1024 * 1024,
            deadline_secs: 60,
        },
        status: FeedbackRecipeTemplateStatus::Approved,
        approved_by_user_id: Some(UserId::new(Uuid::from_u128(0x502))),
        approved_by_role: Some(RoleCode::new("system_fixture")),
        approved_at: Some(shared_evaluation.window.window_start()),
        governance_reason: "immutable feedback schema fixture".to_owned(),
    })
    .expect("seal fixture recipe template")
}

fn fixture_recipe_diagnostics(
    shared_evaluation: &FeedbackDatasetBuildRequest,
) -> Vec<FeedbackRecipeDiagnosticEvidence> {
    vec![FeedbackRecipeDiagnosticEvidence {
        source_feedback_cycle_id: FeedbackCycleId::new(Uuid::from_u128(0x503)),
        artifact_kind: AttributionArtifactKind::PredictionExplanation,
        source_cohort: AttributionCohort::Training,
        artifact_hash: content_hash('7'),
        available_at: shared_evaluation.window.window_start(),
        matched_feature_names: vec!["fixture_feature".to_owned()],
    }]
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
    let template = fixture_recipe_template(&shared_evaluation);
    let diagnostic_evidence = fixture_recipe_diagnostics(&shared_evaluation);
    let planner_evidence_hash = CandidateRecipeSelection::planner_evidence_hash(
        template.template_hash,
        content_hash('2'),
        &[FeedbackDriftMetric::PopulationStabilityIndex],
        &diagnostic_evidence,
        &None,
    )
    .expect("derive fixture planner evidence hash");
    let recipe = FeedbackCandidateRecipe::try_seal(FeedbackCandidateRecipeInput {
        recipe_template_hash: template.template_hash,
        planner_evidence_hash,
        resource_budget: template.resource_budget,
        training: request(
            DatasetPurpose::Training,
            evaluation_start - Duration::hours(8),
            evaluation_start - Duration::hours(6),
        ),
        calibration: request(
            DatasetPurpose::Calibration,
            evaluation_start - Duration::hours(5),
            evaluation_start - Duration::hours(3),
        ),
        calibration_method: CalibrationMethod::Platt,
        cpcv_spec: template.cpcv_spec,
        downside_source: DownsideSource::MfeMae,
        decision_policy_snapshot_id: policy_id,
    })
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
    second_label_cutoff: DateTime<Utc>,
    champion_model_version_id: ModelVersionId,
    champion_serving_contract_hash: ContentHash,
    model_spec_id: ModelSpecId,
    model_spec_definition_hash: ContentHash,
    model_family: ModelFamily,
    route: BuyModelRoute,
    decision_policy_snapshot_id: DecisionPolicySnapshotId,
    decision_policy_snapshot_hash: ContentHash,
}

impl FeedbackCyclePairSeed {
    fn seal(
        self,
        first_family: &FeedbackCandidateFamily,
        second_family: &FeedbackCandidateFamily,
    ) -> FeedbackCyclePair {
        let first_family_hash = first_family.candidate_family_hash();
        let second_family_hash = second_family.candidate_family_hash();
        let first_comparison_hash = first_family.comparison_contract_hash();
        let second_comparison_hash = second_family.comparison_contract_hash();
        let seal = |label_cutoff| {
            NewFeedbackCycle::try_seal(
                FeedbackCycleKey::try_new(FeedbackCycleKeyInput {
                    profile_ref: self.profile_ref.clone(),
                    feedback_policy_hash: self.feedback_policy_hash,
                    label_cutoff,
                    champion_model_version_id: self.champion_model_version_id,
                    champion_serving_contract_hash: self.champion_serving_contract_hash,
                    champion_model_spec_id: self.model_spec_id,
                    champion_model_spec_definition_hash: self.model_spec_definition_hash,
                    champion_model_family: self.model_family,
                    route: self.route,
                    decision_policy_snapshot_id: self.decision_policy_snapshot_id,
                    decision_policy_snapshot_hash: self.decision_policy_snapshot_hash,
                    policy_bundle_generation: PolicyBundleGeneration::FIRST,
                    route_generation: 1,
                    evaluation_mode: FeedbackEvaluationMode::Conditional,
                    parent_cycle_id: None,
                    forced_idempotency_key: None,
                })
                .expect("freeze feedback-cycle identity"),
            )
            .expect("seal feedback cycle")
        };
        FeedbackCyclePair {
            first: seal(self.label_cutoff),
            second: seal(self.second_label_cutoff),
            first_family_hash,
            second_family_hash,
            first_comparison_hash,
            second_comparison_hash,
        }
    }
}

struct FeedbackChampionFixture {
    model_spec_id: ModelSpecId,
    model_version: ModelVersionInfo,
    training_dataset: TrainingDatasetInfo,
}

struct FeedbackWindows {
    cadence_cutoff: DateTime<Utc>,
    evaluation_window_start: DateTime<Utc>,
    evaluation_window_end: DateTime<Utc>,
    second_evaluation_window_start: DateTime<Utc>,
    second_evaluation_window_end: DateTime<Utc>,
    observed_at: DateTime<Utc>,
}

impl FeedbackWindows {
    fn resolve(
        profile_ref: &ResearchProfileRef,
        prediction_horizon_secs: u64,
        now: DateTime<Utc>,
    ) -> Self {
        let profile = profile_ref
            .resolve_builtin_research_profile()
            .expect("resolve feedback-schema cadence profile");
        let cadence_secs = i64::try_from(profile.spec.feedback_policy.feedback_cadence_secs)
            .expect("feedback cadence fits chrono duration");
        let feedback_cadence = Duration::seconds(cadence_secs);
        let cadence_cutoff =
            DateTime::from_timestamp(now.timestamp().div_euclid(cadence_secs) * cadence_secs, 0)
                .expect("feedback cadence cutoff fits chrono");
        let horizon = Duration::seconds(
            i64::try_from(prediction_horizon_secs)
                .expect("prediction horizon fits chrono duration"),
        );
        let evaluation_window_end = cadence_cutoff - horizon;
        let evaluation_window_start = evaluation_window_end - Duration::hours(1);
        Self {
            cadence_cutoff,
            evaluation_window_start,
            evaluation_window_end,
            second_evaluation_window_start: evaluation_window_start - feedback_cadence,
            second_evaluation_window_end: evaluation_window_end - feedback_cadence,
            observed_at: now - Duration::milliseconds(1),
        }
    }
}

async fn prepare_champion_fixture(
    db: &DatabaseConnection,
    expected_profile_ref: ResearchProfileRef,
    prediction_horizon_secs: i64,
) -> FeedbackChampionFixture {
    bootstrap_default_policy_bundle(db, "pg-feedback-schema", "feedback boot-schema contract")
        .await;
    let registry = PgModelRegistryRepository::new(db.clone());
    let model_spec_id = ModelSpecId::from_v7();
    registry
        .create_model_spec(model_spec_fixtures::new_model_spec_fixture(
            model_spec_id,
            "pg-feedback-schema",
            ModelFamily::WeightedFactor,
            prediction_horizon_secs,
            ModelInputContract::single_required("book.mid"),
            ModelTrainingContract::outcome_default(),
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
    assert_eq!(
        model_version.profile_ref, expected_profile_ref,
        "feedback-schema model must bind the requested exact ResearchProfile"
    );
    let training_dataset_id = model_version
        .training_dataset_id
        .expect("feedback-schema model has Training Dataset");
    let training_dataset = PgTrainingDatasetRepository::new(db.clone())
        .find_by_id(&training_dataset_id)
        .await
        .expect("load feedback-schema Training Dataset")
        .expect("feedback-schema Training Dataset exists");
    FeedbackChampionFixture {
        model_spec_id,
        model_version,
        training_dataset,
    }
}

fn evaluation_request(
    dataset: &TrainingDatasetInfo,
    profile_ref: &ResearchProfileRef,
) -> FeedbackDatasetBuildRequest {
    FeedbackDatasetBuildRequest {
        training_dataset_id: dataset.training_dataset_id,
        model_spec_id: dataset.model_spec_id,
        model_spec_definition_hash: dataset.model_spec_definition_hash,
        source_lineage: dataset.source_lineage.clone(),
        window: FeedbackCohortWindow::try_new(
            profile_ref.clone(),
            dataset.window_start,
            dataset.window_end,
        )
        .expect("freeze Evaluation Dataset window"),
        purpose: DatasetPurpose::Evaluation,
    }
}

pub async fn prepare_profile_fixture(
    db: &DatabaseConnection,
    expected_profile_ref: ResearchProfileRef,
    prediction_horizon_secs: i64,
) -> FeedbackSchemaFixture {
    let FeedbackChampionFixture {
        model_spec_id,
        model_version,
        training_dataset,
    } = prepare_champion_fixture(db, expected_profile_ref, prediction_horizon_secs).await;
    let training = training_dataset
        .materialization()
        .expect("feedback-schema Training Dataset materialization");
    let bindings = model_version
        .verified_serving_contract()
        .expect("verify feedback-schema serving contract")
        .bindings();
    let now = db_clock(db).await;
    let FeedbackWindows {
        cadence_cutoff,
        evaluation_window_start,
        evaluation_window_end,
        second_evaluation_window_start,
        second_evaluation_window_end,
        observed_at,
    } = FeedbackWindows::resolve(
        &model_version.profile_ref,
        bindings.model.prediction_horizon_secs,
        now,
    );
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
    let second_evaluation_dataset = ModelDatasetLedgerFixture::persist(
        db,
        &ModelDatasetLedgerFixture::local_store(),
        ModelDatasetLedgerSeed {
            scope: "pg-feedback-schema:previous-evaluation".to_owned(),
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
            window_start: second_evaluation_window_start,
            window_end: second_evaluation_window_end,
            research_program_hash: training_dataset.source_lineage.research_program_hash,
            sample_count: 10,
            decision_interval_secs: 1,
            trade_policy: bindings.trade_policy.clone(),
        },
    )
    .await
    .expect("persist previous feedback-schema Evaluation Dataset");
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
    assert_eq!(label_cutoff, cadence_cutoff);
    let second_label_cutoff = second_evaluation_dataset.source_lineage.pit_cutoff;
    let (feedback_policy_hash, comparison_contract) = feedback_method(&model_version.profile_ref);
    let shared_evaluation = evaluation_request(&evaluation_dataset, &model_version.profile_ref);
    let second_shared_evaluation =
        evaluation_request(&second_evaluation_dataset, &model_version.profile_ref);
    let candidate_family = build_candidate_family(shared_evaluation, comparison_contract.clone());
    let second_candidate_family =
        build_candidate_family(second_shared_evaluation, comparison_contract);
    let retained_candidate_family = candidate_family.clone();
    let retained_second_candidate_family = second_candidate_family.clone();
    let pair = FeedbackCyclePairSeed {
        profile_ref: model_version.profile_ref.clone(),
        feedback_policy_hash,
        label_cutoff,
        second_label_cutoff,
        champion_model_version_id: model_version.model_version_id,
        champion_serving_contract_hash: model_version.serving_contract_hash,
        model_spec_id,
        model_spec_definition_hash: model_version.model_spec_definition_hash,
        model_family: model_version.model_family,
        route: BuyModelRoute::try_from(model_version.category_scope)
            .expect("feedback-schema model route"),
        decision_policy_snapshot_id: bindings.policy_snapshot.decision_policy_snapshot_id,
        decision_policy_snapshot_hash: bindings.policy_snapshot.snapshot_hash,
    }
    .seal(&candidate_family, &second_candidate_family);
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
        second_candidate_family: retained_second_candidate_family,
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
        second_evaluation_window_start,
        second_evaluation_window_end,
        label_cutoff,
        second_label_cutoff,
        observed_at,
    }
}

pub async fn prepare_fixture(db: &DatabaseConnection) -> FeedbackSchemaFixture {
    Box::pin(prepare_profile_fixture(
        db,
        model_spec_fixtures::pooled_profile_ref(),
        model_spec_fixtures::pooled_horizon_secs(),
    ))
    .await
}

fn isolated_cycle(
    fixture: &FeedbackSchemaFixture,
    profile_ref: ResearchProfileRef,
    route: BuyModelRoute,
    model_family: ModelFamily,
    label_cutoff: DateTime<Utc>,
) -> NewFeedbackCycle {
    let evaluation = fixture.candidate_family.shared_evaluation();
    let feedback_policy_hash = profile_ref
        .resolve_builtin_research_profile()
        .expect("resolve isolated feedback profile")
        .spec
        .feedback_policy
        .content_hash()
        .expect("hash isolated feedback policy");
    NewFeedbackCycle::try_seal(
        FeedbackCycleKey::try_new(FeedbackCycleKeyInput {
            profile_ref,
            feedback_policy_hash,
            label_cutoff,
            champion_model_version_id: fixture.champion_model_version_id,
            champion_serving_contract_hash: fixture.champion_serving_contract_hash,
            champion_model_spec_id: evaluation.model_spec_id,
            champion_model_spec_definition_hash: evaluation.model_spec_definition_hash,
            champion_model_family: model_family,
            route,
            decision_policy_snapshot_id: evaluation.source_lineage.decision_policy_snapshot_id,
            decision_policy_snapshot_hash: evaluation.source_lineage.runtime_config_hash,
            policy_bundle_generation: PolicyBundleGeneration::FIRST,
            route_generation: 1,
            evaluation_mode: FeedbackEvaluationMode::Conditional,
            parent_cycle_id: None,
            forced_idempotency_key: None,
        })
        .expect("freeze isolated feedback-cycle identity"),
    )
    .expect("seal isolated feedback cycle")
}

async fn insert_attribution(
    db: &DatabaseConnection,
    fixture: &FeedbackSchemaFixture,
    source_cycle_id: FeedbackCycleId,
    source_cutoff: DateTime<Utc>,
    available_at: DateTime<Utc>,
    seed: char,
) -> ContentHash {
    let artifact_hash = content_hash(seed);
    let artifact = NewAttributionArtifact::try_new(
        AttributionCohort::Evaluation,
        source_cycle_id,
        AttributionSubject::ResolutionOutcome {
            model_version_id: fixture.champion_model_version_id,
        },
        ArtifactUri::parse(format!("s3://fixture/attribution/{seed}.json"))
            .expect("valid attribution fixture URI"),
        artifact_hash,
        source_cutoff,
    )
    .expect("seal attribution PIT fixture");
    let mut active = artifact.into_active_model();
    active.available_at = Set(available_at);
    active.created_at = Set(available_at);
    AttributionArtifactEntity::insert(active)
        .exec_without_returning(db)
        .await
        .expect("insert attribution PIT fixture");
    artifact_hash
}

fn catalog_template(
    base: &FeedbackRecipeTemplate,
    revision: u32,
    status: FeedbackRecipeTemplateStatus,
    catalog_priority: i32,
    approved_by_user_id: UserId,
    governance_reason: &str,
) -> FeedbackRecipeTemplate {
    FeedbackRecipeTemplate::try_seal(FeedbackRecipeTemplateInput {
        recipe_template_id: base.recipe_template_id,
        revision,
        profile_ref: base.profile_ref.clone(),
        route: base.route,
        model_family: base.model_family,
        training_spec: base.training_spec.clone(),
        calibration_spec: base.calibration_spec.clone(),
        cpcv_spec: base.cpcv_spec.clone(),
        downside_spec: base.downside_spec.clone(),
        diagnostic_spec: base.diagnostic_spec.clone(),
        responsive_triggers: base.responsive_triggers.clone(),
        catalog_priority,
        resource_budget: base.resource_budget,
        status,
        approved_by_user_id: Some(approved_by_user_id),
        approved_by_role: Some(RoleCode::new("research_approver")),
        approved_at: Some(base.approved_at.expect("fixture approval timestamp")),
        governance_reason: governance_reason.to_owned(),
    })
    .expect("seal catalog revision fixture")
}

pub async fn attribution_pit_contracts() {
    let (pool, _container) = setup_pg().await;
    let db = pool.connection().clone();
    let fixture = Box::pin(prepare_fixture(&db)).await;
    fixture.insert_cycles(&db).await;

    let route_cycle = isolated_cycle(
        &fixture,
        fixture.profile_ref.clone(),
        BuyModelRoute::Crypto,
        ModelFamily::WeightedFactor,
        fixture.second_label_cutoff - Duration::hours(1),
    );
    let family_cycle = isolated_cycle(
        &fixture,
        fixture.profile_ref.clone(),
        BuyModelRoute::Pooled,
        ModelFamily::ClassicalRidge,
        fixture.second_label_cutoff - Duration::hours(2),
    );
    let profile_cycle = isolated_cycle(
        &fixture,
        model_spec_fixtures::crypto_profile_ref(),
        BuyModelRoute::Crypto,
        ModelFamily::WeightedFactor,
        fixture.second_label_cutoff - Duration::hours(3),
    );
    for cycle in [&route_cycle, &family_cycle, &profile_cycle] {
        cycle
            .clone()
            .into_active_model()
            .insert(&db)
            .await
            .expect("insert isolated attribution source cycle");
    }

    let valid_hash = insert_attribution(
        &db,
        &fixture,
        fixture.second_cycle_id,
        fixture.second_label_cutoff,
        fixture.label_cutoff,
        '1',
    )
    .await;
    insert_attribution(
        &db,
        &fixture,
        fixture.cycle_id,
        fixture.label_cutoff,
        fixture.label_cutoff,
        '2',
    )
    .await;
    insert_attribution(
        &db,
        &fixture,
        fixture.second_cycle_id,
        fixture.second_label_cutoff,
        fixture.label_cutoff + Duration::seconds(1),
        '3',
    )
    .await;
    for (cycle, seed) in [
        (&route_cycle, '4'),
        (&family_cycle, '5'),
        (&profile_cycle, '6'),
    ] {
        insert_attribution(
            &db,
            &fixture,
            cycle.feedback_cycle_id(),
            cycle.label_cutoff(),
            fixture.label_cutoff,
            seed,
        )
        .await;
    }

    let repository = PgAttributionArtifactRepository::new(db.clone());
    let available = repository
        .list_available(fixture.cycle_id, fixture.label_cutoff)
        .await
        .expect("list exact N-to-N+1 attribution evidence");
    assert_eq!(available.len(), 1);
    assert_eq!(available[0].artifact_hash, valid_hash);
    assert_eq!(
        available[0].source_feedback_cycle_id,
        fixture.second_cycle_id
    );
    assert!(
        repository
            .list_available(fixture.cycle_id, fixture.second_label_cutoff)
            .await
            .expect("list evidence at predecessor cutoff")
            .is_empty(),
        "same-cutoff or future evidence must not enter the planner"
    );
}

pub async fn recipe_catalog_contracts() {
    let (pool, _container) = setup_pg().await;
    let db = pool.connection().clone();
    let fixture = Box::pin(prepare_fixture(&db)).await;
    let user = db
        .query_one_raw(Statement::from_string(
            DbBackend::Postgres,
            "SELECT id FROM \"user\" ORDER BY id LIMIT 1",
        ))
        .await
        .expect("query recipe approver")
        .expect("seeded recipe approver");
    let approved_by_user_id =
        UserId::new(Uuid::try_get(&user, "", "id").expect("decode recipe approver identifier"));
    let base = fixture_recipe_template(fixture.candidate_family.shared_evaluation());
    let repository = PgFeedbackRecipeTemplateRepository::new(db);
    let revision_one = catalog_template(
        &base,
        1,
        FeedbackRecipeTemplateStatus::Approved,
        20,
        approved_by_user_id,
        "approve initial governed challenger",
    );
    assert_eq!(
        repository
            .insert(revision_one.clone())
            .await
            .expect("insert first recipe revision"),
        FeedbackRecipeTemplateWriteOutcome::Inserted
    );
    assert_eq!(
        repository
            .insert(revision_one)
            .await
            .expect("replay first recipe revision"),
        FeedbackRecipeTemplateWriteOutcome::ExactReplay
    );

    let revision_two = catalog_template(
        &base,
        2,
        FeedbackRecipeTemplateStatus::Retired,
        10,
        approved_by_user_id,
        "retire challenger after governed review",
    );
    repository
        .insert(revision_two)
        .await
        .expect("insert retired recipe revision");
    assert!(
        repository
            .list_approved(&base.profile_ref, base.route, base.model_family)
            .await
            .expect("list catalog after retirement")
            .is_empty(),
        "latest retired revision must remove the template from the approved catalog"
    );

    let revision_three = catalog_template(
        &base,
        3,
        FeedbackRecipeTemplateStatus::Approved,
        -5,
        approved_by_user_id,
        "approve revised bounded challenger",
    );
    repository
        .insert(revision_three.clone())
        .await
        .expect("insert re-approved recipe revision");
    let approved = repository
        .list_approved(&base.profile_ref, base.route, base.model_family)
        .await
        .expect("list exact approved catalog");
    assert_eq!(approved, vec![revision_three.clone()]);
    assert!(
        repository
            .list_approved(&base.profile_ref, BuyModelRoute::Crypto, base.model_family)
            .await
            .expect("list route-isolated catalog")
            .is_empty()
    );
    assert!(
        repository
            .list_approved(&base.profile_ref, base.route, ModelFamily::ClassicalRidge,)
            .await
            .expect("list family-isolated catalog")
            .is_empty()
    );
    assert!(
        repository
            .list_approved(
                &model_spec_fixtures::crypto_profile_ref(),
                base.route,
                base.model_family,
            )
            .await
            .expect("list profile-isolated catalog")
            .is_empty()
    );

    let drifted_replay = catalog_template(
        &base,
        3,
        FeedbackRecipeTemplateStatus::Approved,
        -5,
        approved_by_user_id,
        "attempt semantic drift on an immutable revision",
    );
    let error = repository
        .insert(drifted_replay)
        .await
        .expect_err("same recipe revision cannot drift");
    assert!(error.to_string().contains("semantic drift"));
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
