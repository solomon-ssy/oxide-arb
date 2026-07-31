//! First-champion route bootstrap contracts against real `PostgreSQL`.

use std::sync::Arc;

use chrono::{DateTime, Duration, Utc};
use quant_pivot_models::{
    domain::quant::{
        BootstrapModelRoute, CandidateExplanationValidation, CommitModelRouteBootstrap,
        ModelBootstrapManifest, ModelBootstrapManifestInput, ModelBootstrapPolicyProjection,
        ModelGovernanceAuditDetail, ModelRouteBootstrapPreflight,
        ModelRouteBootstrapPreflightInput, NewBacktestPathSet, NewBacktestPathSetInput,
        NewBacktestReport, NewModelRun, PromotionPermitActor,
    },
    entities::{
        decision_policy_snapshot::Entity as SnapshotEntity,
        policy_activation::Entity as ActivationEntity,
        policy_activation_audit::Entity as ActivationAuditEntity,
        policy_activation_event_outbox::{
            Entity as ActivationOutboxEntity, Model as ActivationOutboxModel,
        },
        policy_approval::Entity as ApprovalEntity,
        policy_revision::Entity as RevisionEntity,
        quant_model_governance_audit::Entity as ModelAuditEntity,
        user::{Column as UserColumn, Entity as UserEntity},
    },
    enums::{
        quant::{DatasetPurpose, FeatureParityLatchState, ModelRunKind, QuantRuntimeMode},
        runtime_config::ConfigResourceKind,
    },
    runtime_config::{ActivePolicyBundle, BuyModelRoute},
    types::{
        BacktestPathSetId, BacktestReportId, ContentHash, ModelRunId, ModelVersionId,
        PolicyIdempotencyKey, Probability, RoleCode,
        backtest::{
            BacktestPath, CategoryMetrics, CpcvFoldArtifact, CpcvFoldArtifacts,
            CpcvFoldCalibrationPolicy, CpcvFoldRole, CpcvMethodologyBinding, CpcvPathSetSubject,
            ExpectedVsRealized, PnlSimulation, SharpeDistribution,
        },
        model_lineage::ModelVersionDerivation,
        model_quality::{
            GateClass, GateId, GateIntent, GateOutcome, GateStatus, GateSubject, QualityGateReport,
            QualityGateReportInput,
        },
    },
};
use quant_pivot_repository::{
    postgres::{
        PgBacktestPathSetRepository, PgBacktestReportRepository, PgFeatureParityRepository,
        PgFeedbackCycleRepository, PgModelRegistryRepository, PgModelRouteBootstrapRepository,
        PgModelRunRepository, PgPolicyRepository, PgRuntimeControlRepository,
        PgTrainingDatasetRepository,
    },
    traits::{
        BacktestPathSetRepository, BacktestReportRepository, CpcvPathSetCommit,
        FeatureParityRepository, FeedbackCycleRepository, ModelRegistryRepository,
        ModelRouteBootstrapCommit, ModelRouteBootstrapOutcome, ModelRouteBootstrapRepository,
        ModelRunRepository, PolicyRepository, RuntimeControlRepository, TrainingDatasetRepository,
    },
};
use quant_pivot_system_tests::{
    postgres::{ScenarioDatabase, setup_pg},
    support::{
        execution_pg_seed::seed_demo_with_store,
        model_serving_fixtures::{ModelDatasetLedgerFixture, ModelDatasetLedgerSeed},
    },
};
use rust_decimal_macros::dec;
use sea_orm::{
    ColumnTrait, ConnectionTrait, DatabaseConnection, EntityTrait, PaginatorTrait, QueryFilter,
};

use quant_pivot_research::{artifact::ArtifactStore, hashing::ResearchHasher};

fn hash(seed: char) -> ContentHash {
    ContentHash::parse(&format!("blake3:{}", seed.to_string().repeat(64)))
        .expect("bootstrap fixture hash")
}

fn quality_report(model_id: ModelVersionId, evaluated_at: DateTime<Utc>) -> QualityGateReport {
    let pass = |gate, class| GateOutcome {
        gate,
        class,
        status: GateStatus::Pass,
        observed: "bootstrap_fixture_pass".to_owned(),
        threshold: "governed".to_owned(),
        detail: "first-champion fixture carries a complete passing scorecard".to_owned(),
    };
    let not_applicable = |gate, class| GateOutcome {
        gate,
        class,
        status: GateStatus::NotApplicable,
        observed: "n/a".to_owned(),
        threshold: "n/a".to_owned(),
        detail: "gate does not apply to Candidate intent".to_owned(),
    };
    QualityGateReport::try_new(QualityGateReportInput {
        subject: GateSubject::ModelVersion(model_id),
        intent: GateIntent::Candidate,
        evaluated_at,
        gates: vec![
            pass(GateId::SampleCount, GateClass::Hard),
            pass(GateId::LabelCoverage, GateClass::Hard),
            pass(GateId::MaterializationCoverage, GateClass::Hard),
            pass(GateId::NoPitLeakage, GateClass::Hard),
            pass(GateId::BacktestRequired, GateClass::Hard),
            pass(GateId::MaxDrawdown, GateClass::Hard),
            pass(GateId::TurnoverBudget, GateClass::Hard),
            pass(GateId::TailLossBudget, GateClass::Hard),
            pass(GateId::HitRate, GateClass::Soft),
            pass(GateId::CategoryConcentration, GateClass::Soft),
            pass(GateId::CpcvRequired, GateClass::Hard),
            pass(GateId::RankIc, GateClass::Hard),
            pass(GateId::DeflatedSharpe, GateClass::Hard),
            pass(GateId::Pbo, GateClass::Hard),
            pass(GateId::MinTrackRecordLength, GateClass::Soft),
            not_applicable(GateId::LiquidityExitFeasible, GateClass::Hard),
            not_applicable(GateId::ShadowOverlapStability, GateClass::Hard),
            not_applicable(GateId::CalibrationRequired, GateClass::Hard),
            pass(GateId::ExplainabilityRequired, GateClass::Hard),
        ],
    })
    .expect("seal bootstrap Candidate quality report")
}

struct ValidationEvidence {
    path_set_id: BacktestPathSetId,
    path_set_hash: ContentHash,
    backtest_report_id: BacktestReportId,
    backtest_report_hash: ContentHash,
}

async fn seed_path_set(
    db: &DatabaseConnection,
    model_id: ModelVersionId,
) -> (BacktestPathSetId, ContentHash) {
    let models = PgModelRegistryRepository::new(db.clone());
    let model = models
        .find_model_version(&model_id)
        .await
        .expect("load bootstrap candidate")
        .expect("bootstrap candidate exists");
    let bindings = model
        .verified_serving_contract()
        .expect("verify bootstrap serving contract")
        .bindings();
    let training_dataset_id = model
        .training_dataset_id
        .expect("bootstrap candidate training dataset");
    let derivation = model
        .verified_derivation()
        .expect("verify bootstrap candidate derivation");
    let ModelVersionDerivation::ReturnCalibration {
        parent_model_version_id,
        calibration_artifact_id,
    } = derivation
    else {
        panic!("bootstrap fixture candidate must be a calibrated child");
    };
    let calibration = bindings
        .model
        .calibration
        .as_ref()
        .expect("bootstrap candidate calibration binding");
    let parent = models
        .find_model_version(&parent_model_version_id)
        .await
        .expect("load bootstrap calibration parent")
        .expect("bootstrap calibration parent exists");
    let model_run_id = ModelRunId::from_v7();
    let path_set_id = BacktestPathSetId::from_v7();
    let window_start = Utc::now() - Duration::days(3);
    let window_end = window_start + Duration::days(1);
    let input_hash = ResearchHasher::canonical(&("bootstrap-cpcv-input-v1", model_id, path_set_id))
        .expect("bootstrap CPCV input hash");
    PgModelRunRepository::new(db.clone())
        .start_exact(NewModelRun {
            model_run_id,
            run_kind: ModelRunKind::Cpcv,
            model_version_id: Some(model_id),
            decision_policy_snapshot_id: bindings.policy_snapshot.decision_policy_snapshot_id,
            market_selection_id: None,
            window_start,
            window_end,
            input_hash,
        })
        .await
        .expect("start bootstrap CPCV run");
    let path_set = NewBacktestPathSet::try_seal(NewBacktestPathSetInput {
        path_set_id,
        model_version_id: model_id,
        model_run_id,
        training_dataset_id,
        decision_policy_snapshot_id: bindings.policy_snapshot.decision_policy_snapshot_id,
        window_start,
        window_end,
        subject: CpcvPathSetSubject::new(
            model.artifact_hash,
            model.serving_contract_hash,
            bindings.transform.training_dataset_hash,
            bindings.dataset.manifest_hash,
            bindings.dataset.artifact_bytes_hash,
            bindings.policy_snapshot.snapshot_hash,
        ),
        methodology: CpcvMethodologyBinding::new(
            hash('1'),
            hash('2'),
            hash('3'),
            CpcvFoldCalibrationPolicy::CalibratedSubjectParentHeuristic {
                calibration_artifact_id,
                calibration_hash: calibration.content_hash,
                parent_model_version_id,
                parent_artifact_hash: parent.artifact_hash,
                parent_serving_contract_hash: parent.serving_contract_hash,
                parent_return_model_hash: hash('4'),
            },
        ),
        fold_artifacts: CpcvFoldArtifacts::try_new(vec![
            CpcvFoldArtifact {
                role: CpcvFoldRole::Validation,
                training_groups_hash: hash('5'),
                training_group_count: 24,
                model_artifact_hash: hash('6'),
                serving_contract_hash: hash('7'),
                model_payload_hash: hash('8'),
            },
            CpcvFoldArtifact {
                role: CpcvFoldRole::Trial { trial_id: 0 },
                training_groups_hash: hash('9'),
                training_group_count: 24,
                model_artifact_hash: hash('a'),
                serving_contract_hash: hash('b'),
                model_payload_hash: hash('c'),
            },
        ])
        .expect("bootstrap CPCV fold artifacts"),
        path_count: 1,
        combination_count: 1,
        median_rank_ic: dec!(0.25),
        sharpe_distribution: SharpeDistribution {
            min: dec!(0.8),
            p25: dec!(0.9),
            median: dec!(1.1),
            p75: dec!(1.2),
            max: dec!(1.4),
            median_max_drawdown: Some(dec!(0.05)),
            median_tail_loss: Some(dec!(-0.02)),
            baseline_uplift: Some(dec!(0.1)),
        },
        paths: vec![BacktestPath {
            path_index: 0,
            group_returns: vec![dec!(0.02), dec!(-0.005), dec!(0.03)],
            sharpe: dec!(1.1),
            rank_ic: dec!(0.25),
            max_drawdown: dec!(0.05),
            tail_loss: dec!(-0.02),
        }]
        .into(),
        deflated_sharpe: dec!(0.95),
        dsr_benchmark_sharpe: dec!(0.4),
        pbo: dec!(0.1),
        min_track_record_length_secs: Some(86_400),
        trial_count: 1,
        trial_grid_count: 1,
        coord_search_effective_n: 1,
    })
    .expect("seal bootstrap CPCV path set");
    let committed = PgBacktestPathSetRepository::new(db.clone())
        .commit_cpcv(CpcvPathSetCommit {
            path_set,
            input_hash,
        })
        .await
        .expect("commit bootstrap CPCV path set");
    (committed.path_set_id, committed.path_set_hash)
}

async fn seed_backtest(
    db: &DatabaseConnection,
    store: &Arc<dyn ArtifactStore>,
    model_id: ModelVersionId,
) -> (BacktestReportId, ContentHash) {
    let model = PgModelRegistryRepository::new(db.clone())
        .find_model_version(&model_id)
        .await
        .expect("load bootstrap backtest model")
        .expect("bootstrap backtest model exists");
    let training_dataset_id = model
        .training_dataset_id
        .expect("bootstrap backtest training dataset");
    let training = PgTrainingDatasetRepository::new(db.clone())
        .find_by_id(&training_dataset_id)
        .await
        .expect("load bootstrap training dataset")
        .expect("bootstrap training dataset exists");
    let materialization = training
        .materialization()
        .expect("bootstrap training materialization");
    let bindings = model
        .verified_serving_contract()
        .expect("verify bootstrap backtest contract")
        .bindings();
    let window_start = training.window_end + Duration::hours(1);
    let window_end = window_start + Duration::hours(1);
    let evaluation = ModelDatasetLedgerFixture::persist(
        db,
        store,
        ModelDatasetLedgerSeed {
            scope: format!("model-route-bootstrap-evaluation-{model_id}"),
            model_spec_id: model.model_spec_id,
            model_family: model.model_family,
            model_spec_definition_hash: model.model_spec_definition_hash,
            factor_serving_plane: materialization.factor_serving_plane.clone(),
            feature_schema_version: materialization.manifest.feature_schema_version,
            feature_schema_hash: *materialization.feature_schema_hash,
            decision_policy_snapshot_id: bindings.policy_snapshot.decision_policy_snapshot_id,
            profile_ref: model.profile_ref.clone(),
            prediction_horizon_secs: bindings.model.prediction_horizon_secs,
            purpose: DatasetPurpose::Evaluation,
            window_start,
            window_end,
            research_program_hash: training.source_lineage.research_program_hash,
            sample_count: 32,
            decision_interval_secs: 1,
            trade_policy: bindings.trade_policy.clone(),
        },
    )
    .await
    .expect("persist bootstrap Evaluation Dataset");
    let evaluation_hash = *evaluation
        .materialization()
        .expect("bootstrap Evaluation materialization")
        .dataset_hash;
    let model_run_id = ModelRunId::from_v7();
    PgModelRunRepository::new(db.clone())
        .start_exact(NewModelRun {
            model_run_id,
            run_kind: ModelRunKind::Backtest,
            model_version_id: Some(model_id),
            decision_policy_snapshot_id: bindings.policy_snapshot.decision_policy_snapshot_id,
            market_selection_id: None,
            window_start,
            window_end,
            input_hash: evaluation_hash,
        })
        .await
        .expect("start bootstrap Backtest run");
    let backtest_report_id = BacktestReportId::from_v7();
    let mut report = NewBacktestReport {
        backtest_report_id,
        model_version_id: model_id,
        evaluation_dataset_id: evaluation.training_dataset_id,
        model_run_id,
        decision_policy_snapshot_id: bindings.policy_snapshot.decision_policy_snapshot_id,
        window_start,
        window_end,
        coverage: dec!(1),
        sample_count: 32,
        missing_feature_count: 0,
        rank_ic: dec!(0.25),
        sharpe: dec!(1.1),
        hit_rate: Probability::new(dec!(0.7)),
        expected_vs_realized: ExpectedVsRealized {
            mean_expected_bps: dec!(50),
            mean_realized_bps: dec!(45),
            correlation: dec!(0.85),
            bias_bps: dec!(5),
        },
        max_drawdown: dec!(0.05),
        turnover: dec!(0.2),
        liquidity_feasibility: Probability::new(dec!(1)),
        category_breakdown: CategoryMetrics::default(),
        tail_loss: dec!(-20),
        report_pnl_simulation: PnlSimulation {
            total_allocated_usd: dec!(1000),
            realized_pnl_usd: dec!(100),
            gross_return: dec!(0.1),
            pnl_curve: Vec::new(),
        },
        report_hash: hash('d'),
        parquet_uri: None,
    };
    report.report_hash = report
        .recomputed_hash()
        .expect("hash bootstrap backtest report");
    let persisted = PgBacktestReportRepository::new(db.clone())
        .create(report)
        .await
        .expect("persist bootstrap backtest report");
    (persisted.backtest_report_id, persisted.report_hash)
}

async fn seed_validation(
    db: &DatabaseConnection,
    store: &Arc<dyn ArtifactStore>,
    model_id: ModelVersionId,
) -> ValidationEvidence {
    let (path_set_id, path_set_hash) = seed_path_set(db, model_id).await;
    let (backtest_report_id, backtest_report_hash) =
        Box::pin(seed_backtest(db, store, model_id)).await;
    ValidationEvidence {
        path_set_id,
        path_set_hash,
        backtest_report_id,
        backtest_report_hash,
    }
}

struct BootstrapFixture {
    _database: ScenarioDatabase,
    db: DatabaseConnection,
    repository: PgModelRouteBootstrapRepository,
    bundle: ActivePolicyBundle,
    model_id: ModelVersionId,
    route: BuyModelRoute,
    manifest: ModelBootstrapManifest,
    non_route_policy_hash: ContentHash,
    runtime_revision: i64,
    actor: PromotionPermitActor,
    evaluated_at: DateTime<Utc>,
}

impl BootstrapFixture {
    async fn build() -> Self {
        let (pool, database) = setup_pg().await;
        let db = pool.connection().clone();
        let store = ModelDatasetLedgerFixture::local_store();
        let infra = Box::pin(seed_demo_with_store(&db, &store)).await;
        let models = PgModelRegistryRepository::new(db.clone());
        let model = models
            .find_model_version(&infra.model_version_id)
            .await
            .expect("load first-champion candidate")
            .expect("first-champion candidate exists");
        let route = BuyModelRoute::try_from(model.category_scope)
            .expect("first-champion candidate owns a supported Buy route");
        let training_dataset_id = model
            .training_dataset_id
            .expect("first-champion training dataset");
        let bindings = model
            .verified_serving_contract()
            .expect("verify first-champion contract")
            .bindings();
        let bundle = PgPolicyRepository::new(db.clone())
            .load_current_bundle()
            .await
            .expect("load first-champion policy")
            .expect("first-champion policy exists");
        assert!(
            bundle
                .snapshot
                .model_routing
                .model
                .active_pointer(route)
                .is_err(),
            "fresh bootstrap fixture must have no target-route champion"
        );
        let parity = PgFeatureParityRepository::new(db.clone());
        let run = parity
            .latest_full_for_model(&model.model_version_id, &training_dataset_id)
            .await
            .expect("load first-champion parity")
            .expect("first-champion parity exists");
        let state = parity
            .current_state()
            .await
            .expect("load first-champion parity latch")
            .expect("first-champion parity latch state exists");
        assert_eq!(
            state.state,
            FeatureParityLatchState::Clear,
            "first-champion bootstrap must not bypass an open parity latch"
        );
        let subjects = parity
            .load_frozen_subjects(&run.run_id)
            .await
            .expect("load first-champion frozen parity subject");
        assert_eq!(subjects.len(), 1);
        let validation = Box::pin(seed_validation(&db, &store, model.model_version_id)).await;
        let evaluated_at = PgFeedbackCycleRepository::new(db.clone())
            .database_time()
            .await
            .expect("load PostgreSQL bootstrap time");
        let calibration = bindings
            .model
            .calibration
            .as_ref()
            .expect("first-champion calibration binding");
        let manifest = ModelBootstrapManifest::try_seal(ModelBootstrapManifestInput {
            model_version_id: model.model_version_id,
            model_spec_id: model.model_spec_id,
            model_family: model.model_family,
            model_spec_definition_hash: model.model_spec_definition_hash,
            model_artifact_hash: model.artifact_hash,
            serving_contract_hash: model.serving_contract_hash,
            training_dataset_id,
            training_dataset_hash: bindings.transform.training_dataset_hash,
            dataset_manifest_hash: bindings.dataset.manifest_hash,
            dataset_artifact_hash: bindings.dataset.artifact_bytes_hash,
            feature_schema_hash: bindings.schemas.feature_schema_hash,
            input_contract_hash: bindings.transform.input_contract_hash,
            input_transform_hash: bindings.transform.input_transform_hash,
            calibration_artifact_id: Some(calibration.artifact_id),
            calibration_artifact_hash: Some(calibration.content_hash),
            profile_ref: model.profile_ref.clone(),
            route,
            cpcv_path_set_id: validation.path_set_id,
            cpcv_path_set_hash: validation.path_set_hash,
            backtest_report_id: validation.backtest_report_id,
            backtest_report_hash: validation.backtest_report_hash,
            explanation_validation: CandidateExplanationValidation::try_from(bindings)
                .expect("verify first-champion explanation"),
            quality_gate_report: quality_report(model.model_version_id, evaluated_at),
            feature_parity_run_id: run.run_id,
            feature_parity_state_id: state.state_id,
            feature_parity_evidence_hash: subjects[0].evidence_hash,
        })
        .expect("seal first-champion manifest");
        let projection =
            ModelBootstrapPolicyProjection::try_new(&bundle, route, model.model_version_id)
                .expect("derive first-champion route projection");
        let runtime = PgRuntimeControlRepository::new(db.clone())
            .load()
            .await
            .expect("load first-champion runtime control");
        let actor = UserEntity::find()
            .filter(UserColumn::Username.eq("admin"))
            .one(&db)
            .await
            .expect("load bootstrap admin")
            .expect("bootstrap admin exists");
        Self {
            _database: database,
            repository: PgModelRouteBootstrapRepository::new(db.clone()),
            db,
            bundle,
            model_id: model.model_version_id,
            route,
            manifest,
            non_route_policy_hash: projection.non_route_policy_hash(),
            runtime_revision: runtime.revision,
            actor: PromotionPermitActor {
                user_id: actor.id,
                acting_role: RoleCode::new("super_admin"),
            },
            evaluated_at,
        }
    }

    fn command(&self, key: &str, note: &str, runtime_revision: i64) -> CommitModelRouteBootstrap {
        let preflight = ModelRouteBootstrapPreflight::try_seal(ModelRouteBootstrapPreflightInput {
            manifest: self.manifest.clone(),
            expected_policy_generation: self.bundle.generation,
            expected_snapshot_id: self.bundle.decision_policy_snapshot_id,
            expected_snapshot_hash: self.bundle.snapshot_hash,
            expected_model_routing_revision_id: self
                .bundle
                .snapshot
                .resource_revision_id(ConfigResourceKind::ModelRouting)
                .copied()
                .expect("bootstrap ModelRouting revision"),
            expected_runtime_control_revision: runtime_revision,
            current_runtime_mode: QuantRuntimeMode::ReportOnly,
            non_route_policy_hash: self.non_route_policy_hash,
            evaluated_at: self.evaluated_at,
        })
        .expect("seal first-champion preflight");
        CommitModelRouteBootstrap::try_new(
            BootstrapModelRoute {
                model_version_id: self.model_id,
                expected_policy_generation: self.bundle.generation,
                expected_runtime_control_revision: runtime_revision,
                idempotency_key: key
                    .parse::<PolicyIdempotencyKey>()
                    .expect("bootstrap idempotency key"),
                actor: self.actor.clone(),
                reason_code: "first_champion".to_owned(),
                note: note.to_owned(),
            },
            preflight,
        )
        .expect("bind first-champion request")
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct BootstrapCounts {
    revisions: u64,
    approvals: u64,
    snapshots: u64,
    activations: u64,
    activation_audits: u64,
    activation_outboxes: u64,
    model_audits: u64,
}

async fn load_counts(db: &DatabaseConnection) -> BootstrapCounts {
    BootstrapCounts {
        revisions: RevisionEntity::find()
            .count(db)
            .await
            .expect("count revisions"),
        approvals: ApprovalEntity::find()
            .count(db)
            .await
            .expect("count approvals"),
        snapshots: SnapshotEntity::find()
            .count(db)
            .await
            .expect("count snapshots"),
        activations: ActivationEntity::find()
            .count(db)
            .await
            .expect("count activations"),
        activation_audits: ActivationAuditEntity::find()
            .count(db)
            .await
            .expect("count activation audits"),
        activation_outboxes: ActivationOutboxEntity::find()
            .count(db)
            .await
            .expect("count activation outboxes"),
        model_audits: ModelAuditEntity::find()
            .count(db)
            .await
            .expect("count model audits"),
    }
}

async fn install_outbox_fault(db: &DatabaseConnection) {
    db.execute_unprepared(
        "CREATE FUNCTION qp_test_fail_bootstrap_outbox() RETURNS trigger
         LANGUAGE plpgsql AS $$
         BEGIN
             RAISE EXCEPTION 'injected bootstrap outbox failure';
         END;
         $$;
         CREATE TRIGGER qp_test_fail_bootstrap_outbox
         BEFORE INSERT ON policy_activation_event_outbox
         FOR EACH ROW EXECUTE FUNCTION qp_test_fail_bootstrap_outbox();",
    )
    .await
    .expect("install bootstrap outbox fault");
}

async fn remove_outbox_fault(db: &DatabaseConnection) {
    db.execute_unprepared(
        "DROP TRIGGER qp_test_fail_bootstrap_outbox ON policy_activation_event_outbox;
         DROP FUNCTION qp_test_fail_bootstrap_outbox();",
    )
    .await
    .expect("remove bootstrap outbox fault");
}

fn assert_linkage(commit: &ModelRouteBootstrapCommit, outbox: &ActivationOutboxModel) {
    assert_eq!(
        commit.activation.model_governance_audit_id,
        Some(commit.audit.audit_id)
    );
    assert_eq!(
        outbox.model_governance_audit_id,
        Some(commit.audit.audit_id)
    );
    assert_eq!(
        outbox.policy_activation_id,
        commit.activation.policy_activation_id
    );
    let ModelGovernanceAuditDetail::BootstrapRoute { record } = &commit.audit.detail else {
        panic!("bootstrap audit must preserve its typed transaction record");
    };
    assert_eq!(record.transaction_hash(), commit.transaction_hash);
}

pub async fn model_route_bootstrap_contracts() {
    let fixture = Box::pin(BootstrapFixture::build()).await;
    let before = load_counts(&fixture.db).await;
    let stale = fixture.command(
        "model-route-bootstrap-stale-runtime",
        "reject a stale runtime revision",
        fixture.runtime_revision + 1,
    );
    fixture
        .repository
        .commit(stale)
        .await
        .expect_err("stale bootstrap runtime revision must fail");
    assert_eq!(load_counts(&fixture.db).await, before);

    let command = fixture.command(
        "model-route-bootstrap-first-champion",
        "activate the first governed champion",
        fixture.runtime_revision,
    );
    install_outbox_fault(&fixture.db).await;
    fixture
        .repository
        .commit(command.clone())
        .await
        .expect_err("outbox failure must roll back the bootstrap graph");
    remove_outbox_fault(&fixture.db).await;
    assert_eq!(load_counts(&fixture.db).await, before);
    assert!(
        fixture
            .repository
            .find_committed(&command.request().idempotency_key)
            .await
            .expect("read rolled-back bootstrap key")
            .is_none()
    );
    assert_eq!(
        PgPolicyRepository::new(fixture.db.clone())
            .load_current_bundle()
            .await
            .expect("reload policy after bootstrap fault")
            .expect("policy survives bootstrap fault"),
        fixture.bundle
    );

    let committed = fixture
        .repository
        .commit(command.clone())
        .await
        .expect("commit first-champion bootstrap");
    assert_eq!(committed.outcome, ModelRouteBootstrapOutcome::Committed);
    let pointer = committed
        .bundle
        .snapshot
        .model_routing
        .model
        .active_pointer(fixture.route)
        .expect("first-champion route pointer");
    assert_eq!(*pointer.id(), fixture.model_id);
    let outbox = ActivationOutboxEntity::find_by_id(committed.activation.audit_event_id)
        .one(&fixture.db)
        .await
        .expect("load bootstrap outbox")
        .expect("bootstrap outbox exists");
    assert_linkage(&committed, &outbox);

    let replayed = fixture
        .repository
        .find_committed(&command.request().idempotency_key)
        .await
        .expect("read committed first-champion bootstrap")
        .expect("first-champion bootstrap exists");
    let committed_counts = load_counts(&fixture.db).await;
    assert_eq!(replayed.transaction_hash, committed.transaction_hash);
    let replayed = fixture
        .repository
        .commit(command.clone())
        .await
        .expect("replay first-champion bootstrap");
    assert_eq!(replayed.outcome, ModelRouteBootstrapOutcome::ExactReplay);
    assert_eq!(
        replayed.activation.policy_activation_id,
        committed.activation.policy_activation_id
    );
    assert_eq!(replayed.transaction_hash, committed.transaction_hash);
    assert_eq!(load_counts(&fixture.db).await, committed_counts);

    let drifted = fixture.command(
        "model-route-bootstrap-first-champion",
        "same key with semantic drift",
        fixture.runtime_revision,
    );
    fixture
        .repository
        .commit(drifted)
        .await
        .expect_err("bootstrap key reuse with intent drift must fail");
    assert!(
        ModelBootstrapPolicyProjection::try_new(
            &committed.bundle,
            fixture.route,
            fixture.model_id,
        )
        .is_err(),
        "a populated Buy route must reject every second bootstrap"
    );
    drop(fixture);
}
