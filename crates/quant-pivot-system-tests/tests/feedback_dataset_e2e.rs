//! Production-storage feedback outcome → cohort → Dataset contracts.

use std::{
    collections::BTreeMap,
    env, fs,
    path::{Path, PathBuf},
    result::Result as StdResult,
    sync::Arc,
};

use anyhow::{Context, Error as AnyhowError, Result, ensure};
use async_trait::async_trait;
use chrono::{DateTime, Duration, Utc};
use quant_pivot_api::settlement::resolution::{
    FinalizedResolutionBlock, FinalizedResolutionObservation, FinalizedResolutionScan,
    FinalizedResolutionVector, ResolutionSourceReadError, ResolutionSourceReader,
};
use quant_pivot_compute::ComputeExecutor;
use quant_pivot_core::{
    execution::{
        OutcomeReconciliationPassConfig, OutcomeReconciliationService,
        OutcomeReconciliationServiceDeps,
    },
    service::{
        feedback_dataset::{FeedbackDatasetService, FeedbackDatasetServiceDeps},
        feedback_signals::{FeedbackSignalService, FeedbackSignalServiceDeps},
        training_dataset::{
            TrainingDatasetBuildConfig, TrainingDatasetService, TrainingDatasetServiceDeps,
            default_labelers,
        },
    },
};
use quant_pivot_error::storage::entity::MARKET_RESOLUTION_EVENT;
use quant_pivot_models::{
    clickhouse::MarketResolutionRow,
    domain::{
        api::FeedbackCoverageJobParams,
        data_plane::{
            DecisionClock, DecisionSource, DomainCursorStatus, DomainSourceCheckpoint,
            DomainSourceCursorCasOutcome, UpsertDomainSourceCursor,
        },
        ports::{
            FeedbackCandidateFamily, FeedbackCandidateFamilyInput, FeedbackCandidateRecipe,
            FeedbackComparisonContract, FeedbackCoverageExecutionPort, FeedbackDatasetBuildRequest,
        },
        quant::{
            FeatureParityStateInfo, FeedbackCohortWindow, FeedbackCycleInfo, FeedbackCycleKey,
            FeedbackCycleKeyInput, FeedbackRecommendationContext, FeedbackStageEventInput,
            NewFeedbackCycle, NewFeedbackStageEvent, NewRecommendation, NoopProgressSink,
            TrainingDatasetInfo,
        },
    },
    enums::{
        catalog::CatalogTimestampQuality,
        feature::EvidenceSourceKind,
        market::MarketStatus,
        quant::{
            CalibrationMethod, DataQualityStatus, DatasetPurpose, DownsideSource, FeedbackStage,
            FeedbackStageEventKind, FeedbackTriggerFamily,
        },
    },
    hashing::CanonicalDigest,
    types::{
        CatalogDecisionRef, CatalogEventChangeId, CatalogMarketChangeId, CatalogSyncBatchId,
        ContentHash, DatasetSourceLineage, DecisionCaptureEvidence, DecisionPolicySnapshotId,
        DecisionSnapshotEvidence, DomainInstrumentKey, DomainSourceId, EvidenceSourceRef,
        EvmAddress, EvmBlockHash, EvmTransactionHash, FeatureCell, FeatureStaleness, FeatureValue,
        FeedbackCoverageArtifactId, MarketId, ModelVersionId, PayoutRatio, Probability,
        ResearchEvaluationTrack, ResearchProfileRef, SchemaVersion, TrainingDatasetId,
        TrainingExampleId, TrainingSampleSource, WorkerId,
    },
};
use quant_pivot_repository::{
    clickhouse::{ChFactWriter, ChQuantFactReadRepository},
    postgres::{
        PgCalibrationArtifactRepository, PgCatalogLedgerRepository, PgClobMarketInfoRepository,
        PgDomainSourceCursorRepository, PgFactorRepository, PgFeatureParityRepository,
        PgFeatureRepository, PgFeedbackCohortRepository, PgFeedbackCycleRepository,
        PgMarketLinkageRepository, PgMarketRepository, PgModelRegistryRepository,
        PgPolicyRepository, PgPositionRepository, PgRecommendationExecutionOutcomeRepository,
        PgRecommendationReportRepository, PgRecommendationRepository,
        PgRecommendationResolutionOutcomeRepository, PgTradePolicyRepository,
        PgTrainingDatasetRepository,
    },
    traits::{
        DomainSourceCursorRepository, FactWriter, FactorRepository, FeatureParityRepository,
        FeatureRepository, FeedbackCycleRepository, MarketRepository, ModelRegistryRepository,
        PolicyRepository, RecommendationReportRepository, RecommendationRepository,
        RecommendationResolutionOutcomeRepository, TrainingDatasetRepository,
    },
};
use quant_pivot_research::{
    artifact::{ArtifactStore, LocalArtifactStore},
    factors::{FactorValue, FactorValueInsertContext},
    features::{FeatureVector, names::book::MID},
    feedback::{CoverageGateOutcome, CoverageNoActionReason, FeedbackCoverageCodec},
    selection::SelectedMarket,
    training::{
        TOKEN_PAYOUT_RATIO, TrainingDatasetArtifact, TrainingExample, TrainingLabel,
        dataset_manifest_hash,
    },
};
use quant_pivot_storage::clickhouse::{ChWriteManager, ClickHousePool};
use quant_pivot_system_tests::{
    postgres::PostgresClock,
    stack::SystemStack,
    support::{
        artifact_store::VersionedArtifactStoreFixture,
        execution_pg_seed::{
            ExecutionTxnIds, FEEDBACK_SCALE_REPORT_COUNT, FEEDBACK_SCALE_TOTAL, ReportBuildOptions,
            ReportSeedConfig, SharedDemoInfra, build_custom_report_transaction,
            fixture_no_token_id, fixture_profile_ref, prepare_report_on_infra,
            seed_demo_with_store, seed_feedback_scale,
        },
        model_serving_runtime::ModelServingRegistryFixture,
        report_lifecycle_seed::persist_and_publish_report,
        research_fixtures::{
            ReplayableSourceSliceFixture, persist_replayable_source_slice, seed_source_manifest,
        },
        trade_policy_fixtures::PublishedTradePolicyFixture,
    },
};
use rust_decimal_macros::dec;
use sea_orm::{ConnectionTrait, DatabaseConnection, DbBackend, Statement};
use tokio_util::sync::CancellationToken;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn outcome_dataset_production_stack() {
    let artifact_root = artifact_root();
    fs::create_dir_all(&artifact_root).expect("create W2-A11 artifact root");
    let stack = SystemStack::start()
        .await
        .expect("start schema-complete W2-A11 production stack");

    let scenario = Box::pin(run_contract(&stack, &artifact_root)).await;
    let shutdown = Box::pin(stack.shutdown()).await;
    let cleanup = fs::remove_dir_all(&artifact_root);

    scenario.expect("W2-A11 outcome → cohort → Dataset contract");
    shutdown.expect("shutdown W2-A11 production stack");
    cleanup.expect("remove W2-A11 artifact root");
}

async fn run_contract(stack: &SystemStack, artifact_root: &Path) -> Result<()> {
    let db = stack.postgres.connection().clone();
    let inner: Arc<dyn ArtifactStore> =
        Arc::new(LocalArtifactStore::new(artifact_root.to_path_buf()));
    let store: Arc<dyn ArtifactStore> = Arc::new(VersionedArtifactStoreFixture::new(inner));
    let ch = Arc::new(
        ClickHousePool::connect(&stack.clickhouse_config)
            .await
            .context("connect second ClickHouse production-stack handle")?,
    );
    let facts = Arc::new(ChQuantFactReadRepository::new(Arc::clone(&ch)));
    let infra = seed_demo_with_store(&db, &store).await;
    let model = PgModelRegistryRepository::new(db.clone())
        .find_model_version(&infra.model_version_id)
        .await?
        .context("seeded model version")?;

    let mut reports = Vec::with_capacity(FEEDBACK_SCALE_REPORT_COUNT);
    let mut serving = Vec::with_capacity(FEEDBACK_SCALE_REPORT_COUNT);
    for ordinal in 0..FEEDBACK_SCALE_REPORT_COUNT {
        let (ids, evidence) = seed_feedback_report(&db, &infra, ordinal).await?;
        reports.push(ids);
        serving.push(evidence);
    }
    assert_publication_history(&db, &reports).await?;
    let reconciliation =
        reconcile_outcomes(&db, Arc::clone(&ch), Arc::clone(&facts), &reports).await?;

    let dataset_service = feedback_service(
        &db,
        Arc::clone(&store),
        facts,
        infra.decision_policy_snapshot_id,
    )
    .await?;
    let old_source = source_lineage(
        &db,
        &store,
        &[source_example(
            &serving[0],
            reconciliation.first_resolved_at,
        )],
        &infra,
        reconciliation.window_start,
        reconciliation.frozen_cutoff,
        "w2-a11-frozen",
    )
    .await?;
    let old_request = FeedbackDatasetBuildRequest {
        training_dataset_id: TrainingDatasetId::from_v7(),
        model_spec_id: model.model_spec_id,
        model_spec_definition_hash: model.model_spec_definition_hash,
        source_lineage: old_source,
        window: feedback_window(reconciliation.window_start, reconciliation.frozen_cutoff)?,
        purpose: DatasetPurpose::Training,
    };
    let first_artifact = Box::pin(dataset_service.build(
        old_request.clone(),
        Arc::new(NoopProgressSink),
        CancellationToken::new(),
    ))
    .await?;
    assert_cohort(&first_artifact, FEEDBACK_SCALE_TOTAL as u64, 1)?;

    let rerun_artifact = Box::pin(dataset_service.build(
        old_request,
        Arc::new(NoopProgressSink),
        CancellationToken::new(),
    ))
    .await?;
    ensure!(
        rerun_artifact == first_artifact,
        "same frozen request did not reload the exact immutable Dataset"
    );
    let first_bytes = store.get(&first_artifact.parquet_uri).await?;
    ensure!(
        CanonicalDigest::content_hash_bytes(&first_bytes) == first_artifact.artifact_bytes_hash,
        "persisted Dataset byte hash does not reproduce"
    );
    ensure!(
        dataset_manifest_hash(&first_artifact.manifest)?
            == dataset_manifest_hash(&rerun_artifact.manifest)?,
        "same frozen request changed its manifest hash"
    );

    let next_source = source_lineage(
        &db,
        &store,
        &[
            source_example(&serving[0], reconciliation.first_resolved_at),
            source_example(&serving[1], reconciliation.second_resolved_at),
        ],
        &infra,
        reconciliation.window_start,
        reconciliation.next_cutoff,
        "w2-a11-next",
    )
    .await?;
    let next_artifact = Box::pin(dataset_service.build(
        FeedbackDatasetBuildRequest {
            training_dataset_id: TrainingDatasetId::from_v7(),
            model_spec_id: model.model_spec_id,
            model_spec_definition_hash: model.model_spec_definition_hash,
            source_lineage: next_source,
            window: feedback_window(reconciliation.window_start, reconciliation.next_cutoff)?,
            purpose: DatasetPurpose::Training,
        },
        Arc::new(NoopProgressSink),
        CancellationToken::new(),
    ))
    .await?;
    assert_cohort(&next_artifact, FEEDBACK_SCALE_TOTAL as u64, 2)?;
    ensure!(
        first_artifact.dataset_hash != next_artifact.dataset_hash,
        "next cutoff did not incorporate the late outcome"
    );
    assert_signal_coverage(
        &db,
        Arc::clone(&store),
        infra.model_version_id,
        reconciliation.next_cutoff,
    )
    .await?;

    let storage_counts = assert_storage_idempotency(&db, ch.as_ref(), &reports).await?;
    print_evidence(&first_artifact, &next_artifact, storage_counts)?;
    Ok(())
}

struct ReconciledOutcomes {
    window_start: DateTime<Utc>,
    frozen_cutoff: DateTime<Utc>,
    next_cutoff: DateTime<Utc>,
    first_resolved_at: DateTime<Utc>,
    second_resolved_at: DateTime<Utc>,
}

async fn assert_signal_coverage(
    db: &DatabaseConnection,
    store: Arc<dyn ArtifactStore>,
    model_version_id: ModelVersionId,
    label_cutoff: DateTime<Utc>,
) -> Result<()> {
    let cycle = record_signal_cycle(db, model_version_id, label_cutoff).await?;
    let parity = PgFeatureParityRepository::new(db.clone());
    let before = parity
        .current_state()
        .await?
        .context("seeded parity state before feedback coverage")?;
    let service = signal_service(db, Arc::clone(&store))?;
    let artifact_id = FeedbackCoverageArtifactId::from_cycle_id(cycle.feedback_cycle_id);
    let result = Box::pin(service.execute(
        FeedbackCoverageJobParams {
            feedback_cycle_id: cycle.feedback_cycle_id,
            cycle_idempotency_hash: cycle.idempotency_hash,
            artifact_id,
        },
        Arc::new(NoopProgressSink),
        CancellationToken::new(),
    ))
    .await?;
    ensure!(
        result.artifact_id == artifact_id,
        "coverage executor returned another artifact identity"
    );
    let bytes = store.get(&result.artifact.uri).await?;
    ensure!(
        FeedbackCoverageCodec::bytes_hash(&bytes) == result.artifact.content_hash,
        "coverage result hash does not bind its object bytes"
    );
    let artifact = FeedbackCoverageCodec::decode(&bytes)?;
    let cycle_hash_matches = artifact.cycle_idempotency_hash == cycle.idempotency_hash;
    ensure!(
        artifact.feedback_cycle_id == cycle.feedback_cycle_id && cycle_hash_matches,
        "coverage object differs from its live frozen cycle"
    );
    ensure!(
        matches!(
            artifact.gate_outcome,
            CoverageGateOutcome::NoAction {
                reason: CoverageNoActionReason::InsufficientMatureLabels,
                ..
            }
        ),
        "two real mature outcomes must not pass the production 500-label floor"
    );
    ensure!(
        artifact.cohorts.policy_evaluation.eligible_count() == FEEDBACK_SCALE_TOTAL as u64,
        "coverage denominator did not retain the complete PolicyEvaluation universe"
    );
    ensure!(
        artifact.cohorts.model_learning.eligible_count() == 2
            && artifact.new_mature_label_count == 2
            && artifact.champion_rows.len() == 2
            && artifact.champion_examples.len() == 2,
        "coverage did not freeze the two real mature champion labels"
    );
    let after = parity
        .current_state()
        .await?
        .context("seeded parity state after feedback coverage")?;
    assert_parity_unchanged(&before, &after)?;
    Ok(())
}

async fn record_signal_cycle(
    db: &DatabaseConnection,
    model_version_id: ModelVersionId,
    label_cutoff: DateTime<Utc>,
) -> Result<FeedbackCycleInfo> {
    let models = PgModelRegistryRepository::new(db.clone());
    let model = models
        .find_model_version(&model_version_id)
        .await?
        .context("feedback coverage champion")?;
    let dataset_id = model
        .training_dataset_id
        .context("feedback coverage champion Training Dataset")?;
    let dataset = PgTrainingDatasetRepository::new(db.clone())
        .find_by_id(&dataset_id)
        .await?
        .context("feedback coverage champion Dataset row")?;
    let profile = model
        .profile_ref
        .resolve_builtin_research_profile()
        .map_err(AnyhowError::msg)?;
    let feedback_policy_hash = profile.spec.feedback_policy.content_hash()?;
    let candidate_family = signal_candidate_family(&dataset, &model.profile_ref, label_cutoff)?;
    let cycle = NewFeedbackCycle::try_seal(FeedbackCycleKey::try_new(FeedbackCycleKeyInput {
        trigger_family: FeedbackTriggerFamily::Scheduled,
        profile_ref: model.profile_ref,
        feedback_policy_hash,
        label_cutoff,
        capability_registry_hashes: dataset.source_lineage.capability_registry_hashes,
        champion_model_version_id: model.model_version_id,
        champion_serving_contract_hash: model.serving_contract_hash,
        candidate_family,
    })?)?;
    let cycle_id = cycle.feedback_cycle_id();
    let trigger = NewFeedbackStageEvent::try_seal(FeedbackStageEventInput {
        feedback_cycle_id: cycle_id,
        event_sequence: 1,
        stage: FeedbackStage::Trigger,
        event_kind: FeedbackStageEventKind::Triggered,
        research_job_id: None,
        actor: Some("feedback-dataset-e2e".to_owned()),
        reason_code: Some("w2_f06_real_coverage".to_owned()),
        evidence_uri: None,
        evidence_hash: None,
        occurred_at: label_cutoff,
    })?;
    let cycles = PgFeedbackCycleRepository::new(db.clone());
    cycles.record_trigger(cycle, trigger).await?;
    let claim = cycles
        .claim_cycle(WorkerId::from_v7(), 90)
        .await?
        .context("claim real feedback coverage cycle")?;
    ensure!(
        claim.cycle.feedback_cycle_id == cycle_id,
        "feedback coordinator claimed another cycle"
    );
    Ok(claim.cycle)
}

fn signal_candidate_family(
    seed: &TrainingDatasetInfo,
    profile_ref: &ResearchProfileRef,
    label_cutoff: DateTime<Utc>,
) -> Result<FeedbackCandidateFamily> {
    let policy_id = seed.source_lineage.decision_policy_snapshot_id;
    let request = |purpose, window_start, cutoff| -> Result<FeedbackDatasetBuildRequest> {
        let mut source_lineage = seed.source_lineage.clone();
        source_lineage.source_window_start = window_start;
        source_lineage.source_window_end = cutoff;
        source_lineage.pit_cutoff = cutoff;
        Ok(FeedbackDatasetBuildRequest {
            training_dataset_id: TrainingDatasetId::from_v7(),
            model_spec_id: seed.model_spec_id,
            model_spec_definition_hash: seed.model_spec_definition_hash,
            source_lineage,
            window: FeedbackCohortWindow::try_new(profile_ref.clone(), window_start, cutoff)?,
            purpose,
        })
    };
    let recipe = FeedbackCandidateRecipe::try_seal(
        request(
            DatasetPurpose::Training,
            label_cutoff - Duration::days(9),
            label_cutoff - Duration::days(7),
        )?,
        request(
            DatasetPurpose::Calibration,
            label_cutoff - Duration::days(6),
            label_cutoff - Duration::days(4),
        )?,
        CalibrationMethod::Platt,
        DownsideSource::MfeMae,
        policy_id,
    )?;
    let comparison_contract = FeedbackComparisonContract::try_from_policy(
        &profile_ref
            .resolve_builtin_research_profile()
            .map_err(AnyhowError::msg)?
            .spec
            .feedback_policy,
    )?;
    FeedbackCandidateFamily::try_seal(FeedbackCandidateFamilyInput {
        shared_evaluation: request(
            DatasetPurpose::Evaluation,
            label_cutoff - Duration::days(3),
            label_cutoff,
        )?,
        comparison_contract,
        candidates: vec![recipe],
    })
    .map_err(Into::into)
}

fn signal_service(
    db: &DatabaseConnection,
    store: Arc<dyn ArtifactStore>,
) -> Result<FeedbackSignalService> {
    let preimages = ModelServingRegistryFixture {
        db: db.clone(),
        artifact_store: Arc::clone(&store),
        evidence_scope: PublishedTradePolicyFixture::evidence_scope()?,
        evidence_attestor: Some(PublishedTradePolicyFixture::evidence_attestor()?),
    }
    .build_preimages();
    Ok(FeedbackSignalService::new(FeedbackSignalServiceDeps {
        cycles: Arc::new(PgFeedbackCycleRepository::new(db.clone())),
        models: Arc::new(PgModelRegistryRepository::new(db.clone())),
        preimages,
        cohort_repository: Arc::new(PgFeedbackCohortRepository::new(db.clone())),
        feature_repository: Arc::new(PgFeatureRepository::new(db.clone())),
        factor_repository: Arc::new(PgFactorRepository::new(db.clone())),
        artifact_store: store,
    }))
}

fn assert_parity_unchanged(
    before: &FeatureParityStateInfo,
    after: &FeatureParityStateInfo,
) -> Result<()> {
    ensure!(
        after.state_id == before.state_id
            && after.state == before.state
            && after.transition == before.transition
            && after.cause_run_id == before.cause_run_id
            && after.recovery_run_id == before.recovery_run_id
            && after.previous_state_id == before.previous_state_id,
        "coverage execution mutated the deterministic parity latch"
    );
    Ok(())
}

async fn reconcile_outcomes(
    db: &DatabaseConnection,
    ch: Arc<ClickHousePool>,
    facts: Arc<ChQuantFactReadRepository>,
    reports: &[ExecutionTxnIds],
) -> Result<ReconciledOutcomes> {
    let window_start = reports
        .iter()
        .map(|ids| ids.decision_at)
        .min()
        .context("feedback report decisions")?
        - Duration::minutes(1);

    let first = &reports[0];
    PgMarketRepository::new(db.clone())
        .update_status(
            &MarketId::new(&first.market),
            MarketStatus::Settled,
            Some("Split"),
        )
        .await?;
    let first_resolved_at = first.decision_at + Duration::milliseconds(1);
    seed_resolution_cursor(db, 100, first.decision_at - Duration::seconds(1)).await?;
    let first_summary = reconciliation_service(
        db,
        Arc::clone(&ch),
        Arc::clone(&facts),
        resolution_source(first, 101, first_resolved_at, '1'),
    )
    .run_resolution_pass(pass_config(db.statement_time().await, 1))
    .await?;
    ensure!(
        first_summary.source_facts_written == 1 && first_summary.resolution_inserted == 1,
        "first production reconciliation did not commit one CH fact and one PG outcome"
    );

    let report_cutoff = db.statement_time().await;
    seed_feedback_scale(db, reports, window_start, report_cutoff).await;
    let frozen_cutoff = db.statement_time().await;

    let second = &reports[1];
    PgMarketRepository::new(db.clone())
        .update_status(
            &MarketId::new(&second.market),
            MarketStatus::Settled,
            Some("Split"),
        )
        .await?;
    let second_resolved_at = second.decision_at + Duration::milliseconds(1);
    let second_summary = reconciliation_service(
        db,
        ch,
        facts,
        resolution_source(second, 102, second_resolved_at, '2'),
    )
    .run_resolution_pass(pass_config(db.statement_time().await, 1))
    .await?;
    ensure!(
        second_summary.source_facts_written == 1 && second_summary.resolution_inserted == 1,
        "late production reconciliation did not commit one CH fact and one PG outcome"
    );
    Ok(ReconciledOutcomes {
        window_start,
        frozen_cutoff,
        next_cutoff: db.statement_time().await,
        first_resolved_at,
        second_resolved_at,
    })
}

struct ServingSeed {
    selected_market: SelectedMarket,
    feature_vector: FeatureVector,
    factor_values: Vec<FactorValue>,
    capture: DecisionCaptureEvidence,
}

async fn seed_feedback_report(
    db: &DatabaseConnection,
    infra: &SharedDemoInfra,
    ordinal: usize,
) -> Result<(ExecutionTxnIds, ServingSeed)> {
    let config = ReportSeedConfig {
        event_id: format!("feedback-dataset-event-{ordinal}"),
        market_id: format!("0xfeedback-dataset-market-{ordinal}"),
        market_question: format!("Will feedback Dataset fixture {ordinal} settle?"),
        market_slug: format!("feedback-dataset-fixture-{ordinal}"),
        token_id: format!("{}", 90_000 + ordinal),
        trigger_key: format!("feedback-dataset:{ordinal}"),
    };
    let ids = prepare_report_on_infra(db, infra, &config).await;
    let options = ReportBuildOptions::published_single(&ids);
    let recommendation = options
        .recommendations
        .first()
        .context("prepared feedback recommendation")?;
    let serving = serving_evidence(&ids, recommendation)?;
    let mut feature = serving
        .feature_vector
        .try_to_new(&serving.capture.snapshot.boundary, &serving.capture)?;
    feature.feature_vector_id = recommendation.evidence_refs.feature_vector_id;
    PgFeatureRepository::new(db.clone()).create(feature).await?;
    let factor_values = ids.factor_values();
    let factor_context = FactorValueInsertContext {
        model_run_id: &ids.model_run,
        feature_vector_id: &recommendation.evidence_refs.feature_vector_id,
        market_id: &recommendation.market_id,
        decision_at: ids.decision_at,
    };
    PgFactorRepository::new(db.clone())
        .create_values(
            factor_values
                .iter()
                .map(|factor| factor.try_to_new(&factor_context))
                .collect::<StdResult<Vec<_>, _>>()?,
        )
        .await?;
    ids.complete_model_run(db).await;
    persist_and_publish_report(
        db,
        build_custom_report_transaction(&ids, options),
        &config.trigger_key,
        10,
    )
    .await;
    Ok((
        ids,
        ServingSeed {
            selected_market: serving.selected_market,
            feature_vector: serving.feature_vector,
            factor_values,
            capture: serving.capture,
        },
    ))
}

async fn assert_publication_history(
    db: &DatabaseConnection,
    reports: &[ExecutionTxnIds],
) -> Result<()> {
    let report_repository = PgRecommendationReportRepository::new(db.clone());
    let recommendation_repository = PgRecommendationRepository::new(db.clone());
    for ids in reports {
        let report = report_repository
            .find_by_id(&ids.report)
            .await?
            .context("published feedback report")?;
        let recommendation = recommendation_repository
            .find_by_id(&ids.recommendation)
            .await?
            .context("published feedback recommendation")?;
        FeedbackRecommendationContext::try_from_report(&recommendation, &report).with_context(
            || {
                format!(
                    "historical publication mismatch: report={} report_status={:?} recommendation_status={:?} decision_at={} report_created_at={} recommendation_created_at={} published_at={:?} superseded_at={:?}",
                    report.recommendation_report_id,
                    report.status,
                    recommendation.status,
                    report.decision_at,
                    report.created_at,
                    recommendation.created_at,
                    report.published_at,
                    report.superseded_at,
                )
            },
        )?;
    }
    Ok(())
}

struct ServingEvidence {
    selected_market: SelectedMarket,
    feature_vector: FeatureVector,
    capture: DecisionCaptureEvidence,
}

fn serving_evidence(
    ids: &ExecutionTxnIds,
    recommendation: &NewRecommendation,
) -> Result<ServingEvidence> {
    let boundary = DecisionClock::new(0).boundary(ids.decision_at)?;
    let catalog_effective_at = boundary.cutoff_for(DecisionSource::Catalog);
    let book_effective_at = boundary.cutoff_for(DecisionSource::Book);
    let source_ref = EvidenceSourceRef {
        source_kind: EvidenceSourceKind::Book,
        reference: recommendation.evidence_refs.book_snapshot_ref.to_string(),
        effective_at: book_effective_at,
        available_at: Some(ids.decision_at),
    };
    let selected_market = SelectedMarket {
        market_id: recommendation.market_id.clone(),
        event_id: recommendation.event_id.clone(),
        category: recommendation.identity.category,
        primary_token_id: recommendation.token_id.clone(),
        secondary_token_id: Some(fixture_no_token_id(
            recommendation.market_id.as_str(),
            recommendation.token_id.as_str(),
        )),
        liquidity_usd: Some(recommendation.market_context.depth_usd),
        volume_24h_usd: recommendation.market_context.volume_24h_usd,
        source_refs: vec![source_ref.clone()],
    };
    let feature_vector = FeatureVector {
        market_id: recommendation.market_id.clone(),
        token_id: Some(recommendation.token_id.clone()),
        decision_at: ids.decision_at,
        generic_schema_version: SchemaVersion::FIRST,
        generic: BTreeMap::from([(
            MID,
            FeatureCell::observed(
                FeatureValue::Probability(Probability::new(dec!(0.42))),
                Some(source_ref),
                FeatureStaleness::Known { age_ms: 0 },
            ),
        )]),
        domain: None,
        data_quality: DataQualityStatus::Fresh,
    };
    let capture = DecisionCaptureEvidence {
        snapshot: DecisionSnapshotEvidence {
            boundary,
            market_id: recommendation.market_id.clone(),
            event_id: recommendation.event_id.clone(),
            token_id: recommendation.token_id.clone(),
            catalog: CatalogDecisionRef {
                catalog_sync_batch_id: CatalogSyncBatchId::from_v7(),
                market_change_id: CatalogMarketChangeId::from_v7(),
                event_change_id: CatalogEventChangeId::from_v7(),
                market_content_hash: evidence_hash(recommendation, "market")?,
                event_content_hash: evidence_hash(recommendation, "event")?,
                membership_hash: evidence_hash(recommendation, "membership")?,
                market_effective_at: catalog_effective_at,
                market_available_at: ids.decision_at,
                event_effective_at: catalog_effective_at,
                event_available_at: ids.decision_at,
                market_timestamp_quality: CatalogTimestampQuality::Source,
                event_timestamp_quality: CatalogTimestampQuality::Source,
            },
            book_snapshot_ref: recommendation.evidence_refs.book_snapshot_ref.clone(),
            book_effective_at,
            book_available_at: ids.decision_at,
            selection: (&selected_market).into(),
        },
        identity: recommendation.identity.clone(),
        market_context: recommendation.market_context.clone(),
        data_quality: DataQualityStatus::Fresh,
        liquidity_score: recommendation.liquidity_score,
    };
    Ok(ServingEvidence {
        selected_market,
        feature_vector,
        capture,
    })
}

fn source_example(seed: &ServingSeed, resolved_at: DateTime<Utc>) -> TrainingExample {
    TrainingExample {
        example_id: TrainingExampleId::from_v7(),
        market_id: seed.feature_vector.market_id.clone(),
        token_id: seed
            .feature_vector
            .token_id
            .clone()
            .expect("serving feature token"),
        selected_market: seed.selected_market.clone(),
        decision_boundary: seed.capture.snapshot.boundary.clone(),
        sample_source: TrainingSampleSource::RecommendationFeedback,
        feature_vector: seed.feature_vector.clone(),
        factor_values: seed.factor_values.clone(),
        labels: vec![TrainingLabel {
            label_name: TOKEN_PAYOUT_RATIO,
            horizon_secs: 0,
            value: dec!(0.5),
            is_resolved: true,
            matured_at: resolved_at,
        }],
        source_refs: seed.feature_vector.evidence_refs(),
        decision_capture: Some(seed.capture.clone()),
        lot_context: None,
        position_state: None,
        book_fidelity: None,
    }
}

async fn source_lineage(
    db: &DatabaseConnection,
    store: &Arc<dyn ArtifactStore>,
    examples: &[TrainingExample],
    infra: &SharedDemoInfra,
    window_start: DateTime<Utc>,
    cutoff: DateTime<Utc>,
    scope: &str,
) -> Result<DatasetSourceLineage> {
    let stored = persist_replayable_source_slice(
        store,
        examples,
        ReplayableSourceSliceFixture {
            profile_ref: fixture_profile_ref(),
            evaluation_track: ResearchEvaluationTrack::ResearchOnly,
            research_program_hash: CanonicalDigest::content_hash_json(&(scope, "program"))?,
            decision_policy_snapshot_id: infra.decision_policy_snapshot_id,
            runtime_config_hash: PgPolicyRepository::new(db.clone())
                .load_snapshot(&infra.decision_policy_snapshot_id)
                .await?
                .context("feedback Source Slice runtime policy")?
                .snapshot_hash,
            window_start,
            window_end: cutoff,
        },
    )
    .await?;
    seed_source_manifest(db, &stored).await.map_err(Into::into)
}

async fn feedback_service(
    db: &DatabaseConnection,
    store: Arc<dyn ArtifactStore>,
    fact_read: Arc<ChQuantFactReadRepository>,
    decision_policy_snapshot_id: DecisionPolicySnapshotId,
) -> Result<FeedbackDatasetService> {
    let runtime = PgPolicyRepository::new(db.clone())
        .load_snapshot(&decision_policy_snapshot_id)
        .await?
        .context("feedback training runtime policy")?
        .snapshot;
    let training = TrainingDatasetService::new(
        TrainingDatasetServiceDeps {
            compute: Arc::new(ComputeExecutor::new().expect("feedback compute executor")),
            fact_read,
            catalog_repo: Arc::new(PgCatalogLedgerRepository::new(db.clone())),
            market_repo: Arc::new(PgMarketRepository::new(db.clone())),
            artifact_store: Arc::clone(&store),
            dataset_repo: Arc::new(PgTrainingDatasetRepository::new(db.clone())),
            position_repo: Arc::new(PgPositionRepository::new(db.clone())),
            clob_market_info_repo: Arc::new(PgClobMarketInfoRepository::new(db.clone())),
            linkage_repo: Arc::new(PgMarketLinkageRepository::new(db.clone())),
            model_registry: Arc::new(PgModelRegistryRepository::new(db.clone())),
            trade_policy_repo: Arc::new(PgTradePolicyRepository::new(db.clone())),
            calibration_repo: Arc::new(PgCalibrationArtifactRepository::new(db.clone())),
        },
        TrainingDatasetBuildConfig {
            features: runtime.profile_artifacts.features.definition,
            factors: runtime.profile_artifacts.scoring.definition,
            domain: runtime.profile_artifacts.domain.definition,
            data_quality: runtime.recommendation.data_quality,
            training: runtime.profile_artifacts.research_method.training,
            selection: runtime.recommendation.selection,
            labelers: default_labelers(),
            bias_table: None,
        },
        20_000,
    )?;
    Ok(FeedbackDatasetService::new(FeedbackDatasetServiceDeps {
        cohort_repository: Arc::new(PgFeedbackCohortRepository::new(db.clone())),
        feature_repository: Arc::new(PgFeatureRepository::new(db.clone())),
        factor_repository: Arc::new(PgFactorRepository::new(db.clone())),
        artifact_store: store,
        dataset_service: Arc::new(training),
    }))
}

fn reconciliation_service(
    db: &DatabaseConnection,
    ch: Arc<ClickHousePool>,
    facts: Arc<ChQuantFactReadRepository>,
    source: Arc<dyn ResolutionSourceReader>,
) -> OutcomeReconciliationService {
    let writer: Arc<dyn FactWriter<MarketResolutionRow>> = Arc::new(ChFactWriter::new(
        ch,
        Arc::new(ChWriteManager::new(2)),
        MARKET_RESOLUTION_EVENT,
    ));
    OutcomeReconciliationService::new(OutcomeReconciliationServiceDeps {
        resolution_source: source,
        resolution_fact_writer: writer,
        resolution_facts: facts,
        cursors: Arc::new(PgDomainSourceCursorRepository::new(db.clone())),
        markets: Arc::new(PgMarketRepository::new(db.clone())),
        resolution_outcomes: Arc::new(PgRecommendationResolutionOutcomeRepository::new(db.clone())),
        execution_outcomes: Arc::new(PgRecommendationExecutionOutcomeRepository::new(db.clone())),
    })
}

struct ScriptedResolutionSource {
    head: FinalizedResolutionBlock,
    scan: FinalizedResolutionScan,
}

#[async_trait]
impl ResolutionSourceReader for ScriptedResolutionSource {
    async fn finalized_head(&self) -> Result<FinalizedResolutionBlock, ResolutionSourceReadError> {
        Ok(self.head.clone())
    }

    async fn block_at_or_before(
        &self,
        _timestamp: DateTime<Utc>,
    ) -> Result<FinalizedResolutionBlock, ResolutionSourceReadError> {
        Ok(self.head.clone())
    }

    async fn scan_finalized(
        &self,
        from_block: u64,
        requested_to_block: u64,
    ) -> Result<Option<FinalizedResolutionScan>, ResolutionSourceReadError> {
        if from_block > self.scan.to_block {
            return Ok(None);
        }
        if from_block != self.scan.from_block || requested_to_block < self.scan.to_block {
            return Err(ResolutionSourceReadError::InvalidRange {
                from_block,
                to_block: requested_to_block,
            });
        }
        Ok(Some(self.scan.clone()))
    }
}

fn resolution_source(
    ids: &ExecutionTxnIds,
    block_number: u64,
    resolved_at: DateTime<Utc>,
    hash_seed: char,
) -> Arc<dyn ResolutionSourceReader> {
    let block = FinalizedResolutionBlock {
        block_number,
        block_hash: block_hash(hash_seed),
        block_time: resolved_at,
    };
    Arc::new(ScriptedResolutionSource {
        head: block.clone(),
        scan: FinalizedResolutionScan {
            from_block: block_number,
            to_block: block_number,
            to_block_hash: block.block_hash.clone(),
            to_block_time: block.block_time,
            observations: vec![FinalizedResolutionObservation {
                market_id: MarketId::new(&ids.market),
                vector: FinalizedResolutionVector::try_from_decimal_parts("2", ["1", "1"])
                    .expect("split resolution vector"),
                oracle: EvmAddress::parse("0x1111111111111111111111111111111111111111")
                    .expect("resolution oracle"),
                question_id: format!("0x{}", hash_seed.to_string().repeat(64)),
                transaction_hash: transaction_hash(hash_seed),
                block_number,
                block_hash: block.block_hash,
                log_index: block_number,
                resolved_at,
                source_checkpoint_hash: content_hash(hash_seed),
            }],
        },
    })
}

async fn seed_resolution_cursor(
    db: &DatabaseConnection,
    block_number: u64,
    block_time: DateTime<Utc>,
) -> Result<()> {
    let checkpoint_json = DomainSourceCheckpoint::PolymarketCtfResolution {
        finalized_block: block_number,
        block_hash: block_hash('0'),
        block_time,
    };
    let checkpoint_hash = CanonicalDigest::content_hash_json(&checkpoint_json)?;
    let outcome = PgDomainSourceCursorRepository::new(db.clone())
        .compare_and_set(
            None,
            UpsertDomainSourceCursor {
                source_id: DomainSourceId::polymarket_ctf_resolution(),
                instrument_key: DomainInstrumentKey::polymarket_ctf_resolution(),
                checkpoint_json,
                checkpoint_hash,
                status: DomainCursorStatus::Live,
                last_error: None,
                updated_at: Utc::now(),
            },
        )
        .await?;
    ensure!(
        matches!(outcome, DomainSourceCursorCasOutcome::Advanced(_)),
        "resolution cursor was not initialized"
    );
    Ok(())
}

const fn pass_config(
    pass_started_at: DateTime<Utc>,
    candidate_batch_size: u64,
) -> OutcomeReconciliationPassConfig {
    OutcomeReconciliationPassConfig {
        pass_started_at,
        candidate_batch_size,
        source_block_span: 32,
    }
}

fn feedback_window(
    window_start: DateTime<Utc>,
    cutoff: DateTime<Utc>,
) -> Result<FeedbackCohortWindow> {
    FeedbackCohortWindow::try_new(fixture_profile_ref(), window_start, cutoff).map_err(Into::into)
}

fn assert_cohort(
    artifact: &TrainingDatasetArtifact,
    candidate_count: u64,
    included_count: u64,
) -> Result<()> {
    let example_count =
        u64::try_from(artifact.examples.len()).context("feedback example count exceeds u64")?;
    let cohort = artifact
        .manifest
        .cohort_manifest
        .as_ref()
        .context("feedback Dataset cohort manifest")?;
    ensure!(
        cohort.counts.candidate_count() == candidate_count
            && cohort.counts.eligible_count() == included_count
            && cohort.counts.included_count() == included_count
            && cohort.artifact.row_count == included_count
            && example_count == included_count,
        "feedback cohort accounting does not reconcile"
    );
    Ok(())
}

async fn assert_storage_idempotency(
    db: &DatabaseConnection,
    ch: &ClickHousePool,
    reports: &[ExecutionTxnIds],
) -> Result<StorageCounts> {
    let dataset_count = pg_count(db, "quant_training_dataset").await?;
    let outcome_count = pg_count(db, "quant_recommendation_resolution_outcome").await?;
    let ch_count = ch
        .client()
        .query("SELECT count() FROM market_resolution_event")
        .fetch_one::<u64>()
        .await?;
    ensure!(
        dataset_count == 2 && outcome_count == 2 && ch_count == 2,
        "rerun storage cardinality drifted: feedback_datasets={dataset_count}, \
         resolution_outcomes={outcome_count}, resolution_facts={ch_count}"
    );
    let outcomes = PgRecommendationResolutionOutcomeRepository::new(db.clone());
    for ids in reports.iter().take(2) {
        let outcome = outcomes
            .find_by_recommendation(&ids.recommendation)
            .await?
            .context("reconciled recommendation outcome")?;
        ensure!(
            outcome.token_payout_ratio == PayoutRatio::try_new(dec!(0.5)).expect("split payout"),
            "split payout ratio changed across storage boundaries"
        );
    }
    Ok(StorageCounts {
        datasets: dataset_count,
        outcomes: outcome_count,
        resolution_facts: ch_count,
    })
}

#[derive(Clone, Copy)]
struct StorageCounts {
    datasets: u64,
    outcomes: u64,
    resolution_facts: u64,
}

fn print_evidence(
    first: &TrainingDatasetArtifact,
    next: &TrainingDatasetArtifact,
    counts: StorageCounts,
) -> Result<()> {
    let first_cohort = first
        .manifest
        .cohort_manifest
        .as_ref()
        .context("first feedback cohort manifest")?;
    let next_cohort = next
        .manifest
        .cohort_manifest
        .as_ref()
        .context("next feedback cohort manifest")?;
    println!(
        concat!(
            "W2_A11_EVIDENCE ",
            "first_dataset_id={} first_dataset_hash={} first_bytes_hash={} ",
            "first_manifest_hash={} first_source_identity_hash={} ",
            "first_source_manifest_hash={} first_cohort_source_hash={} ",
            "first_cohort_bytes_hash={} first_candidate_count={} first_included_count={} ",
            "next_dataset_id={} next_dataset_hash={} next_bytes_hash={} ",
            "next_manifest_hash={} next_source_identity_hash={} ",
            "next_source_manifest_hash={} next_cohort_source_hash={} ",
            "next_cohort_bytes_hash={} next_candidate_count={} next_included_count={} ",
            "pg_dataset_count={} pg_outcome_count={} ch_resolution_fact_count={}"
        ),
        first.training_dataset_id,
        first.dataset_hash,
        first.artifact_bytes_hash,
        dataset_manifest_hash(&first.manifest)?,
        first.manifest.source_lineage.source_slice_identity_hash,
        first.manifest.source_lineage.source_slice.manifest_hash,
        first_cohort.artifact.source_hash,
        first_cohort.artifact.bytes_hash,
        first_cohort.counts.candidate_count(),
        first_cohort.counts.included_count(),
        next.training_dataset_id,
        next.dataset_hash,
        next.artifact_bytes_hash,
        dataset_manifest_hash(&next.manifest)?,
        next.manifest.source_lineage.source_slice_identity_hash,
        next.manifest.source_lineage.source_slice.manifest_hash,
        next_cohort.artifact.source_hash,
        next_cohort.artifact.bytes_hash,
        next_cohort.counts.candidate_count(),
        next_cohort.counts.included_count(),
        counts.datasets,
        counts.outcomes,
        counts.resolution_facts,
    );
    Ok(())
}

async fn pg_count(db: &DatabaseConnection, table: &str) -> Result<u64> {
    let sql = match table {
        "quant_training_dataset" => {
            "SELECT COUNT(*) AS row_count
             FROM quant_training_dataset
             WHERE feedback_cohort = 'model_learning'"
        }
        "quant_recommendation_resolution_outcome" => {
            "SELECT COUNT(*) AS row_count FROM quant_recommendation_resolution_outcome"
        }
        _ => anyhow::bail!("unsupported W2-A11 count target {table}"),
    };
    let statement = Statement::from_string(DbBackend::Postgres, sql);
    let row = db
        .query_one_raw(statement)
        .await?
        .context("count query returned no row")?;
    let count = row.try_get::<i64>("", "row_count")?;
    u64::try_from(count).context("count is negative")
}

fn evidence_hash(recommendation: &NewRecommendation, role: &str) -> Result<ContentHash> {
    CanonicalDigest::content_hash_json(&(
        "w2-a11-serving-evidence",
        recommendation.recommendation_id,
        role,
    ))
    .map_err(Into::into)
}

fn artifact_root() -> PathBuf {
    env::temp_dir().join(format!(
        "quant-pivot-w2-a11-{}",
        TrainingDatasetId::from_v7()
    ))
}

fn content_hash(seed: char) -> ContentHash {
    ContentHash::parse(&format!("blake3:{}", seed.to_string().repeat(64))).expect("content hash")
}

fn block_hash(seed: char) -> EvmBlockHash {
    EvmBlockHash::parse(format!("0x{}", seed.to_string().repeat(64))).expect("block hash")
}

fn transaction_hash(seed: char) -> EvmTransactionHash {
    EvmTransactionHash::parse(format!("0x{}", seed.to_string().repeat(64)))
        .expect("transaction hash")
}
