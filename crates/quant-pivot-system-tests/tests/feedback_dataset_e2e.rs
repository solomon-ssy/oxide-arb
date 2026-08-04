//! Production-storage feedback outcome → cohort → Dataset contracts.

use std::{
    collections::BTreeMap,
    env, fs,
    path::{Path, PathBuf},
    result::Result as StdResult,
    sync::Arc,
    time::Duration as StdDuration,
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
    observability::serving_evidence::{
        ModelInputEvidenceBatch, completion_marker, feature_commitment, verify_completion,
    },
    projection::inference_context::build_market_inference_context,
    service::{
        feedback_dataset::{FeedbackDatasetService, FeedbackDatasetServiceDeps},
        training_dataset::{
            TrainingDatasetBuildConfig, TrainingDatasetService, TrainingDatasetServiceDeps,
            default_labelers,
        },
    },
};
use quant_pivot_error::storage::entity::MARKET_RESOLUTION_EVENT;
use quant_pivot_models::{
    clickhouse::{
        MarketResolutionRow, QuantFeatureEventRow, QuantModelInputEventRow,
        QuantServingEvidenceCompletionRow,
    },
    config::ClickHouseConfig,
    domain::{
        data_plane::{
            DecisionClock, DecisionSource, DomainCursorStatus, DomainSourceCheckpoint,
            DomainSourceCursorCasOutcome, UpsertDomainSourceCursor,
        },
        ports::FeedbackDatasetBuildRequest,
        quant::{
            FeedbackCohortWindow, FeedbackRecommendationContext, ModelVersionInfo,
            NewRecommendation, NoopProgressSink,
        },
    },
    enums::{
        catalog::CatalogTimestampQuality,
        feature::EvidenceSourceKind,
        market::MarketStatus,
        quant::{DataQualityStatus, DatasetPurpose, FeedbackCohort, QuantRuntimeMode},
    },
    hashing::CanonicalDigest,
    types::{
        CatalogDecisionRef, CatalogEventChangeId, CatalogMarketChangeId, CatalogSyncBatchId,
        ContentHash, DatasetSourceLineage, DecisionCaptureEvidence, DecisionPolicySnapshotId,
        DecisionSnapshotEvidence, DomainInstrumentKey, DomainSourceId, EvidenceSourceRef,
        EvmAddress, EvmBlockHash, EvmTransactionHash, FeatureCell, FeatureStaleness, FeatureValue,
        MarketId, PayoutRatio, Probability, ResearchEvaluationTrack, SchemaVersion,
        TrainingDatasetId, TrainingExampleId, TrainingSampleSource,
    },
};
use quant_pivot_repository::{
    clickhouse::{ChFactWriter, ChFeatureParityEventRepository, ChQuantFactReadRepository},
    postgres::{
        PgCalibrationArtifactRepository, PgCatalogLedgerRepository, PgClobMarketInfoRepository,
        PgDomainSourceCursorRepository, PgExecutionAttemptOutcomeRepository, PgFactorRepository,
        PgFeatureRepository, PgMarketLinkageRepository, PgMarketRepository,
        PgModelRegistryRepository, PgPolicyRepository, PgPositionRepository,
        PgRecommendationExecutionRollupRepository, PgRecommendationReportRepository,
        PgRecommendationRepository, PgRecommendationResolutionOutcomeRepository,
        PgResolutionObservationRepository, PgTradePolicyRepository, PgTrainingDatasetRepository,
    },
    traits::{
        DomainSourceCursorRepository, FactWriter, FactorRepository, FeatureRepository,
        MarketRepository, ModelRegistryRepository, PolicyRepository, QuantFactReadRepository,
        RecommendationReportRepository, RecommendationRepository,
        RecommendationResolutionOutcomeRepository, ServingEvidenceRepository,
    },
};
use quant_pivot_research::{
    artifact::{ArtifactStore, LocalArtifactStore},
    factors::{FactorValue, FactorValueInsertContext},
    features::{
        FeatureSchema, FeatureVector, feature_events,
        names::book::{BEST_ASK, MID, SECONDARY_BEST_ASK},
    },
    model::{FactorInferenceRow, FactorInferenceTable, WeightedInputAuditContract},
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
            ExecutionTxnIds, FEEDBACK_SCALE_REPORT_COUNT, ReportBuildOptions, ReportSeedConfig,
            SharedDemoInfra, build_custom_report_transaction, fixture_no_token_id,
            fixture_profile_ref, prepare_report_on_infra, seed_demo_with_store,
            seed_feedback_scale,
        },
        report_lifecycle_seed::{materialize_report_facts, persist_and_publish_report},
        research_fixtures::{
            ReplayableSourceSliceFixture, persist_replayable_source_slice, seed_source_manifest,
        },
    },
};
use rust_decimal_macros::dec;
use sea_orm::{ConnectionTrait, DatabaseConnection, DbBackend, Statement};
use tokio_util::sync::CancellationToken;

const FEATURE_EVENT_TABLE: &str = "quant_feature_event";
const MODEL_INPUT_EVENT_TABLE: &str = "quant_model_input_event";
const SERVING_COMPLETION_TABLE: &str = "quant_serving_evidence_completion";

struct ServingEvidenceSinks {
    features: Arc<dyn FactWriter<QuantFeatureEventRow>>,
    inputs: Arc<dyn FactWriter<QuantModelInputEventRow>>,
    completions: Arc<dyn FactWriter<QuantServingEvidenceCompletionRow>>,
}

impl ServingEvidenceSinks {
    fn new(ch: &Arc<ClickHousePool>, max_concurrent_inserts: usize) -> Self {
        let write_manager = Arc::new(ChWriteManager::new(max_concurrent_inserts));
        Self {
            features: Arc::new(ChFactWriter::new(
                Arc::clone(ch),
                Arc::clone(&write_manager),
                FEATURE_EVENT_TABLE,
            )),
            inputs: Arc::new(ChFactWriter::new(
                Arc::clone(ch),
                Arc::clone(&write_manager),
                MODEL_INPUT_EVENT_TABLE,
            )),
            completions: Arc::new(ChFactWriter::new(
                Arc::clone(ch),
                write_manager,
                SERVING_COMPLETION_TABLE,
            )),
        }
    }
}

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
    let runtime = PgPolicyRepository::new(db.clone())
        .load_snapshot(&infra.decision_policy_snapshot_id)
        .await?
        .context("seeded runtime policy")?
        .snapshot;
    let schema = FeatureSchema::build(&runtime.profile_artifacts.features.definition)?;
    let sinks = ServingEvidenceSinks::new(&ch, stack.clickhouse_config.max_concurrent_inserts);
    let population = seed_feedback_population(
        &FeedbackReportSeedContext {
            db: &db,
            infra: &infra,
            artifacts: &store,
            clickhouse: &stack.clickhouse_config,
            schema: &schema,
            model: &model,
            sinks: &sinks,
        },
        FEEDBACK_SCALE_REPORT_COUNT,
    )
    .await?;
    assert_publication_history(&db, &population.reports).await?;
    let reconciliation = reconcile_outcomes(
        &db,
        Arc::clone(&ch),
        Arc::clone(&facts),
        &population.reports,
    )
    .await?;
    assert_scored_population(
        &db,
        ch.as_ref(),
        facts.as_ref(),
        reconciliation.window_start,
        reconciliation.frozen_cutoff,
    )
    .await?;

    let dataset_service = feedback_service(
        &db,
        Arc::clone(&store),
        Arc::clone(&facts),
        Arc::clone(&ch),
        infra.decision_policy_snapshot_id,
    )
    .await?;
    let old_source = source_lineage(
        &db,
        &store,
        &[source_example(
            &population.serving[0],
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
    let first_artifact = assert_dataset_replay(
        &dataset_service,
        &store,
        old_request,
        u64::try_from(FEEDBACK_SCALE_REPORT_COUNT)?,
    )
    .await?;

    let next_source = source_lineage(
        &db,
        &store,
        &[
            source_example(&population.serving[0], reconciliation.first_resolved_at),
            source_example(&population.serving[1], reconciliation.second_resolved_at),
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
    assert_cohort(
        &next_artifact,
        u64::try_from(FEEDBACK_SCALE_REPORT_COUNT)?,
        2,
    )?;
    ensure!(
        first_artifact.dataset_hash != next_artifact.dataset_hash,
        "next cutoff did not incorporate the late outcome"
    );
    let storage_counts = assert_storage_idempotency(&db, ch.as_ref(), &population.reports).await?;
    print_evidence(&first_artifact, &next_artifact, storage_counts)?;
    Ok(())
}

async fn assert_dataset_replay(
    service: &FeedbackDatasetService,
    store: &Arc<dyn ArtifactStore>,
    request: FeedbackDatasetBuildRequest,
    expected_population: u64,
) -> Result<TrainingDatasetArtifact> {
    let first = Box::pin(service.build(
        request.clone(),
        Arc::new(NoopProgressSink),
        CancellationToken::new(),
    ))
    .await?;
    assert_cohort(&first, expected_population, 1)?;
    let replayed = Box::pin(service.build(
        request,
        Arc::new(NoopProgressSink),
        CancellationToken::new(),
    ))
    .await?;
    ensure!(
        replayed == first,
        "same frozen request did not reload the exact immutable Dataset"
    );
    let bytes = store.get(&first.parquet_uri).await?;
    ensure!(
        CanonicalDigest::content_hash_bytes(&bytes) == first.artifact_bytes_hash,
        "persisted Dataset byte hash does not reproduce"
    );
    ensure!(
        dataset_manifest_hash(&first.manifest)? == dataset_manifest_hash(&replayed.manifest)?,
        "same frozen request changed its manifest hash"
    );
    Ok(first)
}

struct ReconciledOutcomes {
    window_start: DateTime<Utc>,
    frozen_cutoff: DateTime<Utc>,
    next_cutoff: DateTime<Utc>,
    first_resolved_at: DateTime<Utc>,
    second_resolved_at: DateTime<Utc>,
}

async fn assert_scored_population(
    db: &DatabaseConnection,
    ch: &ClickHousePool,
    facts: &ChQuantFactReadRepository,
    window_start: DateTime<Utc>,
    cutoff: DateTime<Utc>,
) -> Result<()> {
    let reports = PgRecommendationReportRepository::new(db.clone())
        .list_committed_between(window_start, cutoff)
        .await?;
    let profile_ref = fixture_profile_ref();
    let funnels = facts
        .report_funnel_between(
            &profile_ref,
            window_start.timestamp_millis(),
            cutoff.timestamp_millis(),
        )
        .await?;
    let raw_funnel_count = ch
        .client()
        .query("SELECT count() FROM quant_report_market_funnel")
        .fetch_one::<u64>()
        .await?;
    let sample = match reports.first() {
        Some(report) => facts
            .report_market_funnel_page(&report.recommendation_report_id, None, None, 0, 1)
            .await?
            .into_iter()
            .next(),
        None => None,
    };
    ensure!(
        reports.len() == FEEDBACK_SCALE_REPORT_COUNT
            && funnels.len() == FEEDBACK_SCALE_REPORT_COUNT,
        "complete scored-serving population is not queryable at the frozen cutoff: reports={} filtered_funnels={} raw_funnels={} window=[{}, {}) expected_profile={profile_ref:?} sample={sample:?}",
        reports.len(),
        funnels.len(),
        raw_funnel_count,
        window_start,
        cutoff,
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
    let first_resolved_at = floor_millisecond(db.statement_time().await)?;
    seed_resolution_cursor(db, 100, first.decision_at - Duration::seconds(1)).await?;
    let first_reconciliation = reconciliation_service(
        db,
        Arc::clone(&ch),
        Arc::clone(&facts),
        resolution_source(first, 101, first_resolved_at, '1'),
    );
    let first_summary = first_reconciliation
        .run_resolution_pass(pass_config(db.statement_time().await, 1))
        .await?;
    ensure!(
        first_summary.source_facts_written == 1
            && first_summary.resolution_inserted == 0
            && first_summary.resolution_deferred == 1,
        "first production reconciliation did not preserve its PIT boundary"
    );
    release_resolution_retries(db).await?;
    let first_outcome = first_reconciliation
        .run_resolution_pass(pass_config(db.statement_time().await, 1))
        .await?;
    ensure!(
        first_outcome.resolution_inserted == 1 && first_outcome.resolution_deferred == 0,
        "next frozen window did not seal the first PG outcome"
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
    let second_resolved_at = floor_millisecond(db.statement_time().await)?;
    let second_reconciliation = reconciliation_service(
        db,
        ch,
        facts,
        resolution_source(second, 102, second_resolved_at, '2'),
    );
    let second_summary = second_reconciliation
        .run_resolution_pass(pass_config(db.statement_time().await, 1))
        .await?;
    ensure!(
        second_summary.source_facts_written == 1
            && second_summary.resolution_inserted == 0
            && second_summary.resolution_deferred == 1,
        "late production reconciliation crossed its PIT boundary"
    );
    release_resolution_retries(db).await?;
    let second_outcome = second_reconciliation
        .run_resolution_pass(pass_config(db.statement_time().await, 1))
        .await?;
    ensure!(
        second_outcome.resolution_inserted == 1 && second_outcome.resolution_deferred == 0,
        "next frozen window did not seal the late PG outcome"
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

struct FeedbackReportSeedContext<'a> {
    db: &'a DatabaseConnection,
    infra: &'a SharedDemoInfra,
    artifacts: &'a Arc<dyn ArtifactStore>,
    clickhouse: &'a ClickHouseConfig,
    schema: &'a FeatureSchema,
    model: &'a ModelVersionInfo,
    sinks: &'a ServingEvidenceSinks,
}

struct SeededFeedbackPopulation {
    reports: Vec<ExecutionTxnIds>,
    serving: Vec<ServingSeed>,
}

async fn seed_feedback_population(
    context: &FeedbackReportSeedContext<'_>,
    count: usize,
) -> Result<SeededFeedbackPopulation> {
    let mut reports = Vec::with_capacity(count);
    let mut serving = Vec::with_capacity(count);
    for ordinal in 0..count {
        let (ids, evidence) = Box::pin(seed_feedback_report(context, ordinal)).await?;
        reports.push(ids);
        serving.push(evidence);
    }
    Ok(SeededFeedbackPopulation { reports, serving })
}

async fn seed_feedback_report(
    context: &FeedbackReportSeedContext<'_>,
    ordinal: usize,
) -> Result<(ExecutionTxnIds, ServingSeed)> {
    let db = context.db;
    let infra = context.infra;
    let artifacts = context.artifacts;
    let clickhouse = context.clickhouse;
    let schema = context.schema;
    let model = context.model;
    let sinks = context.sinks;
    let config = ReportSeedConfig {
        event_id: format!("feedback-dataset-event-{ordinal}"),
        market_id: format!("0xfeedback-dataset-market-{ordinal}"),
        market_question: format!("Will feedback Dataset fixture {ordinal} settle?"),
        market_slug: format!("feedback-dataset-fixture-{ordinal}"),
        token_id: format!("{}", 90_000 + ordinal),
        trigger_key: format!("feedback-dataset:{ordinal}"),
    };
    let database_now = db.statement_time().await;
    let profile = fixture_profile_ref()
        .resolve_builtin_research_profile()
        .map_err(AnyhowError::msg)?;
    let horizon_secs = i64::try_from(profile.spec.target_horizon_secs)
        .context("feedback profile horizon exceeds chrono's signed range")?;
    let historical_decision_at = database_now
        .checked_sub_signed(Duration::seconds(horizon_secs))
        .and_then(|value| value.checked_sub_signed(Duration::hours(1)))
        .context("feedback decision timestamp underflow")?;
    let decision_at = DateTime::from_timestamp_millis(historical_decision_at.timestamp_millis())
        .context("database time is outside chrono's millisecond range")?;
    let ids = prepare_report_on_infra(db, infra, &config, decision_at).await;
    let mut options = ReportBuildOptions::published_single(&ids);
    if ordinal == 0 {
        options.runtime_mode = QuantRuntimeMode::ReportOnly;
    }
    let recommendation = options
        .recommendations
        .first()
        .context("prepared feedback recommendation")?;
    let serving = serving_evidence(&ids, recommendation)?;
    let mut feature = serving
        .feature_vector
        .try_to_new(&serving.capture.snapshot.boundary, &serving.capture)?;
    feature.feature_vector_id = recommendation.evidence_refs.feature_vector_id;
    let persisted_feature = PgFeatureRepository::new(db.clone()).create(feature).await?;
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
    let event_time = ids.decision_at.timestamp_millis();
    let evidence_ingestion_time = next_millisecond(db.statement_time().await)?.timestamp_millis();
    let feature_rows = feature_events(
        &serving.feature_vector,
        &persisted_feature,
        &serving.capture.snapshot.boundary,
        &ids.decision_policy_snapshot,
        schema,
        evidence_ingestion_time,
    )?;
    let feature_vector_ids = [recommendation.evidence_refs.feature_vector_id];
    let feature_evidence =
        feature_commitment(&feature_rows)?.bind_model_vectors(&feature_vector_ids)?;
    let inference_context =
        build_market_inference_context(&serving.feature_vector, &serving.selected_market)
            .context("feedback Dataset fixture has no executable inference context")?;
    let input_table = FactorInferenceTable {
        model_run_id: ids.model_run,
        decision_at: ids.decision_at,
        rows: vec![FactorInferenceRow {
            market_id: recommendation.market_id.clone(),
            token_id: recommendation.token_id.clone(),
            factors: factor_values.clone(),
            context: inference_context,
        }],
    };
    let serving_contract = model.verified_serving_contract()?;
    let bindings = serving_contract.bindings();
    let input_audit = input_table.weighted_input_audit(WeightedInputAuditContract {
        model_version_id: model.model_version_id,
        input_contract_hash: bindings.transform.input_contract_hash,
        transform_hash: bindings.transform.input_transform_hash,
        training_input_hash: bindings.transform.training_input_hash,
    })?;
    let vectors = [serving.feature_vector.clone()];
    let input_rows = ModelInputEvidenceBatch::try_new(&vectors, &feature_vector_ids)?.project(
        &ids.model_run,
        &serving.capture.snapshot.boundary,
        &input_audit,
        event_time,
    )?;
    let completion = completion_marker(
        &ids.model_run,
        &serving.capture.snapshot.boundary,
        &feature_evidence,
        &input_rows,
        evidence_ingestion_time,
    )?;
    verify_completion(&completion, &feature_rows, &input_rows)?;
    sinks.features.write_batch(feature_rows).await?;
    sinks.inputs.write_batch(input_rows).await?;
    sinks.completions.write_batch(vec![completion]).await?;
    ids.complete_model_run(db).await;
    let mut transaction = build_custom_report_transaction(&ids, options);
    materialize_report_facts(artifacts, clickhouse, &mut transaction).await?;
    persist_and_publish_report(db, transaction, &config.trigger_key, 10).await;
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
        generic: BTreeMap::from([
            (
                BEST_ASK,
                FeatureCell::observed(
                    FeatureValue::Probability(Probability::new(dec!(0.43))),
                    Some(source_ref.clone()),
                    FeatureStaleness::Known { age_ms: 0 },
                ),
            ),
            (
                MID,
                FeatureCell::observed(
                    FeatureValue::Probability(Probability::new(dec!(0.42))),
                    Some(source_ref.clone()),
                    FeatureStaleness::Known { age_ms: 0 },
                ),
            ),
            (
                SECONDARY_BEST_ASK,
                FeatureCell::observed(
                    FeatureValue::Probability(Probability::new(dec!(0.59))),
                    Some(source_ref),
                    FeatureStaleness::Known { age_ms: 0 },
                ),
            ),
        ]),
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
        sample_source: TrainingSampleSource::PublishedDecisionDiagnostic,
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
    ch: Arc<ClickHousePool>,
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
            fact_read: Arc::clone(&fact_read) as Arc<dyn QuantFactReadRepository>,
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
        report_repository: Arc::new(PgRecommendationReportRepository::new(db.clone())),
        fact_repository: fact_read,
        serving_evidence_repository: Arc::new(ChFeatureParityEventRepository::new(ch))
            as Arc<dyn ServingEvidenceRepository>,
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
        resolution_observations: Arc::new(PgResolutionObservationRepository::new(db.clone())),
        markets: Arc::new(PgMarketRepository::new(db.clone())),
        resolution_outcomes: Arc::new(PgRecommendationResolutionOutcomeRepository::new(db.clone())),
        execution_outcomes: Arc::new(PgExecutionAttemptOutcomeRepository::new(db.clone())),
        execution_rollups: Arc::new(PgRecommendationExecutionRollupRepository::new(db.clone())),
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

async fn release_resolution_retries(db: &DatabaseConnection) -> Result<()> {
    db.execute_raw(Statement::from_string(
        DbBackend::Postgres,
        "UPDATE quant_resolution_outcome_reconciliation_task \
         SET next_attempt_at = statement_timestamp() + INTERVAL '1 millisecond' \
         WHERE status = 'retrying'",
    ))
    .await?;
    tokio::time::sleep(StdDuration::from_millis(5)).await;
    Ok(())
}

fn feedback_window(
    window_start: DateTime<Utc>,
    cutoff: DateTime<Utc>,
) -> Result<FeedbackCohortWindow> {
    FeedbackCohortWindow::try_new(fixture_profile_ref(), window_start, cutoff).map_err(Into::into)
}

fn next_millisecond(value: DateTime<Utc>) -> Result<DateTime<Utc>> {
    let timestamp = value
        .timestamp_millis()
        .checked_add(1)
        .context("millisecond timestamp overflow")?;
    DateTime::from_timestamp_millis(timestamp).context("timestamp is outside chrono's range")
}

fn floor_millisecond(value: DateTime<Utc>) -> Result<DateTime<Utc>> {
    DateTime::from_timestamp_millis(value.timestamp_millis())
        .context("timestamp is outside chrono's range")
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
        cohort.cohort == FeedbackCohort::ModelScoreLearning
            && cohort.counts.candidate_count() == candidate_count
            && cohort.counts.eligible_count() == included_count
            && cohort.counts.included_count() == included_count
            && cohort.artifact.row_count == included_count
            && example_count == included_count
            && artifact.coverage.planned_samples == candidate_count
            && artifact.coverage.built_examples == included_count
            && artifact.coverage.labels_available == included_count
            && artifact.coverage.labels_not_mature == candidate_count - included_count
            && artifact
                .examples
                .iter()
                .all(|example| example.sample_source == TrainingSampleSource::ModelScoreFeedback),
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
             WHERE feedback_cohort = 'model_score_learning'"
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
