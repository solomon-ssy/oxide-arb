//! Production-storage feedback outcome → cohort → Dataset contracts.

use std::{
    collections::BTreeMap,
    env, fs,
    path::{Path, PathBuf},
    sync::Arc,
};

use anyhow::{Context, Result, ensure};
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
        feedback_dataset::{
            FeedbackDatasetBuildRequest, FeedbackDatasetService, FeedbackDatasetServiceDeps,
        },
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
        data_plane::{
            DecisionClock, DecisionSource, DomainCursorStatus, DomainSourceCheckpoint,
            DomainSourceCursorCasOutcome, UpsertDomainSourceCursor,
        },
        quant::{
            FeedbackCohortWindow, FeedbackRecommendationContext, NewRecommendation,
            NoopProgressSink,
        },
    },
    enums::{
        catalog::CatalogTimestampQuality,
        common::MarketCategory,
        factor::FactorFamily,
        feature::EvidenceSourceKind,
        market::MarketStatus,
        quant::{DataQualityStatus, DatasetPurpose},
    },
    hashing::CanonicalDigest,
    runtime_config::{
        DataQualityConfig, DomainConfig, FactorsConfig, FeatureFamily, FeaturesConfig,
        SelectionConfig, TrainingConfig,
    },
    types::{
        CatalogDecisionRef, CatalogEventChangeId, CatalogMarketChangeId, CatalogSyncBatchId,
        ContentHash, DatasetSourceLineage, DecisionCaptureEvidence, DecisionSnapshotEvidence,
        DomainInstrumentKey, DomainSourceId, EvidenceSourceRef, EvmAddress, EvmBlockHash,
        EvmTransactionHash, FeatureCell, FeatureStaleness, FeatureValue, MarketId, PayoutRatio,
        Probability, RecommendationFactorBreakdown, SchemaVersion, TrainingDatasetId,
        TrainingExampleId, TrainingSampleSource,
    },
};
use quant_pivot_repository::{
    clickhouse::{ChFactWriter, ChQuantFactReadRepository},
    postgres::{
        PgCalibrationArtifactRepository, PgCatalogLedgerRepository, PgClobMarketInfoRepository,
        PgDomainSourceCursorRepository, PgFactorRepository, PgFeatureRepository,
        PgFeedbackCohortRepository, PgMarketLinkageRepository, PgMarketRepository,
        PgModelRegistryRepository, PgPositionRepository,
        PgRecommendationExecutionOutcomeRepository, PgRecommendationReportRepository,
        PgRecommendationRepository, PgRecommendationResolutionOutcomeRepository,
        PgTradePolicyRepository, PgTrainingDatasetRepository,
    },
    traits::{
        DomainSourceCursorRepository, FactWriter, FeatureRepository, MarketRepository,
        ModelRegistryRepository, RecommendationReportRepository, RecommendationRepository,
        RecommendationResolutionOutcomeRepository,
    },
};
use quant_pivot_research::{
    artifact::{ArtifactStore, LocalArtifactStore},
    features::{FeatureVector, names::book::MID},
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
        execution_pg_seed::{
            ExecutionTxnIds, FEEDBACK_SCALE_REPORT_COUNT, FEEDBACK_SCALE_TOTAL, ReportBuildOptions,
            ReportSeedConfig, SharedDemoInfra, build_custom_report_transaction,
            fixture_no_token_id, fixture_profile_ref, prepare_report_on_infra,
            seed_demo_with_store, seed_feedback_scale,
        },
        report_lifecycle_seed::persist_and_publish_report,
        research_fixtures::{
            ReplayableSourceSliceFixture, persist_replayable_source_slice, seed_source_manifest,
        },
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
    let store: Arc<dyn ArtifactStore> =
        Arc::new(LocalArtifactStore::new(artifact_root.to_path_buf()));
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

    let dataset_service = feedback_service(&db, Arc::clone(&store), facts);
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
    let mut options = ReportBuildOptions::published_single(&ids);
    let recommendation = options
        .recommendations
        .first_mut()
        .context("prepared feedback recommendation")?;
    recommendation.factor_breakdown = RecommendationFactorBreakdown(Vec::new());
    let serving = serving_evidence(&ids, recommendation)?;
    let mut feature = serving
        .feature_vector
        .try_to_new(&serving.capture.snapshot.boundary, &serving.capture)?;
    feature.feature_vector_id = recommendation.evidence_refs.feature_vector_id;
    PgFeatureRepository::new(db.clone()).create(feature).await?;
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
        factor_values: Vec::new(),
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
            research_program_hash: CanonicalDigest::content_hash_json(&(scope, "program"))?,
            decision_policy_snapshot_id: infra.decision_policy_snapshot_id,
            runtime_config_hash: CanonicalDigest::content_hash_json(&(scope, "runtime"))?,
            window_start,
            window_end: cutoff,
        },
    )
    .await?;
    seed_source_manifest(db, &stored).await.map_err(Into::into)
}

fn feedback_service(
    db: &DatabaseConnection,
    store: Arc<dyn ArtifactStore>,
    fact_read: Arc<ChQuantFactReadRepository>,
) -> FeedbackDatasetService {
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
            features: FeaturesConfig {
                enabled_feature_families: vec![
                    FeatureFamily::PriceBook,
                    FeatureFamily::MarketMetadata,
                ],
                ..FeaturesConfig::default()
            },
            factors: FactorsConfig {
                enabled_factor_families: vec![FactorFamily::DataQuality],
                ..FactorsConfig::default()
            },
            domain: DomainConfig::default(),
            data_quality: DataQualityConfig::default(),
            training: TrainingConfig::default(),
            selection: SelectionConfig {
                enabled_categories: vec![MarketCategory::Politics],
                ..SelectionConfig::default()
            },
            labelers: default_labelers(),
            bias_table: None,
        },
        20_000,
    )
    .expect("feedback training service");
    FeedbackDatasetService::new(FeedbackDatasetServiceDeps {
        cohort_repository: Arc::new(PgFeedbackCohortRepository::new(db.clone())),
        feature_repository: Arc::new(PgFeatureRepository::new(db.clone())),
        factor_repository: Arc::new(PgFactorRepository::new(db.clone())),
        artifact_store: store,
        dataset_service: Arc::new(training),
    })
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
        "rerun created duplicate Dataset/outcome/fact rows"
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
        "quant_training_dataset" => "SELECT COUNT(*) AS row_count FROM quant_training_dataset",
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
