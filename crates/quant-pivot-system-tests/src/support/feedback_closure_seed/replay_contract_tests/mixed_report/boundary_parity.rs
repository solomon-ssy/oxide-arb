//! A real published report preserves fractional PIT clocks through full replay.

use std::{
    collections::{BTreeMap, BTreeSet, HashSet},
    sync::Arc,
    time::Duration as StdDuration,
};

use anyhow::{Context, Result, bail, ensure};
use async_trait::async_trait;
use chrono::{DateTime, Duration, Utc};
use quant_pivot_core::{
    ingest::{data_plane_index::DataPlane, market_registry::MarketRegistry},
    observability::{
        feature_fact_writer::FeatureEventWriter, metrics_hub::MetricsHub,
        model_input_fact_writer::ModelInputEventWriter,
    },
    pit::platform::ch_historical::DurablePitSource,
    prefetch::market_candidates::MarketCandidateProvider,
    report::{
        BuildReportRequest, ComposedReport, DefaultRecommendationComposer, ReportBuilder,
        ReportTrigger,
    },
    service::{
        durable_feature_parity::{DurableFeatureParityDeps, DurableFeatureParitySource},
        feature_integrity::{
            FeatureParityGatePort, FeatureParityRunCoordinator, RepositoryFeatureParityGate,
        },
        feature_parity_executor::{
            FeatureParityCandidate, FeatureParityExecutor, FeatureParityIncidentPort,
            FeatureParityInputWitness, FeatureParityReplaySource, FeatureParitySubject,
        },
    },
};
use quant_pivot_error::{QuantResult, research::ResearchError};
use quant_pivot_models::{
    clickhouse::QuantFeatureParityEventRow,
    domain::{
        api::{FeatureParityEventListQuery, FeatureParityEventView, FeatureParityRunView},
        data_plane::DecisionSource,
        pagination::PageRequest,
        ports::{FeatureParityExecutionOutcome, FeatureParityExecutionPort},
        quant::{FeatureParityRunInfo, NoopProgressSink, RecommendationReportInfo, ReportRunClaim},
    },
    entities::{
        quant_feature_parity_subject::{
            Column as ParitySubjectColumn, Entity as ParitySubjectEntity,
        },
        quant_market_selection_member::{
            Column as SelectionMemberColumn, Entity as SelectionMemberEntity,
        },
        quant_model_run::{Column as ModelRunColumn, Entity as ModelRunEntity},
    },
    enums::quant::{
        FeatureParityEventStatus, FeatureParityRunKind, FeatureParityRunStatus, FeatureParityStage,
        ParitySubjectKind, RecommendationReportStatus,
    },
    runtime_config::BuyModelRoute,
    types::{
        CorrelationId, FeatureParityDetail, FeatureParityDetailSource, FeatureVectorId, ModelRunId,
        RecommendationReportId, ReportRunId, ResearchJobId, ResearchJobParams, Usd, WorkerId,
        stable_name::FeatureName,
    },
};
use quant_pivot_repository::{
    clickhouse::{ChFactWriter, ChFeatureParityEventRepository},
    postgres::{
        PgCatalogLedgerRepository, PgClobMarketInfoRepository, PgExchangeHistoryRepository,
        PgFeatureParityRepository, PgMarketLinkageRepository, PgPolicyRepository,
        PgRecommendationReportRepository, PgReportRunRepository, PgResearchJobRepository,
    },
    traits::{
        FactWriter, FeatureParityEventRepository, FeatureParityRepository,
        RecommendationReportRepository, ReportRunRepository, ResearchJobRepository,
    },
};
use quant_pivot_research::artifact::{ArtifactStore, LocalArtifactStore};
use quant_pivot_storage::clickhouse::{ChWriteManager, ClickHousePool};
use rust_decimal_macros::dec;
use sea_orm::{ColumnTrait, DatabaseConnection, EntityTrait, QueryFilter};
use serde_json::json;
use tempfile::TempDir;
use tokio::time::{Instant, timeout_at};
use tokio_util::sync::CancellationToken;

use super::{
    super::{
        super::{ClosureFactWriters, await_database_time},
        ReplayContractFixture,
    },
    MixedReportFixture,
};
use crate::{
    stack::SystemStack,
    support::{
        artifact_store::VersionedArtifactStoreFixture,
        report_lifecycle_seed::{ReportRunFixture, seal_report_facts},
        report_pipeline_harness::{
            HarnessOptions, ReportBuilderHarnessInput, ReportEvidenceWriters, account_factory,
            build_model_runner, build_report_builder, calibration_artifact_loader,
            ensure_harness_execution_account,
        },
        trade_policy_fixtures::FixtureBookTiming,
    },
};

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fractional_report_replays() -> Result<()> {
    let stack = Box::pin(SystemStack::start()).await?;
    let directory = TempDir::with_prefix("quant-pivot-boundary-parity-")?;
    let store: Arc<dyn ArtifactStore> = Arc::new(VersionedArtifactStoreFixture::new(Arc::new(
        LocalArtifactStore::new(directory.path().to_owned()),
    )));
    let deadline = Instant::now() + StdDuration::from_mins(10);
    let result = async {
        let fixture = Box::pin(MixedReportFixture::bootstrap(
            stack.postgres.connection().clone(),
            stack.clickhouse_config.clone(),
            Arc::clone(&store),
            deadline,
        ))
        .await?;
        timeout_at(
            deadline,
            Box::pin(async {
                let proof = Box::pin(fixture.build_boundary(&stack, &store)).await?;
                Box::pin(proof.verify(&fixture, &stack, &store)).await
            }),
        )
        .await
        .context("fractional report replay exceeded its ten-minute fixture budget")?
    }
    .await;
    drop(store);
    let shutdown = Box::pin(stack.shutdown()).await;
    let cleanup = directory.close();
    println!(
        "fractional-report-cleanup stack_ok={} artifacts_ok={}",
        shutdown.is_ok(),
        cleanup.is_ok()
    );
    result?;
    shutdown?;
    cleanup.context("remove fractional report replay artifacts")?;
    Ok(())
}

impl MixedReportFixture {
    async fn build_boundary(
        &self,
        stack: &SystemStack,
        store: &Arc<dyn ArtifactStore>,
    ) -> Result<PublishedBoundaryProof> {
        let db = stack.postgres.connection();
        RepositoryFeatureParityGate::new(Arc::new(PgFeatureParityRepository::new(db.clone())))
            .ensure_clear("new report generation")
            .await?;
        ensure_harness_execution_account(db).await;
        let feature_facts = Arc::clone(&self.setup.fact_writers.features);
        let input_facts = Arc::clone(&self.setup.fact_writers.inputs);
        let completions = Arc::clone(&self.setup.fact_writers.completions);
        let evidence = ReportEvidenceWriters {
            features: Arc::new(FeatureEventWriter::new(feature_facts)),
            model_inputs: Arc::new(ModelInputEventWriter::new(input_facts, completions)),
        };
        let model_runner = Box::pin(build_model_runner(
            db,
            store,
            Arc::clone(&evidence.model_inputs),
        ))
        .await;
        let registry = Arc::new(MarketRegistry::new(Arc::new(DataPlane::new())));
        let accounts = account_factory(
            db,
            registry,
            &HarnessOptions {
                collateral: Usd::new(dec!(555.56)),
                ..HarnessOptions::default()
            },
        );
        let facts = Arc::clone(&self.setup.fact_writers.fact_read);
        let pit = Arc::new(DurablePitSource::new(
            Arc::clone(&facts),
            Arc::new(PgCatalogLedgerRepository::new(db.clone())),
            Arc::new(PgClobMarketInfoRepository::new(db.clone())),
        ));
        let builder = build_report_builder(ReportBuilderHarnessInput {
            db,
            runtime_config_repo: Arc::new(PgPolicyRepository::new(db.clone())),
            candidate_provider: Arc::new(MarketCandidateProvider::new(
                pit,
                Arc::new(PgMarketLinkageRepository::new(db.clone())),
                Arc::clone(&facts),
            )),
            model_runner,
            account_factory: accounts,
            artifact_store: Arc::clone(store),
            calibration_loader: calibration_artifact_loader(db),
            feature_writer: evidence.features,
            exchange_history_repo: Arc::new(PgExchangeHistoryRepository::new(db.clone())),
            fact_read: facts,
            composer: Arc::new(DefaultRecommendationComposer::new()),
        });
        // Exercise R7's actual millisecond decision clock against whole-second
        // finalized block watermarks. Submillisecond recovery has separate core tests.
        let decision_at = Self::future_crypto_decision(db).await? + Duration::milliseconds(931);
        await_database_time(db, decision_at).await?;
        self.persist_sources(db, decision_at).await?;
        let trigger = ReportTrigger::AdHoc {
            request_id: CorrelationId::new("fractional-report-parity"),
        };
        let report = builder
            .build(BuildReportRequest {
                report_run_id: ReportRunId::from_v7(),
                trigger: trigger.clone(),
                trigger_time: decision_at,
                top_n_override: Some(10),
                knowledge_lag_secs_override: Some(FixtureBookTiming::REPORT_LAG_SECS),
            })
            .await?;
        ensure!(
            !report.transaction.recommendations.is_empty(),
            "normal report must publish real recommendations"
        );
        ensure!(
            report.funnel_rows.len() == 10,
            "normal report must preserve all ten terminal rows, including risk rejections"
        );
        PublishedBoundaryProof::publish(db, store, &self.setup.fact_writers, report, &trigger).await
    }
}

struct PublishedBoundaryProof {
    report: RecommendationReportInfo,
    vector_ids: Vec<FeatureVectorId>,
    sampled_job_id: ResearchJobId,
}

impl PublishedBoundaryProof {
    async fn publish(
        db: &DatabaseConnection,
        store: &Arc<dyn ArtifactStore>,
        facts: &ClosureFactWriters,
        mut report: ComposedReport,
        trigger: &ReportTrigger,
    ) -> Result<Self> {
        let report_id = report.transaction.report.recommendation_report_id;
        let decision_at = report.transaction.report.decision_at;
        let vector_ids = report
            .transaction
            .data_quality_snapshot
            .tokens_json
            .0
            .iter()
            .map(|row| row.feature_vector_id)
            .collect::<Vec<_>>();
        let run = ReportRunFixture::new(db)
            .create_claimed(
                &report.transaction,
                trigger,
                i64::try_from(FixtureBookTiming::REPORT_LAG_SECS)?,
            )
            .await?;
        let parity: Arc<dyn FeatureParityRepository> =
            Arc::new(PgFeatureParityRepository::new(db.clone()));
        let coordinator = FeatureParityRunCoordinator::new(
            Arc::clone(&parity),
            Arc::new(PgPolicyRepository::new(db.clone())),
            3,
        );
        let sampled = coordinator
            .build_report_sample(&report.transaction.report, &run)
            .await?;
        let sampled_job_id = sampled.job.job_id;
        report.transaction.sampled_feature_parity = Some(sampled);
        report.transaction.feature_parity_state_id = Some(
            RepositoryFeatureParityGate::new(parity)
                .commit_state_id("report commit")
                .await?,
        );
        seal_report_facts(
            store,
            &mut report.transaction,
            report.ch_rows.clone(),
            report.funnel_rows.clone(),
        )
        .await?;
        facts
            .commit_report(report.ch_rows, report.funnel_rows)
            .await?;
        let repository = PgRecommendationReportRepository::new(db.clone());
        repository
            .create_prepared_report(
                ReportRunClaim {
                    report_run_id: run.report_run_id,
                    lease_owner: run.lease_owner.context("actual report run lease owner")?,
                    lease_expires_at: run
                        .lease_expires_at
                        .context("actual report run lease deadline")?,
                },
                report.transaction,
            )
            .await?;
        let worker = WorkerId::from_v7();
        let claimed = repository
            .claim_fact_delivery(worker, 600)
            .await?
            .context("new report fact delivery claim")?;
        ensure!(
            claimed.recommendation_report_id == report_id,
            "delivery claim belongs to another report"
        );
        let now = PgReportRunRepository::new(db.clone())
            .database_time()
            .await?;
        let publication = repository
            .verify_and_publish_report(&report_id, worker, now)
            .await?
            .into_applied();
        let published = match publication {
            Ok(applied) => applied.report,
            Err(current) => bail!(
                "owned report delivery lost its claim: report={report_id} status={:?}",
                current.status
            ),
        };
        ensure!(
            published.status == RecommendationReportStatus::Published
                && published.decision_at == decision_at,
            "normal publication must preserve the exact report clock"
        );

        Ok(Self {
            report: published,
            vector_ids,
            sampled_job_id,
        })
    }

    async fn verify(
        &self,
        fixture: &MixedReportFixture,
        stack: &SystemStack,
        store: &Arc<dyn ArtifactStore>,
    ) -> Result<()> {
        let db = stack.postgres.connection();
        let report_id = self.report.recommendation_report_id;
        let report_run_id = self.report.report_run_id;
        let decision_at = self.report.decision_at;
        let vector_ids = &self.vector_ids;
        let deps = ReplayContractFixture::replay_deps(db, store, &fixture.setup, stack).await?;
        let vectors = deps.feature_vectors.find_by_ids(vector_ids).await?;
        ensure!(
            vectors.len() == 10 && vector_ids.len() == 10,
            "ten real PG feature vectors must survive publication"
        );
        let boundary = &vectors
            .first()
            .context("published feature boundary")?
            .decision_boundary;
        let finalized = boundary.cutoff_for(DecisionSource::FinalizedExecution);
        let lag = decision_at.signed_duration_since(finalized);
        ensure!(
            lag.num_microseconds()
                .context("execution lag fits microseconds")?
                .rem_euclid(1_000_000)
                == 931_000,
            "real finalized execution watermark must produce a fractional lag: {lag}"
        );
        for vector in &vectors {
            ensure!(
                vector.decision_at == decision_at
                    && vector.decision_boundary == *boundary
                    && vector.decision_capture.snapshot.boundary == *boundary
                    && vector
                        .payload
                        .generic
                        .contains_key(&FeatureName::new("ts.price_reversal")),
                "real PG feature/capture boundary or source features drifted"
            );
        }
        let cells = deps
            .serving_evidence
            .feature_cells_for_vectors(vector_ids)
            .await?;
        ensure!(!cells.is_empty(), "real CH feature evidence is required");
        for cell in &cells {
            let cutoffs: BTreeMap<DecisionSource, DateTime<Utc>> =
                serde_json::from_str(&cell.per_source_cutoffs_json)?;
            ensure!(
                cell.decision_at == decision_at.timestamp_millis()
                    && cell.knowledge_cutoff == boundary.knowledge_cutoff().timestamp_millis()
                    && &cutoffs == boundary.per_source_cutoffs(),
                "CH scalar projection or precise JSON cutoffs drifted"
            );
        }
        let routes = deps.reports.find_route_runs(&[report_run_id]).await?;
        let model_runs = routes
            .iter()
            .filter_map(|route| route.model_run_id.map(|id| (route.route, id)))
            .collect::<BTreeMap<_, _>>();
        ensure!(
            model_runs.len() == 2
                && model_runs.contains_key(&BuyModelRoute::Crypto)
                && model_runs.contains_key(&BuyModelRoute::Weather),
            "both real Route model runs are required"
        );
        let model_ids = model_runs.values().copied().collect::<Vec<_>>();
        let markers = deps
            .serving_evidence
            .completions_for_runs(&model_ids)
            .await?;
        ensure!(
            markers.len() == model_ids.len()
                && markers
                    .iter()
                    .all(|marker| marker.decision_at == decision_at.timestamp_millis()),
            "real CH model completion must retain the intended millisecond projection"
        );
        println!(
            "fractional-report-boundary report={report_id} decision={decision_at} finalized={finalized} lag_us={:?} pg_vectors={} ch_cells={} model_runs={}",
            lag.num_microseconds(),
            vectors.len(),
            cells.len(),
            model_ids.len()
        );
        self.replay(stack, deps, &model_ids).await
    }

    async fn replay(
        &self,
        stack: &SystemStack,
        deps: DurableFeatureParityDeps,
        model_ids: &[ModelRunId],
    ) -> Result<()> {
        let db = stack.postgres.connection();
        let report_id = self.report.recommendation_report_id;
        let subject = ParitySubjectEntity::find()
            .filter(ParitySubjectColumn::RecommendationReportId.eq(report_id))
            .filter(ParitySubjectColumn::SubjectKind.eq(ParitySubjectKind::RecommendationReport))
            .one(db)
            .await?
            .context("publication must atomically freeze its report parity subject")?;
        let run = deps
            .parity
            .find_run(&subject.run_id)
            .await?
            .context("automatic report sampled run")?;
        ensure!(
            run.kind == FeatureParityRunKind::Sampled && run.report_id == Some(report_id),
            "replay must use the report's real atomic sampled run"
        );
        let source = Arc::new(DurableFeatureParitySource::try_new(deps.clone())?);
        let cancel = CancellationToken::new();
        let candidates = source.list_candidates(&run, &cancel).await?;
        self.trace_populations(db, &deps, model_ids, &candidates)
            .await?;
        ensure!(
            candidates.iter().any(|candidate| candidate.subject
                == FeatureParitySubject::RecommendationReport(report_id))
                && model_ids.iter().all(|id| candidates
                    .iter()
                    .any(|candidate| candidate.subject == FeatureParitySubject::ModelRun(*id))),
            "atomic discovery must include the report and both Route models without test-added candidates"
        );
        println!(
            "fractional-report-subjects report={report_id} model_runs={} candidates={}",
            model_ids.len(),
            candidates.len()
        );
        let attempt = source.replay(&run, &candidates, &cancel).await?;
        ensure!(
            attempt.pending.is_empty(),
            "complete report replay remains pending: {:?}",
            attempt.pending
        );
        for comparison in &attempt.comparisons {
            ensure!(
                comparison.online == comparison.replay,
                "fractional report parity mismatch at {:?}/{:?}: online={:?} replay={:?}",
                comparison.stage,
                comparison.market_id,
                comparison.online,
                comparison.replay
            );
        }
        for stage in [
            FeatureParityStage::Selection,
            FeatureParityStage::Snapshot,
            FeatureParityStage::Capture,
            FeatureParityStage::FeatureCell,
            FeatureParityStage::DataQuality,
            FeatureParityStage::Factor,
            FeatureParityStage::ModelInput,
            FeatureParityStage::Prediction,
        ] {
            ensure!(
                attempt
                    .comparisons
                    .iter()
                    .any(|comparison| comparison.stage == stage),
                "full report replay omitted {stage:?}"
            );
        }
        println!(
            "fractional-report-replay comparisons={} pending=0 stages=8",
            attempt.comparisons.len()
        );
        for id in model_ids {
            ensure!(
                attempt
                    .comparisons
                    .iter()
                    .any(|comparison| comparison.model_run_id == Some(*id)
                        && comparison.stage == FeatureParityStage::Prediction),
                "complete replay omitted real model run {id}"
            );
        }
        self.verify_sampled(
            stack,
            &deps,
            source,
            &candidates,
            model_ids,
            attempt.comparisons.len(),
        )
        .await
    }

    async fn verify_sampled(
        &self,
        stack: &SystemStack,
        deps: &DurableFeatureParityDeps,
        source: Arc<dyn FeatureParityReplaySource>,
        candidates: &[FeatureParityCandidate],
        model_ids: &[ModelRunId],
        full_comparison_count: usize,
    ) -> Result<()> {
        let job = PgResearchJobRepository::new(stack.postgres.connection().clone())
            .find_by_id(&self.sampled_job_id)
            .await?
            .context("publication must persist the coordinator's actual sampled job")?;
        let ResearchJobParams::FeatureParity(params) = job.params_json else {
            bail!("published parity job has another typed contract");
        };
        let queued = deps
            .parity
            .find_run(&params.parity_run_id)
            .await?
            .context("sampled executor requires the actual persisted run")?;
        ensure!(
            queued.kind == FeatureParityRunKind::Sampled
                && queued.status == FeatureParityRunStatus::Queued
                && queued.report_id == Some(self.report.recommendation_report_id),
            "executor must run the new report's queued sampled proof, not resume old success"
        );
        let config = &stack.clickhouse_config;
        let pool = Arc::new(ClickHousePool::connect(config).await?);
        let writer: Arc<dyn FactWriter<QuantFeatureParityEventRow>> = Arc::new(ChFactWriter::new(
            Arc::clone(&pool),
            Arc::new(ChWriteManager::new(
                config.max_concurrent_inserts,
                &config.io,
            )),
            "quant_feature_parity_event",
        ));
        let executor = FeatureParityExecutor::new(
            Arc::clone(&deps.parity),
            source,
            writer,
            Arc::new(UnexpectedParityIncident),
            Arc::new(MetricsHub::new()),
            Duration::minutes(10),
            StdDuration::from_secs(1),
        );
        let outcome = executor
            .execute(params, Arc::new(NoopProgressSink), CancellationToken::new())
            .await?;
        let FeatureParityExecutionOutcome::Completed(completed) = outcome else {
            bail!("fully committed real serving inputs remained pending in sampled execution");
        };
        let persisted = deps
            .parity
            .find_run(&queued.run_id)
            .await?
            .context("sampled executor must persist its terminal run")?;
        ensure!(
            completed.status == FeatureParityRunStatus::Passed
                && persisted.status == FeatureParityRunStatus::Passed
                && completed.parity_run_id == persisted.run_id
                && completed.kind == FeatureParityRunKind::Sampled
                && completed.report_id == Some(self.report.recommendation_report_id)
                && completed.total_count == persisted.total_count
                && completed.compared_count == persisted.compared_count
                && completed.matched_count == persisted.matched_count
                && completed.mismatched_count == persisted.mismatched_count
                && completed.pending_materialization_count
                    == persisted.pending_materialization_count
                && persisted.total_count > 0
                && usize::try_from(persisted.total_count)? <= full_comparison_count
                && persisted.compared_count == persisted.total_count
                && persisted.matched_count == persisted.total_count
                && persisted.mismatched_count == 0
                && persisted.pending_materialization_count == 0
                && persisted.finished_at.is_some(),
            "real sampled executor did not persist complete matching evidence"
        );
        let events = self.sampled_events(&pool, &completed).await?;
        self.verify_sampled_inputs(deps, &events, candidates, model_ids)
            .await?;
        ensure!(
            deps.reports
                .find_by_id(&self.report.recommendation_report_id)
                .await?
                .context("report after sampled replay")?
                .status
                == RecommendationReportStatus::Published,
            "successful sampled replay changed the published report"
        );
        println!(
            "fractional-report-sampled run={} job={} candidates={} selected=20 events={} models={} status=passed scope=executor-parity-only",
            persisted.run_id,
            self.sampled_job_id,
            candidates.len(),
            events.len(),
            model_ids.len()
        );
        Ok(())
    }

    async fn sampled_events(
        &self,
        pool: &Arc<ClickHousePool>,
        completed: &FeatureParityRunView,
    ) -> Result<Vec<FeatureParityEventView>> {
        let repository = ChFeatureParityEventRepository::new(Arc::clone(pool));
        let total = u64::try_from(completed.total_count)?;
        let pages = total.div_ceil(PageRequest::MAX_SIZE);
        let mut events = Vec::with_capacity(usize::try_from(total)?);
        for page in 1..=pages {
            let response = repository
                .page_events(FeatureParityEventListQuery {
                    parity_run_id: Some(completed.parity_run_id),
                    from: Some(completed.window_start),
                    to: Some(completed.window_end),
                    page: PageRequest::new(page, PageRequest::MAX_SIZE),
                    ..FeatureParityEventListQuery::default()
                })
                .await?;
            let expected_size = (total - u64::try_from(events.len())?).min(PageRequest::MAX_SIZE);
            ensure!(
                response.total == total
                    && response.page == page
                    && response.size == PageRequest::MAX_SIZE
                    && u64::try_from(response.items.len())? == expected_size
                    && response.has_next == (page < pages),
                "sampled CH evidence pagination changed or omitted committed rows"
            );
            ensure!(
                response.items.iter().all(|event| {
                    event.parity_run_id == completed.parity_run_id
                        && event.report_id == Some(self.report.recommendation_report_id)
                }),
                "sampled CH page contains evidence for another parity run or report"
            );
            events.extend(response.items);
        }
        ensure!(
            u64::try_from(events.len())? == total
                && events
                    .iter()
                    .map(|event| event.parity_event_id)
                    .collect::<HashSet<_>>()
                    .len()
                    == events.len(),
            "sampled CH evidence must contain every unique committed comparison"
        );
        Ok(events)
    }

    async fn verify_sampled_inputs(
        &self,
        deps: &DurableFeatureParityDeps,
        events: &[FeatureParityEventView],
        candidates: &[FeatureParityCandidate],
        model_ids: &[ModelRunId],
    ) -> Result<()> {
        let native = candidates
            .iter()
            .map(|candidate| (candidate.sampling_key.as_str(), candidate))
            .collect::<BTreeMap<_, _>>();
        let inputs = deps
            .serving_evidence
            .model_inputs_for_runs(model_ids)
            .await?;
        let mut sampled_keys = BTreeSet::new();
        let mut verified_models = HashSet::new();
        let mut predicted_models = HashSet::new();
        for event in events {
            ensure!(
                event.status == FeatureParityEventStatus::Matched
                    && event.online.fingerprint == event.replay.fingerprint
                    && event.online.state == event.replay.state
                    && event.online.value == event.replay.value
                    && event.online.effective_at == event.replay.effective_at
                    && event.online.available_at == event.replay.available_at
                    && event.online.cutoff == event.replay.cutoff
                    && event.decision_at == self.report.decision_at,
                "sampled event must retain the exact matching decision evidence"
            );
            let FeatureParityDetail::Compared {
                sampling_key,
                source,
            } = &event.detail
            else {
                bail!("completed sampled proof contains pending evidence");
            };
            let candidate = native
                .get(sampling_key.as_str())
                .context("executor emitted a sampling key outside native discovery")?;
            sampled_keys.insert(sampling_key);
            match source.as_ref() {
                FeatureParityDetailSource::ModelInput {
                    raw_input_name,
                    feature_vector_id,
                } => {
                    let FeatureParitySubject::ModelRun(model_id) = candidate.subject else {
                        bail!("report-selection candidate fabricated model-input evidence");
                    };
                    ensure!(
                        event.model_run_id == Some(model_id)
                            && event.market_id == candidate.market_id
                            && candidate.input_witness
                                == (FeatureParityInputWitness::VerifiedModelInput {
                                    feature_vector_id: *feature_vector_id,
                                }),
                        "sampled input event changed its real candidate model/market binding"
                    );
                    let input = inputs
                        .iter()
                        .find(|input| {
                            input.model_run_id == model_id
                                && Some(&input.market_id) == event.market_id.as_ref()
                                && input.feature_vector_id == *feature_vector_id
                                && input.raw_input_name == raw_input_name.as_str()
                                && Some(&input.encoded_column) == event.feature_name.as_ref()
                        })
                        .context("sampled ModelInput lacks the actual CH input/vector/name")?;
                    ensure!(
                        event.online.fingerprint == input.audit_fingerprint
                            && event.model_version_id == Some(input.model_version_id),
                        "sampled input hash/version differs from the real CH input row"
                    );
                    verified_models.insert(model_id);
                }
                FeatureParityDetailSource::Prediction { .. } => {
                    let FeatureParitySubject::ModelRun(model_id) = candidate.subject else {
                        bail!("report-selection candidate fabricated model prediction");
                    };
                    ensure!(
                        event.model_run_id == Some(model_id),
                        "prediction changed its model owner"
                    );
                    predicted_models.insert(model_id);
                }
                _ => {}
            }
        }
        ensure!(
            candidates.len() == 30 && native.len() == candidates.len() && sampled_keys.len() == 20,
            "normal sampled executor must select twenty distinct keys from thirty native candidates"
        );
        let expected = model_ids.iter().copied().collect::<HashSet<_>>();
        ensure!(
            verified_models == expected && predicted_models == expected,
            "both real Route models require matched sampled inputs and predictions"
        );
        Ok(())
    }

    async fn trace_populations(
        &self,
        db: &DatabaseConnection,
        deps: &DurableFeatureParityDeps,
        model_ids: &[ModelRunId],
        candidates: &[FeatureParityCandidate],
    ) -> Result<()> {
        // Read the two actual model rows and their selection members in batches;
        // diagnostics never change the source's frozen candidate population.
        let models = ModelRunEntity::find()
            .filter(ModelRunColumn::ModelRunId.is_in(model_ids.iter().copied()))
            .all(db)
            .await?;
        let mut selection_ids = HashSet::from([self.report.market_selection_id]);
        selection_ids.extend(models.iter().filter_map(|model| model.market_selection_id));
        let members = SelectionMemberEntity::find()
            .filter(SelectionMemberColumn::MarketSelectionId.is_in(selection_ids))
            .all(db)
            .await?;
        let markers = deps
            .serving_evidence
            .completions_for_runs(model_ids)
            .await?;
        let inputs = deps
            .serving_evidence
            .model_inputs_for_runs(model_ids)
            .await?;
        let vectors = deps.feature_vectors.find_by_ids(&self.vector_ids).await?;
        let mut model_evidence = Vec::new();
        for model in models {
            let marker = markers
                .iter()
                .find(|row| row.model_run_id == model.model_run_id);
            let committed_ids = marker
                .map(|row| {
                    serde_json::from_str::<Vec<FeatureVectorId>>(&row.feature_vector_ids_json)
                })
                .transpose()?;
            model_evidence.push(json!({
                "model_run_id": model.model_run_id,
                "model_version_id": model.model_version_id,
                "market_selection_id": model.market_selection_id,
                "selection_members": members.iter().filter(|member| Some(member.market_selection_id) == model.market_selection_id)
                    .map(|member| json!({"market_id": member.market_id, "category": member.category})).collect::<Vec<_>>(),
                "completion_vectors": committed_ids.as_ref().map(|ids| ids.iter().map(|id| json!({
                    "feature_vector_id": id,
                    "pg_market_id": vectors.iter().find(|vector| vector.feature_vector_id == *id).map(|vector| &vector.market_id)
                })).collect::<Vec<_>>()),
                "completion_feature_rows": marker.map(|row| row.expected_feature_row_count),
                "input_market_ids": inputs.iter().filter(|row| row.model_run_id == model.model_run_id)
                    .map(|row| &row.market_id).collect::<BTreeSet<_>>(),
                "candidate_market_ids": candidates.iter().filter(|candidate| candidate.subject == FeatureParitySubject::ModelRun(model.model_run_id))
                    .map(|candidate| &candidate.market_id).collect::<Vec<_>>()
            }));
        }
        println!(
            "fractional-report-populations {}",
            json!({
                "report_id": self.report.recommendation_report_id,
                "global_selection_id": self.report.market_selection_id,
                "global_members": members.iter().filter(|member| member.market_selection_id == self.report.market_selection_id)
                    .map(|member| json!({"market_id": member.market_id, "category": member.category})).collect::<Vec<_>>(),
                "report_feature_vectors": vectors.iter().map(|vector| json!({"feature_vector_id": vector.feature_vector_id, "market_id": vector.market_id})).collect::<Vec<_>>(),
                "report_candidate_market_ids": candidates.iter().filter(|candidate| candidate.subject == FeatureParitySubject::RecommendationReport(self.report.recommendation_report_id))
                    .map(|candidate| &candidate.market_id).collect::<Vec<_>>(),
                "models": model_evidence,
            })
        );
        Ok(())
    }
}

struct UnexpectedParityIncident;

#[async_trait]
impl FeatureParityIncidentPort for UnexpectedParityIncident {
    async fn contain(
        &self,
        run: &FeatureParityRunInfo,
        report_ids: &[RecommendationReportId],
    ) -> QuantResult<()> {
        Err(ResearchError::Determinism {
            detail: format!(
                "real sampled proof unexpectedly required containment: run={} status={:?} failure_code={:?} failure_detail={:?} reports={report_ids:?}",
                run.run_id, run.status, run.failure_code, run.failure_detail
            ),
        }
        .into())
    }
}
