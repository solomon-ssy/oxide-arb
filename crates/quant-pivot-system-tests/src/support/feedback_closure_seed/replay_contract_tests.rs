//! Small real-storage replay proofs for the two closure history authorities.

mod mixed_report;

use std::{
    collections::{BTreeMap, HashSet},
    sync::Arc,
    time::Duration as StdDuration,
};

use anyhow::{Context, Result, bail, ensure};
use async_trait::async_trait;
use chrono::{DateTime, Duration, Utc};
use quant_pivot_compute::ComputeExecutor;
use quant_pivot_core::{
    observability::serving_evidence::verify_completion,
    report::universe::{ReportUniverseContract, ReportUniverseRoute},
    service::{
        durable_feature_parity::{DurableFeatureParityDeps, DurableFeatureParitySource},
        feature_parity_executor::{
            FeatureParityCandidate, FeatureParityInputWitness, FeatureParityReplaySource,
            FeatureParitySubject,
        },
    },
};
use quant_pivot_error::{QuantError, research::ResearchError, storage::StorageError};
use quant_pivot_models::{
    config::FeatureParityComputeConfig,
    domain::{
        data_plane::{DecisionSource, ExchangeHistoryFrontier},
        quant::{
            FeatureParityRunInfo, MarketSelectionInfo, MarketSelectionMemberInfo,
            NewMarketSelection, NewMarketSelectionMember, RouteHistoryLineage,
        },
    },
    entities::quant_feature_parity_subject::{
        Column as ParitySubjectColumn, Entity as ParitySubjectEntity,
    },
    enums::quant::FeatureParityStage,
    runtime_config::BuyModelRoute,
    types::{ContentHash, FeatureVectorId, FinalizedExecutionEvidence, MarketSelectionId},
};
use quant_pivot_repository::{
    clickhouse::ChFeatureParityEventRepository,
    postgres::{
        PgCalibrationArtifactRepository, PgCatalogLedgerRepository, PgClobMarketInfoRepository,
        PgExchangeHistoryRepository, PgFactorRepository, PgFeatureParityRepository,
        PgFeatureRepository, PgMarketLinkageRepository, PgMarketSelectionRepository,
        PgModelRunRepository, PgPolicyRepository, PgRecommendationReportRepository,
        PgReportRunRepository,
    },
    traits::{ExchangeHistoryRepository, MarketSelectionRepository},
};
use quant_pivot_research::artifact::{ArtifactStore, LocalArtifactStore};
use quant_pivot_storage::clickhouse::ClickHousePool;
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use sea_orm::{ColumnTrait, DatabaseConnection, EntityTrait, QueryFilter};
use tempfile::TempDir;
use tokio_util::sync::CancellationToken;

use super::{
    ClosureHistoryInterval, ClosureSeedSetup, CohortDecisionContract, CohortSeed,
    CohortSeedContext, CohortSpecification, FeedbackClosureSeedRequest, PreparedCohort,
    SelectionModelBuild, build_selection_model, seed_catalog_baseline,
};
use crate::{
    postgres::PostgresClock,
    stack::SystemStack,
    support::{
        artifact_store::VersionedArtifactStoreFixture,
        execution_pg_seed::{FeedbackServingFixtureConfig, seed_feedback_serving_infra},
        model_serving_runtime::ModelServingRegistryFixture,
        production_history::DeterministicPolygonChain,
        report_pipeline_harness::publish_pooled_control_model,
        research_browser_seed::seed_closure_feedback_research,
        trade_policy_fixtures::{FixtureBookTiming, PublishedTradePolicyFixture},
    },
};

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn history_authorities_replay_exactly() -> Result<()> {
    let stack = Box::pin(SystemStack::start()).await?;
    let artifact_root = TempDir::with_prefix("quant-pivot-closure-replay-")?;
    let store: Arc<dyn ArtifactStore> = Arc::new(VersionedArtifactStoreFixture::new(Arc::new(
        LocalArtifactStore::new(artifact_root.path().to_owned()),
    )));
    let result = tokio::time::timeout(
        StdDuration::from_mins(10),
        Box::pin(ReplayContractFixture::verify(&stack, &store)),
    )
    .await
    .context("small closure replay contract exceeded its bounded deadline")
    .and_then(|result| result);
    drop(store);
    let shutdown = Box::pin(stack.shutdown()).await;
    let cleanup = artifact_root.close();
    result?;
    shutdown?;
    cleanup.context("remove this test's unique artifact directory")?;
    Ok(())
}

struct ReplayContractFixture {
    setup: ClosureSeedSetup,
    deps: DurableFeatureParityDeps,
}

struct CohortReplayProof {
    run: FeatureParityRunInfo,
    candidates: Vec<FeatureParityCandidate>,
}

impl ReplayContractFixture {
    async fn verify(stack: &SystemStack, artifacts: &Arc<dyn ArtifactStore>) -> Result<()> {
        let db = stack.postgres.connection();
        let governed = Box::pin(seed_feedback_serving_infra(
            db,
            artifacts,
            FeedbackServingFixtureConfig {
                book_timing: FixtureBookTiming::closure()?,
                required_shadow_window_secs: 900,
                shadow_diff_threshold: Decimal::ONE,
                feedback_budget_usd: dec!(555.56),
                outcome_reconciliation_enabled: false,
                outcome_reconciliation_sweep_secs: 10,
                ad_hoc_report_enabled: true,
                knowledge_lag_secs: 90,
            },
        ))
        .await;
        Box::pin(publish_pooled_control_model(
            db,
            artifacts,
            governed.pooled_model_version_id,
            governed.template.decision_policy_snapshot_id,
        ))
        .await;
        let research = Box::pin(seed_closure_feedback_research(
            db,
            artifacts,
            &governed.template,
            governed.champion_model_version_id,
        ))
        .await?;
        let polygon = Arc::new(DeterministicPolygonChain::new());
        polygon.freeze();
        let history = PgExchangeHistoryRepository::new(db.clone())
            .load_plan(137)
            .await?
            .context("serving model fixture must establish the canonical history plan")?;
        let head = polygon.head();
        let request = FeedbackClosureSeedRequest {
            db,
            clickhouse_config: &stack.clickhouse_config,
            artifact_store: artifacts,
            infra: &governed.template,
            champion_model_version_id: research.model_version_id,
            historical_feedback_cycle_id: research.feedback_cycle_id,
            report_resolves_at: db.statement_time().await + Duration::days(1),
            runtime_finalized_execution_evidence: FinalizedExecutionEvidence::runtime(
                true,
                Some(u64::try_from(history.activation_through_block)?),
                Some(
                    DateTime::from_timestamp(head.timestamp, 0)
                        .context("Polygon head timestamp")?,
                ),
            ),
            polygon: &polygon,
        };
        let setup = ClosureSeedSetup::load(&request).await?;
        let deps = Self::replay_deps(db, artifacts, &setup, stack).await?;
        let fixture = Self { setup, deps };
        let activation_from =
            DeterministicPolygonChain::block(u64::try_from(history.activation_from_block)?, head)
                .context("activation start block")?;
        let materialized_at = DateTime::from_timestamp(activation_from.timestamp, 0)
            .context("activation start timestamp")?
            - Duration::days(1);
        let runtime_at = DateTime::from_timestamp(head.timestamp, 0)
            .context("runtime decision timestamp")?
            - Duration::days(1);
        ensure!(
            materialized_at < runtime_at,
            "history fixture needs disjoint Retention and Activation decisions"
        );
        seed_catalog_baseline(db, materialized_at - Duration::days(1)).await?;
        let context = CohortSeedContext {
            db,
            artifacts,
            infra: &fixture.setup.closure_infra,
            champion: &fixture.setup.champion,
            schema: &fixture.setup.schema,
            runtime: &fixture.setup.runtime,
            facts: fixture.setup.fact_writers.as_ref(),
            replay: fixture.setup.replay.as_ref(),
            report_universe: &fixture.setup.report_universe,
            capability_registry_hash: fixture.setup.capability_registry_hash,
            account_capital_usd: fixture.setup.account_capital_usd,
            runtime_finalized_execution_evidence: &request.runtime_finalized_execution_evidence,
            activation_from_block: u64::try_from(history.activation_from_block)?,
            activation_through_block: u64::try_from(history.activation_through_block)?,
            history_policy_hash: history.policy_hash,
        };
        let materialized =
            Self::publish_cohort(&context, "calibration", materialized_at, 8, None).await?;
        fixture.verify_storage(&materialized, false).await?;
        fixture.verify_replay(db, &materialized).await?;
        let interval = context.evaluation_intervals(&[runtime_at])?.remove(0);
        let runtime =
            Self::publish_cohort(&context, "evaluation", runtime_at, 5, Some(interval)).await?;
        fixture.verify_storage(&runtime, true).await?;
        let proof = fixture.verify_replay(db, &runtime).await?;
        fixture
            .reject_selector_drift(&context, &runtime, &proof)
            .await?;
        fixture.verify_replay(db, &runtime).await?;
        Ok(())
    }

    async fn replay_deps(
        db: &DatabaseConnection,
        artifacts: &Arc<dyn ArtifactStore>,
        setup: &ClosureSeedSetup,
        stack: &SystemStack,
    ) -> Result<DurableFeatureParityDeps> {
        let serving_generations = ModelServingRegistryFixture {
            db: db.clone(),
            artifact_store: Arc::clone(artifacts),
            evidence_scope: PublishedTradePolicyFixture::evidence_scope()?,
            evidence_attestor: Some(PublishedTradePolicyFixture::evidence_attestor()?),
        }
        .build_generation()
        .await?;
        let clickhouse = Arc::new(ClickHousePool::connect(&stack.clickhouse_config).await?);
        Ok(DurableFeatureParityDeps {
            parity: Arc::new(PgFeatureParityRepository::new(db.clone())),
            model_runs: Arc::new(PgModelRunRepository::new(db.clone())),
            serving_generations,
            runtime_configs: Arc::new(PgPolicyRepository::new(db.clone())),
            selections: Arc::new(PgMarketSelectionRepository::new(db.clone())),
            feature_vectors: Arc::new(PgFeatureRepository::new(db.clone())),
            factors: Arc::new(PgFactorRepository::new(db.clone())),
            reports: Arc::new(PgRecommendationReportRepository::new(db.clone())),
            report_runs: Arc::new(PgReportRunRepository::new(db.clone())),
            serving_evidence: Arc::new(ChFeatureParityEventRepository::new(clickhouse)),
            fact_read: Arc::clone(&setup.fact_writers.fact_read),
            catalog: Arc::new(PgCatalogLedgerRepository::new(db.clone())),
            clob_market_info: Arc::new(PgClobMarketInfoRepository::new(db.clone())),
            linkages: Arc::new(PgMarketLinkageRepository::new(db.clone())),
            calibration_artifacts: Arc::new(PgCalibrationArtifactRepository::new(db.clone())),
            exchange_history: Arc::new(PgExchangeHistoryRepository::new(db.clone())),
            compute: Arc::new(ComputeExecutor::new()?),
            compute_budget: FeatureParityComputeConfig {
                page_size: 20,
                deadline_secs: 120,
                ..FeatureParityComputeConfig::default()
            },
        })
    }

    async fn publish_cohort(
        context: &CohortSeedContext<'_>,
        scope: &'static str,
        decision_at: DateTime<Utc>,
        count: usize,
        history_interval: Option<ClosureHistoryInterval>,
    ) -> Result<CohortSeed> {
        let resolutions = (1..=count)
            .map(|ordinal| (ordinal, decision_at + Duration::days(1)))
            .collect::<BTreeMap<_, _>>();
        let prepared = Box::pin(PreparedCohort::prepare(
            context,
            CohortSpecification {
                scope,
                decision_at,
                market_created_at: decision_at - Duration::days(1),
                resolutions: &resolutions,
                first_ordinal: 1,
                observation_count: count,
                book_price_shift: Decimal::ZERO,
                history_interval,
            },
        ))
        .await?;
        Box::pin(prepared.publish(context.db, context.artifacts, context.facts)).await
    }

    async fn verify_storage(&self, cohort: &CohortSeed, runtime: bool) -> Result<()> {
        ensure!(
            (cohort.history.chunk_ref.frontier == ExchangeHistoryFrontier::Activation) == runtime,
            "cohort source frontier differs from its intended authority"
        );
        let report = self
            .deps
            .reports
            .find_by_id(&cohort.ids.report)
            .await?
            .context("published cohort report")?;
        let routes = self
            .deps
            .reports
            .find_route_runs(&[report.report_run_id])
            .await?;
        let expected = if runtime {
            vec![BuyModelRoute::Pooled, BuyModelRoute::Weather]
        } else {
            vec![BuyModelRoute::Weather]
        };
        ensure!(
            report.represented_routes_json.routes == expected,
            "cohort represented Routes differ"
        );
        ensure!(routes.len() == expected.len(), "cohort omitted a Route row");
        let weather = routes
            .iter()
            .find(|row| row.route == BuyModelRoute::Weather)
            .context("Weather Route")?;
        ensure!(
            weather.model_run_id == Some(cohort.ids.model_run),
            "Weather Route lacks its actual inference"
        );
        let lineage = weather.lineage_json.as_ref().context("Weather lineage")?;
        if runtime {
            let pooled = routes
                .iter()
                .find(|row| row.route == BuyModelRoute::Pooled)
                .context("Pooled Route")?;
            let pooled_lineage = pooled
                .lineage_json
                .as_ref()
                .context("Pooled immutable lineage")?;
            ensure!(
                pooled.model_run_id.is_none()
                    && pooled_lineage.model_run_id.is_none()
                    && pooled.funnel_json.eligible_markets == 0,
                "zero-candidate Pooled Route fabricated inference"
            );
            ensure!(
                pooled_lineage.model_version_id != lineage.model_version_id
                    && pooled_lineage.report_universe_plan_hash
                        == lineage.report_universe_plan_hash,
                "all-active universe does not bind distinct Pooled and Weather models"
            );
            ensure!(
                matches!(lineage.history, RouteHistoryLineage::Runtime { .. }),
                "Runtime Route lost its serving head"
            );
        } else {
            ensure!(
                matches!(lineage.history, RouteHistoryLineage::Materialized { .. }),
                "Retention Route lost materialized lineage"
            );
        }
        let quality = self
            .deps
            .reports
            .find_data_quality_snapshot(&cohort.ids.report)
            .await?
            .context("cohort DQ evidence")?;
        let ids = quality
            .tokens_json
            .0
            .iter()
            .map(|token| token.feature_vector_id)
            .collect::<Vec<_>>();
        let vectors = self.deps.feature_vectors.find_by_ids(&ids).await?;
        ensure!(
            !vectors.is_empty() && vectors.len() == ids.len(),
            "PG feature population differs"
        );
        for vector in &vectors {
            ensure!(
                vector.decision_boundary == cohort.boundary
                    && vector.decision_capture.snapshot.boundary == cohort.boundary,
                "PG feature/capture boundary drifted from prepare"
            );
            ensure!(
                matches!(
                    (
                        &vector.decision_capture.finalized_execution_evidence,
                        runtime
                    ),
                    (FinalizedExecutionEvidence::Runtime { .. }, true)
                        | (FinalizedExecutionEvidence::Materialized { .. }, false)
                ),
                "capture history authority differs"
            );
        }
        let cells = self
            .deps
            .serving_evidence
            .feature_cells_for_vectors(&ids)
            .await?;
        ensure!(!cells.is_empty(), "real CH feature evidence is absent");
        for cell in cells {
            let cutoffs: BTreeMap<DecisionSource, DateTime<Utc>> =
                serde_json::from_str(&cell.per_source_cutoffs_json)?;
            ensure!(
                cell.decision_at == cohort.boundary.decision_at().timestamp_millis()
                    && cell.knowledge_cutoff
                        == cohort.boundary.knowledge_cutoff().timestamp_millis()
                    && &cutoffs == cohort.boundary.per_source_cutoffs(),
                "CH projection lost the full boundary"
            );
        }
        ensure!(
            !self
                .deps
                .serving_evidence
                .completions_for_runs(&[cohort.ids.model_run])
                .await?
                .is_empty(),
            "real CH model-run completion is absent"
        );
        Ok(())
    }

    async fn verify_replay(
        &self,
        db: &DatabaseConnection,
        cohort: &CohortSeed,
    ) -> Result<CohortReplayProof> {
        let subject = ParitySubjectEntity::find()
            .filter(ParitySubjectColumn::RecommendationReportId.eq(cohort.ids.report))
            .one(db)
            .await?
            .context("publish must atomically freeze a report parity subject")?;
        let run = self
            .deps
            .parity
            .find_run(&subject.run_id)
            .await?
            .context("automatic sampled parity run")?;
        let source = DurableFeatureParitySource::try_new(self.deps.clone())?;
        let cancel = CancellationToken::new();
        let mut candidates = source.list_candidates(&run, &cancel).await?;
        if !candidates.iter().any(|candidate| {
            candidate.subject == FeatureParitySubject::ModelRun(cohort.ids.model_run)
        }) {
            let model_run = self
                .deps
                .model_runs
                .find_by_id(&cohort.ids.model_run)
                .await?
                .context("actual cohort model run")?;
            let selection_id = model_run
                .market_selection_id
                .context("actual model selection")?;
            let completions = self
                .deps
                .serving_evidence
                .completions_for_runs(&[model_run.model_run_id])
                .await?;
            ensure!(
                completions.len() == 1,
                "cohort model requires one real completion"
            );
            let completion = &completions[0];
            ensure!(
                completion.model_run_id == model_run.model_run_id
                    && completion.decision_at == model_run.window_start.timestamp_millis(),
                "cohort completion must match its actual model and decision"
            );
            let vector_ids: Vec<FeatureVectorId> =
                serde_json::from_str(&completion.feature_vector_ids_json)?;
            let features = self
                .deps
                .serving_evidence
                .feature_cells_for_vectors(&vector_ids)
                .await?;
            let inputs = self
                .deps
                .serving_evidence
                .model_inputs_for_runs(&[model_run.model_run_id])
                .await?;
            verify_completion(completion, &features, &inputs)?;
            let mut inputs_by_market = BTreeMap::new();
            for input in &inputs {
                if let Some(previous) =
                    inputs_by_market.insert(input.market_id.clone(), input.feature_vector_id)
                {
                    ensure!(
                        previous == input.feature_vector_id,
                        "cohort input market binds more than one verified feature vector"
                    );
                }
            }
            for member in self.deps.selections.list_members(&selection_id).await? {
                let input_witness = inputs_by_market.get(&member.market_id).map_or(
                    FeatureParityInputWitness::SelectionOnly,
                    |feature_vector_id| FeatureParityInputWitness::VerifiedModelInput {
                        feature_vector_id: *feature_vector_id,
                    },
                );
                candidates.push(FeatureParityCandidate {
                    sampling_key: format!("{}/{}", model_run.model_run_id, member.market_id),
                    subject: FeatureParitySubject::ModelRun(model_run.model_run_id),
                    market_id: Some(member.market_id),
                    decision_at: model_run.window_start,
                    input_witness,
                });
            }
        }
        ensure!(
            candidates.iter().any(|candidate| candidate.subject
                == FeatureParitySubject::RecommendationReport(cohort.ids.report))
                && candidates.iter().any(|candidate| candidate.subject
                    == FeatureParitySubject::ModelRun(cohort.ids.model_run)),
            "both real subject types must enter durable replay"
        );
        let attempt = source.replay(&run, &candidates, &cancel).await?;
        ensure!(
            attempt.pending.is_empty(),
            "small cohort parity remains pending: {:?}",
            attempt.pending
        );
        for comparison in &attempt.comparisons {
            ensure!(
                comparison.online == comparison.replay,
                "cohort {:?} parity mismatch at {:?}/{:?}: online={:?} replay={:?}",
                cohort.history.chunk_ref.frontier,
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
                "true replay did not reach {stage:?}"
            );
        }
        Ok(CohortReplayProof { run, candidates })
    }

    async fn reject_selector_drift(
        &self,
        context: &CohortSeedContext<'_>,
        cohort: &CohortSeed,
        proof: &CohortReplayProof,
    ) -> Result<()> {
        let expected_markets = cohort
            .market_universe
            .iter()
            .map(|row| row.market_id.clone())
            .collect::<HashSet<_>>();
        let route = self
            .deps
            .reports
            .find_model_route_run(&cohort.ids.model_run)
            .await?
            .context("Runtime report Route owning the actual model run")?;
        let lineage = route.lineage_json.context("Runtime report lineage")?;
        let (head_id, head_hash) = match &lineage.history {
            RouteHistoryLineage::Runtime {
                serving_head_seal_id,
                serving_head_seal_hash,
            } => (*serving_head_seal_id, *serving_head_seal_hash),
            RouteHistoryLineage::Materialized { .. } => {
                bail!("selector fault requires an actual Runtime cohort")
            }
        };
        for missing_availability in [false, true] {
            let universe = ReportUniverseContract::try_new(
                self.setup.report_universe.policy_id,
                self.setup.report_universe.snapshot_hash,
                self.setup
                    .report_universe
                    .routes
                    .iter()
                    .map(|active| ReportUniverseRoute::from(&active.serving))
                    .collect(),
                head_id,
                head_hash,
            )?;
            ensure!(
                universe.availability.universe_plan_hash == lineage.report_universe_plan_hash,
                "negative control must start from the original immutable universe"
            );
            let mut decision = CohortDecisionContract {
                boundary: cohort.boundary.clone(),
                history: lineage.history.clone(),
                universe: Some(universe),
            };
            if missing_availability {
                decision.universe = None;
            } else {
                decision
                    .universe
                    .as_mut()
                    .context("real Runtime universe")?
                    .availability
                    .universe_plan_hash = ContentHash::from_bytes([0x91; 32]);
            }
            let faulty = build_selection_model(SelectionModelBuild {
                db: context.db,
                facts: context.facts,
                replay: context.replay,
                infra: context.infra,
                champion: context.champion,
                runtime: context.runtime,
                decision: &decision,
                expected_markets: &expected_markets,
            })
            .await?;
            let mut deps = self.deps.clone();
            deps.selections = Arc::new(SelectorReadFault {
                inner: PgMarketSelectionRepository::new(context.db.clone()),
                target: cohort.ids.market_selection,
                replacement: faulty.snapshot,
            });
            let source = DurableFeatureParitySource::try_new(deps)?;
            let cancel = CancellationToken::new();
            ensure!(
                matches!(
                    source.list_candidates(&proof.run, &cancel).await,
                    Err(QuantError::Research(ResearchError::Determinism { .. }))
                ),
                "frozen subject accepted selector drift, missing_availability={missing_availability}"
            );
            let attempt = source
                .replay(&proof.run, &proof.candidates, &cancel)
                .await?;
            ensure!(
                attempt.pending.is_empty()
                    && attempt.comparisons.iter().any(|comparison| comparison.stage
                        == FeatureParityStage::Selection
                        && comparison.online != comparison.replay),
                "durable replay accepted selector drift, missing_availability={missing_availability}"
            );
        }
        Ok(())
    }
}

struct SelectorReadFault {
    inner: PgMarketSelectionRepository,
    target: MarketSelectionId,
    replacement: NewMarketSelection,
}

#[async_trait]
impl MarketSelectionRepository for SelectorReadFault {
    async fn create_snapshot(
        &self,
        _snapshot: NewMarketSelection,
        _members: Vec<NewMarketSelectionMember>,
    ) -> Result<MarketSelectionInfo, StorageError> {
        Err(StorageError::invariant_violation(
            Some("quant_market_selection"),
            "selector fault adapter is read-only",
        ))
    }

    async fn find_by_id(
        &self,
        id: &MarketSelectionId,
    ) -> Result<Option<MarketSelectionInfo>, StorageError> {
        let mut result = self.inner.find_by_id(id).await?;
        if let Some(row) = &mut result
            && *id == self.target
        {
            row.selector_hash = self.replacement.selector_hash;
            row.selector_evidence = self.replacement.selector_evidence;
        }
        Ok(result)
    }

    async fn list_members(
        &self,
        id: &MarketSelectionId,
    ) -> Result<Vec<MarketSelectionMemberInfo>, StorageError> {
        self.inner.list_members(id).await
    }

    async fn list_snapshot_members(
        &self,
        ids: &[MarketSelectionId],
    ) -> Result<Vec<MarketSelectionMemberInfo>, StorageError> {
        self.inner.list_snapshot_members(ids).await
    }
}
