//! Real pre-start Browser report diagnostics without a running backend.

use std::{
    sync::{Arc, Mutex},
    time::Duration as StdDuration,
};

use anyhow::{Context, Result, bail, ensure};
use async_trait::async_trait;
use chrono::{DateTime, Duration, Utc};
use quant_pivot_core::{
    app::ports::quant_report::{CoreQuantReportPort, CoreQuantReportPortDeps},
    observability::{alert_dispatcher::AlertDispatcher, metrics_hub::MetricsHub},
    report::{
        BuildReportRequest, ComposedReport, ReportBuilder, ReportLifecycleDeps,
        ReportLifecycleService, ReportPublisher, ReportPublisherDeps,
    },
    service::feature_integrity::{FeatureParityRunCoordinator, RepositoryFeatureParityGate},
};
use quant_pivot_error::{QuantResult, report::ReportError, storage::StorageError};
use quant_pivot_models::{
    domain::{
        api::DecisionBoundaryEvidenceView,
        data_plane::{DecisionClock, DecisionSource, HistoryServingHeadSeal},
        ports::QuantReportPort,
        quant::{RecommendationReportInfo, RouteHistoryLineage},
        runtime::CoreEventPublisher,
    },
    entities::{
        quant_account_snapshot::Entity as AccountSnapshotEntity,
        quant_order_intent::{Column as IntentColumn, Entity as IntentEntity},
        quant_recommendation::{Column as RecommendationColumn, Entity as RecommendationEntity},
        quant_recommendation_report::Entity as ReportEntity,
        quant_strategy_position_lot::Entity as PositionLotEntity,
    },
    enums::quant::{FeatureParityStage, OrderIntentStatus},
    runtime_config::BuyModelRoute,
    types::{
        FinalizedExecutionEvidence, HistoryServingHeadSealId, MarketId, RecommendationReportId, Usd,
    },
};
use quant_pivot_repository::{
    clickhouse::{ChFeatureParityEventRepository, ChQuantFactReadRepository},
    postgres::{
        PgExchangeHistoryRepository, PgFeatureParityRepository, PgFeatureRepository,
        PgOperationLogRepository, PgOrderIntentRepository, PgPolicyRepository,
        PgPortfolioPlanRepository, PgRecommendationReportRepository, PgRecommendationRepository,
        PgReportRunRepository, PgRuntimeControlRepository,
    },
    traits::{
        ExchangeHistoryRepository, FeatureParityRepository, PolicyRepository,
        RecommendationReportRepository, RecommendationRepository, ReportRunRepository,
        RuntimeControlRepository,
    },
};
use quant_pivot_research::artifact::{ArtifactStore, LocalArtifactStore};
use quant_pivot_storage::clickhouse::ClickHousePool;
use sea_orm::{ColumnTrait, DatabaseConnection, EntityTrait, PaginatorTrait, QueryFilter};
use tempfile::TempDir;

use super::{
    BROWSER_MARKET_ID, BROWSER_SETTLEMENT_MARKET_ID, BROWSER_TOKEN_ID, BrowserAccountStage,
    DeterministicPolygonChain, ENTRY_FILLED_SHARES, EXECUTION_NOTIONAL, MODEL_CONFIRMATION_BLOCKS,
    ProductionStackFixture, seed_browser_fixture,
};
use crate::{
    postgres::PostgresClock,
    stack::SystemStack,
    support::{
        artifact_store::VersionedArtifactStoreFixture,
        execution_pg_seed::{ProductionReportSeed, ReportSeedConfig},
    },
};

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn startup_report_diagnostics_resolve() -> Result<()> {
    for fixture in [
        ProductionStackFixture::Browser,
        ProductionStackFixture::GovernedFeedback,
    ] {
        let stack = Box::pin(SystemStack::start()).await?;
        let directory = TempDir::with_prefix("quant-pivot-browser-report-")?;
        let artifacts: Arc<dyn ArtifactStore> = Arc::new(VersionedArtifactStoreFixture::new(
            Arc::new(LocalArtifactStore::new(directory.path().to_owned())),
        ));
        let verification = tokio::time::timeout(
            StdDuration::from_mins(10),
            Box::pin(BrowserReportSeedCase::verify(fixture, &stack, &artifacts)),
        )
        .await
        .context("Browser pre-start diagnostics exceeded the bounded scenario budget")
        .and_then(|result| result);
        drop(artifacts);
        let shutdown = Box::pin(stack.shutdown()).await;
        let cleanup = directory.close();
        verification?;
        shutdown?;
        cleanup.context("remove isolated Browser report artifacts")?;
    }
    Ok(())
}

struct BrowserReportSeedCase;

impl BrowserReportSeedCase {
    async fn verify(
        fixture: ProductionStackFixture,
        stack: &SystemStack,
        artifacts: &Arc<dyn ArtifactStore>,
    ) -> Result<()> {
        let db = stack.postgres.connection();
        let polygon = Arc::new(DeterministicPolygonChain::new());
        let source = polygon.head();
        let source_at =
            DateTime::from_timestamp(source.timestamp, 0).context("fixture source timestamp")?;
        let seeded = Box::pin(seed_browser_fixture(
            db,
            &stack.clickhouse_config,
            artifacts,
            fixture,
            db.statement_time().await + Duration::days(1),
            FinalizedExecutionEvidence::runtime(
                true,
                Some(
                    source
                        .block_number
                        .checked_sub(MODEL_CONFIRMATION_BLOCKS)
                        .context("source N+12 head")?,
                ),
                Some(source_at),
            ),
            &polygon,
        ))
        .await?;
        ensure!(
            seeded.closure.is_none() && seeded.await_settlement_discovery,
            "Browser fixtures must preserve pre-start settlement discovery without the full closure DAG"
        );
        let parity_report = seeded
            .sampled_parity_report_id
            .context("seeded containment report")?;
        fixture.verify_account_positions(db).await?;
        let authority = PgRuntimeControlRepository::new(db.clone()).load().await?;
        let policy = PgPolicyRepository::new(db.clone())
            .load_current_bundle()
            .await?
            .context("seed policy")?;
        let before = Self::counts(db).await?;
        let pool = Arc::new(ClickHousePool::connect(&stack.clickhouse_config).await?);
        let port = Self::port(db, pool, artifacts);
        let reports = PgRecommendationReportRepository::new(db.clone());
        let mut recovered = Vec::new();
        for (market, expected_intent) in [
            (BROWSER_SETTLEMENT_MARKET_ID, OrderIntentStatus::Filled),
            (BROWSER_MARKET_ID, OrderIntentStatus::PendingAuthorization),
        ] {
            let recommendation = RecommendationEntity::find()
                .filter(RecommendationColumn::MarketId.eq(MarketId::new(market)))
                .one(db)
                .await?
                .context("seeded Browser recommendation")?;
            let report = reports
                .find_by_id(&recommendation.recommendation_report_id)
                .await?
                .context("seeded Browser report")?;
            let intent = IntentEntity::find()
                .filter(IntentColumn::RecommendationId.eq(recommendation.recommendation_id))
                .one(db)
                .await?
                .context("seeded Browser intent")?;
            ensure!(
                intent.status == expected_intent,
                "report seed changed its governed intent lifecycle"
            );
            let account = AccountSnapshotEntity::find_by_id(report.account_snapshot_ref)
                .one(db)
                .await?
                .context("report account snapshot")?;
            ensure!(
                account.execution_account_id == intent.execution_account_id,
                "report/intent account identity differs"
            );
            let expected_equity = fixture.account_collateral_usd()
                + if market == BROWSER_MARKET_ID {
                    ENTRY_FILLED_SHARES
                } else {
                    EXECUTION_NOTIONAL
                };
            ensure!(
                account.venue_net_liquidation_usd == Usd::new(expected_equity),
                "pre-start report equity does not match its exact account stage"
            );
            let history = Self::verify_report(db, &port, &report).await?;
            if market == BROWSER_MARKET_ID {
                ensure!(
                    report.recommendation_report_id == parity_report,
                    "seed returned another containment report"
                );
                Self::reject_fake_head(
                    fixture,
                    db,
                    report.recommendation_report_id,
                    report.decision_at,
                    history,
                )
                .await?;
            }
            recovered.push(report.recommendation_report_id);
        }
        ensure!(
            recovered.len() == 2 && recovered[0] != recovered[1],
            "both Browser reports must resolve independently"
        );
        fixture.verify_account_positions(db).await?;
        ensure!(
            Self::counts(db).await? == before,
            "read-only diagnostics created a report, account, intent, or lot"
        );
        let after = PgPolicyRepository::new(db.clone())
            .load_current_bundle()
            .await?
            .context("policy after diagnostics")?;
        ensure!(
            PgRuntimeControlRepository::new(db.clone()).load().await? == authority
                && after.decision_policy_snapshot_id == policy.decision_policy_snapshot_id
                && after.snapshot_hash == policy.snapshot_hash,
            "diagnostics changed execution authority or policy"
        );
        Ok(())
    }

    async fn counts(db: &DatabaseConnection) -> Result<(u64, u64, u64, u64)> {
        Ok((
            ReportEntity::find().count(db).await?,
            AccountSnapshotEntity::find().count(db).await?,
            IntentEntity::find().count(db).await?,
            PositionLotEntity::find().count(db).await?,
        ))
    }

    fn port(
        db: &DatabaseConnection,
        clickhouse: Arc<ClickHousePool>,
        artifacts: &Arc<dyn ArtifactStore>,
    ) -> CoreQuantReportPort {
        let reports: Arc<dyn RecommendationReportRepository> =
            Arc::new(PgRecommendationReportRepository::new(db.clone()));
        let runs: Arc<dyn ReportRunRepository> = Arc::new(PgReportRunRepository::new(db.clone()));
        let recommendations: Arc<dyn RecommendationRepository> =
            Arc::new(PgRecommendationRepository::new(db.clone()));
        let policies: Arc<dyn PolicyRepository> = Arc::new(PgPolicyRepository::new(db.clone()));
        let parity: Arc<dyn FeatureParityRepository> =
            Arc::new(PgFeatureParityRepository::new(db.clone()));
        let (events, _receiver) = CoreEventPublisher::bounded(16);
        let lifecycle = Arc::new(ReportLifecycleService::new(ReportLifecycleDeps {
            report_repo: Arc::clone(&reports),
            run_repo: Arc::clone(&runs),
            recommendation_repo: Arc::clone(&recommendations),
            builder: Arc::new(ReadOnlyReportBuilder),
            publisher: Arc::new(ReportPublisher::new(ReportPublisherDeps {
                events,
                alerts: Arc::new(AlertDispatcher::with_recordings(Arc::new(Mutex::new(
                    Vec::new(),
                )))),
                metrics: Arc::new(MetricsHub::new()),
            })),
            feature_parity_gate: Arc::new(RepositoryFeatureParityGate::new(Arc::clone(&parity))),
            feature_parity_runs: Arc::new(FeatureParityRunCoordinator::new(
                parity,
                Arc::clone(&policies),
                3,
            )),
            artifact_store: Arc::clone(artifacts),
            ad_hoc_queue_capacity: 1,
            ad_hoc_queue_ttl_secs: 60,
        }));
        CoreQuantReportPort::new(CoreQuantReportPortDeps {
            report_repo: reports,
            report_run_repo: runs,
            portfolio_plan_repo: Arc::new(PgPortfolioPlanRepository::new(db.clone())),
            recommendation_repo: recommendations,
            order_intent_repo: Arc::new(PgOrderIntentRepository::new(db.clone())),
            lifecycle,
            serving_evidence: Arc::new(ChFeatureParityEventRepository::new(Arc::clone(
                &clickhouse,
            ))),
            feature_repo: Arc::new(PgFeatureRepository::new(db.clone())),
            exchange_history_repo: Arc::new(PgExchangeHistoryRepository::new(db.clone())),
            runtime_config_repo: policies,
            quant_fact_read: Arc::new(ChQuantFactReadRepository::new(clickhouse)),
            operation_logs: Arc::new(PgOperationLogRepository::new(db.clone())),
        })
    }

    async fn verify_report(
        db: &DatabaseConnection,
        port: &CoreQuantReportPort,
        report: &RecommendationReportInfo,
    ) -> Result<HistoryServingHeadSeal> {
        let quality = PgRecommendationReportRepository::new(db.clone())
            .find_data_quality_snapshot(&report.recommendation_report_id)
            .await?
            .context("pre-start report data-quality snapshot")?;
        ensure!(
            quality.tokens_json.0.is_empty(),
            "containment seed must not invent feature/capture evidence"
        );
        let diagnostics = port
            .find_report_diagnostics(&report.recommendation_report_id)
            .await?
            .context("seeded report diagnostics must resolve instead of returning 404")?;
        let routes = PgRecommendationReportRepository::new(db.clone())
            .find_route_runs(&[report.report_run_id])
            .await?;
        ensure!(
            routes.iter().map(|route| route.route).collect::<Vec<_>>()
                == vec![BuyModelRoute::Pooled, BuyModelRoute::Weather],
            "pre-start report must preserve both real Route identities"
        );
        let weather = routes
            .iter()
            .find(|route| route.route == BuyModelRoute::Weather)
            .context("Weather Route")?;
        let lineage = weather
            .lineage_json
            .as_ref()
            .context("Weather Route lineage")?;
        let (head_id, head_hash) = match &lineage.history {
            RouteHistoryLineage::Runtime {
                serving_head_seal_id,
                serving_head_seal_hash,
            } => (*serving_head_seal_id, *serving_head_seal_hash),
            RouteHistoryLineage::Materialized { .. } => {
                bail!("Browser report must bind a real Runtime head")
            }
        };
        ensure!(
            head_id.as_uuid() != report.recommendation_report_id.as_uuid(),
            "history head cannot be forged from the report UUID"
        );
        let history = PgExchangeHistoryRepository::new(db.clone())
            .validate_serving_head(head_id, head_hash)
            .await?;
        let run = PgReportRunRepository::new(db.clone())
            .find_by_output_report(&report.recommendation_report_id)
            .await?
            .context("report decision owner")?;
        let policy = PgPolicyRepository::new(db.clone())
            .load_snapshot(&report.decision_policy_snapshot_id)
            .await?
            .context("report frozen policy")?;
        let boundary = DecisionClock::new(u64::try_from(
            run.knowledge_lag_secs
                .context("report frozen knowledge lag")?,
        )?)
        .serving_boundary(
            report.decision_at,
            policy
                .snapshot
                .profile_artifacts
                .domain
                .definition
                .crypto
                .availability_lag_secs,
            policy
                .snapshot
                .profile_artifacts
                .domain
                .definition
                .weather
                .availability_lag_secs,
        )?
        .with_source_watermark(
            DecisionSource::FinalizedExecution,
            history.seal.effective_through_at,
        )?;
        ensure!(
            diagnostics.decision_boundary == DecisionBoundaryEvidenceView::from(&boundary),
            "diagnostics did not restore every configured source cutoff and the exact head watermark"
        );
        ensure!(
            diagnostics.global.stage_ceiling == FeatureParityStage::Selection
                && diagnostics.global.evidence_complete
                && diagnostics.global.decision_capture_count.is_none()
                && diagnostics.global.feature_vector_count.is_none()
                && diagnostics.global.feature_cell_count.is_none()
                && diagnostics.global.model_input_count.is_none(),
            "empty DQ must not fabricate post-selection evidence"
        );
        let weather_evidence = &diagnostics
            .routes
            .iter()
            .find(|route| route.route == BuyModelRoute::Weather)
            .context("Weather diagnostics")?
            .evidence;
        ensure!(
            weather_evidence.stage_ceiling == FeatureParityStage::Prediction
                && !weather_evidence.evidence_complete
                && weather_evidence.model_input_count.is_none(),
            "missing serving input must remain explicitly incomplete"
        );
        let pooled = routes
            .iter()
            .find(|route| route.route == BuyModelRoute::Pooled)
            .context("Pooled Route")?;
        let pooled_lineage = pooled
            .lineage_json
            .as_ref()
            .context("Pooled real lineage")?;
        ensure!(
            pooled.model_run_id.is_none()
                && pooled_lineage.model_run_id.is_none()
                && pooled_lineage.model_version_id != lineage.model_version_id
                && pooled_lineage.history == lineage.history
                && pooled_lineage.report_universe_plan_hash == lineage.report_universe_plan_hash,
            "Pooled zero-candidate Route lost its distinct model or shared source authority"
        );
        Ok(history)
    }

    async fn reject_fake_head(
        fixture: ProductionStackFixture,
        db: &DatabaseConnection,
        report_id: RecommendationReportId,
        decision_at: DateTime<Utc>,
        history: HistoryServingHeadSeal,
    ) -> Result<()> {
        let mut seed = ProductionReportSeed {
            catalog: ReportSeedConfig {
                event_id: "evt-1".to_owned(),
                market_id: BROWSER_MARKET_ID.to_owned(),
                market_question: "Will it?".to_owned(),
                market_slug: "will-it".to_owned(),
                token_id: BROWSER_TOKEN_ID.to_owned(),
                trigger_key: format!("test:invalid-head:{report_id}"),
            },
            account: fixture
                .browser_equity(db, BrowserAccountStage::SettledHolding)
                .await?,
            history_head: history,
        };
        ensure!(
            seed.validated_history(db, decision_at).await? == seed.history_head,
            "valid pre-start head changed during validation"
        );
        seed.history_head.seal.serving_head_seal_id =
            HistoryServingHeadSealId::new(report_id.as_uuid());
        seed.history_head.seal.seal_hash = seed.history_head.derive_hash()?;
        let error = seed
            .validated_history(db, decision_at)
            .await
            .err()
            .context("forged report-UUID head must fail before publication")?;
        ensure!(
            matches!(
                error.downcast_ref::<StorageError>(),
                Some(StorageError::NotFound {
                    entity: "quant_history_serving_head_seal",
                    ..
                })
            ),
            "forged history head failed for an unrelated reason: {error:#}"
        );
        Ok(())
    }
}

struct ReadOnlyReportBuilder;

#[async_trait]
impl ReportBuilder for ReadOnlyReportBuilder {
    async fn build(&self, _request: BuildReportRequest) -> QuantResult<ComposedReport> {
        Err(ReportError::InvariantViolation {
            stage: "browser_diagnostics_test",
            detail: "read-only diagnostics must never build another report".to_owned(),
        }
        .into())
    }
}
