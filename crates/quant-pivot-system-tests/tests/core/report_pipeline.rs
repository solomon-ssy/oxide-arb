//! Report-pipeline system contracts against disposable `PostgreSQL` and `ClickHouse`.

use std::{sync::Arc, time::Duration as StdDuration};

use async_trait::async_trait;
use chrono::{DateTime, Duration, Utc};
use quant_pivot_core::app::ports::quant_report::{CoreQuantReportPort, CoreQuantReportPortDeps};
use quant_pivot_error::{
    QuantError, account::AccountError, report::ReportError, research::ResearchError,
    storage::StorageError,
};
use quant_pivot_models::{
    domain::{
        api::{DecisionBoundaryEvidenceView, OperationLogQuery},
        data_plane::{DecisionClock, DecisionSource},
        ports::QuantReportPort,
        quant::{FeatureVectorInfo, NewEquitySnapshot, NewFeatureVector},
    },
    entities::{
        quant_market_selection::Entity as MarketSelectionEntity,
        quant_model_run::Entity as ModelRunEntity,
    },
    enums::quant::{
        AccountSource, EmptyReportReason, RecommendationReportStatus, RecommendationStatus,
    },
    runtime_config::BuyModelRoute,
    types::{
        EquitySnapshotId, FeatureVectorId, FinalizedExecutionEvidence, RecommendationReportId,
        ReportRunId, ReportTriggerKey, Usd,
    },
};
use quant_pivot_repository::{
    clickhouse::{ChFeatureParityEventRepository, ChQuantFactReadRepository},
    postgres::{
        PgEquitySnapshotRepository, PgFactorRepository, PgFeatureRepository,
        PgMarketSelectionRepository, PgOperationLogRepository, PgOrderIntentRepository,
        PgPolicyRepository, PgPortfolioPlanRepository,
    },
    traits::{
        EquitySnapshotRepository, FactorRepository, FeatureRepository, MarketSelectionRepository,
        OperationLogRepository, PolicyRepository, PortfolioPlanRepository,
        RecommendationReportRepository, RecommendationRepository, ReportRunRepository,
    },
};
use quant_pivot_storage::clickhouse::{ChWriteManager, ClickHousePool};
use quant_pivot_system_tests::{
    postgres::setup_pg,
    stack::SystemStack,
    support::report_pipeline_harness::{
        HarnessOptions, MARKET_ID_2, ReportEvidenceWriters, ReportPipelineHarness,
    },
};
use rust_decimal_macros::dec;
use sea_orm::{EntityTrait, PaginatorTrait};
use uuid::Uuid;

pub async fn diagnostics_full_boundary() {
    let stack = Box::pin(SystemStack::start())
        .await
        .expect("start report diagnostics PG/CH stack");
    let verification = tokio::time::timeout(
        StdDuration::from_mins(5),
        Box::pin(ReportDiagnosticsFixture::verify(&stack)),
    )
    .await;
    Box::pin(stack.shutdown())
        .await
        .expect("remove report diagnostics PG/CH stack");
    verification.expect("report diagnostics must finish within its bounded budget");
}

struct ReportDiagnosticsFixture {
    harness: ReportPipelineHarness,
    clickhouse: Arc<ClickHousePool>,
}

impl ReportDiagnosticsFixture {
    async fn verify(stack: &SystemStack) {
        let db = stack.postgres.connection();
        let clickhouse = Arc::new(
            ClickHousePool::connect(&stack.clickhouse_config)
                .await
                .expect("connect report diagnostics evidence reader"),
        );
        let manager = Arc::new(ChWriteManager::new(
            stack.clickhouse_config.max_concurrent_inserts,
            &stack.clickhouse_config.io,
        ));
        // PostgreSQL keeps microseconds, while CH scalar clocks project to
        // milliseconds. The JSON source watermark must retain this precision.
        let watermark = DateTime::from_timestamp_micros(
            (Utc::now() - Duration::minutes(1)).timestamp_millis() * 1_000 + 123,
        )
        .expect("microsecond finalized-history watermark");
        let harness = Box::pin(ReportPipelineHarness::bootstrap(
            db,
            HarnessOptions {
                evidence: ReportEvidenceWriters::clickhouse(Arc::clone(&clickhouse), manager),
                finalized_history_watermark: Some(watermark),
                ..HarnessOptions::default()
            },
        ))
        .await;
        let report = harness
            .execute_ad_hoc(harness.ad_hoc_request("diagnostics-full-boundary"))
            .await
            .expect("production report lifecycle with acknowledged serving evidence");
        assert_eq!(report.status, RecommendationReportStatus::Published);
        let policy = PgPolicyRepository::new(db.clone())
            .load_snapshot(&report.decision_policy_snapshot_id)
            .await
            .expect("load frozen report policy")
            .expect("report policy exists")
            .snapshot;
        let expected_boundary = DecisionClock::new(0)
            .serving_boundary(
                report.decision_at,
                policy
                    .profile_artifacts
                    .domain
                    .definition
                    .crypto
                    .availability_lag_secs,
                policy
                    .profile_artifacts
                    .domain
                    .definition
                    .weather
                    .availability_lag_secs,
            )
            .expect("canonical report clock")
            .with_source_watermark(DecisionSource::FinalizedExecution, watermark)
            .expect("bind the observed finalized-history watermark");
        assert!(watermark < expected_boundary.knowledge_cutoff());
        let snapshot = harness
            .report_repo
            .find_data_quality_snapshot(&report.recommendation_report_id)
            .await
            .expect("read production data-quality snapshot")
            .expect("report data-quality snapshot exists");
        let vector_ids = snapshot
            .tokens_json
            .0
            .iter()
            .map(|token| token.feature_vector_id)
            .collect::<Vec<_>>();
        let features = Arc::new(PgFeatureRepository::new(db.clone()));
        let persisted = features
            .find_by_ids(&vector_ids)
            .await
            .expect("read actual persisted feature and capture evidence");
        assert!(!persisted.is_empty());
        assert!(persisted.iter().all(|feature| {
            feature.decision_boundary == expected_boundary
                && feature.decision_capture.finalized_execution_evidence
                    == FinalizedExecutionEvidence::runtime(
                        true,
                        Some((i64::MAX - 1).unsigned_abs()),
                        Some(watermark),
                    )
        }));
        let fixture = Self {
            harness,
            clickhouse,
        };
        let diagnostics = fixture
            .port(features)
            .find_report_diagnostics(&report.recommendation_report_id)
            .await
            .expect("canonical report diagnostics accepts the complete persisted boundary")
            .expect("report diagnostics exists");
        assert_eq!(
            diagnostics.decision_boundary,
            DecisionBoundaryEvidenceView::from(&expected_boundary)
        );
        assert!(diagnostics.global.evidence_complete);
        assert_eq!(
            diagnostics.global.feature_vector_count,
            Some(u64::try_from(persisted.len()).expect("fixture vector count fits u64"))
        );
        assert!(
            diagnostics
                .global
                .feature_cell_count
                .is_some_and(|count| count > 0)
        );
        assert_eq!(
            diagnostics.routes.len(),
            report.represented_routes_json.routes.len()
        );
        assert!(
            diagnostics
                .routes
                .iter()
                .all(|route| route.evidence.evidence_complete)
        );
        assert!(diagnostics.routes.iter().any(|route| {
            route.route == BuyModelRoute::Weather
                && route
                    .evidence
                    .model_input_count
                    .is_some_and(|count| count > 0)
                && route.evidence.model_route.is_some()
        }));
        fixture
            .reject_corruption(&report.recommendation_report_id)
            .await;
    }

    fn port(&self, feature_repo: Arc<dyn FeatureRepository>) -> CoreQuantReportPort {
        let db = &self.harness.db;
        CoreQuantReportPort::new(CoreQuantReportPortDeps {
            report_repo: Arc::clone(&self.harness.report_repo)
                as Arc<dyn RecommendationReportRepository>,
            report_run_repo: Arc::clone(&self.harness.report_run_repo)
                as Arc<dyn ReportRunRepository>,
            portfolio_plan_repo: Arc::new(PgPortfolioPlanRepository::new(db.clone())),
            recommendation_repo: Arc::clone(&self.harness.recommendation_repo)
                as Arc<dyn RecommendationRepository>,
            order_intent_repo: Arc::new(PgOrderIntentRepository::new(db.clone())),
            lifecycle: Arc::clone(&self.harness.lifecycle),
            serving_evidence: Arc::new(ChFeatureParityEventRepository::new(Arc::clone(
                &self.clickhouse,
            ))),
            feature_repo,
            runtime_config_repo: Arc::new(PgPolicyRepository::new(db.clone())),
            exchange_history_repo: Arc::clone(&self.harness.exchange_history_repo),
            quant_fact_read: Arc::new(ChQuantFactReadRepository::new(Arc::clone(&self.clickhouse))),
            operation_logs: Arc::new(PgOperationLogRepository::new(db.clone())),
        })
    }

    async fn reject_corruption(&self, report_id: &RecommendationReportId) {
        for fault in [
            FeatureEvidenceFault::Clock,
            FeatureEvidenceFault::Cutoff,
            FeatureEvidenceFault::Capture,
        ] {
            let port = self.port(Arc::new(FaultedFeatureReader {
                inner: PgFeatureRepository::new(self.harness.db.clone()),
                fault,
            }));
            assert!(
                matches!(
                    port.find_report_diagnostics(report_id).await,
                    Err(QuantError::Research(ResearchError::Determinism { .. }))
                ),
                "diagnostics must reject read-time {fault:?} corruption"
            );
        }
        assert!(
            self.port(Arc::new(PgFeatureRepository::new(self.harness.db.clone())))
                .find_report_diagnostics(report_id)
                .await
                .expect("fault injection cannot mutate WORM rows")
                .expect("report still exists")
                .global
                .evidence_complete
        );
    }
}

#[derive(Debug, Clone, Copy)]
enum FeatureEvidenceFault {
    Clock,
    Cutoff,
    Capture,
}

struct FaultedFeatureReader {
    inner: PgFeatureRepository,
    fault: FeatureEvidenceFault,
}

impl FaultedFeatureReader {
    fn corrupt(&self, feature: &mut FeatureVectorInfo) {
        match self.fault {
            FeatureEvidenceFault::Clock => feature.decision_at += Duration::microseconds(1),
            FeatureEvidenceFault::Cutoff => {
                feature.decision_boundary = feature
                    .decision_boundary
                    .clone()
                    .with_source_watermark(
                        DecisionSource::FinalizedExecution,
                        feature
                            .decision_boundary
                            .cutoff_for(DecisionSource::FinalizedExecution)
                            - Duration::microseconds(1),
                    )
                    .expect("tightened corruption remains a valid boundary shape");
            }
            FeatureEvidenceFault::Capture => {
                feature.decision_capture.finalized_execution_evidence =
                    FinalizedExecutionEvidence::NotRequired;
            }
        }
    }
}

#[async_trait]
impl FeatureRepository for FaultedFeatureReader {
    async fn create(&self, _vector: NewFeatureVector) -> Result<FeatureVectorInfo, StorageError> {
        Err(StorageError::InvariantViolation {
            entity: Some("quant_feature_vector"),
            detail: "diagnostic fault injection is read-only".to_owned(),
        })
    }

    async fn create_batch(
        &self,
        _vectors: Vec<NewFeatureVector>,
    ) -> Result<Vec<FeatureVectorInfo>, StorageError> {
        Err(StorageError::InvariantViolation {
            entity: Some("quant_feature_vector"),
            detail: "diagnostic fault injection is read-only".to_owned(),
        })
    }

    async fn find_by_id(
        &self,
        id: &FeatureVectorId,
    ) -> Result<Option<FeatureVectorInfo>, StorageError> {
        let mut feature = self.inner.find_by_id(id).await?;
        if let Some(feature) = &mut feature {
            self.corrupt(feature);
        }
        Ok(feature)
    }

    async fn find_by_ids(
        &self,
        ids: &[FeatureVectorId],
    ) -> Result<Vec<FeatureVectorInfo>, StorageError> {
        let mut features = self.inner.find_by_ids(ids).await?;
        for feature in &mut features {
            self.corrupt(feature);
        }
        Ok(features)
    }
}

pub async fn ad_hoc_publishes_recommendations() {
    let (pool, _container) = setup_pg().await;
    let db = pool.connection().clone();
    let harness = Box::pin(ReportPipelineHarness::bootstrap(
        &db,
        HarnessOptions::default(),
    ))
    .await;

    let request_id = "ad-hoc-publish-recs";
    let report = harness
        .execute_ad_hoc(harness.ad_hoc_request(request_id))
        .await
        .expect("ad-hoc report");

    assert_eq!(
        report.status,
        RecommendationReportStatus::Published,
        "unexpected empty report: reason={:?}, summary={:?}",
        report.status_reason,
        report.summary_json
    );
    let route_runs = harness
        .report_repo
        .find_route_runs(&[report.report_run_id])
        .await
        .expect("load report Route runs");
    assert!(
        harness
            .report_repo
            .find_route_runs(&[])
            .await
            .expect("empty Route-run batch")
            .is_empty()
    );
    assert!(
        harness
            .report_repo
            .find_route_runs(&[ReportRunId::new(Uuid::now_v7())])
            .await
            .expect("missing Route-run batch")
            .is_empty(),
        "missing report runs must not fabricate Route outcomes"
    );
    let second_report = harness
        .execute_ad_hoc(harness.ad_hoc_request("ad-hoc-route-batch"))
        .await
        .expect("second ad-hoc report");
    let batched_route_runs = harness
        .report_repo
        .find_route_runs(&[second_report.report_run_id, report.report_run_id])
        .await
        .expect("load two reports' Route runs in one batch");
    assert_eq!(
        batched_route_runs.len(),
        route_runs.len() * 2,
        "one batch must return the exact Route rows for both report runs"
    );
    assert!(batched_route_runs.windows(2).all(|window| {
        (window[0].report_run_id.as_uuid(), window[0].route)
            <= (window[1].report_run_id.as_uuid(), window[1].route)
    }));
    let mut factors = Vec::new();
    for model_run_id in route_runs.iter().filter_map(|run| run.model_run_id) {
        factors.extend(
            PgFactorRepository::new(db.clone())
                .list_values_for_run(&model_run_id)
                .await
                .expect("load Route-run factor values"),
        );
    }
    let selection_members = PgMarketSelectionRepository::new(db.clone())
        .list_members(&report.market_selection_id)
        .await
        .expect("load report market-selection members");
    let selection = PgMarketSelectionRepository::new(db.clone())
        .find_by_id(&report.market_selection_id)
        .await
        .expect("load report market-selection snapshot")
        .expect("report market-selection snapshot");
    let portfolio_plan = PgPortfolioPlanRepository::new(db.clone())
        .find_by_id(&report.portfolio_plan_id)
        .await
        .expect("load report portfolio plan")
        .expect("report portfolio plan");
    assert!(
        report.summary_json.published_recommendation_count >= 1,
        "report published no recommendations: summary={:?}, decision={:?}, route_runs={route_runs:?}, selection={selection:?}, selection_members={selection_members:?}, factors={factors:?}",
        report.summary_json,
        portfolio_plan.decision_json,
    );

    let operation_logs = PgOperationLogRepository::new(db.clone());
    let prepare_logs = operation_logs
        .page(OperationLogQuery {
            request_id: Some(format!("ad_hoc:{request_id}")),
            ..OperationLogQuery::default()
        })
        .await
        .expect("prepare operation log");
    assert_eq!(prepare_logs.total, 1);
    assert_eq!(prepare_logs.items[0].action.as_str(), "prepare");
    assert!(
        prepare_logs.items[0].after_hash.is_some(),
        "prepare must record after_hash"
    );

    let publish_logs = operation_logs
        .page(OperationLogQuery {
            request_id: Some(format!(
                "quant-report:publish:{}",
                report.recommendation_report_id
            )),
            ..OperationLogQuery::default()
        })
        .await
        .expect("publish operation log");
    assert_eq!(publish_logs.total, 1);
    assert_eq!(publish_logs.items[0].action.as_str(), "report.publish");
    assert!(publish_logs.items[0].before_hash.is_some());
    assert!(publish_logs.items[0].after_hash.is_some());

    let recs = harness
        .recommendation_repo
        .find_by_report(&report.recommendation_report_id)
        .await
        .expect("load recommendations");
    assert!(!recs.is_empty());
    assert!(recs.iter().all(|recommendation| {
        recommendation
            .economics_json
            .robust_expected_net_usd
            .is_positive()
            && recommendation
                .economics_json
                .marginal_portfolio_value_usd
                .is_positive()
    }));
    assert!(recs.windows(2).all(|pair| {
        pair[0].economics_json.marginal_portfolio_value_usd
            >= pair[1].economics_json.marginal_portfolio_value_usd
    }));
    assert_eq!(recs[0].market_id.as_str(), MARKET_ID_2);
}

pub async fn pinned_route_uses_generation() {
    let (pool, _container) = setup_pg().await;
    let db = pool.connection().clone();
    let harness = Box::pin(ReportPipelineHarness::bootstrap(
        &db,
        HarnessOptions::default(),
    ))
    .await;
    let selection_count = MarketSelectionEntity::find()
        .count(&db)
        .await
        .expect("count market selections before route rejection");
    let model_run_count = ModelRunEntity::find()
        .count(&db)
        .await
        .expect("count model runs before route rejection");

    let report = harness
        .execute_ad_hoc(harness.ad_hoc_request("pinned-weather-route"))
        .await
        .expect("a complete route generation must use its immutable model contract");
    let route_runs = harness
        .report_repo
        .find_route_runs(&[report.report_run_id])
        .await
        .expect("load pinned report Route runs");
    assert_eq!(route_runs.len(), 2);
    assert!(
        route_runs
            .iter()
            .any(|route_run| route_run.route == BuyModelRoute::Pooled),
        "the immutable report universe must retain its Pooled primary Route"
    );
    let weather_run = route_runs
        .iter()
        .find(|route_run| route_run.route == BuyModelRoute::Weather)
        .expect("the immutable report universe includes the selected Weather Route");
    assert_eq!(weather_run.model_version_id, Some(harness.model_version_id));
    assert_eq!(
        MarketSelectionEntity::find()
            .count(&db)
            .await
            .expect("count market selections after pinned route"),
        selection_count + 1,
        "a pinned route must advance through market selection"
    );
    assert!(
        ModelRunEntity::find()
            .count(&db)
            .await
            .expect("count model runs after pinned route")
            > model_run_count,
        "the pinned generation must execute without a mutable registry re-read"
    );
}

pub async fn ad_hoc_idempotent_key() {
    let (pool, _container) = setup_pg().await;
    let db = pool.connection().clone();
    let harness = Box::pin(ReportPipelineHarness::bootstrap(
        &db,
        HarnessOptions::default(),
    ))
    .await;

    let request = harness.ad_hoc_request("idempotent-ad-hoc");
    let first = harness
        .execute_ad_hoc(request.clone())
        .await
        .expect("first ad-hoc");
    let second = harness
        .execute_ad_hoc(request)
        .await
        .expect("second ad-hoc");

    assert_eq!(
        first.recommendation_report_id,
        second.recommendation_report_id
    );

    let trigger_key =
        ReportTriggerKey::parse("ad_hoc:idempotent-ad-hoc").expect("report trigger key");
    let row = harness
        .report_run_repo
        .find_by_trigger_key(&trigger_key)
        .await
        .expect("lookup trigger key")
        .expect("single committed row");
    assert_eq!(row.output_report_id, Some(first.recommendation_report_id));
}

pub async fn empty_selection_publishes_report() {
    let (pool, _container) = setup_pg().await;
    let db = pool.connection().clone();
    let harness = Box::pin(ReportPipelineHarness::bootstrap(
        &db,
        HarnessOptions::empty_selection(),
    ))
    .await;

    let report = harness
        .execute_ad_hoc(harness.ad_hoc_request("empty-selection"))
        .await
        .expect("empty selection report");

    assert_eq!(report.status, RecommendationReportStatus::Published);
    assert_eq!(report.summary_json.published_recommendation_count, 0);
    assert_eq!(
        report.summary_json.empty_reason,
        Some(EmptyReportReason::EmptySelection)
    );

    let recs = harness
        .recommendation_repo
        .find_by_report(&report.recommendation_report_id)
        .await
        .expect("load recommendations");
    assert!(recs.is_empty());
}

pub async fn missing_non_empty_report() {
    let (pool, _container) = setup_pg().await;
    let db = pool.connection().clone();
    let harness = Box::pin(ReportPipelineHarness::bootstrap(
        &db,
        HarnessOptions::missing_trade_policy(),
    ))
    .await;

    let request_id = "missing-trade-policy";
    let error = harness
        .execute_ad_hoc(harness.ad_hoc_request(request_id))
        .await
        .expect_err("missing Route trade policy must fail the report run");
    assert!(
        matches!(
            error,
            QuantError::Report(ReportError::RouteReadiness { .. })
        ),
        "unexpected missing-policy error: {error}"
    );
    let trigger_key =
        ReportTriggerKey::parse(format!("ad_hoc:{request_id}")).expect("report trigger key");
    let existing = harness
        .report_run_repo
        .find_by_trigger_key(&trigger_key)
        .await
        .expect("lookup trigger key");
    assert!(
        existing.is_some_and(|run| run.output_report_id.is_none()),
        "missing Route readiness must retain run diagnostics without publishing a report"
    );
}

pub async fn account_fails_without_row() {
    let (pool, _container) = setup_pg().await;
    let db = pool.connection().clone();
    let harness = Box::pin(ReportPipelineHarness::bootstrap(
        &db,
        HarnessOptions::unavailable_account(),
    ))
    .await;

    let request_id = "account-unavailable";
    let error = harness
        .execute_ad_hoc(harness.ad_hoc_request(request_id))
        .await
        .expect_err("account unavailable must fail closed");

    assert!(matches!(
        error,
        QuantError::Account(AccountError::CredentialsMissing)
    ));

    let trigger_key =
        ReportTriggerKey::parse(format!("ad_hoc:{request_id}")).expect("report trigger key");
    let existing = harness
        .report_run_repo
        .find_by_trigger_key(&trigger_key)
        .await
        .expect("lookup trigger key");
    assert!(
        existing.is_some_and(|run| run.output_report_id.is_none()),
        "failed build must retain its run but not persist a report artifact"
    );
}

pub async fn revoke_after_publish() {
    let (pool, _container) = setup_pg().await;
    let db = pool.connection().clone();
    let harness = Box::pin(ReportPipelineHarness::bootstrap(
        &db,
        HarnessOptions::default(),
    ))
    .await;

    let report = harness
        .execute_ad_hoc(harness.ad_hoc_request("revoke-me"))
        .await
        .expect("publish report");

    let revoked = harness
        .lifecycle
        .revoke(
            &report.recommendation_report_id,
            "operator revoke",
            Utc::now(),
        )
        .await
        .expect("revoke report");

    assert_eq!(revoked.status, RecommendationReportStatus::Revoked);
    assert!(revoked.revoked_at.is_some());
    assert_eq!(revoked.status_reason.as_deref(), Some("operator revoke"));

    let recs = harness
        .recommendation_repo
        .find_by_report(&report.recommendation_report_id)
        .await
        .expect("load recommendations");
    assert!(
        recs.iter()
            .all(|rec| rec.status == RecommendationStatus::Revoked)
    );

    let op_logs = PgOperationLogRepository::new(db.clone())
        .page(OperationLogQuery {
            request_id: Some(format!(
                "quant-report:revoke:{}",
                report.recommendation_report_id
            )),
            ..OperationLogQuery::default()
        })
        .await
        .expect("revoke operation log");
    assert_eq!(op_logs.total, 1);
    assert!(
        op_logs.items[0].before_hash.is_some(),
        "system revoke must record before_hash"
    );
    assert!(
        op_logs.items[0].after_hash.is_some(),
        "system revoke must record after_hash"
    );
}

pub async fn evidence_refs_rank_populated() {
    let (pool, _container) = setup_pg().await;
    let db = pool.connection().clone();
    let harness = Box::pin(ReportPipelineHarness::bootstrap(
        &db,
        HarnessOptions::default(),
    ))
    .await;

    let report = harness
        .execute_ad_hoc(harness.ad_hoc_request("evidence-and-ranks"))
        .await
        .expect("published report");

    let recs = harness
        .recommendation_repo
        .find_by_report(&report.recommendation_report_id)
        .await
        .expect("load recommendations");
    assert!(!recs.is_empty(), "expected at least one recommendation");

    let rec = &recs[0];
    assert!(
        !rec.evidence_refs.feature_vector_id.to_string().is_empty(),
        "feature_vector_id must be populated"
    );
    assert!(
        !rec.evidence_refs.model_run_id.to_string().is_empty(),
        "model_run_id must be populated"
    );
    assert!(
        !rec.evidence_refs.signal_candidate_id.to_string().is_empty(),
        "signal_candidate_id must be populated"
    );
    assert!(
        !rec.evidence_refs
            .book_snapshot_ref
            .token_id
            .to_string()
            .is_empty(),
        "book_snapshot_ref must be populated from decision capture"
    );
    assert!(
        rec.economic_tier_json
            .entry_execution
            .visible_liquidity_usd()
            .is_positive()
    );
    assert!(rec.economics_json.robust_expected_net_usd.is_positive());
    assert_eq!(
        rec.economics_json, rec.economic_tier_json.economics,
        "published economics must be the exact selected tier economics"
    );
    assert!(
        !rec.factor_breakdown.0.is_empty(),
        "factor breakdown evidence should be present"
    );
}

pub async fn report_persists_real_history() {
    let collateral = Usd::new(dec!(8000));
    let peak = Usd::new(dec!(10000));

    let (pool, _container) = setup_pg().await;
    let db = pool.connection().clone();

    PgEquitySnapshotRepository::new(db.clone())
        .create(NewEquitySnapshot {
            equity_snapshot_id: EquitySnapshotId::from_v7(),
            as_of: Utc::now() - Duration::hours(1),
            source: AccountSource::Polymarket,
            venue_net_liquidation_usd: peak,
            capital_base_usd: peak,
            available_usd: peak,
            reserved_usd: Usd::ZERO,
            realized_pnl_cumulative_usd: Usd::ZERO,
            unrealized_pnl_usd: Usd::ZERO,
            incentive_credit_cumulative_usd: Usd::ZERO,
            high_water_mark_usd: peak,
            drawdown_pct: dec!(0),
            account_snapshot_ref: None,
        })
        .await
        .expect("seed peak equity history");

    let harness = Box::pin(ReportPipelineHarness::bootstrap(
        &db,
        HarnessOptions {
            collateral,
            ..HarnessOptions::default()
        },
    ))
    .await;

    let report = harness
        .execute_ad_hoc(harness.ad_hoc_request("drawdown-aware-sizing"))
        .await
        .expect("drawdown-aware report");

    let equity = PgEquitySnapshotRepository::new(db.clone())
        .find_by_id(&report.equity_snapshot_ref)
        .await
        .expect("load equity snapshot")
        .expect("equity snapshot row");
    assert_eq!(equity.drawdown_pct, dec!(0.2));
    assert_eq!(equity.high_water_mark_usd, peak);
    assert_eq!(equity.capital_base_usd, collateral);

    let portfolio_plan = PgPortfolioPlanRepository::new(db)
        .find_by_id(&report.portfolio_plan_id)
        .await
        .expect("load drawdown portfolio plan")
        .expect("drawdown portfolio plan row");
    assert_eq!(
        portfolio_plan.existing_state_json.current_drawdown_usd,
        Usd::new(dec!(2000)),
        "the global optimizer must freeze the real account drawdown in USD"
    );
}
