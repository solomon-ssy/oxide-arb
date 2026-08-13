//! Report-pipeline system contracts against disposable `PostgreSQL`.

use chrono::{Duration, Utc};
use quant_pivot_error::{QuantError, account::AccountError, report::ReportError};
use quant_pivot_models::{
    domain::{api::OperationLogQuery, quant::NewEquitySnapshot},
    entities::{
        quant_market_selection::Entity as MarketSelectionEntity,
        quant_model_run::Entity as ModelRunEntity,
    },
    enums::quant::{
        AccountSource, EmptyReportReason, RecommendationReportStatus, RecommendationStatus,
    },
    types::{EquitySnapshotId, ReportTriggerKey, Usd},
};
use quant_pivot_repository::{
    postgres::{
        PgEquitySnapshotRepository, PgFactorRepository, PgMarketSelectionRepository,
        PgOperationLogRepository, PgPortfolioPlanRepository,
    },
    traits::{
        EquitySnapshotRepository, FactorRepository, MarketSelectionRepository,
        OperationLogRepository, PortfolioPlanRepository, RecommendationReportRepository,
        RecommendationRepository, ReportRunRepository,
    },
};
use quant_pivot_system_tests::{
    postgres::setup_pg,
    support::report_pipeline_harness::{HarnessOptions, MARKET_ID_2, ReportPipelineHarness},
};
use rust_decimal_macros::dec;
use sea_orm::{EntityTrait, PaginatorTrait};

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
        .list_route_runs(&report.recommendation_report_id)
        .await
        .expect("load report Route runs");
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
        .list_route_runs(&report.recommendation_report_id)
        .await
        .expect("load pinned report Route runs");
    assert_eq!(route_runs.len(), 1);
    assert_eq!(
        route_runs[0].model_version_id,
        Some(harness.model_version_id)
    );
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
            .entry
            .visible_liquidity_usd
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
