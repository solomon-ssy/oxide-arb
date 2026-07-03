//! Phase 04 report pipeline end-to-end tests (Postgres + testcontainers).

use chrono::Utc;
use quant_pivot_core::report::AdHocReportRequest;
use quant_pivot_error::{QuantError, account::AccountError};
use quant_pivot_models::{
    domain::{NewEquitySnapshot, OperationLogQuery},
    enums::quant::{
        AccountSource, EmptyReportReason, RecommendationReportStatus, RecommendationStatus,
    },
    types::{EquitySnapshotId, Usd},
};
use quant_pivot_repository::{
    postgres::{PgEquitySnapshotRepository, PgOperationLogRepository},
    traits::{
        EquitySnapshotRepository, OperationLogRepository, RecommendationReportRepository,
        RecommendationRepository,
    },
};
use quant_pivot_test_support::{
    pg::setup_pg,
    report_pipeline_harness::{HarnessOptions, MARKET_ID, ReportPipelineHarness},
};
use rust_decimal_macros::dec;

#[tokio::test]
#[ignore = "requires Docker"]
async fn ad_hoc_publishes_report_with_recommendations() {
    let (pool, _container) = setup_pg().await;
    let db = pool.connection().clone();
    let harness = ReportPipelineHarness::bootstrap(&db, HarnessOptions::default()).await;

    let request_id = "ad-hoc-publish-recs";
    let report = harness
        .lifecycle
        .run_ad_hoc(AdHocReportRequest {
            request_id: request_id.to_owned(),
            trigger_time: Utc::now(),
            top_n: Some(5),
            source_delay_secs: Some(0),
        })
        .await
        .expect("ad-hoc report");

    assert_eq!(report.status, RecommendationReportStatus::Published);
    assert!(report.summary_json.published_recommendation_count >= 1);

    let publish_logs = PgOperationLogRepository::new(db.clone())
        .page(OperationLogQuery {
            request_id: Some(format!("ad_hoc:{request_id}")),
            ..OperationLogQuery::default()
        })
        .await
        .expect("publish operation log");
    assert_eq!(publish_logs.total, 1);
    assert!(
        publish_logs.items[0].after_hash.is_some(),
        "publish must record after_hash"
    );

    let recs = harness
        .recommendation_repo
        .find_by_report(&report.recommendation_report_id)
        .await
        .expect("load recommendations");
    assert!(!recs.is_empty());
    assert_eq!(recs[0].market_id.as_str(), MARKET_ID);
}

#[tokio::test]
#[ignore = "requires Docker"]
async fn ad_hoc_idempotent_on_trigger_key() {
    let (pool, _container) = setup_pg().await;
    let db = pool.connection().clone();
    let harness = ReportPipelineHarness::bootstrap(&db, HarnessOptions::default()).await;

    let request = AdHocReportRequest {
        request_id: "idempotent-ad-hoc".to_owned(),
        trigger_time: Utc::now(),
        top_n: Some(5),
        source_delay_secs: Some(0),
    };
    let first = harness
        .lifecycle
        .run_ad_hoc(request.clone())
        .await
        .expect("first ad-hoc");
    let second = harness
        .lifecycle
        .run_ad_hoc(request)
        .await
        .expect("second ad-hoc");

    assert_eq!(
        first.recommendation_report_id,
        second.recommendation_report_id
    );

    let trigger_key = "ad_hoc:idempotent-ad-hoc";
    let row = harness
        .report_repo
        .find_by_trigger_key(trigger_key)
        .await
        .expect("lookup trigger key")
        .expect("single committed row");
    assert_eq!(row.recommendation_report_id, first.recommendation_report_id);
}

#[tokio::test]
#[ignore = "requires Docker"]
async fn empty_selection_publishes_published_empty() {
    let (pool, _container) = setup_pg().await;
    let db = pool.connection().clone();
    let harness = ReportPipelineHarness::bootstrap(&db, HarnessOptions::empty_selection()).await;

    let report = harness
        .lifecycle
        .run_ad_hoc(AdHocReportRequest {
            request_id: "empty-selection".to_owned(),
            trigger_time: Utc::now(),
            top_n: Some(5),
            source_delay_secs: Some(0),
        })
        .await
        .expect("empty selection report");

    assert_eq!(report.status, RecommendationReportStatus::PublishedEmpty);
    assert_eq!(report.summary_json.published_recommendation_count, 0);
    assert_eq!(
        report.status_reason.as_deref(),
        Some(EmptyReportReason::EmptySelection.as_str())
    );

    let recs = harness
        .recommendation_repo
        .find_by_report(&report.recommendation_report_id)
        .await
        .expect("load recommendations");
    assert!(recs.is_empty());
}

#[tokio::test]
#[ignore = "requires Docker"]
async fn account_unavailable_fails_without_report_row() {
    let (pool, _container) = setup_pg().await;
    let db = pool.connection().clone();
    let harness =
        ReportPipelineHarness::bootstrap(&db, HarnessOptions::unavailable_account()).await;

    let request_id = "account-unavailable";
    let error = harness
        .lifecycle
        .run_ad_hoc(AdHocReportRequest {
            request_id: request_id.to_owned(),
            trigger_time: Utc::now(),
            top_n: Some(5),
            source_delay_secs: Some(0),
        })
        .await
        .expect_err("account unavailable must fail closed");

    assert!(matches!(
        error,
        QuantError::Account(AccountError::CredentialsMissing)
    ));

    let trigger_key = format!("ad_hoc:{request_id}");
    let existing = harness
        .report_repo
        .find_by_trigger_key(&trigger_key)
        .await
        .expect("lookup trigger key");
    assert!(
        existing.is_none(),
        "failed build must not persist a report row"
    );
}

#[tokio::test]
#[ignore = "requires Docker"]
async fn revoke_after_publish() {
    let (pool, _container) = setup_pg().await;
    let db = pool.connection().clone();
    let harness = ReportPipelineHarness::bootstrap(&db, HarnessOptions::default()).await;

    let report = harness
        .lifecycle
        .run_ad_hoc(AdHocReportRequest {
            request_id: "revoke-me".to_owned(),
            trigger_time: Utc::now(),
            top_n: Some(5),
            source_delay_secs: Some(0),
        })
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

#[tokio::test]
#[ignore = "requires Docker"]
async fn evidence_refs_and_rank_scores_populated() {
    let (pool, _container) = setup_pg().await;
    let db = pool.connection().clone();
    let harness = ReportPipelineHarness::bootstrap(&db, HarnessOptions::default()).await;

    let report = harness
        .lifecycle
        .run_ad_hoc(AdHocReportRequest {
            request_id: "evidence-and-ranks".to_owned(),
            trigger_time: Utc::now(),
            top_n: Some(5),
            source_delay_secs: Some(0),
        })
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
        rec.liquidity_score.inner() > dec!(0),
        "liquidity_score should be derived from factor breakdown"
    );
    assert!(
        rec.data_quality_score.inner() > dec!(0),
        "data_quality_score should be derived from factor breakdown"
    );
    assert!(
        !rec.factor_breakdown.0.is_empty(),
        "factor breakdown evidence should be present"
    );
}

#[tokio::test]
#[ignore = "requires Docker"]
async fn report_persists_real_drawdown_from_equity_history() {
    let collateral = Usd::new(dec!(8000));
    let peak = Usd::new(dec!(10000));

    let neutral_kelly = {
        let (pool, _container) = setup_pg().await;
        let db = pool.connection().clone();
        let harness = ReportPipelineHarness::bootstrap(
            &db,
            HarnessOptions {
                collateral,
                ..HarnessOptions::default()
            },
        )
        .await;
        let report = harness
            .lifecycle
            .run_ad_hoc(AdHocReportRequest {
                request_id: "drawdown-neutral-baseline".to_owned(),
                trigger_time: Utc::now(),
                top_n: Some(5),
                source_delay_secs: Some(0),
            })
            .await
            .expect("neutral drawdown baseline report");
        let recs = harness
            .recommendation_repo
            .find_by_report(&report.recommendation_report_id)
            .await
            .expect("load neutral recommendations");
        assert!(
            !recs.is_empty(),
            "neutral baseline must publish at least one recommendation"
        );
        recs[0]
            .sizing_plan
            .kelly_fraction_applied
            .expect("neutral kelly fraction")
    };

    let (pool, _container) = setup_pg().await;
    let db = pool.connection().clone();

    PgEquitySnapshotRepository::new(db.clone())
        .create(NewEquitySnapshot {
            equity_snapshot_id: EquitySnapshotId::from_v7(),
            as_of: Utc::now() - chrono::Duration::hours(1),
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

    let harness = ReportPipelineHarness::bootstrap(
        &db,
        HarnessOptions {
            collateral,
            ..HarnessOptions::default()
        },
    )
    .await;

    let report = harness
        .lifecycle
        .run_ad_hoc(AdHocReportRequest {
            request_id: "drawdown-aware-sizing".to_owned(),
            trigger_time: Utc::now(),
            top_n: Some(5),
            source_delay_secs: Some(0),
        })
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

    let drawdown_recs = harness
        .recommendation_repo
        .find_by_report(&report.recommendation_report_id)
        .await
        .expect("load drawdown recommendations");
    assert!(
        !drawdown_recs.is_empty(),
        "drawdown report must publish at least one recommendation"
    );
    let drawdown_kelly = drawdown_recs[0]
        .sizing_plan
        .kelly_fraction_applied
        .expect("drawdown kelly fraction");
    assert_eq!(
        drawdown_kelly,
        neutral_kelly * dec!(0.8),
        "20% drawdown must shrink Kelly multiplier by (1 - drawdown)"
    );
}
