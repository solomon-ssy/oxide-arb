//! Phase 04 report pipeline end-to-end tests (Postgres + testcontainers).

use chrono::Utc;
use quant_pivot_core::report::AdHocReportRequest;
use quant_pivot_error::{QuantError, account::AccountError};
use quant_pivot_models::enums::quant::{EmptyReason, RecommendationReportStatus};
use quant_pivot_repository::traits::{RecommendationReportRepository, RecommendationRepository};
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
        Some(EmptyReason::EmptySelection.as_str())
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
        recs.iter().all(
            |rec| rec.status == quant_pivot_models::enums::quant::RecommendationStatus::Revoked
        )
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
