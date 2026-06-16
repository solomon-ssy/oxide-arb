//! Phase 7.3 opportunities read-route integration tests.
//!
//! Exercises the full projection contract: detections come back as
//! `OpportunityListView` (decimal strings + semantic enums, never the raw
//! `ClickHouse` scaled integers), the per-opportunity detail is an
//! `OpportunityAuditView` timeline, and `stats` returns the aggregated stage
//! funnel with a detection baseline.

use actix_web::http::StatusCode;
use chrono::Utc;
use oxide_arb_models::{
    clickhouse::{
        ChBps, ChDecimal64, ChFactor, ChPrice, ChProbability, ChSchemaVersion, ChShares, ChUsd,
        OpportunityAuditRow, OpportunityDetectionRow,
    },
    enums::clickhouse::{
        ChAuditOutcome, ChDurationBucket, ChMarketCategory, ChOpportunityAuditStage, ChPriceZone,
        ChRejectionStage, ChSide, ChStalenessLevel,
    },
    types::{EventId, ExecutionId, MarketId, OpportunityId, Price, Shares, TokenId, Usd},
};
use rust_decimal_macros::dec;
use uuid::Uuid;

use crate::{client, harness::TestEnv};

fn detection_row(opportunity_id: &OpportunityId, detected_at: i64) -> OpportunityDetectionRow {
    OpportunityDetectionRow {
        opportunity_id: opportunity_id.clone(),
        market_id: MarketId::new("0xopp-mkt"),
        event_id: EventId::new("evt-opp"),
        token_id: TokenId::new("tok-yes"),
        token_yes: Some(TokenId::new("tok-yes")),
        token_no: Some(TokenId::new("tok-no")),
        side: ChSide::Buy,
        entry_price: ChPrice::from(Price::new(dec!(0.94))),
        edge_bps: ChBps::from(dec!(250)),
        expected_net_profit_usd: ChUsd::from(Usd::new(dec!(3))),
        net_profit_if_correct_usd: ChUsd::from(Usd::new(dec!(6))),
        shares: ChShares::from(Shares::new(dec!(10))),
        total_cost_usd: ChUsd::from(Usd::new(dec!(9.4))),
        total_fees_usd: ChUsd::from(Usd::new(dec!(0.1))),
        resolution_prob: ChProbability::from(dec!(0.98)),
        confidence: ChProbability::from(dec!(0.9)),
        fill_probability: Some(ChProbability::from(dec!(0.8))),
        score: Some(1_500_000),
        urgency_factor: None,
        category_weight: None,
        staleness_discount: None,
        depth_used_pct: ChFactor::from(dec!(10)),
        convergence_secs: 120,
        category: ChMarketCategory::Politics,
        price_zone: ChPriceZone::Z97,
        duration_bucket: ChDurationBucket::Medium,
        calibration_sample_size: 10,
        calibration_fallback_tier: 1,
        calibration_alpha: ChDecimal64::from(dec!(2)),
        calibration_beta: ChDecimal64::from(dec!(1)),
        calibration_posterior_mean: ChProbability::from(dec!(0.8)),
        calibration_snapshot_hash: None,
        book_age_ms: None,
        yes_book_version: None,
        no_book_version: None,
        control_publication_id: None,
        score_components_json: "{}".to_owned(),
        calibration_snapshot_json: "{}".to_owned(),
        book_context_json: None,
        applied_factors_json: None,
        applied_factor_ids_json: None,
        latency_trace_json: None,
        missing_fields_json: None,
        detected_at,
        ingestion_time: detected_at,
        sequence: 1,
        schema_version: ChSchemaVersion(2),
    }
}

fn rejection_audit_row(opportunity_id: &OpportunityId, detected_at: i64) -> OpportunityAuditRow {
    OpportunityAuditRow {
        opportunity_id: opportunity_id.clone(),
        execution_id: ExecutionId::from_v7(),
        trade_id: None,
        market_id: MarketId::new("0xopp-mkt"),
        event_id: EventId::new("evt-opp"),
        token_id: TokenId::new("tok-yes"),
        side: ChSide::Buy,
        entry_price: Some(ChPrice::from(Price::new(dec!(0.94)))),
        fill_price: None,
        requested_shares: Some(ChShares::from(Shares::new(dec!(10)))),
        filled_shares: None,
        total_cost_usd: None,
        fees_usd: None,
        net_profit_usd: None,
        expected_profit_usd: Some(ChUsd::from(Usd::new(dec!(3)))),
        edge_bps: Some(ChBps::from(dec!(250))),
        resolution_prob: Some(ChProbability::from(dec!(0.98))),
        confidence: Some(ChProbability::from(dec!(0.9))),
        fill_probability: None,
        convergence_secs: Some(120),
        price_zone: Some(ChPriceZone::Z97),
        duration_bucket: Some(ChDurationBucket::Medium),
        depth_used_pct: Some(ChFactor::from(dec!(10))),
        staleness: Some(ChStalenessLevel::Fresh),
        category: Some(ChMarketCategory::Politics),
        stage: ChOpportunityAuditStage::RiskRejected,
        stage_order: 30,
        stage_at: detected_at + 5,
        payout_usd: None,
        realized_pnl_usd: None,
        settlement_status: None,
        settlement_trigger: None,
        winning_token_id: None,
        accounting_status: None,
        fee_source: None,
        redeem_route: None,
        redeem_resolution: None,
        outcome: Some(ChAuditOutcome::Rejected),
        rejection_stage: Some(ChRejectionStage::Risk),
        rejection_reason: Some("max exposure".to_owned()),
        scored_snapshot_json: Some(r#"{"schema_version":2}"#.to_owned()),
        book_context_json: None,
        applied_factor_ids_json: None,
        missing_fields_json: None,
        detected_at,
        ingestion_time: detected_at,
        sequence: 1,
        schema_version: ChSchemaVersion(2),
        updated_at: detected_at,
    }
}

#[actix_web::test]
#[ignore = "requires Docker"]
async fn opportunities_routes_project_views_and_funnel() {
    let env = TestEnv::start().await;
    let admin = client::login(&env, "admin", "admin").await;

    let opportunity_id = OpportunityId::from_v7();
    let now = Utc::now().timestamp_millis();
    env.evidence
        .set_detections(vec![detection_row(&opportunity_id, now)]);
    env.evidence
        .set_audits(vec![rejection_audit_row(&opportunity_id, now)]);

    // recent → Paginated<OpportunityListView>: decimal strings + semantic enums.
    let recent = client::get(&env, "/api/opportunities/recent", &admin).await;
    assert_eq!(recent.status, StatusCode::OK, "GET /opportunities/recent");
    let body = recent.json();
    let items = body["data"]["items"].as_array().expect("items array");
    assert_eq!(items.len(), 1);
    let item = &items[0];
    assert_eq!(item["opportunity_id"], opportunity_id.to_string());
    assert_eq!(item["side"], "BUY", "side is the semantic wire enum");
    assert_eq!(item["edge_bps"], "250", "bps is a decimal string");
    assert_eq!(
        item["expected_net_profit_usd"], "3",
        "usd is a decimal string"
    );
    assert_eq!(item["category"], "politics");
    assert_eq!(item["price_zone"], "z97");
    assert_eq!(item["duration_bucket"], "medium");
    assert_eq!(item["score"], "1.5", "micro score surfaces as decimal");
    assert!(
        item.get("calibration_snapshot_json").is_none(),
        "detection internals are stripped from the list view"
    );

    // history honours the window query and shares the projection.
    let history = client::get(
        &env,
        "/api/opportunities/history?market_id=0xopp-mkt",
        &admin,
    )
    .await;
    assert_eq!(history.status, StatusCode::OK, "GET /opportunities/history");
    assert_eq!(
        history.json()["data"]["items"]
            .as_array()
            .expect("items")
            .len(),
        1
    );

    // Inverted window is rejected at the boundary.
    assert_eq!(
        client::get(
            &env,
            "/api/opportunities/history?from=2026-06-02T00:00:00Z&to=2026-06-01T00:00:00Z",
            &admin
        )
        .await
        .status,
        StatusCode::BAD_REQUEST,
        "inverted window must be 400"
    );

    // stats → aggregated funnel with detection baseline + per-stage rates.
    let stats = client::get(&env, "/api/opportunities/stats", &admin).await;
    assert_eq!(stats.status, StatusCode::OK, "GET /opportunities/stats");
    let funnel = &stats.json()["data"];
    assert_eq!(funnel["total_detected"], 1);
    let stages = funnel["stages"].as_array().expect("stages array");
    assert_eq!(stages.len(), 1);
    assert_eq!(stages[0]["stage"], "risk_rejected");
    assert_eq!(stages[0]["count"], 1);
    assert_eq!(stages[0]["rate"], "1", "rate over the detected baseline");

    // detail → OpportunityAuditView timeline with parsed snapshot.
    let detail = client::get(
        &env,
        &format!("/api/opportunities/{opportunity_id}"),
        &admin,
    )
    .await;
    assert_eq!(detail.status, StatusCode::OK, "GET /opportunities/{{id}}");
    let rows = detail.json()["data"].as_array().cloned().expect("rows");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0]["stage"], "risk_rejected");
    assert_eq!(rows[0]["outcome"], "rejected");
    assert_eq!(rows[0]["rejection_stage"], "risk");
    assert_eq!(rows[0]["rejection_reason"], "max exposure");
    assert_eq!(
        rows[0]["scored_snapshot"]["schema_version"], 2,
        "snapshot JSON is parsed, not a string blob"
    );

    // Unknown opportunity → 404.
    assert_eq!(
        client::get(
            &env,
            &format!("/api/opportunities/{}", Uuid::now_v7()),
            &admin
        )
        .await
        .status,
        StatusCode::NOT_FOUND,
        "unknown opportunity must be 404"
    );
}
