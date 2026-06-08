//! `ClickHouse` timeseries repository integration tests (requires Docker).

#[path = "common/ch.rs"]
mod ch;

use std::time::Duration;

use ch::setup_timeseries_repo;
use chrono::Utc;
use oxide_arb_models::{
    clickhouse::{
        BookSnapshotRow, CalibrationSnapshotRow, ChBps, ChDecimal64, ChFactor, ChPrice,
        ChProbability, ChSchemaVersion, ChShares, ChUsd, OpportunityAuditRow,
        OpportunityDetectionRow, TickEventL2Row, TickEventRow,
    },
    enums::clickhouse::{
        ChAuditOutcome, ChBookEventType, ChDurationBucket, ChFactSource, ChMarketCategory,
        ChOpportunityAuditStage, ChPriceZone, ChSide, ChSnapshotReason, ChStalenessLevel,
    },
    types::{EventId, ExecutionId, MarketId, OpportunityId, Price, Shares, TokenId, TradeId, Usd},
};
use oxide_arb_repository::traits::{
    EvidenceTimeseriesRepository, MarketFilter, TimeWindow, TimeseriesFactWriter,
};
use rust_decimal_macros::dec;

fn sample_tick(token_id: &str, ts: i64) -> TickEventRow {
    TickEventRow {
        token_id: TokenId::new(token_id),
        market_id: None,
        event_type: ChBookEventType::Bbo,
        best_bid: Some(ChPrice::from(Price::new(dec!(0.94)))),
        best_ask: Some(ChPrice::from(Price::new(dec!(0.95)))),
        last_trade_price: None,
        bid_depth_usd: Some(ChUsd::from(Usd::new(dec!(500)))),
        ask_depth_usd: Some(ChUsd::from(Usd::new(dec!(400)))),
        spread_bps: Some(ChBps::from(dec!(10))),
        book_version: 1,
        raw_payload_json: Some("{}".into()),
        event_time: ts,
        ingestion_time: ts,
        sequence: 1,
        source: ChFactSource::WsBbo,
        schema_version: ChSchemaVersion(2),
    }
}

fn sample_l2(token_id: &str, ts: i64, sequence: u64) -> TickEventL2Row {
    TickEventL2Row {
        token_id: TokenId::new(token_id),
        market_id: Some(MarketId::new("0xch-market")),
        event_type: ChBookEventType::Snapshot,
        bid_prices: vec![ChPrice::from(Price::new(dec!(0.94)))],
        bid_sizes: vec![ChShares::from(Shares::new(dec!(10)))],
        ask_prices: vec![ChPrice::from(Price::new(dec!(0.95)))],
        ask_sizes: vec![ChShares::from(Shares::new(dec!(11)))],
        changed_levels_json: None,
        book_version: sequence,
        levels_count: 2,
        is_full_snapshot: true,
        event_time: ts,
        ingestion_time: ts,
        sequence,
        source: ChFactSource::WsSnapshot,
        schema_version: ChSchemaVersion(2),
    }
}

fn sample_book(token_id: &str, ts: i64, sequence: u64) -> BookSnapshotRow {
    BookSnapshotRow {
        token_id: TokenId::new(token_id),
        market_id: Some(MarketId::new("0xch-market")),
        snapshot_reason: ChSnapshotReason::Periodic,
        top_n: 2,
        bids_json: r#"[["0.94","10"]]"#.into(),
        asks_json: r#"[["0.95","11"]]"#.into(),
        bid_depth_usd: Some(ChUsd::from(Usd::new(dec!(9.4)))),
        ask_depth_usd: Some(ChUsd::from(Usd::new(dec!(10.45)))),
        mid_price: Some(ChPrice::from(Price::new(dec!(0.945)))),
        spread_bps: Some(ChBps::from(dec!(105.82))),
        book_version: sequence,
        levels_count: 2,
        event_time: ts,
        ingestion_time: ts,
        sequence,
        source: ChFactSource::Scanner,
        schema_version: ChSchemaVersion(2),
    }
}

fn sample_calibration(ts: i64, sequence: u64) -> CalibrationSnapshotRow {
    CalibrationSnapshotRow {
        category: ChMarketCategory::Politics,
        price_zone: ChPriceZone::Z97,
        duration_bucket: ChDurationBucket::Medium,
        total_count: 10,
        correct_count: 8,
        alpha_prior: ChDecimal64::from(dec!(2)),
        beta_prior: ChDecimal64::from(dec!(1)),
        posterior_mean: Some(ChProbability::from(dec!(0.8))),
        fallback_tier: 1,
        config_hash: "hash".into(),
        snapshot_hash: "snapshot".into(),
        event_time: ts,
        ingestion_time: ts,
        sequence,
        source: ChFactSource::CalibrationUpdater,
        schema_version: ChSchemaVersion(2),
    }
}

fn sample_detection(
    opportunity_id: &OpportunityId,
    market_id: &MarketId,
    event_id: &EventId,
    token_id: &TokenId,
    ts: i64,
) -> OpportunityDetectionRow {
    OpportunityDetectionRow {
        opportunity_id: opportunity_id.clone(),
        market_id: market_id.clone(),
        event_id: event_id.clone(),
        token_id: token_id.clone(),
        token_yes: Some(token_id.clone()),
        token_no: Some(TokenId::new("tok-no-core-facts")),
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
        score: Some(123),
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
        calibration_snapshot_hash: Some("calibration-hash".to_owned()),
        book_age_ms: Some(10),
        yes_book_version: Some(1),
        no_book_version: Some(1),
        control_publication_id: None,
        score_components_json: "{}".to_owned(),
        calibration_snapshot_json: "{}".to_owned(),
        book_context_json: None,
        applied_factors_json: None,
        applied_factor_ids_json: None,
        latency_trace_json: None,
        missing_fields_json: None,
        detected_at: ts,
        ingestion_time: ts,
        sequence: 1,
        schema_version: ChSchemaVersion(2),
    }
}

fn sample_audit(
    opportunity_id: &OpportunityId,
    market_id: &MarketId,
    event_id: &EventId,
    token_id: &TokenId,
    stage: ChOpportunityAuditStage,
    ts: i64,
    sequence: u64,
) -> OpportunityAuditRow {
    OpportunityAuditRow {
        opportunity_id: opportunity_id.clone(),
        execution_id: ExecutionId::from_v7(),
        trade_id: Some(TradeId::from_v7()),
        market_id: market_id.clone(),
        event_id: event_id.clone(),
        token_id: token_id.clone(),
        side: ChSide::Buy,
        entry_price: Some(ChPrice::from(Price::new(dec!(0.94)))),
        fill_price: Some(ChPrice::from(Price::new(dec!(0.94)))),
        requested_shares: Some(ChShares::from(Shares::new(dec!(10)))),
        filled_shares: Some(ChShares::from(Shares::new(dec!(10)))),
        total_cost_usd: Some(ChUsd::from(Usd::new(dec!(9.4)))),
        fees_usd: Some(ChUsd::from(Usd::new(dec!(0.1)))),
        net_profit_usd: None,
        expected_profit_usd: Some(ChUsd::from(Usd::new(dec!(3)))),
        edge_bps: Some(ChBps::from(dec!(250))),
        resolution_prob: Some(ChProbability::from(dec!(0.98))),
        confidence: Some(ChProbability::from(dec!(0.9))),
        fill_probability: Some(ChProbability::from(dec!(0.8))),
        convergence_secs: Some(120),
        price_zone: Some(ChPriceZone::Z97),
        duration_bucket: Some(ChDurationBucket::Medium),
        depth_used_pct: Some(ChFactor::from(dec!(10))),
        staleness: Some(ChStalenessLevel::Fresh),
        category: Some(ChMarketCategory::Politics),
        stage,
        stage_order: match stage {
            ChOpportunityAuditStage::Detected => 10,
            ChOpportunityAuditStage::Filled => 70,
            _ => 90,
        },
        stage_at: ts,
        payout_usd: None,
        realized_pnl_usd: None,
        settlement_status: None,
        settlement_trigger: None,
        winning_token_id: None,
        accounting_status: None,
        fee_source: None,
        outcome: if stage == ChOpportunityAuditStage::Filled {
            Some(ChAuditOutcome::Success)
        } else {
            None
        },
        rejection_stage: None,
        rejection_reason: None,
        scored_snapshot_json: Some("{}".to_owned()),
        book_context_json: None,
        applied_factor_ids_json: None,
        missing_fields_json: None,
        detected_at: ts,
        ingestion_time: ts,
        sequence,
        schema_version: ChSchemaVersion(2),
        updated_at: ts,
    }
}

#[tokio::test]
#[ignore = "requires Docker"]
async fn timeseries_insert_and_query_roundtrip() {
    let (repo, shutdown, _container) = setup_timeseries_repo().await;
    let now = Utc::now().timestamp_millis();
    let token = "tok-roundtrip";

    repo.insert_tick_events(vec![
        sample_tick(token, now - 2_000),
        sample_tick(token, now - 1_000),
        sample_tick(token, now),
    ])
    .await
    .expect("insert");

    shutdown.cancel();
    tokio::time::sleep(Duration::from_millis(500)).await;

    let rows = repo
        .tick_events(
            &[TokenId::new(token)],
            TimeWindow::new(
                Utc::now() - chrono::Duration::minutes(5),
                Utc::now() + chrono::Duration::minutes(1),
            ),
            10,
        )
        .await
        .expect("query");

    assert_eq!(rows.len(), 3, "expected all inserted tick events");
    assert!(
        rows.fingerprint
            .0
            .starts_with("ChTimeseriesRepository.tick_events:v1:blake3:"),
        "expected canonical tick_events fingerprint"
    );
    assert_eq!(rows.rows[0].token_id, TokenId::new(token));
}

#[tokio::test]
#[ignore = "requires Docker"]
async fn evidence_timeseries_queries_roundtrip_core_fact_tables() {
    let (repo, shutdown, _container) = setup_timeseries_repo().await;
    let now = Utc::now().timestamp_millis();
    let token = TokenId::new("tok-core-facts");
    let market_id = MarketId::new("0xch-market");
    let event_id = EventId::new("evt-core-facts");
    let opportunity_id = OpportunityId::from_v7();

    repo.insert_l2_events(vec![
        sample_l2(token.as_str(), now, 2),
        sample_l2(token.as_str(), now, 1),
    ])
    .await
    .expect("insert l2");
    repo.insert_book_snapshots(vec![sample_book(token.as_str(), now, 1)])
        .await
        .expect("insert book");
    repo.insert_calibration_snapshots(vec![sample_calibration(now, 1)])
        .await
        .expect("insert calibration");
    repo.insert_detections(vec![sample_detection(
        &opportunity_id,
        &market_id,
        &event_id,
        &token,
        now,
    )])
    .await
    .expect("insert detection");
    repo.insert_audits(vec![
        sample_audit(
            &opportunity_id,
            &market_id,
            &event_id,
            &token,
            ChOpportunityAuditStage::Detected,
            now,
            1,
        ),
        sample_audit(
            &opportunity_id,
            &market_id,
            &event_id,
            &token,
            ChOpportunityAuditStage::Filled,
            now + 1,
            2,
        ),
    ])
    .await
    .expect("insert audits");

    shutdown.cancel();
    tokio::time::sleep(Duration::from_millis(500)).await;
    let window = TimeWindow::new(
        Utc::now() - chrono::Duration::minutes(5),
        Utc::now() + chrono::Duration::minutes(1),
    );

    let l2 = repo
        .l2_events(std::slice::from_ref(&token), window)
        .await
        .unwrap();
    assert_eq!(l2.len(), 2);
    assert_eq!(
        l2.rows[0].sequence, 1,
        "same timestamp rows must be sequence ordered"
    );

    let books = repo
        .book_snapshots_before(std::slice::from_ref(&token), Utc::now(), 1)
        .await
        .unwrap();
    assert_eq!(books.len(), 1);
    assert_eq!(books.rows[0].snapshot_reason, ChSnapshotReason::Periodic);

    let calibration = repo.calibration_snapshots(window).await.unwrap();
    assert_eq!(calibration.len(), 1);
    assert!(calibration.rows[0].posterior_mean.is_some());

    let filter = MarketFilter {
        market_ids: vec![market_id.clone()],
        event_ids: vec![event_id],
        token_ids: vec![token],
        categories: vec![ChMarketCategory::Politics],
    };
    let detections = repo.detections(filter.clone(), window).await.unwrap();
    assert_eq!(detections.len(), 1);
    assert_eq!(detections.rows[0].opportunity_id, opportunity_id);

    let funnel = repo.audit_funnel(filter, window).await.unwrap();
    assert_eq!(funnel.len(), 2);
    assert_eq!(funnel.rows[0].stage, ChOpportunityAuditStage::Detected);
    assert_eq!(funnel.rows[1].stage, ChOpportunityAuditStage::Filled);

    let terminal = repo
        .terminal_audits(std::slice::from_ref(&opportunity_id))
        .await
        .unwrap();
    assert_eq!(terminal.len(), 1);
    assert_eq!(terminal.rows[0].stage, ChOpportunityAuditStage::Filled);
}
