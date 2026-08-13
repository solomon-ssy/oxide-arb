//! `ClickHouse` point-in-time repository read system contracts.

use std::{slice, sync::Arc, time::Duration};

use chrono::Utc;
use clickhouse::Client;
use quant_pivot_error::storage::StorageError;
use quant_pivot_models::{
    clickhouse::{
        BookL2LedgerRow, BookMicrostructureRow, ChDecimal64, ChDigest, ChPrice, ChSchemaVersion,
        ChShares, ChUsd, EntryConditionEvaluationEventRow, MarketResolutionFactInput,
        MarketResolutionRow, QuantFeatureParityEventRow, QuantReportRecommendationFactRow,
        ReportMarketFunnelRow, TradeTapeRow, WeatherForecastFactRow, WeatherObservationFactRow,
    },
    domain::api::FeatureParityEventListQuery,
    enums::clickhouse::{
        ChCanonicalBookEventType, ChOutcomeSide, ChTradeParticipantRole,
        ChTradeReconciliationStatus, ChTradeSide, ChTradeTapeSource,
    },
    types::{
        ContentHash, DecisionPolicySnapshotId, DomainInstrumentKey, DomainSourceId, EconomicTierId,
        EntryConditionInstanceId, EventId, EvmBlockHash, EvmTransactionHash, FeatureParityEventId,
        FeatureParityRunId, MarketId, MarketSelectionId, ModelRunId, ModelVersionId, PayoutRatio,
        Price, RecommendationId, RecommendationReportId, ReportRouteRunId, Shares, TokenId, Usd,
    },
};
use quant_pivot_repository::{
    clickhouse::{
        ChFeatureParityEventRepository, ChNativeReadRepository, ChQuantFactReadRepository,
    },
    traits::{FeatureParityEventRepository, QuantFactReadRepository},
};
use quant_pivot_storage::clickhouse::{ClickHousePool, apply_offline_schema_migrations};
use quant_pivot_system_tests::resources::fresh_clickhouse_config;
use rust_decimal::Decimal;
use uuid::Uuid;

async fn setup_clickhouse() -> (Arc<ClickHousePool>, Client, ()) {
    let config = fresh_clickhouse_config("repository_reads");
    apply_offline_schema_migrations(&config)
        .await
        .expect("schema deploy");
    let pool = Arc::new(ClickHousePool::connect(&config).await.expect("connect"));
    pool.verify_schema().await.expect("schema verify");
    let client = pool.client().clone();
    (pool, client, ())
}

async fn insert_book_rows(client: &Client, rows: &[BookL2LedgerRow]) {
    let mut insert = client
        .insert::<BookL2LedgerRow>("quant_book_l2_ledger")
        .await
        .expect("insert");
    for row in rows {
        insert.write(row).await.expect("write row");
    }
    insert.end().await.expect("end insert");
}

fn resolution_row(
    market_id: MarketId,
    token_ids: [TokenId; 2],
    payout_ratios: [PayoutRatio; 2],
    resolved_at: i64,
    observed_at: i64,
    source_byte: u8,
) -> MarketResolutionRow {
    MarketResolutionRow::seal(MarketResolutionFactInput {
        market_id,
        token_ids,
        payout_ratios,
        resolved_at,
        observed_at,
        source_block_number: u64::from(source_byte),
        source_block_hash: EvmBlockHash::parse(format!(
            "0x{}",
            format!("{source_byte:02x}").repeat(32)
        ))
        .expect("resolution block hash"),
        source_transaction_hash: EvmTransactionHash::parse(format!(
            "0x{}",
            format!("{:02x}", source_byte.saturating_add(64)).repeat(32)
        ))
        .expect("resolution transaction hash"),
        source_log_index: u64::from(source_byte),
        source_checkpoint_hash: ContentHash::from_bytes([source_byte; 32]),
    })
    .expect("sealed resolution row")
}

async fn insert_microstructure_rows(client: &Client, rows: &[BookMicrostructureRow]) {
    let mut insert = client
        .insert::<BookMicrostructureRow>("book_microstructure_1s")
        .await
        .expect("insert microstructure");
    for row in rows {
        insert.write(row).await.expect("write microstructure row");
    }
    insert.end().await.expect("end microstructure insert");
}

/// Wait until inserted snapshot rows are query-visible.
///
/// Fresh `MergeTree` parts can lag briefly behind HTTP insert ack on a cold
/// testcontainer; PIT reads that race this window return `None`.
async fn wait_book_snapshot_rows(client: &Client, token: &TokenId, expected: u64) {
    const ATTEMPTS: usize = 40;
    const PAUSE: Duration = Duration::from_millis(50);
    for attempt in 1..=ATTEMPTS {
        let count: u64 = client
            .query("SELECT count() FROM quant_book_l2_ledger WHERE token_id = ?")
            .bind(token.clone())
            .fetch_one()
            .await
            .expect("count quant_book_l2_ledger");
        if count >= expected {
            return;
        }
        assert!(
            attempt < ATTEMPTS,
            "snapshot rows for {} not visible after insert \
             (count={count}, expected>={expected}, attempts={ATTEMPTS})",
            token.as_str()
        );
        tokio::time::sleep(PAUSE).await;
    }
}

/// Current epoch millis for deterministic point-in-time visibility checks.
fn fresh_event_time_ms() -> i64 {
    Utc::now().timestamp_millis()
}

fn content_hash(digit: char) -> ContentHash {
    ContentHash::parse(&format!("blake3:{}", digit.to_string().repeat(64)))
        .expect("valid content hash")
}

const REPLACING_READER_TABLES: [&str; 4] = [
    "quant_report_recommendation_fact",
    "quant_report_market_funnel",
    "quant_entry_condition_evaluation_event",
    "quant_feature_parity_event",
];

async fn set_background_merges(client: &Client, action: &str) {
    for table in REPLACING_READER_TABLES {
        client
            .query(&format!("SYSTEM {action} MERGES {table}"))
            .execute()
            .await
            .unwrap_or_else(|error| panic!("{action} merges for {table}: {error}"));
    }
}

async fn insert_recommendation_revisions(
    client: &Client,
    now: i64,
    report_id: &RecommendationReportId,
    recommendation_id: &RecommendationId,
    market_id: &MarketId,
    token_id: &TokenId,
) {
    let recommendation = QuantReportRecommendationFactRow {
        event_time: now,
        recommendation_report_id: report_id.to_owned(),
        recommendation_id: recommendation_id.to_owned(),
        report_route_run_id: ReportRouteRunId::from_v7(),
        economic_tier_id: EconomicTierId::from_v7(),
        route: "pooled".to_owned(),
        rank: 2,
        market_id: market_id.to_owned(),
        token_id: token_id.to_owned(),
        side: ChOutcomeSide::Yes,
        profit_probability_bps: 6_000,
        nominal_expected_net_usd: ChUsd::from(Usd::new(Decimal::new(20, 0))),
        robust_expected_net_usd: ChUsd::from(Usd::new(Decimal::new(15, 0))),
        max_loss_usd: ChUsd::from(Usd::new(Decimal::new(25, 0))),
        cvar_contribution_usd: ChUsd::from(Usd::new(Decimal::new(10, 0))),
        capital_occupancy_usd_hours: ChUsd::from(Usd::new(Decimal::new(100, 0))),
        marginal_portfolio_value_usd: ChUsd::from(Usd::new(Decimal::new(12, 0))),
        suggested_usd: ChUsd::from(Usd::new(Decimal::new(25, 0))),
        valid_until: now + 60_000,
    };
    let revised_recommendation = QuantReportRecommendationFactRow {
        event_time: now + 1,
        rank: 1,
        profit_probability_bps: 7_000,
        ..recommendation.clone()
    };
    let mut original_insert = client
        .insert::<QuantReportRecommendationFactRow>("quant_report_recommendation_fact")
        .await
        .expect("start original recommendation insert");
    original_insert
        .write(&recommendation)
        .await
        .expect("write original recommendation");
    original_insert
        .end()
        .await
        .expect("finish original recommendation insert");
    let mut revised_insert = client
        .insert::<QuantReportRecommendationFactRow>("quant_report_recommendation_fact")
        .await
        .expect("start revised recommendation insert");
    revised_insert
        .write(&revised_recommendation)
        .await
        .expect("write revised recommendation");
    revised_insert
        .end()
        .await
        .expect("finish revised recommendation insert");
}

async fn insert_funnel_revisions(
    client: &Client,
    now: i64,
    report_id: &RecommendationReportId,
    recommendation_id: &RecommendationId,
    market_id: &MarketId,
    token_id: &TokenId,
) {
    let funnel = ReportMarketFunnelRow {
        event_time: now,
        recommendation_report_id: report_id.to_owned(),
        market_selection_id: MarketSelectionId::from_v7(),
        decision_policy_snapshot_id: DecisionPolicySnapshotId::from_v7(),
        report_route_run_id: Some(ReportRouteRunId::from_v7()),
        route: Some("pooled".to_owned()),
        model_version_id: Some(ModelVersionId::from_v7()),
        model_run_id: Some(ModelRunId::from_v7()),
        market_id: market_id.to_owned(),
        event_id: EventId::new("replacing-contract"),
        primary_token_id: token_id.to_owned(),
        terminal_stage: "eligible".to_owned(),
        primary_reason: "initial".to_owned(),
        secondary_diagnostics_json: "[]".to_owned(),
        feature_vector_id: None,
        signal_candidate_id: None,
        recommendation_id: Some(recommendation_id.to_owned()),
        row_hash: content_hash('2').to_string(),
        ingestion_time: now,
    };
    let revised_funnel = ReportMarketFunnelRow {
        terminal_stage: "recommended".to_owned(),
        primary_reason: "latest".to_owned(),
        row_hash: content_hash('3').to_string(),
        ingestion_time: now + 1,
        ..funnel.clone()
    };
    let mut original_insert = client
        .insert::<ReportMarketFunnelRow>("quant_report_market_funnel")
        .await
        .expect("start original funnel insert");
    original_insert
        .write(&funnel)
        .await
        .expect("write original funnel row");
    original_insert
        .end()
        .await
        .expect("finish original funnel insert");
    let mut revised_insert = client
        .insert::<ReportMarketFunnelRow>("quant_report_market_funnel")
        .await
        .expect("start revised funnel insert");
    revised_insert
        .write(&revised_funnel)
        .await
        .expect("write revised funnel row");
    revised_insert
        .end()
        .await
        .expect("finish revised funnel insert");
}

async fn insert_evaluation_revisions(client: &Client, now: i64) -> EntryConditionInstanceId {
    let condition_instance_id = EntryConditionInstanceId::from_v7();
    let evaluation_id = content_hash('4');
    let evaluation = EntryConditionEvaluationEventRow {
        evaluation_id,
        condition_instance_id,
        base_revision: 1,
        applied_revision: Some(1),
        trace_kind: "applied".to_owned(),
        evaluator_version: 1,
        evaluated_at: now,
        state: "waiting".to_owned(),
        truth: "false".to_owned(),
        evaluation_hash: content_hash('5'),
        input_fingerprint: content_hash('6'),
        continuity_hash: content_hash('7'),
        tree_json: "{}".to_owned(),
        schema_version: ChSchemaVersion::FIRST,
    };
    let revised_evaluation = EntryConditionEvaluationEventRow {
        applied_revision: Some(2),
        evaluated_at: now + 1,
        state: "qualified".to_owned(),
        truth: "true".to_owned(),
        evaluation_hash: content_hash('8'),
        ..evaluation.clone()
    };
    let mut original_insert = client
        .insert::<EntryConditionEvaluationEventRow>("quant_entry_condition_evaluation_event")
        .await
        .expect("start original entry evaluation insert");
    original_insert
        .write(&evaluation)
        .await
        .expect("write original entry evaluation");
    original_insert
        .end()
        .await
        .expect("finish original entry evaluation insert");
    let mut revised_insert = client
        .insert::<EntryConditionEvaluationEventRow>("quant_entry_condition_evaluation_event")
        .await
        .expect("start revised entry evaluation insert");
    revised_insert
        .write(&revised_evaluation)
        .await
        .expect("write revised entry evaluation");
    revised_insert
        .end()
        .await
        .expect("finish revised entry evaluation insert");
    condition_instance_id
}

async fn insert_parity_revisions(
    client: &Client,
    now: i64,
    report_id: &RecommendationReportId,
    market_id: &MarketId,
) -> FeatureParityRunId {
    let parity_run_id = FeatureParityRunId::from_v7();
    let parity_event_id = FeatureParityEventId::from_v7();
    let parity = QuantFeatureParityEventRow {
        event_time: now,
        parity_event_id,
        parity_run_id,
        decision_at: now,
        stage: "model_input".to_owned(),
        status: "matched".to_owned(),
        report_id: Some(report_id.to_owned()),
        model_run_id: None,
        model_version_id: None,
        training_dataset_id: None,
        market_id: Some(market_id.to_owned()),
        feature_name: Some("stale_feature".to_owned()),
        reason: None,
        online_state: Some("observed".to_owned()),
        replay_state: Some("observed".to_owned()),
        online_value: Some("1".to_owned()),
        replay_value: Some("1".to_owned()),
        online_effective_at: Some(now),
        online_available_at: Some(now),
        online_cutoff: Some(now),
        replay_effective_at: Some(now),
        replay_available_at: Some(now),
        replay_cutoff: Some(now),
        feature_contract_hash: content_hash('9').to_string(),
        transform_hash: content_hash('a').to_string(),
        online_fingerprint: content_hash('b').to_string(),
        replay_fingerprint: content_hash('c').to_string(),
        detail_json: format!(
            r#"{{"kind":"compared","sampling_key":"report:market","source":{{"kind":"model_input","raw_input_name":"spread_bps","feature_vector_id":"{}"}}}}"#,
            uuid::Uuid::now_v7()
        ),
        ingestion_time: now,
    };
    let revised_parity = QuantFeatureParityEventRow {
        feature_name: Some("latest_feature".to_owned()),
        ingestion_time: now + 1,
        ..parity.clone()
    };
    let mut original_insert = client
        .insert::<QuantFeatureParityEventRow>("quant_feature_parity_event")
        .await
        .expect("start original parity insert");
    original_insert
        .write(&parity)
        .await
        .expect("write original parity row");
    original_insert
        .end()
        .await
        .expect("finish original parity insert");
    let mut revised_insert = client
        .insert::<QuantFeatureParityEventRow>("quant_feature_parity_event")
        .await
        .expect("start revised parity insert");
    revised_insert
        .write(&revised_parity)
        .await
        .expect("write revised parity row");
    revised_insert
        .end()
        .await
        .expect("finish revised parity insert");
    parity_run_id
}

async fn assert_physical_revision_counts(
    client: &Client,
    report_id: &RecommendationReportId,
    condition_instance_id: &EntryConditionInstanceId,
    parity_run_id: &FeatureParityRunId,
) {
    for (table, predicate) in [
        (
            "quant_report_recommendation_fact",
            format!("recommendation_report_id = '{report_id}'"),
        ),
        (
            "quant_report_market_funnel",
            format!("recommendation_report_id = '{report_id}'"),
        ),
        (
            "quant_entry_condition_evaluation_event",
            format!("condition_instance_id = '{condition_instance_id}'"),
        ),
        (
            "quant_feature_parity_event",
            format!("parity_run_id = '{parity_run_id}'"),
        ),
    ] {
        let raw_count: u64 = client
            .query(&format!("SELECT count() FROM {table} WHERE {predicate}"))
            .fetch_one()
            .await
            .unwrap_or_else(|error| panic!("count raw rows in {table}: {error}"));
        assert_eq!(raw_count, 2, "{table} must contain two physical versions");
    }
}

async fn assert_latest_logical_revisions(
    pool: Arc<ClickHousePool>,
    report_id: &RecommendationReportId,
    condition_instance_id: &EntryConditionInstanceId,
    parity_run_id: &FeatureParityRunId,
) {
    let native_read = ChNativeReadRepository::new(Arc::clone(&pool));
    let recommendations = native_read
        .report_recommendation_rows(report_id)
        .await
        .expect("read recommendation logical row");
    assert_eq!(recommendations.len(), 1);
    assert_eq!(recommendations[0].rank, 1);

    let fact_read = ChQuantFactReadRepository::new(Arc::clone(&pool));
    let funnel_rows = fact_read
        .report_market_funnel_page(report_id, None, None, 0, 10)
        .await
        .expect("read funnel logical row");
    assert_eq!(funnel_rows.len(), 1);
    assert_eq!(funnel_rows[0].terminal_stage, "recommended");
    assert_eq!(
        fact_read
            .report_market_funnel_count(report_id, None, None)
            .await
            .expect("count funnel logical rows"),
        1
    );
    let evaluation = fact_read
        .latest_entry_evaluation(condition_instance_id)
        .await
        .expect("read entry evaluation logical row")
        .expect("entry evaluation exists");
    assert_eq!(evaluation.applied_revision, Some(2));
    assert_eq!(evaluation.state, "qualified");

    let parity_read = ChFeatureParityEventRepository::new(pool);
    let parity_page = parity_read
        .page_events(FeatureParityEventListQuery {
            parity_run_id: Some(parity_run_id.to_owned()),
            ..FeatureParityEventListQuery::default()
        })
        .await
        .expect("read parity logical row");
    assert_eq!(parity_page.total, 1);
    assert_eq!(parity_page.items.len(), 1);
    assert_eq!(
        parity_page.items[0].feature_name.as_deref(),
        Some("latest_feature")
    );
}

pub async fn replacing_merge_tree_row() {
    let (pool, client, _container) = setup_clickhouse().await;
    let now = fresh_event_time_ms();
    let report_id = RecommendationReportId::from_v7();
    let recommendation_id = RecommendationId::from_v7();
    let market_id = MarketId::new(format!("0xreplacing-{report_id}"));
    let token_id = TokenId::new(format!("token-{report_id}"));

    set_background_merges(&client, "STOP").await;
    insert_recommendation_revisions(
        &client,
        now,
        &report_id,
        &recommendation_id,
        &market_id,
        &token_id,
    )
    .await;
    insert_funnel_revisions(
        &client,
        now,
        &report_id,
        &recommendation_id,
        &market_id,
        &token_id,
    )
    .await;
    let condition_instance_id = insert_evaluation_revisions(&client, now).await;
    let parity_run_id = insert_parity_revisions(&client, now, &report_id, &market_id).await;
    assert_physical_revision_counts(&client, &report_id, &condition_instance_id, &parity_run_id)
        .await;
    assert_latest_logical_revisions(pool, &report_id, &condition_instance_id, &parity_run_id).await;
    set_background_merges(&client, "START").await;
}

fn book_row(
    token: &str,
    event_time_ms: i64,
    ingestion_time_ms: i64,
    sequence: u64,
    mid: Decimal,
) -> BookL2LedgerRow {
    BookL2LedgerRow {
        stream_session_id: Uuid::nil(),
        shard_id: 0,
        token_id: TokenId::new(token),
        market_id: Some(MarketId::new("0xchpit")),
        token_sequence: sequence,
        event_type: ChCanonicalBookEventType::Snapshot,
        bid_prices: vec![ChPrice::from(Price::new(mid))],
        bid_sizes: vec![ChShares::from(Shares::new(Decimal::from(100)))],
        ask_prices: vec![ChPrice::from(Price::new(Decimal::new(52, 2)))],
        ask_sizes: vec![ChShares::from(Shares::new(Decimal::from(100)))],
        old_tick_size: None,
        new_tick_size: None,
        trade_price: None,
        trade_side: None,
        trade_size: None,
        fee_rate_bps: None,
        venue_event_time: event_time_ms,
        ingress_time: ingestion_time_ms,
        persisted_time: ingestion_time_ms,
        event_hash: ChDigest::new([0; 32]),
        schema_version: BookL2LedgerRow::SCHEMA_VERSION,
    }
    .seal()
    .expect("seal ledger fixture")
}

fn microstructure_row(
    token_id: &TokenId,
    market_id: &MarketId,
    bucket_time: i64,
    available_at: i64,
    mid: Decimal,
) -> BookMicrostructureRow {
    let price = ChPrice::from(Price::new(mid));
    BookMicrostructureRow {
        token_id: token_id.clone(),
        market_id: Some(market_id.clone()),
        bucket_time,
        best_bid_open: None,
        best_bid_high: None,
        best_bid_low: None,
        best_bid_close: None,
        best_ask_open: None,
        best_ask_high: None,
        best_ask_low: None,
        best_ask_close: None,
        spread_bps_min: None,
        spread_bps_avg: None,
        spread_bps_max: None,
        mid_price_open: Some(price),
        mid_price_close: Some(price),
        top1_depth_usd_avg: None,
        top5_depth_usd_avg: None,
        top20_depth_usd_avg: None,
        imbalance_avg: None,
        update_count: 1,
        snapshot_count: 1,
        delta_count: 0,
        delete_count: 0,
        crossed_count: 0,
        invalid_level_count: 0,
        gap_count: 0,
        last_trade_count: 0,
        max_book_age_ms: 0,
        schema_version: ChSchemaVersion::FIRST,
        available_at,
    }
}

pub async fn ch_read_orders_tiebreaker() {
    let (pool, client, _container) = setup_clickhouse().await;
    let read = ChQuantFactReadRepository::new(pool);
    let token = TokenId::new("ch-pit-yes");
    let event_time = fresh_event_time_ms();

    insert_book_rows(
        &client,
        &[
            book_row(
                token.as_str(),
                event_time,
                event_time + 1,
                1,
                Decimal::new(49, 2),
            ),
            book_row(
                token.as_str(),
                event_time,
                event_time + 2,
                1,
                Decimal::new(50, 2),
            ),
        ],
    )
    .await;
    wait_book_snapshot_rows(&client, &token, 2).await;

    let before_late_arrival = read
        .book_ledger_snapshot_at(&token, event_time + 5, event_time + 1)
        .await
        .expect("read before late arrival")
        .expect("earlier visible revision");
    assert_eq!(
        Price::from(before_late_arrival.bid_prices[0]).inner(),
        Decimal::new(49, 2),
        "a backdated revision must not be visible before its ingestion time"
    );

    let row = read
        .book_ledger_snapshot_at(&token, event_time + 5, event_time + 5)
        .await
        .expect("read")
        .unwrap_or_else(|| {
            panic!(
                "PIT book_ledger_snapshot_at returned None for token={} as_of={}",
                token.as_str(),
                event_time + 5
            )
        });
    assert_eq!(
        Price::from(row.bid_prices[0]).inner(),
        Decimal::new(50, 2),
        "tie-breaker must prefer later ingestion_time at same event_time"
    );
}

pub async fn scans_reject_unavailable_rows() {
    let (pool, client, _container) = setup_clickhouse().await;
    let read = ChQuantFactReadRepository::new(pool);
    let event_time = fresh_event_time_ms();

    let market_id = MarketId::new("0xavailability-axis");
    let token_id = TokenId::new("availability-axis-yes");
    let late_ingestion = event_time + 10_000;
    let mut late_book = book_row(
        token_id.as_str(),
        event_time,
        late_ingestion,
        1,
        Decimal::new(50, 2),
    );
    late_book.market_id = Some(market_id.clone());
    insert_book_rows(&client, slice::from_ref(&late_book)).await;
    wait_book_snapshot_rows(&client, &token_id, 1).await;

    assert!(
        read.observed_markets_between(event_time - 1, event_time + 1, event_time)
            .await
            .expect("markets before ingestion")
            .is_empty(),
        "a historical candidate must not exist before its book ingestion time"
    );
    assert_eq!(
        read.observed_markets_between(event_time - 1, event_time + 1, late_ingestion)
            .await
            .expect("markets after ingestion"),
        vec![market_id.clone()]
    );

    let visible_at = event_time + 1_000;
    let corrected_at = event_time + 2_000;
    let completed_bucket_end = event_time + 1_000;
    insert_microstructure_rows(
        &client,
        &[
            microstructure_row(
                &token_id,
                &market_id,
                event_time,
                visible_at,
                Decimal::new(40, 2),
            ),
            microstructure_row(
                &token_id,
                &market_id,
                event_time,
                corrected_at,
                Decimal::new(60, 2),
            ),
        ],
    )
    .await;

    let before_correction = read
        .mid_price_series(
            vec![token_id.clone()],
            event_time - 1,
            completed_bucket_end,
            visible_at,
            60,
        )
        .await
        .expect("mid series before correction");
    assert_eq!(before_correction.len(), 1);
    assert_eq!(
        before_correction[0].mid_price.map(Price::from),
        Some(Price::new(Decimal::new(40, 2)))
    );

    let after_correction = read
        .mid_price_series(
            vec![token_id],
            event_time - 1,
            completed_bucket_end,
            corrected_at,
            60,
        )
        .await
        .expect("mid series after correction");
    assert_eq!(after_correction.len(), 1);
    assert_eq!(
        after_correction[0].mid_price.map(Price::from),
        Some(Price::new(Decimal::new(60, 2)))
    );
}

pub async fn resolution_pit_bounded() {
    let (pool, client, _container) = setup_clickhouse().await;
    let read = ChQuantFactReadRepository::new(pool);
    let market_id = MarketId::new("0xchpit-res");
    let late_observed_market_id = MarketId::new("0xchpit-res-late-observed");
    let future_market_id = MarketId::new("0xchpit-res-future");
    let conflicting_market_id = MarketId::new("0xchpit-res-conflicting-checkpoint");
    let yes = TokenId::new("101");
    let no = TokenId::new("202");
    let half = PayoutRatio::try_new(Decimal::new(5, 1)).expect("half payout");

    let early = fresh_event_time_ms();
    let late = early + 10_000;
    let as_of = early + 5_000;

    let rows = vec![
        resolution_row(
            market_id.clone(),
            [yes.clone(), no.clone()],
            [PayoutRatio::ONE, PayoutRatio::ZERO],
            early,
            early,
            1,
        ),
        resolution_row(
            late_observed_market_id.clone(),
            [yes.clone(), no.clone()],
            [half, half],
            early + 1_000,
            as_of + 1_000,
            2,
        ),
        resolution_row(
            future_market_id.clone(),
            [yes.clone(), no.clone()],
            [PayoutRatio::ZERO, PayoutRatio::ONE],
            late,
            late,
            3,
        ),
        resolution_row(
            conflicting_market_id.clone(),
            [yes.clone(), no.clone()],
            [PayoutRatio::ONE, PayoutRatio::ZERO],
            early + 2_000,
            early + 2_000,
            4,
        ),
        resolution_row(
            conflicting_market_id.clone(),
            [yes.clone(), no.clone()],
            [PayoutRatio::ZERO, PayoutRatio::ONE],
            early + 2_000,
            early + 3_000,
            4,
        ),
    ];
    let mut insert = client
        .insert::<MarketResolutionRow>("market_resolution_event")
        .await
        .expect("insert");
    for row in &rows {
        insert.write(row).await.expect("write");
    }
    insert.write(&rows[0]).await.expect("write exact duplicate");
    insert.end().await.expect("end");

    let resolved = read
        .resolution_at(&market_id, as_of, as_of)
        .await
        .expect("read")
        .expect("resolution");
    assert_eq!(resolved.resolved_at, early);
    assert_eq!(
        resolved.payout_for(&yes).expect("YES payout"),
        PayoutRatio::ONE
    );
    assert_resolution_identity_queries(
        &read,
        &rows,
        &market_id,
        &conflicting_market_id,
        early,
        late,
    )
    .await;

    let hidden = read
        .resolution_at(&late_observed_market_id, as_of, as_of)
        .await
        .expect("read hidden resolution");
    assert!(hidden.is_none());

    let newly_visible = read
        .resolution_at(&late_observed_market_id, as_of, as_of + 1_000)
        .await
        .expect("read newly visible")
        .expect("newly visible resolution");
    assert_eq!(
        newly_visible.payout_for(&yes).expect("YES split payout"),
        half
    );
    assert_eq!(
        newly_visible.payout_for(&no).expect("NO split payout"),
        half
    );

    assert!(
        read.resolution_at(&future_market_id, as_of, late)
            .await
            .expect("read future resolution")
            .is_none()
    );

    let before_visibility = read
        .resolutions_between(
            vec![
                market_id.clone(),
                late_observed_market_id.clone(),
                future_market_id.clone(),
            ],
            early,
            as_of,
            as_of,
        )
        .await
        .expect("bounded resolution range");
    assert_eq!(before_visibility.len(), 1);
    let after_visibility = read
        .resolutions_between(
            vec![market_id, late_observed_market_id, future_market_id],
            early,
            as_of,
            as_of + 1_000,
        )
        .await
        .expect("visible resolution range");
    assert_eq!(after_visibility.len(), 2);
}

async fn assert_resolution_identity_queries(
    read: &ChQuantFactReadRepository,
    rows: &[MarketResolutionRow],
    market_id: &MarketId,
    conflicting_market_id: &MarketId,
    early: i64,
    late: i64,
) {
    let exact = read
        .resolution_by_checkpoint(&rows[0].source_checkpoint_hash)
        .await
        .expect("read exact resolution checkpoint")
        .expect("exact checkpoint exists");
    assert_eq!(exact, rows[0]);
    assert_eq!(
        read.resolution_by_market(market_id)
            .await
            .expect("read one canonical resolution by market")
            .expect("canonical market resolution exists"),
        rows[0]
    );
    assert!(
        read.resolution_by_market(&MarketId::new("0xchpit-res-missing"))
            .await
            .expect("read missing market resolution")
            .is_none()
    );
    assert!(matches!(
        read.resolution_by_checkpoint(&rows[3].source_checkpoint_hash)
            .await
            .expect_err("one checkpoint cannot bind conflicting resolution content"),
        StorageError::InvariantViolation { .. }
    ));
    assert!(matches!(
        read.resolution_by_market(conflicting_market_id)
            .await
            .expect_err("one market cannot bind conflicting resolution content"),
        StorageError::InvariantViolation { .. }
    ));
    assert!(matches!(
        read.resolution_at(conflicting_market_id, late, late)
            .await
            .expect_err("PIT lookup cannot choose between conflicting market truths"),
        StorageError::InvariantViolation { .. }
    ));
    assert!(matches!(
        read.resolutions_between(vec![conflicting_market_id.clone()], early, late, late,)
            .await
            .expect_err("range lookup cannot emit conflicting market truths"),
        StorageError::InvariantViolation { .. }
    ));
}

pub async fn weather_long_form_preserving() {
    let (pool, client, _container) = setup_clickhouse().await;
    let read = ChQuantFactReadRepository::new(pool);
    let observed_at = fresh_event_time_ms();
    let first_visible_at = observed_at + 1_000;
    let correction_visible_at = observed_at + 2_000;
    let station = "KJFK".to_owned();
    let observation_instrument = DomainInstrumentKey::new("AVIATION_WEATHER:KJFK");
    let original_hash = content_hash('3');

    let original = WeatherObservationFactRow {
        source_id: DomainSourceId::aviation_weather(),
        instrument_key: observation_instrument.clone(),
        subject_key: station.clone(),
        local_date: Utc::now().date_naive().into(),
        report_kind: "metar".to_owned(),
        variable: "temperature".to_owned(),
        value: ChDecimal64::from(Decimal::new(21_25, 2)),
        unit: "celsius".to_owned(),
        precision: ChDecimal64::from(Decimal::new(25, 2)),
        observed_at,
        valid_from: Some(observed_at),
        valid_to: None,
        published_at: observed_at,
        available_at: first_visible_at,
        revision: 1,
        report_hash: original_hash,
        supersedes_report_hash: None,
        raw_report: "METAR KJFK TEST".to_owned(),
        schema_version: ChSchemaVersion::FIRST,
    };
    let correction = WeatherObservationFactRow {
        report_kind: "correction".to_owned(),
        value: ChDecimal64::from(Decimal::new(22_00, 2)),
        published_at: observed_at + 1_500,
        available_at: correction_visible_at,
        revision: 2,
        report_hash: content_hash('4'),
        supersedes_report_hash: Some(original_hash),
        raw_report: "METAR KJFK COR TEST".to_owned(),
        ..original.clone()
    };
    let precipitation = WeatherObservationFactRow {
        source_id: DomainSourceId::new("nws_precipitation"),
        instrument_key: DomainInstrumentKey::new("NWS_PRECIPITATION:KJFK"),
        report_kind: "historical_ghcnh".to_owned(),
        variable: "precipitation".to_owned(),
        value: ChDecimal64::from(Decimal::new(12_70, 2)),
        unit: "millimeter".to_owned(),
        precision: ChDecimal64::from(Decimal::new(10, 2)),
        available_at: observed_at + 500,
        report_hash: content_hash('5'),
        raw_report: "PRECIP KJFK TEST".to_owned(),
        ..original.clone()
    };
    let mut observation_insert = client
        .insert::<WeatherObservationFactRow>("quant_weather_observation_fact")
        .await
        .expect("insert weather observations");
    for row in [&original, &original, &correction, &precipitation] {
        observation_insert
            .write(row)
            .await
            .expect("write weather observation");
    }
    observation_insert
        .end()
        .await
        .expect("end weather observation insert");

    assert_preposted_nhc_pit(&client, &read, observed_at, first_visible_at).await;

    let before_correction = read
        .weather_observation_facts_between(
            vec![station.clone()],
            observed_at - 1,
            observed_at + 1,
            first_visible_at,
            first_visible_at,
        )
        .await
        .expect("read observations before correction");
    assert_eq!(before_correction.len(), 2);
    assert_eq!(
        before_correction
            .iter()
            .filter(|row| row.variable == "temperature")
            .count(),
        1,
        "an exact writer retry must not duplicate one immutable report"
    );
    assert!(
        before_correction
            .iter()
            .any(|row| row.variable == "precipitation" && row.unit == "millimeter"),
        "the long-form table must retain a non-temperature variable"
    );

    let after_correction = read
        .weather_observation_facts_between(
            vec![station.clone()],
            observed_at - 1,
            observed_at + 1,
            correction_visible_at,
            correction_visible_at,
        )
        .await
        .expect("read observations after correction");
    assert_eq!(after_correction.len(), 3);
    assert!(
        after_correction.iter().any(|row| {
            row.variable == "temperature"
                && row.revision == 2
                && row.supersedes_report_hash.as_ref() == Some(&original.report_hash)
        }),
        "a visible correction must retain explicit supersession provenance"
    );

    assert_weather_forecast_pit(
        &client,
        &read,
        &station,
        observed_at,
        first_visible_at,
        correction_visible_at,
    )
    .await;
}

async fn assert_preposted_nhc_pit(
    client: &Client,
    read: &ChQuantFactReadRepository,
    base_time: i64,
    first_visible_at: i64,
) {
    let advisory_issuance = base_time + 5_000;
    let advisory = WeatherObservationFactRow {
        source_id: DomainSourceId::nhc_advisory(),
        instrument_key: DomainInstrumentKey::new("NHC_ADVISORY:eastern_pacific:EP052026"),
        subject_key: "EP052026".to_owned(),
        local_date: Utc::now().date_naive().into(),
        report_kind: "nhc_advisory".to_owned(),
        variable: "cyclone_intensity".to_owned(),
        value: ChDecimal64::from(Decimal::new(55, 0)),
        unit: "knot".to_owned(),
        precision: ChDecimal64::from(Decimal::ONE),
        observed_at: advisory_issuance,
        valid_from: Some(advisory_issuance),
        valid_to: None,
        published_at: base_time + 500,
        available_at: first_visible_at,
        revision: 0,
        report_hash: content_hash('6'),
        supersedes_report_hash: None,
        raw_report: "NHC EP052026 ADVISORY 013".to_owned(),
        schema_version: ChSchemaVersion::FIRST,
    };
    let mut insert = client
        .insert::<WeatherObservationFactRow>("quant_weather_observation_fact")
        .await
        .expect("insert preposted NHC advisory");
    insert.write(&advisory).await.expect("write NHC advisory");
    insert.end().await.expect("end NHC advisory insert");

    let before_issuance = read
        .weather_observation_facts_between(
            vec![advisory.subject_key.clone()],
            base_time,
            advisory_issuance,
            advisory_issuance - 1,
            advisory_issuance - 1,
        )
        .await
        .expect("read before nominal advisory issuance");
    assert!(
        before_issuance.is_empty(),
        "a preposted NHC file must not enter the observation window before nominal issuance"
    );
    let at_issuance = read
        .weather_observation_facts_between(
            vec![advisory.subject_key.clone()],
            base_time,
            advisory_issuance + 1,
            advisory_issuance,
            advisory_issuance,
        )
        .await
        .expect("read at nominal advisory issuance");
    assert_eq!(at_issuance.len(), 1);
    assert_eq!(at_issuance[0].report_hash, advisory.report_hash);
}

async fn assert_weather_forecast_pit(
    client: &Client,
    read: &ChQuantFactReadRepository,
    station: &str,
    reference_time: i64,
    first_visible_at: i64,
    correction_visible_at: i64,
) {
    let valid_time = reference_time + 86_400_000;
    let deterministic = WeatherForecastFactRow {
        source_id: DomainSourceId::gefs(),
        instrument_key: DomainInstrumentKey::new("GEFS:KJFK"),
        subject_key: station.to_owned(),
        variable: "temperature_maximum".to_owned(),
        value: ChDecimal64::from(Decimal::new(23_50, 2)),
        unit: "celsius".to_owned(),
        precision: ChDecimal64::from(Decimal::new(10, 2)),
        reference_time,
        valid_time,
        published_at: reference_time + 500,
        available_at: first_visible_at,
        lead_hours: 24,
        member: None,
        revision: 1,
        grid_binding_hash: content_hash('6'),
        run_manifest_hash: content_hash('7'),
        report_hash: content_hash('8'),
        schema_version: ChSchemaVersion::FIRST,
    };
    let ensemble_member = WeatherForecastFactRow {
        value: ChDecimal64::from(Decimal::new(24_25, 2)),
        available_at: correction_visible_at,
        member: Some(0),
        report_hash: content_hash('9'),
        ..deterministic.clone()
    };
    let mut forecast_insert = client
        .insert::<WeatherForecastFactRow>("quant_weather_forecast_fact")
        .await
        .expect("insert weather forecasts");
    for row in [&deterministic, &ensemble_member] {
        forecast_insert
            .write(row)
            .await
            .expect("write weather forecast");
    }
    forecast_insert
        .end()
        .await
        .expect("end weather forecast insert");

    let forecast_before_member = read
        .weather_forecast_facts_between(
            vec![station.to_owned()],
            valid_time - 1,
            valid_time + 1,
            reference_time,
            first_visible_at,
        )
        .await
        .expect("read forecast before ensemble member");
    assert_eq!(forecast_before_member.len(), 1);
    assert_eq!(forecast_before_member[0].member, None);

    let forecast_after_member = read
        .weather_forecast_facts_between(
            vec![station.to_owned()],
            valid_time - 1,
            valid_time + 1,
            reference_time,
            correction_visible_at,
        )
        .await
        .expect("read forecast after ensemble member");
    assert_eq!(forecast_after_member.len(), 2);
    assert!(
        forecast_after_member
            .iter()
            .any(|row| row.member == Some(0))
    );
}

pub async fn trade_preserves_after_merge() {
    let (pool, client, _container) = setup_clickhouse().await;
    let read = ChQuantFactReadRepository::new(Arc::clone(&pool));
    let market_id = MarketId::new("0xtrade-tape-dedupe");
    let token_id = TokenId::new("tok-dedupe");
    // Keep the test partition close to wall clock for compact integration scans.
    let event_time = fresh_event_time_ms();

    let base = TradeTapeRow {
        market_id: market_id.clone(),
        token_id: token_id.clone(),
        event_time,
        ingestion_time: event_time,
        stream_session_id: None,
        token_sequence: None,
        participant_address: "0xparticipant".to_owned(),
        participant_role: ChTradeParticipantRole::Maker,
        side: ChTradeSide::Buy,
        price: ChPrice::from(Price::new(Decimal::new(55, 2))),
        size_shares: ChShares::from(Shares::new(Decimal::from(10))),
        notional_usd: ChUsd::from(Usd::new(Decimal::from(5))),
        tx_hash: Some("0xtx".to_owned()),
        source_event_id: "trade-dedupe-1".to_owned(),
        source: ChTradeTapeSource::OnChainOrderFilled,
        observed_field_flags: u16::MAX,
        fee_rate_bps: None,
        reconciliation_status: ChTradeReconciliationStatus::OnChainOnly,
        matched_source_event_id: None,
        revision: 1,
        reconciled_at: None,
        raw_payload_json: None,
        schema_version: ChSchemaVersion(2),
    };
    let mut stale = base.clone();
    stale.ingestion_time = event_time - 1_000;
    let mut fresh = base.clone();
    fresh.ingestion_time = event_time + 1_000;
    fresh.revision = 2;
    fresh.reconciliation_status = ChTradeReconciliationStatus::Matched;
    fresh.matched_source_event_id = Some("ws:trade-dedupe-1".to_owned());
    fresh.reconciled_at = Some(fresh.ingestion_time);

    let mut insert = client
        .insert::<TradeTapeRow>("quant_trade_tape")
        .await
        .expect("insert");
    insert.write(&stale).await.expect("write stale");
    insert.write(&fresh).await.expect("write fresh");
    insert.end().await.expect("end");
    client
        .query("OPTIMIZE TABLE quant_trade_tape FINAL")
        .execute()
        .await
        .expect("merge trade tape parts");

    let rows_before_revision = read
        .market_tape_window(
            vec![market_id.clone()],
            event_time - 1,
            event_time + 1,
            event_time + 1,
        )
        .await
        .expect("read before revision");
    assert!(rows_before_revision.is_empty());

    let rows = read
        .market_tape_window(
            vec![market_id.clone()],
            event_time - 1,
            event_time + 1,
            event_time + 1_000,
        )
        .await
        .expect("read after revision");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].ingestion_time, fresh.ingestion_time);
}
