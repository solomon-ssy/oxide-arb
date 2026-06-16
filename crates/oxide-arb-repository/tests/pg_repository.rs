//! `PostgreSQL` repository integration tests (requires Docker).

#[path = "common/market_fixtures.rs"]
mod market_fixtures;
#[path = "common/pg.rs"]
mod pg;

use std::collections::HashSet;

use chrono::{NaiveDate, Utc};
use market_fixtures::{make_event, make_market};
use oxide_arb_models::{
    domain::control_factor::{
        AuditChain, BucketRiskDimensions, BucketRiskPayload, ConfidenceInterval,
        ControlFactorValue, DataCoverageReport, FactorDimensions, FactorEvidence, FactorPayload,
        NewControlFactorAuditEvent, NewControlFactorPublication, NewControlFactorValue,
        PointInTimeInputManifest, PublishPublicationOutcome, TailRiskEvidence,
    },
    enums::control_factor::{
        AuditResourceType, ControlAuditEventType, ControlFactorType, FactorMaturity, FactorStatus,
        PublicationMode, PublicationStatus,
    },
};
use oxide_arb_models::{
    domain::{
        AcquireMaterializationRunOutcome, CancelMaterializationRunOutcome,
        EnqueueMaterializationRunOptions, EnqueueMaterializationRunOutcome,
        MaterializationRunStatusPatch, NewAccountingPeriod, NewBalanceSnapshot,
        NewCalibrationOutcome, NewControlFactorMaterializationRun, NewControlFactorShadowDecision,
        NewControlFactorStageReport, NewEmergencySnapshot, NewPosition, NewPotentialLoss,
        NewReconciliationReport, NewRiskAuditEvent, NewRuntimeConfigActivation,
        NewRuntimeConfigVersion, NewTrade, ResolvePotentialLoss, RunTransitionOutcome,
        SettlePositionParams, ShadowDecisionAggregate, TradeObservation, TradePageQuery,
        UpsertBlacklistEntry, UpsertCalibration, UpsertRiskEngineState,
    },
    domain::{
        pagination::PageRequest,
        query::{TimeWindow, TradeAnalyticsFilter},
    },
    enums::{
        calibration::{DurationBucket, PriceZone},
        common::{
            ExecutionMode, MarketCategory, PositionStatus, RedeemResolutionSource, RedeemStatus,
            ReportType, SettlementTrigger, Side, TradeBusinessOutcome, TradeState,
        },
        control_factor::{
            EvidenceStageStatus, MaterializationOutputPolicy, MaterializationRunKind,
            MaterializationRunStatus, MaterializationStageName, RunTriggerType,
        },
        fact::{BalanceSnapshotSource, ShadowDecisionType},
        risk::{
            BlacklistReason, BlacklistScope, BreakerStateName, CircuitBreakerLevel,
            ReconciliationStatus, RiskAuditEventType,
        },
        runtime_config::{RuntimeConfigActivationKind, RuntimeConfigVersionSource},
    },
    types::*,
};
use oxide_arb_repository::{postgres::*, traits::*};
use oxide_arb_storage::postgres::PostgresPool;
use pg::setup_pg;
use rust_decimal::Decimal;
use rust_decimal_macros::dec;

async fn seed_market(
    pool: &PostgresPool,
    event_id: &str,
    market_id: &str,
    category: MarketCategory,
) {
    let event_repo = PgEventRepository::new(pool.connection().clone());
    let market_repo = PgMarketRepository::new(pool.connection().clone());
    event_repo
        .upsert(make_event(
            event_id,
            "Seed Event",
            &format!("{event_id}-slug"),
            category,
        ))
        .await
        .unwrap();
    market_repo
        .upsert(make_market(
            market_id,
            event_id,
            "Seed question?",
            &format!("{market_id}-slug"),
            category,
            None,
        ))
        .await
        .unwrap();
}

#[tokio::test]
#[ignore = "requires Docker"]
async fn event_repository_crud() {
    let (pool, _container) = setup_pg().await;
    let repo = PgEventRepository::new(pool.connection().clone());

    let model = make_event(
        "evt-test-1",
        "Test Event",
        "test-event",
        MarketCategory::Sports,
    );
    let inserted = repo.upsert(model).await.expect("insert event");
    assert_eq!(inserted.title, "Test Event");

    let found = repo
        .find_by_id(&EventId::new("evt-test-1"))
        .await
        .expect("find");
    assert!(found.is_some());
    assert_eq!(found.unwrap().slug, "test-event");

    let active = repo.find_active().await.expect("find_active");
    assert_eq!(active.len(), 1);
}

#[tokio::test]
#[ignore = "requires Docker"]
async fn market_repository_crud() {
    let (pool, _container) = setup_pg().await;
    let event_repo = PgEventRepository::new(pool.connection().clone());
    let market_repo = PgMarketRepository::new(pool.connection().clone());

    event_repo
        .upsert(make_event(
            "evt-mkt-test",
            "Market Test Event",
            "market-test-event",
            MarketCategory::Finance,
        ))
        .await
        .unwrap();

    let mkt = make_market(
        "0xmarket1",
        "evt-mkt-test",
        "Will X happen?",
        "will-x-happen",
        MarketCategory::Finance,
        Some(Utc::now() + chrono::Duration::hours(24)),
    );

    let inserted = market_repo.upsert(mkt).await.expect("insert market");
    assert_eq!(inserted.question, "Will X happen?");

    let found = market_repo
        .find_by_id(&MarketId::new("0xmarket1"))
        .await
        .unwrap();
    assert!(found.is_some());

    let active = market_repo.find_active().await.unwrap();
    assert_eq!(active.len(), 1);

    let candidates = market_repo
        .find_endgame_candidates(Utc::now() + chrono::Duration::hours(48))
        .await
        .unwrap();
    assert_eq!(candidates.len(), 1);
}

#[tokio::test]
#[ignore = "requires Docker"]
async fn market_insert_then_update() {
    let (pool, _container) = setup_pg().await;
    let event_repo = PgEventRepository::new(pool.connection().clone());
    let market_repo = PgMarketRepository::new(pool.connection().clone());

    event_repo
        .upsert(make_event(
            "evt-update",
            "Update Test",
            "update-test",
            MarketCategory::Tech,
        ))
        .await
        .unwrap();

    let mkt = make_market(
        "0xupdate-market",
        "evt-update",
        "Original question?",
        "original",
        MarketCategory::Tech,
        None,
    );
    let inserted = market_repo.upsert(mkt).await.unwrap();
    assert_eq!(inserted.question, "Original question?");

    let updated_mkt = make_market(
        "0xupdate-market",
        "evt-update",
        "Updated question?",
        "updated",
        MarketCategory::Tech,
        None,
    );
    let updated = market_repo.upsert(updated_mkt).await.unwrap();
    assert_eq!(updated.question, "Updated question?");
    assert_eq!(updated.slug, "updated");

    let upsert_model = make_market(
        "0xupsert-market",
        "evt-update",
        "Upsert question?",
        "upsert-slug",
        MarketCategory::Tech,
        None,
    );
    market_repo.upsert_batch(vec![upsert_model]).await.unwrap();

    let upserted = market_repo
        .find_by_id(&MarketId::new("0xupsert-market"))
        .await
        .unwrap()
        .expect("upserted market should exist");
    assert_eq!(upserted.question, "Upsert question?");

    let upsert_update = make_market(
        "0xupsert-market",
        "evt-update",
        "Upsert updated?",
        "upsert-slug",
        MarketCategory::Tech,
        None,
    );
    market_repo.upsert_batch(vec![upsert_update]).await.unwrap();
    let reloaded = market_repo
        .find_by_id(&MarketId::new("0xupsert-market"))
        .await
        .unwrap()
        .expect("upserted market should still exist");
    assert_eq!(reloaded.question, "Upsert updated?");

    let all_active = market_repo.find_active().await.unwrap();
    assert_eq!(
        all_active.len(),
        2,
        "upserts should update in place, not duplicate rows per market_id"
    );
    let ids: HashSet<_> = all_active.iter().map(|m| m.market_id.clone()).collect();
    assert_eq!(ids.len(), 2, "each market_id should appear once");
}

/// Insert payload for one BUY trade on the seeded `0xtrade-mkt` market.
fn sample_buy_trade(execution_id: &ExecutionId) -> NewTrade {
    NewTrade {
        trade_id: TradeId::from_v7(),
        execution_id: execution_id.clone(),
        reservation_id: ReservationId::from_v7(),
        opportunity_id: OpportunityId::from_v7(),
        market_id: MarketId::new("0xtrade-mkt"),
        event_id: EventId::new("evt-trade"),
        token_id: TokenId::new("999001"),
        side: Side::Buy,
        shares: Shares::from(Decimal::new(10, 0)),
        price: Price::from(Decimal::new(95, 2)),
        cost_usd: Usd::from(Decimal::new(95, 1)),
        fee_usd: Usd::ONE,
        detected_edge_bps: Some(Bps::from(Decimal::from(200))),
        detected_profit_usd: Some(Usd::from(Decimal::new(5, 0))),
        scored_snapshot: serde_json::json!({}),
        category: MarketCategory::Sports,
        execution_mode: ExecutionMode::DryRun,
    }
}

#[tokio::test]
#[ignore = "requires Docker"]
async fn trade_repository_crud() {
    let (pool, _container) = setup_pg().await;
    seed_market(&pool, "evt-trade", "0xtrade-mkt", MarketCategory::Sports).await;

    let trade_repo = PgTradeRepository::new(pool.connection().clone());
    let execution_id = ExecutionId::from_v7();

    let created = trade_repo
        .create(sample_buy_trade(&execution_id))
        .await
        .expect("create trade");
    assert_eq!(created.state, TradeState::Intent);

    assert!(
        trade_repo
            .mark_submitted(&created.trade_id, Utc::now())
            .await
            .expect("mark submitted")
    );
    trade_repo
        .mark_observed(
            &created.trade_id,
            TradeObservation {
                state: TradeState::FillObserved,
                shares: created.shares,
                price: created.price,
                cost_usd: created.cost_usd,
                fee_usd: created.fee_usd,
                order_id: Some(OrderId::new("order-123")),
                tx_hash: Some("0xdead".into()),
                net_profit_usd: Some(Usd::from(Decimal::new(4, 0))),
                latency_ms: Some(42),
                error_message: None,
                confirmed_at: Utc::now(),
            },
        )
        .await
        .expect("mark observed");
    let updated = trade_repo
        .find_by_id(&created.trade_id)
        .await
        .unwrap()
        .expect("trade still exists");
    assert_eq!(updated.state, TradeState::FillObserved);
    let claimed = trade_repo
        .claim_unprocessed(
            10,
            "pg-repository-test",
            Utc::now(),
            Utc::now() - chrono::Duration::minutes(5),
        )
        .await
        .expect("claim observed trade");
    assert_eq!(claimed.len(), 1);
    assert_eq!(claimed[0].state, TradeState::FillProcessing);
    assert_eq!(claimed[0].post_trade_attempts, 1);
    assert_eq!(
        claimed[0].post_trade_claim_owner.as_deref(),
        Some("pg-repository-test")
    );

    let by_exec = trade_repo.find_by_execution(&execution_id).await.unwrap();
    assert_eq!(by_exec.len(), 1);

    let by_market = trade_repo
        .find_by_market(&MarketId::new("0xtrade-mkt"), 10)
        .await
        .unwrap();
    assert_eq!(by_market.len(), 1);

    let recent = trade_repo
        .find_recent(Utc::now() - chrono::Duration::hours(1), 10)
        .await
        .unwrap();
    assert!(!recent.is_empty());

    let counts = trade_repo
        .count_by_outcome(Utc::now() - chrono::Duration::hours(1))
        .await
        .unwrap();
    assert!(
        counts
            .get(&TradeBusinessOutcome::Success)
            .copied()
            .unwrap_or(0)
            >= 1
    );
}

#[tokio::test]
#[ignore = "requires Docker"]
async fn trade_page_filters_by_side() {
    let (pool, _container) = setup_pg().await;
    seed_market(&pool, "evt-trade", "0xtrade-mkt", MarketCategory::Sports).await;
    let trade_repo = PgTradeRepository::new(pool.connection().clone());
    trade_repo
        .create(sample_buy_trade(&ExecutionId::from_v7()))
        .await
        .expect("create trade");

    // The seeded trade is a BUY, so SELL must page empty.
    let buys = trade_repo
        .page(TradePageQuery {
            side: Some(Side::Buy),
            ..TradePageQuery::default()
        })
        .await
        .expect("page buys");
    assert_eq!(buys.total, 1, "side=BUY matches the seeded trade");
    let sells = trade_repo
        .page(TradePageQuery {
            side: Some(Side::Sell),
            ..TradePageQuery::default()
        })
        .await
        .expect("page sells");
    assert_eq!(sells.total, 0, "side=SELL matches nothing");
}

#[tokio::test]
#[ignore = "requires Docker"]
async fn trade_repository_batch_create() {
    let (pool, _container) = setup_pg().await;
    seed_market(&pool, "evt-batch", "0xbatch-mkt", MarketCategory::Finance).await;

    let trade_repo = PgTradeRepository::new(pool.connection().clone());
    let trades: Vec<NewTrade> = (0..3)
        .map(|i| NewTrade {
            trade_id: TradeId::from_v7(),
            execution_id: ExecutionId::from_v7(),
            reservation_id: ReservationId::from_v7(),
            opportunity_id: OpportunityId::from_v7(),
            market_id: MarketId::new("0xbatch-mkt"),
            event_id: EventId::new("evt-batch"),
            token_id: TokenId::new(format!("batch-{i}")),
            side: Side::Buy,
            shares: Shares::from(Decimal::ONE),
            price: Price::from(Decimal::new(50, 2)),
            cost_usd: Usd::from(Decimal::ONE),
            fee_usd: Usd::ZERO,
            detected_edge_bps: None,
            detected_profit_usd: None,
            scored_snapshot: serde_json::json!({}),
            category: MarketCategory::Finance,
            execution_mode: ExecutionMode::Paper,
        })
        .collect();

    let inserted = trade_repo.create_batch(trades).await.unwrap();
    assert_eq!(inserted, 3);

    let rows = trade_repo
        .find_by_market(&MarketId::new("0xbatch-mkt"), 10)
        .await
        .unwrap();
    assert_eq!(rows.len(), 3);
}

#[tokio::test]
#[ignore = "requires Docker"]
async fn position_lifecycle() {
    let (pool, _container) = setup_pg().await;
    seed_market(
        &pool,
        "evt-pos-test",
        "0xpos-market",
        MarketCategory::Politics,
    )
    .await;

    let trade_repo = PgTradeRepository::new(pool.connection().clone());
    let trade_id = TradeId::from_v7();
    trade_repo
        .create(NewTrade {
            trade_id: trade_id.clone(),
            execution_id: ExecutionId::from_v7(),
            reservation_id: ReservationId::from_v7(),
            opportunity_id: OpportunityId::from_v7(),
            market_id: MarketId::new("0xpos-market"),
            event_id: EventId::new("evt-pos-test"),
            token_id: TokenId::new("111"),
            side: Side::Buy,
            shares: Shares::from(Decimal::new(100, 0)),
            price: Price::from(Decimal::new(95, 2)),
            cost_usd: Usd::from(Decimal::new(95, 0)),
            fee_usd: Usd::from(Decimal::ONE),
            detected_edge_bps: None,
            detected_profit_usd: None,
            scored_snapshot: serde_json::json!({}),
            category: MarketCategory::Politics,
            execution_mode: ExecutionMode::Paper,
        })
        .await
        .expect("create trade");

    let position_repo = PgPositionRepository::new(pool.connection().clone());
    let opened = position_repo
        .create(NewPosition {
            position_id: PositionId::from_v7(),
            trade_id,
            market_id: MarketId::new("0xpos-market"),
            token_id: TokenId::new("111"),
            side: Side::Buy,
            execution_mode: ExecutionMode::Paper,
            shares: Shares::from(Decimal::new(100, 0)),
            avg_entry_price: Price::from(Decimal::new(95, 2)),
            total_cost_usd: Usd::from(Decimal::new(95, 0)),
            total_fees_usd: Usd::from(Decimal::ONE),
            redeem_status: RedeemStatus::NotRequired,
            redeem_neg_risk: false,
            redeem_route: "standard_ctf".into(),
            redeem_holder_address: None,
            redeem_resolution: RedeemResolutionSource::ClassStandard,
            redeem_gas_limit: 500_000,
            redeem_gas_paid_usd: None,
        })
        .await
        .expect("create position");
    assert_eq!(opened.status, PositionStatus::Open);
    let pos_id = opened.position_id.clone();

    assert_eq!(position_repo.count_open().await.unwrap(), 1);
    assert_eq!(
        position_repo.total_exposure().await.unwrap(),
        Usd::from(Decimal::new(95, 0))
    );

    position_repo
        .close_position(&pos_id, Decimal::new(5, 0))
        .await
        .expect("close position");
    assert_eq!(position_repo.count_open().await.unwrap(), 0);
}

#[tokio::test]
#[ignore = "requires Docker"]
async fn position_settle() {
    let (pool, _container) = setup_pg().await;
    seed_market(&pool, "evt-settle", "0xsettle-mkt", MarketCategory::Crypto).await;

    let trade_repo = PgTradeRepository::new(pool.connection().clone());
    let trade_id = TradeId::from_v7();
    trade_repo
        .create(NewTrade {
            trade_id: trade_id.clone(),
            execution_id: ExecutionId::from_v7(),
            reservation_id: ReservationId::from_v7(),
            opportunity_id: OpportunityId::from_v7(),
            market_id: MarketId::new("0xsettle-mkt"),
            event_id: EventId::new("evt-settle"),
            token_id: TokenId::new("333"),
            side: Side::Sell,
            shares: Shares::from(Decimal::new(50, 0)),
            price: Price::from(Decimal::new(80, 2)),
            cost_usd: Usd::from(Decimal::new(40, 0)),
            fee_usd: Usd::ZERO,
            detected_edge_bps: None,
            detected_profit_usd: None,
            scored_snapshot: serde_json::json!({}),
            category: MarketCategory::Crypto,
            execution_mode: ExecutionMode::Paper,
        })
        .await
        .expect("create trade");

    let position_repo = PgPositionRepository::new(pool.connection().clone());
    let created = position_repo
        .create(NewPosition {
            position_id: PositionId::from_v7(),
            trade_id,
            market_id: MarketId::new("0xsettle-mkt"),
            token_id: TokenId::new("333"),
            side: Side::Sell,
            execution_mode: ExecutionMode::Paper,
            shares: Shares::from(Decimal::new(50, 0)),
            avg_entry_price: Price::from(Decimal::new(80, 2)),
            total_cost_usd: Usd::from(Decimal::new(40, 0)),
            total_fees_usd: Usd::ZERO,
            redeem_status: RedeemStatus::NotRequired,
            redeem_neg_risk: false,
            redeem_route: "standard_ctf".into(),
            redeem_holder_address: None,
            redeem_resolution: RedeemResolutionSource::ClassStandard,
            redeem_gas_limit: 500_000,
            redeem_gas_paid_usd: None,
        })
        .await
        .expect("create position");
    position_repo
        .settle_position(
            &created.position_id,
            SettlePositionParams {
                winning_token_id: TokenId::new("333"),
                settlement_payout_usd: Usd::from(Decimal::new(50, 0)),
                realized_pnl: Decimal::new(10, 0),
                redeem_tx_hash: None,
                redeem_status: RedeemStatus::NotRequired,
                settlement_trigger: SettlementTrigger::Manual,
                oracle_verdict: None,
            },
        )
        .await
        .expect("settle position");
    assert!(
        position_repo
            .find_open(ExecutionMode::Paper)
            .await
            .unwrap()
            .is_empty()
    );
}

/// One successful (filled) trade in the given mode; returns its id.
async fn seed_successful_trade(
    trade_repo: &PgTradeRepository,
    market_id: &str,
    event_id: &str,
    token_id: &str,
    mode: ExecutionMode,
    cost: Decimal,
    fee: Decimal,
) -> TradeId {
    let trade_id = TradeId::from_v7();
    let created = trade_repo
        .create(NewTrade {
            trade_id: trade_id.clone(),
            execution_id: ExecutionId::from_v7(),
            reservation_id: ReservationId::from_v7(),
            opportunity_id: OpportunityId::from_v7(),
            market_id: MarketId::new(market_id),
            event_id: EventId::new(event_id),
            token_id: TokenId::new(token_id),
            side: Side::Buy,
            shares: Shares::from(Decimal::new(100, 0)),
            price: Price::from(Decimal::new(50, 2)),
            cost_usd: Usd::from(cost),
            fee_usd: Usd::from(fee),
            detected_edge_bps: None,
            detected_profit_usd: None,
            scored_snapshot: serde_json::json!({}),
            category: MarketCategory::Crypto,
            execution_mode: mode,
        })
        .await
        .expect("create trade");
    assert!(
        trade_repo
            .mark_submitted(&created.trade_id, Utc::now())
            .await
            .expect("mark submitted")
    );
    trade_repo
        .mark_observed(
            &created.trade_id,
            TradeObservation {
                state: TradeState::FillObserved,
                shares: created.shares,
                price: created.price,
                cost_usd: created.cost_usd,
                fee_usd: created.fee_usd,
                order_id: None,
                tx_hash: None,
                net_profit_usd: None,
                latency_ms: None,
                error_message: None,
                confirmed_at: Utc::now(),
            },
        )
        .await
        .expect("mark observed");
    trade_id
}

/// Seed payload for analytics aggregation tests.
struct ObservedTradeSpec {
    market_id: &'static str,
    event_id: &'static str,
    token_id: &'static str,
    mode: ExecutionMode,
    state: TradeState,
    cost: Decimal,
    fee: Decimal,
    net_profit: Option<Decimal>,
    edge_bps: Option<Decimal>,
}

/// Insert one trade row and optionally drive it through submit + observe.
async fn seed_observed_trade(trade_repo: &PgTradeRepository, spec: ObservedTradeSpec) -> TradeId {
    let trade_id = TradeId::from_v7();
    let created = trade_repo
        .create(NewTrade {
            trade_id: trade_id.clone(),
            execution_id: ExecutionId::from_v7(),
            reservation_id: ReservationId::from_v7(),
            opportunity_id: OpportunityId::from_v7(),
            market_id: MarketId::new(spec.market_id),
            event_id: EventId::new(spec.event_id),
            token_id: TokenId::new(spec.token_id),
            side: Side::Buy,
            shares: Shares::from(Decimal::new(100, 0)),
            price: Price::from(Decimal::new(50, 2)),
            cost_usd: Usd::from(spec.cost),
            fee_usd: Usd::from(spec.fee),
            detected_edge_bps: spec.edge_bps.map(Bps::from),
            detected_profit_usd: None,
            scored_snapshot: serde_json::json!({}),
            category: MarketCategory::Crypto,
            execution_mode: spec.mode,
        })
        .await
        .expect("create trade");

    if spec.state == TradeState::Intent {
        return trade_id;
    }

    assert!(
        trade_repo
            .mark_submitted(&created.trade_id, Utc::now())
            .await
            .expect("mark submitted")
    );
    trade_repo
        .mark_observed(
            &created.trade_id,
            TradeObservation {
                state: spec.state,
                shares: created.shares,
                price: created.price,
                cost_usd: created.cost_usd,
                fee_usd: created.fee_usd,
                order_id: None,
                tx_hash: None,
                net_profit_usd: spec.net_profit.map(Usd::from),
                latency_ms: None,
                error_message: None,
                confirmed_at: Utc::now(),
            },
        )
        .await
        .expect("mark observed");
    trade_id
}

fn analytics_window() -> TimeWindow {
    TimeWindow::new(
        Utc::now() - chrono::Duration::hours(1),
        Utc::now() + chrono::Duration::hours(1),
    )
}

#[tokio::test]
#[ignore = "requires Docker"]
async fn trade_aggregate_between_empty_window_returns_zeros() {
    let (pool, _container) = setup_pg().await;
    let trade_repo = PgTradeRepository::new(pool.connection().clone());

    let stats = trade_repo
        .aggregate_between(
            Utc::now() - chrono::Duration::days(365),
            Utc::now() - chrono::Duration::days(364),
        )
        .await
        .expect("aggregate_between on empty window");

    assert_eq!(stats.trade_count, 0);
    assert_eq!(stats.success_count, 0);
    assert_eq!(stats.miss_count, 0);
    assert_eq!(stats.failed_count, 0);
    assert_eq!(stats.total_fill_cost, Usd::ZERO);
    assert_eq!(stats.total_fill_fees, Usd::ZERO);
    assert_eq!(stats.fill_expected_pnl, Usd::ZERO);
}

#[tokio::test]
#[ignore = "requires Docker"]
async fn trade_aggregate_between_counts_outcomes_and_sums() {
    let (pool, _container) = setup_pg().await;
    seed_market(&pool, "evt-agg-a", "0xagg-a", MarketCategory::Crypto).await;
    seed_market(&pool, "evt-agg-b", "0xagg-b", MarketCategory::Crypto).await;

    let trade_repo = PgTradeRepository::new(pool.connection().clone());
    seed_observed_trade(
        &trade_repo,
        ObservedTradeSpec {
            market_id: "0xagg-a",
            event_id: "evt-agg-a",
            token_id: "agg-a-1",
            mode: ExecutionMode::Paper,
            state: TradeState::FillObserved,
            cost: dec!(10),
            fee: dec!(1),
            net_profit: Some(dec!(5)),
            edge_bps: None,
        },
    )
    .await;
    seed_observed_trade(
        &trade_repo,
        ObservedTradeSpec {
            market_id: "0xagg-b",
            event_id: "evt-agg-b",
            token_id: "agg-b-1",
            mode: ExecutionMode::Paper,
            state: TradeState::MissObserved,
            cost: dec!(8),
            fee: dec!(1),
            net_profit: None,
            edge_bps: None,
        },
    )
    .await;
    seed_observed_trade(
        &trade_repo,
        ObservedTradeSpec {
            market_id: "0xagg-a",
            event_id: "evt-agg-a",
            token_id: "agg-a-2",
            mode: ExecutionMode::Paper,
            state: TradeState::FailObserved,
            cost: dec!(20),
            fee: dec!(2),
            net_profit: Some(dec!(-3)),
            edge_bps: None,
        },
    )
    .await;
    seed_observed_trade(
        &trade_repo,
        ObservedTradeSpec {
            market_id: "0xagg-b",
            event_id: "evt-agg-b",
            token_id: "agg-b-2",
            mode: ExecutionMode::Paper,
            state: TradeState::Intent,
            cost: dec!(5),
            fee: dec!(0),
            net_profit: None,
            edge_bps: None,
        },
    )
    .await;

    let window = analytics_window();
    let stats = trade_repo
        .aggregate_between(window.from, window.to)
        .await
        .expect("aggregate_between");

    assert_eq!(stats.trade_count, 4);
    assert_eq!(stats.success_count, 1);
    assert_eq!(stats.miss_count, 1);
    assert_eq!(stats.failed_count, 1);
    assert_eq!(stats.total_fill_cost, Usd::from(dec!(43)));
    assert_eq!(stats.total_fill_fees, Usd::from(dec!(4)));
    assert_eq!(stats.fill_expected_pnl, Usd::from(dec!(2)));
}

/// Paper trades on two markets plus one live trade excluded by mode filter.
async fn seed_market_performance_trades(trade_repo: &PgTradeRepository) {
    seed_observed_trade(
        trade_repo,
        ObservedTradeSpec {
            market_id: "0xperf-a",
            event_id: "evt-perf-a",
            token_id: "perf-a-1",
            mode: ExecutionMode::Paper,
            state: TradeState::FillObserved,
            cost: dec!(10),
            fee: dec!(1),
            net_profit: Some(dec!(10)),
            edge_bps: Some(dec!(100)),
        },
    )
    .await;
    seed_observed_trade(
        trade_repo,
        ObservedTradeSpec {
            market_id: "0xperf-a",
            event_id: "evt-perf-a",
            token_id: "perf-a-2",
            mode: ExecutionMode::Paper,
            state: TradeState::MissObserved,
            cost: dec!(6),
            fee: dec!(0),
            net_profit: None,
            edge_bps: Some(dec!(50)),
        },
    )
    .await;
    seed_observed_trade(
        trade_repo,
        ObservedTradeSpec {
            market_id: "0xperf-b",
            event_id: "evt-perf-b",
            token_id: "perf-b-1",
            mode: ExecutionMode::Paper,
            state: TradeState::FillObserved,
            cost: dec!(15),
            fee: dec!(2),
            net_profit: Some(dec!(30)),
            edge_bps: Some(dec!(200)),
        },
    )
    .await;
    seed_observed_trade(
        trade_repo,
        ObservedTradeSpec {
            market_id: "0xperf-a",
            event_id: "evt-perf-a",
            token_id: "perf-a-live",
            mode: ExecutionMode::Live,
            state: TradeState::FillObserved,
            cost: dec!(99),
            fee: dec!(9),
            net_profit: Some(dec!(99)),
            edge_bps: Some(dec!(999)),
        },
    )
    .await;
}

#[tokio::test]
#[ignore = "requires Docker"]
async fn trade_market_performance_groups_pages_and_filters_by_mode() {
    let (pool, _container) = setup_pg().await;
    seed_market(&pool, "evt-perf-a", "0xperf-a", MarketCategory::Crypto).await;
    seed_market(&pool, "evt-perf-b", "0xperf-b", MarketCategory::Crypto).await;

    let trade_repo = PgTradeRepository::new(pool.connection().clone());
    seed_market_performance_trades(&trade_repo).await;

    let window = analytics_window();
    let filter = TradeAnalyticsFilter {
        window,
        execution_mode: Some(ExecutionMode::Paper),
    };

    let page = trade_repo
        .market_performance(filter, PageRequest::new(1, 10))
        .await
        .expect("market_performance page 1");
    assert_eq!(page.total, 2, "two paper markets");
    assert_eq!(page.items.len(), 2);
    assert_eq!(page.items[0].market_id, MarketId::new("0xperf-b"));
    assert_eq!(page.items[0].trade_count, 1);
    assert_eq!(page.items[0].success_count, 1);
    assert_eq!(page.items[0].net_profit_usd, Usd::from(dec!(30)));
    assert_eq!(page.items[0].total_cost_usd, Usd::from(dec!(15)));
    assert_eq!(page.items[0].avg_edge_bps, Some(dec!(200)));

    let perf_a = &page.items[1];
    assert_eq!(perf_a.market_id, MarketId::new("0xperf-a"));
    assert_eq!(perf_a.trade_count, 2);
    assert_eq!(perf_a.success_count, 1);
    assert_eq!(perf_a.net_profit_usd, Usd::from(dec!(10)));
    assert_eq!(perf_a.total_cost_usd, Usd::from(dec!(16)));
    assert_eq!(perf_a.avg_edge_bps, Some(dec!(75)));

    let page_one = trade_repo
        .market_performance(filter, PageRequest::new(1, 1))
        .await
        .expect("market_performance paginated");
    assert_eq!(page_one.total, 2);
    assert_eq!(page_one.items.len(), 1);
    assert_eq!(page_one.items[0].market_id, MarketId::new("0xperf-b"));

    let page_two = trade_repo
        .market_performance(filter, PageRequest::new(2, 1))
        .await
        .expect("market_performance page 2");
    assert_eq!(page_two.items.len(), 1);
    assert_eq!(page_two.items[0].market_id, MarketId::new("0xperf-a"));
}

/// One open position on the mode-ledger market for the given mode.
async fn seed_open_position(
    position_repo: &PgPositionRepository,
    trade_id: TradeId,
    token_id: &str,
    mode: ExecutionMode,
    cost: Decimal,
    fee: Decimal,
) -> PositionId {
    position_repo
        .create(NewPosition {
            position_id: PositionId::from_v7(),
            trade_id,
            market_id: MarketId::new("0xmode-ledger"),
            token_id: TokenId::new(token_id),
            side: Side::Buy,
            execution_mode: mode,
            shares: Shares::from(Decimal::new(100, 0)),
            avg_entry_price: Price::from(Decimal::new(50, 2)),
            total_cost_usd: Usd::from(cost),
            total_fees_usd: Usd::from(fee),
            redeem_status: RedeemStatus::NotRequired,
            redeem_neg_risk: false,
            redeem_route: "standard_ctf".into(),
            redeem_holder_address: None,
            redeem_resolution: RedeemResolutionSource::ClassStandard,
            redeem_gas_limit: 500_000,
            redeem_gas_paid_usd: None,
        })
        .await
        .expect("create position")
        .position_id
}

/// Seed one successful paper trade ($40 + $1) and one successful live trade
/// ($70 + $2) on the shared mode-ledger market; returns their trade ids.
async fn seed_mode_ledger_trades(pool: &PostgresPool) -> (TradeId, TradeId) {
    seed_market(
        pool,
        "evt-mode-ledger",
        "0xmode-ledger",
        MarketCategory::Crypto,
    )
    .await;
    let trade_repo = PgTradeRepository::new(pool.connection().clone());
    let paper_trade = seed_successful_trade(
        &trade_repo,
        "0xmode-ledger",
        "evt-mode-ledger",
        "555001",
        ExecutionMode::Paper,
        Decimal::new(40, 0),
        Decimal::ONE,
    )
    .await;
    let live_trade = seed_successful_trade(
        &trade_repo,
        "0xmode-ledger",
        "evt-mode-ledger",
        "555002",
        ExecutionMode::Live,
        Decimal::new(70, 0),
        Decimal::new(2, 0),
    )
    .await;
    (paper_trade, live_trade)
}

async fn spend_total(repo: &PgTradeRepository, mode: ExecutionMode) -> Usd {
    repo.successful_spend_total(mode)
        .await
        .expect("spend total")
}

async fn payout_total(repo: &PgPositionRepository, mode: ExecutionMode) -> Usd {
    repo.settlement_payout_total(mode)
        .await
        .expect("payout total")
}

async fn open_count(repo: &PgPositionRepository, mode: ExecutionMode) -> usize {
    repo.find_open(mode).await.expect("find open").len()
}

#[tokio::test]
#[ignore = "requires Docker"]
async fn successful_spend_total_is_mode_scoped() {
    let (pool, _container) = setup_pg().await;
    seed_mode_ledger_trades(&pool).await;
    let trade_repo = PgTradeRepository::new(pool.connection().clone());

    // Spend aggregates must never mix modes: paper = 40+1, live = 70+2.
    assert_eq!(
        spend_total(&trade_repo, ExecutionMode::Paper).await,
        Usd::from(Decimal::new(41, 0))
    );
    assert_eq!(
        spend_total(&trade_repo, ExecutionMode::Live).await,
        Usd::from(Decimal::new(72, 0))
    );
    assert_eq!(
        spend_total(&trade_repo, ExecutionMode::DryRun).await,
        Usd::ZERO
    );
}

#[tokio::test]
#[ignore = "requires Docker"]
async fn position_ledger_queries_are_mode_scoped() {
    let (pool, _container) = setup_pg().await;
    let (paper_trade, live_trade) = seed_mode_ledger_trades(&pool).await;
    let position_repo = PgPositionRepository::new(pool.connection().clone());

    let paper_position_id = seed_open_position(
        &position_repo,
        paper_trade,
        "555001",
        ExecutionMode::Paper,
        Decimal::new(40, 0),
        Decimal::ONE,
    )
    .await;
    seed_open_position(
        &position_repo,
        live_trade,
        "555002",
        ExecutionMode::Live,
        Decimal::new(70, 0),
        Decimal::new(2, 0),
    )
    .await;

    // Open positions are mode-contextual: one per mode here.
    assert_eq!(open_count(&position_repo, ExecutionMode::Paper).await, 1);
    assert_eq!(open_count(&position_repo, ExecutionMode::Live).await, 1);
    assert_eq!(open_count(&position_repo, ExecutionMode::DryRun).await, 0);

    // Settle only the paper position with a $100 payout.
    position_repo
        .settle_position(
            &paper_position_id,
            SettlePositionParams {
                winning_token_id: TokenId::new("555001"),
                settlement_payout_usd: Usd::from(Decimal::new(100, 0)),
                realized_pnl: Decimal::new(59, 0),
                redeem_tx_hash: None,
                redeem_status: RedeemStatus::NotRequired,
                settlement_trigger: SettlementTrigger::Manual,
                oracle_verdict: None,
            },
        )
        .await
        .expect("settle paper position");

    // Payout aggregates stay isolated per mode as well.
    assert_eq!(
        payout_total(&position_repo, ExecutionMode::Paper).await,
        Usd::from(Decimal::new(100, 0))
    );
    assert_eq!(
        payout_total(&position_repo, ExecutionMode::Live).await,
        Usd::ZERO
    );
    assert_eq!(open_count(&position_repo, ExecutionMode::Paper).await, 0);
}

#[tokio::test]
#[ignore = "requires Docker"]
async fn calibration_repository_crud() {
    let (pool, _container) = setup_pg().await;
    seed_market(&pool, "evt-cal", "0xcal-mkt", MarketCategory::Sports).await;

    let cal_repo = PgCalibrationRepository::new(pool.connection().clone());
    let bucket = UpsertCalibration {
        category: MarketCategory::Sports,
        price_zone: PriceZone::Z99,
        duration_bucket: DurationBucket::Short,
        total_count: 10,
        correct_count: 9,
        alpha_prior: Probability::from(Decimal::new(10, 1)),
        beta_prior: Probability::from(Decimal::new(10, 1)),
        posterior_mean: Some(Probability::from(Decimal::new(9, 1))),
    };

    let inserted = cal_repo.upsert(bucket).await.unwrap();
    assert_eq!(inserted.total_count, 10);

    let found = cal_repo
        .get_bucket(
            MarketCategory::Sports,
            PriceZone::Z99,
            DurationBucket::Short,
        )
        .await
        .unwrap();
    assert!(found.is_some());

    let outcome = NewCalibrationOutcome {
        trade_id: TradeId::from_v7(),
        opportunity_id: OpportunityId::from_v7(),
        market_id: MarketId::new("0xcal-mkt"),
        category: MarketCategory::Sports,
        price_zone: PriceZone::Z99,
        duration_bucket: DurationBucket::Short,
        predicted_yes: true,
        actual_yes: None,
        entry_price: Price::from(Decimal::new(99, 2)),
        confidence_at_entry: Probability::from(Decimal::new(95, 2)),
        convergence_secs: 3600,
        resolved_at: None,
    };
    cal_repo.create_outcome(outcome).await.unwrap();

    let unresolved = cal_repo.get_unresolved_outcomes().await.unwrap();
    assert_eq!(unresolved.len(), 1);
    let outcome_id = unresolved[0].id;

    cal_repo.resolve_outcome(outcome_id, true).await.unwrap();
    let resolved = cal_repo.get_unresolved_outcomes().await.unwrap();
    assert!(resolved.is_empty());
}

#[tokio::test]
#[ignore = "requires Docker"]
async fn risk_state_repository_crud() {
    let (pool, _container) = setup_pg().await;
    let repo = PgRiskStateRepository::new(pool.connection().clone());

    let state = repo.load().await.expect("seeded singleton row");
    assert_eq!(state.id, 1);

    let upsert = UpsertRiskEngineState {
        id: state.id,
        breaker_state: BreakerStateName::Open,
        breaker_level: state.breaker_level,
        is_halted: state.is_halted,
        halt_reason: state.halt_reason,
        consecutive_misses: 2,
        cooldown_until: state.cooldown_until,
        cooldown_multiplier: state.cooldown_multiplier,
        total_exposure: state.total_exposure,
        hourly_loss_usd: state.hourly_loss_usd,
        hourly_fee_usd: state.hourly_fee_usd,
        hourly_trade_count: state.hourly_trade_count,
        hourly_success_count: state.hourly_success_count,
        hourly_miss_count: state.hourly_miss_count,
        hourly_window_start: state.hourly_window_start,
        daily_loss_usd: state.daily_loss_usd,
        daily_fee_usd: state.daily_fee_usd,
        daily_pnl: state.daily_pnl,
        daily_budget_spent: state.daily_budget_spent,
        daily_trade_count: state.daily_trade_count,
        daily_success_count: state.daily_success_count,
        daily_miss_count: state.daily_miss_count,
        daily_window_start: state.daily_window_start,
        weekly_loss_usd: state.weekly_loss_usd,
        weekly_trade_count: state.weekly_trade_count,
        weekly_window_start: state.weekly_window_start,
        hwm_equity: state.hwm_equity,
        total_realized_pnl: state.total_realized_pnl,
        last_emergency_at: state.last_emergency_at,
        last_emergency_reason: state.last_emergency_reason,
    };
    repo.upsert(upsert).await.unwrap();

    let reloaded = repo.load().await.unwrap();
    assert_eq!(reloaded.consecutive_misses, 2);
    assert_eq!(reloaded.breaker_state, BreakerStateName::Open);

    repo.reset_hourly_window().await.unwrap();
    repo.reset_daily_window().await.unwrap();
    repo.reset_weekly_window().await.unwrap();

    let after_reset = repo.load().await.unwrap();
    assert_eq!(after_reset.hourly_loss_usd, Usd::ZERO);
    assert_eq!(after_reset.daily_loss_usd, Usd::ZERO);
    assert_eq!(after_reset.weekly_loss_usd, Usd::ZERO);
}

#[tokio::test]
#[ignore = "requires Docker"]
async fn accounting_repository_crud() {
    let (pool, _container) = setup_pg().await;
    let repo = PgAccountingRepository::new(pool.connection().clone());
    let today = Utc::now().date_naive();
    let period_id = PeriodId::from_v7();

    let period = NewAccountingPeriod {
        period_id: period_id.clone(),
        period_type: ReportType::Daily,
        start_date: today,
        end_date: today,
    };

    let created = repo.create(period).await.unwrap();
    assert_eq!(created.period_id, period_id);
    assert!(!created.finalized);

    let current = repo.get_current_daily().await.unwrap();
    assert!(current.is_some());

    repo.finalize_period(&period_id).await.unwrap();
    let history = repo.get_history("daily", 5).await.unwrap();
    assert!(
        history
            .iter()
            .any(|p| p.period_id == period_id && p.finalized)
    );
}

#[tokio::test]
#[ignore = "requires Docker"]
async fn runtime_config_version_repository_records_activation_history() {
    let (pool, _container) = setup_pg().await;
    let repo = PgRuntimeConfigVersionRepository::new(pool.connection().clone());

    let version_id = RuntimeConfigVersionId::from_v7();
    let version = repo
        .create_version(NewRuntimeConfigVersion {
            runtime_config_version_id: version_id.clone(),
            config_hash: "hash:repo-test".into(),
            schema_version: 1,
            config_json: serde_json::json!({ "schema_version": 1 }),
            source: RuntimeConfigVersionSource::Operator,
            created_by: "test".into(),
            reason: "repository test".into(),
        })
        .await
        .unwrap();
    assert_eq!(version.runtime_config_version_id, version_id);
    let by_hash = repo
        .load_by_hash("hash:repo-test")
        .await
        .unwrap()
        .expect("version by hash");
    assert_eq!(by_hash.runtime_config_version_id, version_id);

    let activated_at = Utc::now();
    let activation = repo
        .activate_version(NewRuntimeConfigActivation {
            runtime_config_activation_id: RuntimeConfigActivationId::from_v7(),
            runtime_config_version_id: version_id.clone(),
            activated_at,
            activated_by: "test".into(),
            reason: "activate test version".into(),
            activation_kind: RuntimeConfigActivationKind::Promote,
            previous_runtime_config_version_id: None,
            rollback_target_version_id: None,
            audit_event_id: None,
        })
        .await
        .unwrap();
    assert_eq!(activation.runtime_config_version_id, version_id);

    let current = repo.load_current().await.unwrap().expect("current version");
    assert_eq!(current.runtime_config_version_id, version_id);

    let active_at = repo
        .load_active_at(activated_at + chrono::Duration::seconds(1))
        .await
        .unwrap()
        .expect("active version at timestamp");
    assert_eq!(active_at.runtime_config_version_id, version_id);

    assert_eq!(repo.list_activations(10).await.unwrap().len(), 1);
}

fn materialization_run(dedupe_key: Option<&str>) -> NewControlFactorMaterializationRun {
    let now = Utc::now();
    NewControlFactorMaterializationRun {
        materialization_run_id: MaterializationRunId::from_v7(),
        run_dedupe_key: dedupe_key.map(str::to_owned),
        run_kind: MaterializationRunKind::Scheduled,
        trigger_type: RunTriggerType::Scheduled,
        trigger_ref: Some("test-schedule".into()),
        status: MaterializationRunStatus::Queued,
        window_from: now - chrono::Duration::hours(1),
        window_to: now,
        source_delay_secs: 900,
        market_filter: serde_json::json!({ "market_ids": [] }),
        requested_factor_types: serde_json::json!(["bucket_risk"]),
        data_requirements: serde_json::json!({ "required_inputs": ["runtime_config"] }),
        runtime_config_ref: serde_json::json!({ "mode": "active_at", "at": now }),
        simulation_config_hash: "blake3:sim".into(),
        quality_gate_policy_hash: "blake3:gate".into(),
        output_policy: MaterializationOutputPolicy::NoFactorOutput,
        manifest: serde_json::json!({ "run": "test" }),
        manifest_hash: "blake3:manifest".into(),
        report: serde_json::json!({}),
        code_git_sha: "abc".into(),
        created_by: "test".into(),
        started_at: None,
        finished_at: None,
        failure_code: None,
        failure_detail: None,
        report_uri: None,
    }
}

fn stage_report(
    run_id: &MaterializationRunId,
    status: EvidenceStageStatus,
) -> NewControlFactorStageReport {
    NewControlFactorStageReport {
        stage_report_id: StageReportId::from_v7(),
        materialization_run_id: run_id.clone(),
        stage_name: MaterializationStageName::ResolveInputs,
        status,
        started_at: Utc::now(),
        finished_at: Some(Utc::now()),
        input_artifact_hashes: serde_json::json!([]),
        output_artifact_hash: Some("blake3:artifact".into()),
        coverage: serde_json::json!({ "coverage_ratio": "1" }),
        metrics: serde_json::json!({ "records": 1 }),
        records_read: 1,
        records_written: 0,
        warnings: serde_json::json!([]),
        errors: serde_json::json!([]),
        query_fingerprints: serde_json::json!(["runtime_config.load_active_at:v1"]),
    }
}

async fn assert_failed_run_can_retry(
    repo: &PgControlFactorRepository,
    run_id: &MaterializationRunId,
    previous_stage_report_id: &StageReportId,
) {
    let failed = repo
        .transition_materialization_run(
            run_id,
            MaterializationRunStatus::Running,
            MaterializationRunStatus::Failed,
            MaterializationRunStatusPatch {
                finished_at: Some(Utc::now()),
                failure_code: Some("run.invalid_transition".into()),
                failure_detail: Some("forced failure for retry test".into()),
                report: None,
                report_uri: None,
            },
        )
        .await
        .expect("fail run");
    assert!(matches!(failed, RunTransitionOutcome::Transitioned(_)));
    let retried = repo
        .retry_materialization_run(run_id)
        .await
        .expect("retry run");
    assert!(matches!(retried, RunTransitionOutcome::Transitioned(_)));
    let reacquired = repo
        .try_acquire_materialization_run(run_id, Utc::now())
        .await
        .expect("reacquire run");
    assert!(matches!(
        reacquired,
        AcquireMaterializationRunOutcome::Acquired(_)
    ));
    let retried_stage = repo
        .upsert_stage_report(stage_report(run_id, EvidenceStageStatus::Completed))
        .await
        .expect("upsert retried stage");
    assert_eq!(&retried_stage.stage_report_id, previous_stage_report_id);
}

#[tokio::test]
#[ignore = "requires Docker"]
async fn control_factor_materialization_run_lifecycle_is_idempotent() {
    let (pool, _container) = setup_pg().await;
    let repo = PgControlFactorRepository::new(pool.connection().clone());
    let created = match repo
        .enqueue_materialization_run(
            materialization_run(Some("dedupe:test")),
            EnqueueMaterializationRunOptions {
                force_new_run: false,
                reason: None,
            },
        )
        .await
        .expect("enqueue run")
    {
        EnqueueMaterializationRunOutcome::Created(run) => run,
        other => panic!("expected created, got {other:?}"),
    };
    match repo
        .enqueue_materialization_run(
            materialization_run(Some("dedupe:test")),
            EnqueueMaterializationRunOptions {
                force_new_run: false,
                reason: None,
            },
        )
        .await
        .expect("dedupe run")
    {
        EnqueueMaterializationRunOutcome::DuplicateActive(run) => {
            assert_eq!(run.materialization_run_id, created.materialization_run_id);
        }
        other => panic!("expected duplicate active, got {other:?}"),
    }
    let acquired = repo
        .try_acquire_materialization_run(&created.materialization_run_id, Utc::now())
        .await
        .expect("acquire run");
    assert!(matches!(
        acquired,
        AcquireMaterializationRunOutcome::Acquired(_)
    ));
    let first = repo
        .upsert_stage_report(stage_report(
            &created.materialization_run_id,
            EvidenceStageStatus::Completed,
        ))
        .await
        .expect("insert stage");
    let second = repo
        .upsert_stage_report(stage_report(
            &created.materialization_run_id,
            EvidenceStageStatus::CompletedWithWarnings,
        ))
        .await
        .expect("upsert stage");
    assert_eq!(first.stage_report_id, second.stage_report_id);
    let reports = repo
        .list_stage_reports(&created.materialization_run_id)
        .await
        .expect("list stages");
    assert_eq!(reports.len(), 1);
    assert_failed_run_can_retry(
        &repo,
        &created.materialization_run_id,
        &second.stage_report_id,
    )
    .await;
    let cancelled = repo
        .cancel_materialization_run(
            &created.materialization_run_id,
            "operator cancelled test",
            Utc::now(),
        )
        .await
        .expect("cancel run");
    assert!(matches!(
        cancelled,
        CancelMaterializationRunOutcome::Cancelled(_)
    ));
}

#[tokio::test]
#[ignore = "requires Docker"]
async fn fact_data_repository_records_balance_snapshots() {
    let (pool, _container) = setup_pg().await;
    seed_market(&pool, "evt-facts", "0xfact-mkt", MarketCategory::Politics).await;
    let repo = PgFactDataRepository::new(pool.connection().clone());
    let observed_at = Utc::now();

    let balance = repo
        .create_balance_snapshot(NewBalanceSnapshot {
            balance_snapshot_id: BalanceSnapshotId::from_v7(),
            holder_address: "0xholder".into(),
            internal_available_usd: Usd::new(dec!(900)),
            internal_reserved_usd: Usd::new(dec!(100)),
            external_available_usd: Usd::new(dec!(995)),
            external_locked_usd: Usd::ZERO,
            drift_usd: Usd::new(dec!(5)),
            source: BalanceSnapshotSource::ClobApi,
            block_number: None,
            reconciliation_report_id: None,
            observed_at,
        })
        .await
        .unwrap();
    assert_eq!(balance.drift_usd, Usd::new(dec!(5)));

    let latest = repo
        .latest_balance_before("0xholder", Utc::now())
        .await
        .unwrap()
        .expect("balance snapshot");
    assert_eq!(latest.drift_usd, Usd::new(dec!(5)));
}

#[tokio::test]
#[ignore = "requires Docker"]
async fn potential_loss_repository_crud() {
    let (pool, _container) = setup_pg().await;
    seed_market(&pool, "evt-pll", "0xpll-mkt", MarketCategory::Tech).await;

    let repo = PgPotentialLossRepository::new(pool.connection().clone());
    let ledger_id = LedgerId::from_v7();

    let entry = NewPotentialLoss {
        ledger_id: ledger_id.clone(),
        market_id: MarketId::new("0xpll-mkt"),
        token_id: TokenId::new("555"),
        shares: Shares::from(Decimal::new(20, 0)),
        entry_price: Price::from(Decimal::new(90, 2)),
        max_loss_usd: Usd::from(Decimal::new(18, 0)),
    };

    repo.create(entry).await.unwrap();
    assert_eq!(repo.find_active().await.unwrap().len(), 1);
    assert_eq!(
        repo.total_active_loss().await.unwrap(),
        Usd::from(Decimal::new(18, 0))
    );

    repo.resolve(
        &ledger_id,
        ResolvePotentialLoss {
            resolved_at: Utc::now(),
        },
    )
    .await
    .unwrap();
    assert!(repo.find_active().await.unwrap().is_empty());
}

#[tokio::test]
#[ignore = "requires Docker"]
async fn blacklist_persistence_repository_crud() {
    let (pool, _container) = setup_pg().await;
    seed_market(&pool, "evt-bl", "0xbl-mkt", MarketCategory::Politics).await;

    let repo = PgBlacklistPersistenceRepository::new(pool.connection().clone());
    repo.upsert(UpsertBlacklistEntry {
        market_id: MarketId::new("0xbl-mkt"),
        token_id: None,
        scope: BlacklistScope::TradingPath,
        reason: BlacklistReason::Manual,
        expires_at: None,
        miss_count: 0,
    })
    .await
    .unwrap();

    let active = repo.load_active().await.unwrap();
    assert_eq!(active.len(), 1);

    repo.remove(&MarketId::new("0xbl-mkt")).await.unwrap();
    assert!(repo.load_active().await.unwrap().is_empty());
}

#[tokio::test]
#[ignore = "requires Docker"]
async fn emergency_repository_create() {
    let (pool, _container) = setup_pg().await;
    let repo = PgEmergencyRepository::new(pool.connection().clone());

    repo.create(NewEmergencySnapshot {
        trigger_level: CircuitBreakerLevel::System,
        reason: "integration test".into(),
        risk_state: serde_json::json!({}),
        open_positions_count: 0,
        open_reservations_count: 0,
        triggered_at: Utc::now(),
    })
    .await
    .unwrap();
}

#[tokio::test]
#[ignore = "requires Docker"]
async fn reconciliation_repository_create() {
    let (pool, _container) = setup_pg().await;
    let repo = PgReconciliationRepository::new(pool.connection().clone());

    repo.create(NewReconciliationReport {
        status: ReconciliationStatus::Ok,
        mismatches: serde_json::json!([]),
        internal_balance: Usd::ZERO,
        external_balance: Usd::ZERO,
        internal_exposure: Usd::ZERO,
        external_exposure: Usd::ZERO,
        reserved: Usd::ZERO,
        tolerance: Usd::new(dec!(1)),
        checked_at: Utc::now(),
        duration_ms: 5,
    })
    .await
    .unwrap();
}

#[tokio::test]
#[ignore = "requires Docker"]
async fn report_repository_daily_upsert() {
    let (pool, _container) = setup_pg().await;
    let repo = PgReportRepository::new(pool.connection().clone());
    let date = NaiveDate::from_ymd_opt(2026, 1, 15).unwrap();

    repo.save_daily(date, serde_json::json!({"trades": 1}))
        .await
        .unwrap();

    let found = repo
        .find_latest(ReportType::Daily)
        .await
        .unwrap()
        .expect("daily report");
    assert_eq!(found.report_type, ReportType::Daily);
}

#[tokio::test]
#[ignore = "requires Docker"]
async fn risk_audit_repository_create_batch() {
    let (pool, _container) = setup_pg().await;
    let repo = PgRiskAuditRepository::new(pool.connection().clone());

    repo.create_batch(vec![NewRiskAuditEvent {
        event_type: RiskAuditEventType::EngineHalted,
        market_id: None,
        opportunity_id: None,
        trade_id: None,
        rejection_reason: Some("test halt".to_owned()),
        payload: serde_json::json!({"reason": "test"}),
    }])
    .await
    .unwrap();
}

// ── Control-factor governance integration ──────────────────────────────────

async fn seed_control_run(repo: &PgControlFactorRepository) -> MaterializationRunId {
    let created = match repo
        .enqueue_materialization_run(
            materialization_run(None),
            EnqueueMaterializationRunOptions {
                force_new_run: false,
                reason: None,
            },
        )
        .await
        .expect("enqueue run")
    {
        EnqueueMaterializationRunOutcome::Created(run) => run,
        other => panic!("expected created, got {other:?}"),
    };
    created.materialization_run_id
}

fn candidate_factor(run_id: &MaterializationRunId, size_multiplier: Decimal) -> ControlFactorValue {
    let now = Utc::now();
    ControlFactorValue {
        factor_id: ControlFactorId::from_v7(),
        factor_type: ControlFactorType::BucketRisk,
        dimensions: FactorDimensions::BucketRisk(BucketRiskDimensions {
            category: MarketCategory::Politics,
            price_zone: PriceZone::Z99,
            duration_bucket: DurationBucket::Short,
            hours_to_settlement_bucket: None,
            neg_risk: Some(false),
            fee_profile: None,
        }),
        payload: FactorPayload::BucketRisk(BucketRiskPayload {
            resolution_haircut_factor: dec!(0.9),
            size_multiplier,
            min_edge_bps_addon: dec!(0),
            block_new_entries: false,
        }),
        evidence: FactorEvidence {
            materialization_run_id: run_id.clone(),
            stage_report_ids: vec![StageReportId::from_v7()],
            window_from: now - chrono::Duration::hours(1),
            window_to: now,
            source_delay_secs: 30,
            market_count: 1,
            event_count: 1,
            opportunity_count: 1,
            settlement_count: 0,
            sample_count: 10,
            data_coverage: DataCoverageReport {
                expected_rows: 10,
                observed_rows: 10,
                missing_rows: 0,
                coverage_ratio: Decimal::ONE,
                insufficient_reasons: Vec::new(),
            },
            point_in_time_inputs: PointInTimeInputManifest {
                inputs: Vec::new(),
                production_eligible: true,
                missing_inputs: Vec::new(),
                fatal_errors: Vec::new(),
                warnings: Vec::new(),
                manifest_hash: "blake3:pit".into(),
            },
            baseline_config_hash: "blake3:cfg".into(),
            code_git_sha: "abc".into(),
            dataset_hash: "blake3:dataset".into(),
            feature_schema_hash: "blake3:features".into(),
            label_schema_hash: "blake3:labels".into(),
            query_fingerprint: "fp".into(),
            confidence_interval: ConfidenceInterval {
                lower: dec!(0),
                point_estimate: dec!(0),
                upper: dec!(0),
                confidence_level: dec!(0.95),
            },
            tail_risk: TailRiskEvidence {
                p95_loss: dec!(0),
                p99_loss: dec!(0),
                max_loss: dec!(0),
                expected_shortfall: dec!(0),
            },
            maturity: FactorMaturity::StatisticallyMaterialized,
            source_refs: Vec::new(),
            warnings: Vec::new(),
        },
        status: FactorStatus::Candidate,
        generated_at: now,
        expires_at: now + chrono::Duration::days(1),
        owner: "materializer".into(),
        schema_version: 1,
    }
}

fn factor_audit(
    event_type: ControlAuditEventType,
    request_id: &str,
    factor_id: &ControlFactorId,
) -> NewControlFactorAuditEvent {
    NewControlFactorAuditEvent {
        event_type,
        actor: "materializer".into(),
        actor_role: "operator".into(),
        resource_type: AuditResourceType::Factor,
        resource_id: factor_id.to_string(),
        request_id: request_id.into(),
        reason: "integration test".into(),
        before_hash: None,
        after_hash: None,
        diff: serde_json::json!({}),
    }
}

fn publication_audit(
    request_id: &str,
    publication_id: &FactorPublicationId,
) -> NewControlFactorAuditEvent {
    NewControlFactorAuditEvent {
        event_type: ControlAuditEventType::PublicationCreated,
        actor: "risk_owner_1".into(),
        actor_role: "risk_owner".into(),
        resource_type: AuditResourceType::Publication,
        resource_id: publication_id.to_string(),
        request_id: request_id.into(),
        reason: "integration governance test".into(),
        before_hash: None,
        after_hash: None,
        diff: serde_json::json!({}),
    }
}

fn shadow_publication(
    factor_id: &ControlFactorId,
    idempotency_key: &str,
) -> NewControlFactorPublication {
    let now = Utc::now();
    NewControlFactorPublication {
        publication_id: FactorPublicationId::from_v7(),
        mode: PublicationMode::Shadow,
        factor_ids: vec![factor_id.clone()],
        previous_publication_id: None,
        status: PublicationStatus::Pending,
        effective_from: now,
        expires_at: now + chrono::Duration::days(1),
        approved_by: Some("risk_owner_1".into()),
        approval_reason: "shadow window".into(),
        idempotency_key: idempotency_key.into(),
        publication_hash: "blake3:test".into(),
    }
}

#[tokio::test]
#[ignore = "requires Docker"]
async fn control_factor_publish_is_idempotent_one_active_and_chained() {
    let (pool, _container) = setup_pg().await;
    let repo = PgControlFactorRepository::new(pool.connection().clone());
    let run_id = seed_control_run(&repo).await;

    let factor = candidate_factor(&run_id, dec!(0.5));
    repo.create_factor(
        NewControlFactorValue::from_typed(&factor, None).expect("typed factor"),
        factor_audit(
            ControlAuditEventType::FactorCreated,
            "req-factor",
            &factor.factor_id,
        ),
    )
    .await
    .expect("create factor");

    let publication = shadow_publication(&factor.factor_id, "idem-shadow-1");
    let first_id = publication.publication_id.clone();
    let outcome = repo
        .publish_publication(publication, publication_audit("req-pub-1", &first_id))
        .await
        .expect("publish shadow");
    assert!(matches!(outcome, PublishPublicationOutcome::Published(_)));

    // Retry with the same idempotency key returns the existing publication.
    let retry = shadow_publication(&factor.factor_id, "idem-shadow-1");
    let retry_outcome = repo
        .publish_publication(retry, publication_audit("req-pub-2", &first_id))
        .await
        .expect("idempotent retry");
    match retry_outcome {
        PublishPublicationOutcome::AlreadyApplied(info) => {
            assert_eq!(info.publication_id, first_id);
        }
        PublishPublicationOutcome::Published(_) => {
            panic!("expected idempotent replay, got a new publication")
        }
    }

    // Exactly one active Shadow publication, and the member factor is Shadow.
    let active = repo
        .load_active_publication(PublicationMode::Shadow)
        .await
        .expect("load active")
        .expect("active publication");
    assert_eq!(active.publication_id, first_id);
    assert_eq!(active.status, PublicationStatus::Active);
    let stored_factor = repo
        .load_factor(&factor.factor_id)
        .await
        .expect("load factor")
        .expect("factor row");
    assert_eq!(stored_factor.status, FactorStatus::Shadow);

    // The audit chain is contiguous and verifiable; the retry wrote no new event.
    let chain = repo.load_audit_chain(1, 1000).await.expect("audit chain");
    assert_eq!(chain.len(), 2, "factor_created + publication_created");
    assert_eq!(chain[0].sequence, 1);
    assert_eq!(chain[1].sequence, 2);
    AuditChain::verify(&chain).expect("chain verifies");
}

#[tokio::test]
#[ignore = "requires Docker"]
async fn control_factor_reject_and_expire_write_chained_audit() {
    let (pool, _container) = setup_pg().await;
    let repo = PgControlFactorRepository::new(pool.connection().clone());
    let run_id = seed_control_run(&repo).await;

    let factor = candidate_factor(&run_id, dec!(0.5));
    repo.create_factor(
        NewControlFactorValue::from_typed(&factor, None).expect("typed factor"),
        factor_audit(
            ControlAuditEventType::FactorCreated,
            "req-factor",
            &factor.factor_id,
        ),
    )
    .await
    .expect("create factor");

    let rejected = repo
        .reject_factor(
            &factor.factor_id,
            "sample below minimum",
            factor_audit(
                ControlAuditEventType::FactorRejected,
                "req-reject",
                &factor.factor_id,
            ),
        )
        .await
        .expect("reject factor")
        .expect("factor exists");
    assert_eq!(rejected.value.status, FactorStatus::Rejected);
    assert_eq!(
        rejected.value.status_reason.as_deref(),
        Some("sample below minimum")
    );

    let chain = repo.load_audit_chain(1, 1000).await.expect("audit chain");
    assert_eq!(chain.len(), 2);
    AuditChain::verify(&chain).expect("chain verifies");
    assert_eq!(chain[1].event_type, ControlAuditEventType::FactorRejected);
}

#[tokio::test]
#[ignore = "requires Docker"]
async fn control_factor_rollback_restores_previous_publication() {
    let (pool, _container) = setup_pg().await;
    let repo = PgControlFactorRepository::new(pool.connection().clone());
    let run_id = seed_control_run(&repo).await;

    // Two distinct factors so we can stage two successive Published publications.
    // Different payloads keep their (run, type, dimensions, payload) dedup keys
    // distinct under `uniq_control_factor_value_run_payload`.
    let factor_genesis = candidate_factor(&run_id, dec!(0.5));
    let factor_successor = candidate_factor(&run_id, dec!(0.6));
    for factor in [&factor_genesis, &factor_successor] {
        repo.create_factor(
            NewControlFactorValue::from_typed(factor, None).expect("typed factor"),
            factor_audit(
                ControlAuditEventType::FactorCreated,
                &factor.factor_id.to_string(),
                &factor.factor_id,
            ),
        )
        .await
        .expect("create factor");
    }

    // Promote both to Shadow.
    for (factor, key) in [
        (&factor_genesis, "shadow-genesis"),
        (&factor_successor, "shadow-successor"),
    ] {
        let publication = shadow_publication(&factor.factor_id, key);
        let id = publication.publication_id.clone();
        repo.publish_publication(publication, publication_audit(key, &id))
            .await
            .expect("shadow publish");
    }

    // Publish the genesis Published publication.
    let mut genesis = shadow_publication(&factor_genesis.factor_id, "publish-genesis");
    genesis.mode = PublicationMode::Published;
    let genesis_id = genesis.publication_id.clone();
    repo.publish_publication(genesis, publication_audit("publish-genesis", &genesis_id))
        .await
        .expect("publish genesis");

    // Publish the successor, superseding genesis and recording it as rollback target.
    let mut successor = shadow_publication(&factor_successor.factor_id, "publish-successor");
    successor.mode = PublicationMode::Published;
    successor.previous_publication_id = Some(genesis_id.clone());
    let successor_id = successor.publication_id.clone();
    repo.publish_publication(
        successor,
        publication_audit("publish-successor", &successor_id),
    )
    .await
    .expect("publish successor");

    // Roll back to the genesis publication.
    repo.rollback_publication(
        &successor_id,
        &genesis_id,
        publication_audit("rollback", &successor_id),
    )
    .await
    .expect("rollback");

    let active = repo
        .load_active_publication(PublicationMode::Published)
        .await
        .expect("load active")
        .expect("active publication");
    assert_eq!(active.publication_id, genesis_id);

    let chain = repo.load_audit_chain(1, 1000).await.expect("audit chain");
    AuditChain::verify(&chain).expect("chain verifies");
}

#[tokio::test]
#[ignore = "requires Docker"]
async fn runtime_config_governed_activation_links_audit_event() {
    let (pool, _container) = setup_pg().await;
    let repo = PgRuntimeConfigVersionRepository::new(pool.connection().clone());

    let version_id = RuntimeConfigVersionId::from_v7();
    let version = NewRuntimeConfigVersion {
        runtime_config_version_id: version_id.clone(),
        config_hash: "blake3:rcv-1".into(),
        schema_version: 1,
        config_json: serde_json::json!({ "mode": "live" }),
        source: RuntimeConfigVersionSource::Operator,
        created_by: "admin_1".into(),
        reason: "initial".into(),
    };
    let version_audit = NewControlFactorAuditEvent {
        event_type: ControlAuditEventType::RuntimeConfigVersionCreated,
        actor: "admin_1".into(),
        actor_role: "admin".into(),
        resource_type: AuditResourceType::RuntimeConfigVersion,
        resource_id: version_id.to_string(),
        request_id: "req-rcv-create".into(),
        reason: "initial".into(),
        before_hash: None,
        after_hash: None,
        diff: serde_json::json!({}),
    };
    repo.create_version_governed(version, version_audit)
        .await
        .expect("create version governed");

    let activation = NewRuntimeConfigActivation {
        runtime_config_activation_id: RuntimeConfigActivationId::from_v7(),
        runtime_config_version_id: version_id.clone(),
        activated_at: Utc::now(),
        activated_by: "admin_1".into(),
        reason: "activate".into(),
        activation_kind: RuntimeConfigActivationKind::Initial,
        previous_runtime_config_version_id: None,
        rollback_target_version_id: None,
        audit_event_id: None,
    };
    let activation_audit = NewControlFactorAuditEvent {
        event_type: ControlAuditEventType::RuntimeConfigActivated,
        actor: "admin_1".into(),
        actor_role: "admin".into(),
        resource_type: AuditResourceType::RuntimeConfigVersion,
        resource_id: version_id.to_string(),
        request_id: "req-rcv-activate".into(),
        reason: "activate".into(),
        before_hash: None,
        after_hash: None,
        diff: serde_json::json!({}),
    };
    let activation_outcome = repo
        .activate_version_governed(activation, activation_audit)
        .await
        .expect("activate version governed");

    // The activation row links to the chained audit event.
    assert_eq!(
        activation_outcome.value.audit_event_id.as_ref(),
        Some(&activation_outcome.audit_event_id),
    );

    let control_repo = PgControlFactorRepository::new(pool.connection().clone());
    let chain = control_repo
        .load_audit_chain(1, 1000)
        .await
        .expect("audit chain");
    assert_eq!(chain.len(), 2);
    AuditChain::verify(&chain).expect("chain verifies");
    let linked = activation_outcome.audit_event_id;
    assert!(chain.iter().any(|event| event.event_id == linked));
}

#[tokio::test]
#[ignore = "requires Docker"]
async fn latest_run_for_schedule_filters_by_trigger_ref_and_status() {
    let (pool, _container) = setup_pg().await;
    let repo = PgControlFactorRepository::new(pool.connection().clone());

    // First run: drive it to Completed (Queued -> Running -> Completed).
    let first = match repo
        .enqueue_materialization_run(
            materialization_run(None),
            EnqueueMaterializationRunOptions {
                force_new_run: false,
                reason: None,
            },
        )
        .await
        .expect("enqueue first")
    {
        EnqueueMaterializationRunOutcome::Created(run) => run,
        other => panic!("expected created, got {other:?}"),
    };
    let acquired = repo
        .try_acquire_materialization_run(&first.materialization_run_id, Utc::now())
        .await
        .expect("acquire first");
    assert!(matches!(
        acquired,
        AcquireMaterializationRunOutcome::Acquired(_)
    ));
    let completed = repo
        .transition_materialization_run(
            &first.materialization_run_id,
            MaterializationRunStatus::Running,
            MaterializationRunStatus::Completed,
            MaterializationRunStatusPatch {
                finished_at: Some(Utc::now()),
                failure_code: None,
                failure_detail: None,
                report: None,
                report_uri: None,
            },
        )
        .await
        .expect("complete first");
    assert!(matches!(completed, RunTransitionOutcome::Transitioned(_)));

    // Second run for the same schedule stays Queued and is the newest.
    let second = match repo
        .enqueue_materialization_run(
            materialization_run(None),
            EnqueueMaterializationRunOptions {
                force_new_run: false,
                reason: None,
            },
        )
        .await
        .expect("enqueue second")
    {
        EnqueueMaterializationRunOutcome::Created(run) => run,
        other => panic!("expected created, got {other:?}"),
    };

    let latest_any = repo
        .latest_run_for_schedule("test-schedule", &[])
        .await
        .expect("latest any")
        .expect("a run exists");
    assert_eq!(
        latest_any.materialization_run_id,
        second.materialization_run_id
    );

    let latest_completed = repo
        .latest_run_for_schedule("test-schedule", &[MaterializationRunStatus::Completed])
        .await
        .expect("latest completed")
        .expect("a completed run exists");
    assert_eq!(
        latest_completed.materialization_run_id,
        first.materialization_run_id
    );

    let unknown = repo
        .latest_run_for_schedule("no-such-schedule", &[])
        .await
        .expect("latest unknown");
    assert!(unknown.is_none());
}

fn shadow_decision(
    publication_id: &FactorPublicationId,
    market_id: &str,
    decision_type: ShadowDecisionType,
    decided_at: chrono::DateTime<Utc>,
) -> NewControlFactorShadowDecision {
    NewControlFactorShadowDecision {
        shadow_decision_id: ShadowDecisionId::from_v7(),
        publication_id: publication_id.clone(),
        opportunity_id: OpportunityId::from_v7(),
        event_id: EventId::new("evt-shadow"),
        market_id: MarketId::new(market_id),
        decision_type,
        baseline_decision: serde_json::json!({ "size": "0" }),
        shadow_decision: serde_json::json!({ "size": "1" }),
        delta: serde_json::json!({ "size_delta": "1" }),
        affected_factor_ids: serde_json::json!([]),
        decided_at,
    }
}

#[tokio::test]
#[ignore = "requires Docker"]
async fn aggregate_shadow_decisions_counts_by_type_and_distinct_markets() {
    let (pool, _container) = setup_pg().await;
    let control_repo = PgControlFactorRepository::new(pool.connection().clone());
    let run_id = seed_control_run(&control_repo).await;

    let factor = candidate_factor(&run_id, dec!(0.5));
    control_repo
        .create_factor(
            NewControlFactorValue::from_typed(&factor, None).expect("typed factor"),
            factor_audit(
                ControlAuditEventType::FactorCreated,
                "req-shadow-factor",
                &factor.factor_id,
            ),
        )
        .await
        .expect("create factor");
    let publication = shadow_publication(&factor.factor_id, "idem-shadow-agg");
    let publication_id = publication.publication_id.clone();
    control_repo
        .publish_publication(
            publication,
            publication_audit("req-shadow-pub", &publication_id),
        )
        .await
        .expect("publish shadow");

    let fact_repo = PgFactDataRepository::new(pool.connection().clone());
    let now = Utc::now();
    let in_window = [
        ("0xmkt-a", ShadowDecisionType::WouldReject),
        ("0xmkt-a", ShadowDecisionType::WouldReject),
        ("0xmkt-b", ShadowDecisionType::WouldSize),
        ("0xmkt-a", ShadowDecisionType::WouldScore),
        ("0xmkt-c", ShadowDecisionType::NoEffect),
    ];
    for (offset, (market, decision_type)) in in_window.iter().enumerate() {
        let decided_at = now - chrono::Duration::minutes(i64::try_from(offset).unwrap_or(0));
        fact_repo
            .append_shadow_decision(shadow_decision(
                &publication_id,
                market,
                *decision_type,
                decided_at,
            ))
            .await
            .expect("append in-window decision");
    }
    // Out-of-window decision must be excluded from the aggregate and list.
    fact_repo
        .append_shadow_decision(shadow_decision(
            &publication_id,
            "0xmkt-d",
            ShadowDecisionType::WouldReject,
            now - chrono::Duration::hours(2),
        ))
        .await
        .expect("append out-of-window decision");

    let from = now - chrono::Duration::hours(1);
    let to = now + chrono::Duration::hours(1);
    let aggregate = fact_repo
        .aggregate_shadow_decisions(&publication_id, from, to)
        .await
        .expect("aggregate shadow decisions");
    assert_eq!(
        aggregate,
        ShadowDecisionAggregate {
            publication_id: publication_id.clone(),
            total: 5,
            would_reject: 2,
            would_size: 1,
            would_score: 1,
            no_effect: 1,
            distinct_markets: 3,
        }
    );

    let listed = fact_repo
        .list_shadow_decisions(&publication_id, from, to, 100)
        .await
        .expect("list shadow decisions");
    assert_eq!(listed.len(), 5);
    // Ordered newest-first by decided_at.
    assert!(
        listed
            .windows(2)
            .all(|pair| pair[0].decided_at >= pair[1].decided_at)
    );
}
