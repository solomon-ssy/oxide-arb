//! `PostgreSQL` repository integration tests (requires Docker).

mod common;

use chrono::Utc;
use common::{make_event, make_market, setup_pg};
use oxide_arb_models::domain::{
    NewAccountingPeriod, NewCalibrationOutcome, NewLifecycleEvent, NewPosition, NewPotentialLoss,
    NewTrade, UpdatePotentialLoss, UpdateTradeOutcome, UpsertCalibration, UpsertRiskEngineState,
    UpsertRuntimeConfig,
};
use oxide_arb_models::enums::calibration::{DurationBucket, PriceZone};
use oxide_arb_models::enums::common::{
    ExecutionMode, LedgerStatus, MarketCategory, PositionStatus, ReportType, Side, TradeOutcome,
};
use oxide_arb_models::enums::lifecycle::{LifecyclePhase, LifecycleRecorder};
use oxide_arb_models::enums::risk::BreakerStateName;
use oxide_arb_models::enums::runtime_config::RuntimeConfigKey;
use oxide_arb_models::types::*;
use oxide_arb_repository::postgres::*;
use oxide_arb_repository::traits::*;
use oxide_arb_storage::postgres::PostgresPool;
use rust_decimal::Decimal;

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
    assert_eq!(all_active.len(), 1, "Update should not create duplicates");
}

#[tokio::test]
#[ignore = "requires Docker"]
async fn trade_repository_crud() {
    let (pool, _container) = setup_pg().await;
    seed_market(&pool, "evt-trade", "0xtrade-mkt", MarketCategory::Sports).await;

    let trade_repo = PgTradeRepository::new(pool.connection().clone());
    let execution_id = ExecutionId::generate();

    let created = trade_repo
        .create(NewTrade {
            execution_id: execution_id.clone(),
            opportunity_id: OpportunityId::new_v7(),
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
            execution_mode: ExecutionMode::DryRun,
        })
        .await
        .expect("create trade");
    assert_eq!(created.outcome, TradeOutcome::Pending);

    let updated = trade_repo
        .update(
            &created.trade_id,
            UpdateTradeOutcome {
                outcome: TradeOutcome::Success,
                order_id: Some(OrderId::new("order-123")),
                tx_hash: Some("0xdead".into()),
                net_profit_usd: Some(Usd::from(Decimal::new(4, 0))),
                latency_ms: Some(42),
                error_message: None,
                confirmed_at: Some(Utc::now()),
            },
        )
        .await
        .expect("update outcome");
    assert_eq!(updated.outcome, TradeOutcome::Success);

    let by_exec = trade_repo
        .find_by_execution(execution_id.as_str())
        .await
        .unwrap();
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
    assert!(counts.get("success").copied().unwrap_or(0) >= 1);
}

#[tokio::test]
#[ignore = "requires Docker"]
async fn trade_repository_batch_create() {
    let (pool, _container) = setup_pg().await;
    seed_market(&pool, "evt-batch", "0xbatch-mkt", MarketCategory::Finance).await;

    let trade_repo = PgTradeRepository::new(pool.connection().clone());
    let trades: Vec<NewTrade> = (0..3)
        .map(|i| NewTrade {
            execution_id: ExecutionId::generate(),
            opportunity_id: OpportunityId::new_v7(),
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

    let position_repo = PgPositionRepository::new(pool.connection().clone());
    let opened = position_repo
        .create(NewPosition {
            market_id: MarketId::new("0xpos-market"),
            token_id: TokenId::new("111"),
            side: Side::Buy,
            shares: Shares::from(Decimal::new(100, 0)),
            avg_entry_price: Price::from(Decimal::new(95, 2)),
            total_cost_usd: Usd::from(Decimal::new(95, 0)),
            total_fees_usd: Usd::from(Decimal::ONE),
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

    let position_repo = PgPositionRepository::new(pool.connection().clone());
    let created = position_repo
        .create(NewPosition {
            market_id: MarketId::new("0xsettle-mkt"),
            token_id: TokenId::new("333"),
            side: Side::Sell,
            shares: Shares::from(Decimal::new(50, 0)),
            avg_entry_price: Price::from(Decimal::new(80, 2)),
            total_cost_usd: Usd::from(Decimal::new(40, 0)),
            total_fees_usd: Usd::ZERO,
        })
        .await
        .expect("create position");
    position_repo
        .settle_position(&created.position_id, Decimal::new(10, 0))
        .await
        .expect("settle position");
    assert!(position_repo.find_open().await.unwrap().is_empty());
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
        updated_at: Utc::now(),
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
    let period_id = PeriodId::generate();

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
async fn lifecycle_repository_crud() {
    let (pool, _container) = setup_pg().await;
    let repo = PgLifecycleRepository::new(pool.connection().clone());

    let event = NewLifecycleEvent {
        phase: LifecyclePhase::Detected,
        stage: Some(LifecycleRecorder::System),
        message: "Application started".into(),
        metadata: Some(serde_json::json!({ "version": "test" })),
    };
    let recorded = repo.create(event).await.unwrap();
    assert_eq!(recorded.phase, LifecyclePhase::Detected);

    let recent = repo.get_recent(5).await.unwrap();
    assert!(!recent.is_empty());
    assert_eq!(recent[0].message, "Application started");
}

#[tokio::test]
#[ignore = "requires Docker"]
async fn runtime_config_repository_crud() {
    let (pool, _container) = setup_pg().await;
    let repo = PgRuntimeConfigRepository::new(pool.connection().clone());

    let key = RuntimeConfigKey::MinProfitThresholdUsd;
    let config = UpsertRuntimeConfig {
        key,
        value: serde_json::json!({ "threshold_usd": 100.0 }),
        updated_by: "test".into(),
    };
    let set = repo.upsert(config).await.unwrap();
    assert_eq!(set.key, key);

    let got = repo.get(key).await.unwrap();
    assert!(got.is_some());

    let all = repo.get_all().await.unwrap();
    assert!(!all.is_empty());

    assert!(repo.delete(key).await.unwrap());
    assert!(repo.get(key).await.unwrap().is_none());
}

#[tokio::test]
#[ignore = "requires Docker"]
async fn potential_loss_repository_crud() {
    let (pool, _container) = setup_pg().await;
    seed_market(&pool, "evt-pll", "0xpll-mkt", MarketCategory::Tech).await;

    let repo = PgPotentialLossRepository::new(pool.connection().clone());
    let ledger_id = LedgerId::generate();

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

    repo.update(
        &ledger_id,
        UpdatePotentialLoss {
            status: Some(LedgerStatus::Resolved),
            resolved_at: Some(Utc::now()),
        },
    )
    .await
    .unwrap();
    assert!(repo.find_active().await.unwrap().is_empty());
}
