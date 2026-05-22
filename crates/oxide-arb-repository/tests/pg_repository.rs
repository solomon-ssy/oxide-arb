//! `PostgreSQL` repository integration tests (requires Docker).

mod common;

use chrono::Utc;
use common::{make_event, make_market, setup_pg};
use oxide_arb_models::domain::calibration::{DurationBucket, PriceZone};
use oxide_arb_models::domain::{NewPosition, NewTrade, UpdateTradeOutcome};
use oxide_arb_models::entities::{
    accounting_period, calibration, calibration_outcome, potential_loss_ledger, risk_state,
};
use oxide_arb_models::enums::common::{
    ExecutionMode, LedgerStatus, MarketCategory, PositionStatus, ReportType, Side, TradeOutcome,
};
use oxide_arb_models::enums::lifecycle::LifecyclePhase;
use oxide_arb_models::enums::risk::BreakerStateName;
use oxide_arb_models::types::*;
use oxide_arb_repository::postgres::*;
use oxide_arb_repository::traits::*;
use rust_decimal::Decimal;
use sea_orm::ActiveValue::Set;
use uuid::Uuid;

async fn seed_market(
    pool: &oxide_arb_storage::postgres::PostgresPool,
    event_id: &str,
    market_id: &str,
    category: MarketCategory,
) {
    let event_repo = PgEventRepository::new(pool.connection().clone());
    let market_repo = PgMarketRepository::new(pool.connection().clone());
    event_repo
        .insert(make_event(
            event_id,
            "Seed Event",
            &format!("{event_id}-slug"),
            category,
        ))
        .await
        .unwrap();
    market_repo
        .insert(make_market(
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
    let inserted = repo.insert(model).await.expect("insert event");
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
        .insert(make_event(
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

    let inserted = market_repo.insert(mkt).await.expect("insert market");
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
        .insert(make_event(
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
    let inserted = market_repo.insert(mkt).await.unwrap();
    assert_eq!(inserted.question, "Original question?");

    let mut active: oxide_arb_models::entities::market::ActiveModel = inserted.into();
    active.question = Set("Updated question?".into());
    active.slug = Set("updated".into());
    let updated = market_repo.update(active).await.unwrap();
    assert_eq!(updated.question, "Updated question?");
    assert_eq!(updated.slug, "updated");

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
        .update_outcome(
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
    let bucket = calibration::ActiveModel {
        category: Set(MarketCategory::Sports),
        price_zone: Set(PriceZone::Z99),
        duration_bucket: Set(DurationBucket::Short),
        total_count: Set(10),
        correct_count: Set(9),
        alpha_prior: Set(Probability::from(Decimal::new(10, 1))),
        beta_prior: Set(Probability::from(Decimal::new(10, 1))),
        posterior_mean: Set(Some(Probability::from(Decimal::new(9, 1)))),
        updated_at: Set(Utc::now()),
        ..Default::default()
    };

    let inserted = cal_repo.insert_bucket(bucket).await.unwrap();
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

    let outcome = calibration_outcome::ActiveModel {
        market_id: Set(MarketId::new("0xcal-mkt")),
        category: Set(MarketCategory::Sports),
        price_zone: Set(PriceZone::Z99),
        duration_bucket: Set(DurationBucket::Short),
        predicted_yes: Set(true),
        actual_yes: Set(None),
        entry_price: Set(Price::from(Decimal::new(99, 2))),
        confidence_at_entry: Set(Probability::from(Decimal::new(95, 2))),
        convergence_secs: Set(3600),
        ..Default::default()
    };
    cal_repo.record_outcome(outcome).await.unwrap();

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

    let mut active: risk_state::ActiveModel = state.into();
    active.consecutive_misses = Set(2);
    active.breaker_state = Set(BreakerStateName::Open);
    repo.save(active).await.unwrap();

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
    let period_id = Uuid::new_v4().to_string();

    let period = accounting_period::ActiveModel {
        period_id: Set(period_id.clone()),
        period_type: Set(ReportType::Daily),
        start_date: Set(today),
        end_date: Set(today),
        realized_pnl: Set(Usd::from(Decimal::new(100, 0))),
        total_fees: Set(Usd::from(Decimal::new(5, 0))),
        trade_count: Set(3),
        win_count: Set(2),
        loss_count: Set(1),
        miss_count: Set(0),
        max_drawdown: Set(Usd::ZERO),
        sharpe_ratio: Set(None),
        finalized: Set(false),
        created_at: Set(Utc::now()),
    };

    let created = repo.create_period(period).await.unwrap();
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

    let recorded = repo
        .record(
            LifecyclePhase::Detected,
            Some("startup"),
            "Application started",
            Some(serde_json::json!({ "version": "test" })),
        )
        .await
        .unwrap();
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

    let value = serde_json::json!({ "threshold_usd": 100.0 });
    let set = repo
        .set("min_profit_threshold_usd", &value, "test")
        .await
        .unwrap();
    assert_eq!(set.key, "min_profit_threshold_usd");

    let got = repo.get("min_profit_threshold_usd").await.unwrap();
    assert!(got.is_some());

    let typed = repo
        .get_typed(
            oxide_arb_models::entities::runtime_config::RuntimeConfigKey::MinProfitThresholdUsd,
        )
        .await
        .unwrap();
    assert!(typed.is_some());

    let all = repo.get_all().await.unwrap();
    assert!(!all.is_empty());

    assert!(repo.delete("min_profit_threshold_usd").await.unwrap());
    assert!(
        repo.get("min_profit_threshold_usd")
            .await
            .unwrap()
            .is_none()
    );
}

#[tokio::test]
#[ignore = "requires Docker"]
async fn potential_loss_repository_crud() {
    let (pool, _container) = setup_pg().await;
    seed_market(&pool, "evt-pll", "0xpll-mkt", MarketCategory::Tech).await;

    let repo = PgPotentialLossRepository::new(pool.connection().clone());
    let ledger_id = Uuid::new_v4().to_string();

    let entry = potential_loss_ledger::ActiveModel {
        ledger_id: Set(ledger_id.clone()),
        market_id: Set(MarketId::new("0xpll-mkt")),
        token_id: Set(TokenId::new("555")),
        shares: Set(Shares::from(Decimal::new(20, 0))),
        entry_price: Set(Price::from(Decimal::new(90, 2))),
        max_loss_usd: Set(Usd::from(Decimal::new(18, 0))),
        status: Set(LedgerStatus::Active),
        created_at: Set(Utc::now()),
        resolved_at: Set(None),
    };

    repo.record(entry).await.unwrap();
    assert_eq!(repo.find_active().await.unwrap().len(), 1);
    assert_eq!(
        repo.total_active_loss().await.unwrap(),
        Usd::from(Decimal::new(18, 0))
    );

    repo.resolve(&ledger_id).await.unwrap();
    assert!(repo.find_active().await.unwrap().is_empty());
}
