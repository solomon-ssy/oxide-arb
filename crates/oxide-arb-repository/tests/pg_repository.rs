//! `PostgreSQL` repository integration tests (requires Docker).

#[path = "common/pg.rs"]
mod pg;

use std::collections::HashSet;

use chrono::{NaiveDate, Utc};
use oxide_arb_models::{
    domain::{
        AcquireMaterializationRunOutcome, CancelMaterializationRunOutcome,
        EnqueueMaterializationRunOptions, EnqueueMaterializationRunOutcome,
        MaterializationRunStatusPatch, NewAccountingPeriod, NewBalanceSnapshot,
        NewCalibrationOutcome, NewControlFactorMaterializationRun, NewControlFactorStageReport,
        NewEmergencySnapshot, NewPosition, NewPotentialLoss, NewReconciliationReport,
        NewRiskAuditEvent, NewRuntimeConfigActivation, NewRuntimeConfigVersion,
        NewTokenBalanceSnapshot, NewTrade, ResolvePotentialLoss, RunTransitionOutcome,
        SettlePositionParams, TradeObservation, UpsertBlacklistEntry, UpsertCalibration,
        UpsertRiskEngineState,
    },
    enums::{
        calibration::{DurationBucket, PriceZone},
        common::{
            ExecutionMode, MarketCategory, PositionStatus, RedeemStatus, ReportType,
            SettlementTrigger, Side, TradeBusinessOutcome, TradeState,
        },
        control_factor::{
            EvidenceStageStatus, MaterializationOutputPolicy, MaterializationRunKind,
            MaterializationRunStatus, MaterializationStageName, RunTriggerType,
        },
        fact::BalanceSnapshotSource,
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
use pg::{make_event, make_market, setup_pg};
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

#[tokio::test]
#[ignore = "requires Docker"]
async fn trade_repository_crud() {
    let (pool, _container) = setup_pg().await;
    seed_market(&pool, "evt-trade", "0xtrade-mkt", MarketCategory::Sports).await;

    let trade_repo = PgTradeRepository::new(pool.connection().clone());
    let execution_id = ExecutionId::generate();

    let created = trade_repo
        .create(NewTrade {
            trade_id: TradeId::generate(),
            execution_id: execution_id.clone(),
            reservation_id: ReservationId::new("res-trade"),
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
            scored_snapshot: serde_json::json!({}),
            category: MarketCategory::Sports,
            execution_mode: ExecutionMode::DryRun,
        })
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
async fn trade_repository_batch_create() {
    let (pool, _container) = setup_pg().await;
    seed_market(&pool, "evt-batch", "0xbatch-mkt", MarketCategory::Finance).await;

    let trade_repo = PgTradeRepository::new(pool.connection().clone());
    let trades: Vec<NewTrade> = (0..3)
        .map(|i| NewTrade {
            trade_id: TradeId::generate(),
            execution_id: ExecutionId::generate(),
            reservation_id: ReservationId::new(format!("res-batch-{i}")),
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
    let trade_id = TradeId::generate();
    trade_repo
        .create(NewTrade {
            trade_id: trade_id.clone(),
            execution_id: ExecutionId::new("exec-pos-test"),
            reservation_id: ReservationId::new("res-pos-test"),
            opportunity_id: OpportunityId::new_v7(),
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
            position_id: PositionId::generate(),
            trade_id,
            market_id: MarketId::new("0xpos-market"),
            token_id: TokenId::new("111"),
            side: Side::Buy,
            shares: Shares::from(Decimal::new(100, 0)),
            avg_entry_price: Price::from(Decimal::new(95, 2)),
            total_cost_usd: Usd::from(Decimal::new(95, 0)),
            total_fees_usd: Usd::from(Decimal::ONE),
            redeem_status: RedeemStatus::NotRequired,
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
    let trade_id = TradeId::generate();
    trade_repo
        .create(NewTrade {
            trade_id: trade_id.clone(),
            execution_id: ExecutionId::new("exec-settle"),
            reservation_id: ReservationId::new("res-settle"),
            opportunity_id: OpportunityId::new_v7(),
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
            position_id: PositionId::generate(),
            trade_id,
            market_id: MarketId::new("0xsettle-mkt"),
            token_id: TokenId::new("333"),
            side: Side::Sell,
            shares: Shares::from(Decimal::new(50, 0)),
            avg_entry_price: Price::from(Decimal::new(80, 2)),
            total_cost_usd: Usd::from(Decimal::new(40, 0)),
            total_fees_usd: Usd::ZERO,
            redeem_status: RedeemStatus::NotRequired,
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
        trade_id: TradeId::generate(),
        opportunity_id: OpportunityId::new_v7(),
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
async fn runtime_config_version_repository_records_activation_history() {
    let (pool, _container) = setup_pg().await;
    let repo = PgRuntimeConfigVersionRepository::new(pool.connection().clone());

    let version_id = RuntimeConfigVersionId::new_v7();
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
            runtime_config_activation_id: RuntimeConfigActivationId::new_v7(),
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
        materialization_run_id: MaterializationRunId::new_v7(),
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
        stage_report_id: StageReportId::new_v7(),
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
async fn fact_data_repository_records_balance_and_token_snapshots() {
    let (pool, _container) = setup_pg().await;
    seed_market(&pool, "evt-facts", "0xfact-mkt", MarketCategory::Politics).await;
    let repo = PgFactDataRepository::new(pool.connection().clone());
    let observed_at = Utc::now();

    let balance = repo
        .create_balance_snapshot(NewBalanceSnapshot {
            balance_snapshot_id: BalanceSnapshotId::new_v7(),
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

    let token_id = TokenId::new("tok-fact");
    repo.create_token_balance_snapshots(vec![NewTokenBalanceSnapshot {
        token_balance_snapshot_id: TokenBalanceSnapshotId::new_v7(),
        holder_address: "0xholder".into(),
        market_id: MarketId::new("0xfact-mkt"),
        token_id: token_id.clone(),
        side: Side::Buy,
        internal_shares: Shares::new(dec!(10)),
        external_shares: None,
        drift_shares: None,
        source: BalanceSnapshotSource::InternalLedger,
        block_number: None,
        reconciliation_report_id: None,
        observed_at,
    }])
    .await
    .unwrap();

    let latest = repo
        .latest_token_balance_before(
            "0xholder",
            &MarketId::new("0xfact-mkt"),
            &token_id,
            Utc::now(),
        )
        .await
        .unwrap()
        .expect("token balance snapshot");
    assert_eq!(latest.internal_shares, Shares::new(dec!(10)));
    assert!(latest.external_shares.is_none());
    assert!(latest.drift_shares.is_none());
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
        opportunity_id: None,
        trade_id: None,
        payload: serde_json::json!({"reason": "test"}),
    }])
    .await
    .unwrap();
}
