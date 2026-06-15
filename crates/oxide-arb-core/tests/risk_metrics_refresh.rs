//! Mode-aware `RiskMetricsRefreshService` integration tests (Docker Postgres).
//!
//! Pins the simulated derived-ledger semantics: DryRun/Paper cash is
//! `bankroll − successful spend(mode) + settlement payout(mode)` recomputed
//! from Postgres, snapshots keep their `Simulated*` source, and mode-scoped
//! isolation keeps Live rows out of simulated snapshots (and vice versa).

use chrono::Utc;
use oxide_arb_algorithm::calibration::ResolutionCalibrator;
use oxide_arb_core::{
    bridge::execution_mode::ExecutionModeHandle,
    observability::metrics_hub::MetricsHub,
    pipeline::{book_store::BookStore, market_registry::MarketRegistry},
    runtime_config::RuntimeConfigStore,
    service::{
        equity_valuator::EquityValuator,
        risk_metrics::{
            ApiHealthTracker, RiskMetricsRefreshDeps, RiskMetricsRefreshService, RiskMetricsSource,
            RiskMetricsState,
        },
    },
};
use oxide_arb_models::{
    config::PostgresConfig,
    domain::{
        NewPosition, NewTrade, SettlePositionParams, TradeObservation, UpsertEvent, UpsertMarket,
    },
    enums::{
        common::{
            CategorySet, ExecutionMode, MarketCategory, RedeemStatus, SettlementTrigger, Side,
            TickSize, TradeState,
        },
        market::{EventStatus, MarketStatus},
    },
    runtime_config::RuntimeConfig,
    types::{
        EventId, ExecutionId, MarketId, OpportunityId, PositionId, Price, ReservationId, Shares,
        TokenId, TradeId, Usd,
    },
};
use oxide_arb_repository::{
    postgres::{PgEventRepository, PgMarketRepository, PgPositionRepository, PgTradeRepository},
    traits::{EventRepository, MarketRepository, PositionRepository, TradeRepository},
};
use oxide_arb_storage::postgres::{
    PostgresPool,
    migration::{Migrator, MigratorTrait},
};
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use std::sync::Arc;
use testcontainers::{ImageExt, runners::AsyncRunner};

const MARKET: &str = "0xrefresh-market";
const EVENT: &str = "evt-refresh";

fn test_pg_config(port: u16) -> PostgresConfig {
    PostgresConfig {
        host: "localhost".into(),
        port,
        user: "postgres".into(),
        password: "postgres".into(),
        database: "test_oxide_arb".into(),
        schema: "public".into(),
        max_connections: 5,
        min_connections: 1,
        connect_timeout_secs: 10,
        idle_timeout_secs: 300,
        acquire_timeout_secs: 10,
        max_lifetime_secs: 1800,
        statement_timeout_ms: 30_000,
        idle_in_transaction_timeout_ms: 60_000,
        lock_timeout_ms: 5_000,
        work_mem: "16MB".into(),
        verify_session_params: false,
        statement_cache_capacity: 100,
        application_name: "oxide-arb-refresh-test".into(),
    }
}

async fn seed_catalog(pool: &PostgresPool) {
    PgEventRepository::new(pool.connection().clone())
        .upsert(UpsertEvent {
            event_id: EventId::new(EVENT),
            title: "Refresh Event".into(),
            slug: "refresh-event".into(),
            status: EventStatus::Active,
            tags: vec!["crypto".to_owned()].into(),
            neg_risk: false,
            end_date: None,
            raw_gamma: None,
        })
        .await
        .expect("upsert event");
    PgMarketRepository::new(pool.connection().clone())
        .upsert(UpsertMarket {
            market_id: MarketId::new(MARKET),
            event_id: EventId::new(EVENT),
            question: "Refresh?".into(),
            slug: "refresh-market".into(),
            categories: CategorySet::from(MarketCategory::Crypto),
            status: MarketStatus::Active,
            outcome: None,
            yes_token_id: TokenId::new("12345"),
            no_token_id: TokenId::new("67890"),
            tick_size: TickSize::Hundredth,
            neg_risk: false,
            end_date: None,
            resolved_at: None,
            fees_enabled: true,
            fee_rate: None,
            fee_exponent: None,
            fee_taker_only: None,
            fee_rebate_rate: None,
            fee_source: None,
            fee_observed_at: None,
        })
        .await
        .expect("upsert market");
}

async fn seed_successful_trade(
    trade_repo: &PgTradeRepository,
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
            market_id: MarketId::new(MARKET),
            event_id: EventId::new(EVENT),
            token_id: TokenId::new(token_id),
            side: Side::Buy,
            shares: Shares::new(dec!(100)),
            price: Price::new(dec!(0.5)),
            cost_usd: Usd::new(cost),
            fee_usd: Usd::new(fee),
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

fn refresh_service(
    pool: &PostgresPool,
    state: Arc<RiskMetricsState>,
    mode: ExecutionModeHandle,
) -> RiskMetricsRefreshService {
    let metrics = Arc::new(MetricsHub::new());
    let equity_valuator = Arc::new(EquityValuator::new(
        Arc::new(MarketRegistry::new()),
        Arc::new(BookStore::new(Arc::clone(&metrics))),
        Arc::new(ResolutionCalibrator::empty(
            RuntimeConfig::default().detection.calibration,
        )),
    ));
    RiskMetricsRefreshService::new(RiskMetricsRefreshDeps {
        state,
        execution_mode: mode,
        runtime_config: Arc::new(RuntimeConfigStore::new(RuntimeConfig::default())),
        clob_client: None,
        trade_repo: Arc::new(PgTradeRepository::new(pool.connection().clone())),
        position_repo: Arc::new(PgPositionRepository::new(pool.connection().clone())),
        equity_valuator,
        metrics,
    })
}

/// Paper: $41 spent, $30 recovered through settlement. Live: $72 spent,
/// position still open — must never leak into the Paper snapshot.
async fn seed_ledger_rows(pool: &PostgresPool) {
    let trade_repo = PgTradeRepository::new(pool.connection().clone());
    let position_repo = PgPositionRepository::new(pool.connection().clone());

    let paper_trade = seed_successful_trade(
        &trade_repo,
        "12345",
        ExecutionMode::Paper,
        dec!(40),
        dec!(1),
    )
    .await;
    let live_trade =
        seed_successful_trade(&trade_repo, "67890", ExecutionMode::Live, dec!(70), dec!(2)).await;

    let paper_position = position_repo
        .create(NewPosition {
            position_id: PositionId::from_v7(),
            trade_id: paper_trade,
            market_id: MarketId::new(MARKET),
            token_id: TokenId::new("12345"),
            side: Side::Buy,
            execution_mode: ExecutionMode::Paper,
            shares: Shares::new(dec!(100)),
            avg_entry_price: Price::new(dec!(0.4)),
            total_cost_usd: Usd::new(dec!(40)),
            total_fees_usd: Usd::new(dec!(1)),
            redeem_status: RedeemStatus::NotRequired,
        })
        .await
        .expect("create paper position");
    position_repo
        .settle_position(
            &paper_position.position_id,
            SettlePositionParams {
                winning_token_id: TokenId::new("12345"),
                settlement_payout_usd: Usd::new(dec!(30)),
                realized_pnl: dec!(-11),
                redeem_tx_hash: None,
                redeem_status: RedeemStatus::NotRequired,
                settlement_trigger: SettlementTrigger::Manual,
                oracle_verdict: None,
            },
        )
        .await
        .expect("settle paper position");
    position_repo
        .create(NewPosition {
            position_id: PositionId::from_v7(),
            trade_id: live_trade,
            market_id: MarketId::new(MARKET),
            token_id: TokenId::new("67890"),
            side: Side::Buy,
            execution_mode: ExecutionMode::Live,
            shares: Shares::new(dec!(100)),
            avg_entry_price: Price::new(dec!(0.7)),
            total_cost_usd: Usd::new(dec!(70)),
            total_fees_usd: Usd::new(dec!(2)),
            redeem_status: RedeemStatus::Pending,
        })
        .await
        .expect("create live position");
}

#[tokio::test]
#[ignore = "requires Docker"]
async fn simulated_refresh_derives_mode_scoped_ledger() {
    let pg_container = testcontainers_modules::postgres::Postgres::default()
        .with_db_name("test_oxide_arb")
        .with_user("postgres")
        .with_password("postgres")
        .with_tag("16")
        .start()
        .await
        .expect("PG container");
    let pg_port = pg_container
        .get_host_port_ipv4(5432)
        .await
        .expect("PG port");
    let pool = PostgresPool::connect(&test_pg_config(pg_port))
        .await
        .expect("PG connect");
    Migrator::up(pool.connection(), None)
        .await
        .expect("migrate");
    seed_catalog(&pool).await;
    seed_ledger_rows(&pool).await;

    let state = Arc::new(RiskMetricsState::new(Arc::new(ApiHealthTracker::new(
        std::time::Duration::from_secs(60),
    ))));
    let mode = ExecutionModeHandle::new(ExecutionMode::Paper);
    let service = refresh_service(&pool, Arc::clone(&state), mode.clone());

    // Paper: bankroll(1000) − spend(41) + payout(30) = 989; Live rows excluded.
    service.refresh().await.expect("paper refresh");
    assert_eq!(state.cash_balance(), Usd::new(dec!(989)));
    assert_eq!(state.source(), RiskMetricsSource::SimulatedPaper);
    assert_eq!(state.open_position_count(), 0);

    // DryRun: no dry-run history → pristine bankroll, dry-run source.
    mode.store(ExecutionMode::DryRun);
    service.refresh().await.expect("dry-run refresh");
    assert_eq!(state.cash_balance(), Usd::new(dec!(1000)));
    assert_eq!(state.source(), RiskMetricsSource::SimulatedDryRun);
    assert_eq!(state.open_position_count(), 0);

    // Live without a ClobClient must fail closed and leave the snapshot as-is.
    mode.store(ExecutionMode::Live);
    let error = service
        .refresh()
        .await
        .expect_err("Live refresh without ClobClient must fail");
    assert!(
        error.to_string().contains("ClobClient"),
        "unexpected error: {error}"
    );
    assert_eq!(state.source(), RiskMetricsSource::SimulatedDryRun);
}
