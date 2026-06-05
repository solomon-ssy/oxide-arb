//! Execution pipeline control-factor gates: publication TTL fail-closed and
//! execution-quality stricter depth validation.

use chrono::{Duration, Utc};
use oxide_arb_algorithm::scorer::ScoredOpportunity;
use oxide_arb_api::{fees::FeeCalculator, ws::ClobWsManager};
use oxide_arb_core::{
    bridge::risk_metrics::CoreRiskMetrics,
    control::factor_snapshot::FactorSnapshotStore,
    execution::{
        capital_manager::CapitalManager,
        dispatcher::Dispatcher,
        execution_pipeline::{ExecutionPipeline, ExecutionPipelineDeps},
        fok_strategy::FokOrderStrategy,
        fsm::ExecutionFSM,
        market_inflight::MarketInFlightRegistry,
        plan_builder::PlanBuilder,
        validator::Validator,
    },
    exposure::in_memory::InMemoryExposureReservation,
    infra::async_writer::AsyncWriter,
    observability::{
        alert_dispatcher::AlertDispatcher, execution_audit::ExecutionAuditWriter,
        metrics_hub::MetricsHub,
    },
    pipeline::{
        book_store::BookStore, market_registry::MarketRegistry,
        staleness_classifier::StalenessClassifier,
    },
    service::risk_metrics::{ApiHealthTracker, RiskMetricsState},
};
use oxide_arb_models::{
    clickhouse::OpportunityAuditRow,
    config::{MarketDataConfig, PolymarketConfig, RiskConfig, WebSocketConfig},
    domain::{
        book::BookLevel,
        control_factor::{
            ConfidenceInterval, ControlFactorPublication, ControlFactorSnapshot,
            ControlFactorValue, DataCoverageReport, ExecutionQualityDimensions,
            ExecutionQualityPayload, FactorDimensions, FactorEvidence, FactorPayload,
            LIVE_SNAPSHOT_SCHEMA_VERSION, PointInTimeInputManifest, TailRiskEvidence,
            execution_quality_dimensions,
        },
        latency::LatencyTrace,
        opportunity::{EndgameMeta, Opportunity},
    },
    enums::{
        calibration::{DurationBucket, PriceZone},
        common::{ExecutionMode, MarketCategory, Side, StalenessLevel},
        control_factor::{
            ControlFactorType, FactorMaturity, FactorStatus, PublicationMode, PublicationStatus,
        },
        opportunity::PayoutModel,
    },
    types::{
        Bps, EventId, MarketId, MicroProb, MicroScore, OpportunityId, Price, Shares, TokenId, Usd,
    },
};
use oxide_arb_risk::{
    builder::RiskEngineBuilder, clock::utc_clock, engine::RiskEngine, traits::RiskMetrics,
};
use oxide_arb_test_support::mocks::MockTradeRepository;
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use std::{sync::Arc, time::Duration as StdDuration};
use tokio::sync::Notify;
use tokio_util::sync::CancellationToken;

const TOKEN_YES: &str =
    "15871154585880608648532107628464183779895785213830018178010423617714102767076";
const TOKEN_NO: &str =
    "25871154585880608648532107628464183779895785213830018178010423617714102767076";

struct StaticRiskMetrics;

impl RiskMetrics for StaticRiskMetrics {
    fn total_exposure(&self) -> Usd {
        Usd::ZERO
    }
    fn market_exposure(&self, _: &MarketId) -> Usd {
        Usd::ZERO
    }
    fn open_position_count(&self) -> usize {
        0
    }
    fn open_positions(&self) -> Vec<oxide_arb_models::domain::PositionInfo> {
        Vec::new()
    }
    fn cash_balance(&self) -> Usd {
        Usd::new(dec!(5000))
    }
    fn position_mark_value(&self) -> Usd {
        Usd::ZERO
    }
    fn equity(&self) -> Usd {
        Usd::new(dec!(5000))
    }
    fn active_reservation_count(&self) -> usize {
        0
    }
    fn reserved_usd(&self) -> Usd {
        Usd::ZERO
    }
    fn open_directional_count(&self, _: Side) -> usize {
        0
    }
    fn daily_directional_trades(&self, _: Side) -> u32 {
        0
    }
    fn consecutive_market_misses(&self, _: &MarketId) -> u32 {
        0
    }
    fn record_trade_outcome(&self, _: Side, _: &MarketId, _: bool) {}
    fn ws_disconnect_secs(&self) -> u64 {
        0
    }
    fn api_error_count(&self) -> u64 {
        0
    }
    fn api_request_count(&self) -> u64 {
        0
    }
    fn metrics_age_secs(&self) -> u64 {
        0
    }
    fn is_stale(&self) -> bool {
        false
    }
    fn is_authoritative(&self) -> bool {
        true
    }
}

fn evidence() -> FactorEvidence {
    let now = Utc::now();
    FactorEvidence {
        materialization_run_id: oxide_arb_models::types::MaterializationRunId::new_v7(),
        stage_report_ids: vec![oxide_arb_models::types::StageReportId::new_v7()],
        window_from: now - Duration::hours(1),
        window_to: now,
        source_delay_secs: 60,
        market_count: 1,
        event_count: 1,
        opportunity_count: 1,
        settlement_count: 1,
        sample_count: 1,
        data_coverage: DataCoverageReport {
            expected_rows: 1,
            observed_rows: 1,
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
            manifest_hash: "pit".into(),
        },
        baseline_config_hash: "cfg".into(),
        code_git_sha: "sha".into(),
        dataset_hash: "ds".into(),
        feature_schema_hash: "fs".into(),
        label_schema_hash: "ls".into(),
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
    }
}

fn scored_opportunity(depth_used_pct: Decimal) -> Arc<ScoredOpportunity> {
    Arc::new(ScoredOpportunity {
        opportunity: Arc::new(Opportunity {
            opportunity_id: OpportunityId::new_v7(),
            market_id: MarketId::new("0xfactor-exec-market"),
            event_id: EventId::new("evt-factor-exec"),
            token_id: TokenId::new(TOKEN_YES),
            side: Side::Buy,
            payout_model: PayoutModel::DirectionalSettlement {
                projected_payout_if_correct: Usd::new(dec!(100)),
                expected_payout: Usd::new(dec!(95)),
                predicted_side: Side::Buy,
            },
            shares: Shares::new(dec!(100)),
            entry_price: Price::new(dec!(0.92)),
            total_cost: Usd::new(dec!(20)),
            total_fees: Usd::new(dec!(0.40)),
            net_profit: Usd::new(dec!(5)),
            expected_net_profit: Usd::new(dec!(4.5)),
            edge_bps: Bps::new(dec!(300)),
            resolution_adjust: dec!(0.95),
            depth_used_pct,
            staleness: StalenessLevel::Fresh,
            category: MarketCategory::Politics,
            meta: EndgameMeta {
                predicted_yes: true,
                confidence: dec!(0.95),
                convergence_duration_secs: 600,
                price_zone: PriceZone::Z97,
                duration_bucket: DurationBucket::Medium,
                settlement_deadline: None,
            },
            calibration: oxide_arb_models::domain::calibration::CalibrationSnapshot {
                bucket_key: oxide_arb_models::domain::calibration::BucketKey {
                    category: MarketCategory::Politics,
                    price_zone: PriceZone::Z97,
                    duration_bucket: DurationBucket::Medium,
                },
                posterior_mean: dec!(0.93),
                sample_size: 50,
                alpha_prior: dec!(2.0),
                beta_prior: dec!(1.0),
                fallback_tier: 1,
                fused_probability: dec!(0.99),
            },
            detected_at: Utc::now(),
        }),
        token_yes: TokenId::new(TOKEN_YES),
        token_no: TokenId::new(TOKEN_NO),
        score: MicroScore::try_from_decimal(dec!(0.9)).expect("score"),
        fill_probability: MicroProb::ONE,
        urgency_factor: MicroProb::ONE,
        category_weight: MicroProb::ONE,
        staleness_discount: MicroProb::ONE,
        book_yes_version: 1,
        book_no_version: 1,
        applied_factors: Arc::from([]),
        trace: Arc::new(LatencyTrace::default()),
    })
}

fn seed_books(store: &BookStore, scored: &ScoredOpportunity) {
    let yes_asks = vec![BookLevel::from_decimal_unchecked(
        Price::new(dec!(0.92)),
        Shares::new(dec!(1000)),
    )];
    let no_bids = vec![BookLevel::from_decimal_unchecked(
        Price::new(dec!(0.07)),
        Shares::new(dec!(1000)),
    )];
    let no_asks = vec![BookLevel::from_decimal_unchecked(
        Price::new(dec!(0.08)),
        Shares::new(dec!(1000)),
    )];
    let now_ms = u64::try_from(Utc::now().timestamp_millis().max(0)).unwrap_or(0);
    store.apply_snapshot(&scored.token_yes, vec![], yes_asks, now_ms, None);
    store.apply_snapshot(&scored.token_no, no_bids, no_asks, now_ms, None);
}

fn execution_quality_factor(
    dims: ExecutionQualityDimensions,
    max_depth_usage_pct: Decimal,
) -> ControlFactorValue {
    ControlFactorValue {
        factor_id: oxide_arb_models::types::ControlFactorId::new_v7(),
        factor_type: ControlFactorType::ExecutionQuality,
        dimensions: FactorDimensions::ExecutionQuality(dims),
        payload: FactorPayload::ExecutionQuality(ExecutionQualityPayload {
            fill_probability_multiplier: Decimal::ONE,
            max_depth_usage_pct: Some(max_depth_usage_pct),
            slippage_bps_addon: Decimal::ZERO,
            min_liquidity_score: None,
        }),
        evidence: evidence(),
        status: FactorStatus::Published,
        generated_at: Utc::now() - Duration::hours(2),
        expires_at: Utc::now() + Duration::days(1),
        owner: "test".into(),
        schema_version: LIVE_SNAPSHOT_SCHEMA_VERSION,
    }
}

fn compiled_snapshot(
    factors: &[ControlFactorValue],
    publication_expires_at: chrono::DateTime<Utc>,
) -> ControlFactorSnapshot {
    let publication = ControlFactorPublication {
        publication_id: oxide_arb_models::types::FactorPublicationId::new_v7(),
        mode: PublicationMode::Published,
        factor_ids: factors.iter().map(|f| f.factor_id.clone()).collect(),
        previous_publication_id: None,
        status: PublicationStatus::Active,
        effective_from: Utc::now() - Duration::hours(1),
        expires_at: publication_expires_at,
        approved_by: Some("op".into()),
        approval_reason: "test".into(),
        publication_hash: "hash-v1".into(),
    };
    ControlFactorSnapshot::compile(&publication, factors, Utc::now(), true).expect("compile")
}

fn factor_test_risk_config() -> RiskConfig {
    RiskConfig {
        max_total_exposure_usd: dec!(5000),
        max_single_market_exposure_usd: dec!(500),
        max_single_bet_usd: dec!(25),
        max_open_positions: 5,
        max_daily_loss_usd: dec!(75),
        max_weekly_loss_usd: dec!(120),
        daily_budget_usd: dec!(200),
        min_balance_usd: dec!(50),
        reserve_balance_usd: dec!(100),
        min_trade_usd: dec!(1),
        max_consecutive_misses: 3,
        ..RiskConfig::default()
    }
}

fn factor_test_risk_engine() -> Arc<RiskEngine> {
    Arc::new(
        RiskEngineBuilder::new()
            .config(factor_test_risk_config())
            .clock(utc_clock())
            .initial_equity(Usd::new(dec!(5000)))
            .build(&StaticRiskMetrics)
            .expect("risk engine"),
    )
}

fn factor_test_audit_writer(metrics: Arc<MetricsHub>) -> Arc<ExecutionAuditWriter> {
    let (writer, _worker) = AsyncWriter::new(
        "test-factor-exec-audit",
        128,
        StdDuration::from_secs(3600),
        move |_batch: Vec<OpportunityAuditRow>| Box::pin(async move { Ok(()) }),
        metrics,
        CancellationToken::new(),
    );
    Arc::new(ExecutionAuditWriter::new(Arc::new(writer)))
}

fn factor_test_risk_metrics(
    execution_mode: ExecutionMode,
    exposure: Arc<InMemoryExposureReservation>,
) -> Arc<CoreRiskMetrics> {
    let metrics_state = Arc::new(RiskMetricsState::new(Arc::new(ApiHealthTracker::new(
        StdDuration::from_secs(60),
    ))));
    metrics_state.seed_simulated_snapshot(execution_mode, Usd::new(dec!(5000)));
    let ws_manager = Arc::new(ClobWsManager::new(
        &PolymarketConfig::default(),
        &WebSocketConfig::default(),
        CancellationToken::new(),
        None,
        None,
    ));
    ws_manager.seed_test_connectivity();
    Arc::new(CoreRiskMetrics::new(
        metrics_state,
        exposure,
        ws_manager,
        execution_mode,
    ))
}

fn pipeline(
    store: Arc<FactorSnapshotStore>,
    execution_mode: ExecutionMode,
    scored: &Arc<ScoredOpportunity>,
) -> ExecutionPipeline<MockTradeRepository> {
    let metrics = Arc::new(MetricsHub::new());
    let alerts = Arc::new(AlertDispatcher::new(None, None, None, 0));
    let fsm = Arc::new(ExecutionFSM::new(Arc::clone(&metrics), alerts));
    let book_store = Arc::new(BookStore::new(Arc::clone(&metrics)));
    seed_books(&book_store, scored);
    let reservation_config = RiskConfig::default().exposure_reservation_config();
    let exposure = Arc::new(InMemoryExposureReservation::new(reservation_config.clone()));
    let capital = Arc::new(CapitalManager::new(
        Arc::clone(&exposure),
        reservation_config,
    ));
    let fee_calculator = Arc::new(FeeCalculator::default());
    let risk_metrics = factor_test_risk_metrics(execution_mode, exposure);
    let audit_writer = factor_test_audit_writer(Arc::clone(&metrics));
    let risk_engine = factor_test_risk_engine();

    ExecutionPipeline::new(ExecutionPipelineDeps {
        validator: Validator::new(
            Arc::clone(&book_store),
            StalenessClassifier::new(&MarketDataConfig::default()),
            dec!(50),
            5_000,
            Arc::clone(&metrics),
        ),
        plan_builder: PlanBuilder::new(
            Arc::clone(&fee_calculator),
            Arc::new(MarketRegistry::new()),
        ),
        dispatcher: Dispatcher::new(
            execution_mode,
            Some(Arc::clone(&book_store)),
            Arc::clone(&fee_calculator),
            Arc::clone(&metrics),
        ),
        order_strategy: FokOrderStrategy::new(
            execution_mode,
            None,
            fee_calculator,
            30_000,
            Arc::clone(&metrics),
        ),
        capital_manager: capital,
        risk_engine,
        risk_metrics,
        fsm,
        market_inflight: Arc::new(MarketInFlightRegistry::new()),
        metrics,
        execution_mode,
        trade_repo: Arc::new(MockTradeRepository::default()),
        audit_writer,
        relay_notify: Arc::new(Notify::new()),
        metrics_state: Arc::new(RiskMetricsState::new(Arc::new(ApiHealthTracker::new(
            StdDuration::from_secs(60),
        )))),
        factors: store,
        shadow_writer: None,
    })
}

#[tokio::test]
async fn expired_publication_rejects_at_risk_in_live_mode() {
    let scored = scored_opportunity(dec!(10));
    let snapshot = compiled_snapshot(&[], Utc::now() - Duration::minutes(1));
    let store = Arc::new(FactorSnapshotStore::new(Utc::now()));
    store.store_published(Arc::new(snapshot));
    let pipeline = pipeline(Arc::clone(&store), ExecutionMode::Live, &scored);

    let result = pipeline.execute(scored).await;

    assert_eq!(result.rejection_stage.as_deref(), Some("risk"));
    assert!(
        result
            .rejection_reason
            .as_deref()
            .is_some_and(|reason| reason.starts_with("ControlFactorSnapshotExpired")),
        "expected named TTL gate, got {:?}",
        result.rejection_reason
    );
}

#[tokio::test]
async fn expired_publication_is_allowed_in_dry_run_mode() {
    let scored = scored_opportunity(dec!(10));
    let snapshot = compiled_snapshot(&[], Utc::now() - Duration::minutes(1));
    let store = Arc::new(FactorSnapshotStore::new(Utc::now()));
    store.store_published(Arc::new(snapshot));
    let pipeline = pipeline(Arc::clone(&store), ExecutionMode::DryRun, &scored);

    let result = pipeline.execute(scored).await;

    assert_ne!(
        result.rejection_stage.as_deref(),
        Some("risk"),
        "dry run must not fail closed on expired publication TTL"
    );
}

#[tokio::test]
async fn execution_quality_depth_cap_rejects_at_factor_validation() {
    let scored = scored_opportunity(dec!(10));
    let metrics = Arc::new(MetricsHub::new());
    let book_store = Arc::new(BookStore::new(Arc::clone(&metrics)));
    seed_books(&book_store, &scored);
    let book = book_store
        .load(&scored.token_yes)
        .expect("yes book must exist for execution-quality lookup");
    let dims = execution_quality_dimensions(
        MarketCategory::Politics,
        PriceZone::Z97,
        StalenessLevel::Fresh,
        &book,
        book.timestamp_ms,
    );
    let factor = execution_quality_factor(dims, dec!(0.05));
    let snapshot = compiled_snapshot(&[factor], Utc::now() + Duration::days(1));
    let store = Arc::new(FactorSnapshotStore::new(Utc::now()));
    store.store_published(Arc::new(snapshot));
    let pipeline = pipeline(store, ExecutionMode::Live, &scored);

    let result = pipeline.execute(scored).await;

    assert_eq!(result.rejection_stage.as_deref(), Some("factor_validation"));
    assert!(
        result
            .rejection_reason
            .as_deref()
            .is_some_and(|reason| reason.contains("depth usage")),
        "expected depth cap rejection, got {:?}",
        result.rejection_reason
    );
}
