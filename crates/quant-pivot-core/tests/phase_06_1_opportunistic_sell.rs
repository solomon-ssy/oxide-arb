//! Phase 06.1 — opportunistic Sell exit signal: composite short-circuit +
//! evaluator threshold / shadow / gating behaviour.

use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use quant_pivot_core::{
    execution::{
        CompositeExitSignalEvaluator, ExitSignalContext, ExitSignalEvaluator, ExitSignalVerdict,
    },
    observability::{
        exit_signal_fact_writer::ExitSignalEvaluationEventWriter, metrics_hub::MetricsHub,
    },
    runtime_config::RuntimeConfigStore,
    service::opportunistic_sell::{
        OpportunisticSellScorer, OpportunisticSellSignalEvaluator,
        OpportunisticSellSignalEvaluatorDeps,
    },
};
use quant_pivot_models::{
    domain::{OrderIntentInfo, PositionInfo},
    enums::{
        common::{MarketCategory, OrderType, Side},
        execution::{ExitState, OrderIntentKind, PositionLedgerState},
        quant::{
            AccountSource, ApprovalStatus, ExitSettlementMode, OrderIntentStatus, OutcomeSide,
            QuantRuntimeMode, RedeemPolicy,
        },
    },
    runtime_config::RuntimeConfig,
    types::{
        Bps, ContentHash, EntryOrderSpec, EventId, ExecutedPartialExitNodes, ExitPolicySpec,
        MarketId, ModelVersionId, OpportunisticExitState, OrderIntentId, PositionId, Price,
        Probability, RecommendationId, RuntimeConfigVersionId, Shares, TokenId, Usd,
    },
};
use quant_pivot_research::model::SellScore;
use rust_decimal_macros::dec;

/// A canned exit-signal evaluator that records whether it was invoked.
struct FixedEvaluator {
    verdict: ExitSignalVerdict,
    called: Arc<AtomicBool>,
}

#[async_trait]
impl ExitSignalEvaluator for FixedEvaluator {
    async fn evaluate(&self, _ctx: ExitSignalContext<'_>) -> ExitSignalVerdict {
        self.called.store(true, Ordering::SeqCst);
        self.verdict.clone()
    }
}

/// A canned Sell scorer.
struct FixedScorer(Option<SellScore>);

#[async_trait]
impl OpportunisticSellScorer for FixedScorer {
    async fn score(
        &self,
        _intent: &OrderIntentInfo,
        _lot: &PositionInfo,
        _mark_price: Option<Price>,
        _now: DateTime<Utc>,
    ) -> quant_pivot_error::QuantResult<Option<SellScore>> {
        Ok(self.0.clone())
    }
}

fn sample_intent(runtime_mode: QuantRuntimeMode) -> OrderIntentInfo {
    OrderIntentInfo {
        order_intent_id: OrderIntentId::from_v7(),
        recommendation_id: RecommendationId::from_v7(),
        runtime_mode,
        runtime_config_version_id: RuntimeConfigVersionId::from_v7(),
        model_version_id: ModelVersionId::from_v7(),
        intent_kind: OrderIntentKind::Buy,
        status: OrderIntentStatus::Filled,
        approval_status: ApprovalStatus::Approved,
        approved_by: None,
        approval_reason: None,
        approved_at: Some(Utc::now()),
        policy_id: None,
        policy_hash: None,
        status_reason: None,
        admission_trace_ref: None,
        entry_order_json: EntryOrderSpec {
            token_id: TokenId::new("yes"),
            side: Side::Buy,
            order_type: OrderType::Gtc,
            limit_price: Price::new(dec!(0.5)),
            shares: Shares::new(dec!(10)),
            max_slippage_bps: Bps::new(dec!(50)),
            valid_until: Utc::now(),
        },
        exit_policy_json: ExitPolicySpec {
            take_profit_price: None,
            take_profit_pct: None,
            stop_loss_price: None,
            stop_loss_pct: None,
            time_exit_at: None,
            max_hold_secs: None,
            trailing_stop: None,
            signal_invalidation_rules: Vec::new(),
            partial_exit_nodes: Vec::new(),
            settlement_mode: ExitSettlementMode::ExitBeforeResolution,
            redeem_policy: RedeemPolicy::Manual,
            manual_review_at: None,
            entry_reference_price: Price::new(dec!(0.5)),
            entry_composite_score: Probability::new(dec!(0.8)),
        },
        risk_envelope_hash: ContentHash::parse(format!("blake3:{}", "a".repeat(64))).expect("hash"),
        expires_at: Utc::now(),
        exit_state: ExitState::Monitoring,
        exit_reason: None,
        next_check_at: None,
        peak_mark_price: None,
        last_signal_recheck_at: None,
        executed_partial_exit_node_ids: ExecutedPartialExitNodes::default(),
        pending_partial_exit_node_id: None,
        opportunistic_exit_state: OpportunisticExitState::default(),
        created_at: Utc::now(),
        updated_at: Utc::now(),
    }
}

fn sample_lot() -> PositionInfo {
    PositionInfo {
        position_id: PositionId::from_v7(),
        order_intent_id: OrderIntentId::from_v7(),
        token_id: TokenId::new("yes"),
        market_id: MarketId::new("m1"),
        event_id: Some(EventId::new("e1")),
        category: MarketCategory::Sports,
        side: OutcomeSide::Yes,
        state: PositionLedgerState::Open,
        shares: Shares::new(dec!(10)),
        avg_price: Price::new(dec!(0.5)),
        cost_usd: Usd::new(dec!(5)),
        realized_pnl_usd: Usd::ZERO,
        source: AccountSource::Polymarket,
        opened_at: Utc::now(),
        updated_at: Utc::now(),
        closed_at: None,
    }
}

/// A high-confidence, high-alpha score that clears the default gates.
const fn strong_score() -> SellScore {
    SellScore {
        exit_alpha_bps: Bps::new(dec!(120)),
        p_exit_better: Probability::new(dec!(0.8)),
        confidence: Probability::new(dec!(0.9)),
        recommended_cumulative_exit_pct: dec!(1.0),
        net: dec!(0.4),
    }
}

fn with_ctx<'a>(intent: &'a OrderIntentInfo, lot: &'a PositionInfo) -> ExitSignalContext<'a> {
    ExitSignalContext {
        intent,
        lot,
        mark_price: Some(Price::new(dec!(0.55))),
        now: Utc::now(),
    }
}

fn opportunistic_config(enabled: bool, shadow_mode: bool) -> RuntimeConfig {
    let mut config = RuntimeConfig::default();
    let policy = &mut config.execution.exit_monitor.opportunistic_sell;
    policy.enabled = enabled;
    policy.shadow_mode = shadow_mode;
    config
}

fn build_evaluator(
    score: Option<SellScore>,
    enabled: bool,
    shadow_mode: bool,
) -> OpportunisticSellSignalEvaluator<FixedScorer> {
    let metrics = Arc::new(MetricsHub::new());
    let audit = Arc::new(ExitSignalEvaluationEventWriter::drop_only(&metrics));
    OpportunisticSellSignalEvaluator::new(OpportunisticSellSignalEvaluatorDeps {
        scorer: FixedScorer(score),
        config: Arc::new(RuntimeConfigStore::new(opportunistic_config(
            enabled,
            shadow_mode,
        ))),
        metrics,
        audit,
    })
}

#[tokio::test]
async fn thesis_invalidated_short_circuits_before_opportunistic() {
    let opp_called = Arc::new(AtomicBool::new(false));
    let reinference: Arc<dyn ExitSignalEvaluator> = Arc::new(FixedEvaluator {
        verdict: ExitSignalVerdict::ThesisInvalidated {
            detail: "broke".to_owned(),
        },
        called: Arc::new(AtomicBool::new(false)),
    });
    let opportunistic: Arc<dyn ExitSignalEvaluator> = Arc::new(FixedEvaluator {
        verdict: ExitSignalVerdict::OpportunisticSell {
            target_cumulative_exit_pct: dec!(1.0),
            detail: "would sell".to_owned(),
        },
        called: Arc::clone(&opp_called),
    });
    let composite = CompositeExitSignalEvaluator::new(reinference, opportunistic);
    let intent = sample_intent(QuantRuntimeMode::AutoExecution);
    let lot = sample_lot();
    let verdict = composite.evaluate(with_ctx(&intent, &lot)).await;
    assert!(matches!(
        verdict,
        ExitSignalVerdict::ThesisInvalidated { .. }
    ));
    assert!(
        !opp_called.load(Ordering::SeqCst),
        "opportunistic must not run once the thesis is invalidated"
    );
}

#[tokio::test]
async fn reinference_indeterminate_short_circuits() {
    let opp_called = Arc::new(AtomicBool::new(false));
    let reinference: Arc<dyn ExitSignalEvaluator> = Arc::new(FixedEvaluator {
        verdict: ExitSignalVerdict::Indeterminate {
            detail: "disabled".to_owned(),
        },
        called: Arc::new(AtomicBool::new(false)),
    });
    let opportunistic: Arc<dyn ExitSignalEvaluator> = Arc::new(FixedEvaluator {
        verdict: ExitSignalVerdict::OpportunisticSell {
            target_cumulative_exit_pct: dec!(1.0),
            detail: "would sell".to_owned(),
        },
        called: Arc::clone(&opp_called),
    });
    let composite = CompositeExitSignalEvaluator::new(reinference, opportunistic);
    let intent = sample_intent(QuantRuntimeMode::AutoExecution);
    let lot = sample_lot();
    let verdict = composite.evaluate(with_ctx(&intent, &lot)).await;
    assert!(matches!(verdict, ExitSignalVerdict::Indeterminate { .. }));
    assert!(
        !opp_called.load(Ordering::SeqCst),
        "opportunistic requires re-inference to hold (thesis checkable)"
    );
}

#[tokio::test]
async fn opportunistic_runs_when_reinference_holds() {
    let opp_called = Arc::new(AtomicBool::new(false));
    let reinference: Arc<dyn ExitSignalEvaluator> = Arc::new(FixedEvaluator {
        verdict: ExitSignalVerdict::Holds,
        called: Arc::new(AtomicBool::new(false)),
    });
    let opportunistic: Arc<dyn ExitSignalEvaluator> = Arc::new(FixedEvaluator {
        verdict: ExitSignalVerdict::OpportunisticSell {
            target_cumulative_exit_pct: dec!(0.5),
            detail: "edge".to_owned(),
        },
        called: Arc::clone(&opp_called),
    });
    let composite = CompositeExitSignalEvaluator::new(reinference, opportunistic);
    let intent = sample_intent(QuantRuntimeMode::AutoExecution);
    let lot = sample_lot();
    let verdict = composite.evaluate(with_ctx(&intent, &lot)).await;
    assert!(matches!(
        verdict,
        ExitSignalVerdict::OpportunisticSell { .. }
    ));
    assert!(opp_called.load(Ordering::SeqCst));
}

#[tokio::test]
async fn opportunistic_fires_when_enabled_and_score_strong() {
    let evaluator = build_evaluator(Some(strong_score()), true, false);
    let intent = sample_intent(QuantRuntimeMode::AutoExecution);
    let lot = sample_lot();
    match evaluator.evaluate(with_ctx(&intent, &lot)).await {
        ExitSignalVerdict::OpportunisticSell {
            target_cumulative_exit_pct,
            ..
        } => assert_eq!(target_cumulative_exit_pct, dec!(1.0)),
        other => panic!("expected opportunistic sell, got {other:?}"),
    }
}

#[tokio::test]
async fn opportunistic_disabled_config_never_submits() {
    let evaluator = build_evaluator(Some(strong_score()), false, false);
    let intent = sample_intent(QuantRuntimeMode::AutoExecution);
    let lot = sample_lot();
    assert_eq!(
        evaluator.evaluate(with_ctx(&intent, &lot)).await,
        ExitSignalVerdict::Holds
    );
}

#[tokio::test]
async fn opportunistic_shadow_mode_holds() {
    let evaluator = build_evaluator(Some(strong_score()), true, true);
    let intent = sample_intent(QuantRuntimeMode::AutoExecution);
    let lot = sample_lot();
    assert_eq!(
        evaluator.evaluate(with_ctx(&intent, &lot)).await,
        ExitSignalVerdict::Holds,
        "shadow mode audits but never submits"
    );
}

#[tokio::test]
async fn opportunistic_low_alpha_holds() {
    let weak = SellScore {
        exit_alpha_bps: Bps::new(dec!(10)), // below the 50 bps default floor
        ..strong_score()
    };
    let evaluator = build_evaluator(Some(weak), true, false);
    let intent = sample_intent(QuantRuntimeMode::AutoExecution);
    let lot = sample_lot();
    assert_eq!(
        evaluator.evaluate(with_ctx(&intent, &lot)).await,
        ExitSignalVerdict::Holds
    );
}

#[tokio::test]
async fn opportunistic_unavailable_scorer_holds() {
    let evaluator = build_evaluator(None, true, false);
    let intent = sample_intent(QuantRuntimeMode::AutoExecution);
    let lot = sample_lot();
    assert_eq!(
        evaluator.evaluate(with_ctx(&intent, &lot)).await,
        ExitSignalVerdict::Holds
    );
}

#[tokio::test]
async fn opportunistic_skips_non_auto_execution_intents() {
    let evaluator = build_evaluator(Some(strong_score()), true, false);
    let intent = sample_intent(QuantRuntimeMode::SemiAuto);
    let lot = sample_lot();
    assert_eq!(
        evaluator.evaluate(with_ctx(&intent, &lot)).await,
        ExitSignalVerdict::Holds
    );
}
