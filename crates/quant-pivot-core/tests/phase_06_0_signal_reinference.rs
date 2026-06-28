//! Phase 06.0 — exit signal re-inference activation tests.

use std::sync::Arc;

use async_trait::async_trait;
use chrono::Utc;
use quant_pivot_core::{
    execution::{ExitSignalContext, ExitSignalEvaluator, ExitSignalVerdict},
    observability::metrics_hub::MetricsHub,
    runtime_config::RuntimeConfigStore,
    service::signal_reinference::{
        ExitSignalReinferer, FreshSignal, ReinferenceSignalEvaluator,
        ReinferenceSignalEvaluatorDeps,
    },
};
use quant_pivot_models::{
    domain::{OrderIntentInfo, PositionInfo},
    enums::{
        common::{MarketCategory, OrderType, Side},
        execution::{ExitState, OrderIntentKind, PositionLedgerState},
        quant::{AccountSource, ApprovalStatus, OrderIntentStatus, OutcomeSide, QuantRuntimeMode},
    },
    runtime_config::RuntimeConfig,
    types::{
        Bps, ContentHash, EventId, ExecutedPartialExitNodes, MarketId, ModelVersionId,
        OrderIntentId, PositionId, Price, Probability, RecommendationId, RuntimeConfigVersionId,
        Shares, TokenId, Usd,
        execution_payload::{EntryOrderSpec, ExitPolicySpec},
    },
};
use rust_decimal_macros::dec;

struct StubReinferer(Option<FreshSignal>);

#[async_trait]
impl ExitSignalReinferer for StubReinferer {
    async fn reinfer(
        &self,
        _intent: &OrderIntentInfo,
        _lot: &PositionInfo,
        _mark_price: Option<Price>,
        _now: chrono::DateTime<Utc>,
    ) -> quant_pivot_error::QuantResult<Option<FreshSignal>> {
        Ok(self.0.clone())
    }
}

fn sample_intent(entry_score: &str) -> OrderIntentInfo {
    OrderIntentInfo {
        order_intent_id: OrderIntentId::from_v7(),
        recommendation_id: RecommendationId::from_v7(),
        runtime_mode: QuantRuntimeMode::AutoExecution,
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
            settlement_policy:
                quant_pivot_models::enums::quant::SettlementPolicy::ExitBeforeResolution,
            manual_review_at: None,
            entry_reference_price: Price::new(dec!(0.5)),
            entry_composite_score: Probability::new(entry_score.parse().unwrap()),
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

fn evaluator_with_config(
    reinferer: StubReinferer,
    mut config: RuntimeConfig,
    shadow_mode: bool,
) -> ReinferenceSignalEvaluator<StubReinferer> {
    config.execution.exit_monitor.signal_reinference.enabled = true;
    config.execution.exit_monitor.signal_reinference.shadow_mode = shadow_mode;
    "0.6".clone_into(
        &mut config
            .execution
            .exit_monitor
            .signal_invalidation_ratio
            .value,
    );
    ReinferenceSignalEvaluator::new(ReinferenceSignalEvaluatorDeps {
        reinferer,
        config: Arc::new(RuntimeConfigStore::new(config)),
        metrics: Arc::new(MetricsHub::new()),
    })
}

#[tokio::test]
async fn shadow_mode_suppresses_thesis_invalidated_exit() {
    let fresh = FreshSignal {
        composite_score: Probability::new(dec!(0.30)),
        expected_return_bps: Bps::new(dec!(100)),
        auto_exec_eligible: true,
    };
    let evaluator =
        evaluator_with_config(StubReinferer(Some(fresh)), RuntimeConfig::default(), true);
    let verdict = evaluator
        .evaluate(ExitSignalContext {
            intent: &sample_intent("0.8"),
            lot: &sample_lot(),
            mark_price: Some(Price::new(dec!(0.45))),
            now: Utc::now(),
        })
        .await;
    assert!(matches!(verdict, ExitSignalVerdict::Indeterminate { .. }));
}

#[tokio::test]
async fn live_mode_triggers_thesis_invalidated_on_score_degradation() {
    let fresh = FreshSignal {
        composite_score: Probability::new(dec!(0.30)),
        expected_return_bps: Bps::new(dec!(100)),
        auto_exec_eligible: true,
    };
    let evaluator =
        evaluator_with_config(StubReinferer(Some(fresh)), RuntimeConfig::default(), false);
    let verdict = evaluator
        .evaluate(ExitSignalContext {
            intent: &sample_intent("0.8"),
            lot: &sample_lot(),
            mark_price: Some(Price::new(dec!(0.45))),
            now: Utc::now(),
        })
        .await;
    assert!(matches!(
        verdict,
        ExitSignalVerdict::ThesisInvalidated { .. }
    ));
}

#[tokio::test]
async fn disabled_reinference_yields_indeterminate() {
    let mut config = RuntimeConfig::default();
    config.execution.exit_monitor.signal_reinference.enabled = false;
    let evaluator = ReinferenceSignalEvaluator::new(ReinferenceSignalEvaluatorDeps {
        reinferer: StubReinferer(Some(FreshSignal {
            composite_score: Probability::new(dec!(0.30)),
            expected_return_bps: Bps::new(dec!(100)),
            auto_exec_eligible: true,
        })),
        config: Arc::new(RuntimeConfigStore::new(config)),
        metrics: Arc::new(MetricsHub::new()),
    });
    let verdict = evaluator
        .evaluate(ExitSignalContext {
            intent: &sample_intent("0.8"),
            lot: &sample_lot(),
            mark_price: None,
            now: Utc::now(),
        })
        .await;
    assert!(matches!(verdict, ExitSignalVerdict::Indeterminate { .. }));
}
