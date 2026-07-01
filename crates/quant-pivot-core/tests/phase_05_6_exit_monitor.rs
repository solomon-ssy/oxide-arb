//! Phase 05.6 — exit-monitor decision-ladder tests (pure `decide_exit`).
//!
//! Exercise the deterministic priority ladder and the exit-monitor health
//! readiness handle without Postgres or a live venue.

use chrono::{Duration, Utc};
use quant_pivot_core::execution::{
    ExitDecision, ExitMonitorHealthHandle, ExitMonitorInput, ExitSignalVerdict, decide_exit,
};
use quant_pivot_models::{
    domain::PositionInfo,
    enums::{
        common::{MarketCategory, OrderType},
        execution::{ExitReason, KillSwitchState, PositionLedgerState},
        quant::{AccountSource, ExitSettlementMode, ExitTriggerKind, OutcomeSide, RedeemPolicy},
    },
    runtime_config::{EmergencyExitKind, EmergencyExitPolicy},
    types::{
        Bps, ExitPolicySpec, MarketId, OpportunisticExitState, OrderIntentId, PartialExitNode,
        PositionId, Price, Probability, Shares, TokenId, TrailingStop, Usd,
    },
};
use rust_decimal::Decimal;
use rust_decimal_macros::dec;

fn lot(shares: Decimal, avg_price: Decimal) -> PositionInfo {
    PositionInfo {
        position_id: PositionId::from_v7(),
        order_intent_id: OrderIntentId::from_v7(),
        token_id: TokenId::new("token-1"),
        market_id: MarketId::new("0xmkt"),
        event_id: None,
        category: MarketCategory::Politics,
        side: OutcomeSide::Yes,
        state: PositionLedgerState::Open,
        shares: Shares::new(shares),
        avg_price: Price::new(avg_price),
        cost_usd: Shares::new(shares) * Price::new(avg_price),
        realized_pnl_usd: Usd::ZERO,
        source: AccountSource::Polymarket,
        opened_at: Utc::now() - Duration::hours(2),
        updated_at: Utc::now(),
        closed_at: None,
    }
}

const fn empty_policy() -> ExitPolicySpec {
    ExitPolicySpec {
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
        entry_reference_price: Price::new(dec!(0.50)),
        entry_composite_score: Probability::new(dec!(0.80)),
    }
}

fn input(policy: ExitPolicySpec, mark: Option<Decimal>, kill: KillSwitchState) -> ExitMonitorInput {
    ExitMonitorInput {
        lot: lot(dec!(100), dec!(0.50)),
        exit_policy: policy,
        mark_price: mark.map(Price::new),
        book_fresh: true,
        market_abnormal: false,
        kill_switch: kill,
        emergency_policy: EmergencyExitPolicy {
            kind: EmergencyExitKind::LiquidateAll,
            max_slippage_bps: 100,
        },
        peak_mark_price: None,
        signal: ExitSignalVerdict::Holds,
        executed_partial_exit_node_ids: Vec::new(),
        opportunistic_exit_state: OpportunisticExitState::default(),
        min_opportunistic_clip_pct: Decimal::ZERO,
        now: Utc::now(),
    }
}

const fn submit_reason(decision: &ExitDecision) -> Option<ExitReason> {
    match decision {
        ExitDecision::SubmitExitOrder { reason, .. } => Some(*reason),
        _ => None,
    }
}

#[test]
fn emergency_liquidate_all_submits_fok() {
    let i = input(
        empty_policy(),
        Some(dec!(0.50)),
        KillSwitchState::EmergencyHalted,
    );
    match decide_exit(&i) {
        ExitDecision::SubmitExitOrder { reason, order, .. } => {
            assert_eq!(reason, ExitReason::KillSwitchEmergency);
            assert_eq!(order.order_type, OrderType::Fok);
            assert_eq!(order.shares, Shares::new(dec!(100)));
        }
        other => panic!("expected emergency submit, got {other:?}"),
    }
}

#[test]
fn emergency_manual_only_routes_to_manual() {
    let mut i = input(
        empty_policy(),
        Some(dec!(0.50)),
        KillSwitchState::EmergencyHalted,
    );
    i.emergency_policy.kind = EmergencyExitKind::ManualOnly;
    assert!(matches!(
        decide_exit(&i),
        ExitDecision::RequireManualReview {
            reason: ExitReason::KillSwitchEmergency
        }
    ));
}

#[test]
fn data_stale_routes_to_manual() {
    let mut i = input(empty_policy(), None, KillSwitchState::Closed);
    i.book_fresh = false;
    assert!(matches!(
        decide_exit(&i),
        ExitDecision::RequireManualReview {
            reason: ExitReason::DataStale
        }
    ));
}

#[test]
fn market_abnormal_routes_to_manual() {
    let mut i = input(empty_policy(), Some(dec!(0.50)), KillSwitchState::Closed);
    i.market_abnormal = true;
    assert!(matches!(
        decide_exit(&i),
        ExitDecision::RequireManualReview {
            reason: ExitReason::MarketAbnormal
        }
    ));
}

#[test]
fn stop_loss_triggers_exit_order() {
    let mut policy = empty_policy();
    policy.stop_loss_price = Some(Price::new(dec!(0.45)));
    let i = input(policy, Some(dec!(0.44)), KillSwitchState::Closed);
    assert_eq!(submit_reason(&decide_exit(&i)), Some(ExitReason::StopLoss));
}

#[test]
fn trailing_stop_folds_into_effective_stop() {
    let mut policy = empty_policy();
    policy.trailing_stop = Some(TrailingStop {
        trail_bps: Bps::new(dec!(100)), // 1%
        activation_price: None,
    });
    let mut i = input(policy, Some(dec!(0.59)), KillSwitchState::Closed);
    i.peak_mark_price = Some(Price::new(dec!(0.60))); // trail floor = 0.594
    assert_eq!(submit_reason(&decide_exit(&i)), Some(ExitReason::StopLoss));
}

#[test]
fn take_profit_triggers_exit_order() {
    let mut policy = empty_policy();
    policy.take_profit_price = Some(Price::new(dec!(0.70)));
    let i = input(policy, Some(dec!(0.72)), KillSwitchState::Closed);
    match decide_exit(&i) {
        ExitDecision::SubmitExitOrder { reason, order, .. } => {
            assert_eq!(reason, ExitReason::TakeProfit);
            assert_eq!(order.limit_price, Price::new(dec!(0.70)));
        }
        other => panic!("expected take-profit submit, got {other:?}"),
    }
}

#[test]
fn time_exit_due_triggers_exit() {
    let mut policy = empty_policy();
    policy.time_exit_at = Some(Utc::now() - Duration::seconds(1));
    let i = input(policy, Some(dec!(0.50)), KillSwitchState::Closed);
    assert_eq!(submit_reason(&decide_exit(&i)), Some(ExitReason::TimeExit));
}

#[test]
fn signal_invalidated_triggers_exit() {
    let mut i = input(empty_policy(), Some(dec!(0.50)), KillSwitchState::Closed);
    i.signal = ExitSignalVerdict::ThesisInvalidated {
        detail: "thesis broke".to_owned(),
    };
    assert_eq!(
        submit_reason(&decide_exit(&i)),
        Some(ExitReason::SignalInvalidated)
    );
}

#[test]
fn partial_exit_node_sells_pct() {
    let mut policy = empty_policy();
    policy.partial_exit_nodes = vec![PartialExitNode {
        node_id: "n1".to_owned(),
        trigger_kind: ExitTriggerKind::TakeProfit,
        trigger_value: dec!(0.65),
        sell_pct: dec!(0.5),
        min_price: None,
        valid_after: None,
        valid_until: None,
        reason: "scale out".to_owned(),
    }];
    let i = input(policy, Some(dec!(0.66)), KillSwitchState::Closed);
    match decide_exit(&i) {
        ExitDecision::SubmitExitOrder { reason, order, .. } => {
            assert_eq!(reason, ExitReason::PartialExit);
            assert_eq!(order.shares, Shares::new(dec!(50)));
        }
        other => panic!("expected partial-exit submit, got {other:?}"),
    }
}

#[test]
fn manual_review_at_routes_to_manual_between_time_and_take_profit() {
    let mut policy = empty_policy();
    policy.manual_review_at = Some(Utc::now() - Duration::seconds(1));
    policy.take_profit_price = Some(Price::new(dec!(0.70)));
    let i = input(policy, Some(dec!(0.80)), KillSwitchState::Closed);
    assert!(matches!(
        decide_exit(&i),
        ExitDecision::RequireManualReview {
            reason: ExitReason::Manual
        }
    ));
}

#[test]
fn manual_review_at_fires_after_time_exit_priority() {
    let mut policy = empty_policy();
    policy.time_exit_at = Some(Utc::now() - Duration::seconds(1));
    policy.manual_review_at = Some(Utc::now() - Duration::seconds(1));
    let i = input(policy, Some(dec!(0.50)), KillSwitchState::Closed);
    assert_eq!(submit_reason(&decide_exit(&i)), Some(ExitReason::TimeExit));
}

#[test]
fn partial_exit_node_skips_already_executed_node() {
    let mut policy = empty_policy();
    policy.partial_exit_nodes = vec![PartialExitNode {
        node_id: "n1".to_owned(),
        trigger_kind: ExitTriggerKind::TakeProfit,
        trigger_value: dec!(0.65),
        sell_pct: dec!(0.5),
        min_price: None,
        valid_after: None,
        valid_until: None,
        reason: "scale out".to_owned(),
    }];
    let mut i = input(policy, Some(dec!(0.66)), KillSwitchState::Closed);
    i.executed_partial_exit_node_ids = vec!["n1".to_owned()];
    assert_eq!(decide_exit(&i), ExitDecision::Hold);
}

#[test]
fn partial_exit_node_carries_node_id() {
    let mut policy = empty_policy();
    policy.partial_exit_nodes = vec![PartialExitNode {
        node_id: "n1".to_owned(),
        trigger_kind: ExitTriggerKind::TakeProfit,
        trigger_value: dec!(0.65),
        sell_pct: dec!(0.5),
        min_price: None,
        valid_after: None,
        valid_until: None,
        reason: "scale out".to_owned(),
    }];
    let i = input(policy, Some(dec!(0.66)), KillSwitchState::Closed);
    match decide_exit(&i) {
        ExitDecision::SubmitExitOrder {
            reason,
            order,
            partial_exit_node_id,
            ..
        } => {
            assert_eq!(reason, ExitReason::PartialExit);
            assert_eq!(order.shares, Shares::new(dec!(50)));
            assert_eq!(partial_exit_node_id.as_deref(), Some("n1"));
        }
        other => panic!("expected partial-exit submit, got {other:?}"),
    }
}

#[test]
fn opportunistic_sell_is_lowest_tier() {
    let mut i = input(empty_policy(), Some(dec!(0.50)), KillSwitchState::Closed);
    i.opportunistic_exit_state.denominator_shares = Some(Shares::new(dec!(100)));
    i.signal = ExitSignalVerdict::OpportunisticSell {
        target_cumulative_exit_pct: dec!(0.3),
        detail: "model edge".to_owned(),
    };
    match decide_exit(&i) {
        ExitDecision::SubmitExitOrder { reason, order, .. } => {
            assert_eq!(reason, ExitReason::Opportunistic);
            assert_eq!(order.shares, Shares::new(dec!(30)));
        }
        other => panic!("expected opportunistic submit, got {other:?}"),
    }
}

#[test]
fn opportunistic_delta_requires_frozen_denominator() {
    let mut i = input(empty_policy(), Some(dec!(0.50)), KillSwitchState::Closed);
    i.signal = ExitSignalVerdict::OpportunisticSell {
        target_cumulative_exit_pct: dec!(0.3),
        detail: String::new(),
    };
    assert!(
        matches!(decide_exit(&i), ExitDecision::Hold),
        "missing frozen denominator must fail-closed to hold"
    );
}

/// Regression (money-critical churn bug): a repeated opportunistic verdict at
/// the same cumulative target sells only the incremental delta and then holds —
/// it never re-fires the whole fraction each tick.
#[test]
fn opportunistic_cumulative_delta_no_churn() {
    let mut i = input(empty_policy(), Some(dec!(0.50)), KillSwitchState::Closed);
    // Denominator frozen at 100 shares; 30 already opportunistically sold.
    i.opportunistic_exit_state.denominator_shares = Some(Shares::new(dec!(100)));
    i.opportunistic_exit_state.cumulative_sold_shares = Shares::new(dec!(30));
    i.signal = ExitSignalVerdict::OpportunisticSell {
        target_cumulative_exit_pct: dec!(0.3),
        detail: String::new(),
    };
    assert!(
        matches!(decide_exit(&i), ExitDecision::Hold),
        "target already met ⇒ hold (no churn)"
    );

    // Raising the target sells only the incremental delta (0.5×100 − 30 = 20),
    // carrying the frozen denominator so the dispatcher can advance the total.
    i.signal = ExitSignalVerdict::OpportunisticSell {
        target_cumulative_exit_pct: dec!(0.5),
        detail: String::new(),
    };
    match decide_exit(&i) {
        ExitDecision::SubmitExitOrder {
            reason,
            order,
            opportunistic_denominator,
            ..
        } => {
            assert_eq!(reason, ExitReason::Opportunistic);
            assert_eq!(order.shares, Shares::new(dec!(20)));
            assert_eq!(opportunistic_denominator, Some(Shares::new(dec!(100))));
        }
        other => panic!("expected incremental opportunistic sell, got {other:?}"),
    }
}

/// An incremental delta below the min clip fraction holds (avoids dust churn).
#[test]
fn opportunistic_below_min_clip_holds() {
    let mut i = input(empty_policy(), Some(dec!(0.50)), KillSwitchState::Closed);
    i.opportunistic_exit_state.denominator_shares = Some(Shares::new(dec!(100)));
    i.min_opportunistic_clip_pct = dec!(0.5); // require ≥ 50 shares of 100
    i.signal = ExitSignalVerdict::OpportunisticSell {
        target_cumulative_exit_pct: dec!(0.3), // 30 < 50 ⇒ hold
        detail: String::new(),
    };
    assert!(matches!(decide_exit(&i), ExitDecision::Hold));
}

/// The kill-switch `execution_halted` freezes auto-exit: an opportunistic verdict
/// routes to manual review, never an auto-submitted exit.
#[test]
fn execution_halted_blocks_opportunistic_auto_exit() {
    let mut i = input(
        empty_policy(),
        Some(dec!(0.50)),
        KillSwitchState::ExecutionHalted,
    );
    i.opportunistic_exit_state.denominator_shares = Some(Shares::new(dec!(100)));
    i.signal = ExitSignalVerdict::OpportunisticSell {
        target_cumulative_exit_pct: dec!(0.3),
        detail: String::new(),
    };
    assert!(matches!(
        decide_exit(&i),
        ExitDecision::RequireManualReview {
            reason: ExitReason::Opportunistic
        }
    ));
}

#[test]
fn settlement_hold_does_not_take_profit() {
    let mut policy = empty_policy();
    policy.settlement_mode = ExitSettlementMode::HoldToResolution;
    policy.take_profit_price = Some(Price::new(dec!(0.70)));
    let i = input(policy, Some(dec!(0.80)), KillSwitchState::Closed);
    assert_eq!(decide_exit(&i), ExitDecision::Hold);
}

#[test]
fn settlement_hold_still_stops_loss() {
    // Protective exits still fire under hold-to-resolution.
    let mut policy = empty_policy();
    policy.settlement_mode = ExitSettlementMode::HoldToResolution;
    policy.stop_loss_price = Some(Price::new(dec!(0.45)));
    let i = input(policy, Some(dec!(0.40)), KillSwitchState::Closed);
    assert_eq!(submit_reason(&decide_exit(&i)), Some(ExitReason::StopLoss));
}

#[test]
fn execution_halted_marks_triggered_not_auto_submitted() {
    // A would-be stop-loss exit is routed to manual when auto-exit is frozen.
    let mut policy = empty_policy();
    policy.stop_loss_price = Some(Price::new(dec!(0.45)));
    let i = input(policy, Some(dec!(0.44)), KillSwitchState::ExecutionHalted);
    assert!(matches!(
        decide_exit(&i),
        ExitDecision::RequireManualReview {
            reason: ExitReason::StopLoss
        }
    ));
}

#[test]
fn emergency_overrides_stop_loss_priority() {
    let mut policy = empty_policy();
    policy.stop_loss_price = Some(Price::new(dec!(0.45)));
    let i = input(policy, Some(dec!(0.44)), KillSwitchState::EmergencyHalted);
    assert_eq!(
        submit_reason(&decide_exit(&i)),
        Some(ExitReason::KillSwitchEmergency)
    );
}

#[test]
fn decision_is_deterministic_for_same_input() {
    let mut policy = empty_policy();
    policy.stop_loss_price = Some(Price::new(dec!(0.45)));
    let i = input(policy, Some(dec!(0.44)), KillSwitchState::Closed);
    assert_eq!(decide_exit(&i), decide_exit(&i));
}

#[test]
fn holds_when_no_trigger() {
    let i = input(empty_policy(), Some(dec!(0.50)), KillSwitchState::Closed);
    assert_eq!(decide_exit(&i), ExitDecision::Hold);
}

#[test]
fn exit_monitor_health_readiness_window() {
    let health = ExitMonitorHealthHandle::new();
    let now = Utc::now();
    // Not ready before any scan.
    assert!(!health.is_ready(now));
    // Ready right after a scan within the window.
    health.publish(now, 20);
    assert!(health.is_ready(now + Duration::seconds(10)));
    // Not ready once the healthy window has elapsed.
    assert!(!health.is_ready(now + Duration::seconds(21)));
}
