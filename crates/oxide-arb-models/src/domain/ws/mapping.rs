//! [`CoreEvent`] → wire-envelope projection.
//!
//! A single exhaustive `match` maps each event to its channel, optional market
//! scope, and JSON payload. The market-scoped `MarketBookUpdate` is just another
//! arm yielding `Some(market_id)`; [`SubscriptionKey::new`] then normalizes the
//! scope, so there is no special-case early return and no unreachable arm.

use serde_json::Value;

use crate::{
    domain::{
        ControlFactorMaterializationRunView, CoreEvent, OpportunityView, TradeView,
        ws::{
            channel::{SubscriptionKey, WsChannel},
            envelope::WsEnvelope,
        },
    },
    types::MarketId,
};

/// Map a [`CoreEvent`] to its fan-out [`SubscriptionKey`] and [`WsEnvelope`].
///
/// Returns `None` only if the payload fails to serialize, which never happens
/// for the well-formed domain types carried by [`CoreEvent`].
#[must_use]
pub fn event_envelope(event: &CoreEvent) -> Option<(SubscriptionKey, WsEnvelope)> {
    let (channel, market, data): (WsChannel, Option<MarketId>, Value) = match event {
        CoreEvent::SystemStatusChanged(status) => (
            WsChannel::SystemStatus,
            None,
            serde_json::to_value(status).ok()?,
        ),
        CoreEvent::Alert(alert) => (
            WsChannel::SystemAlert,
            None,
            serde_json::to_value(alert).ok()?,
        ),
        CoreEvent::CircuitBreakerChanged(risk) => (
            WsChannel::RiskCircuitBreaker,
            None,
            serde_json::to_value(risk).ok()?,
        ),
        CoreEvent::PositionChanged(position) => (
            WsChannel::RiskPositionUpdate,
            None,
            serde_json::to_value(position).ok()?,
        ),
        CoreEvent::MarketResolved { market_id, outcome } => (
            WsChannel::MarketResolved,
            None,
            serde_json::json!({ "market_id": market_id, "outcome": outcome }),
        ),
        CoreEvent::MarketBookUpdate { market_id, view } => (
            WsChannel::MarketBookUpdate,
            Some(market_id.clone()),
            serde_json::to_value(view.as_ref()).ok()?,
        ),
        CoreEvent::ControlPublished {
            publication_id,
            mode,
        } => (
            WsChannel::ControlPublished,
            None,
            serde_json::json!({ "publication_id": publication_id, "mode": mode }),
        ),
        CoreEvent::ConfigActivated { version_id } => (
            WsChannel::ConfigActivated,
            None,
            serde_json::json!({ "version_id": version_id }),
        ),
        // Both feed channels project through the same outbound views as their
        // REST counterparts, so WS pushes can never leak internal columns
        // (`scored_snapshot`, post-trade lease bookkeeping, calibration internals).
        CoreEvent::OpportunityDetected(opp) => (
            WsChannel::OpportunityDetected,
            None,
            serde_json::to_value(OpportunityView::from(opp)).ok()?,
        ),
        CoreEvent::TradeFilled(trade) => (
            WsChannel::TradeFilled,
            None,
            serde_json::to_value(TradeView::from(trade.clone())).ok()?,
        ),
        CoreEvent::TradeSettled {
            trade_id,
            outcome,
            pnl,
        } => (
            WsChannel::TradeSettled,
            None,
            serde_json::json!({ "trade_id": trade_id, "outcome": outcome, "pnl": pnl }),
        ),
        CoreEvent::PnlUpdate { daily, total } => (
            WsChannel::PnlUpdate,
            None,
            serde_json::json!({ "daily": daily, "total": total }),
        ),
        CoreEvent::MaterializationRunUpdated(run) => (
            WsChannel::MaterializationRunUpdate,
            None,
            serde_json::to_value(ControlFactorMaterializationRunView::from(run.clone())).ok()?,
        ),
    };

    let key = SubscriptionKey::new(channel, market);
    Some((key, WsEnvelope::channel(channel, data)))
}

#[cfg(test)]
mod tests {
    use super::event_envelope;
    use crate::{
        domain::{
            CoreEvent, MarketBookView, RiskEngineStateView,
            calibration::{BucketKey, CalibrationSnapshot},
            opportunity::{EndgameMeta, Opportunity},
            trade::TradeInfo,
            ws::channel::{SubscriptionKey, WsChannel},
        },
        enums::{
            calibration::{DurationBucket, PriceZone},
            common::{
                ExecutionMode, MarketCategory, Side, StalenessLevel, TradeBusinessOutcome,
                TradeState,
            },
            opportunity::PayoutModel,
            risk::BreakerStateName,
        },
        types::{
            Bps, EventId, ExecutionId, MarketId, OpportunityId, OrderId, Price, ReservationId,
            Shares, TokenId, TradeId, Usd,
        },
    };
    use chrono::{NaiveDate, Utc};
    use rust_decimal_macros::dec;

    fn test_opportunity() -> Opportunity {
        Opportunity {
            opportunity_id: OpportunityId::from_v7(),
            market_id: MarketId::new("0xabc"),
            event_id: EventId::new("e1"),
            token_id: TokenId::new("tok1"),
            side: Side::Buy,
            payout_model: PayoutModel::DirectionalSettlement {
                projected_payout_if_correct: Usd::new(dec!(100)),
                expected_payout: Usd::new(dec!(95)),
                predicted_side: Side::Buy,
            },
            shares: Shares::new(dec!(100)),
            entry_price: Price::new(dec!(0.92)),
            total_cost: Usd::new(dec!(92)),
            total_fees: Usd::new(dec!(0.40)),
            net_profit: Usd::new(dec!(7.60)),
            expected_net_profit: Usd::new(dec!(2.60)),
            edge_bps: Bps::new(dec!(300)),
            resolution_adjust: dec!(0.95),
            depth_used_pct: dec!(10),
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
            calibration: CalibrationSnapshot {
                bucket_key: BucketKey {
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
        }
    }

    fn test_trade() -> TradeInfo {
        let now = Utc::now();
        TradeInfo {
            trade_id: TradeId::from_v7(),
            execution_id: ExecutionId::from_v7(),
            reservation_id: ReservationId::from_v7(),
            opportunity_id: OpportunityId::from_v7(),
            market_id: MarketId::new("0xabc"),
            event_id: EventId::new("e1"),
            token_id: TokenId::new("tok1"),
            side: Side::Buy,
            shares: Shares::new(dec!(80)),
            price: Price::new(dec!(0.93)),
            cost_usd: Usd::new(dec!(74.4)),
            fee_usd: Usd::new(dec!(0.5)),
            detected_edge_bps: Some(Bps::new(dec!(300))),
            detected_profit_usd: Some(Usd::new(dec!(4.5))),
            net_profit_usd: Some(Usd::new(dec!(4.1))),
            order_id: Some(OrderId::new("ord1")),
            tx_hash: None,
            state: TradeState::FillObserved,
            business_outcome: Some(TradeBusinessOutcome::Success),
            scored_snapshot: serde_json::json!({ "internal": "forensic blob" }),
            category: MarketCategory::Politics,
            needs_reconcile: false,
            reconcile_resolution: None,
            reconciled_at: None,
            reconcile_note: None,
            post_trade_claim_owner: Some("relay-1".to_owned()),
            post_trade_claimed_at: Some(now),
            post_trade_attempts: 2,
            execution_mode: ExecutionMode::Paper,
            latency_ms: Some(42),
            error_message: None,
            submitted_at: Some(now),
            confirmed_at: Some(now),
            created_at: now,
            updated_at: now,
        }
    }

    fn test_risk_view() -> RiskEngineStateView {
        RiskEngineStateView {
            breaker_state: BreakerStateName::Closed,
            breaker_level: None,
            is_halted: false,
            halt_reason: None,
            cooldown_until: None,
            total_exposure: Usd::ZERO,
            hourly_loss_usd: Usd::ZERO,
            hourly_fee_usd: Usd::ZERO,
            hourly_trade_count: 0,
            hourly_success_count: 0,
            hourly_miss_count: 0,
            daily_pnl: Usd::ZERO,
            daily_loss_usd: Usd::ZERO,
            daily_fee_usd: Usd::ZERO,
            daily_budget_spent: Usd::ZERO,
            daily_trade_count: 0,
            daily_success_count: 0,
            daily_miss_count: 0,
            daily_window_start: NaiveDate::from_ymd_opt(2026, 1, 1).expect("valid date"),
            weekly_loss_usd: Usd::ZERO,
            weekly_trade_count: 0,
            consecutive_misses: 0,
            hwm_equity: Usd::ZERO,
            last_emergency_at: None,
            last_emergency_reason: None,
            snapshot_at: "2026-01-01T00:00:00Z".parse().expect("valid timestamp"),
        }
    }

    #[test]
    fn trade_filled_projects_outbound_trade_view() {
        let trade = test_trade();
        let trade_id = trade.trade_id.clone();
        let (key, envelope) = event_envelope(&CoreEvent::TradeFilled(trade)).expect("trade maps");
        assert_eq!(key, SubscriptionKey::global(WsChannel::TradeFilled));

        let data = &envelope.data;
        assert_eq!(data["trade_id"], trade_id.to_string());
        assert_eq!(data["market_id"], "0xabc");
        // `opportunity_id` is part of the contract: it joins the trade to its
        // audit trail for the decision-chain timeline.
        assert!(
            data.get("opportunity_id").is_some(),
            "`opportunity_id` must survive the trade.filled projection"
        );
        // The WS push shares the REST `TradeView` projection: forensic /
        // persistence-internal columns must never cross the wire.
        for stripped in [
            "scored_snapshot",
            "execution_id",
            "reservation_id",
            "post_trade_claim_owner",
            "post_trade_claimed_at",
            "post_trade_attempts",
        ] {
            assert!(
                data.get(stripped).is_none(),
                "`{stripped}` must be stripped from trade.filled"
            );
        }
        assert!(
            data.get("needs_reconcile").is_some(),
            "`needs_reconcile` is operator-visible money-state, not relay internals"
        );
    }

    #[test]
    fn opportunity_detected_projects_outbound_view() {
        let opportunity = test_opportunity();
        let opportunity_id = opportunity.opportunity_id.clone();
        let (key, envelope) =
            event_envelope(&CoreEvent::OpportunityDetected(opportunity)).expect("opp maps");
        assert_eq!(key, SubscriptionKey::global(WsChannel::OpportunityDetected));

        let data = &envelope.data;
        assert_eq!(data["opportunity_id"], opportunity_id.to_string());
        assert_eq!(data["market_id"], "0xabc");
        assert_eq!(data["edge_bps"], "300");
        assert_eq!(data["expected_net_profit_usd"], "2.60");
        // Detection internals stay off the feed wire.
        let object = data.as_object().expect("object payload");
        assert_eq!(
            object.len(),
            5,
            "feed view carries exactly its five contract fields: {object:?}"
        );
        for stripped in ["calibration", "meta", "payout_model", "shares"] {
            assert!(
                object.get(stripped).is_none(),
                "`{stripped}` must be stripped from opportunity.detected"
            );
        }
    }

    #[test]
    fn market_book_update_maps_to_market_scoped_key() {
        let event = CoreEvent::MarketBookUpdate {
            market_id: MarketId::new("0xabc"),
            view: Box::new(MarketBookView {
                market_id: MarketId::new("0xabc"),
                yes: None,
                no: None,
            }),
        };
        let (key, envelope) = event_envelope(&event).expect("book update maps");
        assert_eq!(
            key,
            SubscriptionKey::scoped(WsChannel::MarketBookUpdate, MarketId::new("0xabc"))
        );
        assert_eq!(envelope.kind.as_str(), "market.book_update");
    }

    #[test]
    fn circuit_breaker_changed_maps_full_state_view_to_global_risk_key() {
        let event = CoreEvent::CircuitBreakerChanged(test_risk_view());
        let (key, envelope) = event_envelope(&event).expect("breaker maps");
        assert_eq!(key, SubscriptionKey::global(WsChannel::RiskCircuitBreaker));
        assert_eq!(envelope.kind.as_str(), "risk.circuit_breaker");
        assert_eq!(envelope.data["breaker_state"], "closed");
        assert_eq!(
            envelope.data["snapshot_at"].as_str(),
            Some("2026-01-01T00:00:00Z")
        );
    }

    #[test]
    fn market_resolved_is_globally_scoped() {
        let event = CoreEvent::MarketResolved {
            market_id: MarketId::new("0xabc"),
            outcome: true,
        };
        let (key, _envelope) = event_envelope(&event).expect("resolved maps");
        assert_eq!(key, SubscriptionKey::global(WsChannel::MarketResolved));
        assert_eq!(key.market, None, "market rides in the payload, not the key");
    }
}
