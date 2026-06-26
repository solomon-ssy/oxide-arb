//! Entry-plan derivation for published recommendations.

use chrono::{DateTime, Utc};
use quant_pivot_models::{
    enums::quant::EntryTriggerKind,
    runtime_config::RuntimeConfig,
    types::{Bps, EntryPlan, Usd},
};
use quant_pivot_research::model::SignalCandidate;
use rust_decimal::Decimal;

/// Derive the production [`EntryPlan`] for one recommendation.
///
/// Default path is [`EntryTriggerKind::LimitPrice`] with trigger and limit at
/// `entry_price_ref`. When `allow_market_orders` is enabled, the plan uses
/// [`EntryTriggerKind::Immediate`] with `limit_price` as a slippage cap.
#[must_use]
pub fn derive_entry_plan(
    candidate: &SignalCandidate,
    as_of: DateTime<Utc>,
    valid_until: DateTime<Utc>,
    config: &RuntimeConfig,
) -> EntryPlan {
    let policy = &config.execution.entry_order_policy;
    let entry_price = candidate.entry_price_ref;
    let (trigger_kind, trigger_price, limit_price, cancel_if_not_triggered) =
        if policy.allow_market_orders {
            (EntryTriggerKind::Immediate, None, Some(entry_price), false)
        } else {
            (
                EntryTriggerKind::LimitPrice,
                Some(entry_price),
                Some(entry_price),
                true,
            )
        };

    EntryPlan {
        trigger_kind,
        trigger_price,
        limit_price,
        max_slippage_bps: Bps::new(Decimal::from(policy.max_slippage_bps)),
        valid_from: as_of,
        valid_until,
        min_depth_usd: Usd::new(parse_decimal_lossless(
            &config.data_quality.min_book_depth_usd.value,
        )),
        max_book_age_ms: config.data_quality.max_book_age_ms,
        confirmation_window_secs: policy.confirmation_window_secs,
        cancel_if_not_triggered,
        entry_reason: candidate.model_explanation.headline.clone(),
    }
}

fn parse_decimal_lossless(value: &str) -> Decimal {
    value.trim().parse::<Decimal>().unwrap_or(Decimal::ZERO)
}

#[cfg(test)]
mod tests {
    use chrono::{TimeZone, Utc};
    use quant_pivot_models::{
        enums::quant::{EntryTriggerKind, OutcomeSide},
        runtime_config::RuntimeConfig,
        types::{MarketId, ModelRunId, Price, Probability, SignalCandidateId, TokenId},
    };
    use quant_pivot_research::model::{ModelExplanation, SignalCandidate};
    use rust_decimal_macros::dec;

    use super::derive_entry_plan;

    fn candidate(entry_price: Price) -> SignalCandidate {
        SignalCandidate {
            signal_candidate_id: SignalCandidateId::from_v7(),
            model_run_id: ModelRunId::from_v7(),
            market_id: MarketId::new("0xmarket"),
            token_id: TokenId::new("token-1"),
            outcome_side: OutcomeSide::Yes,
            composite_score: Probability::new(dec!(0.75)),
            confidence: Probability::new(dec!(0.80)),
            expected_return_bps: dec!(5000),
            downside_bps: dec!(1000),
            entry_price_ref: entry_price,
            suggested_horizon_secs: 3_600,
            factor_breakdown: Vec::new(),
            model_explanation: ModelExplanation {
                headline: "headline".to_owned(),
                top_positive: Vec::new(),
                top_negative: Vec::new(),
            },
            rejection_warnings: Vec::new(),
            rank_before_portfolio: 1,
            liquidity_score: Probability::ZERO,
            data_quality_score: Probability::ZERO,
            model_score_percentile: Probability::ZERO,
            as_of: Utc
                .with_ymd_and_hms(2026, 6, 1, 0, 0, 0)
                .single()
                .expect("valid time"),
        }
    }

    #[test]
    fn limit_price_entry_sets_trigger_and_limit_to_entry_price_ref() {
        let entry_price = Price::new(dec!(0.50));
        let as_of = Utc
            .with_ymd_and_hms(2026, 6, 1, 0, 0, 0)
            .single()
            .expect("valid time");
        let valid_until = as_of + chrono::Duration::seconds(3_600);
        let config = RuntimeConfig::default();

        let plan = derive_entry_plan(&candidate(entry_price), as_of, valid_until, &config);

        assert_eq!(plan.trigger_kind, EntryTriggerKind::LimitPrice);
        assert_eq!(plan.trigger_price, Some(entry_price));
        assert_eq!(plan.limit_price, Some(entry_price));
        assert!(plan.cancel_if_not_triggered);
        assert_eq!(plan.valid_from, as_of);
        assert_eq!(plan.valid_until, valid_until);
    }
}
