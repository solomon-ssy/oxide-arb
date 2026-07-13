//! Entry-plan derivation for published recommendations.

use chrono::{DateTime, Utc};
use quant_pivot_error::{QuantError, QuantResult};
use quant_pivot_models::{
    runtime_config::RuntimeConfig,
    types::{Bps, EntryOrderPolicy, EntryPlan, EntryTrigger, Usd},
};
use quant_pivot_research::model::SignalCandidate;
use rust_decimal::Decimal;

/// Derive the production [`EntryPlan`] for one recommendation.
///
/// Until a published trade-policy cohort is bound, the fail-safe projection is
/// an immediately armed, bounded post-only order. No runtime flag selects a
/// trading strategy.
pub fn derive_entry_plan(
    candidate: &SignalCandidate,
    valid_from: DateTime<Utc>,
    valid_until: DateTime<Utc>,
    config: &RuntimeConfig,
) -> QuantResult<EntryPlan> {
    let policy = &config.execution.entry_order_policy;
    let entry_price = candidate.entry_price_ref;

    let min_depth_usd = policy
        .min_entry_book_depth_usd
        .value
        .trim()
        .parse::<Decimal>()
        .map_err(|error| {
            QuantError::config(format!(
                "execution.entry_order_policy.min_entry_book_depth_usd is not a valid decimal: {error}"
            ))
        })?;

    Ok(EntryPlan {
        trade_policy: None,
        trigger: EntryTrigger::Immediate,
        order_policy: EntryOrderPolicy::Passive {
            limit_price: entry_price,
            post_only: true,
        },
        max_slippage_bps: Bps::new(Decimal::from(policy.max_slippage_bps)),
        valid_from,
        valid_until,
        min_depth_usd: Usd::new(min_depth_usd),
        max_book_age_ms: config.data_quality.max_book_age_ms,
        cancel_if_not_triggered: true,
        entry_reason: candidate.model_explanation.headline.clone(),
    })
}

#[cfg(test)]
mod tests {
    use chrono::{TimeZone, Utc};
    use quant_pivot_models::{
        enums::quant::OutcomeSide,
        runtime_config::RuntimeConfig,
        types::{
            EntryOrderPolicy, EntryTrigger, MarketId, ModelRunId, Price, Probability,
            SignalCandidateId, TokenId,
        },
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
            win_probability: None,
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
            decision_at: Utc
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

        let plan = derive_entry_plan(&candidate(entry_price), as_of, valid_until, &config)
            .expect("valid entry plan");

        assert_eq!(plan.trigger, EntryTrigger::Immediate);
        assert_eq!(
            plan.order_policy,
            EntryOrderPolicy::Passive {
                limit_price: entry_price,
                post_only: true,
            }
        );
        assert!(plan.cancel_if_not_triggered);
        assert_eq!(plan.valid_from, as_of);
        assert_eq!(plan.valid_until, valid_until);
    }

    #[test]
    fn invalid_minimum_depth_fails_closed() {
        let as_of = Utc
            .with_ymd_and_hms(2026, 6, 1, 0, 0, 0)
            .single()
            .expect("valid time");
        let mut config = RuntimeConfig::default();
        config
            .execution
            .entry_order_policy
            .min_entry_book_depth_usd
            .value = "not-a-decimal".to_owned();

        assert!(
            derive_entry_plan(
                &candidate(Price::new(dec!(0.50))),
                as_of,
                as_of + chrono::Duration::seconds(3_600),
                &config,
            )
            .is_err()
        );
    }
}
