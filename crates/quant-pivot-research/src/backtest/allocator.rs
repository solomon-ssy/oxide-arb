//! Deterministic greedy portfolio allocator (Phase 3.6 placeholder).
//!
//! Phase 03 uses this minimal allocator to produce portfolio-level backtest
//! metrics (turnover / drawdown / category exposure). It sorts candidates by
//! risk-adjusted score and fills each subject to the budget, per-bucket
//! exposure, and liquidity-usage caps. It is **explicitly not** globally optimal;
//! Phase 04's governed `PortfolioPlanner` reuses this same [`PortfolioAllocator`]
//! trait, and Phase 05 may replace the greedy core with an LP/MILP solver.

use std::collections::BTreeMap;

use quant_pivot_error::QuantResult;
use quant_pivot_models::{
    enums::common::MarketCategory,
    types::{EventId, MarketId, SignalCandidateId, Usd},
};
use rust_decimal::Decimal;

use crate::{
    backtest::PortfolioCaps, model::signal::SignalCandidate, precision::RESEARCH_DECIMAL_SCALE,
};

/// A candidate paired with the allocation metadata the caps need.
#[derive(Debug, Clone)]
pub struct CandidateMeta<'a> {
    /// The scored candidate.
    pub candidate: &'a SignalCandidate,
    /// Market category (category exposure cap + breakdown).
    pub category: MarketCategory,
    /// Owning event (event exposure cap), when known.
    pub event_id: Option<EventId>,
    /// Visible liquidity (liquidity-usage cap), when known.
    pub liquidity_usd: Option<Usd>,
}

/// One capital allocation decision.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Allocation {
    /// The allocated candidate.
    pub signal_candidate_id: SignalCandidateId,
    /// Market id.
    pub market_id: MarketId,
    /// Capital allocated (USD); `0` when the candidate was not funded.
    pub allocated_usd: Usd,
    /// Whether the intended size respected the liquidity-usage cap.
    pub liquidity_feasible: bool,
}

/// Allocation request for one cross-section.
pub struct AllocationInput<'a> {
    /// Candidates to consider (any order; sorted internally).
    pub candidates: Vec<CandidateMeta<'a>>,
    /// Portfolio caps.
    pub caps: &'a PortfolioCaps,
}

/// Allocation result.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AllocationOutput {
    /// One entry per input candidate (including zero-funded ones).
    pub allocations: Vec<Allocation>,
}

/// Portfolio allocation strategy. Phase 04's governed planner reuses this trait.
pub trait PortfolioAllocator: Send + Sync {
    /// Allocate capital across the candidates subject to the caps.
    fn allocate(&self, input: &AllocationInput<'_>) -> QuantResult<AllocationOutput>;
}

/// The deterministic greedy allocator.
#[derive(Debug, Clone, Copy, Default)]
pub struct GreedyPortfolioAllocator;

impl GreedyPortfolioAllocator {
    /// Construct the allocator.
    #[must_use]
    pub const fn new() -> Self {
        Self
    }
}

/// Treat a non-positive per-bucket cap as unlimited.
fn bucket_cap(value: Decimal) -> Decimal {
    if value <= Decimal::ZERO {
        Decimal::MAX
    } else {
        value
    }
}

impl PortfolioAllocator for GreedyPortfolioAllocator {
    fn allocate(&self, input: &AllocationInput<'_>) -> QuantResult<AllocationOutput> {
        let caps = input.caps;

        // Deterministic order: risk-adjusted score desc, then market id.
        let mut order: Vec<&CandidateMeta<'_>> = input.candidates.iter().collect();
        order.sort_by(|a, b| {
            risk_adjusted(b.candidate)
                .cmp(&risk_adjusted(a.candidate))
                .then_with(|| {
                    a.candidate
                        .market_id
                        .as_str()
                        .cmp(b.candidate.market_id.as_str())
                })
                .then_with(|| {
                    a.candidate
                        .token_id
                        .as_str()
                        .cmp(b.candidate.token_id.as_str())
                })
        });

        let mut spent_total = Decimal::ZERO;
        let mut spent_market: BTreeMap<String, Decimal> = BTreeMap::new();
        let mut spent_event: BTreeMap<String, Decimal> = BTreeMap::new();
        let mut spent_category: BTreeMap<MarketCategory, Decimal> = BTreeMap::new();

        let mut allocations: BTreeMap<String, Allocation> = BTreeMap::new();
        for meta in order {
            let candidate = meta.candidate;
            let market_key = candidate.market_id.as_str().to_owned();
            let event_key = meta
                .event_id
                .as_ref()
                .map_or_else(|| market_key.clone(), |id| id.as_str().to_owned());

            let total_room = (caps.total_budget_usd - spent_total).max(Decimal::ZERO);
            let market_room = (bucket_cap(caps.max_market_exposure_usd)
                - spent_market
                    .get(&market_key)
                    .copied()
                    .unwrap_or(Decimal::ZERO))
            .max(Decimal::ZERO);
            let event_room = (bucket_cap(caps.max_event_exposure_usd)
                - spent_event
                    .get(&event_key)
                    .copied()
                    .unwrap_or(Decimal::ZERO))
            .max(Decimal::ZERO);
            let category_room = (bucket_cap(caps.max_category_exposure_usd)
                - spent_category
                    .get(&meta.category)
                    .copied()
                    .unwrap_or(Decimal::ZERO))
            .max(Decimal::ZERO);

            // Desired single-position size before the liquidity cap.
            let pre_liquidity = caps
                .max_single_recommendation_usd
                .max(Decimal::ZERO)
                .min(total_room)
                .min(market_room)
                .min(event_room)
                .min(category_room);

            // Liquidity-usage cap (unlimited when liquidity is unknown).
            let (liquidity_room, liquidity_known) =
                meta.liquidity_usd.map_or((Decimal::MAX, false), |liq| {
                    (
                        (liq.inner() * caps.liquidity_usage_cap_pct.max(Decimal::ZERO))
                            .max(Decimal::ZERO),
                        true,
                    )
                });
            let liquidity_feasible = !liquidity_known || pre_liquidity <= liquidity_room;
            let mut alloc = pre_liquidity.min(liquidity_room);
            if alloc < caps.min_recommendation_usd.max(Decimal::ZERO) {
                alloc = Decimal::ZERO;
            }
            alloc = alloc.round_dp(RESEARCH_DECIMAL_SCALE);

            if alloc > Decimal::ZERO {
                spent_total += alloc;
                *spent_market
                    .entry(market_key.clone())
                    .or_insert(Decimal::ZERO) += alloc;
                *spent_event.entry(event_key).or_insert(Decimal::ZERO) += alloc;
                *spent_category.entry(meta.category).or_insert(Decimal::ZERO) += alloc;
            }

            allocations.insert(
                candidate.signal_candidate_id.to_string(),
                Allocation {
                    signal_candidate_id: candidate.signal_candidate_id.clone(),
                    market_id: candidate.market_id.clone(),
                    allocated_usd: Usd::new(alloc),
                    liquidity_feasible,
                },
            );
        }

        Ok(AllocationOutput {
            allocations: allocations.into_values().collect(),
        })
    }
}

/// Risk-adjusted ranking key: `composite_score · confidence`.
fn risk_adjusted(candidate: &SignalCandidate) -> Decimal {
    candidate.composite_score.inner() * candidate.confidence.inner()
}

#[cfg(test)]
mod tests {
    use super::{AllocationInput, CandidateMeta, GreedyPortfolioAllocator, PortfolioAllocator};
    use crate::{
        backtest::PortfolioCaps,
        model::signal::{ModelExplanation, SignalCandidate},
    };
    use chrono::Utc;
    use quant_pivot_models::{
        enums::{common::MarketCategory, quant::SignalSide},
        types::{MarketId, ModelRunId, Price, Probability, SignalCandidateId, TokenId, Usd},
    };
    use rust_decimal::Decimal;
    use rust_decimal_macros::dec;

    fn candidate(market: &str, score: Decimal) -> SignalCandidate {
        SignalCandidate {
            signal_candidate_id: SignalCandidateId::from_v7(),
            model_run_id: ModelRunId::from_v7(),
            market_id: MarketId::new(market),
            token_id: TokenId::new("yes"),
            side: SignalSide::BuyYes,
            composite_score: Probability::new(score),
            confidence: Probability::new(dec!(1)),
            expected_return_bps: dec!(100),
            downside_bps: dec!(50),
            entry_price_ref: Price::new(dec!(0.5)),
            suggested_horizon_secs: 3_600,
            factor_breakdown: Vec::new(),
            model_explanation: ModelExplanation {
                headline: "t".to_owned(),
                top_positive: Vec::new(),
                top_negative: Vec::new(),
            },
            rejection_warnings: Vec::new(),
            rank_before_portfolio: 0,
            as_of: Utc::now(),
        }
    }

    fn caps() -> PortfolioCaps {
        PortfolioCaps {
            total_budget_usd: dec!(1000),
            max_single_recommendation_usd: dec!(300),
            min_recommendation_usd: dec!(10),
            max_market_exposure_usd: dec!(300),
            max_event_exposure_usd: dec!(0),
            max_category_exposure_usd: dec!(400),
            liquidity_usage_cap_pct: dec!(0.1),
        }
    }

    #[test]
    fn greedy_allocator_respects_budget_and_caps() {
        let c1 = candidate("0xa", dec!(0.9));
        let c2 = candidate("0xb", dec!(0.8));
        let c3 = candidate("0xc", dec!(0.7));
        let metas = vec![
            // c1 + c3 share the Crypto category (tests the 400 category cap);
            // c2 is Sports with thin liquidity (tests the liquidity-usage cap).
            CandidateMeta {
                candidate: &c1,
                category: MarketCategory::Crypto,
                event_id: None,
                liquidity_usd: Some(Usd::new(dec!(100000))),
            },
            CandidateMeta {
                candidate: &c2,
                category: MarketCategory::Sports,
                event_id: None,
                liquidity_usd: Some(Usd::new(dec!(1000))), // liquidity cap = 100 < 300
            },
            CandidateMeta {
                candidate: &c3,
                category: MarketCategory::Crypto,
                event_id: None,
                liquidity_usd: Some(Usd::new(dec!(100000))),
            },
        ];
        let caps = caps();
        let out = GreedyPortfolioAllocator::new()
            .allocate(&AllocationInput {
                candidates: metas,
                caps: &caps,
            })
            .expect("allocate");

        let total: Decimal = out
            .allocations
            .iter()
            .map(|a| a.allocated_usd.inner())
            .sum();
        assert!(total <= caps.total_budget_usd, "respects total budget");
        for alloc in &out.allocations {
            assert!(
                alloc.allocated_usd.inner() <= caps.max_single_recommendation_usd,
                "respects single cap"
            );
        }
        // Crypto category (0xa + 0xc) must not exceed the 400 category cap.
        let crypto: Decimal = out
            .allocations
            .iter()
            .filter(|a| matches!(a.market_id.as_str(), "0xa" | "0xc"))
            .map(|a| a.allocated_usd.inner())
            .sum();
        assert!(crypto <= dec!(400), "respects category cap, got {crypto}");
        // The thin-liquidity candidate (cap 100) is flagged not fully feasible.
        let thin = out
            .allocations
            .iter()
            .find(|a| a.market_id.as_str() == "0xb")
            .expect("c2 allocation");
        assert!(!thin.liquidity_feasible, "thin liquidity flagged");
        assert!(
            thin.allocated_usd.inner() <= dec!(100),
            "liquidity-capped size"
        );
    }
}
