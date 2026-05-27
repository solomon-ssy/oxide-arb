//! Map CLOB order responses to execution outcomes with actual fill economics.

use num_traits::ToPrimitive;
use oxide_arb_models::{
    domain::{execution::ExecutionPlan, opportunity::Opportunity, order::OrderResponse},
    enums::{common::ExecutionMode, execution::ExecutionOutcome, order::OrderStatus},
    types::{Price, Shares, Usd},
};
use rust_decimal::Decimal;
use std::time::Instant;

/// Convert a CLOB [`OrderResponse`] into an [`ExecutionOutcome`] using actual fill data.
pub fn map_order_response(
    resp: OrderResponse,
    plan: &ExecutionPlan,
    mode: ExecutionMode,
    started: Instant,
) -> ExecutionOutcome {
    let latency_ms = ToPrimitive::to_u64(&started.elapsed().as_millis()).unwrap_or(u64::MAX);

    match resp.status {
        OrderStatus::Filled | OrderStatus::PartiallyFilled => {
            if resp.filled_shares.inner() <= Decimal::ZERO {
                return ExecutionOutcome::Miss {
                    reason: format!("order {} returned zero fill", resp.order_id),
                    execution_mode: mode,
                };
            }

            let avg_fill_price = resp.avg_fill_price.or(Some(plan.limit_price));

            ExecutionOutcome::Filled {
                order_id: resp.order_id,
                filled_shares: resp.filled_shares,
                avg_fill_price,
                fee_paid: resp.fee_paid,
                tx_hash: resp.tx_hash,
                execution_mode: mode,
                latency_ms,
            }
        }
        OrderStatus::Rejected | OrderStatus::Cancelled | OrderStatus::Expired => {
            ExecutionOutcome::Miss {
                reason: format!("order {} {}", resp.order_id, resp.status),
                execution_mode: mode,
            }
        }
        OrderStatus::Open => ExecutionOutcome::Miss {
            reason: format!(
                "order {} resting open — FOK expected immediate fill or kill",
                resp.order_id
            ),
            execution_mode: mode,
        },
    }
}

/// Scale detection-time profit estimate by actual fill ratio.
pub fn filled_net_profit(opp: &Opportunity, filled_shares: Shares, planned_shares: Shares) -> Usd {
    if planned_shares.inner() <= Decimal::ZERO {
        return opp.expected_net_profit;
    }
    let ratio = (filled_shares.inner() / planned_shares.inner()).min(Decimal::ONE);
    Usd::new(opp.expected_net_profit.inner() * ratio)
}

/// Actual cost basis from fill price and size.
pub fn filled_cost(filled_shares: Shares, avg_fill_price: Price) -> Usd {
    Usd::new(filled_shares.inner() * avg_fill_price.inner())
}
