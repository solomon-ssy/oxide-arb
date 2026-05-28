//! Map CLOB order responses to execution outcomes with actual fill economics.

use num_traits::ToPrimitive;
use oxide_arb_api::fees::FeeCalculator;
use oxide_arb_models::{
    domain::{execution::ExecutionPlan, order::OrderResponse},
    enums::{
        common::{ExecutionMode, MarketCategory},
        execution::ExecutionOutcome,
        order::OrderStatus,
    },
    types::TokenId,
};
use rust_decimal::Decimal;
use std::time::Instant;

/// Convert a CLOB [`OrderResponse`] into an [`ExecutionOutcome`] using actual fill data.
///
/// Fee is always computed via [`FeeCalculator`] — CLOB `fee_paid` is ignored (unreliable).
pub fn map_order_response(
    resp: OrderResponse,
    plan: &ExecutionPlan,
    mode: ExecutionMode,
    started: Instant,
    fee_calculator: &FeeCalculator,
    category: MarketCategory,
    token_id: &TokenId,
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
            let fill_price = avg_fill_price.unwrap_or(plan.limit_price);
            let fee = fee_calculator.calculate(resp.filled_shares, fill_price, category, token_id);

            ExecutionOutcome::Filled {
                order_id: resp.order_id,
                filled_shares: resp.filled_shares,
                avg_fill_price,
                fee_paid: fee,
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
