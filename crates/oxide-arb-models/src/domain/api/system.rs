//! System + risk control-plane API contract.
//!
//! Control endpoints are mutating and money-critical. Each carries a mandatory
//! `reason` (recorded on the operation log) and the execution-mode switch is
//! additionally governed by the `X-Acting-Role` header (authorized by the authz
//! middleware) since entering `Live` is the highest-risk operator action.

use crate::{
    enums::{common::ExecutionMode, risk::BlacklistReason},
    types::MarketId,
};
use serde::Deserialize;
use validator::Validate;

/// Governed runtime execution-mode hot-swap request.
#[derive(Debug, Deserialize, Validate)]
pub struct SwitchModeRequest {
    /// Target execution mode (`dry_run` / `paper` / `live`).
    pub mode: ExecutionMode,
    /// Operator justification, recorded on the operation log.
    #[validate(length(min = 1, max = 1024))]
    pub reason: String,
}

/// Halt trading (risk halt + execution kill switch).
#[derive(Debug, Deserialize, Validate)]
pub struct HaltRequest {
    #[validate(length(min = 1, max = 1024))]
    pub reason: String,
}

/// Resume trading after operator acknowledgement.
#[derive(Debug, Deserialize, Validate)]
pub struct ResumeRequest {
    /// Operator acknowledgement string recorded on the risk audit.
    #[validate(length(min = 1, max = 256))]
    pub operator_ack: String,
}

/// Force the circuit breaker back to `Closed`.
#[derive(Debug, Deserialize, Validate)]
pub struct CircuitBreakerResetRequest {
    #[validate(length(min = 1, max = 1024))]
    pub reason: String,
}

/// Add a market to the runtime blacklist.
#[derive(Debug, Deserialize, Validate)]
pub struct BlacklistCreateRequest {
    /// Polymarket `condition_id` to exclude from trading.
    pub market_id: MarketId,
    /// Classification of why the market is excluded.
    pub reason: BlacklistReason,
}
