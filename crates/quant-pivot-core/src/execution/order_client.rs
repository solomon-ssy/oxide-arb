//! Polymarket CLOB order-write façade (Phase 05.4 — real money).
//!
//! Wraps [`ClobClient`] (which already owns rate limiting, retry, EIP-712
//! signing, and SDK type mapping) behind a venue-neutral, SDK-free boundary. The
//! only types that cross the trait are project value types; the
//! `polymarket_client_sdk_v2` and `quant-pivot-api` order types never leak out of
//! this module.
//!
//! Venue outcomes — including the **unconfirmed** case — are always returned as a
//! [`VenueSubmitResult`]; the façade never surfaces an error. A timeout / 5xx /
//! unparseable response is classified as [`VenueOutcome::Ambiguous`] so the
//! dispatcher holds capital and reconciles (fail-closed: never assume an order
//! that might have reached the matching engine did not).

use std::sync::Arc;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use quant_pivot_api::clob::{ClobClient, OrderSubmissionError, OrderSubmissionStage};
use quant_pivot_error::api::ApiError;
use quant_pivot_models::{
    domain::{OrderRequest, OrderResponse},
    enums::{
        common::{OrderType, Side},
        execution::{ReconciliationResult, VenueOrderStatus},
    },
    types::{
        ContentHash, FeeEvidence, MarketId, OrderId, Price, Shares, TokenId, Usd, VenueOrderAmount,
    },
};

/// The concrete order an admitted intent submits to the venue (SDK-free).
#[derive(Debug, Clone)]
pub struct VenueOrder {
    pub market_id: MarketId,
    pub token_id: TokenId,
    /// Opening entries are always [`Side::Buy`]; kept general for reuse.
    pub side: Side,
    /// Hard limit price.
    pub price: Price,
    /// Tagged venue amount (USD spend for aggressive BUY; shares otherwise).
    pub amount: VenueOrderAmount,
    /// Time-in-force / order type (`Fok` | `Gtc` | `Gtd { expiration }`).
    pub order_type: OrderType,
    /// Maker-only placement for passive limit orders.
    pub post_only: bool,
    /// Fee frozen by final admission from the same inputs that produced amount.
    pub expected_fee: Usd,
    pub fee_schedule_hash: ContentHash,
}

/// Façade classification of a submission outcome, including the unconfirmed case.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VenueOutcome {
    /// Fully filled immediately.
    Filled,
    /// Partially filled (remainder resting or cancelled).
    PartiallyFilled,
    /// Accepted and resting on the book, no fill yet.
    Open,
    /// Cleanly rejected by the venue — definitely not executed.
    Rejected,
    /// Cancelled.
    Cancelled,
    /// Expired (GTD past expiration at submit).
    Expired,
    /// Unconfirmed: timeout / server error / unparseable response. The order may
    /// or may not have executed — capital must be held and reconciled.
    Ambiguous,
}

impl VenueOutcome {
    const fn from_status(status: VenueOrderStatus) -> Self {
        match status {
            VenueOrderStatus::Filled => Self::Filled,
            VenueOrderStatus::PartiallyFilled => Self::PartiallyFilled,
            VenueOrderStatus::Open => Self::Open,
            VenueOrderStatus::Rejected => Self::Rejected,
            VenueOrderStatus::Cancelled => Self::Cancelled,
            VenueOrderStatus::Expired => Self::Expired,
        }
    }

    /// Persisted venue status when the outcome is venue-confirmed; `None` for
    /// [`Self::Ambiguous`].
    pub(crate) const fn venue_order_status(self) -> Option<VenueOrderStatus> {
        match self {
            Self::Filled => Some(VenueOrderStatus::Filled),
            Self::PartiallyFilled => Some(VenueOrderStatus::PartiallyFilled),
            Self::Open => Some(VenueOrderStatus::Open),
            Self::Rejected => Some(VenueOrderStatus::Rejected),
            Self::Cancelled => Some(VenueOrderStatus::Cancelled),
            Self::Expired => Some(VenueOrderStatus::Expired),
            Self::Ambiguous => None,
        }
    }

    /// Reconciliation classification for this outcome.
    pub(crate) const fn reconciliation_result(self) -> ReconciliationResult {
        match self {
            Self::Filled => ReconciliationResult::Filled,
            Self::PartiallyFilled => ReconciliationResult::PartiallyFilled,
            Self::Rejected => ReconciliationResult::NotFilled,
            Self::Cancelled | Self::Expired => ReconciliationResult::Cancelled,
            // Ambiguous (and the unused Open path) are truth-unknown at submit
            // time: enqueue as `Pending` for the 05.5 recon worker to resolve.
            // `Unresolvable` is reserved for that worker's terminal verdict.
            Self::Ambiguous | Self::Open => ReconciliationResult::Pending,
        }
    }

    /// FOK orders are all-or-nothing on Polymarket; a partial fill is a venue-contract
    /// violation and must fail-closed to [`Self::Ambiguous`].
    #[must_use]
    pub fn normalize_for_order_type(self, order_type: &OrderType) -> Self {
        if self == Self::PartiallyFilled && matches!(order_type, OrderType::Fok) {
            tracing::warn!(
                "FOK order reported partial fill — treating as ambiguous (venue contract violation)"
            );
            Self::Ambiguous
        } else {
            self
        }
    }
}

/// Classify a venue error as cleanly-rejected vs unconfirmed (fail-closed).
///
/// Only errors that prove the order never reached the matching engine map to
/// [`VenueOutcome::Rejected`] (safe to release capital): client validation
/// (`4xx` except `429`) and non-retryable CLOB validation. Rate limiting
/// (`429` / [`ApiError::RateLimited`]) is [`VenueOutcome::Ambiguous`]: the
/// request may have been accepted before the throttle response. Everything else
/// (timeout, `5xx`, unparseable, SDK) is also [`VenueOutcome::Ambiguous`].
impl From<&ApiError> for VenueOutcome {
    fn from(error: &ApiError) -> Self {
        match error {
            ApiError::Http { status, .. } if *status < 500 && *status != 429 => Self::Rejected,
            ApiError::Clob {
                retryable: false, ..
            } => Self::Rejected,
            _ => Self::Ambiguous,
        }
    }
}

impl From<ApiError> for VenueOutcome {
    fn from(error: ApiError) -> Self {
        Self::from(&error)
    }
}

impl From<&OrderSubmissionError> for VenueOutcome {
    fn from(error: &OrderSubmissionError) -> Self {
        match error.stage {
            OrderSubmissionStage::Prepare | OrderSubmissionStage::Sign => Self::Rejected,
            OrderSubmissionStage::Post => Self::from(&error.source),
        }
    }
}

/// Result of a venue submission (always returned — never an error).
#[derive(Debug, Clone)]
pub struct VenueSubmitResult {
    pub outcome: VenueOutcome,
    pub venue_order_id: Option<OrderId>,
    pub filled_shares: Shares,
    pub avg_fill_price: Option<Price>,
    /// Fee projected from the immutable schedule at preparation time. This is
    /// suitable for conservative capital accounting, but is never presented as
    /// venue-observed truth.
    pub expected_fee: Usd,
    /// Exact or reconstructed venue fee. Submit responses do not carry enough
    /// evidence, so this remains `None` until authenticated-trade or on-chain
    /// reconciliation supplies it.
    pub observed_fee: Option<Usd>,
    pub fee_evidence: FeeEvidence,
    pub tx_hash: Option<String>,
    /// Human-readable detail (error message for ambiguous / rejected outcomes).
    pub detail: Option<String>,
    pub submitted_at: DateTime<Utc>,
    pub responded_at: DateTime<Utc>,
}

impl VenueSubmitResult {
    fn from_response(
        resp: OrderResponse,
        expected_fee: Usd,
        fee_schedule_hash: ContentHash,
    ) -> Self {
        Self {
            outcome: VenueOutcome::from_status(resp.status),
            venue_order_id: Some(resp.order_id),
            filled_shares: resp.filled_shares,
            avg_fill_price: resp.avg_fill_price,
            expected_fee,
            observed_fee: None,
            fee_evidence: FeeEvidence::PreparedScheduleExpected {
                schedule_hash: fee_schedule_hash,
                expected_fee,
            },
            tx_hash: resp.tx_hash,
            detail: None,
            submitted_at: resp.submitted_at,
            responded_at: resp.responded_at,
        }
    }

    fn from_error(
        error: &OrderSubmissionError,
        submitted_at: DateTime<Utc>,
        expected_fee: Usd,
        fee_schedule_hash: ContentHash,
    ) -> Self {
        Self {
            outcome: error.into(),
            venue_order_id: None,
            filled_shares: Shares::ZERO,
            avg_fill_price: None,
            expected_fee,
            observed_fee: None,
            fee_evidence: FeeEvidence::PreparedScheduleExpected {
                schedule_hash: fee_schedule_hash,
                expected_fee,
            },
            tx_hash: None,
            detail: Some(error.to_string()),
            submitted_at,
            responded_at: Utc::now(),
        }
    }

    /// Apply order-type semantics (e.g. FOK under-fill → [`VenueOutcome::Ambiguous`]).
    #[must_use]
    pub fn with_order_type_semantics(mut self, order_type: &OrderType) -> Self {
        self.outcome = self.outcome.normalize_for_order_type(order_type);
        self
    }
}

/// Result of a venue cancellation (always returned — never an error).
#[derive(Debug, Clone)]
pub struct VenueCancelResult {
    pub venue_order_id: OrderId,
    pub cancelled: bool,
    pub detail: Option<String>,
    pub responded_at: DateTime<Utc>,
}

/// SDK-free adapter boundary for Polymarket CLOB order writes.
#[async_trait]
pub trait PolymarketOrderClient: Send + Sync {
    /// Sign and post an order. Returns a classified outcome; an unconfirmed
    /// response (timeout / 5xx / unparseable) is [`VenueOutcome::Ambiguous`].
    async fn submit(&self, order: VenueOrder) -> VenueSubmitResult;

    /// Cancel a resting order by venue id.
    async fn cancel(&self, venue_order_id: &OrderId) -> VenueCancelResult;
}

/// [`PolymarketOrderClient`] backed by the shared authenticated [`ClobClient`].
pub struct ClobOrderClient {
    clob: Arc<ClobClient>,
}

impl ClobOrderClient {
    #[must_use]
    pub const fn new(clob: Arc<ClobClient>) -> Self {
        Self { clob }
    }
}

#[async_trait]
impl PolymarketOrderClient for ClobOrderClient {
    async fn submit(&self, order: VenueOrder) -> VenueSubmitResult {
        let submitted_at = Utc::now();
        let expected_fee = order.expected_fee;
        let fee_schedule_hash = order.fee_schedule_hash.clone();
        let request = OrderRequest {
            market_id: order.market_id.clone(),
            token_id: order.token_id.clone(),
            side: order.side,
            amount: order.amount,
            price: order.price,
            order_type: order.order_type,
            post_only: order.post_only,
        };
        match self.clob.place_order(&request).await {
            Ok(response) => {
                VenueSubmitResult::from_response(response, expected_fee, fee_schedule_hash)
            }
            Err(error) => {
                VenueSubmitResult::from_error(&error, submitted_at, expected_fee, fee_schedule_hash)
            }
        }
    }

    async fn cancel(&self, venue_order_id: &OrderId) -> VenueCancelResult {
        let responded_at = Utc::now();
        match self.clob.cancel_order(venue_order_id).await {
            Ok(result) => VenueCancelResult {
                venue_order_id: result.order_id,
                cancelled: result.success,
                detail: result.reason,
                responded_at: Utc::now(),
            },
            Err(error) => VenueCancelResult {
                venue_order_id: venue_order_id.clone(),
                cancelled: false,
                detail: Some(error.to_string()),
                responded_at,
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use quant_pivot_models::{
        enums::common::OrderType,
        types::{ContentHash, FeeEvidence, Shares, Usd},
    };

    use super::{VenueOutcome, VenueSubmitResult};
    use chrono::Utc;

    #[test]
    fn fok_partial_normalizes_to_ambiguous() {
        assert_eq!(
            VenueOutcome::PartiallyFilled.normalize_for_order_type(&OrderType::Fok),
            VenueOutcome::Ambiguous
        );
    }

    #[test]
    fn gtc_partial_stays_partially_filled() {
        assert_eq!(
            VenueOutcome::PartiallyFilled.normalize_for_order_type(&OrderType::Gtc),
            VenueOutcome::PartiallyFilled
        );
    }

    #[test]
    fn submit_result_applies_order_type_semantics() {
        let result = VenueSubmitResult {
            outcome: VenueOutcome::PartiallyFilled,
            venue_order_id: None,
            filled_shares: Shares::ZERO,
            avg_fill_price: None,
            expected_fee: Usd::ZERO,
            observed_fee: None,
            fee_evidence: FeeEvidence::PreparedScheduleExpected {
                schedule_hash: ContentHash::parse(
                    "blake3:0000000000000000000000000000000000000000000000000000000000000000",
                )
                .expect("valid hash"),
                expected_fee: Usd::ZERO,
            },
            tx_hash: None,
            detail: None,
            submitted_at: Utc::now(),
            responded_at: Utc::now(),
        }
        .with_order_type_semantics(&OrderType::Fok);
        assert_eq!(result.outcome, VenueOutcome::Ambiguous);
    }
}
