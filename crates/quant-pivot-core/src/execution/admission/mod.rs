//! Execution admission engine — the read-only, deterministic, fixed-order gate
//! between an `Authorized` order intent and real venue
//! submission.
//!
//! Admission never mutates the report and never submits an order. It runs a
//! fixed sequence of 27 hard checks over a frozen [`AdmissionInput`] (built once
//! by the [`AdmissionInputBuilder`], which owns *all* I/O) and produces an
//! [`AdmissionDecision`] of allow / deny / defer plus a full per-check trace and
//! replayable [`StateVersion`].
//!
//! Invariants:
//! - **Pure checks**: every [`AdmissionCheck`] is a function of the frozen input
//!   (no I/O, no wall clock except `input.now`), so the same input always yields
//!   the same decision.
//! - **Fail closed**: building the input may fail (missing account / config /
//!   credentials); a build failure is a `QuantError` the caller treats as
//!   not-executable. Deny and defer are typed *outcomes*, never errors.
//! - **Hash anchor**: `RiskEnvelopeHashCheck` recomputes the canonical risk
//!   envelope hash and compares it to the hash frozen on the intent — the only
//!   link between the report layer and the execution layer.

mod builder;
mod checks;
mod engine;

use std::sync::Arc;

use async_trait::async_trait;
pub(crate) use builder::pit_fee_schedule;
pub use builder::{AdmissionInputBuilder, AdmissionInputBuilderDeps};
use chrono::{DateTime, Utc};
pub use engine::DefaultAdmissionEngine;
use quant_pivot_api::clob::VenueFundingEvidence;
use quant_pivot_error::{QuantResult, execution::ExecutionError};
use quant_pivot_models::{
    domain::{
        data_plane::DataQualitySnapshot,
        market::BookSnapshot,
        order::{CanonicalOrderAmounts, PolymarketOrderRules},
        quant::{
            CapitalAllocationInfo, EntryConditionInstanceInfo, OrderIntentInfo, RecommendationInfo,
            RecommendationReportInfo,
        },
    },
    enums::{
        catalog::{CatalogFilterReason, CatalogFilterReasonSet},
        common::{OrderType, Side, TickSize},
        execution::{AdmissionCheckId, AdmissionOutcome, KillSwitchState},
        market::MarketStatus,
        quant::{EntryAuthorizationPolicy, FillRequirement, RouteEconomicHealthState},
        settlement::SettlementWritePolicy,
    },
    hashing::CanonicalDigest,
    types::{
        ClobMarketInfoVersionId, ContentHash, DecisionPolicySnapshotId, EntryMakerRebateTerms,
        EntryOrderSpec, EvmBlockHash, MarketId, OrderAmount, PreparedFeeSchedule,
        PreparedVenueOrder, Price, ResearchProfileRef, RouteEconomicHealthId, Shares, TokenId, Usd,
        VenueOrderAmount,
    },
};
use quant_pivot_research::{
    execution_semantics::{
        BookWalkFill, BookWalkOutcome, LiquidityRole, PitFeeSchedule, PitMakerRebateEvidence,
        walk_buy_cash_budget, walk_buy_exact_shares,
    },
    portfolio::AccountSnapshot,
};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

use super::settlement_recovery_admission::SettlementRecoveryAdmission;

/// Venue-health seam read by `VenueGuardCheck` (#18).
///
/// The execution breaker is a *transient* accumulator: it publishes
/// `Healthy` / `Degraded` (defer) only. Sustained failure does **not** surface
/// here as a third "halted" variant — it trips the kill-switch, and the
/// authoritative deny of new entries comes from `#17` (`KillSwitchCheck`). This
/// keeps the latch single-sourced in the kill-switch.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub enum VenueHealth {
    /// Venue is accepting orders normally.
    #[default]
    Healthy,
    /// Transient degradation — retryable (defer).
    Degraded { reason: String },
}

/// Readiness seams wired into admission input.
///
/// `venue_health` is driven by the execution breaker; `credentials_ready`
/// reflects boot-time signer/CLOB connectivity; `exit_monitor_ready` is the
/// shared exit-monitor health handle read by check `#20`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AdmissionSeams {
    /// Venue health (`#18`): execution breaker hot read.
    pub venue_health: VenueHealth,
    /// Whether signing credentials are ready (`#19`).
    pub credentials_ready: bool,
    /// Whether the exit monitor worker has completed its first scan (`#20`).
    pub exit_monitor_ready: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AdmissionEconomicHealth {
    Missing,
    Present {
        route_economic_health_id: RouteEconomicHealthId,
        evidence_hash: ContentHash,
        state: RouteEconomicHealthState,
        assessed_through: DateTime<Utc>,
        fresh: bool,
    },
}

impl AdmissionEconomicHealth {
    #[must_use]
    pub const fn evidence_id(&self) -> Option<RouteEconomicHealthId> {
        match self {
            Self::Missing => None,
            Self::Present {
                route_economic_health_id,
                ..
            } => Some(*route_economic_health_id),
        }
    }

    #[must_use]
    pub const fn evidence_hash(&self) -> Option<ContentHash> {
        match self {
            Self::Missing => None,
            Self::Present { evidence_hash, .. } => Some(*evidence_hash),
        }
    }

    #[must_use]
    pub const fn state(&self) -> Option<RouteEconomicHealthState> {
        match self {
            Self::Missing => None,
            Self::Present { state, .. } => Some(*state),
        }
    }
}

/// Versioned provenance of the state the decision was made against.
///
/// Recorded on the decision so the dispatcher can persist *why* an intent
/// was allowed / denied and the verdict can be replayed against the same state.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StateVersion {
    pub config_version_id: DecisionPolicySnapshotId,
    pub account_as_of: DateTime<Utc>,
    pub book_version: Option<u64>,
    pub book_as_of_ms: Option<u64>,
    pub fee_schedule_hash: ContentHash,
    pub maker_rebate_terms_hash: ContentHash,
    pub maker_rebate_decidable: bool,
    pub kill_switch_state: KillSwitchState,
    pub settlement_write_policy: SettlementWritePolicy,
    pub settlement_deployment_digest: Option<ContentHash>,
    pub settlement_verified_block_hash: Option<EvmBlockHash>,
    pub route_economic_health_id: Option<RouteEconomicHealthId>,
    pub route_economic_health_hash: Option<ContentHash>,
    pub route_economic_health_state: Option<RouteEconomicHealthState>,
    /// Canonical hash of the complete venue metadata, funding, and prepared
    /// order evidence that actually authorized the claim.
    pub admission_evidence_hash: ContentHash,
}

#[derive(Serialize)]
struct AdmissionExecutionEvidence<'a> {
    venue_metadata: &'a AdmissionVenueMetadata,
    venue_funding: &'a VenueFundingEvidence,
    prepared_order: &'a PreparedVenueOrder,
}

impl AdmissionExecutionEvidence<'_> {
    fn content_hash(&self) -> QuantResult<ContentHash> {
        Ok(CanonicalDigest::content_hash_json(self)?)
    }
}

/// Model-governance flags distilled at build time (`#5`, `#23`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AdmissionModelState {
    /// Whether the current activated route still authorizes the intent model.
    pub route_bound: bool,
    /// Whether the frozen model artifact's return model is `Calibrated`.
    pub return_model_calibrated: bool,
}

/// Market / exposure blocking flags (`#17`, manual-block check).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AdmissionExposureState {
    /// Whether truth-unknown in-flight exposure exists — an `Ambiguous` order
    /// (capital held, venue truth unknown) or a terminal `Unresolvable` recon
    /// verdict. Blocks new auto entries until venue truth is reconciled.
    pub has_blocking_inflight: bool,
    /// Whether the intent's market is on the operator block list.
    pub manual_block: bool,
}

/// Report-time, current-registry, and live-venue metadata frozen by admission.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AdmissionVenueMetadata {
    pub catalog_market_id: MarketId,
    pub catalog_token_id: Option<TokenId>,
    pub frozen_version_id: ClobMarketInfoVersionId,
    pub frozen_market_id: MarketId,
    pub frozen_token_id: Option<TokenId>,
    pub frozen_tick_size: TickSize,
    pub frozen_minimum_order_size: Shares,
    pub frozen_neg_risk: bool,
    pub frozen_payload_hash: ContentHash,
    pub current_version_id: ClobMarketInfoVersionId,
    pub current_market_id: MarketId,
    pub current_token_id: Option<TokenId>,
    pub current_tick_size: TickSize,
    pub current_minimum_order_size: Shares,
    pub current_neg_risk: bool,
    pub current_payload_hash: ContentHash,
    pub live_market_id: MarketId,
    pub live_token_id: TokenId,
    pub live_tick_size: TickSize,
    pub live_minimum_order_size: Shares,
    pub live_neg_risk: bool,
    pub catalog_status: MarketStatus,
    pub catalog_filter_reasons: CatalogFilterReasonSet,
}

impl AdmissionVenueMetadata {
    fn validate(&self, expected_market: &MarketId, expected_token: &TokenId) -> Result<(), String> {
        if self.catalog_status != MarketStatus::Active || !self.catalog_filter_reasons.is_empty() {
            let reasons = self
                .catalog_filter_reasons
                .iter()
                .map(CatalogFilterReason::as_str)
                .collect::<Vec<_>>()
                .join(",");
            return Err(format!(
                "current Gamma market is not tradeable: status={}, reasons={reasons}",
                self.catalog_status.as_str()
            ));
        }
        if [
            &self.catalog_market_id,
            &self.frozen_market_id,
            &self.current_market_id,
            &self.live_market_id,
        ]
        .into_iter()
        .any(|market_id| market_id != expected_market)
        {
            return Err(format!(
                "catalog/frozen/current/live market mismatch: expected={expected_market}, catalog={}, frozen={}, current={}, live={}",
                self.catalog_market_id,
                self.frozen_market_id,
                self.current_market_id,
                self.live_market_id
            ));
        }
        if self.catalog_token_id.as_ref() != Some(expected_token)
            || self.frozen_token_id.as_ref() != Some(expected_token)
            || self.current_token_id.as_ref() != Some(expected_token)
            || &self.live_token_id != expected_token
        {
            return Err(format!(
                "catalog/frozen/current/live token mismatch: expected={expected_token}, catalog={:?}, frozen={:?}, current={:?}, live={}",
                self.catalog_token_id,
                self.frozen_token_id,
                self.current_token_id,
                self.live_token_id
            ));
        }
        if self.frozen_payload_hash != self.current_payload_hash {
            return Err(format!(
                "report-time/current CLOB payload mismatch: frozen={}, current={}",
                self.frozen_payload_hash, self.current_payload_hash
            ));
        }
        if self.frozen_tick_size != self.current_tick_size
            || self.current_tick_size != self.live_tick_size
        {
            return Err(format!(
                "frozen/current/live tick-size mismatch: frozen={}, current={}, live={}",
                self.frozen_tick_size.as_str(),
                self.current_tick_size.as_str(),
                self.live_tick_size.as_str()
            ));
        }
        if self.frozen_minimum_order_size != self.current_minimum_order_size
            || self.current_minimum_order_size != self.live_minimum_order_size
        {
            return Err(format!(
                "frozen/current/live order-minimum mismatch: frozen={}, current={}, live={}",
                self.frozen_minimum_order_size,
                self.current_minimum_order_size,
                self.live_minimum_order_size
            ));
        }
        if self.frozen_neg_risk != self.current_neg_risk
            || self.current_neg_risk != self.live_neg_risk
        {
            return Err(format!(
                "frozen/current/live NegRisk mismatch: frozen={}, current={}, live={}",
                self.frozen_neg_risk, self.current_neg_risk, self.live_neg_risk
            ));
        }
        Ok(())
    }
}

/// Immutable, decision-time-frozen aggregate consumed by every admission check.
///
/// Built once by [`AdmissionInputBuilder`]; the checks read it as pure
/// functions. Every venue / DB / config read happens during the build, never
/// inside a check.
#[derive(Debug, Clone)]
pub struct AdmissionInput {
    /// Immutable research profile bound through policy and model governance.
    pub profile_ref: ResearchProfileRef,
    /// The intent being admitted (frozen entry spec + risk-envelope hash).
    pub intent: OrderIntentInfo,
    /// Recommendation-owned, revisioned entry-condition state.
    pub condition: EntryConditionInstanceInfo,
    /// Source recommendation (entry/sizing/exit plans + risk envelope).
    pub recommendation: RecommendationInfo,
    /// Source report (governance status + config / model versions).
    pub report: RecommendationReportInfo,
    /// Live governed entry-authorization policy.
    pub authorization_policy: EntryAuthorizationPolicy,
    /// Live operational kill-switch state.
    pub kill_switch: KillSwitchState,
    /// Real venue account snapshot at decision time.
    pub account: AccountSnapshot,
    /// This intent's capital allocation (for the budget add-back).
    pub allocation: Option<CapitalAllocationInfo>,
    /// Latest published L2 book for the intent's token (`None` = absent).
    pub book: Option<Arc<BookSnapshot>>,
    /// Point-in-time fee schedule visible before this admission decision.
    pub fee_schedule: PitFeeSchedule,
    /// Current Gamma program truth independently re-read at final admission.
    pub maker_rebate_evidence: PitMakerRebateEvidence,
    /// Governed total budget cap (`portfolio.budget.total_budget_usd`), distilled
    /// from the active config at build time.
    pub budget_total_usd: Usd,
    /// Number of currently open (non-terminal) order intents holding capital,
    /// counted at build time. Consumed by `#21` (`MaxOpenIntentsCheck`).
    pub open_intent_count: u64,
    /// Governed cap on concurrently open intents, shared with the global
    /// portfolio's open-recommendation cap. Distilled at build.
    pub max_open_intents: u32,
    /// Governed cap on total reserved capital, sourced from
    /// `portfolio.budget.max_open_capital_usd`. Distilled at build.
    pub max_reserved_usd: Usd,
    /// Model publication + calibration flags distilled from the registry.
    pub model_state: AdmissionModelState,
    /// Latest exact-policy Route economic health and freshness.
    pub economic_health: AdmissionEconomicHealth,
    /// Live data-quality classification of the book plane.
    pub data_quality: DataQualitySnapshot,
    /// Plane-wide stale-book ratio cap (`data_quality.max_stale_book_ratio_bps`),
    /// distilled from the active config at build time.
    pub max_stale_book_ratio_bps: u64,
    /// Market / in-flight exposure blocking distilled at build time.
    pub exposure: AdmissionExposureState,
    /// Venue metadata re-read through the official SDK and compared with the
    /// frozen registry before order signing.
    pub venue_metadata: AdmissionVenueMetadata,
    /// Exact canonical order prepared once by the I/O-owning builder. Checks,
    /// WAL construction, and venue submission consume this same value.
    pub prepared_order: PreparedVenueOrder,
    /// Valid live balance/allowance evidence. Closed funding states are pure
    /// Defer outcomes; malformed/transport failures never build an input.
    pub venue_funding: VenueFundingEvidence,
    /// Deferred readiness seams (`#18`/`#19`/`#20`).
    pub seams: AdmissionSeams,
    /// Fresh current-deployment recovery truth for `HoldToResolution + Auto`.
    pub settlement_recovery: SettlementRecoveryAdmission,
    /// Decision time.
    pub now: DateTime<Utc>,
    /// Decision time in epoch milliseconds (book-age comparisons).
    pub now_ms: u64,
    /// Replayable state provenance.
    pub state_version: StateVersion,
}

impl AdmissionInput {
    /// Notional of the (possibly downscaled) frozen entry order.
    #[must_use]
    pub fn order_notional(&self) -> Usd {
        match self.prepared_order.venue_amount {
            VenueOrderAmount::PrincipalUsd(principal) => principal,
            VenueOrderAmount::Shares(shares) => shares * self.prepared_order.limit_price,
        }
    }
}

/// Outcome of one admission check, captured for the audit trace.
///
/// Check-level outcomes reuse [`AdmissionOutcome`]: `Allow` = passed, `Deny` =
/// hard violation, `Defer` = not-now-but-retryable.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AdmissionCheckTrace {
    pub check: AdmissionCheckId,
    pub outcome: AdmissionOutcome,
    pub threshold: Option<String>,
    pub actual: Option<String>,
    pub elapsed_us: u64,
    pub detail: String,
}

impl AdmissionCheckTrace {
    /// Passing trace (`Allow`). `elapsed_us` is set by the engine.
    pub(crate) fn pass(check: AdmissionCheckId, detail: impl Into<String>) -> Self {
        Self::new(check, AdmissionOutcome::Allow, detail)
    }

    /// Hard-violation trace (`Deny`).
    pub(crate) fn deny(check: AdmissionCheckId, detail: impl Into<String>) -> Self {
        Self::new(check, AdmissionOutcome::Deny, detail)
    }

    /// Retryable trace (`Defer`).
    pub(crate) fn defer(check: AdmissionCheckId, detail: impl Into<String>) -> Self {
        Self::new(check, AdmissionOutcome::Defer, detail)
    }

    fn new(check: AdmissionCheckId, outcome: AdmissionOutcome, detail: impl Into<String>) -> Self {
        Self {
            check,
            outcome,
            threshold: None,
            actual: None,
            elapsed_us: 0,
            detail: detail.into(),
        }
    }

    /// Attach the threshold the check compared against (for the trace).
    #[must_use]
    pub(crate) fn with_threshold(mut self, threshold: impl Into<String>) -> Self {
        self.threshold = Some(threshold.into());
        self
    }

    /// Attach the actual observed value (for the trace).
    #[must_use]
    pub(crate) fn with_actual(mut self, actual: impl Into<String>) -> Self {
        self.actual = Some(actual.into());
        self
    }
}

/// The admission verdict plus full per-check trace and state provenance.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AdmissionDecision {
    /// Aggregate outcome: `Deny` if any hard check denied; else `Defer` if any
    /// deferred; else `Allow`.
    pub outcome: AdmissionOutcome,
    /// Per-check trace in fixed evaluation order (truncated at the first deny in
    /// the short-circuit [`ExecutionAdmissionEngine::evaluate`]).
    pub trace: Vec<AdmissionCheckTrace>,
    /// State the decision was made against.
    pub state_version: StateVersion,
    /// Total wall-clock evaluation time.
    pub elapsed_ms: u64,
    /// Attribution of the first hard deny (`None` when allowed / deferred).
    pub denial_reason: Option<String>,
}

impl AdmissionDecision {
    /// Whether the intent may be submitted now.
    #[must_use]
    pub fn is_allowed(&self) -> bool {
        self.outcome == AdmissionOutcome::Allow
    }

    /// Whether submission should be retried later without terminal rejection.
    #[must_use]
    pub fn is_deferred(&self) -> bool {
        self.outcome == AdmissionOutcome::Defer
    }

    /// Whether the intent must not execute (terminal hard violation).
    #[must_use]
    pub fn is_denied(&self) -> bool {
        self.outcome == AdmissionOutcome::Deny
    }
}

/// One ordered admission check: a pure function of the frozen input.
pub trait AdmissionCheck: Send + Sync {
    /// Stable identity (also the metric label and trace key).
    fn id(&self) -> AdmissionCheckId;
    /// Evaluate the check against the frozen input.
    fn run(&self, input: &AdmissionInput) -> AdmissionCheckTrace;
}

/// Execution admission boundary: evaluate a built input into a decision.
#[async_trait]
pub trait ExecutionAdmissionEngine: Send + Sync {
    /// Evaluate in fixed order, short-circuiting on the first hard deny.
    async fn evaluate(&self, input: &AdmissionInput) -> QuantResult<AdmissionDecision>;

    /// Evaluate every check without short-circuit (diagnostics / audit). The
    /// outcome is identical to [`Self::evaluate`]; only the trace is complete.
    async fn evaluate_full(&self, input: &AdmissionInput) -> QuantResult<AdmissionDecision>;
}

pub(super) struct EntryOrderPreparation<'a> {
    pub profile_ref: &'a ResearchProfileRef,
    pub spec: &'a EntryOrderSpec,
    pub book: &'a BookSnapshot,
    pub fee_schedule: &'a PitFeeSchedule,
    pub maker_rebate_evidence: &'a PitMakerRebateEvidence,
    pub venue_metadata: &'a AdmissionVenueMetadata,
    pub now: DateTime<Utc>,
}

struct EntryFillPrediction {
    expected_worst_fill_price: Price,
    expected_filled_shares: Shares,
    expected_fee: Usd,
    total_cash_delta: Decimal,
}

impl EntryFillPrediction {
    fn from_fill(fill: &BookWalkFill, limit_price: Price) -> Self {
        Self {
            expected_worst_fill_price: fill.worst_price.unwrap_or(limit_price),
            expected_filled_shares: fill.filled_shares,
            expected_fee: fill.immediate_cost.total_fee_usd(),
            total_cash_delta: fill.account_cash_delta_usd,
        }
    }

    fn passive(canonical: &CanonicalOrderAmounts, limit_price: Price) -> Self {
        Self {
            expected_worst_fill_price: limit_price,
            expected_filled_shares: canonical.requested_shares,
            expected_fee: Usd::ZERO,
            total_cash_delta: -canonical.principal_usd.inner(),
        }
    }
}

impl EntryOrderPreparation<'_> {
    pub fn prepare(&self) -> QuantResult<PreparedVenueOrder> {
        self.validate_entry_spec()?;
        let rules = PolymarketOrderRules::new(
            self.venue_metadata.current_tick_size,
            self.venue_metadata.current_minimum_order_size,
        )
        .map_err(|error| ExecutionError::IntentDenied {
            reason: format!("live venue order rules are invalid: {error}"),
        })?;
        let requirement = match self.spec.order_type {
            OrderType::Fak => FillRequirement::AllowPartial,
            OrderType::Fok | OrderType::Gtc | OrderType::Gtd { .. } => {
                FillRequirement::AllOrNothing
            }
        };
        let (cash_budget, canonical, fill) = match self.spec.amount {
            OrderAmount::CashBudget(budget) => {
                if !matches!(self.spec.order_type, OrderType::Fok | OrderType::Fak) {
                    return Err(ExecutionError::IntentDenied {
                        reason: "cash-budget BUY is valid only for FOK/FAK".to_owned(),
                    }
                    .into());
                }
                let budget_fill = walk_buy_cash_budget(
                    &self.book.asks,
                    budget,
                    self.spec.limit_price,
                    requirement,
                    self.fee_schedule,
                    LiquidityRole::Taker,
                    self.now,
                )
                .map_err(|error| ExecutionError::IntentDenied {
                    reason: format!("cash-budget execution preparation failed: {error:?}"),
                })?;
                Self::require_fill(&budget_fill, "cash budget")?;
                let canonical = rules
                    .canonical_order(
                        Side::Buy,
                        VenueOrderAmount::PrincipalUsd(budget_fill.immediate_cost.principal_usd),
                        self.spec.limit_price,
                    )
                    .map_err(|error| ExecutionError::IntentDenied {
                        reason: format!("cash-budget venue canonicalization failed: {error}"),
                    })?;
                let fill = self.predict_fill(canonical.requested_shares, requirement)?;
                Self::require_fill(&fill, "canonical cash budget")?;
                if fill.immediate_cost.cash_outlay_usd > budget {
                    return Err(ExecutionError::IntentDenied {
                        reason: "canonical BUY exceeds governed cash budget".to_owned(),
                    }
                    .into());
                }
                (
                    Some(budget),
                    canonical,
                    EntryFillPrediction::from_fill(&fill, self.spec.limit_price),
                )
            }
            OrderAmount::Shares(shares) => {
                let canonical = rules
                    .canonical_order(
                        Side::Buy,
                        VenueOrderAmount::Shares(shares),
                        self.spec.limit_price,
                    )
                    .map_err(|error| ExecutionError::IntentDenied {
                        reason: format!("share venue canonicalization failed: {error}"),
                    })?;
                let prediction = if self.spec.post_only {
                    EntryFillPrediction::passive(&canonical, self.spec.limit_price)
                } else {
                    let fill = self.predict_fill(canonical.requested_shares, requirement)?;
                    Self::require_fill(&fill, "share order")?;
                    EntryFillPrediction::from_fill(&fill, self.spec.limit_price)
                };
                (None, canonical, prediction)
            }
        };
        let book_hash = CanonicalDigest::content_hash_json(&(
            self.book.timestamp_ms,
            self.book.version,
            self.book.bids.as_ref(),
            self.book.asks.as_ref(),
        ))?;
        Ok(PreparedVenueOrder {
            profile_ref: self.profile_ref.clone(),
            market_id: self.venue_metadata.current_market_id.clone(),
            token_id: self.spec.token_id.clone(),
            tick_size: self.venue_metadata.current_tick_size,
            minimum_order_size: self.venue_metadata.current_minimum_order_size,
            neg_risk: self.venue_metadata.current_neg_risk,
            side: self.spec.side,
            order_type: self.spec.order_type,
            post_only: self.spec.post_only,
            limit_price: self.spec.limit_price,
            expected_worst_fill_price: fill.expected_worst_fill_price,
            cash_budget,
            venue_amount: canonical.venue_amount,
            requested_shares: canonical.requested_shares,
            expected_fee: fill.expected_fee,
            total_cash_delta: fill.total_cash_delta,
            expected_filled_shares: fill.expected_filled_shares,
            book_hash,
            clob_market_info_payload_hash: self.venue_metadata.current_payload_hash,
            fee_schedule: self.prepared_fee_schedule(),
            maker_rebate_terms: self.spec.maker_rebate_terms,
            prepared_at: self.now,
            valid_until: self.spec.valid_until,
        })
    }

    fn predict_fill(
        &self,
        shares: Shares,
        requirement: FillRequirement,
    ) -> QuantResult<BookWalkFill> {
        walk_buy_exact_shares(
            &self.book.asks,
            shares,
            self.spec.limit_price,
            requirement,
            self.fee_schedule,
            LiquidityRole::Taker,
            self.now,
        )
        .map_err(|error| {
            ExecutionError::IntentDenied {
                reason: format!("canonical share prediction failed: {error:?}"),
            }
            .into()
        })
    }

    fn require_fill(fill: &BookWalkFill, subject: &str) -> QuantResult<()> {
        if fill.outcome == BookWalkOutcome::Unfilled
            || !fill.immediate_cost.principal_usd.is_positive()
        {
            return Err(ExecutionError::IntentDenied {
                reason: format!("{subject} cannot execute from the admitted PIT L2 book"),
            }
            .into());
        }
        Ok(())
    }

    const fn prepared_fee_schedule(&self) -> PreparedFeeSchedule {
        PreparedFeeSchedule {
            schedule_hash: self.fee_schedule.schedule_hash,
            effective_at: self.fee_schedule.effective_at,
            available_at: self.fee_schedule.available_at,
            platform_rate: self.fee_schedule.platform_rate,
            exponent: self.fee_schedule.exponent,
            taker_only: self.fee_schedule.taker_only,
            builder_maker_fee_bps: self.fee_schedule.builder_maker_fee_bps,
            builder_taker_fee_bps: self.fee_schedule.builder_taker_fee_bps,
            builder_attribution: self.fee_schedule.builder_attribution,
        }
    }

    fn validate_entry_spec(&self) -> QuantResult<()> {
        if self.spec.side != Side::Buy {
            return Err(ExecutionError::IntentDenied {
                reason: "opening entry must be a BUY".to_owned(),
            }
            .into());
        }
        match self.spec.maker_rebate_terms {
            EntryMakerRebateTerms::AggressiveNotApplicable if !self.spec.post_only => {}
            EntryMakerRebateTerms::PassiveNoProgram {
                terms_hash,
                available_at,
            } if self.spec.post_only
                && available_at <= self.now
                && self.fee_schedule.platform_rate.is_zero()
                && matches!(
                    self.maker_rebate_evidence,
                    PitMakerRebateEvidence::NoProgram {
                        terms_hash: current,
                        ..
                    } if *current == terms_hash
                ) => {}
            EntryMakerRebateTerms::PassiveProgram { schedule } if self.spec.post_only => {
                schedule
                    .validate_at(self.now)
                    .map_err(|detail| ExecutionError::IntentDenied {
                        reason: detail.to_owned(),
                    })?;
                if schedule.platform_rate != self.fee_schedule.platform_rate
                    || schedule.exponent != self.fee_schedule.exponent
                    || schedule.taker_only != self.fee_schedule.taker_only
                {
                    return Err(ExecutionError::IntentDenied {
                        reason: "frozen Gamma rebate terms disagree with admitted CLOB fee terms"
                            .to_owned(),
                    }
                    .into());
                }
                if !matches!(
                    self.maker_rebate_evidence,
                    PitMakerRebateEvidence::Available { schedule: current }
                        if current.terms_hash == schedule.terms_hash
                            && current.platform_rate == schedule.platform_rate
                            && current.exponent == schedule.exponent
                            && current.taker_only == schedule.taker_only
                            && current.rebate_rate == schedule.rebate_rate
                ) {
                    return Err(ExecutionError::IntentDenied {
                        reason:
                            "maker-rebate terms drifted after recommendation or intent creation"
                                .to_owned(),
                    }
                    .into());
                }
            }
            _ => {
                return Err(ExecutionError::IntentDenied {
                    reason: "entry route and maker-rebate terms are not admissible".to_owned(),
                }
                .into());
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use quant_pivot_api::clob::{
        VenueBalanceAllowanceSnapshot, VenueFundingAsset, VenueFundingBalance, VenueFundingEvidence,
    };
    use quant_pivot_models::{
        enums::{
            catalog::CatalogFilterReasonSet,
            common::{OrderType, Side, TickSize},
            market::MarketStatus,
        },
        types::{
            ClobMarketInfoVersionId, ContentHash, EvmAddress, EvmUint256, MarketId, Price, Shares,
            TokenId, Usd, VenueOrderAmount,
        },
    };
    use rust_decimal_macros::dec;

    use super::{AdmissionExecutionEvidence, AdmissionVenueMetadata};
    use crate::test_fixtures::execution_pg_seed::PreparedOrderFixture;

    impl AdmissionVenueMetadata {
        fn test_fixture() -> Self {
            let market_id = MarketId::new("0xmarket");
            let token_id = TokenId::new("token-1");
            Self {
                catalog_market_id: market_id.clone(),
                catalog_token_id: Some(token_id.clone()),
                frozen_version_id: ClobMarketInfoVersionId::from_v7(),
                frozen_market_id: market_id.clone(),
                frozen_token_id: Some(token_id.clone()),
                frozen_tick_size: TickSize::Hundredth,
                frozen_minimum_order_size: Shares::new(dec!(5)),
                frozen_neg_risk: false,
                frozen_payload_hash: ContentHash::from_bytes([1; 32]),
                current_version_id: ClobMarketInfoVersionId::from_v7(),
                current_market_id: market_id.clone(),
                current_token_id: Some(token_id.clone()),
                current_tick_size: TickSize::Hundredth,
                current_minimum_order_size: Shares::new(dec!(5)),
                current_neg_risk: false,
                current_payload_hash: ContentHash::from_bytes([1; 32]),
                live_market_id: market_id,
                live_token_id: token_id,
                live_tick_size: TickSize::Hundredth,
                live_minimum_order_size: Shares::new(dec!(5)),
                live_neg_risk: false,
                catalog_status: MarketStatus::Active,
                catalog_filter_reasons: CatalogFilterReasonSet::EMPTY,
            }
        }
    }

    fn funding(required: &str) -> VenueFundingEvidence {
        VenueFundingEvidence::Ready {
            snapshot: VenueBalanceAllowanceSnapshot {
                asset: VenueFundingAsset::Collateral,
                token_id: None,
                spender: EvmAddress::parse(format!("0x{}", "a".repeat(40)))
                    .expect("canonical spender"),
                balance: EvmUint256::parse("100000000").expect("canonical balance"),
                human_balance: VenueFundingBalance::Collateral(Usd::new(dec!(100))),
                allowance: Some(EvmUint256::parse("100000000").expect("canonical allowance")),
            },
            required: EvmUint256::parse(required).expect("canonical required funding"),
        }
    }

    #[test]
    fn unchanged_metadata_is_valid() {
        let metadata = AdmissionVenueMetadata::test_fixture();
        assert!(
            metadata
                .validate(&MarketId::new("0xmarket"), &TokenId::new("token-1"))
                .is_ok()
        );
    }

    #[test]
    fn payload_change_is_rejected() {
        let mut metadata = AdmissionVenueMetadata::test_fixture();
        metadata.current_payload_hash = ContentHash::from_bytes([2; 32]);
        let error = metadata
            .validate(&MarketId::new("0xmarket"), &TokenId::new("token-1"))
            .expect_err("fee-only payload drift must invalidate the report");
        assert!(error.contains("payload mismatch"));
    }

    #[test]
    fn live_rule_change_rejected() {
        let mut metadata = AdmissionVenueMetadata::test_fixture();
        metadata.live_minimum_order_size = Shares::new(dec!(6));
        let error = metadata
            .validate(&MarketId::new("0xmarket"), &TokenId::new("token-1"))
            .expect_err("live rule drift must block preparation");
        assert!(error.contains("order-minimum mismatch"));
    }

    #[test]
    fn evidence_hash_tracks_all() {
        let metadata = AdmissionVenueMetadata::test_fixture();
        let funding_evidence = funding("10000000");
        let prepared = PreparedOrderFixture {
            market_id: metadata.current_market_id.clone(),
            token_id: metadata.current_token_id.clone().expect("current token"),
            side: Side::Buy,
            order_type: OrderType::Fak,
            venue_amount: VenueOrderAmount::PrincipalUsd(Usd::new(dec!(10))),
            expected_fee: Usd::new(dec!(1)),
            expected_filled_shares: Shares::new(dec!(20)),
            limit_price: Price::new(dec!(0.50)),
        }
        .build();
        let baseline = AdmissionExecutionEvidence {
            venue_metadata: &metadata,
            venue_funding: &funding_evidence,
            prepared_order: &prepared,
        }
        .content_hash()
        .expect("baseline admission evidence hash");

        let mut changed_metadata = metadata.clone();
        changed_metadata.current_payload_hash = ContentHash::from_bytes([3; 32]);
        let metadata_hash = AdmissionExecutionEvidence {
            venue_metadata: &changed_metadata,
            venue_funding: &funding_evidence,
            prepared_order: &prepared,
        }
        .content_hash()
        .expect("metadata-change admission evidence hash");

        let changed_funding = funding("10000001");
        let funding_hash = AdmissionExecutionEvidence {
            venue_metadata: &metadata,
            venue_funding: &changed_funding,
            prepared_order: &prepared,
        }
        .content_hash()
        .expect("funding-change admission evidence hash");

        let mut changed_prepared = prepared;
        changed_prepared.expected_fee = Usd::new(dec!(1.01));
        let prepared_hash = AdmissionExecutionEvidence {
            venue_metadata: &metadata,
            venue_funding: &funding_evidence,
            prepared_order: &changed_prepared,
        }
        .content_hash()
        .expect("prepared-change admission evidence hash");

        assert_ne!(baseline, metadata_hash);
        assert_ne!(baseline, funding_hash);
        assert_ne!(baseline, prepared_hash);
    }
}
