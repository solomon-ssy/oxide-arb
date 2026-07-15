//! Execution admission engine — the read-only, deterministic, fixed-order gate
//! between an `Approved` / `ApprovedByPolicy` order intent and real venue
//! submission (05.4).
//!
//! Admission never mutates the report and never submits an order. It runs a
//! fixed sequence of 24 hard checks over a frozen [`AdmissionInput`] (built once
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

pub(crate) use builder::pit_fee_schedule;
pub use builder::{AdmissionInputBuilder, AdmissionInputBuilderDeps};
pub use engine::DefaultAdmissionEngine;

use std::sync::Arc;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use quant_pivot_error::{QuantResult, execution::ExecutionError};
use quant_pivot_models::{
    domain::{
        BookSnapshot, CapitalAllocationInfo, DataQualitySnapshot, EntryConditionInstanceInfo,
        OrderIntentInfo, RecommendationInfo, RecommendationReportInfo,
    },
    enums::{
        common::{OrderType, Side, TickSize},
        execution::{AdmissionCheckId, AdmissionOutcome, KillSwitchState},
        quant::{FillRequirement, QuantRuntimeMode},
    },
    hashing::CanonicalDigest,
    types::{
        ContentHash, OrderAmount, PreparedFeeSchedule, PreparedVenueOrder, ResearchProfileRef,
        RuntimeConfigVersionId, Usd, VenueOrderAmount,
    },
};
use quant_pivot_research::{
    execution_semantics::{
        BookWalkOutcome, LiquidityRole, PitFeeSchedule, walk_buy_cash_budget, walk_buy_exact_shares,
    },
    portfolio::AccountSnapshot,
};
use serde::{Deserialize, Serialize};

/// Venue-health seam read by `VenueGuardCheck` (#18).
///
/// The 05.4 execution breaker is a *transient* accumulator: it publishes
/// `Healthy` / `Degraded` (defer) only. Sustained failure does **not** surface
/// here as a third "halted" variant — it trips the kill-switch, and the
/// authoritative deny of new entries comes from `#17` (`KillSwitchCheck`). This
/// keeps the latch single-sourced in the kill-switch. See the 05.4 phase doc.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub enum VenueHealth {
    /// Venue is accepting orders normally.
    #[default]
    Healthy,
    /// Transient degradation — retryable (defer).
    Degraded { reason: String },
}

/// Readiness seams wired into admission input (05.3–05.6).
///
/// `venue_health` is driven by the 05.4 execution breaker; `credentials_ready`
/// reflects boot-time signer/CLOB connectivity; `exit_monitor_ready` is the
/// shared exit-monitor health handle (05.6) read by check `#20`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AdmissionSeams {
    /// Venue health (`#18`): execution breaker hot read.
    pub venue_health: VenueHealth,
    /// Whether signing credentials are ready (`#19`).
    pub credentials_ready: bool,
    /// Whether the exit monitor worker has completed its first scan (`#20`).
    pub exit_monitor_ready: bool,
}

/// Versioned provenance of the state the decision was made against.
///
/// Recorded on the decision so the 05.4 dispatcher can persist *why* an intent
/// was allowed / denied and the verdict can be replayed against the same state.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StateVersion {
    pub config_version_id: RuntimeConfigVersionId,
    pub account_as_of: DateTime<Utc>,
    pub book_version: Option<u64>,
    pub book_as_of_ms: Option<u64>,
    pub kill_switch_state: KillSwitchState,
}

/// Model-governance flags distilled at build time (`#5`, `#23`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AdmissionModelState {
    /// Whether the intent's model version is still `Published`.
    pub published: bool,
    /// Whether the frozen model artifact's return model is `Calibrated`.
    pub return_model_calibrated: bool,
}

/// Market / exposure blocking flags (`#17`, manual-block check).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AdmissionExposureState {
    /// Whether truth-unknown in-flight exposure exists — an `Ambiguous` order
    /// (capital held, venue truth unknown) or a terminal `Unresolvable` recon
    /// verdict (05.5). Blocks new auto entries (fail-closed, parent §11).
    pub has_blocking_inflight: bool,
    /// Whether the intent's market is on the operator block list.
    pub manual_block: bool,
}

/// Registry-versus-venue metadata frozen by the admission input builder.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AdmissionVenueMetadata {
    pub registry_tick_size: TickSize,
    pub registry_neg_risk: bool,
    pub venue_tick_size: TickSize,
    pub venue_neg_risk: bool,
    pub clob_market_info_hash: ContentHash,
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
    /// Live governed runtime mode (05.1).
    pub mode: QuantRuntimeMode,
    /// Live operational kill-switch state (05.1).
    pub kill_switch: KillSwitchState,
    /// Real venue account snapshot at decision time (09 §1).
    pub account: AccountSnapshot,
    /// This intent's capital allocation (for the budget add-back).
    pub allocation: Option<CapitalAllocationInfo>,
    /// Latest published L2 book for the intent's token (`None` = absent).
    pub book: Option<Arc<BookSnapshot>>,
    /// Point-in-time fee schedule visible before this admission decision.
    pub fee_schedule: PitFeeSchedule,
    /// Governed total budget cap (`portfolio.budget.total_budget_usd`), distilled
    /// from the active config at build time.
    pub budget_total_usd: Usd,
    /// Number of currently open (non-terminal) order intents holding capital,
    /// counted at build time. Consumed by `#21` (`MaxOpenIntentsCheck`).
    pub open_intent_count: u64,
    /// Governed cap on concurrently open intents
    /// (`execution.capital.max_open_intents`; `0` disables). Distilled at build.
    pub max_open_intents: u32,
    /// Governed cap on total reserved capital
    /// (`execution.capital.max_reserved_usd`; `0` disables). Distilled at build.
    pub max_reserved_usd: Usd,
    /// Model publication + calibration flags distilled from the registry.
    pub model_state: AdmissionModelState,
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
    /// Deferred readiness seams (`#18`/`#19`/`#20`).
    pub seams: AdmissionSeams,
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
        self.intent.entry_order_json.notional()
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
    async fn evaluate(&self, input: AdmissionInput) -> QuantResult<AdmissionDecision>;

    /// Evaluate every check without short-circuit (diagnostics / audit). The
    /// outcome is identical to [`Self::evaluate`]; only the trace is complete.
    async fn evaluate_full(&self, input: AdmissionInput) -> QuantResult<AdmissionDecision>;
}

/// Freeze the exact venue amount and all execution-semantic hashes consumed by
/// the dispatcher. The dispatcher must submit this object verbatim.
pub fn prepare_entry_order(input: &AdmissionInput) -> QuantResult<PreparedVenueOrder> {
    let spec = &input.intent.entry_order_json;
    if spec.side != Side::Buy {
        return Err(ExecutionError::IntentDenied {
            reason: "opening entry must be a BUY".to_owned(),
        }
        .into());
    }
    let book = input
        .book
        .as_ref()
        .ok_or_else(|| ExecutionError::IntentDenied {
            reason: "cannot prepare venue order without a PIT L2 book".to_owned(),
        })?;
    let book_hash = CanonicalDigest::content_hash_json(&(
        book.timestamp_ms,
        book.version,
        book.bids.as_ref(),
        book.asks.as_ref(),
    ))?;
    let requirement = match spec.order_type {
        OrderType::Fak => FillRequirement::AllowPartial,
        OrderType::Fok | OrderType::Gtc | OrderType::Gtd { .. } => FillRequirement::AllOrNothing,
    };

    let (cash_budget, venue_amount, expected_fee, total_cash_delta, expected_filled_shares, worst) =
        match spec.amount {
            OrderAmount::CashBudget(budget) => {
                if !matches!(spec.order_type, OrderType::Fok | OrderType::Fak) {
                    return Err(ExecutionError::IntentDenied {
                        reason: "cash-budget BUY is valid only for FOK/FAK".to_owned(),
                    }
                    .into());
                }
                let fill = walk_buy_cash_budget(
                    &book.asks,
                    budget,
                    spec.limit_price,
                    requirement,
                    &input.fee_schedule,
                    LiquidityRole::Taker,
                    input.now,
                )
                .map_err(|error| ExecutionError::IntentDenied {
                    reason: format!("cash-budget execution preparation failed: {error:?}"),
                })?;
                if fill.outcome == BookWalkOutcome::Unfilled
                    || !fill.gross_order_amount.is_positive()
                {
                    return Err(ExecutionError::IntentDenied {
                        reason: "cash budget cannot be executed from the admitted L2 book"
                            .to_owned(),
                    }
                    .into());
                }
                if fill.gross_order_amount.inner() + fill.expected_fee.inner() > budget.inner() {
                    return Err(ExecutionError::IntentDenied {
                        reason: "prepared BUY exceeds governed cash budget".to_owned(),
                    }
                    .into());
                }
                (
                    Some(budget),
                    VenueOrderAmount::GrossUsd(fill.gross_order_amount),
                    fill.expected_fee,
                    fill.total_cash_delta,
                    fill.filled_shares,
                    fill.worst_price.unwrap_or(spec.limit_price),
                )
            }
            OrderAmount::Shares(shares) => {
                let fill = walk_buy_exact_shares(
                    &book.asks,
                    shares,
                    spec.limit_price,
                    requirement,
                    &input.fee_schedule,
                    if spec.post_only {
                        LiquidityRole::Maker
                    } else {
                        LiquidityRole::Taker
                    },
                    input.now,
                )
                .map_err(|error| ExecutionError::IntentDenied {
                    reason: format!("share execution preparation failed: {error:?}"),
                })?;
                if matches!(spec.order_type, OrderType::Fok | OrderType::Fak)
                    && fill.outcome == BookWalkOutcome::Unfilled
                {
                    return Err(ExecutionError::IntentDenied {
                        reason: "share order cannot be executed from the admitted L2 book"
                            .to_owned(),
                    }
                    .into());
                }
                let expected_fee = if spec.post_only {
                    Usd::ZERO
                } else {
                    fill.expected_fee
                };
                (
                    None,
                    VenueOrderAmount::Shares(shares),
                    expected_fee,
                    if spec.post_only {
                        -(shares * spec.limit_price).inner()
                    } else {
                        fill.total_cash_delta
                    },
                    if spec.post_only {
                        shares
                    } else {
                        fill.filled_shares
                    },
                    fill.worst_price.unwrap_or(spec.limit_price),
                )
            }
        };

    Ok(PreparedVenueOrder {
        profile_ref: input.profile_ref.clone(),
        token_id: spec.token_id.clone(),
        side: spec.side,
        order_type: spec.order_type,
        post_only: spec.post_only,
        worst_price: worst,
        cash_budget,
        venue_amount,
        expected_fee,
        total_cash_delta,
        expected_filled_shares,
        book_hash,
        clob_market_info_hash: input.venue_metadata.clob_market_info_hash.clone(),
        fee_schedule: PreparedFeeSchedule {
            schedule_hash: input.fee_schedule.schedule_hash.clone(),
            effective_at: input.fee_schedule.effective_at,
            available_at: input.fee_schedule.available_at,
            platform_rate: input.fee_schedule.platform_rate,
            exponent: input.fee_schedule.exponent,
            taker_only: input.fee_schedule.taker_only,
            builder_maker_fee_bps: input.fee_schedule.builder_maker_fee_bps,
            builder_taker_fee_bps: input.fee_schedule.builder_taker_fee_bps,
            builder_attributed: input.fee_schedule.builder_attributed,
        },
        prepared_at: input.now,
        valid_until: spec.valid_until,
    })
}
