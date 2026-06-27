//! Execution admission engine — the read-only, deterministic, fixed-order gate
//! between an `Approved` / `ApprovedByPolicy` order intent and real venue
//! submission (05.4).
//!
//! Admission never mutates the report and never submits an order. It runs a
//! fixed sequence of 20 hard checks over a frozen [`AdmissionInput`] (built once
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

pub use builder::{AdmissionInputBuilder, AdmissionInputBuilderDeps};
pub use engine::DefaultAdmissionEngine;

use std::sync::Arc;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use quant_pivot_error::QuantResult;
use quant_pivot_models::{
    domain::{
        BookSnapshot, CapitalAllocationInfo, DataQualitySnapshot, OrderIntentInfo,
        RecommendationInfo, RecommendationReportInfo,
    },
    enums::{
        execution::{AdmissionCheckId, AdmissionOutcome, KillSwitchState},
        quant::QuantRuntimeMode,
    },
    types::{RuntimeConfigVersionId, Usd},
};
use quant_pivot_research::portfolio::AccountSnapshot;

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

/// Deferred readiness seams (placeholders in 05.3; real signals land later).
///
/// `venue_health` is driven by the 05.4 execution breaker; `credentials_ready`
/// gains a dedicated signer probe in 05.4; `exit_monitor_ready` becomes the real
/// worker health in 05.6. Grouped so the checks read one cohesive seam surface.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AdmissionSeams {
    /// Venue health (`#18`): 05.3 supplies `Healthy`.
    pub venue_health: VenueHealth,
    /// Whether signing credentials are ready (`#19`).
    pub credentials_ready: bool,
    /// Whether the exit monitor can register (`#20`): placeholder `true`.
    pub exit_monitor_ready: bool,
}

/// Versioned provenance of the state the decision was made against.
///
/// Recorded on the decision so the 05.4 dispatcher can persist *why* an intent
/// was allowed / denied and the verdict can be replayed against the same state.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StateVersion {
    pub config_version_id: RuntimeConfigVersionId,
    pub account_as_of: DateTime<Utc>,
    pub book_version: Option<u64>,
    pub book_as_of_ms: Option<u64>,
    pub kill_switch_state: KillSwitchState,
}

/// Immutable, decision-time-frozen aggregate consumed by every admission check.
///
/// Built once by [`AdmissionInputBuilder`]; the checks read it as pure
/// functions. Every venue / DB / config read happens during the build, never
/// inside a check.
#[derive(Debug, Clone)]
pub struct AdmissionInput {
    /// The intent being admitted (frozen entry spec + risk-envelope hash).
    pub intent: OrderIntentInfo,
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
    /// Governed total budget cap (`portfolio.budget.total_budget_usd`), distilled
    /// from the active config at build time.
    pub budget_total_usd: Usd,
    /// Whether the intent's model version is still `Published`.
    pub model_published: bool,
    /// Live data-quality classification of the book plane.
    pub data_quality: DataQualitySnapshot,
    /// Plane-wide stale-book ratio cap (`data_quality.max_stale_book_ratio_bps`),
    /// distilled from the active config at build time.
    pub max_stale_book_ratio_bps: u64,
    /// Whether truth-unknown in-flight exposure exists — an `Ambiguous` order
    /// (capital held, venue truth unknown) or a terminal `Unresolvable` recon
    /// verdict (05.5). Blocks new auto entries (fail-closed, parent §11).
    pub has_blocking_inflight: bool,
    /// Whether the intent's market is on the operator block list.
    pub manual_block: bool,
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
        self.intent.entry_order_json.shares * self.intent.entry_order_json.limit_price
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
