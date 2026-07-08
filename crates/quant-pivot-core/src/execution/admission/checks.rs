//! The 22 admission checks (parent §4.2), each a pure function of the frozen
//! [`AdmissionInput`]. Every check is hard: a violation is `Deny`, a
//! not-now-but-retryable condition is `Defer`, otherwise `Allow`.

use quant_pivot_models::{
    enums::{
        execution::AdmissionCheckId,
        quant::{
            EntryTriggerKind, OrderIntentStatus, QuantRuntimeMode, RecommendationReportStatus,
        },
    },
    types::{Bps, Price, Shares, Usd},
};

use super::{AdmissionCheck, AdmissionCheckTrace, AdmissionInput, VenueHealth};

// 1 ──────────────────────────────────────────────────────────────────────────
/// Intent is in a submittable state and not expired.
pub(super) struct IntentStateCheck;

impl AdmissionCheck for IntentStateCheck {
    fn id(&self) -> AdmissionCheckId {
        AdmissionCheckId::IntentState
    }

    fn run(&self, input: &AdmissionInput) -> AdmissionCheckTrace {
        let intent = &input.intent;
        if !matches!(
            intent.status,
            OrderIntentStatus::Approved | OrderIntentStatus::ApprovedByPolicy
        ) {
            return AdmissionCheckTrace::deny(
                self.id(),
                format!(
                    "intent status {} is not submittable",
                    intent.status.as_str()
                ),
            );
        }
        if input.now >= intent.expires_at {
            return AdmissionCheckTrace::deny(self.id(), "intent has expired")
                .with_threshold(intent.expires_at.to_rfc3339())
                .with_actual(input.now.to_rfc3339());
        }
        AdmissionCheckTrace::pass(self.id(), "intent approved and not expired")
    }
}

// 2 ──────────────────────────────────────────────────────────────────────────
/// Recommendation is unexpired and the entry time window is open.
pub(super) struct RecommendationFreshnessCheck;

impl AdmissionCheck for RecommendationFreshnessCheck {
    fn id(&self) -> AdmissionCheckId {
        AdmissionCheckId::RecommendationFreshness
    }

    fn run(&self, input: &AdmissionInput) -> AdmissionCheckTrace {
        let rec = &input.recommendation;
        if rec.valid_until < input.now {
            return AdmissionCheckTrace::deny(self.id(), "recommendation has expired")
                .with_threshold(rec.valid_until.to_rfc3339())
                .with_actual(input.now.to_rfc3339());
        }
        let entry = &rec.entry_plan;
        if input.now < entry.valid_from {
            return AdmissionCheckTrace::defer(self.id(), "entry window not open yet")
                .with_threshold(entry.valid_from.to_rfc3339())
                .with_actual(input.now.to_rfc3339());
        }
        if input.now > entry.valid_until {
            return AdmissionCheckTrace::deny(self.id(), "entry window has closed")
                .with_threshold(entry.valid_until.to_rfc3339())
                .with_actual(input.now.to_rfc3339());
        }
        AdmissionCheckTrace::pass(self.id(), "recommendation fresh and entry window open")
    }
}

// 3 ──────────────────────────────────────────────────────────────────────────
/// Source report is still `Published` (not revoked / expired).
pub(super) struct ReportStatusCheck;

impl AdmissionCheck for ReportStatusCheck {
    fn id(&self) -> AdmissionCheckId {
        AdmissionCheckId::ReportStatus
    }

    fn run(&self, input: &AdmissionInput) -> AdmissionCheckTrace {
        if input.report.status == RecommendationReportStatus::Published {
            AdmissionCheckTrace::pass(self.id(), "source report published")
        } else {
            AdmissionCheckTrace::deny(
                self.id(),
                format!("source report status {}", input.report.status.as_str()),
            )
        }
    }
}

// 4 ──────────────────────────────────────────────────────────────────────────
/// Runtime mode allows execution and matches the intent's approval provenance.
pub(super) struct RuntimeModeCheck;

impl AdmissionCheck for RuntimeModeCheck {
    fn id(&self) -> AdmissionCheckId {
        AdmissionCheckId::RuntimeMode
    }

    fn run(&self, input: &AdmissionInput) -> AdmissionCheckTrace {
        match input.mode {
            QuantRuntimeMode::ReportOnly => {
                AdmissionCheckTrace::deny(self.id(), "report-only mode forbids submission")
            }
            QuantRuntimeMode::SemiAuto => {
                if input.intent.status == OrderIntentStatus::Approved {
                    AdmissionCheckTrace::pass(self.id(), "semi-auto with operator-approved intent")
                } else {
                    AdmissionCheckTrace::deny(
                        self.id(),
                        "semi-auto requires an operator-approved intent",
                    )
                }
            }
            QuantRuntimeMode::AutoExecution => {
                if input.intent.status == OrderIntentStatus::ApprovedByPolicy {
                    AdmissionCheckTrace::pass(
                        self.id(),
                        "auto-execution with policy-approved intent",
                    )
                } else {
                    AdmissionCheckTrace::deny(
                        self.id(),
                        "auto-execution requires a policy-approved intent",
                    )
                }
            }
        }
    }
}

// 5 ──────────────────────────────────────────────────────────────────────────
/// The intent's model version is still `Published`.
pub(super) struct ModelPublicationCheck;

impl AdmissionCheck for ModelPublicationCheck {
    fn id(&self) -> AdmissionCheckId {
        AdmissionCheckId::ModelPublication
    }

    fn run(&self, input: &AdmissionInput) -> AdmissionCheckTrace {
        if input.model_state.published {
            AdmissionCheckTrace::pass(self.id(), "model version published")
        } else {
            AdmissionCheckTrace::deny(self.id(), "intent model version is no longer published")
        }
    }
}

// 6 ──────────────────────────────────────────────────────────────────────────
/// Live data-quality classification is green and fresh.
pub(super) struct DataQualityCheck;

impl AdmissionCheck for DataQualityCheck {
    fn id(&self) -> AdmissionCheckId {
        AdmissionCheckId::DataQuality
    }

    fn run(&self, input: &AdmissionInput) -> AdmissionCheckTrace {
        let dq = &input.data_quality;
        if dq.total_tokens == 0 {
            return AdmissionCheckTrace::deny(self.id(), "no tokens classified");
        }
        if dq.ingest_lag_exceeded {
            return AdmissionCheckTrace::deny(self.id(), "clickhouse ingest pipeline lag exceeded")
                .with_threshold(dq.max_ingest_lag_ms.to_string())
                .with_actual(dq.worst_ingest_lag_ms.to_string());
        }
        if dq.insufficient > 0 {
            return AdmissionCheckTrace::deny(self.id(), "tokens with insufficient data present")
                .with_actual(dq.insufficient.to_string());
        }
        let stale_ratio_bps = dq.stale.saturating_mul(10_000) / dq.total_tokens;
        let max_stale_ratio_bps = input.max_stale_book_ratio_bps;
        if stale_ratio_bps > max_stale_ratio_bps {
            return AdmissionCheckTrace::deny(self.id(), "stale book ratio above threshold")
                .with_threshold(max_stale_ratio_bps.to_string())
                .with_actual(stale_ratio_bps.to_string());
        }
        AdmissionCheckTrace::pass(self.id(), "data quality green")
    }
}

// 7 ──────────────────────────────────────────────────────────────────────────
/// A book snapshot exists and is within the entry's max age (stale → defer).
pub(super) struct BookFreshnessCheck;

impl AdmissionCheck for BookFreshnessCheck {
    fn id(&self) -> AdmissionCheckId {
        AdmissionCheckId::BookFreshness
    }

    fn run(&self, input: &AdmissionInput) -> AdmissionCheckTrace {
        let Some(book) = &input.book else {
            return AdmissionCheckTrace::deny(self.id(), "no book snapshot for token");
        };
        let age_ms = input.now_ms.saturating_sub(book.timestamp_ms);
        let max = input.recommendation.entry_plan.max_book_age_ms;
        if age_ms > max {
            return AdmissionCheckTrace::defer(self.id(), "book snapshot stale")
                .with_threshold(max.to_string())
                .with_actual(age_ms.to_string());
        }
        AdmissionCheckTrace::pass(self.id(), "book snapshot fresh")
            .with_threshold(max.to_string())
            .with_actual(age_ms.to_string())
    }
}

// 8 ──────────────────────────────────────────────────────────────────────────
/// The entry trigger condition is satisfied (untriggered → defer).
///
/// Phase 4 publishes only [`EntryTriggerKind::Immediate`] and
/// [`EntryTriggerKind::LimitPrice`] via [`derive_entry_plan`](crate::report::entry_plan::derive_entry_plan).
/// Any other kind is a hard deny (fail closed — not an infinite defer loop).
pub(super) struct EntryTriggerCheck;

impl AdmissionCheck for EntryTriggerCheck {
    fn id(&self) -> AdmissionCheckId {
        AdmissionCheckId::EntryTrigger
    }

    fn run(&self, input: &AdmissionInput) -> AdmissionCheckTrace {
        let entry = &input.recommendation.entry_plan;
        match entry.trigger_kind {
            EntryTriggerKind::Immediate => {
                AdmissionCheckTrace::pass(self.id(), "immediate entry trigger")
            }
            EntryTriggerKind::LimitPrice => {
                let Some(trigger) = entry.trigger_price else {
                    return AdmissionCheckTrace::deny(
                        self.id(),
                        "limit trigger has no trigger price",
                    );
                };
                let Some(book) = &input.book else {
                    return AdmissionCheckTrace::defer(self.id(), "no book to evaluate trigger");
                };
                let Some(best_ask) = book.best_ask() else {
                    return AdmissionCheckTrace::defer(self.id(), "ask side empty");
                };
                if best_ask <= trigger {
                    AdmissionCheckTrace::pass(self.id(), "limit trigger satisfied")
                        .with_threshold(trigger.to_string())
                        .with_actual(best_ask.to_string())
                } else {
                    AdmissionCheckTrace::defer(self.id(), "limit trigger not yet satisfied")
                        .with_threshold(trigger.to_string())
                        .with_actual(best_ask.to_string())
                }
            }
            other => AdmissionCheckTrace::deny(
                self.id(),
                format!(
                    "entry trigger kind {} is not supported for execution (Phase 4 publishes immediate and limit_price only)",
                    other.as_str()
                ),
            ),
        }
    }
}

// 9 ──────────────────────────────────────────────────────────────────────────
/// The recomputed canonical risk-envelope hash matches the intent's frozen hash.
///
/// This is the only anchor between the report layer and the execution layer;
/// any mismatch (or recompute failure) is a hard deny.
pub(super) struct RiskEnvelopeHashCheck;

impl AdmissionCheck for RiskEnvelopeHashCheck {
    fn id(&self) -> AdmissionCheckId {
        AdmissionCheckId::RiskEnvelopeHash
    }

    fn run(&self, input: &AdmissionInput) -> AdmissionCheckTrace {
        match input.recommendation.risk_envelope.canonical_hash() {
            Ok(recomputed) => {
                if recomputed == input.intent.risk_envelope_hash {
                    AdmissionCheckTrace::pass(self.id(), "risk envelope hash matches intent anchor")
                } else {
                    AdmissionCheckTrace::deny(self.id(), "risk envelope hash mismatch")
                        .with_threshold(input.intent.risk_envelope_hash.to_string())
                        .with_actual(recomputed.to_string())
                }
            }
            Err(error) => AdmissionCheckTrace::deny(
                self.id(),
                format!("failed to recompute risk envelope hash: {error}"),
            ),
        }
    }
}

// 10 ─────────────────────────────────────────────────────────────────────────
/// The order is funded by real cash and within the governed budget.
///
/// The intent's own allocation is added back before comparing, because the
/// account snapshot's `available`/`reserved` already account for this intent's
/// `Allocated` reservation (avoids double-counting).
pub(super) struct CapitalBudgetCheck;

impl AdmissionCheck for CapitalBudgetCheck {
    fn id(&self) -> AdmissionCheckId {
        AdmissionCheckId::CapitalBudget
    }

    fn run(&self, input: &AdmissionInput) -> AdmissionCheckTrace {
        let notional = input.order_notional();
        let alloc = input
            .allocation
            .as_ref()
            .map_or(Usd::ZERO, |allocation| allocation.allocated_usd);

        let available_before = input.account.available_usd + alloc;
        if notional > available_before {
            return AdmissionCheckTrace::deny(self.id(), "order notional exceeds available cash")
                .with_threshold(available_before.to_string())
                .with_actual(notional.to_string());
        }

        let reserved_before = if input.account.reserved_usd > alloc {
            input.account.reserved_usd - alloc
        } else {
            Usd::ZERO
        };
        let remaining_budget = if input.budget_total_usd > reserved_before {
            input.budget_total_usd - reserved_before
        } else {
            Usd::ZERO
        };
        if notional > remaining_budget {
            return AdmissionCheckTrace::deny(self.id(), "order notional exceeds remaining budget")
                .with_threshold(remaining_budget.to_string())
                .with_actual(notional.to_string());
        }

        AdmissionCheckTrace::pass(self.id(), "order funded and within budget")
            .with_threshold(remaining_budget.to_string())
            .with_actual(notional.to_string())
    }
}

// 10a ────────────────────────────────────────────────────────────────────────
/// Concurrently open (non-terminal) order intents stay within the governed cap.
///
/// `execution.capital.max_open_intents` bounds how many intents may hold capital
/// in flight at once. `0` disables the cap. The intent under admission is already
/// counted in `open_intent_count` (it is `Approved` / `ApprovedByPolicy`), so the
/// check is `open_intent_count <= cap` — no off-by-one add-back.
pub(super) struct MaxOpenIntentsCheck;

impl AdmissionCheck for MaxOpenIntentsCheck {
    fn id(&self) -> AdmissionCheckId {
        AdmissionCheckId::MaxOpenIntents
    }

    fn run(&self, input: &AdmissionInput) -> AdmissionCheckTrace {
        if input.max_open_intents == 0 {
            return AdmissionCheckTrace::pass(self.id(), "open-intent cap disabled");
        }
        let cap = u64::from(input.max_open_intents);
        if input.open_intent_count > cap {
            return AdmissionCheckTrace::deny(self.id(), "open intent count exceeds cap")
                .with_threshold(cap.to_string())
                .with_actual(input.open_intent_count.to_string());
        }
        AdmissionCheckTrace::pass(self.id(), "open intent count within cap")
            .with_threshold(cap.to_string())
            .with_actual(input.open_intent_count.to_string())
    }
}

// 10b ────────────────────────────────────────────────────────────────────────
/// Total capital reserved by open intents stays within the governed cap.
///
/// `execution.capital.max_reserved_usd` bounds the aggregate reserved capital;
/// `0` disables it. `account.reserved_usd` already includes this intent's
/// `Allocated` reservation, so the check compares the current total reservation
/// directly against the cap.
pub(super) struct MaxReservedCapitalCheck;

impl AdmissionCheck for MaxReservedCapitalCheck {
    fn id(&self) -> AdmissionCheckId {
        AdmissionCheckId::MaxReservedCapital
    }

    fn run(&self, input: &AdmissionInput) -> AdmissionCheckTrace {
        if input.max_reserved_usd == Usd::ZERO {
            return AdmissionCheckTrace::pass(self.id(), "reserved-capital cap disabled");
        }
        let reserved = input.account.reserved_usd;
        if reserved > input.max_reserved_usd {
            return AdmissionCheckTrace::deny(self.id(), "reserved capital exceeds cap")
                .with_threshold(input.max_reserved_usd.to_string())
                .with_actual(reserved.to_string());
        }
        AdmissionCheckTrace::pass(self.id(), "reserved capital within cap")
            .with_threshold(input.max_reserved_usd.to_string())
            .with_actual(reserved.to_string())
    }
}

// 11 ─────────────────────────────────────────────────────────────────────────
/// Current per-market exposure plus this order stays within the envelope cap.
pub(super) struct MarketExposureCheck;

impl AdmissionCheck for MarketExposureCheck {
    fn id(&self) -> AdmissionCheckId {
        AdmissionCheckId::MarketExposure
    }

    fn run(&self, input: &AdmissionInput) -> AdmissionCheckTrace {
        let current = input
            .account
            .exposures
            .per_market
            .get(&input.recommendation.market_id)
            .copied()
            .unwrap_or(Usd::ZERO);
        let cap = input.recommendation.risk_envelope.max_market_exposure_usd;
        exposure_trace(self.id(), "market", current, input.order_notional(), cap)
    }
}

// 12 ─────────────────────────────────────────────────────────────────────────
/// Current per-event exposure plus this order stays within the envelope cap.
pub(super) struct EventExposureCheck;

impl AdmissionCheck for EventExposureCheck {
    fn id(&self) -> AdmissionCheckId {
        AdmissionCheckId::EventExposure
    }

    fn run(&self, input: &AdmissionInput) -> AdmissionCheckTrace {
        let current = input
            .account
            .exposures
            .per_event
            .get(&input.recommendation.event_id)
            .copied()
            .unwrap_or(Usd::ZERO);
        let cap = input.recommendation.risk_envelope.max_event_exposure_usd;
        exposure_trace(self.id(), "event", current, input.order_notional(), cap)
    }
}

// 13 ─────────────────────────────────────────────────────────────────────────
/// Current per-category exposure plus this order stays within the envelope cap.
pub(super) struct CategoryExposureCheck;

impl AdmissionCheck for CategoryExposureCheck {
    fn id(&self) -> AdmissionCheckId {
        AdmissionCheckId::CategoryExposure
    }

    fn run(&self, input: &AdmissionInput) -> AdmissionCheckTrace {
        let current = input
            .account
            .exposures
            .per_category
            .get(&input.recommendation.identity.category)
            .copied()
            .unwrap_or(Usd::ZERO);
        let cap = input.recommendation.risk_envelope.max_category_exposure_usd;
        exposure_trace(self.id(), "category", current, input.order_notional(), cap)
    }
}

/// Shared exposure cap evaluation: `current + order ≤ cap`.
fn exposure_trace(
    id: AdmissionCheckId,
    scope: &str,
    current: Usd,
    order: Usd,
    cap: Usd,
) -> AdmissionCheckTrace {
    let projected = current + order;
    if projected > cap {
        AdmissionCheckTrace::deny(id, format!("{scope} exposure cap breached"))
            .with_threshold(cap.to_string())
            .with_actual(projected.to_string())
    } else {
        AdmissionCheckTrace::pass(id, format!("{scope} exposure within cap"))
            .with_threshold(cap.to_string())
            .with_actual(projected.to_string())
    }
}

// 14 ─────────────────────────────────────────────────────────────────────────
/// Visible ask liquidity can fill the order size and clears the min depth.
pub(super) struct LiquidityDepthCheck;

impl AdmissionCheck for LiquidityDepthCheck {
    fn id(&self) -> AdmissionCheckId {
        AdmissionCheckId::LiquidityDepth
    }

    fn run(&self, input: &AdmissionInput) -> AdmissionCheckTrace {
        let Some(book) = &input.book else {
            return AdmissionCheckTrace::deny(self.id(), "no book snapshot for token");
        };
        let spec = &input.intent.entry_order_json;
        let fillable = book.ask_depth_up_to(spec.limit_price);
        if fillable < spec.shares {
            return AdmissionCheckTrace::defer(self.id(), "insufficient ask depth to fill size")
                .with_threshold(spec.shares.to_string())
                .with_actual(fillable.to_string());
        }
        let visible = book.ask_notional_up_to(spec.limit_price);
        let min_depth = input.recommendation.entry_plan.min_depth_usd;
        if visible < min_depth {
            return AdmissionCheckTrace::defer(self.id(), "visible depth below minimum")
                .with_threshold(min_depth.to_string())
                .with_actual(visible.to_string());
        }
        AdmissionCheckTrace::pass(self.id(), "liquidity depth sufficient")
            .with_threshold(min_depth.to_string())
            .with_actual(visible.to_string())
    }
}

// 15 ─────────────────────────────────────────────────────────────────────────
/// Estimated fill slippage (VWAP vs best ask) is within the entry tolerance.
pub(super) struct SlippageCheck;

impl AdmissionCheck for SlippageCheck {
    fn id(&self) -> AdmissionCheckId {
        AdmissionCheckId::Slippage
    }

    fn run(&self, input: &AdmissionInput) -> AdmissionCheckTrace {
        let Some(book) = &input.book else {
            return AdmissionCheckTrace::deny(self.id(), "no book snapshot for token");
        };
        let spec = &input.intent.entry_order_json;
        if spec.shares <= Shares::ZERO {
            return AdmissionCheckTrace::deny(self.id(), "non-positive order size");
        }
        let Some(best_ask) = book.best_ask() else {
            return AdmissionCheckTrace::defer(self.id(), "ask side empty");
        };

        // Walk the asks at or below the limit, accumulating fill cost.
        let mut remaining = spec.shares;
        let mut cost = Usd::ZERO;
        for level in book.asks.iter() {
            if remaining <= Shares::ZERO {
                break;
            }
            let price = level.price_decimal();
            if price > spec.limit_price {
                break;
            }
            let available = level.size_decimal();
            let take = if available < remaining {
                available
            } else {
                remaining
            };
            cost += take * price;
            remaining -= take;
        }
        if remaining > Shares::ZERO {
            return AdmissionCheckTrace::defer(
                self.id(),
                "insufficient depth at or below limit to estimate fill",
            );
        }

        let vwap = Price::new(cost.inner() / spec.shares.inner());
        let Some(slippage) = Bps::spread(vwap, best_ask) else {
            return AdmissionCheckTrace::deny(self.id(), "degenerate best-ask price");
        };
        if slippage > spec.max_slippage_bps {
            return AdmissionCheckTrace::deny(self.id(), "estimated slippage exceeds tolerance")
                .with_threshold(spec.max_slippage_bps.to_string())
                .with_actual(slippage.to_string());
        }
        AdmissionCheckTrace::pass(self.id(), "estimated slippage within tolerance")
            .with_threshold(spec.max_slippage_bps.to_string())
            .with_actual(slippage.to_string())
    }
}

// 16 ─────────────────────────────────────────────────────────────────────────
/// The intent's market is not on the operator block list.
pub(super) struct ManualBlockCheck;

impl AdmissionCheck for ManualBlockCheck {
    fn id(&self) -> AdmissionCheckId {
        AdmissionCheckId::ManualBlock
    }

    fn run(&self, input: &AdmissionInput) -> AdmissionCheckTrace {
        if input.exposure.manual_block {
            AdmissionCheckTrace::deny(self.id(), "market is on the operator block list")
        } else {
            AdmissionCheckTrace::pass(self.id(), "market not blocked")
        }
    }
}

// 17 ─────────────────────────────────────────────────────────────────────────
/// The kill switch allows new entry; unresolvable reconciliation blocks auto.
pub(super) struct KillSwitchCheck;

impl AdmissionCheck for KillSwitchCheck {
    fn id(&self) -> AdmissionCheckId {
        AdmissionCheckId::KillSwitch
    }

    fn run(&self, input: &AdmissionInput) -> AdmissionCheckTrace {
        if !input.kill_switch.allows_new_entry() {
            return AdmissionCheckTrace::deny(
                self.id(),
                format!(
                    "kill switch state {} blocks new entry",
                    input.kill_switch.as_str()
                ),
            );
        }
        if input.mode == QuantRuntimeMode::AutoExecution && input.exposure.has_blocking_inflight {
            return AdmissionCheckTrace::deny(
                self.id(),
                "blocking in-flight exposure (ambiguous/unresolvable) blocks auto execution",
            );
        }
        AdmissionCheckTrace::pass(self.id(), "kill switch allows new entry")
    }
}

// 18 ─────────────────────────────────────────────────────────────────────────
/// Venue health permits submission. The 05.4 breaker drives this with at most a
/// transient `Degraded` (defer); a sustained halt is authoritative via `#17`.
pub(super) struct VenueGuardCheck;

impl AdmissionCheck for VenueGuardCheck {
    fn id(&self) -> AdmissionCheckId {
        AdmissionCheckId::VenueGuard
    }

    fn run(&self, input: &AdmissionInput) -> AdmissionCheckTrace {
        // The breaker is a transient accumulator: at most `Degraded` (defer). A
        // sustained-failure halt is authoritative via `#17` (kill-switch), not a
        // venue-health deny here — keeping the latch single-sourced.
        match &input.seams.venue_health {
            VenueHealth::Healthy => AdmissionCheckTrace::pass(self.id(), "venue healthy"),
            VenueHealth::Degraded { reason } => {
                AdmissionCheckTrace::defer(self.id(), format!("venue degraded: {reason}"))
            }
        }
    }
}

// 19 ─────────────────────────────────────────────────────────────────────────
/// Signing credentials are ready for the submission.
pub(super) struct CredentialReadinessCheck;

impl AdmissionCheck for CredentialReadinessCheck {
    fn id(&self) -> AdmissionCheckId {
        AdmissionCheckId::CredentialReadiness
    }

    fn run(&self, input: &AdmissionInput) -> AdmissionCheckTrace {
        if input.seams.credentials_ready {
            AdmissionCheckTrace::pass(self.id(), "signing credentials ready")
        } else {
            AdmissionCheckTrace::deny(self.id(), "signing credentials not ready")
        }
    }
}

// 20 ─────────────────────────────────────────────────────────────────────────
/// The exit monitor can register for the resulting position.
pub(super) struct ExitMonitorReadinessCheck;

impl AdmissionCheck for ExitMonitorReadinessCheck {
    fn id(&self) -> AdmissionCheckId {
        AdmissionCheckId::ExitMonitorReadiness
    }

    fn run(&self, input: &AdmissionInput) -> AdmissionCheckTrace {
        if input.seams.exit_monitor_ready {
            AdmissionCheckTrace::pass(self.id(), "exit monitor ready")
        } else {
            AdmissionCheckTrace::deny(self.id(), "exit monitor not ready")
        }
    }
}

// 23 ─────────────────────────────────────────────────────────────────────────
/// Frozen model artifact still carries a calibrated return model.
pub(super) struct CalibratedReturnModelCheck;

impl AdmissionCheck for CalibratedReturnModelCheck {
    fn id(&self) -> AdmissionCheckId {
        AdmissionCheckId::CalibratedReturnModel
    }

    fn run(&self, input: &AdmissionInput) -> AdmissionCheckTrace {
        if input.model_state.return_model_calibrated {
            AdmissionCheckTrace::pass(self.id(), "return model is calibrated")
        } else {
            AdmissionCheckTrace::deny(
                self.id(),
                "return model is heuristic (uncalibrated) — fail-closed",
            )
        }
    }
}
