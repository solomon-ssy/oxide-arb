//! Signal re-inference exit-signal evaluator (Phase 05.6 / 06.0).
//!
//! Implements the model-driven [`ExitSignalEvaluator`] seam via **score
//! degradation**: at exit-evaluation time the market is re-scored and the fresh
//! composite score / expected return / eligibility are compared against the
//! thesis baselines **frozen on the intent** ([`ExitPolicySpec::entry_composite_score`]).
//! The thesis is invalidated when any of:
//!
//! 1. the market would no longer pass the auto-execution eligibility gate, or
//! 2. the fresh composite score drops below `entry_score × invalidation_ratio`, or
//! 3. the fresh expected return is non-positive (the edge is gone).
//!
//! The actual re-inference is delegated to the narrow [`ExitSignalReinferer`]
//! seam (a side-effect-free, single-market re-score that reuses the research
//! factor/model primitives — no `quant_model_run` / factor / selection writes).
//! It is **fail-safe**: when the market cannot be re-scored (missing features,
//! unavailable model, stale data) the reinferer yields `None`, the evaluator
//! returns [`ExitSignalVerdict::Indeterminate`], and the exit ladder does **not**
//! force a signal exit — stop-loss / time / trailing still guard the downside.
//!
//! Phase 06.0 wires [`ModelBackedExitSignalReinferer`] as the production
//! reinferer. `shadow_mode` runs the full pipeline but suppresses
//! `ThesisInvalidated` exits until operators disable it.
//!
//! The opportunistic-Sell verdict is intentionally **not** produced here; it is
//! the Phase 6.1 Sell ranking model behind the same [`ExitSignalEvaluator`] seam,
//! composed ahead of this evaluator when it lands.

use std::sync::Arc;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use quant_pivot_error::QuantResult;
use quant_pivot_models::{
    domain::{OrderIntentInfo, PositionInfo},
    enums::quant::QuantRuntimeMode,
    types::{Bps, Price, Probability},
};
use rust_decimal::Decimal;

use crate::{
    execution::{ExitSignalContext, ExitSignalEvaluator, ExitSignalVerdict},
    observability::metrics_hub::MetricsHub,
    runtime_config::RuntimeConfigStore,
};

/// A freshly re-inferred signal for one market at exit-evaluation time.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FreshSignal {
    /// Fresh composite score.
    pub composite_score: Probability,
    /// Fresh expected return (basis points).
    pub expected_return_bps: Bps,
    /// Whether the market would still pass the auto-execution eligibility gate.
    pub auto_exec_eligible: bool,
}

/// Narrow, side-effect-free single-market re-inference seam.
///
/// Implementations reuse the research factor/model primitives to re-score one
/// market from the current book **without** persisting model-run / factor /
/// selection rows. Returning `Ok(None)` (cannot evaluate) is the fail-safe path.
#[async_trait]
pub trait ExitSignalReinferer: Send + Sync {
    async fn reinfer(
        &self,
        intent: &OrderIntentInfo,
        lot: &PositionInfo,
        mark_price: Option<Price>,
        now: DateTime<Utc>,
    ) -> QuantResult<Option<FreshSignal>>;
}

/// Dependencies for [`ReinferenceSignalEvaluator`].
pub struct ReinferenceSignalEvaluatorDeps<R> {
    pub reinferer: R,
    pub config: Arc<RuntimeConfigStore>,
    pub metrics: Arc<MetricsHub>,
}

/// Model-driven exit-signal evaluator using the score-degradation criterion.
pub struct ReinferenceSignalEvaluator<R> {
    reinferer: R,
    config: Arc<RuntimeConfigStore>,
    metrics: Arc<MetricsHub>,
}

impl<R> ReinferenceSignalEvaluator<R> {
    #[must_use]
    pub fn new(deps: ReinferenceSignalEvaluatorDeps<R>) -> Self {
        Self {
            reinferer: deps.reinferer,
            config: deps.config,
            metrics: deps.metrics,
        }
    }
}

/// Pure score-degradation verdict (testable independent of re-inference I/O).
#[must_use]
pub fn degradation_verdict(
    entry_composite_score: Probability,
    fresh: &FreshSignal,
    invalidation_ratio: Decimal,
) -> ExitSignalVerdict {
    if !fresh.auto_exec_eligible {
        return ExitSignalVerdict::ThesisInvalidated {
            detail: "market no longer auto-execution eligible".to_owned(),
        };
    }
    if fresh.expected_return_bps.inner() <= Decimal::ZERO {
        return ExitSignalVerdict::ThesisInvalidated {
            detail: format!(
                "expected return collapsed to {} bps",
                fresh.expected_return_bps.inner()
            ),
        };
    }
    let floor = entry_composite_score.inner() * invalidation_ratio;
    if fresh.composite_score.inner() < floor {
        return ExitSignalVerdict::ThesisInvalidated {
            detail: format!(
                "composite score {} fell below {}% of entry {}",
                fresh.composite_score.inner(),
                (invalidation_ratio * Decimal::from(100)).normalize(),
                entry_composite_score.inner()
            ),
        };
    }
    ExitSignalVerdict::Holds
}

#[async_trait]
impl<R: ExitSignalReinferer> ExitSignalEvaluator for ReinferenceSignalEvaluator<R> {
    async fn evaluate(&self, ctx: ExitSignalContext<'_>) -> ExitSignalVerdict {
        let policy = self.config.current().execution.exit_monitor.clone();
        if !policy.signal_reinference.enabled {
            self.metrics.inc_exit_signal_reinference("disabled");
            return ExitSignalVerdict::Indeterminate {
                detail: "signal re-inference disabled in config".to_owned(),
            };
        }

        // Thesis-invalidation is a *forced* exit (ladder tier 5, fires even under
        // hold-to-resolution). It is only ever auto-submitted for intents entered
        // under auto-execution — a human owns the exit for `SemiAuto` / report-only
        // positions, so re-inference never force-closes them (fail-safe hold).
        if ctx.intent.runtime_mode != QuantRuntimeMode::AutoExecution {
            self.metrics.inc_exit_signal_reinference("skipped_non_auto");
            return ExitSignalVerdict::Indeterminate {
                detail: "thesis-invalidation forced exit applies to auto-execution intents only"
                    .to_owned(),
            };
        }

        let invalidation_ratio = match policy.signal_invalidation_ratio.value.parse::<Decimal>() {
            Ok(ratio) => ratio,
            Err(error) => {
                // A malformed ratio must never silently become the most
                // aggressive floor (1.0). Fail-safe to hold; config validation
                // rejects this at load, so this only guards a corrupted snapshot.
                self.metrics.inc_exit_signal_reinference("error");
                tracing::warn!(
                    %error,
                    value = %policy.signal_invalidation_ratio.value,
                    "signal_invalidation_ratio is not a valid decimal; holding (fail-safe)"
                );
                return ExitSignalVerdict::Indeterminate {
                    detail: "signal_invalidation_ratio misconfigured".to_owned(),
                };
            }
        };
        let entry_score = ctx.intent.exit_policy_json.entry_composite_score;

        let verdict = match self
            .reinferer
            .reinfer(ctx.intent, ctx.lot, ctx.mark_price, ctx.now)
            .await
        {
            Ok(Some(fresh)) => {
                self.metrics.inc_exit_signal_reinference("fresh");
                degradation_verdict(entry_score, &fresh, invalidation_ratio)
            }
            Ok(None) => {
                self.metrics.inc_exit_signal_reinference("unavailable");
                ExitSignalVerdict::Indeterminate {
                    detail: "signal re-inference unavailable (missing features/model/stale)"
                        .to_owned(),
                }
            }
            Err(error) => {
                self.metrics.inc_exit_signal_reinference("error");
                tracing::warn!(%error, "exit signal re-inference failed; holding (fail-safe)");
                ExitSignalVerdict::Indeterminate {
                    detail: format!("re-inference error: {error}"),
                }
            }
        };

        if policy.signal_reinference.shadow_mode {
            return suppress_shadow_verdict(&self.metrics, &verdict);
        }
        verdict
    }
}

/// Shadow mode: audit what would have happened, but never force a signal exit.
fn suppress_shadow_verdict(metrics: &MetricsHub, verdict: &ExitSignalVerdict) -> ExitSignalVerdict {
    match verdict {
        ExitSignalVerdict::ThesisInvalidated { detail } => {
            metrics.inc_exit_signal_reinference("shadow_would_invalidate");
            tracing::info!(
                detail,
                "exit signal shadow: thesis would invalidate (suppressed)"
            );
            ExitSignalVerdict::Indeterminate {
                detail: format!("shadow_mode: thesis invalidation suppressed ({detail})"),
            }
        }
        ExitSignalVerdict::Holds => {
            metrics.inc_exit_signal_reinference("shadow_hold");
            ExitSignalVerdict::Holds
        }
        other => other.clone(),
    }
}

#[cfg(test)]
mod tests {
    use quant_pivot_models::types::{Bps, Probability};
    use rust_decimal::Decimal;
    use rust_decimal_macros::dec;

    use super::{FreshSignal, degradation_verdict, suppress_shadow_verdict};
    use crate::{execution::ExitSignalVerdict, observability::metrics_hub::MetricsHub};

    fn fresh(score: &str, ret_bps: i64, eligible: bool) -> FreshSignal {
        FreshSignal {
            composite_score: Probability::new(score.parse().unwrap()),
            expected_return_bps: Bps::new(Decimal::from(ret_bps)),
            auto_exec_eligible: eligible,
        }
    }

    #[test]
    fn ineligible_invalidates() {
        let v = degradation_verdict(
            Probability::new(dec!(0.8)),
            &fresh("0.79", 100, false),
            dec!(0.6),
        );
        assert!(matches!(v, ExitSignalVerdict::ThesisInvalidated { .. }));
    }

    #[test]
    fn non_positive_return_invalidates() {
        let v = degradation_verdict(
            Probability::new(dec!(0.8)),
            &fresh("0.79", 0, true),
            dec!(0.6),
        );
        assert!(matches!(v, ExitSignalVerdict::ThesisInvalidated { .. }));
    }

    #[test]
    fn score_below_floor_invalidates() {
        // floor = 0.8 * 0.6 = 0.48; fresh 0.40 < 0.48
        let v = degradation_verdict(
            Probability::new(dec!(0.8)),
            &fresh("0.40", 100, true),
            dec!(0.6),
        );
        assert!(matches!(v, ExitSignalVerdict::ThesisInvalidated { .. }));
    }

    #[test]
    fn healthy_holds() {
        let v = degradation_verdict(
            Probability::new(dec!(0.8)),
            &fresh("0.70", 100, true),
            dec!(0.6),
        );
        assert_eq!(v, ExitSignalVerdict::Holds);
    }

    #[test]
    fn shadow_suppresses_thesis_invalidated() {
        let metrics = MetricsHub::new();
        let verdict = ExitSignalVerdict::ThesisInvalidated {
            detail: "test".to_owned(),
        };
        let suppressed = suppress_shadow_verdict(&metrics, &verdict);
        assert!(matches!(
            suppressed,
            ExitSignalVerdict::Indeterminate { .. }
        ));
    }

    #[test]
    fn shadow_preserves_holds() {
        let metrics = MetricsHub::new();
        let suppressed = suppress_shadow_verdict(&metrics, &ExitSignalVerdict::Holds);
        assert_eq!(suppressed, ExitSignalVerdict::Holds);
    }
}
