//! Signal re-inference exit-signal evaluator.
//!
//! Implements the model-driven [`ExitSignalEvaluator`] seam via **score
//! degradation**: at exit-evaluation time the market is re-scored and the fresh
//! composite score / expected return / eligibility are compared against the
//! thesis baselines **frozen on the intent** (`ExitPolicySpec::entry_composite_score`).
//! The thesis is invalidated when any of:
//!
//! 1. the market would no longer pass its frozen Route-local model gate, or
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
//! wires `ModelBackedExitSignalReinferer` as the production
//! reinferer. `shadow_mode` runs the full pipeline but suppresses
//! `ThesisInvalidated` exits until operators disable it.
//!
//! The opportunistic-Sell verdict is intentionally **not** produced here; it is
//! the Sell ranking model behind the same [`ExitSignalEvaluator`] seam,
//! composed ahead of this evaluator when it lands.

use std::sync::Arc;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use quant_pivot_error::QuantResult;
use quant_pivot_models::{
    clickhouse::{ChPrice, ChProbability, QuantExitSignalEvaluationEventRow},
    domain::quant::{OrderIntentInfo, StrategyPositionLot},
    enums::{
        clickhouse::{ChExitSignalEvaluatorKind, ChExitSignalVerdict},
        quant::AuthorizationKind,
    },
    types::{
        Bps, ContentHash, ExitReinferenceObservation, ExitReinferenceVerdictKind, Price,
        Probability, ThesisInvalidationPolicy,
    },
};
use rust_decimal::Decimal;

use crate::{
    execution::{ExitSignalContext, ExitSignalEvaluation, ExitSignalEvaluator, ExitSignalVerdict},
    observability::{
        exit_signal_fact_writer::ExitSignalEvaluationEventWriter, metrics_hub::MetricsHub,
    },
    runtime_config::DecisionPolicyStore,
};

/// A freshly re-inferred signal for one market at exit-evaluation time.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FreshSignal {
    pub model_artifact_hash: ContentHash,
    pub factor_snapshot_hash: ContentHash,
    /// Fresh composite score.
    pub composite_score: Probability,
    /// Fresh expected return (basis points).
    pub expected_return_bps: Bps,
    /// Whether the market would still pass its frozen Route-local model gate.
    pub route_gate_eligible: bool,
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
        lot: &StrategyPositionLot,
        mark_price: Option<Price>,
        now: DateTime<Utc>,
    ) -> QuantResult<Option<FreshSignal>>;
}

/// Dependencies for [`ReinferenceSignalEvaluator`].
pub struct ReinferenceSignalEvaluatorDeps<R> {
    pub reinferer: R,
    pub config: Arc<DecisionPolicyStore>,
    pub metrics: Arc<MetricsHub>,
    pub audit: Arc<ExitSignalEvaluationEventWriter>,
}

/// Model-driven exit-signal evaluator using the score-degradation criterion.
pub struct ReinferenceSignalEvaluator<R> {
    reinferer: R,
    config: Arc<DecisionPolicyStore>,
    metrics: Arc<MetricsHub>,
    audit: Arc<ExitSignalEvaluationEventWriter>,
}

impl<R> ReinferenceSignalEvaluator<R> {
    #[must_use]
    pub fn new(deps: ReinferenceSignalEvaluatorDeps<R>) -> Self {
        Self {
            reinferer: deps.reinferer,
            config: deps.config,
            metrics: deps.metrics,
            audit: deps.audit,
        }
    }

    /// Mirror one re-inference evaluation into the unified audit fact.
    fn audit(
        &self,
        ctx: &ExitSignalContext<'_>,
        verdict: &ExitSignalVerdict,
        fresh_composite: Option<Probability>,
        shadow: bool,
    ) {
        let now_ms = ctx.now.timestamp_millis();
        let ch_verdict: ChExitSignalVerdict = verdict.into();
        self.audit.write(QuantExitSignalEvaluationEventRow {
            event_time: now_ms,
            order_intent_id: ctx.intent.order_intent_id,
            strategy_position_lot_id: ctx.lot.strategy_position_lot_id,
            market_id: ctx.lot.market_id.clone(),
            token_id: ctx.lot.token_id.clone(),
            evaluator_kind: ChExitSignalEvaluatorKind::Reinference,
            verdict: ch_verdict,
            model_version_id: Some(ctx.intent.model_version_id),
            mark_price: ctx.mark_price.map(ChPrice::from),
            entry_composite_score: ChProbability::from(
                ctx.intent.exit_policy_json.entry_composite_score,
            ),
            fresh_composite_score: fresh_composite.map(ChProbability::from),
            exit_alpha_bps: None,
            confidence: None,
            target_cumulative_exit_pct: None,
            shadow: u8::from(shadow),
            detail: ch_verdict.as_str().to_owned(),
            ingestion_time: now_ms,
        });
    }
}

/// Pure score-degradation verdict (testable independent of re-inference I/O).
#[must_use]
pub fn degradation_verdict(
    entry_composite_score: Probability,
    fresh: &FreshSignal,
    policy: &ThesisInvalidationPolicy,
) -> ExitSignalVerdict {
    if policy.require_route_gate_eligibility && !fresh.route_gate_eligible {
        return ExitSignalVerdict::ThesisInvalidated {
            detail: "market no longer passes its frozen Route-local model gate".to_owned(),
        };
    }
    if fresh.expected_return_bps < policy.min_expected_return_bps {
        return ExitSignalVerdict::ThesisInvalidated {
            detail: format!(
                "expected return collapsed to {} bps",
                fresh.expected_return_bps.inner()
            ),
        };
    }
    let floor = entry_composite_score.inner() * policy.min_score_retention;
    if fresh.composite_score.inner() < floor {
        return ExitSignalVerdict::ThesisInvalidated {
            detail: format!(
                "composite score {} fell below {}% of entry {}",
                fresh.composite_score.inner(),
                (policy.min_score_retention * Decimal::from(100)).normalize(),
                entry_composite_score.inner()
            ),
        };
    }
    ExitSignalVerdict::Holds
}

#[async_trait]
impl<R: ExitSignalReinferer> ExitSignalEvaluator for ReinferenceSignalEvaluator<R> {
    async fn evaluate(&self, ctx: ExitSignalContext<'_>) -> ExitSignalEvaluation {
        let policy = self.config.current().execution_risk.exit_monitor.clone();
        if !policy.signal_reinference.enabled {
            self.metrics.inc_exit_signal_reinference("disabled");
            return ExitSignalEvaluation::verdict(ExitSignalVerdict::Indeterminate {
                detail: "signal re-inference disabled in config".to_owned(),
            });
        }

        // Thesis-invalidation is a *forced* exit (ladder tier 5, fires even under
        // hold-to-resolution). It is only ever auto-submitted for intents entered
        // under active-policy authorization; a human owns the exit for
        // operator-authorized positions, so re-inference holds fail-safe.
        if ctx.intent.authorization_kind != Some(AuthorizationKind::ActivePolicy) {
            self.metrics
                .inc_exit_signal_reinference("skipped_non_active_policy");
            return ExitSignalEvaluation::verdict(ExitSignalVerdict::Indeterminate {
                detail: "thesis-invalidation forced exit requires active-policy authorization"
                    .to_owned(),
            });
        }

        let entry_score = ctx.intent.exit_policy_json.entry_composite_score;
        let invalidation = &ctx.intent.exit_policy_json.thesis_invalidation;

        let (verdict, fresh) = match self
            .reinferer
            .reinfer(ctx.intent, ctx.lot, ctx.mark_price, ctx.now)
            .await
        {
            Ok(Some(fresh)) => {
                self.metrics.inc_exit_signal_reinference("fresh");
                (
                    degradation_verdict(entry_score, &fresh, invalidation),
                    Some(fresh),
                )
            }
            Ok(None) => {
                self.metrics.inc_exit_signal_reinference("unavailable");
                (
                    ExitSignalVerdict::Indeterminate {
                        detail: "signal re-inference unavailable (missing features/model/stale)"
                            .to_owned(),
                    },
                    None,
                )
            }
            Err(error) => {
                self.metrics.inc_exit_signal_reinference("error");
                tracing::warn!(%error, "exit signal re-inference failed; holding (fail-safe)");
                (
                    ExitSignalVerdict::Indeterminate {
                        detail: format!("re-inference error: {error}"),
                    },
                    None,
                )
            }
        };

        // Audit the model-determined verdict (pre-shadow-suppression) so shadow
        // and live re-inference are both measurable ex-post.
        self.audit(
            &ctx,
            &verdict,
            fresh.as_ref().map(|signal| signal.composite_score),
            policy.signal_reinference.shadow_mode,
        );

        let observation = fresh.as_ref().and_then(|signal| {
            let mark = ctx.mark_price?;
            entry_score
                .is_positive()
                .then(|| ExitReinferenceObservation {
                    observed_at: ctx.now,
                    model_version_id: ctx.intent.model_version_id,
                    model_artifact_hash: signal.model_artifact_hash,
                    factor_snapshot_hash: signal.factor_snapshot_hash,
                    mark,
                    score: signal.composite_score,
                    score_retention: signal.composite_score.inner() / entry_score.inner(),
                    expected_return_bps: signal.expected_return_bps,
                    route_gate_eligible: signal.route_gate_eligible,
                    verdict: verdict.reinference_verdict_kind(),
                    detail: verdict.reinference_verdict_detail(),
                    shadow: policy.signal_reinference.shadow_mode,
                })
        });
        let verdict = if policy.signal_reinference.shadow_mode {
            suppress_shadow_verdict(&self.metrics, &verdict)
        } else {
            verdict
        };
        ExitSignalEvaluation {
            verdict,
            reinference: observation,
        }
    }
}

impl ExitSignalVerdict {
    const fn reinference_verdict_kind(&self) -> ExitReinferenceVerdictKind {
        match self {
            Self::ThesisInvalidated { .. } => ExitReinferenceVerdictKind::ThesisInvalidated,
            Self::Holds | Self::OpportunisticSell { .. } => ExitReinferenceVerdictKind::Holds,
            Self::Indeterminate { .. } => ExitReinferenceVerdictKind::Indeterminate,
        }
    }
}

impl ExitSignalVerdict {
    fn reinference_verdict_detail(&self) -> String {
        match self {
            Self::ThesisInvalidated { detail }
            | Self::OpportunisticSell { detail, .. }
            | Self::Indeterminate { detail } => detail.clone(),
            Self::Holds => "thesis holds".to_owned(),
        }
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
    use quant_pivot_models::types::{Bps, ContentHash, Probability, ThesisInvalidationPolicy};
    use rust_decimal::Decimal;
    use rust_decimal_macros::dec;

    use super::{FreshSignal, degradation_verdict, suppress_shadow_verdict};
    use crate::{execution::ExitSignalVerdict, observability::metrics_hub::MetricsHub};

    fn fresh(score: &str, ret_bps: i64, eligible: bool) -> FreshSignal {
        FreshSignal {
            model_artifact_hash: ContentHash::parse(&format!("blake3:{}", "a".repeat(64)))
                .expect("hash"),
            factor_snapshot_hash: ContentHash::parse(&format!("blake3:{}", "b".repeat(64)))
                .expect("hash"),
            composite_score: Probability::new(score.parse().unwrap()),
            expected_return_bps: Bps::new(Decimal::from(ret_bps)),
            route_gate_eligible: eligible,
        }
    }

    fn policy() -> ThesisInvalidationPolicy {
        ThesisInvalidationPolicy {
            min_score_retention: dec!(0.6),
            min_expected_return_bps: Bps::new(Decimal::ONE),
            require_route_gate_eligibility: true,
        }
    }

    #[test]
    fn ineligible_invalidates() {
        let v = degradation_verdict(
            Probability::new(dec!(0.8)),
            &fresh("0.79", 100, false),
            &policy(),
        );
        assert!(matches!(v, ExitSignalVerdict::ThesisInvalidated { .. }));
    }

    #[test]
    fn non_positive_return_invalidates() {
        let v = degradation_verdict(
            Probability::new(dec!(0.8)),
            &fresh("0.79", 0, true),
            &policy(),
        );
        assert!(matches!(v, ExitSignalVerdict::ThesisInvalidated { .. }));
    }

    #[test]
    fn score_below_floor_invalidates() {
        // floor = 0.8 * 0.6 = 0.48; fresh 0.40 < 0.48
        let v = degradation_verdict(
            Probability::new(dec!(0.8)),
            &fresh("0.40", 100, true),
            &policy(),
        );
        assert!(matches!(v, ExitSignalVerdict::ThesisInvalidated { .. }));
    }

    #[test]
    fn healthy_holds() {
        let v = degradation_verdict(
            Probability::new(dec!(0.8)),
            &fresh("0.70", 100, true),
            &policy(),
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
