//! [`LotReplayBacktester`]: the deterministic lot-level Sell replay loop.
//!
//! Symmetric to [`crate::backtest::PortfolioReplayBacktester`], but the
//! atomic unit is one held lot instead of one `as_of` cross-section tick.
//!
//! # Residual-shares state machine (production-aligned)
//!
//! Production opportunistic exits target a **cumulative** fraction of a frozen
//! entry-filled denominator and sell only the incremental delta
//! (`exit_monitor::opportunistic_delta`). This engine mirrors that:
//!
//! 1. The replay denominator is the first decision's
//!    [`LotTrainingContext::remaining_shares`] (shares under management at the
//!    start of the simulated opportunistic window).
//! 2. On each fire, sell
//!    `max(0, target×denominator − already_sold)`, capped by simulated
//!    remaining, and accrue
//!    `(sold / historical_remaining) × (frozen hold_vs_exit_alpha_bps / 1e4)`.
//!    That scale is exact while the simulated path is still on the historical
//!    remaining path — the label was built for exiting the full historical
//!    remaining at that instant.
//! 3. **Path-divergence fail-closed:** after any simulated sale that leaves
//!    residual shares, the next historical row still carries the *unreduced*
//!    historical `remaining_shares` / `position_state`. Continuing to score
//!    those rows would be train-serve skew. When
//!    `simulated_remaining ≠ historical_remaining`, the engine **stops** —
//!    residual shares are treated as held to terminal (zero incremental alpha
//!    by definition). A 100% exit terminates cleanly without divergence.
//!
//! A lot that never fires is held to its natural terminal outcome
//! (`return_value = 0`, by definition zero alpha relative to itself).
//!
//! No exit-fill simulation is re-derived here: [`crate::training::labeler`]'s
//! `HoldVsExitProceedsLabeler` already froze the "exit now vs hold to
//! terminal" net-proceeds delta into every decision's `hold_vs_exit_alpha_bps`
//! training label at dataset-build time (point-in-time correct). This engine
//! only reads that frozen label back while the simulated path remains
//! isomorphic to the historical remaining path.

use chrono::{DateTime, Utc};
use quant_pivot_error::{QuantResult, research::ResearchError};
use quant_pivot_models::{
    enums::quant::OutcomeSide,
    types::{OutcomeTokenBinding, PositionId},
};
use rust_decimal::Decimal;

use crate::{
    model::{
        LabelSelector, SellScoreInput, SellScorerRuntime, SellSignalPolicy,
        sell_scorer::is_position_state_factor, sell_signal_fires, sell_signal_target,
    },
    training::TrainingExample,
};
/// Absolute share tolerance when comparing simulated vs historical remaining
/// (Decimal money-domain equality under rounding).
fn remaining_eps() -> Decimal {
    Decimal::new(1, 8) // 1e-8
}

/// One lot's decision points, already sorted ascending by `as_of`.
#[derive(Debug, Clone)]
pub struct LotDecisionSequence {
    /// The lot this sequence replays.
    pub position_id: PositionId,
    /// The lot's `ExitDecision` training examples, ascending `as_of`.
    pub decisions: Vec<TrainingExample>,
}

/// One lot's replayed outcome.
#[derive(Debug, Clone)]
pub struct LotOutcome {
    /// The lot this outcome replays.
    pub position_id: PositionId,
    /// First decision time (timeline / DSR period anchor).
    pub decision_at: DateTime<Utc>,
    /// This lot's contribution to the reconstructed path's return series (a
    /// fractional return): the sum over every on-path incremental scale-out of
    /// `(sold_shares / historical_remaining) × (frozen hold_vs_exit_alpha_bps / 10_000)`.
    /// `0` when the lot never fires (held to terminal — zero alpha relative
    /// to itself, by definition). Residual shares after path divergence also
    /// contribute `0` (held to terminal under the diverged counterfactual).
    pub return_value: Decimal,
    /// Final simulated cumulative exit fraction of the replay denominator
    /// (`0` = never exited, `1` = fully exited).
    pub cumulative_exit_pct: Decimal,
    /// `(predicted exit_alpha_bps, realized hold_vs_exit_alpha_bps)` for
    /// **every on-path** decision point scored before path divergence — a
    /// general scoring-skill diagnostic, decoupled from the specific business
    /// threshold that decides whether to act.
    pub rank_pairs: Vec<(Decimal, Decimal)>,
    /// Whether replay stopped because simulated remaining diverged from the
    /// historical remaining path (partial exit left residual shares whose
    /// subsequent PIT rows are no longer counterfactual-valid).
    pub path_diverged: bool,
}

/// The result of replaying every lot in one fold.
pub struct LotBacktestRunResult {
    /// One outcome per input [`LotDecisionSequence`], same order.
    pub lots: Vec<LotOutcome>,
}

/// Inputs to one lot-level replay run.
pub struct LotBacktestInputs<'a> {
    /// The Sell scorer under test (already hash/schema-validated by the factory).
    pub model: &'a dyn SellScorerRuntime,
    /// Governed opportunistic-exit thresholds (the same ones production uses).
    pub policy: SellSignalPolicy,
    /// The label the replay reads as ground truth (`hold_vs_exit_alpha_bps`
    /// in production; kept selector-driven rather than hardcoded so a future
    /// horizon variant needs no code change).
    pub label: LabelSelector,
    /// Lots to replay, each already sorted ascending by `as_of`.
    pub lots: &'a [LotDecisionSequence],
}

/// Runs a lot-level hold-vs-exit replay of a Sell scorer.
pub trait LotBacktester: Send + Sync {
    /// # Errors
    ///
    /// Propagates [`SellScorerRuntime::score`] failures and fails closed when
    /// a decision point is missing `position_state`, `lot_context`, or the
    /// selected label (a malformed `ExitDecision` dataset, never silently
    /// skipped).
    fn run(&self, inputs: LotBacktestInputs<'_>) -> QuantResult<LotBacktestRunResult>;
}

/// The production [`LotBacktester`]: synchronous (Sell scoring is pure, no
/// async I/O), residual-shares state machine with path-divergence stop.
#[derive(Debug, Default, Clone, Copy)]
pub struct LotReplayBacktester;

impl LotReplayBacktester {
    /// Construct the backtester.
    #[must_use]
    pub const fn new() -> Self {
        Self
    }
}

impl LotBacktester for LotReplayBacktester {
    fn run(&self, inputs: LotBacktestInputs<'_>) -> QuantResult<LotBacktestRunResult> {
        let lots = inputs
            .lots
            .iter()
            .map(|sequence| replay_lot(inputs.model, &inputs.policy, &inputs.label, sequence))
            .collect::<QuantResult<Vec<_>>>()?;
        Ok(LotBacktestRunResult { lots })
    }
}

/// Replay one lot with the residual-shares / path-divergence state machine.
fn replay_lot(
    model: &dyn SellScorerRuntime,
    policy: &SellSignalPolicy,
    label: &LabelSelector,
    sequence: &LotDecisionSequence,
) -> QuantResult<LotOutcome> {
    let (first_as_of, denominator) = (sequence).replay_denominator()?;
    let mut rank_pairs = Vec::with_capacity(sequence.decisions.len());
    let mut simulated_sold = Decimal::ZERO;
    let mut return_value = Decimal::ZERO;
    let mut path_diverged = false;
    let bps_scale = Decimal::from(10_000);

    for decision in &sequence.decisions {
        let simulated_remaining = (denominator - simulated_sold).max(Decimal::ZERO);
        if simulated_remaining <= remaining_eps() {
            break;
        }
        match score_on_path_decision(&OnPathDecisionArgs {
            model,
            policy,
            label,
            sequence,
            decision,
            simulated_remaining,
            simulated_sold,
            denominator,
            bps_scale,
        })? {
            OnPathStep::Diverged => {
                path_diverged = true;
                break;
            }
            OnPathStep::Hold { rank_pair } => {
                rank_pairs.push(rank_pair);
            }
            OnPathStep::Sold {
                rank_pair,
                incremental,
                alpha,
            } => {
                rank_pairs.push(rank_pair);
                return_value += alpha;
                simulated_sold += incremental;
            }
        }
    }

    Ok(LotOutcome {
        position_id: sequence.position_id,
        decision_at: first_as_of,
        return_value,
        cumulative_exit_pct: (simulated_sold / denominator)
            .min(Decimal::ONE)
            .max(Decimal::ZERO),
        rank_pairs,
        path_diverged,
    })
}

impl LotDecisionSequence {
    fn replay_denominator(&self) -> QuantResult<(DateTime<Utc>, Decimal)> {
        let Some(first) = self.decisions.first() else {
            return Err(ResearchError::ValidationMethodology {
                detail: format!(
                    "lot {} has an empty decision sequence — refuse to invent a zero return",
                    self.position_id
                ),
            }
            .into());
        };
        let Some(first_ctx) = &first.lot_context else {
            return Err(ResearchError::ValidationMethodology {
            detail: format!(
                "lot {} first decision is missing lot_context; rebuild the ExitDecision dataset",
                self.position_id
            ),
        }
        .into());
        };
        let denominator = first_ctx.remaining_shares.inner();
        if denominator <= Decimal::ZERO {
            return Err(ResearchError::ValidationMethodology {
            detail: format!(
                "lot {} first decision has non-positive remaining_shares — refuse to invent a denominator",
                self.position_id
            ),
        }
        .into());
        }
        Ok((first.decision_at(), denominator))
    }
}

enum OnPathStep {
    Diverged,
    Hold {
        rank_pair: (Decimal, Decimal),
    },
    Sold {
        rank_pair: (Decimal, Decimal),
        incremental: Decimal,
        alpha: Decimal,
    },
}

struct OnPathDecisionArgs<'a> {
    model: &'a dyn SellScorerRuntime,
    policy: &'a SellSignalPolicy,
    label: &'a LabelSelector,
    sequence: &'a LotDecisionSequence,
    decision: &'a TrainingExample,
    simulated_remaining: Decimal,
    simulated_sold: Decimal,
    denominator: Decimal,
    bps_scale: Decimal,
}

fn score_on_path_decision(args: &OnPathDecisionArgs<'_>) -> QuantResult<OnPathStep> {
    let Some(ctx) = &args.decision.lot_context else {
        return Err(ResearchError::ValidationMethodology {
            detail: format!(
                "lot {} decision at {} is missing lot_context; rebuild the ExitDecision dataset",
                args.sequence.position_id,
                args.decision.decision_at()
            ),
        }
        .into());
    };
    let hist_remaining = ctx.remaining_shares.inner();
    if (hist_remaining - args.simulated_remaining).abs() > remaining_eps() {
        return Ok(OnPathStep::Diverged);
    }
    let Some(position_state) = args.decision.position_state.clone() else {
        return Err(ResearchError::ValidationMethodology {
            detail: format!(
                "lot {} decision at {} is missing position_state; rebuild the ExitDecision dataset",
                args.sequence.position_id,
                args.decision.decision_at()
            ),
        }
        .into());
    };
    let Some(realized) = selected_label_value(args.decision, args.label) else {
        return Err(ResearchError::ValidationMethodology {
            detail: format!(
                "lot {} decision at {} has no label `{}` @ horizon {}s",
                args.sequence.position_id,
                args.decision.decision_at(),
                args.label.name,
                args.label.horizon_secs
            ),
        }
        .into());
    };
    let market_factors = args
        .decision
        .factor_values
        .iter()
        .filter(|factor| !is_position_state_factor(&factor.name))
        .cloned()
        .collect();
    let lot_context =
        args.decision
            .lot_context
            .as_ref()
            .ok_or_else(|| ResearchError::ValidationMethodology {
                detail: format!(
                    "lot {} decision at {} is missing outcome orientation",
                    args.sequence.position_id,
                    args.decision.decision_at()
                ),
            })?;
    let secondary_token = args
        .decision
        .selected_market
        .secondary_token_id
        .as_ref()
        .ok_or_else(|| ResearchError::ValidationMethodology {
            detail: format!(
                "lot {} decision at {} has no binary secondary token",
                args.sequence.position_id,
                args.decision.decision_at()
            ),
        })?;
    let (yes_token, no_token) = match lot_context.outcome_side {
        OutcomeSide::Yes => (&args.decision.token_id, secondary_token),
        OutcomeSide::No => (secondary_token, &args.decision.token_id),
    };
    let outcome_binding = OutcomeTokenBinding::try_new(
        args.decision.market_id.clone(),
        yes_token.clone(),
        no_token.clone(),
        args.decision.token_id.clone(),
        lot_context.outcome_side,
    )
    .map_err(|error| ResearchError::ValidationMethodology {
        detail: format!(
            "lot {} has an invalid outcome binding: {error}",
            args.sequence.position_id
        ),
    })?;
    let score = args.model.score(&SellScoreInput {
        outcome_binding,
        market_factors,
        position_state,
    })?;
    let rank_pair = (score.exit_alpha_bps.inner(), realized);
    if !sell_signal_fires(&score, args.policy) {
        return Ok(OnPathStep::Hold { rank_pair });
    }
    let target = sell_signal_target(&score, args.policy);
    let desired_sold = (args.denominator * target).min(args.denominator);
    let incremental = (desired_sold - args.simulated_sold)
        .max(Decimal::ZERO)
        .min(args.simulated_remaining);
    if incremental <= remaining_eps() {
        return Ok(OnPathStep::Hold { rank_pair });
    }
    // Label is the alpha of exiting the full historical remaining at this
    // instant; scale by the share of that remaining we actually sell.
    let alpha = (incremental / hist_remaining) * (realized / args.bps_scale);
    Ok(OnPathStep::Sold {
        rank_pair,
        incremental,
        alpha,
    })
}

/// Null-policy baselines for Sell validation (always-hold / exit-at-first).
///
/// These are model-free reference strategies evaluated on the same frozen
/// labels the scorer is judged against — required so CPCV/DSR numbers answer
/// "does the model beat a trivial exit policy?", not only "is the Sharpe
/// distribution non-empty?".
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SellNullBaseline {
    /// Never exit ⇒ zero alpha by definition.
    AlwaysHold,
    /// Exit 100% at the first decision point's frozen label (on-path full exit).
    ExitAtFirstDecision,
}

/// Evaluate a null baseline on one lot sequence (no model scoring).
///
/// # Errors
///
/// Fails closed when the sequence is empty, missing `lot_context`, or the
/// first decision lacks the selected label (`ExitAtFirstDecision`).
pub fn replay_lot_null_baseline(
    baseline: SellNullBaseline,
    label: &LabelSelector,
    sequence: &LotDecisionSequence,
) -> QuantResult<LotOutcome> {
    let Some(first) = sequence.decisions.first() else {
        return Err(ResearchError::ValidationMethodology {
            detail: format!(
                "lot {} has an empty decision sequence — refuse to invent a baseline return",
                sequence.position_id
            ),
        }
        .into());
    };
    if first.lot_context.is_none() {
        return Err(ResearchError::ValidationMethodology {
            detail: format!(
                "lot {} first decision is missing lot_context — refuse to invent a baseline",
                sequence.position_id
            ),
        }
        .into());
    }
    match baseline {
        SellNullBaseline::AlwaysHold => Ok(LotOutcome {
            position_id: sequence.position_id,
            decision_at: first.decision_at(),
            return_value: Decimal::ZERO,
            cumulative_exit_pct: Decimal::ZERO,
            rank_pairs: Vec::new(),
            path_diverged: false,
        }),
        SellNullBaseline::ExitAtFirstDecision => {
            let Some(realized) = selected_label_value(first, label) else {
                return Err(ResearchError::ValidationMethodology {
                    detail: format!(
                        "lot {} first decision at {} has no label `{}` @ horizon {}s",
                        sequence.position_id,
                        first.decision_at(),
                        label.name,
                        label.horizon_secs
                    ),
                }
                .into());
            };
            Ok(LotOutcome {
                position_id: sequence.position_id,
                decision_at: first.decision_at(),
                return_value: realized / Decimal::from(10_000),
                cumulative_exit_pct: Decimal::ONE,
                rank_pairs: vec![(realized, realized)],
                path_diverged: false,
            })
        }
    }
}

/// The selected label's resolved value on `example`, if present.
fn selected_label_value(example: &TrainingExample, label: &LabelSelector) -> Option<Decimal> {
    example
        .labels
        .iter()
        .find(|row| {
            let name_matches = row.label_name == label.name;
            let horizon_matches = row.horizon_secs == label.horizon_secs;
            name_matches && horizon_matches
        })
        .map(|row| row.value)
}

#[cfg(test)]
mod tests {
    use std::{collections::BTreeMap, sync::Mutex, vec::IntoIter};

    use chrono::{DateTime, TimeZone, Utc};
    use quant_pivot_error::QuantResult;
    use quant_pivot_models::{
        domain::data_plane::DecisionClock,
        enums::{
            common::MarketCategory,
            quant::{DataQualityStatus, OutcomeSide},
        },
        types::{
            Bps, ContentHash, MarketId, ModelVersionId, OrderIntentId, PositionId, Price,
            Probability, SchemaVersion, Shares, TokenId, TrainingExampleId, TrainingSampleSource,
        },
    };
    use rust_decimal::Decimal;
    use rust_decimal_macros::dec;

    use super::{
        LotBacktestInputs, LotBacktester, LotDecisionSequence, LotReplayBacktester,
        SellNullBaseline, replay_lot_null_baseline,
    };
    use crate::{
        execution_semantics::BookFidelity,
        features::{FeatureName, FeatureVector},
        model::{
            LabelSelector, PositionStateFeatures, SellScore, SellScoreInput, SellScorerRuntime,
            SellSignalPolicy,
        },
        training::{
            HOLD_VS_EXIT_ALPHA_BPS, LotTrainingContext, TrainingExample, TrainingLabel, fixtures,
        },
    };

    fn ts(secs: i64) -> DateTime<Utc> {
        Utc.timestamp_opt(1_700_000_000 + secs, 0).unwrap()
    }

    impl PositionStateFeatures {
        fn test_fixture() -> Self {
            Self {
                unrealized_pnl_pct: Some(dec!(0)),
                time_in_trade_ratio: dec!(0.1),
                peak_mark_drawdown: Some(dec!(0)),
            }
        }
    }

    fn decision(as_of: DateTime<Utc>, label_value: Decimal, remaining: Decimal) -> TrainingExample {
        let market_id = MarketId::new("0xmarket");
        let token_id = TokenId::new("yes");
        let mut selected_market =
            fixtures::selected_market(&market_id, &token_id, MarketCategory::Sports);
        selected_market.secondary_token_id = Some(TokenId::new("no"));
        TrainingExample {
            example_id: TrainingExampleId::from_v7(),
            market_id: market_id.clone(),
            token_id: token_id.clone(),
            selected_market,
            decision_boundary: DecisionClock::new(0).boundary(as_of).expect("boundary"),
            sample_source: TrainingSampleSource::ExitDecision,
            feature_vector: FeatureVector {
                market_id,
                token_id: Some(token_id),
                decision_at: as_of,
                generic_schema_version: SchemaVersion::FIRST,
                generic: BTreeMap::new(),
                domain: None,
                data_quality: DataQualityStatus::Fresh,
            },
            factor_values: Vec::new(),
            labels: vec![TrainingLabel {
                label_name: HOLD_VS_EXIT_ALPHA_BPS,
                horizon_secs: 0,
                value: label_value,
                is_resolved: true,
                matured_at: as_of,
            }],
            source_refs: Vec::new(),
            decision_capture: None,
            lot_context: Some(LotTrainingContext {
                order_intent_id: OrderIntentId::from_v7(),
                position_id: PositionId::from_v7(),
                outcome_side: OutcomeSide::Yes,
                remaining_shares: Shares::new(remaining),
                avg_price: Price::new(dec!(0.5)),
                peak_mark: None,
                opened_at: as_of,
                max_hold_secs: 86_400,
            }),
            position_state: Some(PositionStateFeatures::test_fixture()),
            book_fidelity: Some(BookFidelity::FullL2),
        }
    }

    impl LabelSelector {
        fn test_fixture() -> Self {
            Self {
                name: HOLD_VS_EXIT_ALPHA_BPS,
                horizon_secs: 0,
            }
        }
    }

    /// Scripted scorer: `(exit_alpha_bps, recommended_cumulative_exit_pct)` per call.
    struct ScriptedScorer {
        script: Mutex<IntoIter<(Decimal, Decimal)>>,
    }

    impl ScriptedScorer {
        fn new(script: Vec<(Decimal, Decimal)>) -> Self {
            Self {
                script: Mutex::new(script.into_iter()),
            }
        }
    }

    impl SellScorerRuntime for ScriptedScorer {
        fn model_version_id(&self) -> ModelVersionId {
            ModelVersionId::from_v7()
        }
        fn feature_schema_hash(&self) -> ContentHash {
            ContentHash::parse(&format!("blake3:{}", "0".repeat(64))).expect("hash")
        }
        fn required_features(&self) -> Vec<FeatureName> {
            Vec::new()
        }
        fn score(&self, _input: &SellScoreInput) -> QuantResult<SellScore> {
            let (alpha, sell_pct) = self
                .script
                .lock()
                .expect("lock")
                .next()
                .expect("scripted scorer ran out of scripted scores");
            Ok(SellScore {
                exit_alpha_bps: Bps::new(alpha),
                p_exit_better: Probability::new(dec!(0.9)),
                confidence: Probability::new(dec!(0.9)),
                recommended_cumulative_exit_pct: sell_pct,
                net: dec!(1),
            })
        }
    }

    impl SellSignalPolicy {
        fn test_fixture() -> Self {
            Self {
                min_confidence: dec!(0.5),
                min_p_exit_better: dec!(0.5),
                min_expected_alpha_bps: dec!(50),
                max_sell_pct: dec!(1),
            }
        }
    }

    #[test]
    fn lot_replay_full_decisions() {
        let position_id = PositionId::from_v7();
        let mut d0 = decision(ts(0), dec!(-20), dec!(100));
        let mut d1 = decision(ts(60), dec!(100), dec!(100));
        let mut d2 = decision(ts(120), dec!(9999), dec!(100));
        d0.lot_context.as_mut().unwrap().position_id = position_id;
        d1.lot_context.as_mut().unwrap().position_id = position_id;
        d2.lot_context.as_mut().unwrap().position_id = position_id;
        let sequence = LotDecisionSequence {
            position_id,
            decisions: vec![d0, d1, d2],
        };
        // First call holds; second fires at 100% cumulative → third must never be scored.
        let model = ScriptedScorer::new(vec![(dec!(30), dec!(1)), (dec!(120), dec!(1))]);
        let result = LotReplayBacktester::new()
            .run(LotBacktestInputs {
                model: &model,
                policy: SellSignalPolicy::test_fixture(),
                label: LabelSelector::test_fixture(),
                lots: &[sequence],
            })
            .expect("replay");
        let outcome = &result.lots[0];
        assert_eq!(outcome.return_value, dec!(100) / Decimal::from(10_000));
        assert_eq!(outcome.cumulative_exit_pct, Decimal::ONE);
        assert_eq!(outcome.rank_pairs.len(), 2);
        assert!(!outcome.path_diverged);
    }

    #[test]
    fn lot_replay_partial_divergence() {
        let position_id = PositionId::from_v7();
        // History never sold — remaining stays 100. After a 50% simulated
        // exit, the next historical row still shows remaining=100 → diverge.
        let mut d0 = decision(ts(0), dec!(100), dec!(100));
        let mut d1 = decision(ts(60), dec!(200), dec!(100));
        let mut d2 = decision(ts(120), dec!(999), dec!(100));
        d0.lot_context.as_mut().unwrap().position_id = position_id;
        d1.lot_context.as_mut().unwrap().position_id = position_id;
        d2.lot_context.as_mut().unwrap().position_id = position_id;
        let sequence = LotDecisionSequence {
            position_id,
            decisions: vec![d0, d1, d2],
        };
        let model = ScriptedScorer::new(vec![(dec!(120), dec!(0.5))]);
        let result = LotReplayBacktester::new()
            .run(LotBacktestInputs {
                model: &model,
                policy: SellSignalPolicy::test_fixture(),
                label: LabelSelector::test_fixture(),
                lots: &[sequence],
            })
            .expect("replay");
        let outcome = &result.lots[0];
        // Only the first decision is scored; 50% of 100bps = 50bps → 0.005.
        // Residual 50% held to terminal → 0 incremental alpha (no polluted
        // second-decision accrual).
        assert_eq!(
            outcome.return_value,
            (dec!(0.5) * dec!(100)) / Decimal::from(10_000)
        );
        assert_eq!(outcome.cumulative_exit_pct, dec!(0.5));
        assert_eq!(outcome.rank_pairs.len(), 1);
        assert!(outcome.path_diverged);
    }

    #[test]
    fn lot_replay_never_exits() {
        let position_id = PositionId::from_v7();
        let mut d0 = decision(ts(0), dec!(-20), dec!(100));
        let mut d1 = decision(ts(60), dec!(-10), dec!(100));
        d0.lot_context.as_mut().unwrap().position_id = position_id;
        d1.lot_context.as_mut().unwrap().position_id = position_id;
        let sequence = LotDecisionSequence {
            position_id,
            decisions: vec![d0, d1],
        };
        let model = ScriptedScorer::new(vec![(dec!(10), dec!(1)), (dec!(20), dec!(1))]);
        let result = LotReplayBacktester::new()
            .run(LotBacktestInputs {
                model: &model,
                policy: SellSignalPolicy::test_fixture(),
                label: LabelSelector::test_fixture(),
                lots: &[sequence],
            })
            .expect("replay");
        let outcome = &result.lots[0];
        assert_eq!(outcome.return_value, Decimal::ZERO);
        assert_eq!(outcome.cumulative_exit_pct, Decimal::ZERO);
        assert_eq!(outcome.rank_pairs.len(), 2);
        assert!(!outcome.path_diverged);
    }

    #[test]
    fn null_baseline_exit_label() {
        let position_id = PositionId::from_v7();
        let mut d0 = decision(ts(0), dec!(80), dec!(100));
        let mut d1 = decision(ts(60), dec!(200), dec!(100));
        d0.lot_context.as_mut().unwrap().position_id = position_id;
        d1.lot_context.as_mut().unwrap().position_id = position_id;
        let sequence = LotDecisionSequence {
            position_id,
            decisions: vec![d0, d1],
        };
        let outcome = replay_lot_null_baseline(
            SellNullBaseline::ExitAtFirstDecision,
            &LabelSelector::test_fixture(),
            &sequence,
        )
        .expect("baseline");
        assert_eq!(outcome.return_value, dec!(80) / Decimal::from(10_000));
        assert_eq!(outcome.cumulative_exit_pct, Decimal::ONE);
    }

    #[test]
    fn null_baseline_zero_alpha() {
        let position_id = PositionId::from_v7();
        let mut d0 = decision(ts(0), dec!(80), dec!(100));
        d0.lot_context.as_mut().unwrap().position_id = position_id;
        let sequence = LotDecisionSequence {
            position_id,
            decisions: vec![d0],
        };
        let outcome = replay_lot_null_baseline(
            SellNullBaseline::AlwaysHold,
            &LabelSelector::test_fixture(),
            &sequence,
        )
        .expect("baseline");
        assert_eq!(outcome.return_value, Decimal::ZERO);
        assert_eq!(outcome.cumulative_exit_pct, Decimal::ZERO);
        assert!(outcome.rank_pairs.is_empty());
    }

    #[test]
    fn position_state_inputs_formula() {
        let count = PositionStateFeatures::test_fixture()
            .direct_exit_evidence()
            .len();
        assert_eq!(count, 4);
    }
}
