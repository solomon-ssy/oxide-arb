//! Sell-side hold-vs-exit scorer runtime (Phase 06.1).
//!
//! Symmetric to the Buy-side [`WeightedFactorRuntime`](crate::model::weighted::WeightedFactorRuntime),
//! but it scores the **exit** decision for one open lot instead of an entry
//! ranking. The signed ranking net folds together the market factors and the
//! lot's own position-state pseudo-factors:
//!
//! ```text
//! signedᵢ    = dir_signᵢ · normalizedᵢ · confidenceᵢ            (∈ [-1, 1])
//! net        = Σ weightᵢ · signedᵢ                              (∈ [-1, 1], +⇒ exit)
//! alpha_bps  = output_spec.max_exit_alpha_bps · net            (may be < 0 ⇒ hold)
//! p_exit     = 1 / (1 + e^{-gain·net})                         (∈ (0, 1))
//! confidence = weighted_mean(confidenceᵢ)                      (∈ [0, 1])
//! sell_pct   = clamp₀₁(default_sell_pct + (1-default)·net⁺)    (target cumulative)
//! ```
//!
//! The scorer is a pure function; the opportunistic evaluator owns the
//! thresholds and fail-safe (a low-confidence / low-alpha score maps to Hold).

pub mod position_state;
pub mod trainer;

pub use position_state::{
    LotStateInput, PositionStateFeatures, is_position_state_factor, position_state_factor_values,
    position_state_features, position_state_signed, position_state_signed_contribution,
};
pub use trainer::{SellScorerTrainer, TrainSellScorerRequest};

use std::collections::BTreeMap;

use quant_pivot_error::{QuantResult, research::ResearchError};
use quant_pivot_models::{
    runtime_config::OpportunisticSellPolicy,
    types::{Bps, ContentHash, ModelVersionId, Probability},
};
use rust_decimal::{
    Decimal,
    prelude::{FromPrimitive, ToPrimitive},
};

use crate::{
    factors::{FactorName, FactorValue},
    features::FeatureName,
    model::artifact::SellScorerArtifact,
    precision::RESEARCH_DECIMAL_SCALE,
};

/// Inputs to one hold-vs-exit scoring call.
#[derive(Debug, Clone)]
pub struct SellScoreInput {
    /// Already-normalized market factors for the lot's market / token.
    pub market_factors: Vec<FactorValue>,
    /// Lot position-state pseudo-factors.
    pub position_state: PositionStateFeatures,
}

/// The Sell scorer's verdict inputs for one lot.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SellScore {
    /// Expected exit alpha over holding, in basis points (negative ⇒ hold wins).
    pub exit_alpha_bps: Bps,
    /// Probability that exiting now beats holding, in `[0, 1]`.
    pub p_exit_better: Probability,
    /// Aggregate model confidence, in `[0, 1]`.
    pub confidence: Probability,
    /// Recommended target cumulative exit fraction of the entry-filled shares.
    pub recommended_cumulative_exit_pct: Decimal,
    /// Raw signed ranking net (`∈ [-1, 1]`), for audit.
    pub net: Decimal,
}

/// A loaded, frozen Sell-side hold-vs-exit scorer. Business layers depend on the
/// [`SellScorerRuntime`] trait so a future ONNX / classical exit scorer can be
/// swapped in without touching the exit monitor.
pub trait SellScorerRuntime: Send + Sync {
    /// The published model version this runtime serves.
    fn model_version_id(&self) -> ModelVersionId;
    /// Feature-schema hash the artifact was built against (mismatch ⇒ abort).
    fn feature_schema_hash(&self) -> ContentHash;
    /// Features the scorer requires (surfaced for eligibility / audit).
    fn required_features(&self) -> Vec<FeatureName>;
    /// Score the hold-vs-exit decision for one lot. Pure; the caller enforces
    /// thresholds and the fail-safe hold.
    ///
    /// # Errors
    ///
    /// Currently infallible for the weighted family; the fallible signature
    /// keeps a future model family free to reject malformed input.
    fn score(&self, input: &SellScoreInput) -> QuantResult<SellScore>;
}

/// A governed weighted hold-vs-exit scorer bound to one frozen artifact.
pub struct WeightedSellScorerRuntime {
    artifact: SellScorerArtifact,
    weights: BTreeMap<FactorName, Decimal>,
}

impl WeightedSellScorerRuntime {
    /// Build a runtime from a validated Sell scorer artifact.
    ///
    /// # Errors
    ///
    /// Propagates [`SellScorerArtifact::validate`] (unnormalized / negative
    /// weights, non-positive gain, out-of-range default exit fraction).
    pub fn new(artifact: SellScorerArtifact) -> QuantResult<Self> {
        artifact.validate()?;
        let weights = artifact.weight_index();
        Ok(Self { artifact, weights })
    }

    /// Fold the market factors and position-state pseudo-factors into the signed
    /// ranking net and the weighted-mean confidence.
    fn net_and_confidence(&self, input: &SellScoreInput) -> (Decimal, Probability) {
        let mut net = Decimal::ZERO;
        let mut confidence_mass = Decimal::ZERO;
        let mut confidence_weighted = Decimal::ZERO;

        for factor in &input.market_factors {
            let Some(weight) = self.weights.get(&factor.name) else {
                continue;
            };
            if weight.is_zero() {
                continue;
            }
            // Only a scored factor contributes; missing / indeterminate factors
            // add nothing (no fabricated neutral).
            let Some(score) = factor.normalized_score() else {
                continue;
            };
            let signed =
                Decimal::from(factor.direction.as_i8()) * score.inner() * factor.confidence.inner();
            net += *weight * signed;
            if factor.confidence.inner() > Decimal::ZERO {
                confidence_mass += *weight;
                confidence_weighted += *weight * factor.confidence.inner();
            }
        }

        for (name, signed) in position_state_signed(&input.position_state) {
            let Some(weight) = self.weights.get(&name) else {
                continue;
            };
            if weight.is_zero() {
                continue;
            }
            net += *weight * signed;
            // Position-state is derived deterministically from the ledger, so it
            // carries full confidence.
            confidence_mass += *weight;
            confidence_weighted += *weight;
        }

        let confidence = if confidence_mass > Decimal::ZERO {
            (confidence_weighted / confidence_mass).round_dp(RESEARCH_DECIMAL_SCALE)
        } else {
            Decimal::ZERO
        };
        let net = net
            .round_dp(RESEARCH_DECIMAL_SCALE)
            .clamp(-Decimal::ONE, Decimal::ONE);
        (
            net,
            Probability::new(confidence.clamp(Decimal::ZERO, Decimal::ONE)),
        )
    }
}

impl SellScorerRuntime for WeightedSellScorerRuntime {
    fn model_version_id(&self) -> ModelVersionId {
        self.artifact.header.model_version_id.clone()
    }

    fn feature_schema_hash(&self) -> ContentHash {
        self.artifact.header.feature_schema_hash.clone()
    }

    fn required_features(&self) -> Vec<FeatureName> {
        self.artifact.required_features.clone()
    }

    fn score(&self, input: &SellScoreInput) -> QuantResult<SellScore> {
        let (net, confidence) = self.net_and_confidence(input);
        let spec = &self.artifact.output_spec;
        let exit_alpha = (spec.max_exit_alpha_bps * net).round_dp(RESEARCH_DECIMAL_SCALE);
        let p_exit_better = logistic_probability(spec.p_exit_gain * net);
        let net_pos = net.max(Decimal::ZERO);
        let recommended = (spec.default_sell_pct
            + (Decimal::ONE - spec.default_sell_pct) * net_pos)
            .clamp(Decimal::ZERO, Decimal::ONE)
            .round_dp(RESEARCH_DECIMAL_SCALE);
        Ok(SellScore {
            exit_alpha_bps: Bps::new(exit_alpha),
            p_exit_better,
            confidence,
            recommended_cumulative_exit_pct: recommended,
            net,
        })
    }
}

/// Governed opportunistic-exit thresholds, frozen from
/// `execution.exit_monitor.opportunistic_sell` runtime-config.
///
/// The single source of truth for "should this score trigger an exit" —
/// consumed identically by the live opportunistic-sell evaluator
/// (`quant-pivot-core`) and by Phase 11.5.1's offline `LotReplayBacktester`,
/// so the live and CPCV-replayed decision rule can never drift apart.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SellSignalPolicy {
    /// Minimum scorer confidence to act (below ⇒ hold).
    pub min_confidence: Decimal,
    /// Minimum P(exit now beats hold) to act (below ⇒ hold).
    pub min_p_exit_better: Decimal,
    /// Minimum expected exit alpha (bps over holding) to act (below ⇒ hold).
    pub min_expected_alpha_bps: Decimal,
    /// Upper bound on the target cumulative exit fraction the model may request.
    pub max_sell_pct: Decimal,
}

impl SellSignalPolicy {
    /// Parse the governed `execution.exit_monitor.opportunistic_sell` section
    /// into the pure research policy used by both live evaluation and CPCV
    /// lot replay. Fail-closed on malformed decimals — never invent a
    /// fallback that could open the live gate while CPCV rejects (or vice
    /// versa).
    ///
    /// # Errors
    ///
    /// Returns [`ResearchError::ValidationMethodology`] when any decimal
    /// field cannot be parsed.
    pub fn try_from_runtime(policy: &OpportunisticSellPolicy) -> QuantResult<Self> {
        let parse = |field: &'static str, value: &str| -> QuantResult<Decimal> {
            value.trim().parse::<Decimal>().map_err(|error| {
                ResearchError::ValidationMethodology {
                    detail: format!(
                        "`execution.exit_monitor.opportunistic_sell.{field}` = `{value}` \
                         is not a valid decimal: {error}"
                    ),
                }
                .into()
            })
        };
        Ok(Self {
            min_confidence: parse("min_confidence", &policy.min_confidence.value)?,
            min_p_exit_better: parse("min_p_exit_better", &policy.min_p_exit_better.value)?,
            min_expected_alpha_bps: Decimal::from(policy.min_expected_alpha_bps),
            max_sell_pct: parse("max_sell_pct", &policy.max_sell_pct.value)?,
        })
    }
}

/// The target cumulative exit fraction `score` requests, capped by
/// `policy.max_sell_pct` and clamped to `[0, 1]`.
///
/// Computed unconditionally (including on the Hold path) so callers can
/// audit "what the model would have requested" even when
/// [`sell_signal_fires`] returns `false`.
#[must_use]
pub fn sell_signal_target(score: &SellScore, policy: &SellSignalPolicy) -> Decimal {
    score
        .recommended_cumulative_exit_pct
        .min(policy.max_sell_pct)
        .clamp(Decimal::ZERO, Decimal::ONE)
}

/// Whether `score` clears every governed opportunistic-exit threshold.
#[must_use]
pub fn sell_signal_fires(score: &SellScore, policy: &SellSignalPolicy) -> bool {
    score.confidence.inner() >= policy.min_confidence
        && score.p_exit_better.inner() >= policy.min_p_exit_better
        && score.exit_alpha_bps.inner() >= policy.min_expected_alpha_bps
}

/// The signed `[-1, 1]` position-state contributions keyed by pseudo-factor name.
/// All three lean positive (toward exit) as their magnitude grows; the trained
/// weights (possibly zero) decide how much each matters.
fn logistic_probability(z: Decimal) -> Probability {
    let z = z.to_f64().unwrap_or(0.0);
    let squashed = 1.0 / (1.0 + (-z).exp());
    let quantized = Decimal::from_f64(squashed)
        .unwrap_or_else(|| Decimal::new(5, 1))
        .round_dp(RESEARCH_DECIMAL_SCALE);
    Probability::new(quantized.clamp(Decimal::ZERO, Decimal::ONE))
}

#[cfg(test)]
mod tests {
    use super::{
        PositionStateFeatures, SellScoreInput, SellScorerRuntime, WeightedSellScorerRuntime,
    };
    use quant_pivot_models::{
        enums::{factor::FactorFamily, quant::FactorDirection},
        types::{ContentHash, FactorDefinitionId, ModelVersionId, Probability},
    };
    use rust_decimal_macros::dec;

    use crate::{
        factors::{FactorExplanation, FactorName, FactorValue, NormalizedFactor, names},
        model::{
            artifact::{
                FactorWeight, ModelArtifactHeader, SellScorerArtifact, SellScorerOutputSpec,
            },
            runtime::ModelFamily,
        },
    };

    fn hash(seed: &str) -> ContentHash {
        let hex = format!("{seed:0>64}");
        ContentHash::parse(format!("blake3:{hex}")).expect("hash")
    }

    fn artifact() -> SellScorerArtifact {
        SellScorerArtifact {
            header: ModelArtifactHeader {
                model_version_id: ModelVersionId::from_v7(),
                model_family: ModelFamily::HoldVsExitWeighted,
                feature_schema_hash: hash("aa"),
                factor_schema_hash: hash("bb"),
            },
            weights: vec![
                FactorWeight {
                    factor: names::MOMENTUM_ROC,
                    weight: dec!(0.5),
                },
                FactorWeight {
                    factor: names::POSITION_UNREALIZED_PNL,
                    weight: dec!(0.5),
                },
            ],
            prediction_horizon_secs: 86_400,
            output_spec: SellScorerOutputSpec::conservative(),
            label_schema_hash: hash("cc"),
            required_features: Vec::new(),
            objective_report: None,
        }
    }

    fn market_factor(name: FactorName, direction: FactorDirection) -> FactorValue {
        FactorValue {
            definition_id: FactorDefinitionId::from_v7(),
            name,
            family: FactorFamily::Momentum,
            raw_value: Some(dec!(1)),
            normalization: NormalizedFactor::cross_section(Probability::new(dec!(0.8))),
            direction,
            confidence: Probability::new(dec!(0.9)),
            explanation: FactorExplanation {
                headline: "t".to_owned(),
                drivers: Vec::new(),
            },
            input_feature_refs: Vec::new(),
        }
    }

    #[test]
    fn strong_exit_signal_scores_positive_alpha_and_high_p_exit() {
        let runtime = WeightedSellScorerRuntime::new(artifact()).expect("runtime");
        let score = runtime
            .score(&SellScoreInput {
                market_factors: vec![market_factor(
                    names::MOMENTUM_ROC,
                    FactorDirection::Positive,
                )],
                position_state: PositionStateFeatures {
                    unrealized_pnl_pct: dec!(0.2),
                    time_in_trade_ratio: dec!(0.5),
                    peak_mark_drawdown: dec!(0.0),
                },
            })
            .expect("score");
        assert!(score.net > dec!(0), "net should be positive: {}", score.net);
        assert!(score.exit_alpha_bps.inner() > dec!(0));
        assert!(score.p_exit_better.inner() > dec!(0.5));
        assert_eq!(score.recommended_cumulative_exit_pct, dec!(1));
    }

    #[test]
    fn hold_signal_scores_negative_alpha_and_low_p_exit() {
        let runtime = WeightedSellScorerRuntime::new(artifact()).expect("runtime");
        let score = runtime
            .score(&SellScoreInput {
                market_factors: vec![market_factor(
                    names::MOMENTUM_ROC,
                    FactorDirection::Negative,
                )],
                position_state: PositionStateFeatures {
                    unrealized_pnl_pct: dec!(-0.2),
                    time_in_trade_ratio: dec!(0.0),
                    peak_mark_drawdown: dec!(0.0),
                },
            })
            .expect("score");
        assert!(score.net < dec!(0), "net should be negative: {}", score.net);
        assert!(score.exit_alpha_bps.inner() < dec!(0));
        assert!(score.p_exit_better.inner() < dec!(0.5));
    }

    #[test]
    fn sell_signal_policy_try_from_runtime_parses_defaults() {
        use quant_pivot_models::runtime_config::OpportunisticSellPolicy;

        use super::SellSignalPolicy;

        let policy = OpportunisticSellPolicy::default();
        let parsed = SellSignalPolicy::try_from_runtime(&policy).expect("parse");
        assert_eq!(parsed.min_confidence, dec!(0.65));
        assert_eq!(parsed.min_p_exit_better, dec!(0.50));
        assert_eq!(parsed.min_expected_alpha_bps, dec!(50));
        assert_eq!(parsed.max_sell_pct, dec!(1.0));
    }

    #[test]
    fn sell_signal_policy_try_from_runtime_rejects_malformed_decimal() {
        use quant_pivot_models::runtime_config::{DecimalString, OpportunisticSellPolicy};

        use super::SellSignalPolicy;

        let policy = OpportunisticSellPolicy {
            min_confidence: DecimalString::new("not-a-decimal"),
            ..Default::default()
        };
        let err = SellSignalPolicy::try_from_runtime(&policy).expect_err("malformed");
        assert!(
            err.to_string().contains("min_confidence"),
            "expected field name in error, got: {err}"
        );
    }
}
