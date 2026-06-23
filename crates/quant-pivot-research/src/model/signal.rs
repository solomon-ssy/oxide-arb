//! Strongly-typed [`SignalCandidate`] and its explanation types.
//!
//! Replaces the deleted `quant-pivot-models` stub (`String` id / `i8` side /
//! bare `Decimal`). This is the model runtime's output before portfolio
//! pruning; 3.4 maps it to `QuantSignalCandidateEventRow` for `ClickHouse`.

use chrono::{DateTime, Utc};
use quant_pivot_models::{
    enums::quant::{FactorDirection, SignalSide},
    types::{MarketId, ModelRunId, Price, Probability, SignalCandidateId, TokenId},
};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

use crate::factors::FactorName;

/// One factor's signed contribution to a candidate's composite score.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FactorContribution {
    /// The contributing factor.
    pub name: FactorName,
    /// Normalized factor score in `[0, 1]`.
    pub normalized_score: Probability,
    /// Weight applied to the factor by the model.
    pub weight: Decimal,
    /// Signed contribution (`weight × score × confidence`).
    pub contribution: Decimal,
    /// Direction the factor pushed the score.
    pub direction: FactorDirection,
}

/// Model-level explanation: headline plus the strongest positive and negative
/// factor contributions.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ModelExplanation {
    /// One-line human summary.
    pub headline: String,
    /// Top positive contributions.
    pub top_positive: Vec<FactorContribution>,
    /// Top negative contributions.
    pub top_negative: Vec<FactorContribution>,
}

/// A non-fatal warning attached to a candidate (it still scores, but the
/// caveat is recorded for the report and any execution gate).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SignalWarning {
    /// Confidence below the configured floor.
    LowConfidence,
    /// Visible liquidity is thin relative to intended size.
    ThinLiquidity,
    /// One or more features were stale (but within degraded tolerance).
    StaleFeatures,
    /// Composite score fell below the candidate floor.
    ScoreBelowFloor,
    /// Any other model-specific caveat.
    Other(String),
}

/// A candidate signal emitted by a model runtime before portfolio pruning.
///
/// All money / price / probability fields use project newtypes — never `f64`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SignalCandidate {
    /// Unique candidate id.
    pub signal_candidate_id: SignalCandidateId,
    /// The model run that emitted this candidate.
    pub model_run_id: ModelRunId,
    /// Market the candidate targets.
    pub market_id: MarketId,
    /// Outcome token the candidate targets.
    pub token_id: TokenId,
    /// Directional action.
    pub side: SignalSide,
    /// Composite score in `[0, 1]`.
    pub composite_score: Probability,
    /// Model confidence in `[0, 1]`.
    pub confidence: Probability,
    /// Expected return in basis points.
    pub expected_return_bps: Decimal,
    /// Estimated downside in basis points.
    pub downside_bps: Decimal,
    /// Reference entry price.
    pub entry_price_ref: Price,
    /// Suggested holding horizon, in seconds.
    pub suggested_horizon_secs: u64,
    /// Per-factor contribution breakdown.
    pub factor_breakdown: Vec<FactorContribution>,
    /// Model-level explanation.
    pub model_explanation: ModelExplanation,
    /// Non-fatal warnings.
    pub rejection_warnings: Vec<SignalWarning>,
    /// Rank among candidates before portfolio pruning.
    pub rank_before_portfolio: u32,
    /// Decision time.
    pub as_of: DateTime<Utc>,
}

#[cfg(test)]
mod tests {
    use super::{ModelExplanation, SignalCandidate, SignalSide};
    use chrono::Utc;
    use quant_pivot_models::types::{
        MarketId, ModelRunId, Price, Probability, SignalCandidateId, TokenId,
    };
    use rust_decimal::Decimal;

    /// Constructs a strongly-typed candidate (replaces the deleted models stub).
    #[test]
    fn typed_signal_candidate_constructs_from_newtypes() {
        let candidate = SignalCandidate {
            signal_candidate_id: SignalCandidateId::from_v7(),
            model_run_id: ModelRunId::from_v7(),
            market_id: MarketId::new("0xmarket"),
            token_id: TokenId::new("123456"),
            side: SignalSide::BuyYes,
            composite_score: Probability::new(Decimal::new(72, 2)),
            confidence: Probability::new(Decimal::new(60, 2)),
            expected_return_bps: Decimal::new(150, 0),
            downside_bps: Decimal::new(40, 0),
            entry_price_ref: Price::new(Decimal::new(65, 2)),
            suggested_horizon_secs: 3_600,
            factor_breakdown: Vec::new(),
            model_explanation: ModelExplanation {
                headline: "liquidity + momentum".to_owned(),
                top_positive: Vec::new(),
                top_negative: Vec::new(),
            },
            rejection_warnings: Vec::new(),
            rank_before_portfolio: 1,
            as_of: Utc::now(),
        };

        assert_eq!(candidate.side, SignalSide::BuyYes);
        assert_eq!(candidate.entry_price_ref, Price::new(Decimal::new(65, 2)));
        // Round-trips through serde (artifact / fact serialization boundary).
        let json = serde_json::to_string(&candidate).expect("serialize");
        let back: SignalCandidate = serde_json::from_str(&json).expect("deserialize");
        assert_eq!(back, candidate);
    }
}
