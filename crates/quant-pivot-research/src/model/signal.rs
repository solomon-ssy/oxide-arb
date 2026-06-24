//! Strongly-typed [`SignalCandidate`] and its explanation types.
//!
//! Replaces the deleted `quant-pivot-models` stub (`String` id / `i8` side /
//! bare `Decimal`). This is the model runtime's output before portfolio
//! pruning; 3.4 maps it to `QuantSignalCandidateEventRow` for `ClickHouse`.

use chrono::{DateTime, Utc};
use quant_pivot_models::{
    clickhouse::{ChPrice, ChProbability, QuantSignalCandidateEventRow},
    enums::quant::{FactorDirection, SignalSide},
    types::{Bps, MarketId, ModelRunId, Price, Probability, SignalCandidateId, TokenId},
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

/// Apply a basis-point move to an entry price, clamped into `[0, 1]`.
///
/// `target` (`positive = true`) scales the entry up by `expected_return_bps`;
/// `stop` (`positive = false`) scales it down by `downside_bps`.
fn apply_bps(entry: Price, bps: Decimal, positive: bool) -> Price {
    let fraction = Bps::new(bps).to_fraction();
    let factor = if positive {
        Decimal::ONE + fraction
    } else {
        Decimal::ONE - fraction
    };
    Price::new((entry.inner() * factor).clamp(Decimal::ZERO, Decimal::ONE))
}

/// Project one candidate into its `quant_signal_candidate_event` fact row.
///
/// `rejection_reason` is empty for an accepted candidate; the runner sets it
/// (e.g. `score_below_floor` / `low_confidence`) when the candidate is recorded
/// for audit but excluded from the report. `target_price` / `stop_price` are
/// derived from the entry reference and the model's expected-return / downside
/// basis points.
#[must_use]
pub fn signal_candidate_event(
    candidate: &SignalCandidate,
    rejection_reason: &str,
    event_time: i64,
) -> QuantSignalCandidateEventRow {
    QuantSignalCandidateEventRow {
        event_time,
        signal_candidate_id: candidate.signal_candidate_id.to_string(),
        model_run_id: candidate.model_run_id.clone(),
        market_id: candidate.market_id.clone(),
        token_id: candidate.token_id.clone(),
        side: candidate.side.as_i8(),
        score: ChProbability::from(candidate.composite_score),
        confidence: ChProbability::from(candidate.confidence),
        entry_price: ChPrice::from(candidate.entry_price_ref),
        target_price: ChPrice::from(apply_bps(
            candidate.entry_price_ref,
            candidate.expected_return_bps,
            true,
        )),
        stop_price: ChPrice::from(apply_bps(
            candidate.entry_price_ref,
            candidate.downside_bps,
            false,
        )),
        rank_before_portfolio: candidate.rank_before_portfolio,
        rejection_reason: rejection_reason.to_owned(),
    }
}

/// Project a batch of accepted candidates into fact rows (empty rejection reason).
#[must_use]
pub fn signal_candidate_events(
    candidates: &[SignalCandidate],
    event_time: i64,
) -> Vec<QuantSignalCandidateEventRow> {
    candidates
        .iter()
        .map(|candidate| signal_candidate_event(candidate, "", event_time))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::{
        ModelExplanation, SignalCandidate, SignalSide, signal_candidate_event,
        signal_candidate_events,
    };
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

    fn sample_candidate() -> SignalCandidate {
        SignalCandidate {
            signal_candidate_id: SignalCandidateId::from_v7(),
            model_run_id: ModelRunId::from_v7(),
            market_id: MarketId::new("0xmarket"),
            token_id: TokenId::new("123456"),
            side: SignalSide::BuyNo,
            composite_score: Probability::new(Decimal::new(72, 2)),
            confidence: Probability::new(Decimal::new(60, 2)),
            // +200 bps target, -500 bps stop on a 0.40 entry.
            expected_return_bps: Decimal::from(200),
            downside_bps: Decimal::from(500),
            entry_price_ref: Price::new(Decimal::new(40, 2)),
            suggested_horizon_secs: 3_600,
            factor_breakdown: Vec::new(),
            model_explanation: ModelExplanation {
                headline: "test".to_owned(),
                top_positive: Vec::new(),
                top_negative: Vec::new(),
            },
            rejection_warnings: Vec::new(),
            rank_before_portfolio: 3,
            as_of: Utc::now(),
        }
    }

    #[test]
    fn signal_candidate_maps_to_ch_row() {
        let candidate = sample_candidate();
        let row = signal_candidate_event(&candidate, "score_below_floor", 1_700_000_000_000);

        assert_eq!(row.event_time, 1_700_000_000_000);
        assert_eq!(
            row.signal_candidate_id,
            candidate.signal_candidate_id.to_string()
        );
        assert_eq!(row.model_run_id, candidate.model_run_id);
        assert_eq!(row.market_id, candidate.market_id);
        assert_eq!(row.token_id, candidate.token_id);
        assert_eq!(row.side, SignalSide::BuyNo.as_i8());
        assert_eq!(row.rank_before_portfolio, 3);
        assert_eq!(row.rejection_reason, "score_below_floor");
        // target = 0.40 × (1 + 0.02) = 0.408; stop = 0.40 × (1 − 0.05) = 0.38.
        assert_eq!(
            row.target_price.to_price(),
            Price::new(Decimal::new(408, 3))
        );
        assert_eq!(row.stop_price.to_price(), Price::new(Decimal::new(380, 3)));

        let batch = signal_candidate_events(std::slice::from_ref(&candidate), 1);
        assert_eq!(batch.len(), 1);
        assert_eq!(batch[0].rejection_reason, "");
    }
}
