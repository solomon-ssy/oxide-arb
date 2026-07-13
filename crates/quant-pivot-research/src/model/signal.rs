//! Strongly-typed [`SignalCandidate`] and its explanation types.
//!
//! Replaces the deleted `quant-pivot-models` stub (`String` id / `i8` side /
//! bare `Decimal`). This is the model runtime's output before portfolio
//! pruning; 3.4 maps it to `QuantSignalCandidateEventRow` for `ClickHouse`.

use std::collections::BTreeSet;

use chrono::{DateTime, Utc};
use quant_pivot_error::{QuantResult, research::ResearchError};
use quant_pivot_models::{
    clickhouse::{ChPrice, ChProbability, QuantSignalCandidateEventRow},
    enums::{
        factor::{FactorFamily, FactorIndeterminateReason, FactorValueState, NormalizationSource},
        quant::{FactorDirection, OutcomeSide},
    },
    types::{
        Bps, ContentHash, FactorDefinitionId, MarketId, ModelRunId, Price, Probability,
        SignalCandidateId, TokenId,
    },
};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

use crate::{factors::FactorName, hashing::ResearchHasher};

/// One factor's signed contribution to a candidate's composite score.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FactorContribution {
    /// Governing factor definition id.
    pub definition_id: FactorDefinitionId,
    /// The contributing factor.
    pub name: FactorName,
    /// Factor family.
    pub family: FactorFamily,
    /// Authoritative value state (scored / missing-input / not-applicable /
    /// indeterminate) — lets the report render "—(not applicable)" distinctly
    /// from "—(missing)".
    pub value_state: FactorValueState,
    /// Raw factor value before normalization.
    pub raw_value: Option<Decimal>,
    /// Normalized factor score in `[0, 1]`; `None` when missing / indeterminate.
    pub normalized_score: Option<Probability>,
    /// How the score was derived; `None` when missing / indeterminate.
    pub normalization_source: Option<NormalizationSource>,
    /// Why the factor was indeterminate; `None` when scored / missing.
    pub indeterminate_reason: Option<FactorIndeterminateReason>,
    /// Weight applied to the factor by the model.
    pub weight: Decimal,
    /// Signed contribution (`weight × score × confidence`); zero when not scored.
    pub contribution: Decimal,
    /// Confidence attached to this factor.
    pub confidence: Probability,
    /// Direction the factor pushed the score.
    pub direction: FactorDirection,
    /// Human-readable explanation from the factor plane.
    pub explanation: String,
    /// Feature/fact refs behind this contribution.
    pub source_refs: Vec<String>,
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
    /// Outcome side this candidate opens (always buy-to-open; `Yes`/`No` only).
    pub outcome_side: OutcomeSide,
    /// Composite score in `[0, 1]`.
    pub composite_score: Probability,
    /// Model confidence in `[0, 1]`.
    pub confidence: Probability,
    /// Expected (probability-weighted **mean**) return in basis points — `E[r]`,
    /// not a conditional take-profit target. Audit-only once `win_probability`
    /// is `Some`: Kelly sizing (Phase 11.3 §4 redesign) uses `win_probability`
    /// directly, never re-derives it from `E[r]`.
    pub expected_return_bps: Decimal,
    /// Estimated downside (stop-loss magnitude) in basis points — `l`. Feeds
    /// the exit plan's stop-loss price (Phase 11.7 territory); no longer part
    /// of the Kelly win-probability derivation (Phase 11.3 §4 redesign).
    pub downside_bps: Decimal,
    /// The calibrated `P(win)` Kelly sizing consumes directly as `q`
    /// (`f* = (q - p) / (1 - p)`, `p` = `entry_price_ref`). `Some` only when
    /// the emitting model's return model is `Calibrated`; `None` for the
    /// `Heuristic` bootstrap path, whose sizing is fenced off from production
    /// by fail-closed publish/admission gates (Phase 11.3 §4 redesign — see
    /// `crate::portfolio::sizing`).
    pub win_probability: Option<Probability>,
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
    /// Governed liquidity-context score in `[0, 1]`, projected directly from
    /// the same decision snapshot the runtime scored.
    pub liquidity_score: Probability,
    /// Governed data-quality-context score in `[0, 1]`.
    pub data_quality_score: Probability,
    /// Within-batch composite-score percentile in `(0, 1]`.
    pub model_score_percentile: Probability,
    /// Frozen decision time that produced this prediction.
    pub decision_at: DateTime<Utc>,
}

/// Stable, identity-free projection of one model prediction.
///
/// Candidate and run ids identify one execution attempt, not the business
/// prediction. Every remaining field is consumed by ranking, sizing, report
/// composition, execution evidence, or operator diagnostics and therefore
/// participates in deterministic parity.
#[derive(Serialize)]
struct CanonicalBusinessPrediction<'a> {
    market_id: &'a MarketId,
    token_id: &'a TokenId,
    outcome_side: &'a OutcomeSide,
    composite_score: &'a Probability,
    confidence: &'a Probability,
    expected_return_bps: &'a Decimal,
    downside_bps: &'a Decimal,
    win_probability: &'a Option<Probability>,
    entry_price_ref: &'a Price,
    suggested_horizon_secs: u64,
    factor_breakdown: &'a [FactorContribution],
    model_explanation: &'a ModelExplanation,
    rejection_warnings: &'a [SignalWarning],
    rank_before_portfolio: u32,
    liquidity_score: &'a Probability,
    data_quality_score: &'a Probability,
    model_score_percentile: &'a Probability,
    decision_at: &'a DateTime<Utc>,
}

impl<'a> From<&'a SignalCandidate> for CanonicalBusinessPrediction<'a> {
    fn from(candidate: &'a SignalCandidate) -> Self {
        Self {
            market_id: &candidate.market_id,
            token_id: &candidate.token_id,
            outcome_side: &candidate.outcome_side,
            composite_score: &candidate.composite_score,
            confidence: &candidate.confidence,
            expected_return_bps: &candidate.expected_return_bps,
            downside_bps: &candidate.downside_bps,
            win_probability: &candidate.win_probability,
            entry_price_ref: &candidate.entry_price_ref,
            suggested_horizon_secs: candidate.suggested_horizon_secs,
            factor_breakdown: &candidate.factor_breakdown,
            model_explanation: &candidate.model_explanation,
            rejection_warnings: &candidate.rejection_warnings,
            rank_before_portfolio: candidate.rank_before_portfolio,
            liquidity_score: &candidate.liquidity_score,
            data_quality_score: &candidate.data_quality_score,
            model_score_percentile: &candidate.model_score_percentile,
            decision_at: &candidate.decision_at,
        }
    }
}

/// Hash canonical business predictions independently of execution ids and
/// caller-provided row order.
///
/// A model may emit at most one prediction for a `(market, token, outcome)`
/// business key. Rejecting duplicates prevents ordering from hiding ambiguous
/// or conflicting predictions.
pub fn canonical_business_prediction_hash(
    candidates: &[SignalCandidate],
) -> QuantResult<ContentHash> {
    let mut business_keys = BTreeSet::new();
    let mut predictions = Vec::with_capacity(candidates.len());
    for candidate in candidates {
        let key = (
            candidate.market_id.as_str(),
            candidate.token_id.as_str(),
            candidate.outcome_side.as_str(),
        );
        if !business_keys.insert(key) {
            return Err(ResearchError::Determinism {
                detail: format!(
                    "duplicate business prediction for market {}, token {}, outcome {}",
                    candidate.market_id, candidate.token_id, candidate.outcome_side
                ),
            }
            .into());
        }
        predictions.push(CanonicalBusinessPrediction::from(candidate));
    }
    predictions.sort_by(|left, right| {
        left.market_id
            .as_str()
            .cmp(right.market_id.as_str())
            .then_with(|| left.token_id.as_str().cmp(right.token_id.as_str()))
            .then_with(|| left.outcome_side.as_str().cmp(right.outcome_side.as_str()))
    });
    ResearchHasher::canonical(&predictions)
}

/// Apply a basis-point move to an entry price, clamped into `[0, 1]`.
///
/// Used to project the CH-fact diagnostic prices: `positive = true` yields the
/// **expected exit price** (`entry · (1 + E[r])`) and `positive = false` the
/// **downside floor** (`entry · (1 − l)`). These are audit projections of the
/// mean / downside estimates, not a tradeable take-profit / stop pair — Kelly
/// sizing derives its own target from a configured reward multiple and never
/// reuses these prices.
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
        signal_candidate_id: candidate.signal_candidate_id.clone(),
        model_run_id: candidate.model_run_id.clone(),
        market_id: candidate.market_id.clone(),
        token_id: candidate.token_id.clone(),
        side: candidate.outcome_side.into(),
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
        ModelExplanation, OutcomeSide, SignalCandidate, SignalWarning,
        canonical_business_prediction_hash, signal_candidate_event, signal_candidate_events,
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
            outcome_side: OutcomeSide::Yes,
            composite_score: Probability::new(Decimal::new(72, 2)),
            confidence: Probability::new(Decimal::new(60, 2)),
            expected_return_bps: Decimal::new(150, 0),
            downside_bps: Decimal::new(40, 0),
            win_probability: Some(Probability::new(Decimal::new(55, 2))),
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
            liquidity_score: Probability::ZERO,
            data_quality_score: Probability::ZERO,
            model_score_percentile: Probability::ZERO,
            decision_at: Utc::now(),
        };

        assert_eq!(candidate.outcome_side, OutcomeSide::Yes);
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
            outcome_side: OutcomeSide::No,
            composite_score: Probability::new(Decimal::new(72, 2)),
            confidence: Probability::new(Decimal::new(60, 2)),
            // +200 bps target, -500 bps stop on a 0.40 entry.
            expected_return_bps: Decimal::from(200),
            downside_bps: Decimal::from(500),
            win_probability: Some(Probability::new(Decimal::new(52, 2))),
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
            liquidity_score: Probability::ZERO,
            data_quality_score: Probability::ZERO,
            model_score_percentile: Probability::ZERO,
            decision_at: Utc::now(),
        }
    }

    #[test]
    fn business_prediction_hash_ignores_execution_ids() {
        let candidate = sample_candidate();
        let mut replayed = candidate.clone();
        replayed.signal_candidate_id = SignalCandidateId::from_v7();
        replayed.model_run_id = ModelRunId::from_v7();

        assert_eq!(
            canonical_business_prediction_hash(std::slice::from_ref(&candidate)).expect("online"),
            canonical_business_prediction_hash(std::slice::from_ref(&replayed)).expect("replay")
        );
    }

    #[test]
    fn business_prediction_hash_is_order_independent() {
        let first = sample_candidate();
        let mut second = first.clone();
        second.signal_candidate_id = SignalCandidateId::from_v7();
        second.market_id = MarketId::new("0xsecond");
        second.token_id = TokenId::new("654321");
        second.outcome_side = OutcomeSide::Yes;

        assert_eq!(
            canonical_business_prediction_hash(&[first.clone(), second.clone()]).expect("forward"),
            canonical_business_prediction_hash(&[second, first]).expect("reverse")
        );
    }

    #[test]
    fn business_prediction_hash_commits_downstream_business_fields() {
        let candidate = sample_candidate();
        let baseline =
            canonical_business_prediction_hash(std::slice::from_ref(&candidate)).expect("baseline");

        macro_rules! assert_business_change {
            ($label:literal, $mutate:expr) => {{
                let mut changed = candidate.clone();
                $mutate(&mut changed);
                assert_ne!(
                    baseline,
                    canonical_business_prediction_hash(std::slice::from_ref(&changed))
                        .expect("changed prediction"),
                    "{} must participate in the business prediction hash",
                    $label
                );
            }};
        }

        assert_business_change!("score", |row: &mut SignalCandidate| {
            row.composite_score = Probability::new(Decimal::new(73, 2));
        });
        assert_business_change!("return", |row: &mut SignalCandidate| {
            row.expected_return_bps += Decimal::ONE;
        });
        assert_business_change!("explanation", |row: &mut SignalCandidate| {
            row.model_explanation.headline = "changed".to_owned();
        });
        assert_business_change!("warnings", |row: &mut SignalCandidate| {
            row.rejection_warnings.push(SignalWarning::LowConfidence);
        });
        assert_business_change!("rank", |row: &mut SignalCandidate| {
            row.rank_before_portfolio += 1;
        });
        assert_business_change!("liquidity", |row: &mut SignalCandidate| {
            row.liquidity_score = Probability::new(Decimal::new(1, 1));
        });
    }

    #[test]
    fn business_prediction_hash_rejects_duplicate_business_key() {
        let first = sample_candidate();
        let mut duplicate = first.clone();
        duplicate.signal_candidate_id = SignalCandidateId::from_v7();
        duplicate.model_run_id = ModelRunId::from_v7();
        duplicate.composite_score = Probability::new(Decimal::new(90, 2));

        let error = canonical_business_prediction_hash(&[first, duplicate])
            .expect_err("duplicate business key must fail");
        assert!(error.to_string().contains("duplicate business prediction"));
    }

    #[test]
    fn signal_candidate_maps_to_ch_row() {
        let candidate = sample_candidate();
        let row = signal_candidate_event(&candidate, "score_below_floor", 1_700_000_000_000);

        assert_eq!(row.event_time, 1_700_000_000_000);
        assert_eq!(row.signal_candidate_id, candidate.signal_candidate_id);
        assert_eq!(row.model_run_id, candidate.model_run_id);
        assert_eq!(row.market_id, candidate.market_id);
        assert_eq!(row.token_id, candidate.token_id);
        assert_eq!(row.side, OutcomeSide::No.into());
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
