//! Strongly-typed [`SignalCandidate`] and its explanation types.
//!
//! This is the model runtime's output before portfolio pruning; the
//! observability boundary maps it to
//! `QuantSignalCandidateEventRow` for `ClickHouse`.

use std::collections::BTreeSet;

use chrono::{DateTime, Utc};
use quant_pivot_error::{QuantResult, research::ResearchError};
use quant_pivot_models::{
    clickhouse::{ChBps, ChPrice, ChProbability, QuantSignalCandidateEventRow},
    enums::{
        factor::{FactorFamily, FactorIndeterminateReason, FactorValueState, NormalizationSource},
        quant::{FactorDirection, OutcomeSide},
    },
    hashing::CanonicalDigest,
    types::{
        Bps, ContentHash, FactorDefinitionId, MarketId, ModelRunId, ModelVersionId, Price,
        Probability, SignalCandidateId, TokenId, calibration::CalibratedPayoutDistribution,
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
    /// Expected terminal-payout return in basis points — `E[r]`, not a
    /// conditional take-profit target. The executable economic tier and joint
    /// scenario artifact supersede this scalar at portfolio construction.
    pub expected_return_bps: Decimal,
    /// Estimated downside from frozen out-of-sample MAE evidence, in basis
    /// points. This remains Route-local diagnostic evidence and is not a
    /// cross-Route objective weight.
    pub downside_bps: Decimal,
    /// Calibrated terminal payout distribution over loss, split, and win.
    /// `Some` only for a verified calibrated serving path. Heuristic bootstrap
    /// models carry `None` and are rejected by report publication.
    pub payout_distribution: Option<CalibratedPayoutDistribution>,
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
    /// Rank inside the candidate's own governed model Route. This value is
    /// never used to compare candidates across Routes.
    pub route_rank: u32,
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

impl SignalCandidate {
    /// Derive the idempotent identity of one model-run decision and business leg.
    pub fn id_for(
        model_run_id: ModelRunId,
        decision_at: DateTime<Utc>,
        market_id: &MarketId,
        token_id: &TokenId,
        outcome_side: OutcomeSide,
    ) -> QuantResult<SignalCandidateId> {
        let content_hash = CanonicalDigest::content_hash_typed(
            "quant-pivot/signal-candidate-identity",
            1,
            &(model_run_id, decision_at, market_id, token_id, outcome_side),
        )?;
        Ok(SignalCandidateId::from_content_hash(&content_hash))
    }
}

/// Stable, identity-free projection of one model prediction.
///
/// Candidate and run ids identify one execution attempt, not the business
/// prediction. Every remaining field is consumed by ranking, sizing, report
/// composition, execution evidence, or operator diagnostics and therefore
/// participates in deterministic parity.
#[derive(Serialize)]
struct CanonicalFactorContribution<'a> {
    definition_id: &'a FactorDefinitionId,
    name: &'a FactorName,
    family: &'a FactorFamily,
    value_state: &'a FactorValueState,
    raw_value: Option<String>,
    normalized_score: Option<String>,
    normalization_source: &'a Option<NormalizationSource>,
    indeterminate_reason: &'a Option<FactorIndeterminateReason>,
    weight: String,
    contribution: String,
    confidence: String,
    direction: &'a FactorDirection,
    explanation: &'a str,
    source_refs: &'a [String],
}

impl<'a> From<&'a FactorContribution> for CanonicalFactorContribution<'a> {
    fn from(contribution: &'a FactorContribution) -> Self {
        Self {
            definition_id: &contribution.definition_id,
            name: &contribution.name,
            family: &contribution.family,
            value_state: &contribution.value_state,
            raw_value: contribution
                .raw_value
                .map(|value| value.normalize().to_string()),
            normalized_score: contribution
                .normalized_score
                .map(|value| value.normalized().to_string()),
            normalization_source: &contribution.normalization_source,
            indeterminate_reason: &contribution.indeterminate_reason,
            weight: contribution.weight.normalize().to_string(),
            contribution: contribution.contribution.normalize().to_string(),
            confidence: contribution.confidence.normalized().to_string(),
            direction: &contribution.direction,
            explanation: &contribution.explanation,
            source_refs: &contribution.source_refs,
        }
    }
}

#[derive(Serialize)]
struct CanonicalModelExplanation<'a> {
    headline: &'a str,
    top_positive: Vec<CanonicalFactorContribution<'a>>,
    top_negative: Vec<CanonicalFactorContribution<'a>>,
}

#[derive(Serialize)]
struct CanonicalPayoutDistribution {
    winner_take_all_win_probability: String,
    split_probability: String,
    split_probability_interval: (String, String),
    split_payout_ratio: String,
}

impl From<CalibratedPayoutDistribution> for CanonicalPayoutDistribution {
    fn from(distribution: CalibratedPayoutDistribution) -> Self {
        Self {
            winner_take_all_win_probability: distribution
                .winner_take_all_win_probability
                .normalized()
                .to_string(),
            split_probability: distribution.split_probability.normalized().to_string(),
            split_probability_interval: (
                distribution
                    .split_probability_interval
                    .0
                    .normalized()
                    .to_string(),
                distribution
                    .split_probability_interval
                    .1
                    .normalized()
                    .to_string(),
            ),
            split_payout_ratio: distribution
                .split_payout_ratio
                .inner()
                .normalize()
                .to_string(),
        }
    }
}

impl<'a> From<&'a ModelExplanation> for CanonicalModelExplanation<'a> {
    fn from(explanation: &'a ModelExplanation) -> Self {
        Self {
            headline: &explanation.headline,
            top_positive: explanation
                .top_positive
                .iter()
                .map(CanonicalFactorContribution::from)
                .collect(),
            top_negative: explanation
                .top_negative
                .iter()
                .map(CanonicalFactorContribution::from)
                .collect(),
        }
    }
}

#[derive(Serialize)]
struct CanonicalBusinessPrediction<'a> {
    market_id: &'a MarketId,
    token_id: &'a TokenId,
    outcome_side: &'a OutcomeSide,
    composite_score: String,
    confidence: String,
    expected_return_bps: String,
    downside_bps: String,
    payout_distribution: Option<CanonicalPayoutDistribution>,
    entry_price_ref: String,
    suggested_horizon_secs: u64,
    factor_breakdown: Vec<CanonicalFactorContribution<'a>>,
    model_explanation: CanonicalModelExplanation<'a>,
    rejection_warnings: &'a [SignalWarning],
    route_rank: u32,
    liquidity_score: String,
    data_quality_score: String,
    model_score_percentile: String,
    decision_at: &'a DateTime<Utc>,
}

impl<'a> From<&'a SignalCandidate> for CanonicalBusinessPrediction<'a> {
    fn from(candidate: &'a SignalCandidate) -> Self {
        Self {
            market_id: &candidate.market_id,
            token_id: &candidate.token_id,
            outcome_side: &candidate.outcome_side,
            composite_score: candidate.composite_score.normalized().to_string(),
            confidence: candidate.confidence.normalized().to_string(),
            expected_return_bps: candidate.expected_return_bps.normalize().to_string(),
            downside_bps: candidate.downside_bps.normalize().to_string(),
            payout_distribution: candidate
                .payout_distribution
                .map(CanonicalPayoutDistribution::from),
            entry_price_ref: candidate.entry_price_ref.normalized().to_string(),
            suggested_horizon_secs: candidate.suggested_horizon_secs,
            factor_breakdown: candidate
                .factor_breakdown
                .iter()
                .map(CanonicalFactorContribution::from)
                .collect(),
            model_explanation: CanonicalModelExplanation::from(&candidate.model_explanation),
            rejection_warnings: &candidate.rejection_warnings,
            route_rank: candidate.route_rank,
            liquidity_score: candidate.liquidity_score.normalized().to_string(),
            data_quality_score: candidate.data_quality_score.normalized().to_string(),
            model_score_percentile: candidate.model_score_percentile.normalized().to_string(),
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
/// mean / downside estimates, not a tradeable take-profit / stop pair — global
/// portfolio construction consumes executable scenario cashflows instead
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
    model_version_id: ModelVersionId,
    rejection_reason: &str,
    event_time: i64,
) -> QuantSignalCandidateEventRow {
    QuantSignalCandidateEventRow {
        event_time,
        signal_candidate_id: candidate.signal_candidate_id,
        model_run_id: candidate.model_run_id,
        model_version_id,
        market_id: candidate.market_id.clone(),
        token_id: candidate.token_id.clone(),
        side: candidate.outcome_side.into(),
        score: ChProbability::from(candidate.composite_score),
        confidence: ChProbability::from(candidate.confidence),
        expected_return_bps: ChBps::from(Bps::new(candidate.expected_return_bps)),
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
        route_rank: candidate.route_rank,
        rejection_reason: rejection_reason.to_owned(),
        ingestion_time: event_time,
    }
}

/// Project a batch of accepted candidates into fact rows (empty rejection reason).
#[must_use]
pub fn signal_candidate_events(
    candidates: &[SignalCandidate],
    model_version_id: ModelVersionId,
    event_time: i64,
) -> Vec<QuantSignalCandidateEventRow> {
    candidates
        .iter()
        .map(|candidate| signal_candidate_event(candidate, model_version_id, "", event_time))
        .collect()
}

#[cfg(test)]
mod tests {
    use std::slice;

    use chrono::{TimeZone, Utc};
    use quant_pivot_models::{
        enums::{
            factor::{FactorFamily, FactorValueState, NormalizationSource},
            quant::FactorDirection,
        },
        types::{
            FactorDefinitionId, MarketId, ModelRunId, ModelVersionId, PayoutRatio, Price,
            Probability, SignalCandidateId, TokenId, calibration::CalibratedPayoutDistribution,
        },
    };
    use rust_decimal::Decimal;

    use super::{
        FactorContribution, FactorName, ModelExplanation, OutcomeSide, SignalCandidate,
        SignalWarning, canonical_business_prediction_hash, signal_candidate_event,
        signal_candidate_events,
    };

    fn payout_distribution(win: Decimal) -> CalibratedPayoutDistribution {
        CalibratedPayoutDistribution {
            winner_take_all_win_probability: Probability::new(win),
            split_probability: Probability::new(Decimal::new(2, 2)),
            split_probability_interval: (
                Probability::new(Decimal::new(1, 2)),
                Probability::new(Decimal::new(4, 2)),
            ),
            split_payout_ratio: PayoutRatio::try_new(Decimal::new(5, 1))
                .expect("canonical split payout"),
        }
    }
    /// Construct a strongly-typed candidate.
    #[test]
    fn typed_signal_candidate_newtypes() {
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
            payout_distribution: Some(payout_distribution(Decimal::new(55, 2))),
            entry_price_ref: Price::new(Decimal::new(65, 2)),
            suggested_horizon_secs: 3_600,
            factor_breakdown: Vec::new(),
            model_explanation: ModelExplanation {
                headline: "liquidity + momentum".to_owned(),
                top_positive: Vec::new(),
                top_negative: Vec::new(),
            },
            rejection_warnings: Vec::new(),
            route_rank: 1,
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

    #[test]
    fn candidate_id_is_idempotent() {
        let model_run_id = ModelRunId::from_v7();
        let decision_at = Utc
            .with_ymd_and_hms(2026, 3, 9, 17, 27, 16)
            .single()
            .expect("fixed decision time");
        let market_id = MarketId::new("0xmarket");
        let token_id = TokenId::new("123456");
        let first = SignalCandidate::id_for(
            model_run_id,
            decision_at,
            &market_id,
            &token_id,
            OutcomeSide::Yes,
        )
        .expect("first candidate id");
        let repeated = SignalCandidate::id_for(
            model_run_id,
            decision_at,
            &market_id,
            &token_id,
            OutcomeSide::Yes,
        )
        .expect("repeated candidate id");
        let other_side = SignalCandidate::id_for(
            model_run_id,
            decision_at,
            &market_id,
            &token_id,
            OutcomeSide::No,
        )
        .expect("other-side candidate id");

        assert_eq!(first, repeated);
        assert_ne!(first, other_side);
    }

    impl SignalCandidate {
        fn test_fixture() -> Self {
            Self {
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
                payout_distribution: Some(payout_distribution(Decimal::new(52, 2))),
                entry_price_ref: Price::new(Decimal::new(40, 2)),
                suggested_horizon_secs: 3_600,
                factor_breakdown: Vec::new(),
                model_explanation: ModelExplanation {
                    headline: "test".to_owned(),
                    top_positive: Vec::new(),
                    top_negative: Vec::new(),
                },
                rejection_warnings: Vec::new(),
                route_rank: 3,
                liquidity_score: Probability::ZERO,
                data_quality_score: Probability::ZERO,
                model_score_percentile: Probability::ZERO,
                decision_at: Utc::now(),
            }
        }
    }

    #[test]
    fn business_prediction_ignores_ids() {
        let candidate = SignalCandidate::test_fixture();
        let mut replayed = candidate.clone();
        replayed.signal_candidate_id = SignalCandidateId::from_v7();
        replayed.model_run_id = ModelRunId::from_v7();

        assert_eq!(
            canonical_business_prediction_hash(slice::from_ref(&candidate)).expect("online"),
            canonical_business_prediction_hash(slice::from_ref(&replayed)).expect("replay")
        );
    }

    #[test]
    fn business_prediction_ignores_scale() {
        let mut candidate = SignalCandidate::test_fixture();
        let contribution = FactorContribution {
            definition_id: FactorDefinitionId::from_v7(),
            name: FactorName::from_static("test.factor"),
            family: FactorFamily::Liquidity,
            value_state: FactorValueState::Scored,
            raw_value: Some(Decimal::new(125, 2)),
            normalized_score: Some(Probability::new(Decimal::new(70, 2))),
            normalization_source: Some(NormalizationSource::CrossSection),
            indeterminate_reason: None,
            weight: Decimal::new(50, 2),
            contribution: Decimal::new(35, 2),
            confidence: Probability::new(Decimal::new(60, 2)),
            direction: FactorDirection::Positive,
            explanation: "test contribution".to_owned(),
            source_refs: vec!["fixture://factor".to_owned()],
        };
        candidate.factor_breakdown.push(contribution.clone());
        candidate.model_explanation.top_positive.push(contribution);

        let mut scaled = candidate.clone();
        scaled.composite_score = Probability::new(Decimal::new(7200, 4));
        scaled.confidence = Probability::new(Decimal::new(6000, 4));
        scaled.expected_return_bps = Decimal::new(20_000, 2);
        scaled.downside_bps = Decimal::new(50_000, 2);
        scaled.payout_distribution = Some(payout_distribution(Decimal::new(5200, 4)));
        scaled.entry_price_ref = Price::new(Decimal::new(4000, 4));
        for factor in scaled
            .factor_breakdown
            .iter_mut()
            .chain(scaled.model_explanation.top_positive.iter_mut())
        {
            factor.raw_value = Some(Decimal::new(12_500, 4));
            factor.normalized_score = Some(Probability::new(Decimal::new(7000, 4)));
            factor.weight = Decimal::new(5000, 4);
            factor.contribution = Decimal::new(3500, 4);
            factor.confidence = Probability::new(Decimal::new(6000, 4));
        }

        assert_eq!(
            canonical_business_prediction_hash(slice::from_ref(&candidate)).expect("canonical"),
            canonical_business_prediction_hash(slice::from_ref(&scaled)).expect("scaled")
        );
    }

    #[test]
    fn business_prediction_hash_independent() {
        let first = SignalCandidate::test_fixture();
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
    fn business_prediction_commits_fields() {
        let candidate = SignalCandidate::test_fixture();
        let baseline =
            canonical_business_prediction_hash(slice::from_ref(&candidate)).expect("baseline");

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
            row.route_rank += 1;
        });
        assert_business_change!("liquidity", |row: &mut SignalCandidate| {
            row.liquidity_score = Probability::new(Decimal::new(1, 1));
        });
    }

    #[test]
    fn business_rejects_duplicate_key() {
        let first = SignalCandidate::test_fixture();
        let mut duplicate = first.clone();
        duplicate.signal_candidate_id = SignalCandidateId::from_v7();
        duplicate.model_run_id = ModelRunId::from_v7();
        duplicate.composite_score = Probability::new(Decimal::new(90, 2));

        let error = canonical_business_prediction_hash(&[first, duplicate])
            .expect_err("duplicate business key must fail");
        assert!(error.to_string().contains("duplicate business prediction"));
    }

    #[test]
    fn signal_candidate_maps_row() {
        let candidate = SignalCandidate::test_fixture();
        let row = signal_candidate_event(
            &candidate,
            ModelVersionId::from_v7(),
            "score_below_floor",
            1_700_000_000_000,
        );

        assert_eq!(row.event_time, 1_700_000_000_000);
        assert_eq!(row.signal_candidate_id, candidate.signal_candidate_id);
        assert_eq!(row.model_run_id, candidate.model_run_id);
        assert_eq!(row.market_id, candidate.market_id);
        assert_eq!(row.token_id, candidate.token_id);
        assert_eq!(row.side, OutcomeSide::No.into());
        assert_eq!(row.route_rank, 3);
        assert_eq!(row.rejection_reason, "score_below_floor");
        // target = 0.40 × (1 + 0.02) = 0.408; stop = 0.40 × (1 − 0.05) = 0.38.
        assert_eq!(
            Price::from(row.target_price),
            Price::new(Decimal::new(408, 3))
        );
        assert_eq!(
            Price::from(row.stop_price),
            Price::new(Decimal::new(380, 3))
        );

        let batch =
            signal_candidate_events(slice::from_ref(&candidate), ModelVersionId::from_v7(), 1);
        assert_eq!(batch.len(), 1);
        assert_eq!(batch[0].rejection_reason, "");
    }
}
