//! Deterministic live/replay entry-condition evaluator.

use chrono::{DateTime, Duration, Utc};
use quant_pivot_error::{QuantError, QuantResult, report::ReportError};
use quant_pivot_models::{
    domain::PriceComparator,
    enums::quant::{EntryConditionState, OutcomeSide, PriceComparison},
    hashing::CanonicalDigest,
    types::{
        ConditionTruth, ConditionUnavailableReason, ContentHash, CryptoSubjectPredicateEntered,
        EntryConditionArtifactV1, EntryConditionBinding, EntryConditionSourceBinding,
        EntryConditionV1, FactorCondition, FactorDefinitionId, FactorMeasure, MarketEventCondition,
        ModelVersionId, Price, PriceCondition, TemperatureCelsius, TokenId, Usd,
        WeatherDailyHighEnteredBand, WeatherDailyHighExceededBandUpper,
        WeatherObservationDayClosedOutsideBand,
    },
};
use rust_decimal::Decimal;
use serde::Serialize;

/// Executable-side book input visible at one PIT cutoff.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ExecutablePriceInput {
    pub token_id: TokenId,
    pub price: Price,
    pub observed_at: DateTime<Utc>,
    pub available_at: DateTime<Utc>,
    pub gap_generation: u64,
}

/// Latest persisted factor value; never the recommendation's frozen breakdown.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct FactorSnapshotInput {
    pub definition_id: FactorDefinitionId,
    pub definition_hash: ContentHash,
    pub model_version_id: ModelVersionId,
    pub raw_value: Decimal,
    pub normalized_value: Decimal,
    pub confidence: Decimal,
    pub observed_at: DateTime<Utc>,
    pub available_at: DateTime<Utc>,
    pub snapshot_hash: ContentHash,
}

/// Current same-source crypto state plus its most recent predicate transition.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct CryptoPriceInput {
    pub source: EntryConditionSourceBinding,
    pub previous_price: Usd,
    pub current_price: Usd,
    pub source_sequence: u64,
    pub transition_at: DateTime<Utc>,
    pub available_at: DateTime<Utc>,
    pub report_hash: ContentHash,
    pub gap_generation: u64,
    pub source_healthy: bool,
}

/// Current corrected NOAA proxy state for one station/local day.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct WeatherDailyHighInput {
    pub source: EntryConditionSourceBinding,
    pub station: String,
    pub local_date: chrono::NaiveDate,
    pub current_high: TemperatureCelsius,
    pub observation_time: DateTime<Utc>,
    pub available_at: DateTime<Utc>,
    pub revision: u64,
    pub day_closed: bool,
    pub report_hash: ContentHash,
    pub gap_generation: u64,
    pub source_healthy: bool,
}

/// Complete PIT input bundle shared by live and replay evaluation.
#[derive(Debug, Clone)]
pub struct EntryConditionInputSet {
    pub binding: EntryConditionBinding,
    pub evaluated_at: DateTime<Utc>,
    pub prices: Vec<ExecutablePriceInput>,
    pub factors: Vec<FactorSnapshotInput>,
    pub crypto: Vec<CryptoPriceInput>,
    pub weather: Vec<WeatherDailyHighInput>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case", tag = "kind")]
pub enum ConditionLeafEvidence {
    Price(ExecutablePriceInput),
    Clock {
        deadline_at: DateTime<Utc>,
        evaluated_at: DateTime<Utc>,
    },
    Factor(FactorSnapshotInput),
    Crypto(CryptoPriceInput),
    Weather(WeatherDailyHighInput),
    Unavailable(ConditionUnavailableReason),
}

/// Compact complete tree persisted to the evaluation fact table.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ConditionNodeEvaluation {
    pub node_id: u16,
    pub truth: ConditionTruth,
    pub decisive_child_id: Option<u16>,
    pub evidence: Option<ConditionLeafEvidence>,
    pub children: Vec<Self>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EntryConditionEvaluation {
    pub truth: ConditionTruth,
    pub tree: ConditionNodeEvaluation,
    pub evaluation_hash: ContentHash,
    pub input_fingerprint: ContentHash,
    pub continuity_hash: ContentHash,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EntryConditionStateDecision {
    pub state: EntryConditionState,
    pub confirmation_started_at: Option<DateTime<Utc>>,
}

/// Evaluate a canonical artifact against a fully materialized PIT input set.
/// Calling this function with the same ordered facts produces identical hashes.
pub fn evaluate_entry_condition(
    artifact: &EntryConditionArtifactV1,
    input: &EntryConditionInputSet,
) -> QuantResult<EntryConditionEvaluation> {
    let artifact =
        artifact
            .clone()
            .canonicalize()
            .map_err(|error| ReportError::ContractViolation {
                detail: error.to_string(),
            })?;
    if artifact.binding != input.binding {
        return unavailable_evaluation(
            &artifact.root,
            ConditionUnavailableReason::BindingDrift,
            input,
        );
    }
    let mut node_id = 0_u16;
    let tree = evaluate_node(&artifact.root, input, &mut node_id)?;
    let evidence = flatten_evidence(&tree);
    let input_fingerprint =
        CanonicalDigest::content_hash_json(&evidence).map_err(QuantError::from)?;
    let continuity = continuity_projection(&evidence);
    let continuity_hash =
        CanonicalDigest::content_hash_json(&continuity).map_err(QuantError::from)?;
    let evaluation_hash = CanonicalDigest::content_hash_json(&tree).map_err(QuantError::from)?;
    Ok(EntryConditionEvaluation {
        truth: tree.truth.clone(),
        tree,
        evaluation_hash,
        input_fingerprint,
        continuity_hash,
    })
}

fn unavailable_evaluation(
    root: &EntryConditionV1,
    reason: ConditionUnavailableReason,
    input: &EntryConditionInputSet,
) -> QuantResult<EntryConditionEvaluation> {
    let tree = ConditionNodeEvaluation {
        node_id: 0,
        truth: ConditionTruth::Unavailable(reason.clone()),
        decisive_child_id: None,
        evidence: Some(ConditionLeafEvidence::Unavailable(reason)),
        children: Vec::new(),
    };
    let input_fingerprint =
        CanonicalDigest::content_hash_json(&input.binding).map_err(QuantError::from)?;
    let continuity_hash = input_fingerprint.clone();
    let evaluation_hash =
        CanonicalDigest::content_hash_json(&(root, &tree)).map_err(QuantError::from)?;
    Ok(EntryConditionEvaluation {
        truth: tree.truth.clone(),
        tree,
        evaluation_hash,
        input_fingerprint,
        continuity_hash,
    })
}

fn evaluate_node(
    condition: &EntryConditionV1,
    input: &EntryConditionInputSet,
    next_node_id: &mut u16,
) -> QuantResult<ConditionNodeEvaluation> {
    let node_id = *next_node_id;
    *next_node_id = next_node_id
        .checked_add(1)
        .ok_or_else(|| ReportError::ContractViolation {
            detail: "entry condition node id overflow".to_owned(),
        })?;
    match condition {
        EntryConditionV1::Price(condition) => Ok(leaf(node_id, evaluate_price(condition, input))),
        EntryConditionV1::Clock(condition) => {
            let truth = if input.evaluated_at >= condition.deadline_at {
                ConditionTruth::Satisfied
            } else {
                ConditionTruth::Unsatisfied
            };
            Ok(ConditionNodeEvaluation {
                node_id,
                truth,
                decisive_child_id: None,
                evidence: Some(ConditionLeafEvidence::Clock {
                    deadline_at: condition.deadline_at,
                    evaluated_at: input.evaluated_at,
                }),
                children: Vec::new(),
            })
        }
        EntryConditionV1::Factor(condition) => Ok(leaf(node_id, evaluate_factor(condition, input))),
        EntryConditionV1::MarketEvent { event: condition } => {
            Ok(leaf(node_id, evaluate_market_event(condition, input)))
        }
        EntryConditionV1::All { children } => {
            evaluate_composite(node_id, children, input, next_node_id, CompositeKind::All)
        }
        EntryConditionV1::Any { children } => {
            evaluate_composite(node_id, children, input, next_node_id, CompositeKind::Any)
        }
    }
}

fn leaf(node_id: u16, result: (ConditionTruth, ConditionLeafEvidence)) -> ConditionNodeEvaluation {
    ConditionNodeEvaluation {
        node_id,
        truth: result.0,
        decisive_child_id: None,
        evidence: Some(result.1),
        children: Vec::new(),
    }
}

#[derive(Clone, Copy)]
enum CompositeKind {
    All,
    Any,
}

fn evaluate_composite(
    node_id: u16,
    conditions: &[EntryConditionV1],
    input: &EntryConditionInputSet,
    next_node_id: &mut u16,
    kind: CompositeKind,
) -> QuantResult<ConditionNodeEvaluation> {
    let children = conditions
        .iter()
        .map(|condition| evaluate_node(condition, input, next_node_id))
        .collect::<QuantResult<Vec<_>>>()?;
    let (truth, decisive_child_id) = composite_truth(&children, kind);
    Ok(ConditionNodeEvaluation {
        node_id,
        truth,
        decisive_child_id,
        evidence: None,
        children,
    })
}

fn composite_truth(
    children: &[ConditionNodeEvaluation],
    kind: CompositeKind,
) -> (ConditionTruth, Option<u16>) {
    match kind {
        CompositeKind::All => {
            if let Some(child) = children
                .iter()
                .find(|child| child.truth == ConditionTruth::Unsatisfied)
            {
                return (ConditionTruth::Unsatisfied, Some(child.node_id));
            }
            if let Some(child) = children
                .iter()
                .find(|child| matches!(child.truth, ConditionTruth::Unavailable(_)))
            {
                return (child.truth.clone(), Some(child.node_id));
            }
            (ConditionTruth::Satisfied, None)
        }
        CompositeKind::Any => {
            if let Some(child) = children
                .iter()
                .find(|child| child.truth == ConditionTruth::Satisfied)
            {
                return (ConditionTruth::Satisfied, Some(child.node_id));
            }
            if let Some(child) = children
                .iter()
                .find(|child| matches!(child.truth, ConditionTruth::Unavailable(_)))
            {
                return (child.truth.clone(), Some(child.node_id));
            }
            (ConditionTruth::Unsatisfied, None)
        }
    }
}

fn evaluate_price(
    condition: &PriceCondition,
    input: &EntryConditionInputSet,
) -> (ConditionTruth, ConditionLeafEvidence) {
    let Some(value) = input
        .prices
        .iter()
        .find(|value| value.token_id == condition.token_id)
        .cloned()
    else {
        return unavailable_leaf(ConditionUnavailableReason::InputMissing);
    };
    if let Some(reason) = freshness_reason(
        value.observed_at,
        value.available_at,
        input.evaluated_at,
        condition.max_input_age_ms,
    ) {
        return (
            ConditionTruth::Unavailable(reason),
            ConditionLeafEvidence::Price(value),
        );
    }
    let satisfied = compare(
        condition.comparison,
        value.price.inner(),
        condition.threshold.inner(),
    );
    (
        truth_from_bool(satisfied),
        ConditionLeafEvidence::Price(value),
    )
}

fn evaluate_factor(
    condition: &FactorCondition,
    input: &EntryConditionInputSet,
) -> (ConditionTruth, ConditionLeafEvidence) {
    let Some(value) = input
        .factors
        .iter()
        .find(|value| value.definition_id == condition.definition_id)
        .cloned()
    else {
        return unavailable_leaf(ConditionUnavailableReason::InputMissing);
    };
    if value.definition_hash != condition.definition_hash
        || value.model_version_id != condition.model_version_id
    {
        return (
            ConditionTruth::Unavailable(ConditionUnavailableReason::FactorDefinitionMismatch),
            ConditionLeafEvidence::Factor(value),
        );
    }
    if let Some(reason) = freshness_reason(
        value.observed_at,
        value.available_at,
        input.evaluated_at,
        condition.max_input_age_ms,
    ) {
        return (
            ConditionTruth::Unavailable(reason),
            ConditionLeafEvidence::Factor(value),
        );
    }
    let measure = match condition.measure {
        FactorMeasure::Raw => value.raw_value,
        FactorMeasure::Normalized => value.normalized_value,
    };
    let satisfied = value.confidence >= condition.minimum_confidence
        && compare(condition.comparison, measure, condition.threshold);
    (
        truth_from_bool(satisfied),
        ConditionLeafEvidence::Factor(value),
    )
}

fn evaluate_market_event(
    condition: &MarketEventCondition,
    input: &EntryConditionInputSet,
) -> (ConditionTruth, ConditionLeafEvidence) {
    match condition {
        MarketEventCondition::CryptoSubjectPredicateEntered(condition) => {
            evaluate_crypto(condition, input)
        }
        MarketEventCondition::WeatherDailyHighEnteredBand(condition) => {
            evaluate_weather_entered(condition, input)
        }
        MarketEventCondition::WeatherDailyHighExceededBandUpper(condition) => {
            evaluate_weather_exceeded(condition, input)
        }
        MarketEventCondition::WeatherObservationDayClosedOutsideBand(condition) => {
            evaluate_weather_closed_outside(condition, input)
        }
    }
}

fn evaluate_crypto(
    condition: &CryptoSubjectPredicateEntered,
    input: &EntryConditionInputSet,
) -> (ConditionTruth, ConditionLeafEvidence) {
    let Some(value) = input
        .crypto
        .iter()
        .find(|value| value.source == condition.source)
        .cloned()
    else {
        return unavailable_leaf(ConditionUnavailableReason::InputMissing);
    };
    if !value.source_healthy {
        return (
            ConditionTruth::Unavailable(ConditionUnavailableReason::SourceUnhealthy {
                source_id: value.source.source_id.clone(),
            }),
            ConditionLeafEvidence::Crypto(value),
        );
    }
    if let Some(reason) = freshness_reason(
        value.transition_at,
        value.available_at,
        input.evaluated_at,
        condition.max_input_age_ms,
    ) {
        return (
            ConditionTruth::Unavailable(reason),
            ConditionLeafEvidence::Crypto(value),
        );
    }
    let outcome = crypto_outcome(
        condition.comparator,
        condition.strike,
        condition.reference_price,
        value.current_price,
    );
    (
        truth_from_bool(outcome == Some(condition.recommended_outcome)),
        ConditionLeafEvidence::Crypto(value),
    )
}

fn crypto_outcome(
    comparator: PriceComparator,
    strike: Option<Usd>,
    reference: Option<Usd>,
    price: Usd,
) -> Option<OutcomeSide> {
    let yes = match comparator {
        PriceComparator::Above => price >= strike?,
        PriceComparator::Below => price <= strike?,
        PriceComparator::Between { hi } => price >= strike? && price <= hi,
        PriceComparator::UpVsReference => price >= reference?,
    };
    Some(if yes {
        OutcomeSide::Yes
    } else {
        OutcomeSide::No
    })
}

fn evaluate_weather_entered(
    condition: &WeatherDailyHighEnteredBand,
    input: &EntryConditionInputSet,
) -> (ConditionTruth, ConditionLeafEvidence) {
    evaluate_weather(condition, input, |value| {
        let high = value.current_high.whole_degrees(condition.unit);
        condition.band.contains(high)
    })
}

fn evaluate_weather_exceeded(
    condition: &WeatherDailyHighExceededBandUpper,
    input: &EntryConditionInputSet,
) -> (ConditionTruth, ConditionLeafEvidence) {
    evaluate_weather(condition, input, |value| {
        value.current_high.whole_degrees(condition.unit) > condition.upper_inclusive
    })
}

fn evaluate_weather_closed_outside(
    condition: &WeatherObservationDayClosedOutsideBand,
    input: &EntryConditionInputSet,
) -> (ConditionTruth, ConditionLeafEvidence) {
    evaluate_weather(condition, input, |value| {
        let high = value.current_high.whole_degrees(condition.unit);
        value.day_closed
            && (condition
                .band
                .lower_inclusive
                .is_some_and(|lower| high < lower)
                || condition
                    .band
                    .upper_inclusive
                    .is_some_and(|upper| high > upper))
    })
}

trait WeatherPredicate {
    fn source(&self) -> &EntryConditionSourceBinding;
    fn station(&self) -> &str;
    fn local_date(&self) -> chrono::NaiveDate;
    fn max_input_age_ms(&self) -> Option<u64>;
}

impl WeatherPredicate for WeatherDailyHighEnteredBand {
    fn source(&self) -> &EntryConditionSourceBinding {
        &self.source
    }
    fn station(&self) -> &str {
        &self.station
    }
    fn local_date(&self) -> chrono::NaiveDate {
        self.local_date
    }
    fn max_input_age_ms(&self) -> Option<u64> {
        Some(self.max_input_age_ms)
    }
}

impl WeatherPredicate for WeatherDailyHighExceededBandUpper {
    fn source(&self) -> &EntryConditionSourceBinding {
        &self.source
    }
    fn station(&self) -> &str {
        &self.station
    }
    fn local_date(&self) -> chrono::NaiveDate {
        self.local_date
    }
    fn max_input_age_ms(&self) -> Option<u64> {
        Some(self.max_input_age_ms)
    }
}

impl WeatherPredicate for WeatherObservationDayClosedOutsideBand {
    fn source(&self) -> &EntryConditionSourceBinding {
        &self.source
    }
    fn station(&self) -> &str {
        &self.station
    }
    fn local_date(&self) -> chrono::NaiveDate {
        self.local_date
    }
    fn max_input_age_ms(&self) -> Option<u64> {
        None
    }
}

fn evaluate_weather<T, F>(
    condition: &T,
    input: &EntryConditionInputSet,
    predicate: F,
) -> (ConditionTruth, ConditionLeafEvidence)
where
    T: WeatherPredicate,
    F: FnOnce(&WeatherDailyHighInput) -> bool,
{
    let Some(value) = input
        .weather
        .iter()
        .find(|value| {
            value.source == *condition.source()
                && value.station == condition.station()
                && value.local_date == condition.local_date()
        })
        .cloned()
    else {
        return unavailable_leaf(ConditionUnavailableReason::InputMissing);
    };
    if !value.source_healthy {
        return (
            ConditionTruth::Unavailable(ConditionUnavailableReason::SourceUnhealthy {
                source_id: value.source.source_id.clone(),
            }),
            ConditionLeafEvidence::Weather(value),
        );
    }
    if let Some(max_age_ms) = condition.max_input_age_ms()
        && let Some(reason) = freshness_reason(
            value.observation_time,
            value.available_at,
            input.evaluated_at,
            max_age_ms,
        )
    {
        return (
            ConditionTruth::Unavailable(reason),
            ConditionLeafEvidence::Weather(value),
        );
    }
    let satisfied = predicate(&value);
    (
        truth_from_bool(satisfied),
        ConditionLeafEvidence::Weather(value),
    )
}

fn unavailable_leaf(reason: ConditionUnavailableReason) -> (ConditionTruth, ConditionLeafEvidence) {
    (
        ConditionTruth::Unavailable(reason.clone()),
        ConditionLeafEvidence::Unavailable(reason),
    )
}

fn freshness_reason(
    observed_at: DateTime<Utc>,
    available_at: DateTime<Utc>,
    evaluated_at: DateTime<Utc>,
    max_age_ms: u64,
) -> Option<ConditionUnavailableReason> {
    if observed_at > available_at || available_at > evaluated_at {
        return Some(ConditionUnavailableReason::ClockSkew);
    }
    let Ok(max_age_ms) = i64::try_from(max_age_ms) else {
        return Some(ConditionUnavailableReason::InputStale);
    };
    (evaluated_at - observed_at > Duration::milliseconds(max_age_ms))
        .then_some(ConditionUnavailableReason::InputStale)
}

fn compare(comparison: PriceComparison, value: Decimal, threshold: Decimal) -> bool {
    match comparison {
        PriceComparison::AtOrAbove => value >= threshold,
        PriceComparison::AtOrBelow => value <= threshold,
    }
}

const fn truth_from_bool(value: bool) -> ConditionTruth {
    if value {
        ConditionTruth::Satisfied
    } else {
        ConditionTruth::Unsatisfied
    }
}

fn flatten_evidence(tree: &ConditionNodeEvaluation) -> Vec<&ConditionLeafEvidence> {
    let mut values = Vec::new();
    append_evidence(tree, &mut values);
    values
}

fn append_evidence<'a>(
    tree: &'a ConditionNodeEvaluation,
    values: &mut Vec<&'a ConditionLeafEvidence>,
) {
    if let Some(evidence) = tree.evidence.as_ref() {
        values.push(evidence);
    }
    for child in &tree.children {
        append_evidence(child, values);
    }
}

#[derive(Serialize)]
struct ContinuityProjection<'a> {
    node_index: usize,
    source: Option<&'a EntryConditionSourceBinding>,
    gap_generation: Option<u64>,
}

fn continuity_projection<'a>(
    evidence: &'a [&'a ConditionLeafEvidence],
) -> Vec<ContinuityProjection<'a>> {
    evidence
        .iter()
        .enumerate()
        .map(|(node_index, evidence)| match evidence {
            ConditionLeafEvidence::Price(value) => ContinuityProjection {
                node_index,
                source: None,
                gap_generation: Some(value.gap_generation),
            },
            ConditionLeafEvidence::Crypto(value) => ContinuityProjection {
                node_index,
                source: Some(&value.source),
                gap_generation: Some(value.gap_generation),
            },
            ConditionLeafEvidence::Weather(value) => ContinuityProjection {
                node_index,
                source: Some(&value.source),
                gap_generation: Some(value.gap_generation),
            },
            ConditionLeafEvidence::Clock { .. }
            | ConditionLeafEvidence::Factor(_)
            | ConditionLeafEvidence::Unavailable(_) => ContinuityProjection {
                node_index,
                source: None,
                gap_generation: None,
            },
        })
        .collect()
}

/// Derive the durable FSM transition. Any unavailable/false/gap reset clears
/// continuity; `Qualified` remains revocable until the repository claim.
#[must_use]
pub fn decide_entry_condition_state(
    current_state: EntryConditionState,
    current_confirmation_started_at: Option<DateTime<Utc>>,
    previous_continuity_hash: Option<&ContentHash>,
    previous_evaluated_at: Option<DateTime<Utc>>,
    artifact: &EntryConditionArtifactV1,
    evaluation: &EntryConditionEvaluation,
    evaluated_at: DateTime<Utc>,
) -> EntryConditionStateDecision {
    if matches!(
        current_state,
        EntryConditionState::Consumed
            | EntryConditionState::Expired
            | EntryConditionState::Invalidated
    ) {
        return EntryConditionStateDecision {
            state: current_state,
            confirmation_started_at: current_confirmation_started_at,
        };
    }
    match &evaluation.truth {
        ConditionTruth::Unavailable(_) => EntryConditionStateDecision {
            state: EntryConditionState::Unavailable,
            confirmation_started_at: None,
        },
        ConditionTruth::Unsatisfied => EntryConditionStateDecision {
            state: EntryConditionState::Waiting,
            confirmation_started_at: None,
        },
        ConditionTruth::Satisfied if artifact.confirmation.required_continuous_ms == 0 => {
            EntryConditionStateDecision {
                state: EntryConditionState::Qualified,
                confirmation_started_at: Some(evaluated_at),
            }
        }
        ConditionTruth::Satisfied => {
            let max_gap =
                i64::try_from(artifact.confirmation.max_observation_gap_ms).unwrap_or(i64::MAX);
            let continuity_changed = previous_continuity_hash
                .is_some_and(|previous| previous != &evaluation.continuity_hash);
            let observation_gap = previous_evaluated_at
                .is_some_and(|previous| evaluated_at - previous > Duration::milliseconds(max_gap));
            let confirmation_started_at = if continuity_changed || observation_gap {
                evaluated_at
            } else {
                current_confirmation_started_at.unwrap_or(evaluated_at)
            };
            let required =
                i64::try_from(artifact.confirmation.required_continuous_ms).unwrap_or(i64::MAX);
            let qualified =
                evaluated_at - confirmation_started_at >= Duration::milliseconds(required);
            EntryConditionStateDecision {
                state: if qualified {
                    EntryConditionState::Qualified
                } else {
                    EntryConditionState::Confirming
                },
                confirmation_started_at: Some(confirmation_started_at),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use chrono::{DateTime, Duration, NaiveDate, TimeZone, Utc};
    use quant_pivot_models::{
        domain::PriceComparator,
        enums::quant::{EntryConditionState, OutcomeSide},
        types::{
            ClockAnchor, ClockCondition, ConditionTruth, ConditionUnavailableReason,
            ConfirmationPolicy, ContentHash, CryptoSubjectPredicateEntered, DomainInstrumentKey,
            DomainSourceId, ENTRY_CONDITION_EVALUATOR_VERSION, ENTRY_CONDITION_SCHEMA_VERSION,
            EntryConditionArtifactV1, EntryConditionBinding, EntryConditionSourceBinding,
            EntryConditionV1, MarketEventCondition, MarketId, MarketLinkageId, MarketSelectionId,
            ModelVersionId, RecommendationId, RuntimeConfigVersionId, TemperatureBand,
            TemperatureCelsius, TemperatureUnit, TokenId, Usd, WeatherDailyHighEnteredBand,
            WeatherDailyHighExceededBandUpper, WeatherObservationDayClosedOutsideBand,
        },
    };
    use rust_decimal_macros::dec;

    use super::{
        CompositeKind, ConditionNodeEvaluation, CryptoPriceInput, EntryConditionInputSet,
        WeatherDailyHighInput, composite_truth, decide_entry_condition_state,
        evaluate_entry_condition,
    };

    fn node(node_id: u16, truth: ConditionTruth) -> ConditionNodeEvaluation {
        ConditionNodeEvaluation {
            node_id,
            truth,
            decisive_child_id: None,
            evidence: None,
            children: Vec::new(),
        }
    }

    fn timestamp() -> DateTime<Utc> {
        Utc.with_ymd_and_hms(2026, 7, 13, 0, 0, 0).unwrap()
    }

    fn hash(seed: char) -> ContentHash {
        ContentHash::parse(format!("blake3:{}", seed.to_string().repeat(64))).expect("hash")
    }

    fn binding() -> EntryConditionBinding {
        EntryConditionBinding {
            recommendation_id: RecommendationId::from_v7(),
            market_id: MarketId::new("market"),
            token_id: TokenId::new("token"),
            outcome_side: OutcomeSide::Yes,
            market_linkage_id: None,
            market_linkage_hash: None,
            catalog_snapshot_id: MarketSelectionId::from_v7(),
            catalog_snapshot_hash: hash('a'),
            model_version_id: ModelVersionId::from_v7(),
            runtime_config_version_id: RuntimeConfigVersionId::from_v7(),
            factor_bindings: Vec::new(),
            source_bindings: Vec::new(),
        }
    }

    fn artifact(confirmation: ConfirmationPolicy) -> EntryConditionArtifactV1 {
        let anchor_at = timestamp();
        EntryConditionArtifactV1 {
            schema_version: ENTRY_CONDITION_SCHEMA_VERSION,
            evaluator_version: ENTRY_CONDITION_EVALUATOR_VERSION,
            binding: binding(),
            confirmation,
            root: EntryConditionV1::Clock(ClockCondition {
                anchor: ClockAnchor::RecommendationDecision,
                anchor_at,
                offset_ms: 0,
                deadline_at: anchor_at,
            }),
        }
    }

    fn source(source_id: DomainSourceId, instrument: &str) -> EntryConditionSourceBinding {
        EntryConditionSourceBinding {
            source_id,
            instrument_key: DomainInstrumentKey::new(instrument),
            binding_hash: hash('b'),
        }
    }

    fn market_event_artifact(
        event: MarketEventCondition,
        source: EntryConditionSourceBinding,
    ) -> EntryConditionArtifactV1 {
        let mut binding = binding();
        binding.market_linkage_id = Some(MarketLinkageId::from_v7());
        binding.market_linkage_hash = Some(hash('c'));
        binding.source_bindings = vec![source];
        EntryConditionArtifactV1 {
            schema_version: ENTRY_CONDITION_SCHEMA_VERSION,
            evaluator_version: ENTRY_CONDITION_EVALUATOR_VERSION,
            binding,
            confirmation: ConfirmationPolicy::none(),
            root: EntryConditionV1::MarketEvent { event },
        }
        .canonicalize()
        .expect("canonical market-event artifact")
    }

    fn input(
        artifact: &EntryConditionArtifactV1,
        evaluated_at: DateTime<Utc>,
    ) -> EntryConditionInputSet {
        EntryConditionInputSet {
            binding: artifact.binding.clone(),
            evaluated_at,
            prices: Vec::new(),
            factors: Vec::new(),
            crypto: Vec::new(),
            weather: Vec::new(),
        }
    }

    #[test]
    fn all_prefers_unsatisfied_over_unavailable() {
        let children = [
            node(
                1,
                ConditionTruth::Unavailable(ConditionUnavailableReason::InputMissing),
            ),
            node(2, ConditionTruth::Unsatisfied),
        ];
        assert_eq!(
            composite_truth(&children, CompositeKind::All),
            (ConditionTruth::Unsatisfied, Some(2))
        );
    }

    #[test]
    fn any_prefers_satisfied_over_unavailable() {
        let children = [
            node(
                1,
                ConditionTruth::Unavailable(ConditionUnavailableReason::InputMissing),
            ),
            node(2, ConditionTruth::Satisfied),
        ];
        assert_eq!(
            composite_truth(&children, CompositeKind::Any),
            (ConditionTruth::Satisfied, Some(2))
        );
        assert_ne!(
            EntryConditionState::Qualified,
            EntryConditionState::Consumed
        );
    }

    #[test]
    fn composite_truth_tables_are_total_and_deterministic() {
        let unavailable = ConditionTruth::Unavailable(ConditionUnavailableReason::InputMissing);
        let truths = [
            ConditionTruth::Satisfied,
            ConditionTruth::Unsatisfied,
            unavailable,
        ];
        for left in &truths {
            for right in &truths {
                let children = [node(1, left.clone()), node(2, right.clone())];
                let all = composite_truth(&children, CompositeKind::All);
                let any = composite_truth(&children, CompositeKind::Any);
                let expected_all = if left == &ConditionTruth::Unsatisfied {
                    (ConditionTruth::Unsatisfied, Some(1))
                } else if right == &ConditionTruth::Unsatisfied {
                    (ConditionTruth::Unsatisfied, Some(2))
                } else if matches!(left, ConditionTruth::Unavailable(_)) {
                    (left.clone(), Some(1))
                } else if matches!(right, ConditionTruth::Unavailable(_)) {
                    (right.clone(), Some(2))
                } else {
                    (ConditionTruth::Satisfied, None)
                };
                let expected_any = if left == &ConditionTruth::Satisfied {
                    (ConditionTruth::Satisfied, Some(1))
                } else if right == &ConditionTruth::Satisfied {
                    (ConditionTruth::Satisfied, Some(2))
                } else if matches!(left, ConditionTruth::Unavailable(_)) {
                    (left.clone(), Some(1))
                } else if matches!(right, ConditionTruth::Unavailable(_)) {
                    (right.clone(), Some(2))
                } else {
                    (ConditionTruth::Unsatisfied, None)
                };
                assert_eq!(all, expected_all);
                assert_eq!(any, expected_any);
            }
        }
    }

    #[test]
    fn same_pit_inputs_produce_identical_evaluation_hashes() {
        let artifact = artifact(ConfirmationPolicy::none());
        let input = input(&artifact, timestamp());
        let live = evaluate_entry_condition(&artifact, &input).expect("live evaluation");
        let replay = evaluate_entry_condition(&artifact, &input).expect("replay evaluation");

        assert_eq!(live, replay);
        assert_eq!(live.truth, ConditionTruth::Satisfied);
    }

    #[test]
    fn binding_drift_fails_closed() {
        let artifact = artifact(ConfirmationPolicy::none());
        let mut drifted = input(&artifact, timestamp());
        drifted.binding.token_id = TokenId::new("other-token");

        let evaluation = evaluate_entry_condition(&artifact, &drifted).expect("evaluation");

        assert_eq!(
            evaluation.truth,
            ConditionTruth::Unavailable(ConditionUnavailableReason::BindingDrift)
        );
    }

    #[test]
    fn confirmation_resets_after_gap_or_unavailable() {
        let artifact = artifact(ConfirmationPolicy {
            required_continuous_ms: 2_000,
            max_observation_gap_ms: 1_000,
        });
        let started_at = timestamp();
        let first = evaluate_entry_condition(&artifact, &input(&artifact, started_at))
            .expect("first evaluation");
        let first_decision = decide_entry_condition_state(
            EntryConditionState::Waiting,
            None,
            None,
            None,
            &artifact,
            &first,
            started_at,
        );
        assert_eq!(first_decision.state, EntryConditionState::Confirming);

        let qualified_at = started_at + Duration::milliseconds(2_000);
        let qualified = evaluate_entry_condition(&artifact, &input(&artifact, qualified_at))
            .expect("qualified evaluation");
        let qualified_decision = decide_entry_condition_state(
            EntryConditionState::Confirming,
            Some(started_at),
            Some(&first.continuity_hash),
            Some(started_at + Duration::milliseconds(1_000)),
            &artifact,
            &qualified,
            qualified_at,
        );
        assert_eq!(qualified_decision.state, EntryConditionState::Qualified);

        let after_gap = qualified_at + Duration::milliseconds(2_000);
        let gap_evaluation = evaluate_entry_condition(&artifact, &input(&artifact, after_gap))
            .expect("gap evaluation");
        let gap_decision = decide_entry_condition_state(
            EntryConditionState::Qualified,
            Some(started_at),
            Some(&qualified.continuity_hash),
            Some(qualified_at),
            &artifact,
            &gap_evaluation,
            after_gap,
        );
        assert_eq!(gap_decision.state, EntryConditionState::Confirming);
        assert_eq!(gap_decision.confirmation_started_at, Some(after_gap));

        let mut drifted = input(&artifact, after_gap);
        drifted.binding.token_id = TokenId::new("drifted");
        let unavailable = evaluate_entry_condition(&artifact, &drifted).expect("unavailable");
        let unavailable_decision = decide_entry_condition_state(
            gap_decision.state,
            gap_decision.confirmation_started_at,
            Some(&gap_evaluation.continuity_hash),
            Some(after_gap),
            &artifact,
            &unavailable,
            after_gap,
        );
        assert_eq!(unavailable_decision.state, EntryConditionState::Unavailable);
        assert_eq!(unavailable_decision.confirmation_started_at, None);
    }

    #[test]
    fn crypto_recross_revokes_and_cross_source_input_cannot_substitute() {
        let evaluated_at = timestamp();
        let frozen_source = source(
            DomainSourceId::chainlink_data_streams(),
            "CHAINLINK_DATA_STREAMS:BTC-USD",
        );
        let artifact = market_event_artifact(
            MarketEventCondition::CryptoSubjectPredicateEntered(CryptoSubjectPredicateEntered {
                source: frozen_source.clone(),
                comparator: PriceComparator::UpVsReference,
                strike: None,
                reference_price: Some(Usd::new(dec!(100))),
                recommended_outcome: OutcomeSide::Yes,
                max_input_age_ms: 2_000,
            }),
            frozen_source.clone(),
        );
        let chainlink = CryptoPriceInput {
            source: frozen_source,
            previous_price: Usd::new(dec!(99)),
            current_price: Usd::new(dec!(101)),
            source_sequence: 1,
            transition_at: evaluated_at,
            available_at: evaluated_at,
            report_hash: hash('d'),
            gap_generation: 0,
            source_healthy: true,
        };
        let mut inputs = input(&artifact, evaluated_at);
        inputs.crypto.push(chainlink);
        let entered = evaluate_entry_condition(&artifact, &inputs).expect("entered evaluation");
        assert_eq!(entered.truth, ConditionTruth::Satisfied);

        inputs.crypto[0].previous_price = Usd::new(dec!(101));
        inputs.crypto[0].current_price = Usd::new(dec!(99));
        inputs.crypto[0].source_sequence = 2;
        inputs.crypto[0].report_hash = hash('e');
        let recrossed = evaluate_entry_condition(&artifact, &inputs).expect("recross evaluation");
        assert_eq!(recrossed.truth, ConditionTruth::Unsatisfied);
        assert_eq!(
            decide_entry_condition_state(
                EntryConditionState::Qualified,
                Some(evaluated_at),
                Some(&entered.continuity_hash),
                Some(evaluated_at),
                &artifact,
                &recrossed,
                evaluated_at,
            )
            .state,
            EntryConditionState::Waiting
        );

        inputs.crypto[0].source = source(
            DomainSourceId::binance_agg_trade(),
            "BINANCE_AGG_TRADE:BTCUSDT",
        );
        let cross_source =
            evaluate_entry_condition(&artifact, &inputs).expect("cross-source evaluation");
        assert_eq!(
            cross_source.truth,
            ConditionTruth::Unavailable(ConditionUnavailableReason::InputMissing)
        );
    }

    #[test]
    fn weather_yes_and_bounded_no_follow_corrected_whole_degree_high() {
        let evaluated_at = timestamp();
        let local_date = NaiveDate::from_ymd_opt(2026, 7, 13).expect("local date");
        let frozen_source = source(DomainSourceId::aviation_weather(), "AVIATION_WEATHER:KJFK");
        let yes_artifact = market_event_artifact(
            MarketEventCondition::WeatherDailyHighEnteredBand(WeatherDailyHighEnteredBand {
                source: frozen_source.clone(),
                station: "KJFK".to_owned(),
                local_date,
                unit: TemperatureUnit::Fahrenheit,
                band: TemperatureBand {
                    lower_inclusive: Some(dec!(80)),
                    upper_inclusive: Some(dec!(81)),
                },
                proxy_methodology_hash: hash('f'),
                max_input_age_ms: 2_000,
            }),
            frozen_source.clone(),
        );
        let weather = WeatherDailyHighInput {
            source: frozen_source.clone(),
            station: "KJFK".to_owned(),
            local_date,
            current_high: TemperatureCelsius::new(dec!(26.7)),
            observation_time: evaluated_at,
            available_at: evaluated_at,
            revision: 0,
            day_closed: false,
            report_hash: hash('1'),
            gap_generation: 0,
            source_healthy: true,
        };
        let mut yes_input = input(&yes_artifact, evaluated_at);
        yes_input.weather.push(weather.clone());
        let entered =
            evaluate_entry_condition(&yes_artifact, &yes_input).expect("weather YES evaluation");
        assert_eq!(entered.truth, ConditionTruth::Satisfied);

        yes_input.weather[0].current_high = TemperatureCelsius::new(dec!(25));
        yes_input.weather[0].revision = 1;
        yes_input.weather[0].report_hash = hash('2');
        let corrected = evaluate_entry_condition(&yes_artifact, &yes_input)
            .expect("weather correction evaluation");
        assert_eq!(corrected.truth, ConditionTruth::Unsatisfied);

        let no_artifact = market_event_artifact(
            MarketEventCondition::WeatherDailyHighExceededBandUpper(
                WeatherDailyHighExceededBandUpper {
                    source: frozen_source,
                    station: "KJFK".to_owned(),
                    local_date,
                    unit: TemperatureUnit::Fahrenheit,
                    upper_inclusive: dec!(80),
                    proxy_methodology_hash: hash('f'),
                    max_input_age_ms: 2_000,
                },
            ),
            weather.source.clone(),
        );
        let mut no_input = input(&no_artifact, evaluated_at);
        no_input.weather.push(WeatherDailyHighInput {
            current_high: TemperatureCelsius::new(dec!(27.3)),
            ..weather
        });
        let exceeded =
            evaluate_entry_condition(&no_artifact, &no_input).expect("bounded NO evaluation");
        assert_eq!(exceeded.truth, ConditionTruth::Satisfied);
    }

    #[test]
    fn weather_open_upper_no_requires_observation_day_close() {
        let evaluated_at = timestamp();
        let local_date = NaiveDate::from_ymd_opt(2026, 7, 13).expect("local date");
        let frozen_source = source(DomainSourceId::aviation_weather(), "AVIATION_WEATHER:KJFK");
        let artifact = market_event_artifact(
            MarketEventCondition::WeatherObservationDayClosedOutsideBand(
                WeatherObservationDayClosedOutsideBand {
                    source: frozen_source.clone(),
                    station: "KJFK".to_owned(),
                    local_date,
                    unit: TemperatureUnit::Fahrenheit,
                    band: TemperatureBand {
                        lower_inclusive: Some(dec!(90)),
                        upper_inclusive: None,
                    },
                    proxy_methodology_hash: hash('f'),
                },
            ),
            frozen_source.clone(),
        );
        let mut inputs = input(&artifact, evaluated_at);
        inputs.weather.push(WeatherDailyHighInput {
            source: frozen_source,
            station: "KJFK".to_owned(),
            local_date,
            current_high: TemperatureCelsius::new(dec!(29.4)),
            observation_time: evaluated_at,
            available_at: evaluated_at,
            revision: 0,
            day_closed: false,
            report_hash: hash('3'),
            gap_generation: 0,
            source_healthy: true,
        });
        let still_open = evaluate_entry_condition(&artifact, &inputs).expect("open-day evaluation");
        assert_eq!(still_open.truth, ConditionTruth::Unsatisfied);

        inputs.weather[0].day_closed = true;
        inputs.weather[0].revision = 1;
        inputs.weather[0].report_hash = hash('4');
        let closed = evaluate_entry_condition(&artifact, &inputs).expect("day-close evaluation");
        assert_eq!(closed.truth, ConditionTruth::Satisfied);

        inputs.weather[0].source_healthy = false;
        let unhealthy =
            evaluate_entry_condition(&artifact, &inputs).expect("source-health evaluation");
        assert_eq!(
            unhealthy.truth,
            ConditionTruth::Unavailable(ConditionUnavailableReason::SourceUnhealthy {
                source_id: DomainSourceId::aviation_weather(),
            })
        );
    }
}
