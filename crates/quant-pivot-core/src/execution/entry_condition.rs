//! Deterministic live/replay entry-condition evaluator.

use chrono::{DateTime, Duration, Utc};
use quant_pivot_error::{QuantError, QuantResult, report::ReportError};
use quant_pivot_models::{
    domain::{PriceBoundaryInclusion, PriceComparator},
    enums::quant::{EntryConditionState, OutcomeSide, PriceComparison},
    hashing::CanonicalDigest,
    types::{
        ConditionTruth, ConditionUnavailableReason, ContentHash, CryptoEnteredFoldState,
        CryptoSubjectPredicateEntered, EntryConditionArtifactV1, EntryConditionBinding,
        EntryConditionFoldState, EntryConditionSourceBinding, EntryConditionV1, FactorCondition,
        FactorDefinitionId, FactorMeasure, MarketEventCondition, ModelVersionId, Price,
        PriceCondition, TemperatureCelsius, TokenId, Usd,
        WeatherDailyTemperatureCrossedTerminalBound, WeatherDailyTemperatureEnteredBand,
        WeatherObservationDayClosedOutsideBand, WeatherTemperatureStatistic,
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

/// One source-native crypto fact in deterministic fold order.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct CryptoPriceReportInput {
    pub source_sequence: u64,
    pub price: Usd,
    pub event_at: DateTime<Utc>,
    pub available_at: DateTime<Utc>,
    pub report_hash: ContentHash,
}

/// Ordered same-source crypto facts and the source discontinuity generation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct CryptoPriceInput {
    pub source: EntryConditionSourceBinding,
    pub reports: Vec<CryptoPriceReportInput>,
    pub gap_generation: u64,
    pub source_healthy: bool,
}

/// Current corrected NOAA proxy state for one station/local day.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct WeatherDailyTemperatureInput {
    pub source: EntryConditionSourceBinding,
    pub station: String,
    pub local_date: chrono::NaiveDate,
    pub temperature_statistic: WeatherTemperatureStatistic,
    pub current_extreme: TemperatureCelsius,
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
    pub binding_revision: ContentHash,
    pub binding_unavailable_reason: Option<ConditionUnavailableReason>,
    pub fold_state: EntryConditionFoldState,
    pub evaluated_at: DateTime<Utc>,
    pub prices: Vec<ExecutablePriceInput>,
    pub factors: Vec<FactorSnapshotInput>,
    pub crypto: Vec<CryptoPriceInput>,
    pub weather: Vec<WeatherDailyTemperatureInput>,
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
    Crypto {
        input: CryptoPriceInput,
        state: CryptoEnteredFoldState,
    },
    Weather(WeatherDailyTemperatureInput),
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
    pub fold_state: EntryConditionFoldState,
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
    let mut fold_state = input.fold_state.clone();
    if let Some(reason) = input.binding_unavailable_reason.clone() {
        clear_crypto_latches(&mut fold_state);
        return unavailable_evaluation(&artifact.root, reason, input, fold_state);
    }
    if artifact.binding != input.binding {
        clear_crypto_latches(&mut fold_state);
        return unavailable_evaluation(
            &artifact.root,
            ConditionUnavailableReason::BindingDrift,
            input,
            fold_state,
        );
    }
    let mut node_id = 0_u16;
    let tree = evaluate_node(&artifact.root, input, &mut fold_state, &mut node_id)?;
    let evidence = flatten_evidence(&tree);
    let input_fingerprint =
        CanonicalDigest::content_hash_json(&evidence).map_err(QuantError::from)?;
    let continuity = ContinuityEnvelope {
        binding: &input.binding,
        binding_revision: &input.binding_revision,
        leaves: continuity_projection(&evidence),
    };
    let continuity_hash =
        CanonicalDigest::content_hash_json(&continuity).map_err(QuantError::from)?;
    let evaluation_hash = CanonicalDigest::content_hash_json(&tree).map_err(QuantError::from)?;
    Ok(EntryConditionEvaluation {
        truth: tree.truth.clone(),
        tree,
        evaluation_hash,
        input_fingerprint,
        continuity_hash,
        fold_state,
    })
}

fn unavailable_evaluation(
    root: &EntryConditionV1,
    reason: ConditionUnavailableReason,
    input: &EntryConditionInputSet,
    fold_state: EntryConditionFoldState,
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
        fold_state,
    })
}

fn evaluate_node(
    condition: &EntryConditionV1,
    input: &EntryConditionInputSet,
    fold_state: &mut EntryConditionFoldState,
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
        EntryConditionV1::MarketEvent { event: condition } => Ok(leaf(
            node_id,
            evaluate_market_event(node_id, condition, input, fold_state),
        )),
        EntryConditionV1::All { children } => evaluate_composite(
            node_id,
            children,
            input,
            fold_state,
            next_node_id,
            CompositeKind::All,
        ),
        EntryConditionV1::Any { children } => evaluate_composite(
            node_id,
            children,
            input,
            fold_state,
            next_node_id,
            CompositeKind::Any,
        ),
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
    fold_state: &mut EntryConditionFoldState,
    next_node_id: &mut u16,
    kind: CompositeKind,
) -> QuantResult<ConditionNodeEvaluation> {
    let children = conditions
        .iter()
        .map(|condition| evaluate_node(condition, input, fold_state, next_node_id))
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
    node_id: u16,
    condition: &MarketEventCondition,
    input: &EntryConditionInputSet,
    fold_state: &mut EntryConditionFoldState,
) -> (ConditionTruth, ConditionLeafEvidence) {
    match condition {
        MarketEventCondition::CryptoSubjectPredicateEntered(condition) => {
            evaluate_crypto(node_id, condition, input, fold_state)
        }
        MarketEventCondition::WeatherDailyTemperatureEnteredBand(condition) => {
            evaluate_weather_entered(condition, input)
        }
        MarketEventCondition::WeatherDailyTemperatureCrossedTerminalBound(condition) => {
            evaluate_weather_crossed_terminal_bound(condition, input)
        }
        MarketEventCondition::WeatherObservationDayClosedOutsideBand(condition) => {
            evaluate_weather_closed_outside(condition, input)
        }
    }
}

fn evaluate_crypto(
    node_id: u16,
    condition: &CryptoSubjectPredicateEntered,
    input: &EntryConditionInputSet,
    fold_state: &mut EntryConditionFoldState,
) -> (ConditionTruth, ConditionLeafEvidence) {
    let Some(value) = input
        .crypto
        .iter()
        .find(|value| value.source == condition.source)
        .cloned()
    else {
        return unavailable_leaf(ConditionUnavailableReason::InputMissing);
    };
    let mut state = fold_crypto_reports(node_id, condition, &value, fold_state);
    if !value.source_healthy {
        clear_crypto_latch(&mut fold_state.crypto, node_id);
        if let Some(cleared) = fold_state
            .crypto
            .iter()
            .find(|state| state.node_id == node_id)
        {
            state = cleared.clone();
        }
        return (
            ConditionTruth::Unavailable(ConditionUnavailableReason::SourceUnhealthy {
                source_id: value.source.source_id.clone(),
            }),
            ConditionLeafEvidence::Crypto {
                input: value,
                state,
            },
        );
    }
    let evidence = ConditionLeafEvidence::Crypto {
        input: value.clone(),
        state: state.clone(),
    };
    let Some(latest) = value.reports.last() else {
        return unavailable_leaf(ConditionUnavailableReason::InputMissing);
    };
    if let Some(reason) = freshness_reason(
        latest.event_at,
        latest.available_at,
        input.evaluated_at,
        condition.max_input_age_ms,
    ) {
        return (ConditionTruth::Unavailable(reason), evidence);
    }
    (truth_from_bool(state.latched), evidence)
}

fn fold_crypto_reports(
    node_id: u16,
    condition: &CryptoSubjectPredicateEntered,
    input: &CryptoPriceInput,
    fold_state: &mut EntryConditionFoldState,
) -> CryptoEnteredFoldState {
    let existing = fold_state
        .crypto
        .iter()
        .position(|state| state.node_id == node_id);
    let mut state = existing.map_or_else(
        || CryptoEnteredFoldState {
            node_id,
            source: input.source.clone(),
            last_outcome: None,
            latched: false,
            last_source_sequence: None,
            last_report_hash: None,
            last_event_at: None,
            last_available_at: None,
            gap_generation: input.gap_generation,
            discontinuity_epoch: 0,
            triggering_report_hash: None,
            triggering_at: None,
        },
        |index| fold_state.crypto[index].clone(),
    );
    if state.source != input.source || state.gap_generation != input.gap_generation {
        reset_crypto_continuity(&mut state, input);
    }
    for report in &input.reports {
        if state.last_source_sequence == Some(report.source_sequence)
            && state.last_report_hash.as_ref() == Some(&report.report_hash)
        {
            continue;
        }
        let correction = state
            .last_source_sequence
            .is_some_and(|sequence| report.source_sequence <= sequence);
        if correction {
            state.latched = false;
            state.last_outcome = None;
            state.triggering_report_hash = None;
            state.triggering_at = None;
            state.discontinuity_epoch = state.discontinuity_epoch.saturating_add(1);
        }
        let outcome = crypto_outcome(
            condition.comparator,
            condition.strike,
            condition.reference_price,
            report.price,
        );
        if let (Some(previous), Some(current)) = (state.last_outcome, outcome) {
            if previous != condition.recommended_outcome && current == condition.recommended_outcome
            {
                state.latched = true;
                state.triggering_report_hash = Some(report.report_hash.clone());
                state.triggering_at = Some(report.event_at);
            } else if previous == condition.recommended_outcome
                && current != condition.recommended_outcome
            {
                state.latched = false;
                state.triggering_report_hash = None;
                state.triggering_at = None;
                state.discontinuity_epoch = state.discontinuity_epoch.saturating_add(1);
            }
        }
        state.last_outcome = outcome;
        state.last_source_sequence = Some(report.source_sequence);
        state.last_report_hash = Some(report.report_hash.clone());
        state.last_event_at = Some(report.event_at);
        state.last_available_at = Some(report.available_at);
    }
    if let Some(index) = existing {
        fold_state.crypto[index] = state.clone();
    } else {
        fold_state.crypto.push(state.clone());
        fold_state.crypto.sort_by_key(|state| state.node_id);
    }
    state
}

fn reset_crypto_continuity(state: &mut CryptoEnteredFoldState, input: &CryptoPriceInput) {
    state.source = input.source.clone();
    state.last_outcome = None;
    state.latched = false;
    state.last_source_sequence = None;
    state.last_report_hash = None;
    state.last_event_at = None;
    state.last_available_at = None;
    state.gap_generation = input.gap_generation;
    state.discontinuity_epoch = state.discontinuity_epoch.saturating_add(1);
    state.triggering_report_hash = None;
    state.triggering_at = None;
}

fn clear_crypto_latches(state: &mut EntryConditionFoldState) {
    for leaf in &mut state.crypto {
        if leaf.latched {
            leaf.latched = false;
            leaf.triggering_report_hash = None;
            leaf.triggering_at = None;
            leaf.discontinuity_epoch = leaf.discontinuity_epoch.saturating_add(1);
        }
    }
}

fn clear_crypto_latch(states: &mut [CryptoEnteredFoldState], node_id: u16) {
    if let Some(state) = states.iter_mut().find(|state| state.node_id == node_id)
        && state.latched
    {
        state.latched = false;
        state.triggering_report_hash = None;
        state.triggering_at = None;
        state.discontinuity_epoch = state.discontinuity_epoch.saturating_add(1);
    }
}

fn crypto_outcome(
    comparator: PriceComparator,
    strike: Option<Usd>,
    reference: Option<Usd>,
    price: Usd,
) -> Option<OutcomeSide> {
    let yes = match comparator {
        PriceComparator::GreaterThan => price > strike?,
        PriceComparator::GreaterThanOrEqual => price >= strike?,
        PriceComparator::LessThan => price < strike?,
        PriceComparator::LessThanOrEqual => price <= strike?,
        PriceComparator::Between { hi, lower, upper } => {
            let strike = strike?;
            let above_lower = match lower {
                PriceBoundaryInclusion::Inclusive => price >= strike,
                PriceBoundaryInclusion::Exclusive => price > strike,
            };
            let below_upper = match upper {
                PriceBoundaryInclusion::Inclusive => price <= hi,
                PriceBoundaryInclusion::Exclusive => price < hi,
            };
            above_lower && below_upper
        }
        PriceComparator::UpVsReference => price >= reference?,
    };
    Some(if yes {
        OutcomeSide::Yes
    } else {
        OutcomeSide::No
    })
}

fn evaluate_weather_entered(
    condition: &WeatherDailyTemperatureEnteredBand,
    input: &EntryConditionInputSet,
) -> (ConditionTruth, ConditionLeafEvidence) {
    evaluate_weather(condition, input, |value| {
        let extreme = value.current_extreme.whole_degrees(condition.unit);
        condition.band.contains(extreme)
    })
}

fn evaluate_weather_crossed_terminal_bound(
    condition: &WeatherDailyTemperatureCrossedTerminalBound,
    input: &EntryConditionInputSet,
) -> (ConditionTruth, ConditionLeafEvidence) {
    evaluate_weather(condition, input, |value| {
        let extreme = value.current_extreme.whole_degrees(condition.unit);
        match condition.temperature_statistic {
            WeatherTemperatureStatistic::Maximum => extreme > condition.terminal_bound,
            WeatherTemperatureStatistic::Minimum => extreme < condition.terminal_bound,
        }
    })
}

fn evaluate_weather_closed_outside(
    condition: &WeatherObservationDayClosedOutsideBand,
    input: &EntryConditionInputSet,
) -> (ConditionTruth, ConditionLeafEvidence) {
    evaluate_weather(condition, input, |value| {
        let extreme = value.current_extreme.whole_degrees(condition.unit);
        value.day_closed
            && (condition
                .band
                .lower_inclusive
                .is_some_and(|lower| extreme < lower)
                || condition
                    .band
                    .upper_inclusive
                    .is_some_and(|upper| extreme > upper))
    })
}

trait WeatherPredicate {
    fn source(&self) -> &EntryConditionSourceBinding;
    fn station(&self) -> &str;
    fn local_date(&self) -> chrono::NaiveDate;
    fn temperature_statistic(&self) -> WeatherTemperatureStatistic;
    fn max_input_age_ms(&self) -> Option<u64>;
}

impl WeatherPredicate for WeatherDailyTemperatureEnteredBand {
    fn source(&self) -> &EntryConditionSourceBinding {
        &self.source
    }
    fn station(&self) -> &str {
        &self.station
    }
    fn local_date(&self) -> chrono::NaiveDate {
        self.local_date
    }
    fn temperature_statistic(&self) -> WeatherTemperatureStatistic {
        self.temperature_statistic
    }
    fn max_input_age_ms(&self) -> Option<u64> {
        Some(self.max_input_age_ms)
    }
}

impl WeatherPredicate for WeatherDailyTemperatureCrossedTerminalBound {
    fn source(&self) -> &EntryConditionSourceBinding {
        &self.source
    }
    fn station(&self) -> &str {
        &self.station
    }
    fn local_date(&self) -> chrono::NaiveDate {
        self.local_date
    }
    fn temperature_statistic(&self) -> WeatherTemperatureStatistic {
        self.temperature_statistic
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
    fn temperature_statistic(&self) -> WeatherTemperatureStatistic {
        self.temperature_statistic
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
    F: FnOnce(&WeatherDailyTemperatureInput) -> bool,
{
    let Some(value) = input
        .weather
        .iter()
        .find(|value| {
            value.source == *condition.source()
                && value.station == condition.station()
                && value.local_date == condition.local_date()
                && value.temperature_statistic == condition.temperature_statistic()
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
    revision_hash: Option<&'a ContentHash>,
    revision: Option<u64>,
    discontinuity_epoch: Option<u64>,
    latched: Option<bool>,
}

#[derive(Serialize)]
struct ContinuityEnvelope<'a> {
    binding: &'a EntryConditionBinding,
    binding_revision: &'a ContentHash,
    leaves: Vec<ContinuityProjection<'a>>,
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
                revision_hash: None,
                revision: None,
                discontinuity_epoch: None,
                latched: None,
            },
            ConditionLeafEvidence::Crypto { input, state } => ContinuityProjection {
                node_index,
                source: Some(&input.source),
                gap_generation: Some(input.gap_generation),
                revision_hash: state.triggering_report_hash.as_ref(),
                revision: None,
                discontinuity_epoch: Some(state.discontinuity_epoch),
                latched: Some(state.latched),
            },
            ConditionLeafEvidence::Weather(value) => ContinuityProjection {
                node_index,
                source: Some(&value.source),
                gap_generation: Some(value.gap_generation),
                revision_hash: Some(&value.report_hash),
                revision: Some(value.revision),
                discontinuity_epoch: None,
                latched: None,
            },
            ConditionLeafEvidence::Factor(value) => ContinuityProjection {
                node_index,
                source: None,
                gap_generation: None,
                revision_hash: Some(&value.snapshot_hash),
                revision: None,
                discontinuity_epoch: None,
                latched: None,
            },
            ConditionLeafEvidence::Clock { .. } | ConditionLeafEvidence::Unavailable(_) => {
                ContinuityProjection {
                    node_index,
                    source: None,
                    gap_generation: None,
                    revision_hash: None,
                    revision: None,
                    discontinuity_epoch: None,
                    latched: None,
                }
            }
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
        domain::{PriceBoundaryInclusion, PriceComparator},
        enums::quant::{EntryConditionState, OutcomeSide},
        types::{
            ClockAnchor, ClockCondition, ConditionTruth, ConditionUnavailableReason,
            ConfirmationPolicy, ContentHash, CryptoSubjectPredicateEntered,
            DecisionPolicySnapshotId, DomainInstrumentKey, DomainSourceId,
            ENTRY_CONDITION_EVALUATOR_VERSION, ENTRY_CONDITION_SCHEMA_VERSION,
            EntryConditionArtifactV1, EntryConditionBinding, EntryConditionFoldState,
            EntryConditionSourceBinding, EntryConditionV1, MarketEventCondition, MarketId,
            MarketLinkageId, MarketSelectionId, ModelVersionId, RecommendationId, TemperatureBand,
            TemperatureCelsius, TemperatureUnit, TokenId, Usd,
            WeatherDailyTemperatureCrossedTerminalBound, WeatherDailyTemperatureEnteredBand,
            WeatherObservationDayClosedOutsideBand, WeatherTemperatureStatistic,
        },
    };
    use rust_decimal::Decimal;
    use rust_decimal_macros::dec;

    use super::{
        CompositeKind, ConditionNodeEvaluation, CryptoPriceInput, CryptoPriceReportInput,
        EntryConditionInputSet, WeatherDailyTemperatureInput, composite_truth, crypto_outcome,
        decide_entry_condition_state, evaluate_entry_condition,
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

    #[test]
    fn crypto_outcome_honors_strict_and_owned_band_boundaries() {
        let strike = Some(Usd::new(dec!(100)));
        let price = Usd::new(dec!(100));
        assert_eq!(
            crypto_outcome(PriceComparator::GreaterThan, strike, None, price),
            Some(OutcomeSide::No)
        );
        assert_eq!(
            crypto_outcome(PriceComparator::GreaterThanOrEqual, strike, None, price),
            Some(OutcomeSide::Yes)
        );

        let band = PriceComparator::Between {
            hi: Usd::new(dec!(110)),
            lower: PriceBoundaryInclusion::Inclusive,
            upper: PriceBoundaryInclusion::Exclusive,
        };
        assert_eq!(
            crypto_outcome(band, strike, None, Usd::new(dec!(100))),
            Some(OutcomeSide::Yes)
        );
        assert_eq!(
            crypto_outcome(band, strike, None, Usd::new(dec!(110))),
            Some(OutcomeSide::No)
        );
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
            decision_policy_snapshot_id: DecisionPolicySnapshotId::from_v7(),
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
            binding_revision: hash('9'),
            binding_unavailable_reason: None,
            fold_state: EntryConditionFoldState::default(),
            evaluated_at,
            prices: Vec::new(),
            factors: Vec::new(),
            crypto: Vec::new(),
            weather: Vec::new(),
        }
    }

    fn crypto_input_set(
        artifact: &EntryConditionArtifactV1,
        evaluated_at: DateTime<Utc>,
        source: &EntryConditionSourceBinding,
        reports: &[(u64, Decimal, char)],
        gap_generation: u64,
        fold_state: EntryConditionFoldState,
    ) -> EntryConditionInputSet {
        let mut inputs = input(artifact, evaluated_at);
        inputs.fold_state = fold_state;
        inputs.crypto.push(CryptoPriceInput {
            source: source.clone(),
            reports: reports
                .iter()
                .map(|(sequence, price, report_hash)| CryptoPriceReportInput {
                    source_sequence: *sequence,
                    price: Usd::new(*price),
                    event_at: evaluated_at,
                    available_at: evaluated_at,
                    report_hash: hash(*report_hash),
                })
                .collect(),
            gap_generation,
            source_healthy: true,
        });
        inputs
    }

    fn assert_crypto_baseline_and_transient_recross(
        artifact: &EntryConditionArtifactV1,
        source: &EntryConditionSourceBinding,
        evaluated_at: DateTime<Utc>,
    ) {
        let already_on_side = crypto_input_set(
            artifact,
            evaluated_at,
            source,
            &[(1, dec!(101), '1')],
            0,
            EntryConditionFoldState::default(),
        );
        let baseline =
            evaluate_entry_condition(artifact, &already_on_side).expect("on-side baseline");
        assert_eq!(baseline.truth, ConditionTruth::Unsatisfied);
        assert!(!baseline.fold_state.crypto[0].latched);

        let transient = crypto_input_set(
            artifact,
            evaluated_at,
            source,
            &[(2, dec!(99), '2'), (3, dec!(101), '3'), (4, dec!(99), '4')],
            0,
            EntryConditionFoldState::default(),
        );
        let result = evaluate_entry_condition(artifact, &transient).expect("transient recross");
        assert_eq!(result.truth, ConditionTruth::Unsatisfied);
        assert!(!result.fold_state.crypto[0].latched);
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
            reports: vec![
                CryptoPriceReportInput {
                    source_sequence: 0,
                    price: Usd::new(dec!(99)),
                    event_at: evaluated_at - Duration::milliseconds(1),
                    available_at: evaluated_at - Duration::milliseconds(1),
                    report_hash: hash('c'),
                },
                CryptoPriceReportInput {
                    source_sequence: 1,
                    price: Usd::new(dec!(101)),
                    event_at: evaluated_at,
                    available_at: evaluated_at,
                    report_hash: hash('d'),
                },
            ],
            gap_generation: 0,
            source_healthy: true,
        };
        let mut inputs = input(&artifact, evaluated_at);
        inputs.crypto.push(chainlink);
        let entered = evaluate_entry_condition(&artifact, &inputs).expect("entered evaluation");
        assert_eq!(entered.truth, ConditionTruth::Satisfied);

        inputs.fold_state = entered.fold_state.clone();
        inputs.crypto[0].reports = vec![CryptoPriceReportInput {
            source_sequence: 2,
            price: Usd::new(dec!(99)),
            event_at: evaluated_at,
            available_at: evaluated_at,
            report_hash: hash('e'),
        }];
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
    fn crypto_fold_baseline_transient_gap_correction_and_restart_are_deterministic() {
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
                max_input_age_ms: 10_000,
            }),
            frozen_source.clone(),
        );
        assert_crypto_baseline_and_transient_recross(&artifact, &frozen_source, evaluated_at);

        let initial = crypto_input_set(
            &artifact,
            evaluated_at,
            &frozen_source,
            &[(10, dec!(99), 'a')],
            0,
            EntryConditionFoldState::default(),
        );
        let persisted = evaluate_entry_condition(&artifact, &initial).expect("persisted baseline");
        let after_restart = crypto_input_set(
            &artifact,
            evaluated_at,
            &frozen_source,
            &[(10, dec!(99), 'a'), (11, dec!(101), 'b')],
            0,
            persisted.fold_state,
        );
        let live = evaluate_entry_condition(&artifact, &after_restart).expect("restart fold");

        let replay = crypto_input_set(
            &artifact,
            evaluated_at,
            &frozen_source,
            &[(10, dec!(99), 'a'), (11, dec!(101), 'b')],
            0,
            EntryConditionFoldState::default(),
        );
        let replayed = evaluate_entry_condition(&artifact, &replay).expect("full replay fold");
        assert_eq!(live.tree, replayed.tree);
        assert_eq!(live.input_fingerprint, replayed.input_fingerprint);
        assert_eq!(live.evaluation_hash, replayed.evaluation_hash);
        assert_eq!(live.continuity_hash, replayed.continuity_hash);
        assert_eq!(live.fold_state, replayed.fold_state);

        let same_side = crypto_input_set(
            &artifact,
            evaluated_at,
            &frozen_source,
            &[(12, dec!(102), 'c')],
            0,
            live.fold_state.clone(),
        );
        let same_side_result =
            evaluate_entry_condition(&artifact, &same_side).expect("same-side tick");
        assert_eq!(same_side_result.truth, ConditionTruth::Satisfied);
        assert_eq!(same_side_result.continuity_hash, live.continuity_hash);

        let gap = crypto_input_set(
            &artifact,
            evaluated_at,
            &frozen_source,
            &[(13, dec!(103), 'd')],
            1,
            same_side_result.fold_state.clone(),
        );
        let after_gap = evaluate_entry_condition(&artifact, &gap).expect("gap reset");
        assert_eq!(after_gap.truth, ConditionTruth::Unsatisfied);
        assert!(!after_gap.fold_state.crypto[0].latched);
        assert_ne!(after_gap.continuity_hash, same_side_result.continuity_hash);

        let correction = crypto_input_set(
            &artifact,
            evaluated_at,
            &frozen_source,
            &[(11, dec!(99), 'e')],
            0,
            live.fold_state,
        );
        let corrected =
            evaluate_entry_condition(&artifact, &correction).expect("same-sequence correction");
        assert_eq!(corrected.truth, ConditionTruth::Unsatisfied);
        assert!(!corrected.fold_state.crypto[0].latched);
    }

    #[test]
    fn weather_maximum_yes_and_bounded_no_follow_corrected_whole_degree_extreme() {
        let evaluated_at = timestamp();
        let local_date = NaiveDate::from_ymd_opt(2026, 7, 13).expect("local date");
        let frozen_source = source(DomainSourceId::aviation_weather(), "AVIATION_WEATHER:KJFK");
        let yes_artifact = market_event_artifact(
            MarketEventCondition::WeatherDailyTemperatureEnteredBand(
                WeatherDailyTemperatureEnteredBand {
                    source: frozen_source.clone(),
                    station: "KJFK".to_owned(),
                    local_date,
                    temperature_statistic: WeatherTemperatureStatistic::Maximum,
                    unit: TemperatureUnit::Fahrenheit,
                    band: TemperatureBand {
                        lower_inclusive: Some(dec!(80)),
                        upper_inclusive: Some(dec!(81)),
                    },
                    proxy_methodology_hash: hash('f'),
                    max_input_age_ms: 2_000,
                },
            ),
            frozen_source.clone(),
        );
        let weather = WeatherDailyTemperatureInput {
            source: frozen_source.clone(),
            station: "KJFK".to_owned(),
            local_date,
            temperature_statistic: WeatherTemperatureStatistic::Maximum,
            current_extreme: TemperatureCelsius::new(dec!(26.7)),
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

        yes_input.weather[0].current_extreme = TemperatureCelsius::new(dec!(25));
        yes_input.weather[0].revision = 1;
        yes_input.weather[0].report_hash = hash('2');
        let corrected = evaluate_entry_condition(&yes_artifact, &yes_input)
            .expect("weather correction evaluation");
        assert_eq!(corrected.truth, ConditionTruth::Unsatisfied);

        let no_artifact = market_event_artifact(
            MarketEventCondition::WeatherDailyTemperatureCrossedTerminalBound(
                WeatherDailyTemperatureCrossedTerminalBound {
                    source: frozen_source,
                    station: "KJFK".to_owned(),
                    local_date,
                    temperature_statistic: WeatherTemperatureStatistic::Maximum,
                    unit: TemperatureUnit::Fahrenheit,
                    terminal_bound: dec!(80),
                    proxy_methodology_hash: hash('f'),
                    max_input_age_ms: 2_000,
                },
            ),
            weather.source.clone(),
        );
        let mut no_input = input(&no_artifact, evaluated_at);
        no_input.weather.push(WeatherDailyTemperatureInput {
            current_extreme: TemperatureCelsius::new(dec!(27.3)),
            ..weather
        });
        let exceeded =
            evaluate_entry_condition(&no_artifact, &no_input).expect("bounded NO evaluation");
        assert_eq!(exceeded.truth, ConditionTruth::Satisfied);
    }

    #[test]
    fn weather_minimum_crosses_lower_terminal_bound_and_rejects_maximum_input() {
        let evaluated_at = timestamp();
        let local_date = NaiveDate::from_ymd_opt(2026, 7, 13).expect("local date");
        let frozen_source = source(DomainSourceId::aviation_weather(), "AVIATION_WEATHER:KJFK");
        let artifact = market_event_artifact(
            MarketEventCondition::WeatherDailyTemperatureCrossedTerminalBound(
                WeatherDailyTemperatureCrossedTerminalBound {
                    source: frozen_source.clone(),
                    station: "KJFK".to_owned(),
                    local_date,
                    temperature_statistic: WeatherTemperatureStatistic::Minimum,
                    unit: TemperatureUnit::Fahrenheit,
                    terminal_bound: dec!(60),
                    proxy_methodology_hash: hash('f'),
                    max_input_age_ms: 2_000,
                },
            ),
            frozen_source.clone(),
        );
        let mut inputs = input(&artifact, evaluated_at);
        inputs.weather.push(WeatherDailyTemperatureInput {
            source: frozen_source,
            station: "KJFK".to_owned(),
            local_date,
            temperature_statistic: WeatherTemperatureStatistic::Minimum,
            current_extreme: TemperatureCelsius::new(dec!(15)),
            observation_time: evaluated_at,
            available_at: evaluated_at,
            revision: 0,
            day_closed: false,
            report_hash: hash('5'),
            gap_generation: 0,
            source_healthy: true,
        });
        let crossed = evaluate_entry_condition(&artifact, &inputs).expect("minimum evaluation");
        assert_eq!(crossed.truth, ConditionTruth::Satisfied);

        inputs.weather[0].temperature_statistic = WeatherTemperatureStatistic::Maximum;
        let wrong_statistic =
            evaluate_entry_condition(&artifact, &inputs).expect("statistic isolation");
        assert_eq!(
            wrong_statistic.truth,
            ConditionTruth::Unavailable(ConditionUnavailableReason::InputMissing)
        );
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
                    temperature_statistic: WeatherTemperatureStatistic::Maximum,
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
        inputs.weather.push(WeatherDailyTemperatureInput {
            source: frozen_source,
            station: "KJFK".to_owned(),
            local_date,
            temperature_statistic: WeatherTemperatureStatistic::Maximum,
            current_extreme: TemperatureCelsius::new(dec!(29.4)),
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
