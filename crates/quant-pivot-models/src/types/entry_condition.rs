//! Versioned, content-addressed entry-condition contracts.
//!
//! Conditions are deterministic data. They cannot contain network calls,
//! expressions, scripts, SQL, `JSONPath`, or dynamically selected sources.

use chrono::{DateTime, Duration, Utc};
use rust_decimal::Decimal;
use sea_orm::FromJsonQueryResult;
use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::{
    domain::PriceComparator,
    enums::quant::{OutcomeSide, PriceComparison},
    hashing::CanonicalDigest,
    types::{
        ContentHash, DomainInstrumentKey, DomainSourceId, EntryConditionArtifactId,
        FactorDefinitionId, MarketId, MarketLinkageId, MarketSelectionId, ModelVersionId, Price,
        RecommendationId, RuntimeConfigVersionId, TemperatureBand, TemperatureUnit, TokenId, Usd,
        WeatherTemperatureStatistic,
    },
};

pub const ENTRY_CONDITION_SCHEMA_VERSION: u32 = 1;
pub const ENTRY_CONDITION_EVALUATOR_VERSION: u32 = 1;
pub const ENTRY_CONDITION_MIN_GROUP_CHILDREN: usize = 2;
pub const ENTRY_CONDITION_MAX_GROUP_CHILDREN: usize = 8;
pub const ENTRY_CONDITION_MAX_DEPTH: usize = 4;
pub const ENTRY_CONDITION_MAX_NODES: usize = 32;
pub const ENTRY_CONDITION_MAX_CANDIDATES: usize = 16;
pub const ENTRY_CONDITION_INPUT_CHANNEL: &str = "quant_entry_condition_input";

/// Recommendation trade-plan reference to an immutable condition artifact.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, FromJsonQueryResult)]
#[serde(rename_all = "snake_case", tag = "kind")]
pub enum EntryConditionPlan {
    /// No conditional wait; admission may proceed immediately.
    Immediate,
    /// Evaluate the exact content-addressed artifact.
    Conditional {
        artifact_id: EntryConditionArtifactId,
        content_hash: ContentHash,
    },
}

/// Exact external source binding consumed by a condition artifact.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct EntryConditionSourceBinding {
    pub source_id: DomainSourceId,
    pub instrument_key: DomainInstrumentKey,
    pub binding_hash: ContentHash,
}

/// Exact factor revision consumed by a factor leaf.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct EntryConditionFactorBinding {
    pub definition_id: FactorDefinitionId,
    pub definition_hash: ContentHash,
}

/// Immutable recommendation and PIT provenance boundary for an artifact.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct EntryConditionBinding {
    pub recommendation_id: RecommendationId,
    pub market_id: MarketId,
    pub token_id: TokenId,
    pub outcome_side: OutcomeSide,
    pub market_linkage_id: Option<MarketLinkageId>,
    pub market_linkage_hash: Option<ContentHash>,
    pub catalog_snapshot_id: MarketSelectionId,
    pub catalog_snapshot_hash: ContentHash,
    pub model_version_id: ModelVersionId,
    pub runtime_config_version_id: RuntimeConfigVersionId,
    pub factor_bindings: Vec<EntryConditionFactorBinding>,
    pub source_bindings: Vec<EntryConditionSourceBinding>,
}

/// Root-only continuous-confirmation policy.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ConfirmationPolicy {
    pub required_continuous_ms: u64,
    pub max_observation_gap_ms: u64,
}

impl ConfirmationPolicy {
    #[must_use]
    pub const fn none() -> Self {
        Self {
            required_continuous_ms: 0,
            max_observation_gap_ms: 0,
        }
    }
}

/// Immutable V1 condition artifact.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, FromJsonQueryResult)]
pub struct EntryConditionArtifactV1 {
    pub schema_version: u32,
    pub evaluator_version: u32,
    pub binding: EntryConditionBinding,
    pub confirmation: ConfirmationPolicy,
    pub root: EntryConditionV1,
}

impl EntryConditionArtifactV1 {
    /// Canonicalize the AST and all set-like binding vectors before hashing.
    pub fn canonicalize(mut self) -> Result<Self, EntryConditionValidationError> {
        if self.schema_version != ENTRY_CONDITION_SCHEMA_VERSION {
            return Err(EntryConditionValidationError::UnsupportedSchemaVersion(
                self.schema_version,
            ));
        }
        if self.evaluator_version != ENTRY_CONDITION_EVALUATOR_VERSION {
            return Err(EntryConditionValidationError::UnsupportedEvaluatorVersion(
                self.evaluator_version,
            ));
        }
        self.binding.factor_bindings.sort_by(|left, right| {
            left.definition_id
                .to_string()
                .cmp(&right.definition_id.to_string())
                .then_with(|| {
                    left.definition_hash
                        .as_str()
                        .cmp(right.definition_hash.as_str())
                })
        });
        reject_adjacent_duplicates(
            &self.binding.factor_bindings,
            EntryConditionValidationError::DuplicateFactorBinding,
        )?;
        self.binding.source_bindings.sort();
        reject_adjacent_duplicates(
            &self.binding.source_bindings,
            EntryConditionValidationError::DuplicateSourceBinding,
        )?;
        validate_confirmation(self.confirmation)?;
        let mut node_count = 0;
        self.root = self.root.canonicalize(1, &mut node_count)?;
        if node_count > ENTRY_CONDITION_MAX_NODES {
            return Err(EntryConditionValidationError::TooManyNodes(node_count));
        }
        validate_root_bindings(&self.binding, &self.root)?;
        Ok(self)
    }

    /// Compute the canonical content address. Callers cannot hash an
    /// uncanonicalized AST accidentally.
    pub fn canonical_content_hash(&self) -> Result<ContentHash, EntryConditionValidationError> {
        let canonical = self.clone().canonicalize()?;
        CanonicalDigest::content_hash_json(&canonical)
            .map_err(|error| EntryConditionValidationError::Hash(error.to_string()))
    }
}

const fn validate_confirmation(
    confirmation: ConfirmationPolicy,
) -> Result<(), EntryConditionValidationError> {
    let valid = match confirmation.required_continuous_ms {
        0 => confirmation.max_observation_gap_ms == 0,
        required => {
            confirmation.max_observation_gap_ms > 0
                && confirmation.max_observation_gap_ms <= required
        }
    };
    if valid {
        Ok(())
    } else {
        Err(EntryConditionValidationError::InvalidConfirmation)
    }
}

fn validate_root_bindings(
    binding: &EntryConditionBinding,
    root: &EntryConditionV1,
) -> Result<(), EntryConditionValidationError> {
    if binding.market_linkage_id.is_some() != binding.market_linkage_hash.is_some() {
        return Err(EntryConditionValidationError::BindingSetMismatch(
            "market linkage id and hash must be present together",
        ));
    }
    let mut factors = Vec::new();
    let mut sources = Vec::new();
    validate_leaf_bindings(root, binding, &mut factors, &mut sources)?;
    factors.sort_by(|left, right| {
        left.definition_id
            .to_string()
            .cmp(&right.definition_id.to_string())
            .then_with(|| {
                left.definition_hash
                    .as_str()
                    .cmp(right.definition_hash.as_str())
            })
    });
    factors.dedup();
    sources.sort();
    sources.dedup();
    if factors != binding.factor_bindings {
        return Err(EntryConditionValidationError::BindingSetMismatch(
            "factor bindings do not exactly match factor leaves",
        ));
    }
    if sources != binding.source_bindings {
        return Err(EntryConditionValidationError::BindingSetMismatch(
            "source bindings do not exactly match market-event leaves",
        ));
    }
    if !sources.is_empty() && binding.market_linkage_id.is_none() {
        return Err(EntryConditionValidationError::BindingSetMismatch(
            "market-event leaves require a frozen market linkage",
        ));
    }
    Ok(())
}

fn validate_leaf_bindings(
    node: &EntryConditionV1,
    binding: &EntryConditionBinding,
    factors: &mut Vec<EntryConditionFactorBinding>,
    sources: &mut Vec<EntryConditionSourceBinding>,
) -> Result<(), EntryConditionValidationError> {
    match node {
        EntryConditionV1::Price(condition) => {
            if condition.token_id != binding.token_id || condition.max_input_age_ms == 0 {
                return Err(EntryConditionValidationError::InvalidLeaf(
                    "price leaf must bind the recommendation token and positive freshness",
                ));
            }
        }
        EntryConditionV1::Clock(condition) => {
            let expected = condition
                .anchor_at
                .checked_add_signed(Duration::milliseconds(condition.offset_ms));
            if expected != Some(condition.deadline_at) {
                return Err(EntryConditionValidationError::InvalidLeaf(
                    "clock deadline does not match anchor plus offset",
                ));
            }
        }
        EntryConditionV1::Factor(condition) => {
            if condition.model_version_id != binding.model_version_id
                || FactorDefinitionId::from_definition_hash(&condition.definition_hash)
                    != condition.definition_id
                || condition.minimum_confidence < Decimal::ZERO
                || condition.minimum_confidence > Decimal::ONE
                || condition.max_input_age_ms == 0
            {
                return Err(EntryConditionValidationError::InvalidLeaf(
                    "factor leaf has invalid definition, model, confidence, or freshness",
                ));
            }
            factors.push(EntryConditionFactorBinding {
                definition_id: condition.definition_id.clone(),
                definition_hash: condition.definition_hash.clone(),
            });
        }
        EntryConditionV1::MarketEvent { event } => {
            let (source, valid) = match event {
                MarketEventCondition::CryptoSubjectPredicateEntered(condition) => {
                    let subject_valid = match condition.comparator {
                        PriceComparator::UpVsReference => {
                            condition.reference_price.is_some() && condition.strike.is_none()
                        }
                        PriceComparator::GreaterThan
                        | PriceComparator::GreaterThanOrEqual
                        | PriceComparator::LessThan
                        | PriceComparator::LessThanOrEqual => {
                            condition.strike.is_some() && condition.reference_price.is_none()
                        }
                        PriceComparator::Between { hi, .. } => {
                            condition.strike.is_some_and(|strike| strike <= hi)
                                && condition.reference_price.is_none()
                        }
                    };
                    (
                        &condition.source,
                        subject_valid && condition.max_input_age_ms > 0,
                    )
                }
                MarketEventCondition::WeatherDailyTemperatureEnteredBand(condition) => (
                    &condition.source,
                    !condition.station.is_empty()
                        && condition.band.is_valid()
                        && condition.max_input_age_ms > 0,
                ),
                MarketEventCondition::WeatherDailyTemperatureCrossedTerminalBound(condition) => (
                    &condition.source,
                    !condition.station.is_empty() && condition.max_input_age_ms > 0,
                ),
                MarketEventCondition::WeatherObservationDayClosedOutsideBand(condition) => (
                    &condition.source,
                    !condition.station.is_empty() && condition.band.is_valid(),
                ),
            };
            if !valid {
                return Err(EntryConditionValidationError::InvalidLeaf(
                    "market-event leaf has inconsistent subject or freshness",
                ));
            }
            sources.push(source.clone());
        }
        EntryConditionV1::All { children } | EntryConditionV1::Any { children } => {
            for child in children {
                validate_leaf_bindings(child, binding, factors, sources)?;
            }
        }
    }
    Ok(())
}

fn reject_adjacent_duplicates<T: PartialEq>(
    values: &[T],
    error: EntryConditionValidationError,
) -> Result<(), EntryConditionValidationError> {
    if values.windows(2).any(|pair| pair[0] == pair[1]) {
        Err(error)
    } else {
        Ok(())
    }
}

/// Deterministic typed condition AST.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "kind")]
pub enum EntryConditionV1 {
    Price(PriceCondition),
    Clock(ClockCondition),
    Factor(FactorCondition),
    MarketEvent { event: MarketEventCondition },
    All { children: Vec<Self> },
    Any { children: Vec<Self> },
}

impl EntryConditionV1 {
    /// Canonicalize a research template tree before candidate hashing.
    pub fn canonicalized(self) -> Result<Self, EntryConditionValidationError> {
        let mut node_count = 0;
        let canonical = self.canonicalize(1, &mut node_count)?;
        if node_count > ENTRY_CONDITION_MAX_NODES {
            return Err(EntryConditionValidationError::TooManyNodes(node_count));
        }
        Ok(canonical)
    }

    fn canonicalize(
        self,
        depth: usize,
        node_count: &mut usize,
    ) -> Result<Self, EntryConditionValidationError> {
        if depth > ENTRY_CONDITION_MAX_DEPTH {
            return Err(EntryConditionValidationError::TooDeep(depth));
        }
        *node_count += 1;
        match self {
            Self::All { children } => {
                canonicalize_group(children, depth, node_count, GroupKind::All)
            }
            Self::Any { children } => {
                canonicalize_group(children, depth, node_count, GroupKind::Any)
            }
            leaf => Ok(leaf),
        }
    }

    /// Stable preorder projection used by evaluation traces and UI node ids.
    pub fn preorder_nodes(&self) -> Result<Vec<EntryConditionNode>, EntryConditionValidationError> {
        let canonical = self.clone().canonicalize(1, &mut 0)?;
        let mut nodes = Vec::new();
        canonical.append_preorder(&mut nodes)?;
        Ok(nodes)
    }

    fn append_preorder(
        &self,
        nodes: &mut Vec<EntryConditionNode>,
    ) -> Result<(), EntryConditionValidationError> {
        let node_id = u16::try_from(nodes.len())
            .map_err(|_| EntryConditionValidationError::TooManyNodes(nodes.len()))?;
        let subtree_hash = CanonicalDigest::content_hash_json(self)
            .map_err(|error| EntryConditionValidationError::Hash(error.to_string()))?;
        nodes.push(EntryConditionNode {
            node_id,
            subtree_hash,
        });
        if let Self::All { children } | Self::Any { children } = self {
            for child in children {
                child.append_preorder(nodes)?;
            }
        }
        Ok(())
    }
}

#[derive(Clone, Copy)]
enum GroupKind {
    All,
    Any,
}

fn canonicalize_group(
    children: Vec<EntryConditionV1>,
    depth: usize,
    node_count: &mut usize,
    kind: GroupKind,
) -> Result<EntryConditionV1, EntryConditionValidationError> {
    let mut canonical = Vec::new();
    for child in children {
        let child = child.canonicalize(depth + 1, node_count)?;
        match (kind, child) {
            (GroupKind::All, EntryConditionV1::All { children })
            | (GroupKind::Any, EntryConditionV1::Any { children }) => {
                // Flattening removes one already-counted group node.
                *node_count -= 1;
                canonical.extend(children);
            }
            (_, child) => canonical.push(child),
        }
    }
    if !(ENTRY_CONDITION_MIN_GROUP_CHILDREN..=ENTRY_CONDITION_MAX_GROUP_CHILDREN)
        .contains(&canonical.len())
    {
        return Err(EntryConditionValidationError::InvalidGroupSize(
            canonical.len(),
        ));
    }
    let mut hashed = canonical
        .into_iter()
        .map(|child| {
            CanonicalDigest::content_hash_json(&child)
                .map(|hash| (hash, child))
                .map_err(|error| EntryConditionValidationError::Hash(error.to_string()))
        })
        .collect::<Result<Vec<_>, _>>()?;
    hashed.sort_by(|left, right| left.0.as_str().cmp(right.0.as_str()));
    if hashed.windows(2).any(|pair| pair[0].0 == pair[1].0) {
        return Err(EntryConditionValidationError::DuplicateSubtree);
    }
    let children = hashed.into_iter().map(|(_, child)| child).collect();
    Ok(match kind {
        GroupKind::All => EntryConditionV1::All { children },
        GroupKind::Any => EntryConditionV1::Any { children },
    })
}

/// Stable node identity within a canonical artifact.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct EntryConditionNode {
    pub node_id: u16,
    pub subtree_hash: ContentHash,
}

/// Executable-side token price predicate.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PriceCondition {
    pub token_id: TokenId,
    pub comparison: PriceComparison,
    pub threshold: Price,
    /// Freshness of this leaf's observation, distinct from final admission book age.
    pub max_input_age_ms: u64,
}

/// Allowed clock anchors. Report construction materializes them to `deadline_at`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ClockAnchor {
    RecommendationDecision,
    MarketStart,
    MarketEnd,
}

/// Materialized clock predicate with frozen anchor evidence.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ClockCondition {
    pub anchor: ClockAnchor,
    pub anchor_at: DateTime<Utc>,
    pub offset_ms: i64,
    pub deadline_at: DateTime<Utc>,
}

/// Whether a factor threshold uses the raw or normalized PIT measure.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FactorMeasure {
    Raw,
    Normalized,
}

/// Latest persisted PIT factor-snapshot predicate.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct FactorCondition {
    pub definition_id: FactorDefinitionId,
    pub definition_hash: ContentHash,
    pub model_version_id: ModelVersionId,
    pub measure: FactorMeasure,
    pub comparison: PriceComparison,
    pub threshold: Decimal,
    pub minimum_confidence: Decimal,
    pub max_input_age_ms: u64,
}

/// Typed business event predicates. Event type and source are never strings.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "kind")]
pub enum MarketEventCondition {
    CryptoSubjectPredicateEntered(CryptoSubjectPredicateEntered),
    WeatherDailyTemperatureEnteredBand(WeatherDailyTemperatureEnteredBand),
    WeatherDailyTemperatureCrossedTerminalBound(WeatherDailyTemperatureCrossedTerminalBound),
    WeatherObservationDayClosedOutsideBand(WeatherObservationDayClosedOutsideBand),
}

/// Crypto source price crossed from the opposite outcome into the recommendation.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct CryptoSubjectPredicateEntered {
    pub source: EntryConditionSourceBinding,
    pub comparator: PriceComparator,
    pub strike: Option<Usd>,
    pub reference_price: Option<Usd>,
    pub recommended_outcome: OutcomeSide,
    pub max_input_age_ms: u64,
}

/// Weather YES predicate: the current corrected daily extreme is inside the band.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct WeatherDailyTemperatureEnteredBand {
    pub source: EntryConditionSourceBinding,
    pub station: String,
    pub local_date: chrono::NaiveDate,
    pub temperature_statistic: WeatherTemperatureStatistic,
    pub unit: TemperatureUnit,
    pub band: TemperatureBand,
    pub proxy_methodology_hash: ContentHash,
    pub max_input_age_ms: u64,
}

/// Weather bounded-NO predicate: the monotonic daily extreme crossed the bound
/// that makes this outcome impossible. Maximum crosses above an upper bound;
/// minimum crosses below a lower bound.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct WeatherDailyTemperatureCrossedTerminalBound {
    pub source: EntryConditionSourceBinding,
    pub station: String,
    pub local_date: chrono::NaiveDate,
    pub temperature_statistic: WeatherTemperatureStatistic,
    pub unit: TemperatureUnit,
    pub terminal_bound: Decimal,
    pub proxy_methodology_hash: ContentHash,
    pub max_input_age_ms: u64,
}

/// Weather open-upper NO predicate; valid only after NOAA observation-day close.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct WeatherObservationDayClosedOutsideBand {
    pub source: EntryConditionSourceBinding,
    pub station: String,
    pub local_date: chrono::NaiveDate,
    pub temperature_statistic: WeatherTemperatureStatistic,
    pub unit: TemperatureUnit,
    pub band: TemperatureBand,
    pub proxy_methodology_hash: ContentHash,
}

/// Three-state leaf/composite truth. Missing input never becomes false.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, FromJsonQueryResult)]
#[serde(rename_all = "snake_case", tag = "kind", content = "reason")]
pub enum ConditionTruth {
    Satisfied,
    Unsatisfied,
    Unavailable(ConditionUnavailableReason),
}

/// Durable ordered-fact fold state for one condition instance.
///
/// Only semantic event state is persisted here. Ordinary same-side market
/// ticks are represented by the cursor fields but do not advance the condition
/// revision or change the continuity epoch.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize, FromJsonQueryResult)]
pub struct EntryConditionFoldState {
    pub crypto: Vec<CryptoEnteredFoldState>,
}

/// Persistent edge-triggered latch for one canonical crypto leaf.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct CryptoEnteredFoldState {
    pub node_id: u16,
    pub source: EntryConditionSourceBinding,
    pub last_outcome: Option<OutcomeSide>,
    pub latched: bool,
    pub last_source_sequence: Option<u64>,
    pub last_report_hash: Option<ContentHash>,
    pub last_event_at: Option<DateTime<Utc>>,
    pub last_available_at: Option<DateTime<Utc>>,
    pub gap_generation: u64,
    pub discontinuity_epoch: u64,
    pub triggering_report_hash: Option<ContentHash>,
    pub triggering_at: Option<DateTime<Utc>>,
}

/// Typed fail-closed reason attached to an unavailable evaluation.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "kind")]
pub enum ConditionUnavailableReason {
    SourceNotConfigured {
        source_id: DomainSourceId,
    },
    SourceUnhealthy {
        source_id: DomainSourceId,
    },
    SourceGap {
        source_id: DomainSourceId,
        generation: u64,
    },
    InputMissing,
    InputStale,
    BindingDrift,
    ArtifactHashMismatch,
    FactorDefinitionMismatch,
    CatalogSnapshotMismatch,
    MarketLinkageMismatch,
    ClockSkew,
}

/// A validation failure before an artifact can enter the WORM ledger.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum EntryConditionValidationError {
    #[error("unsupported entry-condition schema version {0}")]
    UnsupportedSchemaVersion(u32),
    #[error("unsupported entry-condition evaluator version {0}")]
    UnsupportedEvaluatorVersion(u32),
    #[error("condition group must contain 2..=8 canonical children, got {0}")]
    InvalidGroupSize(usize),
    #[error("condition depth exceeds 4, got {0}")]
    TooDeep(usize),
    #[error("condition node count exceeds 32, got {0}")]
    TooManyNodes(usize),
    #[error("duplicate canonical condition subtree")]
    DuplicateSubtree,
    #[error("duplicate factor binding")]
    DuplicateFactorBinding,
    #[error("duplicate source binding")]
    DuplicateSourceBinding,
    #[error("invalid root confirmation policy")]
    InvalidConfirmation,
    #[error("entry-condition leaf is invalid: {0}")]
    InvalidLeaf(&'static str),
    #[error("entry-condition binding mismatch: {0}")]
    BindingSetMismatch(&'static str),
    #[error("condition canonical hash failed: {0}")]
    Hash(String),
}

#[cfg(test)]
mod tests {
    use chrono::{TimeZone, Utc};
    use rust_decimal_macros::dec;

    use super::{
        ClockAnchor, ClockCondition, EntryConditionV1, EntryConditionValidationError,
        PriceCondition,
    };
    use crate::{
        enums::quant::PriceComparison,
        types::{Price, TokenId},
    };

    fn price(token: &str, threshold: rust_decimal::Decimal) -> EntryConditionV1 {
        EntryConditionV1::Price(PriceCondition {
            token_id: TokenId::new(token),
            comparison: PriceComparison::AtOrAbove,
            threshold: Price::new(threshold),
            max_input_age_ms: 500,
        })
    }

    fn clock(offset_ms: i64) -> EntryConditionV1 {
        let anchor_at = Utc.with_ymd_and_hms(2026, 7, 13, 0, 0, 0).unwrap();
        EntryConditionV1::Clock(ClockCondition {
            anchor: ClockAnchor::RecommendationDecision,
            anchor_at,
            offset_ms,
            deadline_at: anchor_at + chrono::Duration::milliseconds(offset_ms),
        })
    }

    #[test]
    fn permutations_have_identical_canonical_tree_and_hash() {
        let left = EntryConditionV1::All {
            children: vec![price("1", dec!(0.55)), clock(1_000)],
        }
        .canonicalize(1, &mut 0)
        .expect("left");
        let right = EntryConditionV1::All {
            children: vec![clock(1_000), price("1", dec!(0.55))],
        }
        .canonicalize(1, &mut 0)
        .expect("right");
        assert_eq!(left, right);
    }

    #[test]
    fn same_kind_groups_flatten_and_duplicates_are_rejected() {
        let tree = EntryConditionV1::All {
            children: vec![
                price("1", dec!(0.55)),
                EntryConditionV1::All {
                    children: vec![clock(1_000), clock(2_000)],
                },
            ],
        }
        .canonicalize(1, &mut 0)
        .expect("flatten");
        assert!(matches!(tree, EntryConditionV1::All { children } if children.len() == 3));

        let duplicate = EntryConditionV1::Any {
            children: vec![clock(1_000), clock(1_000)],
        }
        .canonicalize(1, &mut 0);
        assert_eq!(
            duplicate,
            Err(EntryConditionValidationError::DuplicateSubtree)
        );
    }
}
