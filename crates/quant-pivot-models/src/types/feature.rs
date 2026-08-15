//! Canonical feature-plane values shared by compute and typed persistence.

use std::collections::BTreeMap;

use chrono::{DateTime, Utc};
use quant_pivot_error::{QuantResult, research::ResearchError};
use rust_decimal::Decimal;
use sea_orm::FromJsonQueryResult;
use serde::{Deserialize, Deserializer, Serialize, de::Error};

use crate::{
    domain::data_plane::DecisionBoundary,
    enums::{
        catalog::CatalogTimestampQuality,
        common::MarketCategory,
        domain::DomainFamily,
        feature::{EvidenceSourceKind, FeatureValueKind},
        quant::{DataQualityStatus, FeatureParityEventStatus, FeatureParityStage},
    },
    types::{
        BookSnapshotRef, CatalogEventChangeId, CatalogMarketChangeId, CatalogSyncBatchId,
        ContentHash, EventId, FeatureVectorId, MarketContext, MarketId, Probability,
        RecommendationIdentity, SchemaVersion, SelectorHashEvidence, TokenId, TrainingExampleId,
        Usd, stable_name::FeatureName,
    },
};

/// Raw finalized-execution source state consumed while one feature vector was built.
///
/// Runtime parity replays availability from the frozen deploy toggle and cursor
/// rows instead of copying the already-computed `FeatureCell` state. Historical
/// materialization has a distinct provenance because it reads a sealed source
/// slice rather than a live ingest worker.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "snake_case", tag = "kind")]
pub enum FinalizedExecutionEvidence {
    /// The active feature schema did not consume finalized executions.
    NotRequired,
    /// A sealed historical source slice supplied the PIT-bounded facts.
    Materialized { available_by: DateTime<Utc> },
    /// Exact runtime accepted-frontier state. A boolean alone is insufficient:
    /// the evidence binds both the chain frontier and its N+12 availability.
    Runtime {
        history_enabled: bool,
        accepted_through_block: Option<u64>,
        accepted_through_at: Option<DateTime<Utc>>,
    },
}

impl FinalizedExecutionEvidence {
    #[must_use]
    pub const fn not_required() -> Self {
        Self::NotRequired
    }

    #[must_use]
    pub const fn materialized(available_by: DateTime<Utc>) -> Self {
        Self::Materialized { available_by }
    }

    #[must_use]
    pub const fn runtime(
        history_enabled: bool,
        accepted_through_block: Option<u64>,
        accepted_through_at: Option<DateTime<Utc>>,
    ) -> Self {
        Self::Runtime {
            history_enabled,
            accepted_through_block,
            accepted_through_at,
        }
    }

    #[must_use]
    pub const fn runtime_parts(&self) -> Option<(bool, Option<u64>, Option<DateTime<Utc>>)> {
        match self {
            Self::Runtime {
                history_enabled,
                accepted_through_block,
                accepted_through_at,
            } => Some((
                *history_enabled,
                *accepted_through_block,
                *accepted_through_at,
            )),
            Self::NotRequired | Self::Materialized { .. } => None,
        }
    }
}

/// Why a feature value is absent. Missing values are never silently zero.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum NullReason {
    SourceUnavailable,
    StaleBeyondPolicy,
    OutOfValidRange,
    InsufficientHistory,
    NotApplicable,
    LegBookMissing,
    FinalizedExecutionUnavailable,
    InsufficientExecutionHistory,
    InsufficientRoleCoverage,
    DomainSourceUnavailable,
    LinkageUnresolved,
}

/// A provenance reference tying a feature value back to its evidence.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EvidenceSourceRef {
    pub source_kind: EvidenceSourceKind,
    pub reference: String,
    pub effective_at: DateTime<Utc>,
    pub available_at: Option<DateTime<Utc>>,
}

/// A strongly-typed feature value.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(
    deny_unknown_fields,
    tag = "kind",
    content = "value",
    rename_all = "snake_case"
)]
pub enum FeatureValue {
    Decimal(Decimal),
    Probability(Probability),
    Bps(Decimal),
    Usd(Usd),
    Count(u64),
    Bool(bool),
    Category(MarketCategory),
}

impl FeatureValue {
    #[must_use]
    pub const fn kind(&self) -> FeatureValueKind {
        match self {
            Self::Decimal(_) => FeatureValueKind::Decimal,
            Self::Probability(_) => FeatureValueKind::Probability,
            Self::Bps(_) => FeatureValueKind::Bps,
            Self::Usd(_) => FeatureValueKind::Usd,
            Self::Count(_) => FeatureValueKind::Count,
            Self::Bool(_) => FeatureValueKind::Bool,
            Self::Category(_) => FeatureValueKind::Category,
        }
    }

    pub fn to_fact_decimal(&self) -> QuantResult<Decimal> {
        Ok(match self {
            Self::Decimal(value) | Self::Bps(value) => *value,
            Self::Probability(value) => value.inner(),
            Self::Usd(value) => value.inner(),
            Self::Count(value) => Decimal::from(*value),
            Self::Bool(flag) => Decimal::from(u8::from(*flag)),
            Self::Category(category) => {
                let index = u64::try_from(category.table_index()).map_err(|_| {
                    ResearchError::Determinism {
                        detail: format!("category index does not fit u64: {category}"),
                    }
                })?;
                Decimal::from(index)
            }
        })
    }
}

/// Semantic state of one feature cell.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FeatureCellState {
    Observed,
    Substituted,
    Missing,
    NotApplicable,
}

/// Per-cell source freshness. Unknown is distinct from a fresh age of zero.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "snake_case", tag = "state")]
pub enum FeatureStaleness {
    Known { age_ms: u64 },
    Unknown,
}

/// A complete feature value, state, reason, provenance and freshness record.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FeatureCell {
    pub state: FeatureCellState,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub value: Option<FeatureValue>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reason: Option<NullReason>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub evidence: Option<EvidenceSourceRef>,
    pub staleness: FeatureStaleness,
}

impl FeatureCell {
    #[must_use]
    pub const fn observed(
        value: FeatureValue,
        evidence: Option<EvidenceSourceRef>,
        staleness: FeatureStaleness,
    ) -> Self {
        Self {
            state: FeatureCellState::Observed,
            value: Some(value),
            reason: None,
            evidence,
            staleness,
        }
    }

    #[must_use]
    pub const fn substituted(
        value: FeatureValue,
        reason: NullReason,
        evidence: Option<EvidenceSourceRef>,
        staleness: FeatureStaleness,
    ) -> Self {
        Self {
            state: FeatureCellState::Substituted,
            value: Some(value),
            reason: Some(reason),
            evidence,
            staleness,
        }
    }

    #[must_use]
    pub const fn missing(
        reason: NullReason,
        evidence: Option<EvidenceSourceRef>,
        staleness: FeatureStaleness,
    ) -> Self {
        Self {
            state: FeatureCellState::Missing,
            value: None,
            reason: Some(reason),
            evidence,
            staleness,
        }
    }

    #[must_use]
    pub const fn not_applicable(reason: NullReason) -> Self {
        Self {
            state: FeatureCellState::NotApplicable,
            value: None,
            reason: Some(reason),
            evidence: None,
            staleness: FeatureStaleness::Unknown,
        }
    }

    #[must_use]
    pub const fn value(&self) -> Option<&FeatureValue> {
        self.value.as_ref()
    }

    /// Validate the cross-field state invariant after JSON decoding.
    pub const fn validate(&self) -> Result<(), &'static str> {
        match (self.state, self.value.is_some(), self.reason.is_some()) {
            (FeatureCellState::Observed, true, false)
            | (FeatureCellState::Substituted, true, true)
            | (FeatureCellState::Missing | FeatureCellState::NotApplicable, false, true) => Ok(()),
            _ => Err("feature-cell state/value/reason invariant is invalid"),
        }
    }
}

/// Category-mapped external-vertical feature slice.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DomainFeatureSlice {
    pub family: DomainFamily,
    pub schema_version: SchemaVersion,
    pub values: BTreeMap<FeatureName, FeatureCell>,
}

/// Atomic JSONB body of one persisted feature vector.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, FromJsonQueryResult)]
#[serde(deny_unknown_fields)]
pub struct FeatureVectorPayload {
    pub generic: BTreeMap<FeatureName, FeatureCell>,
    pub domain: Option<DomainFeatureSlice>,
}

impl<'de> Deserialize<'de> for FeatureVectorPayload {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct Wire {
            generic: BTreeMap<FeatureName, FeatureCell>,
            domain: Option<DomainFeatureSlice>,
        }

        let wire = Wire::deserialize(deserializer)?;
        let payload = Self {
            generic: wire.generic,
            domain: wire.domain,
        };
        payload.validate().map_err(Error::custom)?;
        Ok(payload)
    }
}

impl FeatureVectorPayload {
    /// Validate every feature cell after the `SeaORM` JSON conversion.
    pub fn validate(&self) -> Result<(), &'static str> {
        self.generic
            .values()
            .chain(self.domain.iter().flat_map(|domain| domain.values.values()))
            .try_for_each(FeatureCell::validate)
    }
}

/// Typed, whole-value provenance references for a feature vector.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize, FromJsonQueryResult)]
#[serde(transparent)]
pub struct FeatureSourceRefs(pub Vec<EvidenceSourceRef>);

/// Exact immutable catalog revisions used for one decision.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CatalogDecisionRef {
    pub catalog_sync_batch_id: CatalogSyncBatchId,
    pub market_change_id: CatalogMarketChangeId,
    pub event_change_id: CatalogEventChangeId,
    pub market_content_hash: ContentHash,
    pub event_content_hash: ContentHash,
    pub membership_hash: ContentHash,
    pub market_effective_at: DateTime<Utc>,
    pub market_available_at: DateTime<Utc>,
    pub event_effective_at: DateTime<Utc>,
    pub event_available_at: DateTime<Utc>,
    pub market_timestamp_quality: CatalogTimestampQuality,
    pub event_timestamp_quality: CatalogTimestampQuality,
}

/// Source snapshot identity committed before feature computation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DecisionSnapshotEvidence {
    pub boundary: DecisionBoundary,
    pub market_id: MarketId,
    pub event_id: EventId,
    pub token_id: TokenId,
    pub catalog: CatalogDecisionRef,
    pub book_snapshot_ref: BookSnapshotRef,
    pub book_effective_at: DateTime<Utc>,
    pub book_available_at: DateTime<Utc>,
    /// Exact selection member admitted before feature/model computation.
    ///
    /// Feedback datasets reuse this immutable serving input instead of
    /// reconstructing selection state from a mutable catalog projection.
    pub selection: SelectionMemberEvidence,
}

/// Immutable selection member consumed by one serving decision.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SelectionMemberEvidence {
    pub market_id: MarketId,
    pub event_id: EventId,
    pub category: MarketCategory,
    pub primary_token_id: TokenId,
    pub secondary_token_id: Option<TokenId>,
    pub liquidity_usd: Option<Usd>,
    pub volume_24h_usd: Option<Usd>,
    pub source_refs: Vec<EvidenceSourceRef>,
}

/// Full business capture consumed by report composition and parity replay.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, FromJsonQueryResult)]
#[serde(deny_unknown_fields)]
pub struct DecisionCaptureEvidence {
    pub snapshot: DecisionSnapshotEvidence,
    pub finalized_execution_evidence: FinalizedExecutionEvidence,
    pub identity: RecommendationIdentity,
    pub market_context: MarketContext,
    pub data_quality: DataQualityStatus,
    pub liquidity_score: Probability,
}

/// Online/replay selector component commitments carried by parity diagnostics.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SelectorParityEvidence {
    pub online: SelectorHashEvidence,
    pub replay: SelectorHashEvidence,
}

/// Stage-specific, closed diagnostics emitted by the durable parity replay.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "snake_case", tag = "kind")]
pub enum FeatureParityDetailSource {
    FactorClassicalBypass,
    FactorCounts {
        online_count: u64,
        replay_count: u64,
    },
    Selection {
        online_count: u64,
        replay_count: u64,
        selector_evidence: Box<SelectorParityEvidence>,
        replay_excluded_count: u64,
    },
    DataQuality {
        online_count: u64,
        replay_count: u64,
        online_admitted_count: u64,
        replay_admitted_count: u64,
    },
    Snapshot {
        feature_vector_id: FeatureVectorId,
        online_catalog_change_id: CatalogMarketChangeId,
        replay_catalog_change_id: CatalogMarketChangeId,
        online_book_ref: BookSnapshotRef,
        replay_book_ref: BookSnapshotRef,
    },
    Capture {
        feature_vector_id: FeatureVectorId,
        online_capture_hash: ContentHash,
        replay_capture_hash: ContentHash,
    },
    FeatureCell {
        feature_vector_id: FeatureVectorId,
    },
    ModelInput {
        raw_input_name: FeatureName,
        feature_vector_id: FeatureVectorId,
    },
    FrozenModelCommitment {
        example_id: TrainingExampleId,
        decision_boundary: DecisionBoundary,
        feature_contract_hash: ContentHash,
        transform_hash: ContentHash,
        dataset_hash: ContentHash,
        training_input_hash: ContentHash,
    },
    Prediction {
        candidate_count: u64,
    },
}

impl FeatureParityDetailSource {
    const fn matches_stage(&self, stage: FeatureParityStage) -> bool {
        matches!(
            (self, stage),
            (
                Self::FactorClassicalBypass | Self::FactorCounts { .. },
                FeatureParityStage::Factor
            ) | (Self::Selection { .. }, FeatureParityStage::Selection)
                | (Self::DataQuality { .. }, FeatureParityStage::DataQuality)
                | (Self::Snapshot { .. }, FeatureParityStage::Snapshot)
                | (Self::Capture { .. }, FeatureParityStage::Capture)
                | (Self::FeatureCell { .. }, FeatureParityStage::FeatureCell)
                | (
                    Self::ModelInput { .. } | Self::FrozenModelCommitment { .. },
                    FeatureParityStage::ModelInput
                )
                | (Self::Prediction { .. }, FeatureParityStage::Prediction)
        )
    }
}

/// Canonical ClickHouse/API parity diagnostics envelope.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "snake_case", tag = "kind")]
pub enum FeatureParityDetail {
    Compared {
        sampling_key: String,
        source: Box<FeatureParityDetailSource>,
    },
    PendingMaterialization {
        sampling_key: String,
        required_watermark: DateTime<Utc>,
        observed_watermark: Option<DateTime<Utc>>,
    },
}

impl FeatureParityDetail {
    /// Enforce agreement between the row discriminators and diagnostics shape.
    pub fn validate_for(
        &self,
        stage: FeatureParityStage,
        status: FeatureParityEventStatus,
    ) -> Result<(), &'static str> {
        let valid = match self {
            Self::Compared {
                sampling_key,
                source,
            } => {
                status != FeatureParityEventStatus::PendingMaterialization
                    && !sampling_key.trim().is_empty()
                    && source.matches_stage(stage)
            }
            Self::PendingMaterialization { sampling_key, .. } => {
                status == FeatureParityEventStatus::PendingMaterialization
                    && !sampling_key.trim().is_empty()
            }
        };
        valid
            .then_some(())
            .ok_or("feature parity detail disagrees with stage or status")
    }
}

#[cfg(test)]
mod tests {
    use chrono::Utc;

    use super::{
        FeatureCell, FeatureCellState, FeatureParityDetail, FeatureParityDetailSource,
        FinalizedExecutionEvidence,
    };
    use crate::enums::quant::{FeatureParityEventStatus, FeatureParityStage};

    #[test]
    fn feature_cell_rejects_corruption() {
        let cell: FeatureCell = serde_json::from_value(serde_json::json!({
            "state": "observed",
            "reason": "source_unavailable",
            "staleness": { "state": "unknown" }
        }))
        .expect("shape still decodes");
        assert_eq!(cell.state, FeatureCellState::Observed);
        assert!(cell.validate().is_err());
    }

    #[test]
    fn parity_detail_rejects_drift() {
        let detail = FeatureParityDetail::Compared {
            sampling_key: "report/market".to_owned(),
            source: Box::new(FeatureParityDetailSource::Prediction { candidate_count: 3 }),
        };
        assert!(
            detail
                .validate_for(
                    FeatureParityStage::Prediction,
                    FeatureParityEventStatus::Matched,
                )
                .is_ok()
        );
        assert!(
            detail
                .validate_for(
                    FeatureParityStage::FeatureCell,
                    FeatureParityEventStatus::Matched,
                )
                .is_err()
        );
        assert!(
            detail
                .validate_for(
                    FeatureParityStage::Prediction,
                    FeatureParityEventStatus::PendingMaterialization,
                )
                .is_err()
        );
    }

    #[test]
    fn runtime_evidence_is_canonical() {
        let accepted_at = Utc::now();
        let evidence = FinalizedExecutionEvidence::runtime(true, Some(42), Some(accepted_at));
        assert_eq!(
            evidence.runtime_parts(),
            Some((true, Some(42), Some(accepted_at)))
        );
    }
}
