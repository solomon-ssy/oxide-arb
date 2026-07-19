//! Complete stateful `ClickHouse` projection for accepted feature vectors.
//!
//! Every [`FeatureCell`](crate::features::FeatureCell) becomes one
//! [`QuantFeatureEventRow`], including `Missing` and `NotApplicable`. The
//! projection is allowed only after Postgres persistence and strictly verifies
//! that the returned [`FeatureVectorInfo`] still describes the in-memory
//! vector. This prevents a reordered or mismatched persistence response from
//! producing evidence under the wrong `feature_vector_id`.

use std::collections::HashSet;

use quant_pivot_error::{QuantError, QuantResult, research::ResearchError};
use quant_pivot_models::{
    clickhouse::QuantFeatureEventRow,
    domain::{DecisionBoundary, DecisionSource, FeatureVectorInfo},
    enums::{
        clickhouse::{ChFeatureCellState, ChFeatureSourceKind, ChFeatureValueKind},
        feature::EvidenceSourceKind,
    },
    types::{DecisionPolicySnapshotId, FeatureVectorId, MarketId, TokenId},
};
use serde::Serialize;

use crate::{
    features::{
        FeatureCell, FeatureCellState, FeatureName, FeatureSchema, FeatureSpec, FeatureStaleness,
        FeatureValue, FeatureValueKind, FeatureVector, NullReason,
    },
    hashing::ResearchHasher,
};

/// Canonical audit fields hashed into `audit_fingerprint`.
///
/// `ingestion_time` is deliberately excluded: it is transport metadata and may
/// differ when an identical fact is retried. Every field that determines the
/// decision evidence is included, including explicit `None` values.
#[derive(Serialize)]
struct FeatureEventAudit<'a> {
    event_time: i64,
    feature_vector_id: &'a FeatureVectorId,
    decision_policy_snapshot_id: &'a DecisionPolicySnapshotId,
    decision_at: i64,
    knowledge_cutoff: i64,
    per_source_cutoffs_json: &'a str,
    market_id: &'a MarketId,
    token_id: &'a Option<TokenId>,
    feature_schema_version: u32,
    feature_schema_hash: &'a str,
    feature_hash: &'a str,
    decision_capture_hash: &'a str,
    feature_name: &'a str,
    cell_state: i8,
    raw_value: &'a Option<String>,
    value_kind: i8,
    source_kind: &'a str,
    evidence_source_kind: Option<&'a str>,
    evidence_reference: &'a Option<String>,
    evidence_effective_at: Option<i64>,
    evidence_available_at: Option<i64>,
    reason: &'a Option<String>,
    staleness_ms: Option<u64>,
    data_quality: &'a str,
}

struct FeatureProjectionContext<'a> {
    persisted: &'a FeatureVectorInfo,
    decision_policy_snapshot_id: &'a DecisionPolicySnapshotId,
    schema_version: u32,
    schema_hash: &'a str,
    per_source_cutoffs_json: &'a str,
    decision_at: i64,
    knowledge_cutoff: i64,
    data_quality: &'a str,
    ingestion_time: i64,
}

struct ProjectedCell {
    cell_state: ChFeatureCellState,
    raw_value: Option<String>,
    value_kind: ChFeatureValueKind,
    source_kind: ChFeatureSourceKind,
    evidence_source_kind: Option<ChFeatureSourceKind>,
    evidence_reference: Option<String>,
    evidence_effective_at: Option<i64>,
    evidence_available_at: Option<i64>,
    reason: Option<String>,
    staleness_ms: Option<u64>,
}

/// Project every cell of one accepted, persisted feature vector.
///
/// Source availability remains `None` only when the resolver explicitly marks
/// it unknown. The projection never substitutes an adjacent clock.
///
/// # Errors
///
/// Rejects schema/vector/boundary/persistence mismatches, malformed cell state,
/// future evidence timestamps, stale-age inconsistencies, unknown feature
/// names, value-kind mismatches, and canonical serialization failures.
pub fn feature_events(
    vector: &FeatureVector,
    persisted: &FeatureVectorInfo,
    boundary: &DecisionBoundary,
    decision_policy_snapshot_id: &DecisionPolicySnapshotId,
    schema: &FeatureSchema,
    ingestion_time: i64,
) -> QuantResult<Vec<QuantFeatureEventRow>> {
    validate_boundary(vector, boundary)?;
    validate_persisted_binding(vector, persisted, boundary)?;
    if persisted.feature_vector_id.as_uuid().is_nil() {
        return Err(determinism(
            "persisted feature vector id must not be nil".to_owned(),
        ));
    }
    if decision_policy_snapshot_id.as_uuid().is_nil() {
        return Err(determinism(
            "runtime config version id must not be nil".to_owned(),
        ));
    }
    if vector.generic_schema_version != schema.version() {
        return Err(determinism(format!(
            "feature vector schema version {} does not match active schema {}",
            vector.generic_schema_version,
            schema.version()
        )));
    }

    let schema_version = u32::try_from(vector.generic_schema_version.get()).map_err(|_| {
        ResearchError::Serialization {
            detail: format!(
                "feature schema version {} does not fit u32",
                vector.generic_schema_version
            ),
        }
    })?;
    let schema_hash = ResearchHasher::feature_schema(schema)?;
    let per_source_cutoffs_json =
        serde_json::to_string(boundary.per_source_cutoffs()).map_err(|error| {
            ResearchError::Serialization {
                detail: format!("serialize canonical per-source cutoffs: {error}"),
            }
        })?;
    let decision_at = boundary.decision_at().timestamp_millis();
    let knowledge_cutoff = boundary.knowledge_cutoff().timestamp_millis();
    let data_quality = vector.data_quality.as_str();
    let context = FeatureProjectionContext {
        persisted,
        decision_policy_snapshot_id,
        schema_version,
        schema_hash: schema_hash.as_str(),
        per_source_cutoffs_json: &per_source_cutoffs_json,
        decision_at,
        knowledge_cutoff,
        data_quality,
        ingestion_time,
    };
    let mut names = HashSet::with_capacity(vector.value_count());
    let mut rows = Vec::with_capacity(vector.value_count());

    for (name, cell) in vector.iter_cells() {
        if !names.insert(name) {
            return Err(determinism(format!(
                "feature vector contains duplicate cell `{name}` across slices"
            )));
        }
        let spec = schema.by_name(name).ok_or_else(|| {
            determinism(format!(
                "feature vector cell `{name}` is absent from active schema"
            ))
        })?;
        rows.push(project_event(name, cell, spec, boundary, &context)?);
    }

    Ok(rows)
}

fn project_event(
    name: &FeatureName,
    cell: &FeatureCell,
    spec: &FeatureSpec,
    boundary: &DecisionBoundary,
    context: &FeatureProjectionContext<'_>,
) -> QuantResult<QuantFeatureEventRow> {
    validate_cell(
        name.as_str(),
        cell,
        spec.value_kind,
        spec.source_requirement.evidence_kind(),
        boundary,
    )?;
    let projected = project_cell(cell, spec);
    let decision_capture_hash = context
        .persisted
        .decision_capture_hash
        .as_ref()
        .ok_or_else(|| {
            determinism(format!(
                "feature vector {} has no v10 decision-capture hash",
                context.persisted.feature_vector_id
            ))
        })?;
    let audit = FeatureEventAudit {
        event_time: context.decision_at,
        feature_vector_id: &context.persisted.feature_vector_id,
        decision_policy_snapshot_id: context.decision_policy_snapshot_id,
        decision_at: context.decision_at,
        knowledge_cutoff: context.knowledge_cutoff,
        per_source_cutoffs_json: context.per_source_cutoffs_json,
        market_id: &context.persisted.market_id,
        token_id: &context.persisted.token_id,
        feature_schema_version: context.schema_version,
        feature_schema_hash: context.schema_hash,
        feature_hash: context.persisted.feature_hash.as_str(),
        decision_capture_hash: decision_capture_hash.as_str(),
        feature_name: name.as_str(),
        cell_state: projected.cell_state as i8,
        raw_value: &projected.raw_value,
        value_kind: spec.value_kind.as_i8(),
        source_kind: spec.source_requirement.evidence_kind().as_wire(),
        evidence_source_kind: cell
            .evidence
            .as_ref()
            .map(|evidence| evidence.source_kind.as_wire()),
        evidence_reference: &projected.evidence_reference,
        evidence_effective_at: projected.evidence_effective_at,
        evidence_available_at: projected.evidence_available_at,
        reason: &projected.reason,
        staleness_ms: projected.staleness_ms,
        data_quality: context.data_quality,
    };
    let audit_fingerprint = ResearchHasher::canonical(&audit)?.as_str().to_owned();
    Ok(QuantFeatureEventRow {
        event_time: context.decision_at,
        feature_vector_id: context.persisted.feature_vector_id.clone(),
        decision_policy_snapshot_id: context.decision_policy_snapshot_id.clone(),
        decision_at: context.decision_at,
        knowledge_cutoff: context.knowledge_cutoff,
        per_source_cutoffs_json: context.per_source_cutoffs_json.to_owned(),
        market_id: context.persisted.market_id.clone(),
        token_id: context.persisted.token_id.clone(),
        feature_schema_version: context.schema_version,
        feature_schema_hash: context.schema_hash.to_owned(),
        feature_hash: context.persisted.feature_hash.as_str().to_owned(),
        decision_capture_hash: decision_capture_hash.as_str().to_owned(),
        feature_name: name.as_str().to_owned(),
        cell_state: projected.cell_state,
        raw_value: projected.raw_value,
        value_kind: projected.value_kind,
        source_kind: projected.source_kind,
        evidence_source_kind: projected.evidence_source_kind,
        evidence_reference: projected.evidence_reference,
        evidence_effective_at: projected.evidence_effective_at,
        evidence_available_at: projected.evidence_available_at,
        reason: projected.reason,
        staleness_ms: projected.staleness_ms,
        data_quality: context.data_quality.to_owned(),
        audit_fingerprint,
        ingestion_time: context.ingestion_time,
    })
}

fn project_cell(cell: &FeatureCell, spec: &FeatureSpec) -> ProjectedCell {
    ProjectedCell {
        cell_state: project_cell_state(cell.state),
        raw_value: cell.value.as_ref().map(raw_value_text),
        value_kind: spec.value_kind.into(),
        source_kind: spec.source_requirement.evidence_kind().into(),
        evidence_source_kind: cell
            .evidence
            .as_ref()
            .map(|evidence| evidence.source_kind.into()),
        evidence_reference: cell
            .evidence
            .as_ref()
            .map(|evidence| evidence.reference.clone()),
        evidence_effective_at: cell
            .evidence
            .as_ref()
            .map(|evidence| evidence.effective_at.timestamp_millis()),
        evidence_available_at: cell
            .evidence
            .as_ref()
            .and_then(|evidence| evidence.available_at)
            .map(|available_at| available_at.timestamp_millis()),
        reason: cell.reason.map(null_reason_label).map(str::to_owned),
        staleness_ms: match cell.staleness {
            FeatureStaleness::Known { age_ms } => Some(age_ms),
            FeatureStaleness::Unknown => None,
        },
    }
}

fn validate_boundary(vector: &FeatureVector, boundary: &DecisionBoundary) -> QuantResult<()> {
    boundary.validate()?;
    if vector.decision_at != boundary.decision_at() {
        return Err(determinism(format!(
            "feature vector decision time {} does not match boundary {}",
            vector.decision_at,
            boundary.decision_at()
        )));
    }
    Ok(())
}

fn validate_persisted_binding(
    vector: &FeatureVector,
    persisted: &FeatureVectorInfo,
    boundary: &DecisionBoundary,
) -> QuantResult<()> {
    let expected = vector.try_to_new(boundary)?;
    let mismatch = if persisted.market_id != expected.market_id {
        Some("market_id")
    } else if persisted.token_id != expected.token_id {
        Some("token_id")
    } else if persisted.decision_at != expected.decision_at {
        Some("decision_at")
    } else if persisted.decision_boundary != expected.decision_boundary {
        Some("decision_boundary")
    } else if persisted.feature_schema_version != expected.feature_schema_version {
        Some("feature_schema_version")
    } else if persisted.feature_hash != expected.feature_hash {
        Some("feature_hash")
    } else if persisted.data_quality != expected.data_quality {
        Some("data_quality")
    } else if persisted.staleness_ms != expected.staleness_ms {
        Some("staleness_ms")
    } else if persisted.payload != expected.payload {
        Some("payload")
    } else if persisted.source_refs != expected.source_refs {
        Some("source_refs")
    } else {
        None
    };
    if let Some(field) = mismatch {
        return Err(determinism(format!(
            "persisted feature vector {} does not match in-memory vector field `{field}`",
            persisted.feature_vector_id
        )));
    }
    Ok(())
}

fn validate_cell(
    name: &str,
    cell: &FeatureCell,
    expected_kind: FeatureValueKind,
    expected_source: EvidenceSourceKind,
    boundary: &DecisionBoundary,
) -> QuantResult<()> {
    let valid_shape = match cell.state {
        FeatureCellState::Observed => cell.value.is_some() && cell.reason.is_none(),
        FeatureCellState::Substituted => cell.value.is_some() && cell.reason.is_some(),
        FeatureCellState::Missing => cell.value.is_none() && cell.reason.is_some(),
        FeatureCellState::NotApplicable => {
            cell.value.is_none()
                && cell.reason == Some(NullReason::NotApplicable)
                && cell.evidence.is_none()
                && cell.staleness == FeatureStaleness::Unknown
        }
    };
    if !valid_shape {
        return Err(determinism(format!(
            "feature cell `{name}` violates its {:?} state invariant",
            cell.state
        )));
    }
    if let Some(value) = &cell.value
        && value.kind() != expected_kind
    {
        return Err(determinism(format!(
            "feature cell `{name}` has value kind {:?}, expected {expected_kind:?}",
            value.kind()
        )));
    }
    if let Some(evidence) = &cell.evidence {
        if evidence.source_kind != expected_source {
            return Err(determinism(format!(
                "feature cell `{name}` evidence source {:?} does not match schema source {expected_source:?}",
                evidence.source_kind
            )));
        }
        if evidence.reference.trim().is_empty() {
            return Err(determinism(format!(
                "feature cell `{name}` has an empty evidence reference"
            )));
        }
        if evidence.effective_at > boundary.decision_at() {
            return Err(determinism(format!(
                "feature cell `{name}` evidence time {} is after decision time {}",
                evidence.effective_at,
                boundary.decision_at()
            )));
        }
        if evidence
            .available_at
            .is_some_and(|available_at| available_at > boundary.decision_at())
        {
            return Err(determinism(format!(
                "feature cell `{name}` evidence availability {:?} is after decision time {}",
                evidence.available_at,
                boundary.decision_at()
            )));
        }
        let source_cutoff = decision_source(expected_source).map_or_else(
            || boundary.knowledge_cutoff(),
            |source| boundary.cutoff_for(source),
        );
        if evidence.effective_at > source_cutoff {
            return Err(determinism(format!(
                "feature cell `{name}` evidence time {} is after {:?} cutoff {source_cutoff}",
                evidence.effective_at, expected_source
            )));
        }
        let expected_age = u64::try_from(
            boundary
                .decision_at()
                .signed_duration_since(evidence.effective_at)
                .num_milliseconds(),
        )
        .map_err(|error| {
            determinism(format!(
                "feature cell `{name}` evidence age does not fit u64: {error}"
            ))
        })?;
        if cell.staleness
            != (FeatureStaleness::Known {
                age_ms: expected_age,
            })
        {
            return Err(determinism(format!(
                "feature cell `{name}` staleness {:?} does not match evidence age {expected_age}ms",
                cell.staleness
            )));
        }
    } else if matches!(cell.staleness, FeatureStaleness::Known { .. }) {
        return Err(determinism(format!(
            "feature cell `{name}` has known staleness without source evidence"
        )));
    }
    Ok(())
}

const fn decision_source(evidence_source: EvidenceSourceKind) -> Option<DecisionSource> {
    match evidence_source {
        EvidenceSourceKind::Book => Some(DecisionSource::Book),
        EvidenceSourceKind::GammaMetadata => Some(DecisionSource::Catalog),
        EvidenceSourceKind::ClickHouseFact => Some(DecisionSource::Microstructure),
        EvidenceSourceKind::TradeTape => Some(DecisionSource::TradeTape),
        EvidenceSourceKind::DomainExternal => Some(DecisionSource::DomainCrypto),
        EvidenceSourceKind::Linkage => Some(DecisionSource::Linkage),
        EvidenceSourceKind::Derived => None,
    }
}

fn raw_value_text(value: &FeatureValue) -> String {
    match value {
        FeatureValue::Decimal(value) | FeatureValue::Bps(value) => value.to_string(),
        FeatureValue::Probability(value) => value.inner().to_string(),
        FeatureValue::Usd(value) => value.inner().to_string(),
        FeatureValue::Count(value) => value.to_string(),
        FeatureValue::Bool(value) => value.to_string(),
        FeatureValue::Category(value) => value.as_str().to_owned(),
    }
}

const fn project_cell_state(state: FeatureCellState) -> ChFeatureCellState {
    match state {
        FeatureCellState::Observed => ChFeatureCellState::Observed,
        FeatureCellState::Substituted => ChFeatureCellState::Substituted,
        FeatureCellState::Missing => ChFeatureCellState::Missing,
        FeatureCellState::NotApplicable => ChFeatureCellState::NotApplicable,
    }
}

const fn null_reason_label(reason: NullReason) -> &'static str {
    match reason {
        NullReason::SourceUnavailable => "source_unavailable",
        NullReason::StaleBeyondPolicy => "stale_beyond_policy",
        NullReason::OutOfValidRange => "out_of_valid_range",
        NullReason::InsufficientHistory => "insufficient_history",
        NullReason::NotApplicable => "not_applicable",
        NullReason::LegBookMissing => "leg_book_missing",
        NullReason::TradeTapeUnavailable => "trade_tape_unavailable",
        NullReason::InsufficientTradeTape => "insufficient_trade_tape",
        NullReason::InsufficientRoleCoverage => "insufficient_role_coverage",
        NullReason::DomainSourceUnavailable => "domain_source_unavailable",
        NullReason::LinkageUnresolved => "linkage_unresolved",
    }
}

fn determinism(detail: String) -> QuantError {
    ResearchError::Determinism { detail }.into()
}
