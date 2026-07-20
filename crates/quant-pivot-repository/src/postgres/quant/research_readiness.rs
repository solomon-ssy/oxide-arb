//! Postgres append-only operational-readiness evidence index.

use chrono::{DateTime, Utc};
use quant_pivot_error::storage::{StorageError, entity};
use quant_pivot_models::{
    domain::{NewResearchReadinessEvidence, ResearchReadinessEvidenceInfo},
    entities::quant_research_readiness_evidence,
    enums::quant::ResearchReadinessEvidenceKind,
    hashing::CanonicalDigest,
    types::{ContentHash, ResearchReadinessEvidenceId, ResearchReadinessEvidencePayload},
};
use sea_orm::{
    ColumnTrait, DatabaseConnection, EntityTrait, IntoActiveModel, QueryFilter, QueryOrder,
    sea_query::OnConflict,
};

use crate::{
    postgres::primitives,
    traits::{ResearchReadinessEvidenceRepository, ShadowLatencyObservation},
};

pub struct PgResearchReadinessEvidenceRepository {
    db: DatabaseConnection,
}

impl PgResearchReadinessEvidenceRepository {
    pub const fn new(db: DatabaseConnection) -> Self {
        Self { db }
    }
}

#[async_trait::async_trait]
impl ResearchReadinessEvidenceRepository for PgResearchReadinessEvidenceRepository {
    async fn append(
        &self,
        evidence: NewResearchReadinessEvidence,
    ) -> Result<ResearchReadinessEvidenceInfo, StorageError> {
        validate_new(&evidence)?;
        let kind = evidence.kind;
        let scope_hash = evidence.scope_hash.clone();
        let payload_hash = evidence.payload_hash.clone();
        quant_research_readiness_evidence::Entity::insert(evidence.into_active_model())
            .on_conflict(
                OnConflict::columns([
                    quant_research_readiness_evidence::Column::Kind,
                    quant_research_readiness_evidence::Column::ScopeHash,
                    quant_research_readiness_evidence::Column::PayloadHash,
                ])
                .do_nothing()
                .to_owned(),
            )
            .exec_without_returning(&self.db)
            .await
            .map_err(StorageError::from)?;
        quant_research_readiness_evidence::Entity::find()
            .filter(quant_research_readiness_evidence::Column::Kind.eq(kind))
            .filter(quant_research_readiness_evidence::Column::ScopeHash.eq(scope_hash))
            .filter(quant_research_readiness_evidence::Column::PayloadHash.eq(payload_hash))
            .one(&self.db)
            .await
            .map_err(StorageError::from)?
            .map(Into::into)
            .ok_or_else(|| {
                StorageError::invariant_violation(
                    Some(entity::QUANT_RESEARCH_READINESS_EVIDENCE),
                    "readiness evidence was not observable after append",
                )
            })
    }

    async fn latest_valid(
        &self,
        kind: ResearchReadinessEvidenceKind,
        scope_hash: &ContentHash,
        as_of: DateTime<Utc>,
    ) -> Result<Option<ResearchReadinessEvidenceInfo>, StorageError> {
        quant_research_readiness_evidence::Entity::find()
            .filter(quant_research_readiness_evidence::Column::Kind.eq(kind))
            .filter(quant_research_readiness_evidence::Column::ScopeHash.eq(scope_hash.clone()))
            .filter(quant_research_readiness_evidence::Column::ObservedAt.lte(as_of))
            .filter(quant_research_readiness_evidence::Column::ExpiresAt.gt(as_of))
            .order_by_desc(quant_research_readiness_evidence::Column::ObservedAt)
            .one(&self.db)
            .await
            .map_err(StorageError::from)
            .map(|row| row.map(Into::into))
    }

    async fn find_by_id(
        &self,
        evidence_id: &ResearchReadinessEvidenceId,
    ) -> Result<Option<ResearchReadinessEvidenceInfo>, StorageError> {
        quant_research_readiness_evidence::Entity::find_by_id(evidence_id.clone())
            .one(&self.db)
            .await
            .map_err(StorageError::from)
            .map(|row| row.map(Into::into))
    }

    async fn observe_shadow_latency(
        &self,
        window_start: DateTime<Utc>,
        window_end: DateTime<Utc>,
    ) -> Result<ShadowLatencyObservation, StorageError> {
        if window_start >= window_end {
            return Err(StorageError::invariant_violation(
                Some(entity::QUANT_RESEARCH_READINESS_EVIDENCE),
                "shadow latency observation window must be half-open and non-empty",
            ));
        }
        let row = primitives::shadow_latency_aggregate(&self.db, window_start, window_end).await?;
        Ok(ShadowLatencyObservation {
            decision_prepared_count: checked_count(
                "decision_prepared_count",
                row.decision_prepared_count,
            )?,
            decision_prepared_p95_ms: checked_percentile(
                "decision_prepared_p95_ms",
                row.decision_prepared_p95_ms,
            )?,
            endpoint_rtt_count: checked_count("endpoint_rtt_count", row.endpoint_rtt_count)?,
            endpoint_rtt_p95_ms: checked_percentile(
                "endpoint_rtt_p95_ms",
                row.endpoint_rtt_p95_ms,
            )?,
            market_delay_count: checked_count("market_delay_count", row.market_delay_count)?,
            market_delay_p95_ms: checked_percentile(
                "market_delay_p95_ms",
                row.market_delay_p95_ms,
            )?,
        })
    }
}

fn checked_count(column: &str, value: i64) -> Result<u64, StorageError> {
    u64::try_from(value).map_err(|error| {
        StorageError::invariant_violation(
            Some(entity::QUANT_RESEARCH_READINESS_EVIDENCE),
            format!("shadow latency {column} is invalid: {error}"),
        )
    })
}

fn checked_percentile(column: &str, value: Option<i64>) -> Result<Option<u64>, StorageError> {
    value
        .map(|value| {
            u64::try_from(value).map_err(|error| {
                StorageError::invariant_violation(
                    Some(entity::QUANT_RESEARCH_READINESS_EVIDENCE),
                    format!("shadow latency {column} is invalid: {error}"),
                )
            })
        })
        .transpose()
}

fn validate_new(evidence: &NewResearchReadinessEvidence) -> Result<(), StorageError> {
    if evidence.window_start >= evidence.window_end
        || evidence.window_end > evidence.observed_at
        || evidence.observed_at >= evidence.expires_at
        || evidence.artifact_version.trim().is_empty()
        || evidence.attestation_key_id.trim().is_empty()
    {
        return Err(StorageError::invariant_violation(
            Some(entity::QUANT_RESEARCH_READINESS_EVIDENCE),
            "readiness evidence time, artifact, or attestation contract is invalid",
        ));
    }
    let payload_bytes =
        CanonicalDigest::canonical_json_bytes(&evidence.payload_json).map_err(|error| {
            StorageError::invariant_violation(
                Some(entity::QUANT_RESEARCH_READINESS_EVIDENCE),
                format!("readiness evidence payload cannot be serialized: {error}"),
            )
        })?;
    let actual_payload_hash = ContentHash::parse(CanonicalDigest::prefixed_bytes(&payload_bytes))
        .map_err(|error| {
        StorageError::invariant_violation(
            Some(entity::QUANT_RESEARCH_READINESS_EVIDENCE),
            format!("readiness evidence payload hash is invalid: {error}"),
        )
    })?;
    if actual_payload_hash != evidence.payload_hash
        || !payload_matches_kind(evidence.kind, &evidence.payload_json)
    {
        return Err(StorageError::invariant_violation(
            Some(entity::QUANT_RESEARCH_READINESS_EVIDENCE),
            "readiness evidence payload hash or kind binding is invalid",
        ));
    }
    Ok(())
}

const fn payload_matches_kind(
    kind: ResearchReadinessEvidenceKind,
    payload: &ResearchReadinessEvidencePayload,
) -> bool {
    matches!(
        (kind, payload),
        (
            ResearchReadinessEvidenceKind::RetentionRunway,
            ResearchReadinessEvidencePayload::RetentionRunway(_)
        ) | (
            ResearchReadinessEvidenceKind::ShadowLatencyProfile,
            ResearchReadinessEvidencePayload::ShadowLatencyProfile(_)
        )
    )
}
