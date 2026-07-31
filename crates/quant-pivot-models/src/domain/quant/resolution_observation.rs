//! Durable resolution-source inbox and canonical projection contracts.

use chrono::{DateTime, Utc};
use quant_pivot_error::hashing::CanonicalDigestError;
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::{
    domain::data_plane::DomainSourceCursorInfo,
    entities::{quant_resolution_observation_inbox, quant_resolution_observation_projection},
    enums::quant::ResolutionProjectionStatus,
    hashing::CanonicalDigest,
    types::{
        ArtifactUri, ContentHash, DomainInstrumentKey, DomainSourceId, EvmAddress, EvmBlockHash,
        EvmTransactionHash, EvmUint256, MarketId, PayoutRatio, ResolutionObservationId, WorkerId,
    },
};

/// Immutable source observation written before its source cursor can advance.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NewResolutionObservationInbox {
    pub source_checkpoint_hash: ContentHash,
    pub source_id: DomainSourceId,
    pub instrument_key: DomainInstrumentKey,
    pub market_id: MarketId,
    pub denominator: EvmUint256,
    pub yes_numerator: EvmUint256,
    pub no_numerator: EvmUint256,
    pub yes_payout_ratio: PayoutRatio,
    pub no_payout_ratio: PayoutRatio,
    pub oracle: EvmAddress,
    pub question_id: String,
    pub transaction_hash: EvmTransactionHash,
    pub block_number: u64,
    pub block_hash: EvmBlockHash,
    pub log_index: u64,
    pub resolved_at: DateTime<Utc>,
    pub raw_payload_hash: ContentHash,
    pub raw_uri: ArtifactUri,
    pub provider_revision: EvmBlockHash,
}

impl NewResolutionObservationInbox {
    /// Compute the source payload digest independently of database lifecycle fields.
    pub fn expected_raw_payload_hash(
        &self,
    ) -> Result<ContentHash, ResolutionObservationContractError> {
        Ok(CanonicalDigest::content_hash_typed(
            "quant-pivot/resolution-observation",
            1,
            &ResolutionObservationHashInput::from(self),
        )?)
    }

    /// Validate the immutable source envelope before opening a transaction.
    pub fn validate(&self) -> Result<(), ResolutionObservationContractError> {
        if self.source_id != DomainSourceId::polymarket_ctf_resolution()
            || self.instrument_key != DomainInstrumentKey::polymarket_ctf_resolution()
        {
            return Err(ResolutionObservationContractError::InvalidSourceIdentity);
        }
        if self.market_id.as_str().trim().is_empty()
            || self.question_id.trim().is_empty()
            || self.denominator.as_str() == "0"
            || self.block_number == 0
            || self.resolved_at.timestamp_millis() <= 0
        {
            return Err(ResolutionObservationContractError::InvalidSourceContent);
        }
        if self.yes_payout_ratio.inner() + self.no_payout_ratio.inner() != Decimal::ONE {
            return Err(ResolutionObservationContractError::InvalidPayoutVector);
        }
        if self.provider_revision != self.block_hash || self.raw_uri.scheme() != "polygon" {
            return Err(ResolutionObservationContractError::InvalidSourceLineage);
        }
        let expected = self.expected_raw_payload_hash()?;
        if expected != self.raw_payload_hash {
            return Err(ResolutionObservationContractError::RawPayloadHashMismatch {
                expected,
                actual: self.raw_payload_hash,
            });
        }
        Ok(())
    }
}

/// Immutable inbox row with database-authoritative availability time.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, sea_orm::DerivePartialModel)]
#[sea_orm(entity = "crate::entities::quant_resolution_observation_inbox::Entity")]
pub struct ResolutionObservationInboxInfo {
    pub resolution_observation_id: ResolutionObservationId,
    pub source_checkpoint_hash: ContentHash,
    pub source_id: DomainSourceId,
    pub instrument_key: DomainInstrumentKey,
    pub market_id: MarketId,
    pub denominator: EvmUint256,
    pub yes_numerator: EvmUint256,
    pub no_numerator: EvmUint256,
    pub yes_payout_ratio: PayoutRatio,
    pub no_payout_ratio: PayoutRatio,
    pub oracle: EvmAddress,
    pub question_id: String,
    pub transaction_hash: EvmTransactionHash,
    pub block_number: i64,
    pub block_hash: EvmBlockHash,
    pub log_index: i64,
    pub resolved_at: DateTime<Utc>,
    pub raw_payload_hash: ContentHash,
    pub raw_uri: ArtifactUri,
    pub provider_revision: EvmBlockHash,
    pub available_at: DateTime<Utc>,
    pub created_at: DateTime<Utc>,
}

info_from_model!(
    ResolutionObservationInboxInfo,
    quant_resolution_observation_inbox::Model,
    {
        resolution_observation_id,
        source_checkpoint_hash,
        source_id,
        instrument_key,
        market_id,
        denominator,
        yes_numerator,
        no_numerator,
        yes_payout_ratio,
        no_payout_ratio,
        oracle,
        question_id,
        transaction_hash,
        block_number,
        block_hash,
        log_index,
        resolved_at,
        raw_payload_hash,
        raw_uri,
        provider_revision,
        available_at,
        created_at,
    }
);

impl ResolutionObservationInboxInfo {
    /// Recover and validate the immutable producer contract from a stored row.
    pub fn validate(&self) -> Result<(), ResolutionObservationContractError> {
        let block_number = u64::try_from(self.block_number)
            .map_err(|_| ResolutionObservationContractError::InvalidSourceContent)?;
        let log_index = u64::try_from(self.log_index)
            .map_err(|_| ResolutionObservationContractError::InvalidSourceContent)?;
        if self.available_at < self.resolved_at || self.created_at != self.available_at {
            return Err(ResolutionObservationContractError::InvalidAvailability);
        }
        NewResolutionObservationInbox {
            source_checkpoint_hash: self.source_checkpoint_hash,
            source_id: self.source_id.clone(),
            instrument_key: self.instrument_key.clone(),
            market_id: self.market_id.clone(),
            denominator: self.denominator.clone(),
            yes_numerator: self.yes_numerator.clone(),
            no_numerator: self.no_numerator.clone(),
            yes_payout_ratio: self.yes_payout_ratio,
            no_payout_ratio: self.no_payout_ratio,
            oracle: self.oracle.clone(),
            question_id: self.question_id.clone(),
            transaction_hash: self.transaction_hash.clone(),
            block_number,
            block_hash: self.block_hash.clone(),
            log_index,
            resolved_at: self.resolved_at,
            raw_payload_hash: self.raw_payload_hash,
            raw_uri: self.raw_uri.clone(),
            provider_revision: self.provider_revision.clone(),
        }
        .validate()
    }

    /// Whether an idempotent retry contains the exact immutable source payload.
    #[must_use]
    pub fn matches(&self, candidate: &NewResolutionObservationInbox) -> bool {
        self.source_checkpoint_hash == candidate.source_checkpoint_hash
            && self.source_id == candidate.source_id
            && self.instrument_key == candidate.instrument_key
            && self.market_id == candidate.market_id
            && self.denominator == candidate.denominator
            && self.yes_numerator == candidate.yes_numerator
            && self.no_numerator == candidate.no_numerator
            && self.yes_payout_ratio == candidate.yes_payout_ratio
            && self.no_payout_ratio == candidate.no_payout_ratio
            && self.oracle == candidate.oracle
            && self.question_id == candidate.question_id
            && self.transaction_hash == candidate.transaction_hash
            && u64::try_from(self.block_number).ok() == Some(candidate.block_number)
            && self.block_hash == candidate.block_hash
            && u64::try_from(self.log_index).ok() == Some(candidate.log_index)
            && self.resolved_at == candidate.resolved_at
            && self.raw_payload_hash == candidate.raw_payload_hash
            && self.raw_uri == candidate.raw_uri
            && self.provider_revision == candidate.provider_revision
    }
}

/// Mutable projection state kept separate from the WORM inbox.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, sea_orm::DerivePartialModel)]
#[sea_orm(entity = "crate::entities::quant_resolution_observation_projection::Entity")]
pub struct ResolutionObservationProjectionInfo {
    pub resolution_observation_id: ResolutionObservationId,
    pub source_checkpoint_hash: ContentHash,
    pub status: ResolutionProjectionStatus,
    pub attempt_count: i32,
    pub claim_owner: Option<WorkerId>,
    pub lease_expires_at: Option<DateTime<Utc>>,
    pub next_attempt_at: Option<DateTime<Utc>>,
    pub last_error: Option<String>,
    pub canonical_fact_hash: Option<ContentHash>,
    pub verified_at: Option<DateTime<Utc>>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

info_from_model!(
    ResolutionObservationProjectionInfo,
    quant_resolution_observation_projection::Model,
    {
        resolution_observation_id,
        source_checkpoint_hash,
        status,
        attempt_count,
        claim_owner,
        lease_expires_at,
        next_attempt_at,
        last_error,
        canonical_fact_hash,
        verified_at,
        created_at,
        updated_at,
    }
);

/// One leased immutable observation ready for projection.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ResolutionProjectionClaim {
    pub observation: ResolutionObservationInboxInfo,
    pub projection: ResolutionObservationProjectionInfo,
}

/// Point-in-time projection coverage used by the feedback truth-freeze barrier.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct ResolutionProjectionBarrier {
    pub cutoff: DateTime<Utc>,
    pub unresolved_count: u64,
    pub quarantined_count: u64,
    pub oldest_unresolved_at: Option<DateTime<Utc>>,
    pub verified_through: DateTime<Utc>,
}

impl ResolutionProjectionBarrier {
    /// Whether every source observation visible at the cutoff is canonical.
    #[must_use]
    pub fn is_complete(self) -> bool {
        self.unresolved_count == 0 && self.verified_through >= self.cutoff
    }
}

/// Result of atomically committing a source page and its cursor.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ResolutionScanCommitOutcome {
    Committed {
        cursor: DomainSourceCursorInfo,
        inserted: u64,
        existing: u64,
    },
    Conflict(DomainSourceCursorInfo),
}

/// Terminal or retry disposition for one owned projection lease.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ResolutionProjectionSettlement {
    Verified {
        canonical_fact_hash: ContentHash,
    },
    Retry {
        retry_at: DateTime<Utc>,
        error: String,
    },
    Quarantined {
        retry_at: DateTime<Utc>,
        error: String,
    },
    Failed {
        error: String,
    },
}

/// Invalid resolution observation or projection lifecycle content.
#[derive(Debug, Error)]
pub enum ResolutionObservationContractError {
    #[error("resolution observation source identity is not the canonical CTF source")]
    InvalidSourceIdentity,
    #[error("resolution observation contains an empty or invalid source field")]
    InvalidSourceContent,
    #[error("binary payout vector must sum to exactly one")]
    InvalidPayoutVector,
    #[error("resolution raw URI or provider revision does not match source lineage")]
    InvalidSourceLineage,
    #[error("database availability must be no earlier than resolution and equal creation time")]
    InvalidAvailability,
    #[error("resolution raw payload hash mismatch: expected {expected}, got {actual}")]
    RawPayloadHashMismatch {
        expected: ContentHash,
        actual: ContentHash,
    },
    #[error(transparent)]
    Hashing(#[from] CanonicalDigestError),
}

#[derive(Serialize)]
struct ResolutionObservationHashInput<'a> {
    source_checkpoint_hash: ContentHash,
    source_id: &'a DomainSourceId,
    instrument_key: &'a DomainInstrumentKey,
    market_id: &'a MarketId,
    denominator: &'a EvmUint256,
    yes_numerator: &'a EvmUint256,
    no_numerator: &'a EvmUint256,
    yes_payout_ratio: PayoutRatio,
    no_payout_ratio: PayoutRatio,
    oracle: &'a EvmAddress,
    question_id: &'a str,
    transaction_hash: &'a EvmTransactionHash,
    block_number: u64,
    block_hash: &'a EvmBlockHash,
    log_index: u64,
    resolved_at: DateTime<Utc>,
    provider_revision: &'a EvmBlockHash,
}

impl<'a> From<&'a NewResolutionObservationInbox> for ResolutionObservationHashInput<'a> {
    fn from(observation: &'a NewResolutionObservationInbox) -> Self {
        Self {
            source_checkpoint_hash: observation.source_checkpoint_hash,
            source_id: &observation.source_id,
            instrument_key: &observation.instrument_key,
            market_id: &observation.market_id,
            denominator: &observation.denominator,
            yes_numerator: &observation.yes_numerator,
            no_numerator: &observation.no_numerator,
            yes_payout_ratio: observation.yes_payout_ratio,
            no_payout_ratio: observation.no_payout_ratio,
            oracle: &observation.oracle,
            question_id: &observation.question_id,
            transaction_hash: &observation.transaction_hash,
            block_number: observation.block_number,
            block_hash: &observation.block_hash,
            log_index: observation.log_index,
            resolved_at: observation.resolved_at,
            provider_revision: &observation.provider_revision,
        }
    }
}
