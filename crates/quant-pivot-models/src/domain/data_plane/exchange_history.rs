//! Durable control-plane contracts for finalized exchange-history reconstruction.

use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use sea_orm::{DeriveIntoActiveModel, DerivePartialModel};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::{
    clickhouse::ExecutionParticipantFactRow,
    entities::{
        quant_exchange_history_chunk, quant_exchange_history_plan,
        quant_exchange_history_quarantine,
    },
    enums::{
        clickhouse::{ChExchangeSide, ChExecutionParticipantRole},
        common::Side,
    },
    runtime_config::BuyModelRoute,
    types::{ContentHash, EvmBlockHash, MarketId, Price, Shares, TokenId, Usd},
};

/// Participant role for a canonical economic execution. Every accepted
/// execution has exactly one maker row and one taker row.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ExecutionParticipantRole {
    Maker,
    Taker,
}

/// Participant-aware economic execution consumed by finalized-history
/// features. It carries the N+12 model-availability time, never ingestion time
/// disguised as point-in-time availability.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExecutionParticipantPrint {
    pub execution_id: ContentHash,
    pub market_id: MarketId,
    pub token_id: TokenId,
    pub effective_at: DateTime<Utc>,
    pub observed_at: DateTime<Utc>,
    pub model_available_at: DateTime<Utc>,
    pub participant_address: String,
    pub participant_role: ExecutionParticipantRole,
    pub side: Side,
    pub price: Price,
    pub size_shares: Shares,
    pub notional_usd: Usd,
    pub transaction_hash: String,
    pub availability_policy_hash: ContentHash,
}

impl TryFrom<ExecutionParticipantFactRow> for ExecutionParticipantPrint {
    type Error = String;

    fn try_from(row: ExecutionParticipantFactRow) -> Result<Self, Self::Error> {
        let effective_at = DateTime::from_timestamp_millis(row.effective_at)
            .ok_or_else(|| "execution effective_at is outside chrono range".to_owned())?;
        let observed_at = DateTime::from_timestamp_millis(row.observed_at)
            .ok_or_else(|| "execution observed_at is outside chrono range".to_owned())?;
        let model_available_at = DateTime::from_timestamp_millis(row.model_available_at)
            .ok_or_else(|| "execution model_available_at is outside chrono range".to_owned())?;
        let participant_role = match row.participant_role {
            ChExecutionParticipantRole::Maker => ExecutionParticipantRole::Maker,
            ChExecutionParticipantRole::Taker => ExecutionParticipantRole::Taker,
        };
        let side = match row.side {
            ChExchangeSide::Buy => Side::Buy,
            ChExchangeSide::Sell => Side::Sell,
            ChExchangeSide::Unknown => {
                return Err("accepted execution cannot have an unknown side".to_owned());
            }
        };
        Ok(Self {
            execution_id: row.execution_id.into(),
            market_id: row.market_id,
            token_id: row.token_id,
            effective_at,
            observed_at,
            model_available_at,
            participant_address: row.participant_address,
            participant_role,
            side,
            price: row.price.into(),
            size_shares: row.size_shares.into(),
            notional_usd: row.notional_usd.into(),
            transaction_hash: row.transaction_hash,
            availability_policy_hash: row.availability_policy_hash.into(),
        })
    }
}

impl ExecutionParticipantPrint {
    #[must_use]
    pub const fn participant_notional(&self) -> Decimal {
        self.notional_usd.inner()
    }
}

crate::pg_enum! {
    type_name = "qp_exchange_history_frontier",
    pub enum ExchangeHistoryFrontier {
        Activation => "activation",
        Retention => "retention",
    }
}

crate::pg_enum! {
    type_name = "qp_exchange_history_quarantine_disposition",
    /// Governed proof that an immutable quarantine no longer blocks its range.
    pub enum ExchangeHistoryQuarantineDisposition {
        AcceptedReplacement => "accepted_replacement",
        CanonicalSupersession => "canonical_supersession",
    }
}

crate::pg_enum! {
    type_name = "qp_exchange_history_continuity_basis",
    pub enum ExchangeHistoryContinuityBasis {
        HyperSyncRollbackGuard => "hypersync_rollback_guard",
        HyperSyncBoundaryHeaders => "hypersync_boundary_headers",
    }
}

/// Closed operational stages owned exclusively by the exchange-history worker.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ExchangeHistoryStage {
    StartupProbe,
    IdentitySync,
    Extracting,
    Attesting,
    Projecting,
    ActivationReady,
    Quarantined,
}

/// Cold-start completion budget status; correctness gates always take priority.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ColdStartSloStatus {
    OnTrack,
    Warning,
    Violation,
}

/// Operator-facing, secret-free progress snapshot for the activation frontier.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExchangeHistoryFrontierProgress {
    pub stage: ExchangeHistoryStage,
    pub slo_status: ColdStartSloStatus,
    pub started_at: DateTime<Utc>,
    pub activation_from_block: Option<u64>,
    pub accepted_through_block: Option<u64>,
    pub target_block: Option<u64>,
    pub retention_from_block: Option<u64>,
    pub retention_accepted_from_block: Option<u64>,
    pub retention_through_block: Option<u64>,
    pub crypto_required_from_block: Option<u64>,
    pub weather_required_from_block: Option<u64>,
    pub blocks_processed: u64,
    pub logs_accepted: u64,
    /// Thousandths of a block per second, avoiding floating-point telemetry.
    pub block_rate_milli: u64,
    pub hypersync_retry_count: u64,
    pub attestor_retry_count: u64,
    pub unresolved_count: u64,
    pub quarantine_count: u64,
    pub projected_completion_at: Option<DateTime<Utc>>,
    pub updated_at: DateTime<Utc>,
}

/// Immutable block plan that pins one fresh-boot reconstruction to a finalized
/// Polygon anchor. Restarts must reload this row instead of moving the window
/// with the live chain head.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, DerivePartialModel)]
#[sea_orm(entity = "crate::entities::quant_exchange_history_plan::Entity")]
pub struct ExchangeHistoryPlanInfo {
    pub plan_id: Uuid,
    pub chain_id: i64,
    pub policy_hash: ContentHash,
    pub bootstrap_profile_set_hash: ContentHash,
    pub finalized_anchor_block: i64,
    pub finalized_anchor_hash: EvmBlockHash,
    pub finalized_anchor_timestamp: i64,
    pub activation_from_block: i64,
    pub activation_through_block: i64,
    pub crypto_required_from_block: i64,
    pub weather_required_from_block: i64,
    pub retention_from_block: i64,
    pub retention_through_block: i64,
    pub created_at: DateTime<Utc>,
}

impl ExchangeHistoryPlanInfo {
    /// Exact inclusive lower history boundary required by one serving route.
    #[must_use]
    pub const fn required_from_block(&self, route: BuyModelRoute) -> i64 {
        match route {
            BuyModelRoute::Pooled => self.activation_from_block,
            BuyModelRoute::Crypto => self.crypto_required_from_block,
            BuyModelRoute::Weather => self.weather_required_from_block,
        }
    }
}

info_from_model!(ExchangeHistoryPlanInfo, quant_exchange_history_plan::Model, {
    plan_id, chain_id, policy_hash, bootstrap_profile_set_hash, finalized_anchor_block, finalized_anchor_hash,
    finalized_anchor_timestamp, activation_from_block, activation_through_block,
    crypto_required_from_block, weather_required_from_block, retention_from_block,
    retention_through_block, created_at,
});

#[derive(Debug, Clone, Serialize, Deserialize, DeriveIntoActiveModel)]
#[sea_orm(
    active_model = "crate::entities::quant_exchange_history_plan::ActiveModel",
    exhaustive
)]
pub struct NewExchangeHistoryPlan {
    pub plan_id: Uuid,
    pub chain_id: i64,
    pub policy_hash: ContentHash,
    pub bootstrap_profile_set_hash: ContentHash,
    pub finalized_anchor_block: i64,
    pub finalized_anchor_hash: EvmBlockHash,
    pub finalized_anchor_timestamp: i64,
    pub activation_from_block: i64,
    pub activation_through_block: i64,
    pub crypto_required_from_block: i64,
    pub weather_required_from_block: i64,
    pub retention_from_block: i64,
    pub retention_through_block: i64,
    pub created_at: DateTime<Utc>,
}

impl ExchangeHistoryFrontierProgress {
    #[must_use]
    pub const fn fresh_boot(now: DateTime<Utc>) -> Self {
        Self {
            stage: ExchangeHistoryStage::StartupProbe,
            slo_status: ColdStartSloStatus::OnTrack,
            started_at: now,
            activation_from_block: None,
            accepted_through_block: None,
            target_block: None,
            retention_from_block: None,
            retention_accepted_from_block: None,
            retention_through_block: None,
            crypto_required_from_block: None,
            weather_required_from_block: None,
            blocks_processed: 0,
            logs_accepted: 0,
            block_rate_milli: 0,
            hypersync_retry_count: 0,
            attestor_retry_count: 0,
            unresolved_count: 0,
            quarantine_count: 0,
            projected_completion_at: None,
            updated_at: now,
        }
    }
}

crate::pg_enum! {
    type_name = "qp_exchange_history_chunk_status",
    pub enum ExchangeHistoryChunkStatus {
        Planned => "planned",
        Extracting => "extracting",
        Attesting => "attesting",
        Projecting => "projecting",
        Accepted => "accepted",
        Rewound => "rewound",
        Quarantined => "quarantined",
    }
}

crate::pg_enum! {
    type_name = "qp_exchange_history_quarantine_reason",
    pub enum ExchangeHistoryQuarantineReason {
        ProviderMismatch => "provider_mismatch",
        DecodeFailure => "decode_failure",
        UnknownToken => "unknown_token",
        MissingCorrelation => "missing_correlation",
        ContinuityMismatch => "continuity_mismatch",
        ParentHashMismatch => "parent_hash_mismatch",
        ContractMismatch => "contract_mismatch",
        ArchiveProbeFailure => "archive_probe_failure",
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, DerivePartialModel)]
#[sea_orm(entity = "crate::entities::quant_exchange_history_chunk::Entity")]
pub struct ExchangeHistoryChunkInfo {
    pub chunk_id: Uuid,
    pub frontier: ExchangeHistoryFrontier,
    pub from_block: i64,
    pub to_block: i64,
    pub status: ExchangeHistoryChunkStatus,
    pub attempt_count: i32,
    pub hypersync_count: Option<i64>,
    pub attestor_count: Option<i64>,
    pub hypersync_digest: Option<ContentHash>,
    pub attestor_digest: Option<ContentHash>,
    pub first_block_hash: Option<EvmBlockHash>,
    pub last_block_hash: Option<EvmBlockHash>,
    pub archive_height: Option<i64>,
    pub continuity_basis: Option<ExchangeHistoryContinuityBasis>,
    pub continuity_block: Option<i64>,
    pub continuity_hash: Option<EvmBlockHash>,
    pub effective_through_at: Option<DateTime<Utc>>,
    pub accepted_at: Option<DateTime<Utc>>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

info_from_model!(ExchangeHistoryChunkInfo, quant_exchange_history_chunk::Model, {
    chunk_id, frontier, from_block, to_block, status, attempt_count, hypersync_count,
    attestor_count, hypersync_digest, attestor_digest, first_block_hash, last_block_hash,
    archive_height, continuity_basis, continuity_block, continuity_hash, effective_through_at,
    accepted_at, created_at, updated_at,
});

#[derive(Debug, Clone, Serialize, Deserialize, DeriveIntoActiveModel)]
#[sea_orm(
    active_model = "crate::entities::quant_exchange_history_chunk::ActiveModel",
    exhaustive
)]
pub struct NewExchangeHistoryChunk {
    pub chunk_id: Uuid,
    pub frontier: ExchangeHistoryFrontier,
    pub from_block: i64,
    pub to_block: i64,
    pub status: ExchangeHistoryChunkStatus,
    pub attempt_count: i32,
    pub hypersync_count: Option<i64>,
    pub attestor_count: Option<i64>,
    pub hypersync_digest: Option<ContentHash>,
    pub attestor_digest: Option<ContentHash>,
    pub first_block_hash: Option<EvmBlockHash>,
    pub last_block_hash: Option<EvmBlockHash>,
    pub archive_height: Option<i64>,
    pub continuity_basis: Option<ExchangeHistoryContinuityBasis>,
    pub continuity_block: Option<i64>,
    pub continuity_hash: Option<EvmBlockHash>,
    pub effective_through_at: Option<DateTime<Utc>>,
    pub accepted_at: Option<DateTime<Utc>>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, DerivePartialModel)]
#[sea_orm(entity = "crate::entities::quant_exchange_history_quarantine::Entity")]
pub struct ExchangeHistoryQuarantineInfo {
    pub quarantine_id: Uuid,
    pub chunk_id: Uuid,
    pub reason: ExchangeHistoryQuarantineReason,
    pub evidence_hash: ContentHash,
    pub detail: String,
    pub quarantined_at: DateTime<Utc>,
}

info_from_model!(ExchangeHistoryQuarantineInfo, quant_exchange_history_quarantine::Model, {
    quarantine_id, chunk_id, reason, evidence_hash, detail, quarantined_at,
});

#[derive(Debug, Clone, Serialize, Deserialize, DeriveIntoActiveModel)]
#[sea_orm(
    active_model = "crate::entities::quant_exchange_history_quarantine::ActiveModel",
    exhaustive
)]
pub struct NewExchangeHistoryQuarantine {
    pub quarantine_id: Uuid,
    pub chunk_id: Uuid,
    pub reason: ExchangeHistoryQuarantineReason,
    pub evidence_hash: ContentHash,
    pub detail: String,
    pub quarantined_at: DateTime<Utc>,
}

/// Append-only resolution proof for one immutable quarantine record.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, DerivePartialModel)]
#[sea_orm(entity = "crate::entities::quant_exchange_history_quarantine_resolution::Entity")]
pub struct ExchangeHistoryQuarantineResolutionInfo {
    pub resolution_id: Uuid,
    pub quarantine_id: Uuid,
    pub disposition: ExchangeHistoryQuarantineDisposition,
    pub replacement_chunk_id: Uuid,
    pub evidence_hash: ContentHash,
    pub actor: String,
    pub detail: String,
    pub resolved_at: DateTime<Utc>,
}

info_from_model!(
    ExchangeHistoryQuarantineResolutionInfo,
    crate::entities::quant_exchange_history_quarantine_resolution::Model,
    {
        resolution_id,
        quarantine_id,
        disposition,
        replacement_chunk_id,
        evidence_hash,
        actor,
        detail,
        resolved_at,
    }
);

#[derive(Debug, Clone, Serialize, Deserialize, DeriveIntoActiveModel)]
#[sea_orm(
    active_model = "crate::entities::quant_exchange_history_quarantine_resolution::ActiveModel",
    exhaustive
)]
pub struct NewExchangeHistoryQuarantineResolution {
    pub resolution_id: Uuid,
    pub quarantine_id: Uuid,
    pub disposition: ExchangeHistoryQuarantineDisposition,
    pub replacement_chunk_id: Uuid,
    pub evidence_hash: ContentHash,
    pub actor: String,
    pub detail: String,
    pub resolved_at: DateTime<Utc>,
}

/// Governed command that resolves all active quarantine evidence for one
/// accepted, exact history range.
#[derive(Debug, Clone)]
pub struct ResolveAcceptedHistoryRange {
    pub frontier: ExchangeHistoryFrontier,
    pub from_block: i64,
    pub to_block: i64,
    pub replacement_chunk_id: Uuid,
    pub evidence_hash: ContentHash,
    pub actor: String,
    pub resolved_at: DateTime<Utc>,
}
