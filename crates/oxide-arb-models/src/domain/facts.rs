//! Fact data-plane Postgres DTOs.

use crate::{
    enums::{
        control_factor::ControlFactorType,
        fact::{BalanceSnapshotSource, ShadowDecisionType},
    },
    types::{
        BalanceSnapshotId, EventId, FactorPublicationId, MarketId, MaterializationRunId,
        OpportunityId, ShadowDecisionId, TrainingDatasetId, Usd,
    },
};
use chrono::{DateTime, Utc};
use sea_orm::{DeriveIntoActiveModel, DerivePartialModel, FromQueryResult};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, DerivePartialModel, FromQueryResult)]
#[sea_orm(entity = "crate::entities::balance_snapshot::Entity")]
pub struct BalanceSnapshotInfo {
    pub balance_snapshot_id: BalanceSnapshotId,
    pub holder_address: String,
    pub internal_available_usd: Usd,
    pub internal_reserved_usd: Usd,
    pub external_available_usd: Usd,
    pub external_locked_usd: Usd,
    pub drift_usd: Usd,
    pub source: BalanceSnapshotSource,
    pub block_number: Option<i64>,
    pub reconciliation_report_id: Option<i64>,
    pub observed_at: DateTime<Utc>,
    pub created_at: DateTime<Utc>,
}

info_from_model!(BalanceSnapshotInfo, crate::entities::balance_snapshot::Model, {
    balance_snapshot_id, holder_address, internal_available_usd, internal_reserved_usd,
    external_available_usd, external_locked_usd, drift_usd, source, block_number,
    reconciliation_report_id, observed_at, created_at,
});

#[derive(Debug, Clone, DeriveIntoActiveModel)]
#[sea_orm(active_model = "crate::entities::balance_snapshot::ActiveModel")]
pub struct NewBalanceSnapshot {
    pub balance_snapshot_id: BalanceSnapshotId,
    pub holder_address: String,
    pub internal_available_usd: Usd,
    pub internal_reserved_usd: Usd,
    pub external_available_usd: Usd,
    pub external_locked_usd: Usd,
    pub drift_usd: Usd,
    pub source: BalanceSnapshotSource,
    pub block_number: Option<i64>,
    pub reconciliation_report_id: Option<i64>,
    pub observed_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize, DerivePartialModel, FromQueryResult)]
#[sea_orm(entity = "crate::entities::control_factor_training_dataset::Entity")]
pub struct ControlFactorTrainingDatasetInfo {
    pub dataset_id: TrainingDatasetId,
    pub materialization_run_id: MaterializationRunId,
    pub factor_type: ControlFactorType,
    pub window_from: DateTime<Utc>,
    pub window_to: DateTime<Utc>,
    pub entity_count: i32,
    pub example_count: i32,
    pub label_count: i32,
    pub dataset_hash: String,
    pub feature_schema_hash: String,
    pub label_schema_hash: String,
    pub storage_uri: Option<String>,
    pub created_at: DateTime<Utc>,
}

info_from_model!(
    ControlFactorTrainingDatasetInfo,
    crate::entities::control_factor_training_dataset::Model,
    {
        dataset_id, materialization_run_id, factor_type, window_from, window_to,
        entity_count, example_count, label_count, dataset_hash, feature_schema_hash,
        label_schema_hash, storage_uri, created_at,
    }
);

#[derive(Debug, Clone, DeriveIntoActiveModel)]
#[sea_orm(active_model = "crate::entities::control_factor_training_dataset::ActiveModel")]
pub struct NewControlFactorTrainingDataset {
    pub dataset_id: TrainingDatasetId,
    pub materialization_run_id: MaterializationRunId,
    pub factor_type: ControlFactorType,
    pub window_from: DateTime<Utc>,
    pub window_to: DateTime<Utc>,
    pub entity_count: i32,
    pub example_count: i32,
    pub label_count: i32,
    pub dataset_hash: String,
    pub feature_schema_hash: String,
    pub label_schema_hash: String,
    pub storage_uri: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, DerivePartialModel, FromQueryResult)]
#[sea_orm(entity = "crate::entities::control_factor_shadow_decision::Entity")]
pub struct ControlFactorShadowDecisionInfo {
    pub shadow_decision_id: ShadowDecisionId,
    pub publication_id: FactorPublicationId,
    pub opportunity_id: OpportunityId,
    pub event_id: EventId,
    pub market_id: MarketId,
    pub decision_type: ShadowDecisionType,
    pub baseline_decision: serde_json::Value,
    pub shadow_decision: serde_json::Value,
    pub delta: serde_json::Value,
    pub affected_factor_ids: serde_json::Value,
    pub decided_at: DateTime<Utc>,
    pub created_at: DateTime<Utc>,
}

info_from_model!(
    ControlFactorShadowDecisionInfo,
    crate::entities::control_factor_shadow_decision::Model,
    {
        shadow_decision_id, publication_id, opportunity_id, event_id, market_id, decision_type,
        baseline_decision, shadow_decision, delta, affected_factor_ids, decided_at, created_at,
    }
);

#[derive(Debug, Clone, DeriveIntoActiveModel)]
#[sea_orm(active_model = "crate::entities::control_factor_shadow_decision::ActiveModel")]
pub struct NewControlFactorShadowDecision {
    pub shadow_decision_id: ShadowDecisionId,
    pub publication_id: FactorPublicationId,
    pub opportunity_id: OpportunityId,
    pub event_id: EventId,
    pub market_id: MarketId,
    pub decision_type: ShadowDecisionType,
    pub baseline_decision: serde_json::Value,
    pub shadow_decision: serde_json::Value,
    pub delta: serde_json::Value,
    pub affected_factor_ids: serde_json::Value,
    pub decided_at: DateTime<Utc>,
}

/// Aggregate counts over a publication's shadow-decision window.
///
/// Computed by the storage layer with a single `GROUP BY decision_type` query
/// plus a `COUNT(DISTINCT market_id)`. Delta percentile distributions are
/// intentionally **not** part of this aggregate; the promotion-review consumer
/// derives those from the raw `list_shadow_decisions` rows.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ShadowDecisionAggregate {
    pub publication_id: FactorPublicationId,
    pub total: u64,
    pub would_reject: u64,
    pub would_size: u64,
    pub would_score: u64,
    pub no_effect: u64,
    pub distinct_markets: u64,
}

impl ShadowDecisionAggregate {
    /// Builds an empty aggregate for a publication with no decisions in window.
    #[must_use]
    pub const fn empty(publication_id: FactorPublicationId) -> Self {
        Self {
            publication_id,
            total: 0,
            would_reject: 0,
            would_size: 0,
            would_score: 0,
            no_effect: 0,
            distinct_markets: 0,
        }
    }

    /// Accumulates one `(decision_type, count)` bucket and bumps the running total.
    pub const fn add_bucket(&mut self, decision_type: ShadowDecisionType, count: u64) {
        match decision_type {
            ShadowDecisionType::WouldReject => self.would_reject = count,
            ShadowDecisionType::WouldSize => self.would_size = count,
            ShadowDecisionType::WouldScore => self.would_score = count,
            ShadowDecisionType::NoEffect => self.no_effect = count,
        }
        self.total = self.total.saturating_add(count);
    }
}
