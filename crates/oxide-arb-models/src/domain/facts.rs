//! Fact data-plane Postgres DTOs.

use crate::{
    enums::{
        common::Side,
        control_factor::ControlFactorType,
        fact::{
            BalanceSnapshotSource, ExitAction, ExitExecutionOutcome, ExitOrderType, ExitPlanStatus,
            ExitTriggerType, ShadowDecisionType, UnwindAuditEventType,
        },
    },
    types::{
        BalanceSnapshotId, EventId, ExitExecutionId, ExitPlanId, FactorPublicationId, MarketId,
        MaterializationRunId, OpportunityId, PositionId, Price, ShadowDecisionId, Shares,
        TokenBalanceSnapshotId, TokenId, TrainingDatasetId, UnwindAuditId, Usd,
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
#[sea_orm(entity = "crate::entities::token_balance_snapshot::Entity")]
pub struct TokenBalanceSnapshotInfo {
    pub token_balance_snapshot_id: TokenBalanceSnapshotId,
    pub holder_address: String,
    pub market_id: MarketId,
    pub token_id: TokenId,
    pub side: Side,
    pub internal_shares: Shares,
    pub external_shares: Option<Shares>,
    pub drift_shares: Option<Shares>,
    pub source: BalanceSnapshotSource,
    pub block_number: Option<i64>,
    pub reconciliation_report_id: Option<i64>,
    pub observed_at: DateTime<Utc>,
    pub created_at: DateTime<Utc>,
}

info_from_model!(
    TokenBalanceSnapshotInfo,
    crate::entities::token_balance_snapshot::Model,
    {
        token_balance_snapshot_id, holder_address, market_id, token_id, side,
        internal_shares, external_shares, drift_shares, source, block_number,
        reconciliation_report_id, observed_at, created_at,
    }
);

#[derive(Debug, Clone, DeriveIntoActiveModel)]
#[sea_orm(active_model = "crate::entities::token_balance_snapshot::ActiveModel")]
pub struct NewTokenBalanceSnapshot {
    pub token_balance_snapshot_id: TokenBalanceSnapshotId,
    pub holder_address: String,
    pub market_id: MarketId,
    pub token_id: TokenId,
    pub side: Side,
    pub internal_shares: Shares,
    pub external_shares: Option<Shares>,
    pub drift_shares: Option<Shares>,
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
    pub opportunity_id: Option<OpportunityId>,
    pub event_id: Option<EventId>,
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
    pub opportunity_id: Option<OpportunityId>,
    pub event_id: Option<EventId>,
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

#[derive(Debug, Clone, Serialize, Deserialize, DerivePartialModel, FromQueryResult)]
#[sea_orm(entity = "crate::entities::position_exit_plan::Entity")]
pub struct PositionExitPlanInfo {
    pub exit_plan_id: ExitPlanId,
    pub position_id: PositionId,
    pub market_id: MarketId,
    pub token_id: TokenId,
    pub trigger_type: ExitTriggerType,
    pub action: ExitAction,
    pub target_shares: Shares,
    pub min_exit_price: Price,
    pub reason: serde_json::Value,
    pub policy_version: String,
    pub created_by: String,
    pub status: ExitPlanStatus,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

info_from_model!(PositionExitPlanInfo, crate::entities::position_exit_plan::Model, {
    exit_plan_id, position_id, market_id, token_id, trigger_type, action,
    target_shares, min_exit_price, reason, policy_version, created_by, status,
    created_at, updated_at,
});

#[derive(Debug, Clone, DeriveIntoActiveModel)]
#[sea_orm(active_model = "crate::entities::position_exit_plan::ActiveModel")]
pub struct NewPositionExitPlan {
    pub exit_plan_id: ExitPlanId,
    pub position_id: PositionId,
    pub market_id: MarketId,
    pub token_id: TokenId,
    pub trigger_type: ExitTriggerType,
    pub action: ExitAction,
    pub target_shares: Shares,
    pub min_exit_price: Price,
    pub reason: serde_json::Value,
    pub policy_version: String,
    pub created_by: String,
    pub status: ExitPlanStatus,
}

#[derive(Debug, Clone, Serialize, Deserialize, DerivePartialModel, FromQueryResult)]
#[sea_orm(entity = "crate::entities::position_exit_execution::Entity")]
pub struct PositionExitExecutionInfo {
    pub exit_execution_id: ExitExecutionId,
    pub exit_plan_id: ExitPlanId,
    pub order_type: ExitOrderType,
    pub requested_shares: Shares,
    pub filled_shares: Shares,
    pub avg_exit_price: Option<Price>,
    pub fee_usd: Usd,
    pub realized_exit_pnl_usd: Usd,
    pub outcome: ExitExecutionOutcome,
    pub failure_reason: Option<String>,
    pub submitted_at: Option<DateTime<Utc>>,
    pub completed_at: Option<DateTime<Utc>>,
    pub created_at: DateTime<Utc>,
}

info_from_model!(
    PositionExitExecutionInfo,
    crate::entities::position_exit_execution::Model,
    {
        exit_execution_id, exit_plan_id, order_type, requested_shares, filled_shares,
        avg_exit_price, fee_usd, realized_exit_pnl_usd, outcome, failure_reason,
        submitted_at, completed_at, created_at,
    }
);

#[derive(Debug, Clone, DeriveIntoActiveModel)]
#[sea_orm(active_model = "crate::entities::position_exit_execution::ActiveModel")]
pub struct NewPositionExitExecution {
    pub exit_execution_id: ExitExecutionId,
    pub exit_plan_id: ExitPlanId,
    pub order_type: ExitOrderType,
    pub requested_shares: Shares,
    pub filled_shares: Shares,
    pub avg_exit_price: Option<Price>,
    pub fee_usd: Usd,
    pub realized_exit_pnl_usd: Usd,
    pub outcome: ExitExecutionOutcome,
    pub failure_reason: Option<String>,
    pub submitted_at: Option<DateTime<Utc>>,
    pub completed_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, Serialize, Deserialize, DerivePartialModel, FromQueryResult)]
#[sea_orm(entity = "crate::entities::position_unwind_audit::Entity")]
pub struct PositionUnwindAuditInfo {
    pub unwind_audit_id: UnwindAuditId,
    pub position_id: PositionId,
    pub exit_plan_id: Option<ExitPlanId>,
    pub exit_execution_id: Option<ExitExecutionId>,
    pub event_type: UnwindAuditEventType,
    pub before_position: serde_json::Value,
    pub after_position: serde_json::Value,
    pub book_context: serde_json::Value,
    pub token_balance_context: serde_json::Value,
    pub reason: String,
    pub actor: String,
    pub created_at: DateTime<Utc>,
}

info_from_model!(
    PositionUnwindAuditInfo,
    crate::entities::position_unwind_audit::Model,
    {
        unwind_audit_id, position_id, exit_plan_id, exit_execution_id, event_type,
        before_position, after_position, book_context, token_balance_context, reason,
        actor, created_at,
    }
);

#[derive(Debug, Clone, DeriveIntoActiveModel)]
#[sea_orm(active_model = "crate::entities::position_unwind_audit::ActiveModel")]
pub struct NewPositionUnwindAudit {
    pub unwind_audit_id: UnwindAuditId,
    pub position_id: PositionId,
    pub exit_plan_id: Option<ExitPlanId>,
    pub exit_execution_id: Option<ExitExecutionId>,
    pub event_type: UnwindAuditEventType,
    pub before_position: serde_json::Value,
    pub after_position: serde_json::Value,
    pub book_context: serde_json::Value,
    pub token_balance_context: serde_json::Value,
    pub reason: String,
    pub actor: String,
}
