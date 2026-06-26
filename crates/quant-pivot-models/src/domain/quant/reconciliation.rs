//! Execution-order reconciliation persistence DTOs.

use crate::{
    domain::patch::{NullablePatch, Patch},
    enums::execution::ReconciliationResult,
    types::{
        ExecutionOrderId, OrderIntentId, Price, ReconciliationEvidence,
        ReconciliationEvidenceChain, ReconciliationId, Shares, Usd,
    },
};
use chrono::{DateTime, Utc};
use sea_orm::{DeriveIntoActiveModel, DerivePartialModel, FromQueryResult};
use serde::{Deserialize, Serialize};

/// Persisted reconciliation summary for one execution order.
#[derive(Debug, Clone, Serialize, Deserialize, DerivePartialModel, FromQueryResult)]
#[sea_orm(entity = "crate::entities::quant_reconciliation::Entity")]
pub struct ReconciliationInfo {
    pub reconciliation_id: ReconciliationId,
    pub execution_order_id: ExecutionOrderId,
    pub order_intent_id: OrderIntentId,
    pub result: ReconciliationResult,
    pub evidence_json: ReconciliationEvidenceChain,
    pub venue_filled_shares: Option<Shares>,
    pub venue_avg_price: Option<Price>,
    pub discrepancy_usd: Option<Usd>,
    pub resolved_by: Option<String>,
    pub resolved_at: Option<DateTime<Utc>>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

info_from_model!(
    ReconciliationInfo,
    crate::entities::quant_reconciliation::Model,
    {
        reconciliation_id,
        execution_order_id,
        order_intent_id,
        result,
        evidence_json,
        venue_filled_shares,
        venue_avg_price,
        discrepancy_usd,
        resolved_by,
        resolved_at,
        created_at,
        updated_at,
    }
);

/// Insert payload for `quant_reconciliation`.
#[derive(Debug, Clone, Serialize, Deserialize, DeriveIntoActiveModel)]
#[sea_orm(active_model = "crate::entities::quant_reconciliation::ActiveModel")]
pub struct NewReconciliation {
    pub reconciliation_id: ReconciliationId,
    pub execution_order_id: ExecutionOrderId,
    pub order_intent_id: OrderIntentId,
    pub result: ReconciliationResult,
    pub evidence_json: ReconciliationEvidenceChain,
    pub venue_filled_shares: Option<Shares>,
    pub venue_avg_price: Option<Price>,
    pub discrepancy_usd: Option<Usd>,
    pub resolved_by: Option<String>,
    pub resolved_at: Option<DateTime<Utc>>,
}

/// Controlled update payload for resolving or reclassifying a reconciliation.
#[derive(Debug, Clone, Default)]
pub struct ReconciliationPatch {
    pub result: Patch<ReconciliationResult>,
    pub venue_filled_shares: NullablePatch<Shares>,
    pub venue_avg_price: NullablePatch<Price>,
    pub discrepancy_usd: NullablePatch<Usd>,
    pub resolved_by: NullablePatch<String>,
    pub resolved_at: NullablePatch<DateTime<Utc>>,
}

/// Append-only-at-the-repository evidence write intent.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AppendReconciliationEvidence {
    pub evidence: ReconciliationEvidence,
}
