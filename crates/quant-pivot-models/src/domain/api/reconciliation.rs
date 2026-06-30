//! Reconciliation HTTP contract types.

use crate::{
    domain::{
        ExecutionOrderInfo, ExecutionOrderView, ExecutionRecoverySummary, ReconciliationInfo,
        pagination::PageRequest,
    },
    enums::execution::ReconciliationResult,
    types::{
        ExecutionOrderId, OrderIntentId, Price, ReconciliationEvidenceChain, ReconciliationId,
        Shares, Usd,
    },
};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use validator::Validate;

/// Outbound projection of one execution-order reconciliation row.
#[derive(Debug, Clone, Serialize)]
pub struct ReconciliationView {
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

impl From<ReconciliationInfo> for ReconciliationView {
    fn from(info: ReconciliationInfo) -> Self {
        Self {
            reconciliation_id: info.reconciliation_id,
            execution_order_id: info.execution_order_id,
            order_intent_id: info.order_intent_id,
            result: info.result,
            evidence_json: info.evidence_json,
            venue_filled_shares: info.venue_filled_shares,
            venue_avg_price: info.venue_avg_price,
            discrepancy_usd: info.discrepancy_usd,
            resolved_by: info.resolved_by,
            resolved_at: info.resolved_at,
            created_at: info.created_at,
            updated_at: info.updated_at,
        }
    }
}

/// Paginated filter for listing reconciliation rows.
#[derive(Debug, Clone, Default, Deserialize)]
pub struct ReconciliationListQuery {
    pub result: Option<ReconciliationResult>,
    /// When `Some(true)`, only rows with `resolved_at` set; when `Some(false)`, only unresolved.
    pub resolved: Option<bool>,
    pub execution_order_id: Option<ExecutionOrderId>,
    pub order_intent_id: Option<OrderIntentId>,
    pub from: Option<DateTime<Utc>>,
    pub to: Option<DateTime<Utc>>,
    #[serde(flatten)]
    pub page: PageRequest,
}

impl ReconciliationListQuery {
    #[must_use]
    pub fn normalized(self) -> Self {
        Self {
            page: self.page.normalized(),
            ..self
        }
    }
}

/// Inbound body for `POST /quant/reconciliations/{id}/resolve`.
#[derive(Debug, Clone, Deserialize, Validate)]
pub struct ResolveReconciliationRequest {
    pub result: ReconciliationResult,
    pub filled_shares: Option<Shares>,
    pub avg_price: Option<Price>,
    #[validate(length(min = 1, max = 2048))]
    pub reason: String,
}

/// Outbound response after operator reconciliation resolve.
#[derive(Debug, Clone, Serialize)]
pub struct ResolveReconciliationResponse {
    pub execution_order: ExecutionOrderView,
    pub recovery: ExecutionRecoverySummary,
}

/// Core command for operator reconciliation resolve (web → port).
#[derive(Debug, Clone)]
pub struct ResolveReconciliationCommand {
    pub reconciliation_id: ReconciliationId,
    pub result: ReconciliationResult,
    pub filled_shares: Option<Shares>,
    pub avg_price: Option<Price>,
    pub operator: String,
    pub reason: String,
}

/// Operator resolve outcome returned by the reconciliation port.
#[derive(Debug, Clone)]
pub struct ResolveReconciliationOutcome {
    pub execution_order: ExecutionOrderInfo,
    pub recovery: ExecutionRecoverySummary,
}
