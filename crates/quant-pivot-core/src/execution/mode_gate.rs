//! Runtime-mode execution gate contract.

use async_trait::async_trait;
use quant_pivot_error::QuantResult;
use quant_pivot_models::{
    domain::RecommendationInfo,
    enums::{execution::ModeDenialReason, quant::QuantRuntimeMode},
    types::ContentHash,
};
use std::time::Duration;

/// Policy result for turning a recommendation into an order intent.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum IntentPolicyDecision {
    /// Runtime is report-only; no intent should be created.
    ReportOnly,
    /// Semi-auto intent must wait for an operator approval.
    RequiresApproval {
        required_role: String,
        approval_ttl: Duration,
    },
    /// Auto policy approved the intent.
    ApprovedByPolicy {
        policy_id: String,
        policy_hash: Option<ContentHash>,
        reason: String,
    },
    /// Runtime or recommendation state denies intent creation.
    Denied { reason: ModeDenialReason },
}

/// Runtime mode gate used before intent creation.
#[async_trait]
pub trait RuntimeModeGate: Send + Sync {
    async fn evaluate_intent_policy(
        &self,
        mode: QuantRuntimeMode,
        recommendation: &RecommendationInfo,
    ) -> QuantResult<IntentPolicyDecision>;
}
