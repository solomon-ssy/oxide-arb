//! Execution admission-engine contract.

use async_trait::async_trait;
use quant_pivot_error::QuantResult;
use quant_pivot_models::{
    domain::OrderIntentInfo,
    enums::execution::{AdmissionCheckId, AdmissionOutcome},
};
use std::time::Duration;

/// Admission request containing the governed intent and all pre-fetched state.
#[derive(Debug, Clone)]
pub struct AdmissionInput {
    pub order_intent: OrderIntentInfo,
}

/// One admission check result.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AdmissionCheckTrace {
    pub check_id: AdmissionCheckId,
    pub outcome: AdmissionOutcome,
    pub detail: String,
    pub elapsed: Duration,
}

/// Full admission decision with operator/audit trace.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AdmissionDecision {
    pub outcome: AdmissionOutcome,
    pub reason: String,
    pub trace: Vec<AdmissionCheckTrace>,
}

/// Execution admission boundary.
#[async_trait]
pub trait ExecutionAdmissionEngine: Send + Sync {
    async fn evaluate(&self, input: AdmissionInput) -> QuantResult<AdmissionDecision>;
}
